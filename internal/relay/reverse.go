package relay

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"github.com/logoscore/logos-golang-channel-core/v3/pkg/connclient"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/syncclient"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Reverse kinds (docs: contracts/channel-core-connections/): the channel
// listens, and minions dial it, speaking the same frames as a
// bind-persistent connection's.
//
//   - reverse-persistent: the connection stays open. The minion's first
//     frame opens its socket to core (kind reverse-persistent, the
//     minion's address as peer); frames go both ways until either side
//     closes, which closes the other.
//   - reverse-call: one exchange per connection. The minion sends one
//     frame, the channel makes the sync call with it, writes core's reply
//     back as one frame under the same id (the id alone, no sealed
//     message, when core has nothing for the minion: the sync call's 204),
//     and closes. A message core does not take, or a sync that fails,
//     closes the connection without a reply, as the HTTP channel answers
//     its 404: the minion learns nothing from it.

// FirstFrameWait bounds how long a minion that dialed may take to send its
// first frame (reverse-persistent) or its message (reverse-call).
var FirstFrameWait = 30 * time.Second

// reverseKeepalive is a reverse connection's TCP keepalive.
const reverseKeepalive = 30 * time.Second

// Sync is how a reverse-call's message reaches core: the sync call.
type Sync interface {
	Sync(ctx context.Context, in protocol.InboundMinionMessage) (protocol.OutboundMinionMessage, error)
}

// WithSync gives the relay core's sync call, its replies within timeout.
func (r *Relay) WithSync(s Sync, timeout time.Duration) *Relay {
	r.sync, r.syncTimeout = s, timeout
	return r
}

// NewSync is core's sync call under coreURL.
func NewSync(coreURL string) Sync { return syncclient.NewHTTPClient(coreURL, nil) }

// Listen takes the minions that dial addr, of kind (reverse-persistent or
// reverse-call), until ctx ends; it returns once listening, or why it
// cannot.
func (r *Relay) Listen(ctx context.Context, kind, addr string) (net.Addr, error) {
	if kind != protocol.KindReversePersistent && kind != protocol.KindReverseCall {
		return nil, fmt.Errorf("the channel listens for reverse kinds, not %s", kind)
	}
	if kind == protocol.KindReverseCall && r.sync == nil {
		return nil, errors.New("reverse-call needs core's sync call")
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("accept", "kind", kind, "addr", addr, "err", err)
				}
				return
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetKeepAlive(true)
				_ = tc.SetKeepAlivePeriod(reverseKeepalive)
			}
			if kind == protocol.KindReverseCall {
				go r.call(c)
			} else {
				go r.accept(ctx, c)
			}
		}
	}()
	slog.Info("listening for minions", "kind", kind, "addr", ln.Addr().String())
	return ln.Addr(), nil
}

// remotePeer is the address a minion dialed from, as core keeps a peer:
// an IP address, unmapped.
func remotePeer(c net.Conn) string {
	ap, err := netip.ParseAddrPort(c.RemoteAddr().String())
	if err != nil {
		return ""
	}
	return ap.Addr().Unmap().String()
}

// admit reports whether a new connection may come in: not while draining,
// nor past the connections the channel holds at once.
func (r *Relay) admit() error {
	if r.Draining() {
		return errors.New("the channel is draining")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.conns)+int(r.inCalls) >= r.max {
		return fmt.Errorf("the channel has %d connections open, its most", r.max)
	}
	return nil
}

// accept carries a reverse-persistent connection: its first frame opens
// its socket to core, then frames go both ways.
func (r *Relay) accept(ctx context.Context, tcp net.Conn) {
	peer := remotePeer(tcp)
	if err := r.admit(); err != nil {
		slog.Info("reverse connection refused", "peer", peer, "why", err.Error())
		_ = tcp.Close()
		return
	}
	reader := bufio.NewReaderSize(tcp, 64<<10)
	_ = tcp.SetReadDeadline(time.Now().Add(FirstFrameWait))
	id, data, err := ReadFrame(reader)
	_ = tcp.SetReadDeadline(time.Time{})
	if err != nil {
		slog.Info("reverse connection closed before its first frame", "peer", peer, "why", readWhy(err))
		_ = tcp.Close()
		return
	}
	core, err := connclient.Dial(ctx, connclient.Options{BaseURL: r.coreURL, Instance: r.instance,
		Kind: protocol.KindReversePersistent, Peer: peer, MaxFrame: MaxFrame + 64})
	if err != nil {
		slog.Info("reverse connection: core did not take its socket", "peer", peer, "err", err)
		_ = tcp.Close()
		return
	}
	if err := core.Send(ctx, protocol.ConnFrame{ID: id, Data: data}); err != nil {
		slog.Info("reverse connection: its first frame did not go", "peer", peer, "err", err)
		_ = core.Close()
		_ = tcp.Close()
		return
	}
	r.accepted.Add(1)
	c := &conn{tcp: tcp, core: core, done: make(chan struct{}), addr: peer, reader: reader}
	r.mu.Lock()
	r.conns[c] = struct{}{}
	r.mu.Unlock()
	go func() {
		<-c.done
		r.mu.Lock()
		delete(r.conns, c)
		r.mu.Unlock()
	}()
	go c.fromMinion()
	go c.fromCore()
	slog.Info("reverse connection open", "peer", peer)
}

// call carries one reverse-call exchange: the minion's message through the
// sync call, core's reply back.
func (r *Relay) call(tcp net.Conn) {
	defer func() { _ = tcp.Close() }()
	peer := remotePeer(tcp)
	if err := r.admit(); err != nil {
		slog.Info("reverse call refused", "peer", peer, "why", err.Error())
		return
	}
	r.mu.Lock()
	r.inCalls++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.inCalls--
		r.mu.Unlock()
	}()
	_ = tcp.SetReadDeadline(time.Now().Add(FirstFrameWait))
	id, data, err := ReadFrame(bufio.NewReaderSize(tcp, 64<<10))
	if err != nil {
		slog.Info("reverse call closed before its message", "peer", peer, "why", readWhy(err))
		return
	}
	r.calls.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), r.syncTimeout)
	defer cancel()
	start := time.Now()
	out, err := r.sync.Sync(ctx, protocol.InboundMinionMessage{MessageID: protocol.NewID(), BuildID: uuidString(id),
		ChannelInstance: r.instance, PeerAddress: peer, Data: data})
	if err != nil {
		// As the HTTP channel's 404: no reply at all.
		r.callsFailed.Add(1)
		slog.Info("reverse call: core did not take it", "peer", peer, "err", err, "took", time.Since(start).Round(time.Millisecond))
		return
	}
	_ = tcp.SetWriteDeadline(time.Now().Add(writeWait))
	if err := WriteReply(tcp, id, out.Data); err != nil {
		slog.Info("reverse call: the reply did not go", "peer", peer, "err", err)
		return
	}
	slog.Debug("reverse call answered", "peer", peer, "bytes", len(out.Data), "took", time.Since(start).Round(time.Millisecond))
}

// WriteReply writes core's reply to a reverse call under id: the sealed
// message, or the id alone when core has nothing for the minion.
func WriteReply(w io.Writer, id, data []byte) error {
	return WriteFrame(w, id, data)
}

// uuidString is a raw id as the sync call's headers carry it.
func uuidString(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
