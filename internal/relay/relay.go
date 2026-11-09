// Package relay opens the bind-persistent connections core asks for over
// the control link (docs: contracts/channel-core-connections/,
// bind-persistent connections): it dials the minion's listener, opens the
// connection's socket to core naming the open, and carries messages both
// ways until either side closes, which closes the other.
//
// On the TCP connection each message is one frame, both ways:
//
//	[length: 4 bytes, big-endian][id: 16 bytes][sealed message]
//
// length counting the id and the message. The minion speaks first, as a
// process does (a stage, or a hello or check-in under its session). The
// channel never reads the sealed message.
package relay

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/logoscore/logos-golang-channel-core/v3/pkg/connclient"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/linkclient"
	"github.com/logoscore/logos-golang-protocol/protocol"
	"golang.org/x/net/proxy"
)

// MaxFrame bounds a frame's length: an id and the largest sealed message,
// with room to spare.
const MaxFrame = 16 + protocol.MaxMinionMessageSize + 4096

// writeWait bounds writing one frame to the minion.
const writeWait = 30 * time.Second

// Stats are the relay's counters: Open the connections open, of every
// kind; Opens and OpensFailed core's opens (bind-persistent); Accepted the
// reverse-persistent connections taken, Calls and CallsFailed the
// reverse-call exchanges and those core did not take.
type Stats struct {
	Open, Opens, OpensFailed int64
	Accepted, Calls          int64
	CallsFailed              int64
	Draining                 bool
	LinkUp                   bool
}

// Relay opens and carries connections.
type Relay struct {
	coreURL, instance string
	max               int

	draining atomic.Bool
	linkUp   atomic.Bool
	opens    atomic.Int64
	failed   atomic.Int64
	// The reverse kinds' (reverse.go): core's sync call and how long a reply
	// may take, and their counters.
	sync        Sync
	syncTimeout time.Duration
	accepted    atomic.Int64
	calls       atomic.Int64
	callsFailed atomic.Int64

	mu    sync.Mutex
	conns map[*conn]struct{}
	// inCalls are the reverse calls under way: they count toward max.
	inCalls int64
}

// New is a relay for instance, opening sockets under core's base URL, at
// most max connections at once.
func New(coreURL, instance string, max int) *Relay {
	return &Relay{coreURL: coreURL, instance: instance, max: max, conns: map[*conn]struct{}{}}
}

// SetDraining refuses (true) or takes again new opens; open connections
// stay.
func (r *Relay) SetDraining(v bool) { r.draining.Store(v) }

// Draining reports whether opens are refused.
func (r *Relay) Draining() bool { return r.draining.Load() }

// SetLinkUp records the control link's state (linkclient's OnState).
func (r *Relay) SetLinkUp(up bool) { r.linkUp.Store(up) }

// Stats are the counters now.
func (r *Relay) Stats() Stats {
	r.mu.Lock()
	open := int64(len(r.conns))
	r.mu.Unlock()
	return Stats{Open: open, Opens: r.opens.Load(), OpensFailed: r.failed.Load(), Accepted: r.accepted.Load(), Calls: r.calls.Load(),
		CallsFailed: r.callsFailed.Load(), Draining: r.draining.Load(), LinkUp: r.linkUp.Load()}
}

// CloseAll closes every connection open: their sockets to core close with
// them. It returns how many.
func (r *Relay) CloseAll(why string) int {
	r.mu.Lock()
	all := make([]*conn, 0, len(r.conns))
	for c := range r.conns {
		all = append(all, c)
	}
	r.mu.Unlock()
	for _, c := range all {
		c.close(why)
	}
	return len(all)
}

// target is one open's values.
type target struct {
	addr      string
	proxy     *url.URL
	timeout   time.Duration
	keepalive time.Duration
}

// readTarget reads an open's values: core checked them against the
// connection schema, but they come over the wire all the same.
func readTarget(v map[string]any) (target, error) {
	str := func(k string) string { s, _ := v[k].(string); return s }
	dur := func(k string, def time.Duration) time.Duration {
		if d, err := protocol.ParseDuration(v[k]); err == nil && d > 0 {
			return d
		}
		return def
	}
	host := str("host")
	if host == "" {
		return target{}, errors.New("no host")
	}
	port, ok := intOf(v["port"])
	if !ok || port == 0 {
		port, ok = intOf(v["listen_port"])
	}
	if !ok || port < 1 || port > 65535 {
		return target{}, errors.New("no port: the listen port, or the target's")
	}
	t := target{addr: net.JoinHostPort(host, strconv.FormatInt(port, 10)),
		timeout: dur("connect_timeout", 10*time.Second), keepalive: dur("keepalive", 30*time.Second)}
	if p := str("proxy"); p != "" {
		u, err := url.Parse(p)
		if err != nil || u.Host == "" {
			return target{}, fmt.Errorf("proxy %q", p)
		}
		t.proxy = u
	}
	return t, nil
}

// intOf reads an integer as the control link's CBOR decodes it (uint64 for
// a positive one), or as JSON would.
func intOf(v any) (int64, bool) {
	switch n := v.(type) {
	case uint64:
		if n <= 1<<62 {
			return int64(n), true
		}
		return 0, false
	case uint32:
		return int64(n), true
	case uint:
		return int64(n), true
	}
	return protocol.AsInt64(v)
}

// peerOf is the minion's external address, as core shows it: the one c is
// connected to; through a proxy, that is the proxy's, so the target's host
// when it is an IP address (a name, the proxy resolves: x/net/proxy's SOCKS5
// sends names too), none otherwise.
func peerOf(t target, c net.Conn) string {
	if t.proxy != nil {
		host, _, _ := net.SplitHostPort(t.addr)
		if a, err := netip.ParseAddr(host); err == nil {
			return a.Unmap().String()
		}
		return ""
	}
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP.String()
	}
	return ""
}

// Open is the control link's Opener: it dials the target and, once the
// minion's listener took the connection, opens its socket to core naming
// the open, then carries it on its own.
func (r *Relay) Open(ctx context.Context, o linkclient.Open) linkclient.Answer {
	a := r.open(ctx, o)
	if a.Failure != nil {
		r.failed.Add(1)
		slog.Info("open failed", "request", o.Request, "host", o.Values["host"], "kind", protocol.LinkFailName(a.Failure.Kind), "err", a.Failure.Msg)
	} else {
		r.opens.Add(1)
	}
	return a
}

func (r *Relay) open(ctx context.Context, o linkclient.Open) linkclient.Answer {
	if r.Draining() {
		return linkclient.Fail(protocol.LinkFailOther, "the channel is draining")
	}
	r.mu.Lock()
	full := len(r.conns) >= r.max
	r.mu.Unlock()
	if full {
		return linkclient.Fail(protocol.LinkFailOther, "the channel has %d connections open, its most", r.max)
	}
	t, err := readTarget(o.Values)
	if err != nil {
		return linkclient.Fail(protocol.LinkFailOther, "%s", err)
	}
	dctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	tcp, err := dial(dctx, t)
	if err != nil {
		return failure(dctx, err)
	}
	if tc, ok := tcp.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(t.keepalive)
	}
	peer := peerOf(t, tcp)
	core, err := connclient.Dial(ctx, connclient.Options{BaseURL: r.coreURL, Instance: r.instance,
		Kind: protocol.KindBindPersistent, OpenRequest: o.Request, Peer: peer, MaxFrame: MaxFrame + 64})
	if err != nil {
		_ = tcp.Close()
		return linkclient.Fail(protocol.LinkFailOther, "core did not take the connection's socket: %s", err)
	}
	c := &conn{tcp: tcp, core: core, done: make(chan struct{}), request: o.Request, addr: t.addr}
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
	slog.Info("connection open", "request", o.Request, "addr", t.addr, "proxy", t.proxy != nil)
	return linkclient.Answer{}
}

// dial connects to t's address, through its proxy when it has one.
func dial(ctx context.Context, t target) (net.Conn, error) {
	d := &net.Dialer{KeepAlive: t.keepalive}
	if t.proxy == nil {
		return d.DialContext(ctx, "tcp", t.addr)
	}
	switch t.proxy.Scheme {
	case "socks5", "socks5h":
		p, err := proxy.FromURL(t.proxy, d)
		if err != nil {
			return nil, err
		}
		cd, ok := p.(proxy.ContextDialer)
		if !ok {
			return nil, errors.New("the SOCKS dialer takes no context")
		}
		return cd.DialContext(ctx, "tcp", t.addr)
	case "http":
		return dialConnect(ctx, d, t)
	}
	return nil, fmt.Errorf("proxy scheme %q", t.proxy.Scheme)
}

// dialConnect asks an HTTP proxy for a tunnel to t's address (CONNECT).
func dialConnect(ctx context.Context, d *net.Dialer, t target) (net.Conn, error) {
	c, err := d.DialContext(ctx, "tcp", t.proxy.Host)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: t.addr}, Host: t.addr, Header: http.Header{}}
	if u := t.proxy.User; u != nil {
		pw, _ := u.Password()
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.Username()+":"+pw)))
	}
	if err := req.Write(c); err != nil {
		_ = c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	_ = resp.Body.Close() // a CONNECT's answer has no body worth reading
	if resp.StatusCode != http.StatusOK {
		_ = c.Close()
		return nil, &proxyRefused{status: resp.Status}
	}
	_ = c.SetDeadline(time.Time{})
	if br.Buffered() > 0 {
		// The minion's first bytes came with the proxy's answer.
		return &bufferedConn{Conn: c, r: br}, nil
	}
	return c, nil
}

type proxyRefused struct{ status string }

func (e *proxyRefused) Error() string { return "the proxy refused the tunnel: " + e.status }

// bufferedConn reads first what a reader buffered past the proxy's answer.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// conn is an open connection: the minion's TCP connection and its socket
// to core.
type conn struct {
	tcp     net.Conn
	core    *connclient.Conn
	request uint64
	addr    string
	// reader is a reverse connection's, which read its first frame already;
	// nil: one over tcp.
	reader  *bufio.Reader
	writeMu sync.Mutex
	once    sync.Once
	done    chan struct{}
}

// fromMinion carries the minion's frames to core.
func (c *conn) fromMinion() {
	r := c.reader
	if r == nil {
		r = bufio.NewReaderSize(c.tcp, 64<<10)
	}
	for {
		id, data, err := ReadFrame(r)
		if err != nil {
			c.close(readWhy(err))
			return
		}
		if err := c.core.Send(context.Background(), protocol.ConnFrame{ID: id, Data: data}); err != nil {
			c.close("send to core: " + err.Error())
			return
		}
	}
}

// fromCore carries core's frames to the minion.
func (c *conn) fromCore() {
	for {
		f, err := c.core.Receive(context.Background())
		if err != nil {
			c.close("core closed")
			return
		}
		c.writeMu.Lock()
		_ = c.tcp.SetWriteDeadline(time.Now().Add(writeWait))
		err = WriteFrame(c.tcp, f.ID, f.Data)
		c.writeMu.Unlock()
		if err != nil {
			c.close("write to the minion: " + err.Error())
			return
		}
	}
}

// close closes both sides, once.
func (c *conn) close(why string) {
	c.once.Do(func() {
		_ = c.tcp.Close()
		_ = c.core.Close()
		close(c.done)
		slog.Info("connection closed", "request", c.request, "addr", c.addr, "why", why)
	})
}

func readWhy(err error) string {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return "the minion closed"
	}
	return "read from the minion: " + err.Error()
}

// ReadFrame reads one frame: the id it is sent under and the sealed
// message.
func ReadFrame(r io.Reader) ([]byte, []byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n < 16+protocol.MinFrameSize || n > MaxFrame {
		return nil, nil, fmt.Errorf("a frame of %d bytes", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, nil, err
	}
	return b[:16], b[16:], nil
}

// ReadReply reads a reverse call's reply: a frame whose sealed message may
// be empty (core had nothing for the minion).
func ReadReply(r io.Reader) ([]byte, []byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n < 16 || n > MaxFrame {
		return nil, nil, fmt.Errorf("a reply of %d bytes", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, nil, err
	}
	return b[:16], b[16:], nil
}

// WriteFrame writes one frame.
func WriteFrame(w io.Writer, id, data []byte) error {
	if len(id) != 16 {
		return errors.New("the id is 16 bytes")
	}
	b := make([]byte, 4, 4+16+len(data))
	binary.BigEndian.PutUint32(b, uint32(16+len(data)))
	b = append(append(b, id...), data...)
	_, err := w.Write(b)
	return err
}

// failure is the answer for an open that reached nothing.
func failure(ctx context.Context, err error) linkclient.Answer {
	var dnsErr *net.DNSError
	var netErr net.Error
	var opErr *net.OpError
	var refused *proxyRefused
	var recordErr tls.RecordHeaderError
	switch {
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || (errors.As(err, &netErr) && netErr.Timeout()):
		return linkclient.Fail(protocol.LinkFailTimeout, "%s", err)
	case errors.As(err, &recordErr):
		return linkclient.Fail(protocol.LinkFailTLS, "%s", err)
	case errors.As(err, &refused):
		return linkclient.Fail(protocol.LinkFailOther, "%s", err)
	case errors.As(err, &dnsErr), errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.EHOSTUNREACH),
		errors.Is(err, syscall.ENETUNREACH), errors.As(err, &opErr) && opErr.Op == "dial":
		return linkclient.Fail(protocol.LinkFailUnreachable, "%s", err)
	}
	return linkclient.Fail(protocol.LinkFailOther, "%s", err)
}
