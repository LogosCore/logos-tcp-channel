package relay

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// reverseCore takes reverse-persistent connection sockets, hands each frame
// that comes to frames with the socket's peer, and sends back what push
// gets.
func reverseCore(t *testing.T, frames chan<- protocol.ConnFrame, peers chan<- string, push <-chan protocol.ConnFrame) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.ConnectionPath || r.Header.Get(protocol.HeaderConnectionKind) != protocol.KindReversePersistent ||
			r.Header.Get(protocol.HeaderOpenRequest) != "" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		peers <- r.Header.Get(protocol.HeaderPeerAddress)
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()
		go func() {
			for f := range push {
				b, _ := protocol.EncodeConnFrame(f)
				if ws.Write(ctx, websocket.MessageBinary, b) != nil {
					return
				}
			}
		}()
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			f, _ := protocol.DecodeConnFrame(b)
			frames <- f
		}
	}))
}

func listenFor(t *testing.T, r *Relay, kind string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	addr, err := r.Listen(ctx, kind, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return addr.String()
}

// A minion that dials the channel and stays: its first frame opens its
// socket to core, naming its address, then frames go both ways; its close
// closes the socket.
func TestReversePersistent(t *testing.T) {
	frames, peers, push := make(chan protocol.ConnFrame, 4), make(chan string, 1), make(chan protocol.ConnFrame, 4)
	core := reverseCore(t, frames, peers, push)
	defer core.Close()
	r := New(core.URL, "tcp-1", 4)
	addr := listenFor(t, r, protocol.KindReversePersistent)
	minion, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = minion.Close() }()
	id := bytes.Repeat([]byte{1}, 16)
	if err := WriteFrame(minion, id, sealed(1)); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-peers:
		if p != "127.0.0.1" {
			t.Fatalf("peer %q", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no socket")
	}
	if f := <-frames; !bytes.Equal(f.ID, id) || !bytes.Equal(f.Data, sealed(1)) {
		t.Fatalf("the first frame %+v", f)
	}
	push <- protocol.ConnFrame{ID: id, Data: sealed(2)}
	_ = minion.SetReadDeadline(time.Now().Add(5 * time.Second))
	gotID, data, err := ReadFrame(bufio.NewReader(minion))
	if err != nil || !bytes.Equal(gotID, id) || !bytes.Equal(data, sealed(2)) {
		t.Fatalf("core's frame: %v", err)
	}
	if st := r.Stats(); st.Accepted != 1 || st.Open != 1 {
		t.Fatalf("stats %+v", st)
	}
	_ = minion.Close()
	deadline := time.Now().Add(5 * time.Second)
	for r.Stats().Open != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r.Stats().Open != 0 {
		t.Fatal("still open after the minion closed")
	}
}

type fakeSync struct {
	got   chan protocol.InboundMinionMessage
	reply []byte
	err   error
}

func (f fakeSync) Sync(_ context.Context, in protocol.InboundMinionMessage) (protocol.OutboundMinionMessage, error) {
	f.got <- in
	return protocol.OutboundMinionMessage{BuildID: in.BuildID, Data: f.reply}, f.err
}

// call is one reverse-call exchange: the reply's frame, or nil when the
// channel closed without one.
func call(t *testing.T, addr string, id, data []byte) ([]byte, []byte, bool) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := WriteFrame(c, id, data); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	gotID, reply, err := ReadReply(bufio.NewReader(c))
	if err != nil {
		return nil, nil, false
	}
	return gotID, reply, true
}

// A minion that dials for each exchange: its message through the sync
// call (its build id, the channel's instance, its address), core's reply
// back under the same id; the id alone when core has nothing; no reply
// when core does not take it.
func TestReverseCall(t *testing.T) {
	got := make(chan protocol.InboundMinionMessage, 4)
	id := []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	for _, c := range []struct {
		name  string
		sync  fakeSync
		reply []byte
		ok    bool
	}{
		{"a reply", fakeSync{got: got, reply: sealed(7)}, sealed(7), true},
		{"nothing for the minion", fakeSync{got: got}, []byte{}, true},
		{"refused", fakeSync{got: got, err: errors.New("rejected: status=422")}, nil, false},
	} {
		r := New("http://core", "tcp-1", 4).WithSync(c.sync, time.Second)
		addr := listenFor(t, r, protocol.KindReverseCall)
		gotID, reply, ok := call(t, addr, id, sealed(3))
		in := <-got
		if in.BuildID != "550e8400-e29b-41d4-a716-446655440000" || in.ChannelInstance != "tcp-1" || in.PeerAddress != "127.0.0.1" ||
			!bytes.Equal(in.Data, sealed(3)) || in.MessageID == "" {
			t.Fatalf("%s: the sync %+v", c.name, in)
		}
		if ok != c.ok || (ok && (!bytes.Equal(gotID, id) || !bytes.Equal(reply, c.reply))) {
			t.Fatalf("%s: reply %v %x", c.name, ok, reply)
		}
		if st := r.Stats(); st.Calls != 1 || (c.ok == (st.CallsFailed == 1)) {
			t.Fatalf("%s: stats %+v", c.name, st)
		}
	}
}

// A minion that dials and says nothing is closed after FirstFrameWait; the
// channel draining takes no one.
func TestReverseRefuses(t *testing.T) {
	FirstFrameWait = 100 * time.Millisecond
	defer func() { FirstFrameWait = 30 * time.Second }()
	r := New("http://core", "tcp-1", 4).WithSync(fakeSync{got: make(chan protocol.InboundMinionMessage, 1)}, time.Second)
	addr := listenFor(t, r, protocol.KindReversePersistent)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("a silent minion kept")
	}
	r.SetDraining(true)
	callAddr := listenFor(t, r, protocol.KindReverseCall)
	if _, _, ok := call(t, callAddr, bytes.Repeat([]byte{1}, 16), sealed(1)); ok {
		t.Fatal("a call taken while draining")
	}
}
