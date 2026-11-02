package relay

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/linkclient"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// fakeCore takes connection sockets naming open 9, hands each frame that
// comes to frames, and sends back what push gets.
func fakeCore(t *testing.T, frames chan<- protocol.ConnFrame, push <-chan protocol.ConnFrame, closed chan<- struct{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.ConnectionPath || r.Header.Get(protocol.HeaderConnectionKind) != protocol.KindBindPersistent ||
			r.Header.Get(protocol.HeaderOpenRequest) != "9" || r.Header.Get(protocol.HeaderChannelInstance) != "tcp-1" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ws.SetReadLimit(MaxFrame + 64)
		ctx := r.Context()
		go func() {
			for f := range push {
				b, _ := protocol.EncodeConnFrame(f)
				if ws.Write(ctx, websocket.MessageBinary, b) != nil {
					return
				}
			}
		}()
		defer func() { closed <- struct{}{} }()
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			f, err := protocol.DecodeConnFrame(b)
			if err != nil {
				t.Errorf("a frame: %v", err)
				return
			}
			frames <- f
		}
	}))
}

// minion listens, as a bind-persistent minion does, and hands over its
// first connection.
func minion(t *testing.T) (net.Listener, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			got <- c
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln, got
}

func values(addr string, extra map[string]any) map[string]any {
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	v := map[string]any{"host": host, "listen_port": uint64(p), "connect_timeout": "2s", "keepalive": "15s"}
	for k, x := range extra {
		v[k] = x
	}
	return v
}

func sealed(b byte) []byte { return bytes.Repeat([]byte{b}, protocol.MinFrameSize) }

func TestOpenRelaysBothWays(t *testing.T) {
	frames, push, closed := make(chan protocol.ConnFrame, 4), make(chan protocol.ConnFrame, 4), make(chan struct{}, 1)
	core := fakeCore(t, frames, push, closed)
	defer core.Close()
	ln, accepted := minion(t)
	r := New(core.URL, "tcp-1", 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if a := r.Open(ctx, linkclient.Open{Request: 9, Values: values(ln.Addr().String(), nil)}); a.Failure != nil {
		t.Fatalf("open: %+v", a.Failure)
	}
	m := <-accepted
	defer func() { _ = m.Close() }()
	if st := r.Stats(); st.Open != 1 || st.Opens != 1 {
		t.Fatalf("stats %+v", st)
	}

	// The minion speaks first: its frame goes to core as it is.
	id := bytes.Repeat([]byte{1}, 16)
	if err := WriteFrame(m, id, sealed(0xaa)); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-frames:
		if !bytes.Equal(f.ID, id) || !bytes.Equal(f.Data, sealed(0xaa)) {
			t.Fatalf("core got %x", f.Data[:4])
		}
	case <-ctx.Done():
		t.Fatal("nothing reached core")
	}
	// Core's comes back framed.
	push <- protocol.ConnFrame{ID: id, Data: sealed(0xbb)}
	gotID, data, err := ReadFrame(bufio.NewReader(m))
	if err != nil || !bytes.Equal(gotID, id) || !bytes.Equal(data, sealed(0xbb)) {
		t.Fatalf("the minion got %v", err)
	}
	// The minion closes: so does its socket to core.
	_ = m.Close()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("core's socket stayed open")
	}
	deadline := time.Now().Add(2 * time.Second)
	for r.Stats().Open != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r.Stats().Open != 0 {
		t.Fatal("the connection is still counted")
	}
}

func TestOpenThroughConnectProxy(t *testing.T) {
	frames, push, closed := make(chan protocol.ConnFrame, 4), make(chan protocol.ConnFrame, 4), make(chan struct{}, 1)
	core := fakeCore(t, frames, push, closed)
	defer core.Close()
	ln, accepted := minion(t)
	// An HTTP proxy that tunnels CONNECT to the minion only.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != ln.Addr().String() {
			http.Error(w, "no", http.StatusForbidden)
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		hj, _ := w.(http.Hijacker)
		c, _, err := hj.Hijack()
		if err != nil {
			return
		}
		go func() { _, _ = io.Copy(up, c) }()
		_, _ = io.Copy(c, up)
	}))
	defer proxy.Close()
	r := New(core.URL, "tcp-1", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if a := r.Open(ctx, linkclient.Open{Request: 9, Values: values(ln.Addr().String(), map[string]any{"proxy": proxy.URL})}); a.Failure != nil {
		t.Fatalf("open through the proxy: %+v", a.Failure)
	}
	m := <-accepted
	defer func() { _ = m.Close() }()
	if err := WriteFrame(m, bytes.Repeat([]byte{2}, 16), sealed(0xcc)); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-frames:
		if !bytes.Equal(f.Data, sealed(0xcc)) {
			t.Fatal("another frame")
		}
	case <-ctx.Done():
		t.Fatal("nothing came through the proxy")
	}
}

func TestOpenFails(t *testing.T) {
	r := New("http://127.0.0.1:1", "tcp-1", 1)
	ctx := context.Background()
	// Nothing listens there.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()
	if a := r.Open(ctx, linkclient.Open{Request: 1, Values: values(addr, nil)}); a.Failure == nil || a.Failure.Kind != protocol.LinkFailUnreachable {
		t.Errorf("a closed port: %+v", a)
	}
	if a := r.Open(ctx, linkclient.Open{Request: 2, Values: map[string]any{"listen_port": uint64(80)}}); a.Failure == nil {
		t.Error("no host opened")
	}
	r.SetDraining(true)
	if a := r.Open(ctx, linkclient.Open{Request: 3, Values: values(addr, nil)}); a.Failure == nil {
		t.Error("draining, opened")
	}
	if st := r.Stats(); st.OpensFailed != 3 || st.Opens != 0 {
		t.Errorf("stats %+v", st)
	}
	// A listener there, but core refuses the socket: the dial is undone.
	r.SetDraining(false)
	ln2, accepted := minion(t)
	if a := r.Open(ctx, linkclient.Open{Request: 4, Values: values(ln2.Addr().String(), nil)}); a.Failure == nil {
		t.Error("opened with no core")
	}
	select {
	case m := <-accepted:
		_ = m.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := m.Read(make([]byte, 1)); err == nil {
			t.Error("the minion's connection left open")
		}
		_ = m.Close()
	case <-time.After(2 * time.Second):
	}
}

func TestFrames(t *testing.T) {
	var b bytes.Buffer
	id := bytes.Repeat([]byte{3}, 16)
	if err := WriteFrame(&b, id, sealed(1)); err != nil {
		t.Fatal(err)
	}
	gotID, data, err := ReadFrame(&b)
	if err != nil || !bytes.Equal(gotID, id) || !bytes.Equal(data, sealed(1)) {
		t.Fatalf("round trip: %v", err)
	}
	for name, raw := range map[string][]byte{
		"too short": {0, 0, 0, 5, 1, 2, 3, 4, 5},
		"too long":  {0x7f, 0xff, 0xff, 0xff},
	} {
		if _, _, err := ReadFrame(bytes.NewReader(raw)); err == nil {
			t.Errorf("%s read", name)
		}
	}
	if WriteFrame(&b, id[:3], nil) == nil {
		t.Error("a short id written")
	}
}

// The peer is where the dial went: the address connected to, or through a
// proxy the target's when it is an IP address, none for a name.
func TestPeerOf(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	proxy, _ := url.Parse("socks5://10.9.9.9:1080")
	for _, tc := range []struct {
		name string
		t    target
		want string
	}{
		{"direct", target{addr: "localhost:4444"}, "127.0.0.1"},
		{"an IP through a proxy", target{addr: "10.0.12.7:4444", proxy: proxy}, "10.0.12.7"},
		{"IPv6 through a proxy", target{addr: "[2001:db8::7]:4444", proxy: proxy}, "2001:db8::7"},
		{"a name through a proxy", target{addr: "minion.internal:4444", proxy: proxy}, ""},
	} {
		if got := peerOf(tc.t, c); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
