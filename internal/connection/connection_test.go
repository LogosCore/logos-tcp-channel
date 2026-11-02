package connection

import (
	"testing"

	"github.com/logoscore/logos-golang-protocol/protocol"
)

func TestSchema(t *testing.T) {
	s := Schema()
	if err := protocol.ValidateConnectionSchema(s); err != nil {
		t.Fatal(err)
	}
	if got := s.SupportedKinds(); len(got) != 1 || got[0] != protocol.KindBindPersistent {
		t.Fatalf("kinds %v", got)
	}
	if p := protocol.ValidateTargetValues(s, protocol.KindBindPersistent, map[string]any{"host": "10.0.0.5", "proxy": "socks5://127.0.0.1:1080"}); p != nil {
		t.Fatalf("a target: %v", p)
	}
	if p := protocol.ValidateTargetValues(s, protocol.KindBindPersistent, map[string]any{"host": "h", "proxy": "ftp://x"}); p["proxy"] == "" {
		t.Fatalf("an ftp proxy taken: %v", p)
	}
}
