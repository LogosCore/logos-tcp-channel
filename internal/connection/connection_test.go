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
	if got := s.SupportedKinds(); len(got) != 3 || got[0] != protocol.KindReverseCall || got[1] != protocol.KindReversePersistent ||
		got[2] != protocol.KindBindPersistent {
		t.Fatalf("kinds %v", got)
	}
	for _, k := range []string{protocol.KindReverseCall, protocol.KindReversePersistent} {
		in := map[string]any{"host": "203.0.113.10", "port": 4790}
		if problems := protocol.ValidateConnectionValues(s, k, in); problems != nil {
			t.Fatalf("%s: %v", k, problems)
		}
		if v := protocol.ResolveConnectionValues(s, k, in); v["checkin_interval"] != "60s" || v["connect_timeout"] != "10s" {
			t.Fatalf("%s: %v", k, v)
		}
	}
	if p := protocol.ValidateTargetValues(s, protocol.KindBindPersistent, map[string]any{"host": "10.0.0.5", "proxy": "socks5://127.0.0.1:1080"}); p != nil {
		t.Fatalf("a target: %v", p)
	}
	if p := protocol.ValidateTargetValues(s, protocol.KindBindPersistent, map[string]any{"host": "h", "proxy": "ftp://x"}); p["proxy"] == "" {
		t.Fatalf("an ftp proxy taken: %v", p)
	}
}
