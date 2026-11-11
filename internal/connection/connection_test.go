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

// A minion that reads a minion config reads its connection by these
// numbers: each stays its field's, the same in every kind and in a target's
// fields.
func TestFieldNumbers(t *testing.T) {
	want := map[string]int{
		"host": 1, "port": 2, "connect_timeout": 3, "keepalive": 4, "reconnect_backoff": 5, "request_timeout": 6,
		"checkin_interval": 7, "checkin_jitter": 8, "listen_port": 9, "listen_address": 10, "max_connections": 11,
		"proxy": 12,
	}
	s := Schema()
	for kind, k := range s.Kinds {
		if !s.Numbered(kind) {
			t.Errorf("%s: fields without numbers", kind)
		}
		for _, f := range k.Fields {
			if f.Num != want[f.Key] {
				t.Errorf("%s: %s is %d, want %d", kind, f.Key, f.Num, want[f.Key])
			}
		}
		for _, f := range k.Target {
			if f.Num != want[f.Key] {
				t.Errorf("%s: target field %s is %d, want %d", kind, f.Key, f.Num, want[f.Key])
			}
		}
	}
}
