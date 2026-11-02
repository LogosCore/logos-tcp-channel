package main

import (
	"testing"

	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-protocol/protocol"
	"github.com/logoscore/logos-tcp-channel/internal/management"
)

// The icons are sent at registration and the manifest declared: module-core
// refuses to start with either core would not take.
func TestIconsAndManifest(t *testing.T) {
	for name, svg := range map[string]string{"icon": icon, "iconLight": iconLight} {
		if err := protocol.ValidateIcon(svg); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := mgmt.New(management.Manifest()); err != nil {
		t.Fatal(err)
	}
}
