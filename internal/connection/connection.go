// Package connection declares the channel's connection schema: the one kind
// it carries, bind-persistent, with what is built into a minion that
// listens (Fields) and what operators fill in for each deployment of one
// (Target).
package connection

import (
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Version is the built-in fields' version: raise it when minions built
// against the current fields would read new ones wrongly. Target fields
// change without it: nothing is built from them.
const Version = 1

// Schema is the connection schema declared at registration.
func Schema() protocol.ConnectionSchema {
	return protocol.ConnectionSchema{
		SchemaVersion: protocol.ConnectionSchemaV2,
		Version:       Version,
		Kinds: map[string]protocol.ConnectionKind{
			protocol.KindBindPersistent: {
				Fields: []protocol.FieldSpec{
					{
						Key: "listen_port", Type: protocol.FieldInt, Label: "Listen port", Required: true, Min: 1, Max: 65535,
						Description: "The TCP port the minion listens on. The channel dials it, unless a target says another (a port forward).",
						Group:       "Listener", Order: 1,
					},
					{
						Key: "listen_address", Type: protocol.FieldString, Label: "Listen address", Default: "0.0.0.0", MaxLength: 255,
						Description: "The interface the minion binds: 0.0.0.0 every IPv4 one, :: IPv6 too, 127.0.0.1 one reached only from its own host (through a tunnel).",
						Group:       "Listener", Order: 2,
					},
					{
						Key: "max_connections", Type: protocol.FieldInt, Label: "Connections at once", Default: 1, Min: 1, Max: 16,
						Description: "Connections the minion takes at once. More than one lets a new dial in while an old one has not timed out yet.",
						Group:       "Listener", Order: 3,
					},
					{
						Key: "keepalive", Type: protocol.FieldDuration, Label: "Keepalive", Default: "30s", Min: "5s", Max: "1h",
						Description: "TCP keepalive on a connection, the minion's and the channel's: a peer gone without a word is found.",
						Group:       "Connection", Order: 4,
					},
				},
				Target: []protocol.FieldSpec{
					{
						Key: "host", Type: protocol.FieldString, Label: "Host", Required: true, MaxLength: 255,
						Description: "The address or name the channel dials.",
						Group:       "Address", Order: 1, Placeholder: "10.0.12.7",
					},
					{
						Key: "port", Type: protocol.FieldInt, Label: "Port", Min: 1, Max: 65535,
						Description: "The port to dial when it is not the minion's listen port (a port forward, a NAT). Empty: the listen port.",
						Group:       "Address", Order: 2,
					},
					{
						Key: "proxy", Type: protocol.FieldURL, Label: "Proxy", MaxLength: 2048,
						Description: "Dial through a proxy: socks5://host:port (an SSH dynamic forward, a pivot) or http://host:port (CONNECT).",
						Group:       "Address", Order: 3, Pattern: `^(socks5|socks5h|http)://[^\s]+$`,
					},
					{
						Key: "connect_timeout", Type: protocol.FieldDuration, Label: "Connect timeout", Default: "10s", Min: "1s", Max: "2m",
						Description: "How long a dial may take before the open fails.",
						Group:       "Connection", Order: 4,
					},
				},
			},
		},
	}
}
