// Package management declares what core may manage on the channel (its log
// level; drain, resume and closing its connections) and what it shows (the
// control link, the connections), bound to the relay. How the channel
// reaches core stays env-only.
package management

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-protocol/protocol"
	"github.com/logoscore/logos-tcp-channel/internal/config"
	"github.com/logoscore/logos-tcp-channel/internal/relay"
)

var logLevels = []struct {
	value, label, description string
	level                     slog.Level
}{
	{"debug", "Debug", "Everything below, and more of each connection.", slog.LevelDebug},
	{"info", "Info", "Opens, failed opens and closed connections, startup and config changes.", slog.LevelInfo},
	{"warn", "Warn", "The control link going down only.", slog.LevelWarn},
	{"error", "Error", "Errors only.", slog.LevelError},
}

func slogLevel(v any) slog.Level {
	for _, l := range logLevels {
		if l.value == v {
			return l.level
		}
	}
	return slog.LevelInfo
}

// Manifest is the management manifest.
func Manifest() protocol.ManagementManifest {
	levels := make([]protocol.EnumOption, len(logLevels))
	for i, l := range logLevels {
		levels[i] = protocol.EnumOption{Value: l.value, Label: l.label, Description: l.description}
	}
	return protocol.ManagementManifest{
		SchemaVersion: protocol.ManagementSchemaV1,
		Properties: []protocol.PropertySpec{
			{FieldSpec: protocol.FieldSpec{
				Key: "log_level", Type: protocol.FieldEnum, Label: "Log level",
				Description: "Level of the channel's own output (stderr). The Logs tab has its own level.",
				Group:       "Logging", Order: 1, Default: "info", Options: levels,
			}, Apply: protocol.ApplyLive},
		},
		Actions: []protocol.ActionSpec{
			{
				Name: "drain", Label: "Drain", Risk: protocol.RiskHigh,
				Description: "Open no more connections: every open core asks for fails until resumed. Those open stay.",
				Confirm:     "Opens through this channel fail until it is resumed.",
				AvailableWhen: []protocol.ActionCondition{
					{Status: "draining", Op: protocol.OpEq, Value: false, Message: "Already draining"},
				},
			},
			{
				Name: "resume", Label: "Resume", Risk: protocol.RiskLow, Description: "Open connections again after a drain.",
				AvailableWhen: []protocol.ActionCondition{
					{Status: "draining", Op: protocol.OpEq, Value: true, Message: "Not draining"},
				},
			},
			{
				Name: "close_connections", Label: "Close connections", Risk: protocol.RiskHigh,
				Description: "Close every connection open: core sees each session not connected, and opens again as each target's policy says.",
				Confirm:     "Every minion connection through this channel closes.",
				AvailableWhen: []protocol.ActionCondition{
					{Status: "connections_open", Op: protocol.OpGt, Value: 0, Message: "No connection is open"},
				},
			},
		},
		Status: []protocol.StatusSpec{
			{Key: "link_up", Type: protocol.FieldBool, Label: "Control link",
				Description: "Whether the control link to core is open: core asks for connections through it."},
			{Key: "connections_open", Type: protocol.FieldInt, Label: "Connections open"},
			{Key: "opens_total", Type: protocol.FieldInt, Label: "Opens"},
			{Key: "opens_failed_total", Type: protocol.FieldInt, Label: "Opens failed",
				Description: "Nothing answered (DNS, refused, timeout), a proxy refused, or core did not take the socket."},
			{Key: "draining", Type: protocol.FieldBool, Label: "Draining", Alert: true},
		},
	}
}

// New is the management server for r; log_level drives level.
func New(cfg config.Config, r *relay.Relay, level *slog.LevelVar) (*mgmt.Server, error) {
	s, err := mgmt.New(Manifest(), mgmt.WithSource(protocol.Source{Service: protocol.ServiceChannel, Instance: cfg.ChannelID}))
	if err != nil {
		return nil, err
	}
	s.OnApply(func(ctx context.Context, live map[string]any) error {
		lvl := slogLevel(live["log_level"])
		if level.Level() == lvl {
			return nil
		}
		level.Set(lvl)
		slog.Info("management: config applied", "by", actorName(mgmt.ActorFromContext(ctx)), "log_level", lvl.String())
		return nil
	})
	s.Status(func() map[string]any {
		st := r.Stats()
		return map[string]any{"link_up": st.LinkUp, "connections_open": st.Open, "opens_total": st.Opens,
			"opens_failed_total": st.OpensFailed, "draining": st.Draining}
	})
	s.Action("drain", func(_ context.Context, _ map[string]any, actor *protocol.Actor) (any, error) {
		was := r.Draining()
		r.SetDraining(true)
		slog.Info("management: drained", "by", actorName(actor))
		return map[string]any{"draining": true, "was_draining": was}, nil
	})
	s.Action("resume", func(_ context.Context, _ map[string]any, actor *protocol.Actor) (any, error) {
		was := r.Draining()
		r.SetDraining(false)
		slog.Info("management: resumed", "by", actorName(actor))
		return map[string]any{"draining": false, "was_draining": was}, nil
	})
	s.Action("close_connections", func(_ context.Context, _ map[string]any, actor *protocol.Actor) (any, error) {
		n := r.CloseAll("closed by an operator")
		slog.Info("management: connections closed", "by", actorName(actor), "connections", n)
		return map[string]any{"closed": n}, nil
	})
	return s, nil
}

func actorName(a *protocol.Actor) string {
	switch {
	case a == nil:
		return "core"
	case a.Name != "":
		return fmt.Sprintf("%s %s (%s)", a.Type, a.Name, a.ID)
	default:
		return a.Type + " " + a.ID
	}
}
