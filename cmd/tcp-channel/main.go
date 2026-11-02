// Command tcp-channel is the TCP channel: it carries bind-persistent
// connections, dialing a minion's listener when core asks over the control
// link and relaying its messages over a connection socket to core (docs:
// contracts/channel-core-connections/).
package main

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/logoscore/logos-golang-channel-core/v3/pkg/channel"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/linkclient"
	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/logging"
	"github.com/logoscore/logos-golang-module-core/pkg/logstream"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
	"github.com/logoscore/logos-tcp-channel/internal/config"
	"github.com/logoscore/logos-tcp-channel/internal/connection"
	"github.com/logoscore/logos-tcp-channel/internal/management"
	"github.com/logoscore/logos-tcp-channel/internal/relay"
)

// version is reported at registration; set with -ldflags "-X main.version=...".
var version = "dev"

// icon is the channel's icon in core's UI: two hosts and the connection
// between them, the dialer's arrow toward the listener. Line art, so it
// reads light or dark; iconLight has heavier lines for light themes.
//
//go:embed icon.svg
var icon string

//go:embed icon-light.svg
var iconLight string

func main() {
	var envFile string
	flag.StringVar(&envFile, "config", ".env", "path to .env fallback file")
	flag.Parse()
	cfg, err := config.LoadFromEnv(envFile)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	var level slog.LevelVar
	logs := logstream.New(cfg.ChannelID)
	logger, err := logging.New(cfg.LogFormat, os.Stderr, &level, logs)
	if err != nil {
		log.Fatalf("logging: %v", err)
	}
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	r := relay.New(cfg.CoreBaseURL, cfg.ChannelID, cfg.MaxConnections)
	mgmtSrv, err := management.New(cfg, r, &level)
	if err != nil {
		slog.Warn("management disabled", "err", err)
	}
	slog.Info("logos-tcp-channel starting", "channel_id", cfg.ChannelID, "core", cfg.CoreBaseURL, "max_connections", cfg.MaxConnections)

	regCtx, stopReg := context.WithCancel(context.Background())
	regDone := make(chan struct{})
	go func() {
		defer close(regDone)
		schema := connection.Schema()
		mc := channel.Config{
			Config: module.Config{
				AMQPURL: cfg.RabbitMQURL,
				Identity: lifecycle.Identity{
					ModuleType:  channel.ModuleType,
					ModuleName:  "tcp",
					Instance:    cfg.ChannelID,
					Version:     version,
					Description: "TCP transport: dials minions that listen (bind-persistent) and relays their messages",
					Icon:        icon,
					IconLight:   iconLight,
				},
			},
			Connection: &schema,
		}
		if mgmtSrv != nil {
			mc.Management, mc.Logs = mgmtSrv, logs
		}
		if err := channel.Run(regCtx, mc); err != nil {
			fatal("control plane failed", "err", err)
		}
	}()

	linkCtx, stopLink := context.WithCancel(context.Background())
	linkDone := make(chan struct{})
	go func() {
		defer close(linkDone)
		err := linkclient.Run(linkCtx, linkclient.Options{
			BaseURL: cfg.CoreBaseURL, Instance: cfg.ChannelID, OnState: r.SetLinkUp, Opener: r.Open,
			// The channel makes no calls: bind-call is not its kind.
			Handler: func(context.Context, linkclient.Call) linkclient.Answer {
				return linkclient.Fail(protocol.LinkFailOther, "the TCP channel makes no calls (bind-call)")
			},
		})
		if err != nil {
			fatal("control link", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	// No new opens; the connections close (core sees them not connected and
	// opens again by policy once a channel is back), then it deregisters.
	r.SetDraining(true)
	r.CloseAll("the channel is stopping")
	stopLink()
	select {
	case <-linkDone:
	case <-time.After(5 * time.Second):
	}
	stopReg()
	<-regDone
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}
