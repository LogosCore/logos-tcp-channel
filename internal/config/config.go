// Package config reads the channel's settings from the environment (and a
// .env file as fallback).
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config is what the process runs with.
type Config struct {
	// ChannelID is the instance id core knows the process by.
	ChannelID string
	// CoreBaseURL is core's base URL: the control link and connection
	// sockets open under it.
	CoreBaseURL string
	// MaxConnections bounds the connections open at once
	// (MAX_CONNECTIONS, 1000).
	MaxConnections int
	// RabbitMQURL is the AMQP URL for registering with core.
	RabbitMQURL string
	// LogFormat is the stderr log format: "json" (default) or "text".
	LogFormat string
	// ReversePersistentListen and ReverseCallListen are where the channel
	// listens for minions that dial it (host:port; "" for none):
	// REVERSE_PERSISTENT_LISTEN, REVERSE_CALL_LISTEN.
	ReversePersistentListen, ReverseCallListen string
	// SyncTimeout bounds a reverse call's sync with core (CORE_SYNC_TIMEOUT,
	// 30s): keep it below the minions' request timeout.
	SyncTimeout time.Duration
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// LoadFromEnv reads the config; envFile, when it exists, fills in what the
// environment does not set.
func LoadFromEnv(envFile string) (Config, error) {
	if envFile != "" {
		_ = godotenv.Load(envFile)
	}
	cfg := Config{
		ChannelID:   envOr("CHANNEL_ID", "tcp-main"),
		CoreBaseURL: strings.TrimRight(envOr("LOGOS_CORE_BASE_URL", "http://localhost:8002"), "/"),
	}
	if u, err := url.Parse(cfg.CoreBaseURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Config{}, fmt.Errorf("LOGOS_CORE_BASE_URL must be http(s)://host[:port], got %q", cfg.CoreBaseURL)
	}
	n, err := strconv.Atoi(envOr("MAX_CONNECTIONS", "1000"))
	if err != nil || n < 1 || n > 100000 {
		return Config{}, fmt.Errorf("MAX_CONNECTIONS must be an integer from 1 to 100000")
	}
	cfg.MaxConnections = n
	vhost := envOr("RABBITMQ_VHOST", "/")
	cfg.RabbitMQURL = (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(envOr("RABBITMQ_USER", "guest"), envOr("RABBITMQ_PASSWORD", "guest")),
		Host:   net.JoinHostPort(envOr("RABBITMQ_HOST", "localhost"), envOr("RABBITMQ_PORT", "5672")),
		// amqp091 takes the vhost from the path minus its leading "/".
		Path: "/" + vhost,
	}).String()
	cfg.LogFormat = strings.ToLower(envOr("LOG_FORMAT", "json"))
	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		return Config{}, fmt.Errorf("LOG_FORMAT must be json or text")
	}
	cfg.ReversePersistentListen, cfg.ReverseCallListen = os.Getenv("REVERSE_PERSISTENT_LISTEN"), os.Getenv("REVERSE_CALL_LISTEN")
	for key, v := range map[string]string{"REVERSE_PERSISTENT_LISTEN": cfg.ReversePersistentListen, "REVERSE_CALL_LISTEN": cfg.ReverseCallListen} {
		if v == "" {
			continue
		}
		if _, port, err := net.SplitHostPort(v); err != nil || port == "" {
			return Config{}, fmt.Errorf("%s must be host:port (:4790 for every interface), got %q", key, v)
		}
	}
	d, err := time.ParseDuration(envOr("CORE_SYNC_TIMEOUT", "30s"))
	if err != nil || d < time.Second || d > 10*time.Minute {
		return Config{}, fmt.Errorf("CORE_SYNC_TIMEOUT must be a duration from 1s to 10m")
	}
	cfg.SyncTimeout = d
	return cfg, nil
}
