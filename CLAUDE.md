# CLAUDE.md

`logos-tcp-channel` (`github.com/logoscore/logos-tcp-channel`): a Logos channel module, `module_name=tcp`, that carries only `bind-persistent` connections: core asks it over the control link to open a connection to a minion that listens, it dials, opens the connection's socket to core and relays frames both ways until either side closes. It ships as a Docker image (a `v*` tag publishes it, `.github/workflows/docker-image.yml`). README.md documents the wire format, the connection schema, config and management.

## Commands

```bash
go build ./... && go vet ./... && go test -race ./...
golangci-lint run ./...
go run ./cmd/tcp-channel --config .env
```

CI (`.github/workflows/ci.yml`, the same file as the other Go modules) runs tests and golangci-lint v2.13.2 with default linters, no config: keep 0 issues.

## Layout

- `cmd/tcp-channel/main.go`: config, logging (module-core `logging.New` teed into a `logstream.Hub`), the relay, management, the control-plane session (channel-core `channel.Run` with the connection schema) and the control link (channel-core `linkclient.Run`, `Opener: relay.Open`, a `Handler` that fails every call: the channel makes none). Shutdown: drain, close every connection, close the link, deregister. Embeds `icon.svg` / `icon-light.svg`; a test validates both.
- `internal/connection`: the connection schema, one kind, `bind-persistent`: fields `listen_port`, `listen_address`, `max_connections`, `keepalive`; target `host`, `port`, `proxy`, `connect_timeout`.
- `internal/relay`: an open: dial (direct, socks5 through `golang.org/x/net/proxy`, http CONNECT), TCP keepalive, then channel-core `connclient.Dial` (kind bind-persistent, `OpenRequest`, the peer address), and two goroutines copying frames (`[len u32 BE][id 16][sealed]` on TCP, `ConnFrame` on the socket). Failures are classed `unreachable` / `timeout` / `other`. Counters and the link state feed management.
- `internal/management`: `log_level` (live), actions `drain`/`resume`/`close_connections`, status counters.
- `internal/config`: env config (`CHANNEL_ID`, `LOGOS_CORE_BASE_URL`, `MAX_CONNECTIONS`, `LOG_FORMAT`, `RABBITMQ_*`).

## Conventions

- The channel is plaintext-blind: it never opens, reads or logs a frame. Logs carry the open's request id, the address, the outcome and the duration.
- Lifecycle, management serving and the control link's plumbing stay in module-core and channel-core; this repo only dials and relays.
- The compose file maps `host.docker.internal` to the host, so a minion run on the developer's machine is a target by that name.
