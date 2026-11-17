# logos-tcp-channel

TCP channel module for Logos. It carries **bind-persistent** connections:
core reaches a minion that listens on a TCP port by asking the channel to
dial it; the connection then stays open, the minion and core exchanging
messages over it both ways until it closes. And the reverse kinds, minions
that dial it: **reverse-persistent** (the connection stays open) and
**reverse-call** (a connection for each exchange), each on a port of its
own.

To core the channel only dials out: registration over RabbitMQ, the
control link, one socket per open connection, and the sync call for a
reverse call.

## How a connection opens

1. At start the channel registers with core as `module_type=channel`,
   `module_name=tcp`, `instance=CHANNEL_ID`, and opens its **control link**:
   a WebSocket to `LOGOS_CORE_BASE_URL` + `/api/channel/link` (channel-core's
   `linkclient`). Core refuses it (403) until the instance is registered; the
   channel keeps trying, backing off from 1s to 30s, and reopens it whenever
   it drops.
2. Core sends an **open** over the link: the build connection's and the
   target's values. Who decides when is the target's policy (on demand,
   manual, always: see the docs).
3. The channel dials the target (`host`, `port` or the listen port, through
   `proxy` when set, within `connect_timeout`), then opens the connection's
   socket to core, `/api/channel/connection`, naming the open
   (`Logos-Open-Request`) and the minion's address (`Logos-Peer-Address`:
   the one dialed, or through a proxy the target's host when it is an IP
   address, none for a name), and answers the open. From there it relays frames
   both ways without reading them; when either side closes, it closes the
   other.
4. A dial that fails answers the open with a failure: `unreachable` (DNS,
   refused, no route), `timeout`, or `other`.

See the docs' [Channel ↔ Core: Connections](https://logoscore.github.io/logos-docs/contracts/channel-core-connections/).

## Wire format (channel ↔ minion)

Each message is one frame, both ways:

```
[length: 4 bytes, big-endian][id: 16 bytes, raw UUID][sealed frame]
```

`length` counts the id and the sealed frame. The minion speaks first once a
connection is in (its stage, its hello, or a check-in). On a connection that
stays open core answers every message, with an empty one when it has
nothing, and pushes whenever it has something for the session.

A target may name a **delegate** instead (docs: Delegation): another minion
of the operation, whose factory serves this kind (BOB's `delegate`
command), dials the target from its own host in the channel's place, with
the same framing, and carries the frames in its own messages. The minion
listening is the same either way. A target's `proxy` is the channel's
alone: a delegate dials directly.

## The reverse kinds

A minion that dials the channel speaks the same frames:

- **reverse-persistent** (`REVERSE_PERSISTENT_LISTEN`): its first frame
  (within 30s) opens the connection's socket to core, its address the peer;
  frames go both ways until either side closes. A channel whose socket to
  core closes closes the minion's connection, and the minion dials again.
- **reverse-call** (`REVERSE_CALL_LISTEN`): one exchange per connection.
  The minion sends one frame (within 30s); the channel makes the sync call
  with it, waiting `CORE_SYNC_TIMEOUT` for core, and writes core's reply as
  one frame under the same id, the id alone (length 16) when core has
  nothing for the minion, then closes. A message core does not take, or a
  sync that fails, closes the connection with no reply.

A reverse minion may dial a **delegate** instead (docs: Delegation): a
minion of the operation whose factory serves these kinds (BOB's
`reverse_delegate` command) listens on its own host in the channel's place,
with the same framing, and the minion is built with its address as `host`
and `port`.

## Connection schema

Declared at registration (`schema_version` "2", version 1), three kinds:

| Kind | Built into the minion (`fields`) | Per target (`target`) |
|---|---|---|
| `bind-persistent` | `listen_port`, `listen_address`, `max_connections`, `keepalive` | `host`, `port`, `proxy`, `connect_timeout` |
| `reverse-persistent` | `host`, `port`, `connect_timeout`, `keepalive`, `reconnect_backoff`, `checkin_interval`, `checkin_jitter` | — |
| `reverse-call` | `host`, `port`, `connect_timeout`, `request_timeout`, `checkin_interval`, `checkin_jitter` | — |

Every field has a number, the same in each kind that has it, a target's
fields sharing the numbering: a minion built from core's [minion
config](https://logoscore.github.io/logos-docs/contracts/minion-build/#the-minion-config)
reads its settings by these numbers alone, so a number stays its field's
(`TestFieldNumbers` pins them), a new field takes the next one, and a
removed field's is never given again.

| Key | Num | Default | Notes |
|---|---|---|---|
| `host` | 1 | required | A reverse kind's: the channel's address or name, as the minion reaches it. A target's: the address or name the channel dials. At most 255 characters |
| `port` | 2 | | 1 to 65535. A reverse kind's (required): the port of the kind's listener. A target's: the port to dial when it is not the minion's listen port (a port forward, a NAT); empty, the listen port |
| `connect_timeout` | 3 | `10s` | 1s to 2m: how long a dial may take |
| `keepalive` | 4 | `30s` | 5s to 1h: TCP keepalive on the connection, so a peer gone without a word is found |
| `reconnect_backoff` | 5 | `60s` | 1s to 1h: the longest wait between dials once the connection closed, from 1s, doubling |
| `request_timeout` | 6 | `40s` | 1s to 10m: how long the minion waits for core's reply; keep it above `CORE_SYNC_TIMEOUT` |
| `checkin_interval` | 7 | `60s` | 1s to 168h. `reverse-call`: how often the minion dials with nothing to send. `reverse-persistent`: how soon core expects it back once its connection closed (late, then lost, by it); while it is open no check-in is needed |
| `checkin_jitter` | 8 | `20` | 0 to 100 %: random spread around the interval, either way |
| `listen_port` | 9 | required | 1 to 65535: the port the minion listens on |
| `listen_address` | 10 | `0.0.0.0` | The interface the minion binds: `0.0.0.0` every IPv4 one, `::` IPv6 too, `127.0.0.1` one reached only from its own host (through a tunnel) |
| `max_connections` | 11 | `1` | 1 to 16: connections the minion takes at once; more than one lets a new dial in while an old one has not timed out |
| `proxy` | 12 | | A target's: dial through `socks5://` or `socks5h://` (an SSH dynamic forward, a pivot, a minion's SOCKS tunnel: below) or `http://` (CONNECT). At most 2048 characters |

## A minion behind another

The channel reaches a listening minion it cannot dial directly through
another minion, A, that can:

- **A's SOCKS tunnel**: a `tunnel` on a port of its own (`tunnel 41001`,
  say), kept open (`stop_idle` 0s, or the target always connected) so the
  address holds, and the target's `proxy` naming it at core's address as
  the channel reaches core: `socks5://logos-core-dev:41001` on the dev
  stack.
- **A as the target's delegate** (above): A dials the target itself, and
  the target's `proxy` is not used.

See the docs' [Tunnels](https://logoscore.github.io/logos-docs/contracts/minion-messages/tunnels/)
and [Delegation](https://logoscore.github.io/logos-docs/contracts/minion-messages/delegation/).

## Configuration

Environment only (a `.env` file fills in what the environment does not set):

| Variable | Default | |
|---|---|---|
| `CHANNEL_ID` | `tcp-main` | the instance id; run each instance with its own |
| `LOGOS_CORE_BASE_URL` | `http://localhost:8002` | core's base URL: the control link and the sockets open under it |
| `LOGOS_CORE_CA_FILE`, `LOGOS_CORE_CERT_SHA256`, `LOGOS_CORE_TLS_INSECURE` | none (the system's trust) | how it trusts core's certificate over HTTPS, for a self-signed one: a PEM to trust, the certificate's SHA-256 fingerprint, or no verification; see [How modules reach core](https://logoscore.github.io/logos-docs/core-infrastructure/#how-modules-reach-core) |
| `MAX_CONNECTIONS` | `1000` | connections open at once, reverse calls under way among them; one past it fails |
| `REVERSE_PERSISTENT_LISTEN` | none | `host:port` the channel listens on for reverse-persistent minions (`:4790`: every interface) |
| `REVERSE_CALL_LISTEN` | none | `host:port` it listens on for reverse-call minions |
| `CORE_SYNC_TIMEOUT` | `30s` | how long a reverse call waits for core: keep it below the minions' `request_timeout` |
| `LOG_FORMAT` | `json` | `json` or `text`, on stderr |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_VHOST` | `localhost`, `5672`, `logos`, `/` | core's RabbitMQ (logos-core's dev stack, from the host) |
| `RABBITMQ_PASSWORD` | none: required | its password, logos-core's `RABBITMQ_DEFAULT_PASS` |

## Management

| Key | Type | Applies | |
|---|---|---|---|
| `log_level` | `debug` / `info` / `warn` / `error` | live | `info` logs opens, failed opens and closes |

Actions: `drain` (every open fails until resumed; those open stay),
`resume`, and `close_connections` (closes every open connection).
Status: the control link (`link_up`), connections open, opens, opens
failed, reverse connections, reverse calls and those that failed,
draining. Draining refuses reverse connections and calls too.

On SIGINT/SIGTERM the channel stops opening and listening, closes its
connections (core shows them not connected, and opens them again by their
policy once a channel is back), closes the link (waiting up to 5s for it),
and deregisters last.

## Local development

`docker compose up -d --build` joins logos-core's network (start core first;
set `RABBITMQ_PASSWORD` in `.env`). A minion running on the same machine is
reached as `host.docker.internal` (mapped to the host's gateway): add a
target with that host. The compose file listens for the reverse kinds on
`:4790` (reverse-persistent) and `:4791` (reverse-call), published on the
host's same ports.

### Against an unreleased protocol or SDK

The image builds from tagged modules alone (`.dockerignore` keeps a
workspace out). With a local `go.work` (`use .` and
`../logos-golang-protocol`, checkouts side by side), run a binary built on
the host, mounted over the image's:

```bash
CGO_ENABLED=0 GOOS=linux go build -o .dev-bin/tcp-channel ./cmd/tcp-channel
```

```yaml
# docker-compose.override.yml
services:
  tcp-channel:
    volumes:
      - ./.dev-bin/tcp-channel:/app/tcp-channel:ro
```

Then `docker compose up -d`, without `--build`; after each change, build it
again and `docker compose restart`. `go.work`, the override and `.dev-bin/`
are gitignored and never committed (the docs' [Changing the
Protocol](https://logoscore.github.io/logos-docs/contributing/changing-the-protocol/)).

## Code

- `cmd/tcp-channel/main.go` runs a channel's three parts side by side: the
  control-plane session (channel-core's `channel.Run`, with the connection
  schema), the reverse kinds' listeners, and the control link
  (`linkclient.Run`, the relay's `Open` its `Opener`, and a `Handler` that
  fails every call: the channel has no `bind-call` kind).
- `internal/relay`: an open (the dial, the connection's socket, a copy each
  way) and the reverse kinds (`reverse.go`); `internal/connection`: the
  schema.

## Tests

`go test -race ./...`. The relay's tests run against fakes of core:
`fakeCore` takes the connection sockets (`internal/relay/relay_test.go`),
`reverseCore` and `fakeSync` a reverse kind's socket and sync call
(`reverse_test.go`). CI (`.github/workflows/ci.yml`) runs `go test ./...`
and golangci-lint v2.13.2 (its default linters, kept at 0 issues) on pushes
to `main`, `v*` tags and pull requests; a `v*` tag builds and pushes the
image, `linux/amd64` and `linux/arm64` (`docker-image.yml`).
