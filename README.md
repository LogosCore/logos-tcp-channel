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
connection is in (its stage, its hello, or a check-in); core pushes over it
whenever it has something for the session.

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
  The minion sends one frame; the channel makes the sync call with it and
  writes core's reply as one frame under the same id, the id alone (length
  16) when core has nothing for the minion, then closes. A message core does
  not take, or a sync that fails, closes the connection with no reply.

## Connection schema

Declared at registration (`schema_version` "2", version 1), three kinds:

| Kind | Built into the minion (`fields`) | Per target (`target`) |
|---|---|---|
| `bind-persistent` | `listen_port` (required), `listen_address` (`0.0.0.0`), `max_connections` (1, up to 16), `keepalive` (30s) | `host` (required), `port` (the listen port when empty), `proxy` (socks5, socks5h or http CONNECT), `connect_timeout` (10s) |
| `reverse-persistent` | `host` and `port` (required: the channel as the minion reaches it), `connect_timeout` (10s), `keepalive` (30s), `reconnect_backoff` (60s), `checkin_interval` (60s), `checkin_jitter` (20) | — |
| `reverse-call` | `host`, `port`, `connect_timeout` (10s), `request_timeout` (40s), `checkin_interval` (60s), `checkin_jitter` (20) | — |

## Configuration

Environment only (a `.env` file fills in what the environment does not set):

| Variable | Default | |
|---|---|---|
| `CHANNEL_ID` | `tcp-main` | the instance id; run each instance with its own |
| `LOGOS_CORE_BASE_URL` | `http://localhost:8002` | core's base URL: the control link and the sockets open under it |
| `MAX_CONNECTIONS` | `1000` | connections open at once, reverse calls under way among them; one past it fails |
| `REVERSE_PERSISTENT_LISTEN` | none | `host:port` the channel listens on for reverse-persistent minions (`:4790`: every interface) |
| `REVERSE_CALL_LISTEN` | none | `host:port` it listens on for reverse-call minions |
| `CORE_SYNC_TIMEOUT` | `30s` | how long a reverse call waits for core: keep it below the minions' `request_timeout` |
| `LOG_FORMAT` | `json` | `json` or `text`, on stderr |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | `localhost`, `5672`, `guest`, `guest`, `/` | core's RabbitMQ |

## Management

| Key | Type | Applies | |
|---|---|---|---|
| `log_level` | `debug` / `info` / `warn` / `error` | live | `info` logs opens, failed opens and closes |

Actions: `drain` (every open fails until resumed; those open stay),
`resume`, and `close_connections` (closes every open connection).
Status: the control link (up or down), connections open, opens, opens
failed, reverse connections, reverse calls and those that failed,
draining. Draining refuses reverse connections and calls too.

On SIGINT/SIGTERM the channel stops opening, closes its connections and the
link, and deregisters.

## Local development

`docker compose up -d --build` joins logos-core's network (start core first;
set `RABBITMQ_PASSWORD` in `.env`). A minion running on the same machine is
reached as `host.docker.internal` (mapped to the host's gateway): add a
target with that host.
