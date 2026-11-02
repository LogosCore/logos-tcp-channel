# logos-tcp-channel

TCP channel module for Logos: it carries **bind-persistent** connections.
Core reaches a minion that listens on a TCP port by asking the channel to
dial it; the connection then stays open, the minion and core exchanging
messages over it both ways until it closes.

The channel only dials out: to core (registration over RabbitMQ, the control
link, and one socket per open connection) and to minions. It listens on no
port.

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

## Connection schema

Declared at registration (`schema_version` "2", version 1), one kind:

| Kind | Built into the minion (`fields`) | Per target (`target`) |
|---|---|---|
| `bind-persistent` | `listen_port` (required), `listen_address` (`0.0.0.0`), `max_connections` (1, up to 16), `keepalive` (30s) | `host` (required), `port` (the listen port when empty), `proxy` (socks5, socks5h or http CONNECT), `connect_timeout` (10s) |

## Configuration

Environment only (a `.env` file fills in what the environment does not set):

| Variable | Default | |
|---|---|---|
| `CHANNEL_ID` | `tcp-main` | the instance id; run each instance with its own |
| `LOGOS_CORE_BASE_URL` | `http://localhost:8002` | core's base URL: the control link and the sockets open under it |
| `MAX_CONNECTIONS` | `1000` | connections open at once; an open past it fails |
| `LOG_FORMAT` | `json` | `json` or `text`, on stderr |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | `localhost`, `5672`, `guest`, `guest`, `/` | core's RabbitMQ |

## Management

| Key | Type | Applies | |
|---|---|---|---|
| `log_level` | `debug` / `info` / `warn` / `error` | live | `info` logs opens, failed opens and closes |

Actions: `drain` (every open fails until resumed; those open stay),
`resume`, and `close_connections` (closes every open connection).
Status: the control link (up or down), connections open, opens, opens
failed, draining.

On SIGINT/SIGTERM the channel stops opening, closes its connections and the
link, and deregisters.

## Local development

`docker compose up -d --build` joins logos-core's network (start core first;
set `RABBITMQ_PASSWORD` in `.env`). A minion running on the same machine is
reached as `host.docker.internal` (mapped to the host's gateway): add a
target with that host.
