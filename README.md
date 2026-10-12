# action_cable.cr

A 1:1 Crystal port of **actioncable 8.1.4** (Rails' Action Cable) under
the same `ActionCable` namespace. WebSocket layer is fiber-per-connection
on top of the Crystal stdlib `HTTP::WebSocket` (no nio4r + thread pool),
speaking the exact Rails JSON protocol (`welcome` /
`subscription_confirmation` / `ping` / `disconnect`, commands
`subscribe`/`message`/`unsubscribe`) — the official `@rails/actioncable`
JS client works unchanged. Pubsub adapters: Async (in-process, default),
PostgreSQL LISTEN/NOTIFY, Redis pubsub. The hot path carries a
Bun/uWS-style fast broadcast (encode-once envelopes, per-connection cork
buffers, 1 write per batch, 1 MiB backpressure cap): 2.72M msg/s fan-out
vs 2.52M for bun-publish. Suite: **51/51**, including **33 oracle
cases** recorded with the real actioncable 8.1.4.

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

## Installation

```yaml
dependencies:
  action_cable_cr:
    github: OrelSokolov/action_cable.cr
```

```sh
shards install
```

Depends on `active_support.cr`, `active_record.cr`, `action_pack.cr`
and the `pg` / `redis` shards.

## Usage

```crystal
require "action_cable_cr"

class ActionCable::Connection
  identified_by :current_user

  def connect
    reject_unauthorized_connection! unless session["warden.user.user.key"]?
    self.current_user = session["user_gid"]
  end
end

class ChatChannel < ActionCable::Channel::Base
  def subscribed
    stream_from "chat_#{params["room"]}"
  end

  def speak(data)                       # public method = client RPC action
    ActionCable.server.broadcast("chat_#{params["room"]}", data)
  end

  periodically :transmit_presence, every: 30
end

server = ActionCable::Server.new                # Async adapter by default
HTTP::Server.new([server.ws_handler]).listen(3000)  # ws://localhost:3000/cable
```

Cross-process setups swap the adapter
(`SubscriptionAdapter::Redis.new(url)` or
`SubscriptionAdapter::PostgreSQL.new(url)`); remote disconnect:

```crystal
ActionCable.server.remote_connections.where(current_user: "1").disconnect(reconnect: true)
```

## Feature comparison vs actioncable 8.1.4

### Connection & protocol

| Feature | Original (actioncable) | Port (action_cable.cr) | Status |
|---|---|---|---|
| Rails JSON wire protocol, `@rails/actioncable` JS client compat | ✅ | Same protocol end-to-end (verified by WS integration spec) | ✅ |
| `identified_by` / `connection_identifier` (sorted gid join) | ✅ | Macro registry; String values only | ⚠️ partial |
| `connect` / `disconnect` hooks, `reject_unauthorized_connection` (unauthorized disconnect frame) | ✅ | Same | ✅ |
| Cookie auth (`cookies`) + verified signed session in `connect` | ✅ | `request_cookies` + Phase-C signed session (via action_pack.cr) | ✅ |
| `before_command` / `after_command` / `around_command` | ✅ | Not implemented (P2) | ❌ |
| Remote disconnect (`RemoteConnections.where(...).disconnect`) | ✅ | Same internal-channel design, cross-process on Redis/PG | ✅ |

### Channel DSL

| Feature | Original | Port (action_cable.cr) | Status |
|---|---|---|---|
| `subscribed` / `unsubscribed`, `params`, `reject`, `transmit` | ✅ | Same | ✅ |
| `perform_action` dispatch to public methods | ✅ | Compile-time macro dispatch table | ✅ |
| `stream_from` (with/without callback block), `stop_stream_from`, `stop_all_streams` | ✅ | Same; `coder:` option missing | ⚠️ partial |
| `stream_for` / `stop_stream_for` / `stream_or_reject_for` | ✅ | Same, GlobalID-style gid naming | ✅ |
| `broadcast_to` / `broadcasting_for` | ✅ | Class & instance level | ✅ |
| `periodically` (PeriodicTimers) | ✅ | Method name or block, `every:` span/seconds | ✅ |
| `before_subscribe`/`after_subscribe`/`before_unsubscribe`/`after_unsubscribe` hooks | ✅ | Used internally (periodic timers) only, not user-facing | ❌ |

### Server, pubsub & performance

| Feature | Original | Port (action_cable.cr) | Status |
|---|---|---|---|
| Mount at `/cable`, `ActionCable.server.broadcast` | ✅ | `Server#ws_handler` HTTP handler | ✅ |
| Pubsub adapters | async, PostgreSQL, Redis, (Solid Cable in Rails) | Async, PostgreSQL (LISTEN/NOTIFY), Redis | ✅ |
| `config/cable.yml`, `channel_prefix`, `allowed_request_origins`, `worker_pool_size`, `log_tags` | ✅ | Not implemented | ❌ |
| Fast broadcast (encode-once, cork buffers, backpressure) | n/a | 2.72M msg/s ≈ bun-publish | ✅ |
| Server stats, health-check endpoint | ✅ | Not implemented (P2) | ❌ |
| Buffered WS reads, JSON fast-path on hot path | n/a | Backlog (P1.6) | ❌ |

## Deviations & Crystal-specific notes

- `transmit` is public (private in Ruby; Crystal has no `send`);
  `streams` became public `stream_names` for testing.
- `unsubscribe(channel, callback)` → `unsubscribe(channel)`: Crystal
  Procs have no identity, so it drops in-process callbacks but not the
  underlying Redis subscription.
- Identifier values are Strings only (gid strings); `params` is
  `Hash(String, String)` instead of `HashWithIndifferentAccess`;
  `INTERNAL` is named constants instead of a heterogeneous hash.
- `periodically` is a macro DSL (a Proc is unspellable in a class body);
  `every:` accepts `Time::Span` or seconds, `ArgumentError` on ≤ 0.
- The gid app name comes from `ActionCable.gid_app` (ENV
  `CRAILS_APP_NAME`, default "app"), not `GlobalID.app`.
- `broadcast` returns nil, not a PUBLISH recipient count; PG NOTIFY
  payloads are capped at ~8000 bytes.

Full list with rationale: `PLAN.md` (Russian).

## Testing

```sh
crystal spec          # stream_for/pubsub suites need a local PostgreSQL
                      # (db crails_ac_test) and a local Redis, else they fail
```

Oracle: 33 cases (20 channel/protocol + 5 pubsub + 8 stream_for)
recorded with the real actioncable 8.1.4 (`oracle/cases.rb`,
`oracle/pubsub_cases.rb`, `oracle/stream_for_cases.rb` →
`expected*.yml`); the Crystal specs replay them 1:1 without edits. The
pubsub cases mirror the Redis adapter's behavior against both the Redis
and PostgreSQL adapters. Full suite: 51/51.
