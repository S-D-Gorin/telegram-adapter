# Sprotect Telegram Adapter

Sprotect Telegram Adapter is an independent, single-bot service between the Telegram Bot API and Sprotect Backend. It contains no moderation business logic and never decides whether a violation occurred.

The intended transport boundaries are:

```text
Telegram → Adapter → HTTPS Platform Events → Sprotect
Sprotect → WebSocket Operations → Adapter → Telegram
Adapter → HTTPS Platform Results → Sprotect
```

The adapter bootstraps a persistent identity with Sprotect and waits for an Organization owner to pair it in Guardian. Once paired, it long-polls Telegram, sends raw Telegram Updates to Platform Events API, receives WebSocket operations, and executes supported Telegram operations (`send_message`, `delete_message`, `get_chat_member`, `get_resource_administrators`, `get_resource_member_count`, `get_resource_info`, and `get_resource_bot_membership`). Terminal outcomes are durably stored before the adapter sends a Platform Result.

## Run with Docker Compose

Create your local configuration, keeping the real token out of version control:

```bash
cp .env.example .env
# Edit .env and set BOT_TOKEN
docker compose up --build
```

On its first startup, the adapter prints a one-time **Adapter registration key**. Open Guardian as the Organization owner and connect the adapter using this key. The running adapter polls the bootstrap status with exponential backoff; once pairing is confirmed it obtains and durably stores its adapter token, logs `adapter ready`, and starts Telegram long polling.

Each raw Telegram Update is sent to `POST /api/v1/platform-adapters/events/` in this envelope:

```json
{
  "schema_version": 1,
  "event_id": "telegram:<update_id>",
  "platform": "telegram",
  "event_type": "update",
  "occurred_at": "<UTC timestamp>",
  "payload": { "update_id": 123 }
}
```

The adapter uses the following paths derived from `SERVER_API`: bootstrap under `/api/v1/platform-adapters/bootstrap/`, events at `/api/v1/platform-adapters/events/`, results at `/api/v1/platform-adapters/results/`, and commands at `/api/v1/platform-adapters/commands/ws/`. A trailing slash in `SERVER_API` is safe; HTTPS automatically maps the command URL to `wss://`.

A Telegram `409` means another polling instance is active and is retried slowly.

## Inbound event delivery

```text
Telegram getUpdates → SQLite outbox (one commit per batch + offset)
                    → partition by chat_id → FIFO per chat → ≤ EVENT_DELIVERY_CONCURRENCY parallel POSTs → Sprotect
```

**Requested update types.** Every `getUpdates` call explicitly sends `allowed_updates` = `message`, `edited_message`, `channel_post`, `edited_channel_post`, `chat_member`, `my_chat_member` — exactly the types Sprotect ingests. Telegram stores this setting per bot, so sending it on every call keeps the adapter independent of any earlier configuration of the same bot. `chat_member` is delivered by Telegram only when the bot is a chat administrator.

**Offset semantics.** Every `getUpdates` batch is stored in the `platform_events` outbox together with the new Telegram offset in a single SQLite commit. The offset therefore means *durably stored by the adapter*, not *delivered to Sprotect*; from that commit on, delivery is owned by the outbox. A crash before the commit simply makes Telegram resend the batch, and re-storing a known `telegram:<update_id>` is a no-op that keeps the original envelope.

**Ordering.** Updates are partitioned by Telegram `chat_id`. Within a chat, events are delivered strictly in `update_id` order: the next event starts only after Sprotect ACKs the previous one with `202 accepted` or `200 duplicate`, and a retrying event is never overtaken. Different chats are delivered in parallel, with at most `EVENT_DELIVERY_CONCURRENCY` HTTP requests in flight. Updates without a usable chat (e.g. `poll`) are still forwarded raw in one shared `unrouted` FIFO partition. There is no global order across chats.

**Group → supergroup migration.** When the old group's service message carries `migrate_to_chat_id`, later events of the new supergroup wait until that migration event is ACKed, rejected or expired; all other chats continue. The dependency is derived from durable rows, so it survives restarts.

**Retries.** Network errors, `408` and `5xx` retry the unchanged envelope with exponential backoff per event; transient failures across several chats with no success in between pause all delivery briefly (circuit breaker). `429` pauses all chats for `Retry-After` (or the backoff when absent). `401` pauses all chats and is never counted against an event. Other `4xx` are retried up to `EVENT_MAX_ATTEMPTS` times, after which the event becomes `rejected` and stops blocking its chat. Each row keeps its attempt count, last HTTP status, last error and attempt times for diagnosis.

**Retention and backpressure.** A pending event older than `EVENT_PENDING_TTL_SECONDS` becomes `expired` and leaves delivery. `delivered`, `rejected` and `expired` rows are deleted `EVENT_RETENTION_SECONDS` after they finish. When `EVENT_MAX_PENDING` events are pending, Telegram polling pauses until the backlog drains; updates meanwhile stay in Telegram.

**Shutdown.** Stopping the adapter cancels in-flight HTTP deliveries; those events stay pending and are resent after restart, where Sprotect deduplicates them by `event_id` (at-least-once).

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVENT_DELIVERY_CONCURRENCY` | `8` | Max simultaneous event deliveries (distinct chats), 1–256; also the HTTP pool size |
| `EVENT_MAX_ATTEMPTS` | `5` | Permanent `4xx` failures before an event is `rejected` |
| `EVENT_PENDING_TTL_SECONDS` | `172800` | Age after which an undelivered event expires (≥ 60) |
| `EVENT_RETENTION_SECONDS` | `172800` | How long finished rows are kept for diagnosis |
| `EVENT_MAX_PENDING` | `10000` | Pending events at which Telegram polling pauses |

`platform_events_stats` log lines report pending and retrying events, oldest pending age, active partitions and deliveries, throughput, and retry/rejected/expired/migration-wait/backpressure counters; per-event logs carry `event_id`, `telegram_update_id` and `partition_key`.

The adapter creates its durable local SQLite database at `/data/adapter.db`. Compose persists it in the `telegram-adapter-data` named volume. The volume includes the installation identity, pairing secret, and adapter token, so it must be retained when moving the adapter to another server. Losing it creates a new installation that must be paired again. If the server has already issued a one-time token but the local durable write was lost, the token cannot be recovered under the bootstrap security contract.

The Docker healthcheck verifies that this initialized database remains readable; no HTTP server is started for health checks. `BOT_TOKEN` remains only inside the adapter and is used only for Telegram Bot API; RabbitMQ is not required by this container.

Runtime logs include a safe readiness snapshot: `bootstrap_ready`, `telegram_polling_ready`, `platform_events_ready`, and `platform_commands_ready`. Process liveness alone does not mean every transport is connected.

## WebSocket operation receipt

The adapter connects to `/api/v1/platform-adapters/commands/ws/` using `Authorization: Bearer <adapter_token>`. An incoming operation is validated, committed to SQLite, and only then ACKed. The operation inbox has a unique `operation_id`: a redelivery with unchanged content is ACKed again without a second row; a reused ID with changed content is rejected as a protocol violation. WebSocket reconnects do not stop Telegram inbound polling.

ACK means only that the adapter has durably received the operation; it does not mean Telegram has executed it.

## Execution and result delivery

The local inbox lifecycle is `received → executing → completed`; a completed operation has exactly one durable pending result with a stable UUID. The result is posted to `POST /api/v1/platform-adapters/results/` with the adapter Bearer token until Sprotect returns `202 accepted` or `200 duplicate`, after which it is locally marked delivered. Network failures and `5xx` retry with the unchanged result envelope; terminal `4xx` leave the result durable and log a degraded state.

Telegram cannot prove whether a `sendMessage` request succeeded if the process crashes or loses transport after marking the operation `executing`. On startup those rows become `execution_unknown` and are deliberately not replayed automatically, preventing a duplicate user-visible message.

`get_chat_member` and the `get_resource_*` operations are read-only and therefore have different recovery semantics. Telegram `429`, network/timeouts, and `5xx` keep their durable operations pending and retry with bounded backoff; an `executing` read-only lookup is safely returned to `received` after restart. Telegram `400` errors that identify an invalid or unavailable chat/member are terminal and produce a failed Platform Result with `retryable: false`. Successful resource operations use a platform-neutral `resource.id` input and return normalized state plus optional opaque Telegram `raw_data`; Sprotect remains responsible for membership business logic.

Stop it with `Ctrl+C`; Docker sends `SIGTERM` on normal container shutdown and the adapter closes SQLite gracefully.

## Guardian Resource State Sync contract

The following read-only operations implement the Guardian Resource State Sync wire contract:

- `get_resource_administrators`
- `get_resource_bot_membership`
- `get_resource_info`
- `get_resource_member_count`

Each request payload must contain a non-empty **string** Telegram resource ID:

```json
{"resource": {"id": "-100123"}}
```

Numeric JSON IDs are rejected. Every successful snapshot contains `observed_at`: a UTC, timezone-aware ISO-8601 timestamp (emitted as `Z`) captured immediately after the successful Telegram response and before normalization. It is part of the durable terminal result, not generated during delivery, and precedes `completed_at`.

`get_resource_administrators` returns an authoritative Telegram administrator snapshot. In particular, Guardian uses its `observed_at` field as a freshness fence.

```json
{
  "resource": {"id": "-100123"},
  "administrators": [
    {
      "user": {"id": "42", "is_bot": false, "first_name": "Owner"},
      "role": "creator",
      "is_anonymous": false
    }
  ],
  "observed_at": "2026-01-01T00:00:00Z"
}
```

`get_resource_info` supports only `group`, `supergroup`, and `channel`. A private chat produces terminal `unsupported_resource_type`; an absent Telegram username is represented explicitly as `null`.

```json
{
  "resource": {
    "id": "-100123",
    "title": "Example",
    "username": null,
    "type": "supergroup"
  },
  "observed_at": "2026-01-01T00:00:00Z"
}
```

`get_resource_member_count` returns a nonnegative JSON integer, including zero.

```json
{
  "resource": {"id": "-100123"},
  "member_count": 0,
  "observed_at": "2026-01-01T00:00:00Z"
}
```

`get_resource_bot_membership` identifies the current bot with `getMe`, then reads its membership using `getChatMember`. Administrator permissions are normalized as booleans: Telegram `can_delete_messages` becomes `permissions.delete_messages` and `can_post_messages` becomes `permissions.post_messages`. Missing administrator permissions are `false`; a creator and non-member have an empty permissions object.

```json
{
  "resource": {"id": "-100123"},
  "bot": {"id": "777", "is_bot": true, "first_name": "Guardian"},
  "membership": {
    "status": "administrator",
    "role": "administrator",
    "is_member": true,
    "permissions": {"delete_messages": true, "post_messages": false}
  },
  "observed_at": "2026-01-01T00:00:00Z"
}
```

If a successful Telegram membership response authoritatively says `left`, `kicked`, or `restricted` with `is_member=false`, the adapter sends one failed terminal result with `error.code = "bot_not_member"`, `retryable = false`, and the complete normalized membership snapshot in `result`. Transport failures, rate limits, server failures, access denials, unavailable chats, and malformed responses are never converted to `bot_not_member`.

All terminal outcomes use the common Platform Result envelope. Optional `raw_data` is diagnostic-only; Guardian domain logic must consume the normalized fields above.

## Production image versioning

This contract-compatible release is version `0.3.0`. Do not deploy `:latest`. Build and publish the verified image deliberately, then pin Guardian deployment to the resulting immutable digest:

```bash
docker build -t sprotectbots/telegram-adapter:0.3.0 .
docker push sprotectbots/telegram-adapter:0.3.0
docker buildx imagetools inspect sprotectbots/telegram-adapter:0.3.0
```

Set production to `sprotectbots/telegram-adapter:0.3.0@sha256:<verified-digest>`. The checked-in Compose file uses the versioned tag for local and controlled deployments; replace it with the verified digest in the production manifest.

## Local development

Python 3.13 is required. Install the test extras and run:

```bash
python -m pip install -e ".[dev]"
pytest
docker compose build
docker compose up
```

## Dependencies

- `httpx` provides Telegram long polling, Sprotect bootstrap, and Platform Events API transport.
- `websockets` is reserved for the future Sprotect command stream.
- SQLite and `asyncio` use the Python standard library.
- `pytest` and `pytest-asyncio` are development/test-only dependencies.
