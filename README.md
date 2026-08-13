# Sprotect Telegram Adapter

Sprotect Telegram Adapter is an independent, single-bot service between the Telegram Bot API and Sprotect Backend. It contains no moderation business logic and never decides whether a violation occurred.

The intended transport boundaries are:

```text
Telegram → Adapter → HTTPS Platform Events → Sprotect
Sprotect → WebSocket Operations → Adapter → Telegram
Adapter → HTTPS Platform Results → Sprotect
```

The adapter bootstraps a persistent identity with Sprotect and waits for an Organization owner to pair it in Guardian. Once paired, it long-polls Telegram, sends raw Telegram Updates to Platform Events API, receives WebSocket operations, and executes supported Telegram operations (`send_message`, `delete_message`). Terminal outcomes are durably stored before the adapter sends a Platform Result.

## Run with Docker Compose

Create your local configuration, keeping the real token out of version control:

```bash
cp .env.example .env
# Edit .env and set BOT_TOKEN
docker compose up --build
```

On its first startup, the adapter prints a one-time **Adapter registration key**. Open Guardian as the Organization owner and connect the adapter using this key. The running adapter polls the bootstrap status with exponential backoff; once pairing is confirmed it obtains and durably stores its adapter token, logs `adapter ready`, and starts Telegram long polling.

Each raw Telegram Update is sent sequentially to `POST /api/v1/platform/events/` in this envelope:

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

The durable Telegram offset advances only after Sprotect returns `202 accepted` or `200 duplicate`. On first use no offset is sent to Telegram, so pending updates are delivered rather than silently discarded. Temporary backend failures retain the offset and retry the same stable event ID; rejected events and revoked credentials enter a visible degraded state without losing the update. A Telegram `409` means another polling instance is active and is retried slowly.

The adapter creates its durable local SQLite database at `/data/adapter.db`. Compose persists it in the `telegram-adapter-data` named volume. The volume includes the installation identity, pairing secret, and adapter token, so it must be retained when moving the adapter to another server. Losing it creates a new installation that must be paired again. If the server has already issued a one-time token but the local durable write was lost, the token cannot be recovered under the bootstrap security contract.

The Docker healthcheck verifies that this initialized database remains readable; no HTTP server is started for health checks. `BOT_TOKEN` remains only inside the adapter and is used only for Telegram Bot API; RabbitMQ is not required by this container.

## WebSocket operation receipt

The adapter connects to `/api/v1/platform-adapters/commands/ws/` using `Authorization: Bearer <adapter_token>`. An incoming operation is validated, committed to SQLite, and only then ACKed. The operation inbox has a unique `operation_id`: a redelivery with unchanged content is ACKed again without a second row; a reused ID with changed content is rejected as a protocol violation. WebSocket reconnects do not stop Telegram inbound polling.

ACK means only that the adapter has durably received the operation; it does not mean Telegram has executed it.

## Execution and result delivery

The local inbox lifecycle is `received → executing → completed`; a completed operation has exactly one durable pending result with a stable UUID. The result is posted to `POST /api/v1/platform-adapters/results/` with the adapter Bearer token until Sprotect returns `202 accepted` or `200 duplicate`, after which it is locally marked delivered. Network failures and `5xx` retry with the unchanged result envelope; terminal `4xx` leave the result durable and log a degraded state.

Telegram cannot prove whether a `sendMessage` request succeeded if the process crashes or loses transport after marking the operation `executing`. On startup those rows become `execution_unknown` and are deliberately not replayed automatically, preventing a duplicate user-visible message. Rate-limit (`429`) retries are bounded; normal Telegram terminal errors produce a failed result.

Stop it with `Ctrl+C`; Docker sends `SIGTERM` on normal container shutdown and the adapter closes SQLite gracefully.

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
