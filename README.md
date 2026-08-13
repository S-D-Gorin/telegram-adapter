# Sprotect Telegram Adapter

Sprotect Telegram Adapter is an independent, single-bot service between the Telegram Bot API and Sprotect Backend. It contains no moderation business logic and never decides whether a violation occurred.

The intended transport boundaries are:

```text
Telegram → Adapter → HTTPS Platform Events → Sprotect
Sprotect → WebSocket Operations → Adapter
```

The adapter bootstraps a persistent identity with Sprotect and waits for an Organization owner to pair it in Guardian. Once paired, it long-polls Telegram and sends raw Telegram Updates to Sprotect Platform Events API. It also receives WebSocket operations, persistently stores them, and ACKs their receipt. Stage 4 does **not** execute operations through Telegram API and does not send Platform Results.

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

ACK means only that the adapter has durably received the operation. `send_message`, `delete_message`, and every other operation remain unexecuted until a later stage.

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
