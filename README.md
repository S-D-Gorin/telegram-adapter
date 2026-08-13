# Sprotect Telegram Adapter

Sprotect Telegram Adapter is an independent, single-bot service between the Telegram Bot API and Sprotect Backend. It contains no moderation business logic and never decides whether a violation occurred.

The intended transport boundaries are:

```text
Telegram → POST /platform/events → Sprotect
Sprotect → WebSocket /platform/commands → Adapter
Adapter → POST /platform/results → Sprotect
```

The adapter bootstraps a persistent identity with Sprotect and waits for an Organization owner to pair it in Guardian. It still does **not** connect to Telegram or implement events, commands, results, or Telegram polling.

## Run with Docker Compose

Create your local configuration, keeping the real token out of version control:

```bash
cp .env.example .env
# Edit .env and set BOT_TOKEN
docker compose up --build
```

On its first startup, the adapter prints a one-time **Adapter registration key**. Open Guardian as the Organization owner and connect the adapter using this key. The running adapter polls the bootstrap status with exponential backoff; once pairing is confirmed it obtains and durably stores its adapter token, then logs `adapter ready`.

The adapter creates its durable local SQLite database at `/data/adapter.db`. Compose persists it in the `telegram-adapter-data` named volume. The volume includes the installation identity, pairing secret, and adapter token, so it must be retained when moving the adapter to another server. Losing it creates a new installation that must be paired again. If the server has already issued a one-time token but the local durable write was lost, the token cannot be recovered under the bootstrap security contract.

The Docker healthcheck verifies that this initialized database remains readable; no HTTP server is started for health checks.

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

- `httpx` provides the Sprotect bootstrap client and will support future HTTP event/result transport.
- `websockets` is reserved for the future Sprotect command stream.
- SQLite and `asyncio` use the Python standard library.
- `pytest` and `pytest-asyncio` are development/test-only dependencies.
