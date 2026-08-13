# Sprotect Telegram Adapter

Sprotect Telegram Adapter is an independent, single-bot service between the Telegram Bot API and Sprotect Backend. It contains no moderation business logic and never decides whether a violation occurred.

The intended transport boundaries are:

```text
Telegram → POST /platform/events → Sprotect
Sprotect → WebSocket /platform/commands → Adapter
Adapter → POST /platform/results → Sprotect
```

Stage 1 creates only the service skeleton: it does **not** connect to Telegram or Sprotect, and does not implement pairing, commands, results, retries, or polling.

## Run with Docker Compose

Create your local configuration, keeping the real token out of version control:

```bash
cp .env.example .env
# Edit .env and set BOT_TOKEN
docker compose up --build
```

The adapter creates its durable local SQLite database at `/data/adapter.db`. Compose persists it in the `telegram-adapter-data` named volume. The Docker healthcheck verifies that this initialized database remains readable; no HTTP server is started for health checks.

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

- `httpx` is reserved for the future Sprotect HTTP event/result transport.
- `websockets` is reserved for the future Sprotect command stream.
- SQLite and `asyncio` use the Python standard library.
- `pytest` and `pytest-asyncio` are development/test-only dependencies.
