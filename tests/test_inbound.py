import asyncio
import json
import logging

import httpx
import pytest

from app.application import Application
from app.config import Config
from app.sprotect import (
    PairingState,
    PermanentPlatformEventError,
    SprotectPlatformEventsClient,
    TransientPlatformEventError,
)
from app.storage import SQLiteStorage
from app.telegram import TelegramBotClient, TelegramPollingConflictError


class BootstrapUnused:
    async def close(self) -> None:
        pass


class UnpairedBootstrap:
    def __init__(self) -> None:
        self.status_calls = 0

    async def register_adapter(self, adapter_id, pairing_secret):
        return PairingState("unpaired")

    async def get_pairing_status(self, adapter_id, pairing_secret):
        self.status_calls += 1
        return PairingState("unpaired")

    async def close(self) -> None:
        pass


class TelegramStub:
    def __init__(self, responses) -> None:
        self.responses = iter(responses)
        self.offsets: list[int | None] = []
        self.closed = False

    async def get_updates(self, offset, *, timeout=30):
        self.offsets.append(offset)
        response = next(self.responses)
        if isinstance(response, asyncio.Event):
            await response.wait()
        if isinstance(response, Exception):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


class EventsStub:
    def __init__(self, outcomes) -> None:
        self.outcomes = iter(outcomes)
        self.events: list[dict] = []
        self.closed = False

    async def deliver(self, event):
        self.events.append(event)
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def close(self) -> None:
        self.closed = True


async def wait_for(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


async def prepare_active_identity(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    await storage.get_or_create_identity()
    await storage.activate_identity("adapter-token")
    await storage.close()


def update(number: int) -> dict:
    return {"update_id": number, "message": {"message_id": number, "text": "private text"}}


def polling_app(tmp_path, telegram, events, *, delay=0.01) -> Application:
    return Application(
        Config("https://backend.example", "telegram-bot-token", tmp_path, "INFO"),
        bootstrap_client=BootstrapUnused(),  # type: ignore[arg-type]
        telegram_client=telegram,  # type: ignore[arg-type]
        platform_events_client=events,  # type: ignore[arg-type]
        initial_retry_delay=delay,
        max_retry_delay=0.05,
    )


@pytest.mark.asyncio
async def test_polling_does_not_start_before_pairing(tmp_path) -> None:
    telegram = TelegramStub([asyncio.Event()])
    events = EventsStub([])
    bootstrap = UnpairedBootstrap()
    application = Application(
        Config("https://backend.example", "telegram-bot-token", tmp_path, "INFO"),
        bootstrap_client=bootstrap,  # type: ignore[arg-type]
        telegram_client=telegram,  # type: ignore[arg-type]
        platform_events_client=events,  # type: ignore[arg-type]
        initial_retry_delay=1,
    )

    await application.start()
    await wait_for(lambda: bootstrap.status_calls == 1)
    assert telegram.offsets == []
    await application.stop()


@pytest.mark.asyncio
async def test_update_is_sent_as_stage_3a_envelope_and_accepted_advances_offset(tmp_path) -> None:
    await prepare_active_identity(tmp_path)
    telegram = TelegramStub([[update(10)], asyncio.Event()])
    events = EventsStub(["accepted"])
    application = polling_app(tmp_path, telegram, events)

    await application.start()
    await wait_for(lambda: len(events.events) == 1)
    assert await application.storage.get_telegram_offset() == 11
    event = events.events[0]
    assert event["schema_version"] == 1
    assert event["event_id"] == "telegram:10"
    assert event["platform"] == "telegram"
    assert event["event_type"] == "update"
    assert event["payload"] == update(10)
    assert event["occurred_at"].endswith("Z")
    await application.stop()


@pytest.mark.asyncio
async def test_duplicate_advances_offset_and_updates_remain_ordered(tmp_path) -> None:
    await prepare_active_identity(tmp_path)
    telegram = TelegramStub([[update(12), update(11)], asyncio.Event()])
    events = EventsStub(["duplicate", "accepted"])
    application = polling_app(tmp_path, telegram, events)

    await application.start()
    await wait_for(lambda: len(events.events) == 2)
    assert [event["event_id"] for event in events.events] == ["telegram:11", "telegram:12"]
    assert await application.storage.get_telegram_offset() == 13
    await application.stop()


@pytest.mark.asyncio
async def test_timeout_or_5xx_style_failure_retains_offset_then_retries(tmp_path) -> None:
    await prepare_active_identity(tmp_path)
    telegram = TelegramStub([[update(20)], [update(20)], asyncio.Event()])
    events = EventsStub([TransientPlatformEventError("timeout"), "accepted"])
    application = polling_app(tmp_path, telegram, events)

    await application.start()
    await wait_for(lambda: len(events.events) == 2)
    assert telegram.offsets[:2] == [None, None]
    assert await application.storage.get_telegram_offset() == 21
    await application.stop()


@pytest.mark.asyncio
async def test_permanent_rejection_retains_offset(tmp_path) -> None:
    await prepare_active_identity(tmp_path)
    telegram = TelegramStub([[update(30)], asyncio.Event()])
    events = EventsStub([PermanentPlatformEventError("invalid")])
    application = polling_app(tmp_path, telegram, events, delay=1)

    await application.start()
    await wait_for(lambda: len(events.events) == 1)
    assert await application.storage.get_telegram_offset() is None
    await application.stop()


@pytest.mark.asyncio
async def test_restart_continues_from_durable_offset(tmp_path) -> None:
    await prepare_active_identity(tmp_path)
    first_telegram = TelegramStub([[update(40)], asyncio.Event()])
    first_events = EventsStub(["accepted"])
    first = polling_app(tmp_path, first_telegram, first_events)
    await first.start()
    await wait_for(lambda: len(first_events.events) == 1)
    await first.stop()

    second_telegram = TelegramStub([[update(41)], asyncio.Event()])
    second_events = EventsStub(["accepted"])
    second = polling_app(tmp_path, second_telegram, second_events)
    await second.start()
    await wait_for(lambda: len(second_events.events) == 1)
    assert second_telegram.offsets[0] == 41
    await second.stop()


@pytest.mark.asyncio
async def test_telegram_conflict_is_operational_state_and_shutdown_cancels_long_poll(tmp_path, caplog) -> None:
    await prepare_active_identity(tmp_path)
    telegram = TelegramStub([TelegramPollingConflictError("active elsewhere")])
    events = EventsStub([])
    application = polling_app(tmp_path, telegram, events, delay=1)
    with caplog.at_level(logging.INFO):
        await application.start()
        await wait_for(lambda: len(telegram.offsets) == 1)
        await application.stop()

    assert "telegram_polling_conflict" in caplog.text
    assert "telegram-bot-token" not in caplog.text
    assert "adapter-token" not in caplog.text
    assert telegram.closed and events.closed


@pytest.mark.asyncio
async def test_http_clients_keep_bot_token_and_adapter_token_on_their_own_boundaries() -> None:
    telegram_requests: list[httpx.Request] = []
    backend_requests: list[httpx.Request] = []

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: telegram_requests.append(request) or httpx.Response(200, json={"ok": True, "result": []}))
    ) as telegram_transport:
        await TelegramBotClient("bot-secret", telegram_transport).get_updates(None)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: backend_requests.append(request) or httpx.Response(202, json={}))
    ) as backend_transport:
        event = {"schema_version": 1, "event_id": "telegram:1", "platform": "telegram", "event_type": "update", "occurred_at": "2026-01-01T00:00:00Z", "payload": update(1)}
        assert await SprotectPlatformEventsClient("https://backend.example", "adapter-secret", backend_transport).deliver(event) == "accepted"

    assert "bot-secret" in str(telegram_requests[0].url)
    assert telegram_requests[0].headers.get("Authorization") is None
    assert "bot-secret" not in backend_requests[0].content.decode()
    assert backend_requests[0].headers["Authorization"] == "Bearer adapter-secret"
    assert json.loads(backend_requests[0].content)["payload"] == update(1)
