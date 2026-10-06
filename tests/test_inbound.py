import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from app.application import Application
from app.config import Config
from app.sprotect import (
    PairingState,
    PermanentPlatformEventError,
    PlatformAuthenticationError,
    PlatformRateLimitError,
    SprotectPlatformEventsClient,
    TransientPlatformEventError,
)
from app.sprotect.events import parse_retry_after
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
    """Returns scripted getUpdates results, then idles like an empty long poll.

    A script item may be a batch, an exception, an ``asyncio.Event`` that gates the
    next item, or a callable ``(offset, limit) -> batch | None`` used for every call.
    """

    def __init__(self, responses=()) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[int | None, int]] = []
        self.closed = False

    @property
    def offsets(self) -> list[int | None]:
        return [offset for offset, _ in self.calls]

    async def get_updates(self, offset, *, timeout=30, limit=100):
        self.calls.append((offset, limit))
        while self.responses and isinstance(self.responses[0], asyncio.Event):
            await self.responses.pop(0).wait()
        if self.responses and callable(self.responses[0]):
            batch = self.responses[0](offset, limit)
            if batch is not None:
                return batch
            self.responses.pop(0)
        if not self.responses:
            await asyncio.Event().wait()
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


class EventsStub:
    """Platform Events client double that records concurrency and per-chat order."""

    def __init__(self, handler=None) -> None:
        self.handler = handler
        self.events: list[dict] = []
        self.started_at: list[float] = []
        self.completed: list[str] = []
        self.active = 0
        self.max_active = 0
        self.active_by_chat: dict[str, int] = defaultdict(int)
        self.max_active_per_chat = 0
        self.closed = False

    async def deliver(self, event):
        chat = chat_of(event)
        self.events.append(event)
        self.started_at.append(time.time())
        self.active += 1
        self.active_by_chat[chat] += 1
        self.max_active = max(self.max_active, self.active)
        self.max_active_per_chat = max(self.max_active_per_chat, self.active_by_chat[chat])
        try:
            outcome = await self.handler(event) if self.handler is not None else "accepted"
        finally:
            self.active -= 1
            self.active_by_chat[chat] -= 1
        self.completed.append(event["event_id"])
        return outcome

    def attempts(self, event_id: str) -> int:
        return sum(1 for event in self.events if event["event_id"] == event_id)

    async def close(self) -> None:
        self.closed = True


class BlockingCommandsStub:
    async def connect(self):
        await asyncio.Event().wait()


class FakeClock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
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


def update(number: int, chat_id: int = -100, **message_fields) -> dict:
    message = {"message_id": number, "chat": {"id": chat_id, "type": "supergroup"}, "text": "private text"}
    message.update(message_fields)
    return {"update_id": number, "message": message}


def chat_of(event: dict) -> str:
    message = event["payload"].get("message")
    return str(message["chat"]["id"]) if isinstance(message, dict) else "unrouted"


def event_id(number: int) -> str:
    return f"telegram:{number}"


def polling_app(
    tmp_path, telegram, events, *, delay=0.01, max_delay=0.05, clock=None, stats_interval=60.0, **event_config
) -> Application:
    return Application(
        Config("https://backend.example", "telegram-bot-token", tmp_path, "INFO", **event_config),
        bootstrap_client=BootstrapUnused(),  # type: ignore[arg-type]
        telegram_client=telegram,  # type: ignore[arg-type]
        platform_events_client=events,  # type: ignore[arg-type]
        platform_commands_client=BlockingCommandsStub(),  # type: ignore[arg-type]
        initial_retry_delay=delay,
        max_retry_delay=max_delay,
        clock=clock or time.time,
        event_dispatch_interval=0.01,
        event_maintenance_interval=0.0 if clock is not None else 60.0,
        event_stats_interval=stats_interval,
    )


async def started(tmp_path, telegram, events, **kwargs) -> Application:
    await prepare_active_identity(tmp_path)
    application = polling_app(tmp_path, telegram, events, **kwargs)
    await application.start()
    return application


# --- Guardian contract ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_polling_does_not_start_before_pairing(tmp_path) -> None:
    telegram = TelegramStub()
    events = EventsStub()
    bootstrap = UnpairedBootstrap()
    application = Application(
        Config("https://backend.example", "telegram-bot-token", tmp_path, "INFO"),
        bootstrap_client=bootstrap,  # type: ignore[arg-type]
        telegram_client=telegram,  # type: ignore[arg-type]
        platform_events_client=events,  # type: ignore[arg-type]
        platform_commands_client=BlockingCommandsStub(),  # type: ignore[arg-type]
        initial_retry_delay=1,
    )

    await application.start()
    await wait_for(lambda: bootstrap.status_calls == 1)
    assert telegram.calls == []
    await application.stop()


@pytest.mark.asyncio
async def test_update_is_sent_as_stage_3a_envelope_and_offset_means_stored(tmp_path) -> None:
    telegram = TelegramStub([[update(10)]])
    events = EventsStub()
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: events.completed == [event_id(10)])
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
async def test_accepted_and_duplicate_are_both_successful_acks(tmp_path) -> None:
    async def handler(event):
        return "duplicate" if event["event_id"] == event_id(1) else "accepted"

    telegram = TelegramStub([[update(1, chat_id=-1), update(2, chat_id=-2)]])
    events = EventsStub(handler)
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: len(events.completed) == 2)
    await wait_for(lambda: application.event_metrics["delivered_total"] == 2)
    duplicate = await application.storage.get_platform_event(event_id(1))
    accepted = await application.storage.get_platform_event(event_id(2))
    assert duplicate.delivery_status == accepted.delivery_status == "delivered"
    assert (duplicate.last_http_status, accepted.last_http_status) == (200, 202)
    assert application.event_metrics["duplicate_total"] == 1
    assert application.readiness["platform_events_ready"] is True
    await application.stop()


# --- Ingest: atomic batch + offset ---------------------------------------------------------


def new_event(number: int, chat_id: int = -100, *, envelope=None, received_at: float = 1.0):
    from app.storage import NewPlatformEvent

    return NewPlatformEvent(
        event_id=event_id(number),
        telegram_update_id=number,
        partition_key=str(chat_id),
        migrate_to_partition=None,
        envelope=envelope if envelope is not None else {"event_id": event_id(number), "payload": update(number)},
        received_at=received_at,
    )


@pytest.mark.asyncio
async def test_batch_and_offset_are_committed_atomically_and_idempotently(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()

    assert await storage.store_telegram_updates([new_event(1), new_event(2), new_event(3)], 4) == 3
    assert await storage.get_telegram_offset() == 4
    # A refetched update keeps its original envelope; only the new one is inserted.
    changed = new_event(3, envelope={"event_id": event_id(3), "payload": {"changed": True}})
    assert await storage.store_telegram_updates([changed, new_event(4)], 5) == 1
    stored = await storage.get_platform_event(event_id(3))
    assert stored.envelope["payload"] == update(3)
    assert [event.telegram_update_id for event in await storage.list_pending_platform_events()] == [1, 2, 3, 4]
    assert await storage.get_telegram_offset() == 5
    await storage.close()


@pytest.mark.asyncio
async def test_failed_batch_commit_rolls_back_rows_and_does_not_advance_offset(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    unserializable = new_event(2, envelope={"payload": object()})

    with pytest.raises(TypeError):
        await storage.store_telegram_updates([new_event(1), unserializable, new_event(3)], 4)

    assert await storage.list_platform_events() == []
    assert await storage.get_telegram_offset() is None
    await storage.close()


@pytest.mark.asyncio
async def test_crash_between_get_updates_and_commit_refetches_the_batch(tmp_path) -> None:
    telegram = TelegramStub([[update(1), update(2)], [update(1), update(2)]])
    events = EventsStub()
    await prepare_active_identity(tmp_path)
    application = polling_app(tmp_path, telegram, events)
    original = application.storage.store_telegram_updates
    failures = []

    async def failing_once(batch, next_update_id):
        if not failures:
            failures.append(next_update_id)
            raise OSError("disk unavailable")
        return await original(batch, next_update_id)

    application.storage.store_telegram_updates = failing_once  # type: ignore[method-assign]
    await application.start()

    await wait_for(lambda: events.completed == [event_id(1), event_id(2)])
    assert telegram.offsets[:2] == [None, None]
    assert await application.storage.get_telegram_offset() == 3
    await application.stop()


@pytest.mark.asyncio
async def test_restart_after_batch_is_stored_delivers_from_outbox_without_refetch(tmp_path) -> None:
    never = asyncio.Event()

    async def unavailable(event):
        await never.wait()

    first_telegram = TelegramStub([[update(1, -1), update(2, -2), update(3, -1)]])
    first_events = EventsStub(unavailable)
    first = await started(tmp_path, first_telegram, first_events)
    await wait_for(lambda: len(first_events.events) == 2)
    assert await first.storage.get_telegram_offset() == 4
    stored = await first.storage.get_platform_event(event_id(1))
    await first.stop()

    second_telegram = TelegramStub()
    second_events = EventsStub()
    second = polling_app(tmp_path, second_telegram, second_events)
    await second.start()
    await wait_for(lambda: len(second_events.completed) == 3)
    await wait_for(lambda: len(second_telegram.calls) == 1)
    assert second_telegram.offsets[0] == 4
    resent = next(event for event in second_events.events if event["event_id"] == event_id(1))
    assert resent == stored.envelope
    await second.stop()


# --- Partitioning, FIFO and parallelism ----------------------------------------------------


@pytest.mark.asyncio
async def test_events_of_one_chat_are_delivered_strictly_fifo(tmp_path) -> None:
    async def uneven(event):
        number = int(event["event_id"].split(":")[1])
        await asyncio.sleep((6 - number) * 0.005)
        return "accepted"

    telegram = TelegramStub([[update(number) for number in (4, 1, 6, 3, 5, 2)]])
    events = EventsStub(uneven)
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: len(events.completed) == 6)
    assert events.completed == [event_id(number) for number in range(1, 7)]
    assert events.max_active_per_chat == 1
    await application.stop()


@pytest.mark.asyncio
async def test_different_chats_are_delivered_in_parallel(tmp_path) -> None:
    both_started = asyncio.Event()

    async def rendezvous(event):
        if events.active == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), 2)
        return "accepted"

    telegram = TelegramStub([[update(1, -1), update(2, -2)]])
    events = EventsStub(rendezvous)
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: len(events.completed) == 2)
    assert events.max_active == 2
    await application.stop()


@pytest.mark.asyncio
async def test_event_delivery_concurrency_bounds_simultaneous_http_deliveries(tmp_path) -> None:
    async def slow(event):
        await asyncio.sleep(0.02)
        return "accepted"

    batch = [update(number, chat_id=-(number % 12) - 1) for number in range(1, 25)]
    telegram = TelegramStub([batch])
    events = EventsStub(slow)
    application = await started(tmp_path, telegram, events, event_delivery_concurrency=3)

    await wait_for(lambda: len(events.completed) == 24)
    assert events.max_active == 3
    assert events.max_active_per_chat == 1
    await application.stop()


@pytest.mark.asyncio
async def test_retrying_chat_does_not_block_other_chats_and_keeps_its_own_fifo(tmp_path) -> None:
    async def chat_a_down(event):
        if chat_of(event) == "-1":
            raise TransientPlatformEventError("Platform Events API returned 503", status_code=503)
        return "accepted"

    telegram = TelegramStub([[update(1, -1), update(2, -1), update(3, -2), update(4, -2), update(5, -2)]])
    events = EventsStub(chat_a_down)
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: events.completed == [event_id(3), event_id(4), event_id(5)])
    await wait_for(lambda: events.attempts(event_id(1)) >= 3)
    assert events.attempts(event_id(2)) == 0
    head = await application.storage.get_platform_event(event_id(1))
    assert head.delivery_status == "pending"
    assert head.last_http_status == 503
    assert head.attempt_count >= 2
    await application.stop()


@pytest.mark.asyncio
async def test_unexpected_update_without_chat_is_forwarded_and_polling_continues(tmp_path, caplog) -> None:
    poll_update = {"update_id": 1, "poll": {"id": "poll-1", "question": "?"}}
    malformed = {"message": {"chat": {"id": -1}}}
    telegram = TelegramStub([[poll_update, update(2, -1)], [malformed, update(3, -1)], [update(4, -2)]])
    events = EventsStub()
    with caplog.at_level(logging.INFO):
        application = await started(tmp_path, telegram, events)
        await wait_for(lambda: len(events.completed) == 4)

    unrouted = await application.storage.get_platform_event(event_id(1))
    assert unrouted.partition_key == "unrouted"
    assert await application.storage.get_telegram_offset() == 5
    assert "telegram_update_unrouted" in caplog.text
    assert "malformed update" in caplog.text
    await application.stop()


# --- Retry policy --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_network_and_5xx_failures_are_retried_with_the_unchanged_envelope(tmp_path) -> None:
    failures = [
        TransientPlatformEventError("unable to reach Sprotect Platform Events API"),
        TransientPlatformEventError("Platform Events API returned 502", status_code=502),
    ]

    async def flaky(event):
        if failures:
            raise failures.pop(0)
        return "accepted"

    telegram = TelegramStub([[update(20)]])
    events = EventsStub(flaky)
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: events.completed == [event_id(20)])
    assert len(events.events) == 3
    assert events.events[0] == events.events[1] == events.events[2]
    delivered = await application.storage.get_platform_event(event_id(20))
    assert delivered.attempt_count == 3
    assert application.event_metrics["retries_total"] == 2
    await application.stop()


@pytest.mark.asyncio
async def test_rate_limit_honours_retry_after_and_pauses_every_partition(tmp_path) -> None:
    limited = []

    async def rate_limited_once(event):
        if not limited:
            limited.append(time.time())
            raise PlatformRateLimitError("rate limited", status_code=429, retry_after=0.3)
        return "accepted"

    telegram = TelegramStub([[update(1, -1), update(2, -2)]])
    events = EventsStub(rate_limited_once)
    application = await started(tmp_path, telegram, events, event_delivery_concurrency=1)

    await wait_for(lambda: len(events.completed) == 2)
    assert events.completed == [event_id(1), event_id(2)]
    assert min(events.started_at[1:]) >= limited[0] + 0.28
    retried = await application.storage.get_platform_event(event_id(1))
    assert retried.last_http_status == 202
    await application.stop()


@pytest.mark.asyncio
async def test_rate_limit_without_retry_after_falls_back_to_backoff(tmp_path) -> None:
    async def always_limited(event):
        raise PlatformRateLimitError("rate limited", status_code=429, retry_after=None)

    telegram = TelegramStub([[update(1)]])
    events = EventsStub(always_limited)
    application = await started(tmp_path, telegram, events, delay=0.2, max_delay=0.4)

    await wait_for(lambda: len(events.events) == 1)
    await asyncio.sleep(0.05)
    event = await application.storage.get_platform_event(event_id(1))
    assert event.last_http_status == 429
    assert event.next_attempt_at - event.last_attempt_at == pytest.approx(0.2)
    await application.stop()


@pytest.mark.asyncio
async def test_authentication_failure_pauses_all_partitions_without_retry_storm(tmp_path) -> None:
    state = {"revoked": True}

    async def auth(event):
        await asyncio.sleep(0.01)
        if state["revoked"]:
            raise PlatformAuthenticationError("adapter token was rejected or revoked", status_code=401)
        return "accepted"

    telegram = TelegramStub([[update(number, chat_id=-number) for number in range(1, 11)]])
    events = EventsStub(auth)
    application = await started(tmp_path, telegram, events, max_delay=0.4)

    await wait_for(lambda: len(events.events) == 8)
    await asyncio.sleep(0.25)
    # Only the initial concurrent wave reached Guardian; nobody retried during the pause.
    assert len(events.events) == 8
    assert application.readiness["platform_events_ready"] is False

    state["revoked"] = False
    await wait_for(lambda: len(events.completed) == 10)
    for number in range(1, 11):
        event = await application.storage.get_platform_event(event_id(number))
        assert event.delivery_status == "delivered"
        assert event.permanent_failure_count == 0
    assert application.readiness["platform_events_ready"] is True
    await application.stop()


@pytest.mark.asyncio
async def test_guardian_outage_opens_circuit_instead_of_hammering_every_partition(tmp_path) -> None:
    async def down(event):
        raise TransientPlatformEventError("Platform Events API returned 503", status_code=503)

    telegram = TelegramStub([[update(number, chat_id=-number) for number in range(1, 41)]])
    events = EventsStub(down)
    application = await started(tmp_path, telegram, events, event_delivery_concurrency=2, delay=0.2, max_delay=1.0)

    await wait_for(lambda: len(events.events) >= 3)
    await asyncio.sleep(0.2)
    assert len(events.events) <= 6
    assert application.readiness["platform_events_ready"] is False
    await application.stop()


@pytest.mark.asyncio
async def test_permanent_4xx_is_retried_then_rejected_and_unblocks_the_partition(tmp_path) -> None:
    async def invalid_first(event):
        if event["event_id"] == event_id(1):
            raise PermanentPlatformEventError("Platform Events API returned 422", status_code=422)
        return "accepted"

    telegram = TelegramStub([[update(1, -1), update(2, -1)]])
    events = EventsStub(invalid_first)
    application = await started(tmp_path, telegram, events, event_max_attempts=3)

    await wait_for(lambda: events.completed == [event_id(2)])
    assert events.attempts(event_id(1)) == 3
    rejected = await application.storage.get_platform_event(event_id(1))
    assert rejected.delivery_status == "rejected"
    assert rejected.attempt_count == rejected.permanent_failure_count == 3
    assert rejected.last_http_status == 422
    assert "422" in rejected.last_error
    assert rejected.last_attempt_at is not None and rejected.finished_at is not None
    assert application.event_metrics["rejected_total"] == 1
    await application.stop()


# --- TTL, retention, backpressure ----------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_ttl_expires_stuck_event_without_breaking_fifo(tmp_path) -> None:
    clock = FakeClock()
    second_batch = asyncio.Event()

    async def first_event_stuck(event):
        if event["event_id"] == event_id(1):
            raise TransientPlatformEventError("Platform Events API returned 503", status_code=503)
        return "accepted"

    telegram = TelegramStub([[update(1, -1)], second_batch, [update(2, -1), update(3, -2)]])
    events = EventsStub(first_event_stuck)
    application = await started(tmp_path, telegram, events, clock=clock, event_pending_ttl_seconds=100)

    await wait_for(lambda: events.attempts(event_id(1)) == 1)
    clock.advance(50)
    second_batch.set()
    await wait_for(lambda: events.completed == [event_id(3)])
    # The later event of the stuck chat must not overtake its head.
    assert events.attempts(event_id(2)) == 0

    clock.advance(100)
    await wait_for(lambda: event_id(2) in events.completed)
    expired = await application.storage.get_platform_event(event_id(1))
    assert expired.delivery_status == "expired"
    assert expired.last_http_status == 503
    assert application.event_metrics["expired_total"] == 1
    assert (await application.storage.get_platform_event(event_id(2))).delivery_status == "delivered"
    await application.stop()


@pytest.mark.asyncio
async def test_retention_deletes_only_finished_events(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    await storage.store_telegram_updates([new_event(1), new_event(2, -2), new_event(3, -3)], 4)
    await storage.mark_platform_event_delivered(event_id(1), now=100.0, http_status=202)
    assert await storage.record_platform_event_failure(
        event_id(2), now=100.0, error="422", http_status=422, next_attempt_at=101.0,
        permanent=True, max_permanent_failures=1,
    ) == "rejected"

    assert await storage.delete_finished_platform_events(finished_before=100.0, limit=10) == 0
    assert await storage.delete_finished_platform_events(finished_before=200.0, limit=10) == 2
    assert [event.event_id for event in await storage.list_platform_events()] == [event_id(3)]
    await storage.close()


@pytest.mark.asyncio
async def test_max_pending_backpressure_pauses_polling_until_backlog_drains(tmp_path) -> None:
    release = asyncio.Event()
    produced = []

    def endless(offset, limit):
        if len(produced) >= 10:
            return None
        start = offset or 1
        batch = [update(number, chat_id=-number) for number in range(start, min(start + limit, 11))]
        produced.extend(batch)
        return batch

    async def blocked(event):
        await release.wait()
        return "accepted"

    telegram = TelegramStub([endless])
    events = EventsStub(blocked)
    application = await started(tmp_path, telegram, events, event_max_pending=3, event_delivery_concurrency=1)

    await wait_for(lambda: application.event_metrics["backpressure_active"] is True)
    await asyncio.sleep(0.1)
    assert telegram.calls == [(None, 3)]

    release.set()
    await wait_for(lambda: len(events.completed) == 10)
    assert all(limit <= 3 for _, limit in telegram.calls)
    assert application.event_metrics["backpressure_activations_total"] >= 1
    await wait_for(lambda: application.event_metrics["backpressure_active"] is False)
    await application.stop()


# --- Group -> supergroup migration ---------------------------------------------------------

OLD_GROUP, SUPERGROUP = -1, -1002


def migration_batch() -> list[dict]:
    return [
        update(1, OLD_GROUP),
        update(2, OLD_GROUP, migrate_to_chat_id=SUPERGROUP),
        update(3, SUPERGROUP, migrate_from_chat_id=OLD_GROUP),
        update(4, SUPERGROUP),
        update(5, -2),
        update(6, -3),
        update(7, -4),
    ]


@pytest.mark.asyncio
async def test_supergroup_events_wait_for_migration_while_other_chats_continue(tmp_path, caplog) -> None:
    old_group_ack = asyncio.Event()

    async def old_group_slow(event):
        if event["event_id"] == event_id(1):
            await old_group_ack.wait()
        return "accepted"

    telegram = TelegramStub([migration_batch()])
    events = EventsStub(old_group_slow)
    with caplog.at_level(logging.INFO):
        application = await started(tmp_path, telegram, events, stats_interval=0.0)
        await wait_for(lambda: set(events.completed) == {event_id(5), event_id(6), event_id(7)})
        await asyncio.sleep(0.05)
    assert events.attempts(event_id(3)) == events.attempts(event_id(4)) == 0
    assert application.event_metrics["migration_waits_total"] == 1
    assert "platform_event_migration_wait partition_key=-1002" in caplog.text

    old_group_ack.set()
    await wait_for(lambda: len(events.completed) == 7)
    order = events.completed
    assert order.index(event_id(1)) < order.index(event_id(2)) < order.index(event_id(3)) < order.index(event_id(4))
    await application.stop()


@pytest.mark.asyncio
async def test_migration_dependency_survives_restart(tmp_path) -> None:
    never = asyncio.Event()

    async def old_group_down(event):
        if event["event_id"] == event_id(1):
            await never.wait()
        return "accepted"

    first_events = EventsStub(old_group_down)
    first = await started(tmp_path, TelegramStub([migration_batch()]), first_events)
    await wait_for(lambda: set(first_events.completed) == {event_id(5), event_id(6), event_id(7)})
    await first.stop()
    assert first_events.attempts(event_id(3)) == 0

    second_events = EventsStub()
    second = polling_app(tmp_path, TelegramStub(), second_events)
    await second.start()
    await wait_for(lambda: {event_id(n) for n in range(1, 5)} <= set(second_events.completed))
    # Chats B/C/D may be redelivered (at-least-once); the migrated chain must stay ordered.
    chain = [completed for completed in second_events.completed if completed in {event_id(n) for n in range(1, 5)}]
    assert chain == [event_id(1), event_id(2), event_id(3), event_id(4)]
    await second.stop()


# --- Shutdown, observability, throughput ---------------------------------------------------


@pytest.mark.asyncio
async def test_graceful_shutdown_during_delivery_keeps_event_retryable(tmp_path) -> None:
    never = asyncio.Event()

    async def hanging(event):
        await never.wait()

    first_events = EventsStub(hanging)
    first = await started(tmp_path, TelegramStub([[update(30)]]), first_events)
    await wait_for(lambda: first_events.active == 1)
    await first.stop()

    second_events = EventsStub()
    second = polling_app(tmp_path, TelegramStub(), second_events)
    await second.start()
    pending = await second.storage.get_platform_event(event_id(30))
    assert pending.delivery_status == "pending"
    await wait_for(lambda: second_events.completed == [event_id(30)])
    assert second_events.events[0] == first_events.events[0]
    await second.stop()


@pytest.mark.asyncio
async def test_stats_are_logged_without_high_cardinality_metric_labels(tmp_path, caplog) -> None:
    telegram = TelegramStub([[update(1, -1), update(2, -2)]])
    events = EventsStub()
    with caplog.at_level(logging.INFO):
        application = await started(tmp_path, telegram, events, stats_interval=0.0)
        await wait_for(lambda: len(events.completed) == 2)
        await wait_for(lambda: "platform_events_stats pending=0" in caplog.text)

    metrics = application.event_metrics
    assert set(metrics) >= {
        "pending_events", "retrying_events", "oldest_pending_age_seconds", "active_partitions",
        "active_deliveries", "delivered_per_second", "retries_total", "rejected_total", "expired_total",
        "migration_waits_total", "backpressure_active",
    }
    assert all(isinstance(value, (int, float, bool)) for value in metrics.values())
    await application.stop()


@pytest.mark.asyncio
async def test_backlog_of_hundreds_of_updates_across_dozens_of_chats_is_drained_in_parallel(tmp_path) -> None:
    guardian_latency = 0.02

    async def guardian(event):
        await asyncio.sleep(guardian_latency)
        return "accepted"

    count, chats = 400, 40
    backlog = [update(number, chat_id=-(number % chats) - 1) for number in range(1, count + 1)]
    telegram = TelegramStub([backlog[index:index + 100] for index in range(0, count, 100)])
    events = EventsStub(guardian)
    begin = time.monotonic()
    application = await started(tmp_path, telegram, events)

    await wait_for(lambda: len(events.completed) == count, timeout=30)
    elapsed = time.monotonic() - begin
    sequential_lower_bound = count * guardian_latency
    assert elapsed < sequential_lower_bound / 3, (elapsed, sequential_lower_bound)
    assert events.max_active == 8
    by_chat: dict[str, list[int]] = defaultdict(list)
    for completed in events.completed:
        number = int(completed.split(":")[1])
        by_chat[str(-(number % chats) - 1)].append(number)
    assert all(numbers == sorted(numbers) for numbers in by_chat.values())
    await application.stop()


# --- Transport boundaries ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_telegram_conflict_is_operational_state_and_shutdown_cancels_long_poll(tmp_path, caplog) -> None:
    telegram = TelegramStub([TelegramPollingConflictError("active elsewhere")])
    events = EventsStub()
    with caplog.at_level(logging.INFO):
        application = await started(tmp_path, telegram, events, delay=1)
        await wait_for(lambda: len(telegram.calls) == 1)
        await application.stop()

    assert "telegram_polling_conflict" in caplog.text
    assert "telegram-bot-token" not in caplog.text
    assert "adapter-token" not in caplog.text
    assert telegram.closed and events.closed


def events_client(handler) -> tuple[SprotectPlatformEventsClient, httpx.AsyncClient]:
    transport = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return SprotectPlatformEventsClient("https://backend.example", "adapter-secret", transport), transport


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (500, TransientPlatformEventError),
        (503, TransientPlatformEventError),
        (408, TransientPlatformEventError),
        (429, PlatformRateLimitError),
        (401, PlatformAuthenticationError),
        (400, PermanentPlatformEventError),
        (422, PermanentPlatformEventError),
    ],
)
async def test_events_client_classifies_guardian_responses(status, error_type) -> None:
    client, transport = events_client(lambda request: httpx.Response(status, headers={"Retry-After": "7"}))
    with pytest.raises(error_type) as raised:
        await client.deliver({"event_id": "telegram:1"})
    assert raised.value.status_code == status
    if status == 429:
        assert raised.value.retry_after == 7.0
    if status != 401:
        assert not isinstance(raised.value, PlatformAuthenticationError)
    await transport.aclose()


@pytest.mark.asyncio
async def test_events_client_treats_network_errors_as_transient() -> None:
    def unreachable(request):
        raise httpx.ConnectError("connection refused", request=request)

    client, transport = events_client(unreachable)
    with pytest.raises(TransientPlatformEventError) as raised:
        await client.deliver({"event_id": "telegram:1"})
    assert raised.value.status_code is None
    await transport.aclose()


def test_retry_after_accepts_seconds_and_http_dates() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert parse_retry_after("12") == 12.0
    assert parse_retry_after(format_datetime(now + timedelta(seconds=30), usegmt=True), now=now) == 30.0
    assert parse_retry_after("not a date") is None
    assert parse_retry_after("-5") == 0.0
    assert parse_retry_after("999999") == 3600.0
    assert parse_retry_after(None) is None


@pytest.mark.asyncio
async def test_http_clients_keep_bot_token_and_adapter_token_on_their_own_boundaries() -> None:
    telegram_requests: list[httpx.Request] = []
    backend_requests: list[httpx.Request] = []

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: telegram_requests.append(request) or httpx.Response(200, json={"ok": True, "result": []}))
    ) as telegram_transport:
        await TelegramBotClient("bot-secret", telegram_transport).get_updates(None, limit=7)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: backend_requests.append(request) or httpx.Response(202, json={}))
    ) as backend_transport:
        event = {"schema_version": 1, "event_id": "telegram:1", "platform": "telegram", "event_type": "update", "occurred_at": "2026-01-01T00:00:00Z", "payload": update(1)}
        assert await SprotectPlatformEventsClient("https://backend.example", "adapter-secret", backend_transport).deliver(event) == "accepted"

    assert "bot-secret" in str(telegram_requests[0].url)
    assert telegram_requests[0].headers.get("Authorization") is None
    assert json.loads(telegram_requests[0].content) == {
        "timeout": 30,
        "limit": 7,
        "allowed_updates": [
            "message", "edited_message", "channel_post", "edited_channel_post", "chat_member", "my_chat_member",
        ],
    }
    assert "bot-secret" not in backend_requests[0].content.decode()
    assert backend_requests[0].headers["Authorization"] == "Bearer adapter-secret"
    assert backend_requests[0].url.path == "/api/v1/platform-adapters/events/"
    assert json.loads(backend_requests[0].content)["payload"] == update(1)


@pytest.mark.asyncio
async def test_every_get_updates_call_explicitly_sends_guardian_supported_allowed_updates() -> None:
    # Telegram remembers the last allowed_updates of a bot; sending it on every call keeps
    # the adapter independent of what a previous client (e.g. the legacy runtime) configured.
    requests: list[dict] = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(json.loads(request.content)) or httpx.Response(200, json={"ok": True, "result": []})
        )
    ) as transport:
        client = TelegramBotClient("bot-secret", transport)
        await client.get_updates(None)
        await client.get_updates(42, limit=10)

    expected = ["message", "edited_message", "channel_post", "edited_channel_post", "chat_member", "my_chat_member"]
    assert [request["allowed_updates"] for request in requests] == [expected, expected]
    assert requests[1]["offset"] == 42
    for excluded in ("callback_query", "chat_join_request", "inline_query", "poll", "message_reaction"):
        assert excluded not in requests[0]["allowed_updates"]
