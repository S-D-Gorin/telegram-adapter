"""Partitioned, bounded-parallel delivery of the durable Telegram event outbox.

Each partition (a Telegram chat) is delivered strictly FIFO: only its oldest
pending event is ever in flight, and the next one starts only after Guardian
ACKs the head (202 accepted / 200 duplicate) or the head becomes rejected or
expired. Different partitions are delivered in parallel, bounded by
``concurrency`` simultaneous HTTP deliveries.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from functools import partial

from app.sprotect import (
    PermanentPlatformEventError,
    PlatformAuthenticationError,
    PlatformRateLimitError,
    SprotectPlatformEventsClient,
    TransientPlatformEventError,
)
from app.storage import PlatformEventHead, SQLiteStorage

# Transient failures in this many distinct partitions with no success in between
# mean Guardian itself is unavailable, not one problematic resource.
CIRCUIT_BREAKER_PARTITIONS = 3
# Extra heads read per dispatch so migration-blocked partitions can be reported.
MIGRATION_LOOKAHEAD = 16
RETENTION_DELETE_BATCH = 1000


@dataclass
class EventDeliveryMetrics:
    """Low-cardinality gauges and counters; identifiers belong only in logs."""

    pending_events: int = 0
    retrying_events: int = 0
    oldest_pending_age_seconds: float = 0.0
    active_partitions: int = 0
    active_deliveries: int = 0
    migration_blocked_partitions: int = 0
    delivered_per_second: float = 0.0
    delivered_total: int = 0
    duplicate_total: int = 0
    retries_total: int = 0
    rejected_total: int = 0
    expired_total: int = 0
    deleted_total: int = 0
    migration_waits_total: int = 0
    backpressure_active: bool = False
    backpressure_activations_total: int = 0

    def snapshot(self) -> dict[str, float | int | bool]:
        return asdict(self)


class EventDeliveryDispatcher:
    def __init__(
        self,
        storage: SQLiteStorage,
        client: SprotectPlatformEventsClient,
        metrics: EventDeliveryMetrics,
        *,
        concurrency: int,
        max_attempts: int,
        pending_ttl_seconds: float,
        retention_seconds: float,
        initial_retry_delay: float,
        max_retry_delay: float,
        dispatch_interval: float = 0.5,
        maintenance_interval: float = 60.0,
        stats_interval: float = 60.0,
        clock: Callable[[], float] = time.time,
        on_ready_change: Callable[[bool], None] | None = None,
    ) -> None:
        self._storage = storage
        self._client = client
        self.metrics = metrics
        self._concurrency = concurrency
        self._max_attempts = max_attempts
        self._pending_ttl = pending_ttl_seconds
        self._retention = retention_seconds
        self._initial_retry_delay = initial_retry_delay
        self._max_retry_delay = max_retry_delay
        self._dispatch_interval = dispatch_interval
        self._maintenance_interval = maintenance_interval
        self._stats_interval = stats_interval
        self._clock = clock
        self._on_ready_change = on_ready_change
        # partition_key -> (event_id, task); at most one delivery per partition.
        self._in_flight: dict[str, tuple[str, asyncio.Task[None]]] = {}
        self._wakeup = asyncio.Event()
        self._paused_until = 0.0
        self._pause_reason: str | None = None
        # After a global pause only one probe delivery runs until something succeeds.
        self._half_open = False
        self._failing_partitions: set[str] = set()
        self._circuit_delay = initial_retry_delay
        self._migration_waits: dict[str, str] = {}
        self._delivered_since_stats = 0
        self._last_stats_at = clock()
        self._idle_reported = False
        self._logger = logging.getLogger(__name__)

    def wake(self) -> None:
        self._wakeup.set()

    async def run(self) -> None:
        next_maintenance = self._clock()
        next_stats = self._clock() + self._stats_interval
        try:
            while True:
                self._wakeup.clear()
                now = self._clock()
                try:
                    if now >= next_maintenance:
                        await self._maintain(now)
                        next_maintenance = now + self._maintenance_interval
                    if now >= next_stats:
                        await self._report_stats(now)
                        next_stats = now + self._stats_interval
                    await self._dispatch(now)
                except asyncio.CancelledError:
                    raise
                except Exception:  # A local failure must not stop delivery for good.
                    self._logger.exception("platform_event_dispatch_failed")
                try:
                    await asyncio.wait_for(self._wakeup.wait(), self._dispatch_interval)
                except TimeoutError:
                    pass
        finally:
            await self._cancel_in_flight()

    async def _dispatch(self, now: float) -> None:
        if now < self._paused_until:
            return
        capacity = (1 if self._half_open else self._concurrency) - len(self._in_flight)
        if capacity <= 0:
            return
        heads = await self._storage.list_ready_platform_event_heads(
            now=now,
            exclude_partitions=list(self._in_flight),
            limit=capacity,
            blocked_lookahead=MIGRATION_LOOKAHEAD,
        )
        for head in heads:
            if head.blocked_by_event_id is not None:
                self._note_migration_wait(head)
                continue
            if self._migration_waits.pop(head.partition_key, None) is not None:
                self._logger.info(
                    "platform_event_migration_released partition_key=%s telegram_update_id=%s",
                    head.partition_key,
                    head.telegram_update_id,
                )
            self._start(head)

    def _note_migration_wait(self, head: PlatformEventHead) -> None:
        assert head.blocked_by_event_id is not None
        if self._migration_waits.get(head.partition_key) == head.blocked_by_event_id:
            return
        self._migration_waits[head.partition_key] = head.blocked_by_event_id
        self.metrics.migration_waits_total += 1
        self._logger.info(
            "platform_event_migration_wait partition_key=%s telegram_update_id=%s event_id=%s "
            "waiting_for_event_id=%s",
            head.partition_key,
            head.telegram_update_id,
            head.event_id,
            head.blocked_by_event_id,
        )

    def _start(self, head: PlatformEventHead) -> None:
        task = asyncio.create_task(self._deliver(head), name="platform-event-delivery")
        self._in_flight[head.partition_key] = (head.event_id, task)
        self.metrics.active_deliveries = len(self._in_flight)
        task.add_done_callback(partial(self._delivery_finished, head.partition_key))

    def _delivery_finished(self, partition_key: str, task: asyncio.Task[None]) -> None:
        self._in_flight.pop(partition_key, None)
        self.metrics.active_deliveries = len(self._in_flight)
        if not task.cancelled() and task.exception() is not None:
            self._logger.error(
                "platform_event_delivery_failed partition_key=%s", partition_key, exc_info=task.exception()
            )
        self._wakeup.set()

    async def _deliver(self, head: PlatformEventHead) -> None:
        assert head.envelope is not None
        try:
            outcome = await self._client.deliver(head.envelope)
        except PlatformAuthenticationError as error:
            await self._on_authentication_failure(head, error)
        except PlatformRateLimitError as error:
            await self._on_rate_limited(head, error)
        except TransientPlatformEventError as error:
            await self._on_transient_failure(head, error)
        except PermanentPlatformEventError as error:
            await self._on_permanent_failure(head, error)
        except Exception as error:  # An unknown client failure keeps the event retryable.
            self._logger.exception("platform_event_unexpected_delivery_error event_id=%s", head.event_id)
            await self._on_transient_failure(head, error)
        else:
            await self._on_delivered(head, outcome)

    async def _on_delivered(self, head: PlatformEventHead, outcome: str) -> None:
        now = self._clock()
        await self._storage.mark_platform_event_delivered(
            head.event_id, now=now, http_status=202 if outcome == "accepted" else 200
        )
        self.metrics.delivered_total += 1
        if outcome != "accepted":
            self.metrics.duplicate_total += 1
        self._delivered_since_stats += 1
        self._failing_partitions.clear()
        self._circuit_delay = self._initial_retry_delay
        self._half_open = False
        if self._pause_reason is not None:
            self._logger.info("platform_events_delivery_resumed after=%s", self._pause_reason)
            self._pause_reason = None
        self._set_ready(True)
        self._logger.info(
            "platform_event_%s telegram_update_id=%s event_id=%s partition_key=%s attempt=%s",
            "delivered" if outcome == "accepted" else "duplicate",
            head.telegram_update_id,
            head.event_id,
            head.partition_key,
            head.attempt_count + 1,
        )

    async def _on_transient_failure(self, head: PlatformEventHead, error: Exception) -> None:
        now = self._clock()
        delay = self._backoff(head.attempt_count + 1)
        await self._record_retry(head, error, now=now, delay=delay, permanent=False)
        self._failing_partitions.add(head.partition_key)
        if len(self._failing_partitions) >= CIRCUIT_BREAKER_PARTITIONS and now >= self._paused_until:
            self._logger.error(
                "platform_events_unavailable failing_partitions=%s pause_seconds=%.1f",
                len(self._failing_partitions),
                self._circuit_delay,
            )
            self._pause(now, self._circuit_delay, "platform_events_unavailable")
            self._circuit_delay = min(self._circuit_delay * 2, self._max_retry_delay)

    async def _on_rate_limited(self, head: PlatformEventHead, error: PlatformRateLimitError) -> None:
        now = self._clock()
        delay = error.retry_after if error.retry_after is not None else self._backoff(head.attempt_count + 1)
        await self._record_retry(head, error, now=now, delay=delay, permanent=False)
        # Guardian limits the adapter as a whole, so every partition backs off together.
        self._pause(now, delay, "rate_limited")

    async def _on_authentication_failure(self, head: PlatformEventHead, error: PlatformAuthenticationError) -> None:
        now = self._clock()
        # The token, not this event, is at fault: never count it toward rejection.
        await self._storage.record_platform_event_failure(
            head.event_id,
            now=now,
            error=_describe(error),
            http_status=error.status_code,
            next_attempt_at=now,
            permanent=False,
            max_permanent_failures=self._max_attempts,
        )
        if self._pause_reason != "authentication" or now >= self._paused_until:
            self._logger.error(
                "platform events authentication degraded; pausing all partitions for %.0fs: %s",
                self._max_retry_delay,
                error,
            )
        self._pause(now, self._max_retry_delay, "authentication")

    async def _on_permanent_failure(self, head: PlatformEventHead, error: PermanentPlatformEventError) -> None:
        now = self._clock()
        delay = self._backoff(head.permanent_failure_count + 1)
        status = await self._record_retry(head, error, now=now, delay=delay, permanent=True)
        if status == "rejected":
            self.metrics.rejected_total += 1
            self._logger.error(
                "platform_event_rejected telegram_update_id=%s event_id=%s partition_key=%s "
                "attempts=%s http_status=%s: %s",
                head.telegram_update_id,
                head.event_id,
                head.partition_key,
                head.attempt_count + 1,
                error.status_code,
                error,
            )

    async def _record_retry(
        self, head: PlatformEventHead, error: Exception, *, now: float, delay: float, permanent: bool
    ) -> str | None:
        status_code = getattr(error, "status_code", None)
        status = await self._storage.record_platform_event_failure(
            head.event_id,
            now=now,
            error=_describe(error),
            http_status=status_code,
            next_attempt_at=now + delay,
            permanent=permanent,
            max_permanent_failures=self._max_attempts,
        )
        if status == "pending":
            self.metrics.retries_total += 1
            self._logger.warning(
                "platform_event_retry telegram_update_id=%s event_id=%s partition_key=%s attempt=%s "
                "http_status=%s retry_in=%.1fs: %s",
                head.telegram_update_id,
                head.event_id,
                head.partition_key,
                head.attempt_count + 1,
                status_code,
                delay,
                error,
            )
        return status

    def _pause(self, now: float, seconds: float, reason: str) -> None:
        self._paused_until = max(self._paused_until, now + seconds)
        self._pause_reason = reason
        self._half_open = True
        self._set_ready(False)

    def _backoff(self, attempt: int) -> float:
        return min(self._initial_retry_delay * 2 ** max(attempt - 1, 0), self._max_retry_delay)

    def _set_ready(self, value: bool) -> None:
        if self._on_ready_change is not None:
            self._on_ready_change(value)

    async def _maintain(self, now: float) -> None:
        # In-flight heads finish on their own; they are excluded so a late ACK still lands.
        in_flight_event_ids = [event_id for event_id, _ in self._in_flight.values()]
        expired, sample = await self._storage.expire_platform_events(
            received_before=now - self._pending_ttl, now=now, exclude_event_ids=in_flight_event_ids
        )
        if expired:
            self.metrics.expired_total += expired
            self._logger.warning(
                "platform_events_expired count=%s ttl_seconds=%.0f sample=%s",
                expired,
                self._pending_ttl,
                ",".join(f"{event_id}@{partition_key}" for event_id, partition_key in sample),
            )
        deleted = 0
        while True:
            batch = await self._storage.delete_finished_platform_events(
                finished_before=now - self._retention, limit=RETENTION_DELETE_BATCH
            )
            deleted += batch
            if batch < RETENTION_DELETE_BATCH:
                break
        if deleted:
            self.metrics.deleted_total += deleted
            self._logger.info("platform_events_retention_deleted count=%s", deleted)

    async def _report_stats(self, now: float) -> None:
        stats = await self._storage.platform_event_stats(now=now)
        elapsed = max(now - self._last_stats_at, 1e-9)
        self.metrics.pending_events = int(stats["pending_events"])
        self.metrics.retrying_events = int(stats["retrying_events"])
        self.metrics.active_partitions = int(stats["active_partitions"])
        self.metrics.migration_blocked_partitions = int(stats["migration_blocked_partitions"])
        self.metrics.oldest_pending_age_seconds = float(stats["oldest_pending_age_seconds"])
        self.metrics.delivered_per_second = self._delivered_since_stats / elapsed
        idle = not self.metrics.pending_events and not self._delivered_since_stats
        self._delivered_since_stats = 0
        self._last_stats_at = now
        if idle and self._idle_reported:
            return
        self._idle_reported = idle
        metrics = self.metrics
        self._logger.info(
            "platform_events_stats pending=%s retrying=%s oldest_pending_age_seconds=%.0f "
            "active_partitions=%s active_deliveries=%s delivered_per_second=%.2f delivered_total=%s "
            "duplicate_total=%s retries_total=%s rejected_total=%s expired_total=%s "
            "migration_waits_total=%s migration_blocked_partitions=%s backpressure_active=%s "
            "backpressure_activations_total=%s",
            metrics.pending_events,
            metrics.retrying_events,
            metrics.oldest_pending_age_seconds,
            metrics.active_partitions,
            metrics.active_deliveries,
            metrics.delivered_per_second,
            metrics.delivered_total,
            metrics.duplicate_total,
            metrics.retries_total,
            metrics.rejected_total,
            metrics.expired_total,
            metrics.migration_waits_total,
            metrics.migration_blocked_partitions,
            metrics.backpressure_active,
            metrics.backpressure_activations_total,
        )

    async def _cancel_in_flight(self) -> None:
        # A cancelled HTTP delivery leaves its event pending; Guardian dedupes the resend.
        tasks = [task for _, task in self._in_flight.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def _describe(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:500]
