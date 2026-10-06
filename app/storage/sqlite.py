"""Explicit SQLite persistence for adapter credentials and future state."""

from __future__ import annotations

import asyncio
import json
import secrets
import sqlite3
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from app.domain import READ_ONLY_OPERATION_TYPES
from app.telegram.partitioning import migration_target_partition, partition_key_for_update


@dataclass(frozen=True, repr=False)
class AdapterIdentity:
    adapter_id: str
    pairing_secret: str
    adapter_token: str | None
    pairing_status: str
    created_at: str
    paired_at: str | None

    def __repr__(self) -> str:
        return (
            "AdapterIdentity("
            f"adapter_id={self.adapter_id!r}, pairing_secret='***', "
            f"adapter_token={'***' if self.adapter_token else None}, "
            f"pairing_status={self.pairing_status!r}, created_at={self.created_at!r}, "
            f"paired_at={self.paired_at!r})"
        )


@dataclass(frozen=True, repr=False)
class PlatformOperation:
    operation_id: str
    schema_version: int
    platform: str
    operation_type: str
    payload: dict[str, object]
    status: str
    received_at: str
    attempt_count: int = 0

    def __repr__(self) -> str:
        return (
            "PlatformOperation("
            f"operation_id={self.operation_id!r}, schema_version={self.schema_version}, "
            f"platform={self.platform!r}, operation_type={self.operation_type!r}, "
            "payload='***', "
            f"status={self.status!r}, received_at={self.received_at!r})"
        )


@dataclass(frozen=True, repr=False)
class PendingResult:
    result_id: str
    operation_id: str
    schema_version: int
    platform: str
    status: str
    result: dict[str, object]
    error: dict[str, object] | None
    completed_at: str
    delivery_status: str

    def __repr__(self) -> str:
        return (
            "PendingResult("
            f"result_id={self.result_id!r}, operation_id={self.operation_id!r}, "
            f"platform={self.platform!r}, status={self.status!r}, result='***', error='***', "
            f"delivery_status={self.delivery_status!r})"
        )


@dataclass(frozen=True, repr=False)
class NewPlatformEvent:
    """A Telegram update about to be committed to the durable event outbox."""

    event_id: str
    telegram_update_id: int
    partition_key: str
    migrate_to_partition: str | None
    envelope: dict[str, object]
    received_at: float


@dataclass(frozen=True, repr=False)
class PlatformEvent:
    event_id: str
    telegram_update_id: int
    partition_key: str
    envelope: dict[str, object]
    delivery_status: str
    attempt_count: int = 0
    permanent_failure_count: int = 0
    next_attempt_at: float = 0.0
    last_attempt_at: float | None = None
    last_error: str | None = None
    last_http_status: int | None = None
    received_at: float = 0.0
    finished_at: float | None = None
    migrate_to_partition: str | None = None

    def __repr__(self) -> str:
        return (
            "PlatformEvent("
            f"event_id={self.event_id!r}, telegram_update_id={self.telegram_update_id}, "
            f"partition_key={self.partition_key!r}, envelope='***', "
            f"delivery_status={self.delivery_status!r}, attempt_count={self.attempt_count})"
        )


@dataclass(frozen=True)
class PlatformEventHead:
    """The oldest pending event of a partition that is due for delivery.

    ``envelope`` is loaded only for heads that are not blocked by a migration.
    """

    event_id: str
    telegram_update_id: int
    partition_key: str
    attempt_count: int
    permanent_failure_count: int
    blocked_by_event_id: str | None
    envelope: dict[str, object] | None = field(default=None, repr=False)


_EVENT_STATUSES = ("pending", "delivered", "rejected", "expired")
_PLATFORM_EVENT_COLUMNS = """
    event_id, telegram_update_id, partition_key, envelope_json, status, attempt_count,
    permanent_failure_count, next_attempt_at, last_attempt_at, last_error, last_http_status,
    received_at, finished_at, migrate_to_partition
"""


class SQLiteStorage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._connection: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def _run(self, function, *args):
        """Serialize one SQLite connection across the adapter's independent loops."""
        async with self._lock:
            worker = asyncio.ensure_future(asyncio.to_thread(function, *args))
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                # A running SQLite call cannot be interrupted. Keep the connection
                # locked until it finishes so shutdown never closes it mid-commit.
                while not worker.done():
                    try:
                        await asyncio.wait({worker})
                    except asyncio.CancelledError:
                        pass
                raise

    async def initialize(self) -> None:
        await self._run(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.chmod(0o700)
        connection = sqlite3.connect(self.database_path, check_same_thread=False)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS adapter_identity (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    adapter_id TEXT NOT NULL UNIQUE,
                    pairing_secret TEXT NOT NULL,
                    adapter_token TEXT,
                    pairing_status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    paired_at TEXT
                )
                """
            )
            self._migrate_operations_schema(connection)
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_results (
                    result_id TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL UNIQUE,
                    schema_version INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
                    result_json TEXT NOT NULL,
                    error_json TEXT,
                    completed_at TEXT NOT NULL,
                    delivery_status TEXT NOT NULL CHECK (delivery_status IN ('pending', 'delivered'))
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_update_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    next_update_id INTEGER
                )
                """
            )
            self._create_platform_events_schema(connection)
            self._migrate_legacy_platform_events(connection)
            connection.commit()
            self.database_path.chmod(0o600)
        except BaseException:
            connection.close()
            raise
        self._connection = connection

    @staticmethod
    def _migrate_operations_schema(connection: sqlite3.Connection) -> None:
        """Upgrade the Stage 4 receive-only inbox without discarding operations."""
        table = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'platform_operations'"
        ).fetchone()
        if table is not None and "execution_unknown" not in str(table[0]):
            connection.execute("ALTER TABLE platform_operations RENAME TO platform_operations_stage4")
            connection.execute(
                """
                CREATE TABLE platform_operations (
                    operation_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('received', 'executing', 'completed', 'execution_unknown')),
                    received_at TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    execution_started_at TEXT,
                    completed_at TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO platform_operations (
                    operation_id, schema_version, platform, operation_type, payload_json,
                    status, received_at
                )
                SELECT operation_id, schema_version, platform, operation_type, payload_json,
                       'received', received_at
                FROM platform_operations_stage4
                """
            )
            connection.execute("DROP TABLE platform_operations_stage4")
        elif table is None:
            connection.execute(
                """
                CREATE TABLE platform_operations (
                    operation_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('received', 'executing', 'completed', 'execution_unknown')),
                    received_at TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    execution_started_at TEXT,
                    completed_at TEXT
                )
                """
            )

    @staticmethod
    def _create_platform_events_schema(connection: sqlite3.Connection) -> None:
        """Durable inbound outbox: one row per Telegram update, delivered FIFO per partition.

        Times are Unix epoch seconds. ``migrate_to_partition`` is set on a group's
        ``migrate_to_chat_id`` message; while that row is pending, later events of the
        target supergroup partition wait for it.
        """
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS platform_events (
                event_id TEXT PRIMARY KEY,
                telegram_update_id INTEGER NOT NULL UNIQUE,
                partition_key TEXT NOT NULL,
                migrate_to_partition TEXT,
                envelope_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'rejected', 'expired')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                permanent_failure_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_attempt_at REAL,
                last_error TEXT,
                last_http_status INTEGER,
                received_at REAL NOT NULL,
                finished_at REAL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS platform_events_pending_partition
            ON platform_events (partition_key, telegram_update_id) WHERE status = 'pending'
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS platform_events_pending_migration
            ON platform_events (migrate_to_partition, telegram_update_id)
            WHERE status = 'pending' AND migrate_to_partition IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS platform_events_pending_received
            ON platform_events (received_at) WHERE status = 'pending'
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS platform_events_finished
            ON platform_events (finished_at) WHERE status != 'pending'
            """
        )

    @staticmethod
    def _migrate_legacy_platform_events(connection: sqlite3.Connection) -> None:
        """Move undelivered rows of the sequential-delivery table into the partitioned outbox.

        Delivered legacy rows are history only: the Telegram offset already covers them.
        """
        legacy = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pending_platform_events'"
        ).fetchone()
        if legacy is None:
            return
        now = time.time()
        rows = connection.execute(
            """
            SELECT event_id, telegram_update_id, envelope_json FROM pending_platform_events
            WHERE delivery_status = 'pending'
            """
        ).fetchall()
        for event_id, telegram_update_id, envelope_json in rows:
            envelope = json.loads(envelope_json)
            payload = envelope.get("payload") if isinstance(envelope, dict) else None
            update = payload if isinstance(payload, dict) else {}
            connection.execute(
                """
                INSERT INTO platform_events (
                    event_id, telegram_update_id, partition_key, migrate_to_partition,
                    envelope_json, status, received_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    event_id,
                    telegram_update_id,
                    partition_key_for_update(update),
                    migration_target_partition(update),
                    envelope_json,
                    now,
                ),
            )
        connection.execute("DROP TABLE pending_platform_events")

    async def get_or_create_identity(self) -> tuple[AdapterIdentity, bool]:
        return await self._run(self._get_or_create_identity_sync)

    def _get_or_create_identity_sync(self) -> tuple[AdapterIdentity, bool]:
        connection = self._require_connection()
        row = connection.execute("SELECT * FROM adapter_identity WHERE singleton = 1").fetchone()
        if row is not None:
            return self._identity_from_row(row), False

        now = _utc_now()
        identity = AdapterIdentity(
            adapter_id=str(uuid.uuid4()),
            pairing_secret=secrets.token_urlsafe(32),
            adapter_token=None,
            pairing_status="unpaired",
            created_at=now,
            paired_at=None,
        )
        connection.execute(
            """
            INSERT INTO adapter_identity (
                singleton, adapter_id, pairing_secret, adapter_token,
                pairing_status, created_at, paired_at
            ) VALUES (1, ?, ?, NULL, ?, ?, NULL)
            """,
            (identity.adapter_id, identity.pairing_secret, identity.pairing_status, now),
        )
        connection.commit()
        return identity, True

    async def update_pairing_status(self, status: str) -> AdapterIdentity:
        return await self._run(self._update_pairing_status_sync, status)

    def _update_pairing_status_sync(self, status: str) -> AdapterIdentity:
        connection = self._require_connection()
        connection.execute(
            "UPDATE adapter_identity SET pairing_status = ? WHERE singleton = 1", (status,)
        )
        connection.commit()
        return self._load_identity(connection)

    async def activate_identity(self, adapter_token: str) -> AdapterIdentity:
        return await self._run(self._activate_identity_sync, adapter_token)

    def _activate_identity_sync(self, adapter_token: str) -> AdapterIdentity:
        connection = self._require_connection()
        paired_at = _utc_now()
        connection.execute(
            """
            UPDATE adapter_identity
            SET adapter_token = ?, pairing_status = 'active', paired_at = ?
            WHERE singleton = 1
            """,
            (adapter_token, paired_at),
        )
        connection.commit()
        return self._load_identity(connection)

    async def get_telegram_offset(self) -> int | None:
        return await self._run(self._get_telegram_offset_sync)

    def _get_telegram_offset_sync(self) -> int | None:
        row = self._require_connection().execute(
            "SELECT next_update_id FROM telegram_update_state WHERE singleton = 1"
        ).fetchone()
        return int(row[0]) if row is not None and row[0] is not None else None

    async def advance_telegram_offset(self, next_update_id: int) -> None:
        await self._run(self._advance_telegram_offset_sync, next_update_id)

    def _advance_telegram_offset_sync(self, next_update_id: int) -> None:
        connection = self._require_connection()
        connection.execute(
            """
            INSERT INTO telegram_update_state (singleton, next_update_id) VALUES (1, ?)
            ON CONFLICT(singleton) DO UPDATE SET next_update_id = MAX(next_update_id, excluded.next_update_id)
            """,
            (next_update_id,),
        )
        connection.commit()

    async def store_telegram_updates(self, events: Sequence[NewPlatformEvent], next_update_id: int) -> int:
        """Atomically persist a whole getUpdates batch and advance the Telegram offset.

        The offset means "durably stored locally", so it moves only in the same commit
        as the batch. Re-storing an already known update is a no-op. Returns new rows.
        """
        return await self._run(self._store_telegram_updates_sync, events, next_update_id)

    def _store_telegram_updates_sync(self, events: Sequence[NewPlatformEvent], next_update_id: int) -> int:
        connection = self._require_connection()
        connection.execute("BEGIN")
        try:
            inserted = 0
            for event in events:
                cursor = connection.execute(
                    """
                    INSERT INTO platform_events (
                        event_id, telegram_update_id, partition_key, migrate_to_partition,
                        envelope_json, status, received_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        event.event_id,
                        event.telegram_update_id,
                        event.partition_key,
                        event.migrate_to_partition,
                        _canonical_json(event.envelope),
                        event.received_at,
                    ),
                )
                inserted += cursor.rowcount
            connection.execute(
                """
                INSERT INTO telegram_update_state (singleton, next_update_id) VALUES (1, ?)
                ON CONFLICT(singleton) DO UPDATE SET next_update_id = MAX(next_update_id, excluded.next_update_id)
                """,
                (next_update_id,),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        return inserted

    async def count_pending_platform_events(self) -> int:
        return await self._run(self._count_pending_platform_events_sync)

    def _count_pending_platform_events_sync(self) -> int:
        row = self._require_connection().execute(
            "SELECT COUNT(*) FROM platform_events WHERE status = 'pending'"
        ).fetchone()
        return int(row[0])

    async def list_ready_platform_event_heads(
        self, *, now: float, exclude_partitions: Sequence[str], limit: int, blocked_lookahead: int
    ) -> list[PlatformEventHead]:
        """Return due partition heads, unblocked first, oldest update first.

        Only partition heads are considered, so FIFO holds even while a head is
        retrying; at most ``limit + blocked_lookahead`` rows are returned and only the
        first ``limit`` unblocked heads carry a deserialized envelope.
        """
        return await self._run(
            self._list_ready_platform_event_heads_sync, now, tuple(exclude_partitions), limit, blocked_lookahead
        )

    def _list_ready_platform_event_heads_sync(
        self, now: float, exclude_partitions: tuple[str, ...], limit: int, blocked_lookahead: int
    ) -> list[PlatformEventHead]:
        if limit <= 0:
            return []
        connection = self._require_connection()
        placeholders = ", ".join("?" for _ in exclude_partitions)
        exclusion = f"AND e.partition_key NOT IN ({placeholders})" if exclude_partitions else ""
        rows = connection.execute(
            f"""
            WITH heads AS (
                SELECT partition_key, MIN(telegram_update_id) AS head_update_id
                FROM platform_events WHERE status = 'pending' GROUP BY partition_key
            )
            SELECT e.event_id, e.telegram_update_id, e.partition_key, e.attempt_count,
                   e.permanent_failure_count,
                   (
                       SELECT m.event_id FROM platform_events AS m
                       WHERE m.status = 'pending' AND m.migrate_to_partition = e.partition_key
                         AND m.telegram_update_id < e.telegram_update_id
                       ORDER BY m.telegram_update_id LIMIT 1
                   ) AS blocked_by
            FROM heads JOIN platform_events AS e ON e.telegram_update_id = heads.head_update_id
            WHERE e.next_attempt_at <= ? {exclusion}
            ORDER BY blocked_by IS NOT NULL, e.telegram_update_id
            LIMIT ?
            """,
            (now, *exclude_partitions, limit + blocked_lookahead),
        ).fetchall()
        heads: list[PlatformEventHead] = []
        unblocked = 0
        for row in rows:
            envelope = None
            if row[5] is None:
                if unblocked >= limit:
                    continue
                unblocked += 1
                envelope_row = connection.execute(
                    "SELECT envelope_json FROM platform_events WHERE event_id = ?", (row[0],)
                ).fetchone()
                envelope = json.loads(envelope_row[0])
            heads.append(
                PlatformEventHead(
                    event_id=str(row[0]),
                    telegram_update_id=int(row[1]),
                    partition_key=str(row[2]),
                    attempt_count=int(row[3]),
                    permanent_failure_count=int(row[4]),
                    blocked_by_event_id=str(row[5]) if row[5] is not None else None,
                    envelope=envelope,
                )
            )
        return heads

    async def mark_platform_event_delivered(self, event_id: str, *, now: float, http_status: int) -> bool:
        return await self._run(self._mark_platform_event_delivered_sync, event_id, now, http_status)

    def _mark_platform_event_delivered_sync(self, event_id: str, now: float, http_status: int) -> bool:
        connection = self._require_connection()
        cursor = connection.execute(
            """
            UPDATE platform_events
            SET status = 'delivered', attempt_count = attempt_count + 1, last_attempt_at = ?,
                last_http_status = ?, finished_at = ?
            WHERE event_id = ? AND status = 'pending'
            """,
            (now, http_status, now, event_id),
        )
        connection.commit()
        return cursor.rowcount == 1

    async def record_platform_event_failure(
        self,
        event_id: str,
        *,
        now: float,
        error: str,
        http_status: int | None,
        next_attempt_at: float,
        permanent: bool,
        max_permanent_failures: int,
    ) -> str | None:
        """Record one failed attempt; a permanent failure at the limit becomes ``rejected``.

        Returns the resulting status, or None when the event is no longer pending.
        """
        return await self._run(
            self._record_platform_event_failure_sync,
            event_id,
            now,
            error,
            http_status,
            next_attempt_at,
            permanent,
            max_permanent_failures,
        )

    def _record_platform_event_failure_sync(
        self,
        event_id: str,
        now: float,
        error: str,
        http_status: int | None,
        next_attempt_at: float,
        permanent: bool,
        max_permanent_failures: int,
    ) -> str | None:
        connection = self._require_connection()
        permanent_increment = 1 if permanent else 0
        connection.execute(
            """
            UPDATE platform_events
            SET attempt_count = attempt_count + 1,
                permanent_failure_count = permanent_failure_count + ?,
                last_attempt_at = ?, last_error = ?, last_http_status = ?, next_attempt_at = ?,
                status = CASE WHEN permanent_failure_count + ? >= ? THEN 'rejected' ELSE status END,
                finished_at = CASE WHEN permanent_failure_count + ? >= ? THEN ? ELSE finished_at END
            WHERE event_id = ? AND status = 'pending'
            """,
            (
                permanent_increment,
                now,
                error,
                http_status,
                next_attempt_at,
                permanent_increment,
                max_permanent_failures,
                permanent_increment,
                max_permanent_failures,
                now,
                event_id,
            ),
        )
        connection.commit()
        row = connection.execute("SELECT status FROM platform_events WHERE event_id = ?", (event_id,)).fetchone()
        return str(row[0]) if row is not None else None

    async def expire_platform_events(
        self, *, received_before: float, now: float, exclude_event_ids: Sequence[str]
    ) -> tuple[int, list[tuple[str, str]]]:
        """Stop delivering pending events older than the TTL, except in-flight ones.

        Returns the count and a small sample of ``(event_id, partition_key)`` for logs.
        """
        return await self._run(self._expire_platform_events_sync, received_before, now, tuple(exclude_event_ids))

    def _expire_platform_events_sync(
        self, received_before: float, now: float, exclude_event_ids: tuple[str, ...]
    ) -> tuple[int, list[tuple[str, str]]]:
        connection = self._require_connection()
        placeholders = ", ".join("?" for _ in exclude_event_ids)
        exclusion = f"AND event_id NOT IN ({placeholders})" if exclude_event_ids else ""
        sample = connection.execute(
            f"""
            SELECT event_id, partition_key FROM platform_events
            WHERE status = 'pending' AND received_at < ? {exclusion}
            ORDER BY received_at LIMIT 10
            """,
            (received_before, *exclude_event_ids),
        ).fetchall()
        if not sample:
            return 0, []
        cursor = connection.execute(
            f"""
            UPDATE platform_events SET status = 'expired', finished_at = ?
            WHERE status = 'pending' AND received_at < ? {exclusion}
            """,
            (now, received_before, *exclude_event_ids),
        )
        connection.commit()
        return cursor.rowcount, [(str(row[0]), str(row[1])) for row in sample]

    async def delete_finished_platform_events(self, *, finished_before: float, limit: int) -> int:
        """Delete at most ``limit`` delivered, rejected or expired rows past retention."""
        return await self._run(self._delete_finished_platform_events_sync, finished_before, limit)

    def _delete_finished_platform_events_sync(self, finished_before: float, limit: int) -> int:
        connection = self._require_connection()
        cursor = connection.execute(
            """
            DELETE FROM platform_events WHERE rowid IN (
                SELECT rowid FROM platform_events
                WHERE status != 'pending' AND finished_at < ? LIMIT ?
            )
            """,
            (finished_before, limit),
        )
        connection.commit()
        return cursor.rowcount

    async def platform_event_stats(self, *, now: float) -> dict[str, float | int]:
        return await self._run(self._platform_event_stats_sync, now)

    def _platform_event_stats_sync(self, now: float) -> dict[str, float | int]:
        connection = self._require_connection()
        pending, retrying, partitions, oldest = connection.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(attempt_count > 0), 0), COUNT(DISTINCT partition_key), MIN(received_at)
            FROM platform_events WHERE status = 'pending'
            """
        ).fetchone()
        blocked = connection.execute(
            """
            WITH heads AS (
                SELECT partition_key, MIN(telegram_update_id) AS head_update_id
                FROM platform_events WHERE status = 'pending' GROUP BY partition_key
            )
            SELECT COUNT(*) FROM heads WHERE EXISTS (
                SELECT 1 FROM platform_events AS m
                WHERE m.status = 'pending' AND m.migrate_to_partition = heads.partition_key
                  AND m.telegram_update_id < heads.head_update_id
            )
            """
        ).fetchone()[0]
        return {
            "pending_events": int(pending),
            "retrying_events": int(retrying),
            "active_partitions": int(partitions),
            "migration_blocked_partitions": int(blocked),
            "oldest_pending_age_seconds": max(0.0, now - float(oldest)) if oldest is not None else 0.0,
        }

    async def get_platform_event(self, event_id: str) -> PlatformEvent | None:
        return await self._run(self._get_platform_event_sync, event_id)

    def _get_platform_event_sync(self, event_id: str) -> PlatformEvent | None:
        row = self._require_connection().execute(
            f"SELECT {_PLATFORM_EVENT_COLUMNS} FROM platform_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return self._platform_event_from_row(row) if row is not None else None

    async def list_platform_events(self, *, status: str | None = None, limit: int = 100) -> list[PlatformEvent]:
        """Diagnostic listing in update order; delivery never loads the backlog this way."""
        if status is not None and status not in _EVENT_STATUSES:
            raise ValueError("unknown platform event status")
        return await self._run(self._list_platform_events_sync, status, limit)

    def _list_platform_events_sync(self, status: str | None, limit: int) -> list[PlatformEvent]:
        condition = "WHERE status = ?" if status is not None else ""
        parameters = (status, limit) if status is not None else (limit,)
        rows = self._require_connection().execute(
            f"""
            SELECT {_PLATFORM_EVENT_COLUMNS} FROM platform_events {condition}
            ORDER BY telegram_update_id LIMIT ?
            """,
            parameters,
        ).fetchall()
        return [self._platform_event_from_row(row) for row in rows]

    async def list_pending_platform_events(self, *, limit: int = 100) -> list[PlatformEvent]:
        return await self.list_platform_events(status="pending", limit=limit)

    @staticmethod
    def _platform_event_from_row(row: tuple[object, ...]) -> PlatformEvent:
        envelope = json.loads(str(row[3]))
        assert isinstance(envelope, dict)
        return PlatformEvent(
            event_id=str(row[0]),
            telegram_update_id=int(row[1]),
            partition_key=str(row[2]),
            envelope=envelope,
            delivery_status=str(row[4]),
            attempt_count=int(row[5]),
            permanent_failure_count=int(row[6]),
            next_attempt_at=float(row[7]),
            last_attempt_at=float(row[8]) if row[8] is not None else None,
            last_error=str(row[9]) if row[9] is not None else None,
            last_http_status=int(row[10]) if row[10] is not None else None,
            received_at=float(row[11]),
            finished_at=float(row[12]) if row[12] is not None else None,
            migrate_to_partition=str(row[13]) if row[13] is not None else None,
        )

    async def store_platform_operation(self, operation: PlatformOperation) -> str:
        """Durably store an immutable operation before an ACK is sent.

        Returns ``new``, ``duplicate``, or ``conflict`` for a reused operation ID.
        """
        return await self._run(self._store_platform_operation_sync, operation)

    def _store_platform_operation_sync(self, operation: PlatformOperation) -> str:
        connection = self._require_connection()
        payload_json = _canonical_json(operation.payload)
        row = connection.execute(
            """
            SELECT schema_version, platform, operation_type, payload_json
            FROM platform_operations WHERE operation_id = ?
            """,
            (operation.operation_id,),
        ).fetchone()
        if row is not None:
            if (
                row[0] == operation.schema_version
                and row[1] == operation.platform
                and row[2] == operation.operation_type
                and row[3] == payload_json
            ):
                return "duplicate"
            return "conflict"
        connection.execute(
            """
            INSERT INTO platform_operations (
                operation_id, schema_version, platform, operation_type,
                payload_json, status, received_at, attempt_count
            ) VALUES (?, ?, ?, ?, ?, 'received', ?, 0)
            """,
            (
                operation.operation_id,
                operation.schema_version,
                operation.platform,
                operation.operation_type,
                payload_json,
                operation.received_at,
            ),
        )
        connection.commit()
        return "new"

    async def get_platform_operation(self, operation_id: str) -> PlatformOperation | None:
        return await self._run(self._get_platform_operation_sync, operation_id)

    def _get_platform_operation_sync(self, operation_id: str) -> PlatformOperation | None:
        row = self._require_connection().execute(
            """
            SELECT operation_id, schema_version, platform, operation_type, payload_json, status, received_at, attempt_count
            FROM platform_operations WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[4])
        assert isinstance(payload, dict)
        return PlatformOperation(
            operation_id=str(row[0]),
            schema_version=int(row[1]),
            platform=str(row[2]),
            operation_type=str(row[3]),
            payload=payload,
            status=str(row[5]),
            received_at=str(row[6]),
            attempt_count=int(row[7]),
        )

    async def claim_next_platform_operation(self) -> PlatformOperation | None:
        return await self._run(self._claim_next_platform_operation_sync)

    def _claim_next_platform_operation_sync(self) -> PlatformOperation | None:
        connection = self._require_connection()
        row = connection.execute(
            """
            SELECT operation_id, schema_version, platform, operation_type, payload_json, status, received_at, attempt_count
            FROM platform_operations WHERE status = 'received' ORDER BY received_at LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        started_at = _utc_now()
        connection.execute(
            """
            UPDATE platform_operations
            SET status = 'executing', execution_started_at = ?, attempt_count = attempt_count + 1
            WHERE operation_id = ? AND status = 'received'
            """,
            (started_at, row[0]),
        )
        connection.commit()
        return PlatformOperation(
            operation_id=str(row[0]), schema_version=int(row[1]), platform=str(row[2]),
            operation_type=str(row[3]), payload=json.loads(row[4]), status="executing",
            received_at=str(row[6]), attempt_count=int(row[7]) + 1,
        )

    async def requeue_platform_operation(self, operation_id: str) -> None:
        await self._run(self._requeue_platform_operation_sync, operation_id)

    def _requeue_platform_operation_sync(self, operation_id: str) -> None:
        connection = self._require_connection()
        connection.execute(
            """
            UPDATE platform_operations SET status = 'received', execution_started_at = NULL
            WHERE operation_id = ? AND status = 'executing'
            """,
            (operation_id,),
        )
        connection.commit()

    async def mark_executing_operations_unknown(self) -> int:
        return await self._run(self._mark_executing_operations_unknown_sync)

    def _mark_executing_operations_unknown_sync(self) -> int:
        connection = self._require_connection()
        cursor = connection.execute(
            """
            UPDATE platform_operations SET status = 'execution_unknown'
            WHERE status = 'executing'
            """
        )
        connection.commit()
        return cursor.rowcount

    async def recover_executing_read_only_operations(self) -> int:
        return await self._run(self._recover_executing_read_only_operations_sync)

    def _recover_executing_read_only_operations_sync(self) -> int:
        connection = self._require_connection()
        placeholders = ", ".join("?" for _ in READ_ONLY_OPERATION_TYPES)
        cursor = connection.execute(
            f"""
            UPDATE platform_operations
            SET status = 'received', execution_started_at = NULL
            WHERE status = 'executing' AND operation_type IN ({placeholders})
            """,
            tuple(READ_ONLY_OPERATION_TYPES),
        )
        connection.commit()
        return cursor.rowcount

    async def complete_operation_with_result(self, result: PendingResult) -> None:
        await self._run(self._complete_operation_with_result_sync, result)

    def _complete_operation_with_result_sync(self, result: PendingResult) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN")
        try:
            connection.execute(
                """
                UPDATE platform_operations SET status = 'completed', completed_at = ?
                WHERE operation_id = ? AND status = 'executing'
                """,
                (result.completed_at, result.operation_id),
            )
            connection.execute(
                """
                INSERT INTO pending_results (
                    result_id, operation_id, schema_version, platform, status,
                    result_json, error_json, completed_at, delivery_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    result.result_id, result.operation_id, result.schema_version, result.platform,
                    result.status, _canonical_json(result.result),
                    _canonical_json(result.error) if result.error is not None else None,
                    result.completed_at,
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    async def list_pending_results(self) -> list[PendingResult]:
        return await self._run(self._list_pending_results_sync)

    def _list_pending_results_sync(self) -> list[PendingResult]:
        rows = self._require_connection().execute(
            """
            SELECT result_id, operation_id, schema_version, platform, status,
                   result_json, error_json, completed_at, delivery_status
            FROM pending_results WHERE delivery_status = 'pending' ORDER BY completed_at
            """
        ).fetchall()
        return [self._pending_result_from_row(row) for row in rows]

    async def mark_result_delivered(self, result_id: str) -> None:
        await self._run(self._mark_result_delivered_sync, result_id)

    def _mark_result_delivered_sync(self, result_id: str) -> None:
        connection = self._require_connection()
        connection.execute(
            "UPDATE pending_results SET delivery_status = 'delivered' WHERE result_id = ?", (result_id,)
        )
        connection.commit()

    @staticmethod
    def _pending_result_from_row(row: tuple[object, ...]) -> PendingResult:
        error = json.loads(row[6]) if row[6] is not None else None
        assert error is None or isinstance(error, dict)
        return PendingResult(
            result_id=str(row[0]), operation_id=str(row[1]), schema_version=int(row[2]),
            platform=str(row[3]), status=str(row[4]), result=json.loads(row[5]), error=error,
            completed_at=str(row[7]), delivery_status=str(row[8]),
        )

    def _load_identity(self, connection: sqlite3.Connection) -> AdapterIdentity:
        row = connection.execute("SELECT * FROM adapter_identity WHERE singleton = 1").fetchone()
        if row is None:
            raise RuntimeError("adapter identity is not initialized")
        return self._identity_from_row(row)

    @staticmethod
    def _identity_from_row(row: sqlite3.Row | tuple[object, ...]) -> AdapterIdentity:
        # Default tuple order follows the table declaration.
        return AdapterIdentity(
            adapter_id=str(row[1]),
            pairing_secret=str(row[2]),
            adapter_token=str(row[3]) if row[3] is not None else None,
            pairing_status=str(row[4]),
            created_at=str(row[5]),
            paired_at=str(row[6]) if row[6] is not None else None,
        )

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("storage has not been initialized")
        return self._connection

    async def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            await self._run(connection.close)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
