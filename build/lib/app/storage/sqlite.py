"""Explicit SQLite persistence for adapter credentials and future state."""

from __future__ import annotations

import asyncio
import json
import secrets
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


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


class SQLiteStorage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._connection: sqlite3.Connection | None = None

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

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

    async def get_or_create_identity(self) -> tuple[AdapterIdentity, bool]:
        return await asyncio.to_thread(self._get_or_create_identity_sync)

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
        return await asyncio.to_thread(self._update_pairing_status_sync, status)

    def _update_pairing_status_sync(self, status: str) -> AdapterIdentity:
        connection = self._require_connection()
        connection.execute(
            "UPDATE adapter_identity SET pairing_status = ? WHERE singleton = 1", (status,)
        )
        connection.commit()
        return self._load_identity(connection)

    async def activate_identity(self, adapter_token: str) -> AdapterIdentity:
        return await asyncio.to_thread(self._activate_identity_sync, adapter_token)

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
        return await asyncio.to_thread(self._get_telegram_offset_sync)

    def _get_telegram_offset_sync(self) -> int | None:
        row = self._require_connection().execute(
            "SELECT next_update_id FROM telegram_update_state WHERE singleton = 1"
        ).fetchone()
        return int(row[0]) if row is not None and row[0] is not None else None

    async def advance_telegram_offset(self, next_update_id: int) -> None:
        await asyncio.to_thread(self._advance_telegram_offset_sync, next_update_id)

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

    async def store_platform_operation(self, operation: PlatformOperation) -> str:
        """Durably store an immutable operation before an ACK is sent.

        Returns ``new``, ``duplicate``, or ``conflict`` for a reused operation ID.
        """
        return await asyncio.to_thread(self._store_platform_operation_sync, operation)

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
        return await asyncio.to_thread(self._get_platform_operation_sync, operation_id)

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
        return await asyncio.to_thread(self._claim_next_platform_operation_sync)

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
        await asyncio.to_thread(self._requeue_platform_operation_sync, operation_id)

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
        return await asyncio.to_thread(self._mark_executing_operations_unknown_sync)

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

    async def complete_operation_with_result(self, result: PendingResult) -> None:
        await asyncio.to_thread(self._complete_operation_with_result_sync, result)

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
        return await asyncio.to_thread(self._list_pending_results_sync)

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
        await asyncio.to_thread(self._mark_result_delivered_sync, result_id)

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
            await asyncio.to_thread(connection.close)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
