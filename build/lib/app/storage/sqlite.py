"""Explicit SQLite persistence for adapter credentials and future state."""

from __future__ import annotations

import asyncio
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
            connection.commit()
            self.database_path.chmod(0o600)
        except BaseException:
            connection.close()
            raise
        self._connection = connection

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
