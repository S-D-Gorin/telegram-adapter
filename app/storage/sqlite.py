"""Small, explicit SQLite lifecycle wrapper.

Schema is intentionally empty in Stage 1. Future migrations will introduce
adapter_identity, completed_commands, pending_results and telegram_update_state.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path


class SQLiteStorage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._connection: sqlite3.Connection | None = None

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, check_same_thread=False)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA synchronous=FULL")
        except BaseException:
            connection.close()
            raise
        self._connection = connection

    async def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            await asyncio.to_thread(connection.close)
