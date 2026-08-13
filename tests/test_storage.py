import sqlite3

import pytest

from app.storage import SQLiteStorage


@pytest.mark.asyncio
async def test_initializes_sqlite_database(tmp_path) -> None:
    database_path = tmp_path / "nested" / "adapter.db"
    storage = SQLiteStorage(database_path)

    await storage.initialize()
    await storage.close()

    assert database_path.is_file()
    connection = sqlite3.connect(database_path)
    assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)
    connection.close()


@pytest.mark.asyncio
async def test_persists_telegram_offset(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()

    assert await storage.get_telegram_offset() is None
    await storage.advance_telegram_offset(101)
    await storage.advance_telegram_offset(100)
    assert await storage.get_telegram_offset() == 101
    await storage.close()


@pytest.mark.asyncio
async def test_migrates_stage_4_operation_inbox_without_losing_received_operation(tmp_path) -> None:
    database_path = tmp_path / "adapter.db"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE platform_operations (
            operation_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL,
            platform TEXT NOT NULL, operation_type TEXT NOT NULL, payload_json TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status = 'received'), received_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "INSERT INTO platform_operations VALUES ('operation-1', 1, 'telegram', 'send_message', '{}', 'received', 'now')"
    )
    connection.commit()
    connection.close()

    storage = SQLiteStorage(database_path)
    await storage.initialize()
    operation = await storage.claim_next_platform_operation()
    assert operation is not None and operation.operation_id == "operation-1"
    await storage.close()
