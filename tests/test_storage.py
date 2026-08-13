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
