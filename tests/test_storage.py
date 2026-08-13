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
