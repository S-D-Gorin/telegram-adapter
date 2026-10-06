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


@pytest.mark.asyncio
async def test_migrates_sequential_event_table_into_partitioned_outbox(tmp_path) -> None:
    import json

    database_path = tmp_path / "adapter.db"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE pending_platform_events (
            event_id TEXT PRIMARY KEY, telegram_update_id INTEGER NOT NULL UNIQUE,
            envelope_json TEXT NOT NULL,
            delivery_status TEXT NOT NULL CHECK (delivery_status IN ('pending', 'delivered'))
        )
        """
    )
    for number, status, payload in (
        (1, "delivered", {"update_id": 1, "message": {"chat": {"id": -5}}}),
        (2, "pending", {"update_id": 2, "message": {"chat": {"id": -5}, "migrate_to_chat_id": -1009}}),
        (3, "pending", {"update_id": 3, "poll": {"id": "p"}}),
    ):
        envelope = {"event_id": f"telegram:{number}", "occurred_at": "2026-01-01T00:00:00Z", "payload": payload}
        connection.execute(
            "INSERT INTO pending_platform_events VALUES (?, ?, ?, ?)",
            (f"telegram:{number}", number, json.dumps(envelope), status),
        )
    connection.commit()
    connection.close()

    storage = SQLiteStorage(database_path)
    await storage.initialize()
    pending = await storage.list_pending_platform_events()
    assert [(event.event_id, event.partition_key, event.migrate_to_partition) for event in pending] == [
        ("telegram:2", "-5", "-1009"),
        ("telegram:3", "unrouted", None),
    ]
    assert pending[0].envelope["occurred_at"] == "2026-01-01T00:00:00Z"
    await storage.close()

    connection = sqlite3.connect(database_path)
    legacy = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'pending_platform_events'"
    ).fetchone()
    connection.close()
    assert legacy is None


def outbox_event(number: int, partition: str, *, migrate_to: str | None = None):
    from app.storage import NewPlatformEvent

    return NewPlatformEvent(
        event_id=f"telegram:{number}",
        telegram_update_id=number,
        partition_key=partition,
        migrate_to_partition=migrate_to,
        envelope={"event_id": f"telegram:{number}"},
        received_at=1.0,
    )


@pytest.mark.asyncio
async def test_ready_heads_are_bounded_fifo_and_report_migration_blocks(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    await storage.store_telegram_updates(
        [
            outbox_event(1, "A"),
            outbox_event(2, "A", migrate_to="E"),
            outbox_event(3, "B"),
            outbox_event(4, "B"),
            outbox_event(5, "E"),
            outbox_event(6, "C"),
            outbox_event(7, "D"),
        ],
        8,
    )
    await storage.record_platform_event_failure(
        "telegram:6", now=10.0, error="503", http_status=503, next_attempt_at=50.0,
        permanent=False, max_permanent_failures=5,
    )

    heads = await storage.list_ready_platform_event_heads(
        now=20.0, exclude_partitions=["D"], limit=2, blocked_lookahead=4
    )
    # Only partition heads: A=1, B=3; C is backing off, D is in flight, E waits for A's migration.
    assert [(head.event_id, head.envelope is not None) for head in heads] == [
        ("telegram:1", True),
        ("telegram:3", True),
        ("telegram:5", False),
    ]
    assert heads[2].blocked_by_event_id == "telegram:2"

    await storage.mark_platform_event_delivered("telegram:1", now=21.0, http_status=202)
    await storage.mark_platform_event_delivered("telegram:2", now=21.0, http_status=202)
    heads = await storage.list_ready_platform_event_heads(now=60.0, exclude_partitions=[], limit=10, blocked_lookahead=4)
    assert [head.event_id for head in heads] == ["telegram:3", "telegram:5", "telegram:6", "telegram:7"]
    assert all(head.blocked_by_event_id is None for head in heads)
    await storage.close()


@pytest.mark.asyncio
async def test_expiry_skips_in_flight_events(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    await storage.store_telegram_updates([outbox_event(1, "A"), outbox_event(2, "B")], 3)

    count, sample = await storage.expire_platform_events(
        received_before=5.0, now=5.0, exclude_event_ids=["telegram:1"]
    )
    assert (count, sample) == (1, [("telegram:2", "B")])
    assert (await storage.get_platform_event("telegram:1")).delivery_status == "pending"
    assert (await storage.get_platform_event("telegram:2")).delivery_status == "expired"
    await storage.close()
