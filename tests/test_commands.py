import asyncio
import json
import logging
import uuid

import pytest

from app.application import Application
from app.config import Config
from app.sprotect import CommandAuthenticationError, CommandTransportError, PlatformCommandsWebSocketClient
from app.storage import PlatformOperation, SQLiteStorage


class BootstrapUnused:
    async def close(self) -> None:
        pass


class ConnectionStub:
    def __init__(self, incoming=(), *, send_error: Exception | None = None) -> None:
        self.incoming = iter(incoming)
        self.sent: list[dict] = []
        self.send_error = send_error
        self.closed = False

    async def recv(self):
        item = next(self.incoming)
        if isinstance(item, Exception):
            raise item
        if isinstance(item, asyncio.Event):
            await item.wait()
        return item

    async def send(self, payload: str) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(json.loads(payload))

    async def close(self) -> None:
        self.closed = True


class CommandsStub:
    def __init__(self, connections) -> None:
        self.connections = iter(connections)
        self.calls = 0

    async def connect(self):
        self.calls += 1
        item = next(self.connections)
        if isinstance(item, Exception):
            raise item
        return item


async def wait_for(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


def operation(operation_id: str, *, payload=None) -> str:
    return json.dumps(
        {
            "type": "operation",
            "schema_version": 1,
            "operation": {
                "schema_version": 1,
                "operation_id": operation_id,
                "platform": "telegram",
                "operation_type": "send_message",
                "payload": {} if payload is None else payload,
            },
        }
    )


async def prepared_application(tmp_path, commands) -> Application:
    app = Application(
        Config("https://backend.example", "bot-token", tmp_path, "INFO"),
        bootstrap_client=BootstrapUnused(),  # type: ignore[arg-type]
        platform_commands_client=commands,  # type: ignore[arg-type]
    )
    await app.storage.initialize()
    return app


@pytest.mark.asyncio
async def test_operation_is_committed_before_ack_and_duplicate_is_acked_again(tmp_path) -> None:
    app = await prepared_application(tmp_path, CommandsStub([]))
    operation_id = str(uuid.uuid4())
    first = ConnectionStub()
    await app._handle_command_frame(first, operation(operation_id))

    saved = await app.storage.get_platform_operation(operation_id)
    assert saved is not None and saved.status == "received"
    assert first.sent == [{"type": "ack", "schema_version": 1, "operation_id": operation_id}]

    second = ConnectionStub()
    await app._handle_command_frame(second, operation(operation_id))
    assert second.sent == [{"type": "ack", "schema_version": 1, "operation_id": operation_id}]
    await app.storage.close()


@pytest.mark.asyncio
async def test_same_operation_id_with_changed_payload_is_protocol_violation(tmp_path, caplog) -> None:
    app = await prepared_application(tmp_path, CommandsStub([]))
    operation_id = str(uuid.uuid4())
    await app._handle_command_frame(ConnectionStub(), operation(operation_id, payload={"text": "one"}))
    conflict = ConnectionStub()
    with caplog.at_level(logging.ERROR):
        await app._handle_command_frame(conflict, operation(operation_id, payload={"text": "two"}))

    assert conflict.sent == []
    assert "platform_operation_protocol_violation" in caplog.text
    assert "one" not in caplog.text and "two" not in caplog.text
    await app.storage.close()


@pytest.mark.asyncio
async def test_invalid_frame_is_not_acked_and_heartbeat_is_acknowledged(tmp_path) -> None:
    app = await prepared_application(tmp_path, CommandsStub([]))
    connection = ConnectionStub()
    await app._handle_command_frame(connection, "{bad-json")
    await app._handle_command_frame(connection, json.dumps({"type": "heartbeat", "schema_version": 1}))

    assert connection.sent == [{"type": "heartbeat_ack", "schema_version": 1}]
    await app.storage.close()


@pytest.mark.asyncio
async def test_disconnect_after_persist_before_ack_redelivers_and_acks(tmp_path) -> None:
    operation_id = str(uuid.uuid4())
    first = ConnectionStub([operation(operation_id)], send_error=CommandTransportError("disconnect"))
    parked = asyncio.Event()
    second = ConnectionStub([operation(operation_id), parked])
    commands = CommandsStub([first, second])
    app = await prepared_application(tmp_path, commands)
    app._initial_retry_delay = 0.01
    app._max_retry_delay = 0.05
    app._commands_task = asyncio.create_task(app._commands_loop("adapter-id"))

    await wait_for(lambda: len(second.sent) == 1)
    assert (await app.storage.get_platform_operation(operation_id)) is not None
    assert second.sent[0]["operation_id"] == operation_id
    app._commands_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await app._commands_task
    await app.storage.close()


@pytest.mark.asyncio
async def test_disconnect_before_operation_reconnects_and_auth_is_slow_degraded(tmp_path) -> None:
    parked = asyncio.Event()
    commands = CommandsStub([CommandTransportError("offline"), ConnectionStub([parked])])
    app = await prepared_application(tmp_path, commands)
    app._initial_retry_delay = 0.01
    app._max_retry_delay = 0.05
    task = asyncio.create_task(app._commands_loop("adapter-id"))
    await wait_for(lambda: commands.calls == 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await app.storage.close()


@pytest.mark.asyncio
async def test_websocket_client_uses_bearer_header_only(monkeypatch) -> None:
    captured = {}

    async def fake_connect(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return ConnectionStub()

    monkeypatch.setattr("app.sprotect.commands.connect", fake_connect)
    client = PlatformCommandsWebSocketClient("https://backend.example/base", "adapter-secret")
    await client.connect()

    assert captured["url"] == "wss://backend.example/base/api/v1/platform-adapters/commands/ws/"
    assert captured["additional_headers"] == {"Authorization": "Bearer adapter-secret"}
    assert "adapter-secret" not in captured["url"]


@pytest.mark.asyncio
async def test_authentication_failure_is_logged_without_token(tmp_path, caplog) -> None:
    commands = CommandsStub([CommandAuthenticationError("revoked")])
    app = await prepared_application(tmp_path, commands)
    app._initial_retry_delay = 1
    app._max_retry_delay = 1
    with caplog.at_level(logging.INFO):
        task = asyncio.create_task(app._commands_loop("adapter-id"))
        await wait_for(lambda: commands.calls == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert "adapter-secret" not in caplog.text
    await app.storage.close()


@pytest.mark.asyncio
async def test_storage_operation_is_immutable(tmp_path) -> None:
    storage = SQLiteStorage(tmp_path / "adapter.db")
    await storage.initialize()
    identifier = str(uuid.uuid4())
    stored = PlatformOperation(identifier, 1, "telegram", "delete_message", {"chat_id": 1}, "received", "now")

    assert await storage.store_platform_operation(stored) == "new"
    assert await storage.store_platform_operation(stored) == "duplicate"
    changed = PlatformOperation(identifier, 1, "telegram", "delete_message", {"chat_id": 2}, "received", "now")
    assert await storage.store_platform_operation(changed) == "conflict"
    await storage.close()
