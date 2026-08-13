import asyncio
import json
import uuid

import httpx
import pytest

from app.application import Application
from app.config import Config
from app.sprotect import (
    PermanentResultDeliveryError,
    SprotectPlatformResultsClient,
    TransientResultDeliveryError,
)
from app.storage import PendingResult, PlatformOperation, SQLiteStorage
from app.telegram import TelegramOperationError, TelegramOperationsClient


class BootstrapUnused:
    async def close(self) -> None:
        pass


class OperationsStub:
    def __init__(self, *, send=None, delete=None, membership=None) -> None:
        self.send = send if send is not None else {"telegram_chat_id": "1", "telegram_message_id": "2"}
        self.delete = delete if delete is not None else {"telegram_chat_id": "1", "telegram_message_id": "2", "deleted": True}
        self.membership = membership if membership is not None else {
            "chat_member": {"status": "member", "user": {"id": 42, "is_bot": False}}
        }
        self.calls: list[str] = []
        self.closed = False

    async def send_message(self, payload):
        self.calls.append("send_message")
        if isinstance(self.send, Exception):
            raise self.send
        return self.send

    async def delete_message(self, payload):
        self.calls.append("delete_message")
        if isinstance(self.delete, Exception):
            raise self.delete
        return self.delete

    async def get_chat_member(self, payload):
        self.calls.append("get_chat_member")
        if isinstance(self.membership, Exception):
            raise self.membership
        return self.membership

    async def close(self) -> None:
        self.closed = True


class ResultsStub:
    def __init__(self, outcomes=()) -> None:
        self.outcomes = iter(outcomes)
        self.results: list[PendingResult] = []
        self.closed = False

    async def deliver(self, result):
        self.results.append(result)
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def close(self) -> None:
        self.closed = True


async def wait_for(predicate) -> None:
    for _ in range(150):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


async def prepared_app(tmp_path, operations, results) -> Application:
    app = Application(
        Config("https://backend.example", "bot-secret", tmp_path, "INFO"),
        bootstrap_client=BootstrapUnused(),  # type: ignore[arg-type]
        telegram_operations_client=operations,  # type: ignore[arg-type]
        platform_results_client=results,  # type: ignore[arg-type]
        initial_retry_delay=0.01,
        max_retry_delay=0.05,
    )
    await app.storage.initialize()
    return app


async def add_operation(app, operation_type="send_message", payload=None) -> str:
    operation_id = str(uuid.uuid4())
    operation = PlatformOperation(
        operation_id, 1, "telegram", operation_type,
        {"chat_id": 1, "text": "do not log"} if payload is None else payload,
        "received", "now",
    )
    assert await app.storage.store_platform_operation(operation) == "new"
    return operation_id


@pytest.mark.asyncio
async def test_send_and_delete_success_create_durable_results_then_deliver(tmp_path) -> None:
    operations = OperationsStub()
    results = ResultsStub(["accepted", "duplicate"])
    app = await prepared_app(tmp_path, operations, results)
    send_id = await add_operation(app)
    delete_id = await add_operation(app, "delete_message", {"chat_id": 1, "message_id": 2})
    app._executor_task = asyncio.create_task(app._executor_loop())
    app._result_delivery_task = asyncio.create_task(app._result_delivery_loop())

    await wait_for(lambda: len(results.results) == 2)
    assert operations.calls == ["send_message", "delete_message"]
    assert [item.operation_id for item in results.results] == [send_id, delete_id]
    assert all(item.status == "succeeded" for item in results.results)
    for _ in range(150):
        if not await app.storage.list_pending_results():
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("results were not marked delivered")
    for task in (app._executor_task, app._result_delivery_task):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await app.storage.close()


@pytest.mark.asyncio
async def test_terminal_telegram_failure_persists_failed_result(tmp_path) -> None:
    operations = OperationsStub(send=TelegramOperationError("chat_not_found"))
    app = await prepared_app(tmp_path, operations, ResultsStub([]))
    operation_id = await add_operation(app)
    task = asyncio.create_task(app._executor_loop())

    # Let the executor reach the durable terminal result without starting delivery.
    for _ in range(100):
        pending = await app.storage.list_pending_results()
        if pending:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("missing result")
    assert pending[0].operation_id == operation_id
    assert pending[0].status == "failed"
    assert pending[0].error == {
        "code": "chat_not_found",
        "description": "Telegram operation failed",
        "retryable": False,
    }
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await app.storage.close()


@pytest.mark.asyncio
async def test_backend_outage_after_success_retains_result_and_restart_resends(tmp_path) -> None:
    operations = OperationsStub()
    unavailable = ResultsStub([TransientResultDeliveryError("offline")])
    first = await prepared_app(tmp_path, operations, unavailable)
    await add_operation(first)
    first._executor_task = asyncio.create_task(first._executor_loop())
    first._result_delivery_task = asyncio.create_task(first._result_delivery_loop())
    await wait_for(lambda: len(unavailable.results) == 1)
    result_id = unavailable.results[0].result_id
    for task in (first._executor_task, first._result_delivery_task):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await first.storage.close()

    delivered = ResultsStub(["accepted"])
    second = await prepared_app(tmp_path, OperationsStub(), delivered)
    second._result_delivery_task = asyncio.create_task(second._result_delivery_loop())
    await wait_for(lambda: len(delivered.results) == 1)
    assert delivered.results[0].result_id == result_id
    second._result_delivery_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second._result_delivery_task
    await second.storage.close()


@pytest.mark.asyncio
async def test_ambiguous_executing_crash_is_not_replayed(tmp_path) -> None:
    app = await prepared_app(tmp_path, OperationsStub(), ResultsStub([]))
    operation_id = await add_operation(app)
    assert (await app.storage.claim_next_platform_operation()).status == "executing"  # type: ignore[union-attr]
    await app.storage.close()

    recovered = await prepared_app(tmp_path, OperationsStub(), ResultsStub([]))
    assert await recovered.storage.mark_executing_operations_unknown() == 1
    assert (await recovered.storage.get_platform_operation(operation_id)).status == "execution_unknown"  # type: ignore[union-attr]
    assert await recovered.storage.claim_next_platform_operation() is None
    await recovered.storage.close()


@pytest.mark.asyncio
async def test_executing_membership_lookup_is_requeued_after_restart(tmp_path) -> None:
    first = await prepared_app(tmp_path, OperationsStub(), ResultsStub([]))
    operation_id = await add_operation(
        first,
        "get_chat_member",
        {"chat_id": "-100", "user_id": "42"},
    )
    assert (await first.storage.claim_next_platform_operation()).status == "executing"  # type: ignore[union-attr]
    await first.storage.close()

    recovered = await prepared_app(tmp_path, OperationsStub(), ResultsStub([]))
    assert await recovered.storage.recover_executing_membership_operations() == 1
    assert await recovered.storage.mark_executing_operations_unknown() == 0
    claimed = await recovered.storage.claim_next_platform_operation()
    assert claimed is not None
    assert claimed.operation_id == operation_id
    assert claimed.operation_type == "get_chat_member"
    await recovered.storage.close()


@pytest.mark.asyncio
async def test_duplicate_completed_operation_is_never_executed_again(tmp_path) -> None:
    operations = OperationsStub()
    app = await prepared_app(tmp_path, operations, ResultsStub([]))
    operation_id = await add_operation(app)
    claimed = await app.storage.claim_next_platform_operation()
    assert claimed is not None
    await app._complete_with_result(claimed, status="succeeded", result={}, error=None)
    duplicate = PlatformOperation(operation_id, 1, "telegram", "send_message", {"chat_id": 1, "text": "do not log"}, "received", "later")
    assert await app.storage.store_platform_operation(duplicate) == "duplicate"
    assert await app.storage.claim_next_platform_operation() is None
    assert operations.calls == []
    await app.storage.close()


@pytest.mark.asyncio
async def test_permanent_result_rejection_keeps_durable_pending_result(tmp_path) -> None:
    app = await prepared_app(tmp_path, OperationsStub(), ResultsStub([PermanentResultDeliveryError("conflict")]))
    operation_id = await add_operation(app)
    claimed = await app.storage.claim_next_platform_operation()
    assert claimed is not None
    await app._complete_with_result(claimed, status="succeeded", result={}, error=None)
    task = asyncio.create_task(app._result_delivery_loop())
    await asyncio.sleep(0.03)
    assert (await app.storage.list_pending_results())[0].operation_id == operation_id
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await app.storage.close()


@pytest.mark.asyncio
async def test_results_http_client_uses_exact_envelope_and_bearer_token() -> None:
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200, json={}))
    ) as client:
        result = PendingResult(str(uuid.uuid4()), str(uuid.uuid4()), 1, "telegram", "failed", {}, {"code": "x"}, "2026-01-01T00:00:00Z", "pending")
        assert await SprotectPlatformResultsClient("https://backend.example", "adapter-secret", client).deliver(result) == "duplicate"

    assert requests[0].url.path == "/api/v1/platform-adapters/results/"
    assert requests[0].headers["Authorization"] == "Bearer adapter-secret"
    assert json.loads(requests[0].content)["result_id"] == result.result_id


@pytest.mark.asyncio
async def test_telegram_operations_client_does_not_expose_token_in_payloads() -> None:
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200, json={"ok": True, "result": {"message_id": 7, "chat": {"id": 5}}}))
    ) as client:
        result = await TelegramOperationsClient("bot-secret", client).send_message({"chat_id": "5", "text": "private"})
    assert result == {"telegram_chat_id": "5", "telegram_message_id": "7"}
    assert "bot-secret" in str(requests[0].url)
    assert "bot-secret" not in requests[0].content.decode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    ["creator", "administrator", "member", "restricted", "left", "kicked"],
)
async def test_get_chat_member_returns_supported_raw_membership(status) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {"status": status, "user": {"id": 42, "is_bot": False}},
                },
            )
        )
    ) as client:
        result = await TelegramOperationsClient("bot-secret", client).get_chat_member(
            {"chat_id": "-100", "user_id": "42"}
        )

    assert result["chat_member"]["status"] == status


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_get_chat_member_temporary_failure_is_retryable(status_code) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                status_code, json={"ok": False, "description": "temporary"}
            )
        )
    ) as client:
        with pytest.raises(TelegramOperationError) as captured:
            await TelegramOperationsClient("bot-secret", client).get_chat_member(
                {"chat_id": "-100", "user_id": "42"}
            )

    assert captured.value.retryable is True


@pytest.mark.asyncio
async def test_get_chat_member_network_failure_is_ambiguous_and_retryable() -> None:
    def fail(request):
        raise httpx.ConnectError("offline", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        with pytest.raises(TelegramOperationError) as captured:
            await TelegramOperationsClient("bot-secret", client).get_chat_member(
                {"chat_id": "-100", "user_id": "42"}
            )

    assert captured.value.retryable is True
    assert captured.value.ambiguous is True


@pytest.mark.asyncio
async def test_get_chat_member_invalid_chat_is_terminal() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                400, json={"ok": False, "description": "Bad Request: chat not found"}
            )
        )
    ) as client:
        with pytest.raises(TelegramOperationError) as captured:
            await TelegramOperationsClient("bot-secret", client).get_chat_member(
                {"chat_id": "-100", "user_id": "42"}
            )

    assert captured.value.code == "chat_not_found"
    assert captured.value.retryable is False
    assert captured.value.ambiguous is False


@pytest.mark.asyncio
async def test_get_chat_member_retryable_failure_is_not_completed(tmp_path) -> None:
    operations = OperationsStub(
        membership=TelegramOperationError("rate_limited", retryable=True)
    )
    app = await prepared_app(tmp_path, operations, ResultsStub([]))
    operation_id = await add_operation(
        app,
        "get_chat_member",
        {"chat_id": "-100", "user_id": "42"},
    )
    task = asyncio.create_task(app._executor_loop())

    await wait_for(lambda: operations.calls.count("get_chat_member") >= 2)
    assert await app.storage.list_pending_results() == []
    assert (await app.storage.get_platform_operation(operation_id)).status in {
        "received",
        "executing",
    }
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await app.storage.close()
