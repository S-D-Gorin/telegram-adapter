"""Guardian Resource State Sync wire-contract tests."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from app.telegram import TelegramOperationError, TelegramOperationsClient


def _assert_utc_timestamp(value: object) -> datetime:
    assert isinstance(value, str)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == UTC.utcoffset(parsed)
    return parsed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation_name",
    [
        "get_resource_administrators",
        "get_resource_bot_membership",
        "get_resource_info",
        "get_resource_member_count",
    ],
)
@pytest.mark.parametrize("resource_id", [123, "", "   "])
async def test_snapshot_operations_require_nonempty_string_resource_id(operation_name, resource_id) -> None:
    requests: list[httpx.Request] = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(500, json={})
        )
    ) as client:
        operation = getattr(TelegramOperationsClient("bot-token", client), operation_name)
        with pytest.raises(TelegramOperationError, match="invalid_operation_payload"):
            await operation({"resource": {"id": resource_id}})

    assert requests == []


@pytest.mark.asyncio
async def test_administrators_contract_uses_string_ids_and_observed_at() -> None:
    members = [
        {
            "status": "creator",
            "user": {"id": 1, "is_bot": False, "first_name": "Owner"},
            "is_anonymous": False,
        },
        {
            "status": "administrator",
            "user": {"id": 2, "is_bot": True, "first_name": "Helper"},
            "is_anonymous": True,
        },
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": members}))
    ) as client:
        result = await TelegramOperationsClient("bot-token", client).get_resource_administrators(
            {"resource": {"id": "-100"}}
        )

    assert result["resource"] == {"id": "-100"}
    assert [item["role"] for item in result["administrators"]] == ["creator", "administrator"]
    assert [item["user"]["id"] for item in result["administrators"]] == ["1", "2"]
    _assert_utc_timestamp(result["observed_at"])


@pytest.mark.asyncio
async def test_administrators_empty_list_is_an_authoritative_snapshot() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": []}))
    ) as client:
        result = await TelegramOperationsClient("bot-token", client).get_resource_administrators(
            {"resource": {"id": "-100"}}
        )

    assert result["administrators"] == []
    _assert_utc_timestamp(result["observed_at"])


@pytest.mark.asyncio
@pytest.mark.parametrize("chat_type", ["group", "supergroup", "channel"])
@pytest.mark.parametrize("username", ["guardian_resource", None])
async def test_resource_info_contract_supports_guardian_resource_types(chat_type, username) -> None:
    chat = {"id": -100, "type": chat_type, "title": "Resource"}
    if username is not None:
        chat["username"] = username
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": chat}))
    ) as client:
        result = await TelegramOperationsClient("bot-token", client).get_resource_info(
            {"resource": {"id": "-100"}}
        )

    assert result["resource"] == {
        "id": "-100",
        "title": "Resource",
        "username": username,
        "type": chat_type,
    }
    _assert_utc_timestamp(result["observed_at"])


@pytest.mark.asyncio
async def test_resource_info_rejects_private_and_malformed_chats() -> None:
    private = {"id": 1, "type": "private", "first_name": "Private"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": private}))
    ) as client:
        with pytest.raises(TelegramOperationError) as captured:
            await TelegramOperationsClient("bot-token", client).get_resource_info({"resource": {"id": "1"}})
    assert captured.value.code == "unsupported_resource_type"

    malformed = {"id": -100, "type": "supergroup", "title": 123}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": malformed}))
    ) as client:
        with pytest.raises(TelegramOperationError, match="telegram_invalid_response"):
            await TelegramOperationsClient("bot-token", client).get_resource_info({"resource": {"id": "-100"}})


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 27])
async def test_member_count_contract_preserves_nonnegative_json_integer(count) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": count}))
    ) as client:
        result = await TelegramOperationsClient("bot-token", client).get_resource_member_count(
            {"resource": {"id": "-100"}}
        )

    assert result["resource"] == {"id": "-100"}
    assert result["member_count"] == count
    assert type(result["member_count"]) is int
    _assert_utc_timestamp(result["observed_at"])


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [-1, True, "0"])
async def test_member_count_rejects_malformed_values(count) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": count}))
    ) as client:
        with pytest.raises(TelegramOperationError, match="telegram_invalid_response"):
            await TelegramOperationsClient("bot-token", client).get_resource_member_count(
                {"resource": {"id": "-100"}}
            )


async def _membership(member: dict[str, object]) -> dict[str, object]:
    def reply(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("getMe"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"id": 77, "is_bot": True, "first_name": "Guardian"}},
            )
        return httpx.Response(200, json={"ok": True, "result": member})

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        return await TelegramOperationsClient("bot-token", client).get_resource_bot_membership(
            {"resource": {"id": "-100"}}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("member", "expected_permissions"),
    [
        ({"status": "creator", "user": {"id": 77}}, {}),
        (
            {
                "status": "administrator",
                "user": {"id": 77},
                "can_delete_messages": True,
                "can_post_messages": False,
            },
            {"delete_messages": True, "post_messages": False},
        ),
        (
            {
                "status": "administrator",
                "user": {"id": 77},
                "can_delete_messages": False,
                "can_post_messages": True,
            },
            {"delete_messages": False, "post_messages": True},
        ),
        (
            {"status": "administrator", "user": {"id": 77}},
            {"delete_messages": False, "post_messages": False},
        ),
        ({"status": "restricted", "is_member": True, "user": {"id": 77}}, {}),
    ],
)
async def test_bot_membership_normalizes_permissions(member, expected_permissions) -> None:
    result = await _membership(member)

    assert result["resource"] == {"id": "-100"}
    assert result["bot"]["id"] == "77"
    assert result["bot"]["is_bot"] is True
    assert result["membership"]["permissions"] == expected_permissions
    assert all(type(value) is bool for value in expected_permissions.values())
    _assert_utc_timestamp(result["observed_at"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "member",
    [
        {"status": "left", "user": {"id": 77}},
        {"status": "kicked", "user": {"id": 77}},
        {"status": "restricted", "is_member": False, "user": {"id": 77}},
    ],
)
async def test_authoritative_non_member_becomes_terminal_snapshot(member) -> None:
    with pytest.raises(TelegramOperationError) as captured:
        await _membership(member)

    error = captured.value
    assert error.code == "bot_not_member"
    assert error.retryable is False
    assert error.terminal_result is not None
    assert error.terminal_result["membership"]["is_member"] is False
    assert error.terminal_result["membership"]["permissions"] == {}
    _assert_utc_timestamp(error.terminal_result["observed_at"])


@pytest.mark.asyncio
async def test_bot_membership_transient_and_malformed_responses_are_not_bot_not_member() -> None:
    def offline(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(offline)) as client:
        with pytest.raises(TelegramOperationError) as captured:
            await TelegramOperationsClient("bot-token", client).get_resource_bot_membership(
                {"resource": {"id": "-100"}}
            )
    assert captured.value.code == "telegram_transport_unknown"
    assert captured.value.retryable is True

    malformed = {"status": "administrator", "user": {"id": 77}, "can_delete_messages": "yes"}
    with pytest.raises(TelegramOperationError) as captured:
        await _membership(malformed)
    assert captured.value.code == "telegram_invalid_response"
