"""Telegram Bot API execution for the supported platform operations."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx


class TelegramOperationError(Exception):
    def __init__(
        self,
        code: str,
        *,
        description: str = "Telegram operation failed",
        retryable: bool = False,
        ambiguous: bool = False,
        terminal_result: dict[str, object] | None = None,
    ) -> None:
        self.code = code
        self.description = description[:255]
        self.retryable = retryable
        self.ambiguous = ambiguous
        # Some terminal states are themselves authoritative snapshots.  Keeping
        # that snapshot on the error lets the common result pipeline persist a
        # failed result without introducing a second delivery path.
        self.terminal_result = terminal_result
        super().__init__(code)


class TelegramOperationsClient:
    def __init__(self, bot_token: str, client: httpx.AsyncClient | None = None) -> None:
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None
        self._bot_user: dict[str, object] | None = None
        self._bot_user_lock = asyncio.Lock()

    async def send_message(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _required_int(payload, "chat_id")
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 4096:
            raise TelegramOperationError("invalid_operation_payload")
        request: dict[str, object] = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        reply_to = payload.get("reply_to_message_id")
        if reply_to is not None:
            request["reply_parameters"] = {"message_id": _as_int(reply_to)}
        try:
            result = await self._call("sendMessage", request)
        except TelegramOperationError as error:
            if error.code != "reply_target_missing" or reply_to is None:
                raise
            result = await self._call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
        if not isinstance(result, dict):
            raise TelegramOperationError("telegram_invalid_response", ambiguous=True)
        message_id = result.get("message_id")
        chat = result.get("chat")
        if not isinstance(message_id, int) or not isinstance(chat, dict) or not isinstance(chat.get("id"), int):
            raise TelegramOperationError("telegram_invalid_response", ambiguous=True)
        return {"telegram_chat_id": str(chat["id"]), "telegram_message_id": str(message_id)}

    async def delete_message(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _required_int(payload, "chat_id")
        message_id = _required_int(payload, "message_id")
        try:
            await self._call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        except TelegramOperationError as error:
            if error.code != "already_absent":
                raise
        return {"telegram_chat_id": str(chat_id), "telegram_message_id": str(message_id), "deleted": True}

    async def get_chat_member(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _required_int(payload, "chat_id")
        user_id = _required_int(payload, "user_id")
        result = await self._call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
        if not isinstance(result, dict) or result.get("status") not in {
            "creator",
            "administrator",
            "member",
            "restricted",
            "left",
            "kicked",
        }:
            raise TelegramOperationError(
                "telegram_invalid_response",
                description="Telegram returned an invalid chat member",
                retryable=True,
                ambiguous=True,
            )
        user = result.get("user")
        if not isinstance(user, dict) or user.get("id") != user_id:
            raise TelegramOperationError(
                "telegram_invalid_response",
                description="Telegram returned a mismatched chat member",
                retryable=True,
                ambiguous=True,
            )
        return {"chat_member": result}

    async def get_resource_administrators(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _resource_id(payload)
        result = await self._call("getChatAdministrators", {"chat_id": chat_id, "return_bots": True})
        observed_at = _observed_at()
        if not isinstance(result, list):
            raise TelegramOperationError("telegram_invalid_response")
        administrators = [_normalize_administrator(member) for member in result]
        return {
            "resource": {"id": str(chat_id)},
            "administrators": administrators,
            "observed_at": observed_at,
            "raw_data": {"telegram": {"method": "getChatAdministrators", "data": result}},
        }

    async def get_resource_member_count(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _resource_id(payload)
        result = await self._call("getChatMemberCount", {"chat_id": chat_id})
        observed_at = _observed_at()
        if type(result) is not int or result < 0:
            raise TelegramOperationError("telegram_invalid_response")
        return {
            "resource": {"id": str(chat_id)},
            "member_count": result,
            "observed_at": observed_at,
        }

    async def get_resource_info(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _resource_id(payload)
        result = await self._call("getChat", {"chat_id": chat_id})
        observed_at = _observed_at()
        if not isinstance(result, dict):
            raise TelegramOperationError("telegram_invalid_response")
        resource = _normalize_resource(result)
        return {
            "resource": resource,
            "observed_at": observed_at,
            "raw_data": {"telegram": {"method": "getChat", "data": result}},
        }

    async def get_resource_bot_membership(self, payload: dict[str, object]) -> dict[str, object]:
        chat_id = _resource_id(payload)
        bot, raw_bot = await self._get_bot_user()
        bot_id = bot["id"]
        assert isinstance(bot_id, str)
        result = await self._call("getChatMember", {"chat_id": chat_id, "user_id": int(bot_id)})
        observed_at = _observed_at()
        membership = _normalize_membership(result, expected_user_id=int(bot_id))
        snapshot = {
            "resource": {"id": str(chat_id)},
            "bot": bot,
            "membership": membership,
            "observed_at": observed_at,
            "raw_data": {
                "telegram": {
                    "get_me": raw_bot,
                    "chat_member": result,
                }
            },
        }
        if _is_authoritative_non_member(membership):
            raise TelegramOperationError(
                "bot_not_member",
                description="Telegram confirmed that the bot is not a member of the resource",
                terminal_result=snapshot,
            )
        return snapshot

    async def _get_bot_user(self) -> tuple[dict[str, object], dict[str, object]]:
        async with self._bot_user_lock:
            if self._bot_user is None:
                result = await self._call("getMe", {})
                if not isinstance(result, dict):
                    raise TelegramOperationError("telegram_invalid_response")
                _normalize_user(result, require_bot=True)
                self._bot_user = result
            raw_bot = self._bot_user
        return _normalize_user(raw_bot, require_bot=True), raw_bot

    async def _call(self, method: str, payload: dict[str, object]) -> object:
        try:
            response = await self._client.post(f"{self._base_url}/{method}", json=payload)
        except httpx.RequestError as error:
            raise TelegramOperationError(
                "telegram_transport_unknown",
                description="Telegram API transport failed",
                retryable=True,
                ambiguous=True,
            ) from error
        try:
            body = response.json()
        except ValueError as error:
            raise TelegramOperationError(
                "telegram_invalid_response",
                description="Telegram API returned invalid JSON",
                retryable=True,
                ambiguous=True,
            ) from error
        if response.is_success and isinstance(body, dict) and body.get("ok") is True:
            return body.get("result")
        description = str(body.get("description", "")).lower() if isinstance(body, dict) else ""
        if response.status_code == 429:
            raise TelegramOperationError(
                "rate_limited", description=description or "Telegram rate limit", retryable=True
            )
        if response.status_code >= 500:
            raise TelegramOperationError(
                "telegram_server_unknown",
                description=description or "Telegram server error",
                retryable=True,
                ambiguous=True,
            )
        if method == "deleteMessage" and "message to delete not found" in description:
            raise TelegramOperationError("already_absent")
        if method == "sendMessage" and "message to be replied not found" in description:
            raise TelegramOperationError("reply_target_missing")
        if method == "getChatMember":
            if "user not found" in description or "participant_id_invalid" in description:
                raise TelegramOperationError("membership_not_found", description=description)
            if (
                "chat not found" in description
                or "chat was deleted" in description
                or "group chat was deleted" in description
            ):
                raise TelegramOperationError("chat_not_found", description=description)
        if (
            "chat not found" in description
            or "chat was deleted" in description
            or "group chat was deleted" in description
        ):
            raise TelegramOperationError("chat_not_found", description=description)
        if (
            "not enough rights" in description
            or "not authorized" in description
            or "have no rights" in description
            or "forbidden" in description
        ):
            raise TelegramOperationError("permission_denied", description=description)
        if response.status_code == 400:
            raise TelegramOperationError("invalid_request", description=description)
        raise TelegramOperationError("telegram_api_error", description=description)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        raise TelegramOperationError("invalid_operation_payload")
    try:
        return int(value)  # Existing platform command payloads may contain numeric strings.
    except (TypeError, ValueError) as error:
        raise TelegramOperationError("invalid_operation_payload") from error


def _required_int(payload: dict[str, object], field: str) -> int:
    if field not in payload:
        raise TelegramOperationError("invalid_operation_payload")
    return _as_int(payload[field])


def _resource_id(payload: dict[str, object]) -> str:
    resource = payload.get("resource")
    if not isinstance(resource, dict):
        raise TelegramOperationError("invalid_operation_payload")
    resource_id = resource.get("id")
    if not isinstance(resource_id, str):
        raise TelegramOperationError("invalid_operation_payload")
    if not resource_id.strip():
        raise TelegramOperationError("invalid_operation_payload")
    return resource_id


def _normalize_user(raw_user: object, *, require_bot: bool = False) -> dict[str, object]:
    if not isinstance(raw_user, dict):
        raise TelegramOperationError("telegram_invalid_response")
    user_id = raw_user.get("id")
    is_bot = raw_user.get("is_bot")
    first_name = raw_user.get("first_name")
    if type(user_id) is not int or type(is_bot) is not bool or not isinstance(first_name, str):
        raise TelegramOperationError("telegram_invalid_response")
    if require_bot and not is_bot:
        raise TelegramOperationError("telegram_invalid_response")
    user: dict[str, object] = {"id": str(user_id), "is_bot": is_bot, "first_name": first_name}
    for field in ("last_name", "username"):
        value = raw_user.get(field)
        if value is not None:
            if not isinstance(value, str):
                raise TelegramOperationError("telegram_invalid_response")
            user[field] = value
    return user


def _normalize_administrator(raw_member: object) -> dict[str, object]:
    if not isinstance(raw_member, dict):
        raise TelegramOperationError("telegram_invalid_response")
    status = raw_member.get("status")
    is_anonymous = raw_member.get("is_anonymous")
    if status not in {"creator", "administrator"} or type(is_anonymous) is not bool:
        raise TelegramOperationError("telegram_invalid_response")
    administrator: dict[str, object] = {
        "user": _normalize_user(raw_member.get("user")),
        "role": status,
        "is_anonymous": is_anonymous,
        "raw_data": {"telegram": {"chat_member": raw_member}},
    }
    custom_title = raw_member.get("custom_title")
    if custom_title is not None:
        if not isinstance(custom_title, str):
            raise TelegramOperationError("telegram_invalid_response")
        administrator["custom_title"] = custom_title
    return administrator


def _normalize_resource(raw_chat: dict[str, object]) -> dict[str, object]:
    chat_id = raw_chat.get("id")
    kind = raw_chat.get("type")
    if type(chat_id) is not int or not isinstance(kind, str):
        raise TelegramOperationError("telegram_invalid_response")
    if kind == "private":
        raise TelegramOperationError(
            "unsupported_resource_type",
            description="Private chats are not supported Resource State Sync resources",
        )
    if kind not in {"group", "supergroup", "channel"}:
        raise TelegramOperationError("telegram_invalid_response")
    title = raw_chat.get("title")
    if not isinstance(title, str):
        raise TelegramOperationError("telegram_invalid_response")
    username = raw_chat.get("username")
    if username is not None and not isinstance(username, str):
        raise TelegramOperationError("telegram_invalid_response")
    return {"id": str(chat_id), "title": title, "username": username, "type": kind}


def _normalize_membership(raw_member: object, *, expected_user_id: int) -> dict[str, object]:
    if not isinstance(raw_member, dict):
        raise TelegramOperationError("telegram_invalid_response")
    status = raw_member.get("status")
    if status not in {"creator", "administrator", "member", "restricted", "left", "kicked"}:
        raise TelegramOperationError("telegram_invalid_response")
    raw_user = raw_member.get("user")
    if not isinstance(raw_user, dict) or raw_user.get("id") != expected_user_id:
        raise TelegramOperationError("telegram_invalid_response")
    is_member = status in {"creator", "administrator", "member"}
    if status == "restricted":
        restricted_member = raw_member.get("is_member")
        if type(restricted_member) is not bool:
            raise TelegramOperationError("telegram_invalid_response")
        is_member = restricted_member
    permissions: dict[str, bool] = {}
    if status == "administrator":
        permissions = {
            "delete_messages": _permission(raw_member, "can_delete_messages"),
            "post_messages": _permission(raw_member, "can_post_messages"),
        }
    membership: dict[str, object] = {
        "status": status,
        "role": status,
        "is_member": is_member,
        "permissions": permissions,
    }
    return membership


def _permission(raw_member: dict[str, object], field: str) -> bool:
    value = raw_member.get(field, False)
    if type(value) is not bool:
        raise TelegramOperationError("telegram_invalid_response")
    return value


def _is_authoritative_non_member(membership: dict[str, object]) -> bool:
    return membership["status"] in {"left", "kicked"} or membership["is_member"] is False


def _observed_at() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
