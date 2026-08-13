"""Telegram Bot API execution for the supported platform operations."""

from __future__ import annotations

from typing import Any

import httpx


class TelegramOperationError(Exception):
    def __init__(self, code: str, *, retryable: bool = False, ambiguous: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        self.ambiguous = ambiguous
        super().__init__(code)


class TelegramOperationsClient:
    def __init__(self, bot_token: str, client: httpx.AsyncClient | None = None) -> None:
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None

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

    async def _call(self, method: str, payload: dict[str, object]) -> object:
        try:
            response = await self._client.post(f"{self._base_url}/{method}", json=payload)
        except httpx.RequestError as error:
            raise TelegramOperationError("telegram_transport_unknown", ambiguous=True) from error
        try:
            body = response.json()
        except ValueError as error:
            raise TelegramOperationError("telegram_invalid_response", ambiguous=True) from error
        if response.is_success and isinstance(body, dict) and body.get("ok") is True:
            return body.get("result")
        description = str(body.get("description", "")).lower() if isinstance(body, dict) else ""
        if response.status_code == 429:
            raise TelegramOperationError("rate_limited", retryable=True)
        if response.status_code >= 500:
            raise TelegramOperationError("telegram_server_unknown", ambiguous=True)
        if method == "deleteMessage" and "message to delete not found" in description:
            raise TelegramOperationError("already_absent")
        if method == "sendMessage" and "message to be replied not found" in description:
            raise TelegramOperationError("reply_target_missing")
        raise TelegramOperationError("telegram_api_error")

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
