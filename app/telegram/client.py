"""Minimal Telegram Bot API client for inbound long polling."""

from __future__ import annotations

from typing import Any

import httpx

# Telegram Bot API accepts getUpdates limit values from 1 to 100.
MAX_UPDATES_PER_REQUEST = 100

# Exactly the update types Guardian ingests (its SUPPORTED_TYPES); all carry a chat.
# Telegram persists allowed_updates per bot, so it is sent on every getUpdates call
# to stay independent of whatever an earlier client of this bot configured.
ALLOWED_UPDATES = (
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "chat_member",
    "my_chat_member",
)


class TelegramApiError(Exception):
    """A Telegram API failure. Exception messages deliberately omit bot tokens and payloads."""


class TransientTelegramError(TelegramApiError):
    pass


class TelegramPollingConflictError(TelegramApiError):
    pass


class PermanentTelegramError(TelegramApiError):
    pass


class TelegramBotClient:
    def __init__(self, bot_token: str, client: httpx.AsyncClient | None = None) -> None:
        self._url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(35.0))
        self._owns_client = client is None

    async def get_updates(
        self, offset: int | None, *, timeout: int = 30, limit: int = MAX_UPDATES_PER_REQUEST
    ) -> list[dict[str, Any]]:
        request_body: dict[str, object] = {
            "timeout": timeout,
            "limit": max(1, min(limit, MAX_UPDATES_PER_REQUEST)),
            "allowed_updates": list(ALLOWED_UPDATES),
        }
        if offset is not None:
            request_body["offset"] = offset
        try:
            response = await self._client.post(self._url, json=request_body)
        except httpx.RequestError as error:
            raise TransientTelegramError("unable to reach Telegram Bot API") from error
        if response.status_code == 409:
            raise TelegramPollingConflictError("another Telegram polling instance is active")
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientTelegramError(f"Telegram Bot API returned {response.status_code}")
        if response.status_code >= 400:
            raise PermanentTelegramError(f"Telegram Bot API returned {response.status_code}")
        try:
            payload = response.json()
        except ValueError as error:
            raise TransientTelegramError("Telegram Bot API returned invalid JSON") from error
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            raise PermanentTelegramError("Telegram Bot API rejected getUpdates")
        result = payload.get("result")
        if not isinstance(result, list) or not all(isinstance(update, dict) for update in result):
            raise PermanentTelegramError("Telegram Bot API returned invalid update list")
        return result

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
