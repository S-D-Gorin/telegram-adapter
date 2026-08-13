"""Telegram Bot API integration boundary."""

from .client import (
    PermanentTelegramError,
    TelegramBotClient,
    TelegramPollingConflictError,
    TransientTelegramError,
)

__all__ = [
    "PermanentTelegramError",
    "TelegramBotClient",
    "TelegramPollingConflictError",
    "TransientTelegramError",
]
