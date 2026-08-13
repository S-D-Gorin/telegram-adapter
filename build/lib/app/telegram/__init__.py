"""Telegram Bot API integration boundary."""

from .client import (
    PermanentTelegramError,
    TelegramBotClient,
    TelegramPollingConflictError,
    TransientTelegramError,
)
from .operations import TelegramOperationError, TelegramOperationsClient

__all__ = [
    "PermanentTelegramError",
    "TelegramBotClient",
    "TelegramPollingConflictError",
    "TransientTelegramError",
    "TelegramOperationError",
    "TelegramOperationsClient",
]
