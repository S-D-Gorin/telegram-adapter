"""Partitioning of raw Telegram updates for per-resource FIFO delivery."""

from __future__ import annotations

from typing import Any

# Updates that carry no usable chat are still forwarded raw (Guardian ignores
# unsupported updates); they share one FIFO partition so they never block chats.
UNROUTED_PARTITION = "unrouted"


def partition_key_for_update(update: dict[str, Any]) -> str:
    chat_id = _update_chat_id(update)
    return str(chat_id) if chat_id is not None else UNROUTED_PARTITION


def migration_target_partition(update: dict[str, Any]) -> str | None:
    """Partition of the supergroup E for a group A service message ``migrate_to_chat_id``."""
    message = update.get("message")
    if not isinstance(message, dict):
        return None
    target = message.get("migrate_to_chat_id")
    return str(target) if type(target) is int else None


def _update_chat_id(update: dict[str, Any]) -> int | None:
    # A Telegram Update has exactly one optional payload field besides update_id.
    for key, value in update.items():
        if key == "update_id" or not isinstance(value, dict):
            continue
        chat = value.get("chat")
        if not isinstance(chat, dict):
            # callback_query carries its chat on the originating message.
            message = value.get("message")
            chat = message.get("chat") if isinstance(message, dict) else None
        if isinstance(chat, dict) and type(chat.get("id")) is int:
            return chat["id"]
        return None
    return None
