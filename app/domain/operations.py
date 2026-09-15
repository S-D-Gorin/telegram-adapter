"""Platform-neutral operation traits used by the durable executor."""

from __future__ import annotations


# These operations only read remote state. They can therefore be safely retried
# after an ambiguous transport failure and reclaimed after an adapter restart.
READ_ONLY_OPERATION_TYPES = frozenset(
    {
        "get_chat_member",
        "get_resource_administrators",
        "get_resource_member_count",
        "get_resource_info",
        "get_resource_bot_membership",
    }
)
