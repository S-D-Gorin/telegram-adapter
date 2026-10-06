import pytest

from app.telegram.partitioning import UNROUTED_PARTITION, migration_target_partition, partition_key_for_update

CHAT = {"id": -1001234, "type": "supergroup"}


@pytest.mark.parametrize(
    "field",
    [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "my_chat_member",
        "chat_member",
        "chat_join_request",
        "message_reaction",
        "message_reaction_count",
    ],
)
def test_chat_updates_are_partitioned_by_chat_id(field: str) -> None:
    assert partition_key_for_update({"update_id": 1, field: {"chat": CHAT}}) == "-1001234"


def test_callback_query_uses_the_chat_of_its_message() -> None:
    update = {"update_id": 1, "callback_query": {"id": "q", "message": {"chat": CHAT}}}
    assert partition_key_for_update(update) == "-1001234"


@pytest.mark.parametrize(
    "update",
    [
        {"update_id": 1, "poll": {"id": "poll"}},
        {"update_id": 1, "inline_query": {"id": "q", "from": {"id": 5}}},
        {"update_id": 1, "callback_query": {"id": "q", "inline_message_id": "m"}},
        {"update_id": 1, "message": {"chat": {"id": "not-an-int"}}},
        {"update_id": 1},
    ],
)
def test_updates_without_usable_chat_are_unrouted(update: dict) -> None:
    assert partition_key_for_update(update) == UNROUTED_PARTITION


def test_group_migration_points_to_the_supergroup_partition() -> None:
    migration = {"update_id": 1, "message": {"chat": {"id": -5}, "migrate_to_chat_id": -1009}}
    assert partition_key_for_update(migration) == "-5"
    assert migration_target_partition(migration) == "-1009"
    assert migration_target_partition({"update_id": 2, "message": {"chat": {"id": -1009}, "migrate_from_chat_id": -5}}) is None
