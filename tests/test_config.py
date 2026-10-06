from pathlib import Path

import pytest

from app.config import ConfigError, load_config


def test_loads_config_and_keeps_token_out_of_representation() -> None:
    config = load_config(
        {
            "SERVER_API": "https://api.sprotectbots.com/",
            "BOT_TOKEN": "very-secret-token",
            "DATA_DIR": "/var/lib/adapter",
            "LOG_LEVEL": "debug",
        }
    )

    assert config.server_api == "https://api.sprotectbots.com"
    assert config.database_path == Path("/var/lib/adapter/adapter.db")
    assert config.log_level == "DEBUG"
    assert "very-secret-token" not in repr(config)
    assert "very-secret-token" not in str(config.safe_details())


@pytest.mark.parametrize("missing", ["SERVER_API", "BOT_TOKEN"])
def test_requires_server_api_and_bot_token(missing: str) -> None:
    environment = {
        "SERVER_API": "https://api.sprotectbots.com",
        "BOT_TOKEN": "token",
    }
    environment.pop(missing)

    with pytest.raises(ConfigError, match=f"{missing} is required"):
        load_config(environment)


def test_event_delivery_settings_have_safe_defaults() -> None:
    config = load_config({"SERVER_API": "https://api.sprotectbots.com", "BOT_TOKEN": "token"})

    assert config.event_delivery_concurrency == 8
    assert config.event_max_attempts == 5
    assert config.event_pending_ttl_seconds == 172800
    assert config.event_retention_seconds == 172800
    assert config.event_max_pending == 10000


def test_event_delivery_settings_are_configurable() -> None:
    config = load_config(
        {
            "SERVER_API": "https://api.sprotectbots.com",
            "BOT_TOKEN": "token",
            "EVENT_DELIVERY_CONCURRENCY": "16",
            "EVENT_MAX_ATTEMPTS": "3",
            "EVENT_PENDING_TTL_SECONDS": "3600",
            "EVENT_RETENTION_SECONDS": "0",
            "EVENT_MAX_PENDING": "500",
        }
    )

    assert config.event_delivery_concurrency == 16
    assert config.event_max_attempts == 3
    assert config.event_pending_ttl_seconds == 3600
    assert config.event_retention_seconds == 0
    assert config.event_max_pending == 500
    assert config.safe_details()["event_delivery_concurrency"] == "16"


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("EVENT_DELIVERY_CONCURRENCY", "0", "between 1 and 256"),
        ("EVENT_DELIVERY_CONCURRENCY", "1000", "between 1 and 256"),
        ("EVENT_MAX_ATTEMPTS", "0", ">= 1"),
        ("EVENT_PENDING_TTL_SECONDS", "10", ">= 60"),
        ("EVENT_MAX_PENDING", "many", "must be an integer"),
    ],
)
def test_rejects_invalid_event_delivery_settings(name: str, value: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        load_config({"SERVER_API": "https://api.sprotectbots.com", "BOT_TOKEN": "token", name: value})
