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
