import logging

import pytest

from app.application import Application
from app.config import Config


@pytest.mark.asyncio
async def test_startup_logging_does_not_expose_bot_token(tmp_path, caplog) -> None:
    token = "not-for-logs"
    application = Application(
        Config("https://api.sprotectbots.com", token, tmp_path, "INFO")
    )

    with caplog.at_level(logging.INFO):
        await application.start()
        await application.stop()

    assert token not in caplog.text
