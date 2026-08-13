import pytest

from app.application import Application
from app.config import Config


@pytest.mark.asyncio
async def test_application_starts_and_stops_without_external_connections(tmp_path) -> None:
    application = Application(
        Config(
            server_api="https://api.sprotectbots.com",
            bot_token="secret",
            data_dir=tmp_path,
            log_level="INFO",
        )
    )

    await application.start()
    assert application.storage.database_path.is_file()
    await application.stop()
