"""Application orchestration and lifecycle."""

from __future__ import annotations

import logging

from app.config import Config
from app.storage import SQLiteStorage


class Application:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.storage = SQLiteStorage(config.database_path)
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        await self.storage.initialize()
        self._started = True
        self._logger.info("adapter started", extra=self.config.safe_details())

    async def stop(self) -> None:
        if not self._started:
            return
        await self.storage.close()
        self._started = False
        self._logger.info("adapter stopped")
