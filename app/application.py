"""Application orchestration and pairing lifecycle."""

from __future__ import annotations

import asyncio
import logging

from app.config import Config
from app.sprotect import (
    PermanentBootstrapError,
    SprotectBootstrapClient,
    TokenAlreadyIssuedError,
    TransientBootstrapError,
)
from app.storage import AdapterIdentity, SQLiteStorage


class Application:
    """Owns durable identity and the background bootstrap pairing task."""

    def __init__(
        self,
        config: Config,
        *,
        bootstrap_client: SprotectBootstrapClient | None = None,
        initial_retry_delay: float = 2.0,
        max_retry_delay: float = 60.0,
    ) -> None:
        self.config = config
        self.storage = SQLiteStorage(config.database_path)
        self._client = bootstrap_client or SprotectBootstrapClient(config.server_api)
        self._initial_retry_delay = initial_retry_delay
        self._max_retry_delay = max_retry_delay
        self._pairing_task: asyncio.Task[None] | None = None
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        await self.storage.initialize()
        identity, created = await self.storage.get_or_create_identity()
        self._started = True
        self._logger.info("adapter started", extra=self.config.safe_details())
        if created:
            self._logger.warning("Adapter registration key: %s", identity.pairing_secret)
            self._logger.warning("Open Guardian and connect this adapter.")

        if identity.adapter_token:
            await self.storage.update_pairing_status("active")
            self._log_ready(identity)
            return
        if identity.pairing_status == "token_unrecoverable":
            self._logger.error(
                "adapter token cannot be recovered; restore the original /data volume or re-authorize this installation"
            )
            return
        self._pairing_task = asyncio.create_task(self._pairing_loop(), name="bootstrap-pairing")

    async def stop(self) -> None:
        if not self._started:
            return
        if self._pairing_task is not None:
            self._pairing_task.cancel()
            try:
                await self._pairing_task
            except asyncio.CancelledError:
                pass
            self._pairing_task = None
        await self._client.close()
        await self.storage.close()
        self._started = False
        self._logger.info("adapter stopped")

    async def _pairing_loop(self) -> None:
        delay = self._initial_retry_delay
        while True:
            identity, _ = await self.storage.get_or_create_identity()
            try:
                state = await self._client.register_adapter(identity.adapter_id, identity.pairing_secret)
                # Register responses describe current state, but status remains the source of truth.
                state = await self._client.get_pairing_status(identity.adapter_id, identity.pairing_secret)
                identity = await self.storage.update_pairing_status(state.status)
                if state.is_terminal_invalid:
                    self._logger.error("adapter pairing is %s; waiting for backend state change", state.status)
                    await asyncio.sleep(self._max_retry_delay)
                    continue
                if not state.is_active:
                    self._logger.info("adapter awaiting Guardian pairing adapter_id=%s", identity.adapter_id)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
                    continue

                token = await self._client.obtain_token(identity.adapter_id, identity.pairing_secret)
                identity = await self.storage.activate_identity(token)
                self._log_ready(identity)
                return
            except TokenAlreadyIssuedError:
                await self.storage.update_pairing_status("token_unrecoverable")
                self._logger.error(
                    "adapter token was already issued but is absent locally; it cannot be recovered"
                )
                return
            except TransientBootstrapError as error:
                self._logger.warning("Sprotect bootstrap temporarily unavailable; retrying in %.0fs: %s", delay, error)
            except PermanentBootstrapError as error:
                self._logger.error("Sprotect bootstrap rejected adapter registration: %s", error)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_retry_delay)

    def _log_ready(self, identity: AdapterIdentity) -> None:
        self._logger.info("adapter ready platform=telegram adapter_id=%s", identity.adapter_id)
