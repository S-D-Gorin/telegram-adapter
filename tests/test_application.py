import asyncio
import logging

import pytest

from app.application import Application
from app.config import Config
from app.sprotect import PairingState, TokenAlreadyIssuedError, TransientBootstrapError


class BootstrapStub:
    def __init__(self, statuses: list[str], *, token: str = "adapter-token") -> None:
        self.statuses = iter(statuses)
        self.token = token
        self.register_calls = 0
        self.status_calls = 0
        self.token_calls = 0
        self.closed = False

    async def register_adapter(self, adapter_id: str, pairing_secret: str) -> PairingState:
        self.register_calls += 1
        return PairingState("unpaired")

    async def get_pairing_status(self, adapter_id: str, pairing_secret: str) -> PairingState:
        self.status_calls += 1
        return PairingState(next(self.statuses))

    async def obtain_token(self, adapter_id: str, pairing_secret: str) -> str:
        self.token_calls += 1
        return self.token

    async def close(self) -> None:
        self.closed = True


class BlockingTelegramStub:
    async def get_updates(self, offset, *, timeout=30):
        await asyncio.Event().wait()

    async def close(self) -> None:
        pass


class UnusedEventsStub:
    async def deliver(self, event):
        raise AssertionError("no event should be delivered in this test")

    async def close(self) -> None:
        pass


class BlockingCommandsStub:
    async def connect(self):
        await asyncio.Event().wait()


async def wait_for(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


@pytest.mark.asyncio
async def test_unpaired_polling_then_successful_pairing_saves_token(tmp_path) -> None:
    client = BootstrapStub(["unpaired", "active"])
    application = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        telegram_client=BlockingTelegramStub(),  # type: ignore[arg-type]
        platform_events_client=UnusedEventsStub(),  # type: ignore[arg-type]
        platform_commands_client=BlockingCommandsStub(),  # type: ignore[arg-type]
        initial_retry_delay=0.01,
        max_retry_delay=0.02,
    )

    await application.start()
    await wait_for(lambda: client.token_calls == 1)
    identity, _ = await application.storage.get_or_create_identity()

    assert client.status_calls == 2
    assert identity.adapter_token == "adapter-token"
    assert identity.pairing_status == "active"
    await application.stop()


@pytest.mark.asyncio
async def test_restart_with_token_skips_bootstrap_pairing(tmp_path) -> None:
    first = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=BootstrapStub(["active"]),  # type: ignore[arg-type]
        telegram_client=BlockingTelegramStub(),  # type: ignore[arg-type]
        platform_events_client=UnusedEventsStub(),  # type: ignore[arg-type]
        platform_commands_client=BlockingCommandsStub(),  # type: ignore[arg-type]
        initial_retry_delay=0.01,
    )
    await first.start()
    await wait_for(lambda: first._pairing_task is not None and first._pairing_task.done())
    before, _ = await first.storage.get_or_create_identity()
    await first.stop()

    client = BootstrapStub(["unpaired"])
    restarted = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        telegram_client=BlockingTelegramStub(),  # type: ignore[arg-type]
        platform_events_client=UnusedEventsStub(),  # type: ignore[arg-type]
        platform_commands_client=BlockingCommandsStub(),  # type: ignore[arg-type]
    )
    await restarted.start()
    after, created = await restarted.storage.get_or_create_identity()

    assert not created
    assert after.adapter_id == before.adapter_id
    assert after.pairing_secret == before.pairing_secret
    assert client.register_calls == 0
    await restarted.stop()


@pytest.mark.asyncio
async def test_temporary_backend_failure_retries_and_shutdown_cancels_wait(tmp_path) -> None:
    class UnavailableClient(BootstrapStub):
        async def register_adapter(self, adapter_id: str, pairing_secret: str) -> PairingState:
            self.register_calls += 1
            raise TransientBootstrapError("offline")

    client = UnavailableClient([])
    application = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        initial_retry_delay=1,
    )
    await application.start()
    await wait_for(lambda: client.register_calls == 1)
    await application.stop()

    assert client.closed


@pytest.mark.asyncio
async def test_token_already_issued_is_not_retried_or_replaced(tmp_path) -> None:
    class OneTimeTokenClient(BootstrapStub):
        async def obtain_token(self, adapter_id: str, pairing_secret: str) -> str:
            self.token_calls += 1
            raise TokenAlreadyIssuedError("already issued")

    client = OneTimeTokenClient(["active"])
    application = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        initial_retry_delay=0.01,
    )
    await application.start()
    await wait_for(lambda: application._pairing_task is not None and application._pairing_task.done())
    identity, _ = await application.storage.get_or_create_identity()

    assert identity.pairing_status == "token_unrecoverable"
    assert identity.adapter_token is None
    assert client.token_calls == 1
    await application.stop()


@pytest.mark.asyncio
async def test_revoked_pairing_state_does_not_issue_a_token(tmp_path) -> None:
    client = BootstrapStub(["revoked"])
    application = Application(
        Config("https://api.sprotectbots.com", "secret", tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        initial_retry_delay=0.01,
        max_retry_delay=1,
    )
    await application.start()
    await wait_for(lambda: client.status_calls == 1)
    identity, _ = await application.storage.get_or_create_identity()

    assert identity.pairing_status == "revoked"
    assert client.token_calls == 0
    await application.stop()


@pytest.mark.asyncio
async def test_first_registration_key_and_secrets_are_not_written_after_startup(tmp_path, caplog) -> None:
    token = "telegram-secret"
    client = BootstrapStub(["unpaired"])
    application = Application(
        Config("https://api.sprotectbots.com", token, tmp_path, "INFO"),
        bootstrap_client=client,  # type: ignore[arg-type]
        initial_retry_delay=10,
    )
    with caplog.at_level(logging.INFO):
        await application.start()
        identity, _ = await application.storage.get_or_create_identity()
        await application.stop()

    assert token not in caplog.text
    # The deliberate bootstrap-only registration message is the sole exception.
    assert caplog.text.count(identity.pairing_secret) == 1
