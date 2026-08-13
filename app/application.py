"""Application orchestration and pairing lifecycle."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import Config
from app.logging import register_secrets
from app.sprotect import (
    PermanentBootstrapError,
    SprotectBootstrapClient,
    SprotectPlatformEventsClient,
    TokenAlreadyIssuedError,
    TransientBootstrapError,
    TransientPlatformEventError,
    PermanentPlatformEventError,
    PlatformAuthenticationError,
    CommandAuthenticationError,
    CommandTransportError,
    PlatformCommandsWebSocketClient,
    is_authentication_close,
    PermanentResultDeliveryError,
    SprotectPlatformResultsClient,
    TransientResultDeliveryError,
)
from app.storage import AdapterIdentity, PendingPlatformEvent, PendingResult, PlatformOperation, SQLiteStorage
from app.telegram import (
    PermanentTelegramError,
    TelegramBotClient,
    TelegramPollingConflictError,
    TransientTelegramError,
    TelegramOperationError,
    TelegramOperationsClient,
)
from websockets.exceptions import ConnectionClosed


class Application:
    """Owns durable identity and the background bootstrap pairing task."""

    def __init__(
        self,
        config: Config,
        *,
        bootstrap_client: SprotectBootstrapClient | None = None,
        telegram_client: TelegramBotClient | None = None,
        platform_events_client: SprotectPlatformEventsClient | None = None,
        platform_commands_client: PlatformCommandsWebSocketClient | None = None,
        telegram_operations_client: TelegramOperationsClient | None = None,
        platform_results_client: SprotectPlatformResultsClient | None = None,
        initial_retry_delay: float = 2.0,
        max_retry_delay: float = 60.0,
    ) -> None:
        self.config = config
        self.storage = SQLiteStorage(config.database_path)
        self._client = bootstrap_client or SprotectBootstrapClient(config.server_api)
        self._telegram_client = telegram_client
        self._platform_events_client = platform_events_client
        self._platform_commands_client = platform_commands_client
        self._telegram_operations_client = telegram_operations_client
        self._platform_results_client = platform_results_client
        self._initial_retry_delay = initial_retry_delay
        self._max_retry_delay = max_retry_delay
        self._pairing_task: asyncio.Task[None] | None = None
        self._polling_task: asyncio.Task[None] | None = None
        self._commands_task: asyncio.Task[None] | None = None
        self._executor_task: asyncio.Task[None] | None = None
        self._result_delivery_task: asyncio.Task[None] | None = None
        self._transport_states = {
            "bootstrap_ready": False,
            "telegram_polling_ready": False,
            "platform_events_ready": False,
            "platform_commands_ready": False,
        }
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        await self.storage.initialize()
        unknown_count = await self.storage.mark_executing_operations_unknown()
        if unknown_count:
            self._logger.error(
                "platform_operation_execution_unknown count=%s; automatic Telegram replay is disabled", unknown_count
            )
        identity, created = await self.storage.get_or_create_identity()
        register_secrets((identity.pairing_secret, identity.adapter_token, self.config.bot_token))
        self._started = True
        self._logger.info("adapter started", extra=self.config.safe_details())
        if created:
            self._logger.warning("Adapter registration key: %s", identity.pairing_secret)
            self._logger.warning("Open Guardian and connect this adapter.")

        if identity.adapter_token:
            await self.storage.update_pairing_status("active")
            self._log_ready(identity)
            self._start_polling(identity)
            self._start_commands(identity)
            self._start_execution(identity)
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
        for task_name in (
            "_result_delivery_task",
            "_executor_task",
            "_commands_task",
            "_polling_task",
            "_pairing_task",
        ):
            task = getattr(self, task_name)
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            setattr(self, task_name, None)
        if self._telegram_client is not None:
            await self._telegram_client.close()
        if self._platform_events_client is not None:
            await self._platform_events_client.close()
        if self._telegram_operations_client is not None:
            await self._telegram_operations_client.close()
        if self._platform_results_client is not None:
            await self._platform_results_client.close()
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
                self._start_polling(identity)
                self._start_commands(identity)
                self._start_execution(identity)
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
        self._set_transport_state("bootstrap_ready", True)

    @property
    def readiness(self) -> dict[str, bool]:
        """Safe operational snapshot; no secrets or payloads."""
        return dict(self._transport_states)

    def _set_transport_state(self, name: str, value: bool) -> None:
        if self._transport_states[name] == value:
            return
        self._transport_states[name] = value
        self._logger.info(
            "adapter_transport_state bootstrap_ready=%s telegram_polling_ready=%s "
            "platform_events_ready=%s platform_commands_ready=%s",
            self._transport_states["bootstrap_ready"],
            self._transport_states["telegram_polling_ready"],
            self._transport_states["platform_events_ready"],
            self._transport_states["platform_commands_ready"],
        )

    def _start_polling(self, identity: AdapterIdentity) -> None:
        if self._polling_task is not None or identity.adapter_token is None:
            return
        self._telegram_client = self._telegram_client or TelegramBotClient(self.config.bot_token)
        self._platform_events_client = self._platform_events_client or SprotectPlatformEventsClient(
            self.config.server_api, identity.adapter_token
        )
        self._polling_task = asyncio.create_task(self._polling_loop(identity.adapter_id), name="telegram-polling")

    def _start_commands(self, identity: AdapterIdentity) -> None:
        if self._commands_task is not None or identity.adapter_token is None:
            return
        self._platform_commands_client = self._platform_commands_client or PlatformCommandsWebSocketClient(
            self.config.server_api, identity.adapter_token
        )
        self._commands_task = asyncio.create_task(
            self._commands_loop(identity.adapter_id), name="platform-command-websocket"
        )

    def _start_execution(self, identity: AdapterIdentity) -> None:
        if identity.adapter_token is None:
            return
        self._telegram_operations_client = self._telegram_operations_client or TelegramOperationsClient(
            self.config.bot_token
        )
        self._platform_results_client = self._platform_results_client or SprotectPlatformResultsClient(
            self.config.server_api, identity.adapter_token
        )
        if self._executor_task is None:
            self._executor_task = asyncio.create_task(self._executor_loop(), name="platform-operation-executor")
        if self._result_delivery_task is None:
            self._result_delivery_task = asyncio.create_task(
                self._result_delivery_loop(), name="platform-result-delivery"
            )

    async def _polling_loop(self, adapter_id: str) -> None:
        assert self._telegram_client is not None
        assert self._platform_events_client is not None
        delay = self._initial_retry_delay
        self._logger.info("telegram_polling_started adapter_id=%s", adapter_id)
        while True:
            pending_events = await self.storage.list_pending_platform_events()
            if pending_events:
                if not await self._deliver_pending_event(pending_events[0], delay):
                    delay = min(delay * 2, self._max_retry_delay)
                    continue
                delay = self._initial_retry_delay
                continue
            offset = await self.storage.get_telegram_offset()
            try:
                updates = await self._telegram_client.get_updates(offset)
            except TelegramPollingConflictError:
                self._set_transport_state("telegram_polling_ready", False)
                self._logger.error("telegram_polling_conflict another Telegram polling instance is active")
                await asyncio.sleep(self._max_retry_delay)
                continue
            except TransientTelegramError as error:
                self._set_transport_state("telegram_polling_ready", False)
                self._logger.warning("telegram polling temporary failure; retrying in %.0fs: %s", delay, error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_retry_delay)
                continue
            except PermanentTelegramError as error:
                self._set_transport_state("telegram_polling_ready", False)
                self._logger.error("telegram polling degraded: %s", error)
                await asyncio.sleep(self._max_retry_delay)
                continue

            delay = self._initial_retry_delay
            self._set_transport_state("telegram_polling_ready", True)
            delivered_all = True
            for update in sorted(updates, key=lambda item: item.get("update_id", -1)):
                update_id = update.get("update_id")
                if not isinstance(update_id, int):
                    self._logger.error("telegram polling received malformed update without integer update_id")
                    delivered_all = False
                    break
                if offset is not None and update_id < offset:
                    continue
                event_id = f"telegram:{update_id}"
                event = {
                    "schema_version": 1,
                    "event_id": event_id,
                    "platform": "telegram",
                    "event_type": "update",
                    "occurred_at": _occurred_at(),
                    "payload": update,
                }
                pending_event = await self.storage.store_platform_event(event_id, update_id, event)
                self._logger.info("telegram_update_received telegram_update_id=%s event_id=%s", update_id, event_id)
                if not await self._deliver_pending_event(pending_event, delay):
                    delay = min(delay * 2, self._max_retry_delay)
                    delivered_all = False
                    break
            if not delivered_all:
                continue

    async def _deliver_pending_event(self, pending_event: PendingPlatformEvent, delay: float) -> bool:
        assert self._platform_events_client is not None
        try:
            outcome = await self._platform_events_client.deliver(pending_event.envelope)
        except TransientPlatformEventError as error:
            self._set_transport_state("platform_events_ready", False)
            self._logger.warning(
                "platform_event_retry telegram_update_id=%s event_id=%s retry_in=%.0fs: %s",
                pending_event.telegram_update_id,
                pending_event.event_id,
                delay,
                error,
            )
            await asyncio.sleep(delay)
            return False
        except PlatformAuthenticationError as error:
            self._set_transport_state("platform_events_ready", False)
            self._logger.error("platform events authentication degraded: %s", error)
            await asyncio.sleep(self._max_retry_delay)
            return False
        except PermanentPlatformEventError as error:
            self._set_transport_state("platform_events_ready", False)
            self._logger.error(
                "platform event permanently rejected; retaining telegram_update_id=%s: %s",
                pending_event.telegram_update_id,
                error,
            )
            await asyncio.sleep(self._max_retry_delay)
            return False
        await self.storage.mark_event_delivered_and_advance_offset(
            pending_event.event_id, pending_event.telegram_update_id + 1
        )
        self._set_transport_state("platform_events_ready", True)
        if outcome == "accepted":
            self._logger.info(
                "platform_event_delivered telegram_update_id=%s event_id=%s",
                pending_event.telegram_update_id,
                pending_event.event_id,
            )
        else:
            self._logger.info(
                "platform_event_duplicate telegram_update_id=%s event_id=%s",
                pending_event.telegram_update_id,
                pending_event.event_id,
            )
        return True

    async def _commands_loop(self, adapter_id: str) -> None:
        assert self._platform_commands_client is not None
        delay = self._initial_retry_delay
        while True:
            connection = None
            try:
                connection = await self._platform_commands_client.connect()
                self._logger.info("platform_commands_connected adapter_id=%s", adapter_id)
                self._set_transport_state("platform_commands_ready", True)
                delay = self._initial_retry_delay
                while True:
                    await self._handle_command_frame(connection, await connection.recv())
            except CommandAuthenticationError as error:
                self._set_transport_state("platform_commands_ready", False)
                self._logger.error("platform commands authentication degraded: %s", error)
                await asyncio.sleep(self._max_retry_delay)
            except ConnectionClosed as error:
                self._set_transport_state("platform_commands_ready", False)
                if is_authentication_close(error):
                    self._logger.error("platform commands authentication degraded: WebSocket closed with 4401")
                    await asyncio.sleep(self._max_retry_delay)
                else:
                    self._logger.warning("platform commands disconnected; retrying in %.0fs", delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
            except CommandTransportError as error:
                self._set_transport_state("platform_commands_ready", False)
                self._logger.warning(
                    "platform_command_websocket_handshake_failed url=%s http_status=%s retry_in_seconds=%.0f",
                    error.url or "unavailable",
                    error.http_status if error.http_status is not None else "unavailable",
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_retry_delay)
            finally:
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception:  # Connection teardown cannot compromise other adapter loops.
                        pass

    async def _handle_command_frame(self, connection: Any, raw_frame: str | bytes) -> None:
        try:
            frame = json.loads(raw_frame)
            if not isinstance(frame, dict) or frame.get("schema_version") != 1:
                raise ValueError("invalid frame")
            frame_type = frame.get("type")
            if frame_type == "heartbeat":
                await connection.send(json.dumps({"type": "heartbeat_ack", "schema_version": 1}))
                return
            if frame_type == "ack_confirmed":
                return
            if frame_type == "error":
                self._logger.warning("platform command gateway returned an error frame")
                return
            if frame_type != "operation":
                raise ValueError("unsupported frame type")
            operation = _parse_operation(frame)
        except (ValueError, TypeError, json.JSONDecodeError):
            self._logger.error("platform_operation_invalid")
            return

        outcome = await self.storage.store_platform_operation(operation)
        if outcome == "conflict":
            self._logger.error("platform_operation_protocol_violation operation_id=%s", operation.operation_id)
            return
        # SQLite commit in store_platform_operation completes before this ACK is written.
        await connection.send(
            json.dumps({"type": "ack", "schema_version": 1, "operation_id": operation.operation_id})
        )
        if outcome == "new":
            self._logger.info(
                "platform_operation_received operation_id=%s operation_type=%s",
                operation.operation_id,
                operation.operation_type,
            )
        else:
            self._logger.info("platform_operation_duplicate operation_id=%s", operation.operation_id)

    async def _executor_loop(self) -> None:
        assert self._telegram_operations_client is not None
        delay = self._initial_retry_delay
        while True:
            operation = await self.storage.claim_next_platform_operation()
            if operation is None:
                await asyncio.sleep(0.2)
                continue
            self._logger.info(
                "platform_operation_executing operation_id=%s operation_type=%s attempt=%s",
                operation.operation_id,
                operation.operation_type,
                operation.attempt_count,
            )
            try:
                result_payload = await self._execute_operation(operation)
            except TelegramOperationError as error:
                if error.retryable and operation.attempt_count < 5:
                    await self.storage.requeue_platform_operation(operation.operation_id)
                    self._logger.warning(
                        "platform_operation_retry operation_id=%s retry_in=%.0fs code=%s",
                        operation.operation_id,
                        delay,
                        error.code,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
                    continue
                if error.ambiguous:
                    await self.storage.mark_executing_operations_unknown()
                    self._logger.error(
                        "platform_operation_execution_unknown operation_id=%s code=%s",
                        operation.operation_id,
                        error.code,
                    )
                    delay = self._initial_retry_delay
                    continue
                await self._complete_with_result(
                    operation,
                    status="failed",
                    result={},
                    error={"code": error.code},
                )
            except Exception:
                # An unexpected local failure has unknown action state, so never replay it automatically.
                await self.storage.mark_executing_operations_unknown()
                self._logger.exception("platform_operation_execution_unknown operation_id=%s", operation.operation_id)
            else:
                await self._complete_with_result(operation, status="succeeded", result=result_payload, error=None)
            delay = self._initial_retry_delay

    async def _execute_operation(self, operation: PlatformOperation) -> dict[str, object]:
        assert self._telegram_operations_client is not None
        if operation.operation_type == "send_message":
            return await self._telegram_operations_client.send_message(operation.payload)
        if operation.operation_type == "delete_message":
            return await self._telegram_operations_client.delete_message(operation.payload)
        raise TelegramOperationError("unsupported_operation")

    async def _complete_with_result(
        self,
        operation: PlatformOperation,
        *,
        status: str,
        result: dict[str, object],
        error: dict[str, object] | None,
    ) -> None:
        pending_result = PendingResult(
            result_id=str(uuid.uuid4()),
            operation_id=operation.operation_id,
            schema_version=1,
            platform="telegram",
            status=status,
            result=result,
            error=error,
            completed_at=_occurred_at(),
            delivery_status="pending",
        )
        await self.storage.complete_operation_with_result(pending_result)
        self._logger.info(
            "platform_operation_completed operation_id=%s result_id=%s status=%s",
            operation.operation_id,
            pending_result.result_id,
            status,
        )

    async def _result_delivery_loop(self) -> None:
        assert self._platform_results_client is not None
        delay = self._initial_retry_delay
        while True:
            pending_results = await self.storage.list_pending_results()
            if not pending_results:
                await asyncio.sleep(0.2)
                continue
            for pending_result in pending_results:
                try:
                    outcome = await self._platform_results_client.deliver(pending_result)
                except TransientResultDeliveryError as error:
                    self._logger.warning(
                        "platform_result_retry result_id=%s retry_in=%.0fs: %s",
                        pending_result.result_id,
                        delay,
                        error,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_delay)
                    break
                except PermanentResultDeliveryError as error:
                    self._logger.error(
                        "platform_result_delivery_degraded result_id=%s: %s", pending_result.result_id, error
                    )
                    await asyncio.sleep(self._max_retry_delay)
                    break
                await self.storage.mark_result_delivered(pending_result.result_id)
                delay = self._initial_retry_delay
                self._logger.info("platform_result_delivered result_id=%s outcome=%s", pending_result.result_id, outcome)


def _occurred_at() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _parse_operation(frame: dict[str, Any]) -> PlatformOperation:
    raw_operation = frame.get("operation")
    if not isinstance(raw_operation, dict):
        raise ValueError("operation is missing")
    operation_id = raw_operation.get("operation_id")
    schema_version = raw_operation.get("schema_version")
    platform = raw_operation.get("platform")
    operation_type = raw_operation.get("operation_type")
    payload = raw_operation.get("payload")
    if (
        not isinstance(operation_id, str)
        or type(schema_version) is not int
        or schema_version != 1
        or platform != "telegram"
        or not isinstance(operation_type, str)
        or not operation_type
        or not isinstance(payload, dict)
    ):
        raise ValueError("invalid operation")
    try:
        uuid.UUID(operation_id)
    except ValueError as error:
        raise ValueError("invalid operation id") from error
    return PlatformOperation(
        operation_id=operation_id,
        schema_version=schema_version,
        platform=platform,
        operation_type=operation_type,
        payload=payload,
        status="received",
        received_at=datetime.now(UTC).isoformat(),
    )
