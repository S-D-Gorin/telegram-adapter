"""Platform Gateway WebSocket client for durable operation delivery."""

from __future__ import annotations

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus


class CommandTransportError(Exception):
    """Reconnectable WebSocket transport failure without credential details."""

    def __init__(self, message: str, *, url: str | None = None, http_status: int | None = None) -> None:
        self.url = url
        self.http_status = http_status
        super().__init__(message)


class CommandAuthenticationError(Exception):
    """Authentication/revocation failure; reconnects are deliberately slow."""


class PlatformCommandsWebSocketClient:
    def __init__(self, server_api: str, adapter_token: str) -> None:
        self._url = websocket_endpoint(server_api, "/api/v1/platform-adapters/commands/ws/")
        self._adapter_token = adapter_token

    async def connect(self):
        try:
            return await connect(
                self._url,
                additional_headers={"Authorization": f"Bearer {self._adapter_token}"},
                ping_interval=20,
                ping_timeout=20,
            )
        except InvalidStatus as error:
            if error.response.status_code in {401, 403}:
                raise CommandAuthenticationError("platform command authentication was rejected") from error
            raise CommandTransportError(
                "platform command WebSocket handshake failed",
                url=self._url,
                http_status=error.response.status_code,
            ) from error
        except InvalidHandshake as error:
            raise CommandTransportError("platform command WebSocket handshake failed", url=self._url) from error
        except OSError as error:
            raise CommandTransportError("unable to reach platform command WebSocket", url=self._url) from error


def is_authentication_close(error: ConnectionClosed) -> bool:
    return error.code == 4401
from .urls import websocket_endpoint
