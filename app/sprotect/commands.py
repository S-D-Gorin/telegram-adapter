"""Platform Gateway WebSocket client for durable operation delivery."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus


class CommandTransportError(Exception):
    """Reconnectable WebSocket transport failure without credential details."""


class CommandAuthenticationError(Exception):
    """Authentication/revocation failure; reconnects are deliberately slow."""


class PlatformCommandsWebSocketClient:
    def __init__(self, server_api: str, adapter_token: str) -> None:
        parsed = urlsplit(server_api)
        scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme)
        if scheme is None:
            raise ValueError("SERVER_API must use http(s)")
        base_path = parsed.path.rstrip("/")
        self._url = urlunsplit(
            (scheme, parsed.netloc, f"{base_path}/api/v1/platform-adapters/commands/ws/", "", "")
        )
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
            raise CommandTransportError("platform command WebSocket handshake failed") from error
        except InvalidHandshake as error:
            raise CommandTransportError("platform command WebSocket handshake failed") from error
        except OSError as error:
            raise CommandTransportError("unable to reach platform command WebSocket") from error


def is_authentication_close(error: ConnectionClosed) -> bool:
    return error.code == 4401
