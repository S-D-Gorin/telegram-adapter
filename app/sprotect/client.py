"""Async client for the Stage 2A platform-adapter bootstrap API."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from .urls import http_endpoint


class BootstrapError(Exception):
    """Base exception for bootstrap failures that contain no secret values."""


class TransientBootstrapError(BootstrapError):
    """The adapter may retry this request after backoff."""


class PermanentBootstrapError(BootstrapError):
    """The backend rejected a validly delivered bootstrap request."""


class TokenAlreadyIssuedError(PermanentBootstrapError):
    """The one-time token was already revealed and cannot be recovered."""


@dataclass(frozen=True)
class PairingState:
    status: str

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def is_terminal_invalid(self) -> bool:
        return self.status in {"revoked", "invalid"}


class SprotectBootstrapClient:
    def __init__(self, server_api: str, client: httpx.AsyncClient | None = None) -> None:
        self._server_api = server_api
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None

    async def register_adapter(self, adapter_id: str, pairing_secret: str) -> PairingState:
        return await self._request_state(
            "/api/v1/platform-adapters/bootstrap/register/", adapter_id, pairing_secret, platform="telegram"
        )

    async def get_pairing_status(self, adapter_id: str, pairing_secret: str) -> PairingState:
        return await self._request_state(
            "/api/v1/platform-adapters/bootstrap/status/", adapter_id, pairing_secret
        )

    async def obtain_token(self, adapter_id: str, pairing_secret: str) -> str:
        payload = await self._post(
            "/api/v1/platform-adapters/bootstrap/token/", adapter_id, pairing_secret
        )
        data = _data(payload)
        token = data.get("adapter_token")
        if not isinstance(token, str) or not token:
            raise PermanentBootstrapError("token response did not contain adapter_token")
        return token

    async def _request_state(
        self, path: str, adapter_id: str, pairing_secret: str, *, platform: str | None = None
    ) -> PairingState:
        payload = await self._post(path, adapter_id, pairing_secret, platform=platform)
        adapter = _data(payload).get("adapter")
        status = adapter.get("status") if isinstance(adapter, dict) else None
        if not isinstance(status, str) or not status:
            raise PermanentBootstrapError("bootstrap response did not contain status")
        return PairingState(status=status)

    async def _post(
        self, path: str, adapter_id: str, pairing_secret: str, *, platform: str | None = None
    ) -> dict[str, object]:
        request_body: dict[str, str] = {
            "adapter_id": adapter_id,
            "pairing_secret": pairing_secret,
        }
        if platform is not None:
            request_body["platform"] = platform
        try:
            response = await self._client.post(
                http_endpoint(self._server_api, path),
                json=request_body,
            )
        except httpx.RequestError as error:
            raise TransientBootstrapError("unable to reach Sprotect bootstrap API") from error

        if response.status_code >= 500 or response.status_code == 429:
            raise TransientBootstrapError(f"Sprotect bootstrap API returned {response.status_code}")
        try:
            payload = response.json()
        except ValueError as error:
            raise TransientBootstrapError("Sprotect bootstrap API returned invalid JSON") from error
        if not isinstance(payload, dict):
            raise PermanentBootstrapError("Sprotect bootstrap API returned invalid response shape")
        if response.status_code >= 400:
            code = _error_code(payload)
            if code == "adapter_token_already_issued":
                raise TokenAlreadyIssuedError("adapter token was already issued and cannot be recovered")
            raise PermanentBootstrapError(f"Sprotect bootstrap API returned {response.status_code}")
        return payload

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _data(payload: dict[str, object]) -> dict[str, object]:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise PermanentBootstrapError("Sprotect bootstrap API returned invalid response shape")
    return data


def _error_code(payload: dict[str, object]) -> str | None:
    error = payload.get("error")
    if isinstance(error, dict) and isinstance(error.get("code"), str):
        return error["code"]
    return None
