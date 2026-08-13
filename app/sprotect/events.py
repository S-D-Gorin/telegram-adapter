"""Authenticated Platform Events API client."""

from __future__ import annotations

from typing import Any, Literal

import httpx


class PlatformEventError(Exception):
    """Base error that never includes Authorization or event payload values."""


class TransientPlatformEventError(PlatformEventError):
    pass


class PermanentPlatformEventError(PlatformEventError):
    pass


class PlatformAuthenticationError(PermanentPlatformEventError):
    pass


class SprotectPlatformEventsClient:
    def __init__(self, server_api: str, adapter_token: str, client: httpx.AsyncClient | None = None) -> None:
        self._url = f"{server_api.rstrip('/')}/api/v1/platform/events/"
        self._adapter_token = adapter_token
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None

    async def deliver(self, event: dict[str, Any]) -> Literal["accepted", "duplicate"]:
        try:
            response = await self._client.post(
                self._url,
                json=event,
                headers={"Authorization": f"Bearer {self._adapter_token}"},
            )
        except httpx.RequestError as error:
            raise TransientPlatformEventError("unable to reach Sprotect Platform Events API") from error
        if response.status_code >= 500:
            raise TransientPlatformEventError(f"Platform Events API returned {response.status_code}")
        if response.status_code == 202:
            return "accepted"
        if response.status_code == 200:
            return "duplicate"
        if response.status_code == 401:
            raise PlatformAuthenticationError("adapter token was rejected or revoked")
        raise PermanentPlatformEventError(f"Platform Events API returned {response.status_code}")

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
