"""Authenticated Platform Events API client."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal

import httpx

from .urls import http_endpoint

# A misconfigured Retry-After must not park the whole outbox for days.
MAX_RETRY_AFTER_SECONDS = 3600.0


class PlatformEventError(Exception):
    """Base error that never includes Authorization or event payload values."""

    def __init__(self, message: str, *, status_code: int | None = None, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class TransientPlatformEventError(PlatformEventError):
    """Network failure, timeout, 408 or 5xx: retry the unchanged envelope with backoff."""


class PlatformRateLimitError(TransientPlatformEventError):
    """429: retry after ``retry_after`` seconds when Guardian provides it."""


class PermanentPlatformEventError(PlatformEventError):
    """A 4xx rejection of this particular event; retried a bounded number of times."""


class PlatformAuthenticationError(PermanentPlatformEventError):
    """401: the adapter token itself is rejected; affects every partition."""


class SprotectPlatformEventsClient:
    def __init__(
        self,
        server_api: str,
        adapter_token: str,
        client: httpx.AsyncClient | None = None,
        *,
        max_connections: int = 8,
    ) -> None:
        self._url = http_endpoint(server_api, "/api/v1/platform-adapters/events/")
        self._adapter_token = adapter_token
        # The pool matches EVENT_DELIVERY_CONCURRENCY so no delivery waits for a connection.
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections),
        )
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
        status = response.status_code
        if status == 202:
            return "accepted"
        if status == 200:
            return "duplicate"
        if status == 429:
            raise PlatformRateLimitError(
                "Platform Events API rate limited the adapter",
                status_code=status,
                retry_after=parse_retry_after(response.headers.get("Retry-After")),
            )
        if status >= 500 or status == 408:
            raise TransientPlatformEventError(f"Platform Events API returned {status}", status_code=status)
        if status == 401:
            raise PlatformAuthenticationError("adapter token was rejected or revoked", status_code=status)
        raise PermanentPlatformEventError(f"Platform Events API returned {status}", status_code=status)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse delta-seconds or an HTTP-date; invalid values are ignored."""
    if value is None or not value.strip():
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - (now or datetime.now(UTC))).total_seconds()
    if math.isnan(seconds):
        return None
    return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)
