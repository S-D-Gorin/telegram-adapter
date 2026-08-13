"""Platform Result HTTP transport, separate from event ingestion."""

from __future__ import annotations

from typing import Literal

import httpx

from app.storage import PendingResult


class ResultDeliveryError(Exception):
    pass


class TransientResultDeliveryError(ResultDeliveryError):
    pass


class PermanentResultDeliveryError(ResultDeliveryError):
    pass


class SprotectPlatformResultsClient:
    def __init__(self, server_api: str, adapter_token: str, client: httpx.AsyncClient | None = None) -> None:
        self._url = f"{server_api.rstrip('/')}/api/v1/platform-adapters/results/"
        self._adapter_token = adapter_token
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None

    async def deliver(self, result: PendingResult) -> Literal["accepted", "duplicate"]:
        envelope = {
            "schema_version": result.schema_version,
            "result_id": result.result_id,
            "operation_id": result.operation_id,
            "platform": result.platform,
            "status": result.status,
            "result": result.result,
            "error": result.error,
            "completed_at": result.completed_at,
        }
        try:
            response = await self._client.post(
                self._url,
                json=envelope,
                headers={"Authorization": f"Bearer {self._adapter_token}"},
            )
        except httpx.RequestError as error:
            raise TransientResultDeliveryError("unable to reach Platform Results API") from error
        if response.status_code >= 500:
            raise TransientResultDeliveryError(f"Platform Results API returned {response.status_code}")
        if response.status_code == 202:
            return "accepted"
        if response.status_code == 200:
            return "duplicate"
        raise PermanentResultDeliveryError(f"Platform Results API returned {response.status_code}")

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
