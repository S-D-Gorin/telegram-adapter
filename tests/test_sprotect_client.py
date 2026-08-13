import json

import httpx
import pytest

from app.sprotect import (
    PermanentBootstrapError,
    SprotectBootstrapClient,
    TokenAlreadyIssuedError,
    TransientBootstrapError,
)
from app.sprotect.urls import http_endpoint, websocket_endpoint


@pytest.mark.asyncio
async def test_bootstrap_client_uses_stage_2a_paths_and_payloads() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("token/"):
            return httpx.Response(200, json={"data": {"adapter": {"status": "active"}, "adapter_token": "new-token"}, "request_id": "request"})
        return httpx.Response(200, json={"data": {"adapter": {"status": "active"}}, "request_id": "request"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client = SprotectBootstrapClient("https://backend.example/", transport)
        assert (await client.register_adapter("id", "pairing")).status == "active"
        assert (await client.get_pairing_status("id", "pairing")).status == "active"
        assert await client.obtain_token("id", "pairing") == "new-token"

    assert [request.url.path for request in requests] == [
        "/api/v1/platform-adapters/bootstrap/register/",
        "/api/v1/platform-adapters/bootstrap/status/",
        "/api/v1/platform-adapters/bootstrap/token/",
    ]
    assert json.loads(requests[0].content) == {
        "adapter_id": "id", "platform": "telegram", "pairing_secret": "pairing"
    }
    assert json.loads(requests[1].content) == {"adapter_id": "id", "pairing_secret": "pairing"}
    assert json.loads(requests[2].content) == {"adapter_id": "id", "pairing_secret": "pairing"}


@pytest.mark.asyncio
async def test_client_distinguishes_temporary_and_permanent_errors() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503, json={}))
    ) as transport:
        with pytest.raises(TransientBootstrapError):
            await SprotectBootstrapClient("https://backend.example", transport).register_adapter("id", "secret")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(403, json={"detail": "no"}))
    ) as transport:
        with pytest.raises(PermanentBootstrapError):
            await SprotectBootstrapClient("https://backend.example", transport).get_pairing_status("id", "secret")


@pytest.mark.asyncio
async def test_client_handles_one_time_token_recovery_semantics() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                409,
                json={
                    "error": {"code": "adapter_token_already_issued"},
                    "request_id": "request",
                },
            )
        )
    ) as transport:
        with pytest.raises(TokenAlreadyIssuedError):
            await SprotectBootstrapClient("https://backend.example", transport).obtain_token("id", "secret")


@pytest.mark.parametrize("server_api", ["http://backend:8000", "http://backend:8000/"])
def test_http_endpoint_mapping_avoids_double_slashes(server_api: str) -> None:
    assert http_endpoint(server_api, "/api/v1/platform-adapters/events/") == (
        "http://backend:8000/api/v1/platform-adapters/events/"
    )
    assert http_endpoint(server_api, "/api/v1/platform-adapters/results/") == (
        "http://backend:8000/api/v1/platform-adapters/results/"
    )


def test_websocket_endpoint_mapping_converts_http_and_https() -> None:
    assert websocket_endpoint("http://backend:8000/", "/api/v1/platform-adapters/commands/ws/") == (
        "ws://backend:8000/api/v1/platform-adapters/commands/ws/"
    )
    assert websocket_endpoint("https://backend.example", "/api/v1/platform-adapters/commands/ws/") == (
        "wss://backend.example/api/v1/platform-adapters/commands/ws/"
    )
