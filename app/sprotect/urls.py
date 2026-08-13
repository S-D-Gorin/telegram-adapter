"""Single source of truth for Sprotect HTTP and WebSocket endpoint URLs."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


def http_endpoint(server_api: str, endpoint_path: str) -> str:
    parsed = urlsplit(server_api)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("SERVER_API must be an absolute http(s) URL")
    base_path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, f"{base_path}{endpoint_path}", "", ""))


def websocket_endpoint(server_api: str, endpoint_path: str) -> str:
    parsed = urlsplit(server_api)
    scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme)
    if scheme is None or not parsed.netloc:
        raise ValueError("SERVER_API must be an absolute http(s) URL")
    base_path = parsed.path.rstrip("/")
    return urlunsplit((scheme, parsed.netloc, f"{base_path}{endpoint_path}", "", ""))
