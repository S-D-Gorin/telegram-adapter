"""Configuration loaded from the process environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse


DEFAULT_EVENT_DELIVERY_CONCURRENCY = 8
DEFAULT_EVENT_MAX_ATTEMPTS = 5
DEFAULT_EVENT_PENDING_TTL_SECONDS = 172800
DEFAULT_EVENT_RETENTION_SECONDS = 172800
DEFAULT_EVENT_MAX_PENDING = 10000


class ConfigError(ValueError):
    """Raised when required adapter configuration is missing or invalid."""


@dataclass(frozen=True, repr=False)
class Config:
    server_api: str
    bot_token: str
    data_dir: Path
    log_level: str
    event_delivery_concurrency: int = DEFAULT_EVENT_DELIVERY_CONCURRENCY
    event_max_attempts: int = DEFAULT_EVENT_MAX_ATTEMPTS
    event_pending_ttl_seconds: int = DEFAULT_EVENT_PENDING_TTL_SECONDS
    event_retention_seconds: int = DEFAULT_EVENT_RETENTION_SECONDS
    event_max_pending: int = DEFAULT_EVENT_MAX_PENDING

    def __repr__(self) -> str:
        return (
            "Config("
            f"server_api={self.server_api!r}, bot_token='***', "
            f"data_dir={str(self.data_dir)!r}, log_level={self.log_level!r}, "
            f"event_delivery_concurrency={self.event_delivery_concurrency}, "
            f"event_max_attempts={self.event_max_attempts}, "
            f"event_pending_ttl_seconds={self.event_pending_ttl_seconds}, "
            f"event_retention_seconds={self.event_retention_seconds}, "
            f"event_max_pending={self.event_max_pending})"
        )

    @property
    def database_path(self) -> Path:
        return self.data_dir / "adapter.db"

    def safe_details(self) -> dict[str, str]:
        """Fields that are safe to attach to a log record."""
        return {
            "server_api": self.server_api,
            "data_dir": str(self.data_dir),
            "log_level": self.log_level,
            "event_delivery_concurrency": str(self.event_delivery_concurrency),
            "event_max_attempts": str(self.event_max_attempts),
            "event_pending_ttl_seconds": str(self.event_pending_ttl_seconds),
            "event_retention_seconds": str(self.event_retention_seconds),
            "event_max_pending": str(self.event_max_pending),
        }


def load_config(environ: Mapping[str, str] | None = None) -> Config:
    values = os.environ if environ is None else environ
    server_api = _required(values, "SERVER_API")
    bot_token = _required(values, "BOT_TOKEN")
    parsed = urlparse(server_api)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("SERVER_API must be an absolute http(s) URL")

    log_level = values.get("LOG_LEVEL", "INFO").upper()
    if log_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
        raise ConfigError("LOG_LEVEL must be a standard logging level")

    return Config(
        server_api=server_api.rstrip("/"),
        bot_token=bot_token,
        data_dir=Path(values.get("DATA_DIR", "/data")),
        log_level=log_level,
        # The upper bound keeps the partition exclusion list within SQLite's parameter limit.
        event_delivery_concurrency=_integer(
            values, "EVENT_DELIVERY_CONCURRENCY", DEFAULT_EVENT_DELIVERY_CONCURRENCY, minimum=1, maximum=256
        ),
        event_max_attempts=_integer(values, "EVENT_MAX_ATTEMPTS", DEFAULT_EVENT_MAX_ATTEMPTS, minimum=1),
        event_pending_ttl_seconds=_integer(
            values, "EVENT_PENDING_TTL_SECONDS", DEFAULT_EVENT_PENDING_TTL_SECONDS, minimum=60
        ),
        event_retention_seconds=_integer(
            values, "EVENT_RETENTION_SECONDS", DEFAULT_EVENT_RETENTION_SECONDS, minimum=0
        ),
        event_max_pending=_integer(values, "EVENT_MAX_PENDING", DEFAULT_EVENT_MAX_PENDING, minimum=1),
    )


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ConfigError(f"{name} is required")
    return value


def _integer(
    values: Mapping[str, str], name: str, default: int, *, minimum: int, maximum: int | None = None
) -> int:
    raw = values.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ConfigError(f"{name} must be an integer") from error
    if value < minimum or (maximum is not None and value > maximum):
        bounds = f">= {minimum}" if maximum is None else f"between {minimum} and {maximum}"
        raise ConfigError(f"{name} must be {bounds}")
    return value
