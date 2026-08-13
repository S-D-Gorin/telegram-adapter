"""Configuration loaded from the process environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse


class ConfigError(ValueError):
    """Raised when required adapter configuration is missing or invalid."""


@dataclass(frozen=True, repr=False)
class Config:
    server_api: str
    bot_token: str
    data_dir: Path
    log_level: str

    def __repr__(self) -> str:
        return (
            "Config("
            f"server_api={self.server_api!r}, bot_token='***', "
            f"data_dir={str(self.data_dir)!r}, log_level={self.log_level!r})"
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
    )


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ConfigError(f"{name} is required")
    return value
