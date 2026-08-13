"""Logging configuration for the adapter."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable


_SECRETS: set[str] = set()
_BOT_URL = re.compile(r"/bot[^/\s]+/")
_ORIGINAL_FACTORY = logging.getLogRecordFactory()


def register_secrets(values: Iterable[str | None]) -> None:
    """Register process credentials for redaction from every log record."""
    _SECRETS.update(value for value in values if value)


def _redact(value: object) -> object:
    if not isinstance(value, str):
        return value
    redacted = value
    for secret in _SECRETS:
        redacted = redacted.replace(secret, "***")
    return _BOT_URL.sub("/bot***/", redacted)


def _record_factory(*args, **kwargs):
    record = _ORIGINAL_FACTORY(*args, **kwargs)
    # Format before replacing secrets: changing a ``%s`` argument inside the
    # template would otherwise corrupt logging's positional interpolation.
    try:
        message = record.getMessage()
    except Exception:
        message = str(record.msg)
    record.msg = _redact(message)
    record.args = ()
    for key, value in list(record.__dict__.items()):
        if key not in {"msg", "args"}:
            record.__dict__[key] = _redact(value)
    return record


def configure_logging(level: str, *, secrets: Iterable[str | None] = ()) -> None:
    register_secrets(secrets)
    logging.setLogRecordFactory(_record_factory)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
