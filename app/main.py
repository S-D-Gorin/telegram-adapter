"""Process entry point."""

from __future__ import annotations

import asyncio
import logging
import signal

from app.application import Application
from app.config import ConfigError, load_config
from app.logging import configure_logging


async def run() -> None:
    config = load_config()
    configure_logging(config.log_level)
    application = Application(config)
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logging.getLogger(__name__).info("shutdown signal received")
        stop_requested.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:  # Windows event loops do not support this API.
            signal.signal(sig, lambda _number, _frame: loop.call_soon_threadsafe(request_stop))

    try:
        await application.start()
        await stop_requested.wait()
    finally:
        await application.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except ConfigError as error:
        logging.basicConfig(level="ERROR", format="%(levelname)s: %(message)s")
        logging.getLogger(__name__).error("configuration error: %s", error)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
