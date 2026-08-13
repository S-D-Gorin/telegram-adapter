import logging

from app.logging import configure_logging, register_secrets


def test_redacts_known_credentials_and_telegram_bot_url_from_http_logs(caplog) -> None:
    bot_token = "123456:bot-secret"
    pairing_secret = "pairing-secret"
    adapter_token = "adapter-secret"
    configure_logging("INFO", secrets=(bot_token, pairing_secret, adapter_token))
    register_secrets((pairing_secret, adapter_token))
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.addHandler(caplog.handler)

    try:
        with caplog.at_level(logging.WARNING, logger="httpx"):
            httpx_logger.warning(
                "HTTP Request: GET https://api.telegram.org/bot%s/getUpdates pairing=%s auth=%s",
                bot_token,
                pairing_secret,
                adapter_token,
            )
    finally:
        httpx_logger.removeHandler(caplog.handler)

    assert bot_token not in caplog.text
    assert pairing_secret not in caplog.text
    assert adapter_token not in caplog.text
    assert "/bot***/getUpdates" in caplog.text
