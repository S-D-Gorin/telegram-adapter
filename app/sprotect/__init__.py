"""Sprotect transport boundary."""

from .client import (
    BootstrapError,
    PairingState,
    PermanentBootstrapError,
    SprotectBootstrapClient,
    TokenAlreadyIssuedError,
    TransientBootstrapError,
)
from .events import (
    PermanentPlatformEventError,
    PlatformAuthenticationError,
    PlatformRateLimitError,
    SprotectPlatformEventsClient,
    TransientPlatformEventError,
)
from .commands import (
    CommandAuthenticationError,
    CommandTransportError,
    PlatformCommandsWebSocketClient,
    is_authentication_close,
)
from .results import (
    PermanentResultDeliveryError,
    SprotectPlatformResultsClient,
    TransientResultDeliveryError,
)

__all__ = [
    "BootstrapError",
    "PairingState",
    "PermanentBootstrapError",
    "SprotectBootstrapClient",
    "TokenAlreadyIssuedError",
    "TransientBootstrapError",
    "PermanentPlatformEventError",
    "PlatformAuthenticationError",
    "PlatformRateLimitError",
    "SprotectPlatformEventsClient",
    "TransientPlatformEventError",
    "CommandAuthenticationError",
    "CommandTransportError",
    "PlatformCommandsWebSocketClient",
    "is_authentication_close",
    "PermanentResultDeliveryError",
    "SprotectPlatformResultsClient",
    "TransientResultDeliveryError",
]
