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
    SprotectPlatformEventsClient,
    TransientPlatformEventError,
)
from .commands import (
    CommandAuthenticationError,
    CommandTransportError,
    PlatformCommandsWebSocketClient,
    is_authentication_close,
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
    "SprotectPlatformEventsClient",
    "TransientPlatformEventError",
    "CommandAuthenticationError",
    "CommandTransportError",
    "PlatformCommandsWebSocketClient",
    "is_authentication_close",
]
