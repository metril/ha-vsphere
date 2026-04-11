"""Service registration for vSphere Control integration."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant


async def async_register_services(hass: HomeAssistant) -> None:
    """Register vSphere services."""
    pass


def async_unregister_services(hass: HomeAssistant) -> None:
    """Unregister vSphere services."""
    pass
