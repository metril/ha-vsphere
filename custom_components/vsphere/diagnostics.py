"""Diagnostics support for vSphere Control integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.diagnostics import async_redact_data

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .coordinator import VSphereConfigEntry

REDACT_KEYS = {"password", "username", "host", "license_key", "key", "guest_ip", "ssl_ca_path", "mac", "uuid"}


async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: VSphereConfigEntry) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data.coordinator

    return {
        "config_entry": async_redact_data(entry.as_dict(), REDACT_KEYS),
        "coordinator_data": async_redact_data(
            coordinator.data,
            REDACT_KEYS,
        ),
    }
