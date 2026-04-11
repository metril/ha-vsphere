"""Diagnostics support for vSphere Control integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.diagnostics import async_redact_data

from .const import DOMAIN

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

REDACT_KEYS = {"password", "username", "host", "license_key", "key", "guest_ip"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    data = hass.data[DOMAIN].get(entry.entry_id, {})
    coordinator = data.get("coordinator")

    return {
        "config_entry": async_redact_data(entry.as_dict(), REDACT_KEYS),
        "coordinator_data": async_redact_data(
            coordinator.data if coordinator else {},
            REDACT_KEYS,
        ),
    }
