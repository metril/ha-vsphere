"""The vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady

from .const import (
    CONF_CATEGORIES,
    CONF_ENTITY_FILTER,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_RESTRICTIONS,
    CONF_USERNAME,
    CONF_VERIFY_SSL,
    DEFAULT_CATEGORIES,
    DOMAIN,
    Category,
)
from .coordinator import VSphereData, VSpherePerfCoordinator
from .event_listener import VSphereEventListener
from .exceptions import VSphereAuthError, VSphereConnectionError
from .permissions import PermissionResolver
from .services import async_register_services, async_unregister_services
from .vsphere_client import VSphereClient

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SWITCH,
    Platform.BUTTON,
    Platform.SELECT,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up vSphere Control from a config entry."""
    # ------------------------------------------------------------------
    # Create the client and test connectivity
    # ------------------------------------------------------------------
    client = VSphereClient(
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        verify_ssl=entry.data[CONF_VERIFY_SSL],
    )

    try:
        connection_info: dict[str, Any] = await hass.async_add_executor_job(client.test_connection)
    except VSphereAuthError as err:
        raise ConfigEntryAuthFailed(str(err)) from err
    except VSphereConnectionError as err:
        raise ConfigEntryNotReady(str(err)) from err

    # Open the poll connection used by coordinators and services
    try:
        await hass.async_add_executor_job(client.connect_poll)
    except VSphereAuthError as err:
        raise ConfigEntryAuthFailed(str(err)) from err
    except VSphereConnectionError as err:
        raise ConfigEntryNotReady(str(err)) from err

    # ------------------------------------------------------------------
    # Check vSphere privileges (refresh on every load, not stale from setup)
    # ------------------------------------------------------------------
    try:
        privileges: dict[str, bool] = await hass.async_add_executor_job(client.check_privileges)
    except Exception:  # noqa: BLE001
        _LOGGER.debug("Privilege check failed, assuming full access")
        privileges = {}

    # ------------------------------------------------------------------
    # Create permission resolver
    # ------------------------------------------------------------------
    restrictions: dict[str, Any] = entry.options.get(CONF_RESTRICTIONS, {})
    resolver = PermissionResolver(restrictions, privileges)

    # ------------------------------------------------------------------
    # Create data coordinator
    # ------------------------------------------------------------------
    coordinator = VSphereData(hass, entry, client, resolver)
    coordinator.set_connection_info(connection_info)

    # ------------------------------------------------------------------
    # Resolve categories and entity filter from options
    # ------------------------------------------------------------------
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, dict(DEFAULT_CATEGORIES))
    entity_filter: dict[str, Any] = entry.options.get(CONF_ENTITY_FILTER, {})

    # ------------------------------------------------------------------
    # Create and start the event listener
    # ------------------------------------------------------------------
    event_listener = VSphereEventListener(
        hass=hass,
        client=client,
        vsphere_data=coordinator,
        entry_id=entry.entry_id,
        categories=categories,
        entity_filter=entity_filter,
    )

    try:
        await hass.async_add_executor_job(event_listener.start)
    except VSphereAuthError as err:
        await hass.async_add_executor_job(client.disconnect_poll)
        raise ConfigEntryAuthFailed(str(err)) from err
    except VSphereConnectionError as err:
        await hass.async_add_executor_job(client.disconnect_poll)
        raise ConfigEntryNotReady(str(err)) from err

    # ------------------------------------------------------------------
    # Optionally create the performance coordinator
    # ------------------------------------------------------------------
    perf_coordinator: VSpherePerfCoordinator | None = None
    if categories.get(Category.PERFORMANCE, False):
        perf_coordinator = VSpherePerfCoordinator(hass, client, coordinator, entry)
        await perf_coordinator.async_config_entry_first_refresh()

    # ------------------------------------------------------------------
    # Store runtime objects in hass.data
    # ------------------------------------------------------------------
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "client": client,
        "coordinator": coordinator,
        "event_listener": event_listener,
        "perf_coordinator": perf_coordinator,
        "resolver": resolver,
    }

    # ------------------------------------------------------------------
    # Forward to platforms
    # ------------------------------------------------------------------
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # ------------------------------------------------------------------
    # Register services (once, regardless of entry count)
    # ------------------------------------------------------------------
    if len(hass.data[DOMAIN]) == 1:
        await async_register_services(hass)

    # ------------------------------------------------------------------
    # Listen for options changes so we can reload
    # ------------------------------------------------------------------
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a vSphere config entry."""
    entry_data: dict[str, Any] = hass.data[DOMAIN].get(entry.entry_id, {})

    event_listener: VSphereEventListener | None = entry_data.get("event_listener")
    perf_coordinator: VSpherePerfCoordinator | None = entry_data.get("perf_coordinator")
    client: VSphereClient | None = entry_data.get("client")

    # Stop the event listener
    if event_listener is not None:
        await hass.async_add_executor_job(event_listener.stop)

    # Stop the perf coordinator (cancel its refresh timer)
    if perf_coordinator is not None:
        await perf_coordinator.async_shutdown()

    # Disconnect the poll connection
    if client is not None:
        await hass.async_add_executor_job(client.disconnect_poll)

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    # Remove from hass.data
    hass.data[DOMAIN].pop(entry.entry_id, None)

    # Unregister services when the last entry is removed
    if not hass.data[DOMAIN]:
        async_unregister_services(hass)
        hass.data.pop(DOMAIN, None)

    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options updates by reloading the entry."""
    await hass.config_entries.async_reload(entry.entry_id)
