"""The vSphere Control integration."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .const import (
    CONF_CATEGORIES,
    CONF_ENTITY_FILTER,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_RESTRICTIONS,
    CONF_SSL_CA_PATH,
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
    # Validate CA file if configured
    # ------------------------------------------------------------------
    ssl_ca_path = entry.data.get(CONF_SSL_CA_PATH, "")
    if ssl_ca_path:
        from pathlib import Path  # noqa: PLC0415

        if not Path(ssl_ca_path).is_file():
            raise ConfigEntryNotReady(f"CA certificate file not found: {ssl_ca_path}")

    # ------------------------------------------------------------------
    # Create the client and test connectivity
    # ------------------------------------------------------------------
    client = VSphereClient(
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        verify_ssl=entry.data[CONF_VERIFY_SSL],
        ssl_ca_path=entry.data.get(CONF_SSL_CA_PATH, ""),
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
    # Create permission resolver (user-configured restrictions only;
    # vSphere account privileges are enforced by vCenter/ESXi at operation time)
    # ------------------------------------------------------------------
    restrictions: dict[str, Any] = entry.options.get(CONF_RESTRICTIONS, {})
    resolver = PermissionResolver(restrictions)

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

    # Wait for initial data before setting up platforms (event listener
    # pushes data from a background thread via call_soon_threadsafe)
    try:
        await asyncio.wait_for(coordinator.initial_data_ready.wait(), timeout=30)
    except TimeoutError as err:
        await hass.async_add_executor_job(event_listener.stop)
        await hass.async_add_executor_job(client.disconnect_poll)
        raise ConfigEntryNotReady("Timed out waiting for initial vSphere data") from err

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
        "armed": {},
    }

    # ------------------------------------------------------------------
    # Forward to platforms
    # ------------------------------------------------------------------
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # ------------------------------------------------------------------
    # Clean up stale entities/devices from disabled categories or removed objects
    # ------------------------------------------------------------------
    _async_cleanup_stale_entities(hass, entry, coordinator, categories)

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


def _async_cleanup_stale_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator: VSphereData,
    categories: dict[str, bool],
) -> None:
    """Remove entities and devices for disabled categories or removed objects.

    Compares registered entities against the current coordinator data and
    enabled categories.  Any entity whose moref is no longer valid is removed
    from the entity registry, and devices left with no entities are removed
    from the device registry.
    """
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    # Build the set of morefs that should have entities right now
    valid_morefs: set[str] = {entry.entry_id}  # root device
    for cat_key in ("hosts", "vms", "datastores", "clusters", "resource_pools"):
        if categories.get(cat_key):
            valid_morefs.update(coordinator.data.get(cat_key, {}).keys())
    if categories.get("network"):
        valid_morefs.update(coordinator.data.get("networks", {}).keys())
    if categories.get("storage_advanced"):
        valid_morefs.update(coordinator.data.get("storage_advanced", {}).keys())
    if categories.get("licenses"):
        valid_morefs.update(coordinator.data.get("licenses", {}).keys())
    if categories.get("events_alarms"):
        # Alarm entities are on host/VM morefs (already in valid_morefs if those categories enabled)
        for cat_key in ("hosts", "vms"):
            if categories.get(cat_key):
                valid_morefs.update(coordinator.data.get(cat_key, {}).keys())

    # Remove entities whose moref is no longer valid
    prefix = f"{entry.entry_id}_"
    for ent in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
        uid = ent.unique_id
        if not uid.startswith(prefix):
            continue
        remainder = uid[len(prefix) :]
        # Check if any valid moref is a prefix of the remainder
        # (morefs like "host-42" can contain hyphens/numbers)
        moref_found = any(remainder.startswith(m + "_") or remainder == m for m in valid_morefs)
        if not moref_found:
            ent_reg.async_remove(ent.entity_id)

    # Remove devices with no remaining entities
    for dev in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        if not er.async_entries_for_device(ent_reg, dev.id):
            dev_reg.async_remove_device(dev.id)


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
