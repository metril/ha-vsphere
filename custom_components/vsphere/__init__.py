"""The vSphere Control integration."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv
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
from .coordinator import (
    VSphereConfigEntry,
    VSphereData,
    VSphereInventoryCoordinator,
    VSpherePerfCoordinator,
    VSphereRuntimeData,
)
from .entity import is_stale_unique_id, valid_morefs
from .event_listener import VSphereEventListener
from .exceptions import VSphereAuthError, VSphereConnectionError
from .permissions import PermissionResolver
from .services import async_register_services
from .vsphere_client import VSphereClient

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

__all__ = ["CONFIG_SCHEMA", "VSphereConfigEntry", "VSphereRuntimeData", "async_setup", "async_setup_entry"]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.SWITCH,
    Platform.BUTTON,
    Platform.SELECT,
]


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    """Register services once; handlers look up loaded entries at call time."""
    await async_register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: VSphereConfigEntry) -> bool:
    """Set up vSphere Control from a config entry."""
    # ------------------------------------------------------------------
    # Validate CA file if configured
    # ------------------------------------------------------------------
    ssl_ca_path = entry.data.get(CONF_SSL_CA_PATH, "")
    if ssl_ca_path and not await hass.async_add_executor_job(Path(ssl_ca_path).is_file):
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

    # Everything from here on can fail; tear down whatever was started so no
    # listener thread or poll session leaks, whatever the exception type.
    event_listener: VSphereEventListener | None = None
    perf_coordinator: VSpherePerfCoordinator | None = None
    inventory_coordinator: VSphereInventoryCoordinator | None = None
    try:
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
            raise ConfigEntryAuthFailed(str(err)) from err
        except VSphereConnectionError as err:
            raise ConfigEntryNotReady(str(err)) from err
        except Exception as err:
            raise ConfigEntryNotReady(f"Unexpected error starting vSphere listener: {err}") from err

        # Wait for initial data before setting up platforms (event listener
        # pushes data from a background thread via call_soon_threadsafe)
        try:
            await asyncio.wait_for(coordinator.initial_data_ready.wait(), timeout=30)
        except TimeoutError as err:
            raise ConfigEntryNotReady("Timed out waiting for initial vSphere data") from err

        # ------------------------------------------------------------------
        # Optionally create the performance coordinator
        # ------------------------------------------------------------------
        if categories.get(Category.PERFORMANCE, False):
            perf_coordinator = VSpherePerfCoordinator(hass, client, coordinator, entry)
            await perf_coordinator.async_config_entry_first_refresh()
            # Registering a listener is what arms the perf coordinator's own refresh
            # timer — HA only reschedules while a coordinator has listeners.
            entry.async_on_unload(perf_coordinator.async_add_listener(coordinator.async_update_listeners))

        # ------------------------------------------------------------------
        # Optionally create the inventory coordinator (licenses/network/storage_advanced —
        # categories PropertyCollector cannot watch)
        # ------------------------------------------------------------------
        if any(categories.get(c) for c in (Category.LICENSES, Category.NETWORK, Category.STORAGE_ADVANCED)):
            inventory_coordinator = VSphereInventoryCoordinator(hass, client, coordinator, entry, categories)
            # Registering a listener arms the refresh timer — HA only reschedules
            # a coordinator while it has listeners. No first refresh: the event
            # listener's initial fetch already populated these categories.
            entry.async_on_unload(inventory_coordinator.async_add_listener(coordinator.async_update_listeners))

        # ------------------------------------------------------------------
        # Store runtime objects on the config entry
        # ------------------------------------------------------------------
        entry.runtime_data = VSphereRuntimeData(
            client=client,
            coordinator=coordinator,
            event_listener=event_listener,
            resolver=resolver,
            perf_coordinator=perf_coordinator,
            inventory_coordinator=inventory_coordinator,
        )

        # ------------------------------------------------------------------
        # Forward to platforms (each registers its own discovery listener)
        # ------------------------------------------------------------------
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        # ------------------------------------------------------------------
        # Clean up stale entities/devices now, and at setup; runtime removal is driven by push deletions only
        # ------------------------------------------------------------------
        _async_cleanup_stale_entities(hass, entry, coordinator, categories)
        entry.async_on_unload(
            coordinator.async_add_removal_listener(
                lambda category, moref: _async_remove_object_entities(hass, entry, category, moref)
            )
        )

        # ------------------------------------------------------------------
        # Listen for options changes so we can reload
        # ------------------------------------------------------------------
        entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    except BaseException:
        await _async_teardown(hass, entry, event_listener, perf_coordinator, inventory_coordinator, client)
        raise

    return True


async def _async_teardown(
    hass: HomeAssistant,
    entry: VSphereConfigEntry,
    event_listener: VSphereEventListener | None,
    perf_coordinator: VSpherePerfCoordinator | None,
    inventory_coordinator: VSphereInventoryCoordinator | None,
    client: VSphereClient,
) -> None:
    """Best-effort cleanup after a failed setup."""
    with contextlib.suppress(Exception):
        await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if event_listener is not None:
        with contextlib.suppress(Exception):
            await hass.async_add_executor_job(event_listener.stop)
    if perf_coordinator is not None:
        with contextlib.suppress(Exception):
            await perf_coordinator.async_shutdown()
    if inventory_coordinator is not None:
        with contextlib.suppress(Exception):
            await inventory_coordinator.async_shutdown()
    with contextlib.suppress(Exception):
        await hass.async_add_executor_job(client.disconnect_poll)


def _async_cleanup_stale_entities(
    hass: HomeAssistant,
    entry: VSphereConfigEntry,
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

    valid = valid_morefs(coordinator.data or {}, categories, entry.entry_id)
    for ent in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
        if is_stale_unique_id(ent.unique_id, entry.entry_id, valid):
            ent_reg.async_remove(ent.entity_id)

    # Remove devices with no remaining entities
    for dev in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        if not er.async_entries_for_device(ent_reg, dev.id):
            dev_reg.async_remove_device(dev.id)


REMOVABLE_CATEGORIES = frozenset({"hosts", "vms", "datastores", "clusters", "resource_pools"})


def _async_remove_object_entities(hass: HomeAssistant, entry: VSphereConfigEntry, category: str, moref: str) -> None:
    """Purge registry rows for an object the server genuinely deleted."""
    if category not in REMOVABLE_CATEGORIES:
        return
    for discovery in entry.runtime_data.discoveries:
        discovery.forget(moref)
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    prefix = f"{entry.entry_id}_{moref}"
    for ent in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
        if ent.unique_id == prefix or ent.unique_id.startswith(prefix + "_"):
            ent_reg.async_remove(ent.entity_id)
    device = dev_reg.async_get_device(identifiers={(DOMAIN, prefix)})
    if device is not None:
        # Child entities (VM storage/disks, host network rows) live on this device
        for ent in er.async_entries_for_device(ent_reg, device.id, include_disabled_entities=True):
            if ent.config_entry_id == entry.entry_id:
                ent_reg.async_remove(ent.entity_id)
        if not er.async_entries_for_device(ent_reg, device.id, include_disabled_entities=True):
            dev_reg.async_remove_device(device.id)


async def async_unload_entry(hass: HomeAssistant, entry: VSphereConfigEntry) -> bool:
    """Unload a vSphere config entry."""
    rt = entry.runtime_data

    # Unload platforms first to stop entity callbacks before tearing down data sources
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        # Platforms still hold references; keep the listener, coordinators and client alive
        return False

    await hass.async_add_executor_job(rt.event_listener.stop)
    if rt.perf_coordinator is not None:
        await rt.perf_coordinator.async_shutdown()
    if rt.inventory_coordinator is not None:
        await rt.inventory_coordinator.async_shutdown()
    await hass.async_add_executor_job(rt.client.disconnect_poll)

    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: VSphereConfigEntry) -> None:
    """Handle options updates by reloading the entry."""
    await hass.config_entries.async_reload(entry.entry_id)
