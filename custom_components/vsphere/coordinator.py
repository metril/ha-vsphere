"""Data coordinator for vSphere Control integration."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL, DOMAIN
from .exceptions import VSphereAuthError, VSphereConnectionError

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

    from .permissions import PermissionResolver
    from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)


class VSphereData(DataUpdateCoordinator[dict[str, Any]]):
    """Shared state container for vSphere data.

    Primary data comes from EventListener (push). Extends DataUpdateCoordinator
    to get the entity notification mechanism, but _async_update_data is a no-op.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize VSphereData."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{entry.entry_id}",
            update_interval=None,
        )
        self.entry = entry
        self.client = client
        self.resolver = resolver
        self._data: dict[str, Any] = {
            "hosts": {},
            "vms": {},
            "datastores": {},
            "licenses": {},
            "clusters": {},
            "networks": {},
            "resource_pools": {},
            "alarms": {},
            "perf": {},
            "connection_info": {},
            "storage_advanced": {},
        }
        self.data = self._data

    async def _async_update_data(self) -> dict[str, Any]:
        """No-op — data is pushed by EventListener."""
        return self._data

    @callback
    def async_update_from_push(self, category: str, moref: str, properties: dict[str, Any]) -> None:
        """Update from push and notify entities."""
        if category in self._data:
            if moref in self._data[category]:
                self._data[category][moref].update(properties)
            else:
                properties["moref"] = moref
                self._data[category][moref] = properties
        self.async_set_updated_data(self._data)

    @callback
    def async_remove_object(self, category: str, moref: str) -> None:
        """Remove an object that no longer exists."""
        if category in self._data:
            self._data[category].pop(moref, None)
        self.async_set_updated_data(self._data)

    @callback
    def async_set_initial_data(self, data: dict[str, Any]) -> None:
        """Set initial data from EventListener's first fetch."""
        self._data.update(data)
        self.data = self._data
        self.async_set_updated_data(self._data)

    def update_perf(self, perf_data: dict[str, Any]) -> None:
        """Update performance counter data."""
        self._data["perf"] = perf_data

    def set_connection_info(self, info: dict[str, Any]) -> None:
        """Store connection metadata."""
        self._data["connection_info"] = info


class VSpherePerfCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Polls PerformanceManager. Only created if Performance category enabled."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: VSphereClient,
        vsphere_data: VSphereData,
        entry: ConfigEntry,
    ) -> None:
        """Initialize VSpherePerfCoordinator."""
        interval: int = entry.options.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL)
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{entry.entry_id}_perf",
            update_interval=timedelta(seconds=interval),
        )
        self._client = client
        self._vsphere_data = vsphere_data
        # Moref snapshots — captured on the event loop before executor dispatch
        self._host_morefs: set[str] = set()
        self._vm_morefs: set[str] = set()
        self._ds_morefs: set[str] = set()

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch performance data and push it into VSphereData."""
        # Capture morefs on the event loop (thread-safe) before dispatching to executor
        self._host_morefs = set(self._vsphere_data._data.get("hosts", {}).keys())  # noqa: SLF001
        self._vm_morefs = set(self._vsphere_data._data.get("vms", {}).keys())  # noqa: SLF001
        self._ds_morefs = set(self._vsphere_data._data.get("datastores", {}).keys())  # noqa: SLF001

        try:
            perf_data: dict[str, Any] = await self.hass.async_add_executor_job(self._fetch_performance)
            self._vsphere_data.update_perf(perf_data)
            self._vsphere_data.async_set_updated_data(self._vsphere_data._data)  # noqa: SLF001
            return perf_data
        except VSphereAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except VSphereConnectionError as err:
            raise UpdateFailed(str(err)) from err

    def _fetch_performance(self) -> dict[str, Any]:
        """Fetch performance data from PerformanceManager (runs in executor)."""
        self._client.ensure_poll_connection()

        # Use the moref snapshots captured on the event loop — thread-safe
        host_morefs = list(self._host_morefs)
        vm_morefs = list(self._vm_morefs)
        ds_morefs = list(self._ds_morefs)

        return self._client.query_performance(host_morefs, vm_morefs, ds_morefs)
