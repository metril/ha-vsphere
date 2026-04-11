"""PropertyCollector-based event listener for real-time vSphere updates."""

from __future__ import annotations

import contextlib
import logging
import threading
from typing import TYPE_CHECKING, Any

from .const import Category

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .coordinator import VSphereData
    from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)

BACKOFF_SCHEDULE: list[int] = [5, 10, 30, 60]
WAIT_OPTIONS_MAX_WAIT: int = 60


class VSphereEventListener:
    """Listens for vSphere property changes via PropertyCollector WaitForUpdatesEx.

    Runs in a background thread. When properties change, parses updates
    and calls back into VSphereData to update state.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        client: VSphereClient,
        vsphere_data: VSphereData,
        entry_id: str,
        categories: dict[str, bool],
        entity_filter: dict[str, Any],
    ) -> None:
        """Initialize VSphereEventListener."""
        self._hass = hass
        self._client = client
        self._vsphere_data = vsphere_data
        self._entry_id = entry_id
        self._categories = categories
        self._entity_filter = entity_filter
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pc: Any = None
        self._pc_filter: Any = None

    def start(self) -> None:
        """Start listener (called from executor). Connects, fetches initial data, starts loop."""
        _LOGGER.info("Starting vSphere event listener")
        self._client.connect_push()
        self._pc, self._pc_filter = self._client.create_property_filter(self._categories, self._entity_filter)
        self._do_initial_fetch()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"vsphere_event_listener_{self._entry_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the listener."""
        _LOGGER.info("Stopping vSphere event listener")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        if self._pc_filter:
            with contextlib.suppress(Exception):
                self._pc_filter.Destroy()
        self._client.disconnect_push()

    def _do_initial_fetch(self) -> None:
        """Fetch full state via poll connection to populate VSphereData."""
        self._client.ensure_poll_connection()
        initial_data: dict[str, Any] = {}

        if self._categories.get(Category.HOSTS):
            initial_data["hosts"] = self._apply_filter(self._client.get_hosts(), Category.HOSTS)
        if self._categories.get(Category.VMS):
            initial_data["vms"] = self._apply_filter(self._client.get_vms(), Category.VMS)
        if self._categories.get(Category.DATASTORES):
            initial_data["datastores"] = self._apply_filter(self._client.get_datastores(), Category.DATASTORES)
        if self._categories.get(Category.LICENSES):
            initial_data["licenses"] = self._client.get_licenses()
        if self._categories.get(Category.CLUSTERS):
            initial_data["clusters"] = self._apply_filter(self._client.get_clusters(), Category.CLUSTERS)
        if self._categories.get(Category.NETWORK):
            initial_data["networks"] = self._client.get_networks()
        if self._categories.get(Category.RESOURCE_POOLS):
            initial_data["resource_pools"] = self._apply_filter(
                self._client.get_resource_pools(), Category.RESOURCE_POOLS
            )

        self._hass.loop.call_soon_threadsafe(self._vsphere_data.async_set_initial_data, initial_data)
        _LOGGER.info(
            "Initial fetch: %d hosts, %d VMs, %d datastores, %d licenses, %d clusters, %d networks, %d resource_pools",
            len(initial_data.get("hosts", {})),
            len(initial_data.get("vms", {})),
            len(initial_data.get("datastores", {})),
            len(initial_data.get("licenses", {})),
            len(initial_data.get("clusters", {})),
            len(initial_data.get("networks", {})),
            len(initial_data.get("resource_pools", {})),
        )

    def _apply_filter(self, data: dict[str, Any], category: str) -> dict[str, Any]:
        """Apply entity filter (all vs select specific morefs)."""
        filter_config: dict[str, Any] = self._entity_filter.get(category, {})
        if filter_config.get("mode", "all") == "all":
            return data
        selected: set[str] = set(filter_config.get("morefs", []))
        return {k: v for k, v in data.items() if k in selected}

    def _run_loop(self) -> None:
        """Background thread: WaitForUpdatesEx loop."""
        version = ""
        backoff_index = 0
        while not self._stop_event.is_set():
            try:
                wait_options = self._create_wait_options()
                update_set = self._pc.WaitForUpdatesEx(version, wait_options)
                if update_set is None:
                    continue  # timeout, no changes
                version = update_set.version
                backoff_index = 0
                for filter_update in update_set.filterSet:
                    for obj_update in filter_update.objectSet:
                        self._process_object_update(obj_update)
            except Exception as err:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                err_str = str(err).lower()
                if "login" in err_str or "auth" in err_str or "credential" in err_str:
                    _LOGGER.error("Auth error in event listener: %s", err)
                    self._hass.loop.call_soon_threadsafe(self._trigger_reauth)
                    break
                delay = BACKOFF_SCHEDULE[min(backoff_index, len(BACKOFF_SCHEDULE) - 1)]
                _LOGGER.warning("Event listener error: %s. Reconnecting in %ds", err, delay)
                backoff_index += 1
                self._stop_event.wait(delay)
                if self._stop_event.is_set():
                    break
                try:
                    self._reconnect()
                    version = ""
                except Exception as reconnect_err:  # noqa: BLE001
                    _LOGGER.error("Reconnect failed: %s", reconnect_err)

    def _create_wait_options(self) -> Any:
        """Create WaitOptions with maxWaitSeconds set."""
        from pyVmomi import vmodl  # noqa: PLC0415

        options = vmodl.query.PropertyCollector.WaitOptions()
        options.maxWaitSeconds = WAIT_OPTIONS_MAX_WAIT
        return options

    def _process_object_update(self, obj_update: Any) -> None:
        """Process a single ObjectUpdate."""
        obj = obj_update.obj
        moref: str = str(obj._moId)  # noqa: SLF001
        kind: str = obj_update.kind

        category = self._obj_type_to_category(type(obj))
        if not category:
            return

        if kind == "leave":
            self._hass.loop.call_soon_threadsafe(self._vsphere_data.async_remove_object, category, moref)
            self._fire_event(
                "vsphere_inventory_change",
                {
                    "entry_id": self._entry_id,
                    "action": "removed",
                    "entity_type": category.rstrip("s"),
                    "entity_moref": moref,
                },
            )
            return

        properties: dict[str, Any] = {}
        for change in obj_update.changeSet:
            properties[change.name] = change.val

        if kind == "enter":
            filter_config: dict[str, Any] = self._entity_filter.get(category, {})
            if filter_config.get("mode", "all") == "select" and moref not in set(filter_config.get("morefs", [])):
                return
            self._fire_event(
                "vsphere_inventory_change",
                {
                    "entry_id": self._entry_id,
                    "action": "added",
                    "entity_type": category.rstrip("s"),
                    "entity_moref": moref,
                    "entity_name": properties.get("summary.config.name", properties.get("name", moref)),
                },
            )

        self._hass.loop.call_soon_threadsafe(
            self._vsphere_data.async_update_from_push,
            category,
            moref,
            properties,
        )

    def _obj_type_to_category(self, obj_type: type) -> str | None:
        """Map a pyVmomi object type to a data category string."""
        from pyVmomi import vim  # noqa: PLC0415

        mapping: dict[type, str] = {
            vim.HostSystem: "hosts",
            vim.VirtualMachine: "vms",
            vim.Datastore: "datastores",
            vim.ClusterComputeResource: "clusters",
            vim.ResourcePool: "resource_pools",
        }
        return mapping.get(obj_type)

    def _fire_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Fire a Home Assistant event on the event bus."""
        self._hass.loop.call_soon_threadsafe(self._hass.bus.async_fire, event_type, data)

    def _trigger_reauth(self) -> None:
        """Trigger a config entry reload due to auth failure."""
        from homeassistant.config_entries import ConfigEntryState  # noqa: PLC0415

        entry = self._hass.config_entries.async_get_entry(self._entry_id)
        if entry and entry.state == ConfigEntryState.LOADED:
            self._hass.config_entries.async_schedule_reload(self._entry_id)

    def _reconnect(self) -> None:
        """Disconnect and reconnect the push connection, re-creating the property filter."""
        _LOGGER.info("Reconnecting event listener")
        self._client.disconnect_push()
        self._client.connect_push()
        if self._pc_filter:
            with contextlib.suppress(Exception):
                self._pc_filter.Destroy()
        self._pc, self._pc_filter = self._client.create_property_filter(self._categories, self._entity_filter)
        self._do_initial_fetch()
