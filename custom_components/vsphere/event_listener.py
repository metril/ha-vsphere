"""PropertyCollector-based event listener for real-time vSphere updates."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from .const import Category

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .coordinator import VSphereData
    from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)

BACKOFF_SCHEDULE: list[int] = [5, 10, 30, 60]
WAIT_OPTIONS_MAX_WAIT: int = 60

# PropertyCollector path → flat entity key translation maps
_HOST_PROP_MAP: dict[str, str] = {
    "summary.config.name": "name",
    "summary.runtime.powerState": "state",
    "summary.runtime.inMaintenanceMode": "maintenance_mode",
    "summary.quickStats.uptime": "_uptime_raw",
    "summary.quickStats.overallCpuUsage": "_cpu_usage_raw",
    "summary.quickStats.overallMemoryUsage": "_mem_usage_raw",
    "summary.hardware.cpuMhz": "_cpu_mhz",
    "summary.hardware.numCpuCores": "_cpu_cores",
    "summary.hardware.memorySize": "_mem_bytes",
    "summary.config.product.version": "version",
    "summary.config.product.build": "build",
    "config.powerSystemInfo.currentPolicy.shortName": "power_policy",
    "capability.shutdownSupported": "shutdown_supported",
    "vm": "_vm_list",
}

_VM_PROP_MAP: dict[str, str] = {
    "summary.config.name": "name",
    "summary.config.numCpu": "cpu_count",
    "summary.config.memorySizeMB": "memory_allocated_mb",
    "summary.config.uuid": "uuid",
    "summary.config.guestFullName": "_configured_guest_os",
    "summary.runtime.powerState": "power_state",
    "runtime.powerState": "power_state",
    "summary.overallStatus": "status",
    "summary.quickStats.overallCpuUsage": "_cpu_usage_raw",
    "summary.quickStats.hostMemoryUsage": "memory_used_mb",
    "summary.quickStats.guestMemoryUsage": "memory_active_mb",
    "summary.quickStats.uptimeSeconds": "_uptime_raw",
    "summary.guest.toolsStatus": "tools_status",
    "summary.guest.ipAddress": "guest_ip",
    "summary.guest.guestFullName": "guest_os",
    "summary.storage.committed": "_storage_raw",
    "runtime.host": "_host_obj",
    "runtime.maxCpuUsage": "_max_cpu",
    "snapshot": "_snapshot_obj",
    "configStatus": "_config_status",
}

_DATASTORE_PROP_MAP: dict[str, str] = {
    "summary.name": "name",
    "summary.type": "type",
    "summary.capacity": "_capacity_raw",
    "summary.freeSpace": "_free_raw",
    "host": "_host_list",
    "vm": "_vm_list",
}


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
        self._containers: list[Any] = []
        self._event_baseline_time: float = 0.0
        # Local alarm state cache — written/read only on the background thread
        self._alarm_cache: dict[str, list[dict[str, Any]]] = {}
        # VM power state cache for ALL VMs (not just monitored) — tracks
        # (host_moref, power_state) so we can send deltas to the coordinator.
        # Populated by the batch query in _do_initial_fetch, updated by push.
        self._vm_power_cache: dict[str, tuple[str, str]] = {}
        # Suppresses delta dispatch during initial fetch / reconnect to prevent
        # double-counting when the push loop re-delivers enter events.
        self._initial_fetch_in_progress = False

    def start(self) -> None:
        """Start listener (called from executor). Connects, fetches initial data, starts loop."""
        _LOGGER.info("Starting vSphere event listener")
        self._client.connect_push()
        self._pc, self._pc_filter, self._containers = self._client.create_property_filter(
            self._categories, self._entity_filter
        )
        self._do_initial_fetch()
        self._fetch_recent_events()

        # Only start the push loop if we have a PropertyCollector filter
        if self._pc is not None and self._pc_filter is not None:
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name=f"vsphere_event_listener_{self._entry_id}",
                daemon=True,
            )
            self._thread.start()
        else:
            _LOGGER.info("No PropertyCollector filter — running in poll-only mode")

    def stop(self) -> None:
        """Stop the listener."""
        _LOGGER.info("Stopping vSphere event listener")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        if self._pc_filter:
            with contextlib.suppress(Exception):
                self._pc_filter.Destroy()
        for container in self._containers:
            with contextlib.suppress(Exception):
                container.Destroy()
        self._containers = []
        self._client.disconnect_push()

    def _do_initial_fetch(self) -> None:
        """Fetch full state via poll connection to populate VSphereData."""
        self._initial_fetch_in_progress = True
        try:
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
            if self._categories.get(Category.EVENTS_ALARMS):
                initial_data["alarms"] = self._client.get_alarms()
            if self._categories.get(Category.STORAGE_ADVANCED):
                initial_data["storage_advanced"] = self._client.get_vm_storage_details()

            # Compute running VM counts per host via batch PropertyCollector query.
            # Also populates _vm_power_cache for real-time delta tracking on push.
            if "hosts" in initial_data:
                vm_counts: dict[str, int] = {}
                try:
                    vm_counts, self._vm_power_cache = self._client.count_running_vms_by_host()
                except Exception:  # noqa: BLE001
                    _LOGGER.warning("Batch VM count failed; counts may only reflect monitored VMs", exc_info=True)
                    self._vm_power_cache = {}
                    if "vms" in initial_data:
                        for vm_moref, vm in initial_data["vms"].items():
                            hm = vm.get("host_moref", "")
                            ps = str(vm.get("power_state", ""))
                            self._vm_power_cache[vm_moref] = (hm, ps)
                            if hm and ps == "poweredOn":
                                vm_counts[hm] = vm_counts.get(hm, 0) + 1
                for host_moref, host_data in initial_data["hosts"].items():
                    host_data["vm_count"] = vm_counts.get(host_moref, 0)
                    # A powered-off host cannot run VMs regardless of stale VM states
                    if str(host_data.get("state", "")) != "poweredOn":
                        host_data["vm_count"] = 0
                _LOGGER.info(
                    "Running VM counts per host: %s (tracking %d VMs for deltas)",
                    {m: initial_data["hosts"][m]["vm_count"] for m in initial_data["hosts"]},
                    len(self._vm_power_cache),
                )

            # Block until async_set_initial_data has executed on the event loop.
            # This prevents push updates from writing into coordinator data that is
            # about to be replaced, and ensures _initial_fetch_in_progress is only
            # cleared AFTER the coordinator has the fresh data.
            async def _commit() -> None:
                self._vsphere_data.async_set_initial_data(initial_data)

            if self._stop_event.is_set():
                # HA is shutting down — skip the blocking commit
                self._hass.loop.call_soon_threadsafe(self._vsphere_data.async_set_initial_data, initial_data)
            else:
                future = asyncio.run_coroutine_threadsafe(_commit(), self._hass.loop)
                future.result(timeout=10)
        finally:
            self._initial_fetch_in_progress = False
        _LOGGER.info(
            "Initial fetch: %d hosts, %d VMs, %d datastores, %d licenses, "
            "%d clusters, %d networks, %d resource_pools, %d alarm entities, %d storage objects",
            len(initial_data.get("hosts", {})),
            len(initial_data.get("vms", {})),
            len(initial_data.get("datastores", {})),
            len(initial_data.get("licenses", {})),
            len(initial_data.get("clusters", {})),
            len(initial_data.get("networks", {})),
            len(initial_data.get("resource_pools", {})),
            len(initial_data.get("alarms", {})),
            len(initial_data.get("storage_advanced", {})),
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

        # Track VM power/host changes for ALL VMs (before entity filter) so
        # vm_count reflects the real running count, not just monitored VMs.
        if category == "vms":
            self._track_vm_power_delta(moref, kind, obj_update.changeSet)

        # Skip objects not in the entity filter (applies to all update kinds including leave)
        filter_config: dict[str, Any] = self._entity_filter.get(category, {})
        is_filtered_out = filter_config.get("mode", "all") == "select" and moref not in set(
            filter_config.get("morefs", [])
        )

        if kind == "leave":
            if not is_filtered_out:
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

        if is_filtered_out:
            return

        properties: dict[str, Any] = {}
        for change in obj_update.changeSet:
            properties[change.name] = change.val

        # Check for alarm state changes
        if self._categories.get(Category.EVENTS_ALARMS):
            for change in obj_update.changeSet:
                if change.name == "triggeredAlarmState":
                    entity_type = category.rstrip("s")  # "hosts" → "host"
                    self._process_alarm_update(moref, entity_type, change.val)

        # Fire vsphere_event for significant property changes (e.g. power state transitions)
        if kind == "modify" and category in ("hosts", "vms"):
            self._check_and_fire_vsphere_events(category, moref, properties)

        if kind == "enter":
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

        # Translate raw PropertyCollector paths to flat entity keys
        translated = self._translate_properties(category, properties, moref)

        self._hass.loop.call_soon_threadsafe(
            self._vsphere_data.async_update_from_push,
            category,
            moref,
            translated,
        )

    def _translate_properties(
        self, category: str, raw_props: dict[str, Any], moref: str | None = None
    ) -> dict[str, Any]:
        """Translate raw PropertyCollector paths to flat entity keys with derived values."""
        if category == "hosts":
            prop_map = _HOST_PROP_MAP
        elif category == "vms":
            prop_map = _VM_PROP_MAP
        elif category == "datastores":
            prop_map = _DATASTORE_PROP_MAP
        else:
            return raw_props

        translated: dict[str, Any] = {}
        for raw_key, value in raw_props.items():
            flat_key = prop_map.get(raw_key)
            translated[flat_key if flat_key else raw_key] = value

        # Pass existing stored data for lookups (e.g., max_cpu_mhz for CPU % derivation)
        stored = self._vsphere_data._data.get(category, {}).get(moref, {}) if moref else {}  # noqa: SLF001
        if category == "hosts":
            self._derive_host_values(translated, stored)
        elif category == "vms":
            self._derive_vm_values(translated, stored)
        elif category == "datastores":
            self._derive_datastore_values(translated, stored)

        return {k: v for k, v in translated.items() if not k.startswith("_")}

    def _derive_host_values(self, d: dict[str, Any], stored: dict[str, Any] | None = None) -> None:
        """Compute derived host values from raw inputs."""
        if "_uptime_raw" in d:
            val = d.pop("_uptime_raw")
            if val is not None:
                d["uptime_hours"] = round(val / 3600, 2)
        if "_cpu_usage_raw" in d:
            val = d.pop("_cpu_usage_raw")
            if val is not None:
                d["cpu_usage_ghz"] = round(val / 1000, 2)
        if "_mem_usage_raw" in d:
            val = d.pop("_mem_usage_raw")
            if val is not None:
                d["mem_usage_gb"] = round(val / 1024, 2)
        if "_cpu_mhz" in d or "_cpu_cores" in d:
            mhz = d.pop("_cpu_mhz", None)
            if mhz is None:
                mhz = (stored or {}).get("cpu_mhz")
            cores = d.pop("_cpu_cores", None)
            if cores is None:
                cores = (stored or {}).get("cpu_cores")
            if mhz and cores:
                d["cpu_total_ghz"] = round(mhz * cores / 1000, 2)
                d["cpu_mhz"] = mhz
                d["cpu_cores"] = cores
        if "_mem_bytes" in d:
            val = d.pop("_mem_bytes")
            if val:
                d["mem_total_gb"] = round(val / (1024**3), 2)
        # Discard _vm_list — vm_count is computed by coordinator._recompute_host_vm_counts
        # when VM power states change (no RPCs needed on push thread)
        d.pop("_vm_list", None)

    def _derive_vm_values(self, d: dict[str, Any], stored: dict[str, Any] | None = None) -> None:
        """Compute derived VM values from raw inputs."""
        if d.get("power_state") is not None:
            d["power_state"] = str(d["power_state"])
            d["state"] = {"poweredOn": "running", "poweredOff": "off", "suspended": "suspended"}.get(
                d["power_state"], d["power_state"]
            )
        if "_uptime_raw" in d:
            val = d.pop("_uptime_raw")
            if val is not None:
                d["uptime_hours"] = round(val / 3600, 2)
        if "_max_cpu" in d:
            d["max_cpu_mhz"] = d.pop("_max_cpu")
        if "_cpu_usage_raw" in d:
            usage = d.pop("_cpu_usage_raw")
            max_cpu = d.get("max_cpu_mhz") or (stored or {}).get("max_cpu_mhz")
            if usage is not None and max_cpu:
                d["cpu_use_pct"] = round((usage / max_cpu) * 100, 2)
            else:
                d["cpu_use_pct"] = 0.0
        if "_storage_raw" in d:
            val = d.pop("_storage_raw")
            if val:
                d["used_space_gb"] = round(val / (1024**3), 2)
        if "_host_obj" in d:
            host_obj = d.pop("_host_obj")
            if host_obj:
                # Only read _moId (local attribute). Don't access host_obj.name —
                # it triggers a live RPC on the push connection from this thread.
                with contextlib.suppress(Exception):
                    host_moref = str(host_obj._moId)
                    d["host_moref"] = host_moref
                    # Look up host_name from coordinator's hosts data (in-memory, GIL-safe)
                    host_data = self._vsphere_data._data.get("hosts", {}).get(host_moref)  # noqa: SLF001
                    if host_data:
                        d["host_name"] = host_data.get("name", host_moref)
        if "_snapshot_obj" in d:
            snap_obj = d.pop("_snapshot_obj")
            if snap_obj is not None and hasattr(snap_obj, "rootSnapshotList"):
                snapshots = self._flatten_snapshots(snap_obj.rootSnapshotList)
                d["snapshot_count"] = len(snapshots)
                d["snapshots"] = snapshots
            else:
                d["snapshot_count"] = 0
                d["snapshots"] = []
        d.pop("_configured_guest_os", None)
        d.pop("_config_status", None)

    def _derive_datastore_values(self, d: dict[str, Any], stored: dict[str, Any] | None = None) -> None:
        """Compute derived datastore values from raw inputs."""
        if "_capacity_raw" in d:
            val = d.pop("_capacity_raw")
            if val is not None:
                d["capacity_gb"] = round(val / (1024**3), 2)
        if "_free_raw" in d:
            val = d.pop("_free_raw")
            if val is not None:
                d["free_gb"] = round(val / (1024**3), 2)
        # Recompute used_gb from whichever values are available (delta or stored)
        cap = d.get("capacity_gb") if "capacity_gb" in d else (stored or {}).get("capacity_gb")
        free = d.get("free_gb") if "free_gb" in d else (stored or {}).get("free_gb")
        if cap is not None and free is not None:
            d["used_gb"] = round(max(cap - free, 0.0), 2)
        if "_host_list" in d:
            val = d.pop("_host_list")
            d["connected_hosts"] = len(val) if val else 0
            if val:
                with contextlib.suppress(Exception):
                    d["host_morefs"] = [str(h.key._moId) for h in val]  # noqa: SLF001
        if "_vm_list" in d:
            val = d.pop("_vm_list")
            d["virtual_machines"] = len(val) if val else 0

    @staticmethod
    def _flatten_snapshots(snapshot_list: Any) -> list[dict[str, str]]:
        """Recursively flatten a snapshot tree into a list of {name, moref} dicts."""
        result: list[dict[str, str]] = []
        for snap in snapshot_list or []:
            moref_str = ""
            with contextlib.suppress(Exception):
                moref_str = str(snap.snapshot._moId)  # noqa: SLF001
            if moref_str:
                result.append({"name": snap.name, "moref": moref_str})
            result.extend(VSphereEventListener._flatten_snapshots(snap.childSnapshotList))
        return result

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

    def _fetch_recent_events(self) -> None:
        """Fetch recent events and set up event monitoring baseline."""
        if not self._categories.get(Category.EVENTS_ALARMS):
            return
        # Store the current time as our baseline — only fire events for changes after this point
        self._event_baseline_time = time.time()

    def _track_vm_power_delta(self, vm_moref: str, kind: str, change_set: Any) -> None:
        """Track a VM's power state / host for incremental vm_count updates.

        Runs for ALL VMs (before entity filter) so the host Running VMs count
        reflects reality regardless of which VMs are monitored as entities.
        """
        if kind == "leave":
            old_host, old_state = self._vm_power_cache.pop(vm_moref, ("", ""))
            if not self._initial_fetch_in_progress and old_host and old_state == "poweredOn":
                self._hass.loop.call_soon_threadsafe(self._vsphere_data.adjust_host_vm_count, old_host, -1)
            return

        # Extract power state, host, and template flag from the changeset
        new_power: str | None = None
        new_host_moref: str | None = None
        is_template = False
        for change in change_set:
            if change.name in ("runtime.powerState", "summary.runtime.powerState"):
                new_power = str(change.val) if change.val else ""
            elif change.name == "runtime.host" and change.val:
                with contextlib.suppress(Exception):
                    new_host_moref = str(change.val._moId)  # noqa: SLF001
            elif change.name == "config.template" and change.val:
                is_template = True

        # Skip template VMs — they don't count as running VMs
        if is_template:
            return

        if new_power is None and new_host_moref is None:
            return  # no relevant changes

        old_host, old_state = self._vm_power_cache.get(vm_moref, ("", ""))
        cur_host = new_host_moref if new_host_moref is not None else old_host
        cur_state = new_power if new_power is not None else old_state

        # Always update cache (needed for subsequent deltas)
        self._vm_power_cache[vm_moref] = (cur_host, cur_state)

        # Suppress delta dispatch during initial fetch — batch count is authoritative
        if self._initial_fetch_in_progress:
            return

        was_on = old_state == "poweredOn"
        is_on = cur_state == "poweredOn"

        if old_host != cur_host:
            # Host changed (vMotion or initial placement)
            if was_on and old_host:
                self._hass.loop.call_soon_threadsafe(self._vsphere_data.adjust_host_vm_count, old_host, -1)
            if is_on and cur_host:
                self._hass.loop.call_soon_threadsafe(self._vsphere_data.adjust_host_vm_count, cur_host, 1)
        elif was_on != is_on:
            # Same host, power state changed
            delta = 1 if is_on else -1
            if cur_host:
                self._hass.loop.call_soon_threadsafe(self._vsphere_data.adjust_host_vm_count, cur_host, delta)

    def _check_and_fire_vsphere_events(self, category: str, moref: str, properties: dict[str, Any]) -> None:
        """Fire vsphere_event for significant property changes."""
        if not self._categories.get(Category.EVENTS_ALARMS):
            return

        # Detect power state changes (VMs may report via either property path)
        if category == "hosts":
            power_val = properties.get("summary.runtime.powerState")
        else:
            power_val = properties.get("runtime.powerState") or properties.get("summary.runtime.powerState")
        if power_val is None:
            return

        new_state = str(power_val)
        entity_type = category.rstrip("s")

        # Map power state to event class name
        event_class_map: dict[str, str] = {
            "poweredOn": f"{'Vm' if entity_type == 'vm' else 'Host'}PoweredOnEvent",
            "poweredOff": f"{'Vm' if entity_type == 'vm' else 'Host'}PoweredOffEvent",
            "suspended": "VmSuspendedEvent",
        }
        event_class = event_class_map.get(new_state, f"{entity_type.title()}StateChangeEvent")

        # Try to get entity name
        name_key = "summary.config.name"
        entity_name = properties.get(name_key, moref)

        self._fire_event(
            "vsphere_event",
            {
                "entry_id": self._entry_id,
                "event_class": event_class,
                "entity_type": entity_type,
                "entity_moref": moref,
                "entity_name": entity_name,
                "message": f"{entity_name} is {new_state}",
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )

    def _process_alarm_update(self, moref: str, entity_type: str, alarm_states: Any) -> None:
        """Process triggeredAlarmState property change."""
        alarms = []
        if alarm_states:
            for alarm_state in alarm_states:
                try:
                    # Use alarm_state.key as the name — accessing alarm_state.alarm.info.name
                    # would trigger a live RPC on the push thread (forbidden by architecture)
                    alarm_info = {
                        "alarm_key": str(alarm_state.key),
                        "alarm_name": str(alarm_state.key),
                        "status": str(alarm_state.overallStatus),
                        "time": str(alarm_state.time) if alarm_state.time else None,
                        "acknowledged": getattr(alarm_state, "acknowledged", False),
                        "entity_moref": moref,
                        "entity_type": entity_type,
                    }
                    alarms.append(alarm_info)
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Failed to parse alarm state for %s", moref, exc_info=True)

        # Get previous alarm states from local cache (thread-safe — no coordinator read)
        old_alarms = self._alarm_cache.get(moref, [])
        old_statuses = {a.get("alarm_key"): a.get("status") for a in old_alarms}

        # Entity name: use moref as a safe fallback (no cross-thread coordinator read)
        entity_name: str = moref

        # Fire events for changed alarms — skip first-seen (no prior record) to avoid
        # spurious events on initial load
        for alarm in alarms:
            old_status = old_statuses.get(alarm["alarm_key"])
            # Skip alarms that have no prior state — these are first-seen during initial load
            if old_status is None:
                continue
            if old_status != alarm["status"]:
                self._fire_event(
                    "vsphere_alarm_triggered",
                    {
                        "entry_id": self._entry_id,
                        "entity_type": entity_type,
                        "entity_moref": moref,
                        "entity_name": entity_name,
                        "alarm_key": alarm["alarm_key"],
                        "alarm_name": alarm["alarm_name"],
                        "old_status": old_status,
                        "new_status": alarm["status"],
                        "time": alarm["time"],
                        "acknowledged": alarm["acknowledged"],
                    },
                )

        # Update local cache (this thread is the sole writer)
        self._alarm_cache[moref] = alarms

        # Push to coordinator via the event loop
        self._hass.loop.call_soon_threadsafe(self._update_alarms, moref, alarms)

    def _update_alarms(self, moref: str, alarms: list[dict[str, Any]]) -> None:
        """Update alarm data on the coordinator."""
        self._vsphere_data._data.setdefault("alarms", {})[moref] = alarms  # noqa: SLF001
        self._vsphere_data.async_set_updated_data(self._vsphere_data._data)  # noqa: SLF001

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
        # Destroy filter and containers on the old session BEFORE disconnect
        if self._pc_filter:
            with contextlib.suppress(Exception):
                self._pc_filter.Destroy()
        for container in self._containers:
            with contextlib.suppress(Exception):
                container.Destroy()
        self._containers = []
        self._client.disconnect_push()
        self._client.connect_push()
        self._pc, self._pc_filter, self._containers = self._client.create_property_filter(
            self._categories, self._entity_filter
        )
        self._alarm_cache.clear()
        self._vm_power_cache.clear()
        self._do_initial_fetch()
