"""vSphere client — synchronous pyVmomi wrapper.

This is the ONLY file in the integration that imports pyVmomi.
All methods are synchronous and designed to run in hass.async_add_executor_job().
"""

from __future__ import annotations

import logging
import ssl
import time
from typing import Any

from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim, vmodl

from .const import (
    CONN_TYPE_ESXI,
    CONN_TYPE_VCENTER,
    INVALID_LICENSE_KEY,
    SNAP_ALL,
    SNAP_FIRST,
    SNAP_LAST,
    VM_POWER_ON,
)
from .exceptions import VSphereAuthError, VSphereConnectionError, VSphereOperationError

_LOGGER = logging.getLogger(__name__)

_TASK_POLL_INTERVAL = 2  # seconds
_TASK_TIMEOUT = 300  # seconds


class VSphereClient:
    """Synchronous pyVmomi wrapper for all vSphere API interaction."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        verify_ssl: bool,
    ) -> None:
        """Initialize VSphereClient."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._verify_ssl = verify_ssl

        self._push_conn: Any = None
        self._poll_conn: Any = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> Any:
        """Create a SmartConnect session; map pyVmomi faults to our exceptions."""
        ssl_context: ssl.SSLContext | None = None
        if not self._verify_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        try:
            conn = SmartConnect(
                host=self._host,
                port=self._port,
                user=self._username,
                pwd=self._password,
                sslContext=ssl_context,
            )
        except vim.fault.InvalidLogin as exc:
            raise VSphereAuthError(f"Invalid credentials for {self._host}: {exc}") from exc
        except vim.fault.PasswordExpired as exc:
            raise VSphereAuthError(f"Password expired for {self._host}: {exc}") from exc
        except (TimeoutError, ConnectionRefusedError, ssl.SSLError, OSError) as exc:
            raise VSphereConnectionError(f"Cannot connect to {self._host}:{self._port}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereConnectionError(f"vSphere method fault during connect to {self._host}: {exc}") from exc

        _LOGGER.debug("Connected to vSphere at %s:%s", self._host, self._port)
        return conn

    def _disconnect(self, conn: Any) -> None:
        """Safely disconnect a SmartConnect session."""
        if conn is not None:
            try:
                Disconnect(conn)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error during disconnect (ignored)", exc_info=True)

    # Persistent push connection (EventListener)

    def connect_push(self) -> None:
        """Open the persistent connection used by the EventListener."""
        self._push_conn = self._connect()

    def disconnect_push(self) -> None:
        """Close the persistent push connection."""
        self._disconnect(self._push_conn)
        self._push_conn = None

    @property
    def push_connection(self) -> Any:
        """Return the persistent push connection."""
        return self._push_conn

    # On-demand poll connection (perf/services)

    def connect_poll(self) -> None:
        """Open the on-demand poll connection."""
        self._poll_conn = self._connect()

    def disconnect_poll(self) -> None:
        """Close the on-demand poll connection."""
        self._disconnect(self._poll_conn)
        self._poll_conn = None

    @property
    def poll_connection(self) -> Any:
        """Return the on-demand poll connection."""
        return self._poll_conn

    def ensure_poll_connection(self) -> None:
        """Reconnect the poll connection if the session has died."""
        if self._poll_conn is None:
            self.connect_poll()
            return

        try:
            # Accessing currentTime() is a cheap liveness probe
            self._poll_conn.CurrentTime()
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Poll connection lost; reconnecting", exc_info=True)
            self._disconnect(self._poll_conn)
            self._poll_conn = None
            self.connect_poll()

    # ------------------------------------------------------------------
    # Connectivity test
    # ------------------------------------------------------------------

    def test_connection(self) -> dict[str, str]:
        """Test connectivity and return server info dict.

        Returns:
            dict with keys: type, name, version, build
        """
        conn = self._connect()
        try:
            content = conn.RetrieveContent()
            about = content.about
            conn_type = CONN_TYPE_VCENTER if "VirtualCenter" in (about.apiType or "") else CONN_TYPE_ESXI
            return {
                "type": conn_type,
                "name": about.fullName or "",
                "version": about.version or "",
                "build": about.build or "",
            }
        finally:
            self._disconnect(conn)

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------

    def _get_container_view(self, conn: Any, obj_type: list[Any]) -> Any:
        """Return a ContainerView for the given object types."""
        content = conn.RetrieveContent()
        return content.viewManager.CreateContainerView(content.rootFolder, obj_type, True)

    # ------------------------------------------------------------------
    # Data fetching — return dict keyed by MoRef ID
    # ------------------------------------------------------------------

    def get_hosts(self) -> dict[str, dict[str, Any]]:
        """Fetch all HostSystem objects and return parsed host dicts."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.HostSystem])
            result: dict[str, dict[str, Any]] = {}
            for host in view.view:
                moref = host._moId  # noqa: SLF001
                result[moref] = self._parse_host(host, moref)
            view.Destroy()
            return result
        finally:
            self._disconnect(conn)

    def get_vms(self) -> dict[str, dict[str, Any]]:
        """Fetch all VirtualMachine objects and return parsed VM dicts."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.VirtualMachine])
            result: dict[str, dict[str, Any]] = {}
            for vm_obj in view.view:
                moref = vm_obj._moId  # noqa: SLF001
                result[moref] = self._parse_vm(vm_obj, moref)
            view.Destroy()
            return result
        finally:
            self._disconnect(conn)

    def get_datastores(self) -> dict[str, dict[str, Any]]:
        """Fetch all Datastore objects and return parsed dicts."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.Datastore])
            result: dict[str, dict[str, Any]] = {}
            for ds in view.view:
                moref = ds._moId  # noqa: SLF001
                try:
                    summary = ds.summary
                    info: dict[str, Any] = {
                        "name": summary.name,
                        "type": summary.type,
                        "accessible": summary.accessible,
                        "capacity_gb": round(summary.capacity / (1024**3), 2) if summary.capacity else 0.0,
                        "free_gb": round(summary.freeSpace / (1024**3), 2) if summary.freeSpace else 0.0,
                        "used_gb": round((summary.capacity - summary.freeSpace) / (1024**3), 2)
                        if summary.capacity and summary.freeSpace is not None
                        else 0.0,
                        "url": summary.url or "",
                    }
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Error parsing datastore %s", moref, exc_info=True)
                    info = {"name": moref}
                result[moref] = info
            view.Destroy()
            return result
        finally:
            self._disconnect(conn)

    def get_clusters(self) -> dict[str, dict[str, Any]]:
        """Fetch cluster information."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.ClusterComputeResource])
            clusters: dict[str, dict[str, Any]] = {}
            for cluster in view.view:
                moref = cluster._moId  # noqa: SLF001
                try:
                    config = cluster.configuration
                    summary = cluster.summary
                    clusters[moref] = {
                        "moref": moref,
                        "name": cluster.name,
                        "drs_enabled": config.drsConfig.enabled if config.drsConfig else False,
                        "drs_automation_level": str(config.drsConfig.defaultVmBehavior)
                        if config.drsConfig and config.drsConfig.enabled
                        else None,
                        "ha_enabled": config.dasConfig.enabled if config.dasConfig else False,
                        "ha_admission_control": config.dasConfig.admissionControlEnabled if config.dasConfig else False,
                        "total_hosts": summary.numHosts if summary else 0,
                        "effective_hosts": summary.numEffectiveHosts if summary else 0,
                        "total_cpu_mhz": summary.totalCpu if summary else 0,
                        "total_memory_mb": round(summary.totalMemory / (1024 * 1024), 0)
                        if summary and summary.totalMemory
                        else 0,
                        "vm_count": len(cluster.resourcePool.vm)
                        if cluster.resourcePool and cluster.resourcePool.vm
                        else 0,
                    }
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Error parsing cluster %s", moref, exc_info=True)
                    clusters[moref] = {"moref": moref, "name": moref}
            view.Destroy()
            return clusters
        finally:
            self._disconnect(conn)

    def get_networks(self) -> dict[str, dict[str, Any]]:
        """Fetch network information (vSwitches, port groups, physical NICs)."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.HostSystem])
            networks: dict[str, dict[str, Any]] = {}
            try:
                for host in view.view:
                    host_moref = host._moId  # noqa: SLF001
                    try:
                        host_name = host.summary.config.name
                        if host.summary.runtime.powerState != "poweredOn":
                            continue
                        net_sys = host.configManager.networkSystem
                        if not net_sys:
                            continue
                        net_info = net_sys.networkInfo

                        # Virtual switches
                        for vswitch in net_info.vswitch or []:
                            vs_moref = f"{host_moref}_vswitch_{vswitch.name}"
                            networks[vs_moref] = {
                                "moref": vs_moref,
                                "name": f"{host_name} - {vswitch.name}",
                                "type": "vswitch",
                                "num_ports": vswitch.numPorts,
                                "num_ports_available": vswitch.numPorts
                                - (vswitch.numPortsAvailable if hasattr(vswitch, "numPortsAvailable") else 0)
                                if vswitch.numPorts
                                else 0,
                                "mtu": vswitch.mtu,
                                "host_name": host_name,
                            }

                        # Physical NICs
                        for pnic in net_info.pnic or []:
                            pnic_moref = f"{host_moref}_pnic_{pnic.device}"
                            link_speed = pnic.linkSpeed
                            networks[pnic_moref] = {
                                "moref": pnic_moref,
                                "name": f"{host_name} - {pnic.device}",
                                "type": "pnic",
                                "link_up": link_speed is not None,
                                "speed_mbps": link_speed.speedMb if link_speed else None,
                                "mac": pnic.mac,
                                "driver": pnic.driver if hasattr(pnic, "driver") else None,
                                "host_name": host_name,
                            }

                        # Port groups
                        for pg in net_info.portgroup or []:
                            pg_moref = f"{host_moref}_pg_{pg.spec.name}"
                            networks[pg_moref] = {
                                "moref": pg_moref,
                                "name": f"{host_name} - {pg.spec.name}",
                                "type": "portgroup",
                                "vlan_id": pg.spec.vlanId,
                                "vswitch_name": pg.spec.vswitchName,
                                "host_name": host_name,
                            }
                    except Exception:  # noqa: BLE001
                        _LOGGER.debug("Error parsing network info for host %s", host_moref, exc_info=True)
            finally:
                view.Destroy()
            return networks
        finally:
            self._disconnect(conn)

    def get_resource_pools(self) -> dict[str, dict[str, Any]]:
        """Fetch resource pool information."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.ResourcePool])
            pools: dict[str, dict[str, Any]] = {}
            for pool in view.view:
                moref = pool._moId  # noqa: SLF001
                try:
                    config = pool.config
                    cpu_alloc = config.cpuAllocation if config else None
                    mem_alloc = config.memoryAllocation if config else None
                    pools[moref] = {
                        "moref": moref,
                        "name": pool.name,
                        "cpu_reservation_mhz": cpu_alloc.reservation if cpu_alloc else 0,
                        "cpu_limit_mhz": cpu_alloc.limit if cpu_alloc else -1,
                        "memory_reservation_mb": mem_alloc.reservation if mem_alloc else 0,
                        "memory_limit_mb": mem_alloc.limit if mem_alloc else -1,
                        "vm_count": len(pool.vm) if pool.vm else 0,
                    }
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Error parsing resource pool %s", moref, exc_info=True)
                    pools[moref] = {"moref": moref, "name": moref}
            view.Destroy()
            return pools
        finally:
            self._disconnect(conn)

    def get_licenses(self) -> dict[str, dict[str, Any]]:
        """Fetch license information; silently filter evaluation/invalid at DEBUG level."""
        conn = self._connect()
        try:
            content = conn.RetrieveContent()
            lm = content.licenseManager
            result: dict[str, dict[str, Any]] = {}

            for lic in lm.licenses:
                key = getattr(lic, "licenseKey", None) or ""
                product_name = getattr(lic, "name", None)

                # Skip invalid licenses (no WARNING spam)
                if not product_name or str(product_name) == "None":
                    _LOGGER.debug(
                        "Skipping invalid license: key=%s, product=%s",
                        key,
                        product_name,
                    )
                    continue
                if key == INVALID_LICENSE_KEY:
                    _LOGGER.debug("Skipping evaluation license: key=%s", key)
                    continue

                total = getattr(lic, "total", 0) or 0
                used = getattr(lic, "used", 0) or 0
                result[key] = {
                    "name": str(product_name),
                    "key": key,
                    "total": total,
                    "used": used,
                    "free": max(0, total - used),
                }

            return result
        finally:
            self._disconnect(conn)

    def get_alarms(self) -> dict[str, list[dict[str, Any]]]:
        """Fetch current triggered alarms for all hosts and VMs."""
        self.ensure_poll_connection()
        content = self._poll_conn.RetrieveContent()
        alarms: dict[str, list[dict[str, Any]]] = {}

        # Host alarms
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        try:
            for host in container.view:
                moref = str(host._moId)  # noqa: SLF001
                triggered = host.triggeredAlarmState
                if triggered:
                    alarms[moref] = self._parse_alarm_states(triggered, moref, "host")
        finally:
            container.Destroy()

        # VM alarms
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm_obj in container.view:
                moref = str(vm_obj._moId)  # noqa: SLF001
                triggered = vm_obj.triggeredAlarmState
                if triggered:
                    alarms[moref] = self._parse_alarm_states(triggered, moref, "vm")
        finally:
            container.Destroy()

        return alarms

    def _parse_alarm_states(self, alarm_states: Any, moref: str, entity_type: str) -> list[dict[str, Any]]:
        """Parse AlarmState objects into flat dicts."""
        result = []
        for alarm_state in alarm_states:
            try:
                result.append(
                    {
                        "alarm_key": str(alarm_state.key),
                        "alarm_name": str(alarm_state.alarm.info.name)
                        if hasattr(alarm_state.alarm, "info")
                        else str(alarm_state.alarm),
                        "status": str(alarm_state.overallStatus),
                        "time": str(alarm_state.time) if alarm_state.time else None,
                        "acknowledged": getattr(alarm_state, "acknowledged", False),
                        "entity_moref": moref,
                        "entity_type": entity_type,
                    }
                )
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to parse alarm state", exc_info=True)
        return result

    def enumerate_inventory(self) -> dict[str, dict[str, Any]]:
        """Lightweight inventory enumeration for config flow (morefs + names only)."""
        conn = self._connect()
        try:
            result: dict[str, dict[str, Any]] = {}

            for obj_type, category in [
                (vim.HostSystem, "host"),
                (vim.VirtualMachine, "vm"),
            ]:
                view = self._get_container_view(conn, [obj_type])
                for obj in view.view:
                    moref = obj._moId  # noqa: SLF001
                    try:
                        name = obj.summary.config.name
                    except Exception:  # noqa: BLE001
                        name = moref
                    result[moref] = {"moref": moref, "name": name, "type": category}
                view.Destroy()

            return result
        finally:
            self._disconnect(conn)

    # ------------------------------------------------------------------
    # Internal lookup helpers
    # ------------------------------------------------------------------

    def _get_vm_by_moref(self, moref: str) -> Any:
        """Find a VirtualMachine object by MoRef ID using the poll connection."""
        self.ensure_poll_connection()
        content = self._poll_conn.RetrieveContent()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm_obj in view.view:
                if vm_obj._moId == moref:  # noqa: SLF001
                    return vm_obj
        finally:
            view.Destroy()
        raise VSphereOperationError(f"VM with MoRef '{moref}' not found")

    def _get_host_by_moref(self, moref: str) -> Any:
        """Find a HostSystem object by MoRef ID using the poll connection."""
        self.ensure_poll_connection()
        content = self._poll_conn.RetrieveContent()
        view = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        try:
            for host in view.view:
                if host._moId == moref:  # noqa: SLF001
                    return host
        finally:
            view.Destroy()
        raise VSphereOperationError(f"Host with MoRef '{moref}' not found")

    # ------------------------------------------------------------------
    # Performance metrics
    # ------------------------------------------------------------------

    def query_performance(
        self,
        host_morefs: list[str],
        vm_morefs: list[str],
        datastore_morefs: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Query performance counters for hosts, VMs, and datastores.

        Returns dict keyed by moref with counter values.
        Uses the PerformanceManager's QueryPerf API with 20-second interval.
        """
        self.ensure_poll_connection()
        content = self._poll_conn.RetrieveContent()
        perf_manager = content.perfManager

        if not perf_manager:
            return {}

        # Get counter IDs for the metrics we care about
        counter_ids = self._get_counter_ids(perf_manager)
        if not counter_ids:
            return {}

        results: dict[str, dict[str, Any]] = {}

        # Query hosts
        for moref in host_morefs:
            try:
                host_obj = self._get_managed_object(vim.HostSystem, moref)
                if host_obj:
                    data = self._query_entity_perf(perf_manager, host_obj, counter_ids, "host")
                    if data:
                        results[moref] = data
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to query perf for host %s", moref, exc_info=True)

        # Query VMs
        for moref in vm_morefs:
            try:
                vm_obj = self._get_managed_object(vim.VirtualMachine, moref)
                if vm_obj:
                    data = self._query_entity_perf(perf_manager, vm_obj, counter_ids, "vm")
                    if data:
                        results[moref] = data
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to query perf for VM %s", moref, exc_info=True)

        # Query datastores
        for moref in datastore_morefs:
            try:
                ds_obj = self._get_managed_object(vim.Datastore, moref)
                if ds_obj:
                    data = self._query_entity_perf(perf_manager, ds_obj, counter_ids, "datastore")
                    if data:
                        results[moref] = data
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to query perf for datastore %s", moref, exc_info=True)

        return results

    def _get_counter_ids(self, perf_manager: Any) -> dict[str, int]:
        """Build a mapping of counter name → counter ID."""
        counters = {}
        for counter in perf_manager.perfCounter:
            group = counter.groupInfo.key
            name = counter.nameInfo.key
            rollup = counter.rollupType
            key = f"{group}.{name}.{rollup}"
            counters[key] = counter.key
        return counters

    def _get_managed_object(self, obj_type: type, moref: str) -> Any | None:
        """Get a managed object by type and MoRef string."""
        content = self._poll_conn.RetrieveContent()
        container = content.viewManager.CreateContainerView(content.rootFolder, [obj_type], True)
        try:
            for obj in container.view:
                if str(obj._moId) == moref:  # noqa: SLF001
                    return obj
        finally:
            container.Destroy()
        return None

    def _query_entity_perf(
        self,
        perf_manager: Any,
        entity: Any,
        counter_ids: dict[str, int],
        entity_type: str,
    ) -> dict[str, Any]:
        """Query performance counters for a single entity."""
        # Define which counters to query per entity type
        if entity_type in ("host", "vm"):
            wanted = {
                "cpu.usage.average": "cpu_usage_pct",
                "mem.active.average": "mem_active_kb",
                "net.received.average": "net_received_kbps",
                "net.transmitted.average": "net_transmitted_kbps",
                "disk.read.average": "disk_read_kbps",
                "disk.write.average": "disk_write_kbps",
            }
        elif entity_type == "datastore":
            wanted = {
                "datastore.totalReadLatency.average": "read_latency_ms",
                "datastore.totalWriteLatency.average": "write_latency_ms",
                "datastore.numberReadAveraged.average": "read_iops",
                "datastore.numberWriteAveraged.average": "write_iops",
            }
        else:
            return {}

        # Build metric IDs list
        metric_ids = []
        counter_key_map: dict[int, str] = {}
        for counter_name, result_key in wanted.items():
            cid = counter_ids.get(counter_name)
            if cid is not None:
                metric_id = vim.PerformanceManager.MetricId(counterId=cid, instance="")
                metric_ids.append(metric_id)
                counter_key_map[cid] = result_key

        if not metric_ids:
            return {}

        # Build query spec — get the latest single sample (interval 20 = realtime)
        query_spec = vim.PerformanceManager.QuerySpec(
            entity=entity,
            metricId=metric_ids,
            maxSample=1,
            intervalId=20,  # 20-second realtime interval
        )

        try:
            perf_results = perf_manager.QueryPerf(querySpec=[query_spec])
        except Exception:  # noqa: BLE001
            _LOGGER.debug("QueryPerf failed for %s", entity, exc_info=True)
            return {}

        if not perf_results:
            return {}

        data: dict[str, Any] = {}
        for result in perf_results:
            for val in result.value:
                result_key = counter_key_map.get(val.id.counterId)
                if result_key and val.value:
                    raw_value = val.value[-1]  # Latest sample
                    # Apply unit conversions
                    if result_key == "cpu_usage_pct":
                        data[result_key] = round(raw_value / 100, 2)  # hundredths of % → %
                    elif result_key.endswith("_kb"):
                        data[result_key] = round(raw_value / 1024, 2)  # KB → MB
                    elif result_key.endswith("_kbps"):
                        data[result_key] = round(raw_value / 1024, 2)  # KBps → MBps
                    elif result_key.endswith("_ms"):
                        data[result_key] = raw_value  # already ms
                    elif result_key.endswith("_iops"):
                        data[result_key] = raw_value  # already count/sec
                    else:
                        data[result_key] = raw_value

        return data

    # ------------------------------------------------------------------
    # Task management
    # ------------------------------------------------------------------

    def _wait_for_task(self, task: Any, description: str) -> Any:
        """Poll a vSphere task until completion; raise on error.

        Args:
            task: The vim.Task object to wait for.
            description: Human-readable description for log messages.

        Returns:
            task.info.result on success.

        Raises:
            VSphereOperationError: if the task fails or times out.
        """
        _LOGGER.debug("Waiting for task: %s", description)
        deadline = time.monotonic() + _TASK_TIMEOUT

        while time.monotonic() < deadline:
            state = task.info.state
            if state == vim.TaskInfo.State.success:
                _LOGGER.debug("Task succeeded: %s", description)
                return task.info.result
            if state == vim.TaskInfo.State.error:
                error = task.info.error
                raise VSphereOperationError(f"Task '{description}' failed: {error}")
            # queued or running — keep polling
            time.sleep(_TASK_POLL_INTERVAL)

        raise VSphereOperationError(f"Task '{description}' timed out after {_TASK_TIMEOUT}s")

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------

    def _list_snapshots(
        self,
        snapshots: list[Any],
        tree: bool = False,
    ) -> list[Any]:
        """Recursively traverse the snapshot tree and return a flat list."""
        result: list[Any] = []
        for snap in snapshots or []:
            if tree:
                result.append(snap)
            else:
                result.append(snap.snapshot)
            result.extend(self._list_snapshots(snap.childSnapshotList, tree=tree))
        return result

    def get_vm_storage_details(self) -> dict[str, dict[str, Any]]:
        """Fetch per-VM storage details (per-disk usage, policy compliance).

        Returns dict keyed by composite moref: "{vm_moref}_disk_{disk_key}"
        or "{vm_moref}_storage_summary".
        """
        self.ensure_poll_connection()
        content = self._poll_conn.RetrieveContent()
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        storage: dict[str, dict[str, Any]] = {}
        try:
            for vm_obj in container.view:
                vm_moref = str(vm_obj._moId)  # noqa: SLF001
                vm_name = vm_obj.summary.config.name

                # Per-disk storage from vm.layoutEx
                layout = getattr(vm_obj, "layoutEx", None)
                if not layout:
                    continue

                # Get virtual disks from config
                for device in vm_obj.config.hardware.device or []:
                    if not isinstance(device, vim.vm.device.VirtualDisk):
                        continue

                    label = device.deviceInfo.label  # e.g., "Hard disk 1"
                    if hasattr(device, "capacityInBytes") and device.capacityInBytes:
                        capacity_gb = round(device.capacityInBytes / (1024**3), 2)
                    elif hasattr(device, "capacityInKB") and device.capacityInKB:
                        capacity_gb = round(device.capacityInKB / (1024**2), 2)
                    else:
                        capacity_gb = 0.0

                    # Get backing file info
                    backing = device.backing
                    thin_provisioned = getattr(backing, "thinProvisioned", None)
                    datastore_name = None
                    if hasattr(backing, "datastore") and backing.datastore:
                        datastore_name = backing.datastore.name

                    disk_key = f"{vm_moref}_disk_{device.key}"
                    storage[disk_key] = {
                        "moref": disk_key,
                        "name": f"{vm_name} - {label}",
                        "vm_moref": vm_moref,
                        "vm_name": vm_name,
                        "label": label,
                        "capacity_gb": capacity_gb,
                        "thin_provisioned": thin_provisioned,
                        "datastore": datastore_name,
                    }

                # Storage summary per VM
                try:
                    if vm_obj.summary.storage:
                        committed = vm_obj.summary.storage.committed
                        uncommitted = vm_obj.summary.storage.uncommitted
                        unshared = vm_obj.summary.storage.unshared
                        storage[f"{vm_moref}_storage_summary"] = {
                            "moref": f"{vm_moref}_storage_summary",
                            "name": f"{vm_name} - Storage Summary",
                            "vm_moref": vm_moref,
                            "vm_name": vm_name,
                            "label": "Storage Summary",
                            "committed_gb": round(committed / (1024**3), 2) if committed else 0.0,
                            "uncommitted_gb": round(uncommitted / (1024**3), 2) if uncommitted else 0.0,
                            "unshared_gb": round(unshared / (1024**3), 2) if unshared else 0.0,
                        }
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Failed to get storage details for %s", vm_name, exc_info=True)
        finally:
            container.Destroy()
        return storage

    # ------------------------------------------------------------------
    # VM operations
    # ------------------------------------------------------------------

    def vm_power(self, vm_moref: str, action: str) -> None:
        """Execute a VM power/control action.

        Supported actions: power_on, power_off, shutdown, reboot, reset, suspend.
        Smart fallback: if VMware Tools unavailable for guest ops, falls back to
        hard power operation.
        """
        try:
            vm = self._get_vm_by_moref(vm_moref)
            task: Any = None

            if action == "power_on":
                task = vm.PowerOnVM_Task()

            elif action in ("power_off", "shutdown"):
                tools = getattr(getattr(vm.summary, "guest", None), "toolsStatus", None)
                if tools in ("toolsOk", "toolsOld"):
                    try:
                        vm.ShutdownGuest()
                        return  # fire-and-forget
                    except vim.fault.ToolsUnavailable:
                        _LOGGER.debug(
                            "Tools unavailable for ShutdownGuest on %s; falling back to PowerOffVM",
                            vm_moref,
                        )
                task = vm.PowerOffVM_Task()

            elif action in ("reboot",):
                tools = getattr(getattr(vm.summary, "guest", None), "toolsStatus", None)
                if tools in ("toolsOk", "toolsOld"):
                    try:
                        vm.RebootGuest()
                        return  # fire-and-forget
                    except vim.fault.ToolsUnavailable:
                        _LOGGER.debug(
                            "Tools unavailable for RebootGuest on %s; falling back to ResetVM",
                            vm_moref,
                        )
                task = vm.ResetVM_Task()

            elif action == "reset":
                task = vm.ResetVM_Task()

            elif action == "suspend":
                task = vm.SuspendVM_Task()

            else:
                raise VSphereOperationError(f"Unknown VM action: {action}")

            if task is not None:
                self._wait_for_task(task, f"{action} on {vm.name}")

        except VSphereOperationError:
            raise
        except vim.fault.InvalidPowerState as exc:
            raise VSphereOperationError(f"Invalid power state for VM {vm_moref}: {exc}") from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(f"Task already in progress for VM {vm_moref}: {exc}") from exc
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(f"Invalid state for VM {vm_moref}: {exc}") from exc
        except vim.fault.ResourceInUse as exc:
            raise VSphereOperationError(f"Resource in use for VM {vm_moref}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during {action} on VM {vm_moref}: {exc}") from exc

    def create_snapshot(
        self,
        vm_moref: str,
        name: str | None = None,
        description: str | None = None,
        memory: bool = False,
        quiesce: bool = False,
    ) -> None:
        """Create a VM snapshot."""
        try:
            vm = self._get_vm_by_moref(vm_moref)
            snap_name = name or f"snapshot-{int(time.time())}"
            snap_desc = description or ""
            task = vm.CreateSnapshot_Task(
                name=snap_name,
                description=snap_desc,
                memory=memory,
                quiesce=quiesce,
            )
            self._wait_for_task(task, f"create_snapshot on {vm.name}")
        except VSphereOperationError:
            raise
        except vim.fault.SnapshotFault as exc:
            raise VSphereOperationError(f"Snapshot fault for VM {vm_moref}: {exc}") from exc
        except vim.fault.InvalidPowerState as exc:
            raise VSphereOperationError(f"Invalid power state for snapshot on VM {vm_moref}: {exc}") from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(f"Task in progress for VM {vm_moref}: {exc}") from exc
        except vim.fault.InsufficientResourcesFault as exc:
            raise VSphereOperationError(f"Insufficient resources for snapshot on VM {vm_moref}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during create_snapshot on VM {vm_moref}: {exc}") from exc

    def remove_snapshot(self, vm_moref: str, which: str) -> None:
        """Remove VM snapshot(s).

        Args:
            vm_moref: MoRef ID of the VM.
            which: One of SNAP_ALL, SNAP_FIRST, or SNAP_LAST.
        """
        try:
            vm = self._get_vm_by_moref(vm_moref)
            snap_info = vm.snapshot

            if which == SNAP_ALL:
                if snap_info is None:
                    _LOGGER.debug("No snapshots to remove on VM %s", vm_moref)
                    return
                task = vm.RemoveAllSnapshots_Task()
                self._wait_for_task(task, f"remove_all_snapshots on {vm.name}")
                return

            if snap_info is None:
                _LOGGER.debug("No snapshots on VM %s; nothing to remove", vm_moref)
                return

            flat = self._list_snapshots(snap_info.rootSnapshotList)
            if not flat:
                _LOGGER.debug("Empty snapshot list on VM %s", vm_moref)
                return

            if which == SNAP_FIRST:
                target = flat[0]
            elif which == SNAP_LAST:
                target = flat[-1]
            else:
                raise VSphereOperationError(f"Unknown snapshot target '{which}'; expected all/first/last")

            task = target.RemoveSnapshot_Task(removeChildren=False)
            self._wait_for_task(task, f"remove_snapshot({which}) on {vm.name}")

        except VSphereOperationError:
            raise
        except vim.fault.NotFound:
            # Idempotent — already gone
            _LOGGER.debug("Snapshot not found on VM %s (already removed)", vm_moref)
        except vim.fault.SnapshotFault as exc:
            raise VSphereOperationError(f"Snapshot fault for VM {vm_moref}: {exc}") from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(f"Task in progress for VM {vm_moref}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during remove_snapshot on VM {vm_moref}: {exc}") from exc

    def vm_migrate(self, vm_moref: str, target_host_moref: str) -> None:
        """Migrate (vMotion) a VM to a target host."""
        self.ensure_poll_connection()
        vm_obj = self._get_vm_by_moref(vm_moref)
        target_host = self._get_host_by_moref(target_host_moref)
        vm_name = vm_obj.summary.config.name
        host_name = target_host.summary.config.name

        try:
            # RelocateVM with just the host change = live vMotion
            relocate_spec = vim.vm.RelocateSpec()
            relocate_spec.host = target_host
            # Use the target host's default resource pool
            if target_host.parent and hasattr(target_host.parent, "resourcePool"):
                relocate_spec.pool = target_host.parent.resourcePool

            task = vm_obj.RelocateVM_Task(spec=relocate_spec)
            self._wait_for_task(task, f"migrate {vm_name} to {host_name}")
        except vim.fault.MigrationFault as err:
            raise VSphereOperationError(f"Migration failed for {vm_name}: {err.msg}") from err
        except (vim.fault.InvalidState, vim.fault.InvalidHostState) as err:
            raise VSphereOperationError(f"Cannot migrate {vm_name} to {host_name}: {err.msg}") from err
        except vim.fault.InsufficientResourcesFault as err:
            raise VSphereOperationError(f"Insufficient resources on {host_name}: {err.msg}") from err
        except vmodl.MethodFault as err:
            raise VSphereOperationError(f"vSphere error during migration of {vm_name}: {err.msg}") from err

    # ------------------------------------------------------------------
    # Host operations
    # ------------------------------------------------------------------

    def host_power(self, host_moref: str, action: str, force: bool = False) -> None:
        """Shutdown or reboot a host.

        Safety check: refuses if VMs are running and force=False.
        """
        try:
            host = self._get_host_by_moref(host_moref)
            host_name = host.summary.config.name if host.summary.config else host_moref

            if not force:
                powered_vms = sum(
                    1
                    for vm in (host.vm or [])
                    if getattr(getattr(vm, "runtime", None), "powerState", None) == VM_POWER_ON
                )
                if powered_vms > 0:
                    raise VSphereOperationError(
                        f"Cannot {action} {host_name}: {powered_vms} VMs running. Use force=true to override"
                    )

            if action == "shutdown":
                task = host.ShutdownHost_Task(force=force)
            elif action == "reboot":
                task = host.RebootHost_Task(force=force)
            else:
                raise VSphereOperationError(f"Unknown host power action: {action}")

            self._wait_for_task(task, f"host_{action} on {host_name}")

        except VSphereOperationError:
            raise
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(f"Invalid state for host {host_moref}: {exc}") from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(f"Task in progress for host {host_moref}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during host_{action} on {host_moref}: {exc}") from exc

    def host_set_power_policy(self, host_moref: str, policy_name: str) -> None:
        """Change the power policy of a host."""
        try:
            host = self._get_host_by_moref(host_moref)
            power_sys = host.configManager.powerSystem
            if power_sys is None:
                raise VSphereOperationError(f"Host {host_moref} does not support power policy configuration")

            capability = host.config.powerSystemCapability
            available = capability.availablePolicy if capability else []
            for policy in available:
                if policy.shortName == policy_name or policy.key == policy_name:
                    power_sys.ConfigurePowerPolicy(key=policy.key)
                    return

            raise VSphereOperationError(f"Power policy '{policy_name}' not found on host {host_moref}")

        except VSphereOperationError:
            raise
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault setting power policy on {host_moref}: {exc}") from exc

    def host_set_maintenance_mode(self, host_moref: str, enable: bool) -> None:
        """Enter or exit maintenance mode for a host."""
        try:
            host = self._get_host_by_moref(host_moref)
            task = (
                host.EnterMaintenanceMode_Task(timeout=0, evacuatePoweredOffVms=True)
                if enable
                else host.ExitMaintenanceMode_Task(timeout=0)
            )
            host_name = host.summary.config.name if host.summary.config else host_moref
            action = "enter_maintenance" if enable else "exit_maintenance"
            self._wait_for_task(task, f"{action} on {host_name}")

        except VSphereOperationError:
            raise
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(f"Invalid state for host {host_moref}: {exc}") from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(f"Task in progress for host {host_moref}: {exc}") from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault setting maintenance mode on {host_moref}: {exc}") from exc

    # ------------------------------------------------------------------
    # Utility / service-response helpers
    # ------------------------------------------------------------------

    def list_hosts(self) -> list[dict[str, Any]]:
        """Return a list of host summaries for service responses."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.HostSystem])
            hosts: list[dict[str, Any]] = []
            for host in view.view:
                moref = host._moId  # noqa: SLF001
                try:
                    name = host.summary.config.name
                    state = str(host.summary.runtime.powerState)
                except Exception:  # noqa: BLE001
                    name = moref
                    state = "unknown"
                hosts.append({"moref": moref, "name": name, "power_state": state})
            view.Destroy()
            return hosts
        finally:
            self._disconnect(conn)

    def list_power_policies(self, host_moref: str) -> list[dict[str, Any]]:
        """Return available power policies for a host (for service responses)."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.HostSystem])
            try:
                for host in view.view:
                    if host._moId != host_moref:  # noqa: SLF001
                        continue
                    capability = host.config.powerSystemCapability if host.config else None
                    if not capability:
                        return []
                    return [
                        {
                            "key": p.key,
                            "short_name": p.shortName,
                            "name": p.name,
                        }
                        for p in (capability.availablePolicy or [])
                    ]
            finally:
                view.Destroy()
            return []
        finally:
            self._disconnect(conn)

    # ------------------------------------------------------------------
    # PropertyCollector support (EventListener)
    # ------------------------------------------------------------------

    def create_property_filter(
        self,
        categories: list[str],
        entity_filter: dict[str, Any],
    ) -> tuple[Any, Any]:
        """Create a PropertyCollector filter for the EventListener.

        Args:
            categories: List of category names to monitor.
            entity_filter: Entity filter configuration dict.

        Returns:
            Tuple of (property_collector, filter_obj).
        """
        conn = self._push_conn
        if conn is None:
            raise VSphereConnectionError("Push connection not established; call connect_push() first")

        content = conn.RetrieveContent()
        pc = content.propertyCollector

        # Build object specs based on categories
        obj_specs: list[Any] = []
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        prop_specs: list[Any] = []

        # Build base property lists for each watchable category
        host_props = ["summary.runtime.powerState", "summary.config.name"]
        vm_props = ["summary.runtime.powerState", "summary.config.name", "runtime.powerState"]

        # If events/alarms monitoring is on, also watch triggeredAlarmState on hosts and VMs
        if categories.get("events_alarms"):
            if "triggeredAlarmState" not in host_props:
                host_props = [*host_props, "triggeredAlarmState"]
            if "triggeredAlarmState" not in vm_props:
                vm_props = [*vm_props, "triggeredAlarmState"]

        type_map: dict[str, tuple[Any, list[str]]] = {
            "hosts": (
                vim.HostSystem,
                host_props,
            ),
            "vms": (
                vim.VirtualMachine,
                vm_props,
            ),
            "datastores": (
                vim.Datastore,
                ["summary.accessible", "summary.freeSpace"],
            ),
            "clusters": (
                vim.ClusterComputeResource,
                [
                    "name",
                    "configuration.drsConfig.enabled",
                    "configuration.drsConfig.defaultVmBehavior",
                    "configuration.dasConfig.enabled",
                    "configuration.dasConfig.admissionControlEnabled",
                    "summary.numHosts",
                    "summary.numEffectiveHosts",
                    "summary.totalCpu",
                    "summary.totalMemory",
                    "host",
                ],
            ),
            "resource_pools": (
                vim.ResourcePool,
                [
                    "name",
                    "config.cpuAllocation.reservation",
                    "config.cpuAllocation.limit",
                    "config.memoryAllocation.reservation",
                    "config.memoryAllocation.limit",
                    "vm",
                ],
            ),
        }

        # Also include hosts/vms with only triggeredAlarmState when events_alarms is on
        # but those categories are not being watched — tracked in type_map above via the
        # dynamically extended host_props/vm_props lists.

        for category in categories:
            if category not in type_map:
                continue
            obj_type, props = type_map[category]

            traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
            traversal_spec.name = f"traversal_{category}"
            traversal_spec.type = vim.Folder
            traversal_spec.path = "childEntity"
            traversal_spec.skip = False

            obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
            obj_spec.obj = content.rootFolder
            obj_spec.skip = True
            obj_spec.selectSet = [traversal_spec]
            obj_specs.append(obj_spec)

            prop_spec = vmodl.query.PropertyCollector.PropertySpec()
            prop_spec.type = obj_type
            prop_spec.all = False
            prop_spec.pathSet = props
            prop_specs.append(prop_spec)

        # When events_alarms is enabled but hosts/vms are not in categories,
        # add them with just triggeredAlarmState so alarm changes are still watched.
        if categories.get("events_alarms"):
            for category, obj_type in [("hosts", vim.HostSystem), ("vms", vim.VirtualMachine)]:
                if not categories.get(category):
                    traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
                    traversal_spec.name = f"traversal_{category}_alarms"
                    traversal_spec.type = vim.Folder
                    traversal_spec.path = "childEntity"
                    traversal_spec.skip = False

                    obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
                    obj_spec.obj = content.rootFolder
                    obj_spec.skip = True
                    obj_spec.selectSet = [traversal_spec]
                    obj_specs.append(obj_spec)

                    prop_spec = vmodl.query.PropertyCollector.PropertySpec()
                    prop_spec.type = obj_type
                    prop_spec.all = False
                    prop_spec.pathSet = ["triggeredAlarmState"]
                    prop_specs.append(prop_spec)

        filter_spec.objectSet = obj_specs
        filter_spec.propSet = prop_specs

        filter_obj = pc.CreateFilter(spec=filter_spec, partialUpdates=False)
        return pc, filter_obj

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_host(self, host: Any, moref: str) -> dict[str, Any]:
        """Extract host properties into a flat dict."""
        data: dict[str, Any] = {"moref": moref}

        try:
            summary = host.summary
            config = summary.config
            runtime = summary.runtime
            hardware = summary.hardware
            quick = summary.quickStats

            data["name"] = config.name if config else moref
            data["state"] = str(runtime.powerState) if runtime else "unknown"
            data["maintenance_mode"] = bool(runtime.inMaintenanceMode) if runtime else False
            data["version"] = config.product.version if config and config.product else ""
            data["build"] = config.product.build if config and config.product else ""

            # Capability
            cap = host.capability
            data["shutdown_supported"] = bool(cap.shutdownSupported) if cap else False

            # Uptime
            data["uptime_hours"] = round(quick.uptime / 3600, 2) if quick and quick.uptime else 0.0

            # CPU
            if hardware and hardware.cpuMhz and hardware.numCpuCores:
                data["cpu_total_ghz"] = round(hardware.cpuMhz * hardware.numCpuCores / 1000, 2)
            else:
                data["cpu_total_ghz"] = 0.0

            if quick and quick.overallCpuUsage is not None:
                data["cpu_usage_ghz"] = round(quick.overallCpuUsage / 1000, 2)
            else:
                data["cpu_usage_ghz"] = 0.0

            # Memory
            if hardware and hardware.memorySize:
                data["mem_total_gb"] = round(hardware.memorySize / (1024**3), 2)
            else:
                data["mem_total_gb"] = 0.0

            if quick and quick.overallMemoryUsage is not None:
                data["mem_usage_gb"] = round(quick.overallMemoryUsage / 1024, 2)
            else:
                data["mem_usage_gb"] = 0.0

            # VM count
            data["vm_count"] = len(host.vm) if host.vm else 0

            # Power policy
            try:
                ps_info = host.config.powerSystemInfo
                data["power_policy"] = ps_info.currentPolicy.shortName if ps_info and ps_info.currentPolicy else ""
            except Exception:  # noqa: BLE001
                data["power_policy"] = ""

            # Available power policies
            try:
                ps_cap = host.config.powerSystemCapability
                data["available_power_policies"] = [
                    {"key": p.key, "short_name": p.shortName, "name": p.name}
                    for p in (ps_cap.availablePolicy if ps_cap else [])
                ]
            except Exception:  # noqa: BLE001
                data["available_power_policies"] = []

        except Exception:  # noqa: BLE001
            _LOGGER.debug("Error parsing host %s", moref, exc_info=True)
            data.setdefault("name", moref)

        return data

    def _parse_vm(self, vm_obj: Any, moref: str) -> dict[str, Any]:
        """Extract VM properties into a flat dict."""
        data: dict[str, Any] = {"moref": moref}

        try:
            summary = vm_obj.summary
            cfg = summary.config
            runtime = vm_obj.runtime
            quick = summary.quickStats
            guest_summary = summary.guest

            data["name"] = cfg.name if cfg else moref
            data["uuid"] = cfg.uuid if cfg else ""
            data["cpu_count"] = cfg.numCpu if cfg else 0
            data["memory_allocated_mb"] = cfg.memorySizeMB if cfg else 0

            # Power state
            raw_state = str(runtime.powerState) if runtime else "unknown"
            data["power_state"] = raw_state

            state_map = {
                "poweredOn": "running",
                "poweredOff": "off",
                "suspended": "suspended",
            }
            data["state"] = state_map.get(raw_state, raw_state)

            # Storage
            storage = summary.storage
            data["used_space_gb"] = round(storage.committed / (1024**3), 2) if storage and storage.committed else 0.0

            # Tools
            data["tools_status"] = str(guest_summary.toolsStatus) if guest_summary else "toolsNotInstalled"

            # Host info
            if runtime and runtime.host:
                data["host_moref"] = runtime.host._moId  # noqa: SLF001
                try:
                    data["host_name"] = runtime.host.name
                except Exception:  # noqa: BLE001
                    data["host_name"] = data["host_moref"]
            else:
                data["host_moref"] = ""
                data["host_name"] = ""

            # Running-only metrics
            if raw_state == "poweredOn" and quick:
                if quick.overallCpuUsage is not None and runtime and runtime.maxCpuUsage:
                    data["cpu_use_pct"] = round(quick.overallCpuUsage / runtime.maxCpuUsage * 100, 1)
                else:
                    data["cpu_use_pct"] = 0.0

                data["memory_used_mb"] = quick.hostMemoryUsage or 0
                data["memory_active_mb"] = quick.guestMemoryUsage or 0
                data["uptime_hours"] = round(quick.uptimeSeconds / 3600, 2) if quick.uptimeSeconds else 0.0
            else:
                data["cpu_use_pct"] = 0.0
                data["memory_used_mb"] = 0
                data["memory_active_mb"] = 0
                data["uptime_hours"] = 0.0

            # Guest info
            data["guest_ip"] = guest_summary.ipAddress if guest_summary and guest_summary.ipAddress else ""
            data["guest_os"] = guest_summary.guestFullName if guest_summary and guest_summary.guestFullName else ""

            # Snapshot count
            snap_info = vm_obj.snapshot
            if snap_info:
                data["snapshot_count"] = len(self._list_snapshots(snap_info.rootSnapshotList))
            else:
                data["snapshot_count"] = 0

        except Exception:  # noqa: BLE001
            _LOGGER.debug("Error parsing VM %s", moref, exc_info=True)
            data.setdefault("name", moref)

        return data
