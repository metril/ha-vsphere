"""vSphere client — synchronous pyVmomi wrapper.

This is the ONLY file in the integration that imports pyVmomi.
All methods are synchronous and designed to run in hass.async_add_executor_job().
"""

from __future__ import annotations

import contextlib
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
        ssl_ca_path: str = "",
    ) -> None:
        """Initialize VSphereClient.

        Args:
            ssl_ca_path: Optional path to a custom CA certificate file (PEM).
                         Used when verify_ssl is True and the server uses a
                         certificate signed by a private/internal CA.
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._verify_ssl = verify_ssl
        self._ssl_ca_path = ssl_ca_path

        self._push_conn: Any = None
        self._poll_conn: Any = None
        self._counter_cache: dict[str, int] | None = None

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
        elif self._ssl_ca_path:
            # Custom CA — verify against the provided certificate file
            ssl_context = ssl.create_default_context(cafile=self._ssl_ca_path)
            _LOGGER.debug("Using custom CA certificate: %s", self._ssl_ca_path)

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
            self._counter_cache = None
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
            try:
                result: dict[str, dict[str, Any]] = {}
                for host in view.view:
                    moref = host._moId  # noqa: SLF001
                    result[moref] = self._parse_host(host, moref)
                return result
            finally:
                view.Destroy()
        finally:
            self._disconnect(conn)

    def count_running_vms_by_host(self) -> tuple[dict[str, int], dict[str, tuple[str, str]]]:
        """Batch-count running (non-template) VMs per host using PropertyCollector.

        Returns:
            Tuple of (counts, vm_power_cache) where:
            - counts: host moref → number of poweredOn non-template VMs
            - vm_power_cache: vm moref → (host_moref, power_state) for ALL
              non-template VMs.  EventListener uses this cache to track deltas
              on subsequent PropertyCollector push updates.
        """
        conn = self._connect()
        try:
            content = conn.RetrieveContent()
            pc = content.propertyCollector
            view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
            try:
                traversal = vmodl.query.PropertyCollector.TraversalSpec(
                    name="view_vms",
                    type=vim.view.ContainerView,
                    path="view",
                    skip=False,
                )
                obj_spec = vmodl.query.PropertyCollector.ObjectSpec(obj=view, skip=True, selectSet=[traversal])
                prop_spec = vmodl.query.PropertyCollector.PropertySpec(
                    type=vim.VirtualMachine,
                    pathSet=["runtime.powerState", "runtime.host", "config.template"],
                    all=False,
                )
                filter_spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec])
                options = vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=500)
                result = pc.RetrievePropertiesEx(specSet=[filter_spec], options=options)

                counts: dict[str, int] = {}
                cache: dict[str, tuple[str, str]] = {}
                while result:
                    for obj_content in result.objects:
                        vm_moref = obj_content.obj._moId  # noqa: SLF001
                        power_state = ""
                        host_moref = ""
                        is_template = False
                        for prop in obj_content.propSet or []:
                            if prop.name == "runtime.powerState":
                                power_state = str(prop.val)
                            elif prop.name == "runtime.host" and prop.val:
                                host_moref = prop.val._moId  # noqa: SLF001
                            elif prop.name == "config.template":
                                is_template = bool(prop.val)
                        if not is_template:
                            cache[vm_moref] = (host_moref, power_state)
                            if host_moref and power_state == "poweredOn":
                                counts[host_moref] = counts.get(host_moref, 0) + 1
                    if result.token:
                        result = pc.ContinueRetrievePropertiesEx(token=result.token)
                    else:
                        break
                return counts, cache
            finally:
                view.Destroy()
        finally:
            self._disconnect(conn)

    def get_vms(self) -> dict[str, dict[str, Any]]:
        """Fetch all VirtualMachine objects and return parsed VM dicts."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.VirtualMachine])
            try:
                result: dict[str, dict[str, Any]] = {}
                for vm_obj in view.view:
                    moref = vm_obj._moId  # noqa: SLF001
                    result[moref] = self._parse_vm(vm_obj, moref)
                return result
            finally:
                view.Destroy()
        finally:
            self._disconnect(conn)

    def get_datastores(self) -> dict[str, dict[str, Any]]:
        """Fetch all Datastore objects and return parsed dicts."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.Datastore])
            try:
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
                            "connected_hosts": len(ds.host) if ds.host else 0,
                            "host_morefs": [str(h.key._moId) for h in ds.host] if ds.host else [],  # noqa: SLF001
                            "virtual_machines": len(ds.vm) if ds.vm else 0,
                        }
                    except Exception:  # noqa: BLE001
                        _LOGGER.debug("Error parsing datastore %s", moref, exc_info=True)
                        info = {"name": moref}
                    result[moref] = info
                return result
            finally:
                view.Destroy()
        finally:
            self._disconnect(conn)

    def get_clusters(self) -> dict[str, dict[str, Any]]:
        """Fetch cluster information."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.ClusterComputeResource])
            try:
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
                            "ha_admission_control": config.dasConfig.admissionControlEnabled
                            if config.dasConfig
                            else False,
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
                return clusters
            finally:
                view.Destroy()
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
                        if str(host.summary.runtime.powerState) != "poweredOn":
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
                                "num_ports_used": vswitch.numPorts
                                - (vswitch.numPortsAvailable if hasattr(vswitch, "numPortsAvailable") else 0)
                                if vswitch.numPorts
                                else 0,
                                "mtu": vswitch.mtu,
                                "host_moref": host_moref,
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
                                "host_moref": host_moref,
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
                                "host_moref": host_moref,
                                "host_name": host_name,
                            }
                    except Exception:  # noqa: BLE001
                        _LOGGER.debug("Error parsing network info for host %s", host_moref, exc_info=True)
            finally:
                view.Destroy()

            # Distributed virtual switches (datacenter-level)
            # Wrapped in try/except so standalone ESXi or permission errors
            # don't discard the already-collected host-level network data.
            try:
                dvs_view = self._get_container_view(conn, [vim.DistributedVirtualSwitch])
                try:
                    for dvs in dvs_view.view:
                        dvs_moref = dvs._moId  # noqa: SLF001
                        try:
                            dvs_config = dvs.config
                            dvs_summary = dvs.summary
                            _binding_labels = {
                                "earlyBinding": "Static",
                                "lateBinding": "Dynamic",
                                "ephemeral": "Ephemeral",
                            }
                            networks[dvs_moref] = {
                                "moref": dvs_moref,
                                "name": dvs.name,
                                "type": "dvswitch",
                                "num_ports": dvs_summary.numPorts if dvs_summary else 0,
                                "max_ports": dvs_config.maxPorts if dvs_config else 0,
                                "mtu": dvs_config.maxMtu if dvs_config else 0,
                                "num_hosts": dvs_summary.numHosts if dvs_summary else 0,
                                "version": dvs_config.productInfo.version
                                if dvs_config and dvs_config.productInfo
                                else "",
                                "nioc_enabled": bool(getattr(dvs_config, "networkResourceManagementEnabled", False)),
                                "host_moref": "",
                                "host_name": "",
                            }

                            # Distributed port groups on this dvSwitch
                            for pg in dvs.portgroup or []:
                                pg_moref = pg._moId  # noqa: SLF001
                                try:
                                    pg_config = pg.config
                                    vlan_info = ""
                                    try:
                                        vlan_obj = pg_config.defaultPortConfig.vlan
                                        if hasattr(vlan_obj, "vlanId") and isinstance(vlan_obj.vlanId, int):
                                            vlan_info = str(vlan_obj.vlanId)
                                        elif hasattr(vlan_obj, "pvlanId"):
                                            vlan_info = f"pvlan:{vlan_obj.pvlanId}"
                                        elif hasattr(vlan_obj, "vlanId"):
                                            vlan_info = "trunk"
                                    except Exception:  # noqa: BLE001
                                        pass
                                    raw_binding = pg_config.type if pg_config else ""
                                    networks[pg_moref] = {
                                        "moref": pg_moref,
                                        "name": f"{dvs.name} - {pg_config.name}",
                                        "type": "dvportgroup",
                                        "vlan_id": vlan_info,
                                        "port_binding": _binding_labels.get(raw_binding, raw_binding),
                                        "num_ports": pg_config.numPorts if pg_config else 0,
                                        "dvswitch_name": dvs.name,
                                        "host_moref": "",
                                        "host_name": "",
                                    }
                                except Exception:  # noqa: BLE001
                                    _LOGGER.debug("Error parsing dvPortgroup %s", pg_moref, exc_info=True)
                        except Exception:  # noqa: BLE001
                            _LOGGER.debug("Error parsing dvSwitch %s", dvs_moref, exc_info=True)
                finally:
                    dvs_view.Destroy()
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error enumerating distributed virtual switches", exc_info=True)

            return networks
        finally:
            self._disconnect(conn)

    def get_resource_pools(self) -> dict[str, dict[str, Any]]:
        """Fetch resource pool information."""
        conn = self._connect()
        try:
            view = self._get_container_view(conn, [vim.ResourcePool])
            try:
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
                return pools
            finally:
                view.Destroy()
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

                # Parse properties for product name, expiration
                product = str(product_name)
                expiration_days: int | str = "never"
                for prop in getattr(lic, "properties", []):
                    if prop.key == "ProductName":
                        product = str(prop.value)
                    elif prop.key == "count_disabled":
                        expiration_days = "never"
                    elif prop.key == "expirationHours":
                        with contextlib.suppress(TypeError, ValueError):
                            expiration_days = round(int(prop.value) / 24)

                # Determine status from expiration
                if isinstance(expiration_days, int):
                    if expiration_days > 30:
                        status = "Ok"
                    elif expiration_days >= 1:
                        status = "Expiring Soon"
                    else:
                        status = "Expired"
                else:
                    status = "Ok"

                result[key] = {
                    "name": str(product_name),
                    "key": key,
                    "product": product,
                    "status": status,
                    "expiration_days": expiration_days,
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
                (vim.Datastore, "datastore"),
                (vim.ClusterComputeResource, "cluster"),
                (vim.ResourcePool, "resource_pool"),
            ]:
                view = self._get_container_view(conn, [obj_type])
                try:
                    for obj in view.view:
                        moref = obj._moId  # noqa: SLF001
                        try:
                            summary_config = getattr(getattr(obj, "summary", None), "config", None)
                            if summary_config is not None and hasattr(summary_config, "name"):
                                name = summary_config.name
                            elif hasattr(obj, "name"):
                                name = obj.name
                            else:
                                name = moref
                        except Exception:  # noqa: BLE001
                            name = moref
                        result[moref] = {"moref": moref, "name": name, "type": category}
                finally:
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
        """Build a mapping of counter name → counter ID (cached per connection)."""
        if self._counter_cache is not None:
            return self._counter_cache
        counters = {}
        for counter in perf_manager.perfCounter:
            group = counter.groupInfo.key
            name = counter.nameInfo.key
            rollup = counter.rollupType
            key = f"{group}.{name}.{str(rollup)}"
            counters[key] = counter.key
        self._counter_cache = counters
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
                "mem.active.average": "mem_active_mb",
                "net.received.average": "net_received_mbps",
                "net.transmitted.average": "net_transmitted_mbps",
                "disk.read.average": "disk_read_mbps",
                "disk.write.average": "disk_write_mbps",
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
                    elif result_key.endswith("_mb"):
                        data[result_key] = round(raw_value / 1024, 2)  # KB → MB
                    elif result_key.endswith("_mbps"):
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

    def _list_snapshot_objects(self, snapshots: list[Any]) -> list[Any]:
        """Recursively collect snapshot ManagedObject references."""
        result: list[Any] = []
        for snap in snapshots or []:
            result.append(snap.snapshot)
            result.extend(self._list_snapshot_objects(snap.childSnapshotList))
        return result

    def _list_snapshot_nodes(self, snapshots: list[Any]) -> list[Any]:
        """Recursively collect snapshot tree nodes (with name, childSnapshotList, etc.)."""
        result: list[Any] = []
        for snap in snapshots or []:
            result.append(snap)
            result.extend(self._list_snapshot_nodes(snap.childSnapshotList))
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
                if getattr(getattr(vm_obj, "config", None), "template", False):
                    continue
                vm_moref = str(vm_obj._moId)  # noqa: SLF001
                try:
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
                    _LOGGER.debug("Failed to get storage details for VM %s", vm_moref, exc_info=True)
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

            elif action == "shutdown":
                tools = getattr(getattr(vm.summary, "guest", None), "toolsStatus", None)
                if tools not in ("toolsOk", "toolsOld"):
                    raise VSphereOperationError(f"Cannot gracefully shut down VM {vm_moref}: VMware Tools not running")
                vm.ShutdownGuest()
                return  # fire-and-forget

            elif action == "power_off":
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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for {action} on VM {vm_moref}: missing vSphere privilege '{priv}'"
            ) from exc
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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for create_snapshot on VM {vm_moref}: missing vSphere privilege '{priv}'"
            ) from exc
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

            flat = self._list_snapshot_objects(snap_info.rootSnapshotList)
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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for remove_snapshot on VM {vm_moref}: missing vSphere privilege '{priv}'"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during remove_snapshot on VM {vm_moref}: {exc}") from exc

    def remove_snapshot_by_moref(self, vm_moref: str, snapshot_moref: str) -> None:
        """Remove a specific snapshot identified by its MoRef ID."""
        try:
            vm = self._get_vm_by_moref(vm_moref)
            if not vm.snapshot:
                raise VSphereOperationError(f"VM {vm_moref} has no snapshots")
            flat = self._list_snapshot_nodes(vm.snapshot.rootSnapshotList)
            for snap_node in flat:
                if str(snap_node.snapshot._moId) == snapshot_moref:  # noqa: SLF001
                    task = snap_node.snapshot.RemoveSnapshot_Task(removeChildren=False)
                    self._wait_for_task(task, f"remove snapshot '{snap_node.name}'")
                    return
            raise VSphereOperationError(f"Snapshot {snapshot_moref} not found on VM {vm_moref}")
        except VSphereOperationError:
            raise
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for remove_snapshot on VM {vm_moref}: missing vSphere privilege '{priv}'"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(f"vSphere fault during remove_snapshot on VM {vm_moref}: {exc}") from exc

    def vm_migrate(self, vm_moref: str, target_host_moref: str) -> None:
        """Migrate (vMotion) a VM to a target host."""
        self.ensure_poll_connection()
        vm_obj = self._get_vm_by_moref(vm_moref)
        target_host = self._get_host_by_moref(target_host_moref)
        vm_name = vm_moref
        host_name = target_host_moref
        try:
            vm_name = vm_obj.summary.config.name or vm_moref
            host_name = target_host.summary.config.name or target_host_moref

            # RelocateVM with just the host change = live vMotion
            relocate_spec = vim.vm.RelocateSpec()
            relocate_spec.host = target_host
            # Use the target host's default resource pool
            if target_host.parent and hasattr(target_host.parent, "resourcePool"):
                relocate_spec.pool = target_host.parent.resourcePool

            task = vm_obj.RelocateVM_Task(spec=relocate_spec)
            self._wait_for_task(task, f"migrate {vm_name} to {host_name}")
        except vim.fault.MigrationFault as err:
            raise VSphereOperationError(f"Migration failed for {vm_name}: {err}") from err
        except (vim.fault.InvalidState, vim.fault.InvalidHostState) as err:
            raise VSphereOperationError(f"Cannot migrate {vm_name} to {host_name}: {err}") from err
        except vim.fault.InsufficientResourcesFault as err:
            raise VSphereOperationError(f"Insufficient resources on {host_name}: {err}") from err
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for migrate on VM {vm_moref}: missing vSphere privilege '{priv}'"
            ) from exc
        except vmodl.MethodFault as err:
            raise VSphereOperationError(f"vSphere error during migration of {vm_name}: {err}") from err

    # ------------------------------------------------------------------
    # Host operations
    # ------------------------------------------------------------------

    def host_power(self, host_moref: str, action: str, force: bool = False) -> None:
        """Shutdown or reboot a host.

        When force=False, vCenter gracefully shuts down VMs in the configured
        auto startup/shutdown order before powering off/rebooting the host.
        When force=True, running VMs are hard-killed immediately.
        """
        try:
            host = self._get_host_by_moref(host_moref)
            host_name = host.summary.config.name if host.summary.config else host_moref

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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for {action} on host {host_moref}: missing vSphere privilege '{priv}'"
            ) from exc
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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for power policy change on host {host_moref}: missing vSphere privilege '{priv}'"
            ) from exc
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
        except vim.fault.NoPermission as exc:
            priv = getattr(exc, "privilegeId", "unknown")
            raise VSphereOperationError(
                f"Permission denied for maintenance mode on host {host_moref}: missing vSphere privilege '{priv}'"
            ) from exc
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
            try:
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
                return hosts
            finally:
                view.Destroy()
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

    def _get_category_type_info(self, category: str, categories: dict[str, bool]) -> tuple[Any, list[str]] | None:
        """Return (vim_type, property_paths) for a category, or None if not applicable."""
        host_props = [
            "summary.config.name",
            "summary.runtime.powerState",
            "summary.runtime.inMaintenanceMode",
            "summary.quickStats.uptime",
            "summary.quickStats.overallCpuUsage",
            "summary.quickStats.overallMemoryUsage",
            "summary.hardware.cpuMhz",
            "summary.hardware.numCpuCores",
            "summary.hardware.memorySize",
            "summary.config.product.version",
            "summary.config.product.build",
            "config.powerSystemInfo.currentPolicy.shortName",
            "capability.shutdownSupported",
            "vm",
        ]

        vm_props = [
            "summary.config.name",
            "summary.config.numCpu",
            "summary.config.memorySizeMB",
            "summary.config.uuid",
            "summary.config.guestFullName",
            "summary.runtime.powerState",
            "summary.overallStatus",
            "summary.quickStats.overallCpuUsage",
            "summary.quickStats.hostMemoryUsage",
            "summary.quickStats.guestMemoryUsage",
            "summary.quickStats.uptimeSeconds",
            "summary.guest.toolsStatus",
            "summary.guest.ipAddress",
            "summary.guest.guestFullName",
            "summary.storage.committed",
            "runtime.host",
            "runtime.powerState",
            "runtime.maxCpuUsage",
            "snapshot",
            "configStatus",
        ]

        # Add alarm properties if events_alarms is enabled
        if categories.get("events_alarms"):
            if category == "hosts":
                host_props.append("triggeredAlarmState")
            elif category == "vms":
                vm_props.append("triggeredAlarmState")

        mapping: dict[str, tuple[Any, list[str]]] = {
            "hosts": (vim.HostSystem, host_props),
            "vms": (vim.VirtualMachine, vm_props),
            "datastores": (
                vim.Datastore,
                [
                    "summary.name",
                    "summary.type",
                    "summary.capacity",
                    "summary.freeSpace",
                    "host",
                    "vm",
                ],
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

        # events_alarms is not a separate object type — alarms piggyback on hosts/vms
        if category == "events_alarms":
            return None

        return mapping.get(category)

    def create_property_filter(
        self,
        categories: dict[str, bool],
        entity_filter: dict[str, Any],  # noqa: ARG002  # reserved for future per-object filtering
    ) -> tuple[Any, Any, list[Any]]:
        """Create a PropertyCollector filter for the EventListener.

        Uses ContainerView for each object type so the traversal is fully
        recursive regardless of inventory depth.

        Args:
            categories: Mapping of category name → enabled bool.
            entity_filter: Entity filter configuration dict (reserved for future use).

        Returns:
            Tuple of (property_collector, filter_obj, containers).
            Caller must destroy the containers when done.
        """
        conn = self._push_conn
        if conn is None:
            raise VSphereConnectionError("Push connection not established; call connect_push() first")

        content = conn.RetrieveContent()
        pc = content.propertyCollector
        containers: list[Any] = []

        obj_specs: list[Any] = []
        prop_specs: list[Any] = []

        # Collect categories to watch, adding hosts/vms when events_alarms is on
        # even if those categories are not explicitly enabled.
        watch_categories: dict[str, bool] = dict(categories)
        if categories.get("events_alarms"):
            watch_categories["hosts"] = True
            watch_categories["vms"] = True

        for category, enabled in watch_categories.items():
            if not enabled:
                continue
            type_info = self._get_category_type_info(category, categories)
            if type_info is None:
                continue

            obj_type, properties = type_info

            # Create a ContainerView for this object type — fully recursive
            container = content.viewManager.CreateContainerView(
                content.rootFolder,
                [obj_type],
                True,  # recursive=True
            )
            containers.append(container)

            # Traversal spec: ContainerView → view (its contents)
            traversal = vmodl.query.PropertyCollector.TraversalSpec(
                name=f"traverse_{obj_type.__name__}",
                type=vim.view.ContainerView,
                path="view",
                skip=False,
            )

            # Object spec: start at the container, skip it, traverse into its contents
            obj_spec = vmodl.query.PropertyCollector.ObjectSpec(
                obj=container,
                skip=True,
                selectSet=[traversal],
            )
            obj_specs.append(obj_spec)

            # Property spec: what properties to watch on the target objects
            prop_spec = vmodl.query.PropertyCollector.PropertySpec(
                type=obj_type,
                pathSet=properties,
                all=False,
            )
            prop_specs.append(prop_spec)

        if not prop_specs:
            _LOGGER.info("No PropertyCollector-watchable categories enabled; push updates disabled")
            for c in containers:
                with contextlib.suppress(Exception):
                    c.Destroy()
            return None, None, []

        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
            objectSet=obj_specs,
            propSet=prop_specs,
        )

        filter_obj = pc.CreateFilter(filter_spec, partialUpdates=True)
        return pc, filter_obj, containers

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
                data["cpu_mhz"] = hardware.cpuMhz
                data["cpu_cores"] = hardware.numCpuCores
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

            # vm_count is computed post-fetch from VM data (avoids N RPCs per host)
            data["vm_count"] = 0

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
            data["status"] = str(summary.overallStatus) if summary.overallStatus else "gray"

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

            # Store max CPU for push path derivation
            if runtime and runtime.maxCpuUsage:
                data["max_cpu_mhz"] = runtime.maxCpuUsage

            # Running-only metrics
            if raw_state == "poweredOn" and quick:
                if quick.overallCpuUsage is not None and data.get("max_cpu_mhz"):
                    data["cpu_use_pct"] = round(quick.overallCpuUsage / data["max_cpu_mhz"] * 100, 2)
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

            # Snapshot count and list
            snap_info = vm_obj.snapshot
            if snap_info:
                nodes = self._list_snapshot_nodes(snap_info.rootSnapshotList)
                data["snapshot_count"] = len(nodes)
                data["snapshots"] = []
                for sn in nodes:
                    with contextlib.suppress(Exception):
                        data["snapshots"].append(
                            {"name": sn.name, "moref": str(sn.snapshot._moId)}  # noqa: SLF001
                        )
            else:
                data["snapshot_count"] = 0
                data["snapshots"] = []

        except Exception:  # noqa: BLE001
            _LOGGER.debug("Error parsing VM %s", moref, exc_info=True)
            data.setdefault("name", moref)

        return data
