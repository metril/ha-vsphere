"""vSphere client — synchronous pyVmomi wrapper.

This is the ONLY file in the integration that imports pyVmomi.
All methods are synchronous and designed to run in hass.async_add_executor_job().
"""

from __future__ import annotations

import contextlib
import logging
import ssl
import threading
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
    derive_vm_state,
)
from .exceptions import VSphereAuthError, VSphereConnectionError, VSphereOperationError

_LOGGER = logging.getLogger(__name__)

_TASK_POLL_INTERVAL = 2  # seconds
_TASK_TIMEOUT = 300  # seconds


# ----------------------------------------------------------------------
# Pure translation: PropertyCollector {path: value} dicts -> entity dicts.
# No pyVmomi dependency so they can be unit-tested with plain objects.
# ----------------------------------------------------------------------

_GB = 1024**3


def _moref_of(ref: Any) -> str:
    """Return the MoRef ID string of a managed-object reference (or a plain string)."""
    return ref if isinstance(ref, str) else str(ref._moId)  # noqa: SLF001


def _has_prefix(props: dict[str, Any], prefix: str) -> bool:
    return any(k.startswith(prefix) for k in props)


def _snapshot_nodes(snapshots: Any) -> list[Any]:
    """Recursively flatten a snapshot tree node list."""
    result: list[Any] = []
    for snap in snapshots or []:
        result.append(snap)
        result.extend(_snapshot_nodes(snap.childSnapshotList))
    return result


HOST_PROPS = [
    "summary.config.name",
    "summary.runtime.powerState",
    "summary.runtime.connectionState",
    "summary.runtime.inMaintenanceMode",
    "summary.config.product.version",
    "summary.config.product.build",
    "capability.shutdownSupported",
    "summary.quickStats.uptime",
    "summary.hardware.cpuMhz",
    "summary.hardware.numCpuCores",
    "summary.quickStats.overallCpuUsage",
    "summary.hardware.memorySize",
    "summary.quickStats.overallMemoryUsage",
    "config.powerSystemInfo.currentPolicy.shortName",
    "config.powerSystemCapability.availablePolicy",
]

VM_PROPS = [
    "summary.config.name",
    "summary.config.uuid",
    "summary.config.numCpu",
    "summary.config.memorySizeMB",
    "runtime.powerState",
    "runtime.connectionState",
    "runtime.host",
    "runtime.maxCpuUsage",
    "summary.overallStatus",
    "summary.storage.committed",
    "summary.guest.toolsStatus",
    "summary.guest.ipAddress",
    "summary.guest.guestFullName",
    "summary.quickStats.overallCpuUsage",
    "summary.quickStats.hostMemoryUsage",
    "summary.quickStats.guestMemoryUsage",
    "summary.quickStats.uptimeSeconds",
    "snapshot",
]

DATASTORE_PROPS = [
    "summary.name",
    "summary.type",
    "summary.accessible",
    "summary.capacity",
    "summary.freeSpace",
    "summary.url",
    "host",
    "vm",
]

CLUSTER_PROPS = [
    "name",
    "configuration.drsConfig.enabled",
    "configuration.drsConfig.defaultVmBehavior",
    "configuration.dasConfig.enabled",
    "configuration.dasConfig.admissionControlEnabled",
    "summary.numHosts",
    "summary.numEffectiveHosts",
    "summary.totalCpu",
    "summary.totalMemory",
    "resourcePool",
]

RESOURCE_POOL_PROPS = [
    "name",
    "config.cpuAllocation.reservation",
    "config.cpuAllocation.limit",
    "config.memoryAllocation.reservation",
    "config.memoryAllocation.limit",
    "vm",
]


def translate_host(moref: str, props: dict[str, Any]) -> dict[str, Any]:
    """Translate host PropertyCollector values into the flat host dict."""
    data: dict[str, Any] = {"moref": moref}
    data["name"] = props.get("summary.config.name") or moref
    data["state"] = str(props.get("summary.runtime.powerState", "unknown"))
    data["connection_state"] = str(props.get("summary.runtime.connectionState", "unknown"))
    data["maintenance_mode"] = bool(props.get("summary.runtime.inMaintenanceMode", False))
    data["version"] = props.get("summary.config.product.version") or ""
    data["build"] = props.get("summary.config.product.build") or ""
    data["shutdown_supported"] = bool(props.get("capability.shutdownSupported", False))

    uptime = props.get("summary.quickStats.uptime")
    data["uptime_hours"] = round(uptime / 3600, 2) if uptime else 0.0

    cpu_mhz = props.get("summary.hardware.cpuMhz")
    cores = props.get("summary.hardware.numCpuCores")
    if cpu_mhz and cores:
        data["cpu_mhz"] = cpu_mhz
        data["cpu_cores"] = cores
        data["cpu_total_ghz"] = round(cpu_mhz * cores / 1000, 2)
    else:
        data["cpu_total_ghz"] = 0.0

    cpu_usage = props.get("summary.quickStats.overallCpuUsage")
    data["cpu_usage_ghz"] = round(cpu_usage / 1000, 2) if cpu_usage is not None else 0.0

    mem_size = props.get("summary.hardware.memorySize")
    data["mem_total_gb"] = round(mem_size / _GB, 2) if mem_size else 0.0
    mem_usage = props.get("summary.quickStats.overallMemoryUsage")
    data["mem_usage_gb"] = round(mem_usage / 1024, 2) if mem_usage is not None else 0.0

    # vm_count is computed post-fetch from VM data
    data["vm_count"] = 0
    data["power_policy"] = props.get("config.powerSystemInfo.currentPolicy.shortName") or ""
    data["available_power_policies"] = [
        {"key": p.key, "short_name": p.shortName, "name": p.name}
        for p in (props.get("config.powerSystemCapability.availablePolicy") or [])
    ]
    return data


def translate_vm(moref: str, props: dict[str, Any], host_names: dict[str, str]) -> dict[str, Any]:
    """Translate VM PropertyCollector values into the flat VM dict.

    host_names maps host moref -> name (resolved from a bulk host fetch).
    """
    data: dict[str, Any] = {"moref": moref}
    data["name"] = props.get("summary.config.name") or moref
    data["uuid"] = props.get("summary.config.uuid") or ""
    data["cpu_count"] = props.get("summary.config.numCpu") or 0
    data["memory_allocated_mb"] = props.get("summary.config.memorySizeMB") or 0

    raw_state = str(props.get("runtime.powerState", "unknown"))
    raw_conn = str(props.get("runtime.connectionState", "unknown"))
    data["power_state"] = raw_state
    data["connection_state"] = raw_conn
    data["state"] = derive_vm_state(raw_state, raw_conn)
    status = props.get("summary.overallStatus")
    data["status"] = str(status) if status else "gray"

    committed = props.get("summary.storage.committed")
    data["used_space_gb"] = round(committed / _GB, 2) if committed else 0.0

    tools = props.get("summary.guest.toolsStatus")
    data["tools_status"] = str(tools) if tools is not None else "toolsNotInstalled"

    host_ref = props.get("runtime.host")
    if host_ref:
        host_moref = _moref_of(host_ref)
        data["host_moref"] = host_moref
        data["host_name"] = host_names.get(host_moref) or host_moref
    else:
        data["host_moref"] = ""
        data["host_name"] = ""

    max_cpu = props.get("runtime.maxCpuUsage")
    if max_cpu:
        data["max_cpu_mhz"] = max_cpu

    if raw_state == "poweredOn" and _has_prefix(props, "summary.quickStats."):
        cpu_usage = props.get("summary.quickStats.overallCpuUsage")
        if cpu_usage is not None and data.get("max_cpu_mhz"):
            data["cpu_use_pct"] = round(cpu_usage / data["max_cpu_mhz"] * 100, 2)
        else:
            data["cpu_use_pct"] = 0.0
        data["memory_used_mb"] = props.get("summary.quickStats.hostMemoryUsage") or 0
        data["memory_active_mb"] = props.get("summary.quickStats.guestMemoryUsage") or 0
        uptime = props.get("summary.quickStats.uptimeSeconds")
        data["uptime_hours"] = round(uptime / 3600, 2) if uptime else 0.0
    else:
        data["cpu_use_pct"] = 0.0
        data["memory_used_mb"] = 0
        data["memory_active_mb"] = 0
        data["uptime_hours"] = 0.0

    data["guest_ip"] = props.get("summary.guest.ipAddress") or ""
    data["guest_os"] = props.get("summary.guest.guestFullName") or ""

    snap_info = props.get("snapshot")
    data["snapshots"] = []
    if snap_info:
        nodes = _snapshot_nodes(snap_info.rootSnapshotList)
        data["snapshot_count"] = len(nodes)
        for sn in nodes:
            with contextlib.suppress(Exception):
                data["snapshots"].append({"name": sn.name, "moref": str(sn.snapshot._moId)})  # noqa: SLF001
    else:
        data["snapshot_count"] = 0
    return data


def translate_datastore(moref: str, props: dict[str, Any]) -> dict[str, Any]:
    """Translate datastore PropertyCollector values into the flat datastore dict."""
    if not props:
        return {"name": moref}
    capacity = props.get("summary.capacity")
    free = props.get("summary.freeSpace")
    hosts = props.get("host") or []
    vms = props.get("vm") or []
    return {
        "name": props.get("summary.name", moref),
        "type": props.get("summary.type"),
        "accessible": props.get("summary.accessible"),
        "capacity_gb": round(capacity / _GB, 2) if capacity else 0.0,
        "free_gb": round(free / _GB, 2) if free else 0.0,
        "used_gb": round((capacity - free) / _GB, 2) if capacity and free is not None else 0.0,
        "url": props.get("summary.url") or "",
        "connected_hosts": len(hosts),
        "host_morefs": [_moref_of(h.key) for h in hosts],
        "virtual_machines": len(vms),
    }


def translate_cluster(moref: str, props: dict[str, Any], pool_vm_counts: dict[str, int]) -> dict[str, Any]:
    """Translate cluster values; pool_vm_counts maps resource pool moref -> VM count."""
    if not props:
        return {"moref": moref, "name": moref}
    drs_enabled = props.get("configuration.drsConfig.enabled", False)
    behavior = props.get("configuration.drsConfig.defaultVmBehavior")
    total_mem = props.get("summary.totalMemory")
    pool_ref = props.get("resourcePool")
    return {
        "moref": moref,
        "name": props.get("name", moref),
        "drs_enabled": drs_enabled,
        "drs_automation_level": str(behavior) if drs_enabled and behavior is not None else None,
        "ha_enabled": props.get("configuration.dasConfig.enabled", False),
        "ha_admission_control": props.get("configuration.dasConfig.admissionControlEnabled", False),
        "total_hosts": props.get("summary.numHosts", 0),
        "effective_hosts": props.get("summary.numEffectiveHosts", 0),
        "total_cpu_mhz": props.get("summary.totalCpu", 0),
        "total_memory_mb": round(total_mem / (1024 * 1024), 0) if total_mem else 0,
        "vm_count": pool_vm_counts.get(_moref_of(pool_ref), 0) if pool_ref else 0,
    }


def translate_resource_pool(moref: str, props: dict[str, Any]) -> dict[str, Any]:
    """Translate resource pool PropertyCollector values into the flat pool dict."""
    if not props:
        return {"moref": moref, "name": moref}
    return {
        "moref": moref,
        "name": props.get("name", moref),
        "cpu_reservation_mhz": props.get("config.cpuAllocation.reservation", 0),
        "cpu_limit_mhz": props.get("config.cpuAllocation.limit", -1),
        "memory_reservation_mb": props.get("config.memoryAllocation.reservation", 0),
        "memory_limit_mb": props.get("config.memoryAllocation.limit", -1),
        "vm_count": len(props.get("vm") or []),
    }


def translate_host_networks(host_moref: str, props: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Translate one host's config.network into vswitch/pnic/portgroup entries.

    Returns {} for hosts that are not powered on or have no network config.
    """
    if str(props.get("summary.runtime.powerState")) != "poweredOn":
        return {}
    net_info = props.get("config.network")
    if not net_info:
        return {}
    host_name = props.get("summary.config.name") or host_moref
    networks: dict[str, dict[str, Any]] = {}

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
    return networks


def translate_dvswitch(moref: str, props: dict[str, Any]) -> dict[str, Any]:
    """Translate a distributed virtual switch (props: name, config, summary)."""
    cfg = props.get("config")
    summary = props.get("summary")
    return {
        "moref": moref,
        "name": props.get("name", moref),
        "type": "dvswitch",
        "num_ports": summary.numPorts if summary else 0,
        "max_ports": cfg.maxPorts if cfg else 0,
        "mtu": cfg.maxMtu if cfg else 0,
        "num_hosts": summary.numHosts if summary else 0,
        "version": cfg.productInfo.version if cfg and cfg.productInfo else "",
        "nioc_enabled": bool(getattr(cfg, "networkResourceManagementEnabled", False)),
        "host_moref": "",
        "host_name": "",
    }


_BINDING_LABELS = {"earlyBinding": "Static", "lateBinding": "Dynamic", "ephemeral": "Ephemeral"}


def translate_dvportgroup(moref: str, pg_config: Any, dvs_name: str) -> dict[str, Any]:
    """Translate a distributed port group from its `config` object."""
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
    return {
        "moref": moref,
        "name": f"{dvs_name} - {pg_config.name}",
        "type": "dvportgroup",
        "vlan_id": vlan_info,
        "port_binding": _BINDING_LABELS.get(raw_binding, raw_binding),
        "num_ports": pg_config.numPorts if pg_config else 0,
        "dvswitch_name": dvs_name,
        "host_moref": "",
        "host_name": "",
    }


def translate_vm_storage(
    vm_moref: str,
    props: dict[str, Any],
    disks: list[Any],
    ds_names: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Translate per-VM disk + storage summary entries.

    disks: the VM's VirtualDisk devices (already filtered by the caller).
    ds_names: datastore moref -> name.
    Returns {} for templates or VMs without a name.
    """
    vm_name = props.get("summary.config.name")
    if props.get("config.template") or not vm_name:
        return {}
    storage: dict[str, dict[str, Any]] = {}

    for device in disks:
        label = device.deviceInfo.label
        if getattr(device, "capacityInBytes", None):
            capacity_gb = round(device.capacityInBytes / _GB, 2)
        elif getattr(device, "capacityInKB", None):
            capacity_gb = round(device.capacityInKB / (1024**2), 2)
        else:
            capacity_gb = 0.0

        backing = device.backing
        thin_provisioned = getattr(backing, "thinProvisioned", None)
        datastore_name = None
        ds = getattr(backing, "datastore", None)
        if ds:
            datastore_name = ds_names.get(_moref_of(ds))

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

    if _has_prefix(props, "summary.storage."):
        committed = props.get("summary.storage.committed")
        uncommitted = props.get("summary.storage.uncommitted")
        unshared = props.get("summary.storage.unshared")
        key = f"{vm_moref}_storage_summary"
        storage[key] = {
            "moref": key,
            "name": f"{vm_name} - Storage Summary",
            "vm_moref": vm_moref,
            "vm_name": vm_name,
            "label": "Storage Summary",
            "committed_gb": round(committed / _GB, 2) if committed else 0.0,
            "uncommitted_gb": round(uncommitted / _GB, 2) if uncommitted else 0.0,
            "unshared_gb": round(unshared / _GB, 2) if unshared else 0.0,
        }
    return storage


def translate_licenses(licenses: Any) -> dict[str, dict[str, Any]]:
    """Translate LicenseManager.licenses; skips invalid and evaluation licenses."""
    result: dict[str, dict[str, Any]] = {}
    for lic in licenses or []:
        key = getattr(lic, "licenseKey", None) or ""
        product_name = getattr(lic, "name", None)

        if not product_name or str(product_name) == "None":
            _LOGGER.debug("Skipping invalid license: key=%s, product=%s", key, product_name)
            continue
        if key == INVALID_LICENSE_KEY:
            _LOGGER.debug("Skipping evaluation license: key=%s", key)
            continue

        total = getattr(lic, "total", 0) or 0
        used = getattr(lic, "used", 0) or 0

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
        self._poll_lock = threading.RLock()
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
        with self._poll_lock:
            self._poll_conn = self._connect()

    def disconnect_poll(self) -> None:
        """Close the on-demand poll connection."""
        with self._poll_lock:
            self._disconnect(self._poll_conn)
            self._poll_conn = None

    @property
    def poll_connection(self) -> Any:
        """Return the on-demand poll connection."""
        return self._poll_conn

    def ensure_poll_connection(self) -> Any:
        """Reconnect the poll connection if the session has died; return it."""
        with self._poll_lock:
            if self._poll_conn is None:
                self.connect_poll()
                return self._poll_conn

            try:
                # Cheap liveness probe
                self._poll_conn.CurrentTime()
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Poll connection lost; reconnecting", exc_info=True)
                self._disconnect(self._poll_conn)
                self._poll_conn = None
                self._counter_cache = None
                self.connect_poll()
            return self._poll_conn

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

    def _bulk_retrieve(self, conn: Any, obj_type: Any, paths: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch `paths` for every object of `obj_type` in one PropertyCollector call.

        Returns {moref: {path: value}}. Pages via ContinueRetrievePropertiesEx.
        Objects missing per-object (missingSet ManagedObjectNotFound) are skipped
        as genuine deletions. A call-level ManagedObjectNotFound retries the whole
        fetch once, then raises VSphereConnectionError — never a partial result.
        """
        try:
            return self._bulk_retrieve_once(conn, obj_type, paths)
        except vmodl.fault.ManagedObjectNotFound:
            _LOGGER.debug("Object vanished during bulk fetch of %s; retrying", obj_type, exc_info=True)
        try:
            return self._bulk_retrieve_once(conn, obj_type, paths)
        except vmodl.fault.ManagedObjectNotFound as exc:
            raise VSphereConnectionError(f"Inventory changed repeatedly while fetching {obj_type}") from exc

    def _bulk_retrieve_once(self, conn: Any, obj_type: Any, paths: list[str]) -> dict[str, dict[str, Any]]:
        """Single attempt of ``_bulk_retrieve``; the ContainerView is always destroyed."""
        content = conn.RetrieveContent()
        pc = content.propertyCollector
        view = content.viewManager.CreateContainerView(content.rootFolder, [obj_type], True)
        try:
            traversal = vmodl.query.PropertyCollector.TraversalSpec(
                name="view_objs", type=vim.view.ContainerView, path="view", skip=False
            )
            obj_spec = vmodl.query.PropertyCollector.ObjectSpec(obj=view, skip=True, selectSet=[traversal])
            prop_spec = vmodl.query.PropertyCollector.PropertySpec(type=obj_type, pathSet=paths, all=False)
            filter_spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec])
            options = vmodl.query.PropertyCollector.RetrieveOptions(maxObjects=500)

            out: dict[str, dict[str, Any]] = {}
            result = pc.RetrievePropertiesEx(specSet=[filter_spec], options=options)
            while result:
                for obj_content in result.objects:
                    props = {p.name: p.val for p in obj_content.propSet or []}
                    if not props and any(
                        isinstance(getattr(m, "fault", None), vmodl.fault.ManagedObjectNotFound)
                        for m in obj_content.missingSet or []
                    ):
                        continue
                    out[obj_content.obj._moId] = props  # noqa: SLF001
                if not result.token:
                    break
                result = pc.ContinueRetrievePropertiesEx(token=result.token)
            return out
        finally:
            view.Destroy()

    def _host_name_map(self, conn: Any) -> dict[str, str]:
        """Return host moref -> name using one bulk call."""
        hosts = self._bulk_retrieve(conn, vim.HostSystem, ["summary.config.name"])
        return {m: p["summary.config.name"] for m, p in hosts.items() if p.get("summary.config.name")}

    # ------------------------------------------------------------------
    # Data fetching — return dict keyed by MoRef ID
    # ------------------------------------------------------------------

    def get_hosts(self) -> dict[str, dict[str, Any]]:
        """Fetch all HostSystem objects and return parsed host dicts (1 bulk call)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.HostSystem, HOST_PROPS)
        result: dict[str, dict[str, Any]] = {}
        for moref, props in raw.items():
            try:
                result[moref] = translate_host(moref, props)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing host %s", moref, exc_info=True)
                result[moref] = {"moref": moref, "name": moref}
        return result

    def count_running_vms_by_host(self) -> tuple[dict[str, int], dict[str, tuple[str, str]]]:
        """Batch-count running (non-template) VMs per host using PropertyCollector.

        Returns:
            Tuple of (counts, vm_power_cache) where:
            - counts: host moref → number of poweredOn non-template VMs
            - vm_power_cache: vm moref → (host_moref, power_state) for ALL
              non-template VMs.  EventListener uses this cache to track deltas
              on subsequent PropertyCollector push updates.
        """
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.VirtualMachine, ["runtime.powerState", "runtime.host", "config.template"])
        counts: dict[str, int] = {}
        cache: dict[str, tuple[str, str]] = {}
        for vm_moref, props in raw.items():
            if props.get("config.template"):
                continue
            power_state = str(props["runtime.powerState"]) if "runtime.powerState" in props else ""
            host_ref = props.get("runtime.host")
            host_moref = _moref_of(host_ref) if host_ref else ""
            cache[vm_moref] = (host_moref, power_state)
            if host_moref and power_state == "poweredOn":
                counts[host_moref] = counts.get(host_moref, 0) + 1
        return counts, cache

    def get_vms(self) -> dict[str, dict[str, Any]]:
        """Fetch all VirtualMachine objects and return parsed VM dicts (2 bulk calls)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.VirtualMachine, VM_PROPS)
        host_names = self._host_name_map(conn)
        result: dict[str, dict[str, Any]] = {}
        for moref, props in raw.items():
            try:
                result[moref] = translate_vm(moref, props, host_names)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing VM %s", moref, exc_info=True)
                result[moref] = {"moref": moref, "name": moref}
        return result

    def get_datastores(self) -> dict[str, dict[str, Any]]:
        """Fetch all Datastore objects and return parsed dicts (1 bulk call)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.Datastore, DATASTORE_PROPS)
        result: dict[str, dict[str, Any]] = {}
        for moref, props in raw.items():
            try:
                result[moref] = translate_datastore(moref, props)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing datastore %s", moref, exc_info=True)
                result[moref] = {"name": moref}
        return result

    def get_clusters(self) -> dict[str, dict[str, Any]]:
        """Fetch cluster information (2 bulk calls: clusters + resource pool VM counts)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.ClusterComputeResource, CLUSTER_PROPS)
        pools = self._bulk_retrieve(conn, vim.ResourcePool, ["vm"]) if raw else {}
        pool_vm_counts = {m: len(p.get("vm") or []) for m, p in pools.items()}
        clusters: dict[str, dict[str, Any]] = {}
        for moref, props in raw.items():
            try:
                clusters[moref] = translate_cluster(moref, props, pool_vm_counts)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing cluster %s", moref, exc_info=True)
                clusters[moref] = {"moref": moref, "name": moref}
        return clusters

    def get_networks(self) -> dict[str, dict[str, Any]]:
        """Fetch network information (vSwitches, port groups, physical NICs, dvSwitches).

        Uses HostSystem.config.network (same HostNetworkInfo as the network
        system's networkInfo) so no per-host RPC is needed.
        """
        conn = self.ensure_poll_connection()
        networks: dict[str, dict[str, Any]] = {}

        raw_hosts = self._bulk_retrieve(
            conn,
            vim.HostSystem,
            ["summary.config.name", "summary.runtime.powerState", "config.network"],
        )
        for host_moref, props in raw_hosts.items():
            try:
                networks.update(translate_host_networks(host_moref, props))
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing network info for host %s", host_moref, exc_info=True)

        # Distributed virtual switches (datacenter-level). Only not-applicable faults
        # are swallowed; anything else propagates so the inventory refresh fails whole.
        try:
            dvs_raw = self._bulk_retrieve(
                conn, vim.DistributedVirtualSwitch, ["name", "config", "summary", "portgroup"]
            )
            dvs_names: dict[str, str] = {}
            pg_to_dvs: dict[str, str] = {}
            for dvs_moref, props in dvs_raw.items():
                try:
                    networks[dvs_moref] = translate_dvswitch(dvs_moref, props)
                    dvs_names[dvs_moref] = props.get("name", dvs_moref)
                    for pg in props.get("portgroup") or []:
                        pg_to_dvs[_moref_of(pg)] = dvs_moref
                except Exception:  # noqa: BLE001
                    _LOGGER.debug("Error parsing dvSwitch %s", dvs_moref, exc_info=True)
            if pg_to_dvs:
                pg_raw = self._bulk_retrieve(conn, vim.dvs.DistributedVirtualPortgroup, ["config"])
                for pg_moref, props in pg_raw.items():
                    dvs_moref = pg_to_dvs.get(pg_moref)
                    pg_config = props.get("config")
                    if dvs_moref is None or pg_config is None:
                        continue
                    try:
                        networks[pg_moref] = translate_dvportgroup(pg_moref, pg_config, dvs_names[dvs_moref])
                    except Exception:  # noqa: BLE001
                        _LOGGER.debug("Error parsing dvPortgroup %s", pg_moref, exc_info=True)
        except vim.fault.NoPermission, vmodl.fault.NotSupported, vmodl.fault.NotImplemented:  # type: ignore[attr-defined]
            # No DVS on this endpoint (e.g. standalone ESXi) or not permitted: nothing to add
            _LOGGER.debug("Distributed virtual switches not available", exc_info=True)

        return networks

    def get_resource_pools(self) -> dict[str, dict[str, Any]]:
        """Fetch resource pool information (1 bulk call)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.ResourcePool, RESOURCE_POOL_PROPS)
        pools: dict[str, dict[str, Any]] = {}
        for moref, props in raw.items():
            try:
                pools[moref] = translate_resource_pool(moref, props)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Error parsing resource pool %s", moref, exc_info=True)
                pools[moref] = {"moref": moref, "name": moref}
        return pools

    def get_licenses(self) -> dict[str, dict[str, Any]]:
        """Fetch license information; silently filter evaluation/invalid at DEBUG level."""
        conn = self.ensure_poll_connection()
        content = conn.RetrieveContent()
        return translate_licenses(content.licenseManager.licenses)

    def get_alarms(self) -> dict[str, list[dict[str, Any]]]:
        """Fetch current triggered alarms for all hosts and VMs."""
        conn = self.ensure_poll_connection()
        content = conn.RetrieveContent()
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
        """Lightweight inventory enumeration for config flow (morefs + names only).

        Uses a dedicated session: config flow clients are throwaway and never
        open the shared poll connection.
        """
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
                for moref, props in self._bulk_retrieve(conn, obj_type, ["name"]).items():
                    result[moref] = {"moref": moref, "name": props.get("name") or moref, "type": category}
            return result
        finally:
            self._disconnect(conn)

    # ------------------------------------------------------------------
    # Internal lookup helpers
    # ------------------------------------------------------------------

    def _get_vm_by_moref(self, moref: str) -> Any:
        """Build a VirtualMachine proxy by MoRef ID (validated via a property read)."""
        conn = self.ensure_poll_connection()
        vm = vim.VirtualMachine(moref, conn._stub)  # noqa: SLF001
        try:
            _ = vm.name
        except vmodl.fault.ManagedObjectNotFound as exc:
            raise VSphereOperationError(f"VM with MoRef '{moref}' not found") from exc
        return vm

    def _get_host_by_moref(self, moref: str) -> Any:
        """Build a HostSystem proxy by MoRef ID (validated via a property read)."""
        conn = self.ensure_poll_connection()
        host = vim.HostSystem(moref, conn._stub)  # noqa: SLF001
        try:
            _ = host.name
        except vmodl.fault.ManagedObjectNotFound as exc:
            raise VSphereOperationError(f"Host with MoRef '{moref}' not found") from exc
        return host

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
        conn = self.ensure_poll_connection()
        content = conn.RetrieveContent()
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
                host_obj = self._get_managed_object(vim.HostSystem, moref, conn)
                if host_obj:
                    data = self._query_entity_perf(perf_manager, host_obj, counter_ids, "host")
                    if data:
                        results[moref] = data
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to query perf for host %s", moref, exc_info=True)

        # Query VMs
        for moref in vm_morefs:
            try:
                vm_obj = self._get_managed_object(vim.VirtualMachine, moref, conn)
                if vm_obj:
                    data = self._query_entity_perf(perf_manager, vm_obj, counter_ids, "vm")
                    if data:
                        results[moref] = data
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to query perf for VM %s", moref, exc_info=True)

        # Query datastores
        for moref in datastore_morefs:
            try:
                ds_obj = self._get_managed_object(vim.Datastore, moref, conn)
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

    def _get_managed_object(self, obj_type: type, moref: str, conn: Any = None) -> Any | None:
        """Build a managed object proxy by type and MoRef; None if it no longer exists."""
        conn = conn or self.ensure_poll_connection()
        obj = obj_type(moref, conn._stub)  # noqa: SLF001
        try:
            _ = obj.name
        except vmodl.fault.ManagedObjectNotFound:
            return None
        return obj

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
        or "{vm_moref}_storage_summary".  Two bulk calls (VMs + datastore names).
        """
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(
            conn,
            vim.VirtualMachine,
            [
                "config.template",
                "summary.config.name",
                "config.hardware.device",
                "summary.storage.committed",
                "summary.storage.uncommitted",
                "summary.storage.unshared",
            ],
        )
        ds_raw = self._bulk_retrieve(conn, vim.Datastore, ["name"])
        ds_names = {m: p["name"] for m, p in ds_raw.items() if p.get("name")}
        storage: dict[str, dict[str, Any]] = {}
        for vm_moref, props in raw.items():
            try:
                disks = [
                    d for d in props.get("config.hardware.device") or [] if isinstance(d, vim.vm.device.VirtualDisk)
                ]
                storage.update(translate_vm_storage(vm_moref, props, disks, ds_names))
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Failed to get storage details for VM %s", vm_moref, exc_info=True)
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
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.HostSystem, ["summary.config.name", "summary.runtime.powerState"])
        hosts: list[dict[str, Any]] = []
        for moref, props in raw.items():
            if "summary.config.name" in props and "summary.runtime.powerState" in props:
                name = props["summary.config.name"]
                state = str(props["summary.runtime.powerState"])
            else:
                name = moref
                state = "unknown"
            hosts.append({"moref": moref, "name": name, "power_state": state})
        return hosts

    def list_power_policies(self, host_moref: str) -> list[dict[str, Any]]:
        """Return available power policies for a host (for service responses)."""
        conn = self.ensure_poll_connection()
        raw = self._bulk_retrieve(conn, vim.HostSystem, ["config.powerSystemCapability.availablePolicy"])
        policies = raw.get(host_moref, {}).get("config.powerSystemCapability.availablePolicy") or []
        return [{"key": p.key, "short_name": p.shortName, "name": p.name} for p in policies]

    # ------------------------------------------------------------------
    # PropertyCollector support (EventListener)
    # ------------------------------------------------------------------

    def _get_category_type_info(self, category: str, categories: dict[str, bool]) -> tuple[Any, list[str]] | None:
        """Return (vim_type, property_paths) for a category, or None if not applicable."""
        host_props = [
            "summary.config.name",
            "summary.runtime.powerState",
            "summary.runtime.connectionState",
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
            "summary.runtime.connectionState",
            "runtime.connectionState",
            "runtime.maxCpuUsage",
            "snapshot",
            "configStatus",
            "config.template",
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

        # Collect categories to watch, adding hosts/vms when events_alarms is on
        # even if those categories are not explicitly enabled.
        watch_categories: dict[str, bool] = dict(categories)
        if categories.get("events_alarms"):
            watch_categories["hosts"] = True
            watch_categories["vms"] = True

        try:
            return self._build_property_filter(content, pc, categories, watch_categories, containers)
        except Exception:
            for c in containers:
                with contextlib.suppress(Exception):
                    c.Destroy()
            raise

    def _build_property_filter(
        self,
        content: Any,
        pc: Any,
        categories: dict[str, bool],
        watch_categories: dict[str, bool],
        containers: list[Any],
    ) -> tuple[Any, Any, list[Any]]:
        """Create views + filter; caller cleans up containers on failure."""
        obj_specs: list[Any] = []
        prop_specs: list[Any] = []
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
