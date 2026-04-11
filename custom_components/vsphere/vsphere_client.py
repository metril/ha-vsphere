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
            raise VSphereAuthError(
                f"Invalid credentials for {self._host}: {exc}"
            ) from exc
        except vim.fault.PasswordExpired as exc:
            raise VSphereAuthError(
                f"Password expired for {self._host}: {exc}"
            ) from exc
        except (TimeoutError, ConnectionRefusedError, ssl.SSLError, OSError) as exc:
            raise VSphereConnectionError(
                f"Cannot connect to {self._host}:{self._port}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereConnectionError(
                f"vSphere method fault during connect to {self._host}: {exc}"
            ) from exc

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
            conn_type = (
                CONN_TYPE_VCENTER
                if "VirtualCenter" in (about.apiType or "")
                else CONN_TYPE_ESXI
            )
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

    def _get_container_view(
        self, conn: Any, obj_type: list[Any]
    ) -> Any:
        """Return a ContainerView for the given object types."""
        content = conn.RetrieveContent()
        return content.viewManager.CreateContainerView(
            content.rootFolder, obj_type, True
        )

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
                        "capacity_gb": round(summary.capacity / (1024**3), 2)
                        if summary.capacity
                        else 0.0,
                        "free_gb": round(summary.freeSpace / (1024**3), 2)
                        if summary.freeSpace
                        else 0.0,
                        "used_gb": round(
                            (summary.capacity - summary.freeSpace) / (1024**3), 2
                        )
                        if summary.capacity and summary.freeSpace is not None
                        else 0.0,
                        "url": summary.url or "",
                    }
                except Exception:  # noqa: BLE001
                    _LOGGER.debug(
                        "Error parsing datastore %s", moref, exc_info=True
                    )
                    info = {"name": moref}
                result[moref] = info
            view.Destroy()
            return result
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
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )
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
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True
        )
        try:
            for host in view.view:
                if host._moId == moref:  # noqa: SLF001
                    return host
        finally:
            view.Destroy()
        raise VSphereOperationError(f"Host with MoRef '{moref}' not found")

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
                raise VSphereOperationError(
                    f"Task '{description}' failed: {error}"
                )
            # queued or running — keep polling
            time.sleep(_TASK_POLL_INTERVAL)

        raise VSphereOperationError(
            f"Task '{description}' timed out after {_TASK_TIMEOUT}s"
        )

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
                            "Tools unavailable for ShutdownGuest on %s; "
                            "falling back to PowerOffVM",
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
                            "Tools unavailable for RebootGuest on %s; "
                            "falling back to ResetVM",
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
            raise VSphereOperationError(
                f"Invalid power state for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(
                f"Task already in progress for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(
                f"Invalid state for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.ResourceInUse as exc:
            raise VSphereOperationError(
                f"Resource in use for VM {vm_moref}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault during {action} on VM {vm_moref}: {exc}"
            ) from exc

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
            raise VSphereOperationError(
                f"Snapshot fault for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.InvalidPowerState as exc:
            raise VSphereOperationError(
                f"Invalid power state for snapshot on VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(
                f"Task in progress for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.InsufficientResourcesFault as exc:
            raise VSphereOperationError(
                f"Insufficient resources for snapshot on VM {vm_moref}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault during create_snapshot on VM {vm_moref}: {exc}"
            ) from exc

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
                raise VSphereOperationError(
                    f"Unknown snapshot target '{which}'; expected all/first/last"
                )

            task = target.RemoveSnapshot_Task(removeChildren=False)
            self._wait_for_task(task, f"remove_snapshot({which}) on {vm.name}")

        except VSphereOperationError:
            raise
        except vim.fault.NotFound:
            # Idempotent — already gone
            _LOGGER.debug("Snapshot not found on VM %s (already removed)", vm_moref)
        except vim.fault.SnapshotFault as exc:
            raise VSphereOperationError(
                f"Snapshot fault for VM {vm_moref}: {exc}"
            ) from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(
                f"Task in progress for VM {vm_moref}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault during remove_snapshot on VM {vm_moref}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Host operations
    # ------------------------------------------------------------------

    def host_power(
        self, host_moref: str, action: str, force: bool = False
    ) -> None:
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
                    if getattr(getattr(vm, "runtime", None), "powerState", None)
                    == VM_POWER_ON
                )
                if powered_vms > 0:
                    raise VSphereOperationError(
                        f"Cannot {action} {host_name}: {powered_vms} VMs running. "
                        "Use force=true to override"
                    )

            if action == "shutdown":
                task = host.ShutdownHost_Task(force=force)
            elif action == "reboot":
                task = host.RebootHost_Task(force=force)
            else:
                raise VSphereOperationError(
                    f"Unknown host power action: {action}"
                )

            self._wait_for_task(task, f"host_{action} on {host_name}")

        except VSphereOperationError:
            raise
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(
                f"Invalid state for host {host_moref}: {exc}"
            ) from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(
                f"Task in progress for host {host_moref}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault during host_{action} on {host_moref}: {exc}"
            ) from exc

    def host_set_power_policy(self, host_moref: str, policy_name: str) -> None:
        """Change the power policy of a host."""
        try:
            host = self._get_host_by_moref(host_moref)
            power_sys = host.configManager.powerSystem
            if power_sys is None:
                raise VSphereOperationError(
                    f"Host {host_moref} does not support power policy configuration"
                )

            capability = host.config.powerSystemCapability
            available = capability.availablePolicy if capability else []
            for policy in available:
                if policy.shortName == policy_name or policy.key == policy_name:
                    power_sys.ConfigurePowerPolicy(key=policy.key)
                    return

            raise VSphereOperationError(
                f"Power policy '{policy_name}' not found on host {host_moref}"
            )

        except VSphereOperationError:
            raise
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault setting power policy on {host_moref}: {exc}"
            ) from exc

    def host_set_maintenance_mode(self, host_moref: str, enable: bool) -> None:
        """Enter or exit maintenance mode for a host."""
        try:
            host = self._get_host_by_moref(host_moref)
            task = host.EnterMaintenanceMode_Task(
                timeout=0, evacuatePoweredOffVms=True
            ) if enable else host.ExitMaintenanceMode_Task(timeout=0)
            host_name = host.summary.config.name if host.summary.config else host_moref
            action = "enter_maintenance" if enable else "exit_maintenance"
            self._wait_for_task(task, f"{action} on {host_name}")

        except VSphereOperationError:
            raise
        except vim.fault.InvalidState as exc:
            raise VSphereOperationError(
                f"Invalid state for host {host_moref}: {exc}"
            ) from exc
        except vim.fault.TaskInProgress as exc:
            raise VSphereOperationError(
                f"Task in progress for host {host_moref}: {exc}"
            ) from exc
        except vmodl.MethodFault as exc:
            raise VSphereOperationError(
                f"vSphere fault setting maintenance mode on {host_moref}: {exc}"
            ) from exc

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
            raise VSphereConnectionError(
                "Push connection not established; call connect_push() first"
            )

        content = conn.RetrieveContent()
        pc = content.propertyCollector

        # Build object specs based on categories
        obj_specs: list[Any] = []
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        prop_specs: list[Any] = []

        type_map: dict[str, tuple[Any, list[str]]] = {
            "hosts": (
                vim.HostSystem,
                ["summary.runtime.powerState", "summary.config.name"],
            ),
            "vms": (
                vim.VirtualMachine,
                ["summary.runtime.powerState", "summary.config.name", "runtime.powerState"],
            ),
            "datastores": (
                vim.Datastore,
                ["summary.accessible", "summary.freeSpace"],
            ),
        }

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
            data["version"] = (
                config.product.version if config and config.product else ""
            )
            data["build"] = (
                config.product.build if config and config.product else ""
            )

            # Capability
            cap = host.capability
            data["shutdown_supported"] = bool(cap.shutdownSupported) if cap else False

            # Uptime
            data["uptime_hours"] = round(quick.uptime / 3600, 2) if quick and quick.uptime else 0.0

            # CPU
            if hardware and hardware.cpuMhz and hardware.numCpuCores:
                data["cpu_total_ghz"] = round(
                    hardware.cpuMhz * hardware.numCpuCores / 1000, 2
                )
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
                data["power_policy"] = (
                    ps_info.currentPolicy.shortName
                    if ps_info and ps_info.currentPolicy
                    else ""
                )
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
            data["used_space_gb"] = (
                round(storage.committed / (1024**3), 2)
                if storage and storage.committed
                else 0.0
            )

            # Tools
            data["tools_status"] = (
                str(guest_summary.toolsStatus) if guest_summary else "toolsNotInstalled"
            )

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
                    data["cpu_use_pct"] = round(
                        quick.overallCpuUsage / runtime.maxCpuUsage * 100, 1
                    )
                else:
                    data["cpu_use_pct"] = 0.0

                data["memory_used_mb"] = quick.hostMemoryUsage or 0
                data["memory_active_mb"] = quick.guestMemoryUsage or 0
                data["uptime_hours"] = (
                    round(quick.uptimeSeconds / 3600, 2) if quick.uptimeSeconds else 0.0
                )
            else:
                data["cpu_use_pct"] = 0.0
                data["memory_used_mb"] = 0
                data["memory_active_mb"] = 0
                data["uptime_hours"] = 0.0

            # Guest info
            data["guest_ip"] = (
                guest_summary.ipAddress if guest_summary and guest_summary.ipAddress else ""
            )
            data["guest_os"] = (
                guest_summary.guestFullName
                if guest_summary and guest_summary.guestFullName
                else ""
            )

            # Snapshot count
            snap_info = vm_obj.snapshot
            if snap_info:
                data["snapshot_count"] = len(
                    self._list_snapshots(snap_info.rootSnapshotList)
                )
            else:
                data["snapshot_count"] = 0

        except Exception:  # noqa: BLE001
            _LOGGER.debug("Error parsing VM %s", moref, exc_info=True)
            data.setdefault("name", moref)

        return data
