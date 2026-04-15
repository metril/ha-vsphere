"""Service registration for vSphere Control integration."""

from __future__ import annotations

import logging
from functools import partial
from typing import Any

import voluptuous as vol
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr

from .const import (
    DOMAIN,
    SNAP_ALL,
    SNAP_FIRST,
    SNAP_LAST,
    HostAction,
    VmAction,
)
from .exceptions import VSphereConnectionError, VSphereOperationError

_LOGGER = logging.getLogger(__name__)

# Service names
SVC_VM_POWER = "vm_power"
SVC_HOST_POWER = "host_power"
SVC_HOST_POWER_POLICY = "host_power_policy"
SVC_HOST_MAINTENANCE_MODE = "host_maintenance_mode"
SVC_CREATE_SNAPSHOT = "create_snapshot"
SVC_REMOVE_SNAPSHOT = "remove_snapshot"
SVC_LIST_HOSTS = "list_hosts"
SVC_LIST_POWER_POLICIES = "list_power_policies"
SVC_VM_MIGRATE = "vm_migrate"
SVC_REMOVE_SNAPSHOTS = "remove_snapshots"

# Field names
ATTR_DEVICE_ID = "device_id"
ATTR_ACTION = "action"
ATTR_FORCE = "force"
ATTR_POLICY = "policy"
ATTR_ENABLE = "enable"
ATTR_NAME = "name"
ATTR_DESCRIPTION = "description"
ATTR_MEMORY = "memory"
ATTR_QUIESCE = "quiesce"
ATTR_WHICH = "which"

# Schemas
_VM_POWER_ACTIONS = [
    a.value for a in VmAction if a not in (VmAction.SNAPSHOT_CREATE, VmAction.SNAPSHOT_REMOVE, VmAction.MIGRATE)
]

_SCHEMA_VM_POWER = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_ACTION): vol.In(_VM_POWER_ACTIONS),
    }
)

_SCHEMA_HOST_POWER = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_ACTION): vol.In([HostAction.SHUTDOWN.value, HostAction.REBOOT.value]),
        vol.Optional(ATTR_FORCE, default=False): bool,
    }
)

_SCHEMA_HOST_POWER_POLICY = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_POLICY): str,
    }
)

_SCHEMA_HOST_MAINTENANCE_MODE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_ENABLE): bool,
    }
)

_SCHEMA_CREATE_SNAPSHOT = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Optional(ATTR_NAME): str,
        vol.Optional(ATTR_DESCRIPTION): str,
        vol.Optional(ATTR_MEMORY, default=False): bool,
        vol.Optional(ATTR_QUIESCE, default=False): bool,
    }
)

_SCHEMA_REMOVE_SNAPSHOT = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_WHICH): vol.In([SNAP_ALL, SNAP_FIRST, SNAP_LAST]),
    }
)

_SCHEMA_LIST_HOSTS = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
    }
)

_SCHEMA_LIST_POWER_POLICIES = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
    }
)

_SCHEMA_VM_MIGRATE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required("target_host"): str,
    }
)

ATTR_SNAPSHOTS = "snapshots"

_SCHEMA_REMOVE_SNAPSHOTS = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): str,
        vol.Required(ATTR_SNAPSHOTS): vol.All(cv.ensure_list, [str]),
    }
)


# ---------------------------------------------------------------------------
# Device resolver helper
# ---------------------------------------------------------------------------


def _resolve_device(hass: HomeAssistant, device_id: str) -> tuple[Any, Any, str, str]:
    """Resolve a device_id to (client, resolver, entry_id, moref).

    Device identifiers for this integration use the format:
        (DOMAIN, "{entry_id}_{moref}")

    Returns:
        Tuple of (VSphereClient, PermissionResolver, entry_id, moref).

    Raises:
        HomeAssistantError: if the device, config entry, or moref cannot be found.
    """
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get(device_id)
    if device is None:
        raise HomeAssistantError(f"Device '{device_id}' not found in device registry")

    # Find the config entry for this device
    entry_id: str | None = None
    moref: str | None = None
    for identifier in device.identifiers:
        if len(identifier) == 2 and identifier[0] == DOMAIN:
            raw = identifier[1]
            # Format: "{entry_id}_{moref}"
            # entry_id is a 32-char hex string; moref can contain underscores
            # Split on first underscore to separate entry_id from rest
            parts = raw.split("_", 1)
            if len(parts) == 2:
                entry_id = parts[0]
                moref = parts[1]
                break

    if entry_id is None or moref is None:
        raise HomeAssistantError(f"Cannot extract vSphere moref from device identifiers for device '{device_id}'")

    domain_data: dict[str, Any] = hass.data.get(DOMAIN, {})
    entry_data: dict[str, Any] = domain_data.get(entry_id, {})
    if not entry_data:
        raise HomeAssistantError(f"vSphere config entry '{entry_id}' is not loaded")

    client = entry_data.get("client")
    resolver = entry_data.get("resolver")

    if client is None:
        raise HomeAssistantError(f"vSphere client not found for config entry '{entry_id}'")

    return client, resolver, entry_id, moref


# ---------------------------------------------------------------------------
# Service handler helper
# ---------------------------------------------------------------------------


async def _resolve_and_call(
    hass: HomeAssistant,
    device_id: str,
    category: str | None,
    action: str | None,
    call_fn: Any,
) -> tuple[Any, Any, str, str]:
    """Resolve device, check permission, call a function.

    ``call_fn`` receives ``(client, moref)`` and should return the
    awaitable executor job.  Returns ``(client, resolver, entry_id, moref)``.
    """
    client, resolver, entry_id, moref = _resolve_device(hass, device_id)
    if category and action and resolver is not None and not resolver.is_allowed(category, moref, action):
        raise HomeAssistantError(resolver.explain(category, moref, action))
    try:
        await call_fn(client, moref)
    except (VSphereOperationError, VSphereConnectionError) as err:
        raise HomeAssistantError(str(err)) from err
    return client, resolver, entry_id, moref


# ---------------------------------------------------------------------------
# Service handlers
# ---------------------------------------------------------------------------


async def _handle_vm_power(call: ServiceCall) -> None:
    """Handle the vm_power service call."""
    action = call.data[ATTR_ACTION]
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "vms",
        action,
        lambda c, m: call.hass.async_add_executor_job(c.vm_power, m, action),
    )


async def _handle_host_power(call: ServiceCall) -> None:
    """Handle the host_power service call."""
    action = call.data[ATTR_ACTION]
    force = call.data.get(ATTR_FORCE, False)
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "hosts",
        action,
        lambda c, m: call.hass.async_add_executor_job(c.host_power, m, action, force),
    )


async def _handle_host_power_policy(call: ServiceCall) -> None:
    """Handle the host_power_policy service call."""
    policy = call.data[ATTR_POLICY]
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "hosts",
        HostAction.POWER_POLICY,
        lambda c, m: call.hass.async_add_executor_job(c.host_set_power_policy, m, policy),
    )


async def _handle_host_maintenance_mode(call: ServiceCall) -> None:
    """Handle the host_maintenance_mode service call."""
    enable = call.data[ATTR_ENABLE]
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "hosts",
        HostAction.MAINTENANCE,
        lambda c, m: call.hass.async_add_executor_job(c.host_set_maintenance_mode, m, enable),
    )


async def _handle_create_snapshot(call: ServiceCall) -> None:
    """Handle the create_snapshot service call."""
    d = call.data
    await _resolve_and_call(
        call.hass,
        d[ATTR_DEVICE_ID],
        "vms",
        VmAction.SNAPSHOT_CREATE,
        lambda c, m: call.hass.async_add_executor_job(
            c.create_snapshot,
            m,
            d.get(ATTR_NAME),
            d.get(ATTR_DESCRIPTION),
            d.get(ATTR_MEMORY, False),
            d.get(ATTR_QUIESCE, False),
        ),
    )


async def _handle_remove_snapshot(call: ServiceCall) -> None:
    """Handle the remove_snapshot service call."""
    which = call.data[ATTR_WHICH]
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "vms",
        VmAction.SNAPSHOT_REMOVE,
        lambda c, m: call.hass.async_add_executor_job(c.remove_snapshot, m, which),
    )


async def _handle_list_hosts(call: ServiceCall) -> dict[str, Any]:
    """Handle the list_hosts service call."""
    client, _, _, _ = _resolve_device(call.hass, call.data[ATTR_DEVICE_ID])
    try:
        hosts = await call.hass.async_add_executor_job(client.list_hosts)
    except VSphereOperationError as err:
        raise HomeAssistantError(str(err)) from err
    return {"hosts": hosts}


async def _handle_list_power_policies(call: ServiceCall) -> dict[str, Any]:
    """Handle the list_power_policies service call."""
    client, _, _, moref = _resolve_device(call.hass, call.data[ATTR_DEVICE_ID])
    try:
        policies = await call.hass.async_add_executor_job(client.list_power_policies, moref)
    except VSphereOperationError as err:
        raise HomeAssistantError(str(err)) from err
    return {"policies": policies}


async def _handle_vm_migrate(call: ServiceCall) -> None:
    """Handle the vm_migrate service call."""
    _, _, target_entry_id, host_moref = _resolve_device(call.hass, call.data["target_host"])
    _, _, vm_entry_id, _ = _resolve_device(call.hass, call.data[ATTR_DEVICE_ID])
    if target_entry_id != vm_entry_id:
        raise HomeAssistantError("VM and target host must belong to the same vSphere connection")
    await _resolve_and_call(
        call.hass,
        call.data[ATTR_DEVICE_ID],
        "vms",
        VmAction.MIGRATE,
        lambda c, m: call.hass.async_add_executor_job(c.vm_migrate, m, host_moref),
    )


# ---------------------------------------------------------------------------
# Snapshot removal by name(s)
# ---------------------------------------------------------------------------


async def _handle_remove_snapshots(hass: HomeAssistant, call: ServiceCall) -> None:
    """Remove one or more snapshots by name. Pass 'all' to remove all snapshots."""
    client, resolver, entry_id, vm_moref = _resolve_device(hass, call.data[ATTR_DEVICE_ID])
    if resolver is not None and not resolver.is_allowed("vms", vm_moref, VmAction.SNAPSHOT_REMOVE):
        raise HomeAssistantError(resolver.explain("vms", vm_moref, VmAction.SNAPSHOT_REMOVE))
    snapshot_names: list[str] = call.data[ATTR_SNAPSHOTS]

    # Get snapshot data from coordinator
    entry_data = hass.data.get(DOMAIN, {}).get(entry_id, {})
    coordinator = entry_data.get("coordinator")
    if coordinator is None:
        raise HomeAssistantError(f"vSphere config entry '{entry_id}' is not loaded")
    vm_data = (coordinator.data or {}).get("vms", {}).get(vm_moref, {})
    snapshots: list[dict[str, str]] = vm_data.get("snapshots", [])

    for snap_name in snapshot_names:
        if snap_name.lower() == "all":
            try:
                await hass.async_add_executor_job(client.remove_snapshot, vm_moref, SNAP_ALL)
            except (VSphereOperationError, VSphereConnectionError) as err:
                raise HomeAssistantError(str(err)) from err
            return  # "all" removes everything, no need to continue

        snap_moref = next((s["moref"] for s in snapshots if s["name"] == snap_name), None)
        if snap_moref is None:
            raise HomeAssistantError(f"Snapshot '{snap_name}' not found on VM {vm_moref}")
        try:
            await hass.async_add_executor_job(client.remove_snapshot_by_moref, vm_moref, snap_moref)
        except (VSphereOperationError, VSphereConnectionError) as err:
            raise HomeAssistantError(str(err)) from err


# ---------------------------------------------------------------------------
# Registration / unregistration
# ---------------------------------------------------------------------------


async def async_register_services(hass: HomeAssistant) -> None:
    """Register vSphere services (idempotent)."""

    if not hass.services.has_service(DOMAIN, SVC_VM_POWER):
        hass.services.async_register(
            DOMAIN,
            SVC_VM_POWER,
            _handle_vm_power,
            schema=_SCHEMA_VM_POWER,
        )

    if not hass.services.has_service(DOMAIN, SVC_HOST_POWER):
        hass.services.async_register(
            DOMAIN,
            SVC_HOST_POWER,
            _handle_host_power,
            schema=_SCHEMA_HOST_POWER,
        )

    if not hass.services.has_service(DOMAIN, SVC_HOST_POWER_POLICY):
        hass.services.async_register(
            DOMAIN,
            SVC_HOST_POWER_POLICY,
            _handle_host_power_policy,
            schema=_SCHEMA_HOST_POWER_POLICY,
        )

    if not hass.services.has_service(DOMAIN, SVC_HOST_MAINTENANCE_MODE):
        hass.services.async_register(
            DOMAIN,
            SVC_HOST_MAINTENANCE_MODE,
            _handle_host_maintenance_mode,
            schema=_SCHEMA_HOST_MAINTENANCE_MODE,
        )

    if not hass.services.has_service(DOMAIN, SVC_CREATE_SNAPSHOT):
        hass.services.async_register(
            DOMAIN,
            SVC_CREATE_SNAPSHOT,
            _handle_create_snapshot,
            schema=_SCHEMA_CREATE_SNAPSHOT,
        )

    if not hass.services.has_service(DOMAIN, SVC_REMOVE_SNAPSHOT):
        hass.services.async_register(
            DOMAIN,
            SVC_REMOVE_SNAPSHOT,
            _handle_remove_snapshot,
            schema=_SCHEMA_REMOVE_SNAPSHOT,
        )

    if not hass.services.has_service(DOMAIN, SVC_LIST_HOSTS):
        hass.services.async_register(
            DOMAIN,
            SVC_LIST_HOSTS,
            _handle_list_hosts,
            schema=_SCHEMA_LIST_HOSTS,
            supports_response=SupportsResponse.OPTIONAL,
        )

    if not hass.services.has_service(DOMAIN, SVC_LIST_POWER_POLICIES):
        hass.services.async_register(
            DOMAIN,
            SVC_LIST_POWER_POLICIES,
            _handle_list_power_policies,
            schema=_SCHEMA_LIST_POWER_POLICIES,
            supports_response=SupportsResponse.OPTIONAL,
        )

    if not hass.services.has_service(DOMAIN, SVC_VM_MIGRATE):
        hass.services.async_register(
            DOMAIN,
            SVC_VM_MIGRATE,
            _handle_vm_migrate,
            schema=_SCHEMA_VM_MIGRATE,
        )

    if not hass.services.has_service(DOMAIN, SVC_REMOVE_SNAPSHOTS):
        hass.services.async_register(
            DOMAIN,
            SVC_REMOVE_SNAPSHOTS,
            partial(_handle_remove_snapshots, hass),
            schema=_SCHEMA_REMOVE_SNAPSHOTS,
        )

    _LOGGER.debug("vSphere services registered")


def async_unregister_services(hass: HomeAssistant) -> None:
    """Unregister all vSphere services."""
    for service in (
        SVC_VM_POWER,
        SVC_HOST_POWER,
        SVC_HOST_POWER_POLICY,
        SVC_HOST_MAINTENANCE_MODE,
        SVC_CREATE_SNAPSHOT,
        SVC_REMOVE_SNAPSHOT,
        SVC_LIST_HOSTS,
        SVC_LIST_POWER_POLICIES,
        SVC_VM_MIGRATE,
        SVC_REMOVE_SNAPSHOTS,
    ):
        if hass.services.has_service(DOMAIN, service):
            hass.services.async_remove(DOMAIN, service)

    _LOGGER.debug("vSphere services unregistered")
