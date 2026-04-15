"""Select platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.components.select import SelectEntity
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN, SNAP_SELECT_ALL, HostAction, VmAction
from .entity import VSphereEntity
from .exceptions import VSphereOperationError

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .coordinator import VSphereData
    from .permissions import PermissionResolver
    from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)

_ALL_SNAPSHOTS = SNAP_SELECT_ALL

# VM power operation display names → (VmAction, client action string)
VM_POWER_OPERATIONS: dict[str, tuple[VmAction, str]] = {
    "Power On": (VmAction.POWER_ON, "power_on"),
    "Shutdown Guest OS": (VmAction.SHUTDOWN, "shutdown"),
    "Restart Guest OS": (VmAction.REBOOT, "reboot"),
    "Suspend": (VmAction.SUSPEND, "suspend"),
    "Power Off": (VmAction.POWER_OFF, "power_off"),
    "Reset": (VmAction.RESET, "reset"),
}

# Host power operation display names → (HostAction, client action string)
HOST_POWER_OPERATIONS: dict[str, tuple[HostAction, str]] = {
    "Shutdown": (HostAction.SHUTDOWN, "shutdown"),
    "Reboot": (HostAction.REBOOT, "reboot"),
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere select entities from a config entry."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = entry_data["coordinator"]
    client: VSphereClient = entry_data["client"]
    resolver: PermissionResolver = entry_data["resolver"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[SelectEntity] = []

    if categories.get("hosts"):
        for moref, host_data in coordinator.data.get("hosts", {}).items():
            name: str = host_data.get("name", moref)
            entities.append(
                HostPowerOperationSelect(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                )
            )
            policies: list[dict[str, Any]] = host_data.get("available_power_policies", [])
            if policies:
                entities.append(
                    HostPowerPolicySelect(
                        coordinator=coordinator,
                        entry=entry,
                        moref=moref,
                        name=name,
                        client=client,
                        resolver=resolver,
                        policies=policies,
                    )
                )

    if categories.get("vms"):
        for moref, vm_data in coordinator.data.get("vms", {}).items():
            name = vm_data.get("name", moref)
            entities.append(
                VmPowerOperationSelect(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                )
            )
            entities.append(
                VmSnapshotSelect(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                )
            )

    async_add_entities(entities)


class HostPowerPolicySelect(VSphereEntity, SelectEntity):
    """Select entity to change a host's power policy."""

    _attr_entity_category = EntityCategory.CONFIG
    _attr_icon = "mdi:lightning-bolt"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
        policies: list[dict[str, Any]],
    ) -> None:
        """Initialize the host power policy select."""
        super().__init__(coordinator, entry, "hosts", moref, name)
        self._client = client
        self._resolver = resolver
        self._attr_unique_id = f"{entry.entry_id}_{moref}_power_policy"
        self._attr_translation_key = "power_policy"
        # Build initial options list from policy short names
        self._attr_options = [p.get("short_name", str(p.get("key", ""))) for p in policies]

    @property
    def options(self) -> list[str]:
        """Return the list of available options, updated from coordinator data."""
        obj_data = self._get_data()
        if obj_data is None:
            return self._attr_options
        policies: list[dict[str, Any]] = obj_data.get("available_power_policies", [])
        if policies:
            return [p.get("short_name", str(p.get("key", ""))) for p in policies]
        return self._attr_options

    @property
    def current_option(self) -> str | None:
        """Return the currently active power policy."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        policy = obj_data.get("power_policy")
        if policy and policy in self.options:
            return policy
        return None

    async def async_select_option(self, option: str) -> None:
        """Change the host power policy."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.POWER_POLICY):
            raise HomeAssistantError(f"Power policy change is not allowed for host {self._moref}")
        try:
            await self.hass.async_add_executor_job(self._client.host_set_power_policy, self._moref, option)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to set power policy on host {self._moref}: {err}") from err


class VmPowerOperationSelect(VSphereEntity, SelectEntity):
    """Select entity for choosing a VM power operation."""

    _attr_icon = "mdi:power"
    _attr_translation_key = "vm_power_operation"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
    ) -> None:
        """Initialize the VM power operation select."""
        super().__init__(coordinator, entry, "vms", moref, name)
        self._attr_unique_id = f"{entry.entry_id}_{moref}_vm_power_operation"
        self._attr_options = list(VM_POWER_OPERATIONS)
        self._attr_current_option = self._attr_options[0]

    async def async_select_option(self, option: str) -> None:
        """Update the selected power operation."""
        self._attr_current_option = option
        self.async_write_ha_state()


class HostPowerOperationSelect(VSphereEntity, SelectEntity):
    """Select entity for choosing a host power operation."""

    _attr_icon = "mdi:power"
    _attr_translation_key = "host_power_operation"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
    ) -> None:
        """Initialize the host power operation select."""
        super().__init__(coordinator, entry, "hosts", moref, name)
        self._attr_unique_id = f"{entry.entry_id}_{moref}_host_power_operation"
        self._attr_options = list(HOST_POWER_OPERATIONS)
        self._attr_current_option = self._attr_options[0]

    async def async_select_option(self, option: str) -> None:
        """Update the selected power operation."""
        self._attr_current_option = option
        self.async_write_ha_state()


class VmSnapshotSelect(VSphereEntity, SelectEntity):
    """Select entity listing VM snapshots for targeted removal."""

    _attr_icon = "mdi:camera"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
    ) -> None:
        """Initialize the VM snapshot select."""
        super().__init__(coordinator, entry, "vms", moref, name)
        self._attr_unique_id = f"{entry.entry_id}_{moref}_snapshot_select"
        self._attr_translation_key = "snapshot"
        self._selected: str | None = None

    @property
    def available(self) -> bool:
        """Unavailable when the VM has no snapshots."""
        obj_data = self._get_data()
        if obj_data is None:
            return False
        return bool(obj_data.get("snapshots")) and super().available

    @property
    def options(self) -> list[str]:
        """Return snapshot names plus 'All snapshots', or empty when none exist."""
        obj_data = self._get_data()
        snapshots: list[dict[str, str]] = obj_data.get("snapshots", []) if obj_data else []
        if not snapshots:
            return []
        names = [s["name"] for s in snapshots]
        names.append(_ALL_SNAPSHOTS)
        return names

    @property
    def current_option(self) -> str | None:
        """Return the currently selected snapshot.

        Does NOT auto-default to avoid silently targeting the wrong snapshot
        for destructive removal operations after data refreshes.
        """
        opts = self.options
        if self._selected and self._selected in opts:
            return self._selected
        return None

    async def async_select_option(self, option: str) -> None:
        """Update the selected snapshot."""
        self._selected = option
        self.async_write_ha_state()
