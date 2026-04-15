"""Button platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from homeassistant.components.button import ButtonEntity
from homeassistant.exceptions import HomeAssistantError

from .const import (
    CONF_CATEGORIES,
    DEFAULT_CATEGORIES,
    DOMAIN,
    SNAP_ALL,
    SNAP_SELECT_ALL,
    VmAction,
)
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


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere buttons from a config entry."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = entry_data["coordinator"]
    client: VSphereClient = entry_data["client"]
    resolver: PermissionResolver = entry_data["resolver"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[ButtonEntity] = []

    if categories.get("hosts"):
        for moref, host_data in coordinator.data.get("hosts", {}).items():
            name: str = host_data.get("name", moref)
            entities.append(
                HostPowerExecuteButton(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                    client=client,
                    resolver=resolver,
                )
            )

    if categories.get("vms"):
        for moref, vm_data in coordinator.data.get("vms", {}).items():
            name = vm_data.get("name", moref)
            for button_cls in (
                VmPowerExecuteButton,
                VmSnapshotCreateButton,
                VmSnapshotRemoveButton,
            ):
                entities.append(
                    button_cls(
                        coordinator=coordinator,
                        entry=entry,
                        moref=moref,
                        name=name,
                        client=client,
                        resolver=resolver,
                    )
                )

    async_add_entities(entities)


class _VSphereButton(VSphereEntity, ButtonEntity):
    """Base class for vSphere button entities."""

    _unique_id_suffix: str

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize the button."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self._client = client
        self._resolver = resolver
        self._entry_id = entry.entry_id
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{self._unique_id_suffix}"

    async def async_press(self) -> None:
        """Handle button press — implemented by subclasses."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Power execute buttons (read from power operation select)
# ---------------------------------------------------------------------------


class _PowerExecuteButton(_VSphereButton):
    """Base class for power execute buttons that read from a power operation select."""

    _attr_icon = "mdi:play-circle"
    _select_suffix: str  # Override in subclass: "vm_power_operation" or "host_power_operation"

    def _read_power_select(self) -> str | None:
        """Read the current selection from the power operation select entity."""
        from homeassistant.helpers import entity_registry as er  # noqa: PLC0415

        select_unique_id = f"{self._entry_id}_{self._moref}_{self._select_suffix}"
        entity_reg = er.async_get(self.hass)
        select_entity_id = entity_reg.async_get_entity_id("select", DOMAIN, select_unique_id)
        if select_entity_id:
            state = self.hass.states.get(select_entity_id)
            if state and state.state not in ("unknown", "unavailable", ""):
                return state.state
        return None


class VmPowerExecuteButton(_PowerExecuteButton):
    """Button to execute the selected VM power operation."""

    _attr_translation_key = "vm_power_execute"
    _unique_id_suffix = "vm_power_execute"
    _select_suffix = "vm_power_operation"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "vms", moref, name, client, resolver)

    async def async_press(self) -> None:
        """Execute the selected VM power operation."""
        from .select import VM_POWER_OPERATIONS  # noqa: PLC0415

        selected = self._read_power_select()
        if not selected:
            raise HomeAssistantError(
                "No power operation selected. Choose an operation from the Power (Operation) selector first."
            )

        if selected not in VM_POWER_OPERATIONS:
            raise HomeAssistantError(f"Unknown power operation: {selected}")

        action_enum, client_action = VM_POWER_OPERATIONS[selected]
        if not self._resolver.is_allowed("vms", self._moref, action_enum):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, action_enum))
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, client_action)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to execute {selected} on VM {self._moref}: {err}") from err


class HostPowerExecuteButton(_PowerExecuteButton):
    """Button to execute the selected host power operation."""

    _attr_translation_key = "host_power_execute"
    _unique_id_suffix = "host_power_execute"
    _select_suffix = "host_power_operation"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "hosts", moref, name, client, resolver)

    async def async_press(self) -> None:
        """Execute the selected host power operation."""
        from .select import HOST_POWER_OPERATIONS  # noqa: PLC0415

        selected = self._read_power_select()
        if not selected:
            raise HomeAssistantError(
                "No power operation selected. Choose an operation from the Power (Operation) selector first."
            )

        if selected not in HOST_POWER_OPERATIONS:
            raise HomeAssistantError(f"Unknown power operation: {selected}")

        action_enum, client_action = HOST_POWER_OPERATIONS[selected]
        if not self._resolver.is_allowed("hosts", self._moref, action_enum):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, action_enum))
        try:
            await self.hass.async_add_executor_job(self._client.host_power, self._moref, client_action, True)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to execute {selected} on host {self._moref}: {err}") from err


# ---------------------------------------------------------------------------
# Snapshot buttons
# ---------------------------------------------------------------------------


class VmSnapshotCreateButton(_VSphereButton):
    """Button to create a VM snapshot."""

    _attr_translation_key = "vm_snapshot_create"
    _unique_id_suffix = "vm_snapshot_create"
    _attr_icon = "mdi:camera"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "vms", moref, name, client, resolver)

    async def async_press(self) -> None:
        """Create a snapshot of the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SNAPSHOT_CREATE):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SNAPSHOT_CREATE))
        try:
            await self.hass.async_add_executor_job(self._client.create_snapshot, self._moref)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to create snapshot for VM {self._moref}: {err}") from err


class VmSnapshotRemoveButton(_VSphereButton):
    """Button to remove the snapshot selected in the Snapshot select entity.

    If "All snapshots" is selected, removes all snapshots.
    """

    _attr_translation_key = "vm_snapshot_remove"
    _unique_id_suffix = "vm_snapshot_remove"
    _attr_icon = "mdi:camera-off"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "vms", moref, name, client, resolver)

    async def async_press(self) -> None:
        """Remove the selected snapshot (or all)."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SNAPSHOT_REMOVE):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SNAPSHOT_REMOVE))

        obj_data = self._get_data()
        snapshots: list[dict[str, str]] = obj_data.get("snapshots", []) if obj_data else []

        # Look up the VmSnapshotSelect entity to get the current selection
        from homeassistant.helpers import entity_registry as er  # noqa: PLC0415

        select_unique_id = f"{self._entry_id}_{self._moref}_snapshot_select"
        selected: str | None = None
        entity_reg = er.async_get(self.hass)
        select_entity_id = entity_reg.async_get_entity_id("select", DOMAIN, select_unique_id)
        if select_entity_id:
            state = self.hass.states.get(select_entity_id)
            if state:
                selected = state.state

        if not selected or selected in ("unknown", "unavailable", ""):
            raise HomeAssistantError("No snapshot selected. Choose a snapshot from the Snapshot selector first.")

        try:
            if selected == SNAP_SELECT_ALL:
                await self.hass.async_add_executor_job(self._client.remove_snapshot, self._moref, SNAP_ALL)
            else:
                # Find the moref for the selected snapshot name
                snap_moref = next((s["moref"] for s in snapshots if s["name"] == selected), None)
                if snap_moref is None:
                    raise HomeAssistantError(f"Snapshot '{selected}' not found on VM {self._moref}")
                await self.hass.async_add_executor_job(self._client.remove_snapshot_by_moref, self._moref, snap_moref)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to remove snapshot on VM {self._moref}: {err}") from err
