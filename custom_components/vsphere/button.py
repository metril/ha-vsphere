"""Button platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from homeassistant.components.button import ButtonDeviceClass, ButtonEntity
from homeassistant.exceptions import HomeAssistantError

from .const import (
    CONF_CATEGORIES,
    DEFAULT_CATEGORIES,
    DOMAIN,
    SNAP_ALL,
    HostAction,
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
            for host_button_cls in (HostShutdownButton, HostRebootButton):
                entities.append(
                    host_button_cls(
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
                VmRebootButton,
                VmResetButton,
                VmSuspendButton,
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


class HostShutdownButton(_VSphereButton):
    """Button to shut down a host."""

    _attr_translation_key = "host_shutdown"
    _unique_id_suffix = "host_shutdown"
    _attr_icon = "mdi:power"

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
        """Shut down the host (graceful or forced depending on arm state)."""
        from . import clear_armed, is_armed  # noqa: PLC0415

        if not self._resolver.is_allowed("hosts", self._moref, HostAction.SHUTDOWN):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.SHUTDOWN))
        armed = is_armed(self.hass, self._entry_id, self._moref)
        try:
            await self.hass.async_add_executor_job(self._client.host_power, self._moref, "shutdown", armed)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to shut down host {self._moref}: {err}") from err
        if armed:
            clear_armed(self.hass, self._entry_id, self._moref)


class HostRebootButton(_VSphereButton):
    """Button to reboot a host."""

    _attr_translation_key = "host_reboot"
    _unique_id_suffix = "host_reboot"
    _attr_device_class = ButtonDeviceClass.RESTART
    _attr_icon = "mdi:restart"

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
        """Reboot the host (graceful or forced depending on arm state)."""
        from . import clear_armed, is_armed  # noqa: PLC0415

        if not self._resolver.is_allowed("hosts", self._moref, HostAction.REBOOT):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.REBOOT))
        armed = is_armed(self.hass, self._entry_id, self._moref)
        try:
            await self.hass.async_add_executor_job(self._client.host_power, self._moref, "reboot", armed)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to reboot host {self._moref}: {err}") from err
        if armed:
            clear_armed(self.hass, self._entry_id, self._moref)


class VmRebootButton(_VSphereButton):
    """Button to reboot a VM."""

    _attr_translation_key = "vm_reboot"
    _unique_id_suffix = "vm_reboot"
    _attr_device_class = ButtonDeviceClass.RESTART
    _attr_icon = "mdi:restart"

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
        """Reboot the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.REBOOT):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.REBOOT))
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, "reboot")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to reboot VM {self._moref}: {err}") from err


class VmResetButton(_VSphereButton):
    """Button to hard-reset a VM."""

    _attr_translation_key = "vm_reset"
    _unique_id_suffix = "vm_reset"
    _attr_device_class = ButtonDeviceClass.RESTART
    _attr_icon = "mdi:power-cycle"

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
        """Hard-reset the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.RESET):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.RESET))
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, "reset")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to reset VM {self._moref}: {err}") from err


class VmSuspendButton(_VSphereButton):
    """Button to suspend a VM."""

    _attr_translation_key = "vm_suspend"
    _unique_id_suffix = "vm_suspend"
    _attr_icon = "mdi:pause-circle"

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
        """Suspend the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SUSPEND):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SUSPEND))
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, "suspend")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to suspend VM {self._moref}: {err}") from err


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

        # Find the snapshot select entity for this VM to read its current selection
        from .select import _ALL_SNAPSHOTS  # noqa: PLC0415

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
            if selected == _ALL_SNAPSHOTS:
                await self.hass.async_add_executor_job(self._client.remove_snapshot, self._moref, SNAP_ALL)
            else:
                # Find the moref for the selected snapshot name
                snap_moref = next((s["moref"] for s in snapshots if s["name"] == selected), None)
                if snap_moref is None:
                    raise HomeAssistantError(f"Snapshot '{selected}' not found on VM {self._moref}")
                await self.hass.async_add_executor_job(self._client.remove_snapshot_by_moref, self._moref, snap_moref)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to remove snapshot on VM {self._moref}: {err}") from err
