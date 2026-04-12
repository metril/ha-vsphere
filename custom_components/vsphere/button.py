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
    SNAP_FIRST,
    SNAP_LAST,
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
                VmShutdownButton,
                VmRebootButton,
                VmResetButton,
                VmSuspendButton,
                VmSnapshotCreateButton,
                VmSnapshotRemoveAllButton,
                VmSnapshotRemoveFirstButton,
                VmSnapshotRemoveLastButton,
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

    _button_name: str
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
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{self._unique_id_suffix}"
        self._attr_name = self._button_name

    async def async_press(self) -> None:
        """Handle button press — implemented by subclasses."""
        raise NotImplementedError


class HostShutdownButton(_VSphereButton):
    """Button to shut down a host."""

    _button_name = "Shutdown"
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
        """Shut down the host."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.SHUTDOWN):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.SHUTDOWN))
        try:
            await self.hass.async_add_executor_job(self._client.host_power, self._moref, "shutdown")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to shut down host {self._moref}: {err}") from err


class HostRebootButton(_VSphereButton):
    """Button to reboot a host."""

    _button_name = "Reboot"
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
        """Reboot the host."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.REBOOT):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.REBOOT))
        try:
            await self.hass.async_add_executor_job(self._client.host_power, self._moref, "reboot")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to reboot host {self._moref}: {err}") from err


class VmShutdownButton(_VSphereButton):
    """Button to gracefully shut down a VM via VMware Tools."""

    _button_name = "Shutdown"
    _unique_id_suffix = "vm_shutdown"
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
        super().__init__(coordinator, entry, "vms", moref, name, client, resolver)

    async def async_press(self) -> None:
        """Gracefully shut down the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SHUTDOWN):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SHUTDOWN))
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, "shutdown")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to shut down VM {self._moref}: {err}") from err


class VmRebootButton(_VSphereButton):
    """Button to reboot a VM."""

    _button_name = "Reboot"
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

    _button_name = "Reset"
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

    _button_name = "Suspend"
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

    _button_name = "Create Snapshot"
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


class VmSnapshotRemoveAllButton(_VSphereButton):
    """Button to remove all VM snapshots."""

    _button_name = "Remove All Snapshots"
    _unique_id_suffix = "vm_snapshot_remove_all"
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
        """Remove all snapshots from the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SNAPSHOT_REMOVE):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SNAPSHOT_REMOVE))
        try:
            await self.hass.async_add_executor_job(self._client.remove_snapshot, self._moref, SNAP_ALL)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to remove all snapshots for VM {self._moref}: {err}") from err


class VmSnapshotRemoveFirstButton(_VSphereButton):
    """Button to remove the first/oldest VM snapshot."""

    _button_name = "Remove First Snapshot"
    _unique_id_suffix = "vm_snapshot_remove_first"
    _attr_icon = "mdi:camera-minus"

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
        """Remove the oldest snapshot from the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SNAPSHOT_REMOVE):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SNAPSHOT_REMOVE))
        try:
            await self.hass.async_add_executor_job(self._client.remove_snapshot, self._moref, SNAP_FIRST)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to remove first snapshot for VM {self._moref}: {err}") from err


class VmSnapshotRemoveLastButton(_VSphereButton):
    """Button to remove the last/newest VM snapshot."""

    _button_name = "Remove Last Snapshot"
    _unique_id_suffix = "vm_snapshot_remove_last"
    _attr_icon = "mdi:camera-minus"

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
        """Remove the newest snapshot from the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.SNAPSHOT_REMOVE):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, VmAction.SNAPSHOT_REMOVE))
        try:
            await self.hass.async_add_executor_job(self._client.remove_snapshot, self._moref, SNAP_LAST)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to remove last snapshot for VM {self._moref}: {err}") from err
