"""Switch platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_call_later

from .const import (
    CONF_CATEGORIES,
    CONF_FORCE_ARM_TIMEOUT,
    CONF_RESTRICTIONS,
    DEFAULT_CATEGORIES,
    DEFAULT_FORCE_ARM_TIMEOUT,
    DOMAIN,
    HostAction,
    VmAction,
)
from .entity import VSphereEntity
from .exceptions import VSphereOperationError

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import CALLBACK_TYPE, HomeAssistant
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
    """Set up vSphere switches from a config entry."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = entry_data["coordinator"]
    client: VSphereClient = entry_data["client"]
    resolver: PermissionResolver = entry_data["resolver"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[SwitchEntity] = []

    if categories.get("vms"):
        for moref, vm_data in coordinator.data.get("vms", {}).items():
            name: str = vm_data.get("name", moref)
            entities.append(
                VmPowerSwitch(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                    client=client,
                    resolver=resolver,
                )
            )
            entities.append(
                VmForceArmSwitch(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                )
            )

    if categories.get("hosts"):
        for moref, host_data in coordinator.data.get("hosts", {}).items():
            name = host_data.get("name", moref)
            entities.append(
                HostMaintenanceSwitch(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                    client=client,
                    resolver=resolver,
                )
            )
            entities.append(
                HostForceArmSwitch(
                    coordinator=coordinator,
                    entry=entry,
                    moref=moref,
                    name=name,
                )
            )

    async_add_entities(entities)


class VmPowerSwitch(VSphereEntity, SwitchEntity):
    """Switch to power a VM on or gracefully shut down.

    When unarmed: turn off = graceful ShutdownGuest (requires VMware Tools).
    When armed: turn off = hard PowerOffVM (immediate).
    """

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
        """Initialize the VM power switch."""
        super().__init__(coordinator, entry, "vms", moref, name)
        self._client = client
        self._resolver = resolver
        self._entry_id = entry.entry_id
        self._attr_unique_id = f"{entry.entry_id}_{moref}_power_switch"
        self._attr_translation_key = "power"

    @property
    def is_on(self) -> bool | None:
        """Return True if the VM is powered on."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return obj_data.get("power_state") == "poweredOn"

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Power on the VM."""
        if not self._resolver.is_allowed("vms", self._moref, VmAction.POWER_ON):
            raise HomeAssistantError(f"Power on is not allowed for VM {self._moref}")
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, "power_on")
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to power on VM {self._moref}: {err}") from err
        self._attr_is_on = True
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Shut down the VM (graceful or hard depending on arm state)."""
        from . import clear_armed, is_armed  # noqa: PLC0415

        armed = is_armed(self.hass, self._entry_id, self._moref)
        check_action = VmAction.POWER_OFF if armed else VmAction.SHUTDOWN
        if not self._resolver.is_allowed("vms", self._moref, check_action):
            raise HomeAssistantError(self._resolver.explain("vms", self._moref, check_action))
        action = "power_off" if armed else "shutdown"
        try:
            await self.hass.async_add_executor_job(self._client.vm_power, self._moref, action)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to power off VM {self._moref}: {err}") from err
        if armed:
            clear_armed(self.hass, self._entry_id, self._moref)
        self._attr_is_on = False
        self.async_write_ha_state()


# ---------------------------------------------------------------------------
# Force Power arm switches
# ---------------------------------------------------------------------------


class _ForceArmSwitch(VSphereEntity, SwitchEntity):
    """Base class for the force power arm toggle.

    When ON, the associated power control (VM switch / host buttons) uses
    hard/forced operations instead of graceful ones.  Auto-disarms after a
    configurable timeout.
    """

    _attr_icon = "mdi:shield-alert"
    _attr_translation_key = "force_arm"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
    ) -> None:
        """Initialize the force arm switch."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self._entry_id = entry.entry_id
        self._attr_unique_id = f"{entry.entry_id}_{moref}_force_arm"
        self._disarm_cancel: CALLBACK_TYPE | None = None

    @property
    def is_on(self) -> bool:
        """Return True if armed."""
        return self.hass.data[DOMAIN][self._entry_id]["armed"].get(self._moref, False)

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Arm force power."""
        # Cancel any pending disarm timer
        if self._disarm_cancel is not None:
            self._disarm_cancel()
            self._disarm_cancel = None

        self.hass.data[DOMAIN][self._entry_id]["armed"][self._moref] = True

        timeout = self._entry.options.get(CONF_RESTRICTIONS, {}).get(CONF_FORCE_ARM_TIMEOUT, DEFAULT_FORCE_ARM_TIMEOUT)
        self._disarm_cancel = async_call_later(self.hass, timeout, self._auto_disarm)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disarm force power."""
        self._disarm(write_state=True)

    def _disarm(self, *, write_state: bool) -> None:
        """Clear the armed state and cancel the timer."""
        if self._disarm_cancel is not None:
            self._disarm_cancel()
            self._disarm_cancel = None
        self.hass.data[DOMAIN][self._entry_id]["armed"].pop(self._moref, None)
        if write_state:
            self.async_write_ha_state()

    def _auto_disarm(self, _now: Any) -> None:
        """Auto-disarm callback after timeout."""
        self._disarm_cancel = None
        self.hass.data[DOMAIN][self._entry_id]["armed"].pop(self._moref, None)
        self.async_write_ha_state()

    async def async_will_remove_from_hass(self) -> None:
        """Clean up timer on entity removal."""
        if self._disarm_cancel is not None:
            self._disarm_cancel()
            self._disarm_cancel = None


class VmForceArmSwitch(_ForceArmSwitch):
    """Arm toggle for VM force power off."""

    def __init__(self, coordinator: VSphereData, entry: ConfigEntry, moref: str, name: str) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "vms", moref, name)


class HostForceArmSwitch(_ForceArmSwitch):
    """Arm toggle for host force shutdown/reboot."""

    _attr_translation_key = "force_arm_host"

    def __init__(self, coordinator: VSphereData, entry: ConfigEntry, moref: str, name: str) -> None:
        """Initialize."""
        super().__init__(coordinator, entry, "hosts", moref, name)


# ---------------------------------------------------------------------------
# Host maintenance mode switch
# ---------------------------------------------------------------------------


class HostMaintenanceSwitch(VSphereEntity, SwitchEntity):
    """Switch to toggle host maintenance mode."""

    _attr_entity_category = EntityCategory.CONFIG
    _attr_icon = "mdi:wrench"

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        moref: str,
        name: str,
        client: VSphereClient,
        resolver: PermissionResolver,
    ) -> None:
        """Initialize the host maintenance mode switch."""
        super().__init__(coordinator, entry, "hosts", moref, name)
        self._client = client
        self._resolver = resolver
        self._attr_unique_id = f"{entry.entry_id}_{moref}_maintenance_switch"
        self._attr_translation_key = "maintenance_mode"

    @property
    def is_on(self) -> bool | None:
        """Return True if the host is in maintenance mode."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return bool(obj_data.get("maintenance_mode"))

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enter maintenance mode."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.MAINTENANCE):
            raise HomeAssistantError(f"Maintenance mode change is not allowed for host {self._moref}")
        try:
            await self.hass.async_add_executor_job(self._client.host_set_maintenance_mode, self._moref, True)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to enter maintenance mode on host {self._moref}: {err}") from err
        self._attr_is_on = True
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Exit maintenance mode."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.MAINTENANCE):
            raise HomeAssistantError(f"Maintenance mode change is not allowed for host {self._moref}")
        try:
            await self.hass.async_add_executor_job(self._client.host_set_maintenance_mode, self._moref, False)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to exit maintenance mode on host {self._moref}: {err}") from err
        self._attr_is_on = False
        self.async_write_ha_state()
