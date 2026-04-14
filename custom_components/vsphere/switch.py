"""Switch platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN, HostAction
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
    """Set up vSphere switches from a config entry."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = entry_data["coordinator"]
    client: VSphereClient = entry_data["client"]
    resolver: PermissionResolver = entry_data["resolver"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[SwitchEntity] = []

    if categories.get("hosts"):
        for moref, host_data in coordinator.data.get("hosts", {}).items():
            name: str = host_data.get("name", moref)
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

    async_add_entities(entities)


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

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Exit maintenance mode."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.MAINTENANCE):
            raise HomeAssistantError(f"Maintenance mode change is not allowed for host {self._moref}")
        try:
            await self.hass.async_add_executor_job(self._client.host_set_maintenance_mode, self._moref, False)
        except VSphereOperationError as err:
            raise HomeAssistantError(f"Failed to exit maintenance mode on host {self._moref}: {err}") from err
