"""Switch platform for vSphere Control integration."""

from __future__ import annotations

import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError

from .const import CONF_CATEGORIES, CONF_ENTITY_FILTER, DEFAULT_CATEGORIES, HostAction
from .entity import VSphereEntity, async_setup_discovery, iter_rows
from .exceptions import VSphereConnectionError, VSphereOperationError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .coordinator import VSphereConfigEntry, VSphereData, VSphereRuntimeData
    from .entity import Spec
    from .permissions import PermissionResolver
    from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: VSphereConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere switches from a config entry."""
    rt = entry.runtime_data
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)
    entity_filter: dict[str, Any] = entry.options.get(CONF_ENTITY_FILTER, {})

    def specs() -> Iterator[Spec]:
        for moref, row in iter_rows(rt.coordinator.data or {}, categories, entity_filter, "hosts"):
            yield ("hosts", moref), partial(_build_host_switches, rt, entry, moref, row)

    async_setup_discovery(entry, rt.coordinator, async_add_entities, specs)


def _build_host_switches(
    rt: VSphereRuntimeData, entry: VSphereConfigEntry, moref: str, row: dict[str, Any]
) -> list[Entity]:
    return [
        HostMaintenanceSwitch(
            coordinator=rt.coordinator,
            entry=entry,
            moref=moref,
            name=row.get("name", moref),
            client=rt.client,
            resolver=rt.resolver,
        )
    ]


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
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.MAINTENANCE))
        try:
            await self.hass.async_add_executor_job(self._client.host_set_maintenance_mode, self._moref, True)
        except (VSphereOperationError, VSphereConnectionError) as err:
            raise HomeAssistantError(f"Failed to enter maintenance mode on host {self._moref}: {err}") from err

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Exit maintenance mode."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.MAINTENANCE):
            raise HomeAssistantError(self._resolver.explain("hosts", self._moref, HostAction.MAINTENANCE))
        try:
            await self.hass.async_add_executor_job(self._client.host_set_maintenance_mode, self._moref, False)
        except (VSphereOperationError, VSphereConnectionError) as err:
            raise HomeAssistantError(f"Failed to exit maintenance mode on host {self._moref}: {err}") from err
