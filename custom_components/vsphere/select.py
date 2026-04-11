"""Select platform for vSphere Control integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from homeassistant.components.select import SelectEntity
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
            allowed = resolver.allowed_actions("hosts", moref)
            policies: list[dict[str, Any]] = host_data.get("available_power_policies", [])
            if HostAction.POWER_POLICY in allowed and policies:
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
        self._moref = moref
        self._client = client
        self._resolver = resolver
        self._attr_unique_id = f"{entry.entry_id}_{moref}_power_policy"
        self._attr_name = "Power Policy"
        # Build initial options list from policy short names
        self._attr_options = [
            p.get("short_name", str(p.get("key", ""))) for p in policies
        ]

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
        return obj_data.get("power_policy") or None

    async def async_select_option(self, option: str) -> None:
        """Change the host power policy."""
        if not self._resolver.is_allowed("hosts", self._moref, HostAction.POWER_POLICY):
            raise HomeAssistantError(
                f"Power policy change is not allowed for host {self._moref}"
            )
        try:
            await self.hass.async_add_executor_job(
                self._client.host_set_power_policy, self._moref, option
            )
        except VSphereOperationError as err:
            raise HomeAssistantError(
                f"Failed to set power policy on host {self._moref}: {err}"
            ) from err
