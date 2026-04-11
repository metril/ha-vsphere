"""Binary sensor platform for vSphere Control integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.const import EntityCategory

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN
from .entity import VSphereEntity

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .coordinator import VSphereData


@dataclass(frozen=True, kw_only=True)
class VSphereBinarySensorDescription(BinarySensorEntityDescription):
    """Describes a vSphere binary sensor."""

    value_fn: Callable[[dict[str, Any]], bool | None] = lambda d: None


# ---------------------------------------------------------------------------
# Host binary sensors
# ---------------------------------------------------------------------------

HOST_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="powered_on",
        translation_key="powered_on",
        name="Powered On",
        device_class=BinarySensorDeviceClass.POWER,
        value_fn=lambda d: d.get("state") == "poweredOn",
    ),
    VSphereBinarySensorDescription(
        key="maintenance_mode",
        translation_key="maintenance_mode",
        name="Maintenance Mode",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: bool(d.get("maintenance_mode")),
    ),
    VSphereBinarySensorDescription(
        key="shutdown_supported",
        translation_key="shutdown_supported",
        name="Shutdown Supported",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: bool(d.get("shutdown_supported")),
    ),
)

# ---------------------------------------------------------------------------
# VM binary sensors
# ---------------------------------------------------------------------------

VM_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="powered_on",
        translation_key="powered_on",
        name="Powered On",
        device_class=BinarySensorDeviceClass.POWER,
        value_fn=lambda d: d.get("power_state") == "poweredOn",
    ),
    VSphereBinarySensorDescription(
        key="tools_running",
        translation_key="tools_running",
        name="VMware Tools Running",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("tools_status") in ("toolsOk", "toolsOld"),
    ),
)

# ---------------------------------------------------------------------------
# Binary sensor map: category → (descriptions, coordinator data key)
# ---------------------------------------------------------------------------

BINARY_SENSOR_MAP: dict[
    str, tuple[tuple[VSphereBinarySensorDescription, ...], str]
] = {
    "hosts": (HOST_BINARY_SENSORS, "hosts"),
    "vms": (VM_BINARY_SENSORS, "vms"),
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere binary sensors from a config entry."""
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = data["coordinator"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[VSphereBinarySensor] = []

    for category, (descriptions, data_key) in BINARY_SENSOR_MAP.items():
        if not categories.get(category):
            continue
        for moref, obj_data in coordinator.data.get(data_key, {}).items():
            name: str = obj_data.get("name", moref)
            for description in descriptions:
                entities.append(
                    VSphereBinarySensor(
                        coordinator=coordinator,
                        entry=entry,
                        object_type=data_key,
                        moref=moref,
                        name=name,
                        description=description,
                    )
                )

    async_add_entities(entities)


class VSphereBinarySensor(VSphereEntity, BinarySensorEntity):
    """A binary sensor entity representing a vSphere boolean state."""

    entity_description: VSphereBinarySensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
        description: VSphereBinarySensorDescription,
    ) -> None:
        """Initialize the binary sensor."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{description.key}"

    @property
    def is_on(self) -> bool | None:
        """Return True if the binary sensor is on."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return self.entity_description.value_fn(obj_data)
