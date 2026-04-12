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

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN, Category
from .entity import VSphereChildEntity, VSphereEntity

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
# Cluster binary sensors
# ---------------------------------------------------------------------------

CLUSTER_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="drs_enabled",
        translation_key="drs_enabled",
        name="DRS Enabled",
        value_fn=lambda d: bool(d.get("drs_enabled")),
    ),
    VSphereBinarySensorDescription(
        key="ha_enabled",
        translation_key="ha_enabled",
        name="HA Enabled",
        value_fn=lambda d: bool(d.get("ha_enabled")),
    ),
    VSphereBinarySensorDescription(
        key="ha_admission_control",
        translation_key="ha_admission_control",
        name="HA Admission Control",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: bool(d.get("ha_admission_control")),
    ),
)

# ---------------------------------------------------------------------------
# Network binary sensors (pNIC only)
# ---------------------------------------------------------------------------

PNIC_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="link_up",
        translation_key="link_up",
        name="Link Up",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        value_fn=lambda d: bool(d.get("link_up")),
    ),
)

# ---------------------------------------------------------------------------
# Alarm binary sensors (per-entity, keyed by moref in coordinator data["alarms"])
# ---------------------------------------------------------------------------

ALARM_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="alarm_active",
        name="Alarm",
        device_class=BinarySensorDeviceClass.PROBLEM,
        value_fn=lambda data: any(a.get("status") == "red" for a in data) if isinstance(data, list) else False,
    ),
)

# ---------------------------------------------------------------------------
# Binary sensor map: category → (descriptions, coordinator data key)
# ---------------------------------------------------------------------------

BINARY_SENSOR_MAP: dict[str, tuple[tuple[VSphereBinarySensorDescription, ...], str]] = {
    "hosts": (HOST_BINARY_SENSORS, "hosts"),
    "vms": (VM_BINARY_SENSORS, "vms"),
    "clusters": (CLUSTER_BINARY_SENSORS, "clusters"),
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

    entities: list[VSphereBinarySensor | VSphereAlarmBinarySensor] = []

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

    # Network binary sensors: pNIC link_up attached to parent Host device
    if categories.get("network"):
        hosts_data = coordinator.data.get("hosts", {})
        for net_moref, obj_data in coordinator.data.get("networks", {}).items():
            if obj_data.get("type") != "pnic":
                continue
            name = obj_data.get("name", net_moref)
            host_moref = obj_data.get("host_moref", net_moref.split("_")[0] if "_" in net_moref else "")
            host_name = hosts_data.get(host_moref, {}).get("name", host_moref)
            for description in PNIC_BINARY_SENSORS:
                entities.append(
                    VSphereChildBinarySensor(
                        coordinator=coordinator,
                        entry=entry,
                        parent_object_type="hosts",
                        parent_moref=host_moref,
                        parent_name=host_name,
                        data_category="networks",
                        data_moref=net_moref,
                        description=description,
                        entity_name=name,
                    )
                )

    # Alarm status binary sensors — created for each host/VM when events_alarms is enabled
    if categories.get(Category.EVENTS_ALARMS):
        for moref, obj_data in coordinator.data.get("hosts", {}).items():
            name = obj_data.get("name", moref)
            for desc in ALARM_BINARY_SENSORS:
                entities.append(
                    VSphereAlarmBinarySensor(
                        coordinator=coordinator,
                        entry=entry,
                        object_type="hosts",
                        moref=moref,
                        name=name,
                        description=desc,
                    )
                )
        for moref, obj_data in coordinator.data.get("vms", {}).items():
            name = obj_data.get("name", moref)
            for desc in ALARM_BINARY_SENSORS:
                entities.append(
                    VSphereAlarmBinarySensor(
                        coordinator=coordinator,
                        entry=entry,
                        object_type="vms",
                        moref=moref,
                        name=name,
                        description=desc,
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


class VSphereChildBinarySensor(VSphereChildEntity, BinarySensorEntity):
    """Binary sensor attached to a parent device but reading from a different data path."""

    _attr_has_entity_name = False
    entity_description: VSphereBinarySensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        parent_object_type: str,
        parent_moref: str,
        parent_name: str,
        data_category: str,
        data_moref: str,
        description: VSphereBinarySensorDescription,
        entity_name: str,
    ) -> None:
        """Initialize the child binary sensor."""
        super().__init__(coordinator, entry, parent_object_type, parent_moref, parent_name, data_category, data_moref)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{data_moref}_{description.key}"
        self._attr_name = f"{entity_name} {description.name}" if description.name else entity_name

    @property
    def is_on(self) -> bool | None:
        """Return True if the binary sensor is on."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return self.entity_description.value_fn(obj_data)


class VSphereAlarmBinarySensor(VSphereEntity, BinarySensorEntity):
    """Binary sensor for alarm active status per host or VM."""

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
        """Initialize the alarm binary sensor."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{description.key}"

    @property
    def is_on(self) -> bool | None:
        """Return True if any red alarm is active for this entity."""
        if not self.coordinator.data:
            return None
        alarms = self.coordinator.data.get("alarms", {}).get(self._moref, [])
        return self.entity_description.value_fn(alarms)
