"""Sensor platform for vSphere Control integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    PERCENTAGE,
    EntityCategory,
    UnitOfInformation,
    UnitOfTime,
)

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN
from .entity import VSphereEntity

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .coordinator import VSphereData


@dataclass(frozen=True, kw_only=True)
class VSphereSensorDescription(SensorEntityDescription):
    """Describes a vSphere sensor."""

    value_fn: Callable[[dict[str, Any]], Any] = lambda d: None


# ---------------------------------------------------------------------------
# Host sensors
# ---------------------------------------------------------------------------

HOST_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="cpu_total_ghz",
        translation_key="cpu_total_ghz",
        name="CPU Total",
        native_unit_of_measurement="GHz",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_total_ghz"),
    ),
    VSphereSensorDescription(
        key="cpu_usage_ghz",
        translation_key="cpu_usage_ghz",
        name="CPU Usage",
        native_unit_of_measurement="GHz",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_usage_ghz"),
    ),
    VSphereSensorDescription(
        key="mem_total_gb",
        translation_key="mem_total_gb",
        name="Memory Total",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("mem_total_gb"),
    ),
    VSphereSensorDescription(
        key="mem_usage_gb",
        translation_key="mem_usage_gb",
        name="Memory Usage",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("mem_usage_gb"),
    ),
    VSphereSensorDescription(
        key="uptime_hours",
        translation_key="uptime_hours",
        name="Uptime",
        device_class=SensorDeviceClass.DURATION,
        native_unit_of_measurement=UnitOfTime.HOURS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d.get("uptime_hours"),
    ),
    VSphereSensorDescription(
        key="vm_count",
        translation_key="vm_count",
        name="VM Count",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("vm_count"),
    ),
    VSphereSensorDescription(
        key="version",
        translation_key="version",
        name="Version",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("version"),
    ),
    VSphereSensorDescription(
        key="build",
        translation_key="build",
        name="Build",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("build"),
    ),
)

# ---------------------------------------------------------------------------
# VM sensors
# ---------------------------------------------------------------------------

VM_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="cpu_count",
        translation_key="cpu_count",
        name="CPU Count",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_count"),
    ),
    VSphereSensorDescription(
        key="cpu_use_pct",
        translation_key="cpu_use_pct",
        name="CPU Usage",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_use_pct"),
    ),
    VSphereSensorDescription(
        key="memory_allocated_mb",
        translation_key="memory_allocated_mb",
        name="Memory Allocated",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("memory_allocated_mb"),
    ),
    VSphereSensorDescription(
        key="memory_used_mb",
        translation_key="memory_used_mb",
        name="Memory Used",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("memory_used_mb"),
    ),
    VSphereSensorDescription(
        key="memory_active_mb",
        translation_key="memory_active_mb",
        name="Memory Active",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("memory_active_mb"),
    ),
    VSphereSensorDescription(
        key="used_space_gb",
        translation_key="used_space_gb",
        name="Used Space",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("used_space_gb"),
    ),
    VSphereSensorDescription(
        key="uptime_hours",
        translation_key="uptime_hours",
        name="Uptime",
        device_class=SensorDeviceClass.DURATION,
        native_unit_of_measurement=UnitOfTime.HOURS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d.get("uptime_hours"),
    ),
    VSphereSensorDescription(
        key="snapshots",
        translation_key="snapshots",
        name="Snapshots",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("snapshot_count"),
    ),
    VSphereSensorDescription(
        key="guest_os",
        translation_key="guest_os",
        name="Guest OS",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("guest_os"),
    ),
    VSphereSensorDescription(
        key="guest_ip",
        translation_key="guest_ip",
        name="Guest IP",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("guest_ip"),
    ),
    VSphereSensorDescription(
        key="tools_status",
        translation_key="tools_status",
        name="Tools Status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("tools_status"),
    ),
    VSphereSensorDescription(
        key="status",
        translation_key="status",
        name="Status",
        value_fn=lambda d: d.get("state"),
    ),
    VSphereSensorDescription(
        key="state",
        translation_key="state",
        name="Power State",
        value_fn=lambda d: d.get("power_state"),
    ),
    VSphereSensorDescription(
        key="host_name",
        translation_key="host_name",
        name="Host",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("host_name"),
    ),
)

# ---------------------------------------------------------------------------
# Datastore sensors
# ---------------------------------------------------------------------------

DATASTORE_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="free_space_gb",
        translation_key="free_space_gb",
        name="Free Space",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("free_gb"),
    ),
    VSphereSensorDescription(
        key="total_space_gb",
        translation_key="total_space_gb",
        name="Total Space",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("capacity_gb"),
    ),
    VSphereSensorDescription(
        key="type",
        translation_key="type",
        name="Type",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("type"),
    ),
    VSphereSensorDescription(
        key="connected_hosts",
        translation_key="connected_hosts",
        name="Connected Hosts",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("connected_hosts"),
    ),
    VSphereSensorDescription(
        key="virtual_machines",
        translation_key="virtual_machines",
        name="Virtual Machines",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("virtual_machines"),
    ),
)

# ---------------------------------------------------------------------------
# License sensors
# ---------------------------------------------------------------------------

LICENSE_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="status",
        translation_key="license_status",
        name="Status",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: "active" if (d.get("free", 0) or 0) > 0 else "exhausted",
    ),
    VSphereSensorDescription(
        key="expiration_days",
        translation_key="expiration_days",
        name="Expiration Days",
        native_unit_of_measurement=UnitOfTime.DAYS,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("expiration_days"),
    ),
    VSphereSensorDescription(
        key="product",
        translation_key="product",
        name="Product",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("name"),
    ),
)

# ---------------------------------------------------------------------------
# Sensor map: category → (descriptions, coordinator data key)
# ---------------------------------------------------------------------------

SENSOR_MAP: dict[str, tuple[tuple[VSphereSensorDescription, ...], str]] = {
    "hosts": (HOST_SENSORS, "hosts"),
    "vms": (VM_SENSORS, "vms"),
    "datastores": (DATASTORE_SENSORS, "datastores"),
    "licenses": (LICENSE_SENSORS, "licenses"),
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere sensors from a config entry."""
    data = hass.data[DOMAIN][entry.entry_id]
    coordinator: VSphereData = data["coordinator"]
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)

    entities: list[VSphereSensor] = []

    for category, (descriptions, data_key) in SENSOR_MAP.items():
        if not categories.get(category):
            continue
        for moref, obj_data in coordinator.data.get(data_key, {}).items():
            name: str = obj_data.get("name", moref)
            for description in descriptions:
                entities.append(
                    VSphereSensor(
                        coordinator=coordinator,
                        entry=entry,
                        object_type=data_key,
                        moref=moref,
                        name=name,
                        description=description,
                    )
                )

    async_add_entities(entities)


class VSphereSensor(VSphereEntity, SensorEntity):
    """A sensor entity representing a vSphere data point."""

    entity_description: VSphereSensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
        description: VSphereSensorDescription,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{description.key}"

    @property
    def native_value(self) -> Any:
        """Return the sensor value."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return self.entity_description.value_fn(obj_data)
