"""Binary sensor platform for vSphere Control integration."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.const import EntityCategory

from .const import CONF_CATEGORIES, CONF_ENTITY_FILTER, DEFAULT_CATEGORIES, Category
from .entity import VSphereChildEntity, VSphereEntity, async_setup_discovery, iter_rows

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .coordinator import VSphereConfigEntry, VSphereData
    from .entity import Spec


@dataclass(frozen=True, kw_only=True)
class VSphereBinarySensorDescription(BinarySensorEntityDescription):
    """Describes a vSphere binary sensor."""

    value_fn: Callable[[dict[str, Any]], bool | None] = lambda d: None
    disconnect_behavior: Literal["off", "unknown"] | None = None


# ---------------------------------------------------------------------------
# Host binary sensors
# ---------------------------------------------------------------------------

HOST_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="powered_on",
        translation_key="powered_on",
        name="Powered On",
        device_class=BinarySensorDeviceClass.POWER,
        # A host shutting down may have powerState briefly flip before it's
        # actually off. Treat it as on if either powerState is poweredOn OR
        # vCenter is still connected to it.
        value_fn=lambda d: d.get("state") == "poweredOn" or d.get("connection_state") == "connected",
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
        disconnect_behavior="off",
    ),
    VSphereBinarySensorDescription(
        key="tools_running",
        translation_key="tools_running",
        name="VMware Tools Running",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("tools_status") in ("toolsOk", "toolsOld"),
        disconnect_behavior="off",
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

DVSWITCH_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="nioc_enabled",
        translation_key="nioc_enabled",
        name="NIOC Enabled",
        value_fn=lambda d: bool(d.get("nioc_enabled")),
    ),
)

# ---------------------------------------------------------------------------
# Alarm binary sensors (per-entity, keyed by moref in coordinator data["alarms"])
# ---------------------------------------------------------------------------

ALARM_BINARY_SENSORS: tuple[VSphereBinarySensorDescription, ...] = (
    VSphereBinarySensorDescription(
        key="alarm_active",
        translation_key="alarm_active",
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


_NET_BINARY_MAP: dict[str, tuple[VSphereBinarySensorDescription, ...]] = {
    "pnic": PNIC_BINARY_SENSORS,
    "dvswitch": DVSWITCH_BINARY_SENSORS,
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: VSphereConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up vSphere binary sensors from a config entry."""
    coordinator = entry.runtime_data.coordinator
    categories: dict[str, bool] = entry.options.get(CONF_CATEGORIES, DEFAULT_CATEGORIES)
    entity_filter: dict[str, Any] = entry.options.get(CONF_ENTITY_FILTER, {})

    def specs() -> Iterator[Spec]:
        yield from _binary_sensor_specs(entry, coordinator, categories, entity_filter)

    async_setup_discovery(entry, coordinator, async_add_entities, specs)


def _binary_sensor_specs(
    entry: VSphereConfigEntry,
    coordinator: VSphereData,
    categories: dict[str, bool],
    entity_filter: dict[str, Any],
) -> Iterator[Spec]:
    """Yield (key, factory) for every binary sensor group that should exist right now."""
    data: dict[str, Any] = coordinator.data or {}

    for category, (descriptions, data_key) in BINARY_SENSOR_MAP.items():
        for moref, row in iter_rows(data, categories, entity_filter, category, data_key):
            yield (
                (category, moref),
                partial(_build_object, coordinator, entry, data_key, moref, row, descriptions, VSphereBinarySensor),
            )

    # Network binary sensors
    for net_moref, net_row in iter_rows(data, categories, entity_filter, "network", "networks"):
        descriptions = _NET_BINARY_MAP.get(net_row.get("type", ""), ())
        if not descriptions:
            continue
        host_moref = net_row.get("host_moref", "")
        if host_moref:
            host_name = data.get("hosts", {}).get(host_moref, {}).get("name", host_moref)
            parent = ("hosts", host_moref, host_name)
        else:
            parent = ("root", entry.entry_id, entry.title)
        yield (
            ("networks", net_moref),
            partial(
                _build_child,
                coordinator,
                entry,
                *parent,
                net_moref,
                descriptions,
                net_row.get("name", net_moref),
            ),
        )

    # Alarm status binary sensors — one set per host/VM when events_alarms is enabled
    for data_key in ("hosts", "vms"):
        for moref, row in iter_rows(data, categories, entity_filter, Category.EVENTS_ALARMS, data_key):
            yield (
                (f"alarm_{data_key}", moref),
                partial(
                    _build_object,
                    coordinator,
                    entry,
                    data_key,
                    moref,
                    row,
                    ALARM_BINARY_SENSORS,
                    VSphereAlarmBinarySensor,
                ),
            )


def _build_object(
    coordinator: VSphereData,
    entry: VSphereConfigEntry,
    object_type: str,
    moref: str,
    row: dict[str, Any],
    descriptions: tuple[VSphereBinarySensorDescription, ...],
    cls: type[VSphereBinarySensor] | type[VSphereAlarmBinarySensor],
) -> list[Entity]:
    name: str = row.get("name", moref)
    return [
        cls(coordinator=coordinator, entry=entry, object_type=object_type, moref=moref, name=name, description=d)
        for d in descriptions
    ]


def _build_child(
    coordinator: VSphereData,
    entry: VSphereConfigEntry,
    parent_object_type: str,
    parent_moref: str,
    parent_name: str,
    data_moref: str,
    descriptions: tuple[VSphereBinarySensorDescription, ...],
    entity_name: str,
) -> list[Entity]:
    return [
        VSphereChildBinarySensor(
            coordinator=coordinator,
            entry=entry,
            parent_object_type=parent_object_type,
            parent_moref=parent_moref,
            parent_name=parent_name,
            data_category="networks",
            data_moref=data_moref,
            description=d,
            entity_name=entity_name,
        )
        for d in descriptions
    ]


class VSphereBinarySensor(VSphereEntity, BinarySensorEntity):
    """A binary sensor entity representing a vSphere boolean state."""

    entity_description: VSphereBinarySensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: VSphereConfigEntry,
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
        if self._object_type == "vms" and self.entity_description.disconnect_behavior and self._vm_is_disconnected():
            return False if self.entity_description.disconnect_behavior == "off" else None
        return self.entity_description.value_fn(obj_data)


class VSphereChildBinarySensor(VSphereChildEntity, BinarySensorEntity):
    """Binary sensor attached to a parent device but reading from a different data path."""

    _attr_has_entity_name = False
    entity_description: VSphereBinarySensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: VSphereConfigEntry,
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
        entry: VSphereConfigEntry,
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
