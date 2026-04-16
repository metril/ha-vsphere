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
    UnitOfDataRate,
    UnitOfInformation,
    UnitOfTime,
)

from .const import CONF_CATEGORIES, DEFAULT_CATEGORIES, DOMAIN, Category
from .entity import VSphereChildEntity, VSphereEntity

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
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("uptime_hours"),
    ),
    VSphereSensorDescription(
        key="vm_count",
        translation_key="host_vm_count",
        name="Running VMs",
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
        state_class=SensorStateClass.MEASUREMENT,
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
        key="status",
        translation_key="status",
        name="Status",
        value_fn=lambda d: d.get("state"),
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
        value_fn=lambda d: d.get("status", "unknown"),
    ),
    VSphereSensorDescription(
        key="expiration_days",
        translation_key="expiration_days",
        name="Expiration Days",
        native_unit_of_measurement=UnitOfTime.DAYS,
        state_class=SensorStateClass.MEASUREMENT,
        # Return None for perpetual licenses ("never") — HA requires numeric values for measurement sensors
        value_fn=lambda d: d.get("expiration_days") if isinstance(d.get("expiration_days"), int) else None,
    ),
    VSphereSensorDescription(
        key="product",
        translation_key="product",
        name="Product",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("product"),
    ),
)

# ---------------------------------------------------------------------------
# Cluster sensors
# ---------------------------------------------------------------------------

CLUSTER_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="drs_automation_level",
        translation_key="drs_automation_level",
        name="DRS Automation Level",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("drs_automation_level"),
    ),
    VSphereSensorDescription(
        key="total_hosts",
        translation_key="total_hosts",
        name="Total Hosts",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("total_hosts"),
    ),
    VSphereSensorDescription(
        key="effective_hosts",
        translation_key="effective_hosts",
        name="Effective Hosts",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("effective_hosts"),
    ),
    VSphereSensorDescription(
        key="total_cpu_mhz",
        translation_key="total_cpu_mhz",
        name="Total CPU",
        native_unit_of_measurement="MHz",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("total_cpu_mhz"),
    ),
    VSphereSensorDescription(
        key="total_memory_mb",
        translation_key="total_memory_mb",
        name="Total Memory",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("total_memory_mb"),
    ),
    VSphereSensorDescription(
        key="vm_count",
        translation_key="cluster_vm_count",
        name="Cluster VMs",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("vm_count"),
    ),
)

# ---------------------------------------------------------------------------
# Network sensors (split by network object type)
# ---------------------------------------------------------------------------

VSWITCH_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="num_ports",
        translation_key="num_ports",
        name="Port Count",
        icon="mdi:ethernet",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("num_ports"),
    ),
    VSphereSensorDescription(
        key="mtu",
        translation_key="mtu",
        name="MTU",
        icon="mdi:resize",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("mtu"),
    ),
)

PNIC_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="speed_mbps",
        translation_key="speed_mbps",
        name="Link Speed",
        native_unit_of_measurement="Mbit/s",
        icon="mdi:speedometer",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("speed_mbps"),
    ),
)

PORTGROUP_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="vlan_id",
        translation_key="vlan_id",
        name="VLAN ID",
        icon="mdi:lan",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("vlan_id"),
    ),
)

DVSWITCH_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="num_ports",
        translation_key="num_ports",
        name="Port Count",
        icon="mdi:ethernet",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("num_ports"),
    ),
    VSphereSensorDescription(
        key="max_ports",
        translation_key="max_ports",
        name="Max Ports",
        icon="mdi:ethernet",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("max_ports"),
    ),
    VSphereSensorDescription(
        key="mtu",
        translation_key="mtu",
        name="MTU",
        icon="mdi:resize",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("mtu"),
    ),
    VSphereSensorDescription(
        key="num_hosts",
        translation_key="dvs_num_hosts",
        name="Connected Hosts",
        icon="mdi:server-network",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("num_hosts"),
    ),
    VSphereSensorDescription(
        key="version",
        translation_key="dvs_version",
        name="Version",
        icon="mdi:information-outline",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("version"),
    ),
    VSphereSensorDescription(
        key="nioc_enabled",
        translation_key="nioc_enabled",
        name="NIOC Enabled",
        icon="mdi:traffic-light",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("nioc_enabled"),
    ),
)

DVPORTGROUP_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="vlan_id",
        translation_key="vlan_id",
        name="VLAN ID",
        icon="mdi:lan",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("vlan_id"),
    ),
    VSphereSensorDescription(
        key="port_binding",
        translation_key="port_binding",
        name="Port Binding",
        icon="mdi:link-variant",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("port_binding"),
    ),
    VSphereSensorDescription(
        key="num_ports",
        translation_key="num_ports",
        name="Port Count",
        icon="mdi:ethernet",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("num_ports"),
    ),
)

# ---------------------------------------------------------------------------
# Resource pool sensors
# ---------------------------------------------------------------------------

RESOURCE_POOL_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="cpu_reservation_mhz",
        translation_key="cpu_reservation_mhz",
        name="CPU Reservation",
        native_unit_of_measurement="MHz",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_reservation_mhz"),
    ),
    VSphereSensorDescription(
        key="cpu_limit_mhz",
        translation_key="cpu_limit_mhz",
        name="CPU Limit",
        native_unit_of_measurement="MHz",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_limit_mhz"),
    ),
    VSphereSensorDescription(
        key="memory_reservation_mb",
        translation_key="memory_reservation_mb",
        name="Memory Reservation",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("memory_reservation_mb"),
    ),
    VSphereSensorDescription(
        key="memory_limit_mb",
        translation_key="memory_limit_mb",
        name="Memory Limit",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("memory_limit_mb"),
    ),
    VSphereSensorDescription(
        key="vm_count",
        translation_key="pool_vm_count",
        name="Pool VMs",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("vm_count"),
    ),
)

# ---------------------------------------------------------------------------
# Host / VM performance sensors
# ---------------------------------------------------------------------------

HOST_PERF_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="perf_cpu_usage_pct",
        translation_key="perf_cpu_usage_pct",
        name="CPU Usage (Realtime)",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("cpu_usage_pct"),
    ),
    VSphereSensorDescription(
        key="perf_mem_active_mb",
        translation_key="perf_mem_active_mb",
        name="Memory Active (Realtime)",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.MEGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("mem_active_mb"),
    ),
    VSphereSensorDescription(
        key="perf_net_received_mbps",
        translation_key="perf_net_received_mbps",
        name="Network Received (Realtime)",
        device_class=SensorDeviceClass.DATA_RATE,
        native_unit_of_measurement=UnitOfDataRate.MEGABYTES_PER_SECOND,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("net_received_mbps"),
    ),
    VSphereSensorDescription(
        key="perf_net_transmitted_mbps",
        translation_key="perf_net_transmitted_mbps",
        name="Network Transmitted (Realtime)",
        device_class=SensorDeviceClass.DATA_RATE,
        native_unit_of_measurement=UnitOfDataRate.MEGABYTES_PER_SECOND,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("net_transmitted_mbps"),
    ),
    VSphereSensorDescription(
        key="perf_disk_read_mbps",
        translation_key="perf_disk_read_mbps",
        name="Disk Read (Realtime)",
        device_class=SensorDeviceClass.DATA_RATE,
        native_unit_of_measurement=UnitOfDataRate.MEGABYTES_PER_SECOND,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("disk_read_mbps"),
    ),
    VSphereSensorDescription(
        key="perf_disk_write_mbps",
        translation_key="perf_disk_write_mbps",
        name="Disk Write (Realtime)",
        device_class=SensorDeviceClass.DATA_RATE,
        native_unit_of_measurement=UnitOfDataRate.MEGABYTES_PER_SECOND,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("disk_write_mbps"),
    ),
)

VM_PERF_SENSORS: tuple[VSphereSensorDescription, ...] = HOST_PERF_SENSORS

# ---------------------------------------------------------------------------
# Datastore performance sensors
# ---------------------------------------------------------------------------

DATASTORE_PERF_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="perf_read_latency_ms",
        translation_key="perf_read_latency_ms",
        name="Read Latency (Realtime)",
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("read_latency_ms"),
    ),
    VSphereSensorDescription(
        key="perf_write_latency_ms",
        translation_key="perf_write_latency_ms",
        name="Write Latency (Realtime)",
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("write_latency_ms"),
    ),
    VSphereSensorDescription(
        key="perf_read_iops",
        translation_key="perf_read_iops",
        name="Read IOPS (Realtime)",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("read_iops"),
    ),
    VSphereSensorDescription(
        key="perf_write_iops",
        translation_key="perf_write_iops",
        name="Write IOPS (Realtime)",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("write_iops"),
    ),
)

# ---------------------------------------------------------------------------
# Storage advanced sensors
# ---------------------------------------------------------------------------

VM_DISK_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="capacity_gb",
        translation_key="capacity_gb",
        name="Capacity",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("capacity_gb"),
    ),
    VSphereSensorDescription(
        key="thin_provisioned",
        translation_key="thin_provisioned",
        name="Thin Provisioned",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: (
            "Yes" if d.get("thin_provisioned") is True else ("No" if d.get("thin_provisioned") is False else None)
        ),
    ),
    VSphereSensorDescription(
        key="datastore",
        translation_key="datastore",
        name="Datastore",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d.get("datastore"),
    ),
)

VM_STORAGE_SUMMARY_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="committed_gb",
        translation_key="committed_gb",
        name="Committed",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("committed_gb"),
    ),
    VSphereSensorDescription(
        key="uncommitted_gb",
        translation_key="uncommitted_gb",
        name="Uncommitted",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("uncommitted_gb"),
    ),
    VSphereSensorDescription(
        key="unshared_gb",
        translation_key="unshared_gb",
        name="Unshared",
        device_class=SensorDeviceClass.DATA_SIZE,
        native_unit_of_measurement=UnitOfInformation.GIGABYTES,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d.get("unshared_gb"),
    ),
)

# ---------------------------------------------------------------------------
# Alarm sensors (per-entity, keyed by moref in coordinator data["alarms"])
# ---------------------------------------------------------------------------

ALARM_SENSORS: tuple[VSphereSensorDescription, ...] = (
    VSphereSensorDescription(
        key="active_alarm_count",
        translation_key="active_alarm_count",
        name="Active Alarms",
        icon="mdi:alarm-light",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: len(data) if isinstance(data, list) else 0,
    ),
)

# ---------------------------------------------------------------------------
# Sensor map: category → (descriptions, coordinator data key)
# ---------------------------------------------------------------------------

SENSOR_MAP: dict[str, tuple[tuple[VSphereSensorDescription, ...], str]] = {
    "hosts": (HOST_SENSORS, "hosts"),
    "vms": (VM_SENSORS, "vms"),
    "datastores": (DATASTORE_SENSORS, "datastores"),
    "clusters": (CLUSTER_SENSORS, "clusters"),
    "resource_pools": (RESOURCE_POOL_SENSORS, "resource_pools"),
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

    entities: list[VSphereSensor | VSpherePerfSensor | VSphereAlarmSensor] = []

    # Skip static sensors that are superseded by more detailed alternatives.
    # - Performance enabled: realtime perf sensors replace static CPU/memory usage
    # - Storage Advanced enabled: Committed/Uncommitted/Unshared replace Used Space
    perf_enabled = bool(categories.get("performance"))
    storage_advanced = bool(categories.get("storage_advanced"))
    _skip: dict[str, set[str]] = {}
    if perf_enabled:
        _skip.setdefault("hosts", set()).update({"cpu_usage_ghz", "mem_usage_gb"})
        _skip.setdefault("vms", set()).update({"cpu_use_pct", "memory_active_mb"})
    if storage_advanced:
        _skip.setdefault("vms", set()).add("used_space_gb")

    for category, (descriptions, data_key) in SENSOR_MAP.items():
        if not categories.get(category):
            continue
        skip_keys = _skip.get(category, set())
        for moref, obj_data in coordinator.data.get(data_key, {}).items():
            name: str = obj_data.get("name", moref)
            for description in descriptions:
                if description.key in skip_keys:
                    continue
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

    # License sensors: attach to root device, read from "licenses" data
    if categories.get("licenses"):
        for lic_key, lic_data in coordinator.data.get("licenses", {}).items():
            name = lic_data.get("name", lic_key)
            for description in LICENSE_SENSORS:
                entities.append(
                    VSphereChildSensor(
                        coordinator=coordinator,
                        entry=entry,
                        parent_object_type="root",
                        parent_moref=entry.entry_id,
                        parent_name=entry.title,
                        data_category="licenses",
                        data_moref=lic_key,
                        description=description,
                        entity_name=name,
                    )
                )

    # Network sensors: attach to parent Host device, read from "networks" data
    # dvSwitch/dvPortgroup entities attach to root vCenter device (datacenter-level)
    if categories.get("network"):
        _network_type_map: dict[str, tuple[VSphereSensorDescription, ...]] = {
            "vswitch": VSWITCH_SENSORS,
            "pnic": PNIC_SENSORS,
            "portgroup": PORTGROUP_SENSORS,
            "dvswitch": DVSWITCH_SENSORS,
            "dvportgroup": DVPORTGROUP_SENSORS,
        }
        hosts_data = coordinator.data.get("hosts", {})
        for net_moref, obj_data in coordinator.data.get("networks", {}).items():
            net_type = obj_data.get("type", "")
            type_descriptions = _network_type_map.get(net_type, ())
            name = obj_data.get("name", net_moref)
            host_moref = obj_data.get("host_moref", "")

            if host_moref:
                # Standard vSwitch/pNIC/portgroup — child of host device
                host_name = hosts_data.get(host_moref, {}).get("name", host_moref)
                for description in type_descriptions:
                    entities.append(
                        VSphereChildSensor(
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
            else:
                # dvSwitch/dvPortgroup — child of root vCenter device
                for description in type_descriptions:
                    entities.append(
                        VSphereChildSensor(
                            coordinator=coordinator,
                            entry=entry,
                            parent_object_type="root",
                            parent_moref=entry.entry_id,
                            parent_name=entry.title,
                            data_category="networks",
                            data_moref=net_moref,
                            description=description,
                            entity_name=name,
                        )
                    )

    # Performance sensors — only created when performance category is enabled
    if categories.get("performance"):
        for moref, obj_data in coordinator.data.get("hosts", {}).items():
            name = obj_data.get("name", moref)
            for desc in HOST_PERF_SENSORS:
                entities.append(VSpherePerfSensor(coordinator, entry, "hosts", moref, name, desc))
        for moref, obj_data in coordinator.data.get("vms", {}).items():
            name = obj_data.get("name", moref)
            for desc in VM_PERF_SENSORS:
                entities.append(VSpherePerfSensor(coordinator, entry, "vms", moref, name, desc))
        for moref, obj_data in coordinator.data.get("datastores", {}).items():
            name = obj_data.get("name", moref)
            for desc in DATASTORE_PERF_SENSORS:
                entities.append(VSpherePerfSensor(coordinator, entry, "datastores", moref, name, desc))

    # Storage advanced sensors — attach to parent VM device, read from "storage_advanced" data
    if categories.get(Category.STORAGE_ADVANCED):
        vms_data = coordinator.data.get("vms", {})
        for storage_moref, obj_data in coordinator.data.get("storage_advanced", {}).items():
            name = obj_data.get("name", storage_moref)
            vm_moref = obj_data.get("vm_moref", "")
            vm_name = vms_data.get(vm_moref, {}).get("name", vm_moref)
            descriptions = VM_STORAGE_SUMMARY_SENSORS if "_storage_summary" in storage_moref else VM_DISK_SENSORS
            for description in descriptions:
                entities.append(
                    VSphereChildSensor(
                        coordinator=coordinator,
                        entry=entry,
                        parent_object_type="vms",
                        parent_moref=vm_moref,
                        parent_name=vm_name,
                        data_category="storage_advanced",
                        data_moref=storage_moref,
                        description=description,
                        entity_name=name,
                    )
                )

    # Alarm count sensors — created for each host/VM when events_alarms is enabled
    if categories.get(Category.EVENTS_ALARMS):
        for moref, obj_data in coordinator.data.get("hosts", {}).items():
            name = obj_data.get("name", moref)
            for desc in ALARM_SENSORS:
                entities.append(
                    VSphereAlarmSensor(
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
            for desc in ALARM_SENSORS:
                entities.append(
                    VSphereAlarmSensor(
                        coordinator=coordinator,
                        entry=entry,
                        object_type="vms",
                        moref=moref,
                        name=name,
                        description=desc,
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


class VSphereChildSensor(VSphereChildEntity, SensorEntity):
    """Sensor attached to a parent device but reading data from a different coordinator path.

    Used for network sensors (on host device) and storage sensors (on VM device).
    """

    _attr_has_entity_name = False
    entity_description: VSphereSensorDescription

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        parent_object_type: str,
        parent_moref: str,
        parent_name: str,
        data_category: str,
        data_moref: str,
        description: VSphereSensorDescription,
        entity_name: str,
    ) -> None:
        """Initialize the child sensor."""
        super().__init__(coordinator, entry, parent_object_type, parent_moref, parent_name, data_category, data_moref)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{data_moref}_{description.key}"
        self._attr_name = f"{entity_name} {description.name}" if description.name else entity_name

    @property
    def native_value(self) -> Any:
        """Return the sensor value."""
        obj_data = self._get_data()
        if obj_data is None:
            return None
        return self.entity_description.value_fn(obj_data)


class VSpherePerfSensor(VSphereEntity, SensorEntity):
    """Sensor that reads from PerformanceManager polled data."""

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
        """Initialize the performance sensor."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{description.key}"

    @property
    def native_value(self) -> Any:
        """Return the sensor value from performance data."""
        if not self.coordinator.data:
            return None
        perf_data = self.coordinator.data.get("perf", {}).get(self._moref, {})
        return self.entity_description.value_fn(perf_data)


class VSphereAlarmSensor(VSphereEntity, SensorEntity):
    """Sensor for alarm counts per host or VM."""

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
        """Initialize the alarm sensor."""
        super().__init__(coordinator, entry, object_type, moref, name)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{moref}_{description.key}"

    @property
    def native_value(self) -> Any:
        """Return the alarm count for this entity."""
        if not self.coordinator.data:
            return None
        alarms = self.coordinator.data.get("alarms", {}).get(self._moref, [])
        return self.entity_description.value_fn(alarms)
