"""Constants for the vSphere Control integration."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

DOMAIN: Final = "vsphere"

# Platforms
PLATFORMS: Final = ["sensor", "binary_sensor", "switch", "button", "select"]

# Config entry data keys
CONF_HOST: Final = "host"
CONF_PORT: Final = "port"
CONF_USERNAME: Final = "username"
CONF_PASSWORD: Final = "password"
CONF_VERIFY_SSL: Final = "verify_ssl"

# Config entry options keys
CONF_CATEGORIES: Final = "categories"
CONF_ENTITY_FILTER: Final = "entity_filter"
CONF_RESTRICTIONS: Final = "restrictions"
CONF_PERF_INTERVAL: Final = "perf_interval"

# Defaults
DEFAULT_PORT: Final = 443
DEFAULT_VERIFY_SSL: Final = False
DEFAULT_PERF_INTERVAL: Final = 300
MIN_PERF_INTERVAL: Final = 60
MAX_PERF_INTERVAL: Final = 3600

# Entity filter modes
FILTER_MODE_ALL: Final = "all"
FILTER_MODE_SELECT: Final = "select"


class Category(StrEnum):
    """Monitoring categories."""

    HOSTS = "hosts"
    VMS = "vms"
    DATASTORES = "datastores"
    LICENSES = "licenses"
    CLUSTERS = "clusters"
    NETWORK = "network"
    RESOURCE_POOLS = "resource_pools"
    STORAGE_ADVANCED = "storage_advanced"
    PERFORMANCE = "performance"
    EVENTS_ALARMS = "events_alarms"


# Categories enabled by default
DEFAULT_CATEGORIES: Final[dict[str, bool]] = {
    Category.HOSTS: True,
    Category.VMS: True,
    Category.DATASTORES: True,
    Category.LICENSES: True,
    Category.CLUSTERS: False,
    Category.NETWORK: False,
    Category.RESOURCE_POOLS: False,
    Category.STORAGE_ADVANCED: False,
    Category.PERFORMANCE: False,
    Category.EVENTS_ALARMS: False,
}


class VmAction(StrEnum):
    """VM power/control actions."""

    POWER_ON = "power_on"
    POWER_OFF = "power_off"
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"
    RESET = "reset"
    SUSPEND = "suspend"
    SNAPSHOT_CREATE = "snapshot_create"
    SNAPSHOT_REMOVE = "snapshot_remove"
    MIGRATE = "migrate"


class HostAction(StrEnum):
    """Host control actions."""

    SHUTDOWN = "shutdown"
    REBOOT = "reboot"
    MAINTENANCE = "maintenance"
    POWER_POLICY = "power_policy"


# Global restriction shortcut groups
RESTRICTION_GROUP_DESTRUCTIVE: Final = "destructive"
RESTRICTION_GROUP_SNAPSHOTS: Final = "snapshots"
RESTRICTION_GROUP_MIGRATE: Final = "migrate"
RESTRICTION_GROUP_HOST_OPS: Final = "host_ops"

DESTRUCTIVE_ACTIONS: Final[set[str]] = {
    VmAction.POWER_OFF,
    VmAction.SHUTDOWN,
    VmAction.RESET,
    VmAction.SNAPSHOT_REMOVE,
}

SNAPSHOT_ACTIONS: Final[set[str]] = {
    VmAction.SNAPSHOT_CREATE,
    VmAction.SNAPSHOT_REMOVE,
}

HOST_OPS_ACTIONS: Final[set[str]] = {
    HostAction.SHUTDOWN,
    HostAction.REBOOT,
    HostAction.MAINTENANCE,
    HostAction.POWER_POLICY,
}

# Snapshot removal targets
SNAP_ALL: Final = "all"
SNAP_FIRST: Final = "first"
SNAP_LAST: Final = "last"

# Connection types
CONN_TYPE_VCENTER: Final = "vcenter"
CONN_TYPE_ESXI: Final = "esxi"

# VM power states (from vSphere)
VM_POWER_ON: Final = "poweredOn"
VM_POWER_OFF: Final = "poweredOff"
VM_SUSPENDED: Final = "suspended"

# Host power states
HOST_POWER_ON: Final = "poweredOn"
HOST_POWER_OFF: Final = "poweredOff"

# VM state display names
VM_STATE_RUNNING: Final = "running"
VM_STATE_OFF: Final = "off"
VM_STATE_SUSPENDED: Final = "suspended"

# License filtering
INVALID_LICENSE_KEY: Final = "00000-00000-00000-00000-00000"
