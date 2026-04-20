# vSphere Control — Home Assistant Integration

[![HACS Custom](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://hacs.xyz)
[![GitHub Release](https://img.shields.io/github/v/release/metril/ha-vsphere)](https://github.com/metril/ha-vsphere/releases)
[![License: MIT](https://img.shields.io/github/license/metril/ha-vsphere)](LICENSE)

Monitor and control VMware vSphere (ESXi and vCenter) infrastructure directly from Home Assistant. The integration uses a push-based model via the vSphere PropertyCollector API for real-time state updates, with optional polling for performance metrics.

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=metril&repository=ha-vsphere&category=integration)

---

## Features

### Monitoring (10 categories, independently toggleable)
- **Hosts** — power state, maintenance mode, CPU/memory usage, uptime, VM count, version, build, power policy
- **Virtual Machines** — power state, CPU/memory usage, uptime, snapshot count, guest IP/OS, tools status
- **Datastores** — capacity, free space, connected hosts, virtual machines, type
- **Licenses** — status, expiration days, product name
- **Clusters** — DRS/HA status, host/VM counts, CPU/memory totals (optional)
- **Networks** — vSwitch ports/MTU, physical NIC speed/link status, port group VLANs (optional, on host device)
- **Resource Pools** — CPU/memory reservations and limits, VM count (optional)
- **Storage Advanced** — per-VM disk capacity, thin provisioning, storage summaries (optional, on VM device)
- **Performance** — real-time CPU/memory/disk/network metrics via PerformanceManager (optional, polled)
- **Events & Alarms** — alarm state tracking with HA event firing (optional, pushed)

### Control (84 entity types across 5 platforms)
- **Sensors** — 63 sensor types across hosts, VMs, datastores, licenses, clusters, network, resource pools, storage, performance, alarms
- **Binary Sensors** — 11 types (power, maintenance, tools, DRS/HA, NIC link, alarms)
- **Switches** — VM power on/off (graceful shutdown via VMware Tools), host maintenance mode toggle
- **Buttons** — host reboot, VM reboot/reset, snapshot create/remove (all/first/last) — 7 types
- **Selects** — host power policy

### Security
- **vSphere privilege checking** — account privileges verified on every HA load; control entities automatically blocked if the account lacks required privileges
- **Granular permission system** — 9-step resolution chain with per-object, per-category, and global restrictions
- **Custom CA certificate** — optional CA file path for environments with internal PKI

### Services
Nine services with device targeting and permission enforcement (see [Services Reference](#services-reference)).

### HA Events
Three event types fired on the HA event bus (see [Events](#ha-events)).

---

## Installation

### Via HACS (recommended)

1. Open HACS in Home Assistant.
2. Click the three-dot menu in the top right and choose **Custom repositories**.
3. Add `https://github.com/metril/ha-vsphere` with category **Integration**.
4. Search for **vSphere Control** in HACS and click **Download**.
5. Restart Home Assistant.

### Manual

1. Copy the `custom_components/vsphere` directory into your HA `config/custom_components/` folder.
2. Restart Home Assistant.

---

## Configuration

1. Go to **Settings → Devices & Services → Add Integration**.
2. Search for **vSphere Control**.
3. Fill in the connection details:

| Field | Description | Default |
|-------|-------------|---------|
| Host | ESXi hostname or vCenter IP/FQDN | — |
| Port | HTTPS API port | `443` |
| Username | For vCenter: `user@domain.com`. For ESXi: local account name | — |
| Password | vSphere password | — |
| Verify SSL | Validate the server certificate | `false` |
| CA Certificate Path | Path to custom CA PEM file (e.g., `/ssl/vsphere-ca.pem`) | — |

**SSL modes:**
- **Verify SSL off** — no certificate verification (insecure, for testing)
- **Verify SSL on, no CA path** — verifies against system trust store
- **Verify SSL on, CA path set** — verifies against your custom CA certificate

4. Select which **monitoring categories** to enable (hosts, VMs, datastores, licenses on by default).
5. For each enabled category, choose **"Monitor all"** (dynamic — picks up new objects) or select specific objects from the live inventory.
6. If Performance Metrics is enabled, set the **polling interval** (60–3600 seconds, default 300).

After setup, configure additional options via **Configure**:

| Option | Description |
|--------|-------------|
| Categories | Enable/disable monitoring categories |
| Entity Selection | Choose which objects to monitor per category (with live inventory re-fetch) |
| Restrictions | Global operation restrictions (block destructive, snapshots, migration, host ops) |
| Performance Interval | Polling interval for performance metrics |

---

## Entity Reference

### Host Entities

| Entity | Type | Description |
|--------|------|-------------|
| Power State | Sensor | `poweredOn` / `poweredOff` |
| Maintenance Mode | Binary Sensor / Switch | Whether the host is in maintenance mode |
| CPU Usage | Sensor | Current CPU usage in GHz |
| Memory Usage | Sensor | Current memory usage in GB |
| Uptime | Sensor | Host uptime in hours |
| VM Count | Sensor | Number of VMs registered on this host |
| Power Policy | Select | Active power management policy |
| Shutdown | Button | Immediately shuts down the host |
| Reboot | Button | Reboots the host |

### Virtual Machine Entities

| Entity | Type | Description |
|--------|------|-------------|
| Power State | Sensor | `running` / `off` / `suspended` |
| Power | Switch | Turn the VM on/off |
| CPU Usage | Sensor | Current CPU utilization (%) |
| Memory Used | Sensor | Memory used by VM in MB |
| Uptime | Sensor | VM uptime in hours |
| Snapshot Count | Sensor | Number of snapshots |
| Guest IP | Sensor | Guest IP reported by VMware Tools |
| Tools Status | Sensor | VMware Tools installation status |
| Shutdown | Button | Graceful guest shutdown (falls back to hard power-off) |
| Reboot | Button | Graceful guest reboot (falls back to reset) |
| Reset | Button | Hard reset |
| Suspend | Button | Suspend to memory |

### Datastore Entities

| Entity | Type | Description |
|--------|------|-------------|
| Accessible | Binary Sensor | Whether the datastore is accessible |
| Capacity | Sensor | Total capacity in GB |
| Free Space | Sensor | Available space in GB |
| Used Space | Sensor | Used space in GB |

---

## Services Reference

All services target a specific vSphere device via `device_id`. Services are registered under the `vsphere` domain.

### `vsphere.vm_power`

Control the power state of a virtual machine.

```yaml
service: vsphere.vm_power
data:
  device_id: "abc123def456"   # HA device ID for the VM
  action: "power_on"          # power_on | power_off | shutdown | reboot | reset | suspend
```

`shutdown` and `reboot` use VMware Tools for graceful operation when available, with automatic fallback to hard power-off/reset.

### `vsphere.host_power`

Shutdown or reboot an ESXi host.

```yaml
service: vsphere.host_power
data:
  device_id: "abc123def456"
  action: "reboot"            # shutdown | reboot
  force: false                # Set true to proceed even if VMs are running
```

By default the service refuses if powered-on VMs are present. Use `force: true` to override.

### `vsphere.host_power_policy`

Set the active power management policy on a host.

```yaml
service: vsphere.host_power_policy
data:
  device_id: "abc123def456"
  policy: "static"            # Policy short name or key (use list_power_policies to enumerate)
```

### `vsphere.host_maintenance_mode`

Enable or disable maintenance mode on a host.

```yaml
service: vsphere.host_maintenance_mode
data:
  device_id: "abc123def456"
  enable: true
```

### `vsphere.create_snapshot`

Create a VM snapshot.

```yaml
service: vsphere.create_snapshot
data:
  device_id: "abc123def456"
  name: "pre-update"          # Optional; defaults to snapshot-<timestamp>
  description: "Before patch Tuesday"
  memory: false               # Include memory state in snapshot
  quiesce: false              # Quiesce guest file system (requires VMware Tools)
```

### `vsphere.remove_snapshot`

Remove one or all VM snapshots.

```yaml
service: vsphere.remove_snapshot
data:
  device_id: "abc123def456"
  which: "last"               # all | first | last
```

### `vsphere.list_hosts` (returns response)

Return a summary of all ESXi hosts visible from this connection.

```yaml
service: vsphere.list_hosts
response_variable: result
data:
  device_id: "abc123def456"
```

Response: `{ "hosts": [{ "moref": "host-10", "name": "esxi01.lab", "power_state": "poweredOn" }, ...] }`

### `vsphere.vm_migrate`

Migrate a VM to a different host (and optionally datastore).

```yaml
service: vsphere.vm_migrate
data:
  device_id: "abc123def456"
  host_moref: "host-20"        # Target host MoRef
  datastore_moref: "datastore-1"  # Optional target datastore MoRef
```

### `vsphere.list_power_policies` (returns response)

Return the available power policies for the target host device.

```yaml
service: vsphere.list_power_policies
response_variable: result
data:
  device_id: "abc123def456"
```

Response: `{ "policies": [{ "key": 1, "short_name": "static", "name": "High Performance" }, ...] }`

---

## Permission System

The integration includes a layered permission resolver that can block specific actions globally or per managed object. Restrictions are configured in the integration options under **Restrictions**.

### Resolution Chain (first match wins)

0. **vSphere privilege check** — if the account lacks the required privilege (e.g., `VirtualMachine.Interact.PowerOn`), the action is blocked regardless of user restrictions. Privileges are refreshed on every HA restart.
1. Per-object per-action: `restrictions.{category}.{moref}.{action}`
2. Per-object blanket: `restrictions.{category}.{moref}._all`
3. Per-category per-action: `restrictions.categories.{category}.{action}`
4. Per-category blanket: `restrictions.categories.{category}._all`
5. Global per-action: `restrictions.global.{action}`
6. Global shortcut group: `restrictions.global.{group_name}`
7. Global nuclear switch: `restrictions.global._all`
8. Default: **allowed**

### Shortcut Groups

| Group | Actions Covered |
|-------|----------------|
| `destructive` | `power_off`, `shutdown`, `reset`, `snapshot_remove` |
| `snapshots` | `snapshot_create`, `snapshot_remove` |
| `migrate` | `migrate` |
| `host_ops` | `shutdown`, `reboot`, `maintenance`, `power_policy` (hosts only) |

### Example — read-only with snapshot allowance

```yaml
global:
  _all: true          # Block everything by default
  snapshots: false    # Allow snapshot operations
```

### Example — block host operations on a specific host

```yaml
hosts:
  host-42:
    _all: true        # Block all host actions for this host
```

A value of `true` means **blocked**; `false` explicitly **allows** (useful to punch holes through broader restrictions).

---

## HA Events

When the **Events & Alarms** category is enabled, the integration fires events on the Home Assistant event bus.

### `vsphere_alarm_triggered`

Fired when a vSphere alarm changes state.

```yaml
event_type: vsphere_alarm_triggered
data:
  entry_id: "config_entry_id"
  entity_type: "host"       # host, vm
  entity_moref: "host-42"
  entity_name: "esxi01"
  alarm_key: "alarm-1.host-42"
  alarm_name: "Host memory usage"
  old_status: "green"       # null for first-seen (suppressed)
  new_status: "red"         # green, yellow, red
  time: "2026-04-11T13:24:48"
  acknowledged: false
```

### `vsphere_event`

Fired for general vSphere task/event log entries.

```yaml
event_type: vsphere_event
data:
  entry_id: "config_entry_id"
  event_class: "VmPoweredOnEvent"
  entity_type: "vm"
  entity_moref: "vm-101"
  entity_name: "web01"
  message: "web01 is poweredOn"
  time: "2026-04-11T13:25:01"
```

### `vsphere_inventory_change`

Fired when the inventory changes (VM or host added/removed).

```yaml
event_type: vsphere_inventory_change
data:
  entry_id: "config_entry_id"
  action: "added"           # added, removed
  entity_type: "vm"
  entity_moref: "vm-205"
  entity_name: "new-vm-01"  # only present for "added"
```

---

## Excluding from the Recorder

vSphere entities update frequently (push-based PropertyCollector plus optional performance polling), which can grow the Home Assistant recorder database faster than most users want. Home Assistant has no per-integration recorder filter, so exclusion must be configured manually.

Entity IDs are derived from your vSphere object names (e.g. `sensor.esxi01_cpu_usage`, `sensor.web01_memory_used`) — they do **not** share a common `vsphere_*` prefix. Inspect your actual IDs at **Settings → Devices & Services → vSphere Control → Entities**.

### Option A — Exclude every vSphere entity via `configuration.yaml`

Add globs that match your vSphere device names. Example for hosts named `esxi*` and the integration's child sensors:

```yaml
recorder:
  exclude:
    entity_globs:
      - sensor.esxi*
      - binary_sensor.esxi*
      - sensor.*_vm_count
      - sensor.*_snapshot_count
      # ...add globs that match your VM/datastore naming
```

### Option B — Keep history but drop noisy attributes only

If you want to keep state history but stop recording attribute payloads, no integration-side change is required today; HA records state changes regardless of attribute filters. Apply Option A's globs for the specific high-cardinality sensors instead (e.g. performance and storage-advanced sensors).

### Notes

- Restart Home Assistant after editing `configuration.yaml`.
- Globs are matched against entity IDs, not device names directly. Slugified vSphere names appear in the entity ID.
- Performance and Storage Advanced categories produce the most history volume — exclude those first if you only want to trim, not eliminate, recording.

---

## Troubleshooting

### Integration fails to load / ConfigEntryNotReady

- Verify the hostname/IP and port are reachable from your HA instance.
- Check that the vSphere account has at least read-only access.
- If using self-signed certificates, either disable **Verify SSL** or provide the CA certificate path.
- If a custom CA path is configured, verify the file exists at that path on the HA host.

### Config flow doesn't show hosts/VMs in entity selection

- Check HA logs for warnings about inventory enumeration failures.
- If using a custom CA, ensure the CA path was entered in the connection step — it's needed for both the connection test and inventory enumeration.

### Actions blocked despite no restrictions configured

- The integration checks vSphere account privileges on every HA load. If your account lacks a privilege (e.g., `VirtualMachine.Interact.PowerOn`), the action is blocked at the resolver level.
- Check logs for "vSphere privileges" debug entries to see which privileges were detected.
- Grant the required privileges to the vSphere account via vCenter roles.

### Entities are unavailable after initial setup

- The integration uses a push model. Entities become available once the EventListener receives the first property update (usually within a few seconds).
- Check HA logs at DEBUG level: `logger: custom_components.vsphere: debug`.

### Services return "blocked by permission restrictions"

- Review your **Restrictions** options for the config entry.
- Use `logger` to enable debug logging and check the resolver explain output.

### VMware Tools-dependent operations fail

- `shutdown` and `reboot` on VMs fall back to hard power-off/reset automatically when Tools are not running.
- `quiesce: true` in `create_snapshot` requires VMware Tools to be running.

### Performance metrics are missing

- Enable the **Performance** category in integration options.
- The default polling interval is 5 minutes; lower it if you need more frequent updates (minimum 60 s).

---

## Requirements

- Home Assistant 2024.1 or newer
- `pyvmomi >= 8.0.3` (installed automatically)
- VMware ESXi 6.7+ or vCenter Server 6.7+
