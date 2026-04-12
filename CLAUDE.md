# ha-vsphere

Home Assistant custom integration for VMware vSphere monitoring and control.
**Version:** 1.3.4 | **Domain:** `vsphere` | **IoT Class:** `local_push`

## Project Structure

```
custom_components/vsphere/
├── __init__.py          (196)  Entry setup/teardown
├── const.py             (143)  Constants, enums
├── exceptions.py         (19)  VSphereError hierarchy
├── permissions.py       (180)  7-step resolution chain (user restrictions only)
├── vsphere_client.py  (1,510)  ALL pyVmomi interaction (only file importing pyVmomi)
├── coordinator.py       (153)  VSphereData (push) + VSpherePerfCoordinator (poll)
├── event_listener.py    (572)  PropertyCollector push thread + translation maps
├── entity.py            (182)  Base entity + child entity + device hierarchy
├── config_flow.py       (609)  4-step config + 3-step options + reauth + reconfigure
├── sensor.py            (885)  63 sensor descriptions across 15 groups
├── binary_sensor.py     (319)  11 binary sensor descriptions
├── switch.py            (189)  VM power + host maintenance mode
├── button.py            (373)  7 button classes with press-time permission checks
├── select.py            (114)  Host power policy selector
├── services.py          (410)  8 service handlers with device resolution
├── diagnostics.py        (29)  Credential redaction
├── services.yaml               Service definitions for HA
├── manifest.json               Integration metadata
├── strings.json                Translation source
└── translations/en.json        English translations
```

- `docs/superpowers/` — Design specs and plans (local only, gitignored)
- `tests/` — 40 unit tests (permission resolver)

## Development

```bash
uv sync --all-extras
uv run ruff check custom_components/vsphere/
uv run ruff format custom_components/vsphere/
uv run pytest tests/ -v
```

## Key Conventions

- **Domain:** `vsphere`
- **Only `vsphere_client.py` imports pyVmomi** — all other files are pyVmomi-free
- **All vSphere API calls** run in executor via `hass.async_add_executor_job()`
- **Entity unique IDs:** `{entry_id}_{moref}_{entity_key}`
- **MoRef IDs** as stable identifiers (not names — names can change)
- **PermissionResolver** is the single enforcement point for all operation restrictions
- **Push-primary** via PropertyCollector `WaitForUpdatesEx`; polling only for PerformanceManager
- **Property translation:** raw PC paths → flat entity keys via `_HOST_PROP_MAP`, `_VM_PROP_MAP`, `_DATASTORE_PROP_MAP`
- **Thread safety:** alarm cache on background thread, moref snapshots on event loop, no live RPC from background thread
- **ContainerView cleanup:** tracked and destroyed on stop/reconnect
- **Git identity:** author `metril <1517921+metril@users.noreply.github.com>`
- **Never commit `docs/`** — specs and plans are local working files
- **Never mention AI/Claude** in commits or code

## Architecture

```
PropertyCollector (push) ──▶ EventListener ──▶ _translate_properties() ──▶ VSphereData ──▶ Entities
PerformanceManager (poll) ──▶ PerfCoordinator ──▶ VSphereData.perf ──▶ PerfSensors
```

## 10 Monitoring Categories

| Category | Default | Entities |
|----------|---------|----------|
| Hosts | ON | 8 sensors, 2 binary, 1 switch, 2 buttons, 1 select |
| VMs | ON | 12 sensors, 2 binary, 1 switch, 8 buttons |
| Datastores | ON | 4 sensors |
| Licenses | ON | 4 sensors |
| Clusters | OFF | 3 sensors, 2 binary |
| Network | OFF | 10 sensors, 2 binary (on host device) |
| Resource Pools | OFF | 4 sensors |
| Storage Advanced | OFF | 9 sensors (on VM device) |
| Performance | OFF | 19 sensors (polled) |
| Events & Alarms | OFF | 4 sensors + HA events |

## Device Hierarchy

```
Root (vCenter/ESXi)
├── Host
│   ├── VM (+ storage sensors)
│   ├── Datastore (single-host only)
│   (+ network sensors: vSwitch, pNIC, portgroup)
├── Datastore (multi-host)
├── Cluster
└── Resource Pool
```

## Permission System (7-step chain)

vSphere account privileges are NOT pre-checked — vCenter/ESXi enforces them at
operation time and returns `NoPermission` faults with the exact missing privilege.
The resolver handles only user-configured restrictions:

1. Per-object per-action
2. Per-object blanket (_all)
3. Per-category per-action
4. Per-category blanket
5. Global per-action
6. Global shortcut groups (destructive, snapshots, migrate, host_ops)
7. Global nuclear switch (_all)
8. Default: allowed

## Dependencies

- `pyvmomi>=8.0.3` — VMware vSphere API SDK
- Home Assistant Core 2024.6.0+
