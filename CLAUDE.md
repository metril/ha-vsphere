# ha-vsphere

Home Assistant custom integration for VMware vSphere monitoring and control.
**Version:** 3.8.1 | **Domain:** `vsphere` | **IoT Class:** `local_push`

## Project Structure

```
custom_components/vsphere/
├── __init__.py          (301)  Entry setup/teardown, coordinator wiring, entity cleanup
├── const.py             (152)  Constants, enums, derive_vm_state()
├── exceptions.py         (19)  VSphereError hierarchy
├── permissions.py       (186)  7-step resolution chain (user restrictions only)
├── vsphere_client.py  (1,743)  ALL pyVmomi interaction (only file importing pyVmomi)
├── coordinator.py       (272)  VSphereData (push) + Perf + Inventory coordinators
├── event_listener.py    (859)  PropertyCollector push thread + translation maps + shadow cache
├── entity.py            (209)  Base entity + child entity + device hierarchy + is_vm_disconnected()
├── config_flow.py       (997)  Menu-driven options + config + reauth + reconfigure + restriction mixin
├── sensor.py          (1,019)  Sensor descriptions with conditional skip (perf, storage)
├── binary_sensor.py     (352)  Binary sensor descriptions
├── switch.py            (105)  Host maintenance mode switch
├── button.py            (325)  Button classes with snapshot select integration
├── select.py            (291)  Host power policy/operation + VM power operation + snapshot selector
├── services.py          (492)  Service handlers with shared helpers
├── diagnostics.py        (29)  Credential redaction
├── services.yaml               Service definitions for HA
├── manifest.json               Integration metadata
├── strings.json                Translation source
└── translations/en.json        English translations
```

- `docs/superpowers/` — Design specs and plans (local only, gitignored)
- `.github/workflows/` — `validate` (hassfest + HACS), `ci` (ruff + pytest, mypy advisory), `release`
- `tests/` — 106 unit tests. Pure-unit only: `conftest.py` mocks every `homeassistant.*`
  module, so classes subclassing `CoordinatorEntity`/`DataUpdateCoordinator` resolve to
  non-functional mocks and **cannot be tested**. Keep branching logic in module-level
  functions (`derive_vm_state`, `is_vm_disconnected`, `_duration_to_seconds`) so it stays testable.

## Development

```bash
uv sync --extra dev          # lint + test (no Home Assistant)
uv sync --all-extras         # adds the `typing` extra (Home Assistant) for mypy
uv run ruff check custom_components/vsphere/ tests/
uv run ruff format custom_components/vsphere/ tests/
uv run pytest tests/ -v
uv run mypy custom_components/vsphere/   # advisory: 22 pre-existing errors
```

## Key Conventions

- **Domain:** `vsphere`
- **Only `vsphere_client.py` imports pyVmomi** — all other files are pyVmomi-free
- **All vSphere API calls** run in executor via `hass.async_add_executor_job()`
- **Entity unique IDs:** `{entry_id}_{moref}_{entity_key}`
- **MoRef IDs** as stable identifiers (not names — names can change)
- **PermissionResolver** is the single enforcement point for all operation restrictions
- **Push-primary** via PropertyCollector `WaitForUpdatesEx`; polling only for PerformanceManager and
  the three categories PC cannot watch (licenses, network, storage_advanced)
- **Property translation:** raw PC paths → flat entity keys via `_HOST_PROP_MAP`, `_VM_PROP_MAP`,
  `_DATASTORE_PROP_MAP`, `_CLUSTER_PROP_MAP`, `_RESOURCE_POOL_PROP_MAP`. Every watched category needs
  a map — an unmapped one is dropped with a warning so raw pyVmomi objects never reach coordinator data.
- **Pushes are partial merges** — `async_update_from_push` merges deltas, so any derived value combining
  two properties must fall back to `stored` for whichever one is absent from the delta
- **Thread safety:** the listener owns `_local_state_cache` / `_alarm_name_cache` / `_alarm_cache` /
  `_vm_power_cache` and reads only those; never read coordinator `_data` from the background thread,
  and never make a live RPC from it (that rules out `alarm.info.name`, `host.name`, etc.)
- **A coordinator only reschedules while it has listeners** — every extra `DataUpdateCoordinator` must
  get an `async_add_listener` or its timer never re-arms after the first refresh
- **VM reachability:** `runtime.connectionState` is authoritative; `powerState` goes stale when the host
  drops. `is_vm_disconnected()` also cascades to the parent host and is evaluated at read time, because
  a host row changing does not recompute the VM rows pointing at it.
- **ContainerView cleanup:** tracked and destroyed on stop/reconnect
- **Git identity:** author `metril <1517921+metril@users.noreply.github.com>`
- **Never commit `docs/`** — specs and plans are local working files
- **Never mention AI/Claude** in commits or code

## Architecture

```
PropertyCollector (push) ──▶ EventListener ──▶ _translate_properties() ──▶ VSphereData ──▶ Entities
PerformanceManager (poll) ──▶ PerfCoordinator ──────▶ VSphereData.perf ──▶ PerfSensors
licenses/network/storage (poll) ──▶ InventoryCoordinator ──▶ VSphereData ──▶ Entities
```

## 10 Monitoring Categories

| Category | Default | Entities |
|----------|---------|----------|
| Hosts | ON | 8 sensors, 2 binary, 1 switch (maintenance), 1 button, 2 selects (power op + policy) |
| VMs | ON | 12 sensors, 2 binary, 3 buttons, 2 selects (power operation + snapshot) |
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
