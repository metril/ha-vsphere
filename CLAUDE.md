# ha-vsphere

Home Assistant custom integration for VMware vSphere monitoring and control.

## Project Structure

- `custom_components/vsphere/` — Integration source code
- `docs/superpowers/specs/` — Design specifications
- `docs/superpowers/plans/` — Implementation plans
- `tests/` — Unit tests

## Development

```bash
uv sync --all-extras
uv run ruff check custom_components/vsphere/
uv run mypy custom_components/vsphere/
uv run pytest tests/
```

## Key Conventions

- Domain: `vsphere`
- All pyVmomi calls go through `vsphere_client.py` only
- All vSphere API calls run in executor via `hass.async_add_executor_job()`
- Entity unique IDs: `{entry_id}_{moref}_{entity_key}`
- MoRef IDs used as stable identifiers (not names)
- `PermissionResolver` is the single enforcement point for operation restrictions
- Push-primary via PropertyCollector; polling only for PerformanceManager counters

## Dependencies

- `pyvmomi>=8.0.3` — VMware vSphere API SDK
- Home Assistant Core 2024.1.0+
