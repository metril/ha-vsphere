"""Test configuration."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add repo root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock homeassistant modules so we can import custom_components without HA installed
ha_mock = MagicMock()
for mod in [
    "homeassistant",
    "homeassistant.config_entries",
    "homeassistant.const",
    "homeassistant.core",
    "homeassistant.data_entry_flow",
    "homeassistant.exceptions",
    "homeassistant.helpers",
    "homeassistant.helpers.device_registry",
    "homeassistant.helpers.update_coordinator",
    "homeassistant.components.diagnostics",
    "homeassistant.components.sensor",
    "homeassistant.components.binary_sensor",
    "homeassistant.components.switch",
    "homeassistant.components.button",
    "homeassistant.components.select",
    "homeassistant.helpers.aiohttp_client",
    "homeassistant.helpers.entity_platform",
    "homeassistant.helpers.selector",
    "voluptuous",
]:
    sys.modules.setdefault(mod, ha_mock)


# config_flow.py defines `class VSphereConfigFlow(_RestrictionFlowMixin, ConfigFlow,
# domain=DOMAIN)` and `class VSphereOptionsFlow(_RestrictionFlowMixin,
# OptionsFlowWithConfigEntry)`. Mixing a real class with a plain MagicMock *instance*
# as a base raises "metaclass conflict" (the instance's metaclass is MagicMock, which
# isn't a subclass of `type`). These stand-ins are real classes so the module imports
# cleanly; the flow classes themselves are not under test — only module-level helpers
# like `_duration_to_seconds()` are (see the CoordinatorEntity note in
# tests/test_entity_connectivity.py's sibling harness constraint).
class _StubConfigFlow:
    def __init_subclass__(cls, *, domain=None, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)


class _StubOptionsFlow:
    pass


ha_mock.ConfigFlow = _StubConfigFlow
ha_mock.OptionsFlowWithConfigEntry = _StubOptionsFlow
