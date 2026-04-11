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
