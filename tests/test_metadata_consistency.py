"""Cross-file metadata consistency checks (version, HA compatibility)."""

import json
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


def _load_pyproject() -> dict:
    with (REPO_ROOT / "pyproject.toml").open("rb") as f:
        return tomllib.load(f)


def _load_manifest() -> dict:
    with (REPO_ROOT / "custom_components" / "vsphere" / "manifest.json").open(encoding="utf-8") as f:
        return json.load(f)


def _load_hacs() -> dict:
    with (REPO_ROOT / "hacs.json").open(encoding="utf-8") as f:
        return json.load(f)


class TestVersionConsistency:
    def test_pyproject_version_matches_manifest_version(self):
        pyproject = _load_pyproject()
        manifest = _load_manifest()
        assert pyproject["project"]["version"] == manifest["version"]


class TestManifestSchema:
    """Guard the manifest keys hassfest accepts for a custom integration.

    CUSTOM_INTEGRATION_MANIFEST_SCHEMA extends the core schema with only
    documentation/version/issue_tracker/import_executor. `homeassistant` is
    not among them — the minimum HA version belongs in hacs.json, and putting
    it in the manifest fails hassfest.
    """

    def test_manifest_has_no_homeassistant_key(self):
        assert "homeassistant" not in _load_manifest()

    def test_hacs_declares_minimum_ha_version(self):
        assert _load_hacs()["homeassistant"]


class TestServicesSchema:
    """Guard the services.yaml keys hassfest accepts.

    Response support is declared in code via `supports_response=`, not in
    services.yaml — `response_supported` there fails hassfest.
    """

    def test_services_yaml_has_no_response_supported_key(self):
        services_yaml = REPO_ROOT / "custom_components" / "vsphere" / "services.yaml"
        assert "response_supported" not in services_yaml.read_text(encoding="utf-8")

    def test_service_field_translations_have_no_selector_block(self):
        """Option labels live in the top-level `selector` block, not inside fields."""
        for name in ("strings.json", "translations/en.json"):
            data = json.loads((REPO_ROOT / "custom_components" / "vsphere" / name).read_text(encoding="utf-8"))
            for service, service_data in data.get("services", {}).items():
                for field, field_data in service_data.get("fields", {}).items():
                    assert "selector" not in field_data, f"{name}: services.{service}.fields.{field}"
