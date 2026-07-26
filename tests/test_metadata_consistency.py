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


class TestHomeAssistantVersionConsistency:
    def test_hacs_minimum_ha_matches_manifest(self):
        hacs = _load_hacs()
        manifest = _load_manifest()
        assert hacs["homeassistant"] == manifest["homeassistant"]
