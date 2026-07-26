"""Tests for VSphereEventListener's PropertyCollector translation layer.

VSphereEventListener has no HA base class, so it is constructed directly
(the entity classes in this integration are not testable under this harness
because `homeassistant` is mocked — see tests/conftest.py).
"""

from unittest.mock import MagicMock

from custom_components.vsphere.event_listener import (
    _CLUSTER_PROP_MAP,
    _RESOURCE_POOL_PROP_MAP,
    VSphereEventListener,
)

# get_clusters() in vsphere_client.py builds (source read, not imported — that
# module pulls in pyVmomi and this test suite must stay pyVmomi-free):
#   {"moref", "name", "drs_enabled", "drs_automation_level", "ha_enabled",
#    "ha_admission_control", "total_hosts", "effective_hosts", "total_cpu_mhz",
#    "total_memory_mb", "vm_count"}
# minus {"moref", "vm_count"} (vm_count is initial-fetch-only on push, see
# _derive_cluster_values' comment on _host_list):
EXPECTED_CLUSTER_PUSH_KEYS = {
    "name",
    "drs_enabled",
    "drs_automation_level",
    "ha_enabled",
    "ha_admission_control",
    "total_hosts",
    "effective_hosts",
    "total_cpu_mhz",
    "total_memory_mb",
}

# get_resource_pools() in vsphere_client.py builds:
#   {"moref", "name", "cpu_reservation_mhz", "cpu_limit_mhz",
#    "memory_reservation_mb", "memory_limit_mb", "vm_count"}
# minus {"moref"} — resource pools DO get vm_count from the watched `vm` path:
EXPECTED_RESOURCE_POOL_PUSH_KEYS = {
    "name",
    "cpu_reservation_mhz",
    "cpu_limit_mhz",
    "memory_reservation_mb",
    "memory_limit_mb",
    "vm_count",
}


def make_listener() -> VSphereEventListener:
    return VSphereEventListener(
        hass=MagicMock(),
        client=MagicMock(),
        vsphere_data=MagicMock(),
        entry_id="test",
        categories={},
        entity_filter={},
    )


# ---------------------------------------------------------------------------
# VM partial-merge regressions — deltas are partial, derivation must fall
# back to the cached row for whichever field the delta didn't carry.
# ---------------------------------------------------------------------------


class TestVmPartialMerge:
    def test_connection_state_only_delta_overrides_stale_power_state(self):
        listener = make_listener()
        listener._local_state_cache["vms"]["vm-1"] = {"power_state": "poweredOn", "state": "running"}

        translated = listener._translate_properties("vms", {"summary.runtime.connectionState": "disconnected"}, "vm-1")

        assert translated["state"] == "disconnected"

    def test_power_state_only_delta_uses_cached_connection_state(self):
        listener = make_listener()
        listener._local_state_cache["vms"]["vm-1"] = {"connection_state": "connected"}

        translated = listener._translate_properties("vms", {"summary.runtime.powerState": "poweredOff"}, "vm-1")

        assert translated["state"] == "off"

    def test_cpu_use_pct_falls_back_to_cached_max_cpu_mhz(self):
        listener = make_listener()
        listener._local_state_cache["vms"]["vm-1"] = {"max_cpu_mhz": 2000}

        translated = listener._translate_properties("vms", {"summary.quickStats.overallCpuUsage": 500}, "vm-1")

        assert translated["cpu_use_pct"] == 25.0


# ---------------------------------------------------------------------------
# Internal keys must never leak into the translated output
# ---------------------------------------------------------------------------


class TestNoInternalKeysLeak:
    def test_underscore_prefixed_keys_never_appear(self):
        listener = make_listener()
        delta = {
            "summary.config.name": "vm1",
            "summary.quickStats.overallCpuUsage": 500,
            "runtime.maxCpuUsage": 2000,
            "summary.storage.committed": 123456789,
        }

        translated = listener._translate_properties("vms", delta, "vm-2")

        assert not any(k.startswith("_") for k in translated)


# ---------------------------------------------------------------------------
# Unknown category
# ---------------------------------------------------------------------------


class TestUnknownCategory:
    def test_unknown_category_returns_empty_dict(self):
        listener = make_listener()

        translated = listener._translate_properties("bogus", {"whatever": object()}, "obj-1")

        assert translated == {}


# ---------------------------------------------------------------------------
# Cluster translation
# ---------------------------------------------------------------------------


class TestClusterTranslation:
    def _full_delta(self) -> dict:
        return {
            "name": "Cluster A",
            "configuration.drsConfig.enabled": True,
            "configuration.drsConfig.defaultVmBehavior": "fullyAutomated",
            "configuration.dasConfig.enabled": True,
            "configuration.dasConfig.admissionControlEnabled": True,
            "summary.numHosts": 4,
            "summary.numEffectiveHosts": 4,
            "summary.totalCpu": 40000,
            "summary.totalMemory": 137438953472,  # 128 GiB in bytes
            "host": [MagicMock(), MagicMock()],
        }

    def test_key_parity_with_get_clusters(self):
        listener = make_listener()
        # Every path in _CLUSTER_PROP_MAP is exercised by this delta.
        delta = self._full_delta()
        assert set(delta.keys()) == set(_CLUSTER_PROP_MAP.keys())

        translated = listener._translate_properties("clusters", delta, "cluster-1")

        assert set(translated.keys()) == EXPECTED_CLUSTER_PUSH_KEYS

    def test_raw_host_moref_list_never_appears(self):
        listener = make_listener()
        translated = listener._translate_properties("clusters", self._full_delta(), "cluster-1")

        assert "host" not in translated
        assert "_host_list" not in translated

    def test_total_memory_bytes_converts_to_mb_with_get_clusters_rounding(self):
        listener = make_listener()
        translated = listener._translate_properties("clusters", {"summary.totalMemory": 137438953472}, "cluster-1")

        # get_clusters() does: round(summary.totalMemory / (1024 * 1024), 0)
        assert translated["total_memory_mb"] == round(137438953472 / (1024 * 1024), 0)
        assert translated["total_memory_mb"] == 131072.0

    def test_drs_automation_level_none_when_drs_disabled(self):
        listener = make_listener()
        delta = {
            "configuration.drsConfig.enabled": False,
            "configuration.drsConfig.defaultVmBehavior": "fullyAutomated",
        }

        translated = listener._translate_properties("clusters", delta, "cluster-1")

        assert translated["drs_enabled"] is False
        assert translated["drs_automation_level"] is None


# ---------------------------------------------------------------------------
# Resource pool translation
# ---------------------------------------------------------------------------


class TestResourcePoolTranslation:
    def _full_delta(self) -> dict:
        return {
            "name": "Pool A",
            "config.cpuAllocation.reservation": 1000,
            "config.cpuAllocation.limit": -1,
            "config.memoryAllocation.reservation": 2048,
            "config.memoryAllocation.limit": -1,
            "vm": [MagicMock(), MagicMock(), MagicMock()],
        }

    def test_key_parity_with_get_resource_pools(self):
        listener = make_listener()
        delta = self._full_delta()
        assert set(delta.keys()) == set(_RESOURCE_POOL_PROP_MAP.keys())

        translated = listener._translate_properties("resource_pools", delta, "pool-1")

        assert set(translated.keys()) == EXPECTED_RESOURCE_POOL_PUSH_KEYS

    def test_vm_count_derived_from_watched_vm_list(self):
        listener = make_listener()
        translated = listener._translate_properties("resource_pools", self._full_delta(), "pool-1")

        assert translated["vm_count"] == 3

    def test_raw_vm_moref_list_never_appears(self):
        listener = make_listener()
        translated = listener._translate_properties("resource_pools", self._full_delta(), "pool-1")

        assert "vm" not in translated
        assert "_vm_list" not in translated
