"""Tests for derive_vm_state()."""

import pytest

from custom_components.vsphere.const import derive_vm_state

# ---------------------------------------------------------------------------
# Normal mappings — connected VM, power state drives the result
# ---------------------------------------------------------------------------


class TestNormalPowerMapping:
    def test_powered_on_connected_is_running(self):
        assert derive_vm_state("poweredOn", "connected") == "running"

    def test_powered_off_connected_is_off(self):
        assert derive_vm_state("poweredOff", "connected") == "off"

    def test_suspended_connected_is_suspended(self):
        assert derive_vm_state("suspended", "connected") == "suspended"


# ---------------------------------------------------------------------------
# The core regression: connection state wins over a stale powerState
# ---------------------------------------------------------------------------


class TestConnectionStateWins:
    def test_powered_on_disconnected_is_disconnected(self):
        """vCenter freezes powerState when it loses contact — connectionState must win."""
        assert derive_vm_state("poweredOn", "disconnected") == "disconnected"

    @pytest.mark.parametrize("connection_state", ["disconnected", "orphaned", "inaccessible", "invalid"])
    def test_every_non_connected_state_collapses(self, connection_state):
        assert derive_vm_state("poweredOn", connection_state) == "disconnected"

    @pytest.mark.parametrize("connection_state", ["disconnected", "orphaned", "inaccessible", "invalid"])
    def test_non_connected_state_wins_regardless_of_power_state(self, connection_state):
        assert derive_vm_state("poweredOff", connection_state) == "disconnected"
        assert derive_vm_state("suspended", connection_state) == "disconnected"


# ---------------------------------------------------------------------------
# Missing / falsy connection_state falls through to the power mapping
# ---------------------------------------------------------------------------


class TestMissingConnectionState:
    def test_none_connection_state_falls_through(self):
        assert derive_vm_state("poweredOn", None) == "running"

    def test_empty_string_connection_state_falls_through(self):
        assert derive_vm_state("poweredOn", "") == "running"


# ---------------------------------------------------------------------------
# Missing power_state
# ---------------------------------------------------------------------------


class TestMissingPowerState:
    def test_none_power_state_with_connected_is_unknown(self):
        assert derive_vm_state(None, "connected") == "unknown"

    def test_none_power_state_with_none_connection_state_is_unknown(self):
        assert derive_vm_state(None, None) == "unknown"


# ---------------------------------------------------------------------------
# Unrecognised power state passes through unchanged
# ---------------------------------------------------------------------------


class TestUnrecognisedPowerState:
    def test_unknown_power_state_passes_through(self):
        assert derive_vm_state("someWeirdState", "connected") == "someWeirdState"
