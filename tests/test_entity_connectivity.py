"""Tests for entity.is_vm_disconnected()."""

import pytest

from custom_components.vsphere.entity import is_vm_disconnected

# ---------------------------------------------------------------------------
# Healthy path
# ---------------------------------------------------------------------------


class TestConnected:
    def test_vm_and_host_connected_and_powered_on(self):
        data = {
            "vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}},
            "hosts": {"host-1": {"connection_state": "connected", "state": "poweredOn"}},
        }
        assert is_vm_disconnected(data, "vm-1") is False


# ---------------------------------------------------------------------------
# VM's own connection_state
# ---------------------------------------------------------------------------


class TestVmOwnConnectionState:
    @pytest.mark.parametrize("connection_state", ["disconnected", "orphaned", "inaccessible", "invalid"])
    def test_non_connected_vm_state_is_disconnected(self, connection_state):
        data = {"vms": {"vm-1": {"connection_state": connection_state}}}
        assert is_vm_disconnected(data, "vm-1") is True


# ---------------------------------------------------------------------------
# Host cascade — VM reports connected but its host doesn't
# ---------------------------------------------------------------------------


class TestHostCascade:
    def test_host_not_responding_cascades(self):
        data = {
            "vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}},
            "hosts": {"host-1": {"connection_state": "notResponding", "state": "poweredOn"}},
        }
        assert is_vm_disconnected(data, "vm-1") is True

    def test_host_disconnected_cascades(self):
        data = {
            "vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}},
            "hosts": {"host-1": {"connection_state": "disconnected", "state": "poweredOn"}},
        }
        assert is_vm_disconnected(data, "vm-1") is True

    def test_host_powered_off_cascades(self):
        data = {
            "vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}},
            "hosts": {"host-1": {"connection_state": "connected", "state": "poweredOff"}},
        }
        assert is_vm_disconnected(data, "vm-1") is True


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_vm_moref_absent_is_disconnected(self):
        data = {"vms": {}, "hosts": {}}
        assert is_vm_disconnected(data, "vm-missing") is True

    def test_vm_with_no_host_moref_is_connected(self):
        """Nothing to cascade to — VM's own state is authoritative."""
        data = {"vms": {"vm-1": {"connection_state": "connected"}}}
        assert is_vm_disconnected(data, "vm-1") is False

    def test_host_row_missing_entirely_fails_open(self):
        """Hosts category disabled or host not yet known — fail open."""
        data = {
            "vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}},
            "hosts": {},
        }
        assert is_vm_disconnected(data, "vm-1") is False

    def test_no_hosts_key_at_all_fails_open(self):
        data = {"vms": {"vm-1": {"connection_state": "connected", "host_moref": "host-1"}}}
        assert is_vm_disconnected(data, "vm-1") is False

    def test_keys_absent_on_both_rows_is_connected(self):
        """A partially-populated row (keys missing, not explicitly set) must not read as disconnected."""
        data = {
            "vms": {"vm-1": {"host_moref": "host-1"}},
            "hosts": {"host-1": {"name": "esxi1"}},
        }
        assert is_vm_disconnected(data, "vm-1") is False
