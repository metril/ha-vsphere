"""Tests for PermissionResolver."""

import pytest
from custom_components.vsphere.permissions import PermissionResolver
from custom_components.vsphere.const import (
    VmAction,
    HostAction,
    RESTRICTION_GROUP_DESTRUCTIVE,
    RESTRICTION_GROUP_SNAPSHOTS,
    RESTRICTION_GROUP_MIGRATE,
    RESTRICTION_GROUP_HOST_OPS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_VM_ACTIONS = {a.value for a in VmAction}
ALL_HOST_ACTIONS = {a.value for a in HostAction}


# ---------------------------------------------------------------------------
# Empty restrictions → everything allowed
# ---------------------------------------------------------------------------


class TestEmptyRestrictions:
    def test_vm_action_allowed_when_no_restrictions(self):
        resolver = PermissionResolver({})
        assert resolver.is_allowed("vms", "vm-100", "power_on") is True

    def test_host_action_allowed_when_no_restrictions(self):
        resolver = PermissionResolver({})
        assert resolver.is_allowed("hosts", "host-10", "reboot") is True

    def test_allowed_actions_returns_all_vm_actions(self):
        resolver = PermissionResolver({})
        assert resolver.allowed_actions("vms", "vm-100") == ALL_VM_ACTIONS

    def test_allowed_actions_returns_all_host_actions(self):
        resolver = PermissionResolver({})
        assert resolver.allowed_actions("hosts", "host-10") == ALL_HOST_ACTIONS


# ---------------------------------------------------------------------------
# Per-object per-action blocking
# ---------------------------------------------------------------------------


class TestPerObjectPerAction:
    def setup_method(self):
        self.restrictions = {
            "vms": {
                "vm-102": {"power_off": True},  # True = blocked
            }
        }
        self.resolver = PermissionResolver(self.restrictions)

    def test_blocked_action_on_targeted_vm(self):
        assert self.resolver.is_allowed("vms", "vm-102", "power_off") is False

    def test_other_action_on_targeted_vm_still_allowed(self):
        assert self.resolver.is_allowed("vms", "vm-102", "power_on") is True

    def test_same_action_on_different_vm_still_allowed(self):
        assert self.resolver.is_allowed("vms", "vm-101", "power_off") is True

    def test_allowed_actions_excludes_blocked(self):
        actions = self.resolver.allowed_actions("vms", "vm-102")
        assert "power_off" not in actions
        assert "power_on" in actions

    def test_allowed_actions_unaffected_on_other_vm(self):
        actions = self.resolver.allowed_actions("vms", "vm-101")
        assert actions == ALL_VM_ACTIONS


# ---------------------------------------------------------------------------
# Per-object per-action explicit allow overrides global block
# ---------------------------------------------------------------------------


class TestPerObjectExplicitAllowOverridesGlobal:
    """Object-level False (explicitly allowed) must win over global True (blocked)."""

    def setup_method(self):
        # Global blocks power_off, but vm-101 explicitly allows it
        self.restrictions = {
            "global": {"power_off": True},
            "vms": {
                "vm-101": {"power_off": False},
            },
        }
        self.resolver = PermissionResolver(self.restrictions)

    def test_object_level_false_overrides_global_true(self):
        assert self.resolver.is_allowed("vms", "vm-101", "power_off") is True

    def test_other_vm_still_blocked_by_global(self):
        assert self.resolver.is_allowed("vms", "vm-102", "power_off") is False


# ---------------------------------------------------------------------------
# Per-object blanket (_all)
# ---------------------------------------------------------------------------


class TestPerObjectBlanket:
    def setup_method(self):
        self.restrictions = {
            "vms": {
                "vm-200": {"_all": True},
            }
        }
        self.resolver = PermissionResolver(self.restrictions)

    def test_all_actions_blocked_on_targeted_vm(self):
        for action in ALL_VM_ACTIONS:
            assert self.resolver.is_allowed("vms", "vm-200", action) is False

    def test_other_vm_unaffected(self):
        assert self.resolver.is_allowed("vms", "vm-100", "power_on") is True

    def test_allowed_actions_empty_for_blanket_blocked_vm(self):
        assert self.resolver.allowed_actions("vms", "vm-200") == set()

    def test_per_object_per_action_overrides_object_blanket(self):
        """Explicit per-action False wins over object _all True."""
        restrictions = {
            "vms": {
                "vm-200": {"_all": True, "power_on": False},
            }
        }
        resolver = PermissionResolver(restrictions)
        # power_on explicitly allowed at per-object level
        assert resolver.is_allowed("vms", "vm-200", "power_on") is True
        # everything else still blocked by _all
        assert resolver.is_allowed("vms", "vm-200", "power_off") is False


# ---------------------------------------------------------------------------
# Global per-action blocking
# ---------------------------------------------------------------------------


class TestGlobalPerAction:
    def setup_method(self):
        self.restrictions = {
            "global": {"snapshot_create": True},
        }
        self.resolver = PermissionResolver(self.restrictions)

    def test_blocked_action_on_any_vm(self):
        assert self.resolver.is_allowed("vms", "vm-100", "snapshot_create") is False
        assert self.resolver.is_allowed("vms", "vm-999", "snapshot_create") is False

    def test_other_actions_not_affected(self):
        assert self.resolver.is_allowed("vms", "vm-100", "power_on") is True

    def test_allowed_actions_excludes_globally_blocked(self):
        actions = self.resolver.allowed_actions("vms", "vm-100")
        assert "snapshot_create" not in actions


# ---------------------------------------------------------------------------
# Global shortcut groups
# ---------------------------------------------------------------------------


class TestGlobalShortcutGroups:
    def test_destructive_group_blocks_all_destructive_actions(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_DESTRUCTIVE: True}})
        for action in ("power_off", "shutdown", "reset", "snapshot_remove"):
            assert resolver.is_allowed("vms", "vm-1", action) is False, action

    def test_destructive_group_does_not_block_non_destructive(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_DESTRUCTIVE: True}})
        assert resolver.is_allowed("vms", "vm-1", "power_on") is True
        assert resolver.is_allowed("vms", "vm-1", "snapshot_create") is True

    def test_snapshots_group_blocks_snapshot_actions(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_SNAPSHOTS: True}})
        assert resolver.is_allowed("vms", "vm-1", "snapshot_create") is False
        assert resolver.is_allowed("vms", "vm-1", "snapshot_remove") is False
        assert resolver.is_allowed("vms", "vm-1", "power_on") is True

    def test_migrate_group_blocks_migrate(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_MIGRATE: True}})
        assert resolver.is_allowed("vms", "vm-1", "migrate") is False
        assert resolver.is_allowed("vms", "vm-1", "power_on") is True

    def test_host_ops_group_blocks_host_actions(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_HOST_OPS: True}})
        for action in ("shutdown", "reboot", "maintenance", "power_policy"):
            assert resolver.is_allowed("hosts", "host-1", action) is False, action

    def test_host_ops_group_does_not_bleed_into_vms(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_HOST_OPS: True}})
        # VMs have shutdown action too — but host_ops group only covers HOST actions
        assert resolver.is_allowed("vms", "vm-1", "shutdown") is True


# ---------------------------------------------------------------------------
# Global _all (nuclear switch)
# ---------------------------------------------------------------------------


class TestGlobalAll:
    def setup_method(self):
        self.resolver = PermissionResolver({"global": {"_all": True}})

    def test_all_vm_actions_blocked(self):
        for action in ALL_VM_ACTIONS:
            assert self.resolver.is_allowed("vms", "vm-1", action) is False, action

    def test_all_host_actions_blocked(self):
        for action in ALL_HOST_ACTIONS:
            assert self.resolver.is_allowed("hosts", "host-1", action) is False, action

    def test_allowed_actions_empty_for_vms(self):
        assert self.resolver.allowed_actions("vms", "vm-1") == set()

    def test_allowed_actions_empty_for_hosts(self):
        assert self.resolver.allowed_actions("hosts", "host-1") == set()

    def test_per_object_per_action_false_overrides_nuclear(self):
        """Explicit per-object per-action False (allowed) should override global _all."""
        restrictions = {
            "global": {"_all": True},
            "vms": {"vm-special": {"power_on": False}},
        }
        resolver = PermissionResolver(restrictions)
        assert resolver.is_allowed("vms", "vm-special", "power_on") is True
        assert resolver.is_allowed("vms", "vm-special", "power_off") is False

    def test_per_object_blanket_false_overrides_nuclear(self):
        """Per-object _all: False (all allowed on this object) overrides global _all."""
        restrictions = {
            "global": {"_all": True},
            "vms": {"vm-exempt": {"_all": False}},
        }
        resolver = PermissionResolver(restrictions)
        for action in ALL_VM_ACTIONS:
            assert resolver.is_allowed("vms", "vm-exempt", action) is True, action


# ---------------------------------------------------------------------------
# explain()
# ---------------------------------------------------------------------------


class TestExplain:
    def test_explain_allowed_empty_restrictions(self):
        resolver = PermissionResolver({})
        msg = resolver.explain("vms", "vm-1", "power_on")
        assert "allowed" in msg.lower()

    def test_explain_blocked_per_object_per_action(self):
        resolver = PermissionResolver({"vms": {"vm-1": {"power_off": True}}})
        msg = resolver.explain("vms", "vm-1", "power_off")
        assert "blocked" in msg.lower()
        # Should mention where the restriction came from
        assert "vm-1" in msg or "per-object" in msg.lower() or "object" in msg.lower()

    def test_explain_blocked_per_object_blanket(self):
        resolver = PermissionResolver({"vms": {"vm-1": {"_all": True}}})
        msg = resolver.explain("vms", "vm-1", "power_off")
        assert "blocked" in msg.lower()

    def test_explain_blocked_global_per_action(self):
        resolver = PermissionResolver({"global": {"power_off": True}})
        msg = resolver.explain("vms", "vm-1", "power_off")
        assert "blocked" in msg.lower()
        assert "global" in msg.lower()

    def test_explain_blocked_global_group(self):
        resolver = PermissionResolver({"global": {RESTRICTION_GROUP_DESTRUCTIVE: True}})
        msg = resolver.explain("vms", "vm-1", "power_off")
        assert "blocked" in msg.lower()

    def test_explain_blocked_global_all(self):
        resolver = PermissionResolver({"global": {"_all": True}})
        msg = resolver.explain("vms", "vm-1", "power_on")
        assert "blocked" in msg.lower()

    def test_explain_returns_string(self):
        resolver = PermissionResolver({})
        result = resolver.explain("vms", "vm-1", "power_on")
        assert isinstance(result, str)
        assert len(result) > 0
