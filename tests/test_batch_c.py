"""Tests for Batch C fixes (permission shortcut groups)."""

from custom_components.vsphere.const import VmAction
from custom_components.vsphere.permissions import PermissionResolver


def _allowed(global_rules, action):
    return PermissionResolver({"global": global_rules}).is_allowed("vms", "vm-1", action)


def test_snapshots_blocks_remove_even_if_destructive_false():
    assert not _allowed({"snapshots": True, "destructive": False}, VmAction.SNAPSHOT_REMOVE)
    assert not _allowed({"destructive": False, "snapshots": True}, VmAction.SNAPSHOT_REMOVE)


def test_destructive_false_alone_allows():
    assert _allowed({"destructive": False}, VmAction.SNAPSHOT_REMOVE)
    assert _allowed({"destructive": False}, VmAction.POWER_OFF)


def test_no_group_falls_through_to_global_all():
    assert not _allowed({"_all": True}, VmAction.SNAPSHOT_REMOVE)
    assert _allowed({}, VmAction.SNAPSHOT_REMOVE)


def test_matched_false_group_beats_global_all():
    assert _allowed({"destructive": False, "_all": True}, VmAction.POWER_OFF)
