"""Tests for dynamic entity discovery helpers."""

from custom_components.vsphere.entity import (
    EntityDiscovery,
    diff_keys,
    is_stale_unique_id,
    iter_rows,
    row_allowed,
    valid_morefs,
)

ENTRY = "abc123"


def _data(vms=("vm-1",), hosts=("host-1",)):
    return {
        "vms": {m: {"name": m} for m in vms},
        "hosts": {m: {"name": m} for m in hosts},
        "networks": {"net-1": {}},
        "licenses": {},
    }


def test_iter_rows_category_disabled():
    assert list(iter_rows(_data(), {"vms": False}, {}, "vms")) == []
    assert [m for m, _ in iter_rows(_data(), {"vms": True}, {}, "vms")] == ["vm-1"]


def test_filter_select_mode():
    flt = {"vms": {"mode": "select", "morefs": ["vm-2"]}}
    assert not row_allowed(flt, "vms", "vm-1")
    assert row_allowed(flt, "vms", "vm-2")
    assert row_allowed(flt, "hosts", "host-1")
    data = _data(vms=("vm-1", "vm-2"))
    assert [m for m, _ in iter_rows(data, {"vms": True}, flt, "vms")] == ["vm-2"]


def test_iter_rows_alarm_uses_hosts_without_hosts_category():
    cats = {"events_alarms": True, "hosts": False}
    assert [m for m, _ in iter_rows(_data(), cats, {}, "events_alarms", "hosts")] == ["host-1"]


def test_diff_keys():
    a, r = diff_keys(frozenset({("vms", "vm-1")}), frozenset({("vms", "vm-1"), ("vms", "vm-2")}))
    assert a == {("vms", "vm-2")}
    assert r == frozenset()
    a, r = diff_keys(frozenset({("vms", "vm-1")}), frozenset())
    assert a == frozenset()
    assert r == {("vms", "vm-1")}


def _discovery(rows):
    built, added = [], []

    def specs():
        for key in rows():
            yield key, (lambda k=key: built.append(k) or [k])

    return EntityDiscovery(specs, added.extend), built, added


def test_discovery_only_builds_new():
    rows = {("vms", "vm-1")}
    d, built, added = _discovery(lambda: sorted(rows))
    d.run()
    assert added == [("vms", "vm-1")]
    rows.add(("vms", "vm-2"))
    d.run()
    assert built == [("vms", "vm-1"), ("vms", "vm-2")]
    assert added[-1] == ("vms", "vm-2")


def test_discovery_unchanged_no_work():
    rows = {("vms", "vm-1")}
    d, built, _ = _discovery(lambda: sorted(rows))
    d.run()
    d.run()
    assert built == [("vms", "vm-1")]


def test_discovery_conditional_entity_appears_later():
    rows = {("hosts", "h1")}
    d, _, added = _discovery(lambda: sorted(rows))
    d.run()
    rows.add(("host_power_policy", "h1"))
    d.run()
    assert added == [("hosts", "h1"), ("host_power_policy", "h1")]


def test_discovery_absent_then_present_not_readded():
    rows = {("vms", "vm-1")}
    d, _, added = _discovery(lambda: sorted(rows))
    d.run()
    rows.clear()
    d.run()
    rows.add(("vms", "vm-1"))
    d.run()
    assert added == [("vms", "vm-1")]


def test_discovery_forget_then_present_is_recreated():
    rows = {("vms", "vm-1"), ("vms", "vm-2")}
    d, _, added = _discovery(lambda: sorted(rows))
    d.run()
    rows.discard(("vms", "vm-1"))
    d.run()
    d.forget("vm-1")
    d.run()
    assert added == [("vms", "vm-1"), ("vms", "vm-2")]
    rows.add(("vms", "vm-1"))
    d.run()
    assert added[-1] == ("vms", "vm-1")
    assert len(added) == 3


def test_removed_moref_reported_stale():
    cats = {"vms": True, "hosts": True}
    before = valid_morefs(_data(vms=("vm-1", "vm-2")), cats, ENTRY)
    after = valid_morefs(_data(vms=("vm-1",)), cats, ENTRY)
    assert before - after == {"vm-2"}
    assert is_stale_unique_id(f"{ENTRY}_vm-2_cpu", ENTRY, after)
    assert not is_stale_unique_id(f"{ENTRY}_vm-1_cpu", ENTRY, after)
    # vm-1 must not shield vm-10
    assert is_stale_unique_id(f"{ENTRY}_vm-10_cpu", ENTRY, after)
    assert not is_stale_unique_id("other_vm-2_cpu", ENTRY, after)


def test_valid_morefs_categories():
    assert valid_morefs(_data(), {"vms": False, "hosts": False}, ENTRY) == {ENTRY}
    assert "net-1" in valid_morefs(_data(), {"network": True}, ENTRY)
    assert "vm-1" in valid_morefs(_data(), {"events_alarms": True}, ENTRY)
