"""Tests for Batch A listener fixes (alarms, replay, derive helpers, batching)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from custom_components.vsphere.event_listener import VSphereEventListener


def make_listener(categories=None):
    hass = MagicMock()
    hass.loop.call_soon_threadsafe.side_effect = lambda fn, *a: fn(*a)
    return VSphereEventListener(
        hass=hass,
        client=MagicMock(),
        vsphere_data=MagicMock(),
        entry_id="e1",
        categories=categories if categories is not None else {"events_alarms": True},
        entity_filter={},
    )


def fired(listener, event_type):
    return [c.args[1] for c in listener._hass.bus.async_fire.call_args_list if c.args[0] == event_type]


def alarm_state(key="alarm-1", status="red"):
    return SimpleNamespace(key=key, overallStatus=status, time=None, acknowledged=False)


class TestAlarms:
    def test_new_alarm_mid_session_fires(self):
        listener = make_listener()
        listener._local_state_cache["vms"]["vm-1"] = {"name": "web"}
        listener._process_alarm_update("vm-1", "vm", [alarm_state()])
        events = fired(listener, "vsphere_alarm_triggered")
        assert len(events) == 1
        assert events[0]["old_status"] is None
        assert events[0]["entity_name"] == "web"

    def test_seeded_alarm_same_status_silent(self):
        listener = make_listener()
        listener._alarm_cache["vm-1"] = [{"alarm_key": "alarm-1", "status": "red"}]
        listener._process_alarm_update("vm-1", "vm", [alarm_state()])
        assert fired(listener, "vsphere_alarm_triggered") == []


class TestEventName:
    def test_vsphere_event_uses_cached_name_then_moref(self):
        listener = make_listener()
        listener._local_state_cache["hosts"]["host-1"] = {"name": "esx1"}
        listener._check_and_fire_vsphere_events("hosts", "host-1", {"summary.runtime.powerState": "poweredOff"})
        listener._check_and_fire_vsphere_events("hosts", "host-2", {"summary.runtime.powerState": "poweredOn"})
        events = fired(listener, "vsphere_event")
        assert [e["entity_name"] for e in events] == ["esx1", "host-2"]


class TestZeroing:
    def test_vm_off_zeroes_metrics(self):
        listener = make_listener()
        t = listener._translate_properties(
            "vms",
            {"summary.runtime.powerState": "poweredOff", "summary.quickStats.uptimeSeconds": 7200},
            "vm-1",
        )
        assert t["uptime_hours"] == 0
        assert t["cpu_use_pct"] == 0.0
        assert t["memory_used_mb"] == 0

    def test_vm_none_uptime_is_zero_when_on(self):
        listener = make_listener()
        t = listener._translate_properties(
            "vms",
            {"summary.runtime.powerState": "poweredOn", "summary.quickStats.uptimeSeconds": None},
            "vm-1",
        )
        assert t["uptime_hours"] == 0

    def test_host_off_and_none(self):
        listener = make_listener()
        t = listener._translate_properties(
            "hosts", {"summary.runtime.powerState": "poweredOff", "summary.quickStats.uptime": 3600}, "h"
        )
        assert t["uptime_hours"] == 0 and t["cpu_usage_ghz"] == 0 and t["mem_usage_gb"] == 0
        t = listener._translate_properties(
            "hosts", {"summary.runtime.powerState": "poweredOn", "summary.quickStats.overallCpuUsage": None}, "h2"
        )
        assert t["cpu_usage_ghz"] == 0

    def test_host_on_normal(self):
        listener = make_listener()
        t = listener._translate_properties(
            "hosts", {"summary.runtime.powerState": "poweredOn", "summary.quickStats.uptime": 7200}, "h"
        )
        assert t["uptime_hours"] == 2.0


class TestDrs:
    def test_level_restored_on_reenable(self):
        listener = make_listener()
        listener._local_state_cache["clusters"]["c1"] = {}
        full = {
            "configuration.drsConfig.enabled": True,
            "configuration.drsConfig.defaultVmBehavior": "fullyAutomated",
        }
        t = listener._translate_properties("clusters", full, "c1")
        assert t["drs_automation_level"] == "fullyAutomated"
        t = listener._translate_properties("clusters", {"configuration.drsConfig.enabled": False}, "c1")
        assert t["drs_automation_level"] is None
        listener._local_state_cache["clusters"]["c1"] = {"drs_enabled": False, "drs_automation_level": None}
        t = listener._translate_properties("clusters", {"configuration.drsConfig.enabled": True}, "c1")
        assert t["drs_automation_level"] == "fullyAutomated"


class TestTranslate:
    def test_unmapped_paths_dropped(self):
        listener = make_listener()
        t = listener._translate_properties(
            "vms", {"triggeredAlarmState": [object()], "config.template": False, "summary.config.name": "x"}, "vm-1"
        )
        assert t == {"name": "x"}

    def test_vmotion_host_name_fallback(self):
        listener = make_listener()
        host = SimpleNamespace(_moId="host-9")
        t = listener._translate_properties("vms", {"runtime.host": host}, "vm-1")
        assert t["host_moref"] == "host-9"
        assert t["host_name"] == "host-9"
        listener._local_state_cache["hosts"]["host-9"] = {"name": "esx9"}
        t = listener._translate_properties("vms", {"runtime.host": host}, "vm-1")
        assert t["host_name"] == "esx9"


def obj_update(kind, moref, changes):
    from custom_components.vsphere import event_listener as el

    obj = type("VM", (), {"_moId": moref})()
    el_mod = el
    return SimpleNamespace(
        obj=obj, kind=kind, changeSet=[SimpleNamespace(name=n, val=v) for n, v in changes.items()]
    ), el_mod


class TestReplayAndBatch:
    def _listener(self):
        listener = make_listener()
        listener._obj_type_to_category = lambda t: "vms"
        return listener

    def test_batch_collects_and_leave_drops(self):
        listener = self._listener()
        batch = []
        a, _ = obj_update("enter", "vm-1", {"summary.config.name": "a"})
        b, _ = obj_update("enter", "vm-2", {"summary.config.name": "b"})
        listener._process_object_update(a, batch=batch)
        listener._process_object_update(b, batch=batch)
        assert [x[1] for x in batch] == ["vm-1", "vm-2"]
        leave, _ = obj_update("leave", "vm-1", {})
        listener._process_object_update(leave, batch=batch)
        assert [x[1] for x in batch] == ["vm-2"]
        listener._vsphere_data.async_update_from_push.assert_not_called()
        listener._dispatch_batch(batch)
        listener._vsphere_data.async_update_from_push_batch.assert_called_once_with(batch)


class TestStop:
    def test_cancel_wait_called_before_join(self):
        listener = make_listener()
        order = []
        listener._pc = MagicMock()
        listener._pc.CancelWaitForUpdates.side_effect = lambda: order.append("cancel")
        listener._thread = MagicMock()
        listener._thread.is_alive.return_value = True
        listener._thread.join.side_effect = lambda timeout=None: order.append("join")
        listener.stop()
        assert order == ["cancel", "join"]
