"""Tests for the pure PropertyCollector -> entity dict translation in vsphere_client."""

from types import SimpleNamespace as NS

from custom_components.vsphere.vsphere_client import (
    translate_cluster,
    translate_datastore,
    translate_dvportgroup,
    translate_host,
    translate_host_networks,
    translate_licenses,
    translate_resource_pool,
    translate_vm,
    translate_vm_storage,
)

GB = 1024**3


def ref(moid):
    return NS(_moId=moid)


def test_host_full():
    props = {
        "summary.config.name": "esx1",
        "summary.runtime.powerState": "poweredOn",
        "summary.runtime.connectionState": "connected",
        "summary.runtime.inMaintenanceMode": True,
        "summary.config.product.version": "8.0.2",
        "summary.config.product.build": "123",
        "capability.shutdownSupported": True,
        "summary.quickStats.uptime": 7200,
        "summary.hardware.cpuMhz": 2000,
        "summary.hardware.numCpuCores": 16,
        "summary.quickStats.overallCpuUsage": 4000,
        "summary.hardware.memorySize": 64 * GB,
        "summary.quickStats.overallMemoryUsage": 2048,
        "config.powerSystemInfo.currentPolicy.shortName": "static",
        "config.powerSystemCapability.availablePolicy": [NS(key=1, shortName="static", name="High")],
    }
    d = translate_host("host-1", props)
    assert d == {
        "moref": "host-1",
        "name": "esx1",
        "state": "poweredOn",
        "connection_state": "connected",
        "maintenance_mode": True,
        "version": "8.0.2",
        "build": "123",
        "shutdown_supported": True,
        "uptime_hours": 2.0,
        "cpu_mhz": 2000,
        "cpu_cores": 16,
        "cpu_total_ghz": 32.0,
        "cpu_usage_ghz": 4.0,
        "mem_total_gb": 64.0,
        "mem_usage_gb": 2.0,
        "vm_count": 0,
        "power_policy": "static",
        "available_power_policies": [{"key": 1, "short_name": "static", "name": "High"}],
    }


def test_host_missing_props_defaults():
    d = translate_host("host-9", {})
    assert d["name"] == "host-9"
    assert d["state"] == "unknown"
    assert d["connection_state"] == "unknown"
    assert d["maintenance_mode"] is False
    assert d["cpu_total_ghz"] == 0.0
    assert "cpu_mhz" not in d
    assert d["cpu_usage_ghz"] == 0.0
    assert d["mem_total_gb"] == 0.0
    assert d["mem_usage_gb"] == 0.0
    assert d["uptime_hours"] == 0.0
    assert d["power_policy"] == ""
    assert d["available_power_policies"] == []
    assert d["version"] == ""


def _vm_props(**over):
    props = {
        "summary.config.name": "vm1",
        "summary.config.uuid": "u-1",
        "summary.config.numCpu": 4,
        "summary.config.memorySizeMB": 8192,
        "runtime.powerState": "poweredOn",
        "runtime.connectionState": "connected",
        "runtime.host": ref("host-1"),
        "runtime.maxCpuUsage": 4000,
        "summary.overallStatus": "green",
        "summary.storage.committed": 10 * GB,
        "summary.guest.toolsStatus": "toolsOk",
        "summary.guest.ipAddress": "10.0.0.5",
        "summary.guest.guestFullName": "Debian",
        "summary.quickStats.overallCpuUsage": 1000,
        "summary.quickStats.hostMemoryUsage": 2048,
        "summary.quickStats.guestMemoryUsage": 512,
        "summary.quickStats.uptimeSeconds": 3600,
    }
    props.update(over)
    return props


def test_vm_powered_on():
    d = translate_vm("vm-1", _vm_props(), {"host-1": "esx1"})
    assert d["name"] == "vm1"
    assert d["state"] == "running"
    assert d["host_moref"] == "host-1"
    assert d["host_name"] == "esx1"
    assert d["max_cpu_mhz"] == 4000
    assert d["cpu_use_pct"] == 25.0
    assert d["memory_used_mb"] == 2048
    assert d["memory_active_mb"] == 512
    assert d["uptime_hours"] == 1.0
    assert d["used_space_gb"] == 10.0
    assert d["tools_status"] == "toolsOk"
    assert d["snapshot_count"] == 0
    assert d["snapshots"] == []


def test_vm_powered_off_zeroes_running_metrics():
    d = translate_vm("vm-1", _vm_props(**{"runtime.powerState": "poweredOff"}), {"host-1": "esx1"})
    assert d["power_state"] == "poweredOff"
    assert d["cpu_use_pct"] == 0.0
    assert d["memory_used_mb"] == 0
    assert d["memory_active_mb"] == 0
    assert d["uptime_hours"] == 0.0


def test_vm_disconnected_state():
    d = translate_vm("vm-1", _vm_props(**{"runtime.connectionState": "disconnected"}), {})
    assert d["state"] == "disconnected"
    assert d["host_name"] == "host-1"  # unknown host falls back to moref


def test_vm_missing_props():
    d = translate_vm("vm-2", {}, {})
    assert d["name"] == "vm-2"
    assert d["status"] == "gray"
    assert d["tools_status"] == "toolsNotInstalled"
    assert d["host_moref"] == "" and d["host_name"] == ""
    assert d["power_state"] == "unknown"
    assert d["used_space_gb"] == 0.0
    assert d["guest_ip"] == "" and d["guest_os"] == ""
    assert "max_cpu_mhz" not in d


def test_vm_snapshots_nested():
    child = NS(name="child", snapshot=ref("snap-2"), childSnapshotList=[])
    root = NS(name="root", snapshot=ref("snap-1"), childSnapshotList=[child])
    d = translate_vm("vm-1", _vm_props(snapshot=NS(rootSnapshotList=[root])), {})
    assert d["snapshot_count"] == 2
    assert d["snapshots"] == [
        {"name": "root", "moref": "snap-1"},
        {"name": "child", "moref": "snap-2"},
    ]


def test_datastore_full_and_missing():
    props = {
        "summary.name": "ds1",
        "summary.type": "VMFS",
        "summary.accessible": True,
        "summary.capacity": 100 * GB,
        "summary.freeSpace": 40 * GB,
        "summary.url": "ds:///vmfs/1",
        "host": [NS(key=ref("host-1")), NS(key=ref("host-2"))],
        "vm": [ref("vm-1")],
    }
    d = translate_datastore("ds-1", props)
    assert d["capacity_gb"] == 100.0
    assert d["free_gb"] == 40.0
    assert d["used_gb"] == 60.0
    assert d["connected_hosts"] == 2
    assert d["host_morefs"] == ["host-1", "host-2"]
    assert d["virtual_machines"] == 1
    assert translate_datastore("ds-2", {}) == {"name": "ds-2"}
    sparse = translate_datastore("ds-3", {"summary.name": "x"})
    assert sparse["capacity_gb"] == 0.0 and sparse["used_gb"] == 0.0 and sparse["url"] == ""
    assert sparse["host_morefs"] == []


def test_datastore_full_free_zero():
    d = translate_datastore("ds-1", {"summary.capacity": 10 * GB, "summary.freeSpace": 0})
    assert d["free_gb"] == 0.0 and d["used_gb"] == 10.0


def test_cluster():
    props = {
        "name": "c1",
        "configuration.drsConfig.enabled": True,
        "configuration.drsConfig.defaultVmBehavior": "fullyAutomated",
        "configuration.dasConfig.enabled": True,
        "configuration.dasConfig.admissionControlEnabled": False,
        "summary.numHosts": 3,
        "summary.numEffectiveHosts": 2,
        "summary.totalCpu": 30000,
        "summary.totalMemory": 1024 * 1024 * 2048,
        "resourcePool": ref("rp-1"),
    }
    d = translate_cluster("c-1", props, {"rp-1": 7})
    assert d["drs_automation_level"] == "fullyAutomated"
    assert d["total_memory_mb"] == 2048
    assert d["vm_count"] == 7
    assert d["ha_admission_control"] is False


def test_cluster_missing_and_drs_off():
    assert translate_cluster("c-2", {}, {}) == {"moref": "c-2", "name": "c-2"}
    d = translate_cluster("c-3", {"name": "c3", "configuration.drsConfig.enabled": False}, {})
    assert d["drs_automation_level"] is None
    assert d["ha_enabled"] is False
    assert d["total_hosts"] == 0 and d["total_memory_mb"] == 0 and d["vm_count"] == 0


def test_resource_pool():
    props = {
        "name": "pool",
        "config.cpuAllocation.reservation": 100,
        "config.cpuAllocation.limit": 500,
        "config.memoryAllocation.reservation": 256,
        "config.memoryAllocation.limit": 1024,
        "vm": [ref("vm-1"), ref("vm-2")],
    }
    d = translate_resource_pool("rp-1", props)
    assert d["cpu_limit_mhz"] == 500 and d["memory_reservation_mb"] == 256 and d["vm_count"] == 2
    d = translate_resource_pool("rp-2", {"name": "p"})
    assert d["cpu_reservation_mhz"] == 0 and d["cpu_limit_mhz"] == -1
    assert d["memory_limit_mb"] == -1 and d["vm_count"] == 0
    assert translate_resource_pool("rp-3", {}) == {"moref": "rp-3", "name": "rp-3"}


def test_host_networks():
    net = NS(
        vswitch=[NS(name="vSwitch0", numPorts=128, numPortsAvailable=100, mtu=1500)],
        pnic=[NS(device="vmnic0", linkSpeed=NS(speedMb=10000), mac="aa", driver="ixgben")],
        portgroup=[NS(spec=NS(name="VM Network", vlanId=0, vswitchName="vSwitch0"))],
    )
    props = {"summary.config.name": "esx1", "summary.runtime.powerState": "poweredOn", "config.network": net}
    out = translate_host_networks("host-1", props)
    assert out["host-1_vswitch_vSwitch0"]["num_ports_used"] == 28
    assert out["host-1_pnic_vmnic0"]["speed_mbps"] == 10000
    assert out["host-1_pnic_vmnic0"]["link_up"] is True
    assert out["host-1_pg_VM Network"]["name"] == "esx1 - VM Network"
    off = dict(props, **{"summary.runtime.powerState": "poweredOff"})
    assert translate_host_networks("host-1", off) == {}
    assert translate_host_networks("host-1", {"summary.runtime.powerState": "poweredOn"}) == {}


def test_dvportgroup_vlan():
    cfg = NS(name="dvpg", type="earlyBinding", numPorts=8, defaultPortConfig=NS(vlan=NS(vlanId=100)))
    d = translate_dvportgroup("dvportgroup-1", cfg, "dvs1")
    assert d["vlan_id"] == "100" and d["port_binding"] == "Static" and d["name"] == "dvs1 - dvpg"


def test_vm_storage():
    disk = NS(
        key=2000,
        deviceInfo=NS(label="Hard disk 1"),
        capacityInBytes=20 * GB,
        backing=NS(thinProvisioned=True, datastore=ref("ds-1")),
    )
    props = {
        "summary.config.name": "vm1",
        "summary.storage.committed": 5 * GB,
        "summary.storage.uncommitted": 15 * GB,
        "summary.storage.unshared": 0,
    }
    out = translate_vm_storage("vm-1", props, [disk], {"ds-1": "ds1"})
    assert out["vm-1_disk_2000"]["datastore"] == "ds1"
    assert out["vm-1_disk_2000"]["capacity_gb"] == 20.0
    assert out["vm-1_storage_summary"]["committed_gb"] == 5.0
    assert out["vm-1_storage_summary"]["unshared_gb"] == 0.0
    assert translate_vm_storage("vm-1", dict(props, **{"config.template": True}), [disk], {}) == {}
    assert translate_vm_storage("vm-1", {}, [], {}) == {}
    assert "vm-1_storage_summary" not in translate_vm_storage("vm-1", {"summary.config.name": "v"}, [], {})


def test_licenses():
    lic = NS(
        licenseKey="K1",
        name="vSphere",
        total=4,
        used=1,
        properties=[NS(key="expirationHours", value=24 * 10)],
    )
    evals = NS(licenseKey="00000-00000-00000-00000-00000", name="Eval", total=0, used=0, properties=[])
    bad = NS(licenseKey="K2", name=None, total=0, used=0, properties=[])
    out = translate_licenses([lic, evals, bad])
    assert list(out) == ["K1"]
    assert out["K1"]["status"] == "Expiring Soon" and out["K1"]["free"] == 3


def _client():
    from custom_components.vsphere.vsphere_client import VSphereClient

    return object.__new__(VSphereClient)


def test_bulk_retrieve_retries_once_on_moNotFound():
    from unittest.mock import MagicMock

    from pyVmomi import vmodl

    c = _client()
    c._bulk_retrieve_once = MagicMock(side_effect=[vmodl.fault.ManagedObjectNotFound(), {"vm-1": {}}])
    assert c._bulk_retrieve(object(), object(), []) == {"vm-1": {}}
    assert c._bulk_retrieve_once.call_count == 2


def test_bulk_retrieve_raises_after_second_moNotFound():
    from unittest.mock import MagicMock

    import pytest
    from pyVmomi import vmodl

    from custom_components.vsphere.exceptions import VSphereConnectionError

    c = _client()
    c._bulk_retrieve_once = MagicMock(side_effect=vmodl.fault.ManagedObjectNotFound())
    with pytest.raises(VSphereConnectionError):
        c._bulk_retrieve(object(), object(), [])
    assert c._bulk_retrieve_once.call_count == 2


def test_bulk_retrieve_once_propagates_call_level_fault():
    from unittest.mock import MagicMock, patch

    import pytest
    from pyVmomi import vmodl

    c = _client()
    conn = MagicMock()
    conn.RetrieveContent().propertyCollector.RetrievePropertiesEx.side_effect = vmodl.fault.ManagedObjectNotFound()
    with (
        patch("custom_components.vsphere.vsphere_client.vmodl.query.PropertyCollector.ObjectSpec"),
        patch("custom_components.vsphere.vsphere_client.vmodl.query.PropertyCollector.PropertySpec"),
        patch("custom_components.vsphere.vsphere_client.vmodl.query.PropertyCollector.FilterSpec"),
        patch("custom_components.vsphere.vsphere_client.vmodl.query.PropertyCollector.TraversalSpec"),
        patch("custom_components.vsphere.vsphere_client.vmodl.query.PropertyCollector.RetrieveOptions"),
        pytest.raises(vmodl.fault.ManagedObjectNotFound),
    ):
        c._bulk_retrieve_once(conn, object(), [])
