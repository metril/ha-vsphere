"""Base entity for vSphere Control integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry

from .const import DOMAIN
from .coordinator import VSphereData


class VSphereEntity(CoordinatorEntity[VSphereData]):
    """Base entity for vSphere devices."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
    ) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        self._entry = entry
        self._object_type = object_type
        self._moref = moref
        self._attr_device_info = self._build_device_info(entry, object_type, moref, name, coordinator.data)

    @property
    def available(self) -> bool:
        """Entity is available if its object exists in coordinator data."""
        if not self.coordinator.data:
            return False
        category_data = self.coordinator.data.get(self._object_type, {})
        return self._moref in category_data and super().available

    def _get_data(self) -> dict[str, Any] | None:
        """Get this entity's data from the coordinator."""
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get(self._object_type, {}).get(self._moref)

    @staticmethod
    def _build_device_info(
        entry: ConfigEntry,
        object_type: str,
        moref: str,
        name: str,
        data: dict[str, Any] | None,
    ) -> DeviceInfo:
        """Build DeviceInfo based on object type and hierarchy."""
        identifiers = {(DOMAIN, f"{entry.entry_id}_{moref}")}
        conn_info = (data or {}).get("connection_info", {})

        if object_type == "root":
            return DeviceInfo(
                identifiers={(DOMAIN, entry.entry_id)},
                name=entry.title,
                manufacturer="VMware",
                model=conn_info.get("type", "vSphere"),
                sw_version=conn_info.get("version"),
                configuration_url=f"https://{entry.data.get('host', '')}:{entry.data.get('port', 443)}",
            )

        if object_type == "hosts":
            obj_data = (data or {}).get("hosts", {}).get(moref, {})
            version = obj_data.get("version")
            build = obj_data.get("build")
            sw_ver = f"{version} build {build}" if version and build else version
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model="ESXi Host",
                sw_version=sw_ver,
                via_device=(DOMAIN, entry.entry_id),
            )

        if object_type == "vms":
            obj_data = (data or {}).get("vms", {}).get(moref, {})
            host_moref = obj_data.get("host_moref")
            via = (DOMAIN, f"{entry.entry_id}_{host_moref}") if host_moref else (DOMAIN, entry.entry_id)
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model=obj_data.get("guest_os", "Virtual Machine"),
                via_device=via,
            )

        if object_type == "datastores":
            obj_data = (data or {}).get("datastores", {}).get(moref, {})
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model=obj_data.get("type", "Datastore").upper(),
                via_device=(DOMAIN, entry.entry_id),
            )

        if object_type == "clusters":
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model="vSphere Cluster",
                via_device=(DOMAIN, entry.entry_id),
            )

        if object_type == "resource_pools":
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model="Resource Pool",
                via_device=(DOMAIN, entry.entry_id),
            )

        # Fallback
        return DeviceInfo(
            identifiers=identifiers,
            name=name,
            via_device=(DOMAIN, entry.entry_id),
        )
