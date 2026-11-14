"""Base entity for vSphere Control integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.helpers.entity import Entity
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import VSphereData


def is_vm_disconnected(coordinator_data: dict[str, Any], vm_moref: str) -> bool:
    """True if a VM should be treated as unreachable.

    Checks the VM's own connectionState first, then cascades to its parent
    host — vCenter does not always push a VM-level change when a host drops.
    """
    vm_data = coordinator_data.get("vms", {}).get(vm_moref)
    if vm_data is None:
        return True
    if vm_data.get("connection_state") not in (None, "", "connected"):
        return True
    host_moref = vm_data.get("host_moref")
    if not host_moref:
        return False
    host_data = coordinator_data.get("hosts", {}).get(host_moref)
    if not host_data:
        # Hosts category disabled or host not yet known — fail open.
        return False
    return host_data.get("connection_state") not in (None, "connected") or host_data.get("state") not in (
        None,
        "poweredOn",
    )


# ---------------------------------------------------------------------------
# Dynamic entity discovery (pure, unit-testable helpers)
# ---------------------------------------------------------------------------

# A discovery key identifies one unit of entity creation, e.g. ("vms", "vm-12").
SpecKey = tuple[str, str]
# (key, lazy factory) — the factory is only called for keys not yet known.
Spec = tuple[SpecKey, "Callable[[], list[Entity]]"]


def row_allowed(entity_filter: Mapping[str, Any], data_key: str, moref: str) -> bool:
    """Apply the user's entity filter (``mode: select`` restricts to listed morefs)."""
    cfg = entity_filter.get(data_key) or {}
    if cfg.get("mode", "all") != "select":
        return True
    return moref in cfg.get("morefs", ())


def iter_rows(
    data: Mapping[str, Any],
    categories: Mapping[str, bool],
    entity_filter: Mapping[str, Any],
    category: str,
    data_key: str | None = None,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (moref, row) of ``data_key`` rows when ``category`` is enabled and the filter allows them."""
    if not categories.get(category):
        return
    key = data_key or category
    for moref, row in (data.get(key) or {}).items():
        if row_allowed(entity_filter, key, moref):
            yield moref, row


def diff_keys(known: frozenset[SpecKey], current: frozenset[SpecKey]) -> tuple[frozenset[SpecKey], frozenset[SpecKey]]:
    """Return (added, removed) discovery keys."""
    return current - known, known - current


def valid_morefs(data: Mapping[str, Any], categories: Mapping[str, bool], entry_id: str) -> frozenset[str]:
    """Morefs (plus the root device id) that should currently own entities."""
    valid: set[str] = {entry_id}
    for cat in ("hosts", "vms", "datastores", "clusters", "resource_pools", "licenses", "storage_advanced"):
        if categories.get(cat):
            valid.update(data.get(cat) or ())
    if categories.get("network"):
        valid.update(data.get("networks") or ())
    if categories.get("events_alarms"):
        # Alarm entities live on host/VM morefs regardless of whether those categories are enabled
        valid.update(data.get("hosts") or ())
        valid.update(data.get("vms") or ())
    return frozenset(valid)


def is_stale_unique_id(unique_id: str, entry_id: str, valid: Iterable[str]) -> bool:
    """True if ``unique_id`` (``{entry_id}_{moref}_{key}``) references a moref not in ``valid``."""
    prefix = f"{entry_id}_"
    if not unique_id.startswith(prefix):
        return False
    remainder = unique_id[len(prefix) :]
    return not any(remainder == m or remainder.startswith(m + "_") for m in valid)


class EntityDiscovery:
    """Adds entities for newly appearing objects; runs as a coordinator listener.

    ``specs_fn`` lazily yields ``(key, factory)`` for every object that should have
    entities right now. Entity construction only happens for keys not seen before,
    so an unchanged inventory costs one key-set comparison per coordinator update.
    """

    def __init__(self, specs_fn: Callable[[], Iterable[Spec]], add_entities: AddEntitiesCallback) -> None:
        """Initialize."""
        self._specs_fn = specs_fn
        self._add_entities = add_entities
        self._known: frozenset[SpecKey] = frozenset()

    def run(self) -> None:
        """Add entities for keys never seen before.

        Keys are never dropped because a row is transiently absent (a partial
        fetch or powered-off host must not cause duplicate adds). Only
        ``forget`` — called on genuine deletions — allows re-creation.
        """
        specs = list(self._specs_fn())
        current = frozenset(key for key, _ in specs)
        added = current - self._known
        if not added:
            return
        self._known |= added
        entities: list[Entity] = []
        for key, factory in specs:
            if key in added:
                entities.extend(factory())
        if entities:
            self._add_entities(entities)

    def forget(self, moref: str) -> None:
        """Discard all keys belonging to ``moref`` so it can be re-created later."""
        self._known = frozenset(k for k in self._known if k[1] != moref)


def async_setup_discovery(
    entry: ConfigEntry,
    coordinator: VSphereData,
    add_entities: AddEntitiesCallback,
    specs_fn: Callable[[], Iterable[Spec]],
) -> None:
    """Run discovery once now and again on every coordinator update."""
    discovery = EntityDiscovery(specs_fn, add_entities)
    entry.runtime_data.discoveries.append(discovery)
    discovery.run()
    entry.async_on_unload(coordinator.async_add_listener(discovery.run))


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

    def _vm_is_disconnected(self) -> bool:
        """Whether this VM-scoped entity's VM is unreachable."""
        if not self.coordinator.data:
            return True
        return is_vm_disconnected(self.coordinator.data, self._moref)

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
            host_morefs = obj_data.get("host_morefs", [])
            # Single-host: nest under that host. Multi-host: under root.
            via = (DOMAIN, f"{entry.entry_id}_{host_morefs[0]}") if len(host_morefs) == 1 else (DOMAIN, entry.entry_id)
            return DeviceInfo(
                identifiers=identifiers,
                name=name,
                manufacturer="VMware",
                model=obj_data.get("type", "Datastore").upper(),
                via_device=via,
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


class VSphereChildEntity(VSphereEntity):
    """Entity that attaches to a parent device but reads data from a different coordinator path.

    Used for storage_advanced sensors (attached to VM device, data in coordinator["storage_advanced"])
    and network sensors (attached to Host device, data in coordinator["networks"]).
    """

    def __init__(
        self,
        coordinator: VSphereData,
        entry: ConfigEntry,
        parent_object_type: str,
        parent_moref: str,
        parent_name: str,
        data_category: str,
        data_moref: str,
    ) -> None:
        """Initialize the child entity."""
        # Use parent's object_type and moref for device attachment
        super().__init__(coordinator, entry, parent_object_type, parent_moref, parent_name)
        # Store the actual data location for _get_data()
        self._data_category = data_category
        self._data_moref = data_moref

    @property
    def available(self) -> bool:
        """Entity is available if both its parent and child data exist in coordinator."""
        if not self.coordinator.data:
            return False
        # Root-parented entities (e.g., licenses) have no parent row in coordinator data
        if self._object_type != "root":
            parent_data = self.coordinator.data.get(self._object_type, {})
            if self._moref not in parent_data:
                return False
        child_data = self.coordinator.data.get(self._data_category, {})
        return self._data_moref in child_data and CoordinatorEntity.available.fget(self)

    def _get_data(self) -> dict[str, Any] | None:
        """Get this entity's data from the child data location."""
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get(self._data_category, {}).get(self._data_moref)
