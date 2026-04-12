"""Config flow for the vSphere Control integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlowWithConfigEntry,
)
from homeassistant.helpers.selector import (
    BooleanSelector,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
    section,
)

from .const import (
    CONF_CATEGORIES,
    CONF_ENTITY_FILTER,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PERF_INTERVAL,
    CONF_PORT,
    CONF_PRIVILEGES,
    CONF_RESTRICTIONS,
    CONF_SSL_CA_PATH,
    CONF_USERNAME,
    CONF_VERIFY_SSL,
    DEFAULT_CATEGORIES,
    DEFAULT_PERF_INTERVAL,
    DEFAULT_PORT,
    DEFAULT_VERIFY_SSL,
    DOMAIN,
    FILTER_MODE_ALL,
    FILTER_MODE_SELECT,
    MAX_PERF_INTERVAL,
    MIN_PERF_INTERVAL,
    Category,
)
from .exceptions import VSphereAuthError, VSphereConnectionError
from .vsphere_client import VSphereClient

_LOGGER = logging.getLogger(__name__)

# Categories that support per-object entity filtering
_FILTERABLE_CATEGORIES: list[Category] = [
    Category.HOSTS,
    Category.VMS,
    Category.DATASTORES,
    Category.CLUSTERS,
    Category.RESOURCE_POOLS,
]

_CATEGORY_DISPLAY_NAMES: dict[Category, str] = {
    Category.HOSTS: "Hosts",
    Category.VMS: "Virtual Machines",
    Category.DATASTORES: "Datastores",
    Category.CLUSTERS: "Clusters",
    Category.RESOURCE_POOLS: "Resource Pools",
}

_CORE_CATEGORIES: list[Category] = [
    Category.HOSTS,
    Category.VMS,
    Category.DATASTORES,
    Category.LICENSES,
]

_ADVANCED_CATEGORIES: list[Category] = [
    Category.CLUSTERS,
    Category.NETWORK,
    Category.RESOURCE_POOLS,
    Category.STORAGE_ADVANCED,
    Category.PERFORMANCE,
    Category.EVENTS_ALARMS,
]


def _connection_schema(
    defaults: dict[str, Any] | None = None,
) -> vol.Schema:
    """Return the connection step schema with optional defaults."""
    d = defaults or {}
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=d.get(CONF_HOST, "")): TextSelector(
                TextSelectorConfig(type=TextSelectorType.TEXT)
            ),
            vol.Required(CONF_PORT, default=d.get(CONF_PORT, DEFAULT_PORT)): int,
            vol.Required(CONF_USERNAME, default=d.get(CONF_USERNAME, "")): TextSelector(
                TextSelectorConfig(type=TextSelectorType.TEXT)
            ),
            vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
            vol.Required("ssl_options"): section(
                vol.Schema(
                    {
                        vol.Required(
                            CONF_VERIFY_SSL,
                            default=d.get(CONF_VERIFY_SSL, DEFAULT_VERIFY_SSL),
                        ): BooleanSelector(),
                        vol.Optional(
                            CONF_SSL_CA_PATH,
                            default=d.get(CONF_SSL_CA_PATH, ""),
                        ): TextSelector(TextSelectorConfig(type=TextSelectorType.TEXT)),
                    }
                ),
                {"collapsed": True},
            ),
        }
    )


def _categories_schema(
    defaults: dict[str, bool] | None = None,
    perf_interval: int = DEFAULT_PERF_INTERVAL,
) -> vol.Schema:
    """Return the categories step schema with core and advanced sections."""
    effective = dict(DEFAULT_CATEGORIES)
    if defaults:
        effective.update(defaults)

    core_schema = vol.Schema(
        {
            vol.Required(cat.value, default=effective.get(cat.value, False)): BooleanSelector()
            for cat in _CORE_CATEGORIES
        }
    )

    advanced_fields: dict[Any, Any] = {
        vol.Required(cat.value, default=effective.get(cat.value, False)): BooleanSelector()
        for cat in _ADVANCED_CATEGORIES
    }
    advanced_fields[vol.Required(CONF_PERF_INTERVAL, default=perf_interval)] = NumberSelector(
        NumberSelectorConfig(
            min=MIN_PERF_INTERVAL,
            max=MAX_PERF_INTERVAL,
            step=30,
            mode=NumberSelectorMode.SLIDER,
            unit_of_measurement="seconds",
        )
    )
    advanced_schema = vol.Schema(advanced_fields)

    return vol.Schema(
        {
            vol.Required("core_categories"): section(core_schema, {"collapsed": False}),
            vol.Required("advanced_categories"): section(advanced_schema, {"collapsed": True}),
        }
    )


def _restrictions_schema(
    current_restrictions: dict[str, Any] | None = None,
) -> vol.Schema:
    """Return the global restrictions step schema."""
    global_restrictions: dict[str, Any] = (current_restrictions or {}).get("global", {})
    return vol.Schema(
        {
            vol.Required(
                "block_destructive",
                default=global_restrictions.get("destructive", False),
            ): BooleanSelector(),
            vol.Required(
                "block_snapshots",
                default=global_restrictions.get("snapshots", False),
            ): BooleanSelector(),
            vol.Required(
                "block_migrate",
                default=global_restrictions.get("migrate", False),
            ): BooleanSelector(),
            vol.Required(
                "block_host_ops",
                default=global_restrictions.get("host_ops", False),
            ): BooleanSelector(),
        }
    )


def _flatten_ssl_section(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten the ssl_options section from user_input."""
    ssl_opts = user_input.pop("ssl_options", {})
    user_input.update(ssl_opts)
    return user_input


def _flatten_category_sections(user_input: dict[str, Any]) -> tuple[dict[str, bool], int]:
    """Flatten core/advanced sections and return categories dict and perf_interval."""
    core = user_input.pop("core_categories", {})
    advanced = user_input.pop("advanced_categories", {})
    merged = {**core, **advanced}
    categories = {cat.value: merged.get(cat.value, False) for cat in Category}
    perf_interval = int(merged.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL))
    return categories, perf_interval


class VSphereConfigFlow(ConfigFlow, domain=DOMAIN):
    """Multi-step config flow for vSphere Control."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._connection_data: dict[str, Any] = {}
        self._categories: dict[str, bool] = {}
        self._entity_filter: dict[str, Any] = {}
        self._inventory: dict[str, dict[str, Any]] = {}
        self._filterable_remaining: list[Category] = []
        self._current_filter_category: Category | None = None
        self._privileges: dict[str, bool] = {}
        self._restrictions: dict[str, Any] = {}
        self._perf_interval: int = DEFAULT_PERF_INTERVAL

    # ------------------------------------------------------------------
    # Step 1: user -- connection details
    # ------------------------------------------------------------------

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle the initial connection step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Flatten SSL section into a copy (preserve original for form re-render)
            flat_input = dict(user_input)
            _flatten_ssl_section(flat_input)

            errors = await self._test_connection(flat_input)
            if not errors:
                # Set unique ID only after successful connection test
                host = flat_input[CONF_HOST]
                port = flat_input[CONF_PORT]
                await self.async_set_unique_id(f"{host}:{port}")
                self._abort_if_unique_id_configured()

                self._connection_data = flat_input
                return await self.async_step_categories()

        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(user_input),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Step 2: categories
    # ------------------------------------------------------------------

    async def async_step_categories(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle monitoring category selection."""
        if user_input is not None:
            self._categories, self._perf_interval = _flatten_category_sections(user_input)
            return await self._start_entity_selection()

        return self.async_show_form(
            step_id="categories",
            data_schema=_categories_schema(),
        )

    # ------------------------------------------------------------------
    # Step 3: entity_selection (iterated per filterable category)
    # ------------------------------------------------------------------

    async def _start_entity_selection(self) -> ConfigFlowResult:
        """Kick off entity selection for the first enabled filterable category."""
        # Enumerate inventory once
        client = VSphereClient(
            host=self._connection_data[CONF_HOST],
            port=self._connection_data[CONF_PORT],
            username=self._connection_data[CONF_USERNAME],
            password=self._connection_data[CONF_PASSWORD],
            verify_ssl=self._connection_data[CONF_VERIFY_SSL],
            ssl_ca_path=self._connection_data.get(CONF_SSL_CA_PATH, ""),
        )
        try:
            self._inventory = await self.hass.async_add_executor_job(client.enumerate_inventory)
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Could not enumerate inventory; skipping entity selection")
            self._inventory = {}

        # Build list of filterable categories that are enabled
        self._filterable_remaining = [cat for cat in _FILTERABLE_CATEGORIES if self._categories.get(cat.value, False)]
        return await self._next_entity_selection_step()

    async def _next_entity_selection_step(self) -> ConfigFlowResult:
        """Advance to the entity selection for the next category, or move on."""
        if not self._filterable_remaining:
            return await self.async_step_setup_restrictions()

        self._current_filter_category = self._filterable_remaining.pop(0)
        return await self.async_step_entity_selection()

    async def async_step_entity_selection(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle entity selection for the current category."""
        category = self._current_filter_category
        if category is None:
            return await self.async_step_setup_restrictions()

        if user_input is not None:
            selected: list[str] = user_input.get("selected_objects", [])
            if not selected:
                self._entity_filter[category.value] = {"mode": FILTER_MODE_ALL}
            else:
                self._entity_filter[category.value] = {
                    "mode": FILTER_MODE_SELECT,
                    "morefs": selected,
                }
            return await self._next_entity_selection_step()

        # Build options list from inventory for this category
        type_map: dict[Category, str] = {
            Category.HOSTS: "host",
            Category.VMS: "vm",
            Category.DATASTORES: "datastore",
            Category.CLUSTERS: "cluster",
            Category.RESOURCE_POOLS: "resource_pool",
        }
        obj_type = type_map.get(category, "")
        options: list[SelectOptionDict] = [
            SelectOptionDict(value=moref, label=info.get("name", moref))
            for moref, info in self._inventory.items()
            if info.get("type") == obj_type
        ]

        data_schema = vol.Schema(
            {
                vol.Optional("selected_objects", default=[]): SelectSelector(
                    SelectSelectorConfig(
                        options=options,
                        multiple=True,
                        mode=SelectSelectorMode.DROPDOWN,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="entity_selection",
            data_schema=data_schema,
            description_placeholders={"category": _CATEGORY_DISPLAY_NAMES.get(category, category.value)},
        )

    # ------------------------------------------------------------------
    # Step 4: setup_restrictions -- ask whether to configure restrictions
    # ------------------------------------------------------------------

    async def async_step_setup_restrictions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Ask whether to configure operation restrictions during initial setup."""
        if user_input is not None:
            if user_input.get("configure_restrictions"):
                return await self.async_step_restrictions()
            return self._create_entry()

        return self.async_show_form(
            step_id="setup_restrictions",
            data_schema=vol.Schema(
                {
                    vol.Required("configure_restrictions", default=False): BooleanSelector(),
                }
            ),
        )

    # ------------------------------------------------------------------
    # Step 5: restrictions -- global restriction shortcuts
    # ------------------------------------------------------------------

    async def async_step_restrictions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle global operation restriction shortcuts."""
        if user_input is not None:
            self._restrictions["global"] = {
                "destructive": user_input.get("block_destructive", False),
                "snapshots": user_input.get("block_snapshots", False),
                "migrate": user_input.get("block_migrate", False),
                "host_ops": user_input.get("block_host_ops", False),
            }
            return await self.async_step_object_restrictions()

        return self.async_show_form(
            step_id="restrictions",
            data_schema=_restrictions_schema(self._restrictions),
        )

    # ------------------------------------------------------------------
    # Step 6: object_restrictions -- per-host/VM action restrictions
    # ------------------------------------------------------------------

    async def async_step_object_restrictions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle per-object operation restrictions."""
        if user_input is not None:
            vm_restrictions: dict[str, dict[str, bool]] = {}
            for vm_moref in user_input.get("restricted_vms", []):
                vm_restrictions[vm_moref] = {a: True for a in user_input.get("vm_blocked_actions", [])}

            host_restrictions: dict[str, dict[str, bool]] = {}
            for host_moref in user_input.get("restricted_hosts", []):
                host_restrictions[host_moref] = {a: True for a in user_input.get("host_blocked_actions", [])}

            self._restrictions["vms"] = vm_restrictions
            self._restrictions["hosts"] = host_restrictions

            return self._create_entry()

        # Build VM and host options from inventory
        from .const import HostAction, VmAction  # noqa: PLC0415

        vm_options: list[SelectOptionDict] = [
            SelectOptionDict(value=moref, label=info.get("name", moref))
            for moref, info in self._inventory.items()
            if info.get("type") == "vm"
        ]
        host_options: list[SelectOptionDict] = [
            SelectOptionDict(value=moref, label=info.get("name", moref))
            for moref, info in self._inventory.items()
            if info.get("type") == "host"
        ]

        if not vm_options and not host_options:
            return self._create_entry()

        vm_action_options = [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in VmAction]
        host_action_options = [
            SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in HostAction
        ]

        schema_fields: dict[Any, Any] = {}

        if vm_options:
            current_restricted_vms = list(self._restrictions.get("vms", {}).keys())
            schema_fields[vol.Optional("restricted_vms", default=current_restricted_vms)] = SelectSelector(
                SelectSelectorConfig(options=vm_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )
            schema_fields[vol.Optional("vm_blocked_actions", default=[])] = SelectSelector(
                SelectSelectorConfig(options=vm_action_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )

        if host_options:
            current_restricted_hosts = list(self._restrictions.get("hosts", {}).keys())
            schema_fields[vol.Optional("restricted_hosts", default=current_restricted_hosts)] = SelectSelector(
                SelectSelectorConfig(options=host_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )
            schema_fields[vol.Optional("host_blocked_actions", default=[])] = SelectSelector(
                SelectSelectorConfig(options=host_action_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )

        return self.async_show_form(
            step_id="object_restrictions",
            data_schema=vol.Schema(schema_fields),
        )

    # ------------------------------------------------------------------
    # Reauth flow
    # ------------------------------------------------------------------

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        """Handle re-authentication."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle re-auth confirmation step."""
        errors: dict[str, str] = {}

        reauth_entry = self._get_reauth_entry()
        existing_data = dict(reauth_entry.data)

        if user_input is not None:
            candidate = {**existing_data, **user_input}
            errors = await self._test_connection(candidate)
            if not errors:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data={**existing_data, **user_input},
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_USERNAME, default=existing_data.get(CONF_USERNAME, "")): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.TEXT)
                    ),
                    vol.Required(CONF_PASSWORD): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
                }
            ),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Reconfigure flow
    # ------------------------------------------------------------------

    async def async_step_reconfigure(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle reconfiguration of connection details."""
        errors: dict[str, str] = {}

        reconfigure_entry = self._get_reconfigure_entry()
        existing_data = dict(reconfigure_entry.data)

        if user_input is not None:
            # Flatten SSL section into a copy (preserve original for form re-render)
            flat_input = dict(user_input)
            _flatten_ssl_section(flat_input)

            errors = await self._test_connection(flat_input)
            if not errors:
                new_unique_id = f"{flat_input[CONF_HOST]}:{flat_input[CONF_PORT]}"
                await self.async_set_unique_id(new_unique_id)
                self._abort_if_unique_id_configured(updates={**existing_data, **flat_input})
                return self.async_update_reload_and_abort(
                    reconfigure_entry,
                    data={**existing_data, **flat_input},
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_connection_schema(existing_data),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Options flow
    # ------------------------------------------------------------------

    @staticmethod
    def async_get_options_flow(config_entry: ConfigEntry) -> VSphereOptionsFlow:
        """Return the options flow handler."""
        return VSphereOptionsFlow(config_entry)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _test_connection(self, data: dict[str, Any]) -> dict[str, str]:
        """Test vSphere connection and check privileges; return error dict (empty on success)."""
        # Validate CA file path if provided
        ca_path = data.get(CONF_SSL_CA_PATH, "")
        if ca_path:
            from pathlib import Path  # noqa: PLC0415

            if not Path(ca_path).is_file():
                return {"base": "ca_not_found"}

        client = VSphereClient(
            host=data[CONF_HOST],
            port=data[CONF_PORT],
            username=data[CONF_USERNAME],
            password=data[CONF_PASSWORD],
            verify_ssl=data[CONF_VERIFY_SSL],
            ssl_ca_path=data.get(CONF_SSL_CA_PATH, ""),
        )
        try:
            await self.hass.async_add_executor_job(client.test_connection)
        except VSphereAuthError:
            return {"base": "invalid_auth"}
        except VSphereConnectionError:
            return {"base": "cannot_connect"}
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Unexpected error testing vSphere connection")
            return {"base": "unknown"}

        # Check account privileges
        try:
            self._privileges = await self.hass.async_add_executor_job(client.check_privileges)
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Privilege check failed, assuming full access")
            self._privileges = {}

        return {}

    def _create_entry(self) -> ConfigFlowResult:
        """Create the config entry with collected data."""
        host = self._connection_data[CONF_HOST]

        options: dict[str, Any] = {
            CONF_CATEGORIES: self._categories,
            CONF_ENTITY_FILTER: self._entity_filter,
            CONF_RESTRICTIONS: self._restrictions,
            CONF_PERF_INTERVAL: self._perf_interval,
            CONF_PRIVILEGES: self._privileges,
        }

        return self.async_create_entry(
            title=host,
            data=self._connection_data,
            options=options,
        )


class VSphereOptionsFlow(OptionsFlowWithConfigEntry):
    """Options flow for vSphere Control."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize the options flow."""
        super().__init__(config_entry)
        self._new_categories: dict[str, bool] = {}
        self._new_perf_interval: int = DEFAULT_PERF_INTERVAL
        self._inventory: dict[str, dict[str, Any]] = {}
        self._filterable_remaining: list[Category] = []
        self._current_filter_category: Category | None = None
        self._entity_filter: dict[str, Any] = {}
        self._restrictions: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Step 1: init -- category toggles (sectioned) + perf interval
    # ------------------------------------------------------------------

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle the options flow entry point."""
        current_options = dict(self.config_entry.options)
        current_categories: dict[str, bool] = current_options.get(CONF_CATEGORIES, dict(DEFAULT_CATEGORIES))
        current_perf_interval: int = current_options.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL)

        if user_input is not None:
            self._new_categories, self._new_perf_interval = _flatten_category_sections(user_input)
            # Preserve existing entity filter, will be updated in entity_selection step
            self._entity_filter = dict(current_options.get(CONF_ENTITY_FILTER, {}))
            return await self._start_entity_selection()

        return self.async_show_form(
            step_id="init",
            data_schema=_categories_schema(current_categories, current_perf_interval),
        )

    # ------------------------------------------------------------------
    # Step 2: entity_selection -- per-category object selection
    # ------------------------------------------------------------------

    async def _start_entity_selection(self) -> ConfigFlowResult:
        """Enumerate inventory and start per-category entity selection."""
        entry_data: dict[str, Any] = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id, {})
        client = entry_data.get("client")
        if client is not None:
            try:
                self._inventory = await self.hass.async_add_executor_job(client.enumerate_inventory)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Could not enumerate inventory in options flow; skipping entity selection")
                self._inventory = {}
        else:
            self._inventory = {}

        # Only iterate categories that are newly enabled
        self._filterable_remaining = [
            cat for cat in _FILTERABLE_CATEGORIES if self._new_categories.get(cat.value, False)
        ]
        return await self._next_entity_selection_step()

    async def _next_entity_selection_step(self) -> ConfigFlowResult:
        """Advance to the next filterable category, or move to restrictions."""
        if not self._filterable_remaining:
            return await self.async_step_restrictions()

        self._current_filter_category = self._filterable_remaining.pop(0)
        return await self.async_step_entity_selection()

    async def async_step_entity_selection(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle entity selection for the current filterable category."""
        category = self._current_filter_category
        if category is None:
            return await self.async_step_restrictions()

        if user_input is not None:
            selected: list[str] = user_input.get("selected_objects", [])
            if not selected:
                self._entity_filter[category.value] = {"mode": FILTER_MODE_ALL}
            else:
                self._entity_filter[category.value] = {
                    "mode": FILTER_MODE_SELECT,
                    "morefs": selected,
                }
            return await self._next_entity_selection_step()

        type_map: dict[Category, str] = {
            Category.HOSTS: "host",
            Category.VMS: "vm",
            Category.DATASTORES: "datastore",
            Category.CLUSTERS: "cluster",
            Category.RESOURCE_POOLS: "resource_pool",
        }
        obj_type = type_map.get(category, "")
        options: list[SelectOptionDict] = [
            SelectOptionDict(value=moref, label=info.get("name", moref))
            for moref, info in self._inventory.items()
            if info.get("type") == obj_type
        ]

        # Determine existing defaults for this category
        existing_filter = self._entity_filter.get(category.value, {})
        default_selected = existing_filter.get("morefs", [])

        data_schema = vol.Schema(
            {
                vol.Optional("selected_objects", default=default_selected): SelectSelector(
                    SelectSelectorConfig(
                        options=options,
                        multiple=True,
                        mode=SelectSelectorMode.DROPDOWN,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="entity_selection",
            data_schema=data_schema,
            description_placeholders={"category": _CATEGORY_DISPLAY_NAMES.get(category, category.value)},
        )

    # ------------------------------------------------------------------
    # Step 3: restrictions -- global restriction shortcuts
    # ------------------------------------------------------------------

    async def async_step_restrictions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle global operation restriction shortcuts."""
        current_options = dict(self.config_entry.options)
        current_restrictions: dict[str, Any] = current_options.get(CONF_RESTRICTIONS, {})

        if user_input is not None:
            # Store global restrictions, preserve existing per-object restrictions
            self._restrictions = dict(current_restrictions)
            self._restrictions["global"] = {
                "destructive": user_input.get("block_destructive", False),
                "snapshots": user_input.get("block_snapshots", False),
                "migrate": user_input.get("block_migrate", False),
                "host_ops": user_input.get("block_host_ops", False),
            }
            # Move to per-object restrictions
            return await self.async_step_object_restrictions()

        return self.async_show_form(
            step_id="restrictions",
            data_schema=_restrictions_schema(current_restrictions),
        )

    # ------------------------------------------------------------------
    # Step 4: object_restrictions -- per-host/VM action restrictions
    # ------------------------------------------------------------------

    async def async_step_object_restrictions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle per-object operation restrictions."""
        current_options = dict(self.config_entry.options)

        if user_input is not None:
            vm_restrictions: dict[str, dict[str, bool]] = {}
            for vm_moref in user_input.get("restricted_vms", []):
                vm_restrictions[vm_moref] = {a: True for a in user_input.get("vm_blocked_actions", [])}

            host_restrictions: dict[str, dict[str, bool]] = {}
            for host_moref in user_input.get("restricted_hosts", []):
                host_restrictions[host_moref] = {a: True for a in user_input.get("host_blocked_actions", [])}

            self._restrictions["vms"] = vm_restrictions
            self._restrictions["hosts"] = host_restrictions

            return self.async_create_entry(
                data={
                    **current_options,
                    CONF_CATEGORIES: self._new_categories,
                    CONF_PERF_INTERVAL: self._new_perf_interval,
                    CONF_ENTITY_FILTER: self._entity_filter,
                    CONF_RESTRICTIONS: self._restrictions,
                }
            )

        # Build VM and host options from inventory or coordinator data
        entry_data = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id, {})
        coordinator = entry_data.get("coordinator")

        vm_options: list[SelectOptionDict] = []
        host_options: list[SelectOptionDict] = []
        if coordinator and coordinator.data:
            for moref, data in coordinator.data.get("vms", {}).items():
                vm_options.append(SelectOptionDict(value=moref, label=data.get("name", moref)))
            for moref, data in coordinator.data.get("hosts", {}).items():
                host_options.append(SelectOptionDict(value=moref, label=data.get("name", moref)))

        # Current per-object restrictions for defaults
        current_restricted_vms = list(self._restrictions.get("vms", {}).keys())
        current_restricted_hosts = list(self._restrictions.get("hosts", {}).keys())

        # Determine currently blocked actions (use first restricted object as default)
        current_vm_actions: list[str] = []
        if current_restricted_vms:
            first_vm = self._restrictions.get("vms", {}).get(current_restricted_vms[0], {})
            current_vm_actions = [k for k, v in first_vm.items() if v and k != "_all"]

        current_host_actions: list[str] = []
        if current_restricted_hosts:
            first_host = self._restrictions.get("hosts", {}).get(current_restricted_hosts[0], {})
            current_host_actions = [k for k, v in first_host.items() if v and k != "_all"]

        from .const import HostAction, VmAction  # noqa: PLC0415

        vm_action_options = [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in VmAction]
        host_action_options = [
            SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in HostAction
        ]

        schema_fields: dict[Any, Any] = {}

        if vm_options:
            schema_fields[vol.Optional("restricted_vms", default=current_restricted_vms)] = SelectSelector(
                SelectSelectorConfig(options=vm_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )
            schema_fields[vol.Optional("vm_blocked_actions", default=current_vm_actions)] = SelectSelector(
                SelectSelectorConfig(options=vm_action_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )

        if host_options:
            schema_fields[vol.Optional("restricted_hosts", default=current_restricted_hosts)] = SelectSelector(
                SelectSelectorConfig(options=host_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )
            schema_fields[vol.Optional("host_blocked_actions", default=current_host_actions)] = SelectSelector(
                SelectSelectorConfig(options=host_action_options, multiple=True, mode=SelectSelectorMode.DROPDOWN)
            )

        if not schema_fields:
            # No hosts or VMs available -- skip to save
            return self.async_create_entry(
                data={
                    **current_options,
                    CONF_CATEGORIES: self._new_categories,
                    CONF_PERF_INTERVAL: self._new_perf_interval,
                    CONF_ENTITY_FILTER: self._entity_filter,
                    CONF_RESTRICTIONS: self._restrictions,
                }
            )

        return self.async_show_form(
            step_id="object_restrictions",
            data_schema=vol.Schema(schema_fields),
        )
