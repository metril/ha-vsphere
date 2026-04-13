"""Config flow for the vSphere Control integration."""

from __future__ import annotations

import copy
import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlowWithConfigEntry,
)
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    BooleanSelector,
    DurationSelector,
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
)

from .const import (
    CONF_CATEGORIES,
    CONF_ENTITY_FILTER,
    CONF_FORCE_ARM_TIMEOUT,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PERF_INTERVAL,
    CONF_PORT,
    CONF_RESTRICTIONS,
    CONF_SSL_CA_PATH,
    CONF_USERNAME,
    CONF_VERIFY_SSL,
    DEFAULT_CATEGORIES,
    DEFAULT_FORCE_ARM_TIMEOUT,
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


def _seconds_to_duration(seconds: int) -> dict[str, int]:
    """Convert seconds to a duration dict for DurationSelector."""
    return {"hours": seconds // 3600, "minutes": (seconds % 3600) // 60, "seconds": seconds % 60}


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
    advanced_fields[vol.Required(CONF_PERF_INTERVAL, default=_seconds_to_duration(perf_interval))] = DurationSelector()
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
    force_arm_timeout = (current_restrictions or {}).get(CONF_FORCE_ARM_TIMEOUT, DEFAULT_FORCE_ARM_TIMEOUT)
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
            vol.Required(
                CONF_FORCE_ARM_TIMEOUT,
                default=force_arm_timeout,
            ): NumberSelector(
                NumberSelectorConfig(
                    min=10, max=300, step=5, mode=NumberSelectorMode.BOX, unit_of_measurement="seconds"
                )
            ),
        }
    )


def _flatten_ssl_section(user_input: dict[str, Any]) -> dict[str, Any]:
    """Flatten the ssl_options section from user_input."""
    ssl_opts = user_input.pop("ssl_options", {})
    user_input.update(ssl_opts)
    return user_input


def _flatten_category_sections(user_input: dict[str, Any]) -> tuple[dict[str, bool], int]:
    """Flatten core/advanced sections and return (categories, perf_interval)."""
    core = user_input.pop("core_categories", {})
    advanced = user_input.pop("advanced_categories", {})
    merged = {**core, **advanced}
    categories = {cat.value: merged.get(cat.value, False) for cat in Category}
    raw_interval = merged.get(CONF_PERF_INTERVAL, {})
    if isinstance(raw_interval, dict):
        # DurationSelector returns {"hours": h, "minutes": m, "seconds": s}
        perf_interval = (
            int(raw_interval.get("hours", 0)) * 3600
            + int(raw_interval.get("minutes", 0)) * 60
            + int(raw_interval.get("seconds", 0))
        )
    else:
        perf_interval = int(raw_interval)
    perf_interval = max(MIN_PERF_INTERVAL, min(MAX_PERF_INTERVAL, perf_interval))
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
        self._restrictions: dict[str, Any] = {}
        self._perf_interval: int = DEFAULT_PERF_INTERVAL
        self._current_vm_moref: str | None = None
        self._current_host_moref: str | None = None

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

        # Flatten SSL section for re-render defaults so SSL field values are preserved
        render_defaults = None
        if user_input is not None:
            render_defaults = dict(user_input)
            ssl_opts = render_defaults.pop("ssl_options", {})
            if isinstance(ssl_opts, dict):
                render_defaults.update(ssl_opts)

        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(render_defaults),
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
        options: list[SelectOptionDict] = sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == obj_type
            ],
            key=lambda x: x["label"],
        )

        data_schema = vol.Schema(
            {
                vol.Optional("selected_objects", default=[]): SelectSelector(
                    SelectSelectorConfig(
                        options=options,
                        multiple=True,
                        mode=SelectSelectorMode.LIST,
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
                return await self.async_step_restrictions_menu()
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
    # Restrictions menu
    # ------------------------------------------------------------------

    async def async_step_restrictions_menu(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Show the restrictions sub-menu."""
        return self.async_show_menu(
            step_id="restrictions_menu",
            menu_options=["restrictions", "vm_select", "host_select", "restrictions_done"],
        )

    async def async_step_restrictions_done(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Finish restrictions and create the config entry."""
        return self._create_entry()

    # ------------------------------------------------------------------
    # Global restrictions
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
            self._restrictions[CONF_FORCE_ARM_TIMEOUT] = int(
                user_input.get(CONF_FORCE_ARM_TIMEOUT, DEFAULT_FORCE_ARM_TIMEOUT)
            )
            return await self.async_step_restrictions_menu()

        return self.async_show_form(
            step_id="restrictions",
            data_schema=_restrictions_schema(self._restrictions),
        )

    # ------------------------------------------------------------------
    # Per-VM restrictions (select → actions → another loop)
    # ------------------------------------------------------------------

    def _vm_options(self) -> list[SelectOptionDict]:
        """Return sorted VM options from inventory."""
        return sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == "vm"
            ],
            key=lambda x: x["label"],
        )

    def _host_options(self) -> list[SelectOptionDict]:
        """Return sorted host options from inventory."""
        return sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == "host"
            ],
            key=lambda x: x["label"],
        )

    def _obj_name(self, moref: str) -> str:
        """Get the display name for a moref from inventory."""
        return self._inventory.get(moref, {}).get("name", moref)

    async def async_step_vm_select(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select a single VM to configure restrictions for."""
        vm_opts = self._vm_options()
        if not vm_opts:
            return await self.async_step_restrictions_menu()

        if user_input is not None:
            self._current_vm_moref = user_input["vm_to_restrict"]
            return await self.async_step_vm_actions()

        return self.async_show_form(
            step_id="vm_select",
            data_schema=vol.Schema(
                {
                    vol.Required("vm_to_restrict"): SelectSelector(
                        SelectSelectorConfig(options=vm_opts, multiple=False, mode=SelectSelectorMode.DROPDOWN)
                    ),
                }
            ),
        )

    async def async_step_vm_actions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select which actions to block on the chosen VM."""
        from .const import VmAction  # noqa: PLC0415

        moref = self._current_vm_moref or ""
        name = self._obj_name(moref)

        if user_input is not None:
            actions = user_input.get("vm_blocked_actions", [])
            if actions:
                self._restrictions.setdefault("vms", {})[moref] = {a: True for a in actions}
            else:
                self._restrictions.get("vms", {}).pop(moref, None)
            self._current_vm_moref = None
            return await self.async_step_restrictions_menu()

        current_actions = [k for k, v in self._restrictions.get("vms", {}).get(moref, {}).items() if v and k != "_all"]
        vm_action_options: list[SelectOptionDict] = sorted(
            [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in VmAction],
            key=lambda x: x["label"],
        )

        return self.async_show_form(
            step_id="vm_actions",
            data_schema=vol.Schema(
                {
                    vol.Optional("vm_blocked_actions", default=current_actions): SelectSelector(
                        SelectSelectorConfig(options=vm_action_options, multiple=True, mode=SelectSelectorMode.LIST)
                    ),
                }
            ),
            description_placeholders={"vm_name": name},
        )

    # ------------------------------------------------------------------
    # Per-host restrictions (select → actions → back to menu)
    # ------------------------------------------------------------------

    async def async_step_host_select(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select a single host to configure restrictions for."""
        host_opts = self._host_options()
        if not host_opts:
            return await self.async_step_restrictions_menu()

        if user_input is not None:
            self._current_host_moref = user_input["host_to_restrict"]
            return await self.async_step_host_actions()

        return self.async_show_form(
            step_id="host_select",
            data_schema=vol.Schema(
                {
                    vol.Required("host_to_restrict"): SelectSelector(
                        SelectSelectorConfig(options=host_opts, multiple=False, mode=SelectSelectorMode.DROPDOWN)
                    ),
                }
            ),
        )

    async def async_step_host_actions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select which actions to block on the chosen host."""
        from .const import HostAction  # noqa: PLC0415

        moref = self._current_host_moref or ""
        name = self._obj_name(moref)

        if user_input is not None:
            actions = user_input.get("host_blocked_actions", [])
            if actions:
                self._restrictions.setdefault("hosts", {})[moref] = {a: True for a in actions}
            else:
                self._restrictions.get("hosts", {}).pop(moref, None)
            self._current_host_moref = None
            return await self.async_step_restrictions_menu()

        current_actions = [
            k for k, v in self._restrictions.get("hosts", {}).get(moref, {}).items() if v and k != "_all"
        ]
        host_action_options: list[SelectOptionDict] = sorted(
            [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in HostAction],
            key=lambda x: x["label"],
        )

        return self.async_show_form(
            step_id="host_actions",
            data_schema=vol.Schema(
                {
                    vol.Optional("host_blocked_actions", default=current_actions): SelectSelector(
                        SelectSelectorConfig(options=host_action_options, multiple=True, mode=SelectSelectorMode.LIST)
                    ),
                }
            ),
            description_placeholders={"host_name": name},
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
        """Test vSphere connection; return error dict (empty on success)."""
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

        return {}

    def _create_entry(self) -> ConfigFlowResult:
        """Create the config entry with collected data."""
        host = self._connection_data[CONF_HOST]

        options: dict[str, Any] = {
            CONF_CATEGORIES: self._categories,
            CONF_ENTITY_FILTER: self._entity_filter,
            CONF_RESTRICTIONS: self._restrictions,
            CONF_PERF_INTERVAL: self._perf_interval,
        }

        return self.async_create_entry(
            title=host,
            data=self._connection_data,
            options=options,
        )


class VSphereOptionsFlow(OptionsFlowWithConfigEntry):
    """Menu-driven options flow for vSphere Control."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize the options flow — load all current options upfront."""
        super().__init__(config_entry)
        current = dict(config_entry.options)
        self._new_categories: dict[str, bool] = dict(current.get(CONF_CATEGORIES, DEFAULT_CATEGORIES))
        self._new_perf_interval: int = current.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL)
        self._entity_filter: dict[str, Any] = dict(current.get(CONF_ENTITY_FILTER, {}))
        self._restrictions: dict[str, Any] = copy.deepcopy(current.get(CONF_RESTRICTIONS, {}))
        self._inventory: dict[str, dict[str, Any]] = {}
        self._inventory_loaded: bool = False
        self._filterable_remaining: list[Category] = []
        self._current_filter_category: Category | None = None
        self._current_vm_moref: str | None = None
        self._current_host_moref: str | None = None

    async def _ensure_inventory(self) -> None:
        """Load inventory once (lazy, shared across all menu sections)."""
        if self._inventory_loaded:
            return
        entry_data: dict[str, Any] = self.hass.data.get(DOMAIN, {}).get(self.config_entry.entry_id, {})
        client = entry_data.get("client")
        if client is not None:
            try:
                self._inventory = await self.hass.async_add_executor_job(client.enumerate_inventory)
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Could not enumerate inventory in options flow")
                self._inventory = {}
        self._inventory_loaded = True

    def _vm_options(self) -> list[SelectOptionDict]:
        """Return sorted VM options from inventory."""
        return sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == "vm"
            ],
            key=lambda x: x["label"],
        )

    def _host_options(self) -> list[SelectOptionDict]:
        """Return sorted host options from inventory."""
        return sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == "host"
            ],
            key=lambda x: x["label"],
        )

    def _obj_name(self, moref: str) -> str:
        """Get the display name for a moref from inventory."""
        info = self._inventory.get(moref, {})
        return info.get("name", moref)

    # ------------------------------------------------------------------
    # Main menu
    # ------------------------------------------------------------------

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Show the top-level options menu."""
        return self.async_show_menu(
            step_id="init",
            menu_options=["categories", "entity_selection_start", "restrictions_menu", "save"],
        )

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------

    async def async_step_categories(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle monitoring category selection."""
        if user_input is not None:
            self._new_categories, self._new_perf_interval = _flatten_category_sections(user_input)
            return await self.async_step_init()

        return self.async_show_form(
            step_id="categories",
            data_schema=_categories_schema(self._new_categories, self._new_perf_interval),
        )

    # ------------------------------------------------------------------
    # Entity selection
    # ------------------------------------------------------------------

    async def async_step_entity_selection_start(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Kick off entity selection — enumerate inventory, then loop categories."""
        await self._ensure_inventory()
        self._filterable_remaining = [
            cat for cat in _FILTERABLE_CATEGORIES if self._new_categories.get(cat.value, False)
        ]
        return await self._next_entity_selection_step()

    async def _next_entity_selection_step(self) -> ConfigFlowResult:
        """Advance to the next filterable category, or return to menu."""
        if not self._filterable_remaining:
            return await self.async_step_init()
        self._current_filter_category = self._filterable_remaining.pop(0)
        return await self.async_step_entity_selection()

    async def async_step_entity_selection(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle entity selection for the current filterable category."""
        category = self._current_filter_category
        if category is None:
            return await self.async_step_init()

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
        options: list[SelectOptionDict] = sorted(
            [
                SelectOptionDict(value=moref, label=info.get("name", moref))
                for moref, info in self._inventory.items()
                if info.get("type") == obj_type
            ],
            key=lambda x: x["label"],
        )
        existing_filter = self._entity_filter.get(category.value, {})
        default_selected = existing_filter.get("morefs", [])

        return self.async_show_form(
            step_id="entity_selection",
            data_schema=vol.Schema(
                {
                    vol.Optional("selected_objects", default=default_selected): SelectSelector(
                        SelectSelectorConfig(options=options, multiple=True, mode=SelectSelectorMode.LIST)
                    ),
                }
            ),
            description_placeholders={"category": _CATEGORY_DISPLAY_NAMES.get(category, category.value)},
        )

    # ------------------------------------------------------------------
    # Restrictions menu
    # ------------------------------------------------------------------

    async def async_step_restrictions_menu(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Show the restrictions sub-menu."""
        await self._ensure_inventory()
        return self.async_show_menu(
            step_id="restrictions_menu",
            menu_options=["restrictions", "vm_select", "host_select", "init"],
        )

    # ------------------------------------------------------------------
    # Global restrictions
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
            self._restrictions[CONF_FORCE_ARM_TIMEOUT] = int(
                user_input.get(CONF_FORCE_ARM_TIMEOUT, DEFAULT_FORCE_ARM_TIMEOUT)
            )
            return await self.async_step_restrictions_menu()

        return self.async_show_form(
            step_id="restrictions",
            data_schema=_restrictions_schema(self._restrictions),
        )

    # ------------------------------------------------------------------
    # Per-VM restrictions (select → actions → another loop)
    # ------------------------------------------------------------------

    async def async_step_vm_select(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select a single VM to configure restrictions for."""
        await self._ensure_inventory()
        vm_opts = self._vm_options()
        if not vm_opts:
            return await self.async_step_restrictions_menu()

        if user_input is not None:
            self._current_vm_moref = user_input["vm_to_restrict"]
            return await self.async_step_vm_actions()

        return self.async_show_form(
            step_id="vm_select",
            data_schema=vol.Schema(
                {
                    vol.Required("vm_to_restrict"): SelectSelector(
                        SelectSelectorConfig(options=vm_opts, multiple=False, mode=SelectSelectorMode.DROPDOWN)
                    ),
                }
            ),
        )

    async def async_step_vm_actions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select which actions to block on the chosen VM."""
        from .const import VmAction  # noqa: PLC0415

        moref = self._current_vm_moref or ""
        name = self._obj_name(moref)

        if user_input is not None:
            actions = user_input.get("vm_blocked_actions", [])
            if actions:
                self._restrictions.setdefault("vms", {})[moref] = {a: True for a in actions}
            else:
                self._restrictions.get("vms", {}).pop(moref, None)
            self._current_vm_moref = None
            return await self.async_step_restrictions_menu()

        # Pre-populate with existing restrictions for this VM
        current_actions = [k for k, v in self._restrictions.get("vms", {}).get(moref, {}).items() if v and k != "_all"]
        vm_action_options: list[SelectOptionDict] = sorted(
            [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in VmAction],
            key=lambda x: x["label"],
        )

        return self.async_show_form(
            step_id="vm_actions",
            data_schema=vol.Schema(
                {
                    vol.Optional("vm_blocked_actions", default=current_actions): SelectSelector(
                        SelectSelectorConfig(options=vm_action_options, multiple=True, mode=SelectSelectorMode.LIST)
                    ),
                }
            ),
            description_placeholders={"vm_name": name},
        )

    # ------------------------------------------------------------------
    # Per-host restrictions (select → actions → back to menu)
    # ------------------------------------------------------------------

    async def async_step_host_select(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select a single host to configure restrictions for."""
        await self._ensure_inventory()
        host_opts = self._host_options()
        if not host_opts:
            return await self.async_step_restrictions_menu()

        if user_input is not None:
            self._current_host_moref = user_input["host_to_restrict"]
            return await self.async_step_host_actions()

        return self.async_show_form(
            step_id="host_select",
            data_schema=vol.Schema(
                {
                    vol.Required("host_to_restrict"): SelectSelector(
                        SelectSelectorConfig(options=host_opts, multiple=False, mode=SelectSelectorMode.DROPDOWN)
                    ),
                }
            ),
        )

    async def async_step_host_actions(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Select which actions to block on the chosen host."""
        from .const import HostAction  # noqa: PLC0415

        moref = self._current_host_moref or ""
        name = self._obj_name(moref)

        if user_input is not None:
            actions = user_input.get("host_blocked_actions", [])
            if actions:
                self._restrictions.setdefault("hosts", {})[moref] = {a: True for a in actions}
            else:
                self._restrictions.get("hosts", {}).pop(moref, None)
            self._current_host_moref = None
            return await self.async_step_restrictions_menu()

        # Pre-populate with existing restrictions for this host
        current_actions = [
            k for k, v in self._restrictions.get("hosts", {}).get(moref, {}).items() if v and k != "_all"
        ]
        host_action_options: list[SelectOptionDict] = sorted(
            [SelectOptionDict(value=a.value, label=a.value.replace("_", " ").title()) for a in HostAction],
            key=lambda x: x["label"],
        )

        return self.async_show_form(
            step_id="host_actions",
            data_schema=vol.Schema(
                {
                    vol.Optional("host_blocked_actions", default=current_actions): SelectSelector(
                        SelectSelectorConfig(options=host_action_options, multiple=True, mode=SelectSelectorMode.LIST)
                    ),
                }
            ),
            description_placeholders={"host_name": name},
        )

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    async def async_step_save(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Save all options and finish."""
        return self.async_create_entry(
            data={
                **dict(self.config_entry.options),
                CONF_CATEGORIES: self._new_categories,
                CONF_PERF_INTERVAL: self._new_perf_interval,
                CONF_ENTITY_FILTER: self._entity_filter,
                CONF_RESTRICTIONS: self._restrictions,
            }
        )
