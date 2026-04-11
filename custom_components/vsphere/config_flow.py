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
)

from .const import (
    CONF_CATEGORIES,
    CONF_ENTITY_FILTER,
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PERF_INTERVAL,
    CONF_PORT,
    CONF_RESTRICTIONS,
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
            vol.Required(CONF_VERIFY_SSL, default=d.get(CONF_VERIFY_SSL, DEFAULT_VERIFY_SSL)): BooleanSelector(),
        }
    )


def _categories_schema(defaults: dict[str, bool] | None = None) -> vol.Schema:
    """Return the categories step schema."""
    effective = dict(DEFAULT_CATEGORIES)
    if defaults:
        effective.update(defaults)
    return vol.Schema(
        {vol.Required(cat.value, default=effective.get(cat.value, False)): BooleanSelector() for cat in Category}
    )


def _intervals_schema(default_interval: int = DEFAULT_PERF_INTERVAL) -> vol.Schema:
    """Return the intervals step schema."""
    return vol.Schema(
        {
            vol.Required(CONF_PERF_INTERVAL, default=default_interval): NumberSelector(
                NumberSelectorConfig(
                    min=MIN_PERF_INTERVAL,
                    max=MAX_PERF_INTERVAL,
                    step=1,
                    mode=NumberSelectorMode.BOX,
                    unit_of_measurement="seconds",
                )
            ),
        }
    )


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

    # ------------------------------------------------------------------
    # Step 1: user — connection details
    # ------------------------------------------------------------------

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle the initial connection step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            host = user_input[CONF_HOST]
            port = user_input[CONF_PORT]

            await self.async_set_unique_id(f"{host}:{port}")
            self._abort_if_unique_id_configured()

            errors = await self._test_connection(user_input)
            if not errors:
                self._connection_data = user_input
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
            self._categories = {cat.value: user_input.get(cat.value, False) for cat in Category}
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
        )
        try:
            self._inventory = await self.hass.async_add_executor_job(client.enumerate_inventory)
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Could not enumerate inventory; skipping entity selection")
            self._inventory = {}

        # Build list of filterable categories that are enabled
        self._filterable_remaining = [cat for cat in _FILTERABLE_CATEGORIES if self._categories.get(cat.value, False)]
        return await self._next_entity_selection_step()

    async def _next_entity_selection_step(self) -> ConfigFlowResult:
        """Advance to the entity selection for the next category, or move on."""
        if not self._filterable_remaining:
            return await self.async_step_intervals()

        self._current_filter_category = self._filterable_remaining.pop(0)
        return await self.async_step_entity_selection()

    async def async_step_entity_selection(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle entity selection for the current category."""
        category = self._current_filter_category
        if category is None:
            return await self.async_step_intervals()

        if user_input is not None:
            select_all: bool = user_input.get("select_all", True)
            if select_all:
                self._entity_filter[category.value] = {"mode": FILTER_MODE_ALL}
            else:
                selected: list[str] = user_input.get("selected_objects", [])
                self._entity_filter[category.value] = {
                    "mode": FILTER_MODE_SELECT,
                    "morefs": selected,
                }
            return await self._next_entity_selection_step()

        # Build options list from inventory for this category
        type_map: dict[Category, str] = {
            Category.HOSTS: "host",
            Category.VMS: "vm",
        }
        obj_type = type_map.get(category, "")
        options: list[SelectOptionDict] = [
            SelectOptionDict(value=moref, label=info.get("name", moref))
            for moref, info in self._inventory.items()
            if info.get("type") == obj_type
        ]

        data_schema = vol.Schema(
            {
                vol.Required("select_all", default=True): BooleanSelector(),
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
            description_placeholders={"category": category.value},
        )

    # ------------------------------------------------------------------
    # Step 4: intervals (only if Performance enabled)
    # ------------------------------------------------------------------

    async def async_step_intervals(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle polling interval configuration."""
        perf_enabled = self._categories.get(Category.PERFORMANCE, False)

        if not perf_enabled:
            return self._create_entry()

        if user_input is not None:
            perf_interval = int(user_input[CONF_PERF_INTERVAL])
            return self._create_entry(perf_interval=perf_interval)

        return self.async_show_form(
            step_id="intervals",
            data_schema=_intervals_schema(),
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
            errors = await self._test_connection(user_input)
            if not errors:
                new_unique_id = f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}"
                await self.async_set_unique_id(new_unique_id)
                self._abort_if_unique_id_configured(updates={**existing_data, **user_input})
                return self.async_update_reload_and_abort(
                    reconfigure_entry,
                    data={**existing_data, **user_input},
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
        client = VSphereClient(
            host=data[CONF_HOST],
            port=data[CONF_PORT],
            username=data[CONF_USERNAME],
            password=data[CONF_PASSWORD],
            verify_ssl=data[CONF_VERIFY_SSL],
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

    def _create_entry(self, perf_interval: int = DEFAULT_PERF_INTERVAL) -> ConfigFlowResult:
        """Create the config entry with collected data."""
        host = self._connection_data[CONF_HOST]

        options: dict[str, Any] = {
            CONF_CATEGORIES: self._categories,
            CONF_ENTITY_FILTER: self._entity_filter,
            CONF_RESTRICTIONS: {},
            CONF_PERF_INTERVAL: perf_interval,
        }

        return self.async_create_entry(
            title=host,
            data=self._connection_data,
            options=options,
        )


class VSphereOptionsFlow(OptionsFlowWithConfigEntry):
    """Options flow for vSphere Control."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle the options flow entry point."""
        current_options = dict(self.config_entry.options)
        current_categories: dict[str, bool] = current_options.get(CONF_CATEGORIES, dict(DEFAULT_CATEGORIES))
        current_perf_interval: int = current_options.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL)

        if user_input is not None:
            categories = {cat.value: user_input.get(cat.value, False) for cat in Category}
            perf_interval = int(user_input.get(CONF_PERF_INTERVAL, DEFAULT_PERF_INTERVAL))
            return self.async_create_entry(
                data={
                    **current_options,
                    CONF_CATEGORIES: categories,
                    CONF_PERF_INTERVAL: perf_interval,
                }
            )

        schema_fields: dict[Any, Any] = {
            vol.Required(cat.value, default=current_categories.get(cat.value, False)): BooleanSelector()
            for cat in Category
        }
        schema_fields[vol.Required(CONF_PERF_INTERVAL, default=current_perf_interval)] = NumberSelector(
            NumberSelectorConfig(
                min=MIN_PERF_INTERVAL,
                max=MAX_PERF_INTERVAL,
                step=1,
                mode=NumberSelectorMode.BOX,
                unit_of_measurement="seconds",
            )
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(schema_fields),
        )
