"""Tests for config_flow._duration_to_seconds()."""

import pytest

from custom_components.vsphere.config_flow import _duration_to_seconds
from custom_components.vsphere.const import (
    MAX_INVENTORY_INTERVAL,
    MAX_PERF_INTERVAL,
    MIN_INVENTORY_INTERVAL,
    MIN_PERF_INTERVAL,
)

# ---------------------------------------------------------------------------
# DurationSelector dict form
# ---------------------------------------------------------------------------


class TestDictForm:
    def test_full_dict_perf_bounds(self):
        raw = {"hours": 0, "minutes": 15, "seconds": 0}
        assert _duration_to_seconds(raw, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 900

    def test_full_dict_inventory_bounds(self):
        raw = {"hours": 0, "minutes": 15, "seconds": 0}
        assert _duration_to_seconds(raw, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == 900

    def test_hours_minutes_seconds_combine(self):
        raw = {"hours": 1, "minutes": 1, "seconds": 1}
        assert _duration_to_seconds(raw, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == 3661

    def test_missing_hours_key_defaults_zero(self):
        raw = {"minutes": 5, "seconds": 0}
        assert _duration_to_seconds(raw, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 300

    def test_missing_minutes_key_defaults_zero(self):
        raw = {"hours": 0, "seconds": 90}
        assert _duration_to_seconds(raw, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 90

    def test_missing_seconds_key_defaults_zero(self):
        raw = {"hours": 0, "minutes": 2}
        assert _duration_to_seconds(raw, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 120

    def test_empty_dict_defaults_to_zero_then_clamps_to_minimum(self):
        assert _duration_to_seconds({}, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == MIN_PERF_INTERVAL


# ---------------------------------------------------------------------------
# Bare int form
# ---------------------------------------------------------------------------


class TestBareInt:
    def test_bare_int_in_range_perf(self):
        assert _duration_to_seconds(300, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 300

    def test_bare_int_in_range_inventory(self):
        assert _duration_to_seconds(900, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == 900

    def test_bare_int_string_coerces(self):
        # int() accepts numeric strings too — guard against a regression that drops that.
        assert _duration_to_seconds("300", MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 300


# ---------------------------------------------------------------------------
# Clamping
# ---------------------------------------------------------------------------


class TestClamping:
    def test_clamps_below_minimum_perf(self):
        assert _duration_to_seconds(1, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == MIN_PERF_INTERVAL

    def test_clamps_below_minimum_inventory(self):
        assert _duration_to_seconds(1, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == MIN_INVENTORY_INTERVAL

    def test_clamps_above_maximum_perf(self):
        assert _duration_to_seconds(999999, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == MAX_PERF_INTERVAL

    def test_clamps_above_maximum_inventory(self):
        assert _duration_to_seconds(999999999, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == MAX_INVENTORY_INTERVAL

    def test_dict_below_minimum_clamps(self):
        raw = {"hours": 0, "minutes": 0, "seconds": 1}
        assert _duration_to_seconds(raw, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == MIN_INVENTORY_INTERVAL

    def test_dict_above_maximum_clamps(self):
        raw = {"hours": 48, "minutes": 0, "seconds": 0}
        assert _duration_to_seconds(raw, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == MAX_INVENTORY_INTERVAL


# ---------------------------------------------------------------------------
# Value already in range passes through unchanged
# ---------------------------------------------------------------------------


class TestPassThrough:
    def test_value_in_range_perf_passes_through(self):
        assert _duration_to_seconds(1200, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == 1200

    def test_value_in_range_inventory_passes_through(self):
        assert _duration_to_seconds(3600, MIN_INVENTORY_INTERVAL, MAX_INVENTORY_INTERVAL) == 3600

    @pytest.mark.parametrize("seconds", [60, 300, 1800, 3600])
    def test_various_perf_in_range_values(self, seconds):
        assert _duration_to_seconds(seconds, MIN_PERF_INTERVAL, MAX_PERF_INTERVAL) == seconds
