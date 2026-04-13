"""Permission resolver for vSphere Control integration.

Enforces user-configured operation restrictions.  vSphere account privileges
are NOT checked here — they are enforced by vCenter/ESXi at operation time,
which surfaces a clear ``NoPermission`` fault when access is denied.

Resolution chain (most specific wins):
  1. restrictions.{category}["{moref}"].{action}   → per-object per-action
  2. restrictions.{category}["{moref}"]._all        → per-object blanket
  3. restrictions.categories.{category}.{action}   → per-category per-action
  4. restrictions.categories.{category}._all        → per-category blanket
  5. restrictions.global.{action}                   → global per-action
  6. restrictions.global.{group}                    → global shortcut groups
  7. restrictions.global._all                       → nuclear switch
  8. default: allowed
"""

from __future__ import annotations

from typing import Any

from .const import (
    DESTRUCTIVE_ACTIONS,
    HOST_OPS_ACTIONS,
    RESTRICTION_GROUP_DESTRUCTIVE,
    RESTRICTION_GROUP_HOST_OPS,
    RESTRICTION_GROUP_MIGRATE,
    RESTRICTION_GROUP_SNAPSHOTS,
    SNAPSHOT_ACTIONS,
    HostAction,
    VmAction,
)

# All known actions per category
_CATEGORY_ACTIONS: dict[str, set[str]] = {
    "vms": {a.value for a in VmAction},
    "hosts": {a.value for a in HostAction},
}

# Shortcut group → (category filter, action set) mapping.
# category filter of None means the group applies to all categories.
_SHORTCUT_GROUPS: dict[str, tuple[str | None, set[str]]] = {
    RESTRICTION_GROUP_DESTRUCTIVE: ("vms", DESTRUCTIVE_ACTIONS),
    RESTRICTION_GROUP_SNAPSHOTS: ("vms", SNAPSHOT_ACTIONS),
    RESTRICTION_GROUP_MIGRATE: ("vms", {VmAction.MIGRATE}),
    RESTRICTION_GROUP_HOST_OPS: ("hosts", HOST_OPS_ACTIONS),
}

# Sentinel used internally to mean "no value found at this level"
_UNSET = object()


class PermissionResolver:
    """Resolve whether an action is permitted on a vSphere managed object.

    The resolver walks a six-step chain and returns the result of the first
    rule that matches.  A value of ``True`` in the config means *blocked*;
    ``False`` means *explicitly allowed* (used to punch a hole through a
    broader restriction).
    """

    def __init__(
        self,
        restrictions: dict[str, Any],
    ) -> None:
        """Store the user-configured operation restrictions.

        Args:
            restrictions: User-configured operation restrictions.
        """
        self._restrictions = restrictions

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_allowed(self, category: str, moref: str, action: str) -> bool:
        """Return True if *action* is allowed on *moref* in *category*.

        Walks the resolution chain; the first match wins.
        """
        result, _ = self._resolve(category, moref, action)
        return not result  # result is True ↔ blocked

    def allowed_actions(self, category: str, moref: str) -> set[str]:
        """Return the set of allowed actions for *moref* in *category*."""
        known = _CATEGORY_ACTIONS.get(category, set())
        return {action for action in known if self.is_allowed(category, moref, action)}

    def explain(self, category: str, moref: str, action: str) -> str:
        """Return a human-readable string explaining the resolution outcome."""
        blocked, reason = self._resolve(category, moref, action)
        if blocked:
            return f"blocked: {reason}"
        return f"allowed: {reason}"

    # ------------------------------------------------------------------
    # Internal resolution logic
    # ------------------------------------------------------------------

    def _resolve(self, category: str, moref: str, action: str) -> tuple[bool, str]:
        """Walk the resolution chain and return (blocked, reason).

        ``blocked`` is True when the action is denied.
        ``reason`` is a human-readable description of the deciding rule.
        """
        obj_rules: dict[str, Any] = self._restrictions.get(category, {}).get(moref, {})
        cat_restrictions: dict[str, Any] = self._restrictions.get("categories", {}).get(category, {})
        global_rules: dict[str, Any] = self._restrictions.get("global", {})

        # ------------------------------------------------------------------
        # Step 1: per-object per-action
        # ------------------------------------------------------------------
        value = obj_rules.get(action, _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (
                f"{state} by per-object per-action rule (category={category}, moref={moref}, action={action})"
            )

        # ------------------------------------------------------------------
        # Step 2: per-object blanket (_all)
        # ------------------------------------------------------------------
        value = obj_rules.get("_all", _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (f"{state} by per-object blanket rule (category={category}, moref={moref}, _all={value})")

        # ------------------------------------------------------------------
        # Step 3: per-category per-action
        # ------------------------------------------------------------------
        value = cat_restrictions.get(action, _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (f"{state} by per-category per-action rule (category={category}, action={action})")

        # ------------------------------------------------------------------
        # Step 4: per-category blanket (_all)
        # ------------------------------------------------------------------
        value = cat_restrictions.get("_all", _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (f"{state} by per-category blanket rule (category={category}, _all={value})")

        # ------------------------------------------------------------------
        # Step 5: global per-action
        # ------------------------------------------------------------------
        value = global_rules.get(action, _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (f"{state} by global per-action rule (action={action})")

        # ------------------------------------------------------------------
        # Step 6: global shortcut groups
        # ------------------------------------------------------------------
        for group_name, (cat_filter, action_set) in _SHORTCUT_GROUPS.items():
            # Skip this group if it is scoped to a specific category that
            # doesn't match the current category.
            if cat_filter is not None and cat_filter != category:
                continue
            if action not in action_set:
                continue
            value = global_rules.get(group_name, _UNSET)
            if value is not _UNSET:
                blocked = bool(value)
                state = "blocked" if blocked else "allowed"
                return blocked, (f"{state} by global shortcut group '{group_name}' (action={action})")

        # ------------------------------------------------------------------
        # Step 7: global nuclear switch (_all)
        # ------------------------------------------------------------------
        value = global_rules.get("_all", _UNSET)
        if value is not _UNSET:
            blocked = bool(value)
            state = "blocked" if blocked else "allowed"
            return blocked, (f"{state} by global _all switch")

        # ------------------------------------------------------------------
        # Step 8: default — allowed
        # ------------------------------------------------------------------
        return False, "allowed by default (no matching restriction)"
