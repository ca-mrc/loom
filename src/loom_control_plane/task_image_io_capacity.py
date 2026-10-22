"""Pure node-pressure qualification for the existing native capacity admission.

The caller owns physical-scope deduplication, resource/quota fit and reservation
serialization. During revalidation it must exclude the reservation being
revalidated. A bootstrap binding requests provisioning, never certifies health.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeIOPressure,
    NodeIOWindowPressure,
    NodePlacement,
)


class NativeIOUnavailable(ValueError):  # noqa: N818 - existing admission interface name
    """A secret-safe reason suitable for an ordinary capacity wait."""


def _unavailable(reason: str) -> NativeIOUnavailable:
    return NativeIOUnavailable("execution_capacity_native_io_" + reason)


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        return parsed if isinstance(parsed, datetime) and parsed.utcoffset() is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _name(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _binding(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if value.get("mode") == "node":
        if _name(value.get("node_name")) and _name(value.get("node_uid")):
            return value
    if value.get("mode") == "bootstrap":
        names = value.get("excluded_node_names")
        if (isinstance(names, list) and all(_name(name) for name in names)
                and names == sorted(set(names))):
            return value
    return None


def _eligible(node: NodePlacement) -> bool:
    return bool(node.ready and not node.unschedulable and not node.deleting
                and node.draining is False and node.name)


def _pressure(node: NodePlacement) -> NodeIOWindowPressure | NodeIOPressure | None:
    # A present provider window is authoritative even when stale or hot. Never
    # downgrade it to an older healthy kernel observation.
    return node.io_stall_window if node.io_stall_window is not None else node.io_pressure


def _percent(node: NodePlacement) -> float:
    pressure = _pressure(node)
    assert pressure is not None
    return (pressure.full_window_percent if isinstance(pressure, NodeIOWindowPressure)
            else pressure.full_avg10)


def _metric_fresh(node: NodePlacement, now: datetime, max_age_seconds: int) -> bool:
    pressure = _pressure(node)
    if isinstance(pressure, NodeIOWindowPressure) and (
        pressure.source != "nebius_io_stalled_counter" or node.created_at is None
        or pressure.window_started_at < node.created_at
    ):
        return False
    return bool(pressure is not None and timedelta(0) <= now - pressure.sampled_at
                <= timedelta(seconds=max_age_seconds))


def _actual_node(native: dict[str, Any]) -> str | None:
    # A recorded name without an observed Pod UID is not placement evidence.
    return _name(native.get("node_name")) if _name(native.get("pod_uid")) else None


def _can_land(native: dict[str, Any], node: NodePlacement) -> bool:
    actual = _actual_node(native)
    binding = _binding(native.get("io_binding"))
    if binding is not None and binding["mode"] == "node":
        return node.name in (binding["node_name"], actual)
    if actual is not None:
        return node.name == actual
    if binding is not None:
        return node.name not in binding["excluded_node_names"]
    # A legacy/unbound reservation without observed placement may land anywhere.
    return True


def _feedback_observed(native: dict[str, Any], node: NodePlacement) -> bool:
    pressure = _pressure(node)
    if _actual_node(native) != node.name or pressure is None:
        return False
    binding = _binding(native.get("io_binding"))
    if (binding is not None and binding["mode"] == "node"
            and binding["node_name"] == node.name and (
                binding["node_uid"] != node.uid
                or ("provider_id" in binding and binding["provider_id"] != node.provider_id)
            )):
        return False
    phases = native.get("phases")
    if not isinstance(phases, list):
        return False
    builds = [phase for phase in phases if isinstance(phase, dict) and phase.get("name") == "build"]
    if len(builds) != 1 or not isinstance(builds[0].get("state"), dict):
        return False
    state = builds[0]["state"]
    started = [detail.get("startedAt") for kind, detail in state.items()
               if kind in {"running", "terminated"} and isinstance(detail, dict)]
    if len(started) != 1:
        return False
    at = _time(started[0])
    # This is the actual heavy phase, not a materialization or prepare timestamp.
    if isinstance(pressure, NodeIOWindowPressure):
        # A late endpoint alone can hide a new build behind earlier idle time.
        # The strict contract already requires at least ten real seconds.
        return at is not None and pressure.window_started_at >= at
    return at is not None and pressure.sampled_at - at >= timedelta(seconds=10)


def native_io_candidates(
    placement: CapacityPlacement,
    reservations: list[tuple[str, dict[str, Any]]],
    *,
    now: datetime,
    max_age_seconds: int,
    max_full_avg10: float,
    existing_binding: dict[str, Any] | None = None,
    eligible_node_uids: set[str] | frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    """Return measured healthy bindings, or one explicit cold provisioning choice.

    There is no active build-count ceiling. A new start waits only until existing
    starts that could affect its node have a full observed ten-second window.
    Resource feasibility is deliberately left to the common placement planner;
    its eligible UIDs filter returned candidates only. Preserve the complete
    inventory for unknown/feedback checks and bootstrap exclusions.
    """
    if (not isinstance(now, datetime) or _time(now) is None or isinstance(max_age_seconds, bool)
            or not isinstance(max_age_seconds, int) or max_age_seconds <= 0
            or isinstance(max_full_avg10, bool) or not isinstance(max_full_avg10, (int, float))
            or not math.isfinite(max_full_avg10) or not 0 <= max_full_avg10 <= 100):
        raise _unavailable("policy_invalid")
    if (eligible_node_uids is not None and (
            not isinstance(eligible_node_uids, (set, frozenset))
            or not eligible_node_uids <= {node.uid for node in placement.nodes})):
        raise _unavailable("eligibility_invalid")
    active = [native for _, native in reservations if native.get("capacity_released_at") is None]
    nodes = placement.nodes

    def pending(node: NodePlacement) -> bool:
        return any(_can_land(native, node) and not _feedback_observed(native, node)
                   for native in active)

    def unresolved() -> bool:
        for native in active:
            actual = _actual_node(native)
            node = next((row for row in nodes if row.name == actual), None) if actual else None
            if (node is None or not _metric_fresh(node, now, max_age_seconds)
                    or not _feedback_observed(native, node)):
                return True
        return False

    if existing_binding is not None:
        binding = _binding(existing_binding)
        if binding is None:
            raise _unavailable("binding_invalid")
        if binding["mode"] == "node":
            node = next((row for row in nodes if row.name == binding["node_name"]), None)
            if (node is None or node.uid != binding["node_uid"]
                    or ("provider_id" in binding and binding["provider_id"] != node.provider_id)):
                raise _unavailable("node_identity_changed")
            if not _eligible(node):
                raise _unavailable("node_unavailable")
            if not _metric_fresh(node, now, max_age_seconds):
                raise _unavailable("observation_unavailable")
            if _percent(node) > max_full_avg10:
                raise _unavailable("pressure_exceeded")
            if pending(node):
                raise _unavailable("feedback_pending")
        else:
            if unresolved():
                raise _unavailable("feedback_pending")
            allowed = [node for node in nodes if node.name not in binding["excluded_node_names"]
                       and node.ready and not node.unschedulable and not node.deleting]
            if any(not _eligible(node) or not _metric_fresh(node, now, max_age_seconds)
                   for node in allowed):
                raise _unavailable("observation_unavailable")
            if allowed and not any(_percent(node) <= max_full_avg10 for node in allowed):
                raise _unavailable("pressure_exceeded")
        return [dict(existing_binding)]

    candidates: list[tuple[float, dict[str, Any]]] = []
    unknown = False
    for node in nodes:
        if not node.ready or node.unschedulable or node.deleting or node.draining is True:
            continue
        if not _eligible(node) or not _metric_fresh(node, now, max_age_seconds):
            unknown = True
            continue
        pressure = _pressure(node)
        assert pressure is not None and node.name is not None
        percent = _percent(node)
        if (percent <= max_full_avg10 and not pending(node)
                and (eligible_node_uids is None or node.uid in eligible_node_uids)):
            binding = {"mode": "node", "node_name": node.name, "node_uid": node.uid,
                       "sampled_at": pressure.sampled_at.isoformat()}
            if isinstance(pressure, NodeIOWindowPressure):
                binding.update(provider_id=node.provider_id, source=pressure.source,
                               window_started_at=pressure.window_started_at.isoformat(),
                               full_window_percent=percent)
            else:
                binding["full_avg10"] = percent
            candidates.append((percent, binding))
    if candidates:
        return [binding for _, binding in sorted(
            candidates, key=lambda row: (row[0], row[1]["node_name"]),
        )]
    if unresolved():
        raise _unavailable("feedback_pending")
    if unknown:
        raise _unavailable("observation_unavailable")
    return [{"mode": "bootstrap", "excluded_node_names": sorted(
        {node.name for node in nodes if node.name is not None})}]
