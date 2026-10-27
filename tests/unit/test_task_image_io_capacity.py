from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from loom_control_plane.task_image_io_capacity import NativeIOUnavailable, native_io_candidates
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture

NOW = datetime(2026, 10, 20, 16, 0, tzinfo=UTC)


def _placement(*pressures):
    data = placement_fixture(target_id="shared", nodes=len(pressures))
    for index, pressure in enumerate(pressures):
        data["nodes"][index].update(name=f"node-{index}", draining=False)
        if pressure is not None:
            data["nodes"][index]["io_pressure"] = {
                "sampled_at": NOW, "full_avg10": float(pressure),
            }
    return data


def _choose(data, reservations=(), **options):
    return native_io_candidates(CapacityPlacement.model_validate(data), list(reservations),
                                now=NOW, max_age_seconds=30,
                                max_full_avg10=options.pop("max_full_avg10", 25.0), **options)


def _native(*, node=0, elapsed=10, phase="running", pod=True):
    return {
        "io_binding": {"mode": "node", "node_name": f"node-{node}",
                       "node_uid": f"shared-node-{node}"},
        "node_name": f"node-{node}", "pod_uid": "observed-pod" if pod else None,
        "phases": [{"name": "build", "state": {
            phase: {"startedAt": (NOW - timedelta(seconds=elapsed)).isoformat()},
        }}],
    }


def test_measured_least_pressure_and_full_feedback_allow_more_than_one_active_build():
    data = _placement(20, 5, 5)
    active = [(f"build-{i}", _native(node=1)) for i in range(12)]
    assert [row["node_name"] for row in _choose(data, active)] == [
        "node-1", "node-2", "node-0",
    ]
    # The policy is supplied by the caller, not a hardcoded 50-percent threshold.
    assert [row["node_name"] for row in _choose(data, active, max_full_avg10=5)] == [
        "node-1", "node-2",
    ]
    assert _choose(data, eligible_node_uids={"shared-node-0"})[0]["node_name"] == "node-0"
    assert _choose(data, eligible_node_uids=set()) == [
        {"mode": "bootstrap", "excluded_node_names": ["node-0", "node-1", "node-2"]},
    ]


@pytest.mark.parametrize("condition", ["missing", "stale", "future", "drain-unknown"])
def test_unqualified_ready_node_never_becomes_healthy_or_bootstrap(condition):
    data = _placement(1)
    node = data["nodes"][0]
    if condition == "missing":
        del node["io_pressure"]
    elif condition == "drain-unknown":
        node["draining"] = None
    else:
        node["io_pressure"]["sampled_at"] = NOW + timedelta(
            seconds=-31 if condition == "stale" else 1,
        )
    with pytest.raises(NativeIOUnavailable, match=r"native_io_observation_unavailable$"):
        _choose(data)
    with pytest.raises(NativeIOUnavailable, match=r"native_io_observation_unavailable$"):
        _choose(data, eligible_node_uids=set())
    # An independently qualified node remains usable despite that unknown peer.
    good = _placement(1, 2)["nodes"][1]
    data["nodes"].append(good)
    assert [row["node_name"] for row in _choose(data)] == ["node-1"]


@pytest.mark.parametrize("phase,elapsed,pod,allowed", [
    ("running", 9, True, False), ("running", 10, True, True),
    ("terminated", 10, True, True), ("waiting", 60, True, False),
    ("running", 60, False, False),
])
def test_feedback_uses_real_pod_heavy_start_and_complete_window(phase, elapsed, pod, allowed):
    data = _placement(1)
    native = _native(phase=phase, elapsed=elapsed, pod=pod)
    native["started_at"] = (NOW - timedelta(hours=1)).isoformat()
    if allowed:
        assert _choose(data, [("other", native)])[0]["mode"] == "node"
    else:
        with pytest.raises(NativeIOUnavailable, match=r"native_io_feedback_pending$"):
            _choose(data, [("other", native)])
    native["capacity_released_at"] = NOW.isoformat()
    assert _choose(data, [("released", native)])[0]["mode"] == "node"


def test_cold_bootstrap_is_single_unobserved_provision_and_retains_exclusions():
    cold = _placement(80, 90)
    bootstrap = _choose(cold)[0]
    assert bootstrap == {"mode": "bootstrap", "excluded_node_names": ["node-0", "node-1"]}
    pending = {"io_binding": bootstrap}
    with pytest.raises(NativeIOUnavailable, match=r"native_io_feedback_pending$"):
        _choose(cold, [("provisioning", pending)])
    # That bootstrap cannot land on old excluded nodes, so it does not stop
    # independently healthy old capacity being used by another ordinary start.
    old_healthy = _placement(1, 90)
    assert _choose(old_healthy, [("provisioning", pending)])[0]["node_name"] == "node-0"
    # Revalidation is not a new selection and never rewrites its original scope.
    old_unknown = _placement(None, None)
    assert _choose(old_unknown, existing_binding=bootstrap) == [bootstrap]
    assert _choose(_placement()) == [{"mode": "bootstrap", "excluded_node_names": []}]


def test_revalidation_preserves_fixed_binding_and_rejects_node_replacement_or_pressure():
    data = _placement(1, 0)
    binding = next(row for row in _choose(data) if row["node_name"] == "node-0")
    assert _choose(data, existing_binding=binding) == [binding]
    replacement = deepcopy(data)
    replacement["nodes"][0]["uid"] = "replacement-uid"
    with pytest.raises(NativeIOUnavailable, match=r"native_io_node_identity_changed$"):
        _choose(replacement, existing_binding=binding)
    hot = deepcopy(data)
    hot["nodes"][0]["io_pressure"]["full_avg10"] = 26.0
    with pytest.raises(NativeIOUnavailable, match=r"native_io_pressure_exceeded$"):
        _choose(hot, existing_binding=binding)
    # A healthy alternative is never silently substituted for the frozen node.
    data["nodes"][0]["deleting"] = True
    with pytest.raises(NativeIOUnavailable, match=r"native_io_node_unavailable$"):
        _choose(data, existing_binding=binding)


def test_unknown_reservations_block_possible_destinations_but_observed_placement_does_not():
    data = _placement(1, 2)
    with pytest.raises(NativeIOUnavailable, match=r"native_io_feedback_pending$"):
        _choose(data, [("legacy-unbound", {"started_at": NOW.isoformat()})])
    with pytest.raises(NativeIOUnavailable, match=r"native_io_feedback_pending$"):
        _choose(data, [("legacy-unbound", {})], eligible_node_uids=set())
    native = _native(elapsed=1)
    assert [row["node_name"] for row in _choose(data, [("starting", native)])] == ["node-1"]
    del native["io_binding"]
    assert [row["node_name"] for row in _choose(data, [("observed-legacy", native)])] == ["node-1"]
    # An unbound bootstrap can land on every newly appeared, nonexcluded node.
    bootstrap = {"io_binding": {"mode": "bootstrap", "excluded_node_names": ["node-0"]}}
    assert [row["node_name"] for row in _choose(data, [("new", bootstrap)])] == ["node-0"]
    bootstrap.update(node_name="node-1", pod_uid="new-pod", phases=_native(node=1)["phases"])
    assert len(_choose(data, [("observed-bootstrap", bootstrap)])) == 2
