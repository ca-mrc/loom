"""Provider windows must qualify actual heavy-phase feedback, never imply avg10."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from loom_control_plane.task_image_io_capacity import NativeIOUnavailable, native_io_candidates
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture

NOW = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)


def _placement(*percentages):
    data = placement_fixture(target_id="window", nodes=len(percentages))
    for index, percent in enumerate(percentages):
        node = data["nodes"][index]
        node.update(name=f"node-{index}", draining=False,
                    created_at=NOW - timedelta(hours=1))
        if percent is not None:
            node["io_stall_window"] = {
                "source": "nebius_io_stalled_counter",
                "window_started_at": NOW - timedelta(seconds=15),
                "sampled_at": NOW,
                "full_window_percent": float(percent),
            }
    return data


def _choose(data, reservations=(), **options):
    return native_io_candidates(
        CapacityPlacement.model_validate(data), list(reservations), now=NOW,
        max_age_seconds=30, max_full_avg10=50.0, **options,
    )


def _active(*, elapsed=15, node=0, phase="running"):
    return {
        "io_binding": {"mode": "node", "node_name": f"node-{node}",
                       "node_uid": f"window-node-{node}"},
        "node_name": f"node-{node}", "pod_uid": "observed-pod",
        "phases": [{"name": "build", "state": {
            phase: {"startedAt": (NOW - timedelta(seconds=elapsed)).isoformat()},
        }}],
    }


def test_window_binding_preserves_metric_meaning_and_current_provider_identity():
    data = _placement(10)
    binding = _choose(data)[0]
    assert binding == {
        "mode": "node", "node_name": "node-0", "node_uid": "window-node-0",
        "provider_id": data["nodes"][0]["provider_id"],
        "source": "nebius_io_stalled_counter",
        "window_started_at": (NOW - timedelta(seconds=15)).isoformat(),
        "sampled_at": NOW.isoformat(), "full_window_percent": 10.0,
    }
    assert _choose(data, existing_binding=binding) == [binding]
    hot = deepcopy(data)
    hot["nodes"][0]["io_stall_window"]["full_window_percent"] = 65.0
    with pytest.raises(NativeIOUnavailable, match=r"pressure_exceeded$"):
        _choose(hot, existing_binding=binding)
    assert _choose(hot) == [{"mode": "bootstrap", "excluded_node_names": ["node-0"]}]


@pytest.mark.parametrize("offset", [-31, 1])
def test_stale_or_future_window_does_not_fall_back_to_healthy_legacy(offset):
    data = _placement(1)
    node = data["nodes"][0]
    node["io_pressure"] = {"sampled_at": NOW, "full_avg10": 0.0}
    window = node["io_stall_window"]
    window["sampled_at"] += timedelta(seconds=offset)
    window["window_started_at"] += timedelta(seconds=offset)
    with pytest.raises(NativeIOUnavailable, match=r"observation_unavailable$"):
        _choose(data)


def test_hot_provider_window_takes_precedence_over_healthy_legacy():
    data = _placement(65)
    data["nodes"][0]["io_pressure"] = {"sampled_at": NOW, "full_avg10": 0.0}
    assert _choose(data)[0]["mode"] == "bootstrap"


@pytest.mark.parametrize("created_at", [None, NOW - timedelta(seconds=14), NOW])
def test_window_requires_current_node_creation_identity(created_at):
    data = _placement(1)
    data["nodes"][0]["created_at"] = created_at
    with pytest.raises(NativeIOUnavailable, match=r"observation_unavailable$"):
        _choose(data)
    # Missing identity cannot trigger extra provisioning instead.
    with pytest.raises(NativeIOUnavailable, match=r"observation_unavailable$"):
        _choose(data, eligible_node_uids=set())


@pytest.mark.parametrize("elapsed,phase,allowed", [
    (10, "running", False), (14, "running", False),
    (15, "running", True), (25, "terminated", True), (60, "waiting", False),
])
def test_entire_provider_window_must_follow_actual_heavy_start(elapsed, phase, allowed):
    data = _placement(1)
    active = _active(elapsed=elapsed, phase=phase)
    active["started_at"] = (NOW - timedelta(hours=1)).isoformat()
    if allowed:
        assert _choose(data, [("active", active)])[0]["mode"] == "node"
    else:
        with pytest.raises(NativeIOUnavailable, match=r"feedback_pending$"):
            _choose(data, [("active", active)])


def test_full_feedback_has_no_per_node_active_count_ceiling_and_respects_resource_fit():
    data = _placement(10, 1, 5)
    active = [(f"active-{index}", _active(elapsed=15)) for index in range(20)]
    assert [row["node_name"] for row in _choose(data, active)] == [
        "node-1", "node-2", "node-0",
    ]
    assert [row["node_name"] for row in _choose(
        data, active, eligible_node_uids={"window-node-0"},
    )] == ["node-0"]
    assert _choose(data, active, eligible_node_uids=set()) == [{
        "mode": "bootstrap", "excluded_node_names": ["node-0", "node-1", "node-2"],
    }]


@pytest.mark.parametrize("field,value", [
    ("uid", "replacement-node"), ("provider_id", "different-provider-instance"),
])
def test_frozen_window_binding_never_inherits_replaced_node_identity(field, value):
    data = _placement(1)
    binding = _choose(data)[0]
    data["nodes"][0][field] = value
    with pytest.raises(NativeIOUnavailable, match=r"node_identity_changed$"):
        _choose(data, existing_binding=binding)
    active = _active()
    active["io_binding"] = binding
    with pytest.raises(NativeIOUnavailable, match=r"feedback_pending$"):
        _choose(data, [("old-node-build", active)])


def test_unknown_ready_node_waits_and_zero_node_bootstrap_remains_single_unobserved():
    with pytest.raises(NativeIOUnavailable, match=r"observation_unavailable$"):
        _choose(_placement(None))
    cold = _placement()
    binding = _choose(cold)[0]
    assert binding == {"mode": "bootstrap", "excluded_node_names": []}
    with pytest.raises(NativeIOUnavailable, match=r"feedback_pending$"):
        _choose(cold, [("provisioning", {"io_binding": binding})])


def test_legacy_avg10_binding_and_ten_second_feedback_remain_compatible():
    data = _placement(None)
    data["nodes"][0]["io_pressure"] = {"sampled_at": NOW, "full_avg10": 5.0}
    binding = _choose(data, [("legacy", _active(elapsed=10))])[0]
    assert binding == {
        "mode": "node", "node_name": "node-0", "node_uid": "window-node-0",
        "sampled_at": NOW.isoformat(), "full_avg10": 5.0,
    }
