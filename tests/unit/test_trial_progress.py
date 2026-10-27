from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_service.monitor_placement import placement_response
from loom_service.trial_progress import progress_response, wait_message


def test_placement_preserves_shared_totals_without_cross_team_identifiers():
    resources = {"cpu_millis": 1000, "memory_mib": 2048, "storage_mib": 2048}
    own = {"uid": "own-pod", "lease_id": "own", "generation": 1, "requests": resources}
    other = {"uid": "other-private-pod", "lease_id": "task-image:other-private-image",
             "generation": 2, "requests": resources}
    payload = {"nodes": [{
        "uid": "private-node", "provider_id": "private-provider", "ready": True,
        "draining": False, "deleting": False, "unschedulable": False,
        "allocatable": resources, "requested": resources, "managed_pods": [own, other],
    }], "pending_pods": [other]}
    result = placement_response(payload, {"own:1": {"trial_id": "own-trial"}}, admin=False)
    assert result["nodes"][0]["build_pods"] == 1
    assert result["nodes"][0]["execution_pods"] == 1
    assert result["nodes"][0]["workloads"][0]["trial_id"] == "own-trial"
    assert result["pending"] == []
    assert result["pending_builds"] == 1
    assert "private" not in str(result)


def test_idle_placement_keeps_configured_build_limit_without_build_pods():
    payload = {"nodes": [], "pending_pods": [], "build_concurrency_limit": 7}
    result = placement_response(payload, {}, admin=False)
    assert result["build_concurrency_limit"] == 7
    assert result["nodes"] == [] and result["pending_builds"] == 0
    del payload["build_concurrency_limit"]
    assert placement_response(payload, {}, admin=False)["build_concurrency_limit"] is None


def test_terminal_progress_uses_retained_image_timing_not_current_cache():
    now = datetime.now(UTC)
    lease_id = uuid4()
    trial = SimpleNamespace(
        submitted_at=now - timedelta(seconds=100), finished_at=now,
        scheduling_observation={
            "lease_id": str(lease_id), "image_mode": "built",
            "image_ready_at": (now - timedelta(seconds=80)).isoformat(),
        },
    )
    lease = SimpleNamespace(
        id=lease_id, attempt=1, created_at=now - timedelta(seconds=70),
        pod_scheduled_at=now - timedelta(seconds=60), pod_started_at=now - timedelta(seconds=50),
        pod_terminated_at=now - timedelta(seconds=10), materialization_committed_at=now,
        last_reconciled_at=now, node_name="private-node",
    )
    result = progress_response(trial, "succeeded", lease, [
        {"state": "failed", "message": "a later cache rebuild failed"},
    ], admin=False, now=now)
    assert result["stage"] == "succeeded"
    assert result["wait_message"] is None and result["node_name"] is None
    assert result["timeline"][0]["label"] == "Image preparation"
    assert [phase["seconds"] for phase in result["timeline"]] == [20, 10, 10, 10, 40, 10]
    assert "later cache" not in str(result)
    lease.attempt = 2
    retried = progress_response(trial, "succeeded", lease, [], admin=False, now=now)
    assert retried["timeline"][0]["seconds"] is None
    assert retried["timeline"][0]["started_at"] is None
    assert retried["timeline"][1]["seconds"] == 10


def test_wait_reasons_distinguish_quota_from_provisioning_without_echoing_text():
    assert "quota" in wait_message("execution_capacity_provider_quota_memory_exceeded")
    assert "nodes to become ready" in wait_message("execution_capacity_provisioning_delay")
    assert wait_message("password=private") is None
    assert "password" not in wait_message("execution_capacity_password=private")


@pytest.mark.parametrize(("reason", "message_fragment"), [
    ("execution_capacity_native_io_feedback_pending", "disk I/O measurements"),
    ("execution_capacity_bound_node_unavailable", "A reserved node cannot currently accept"),
])
def test_execution_wait_progress_explains_build_capacity_without_private_details(reason, message_fragment):
    now = datetime.now(UTC)
    trial = SimpleNamespace(
        submitted_at=now - timedelta(seconds=100), finished_at=None,
        scheduling_observation={"reason": reason, "observed_at": now.isoformat(),
                                "node_name": "other-team-private-node", "message": "password=private"},
    )
    progress = progress_response(trial, "execution_wait", None, [], admin=False, now=now)
    assert message_fragment in progress["wait_message"]
    assert progress["stage"] == "execution_wait" and not progress["observation_stale"]
    assert "private" not in str(progress)
    trial.finished_at = now
    terminal = progress_response(trial, "succeeded", None, [], admin=False, now=now)
    assert terminal["wait_message"] is None
