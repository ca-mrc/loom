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


def test_wait_duration_is_unknown_without_stage_entry_time():
    now = datetime.now(UTC)
    trial = SimpleNamespace(submitted_at=now-timedelta(days=2), finished_at=None,
                            scheduling_observation={"observed_at": now.isoformat()})
    progress = progress_response(trial, "execution_wait", None, [], admin=False, now=now)
    assert progress["stage_seconds"] is None
    assert progress["stage_started_at"] is None
    assert progress["region"] is None
    assert progress["output_state"] == progress["cleanup_state"] == "unknown"
    assert not progress["observation_missing"]
    trial.scheduling_observation = None
    missing = progress_response(trial, "unknown", None, [], admin=False, now=now)
    assert missing["observation_stale"] and missing["observation_missing"]


def test_terminal_outcome_keeps_cleanup_and_output_independent():
    now = datetime.now(UTC)
    trial = SimpleNamespace(submitted_at=now-timedelta(seconds=100), finished_at=now,
                            scheduling_observation=None)
    result = progress_response(trial, "succeeded", None, [], admin=False, now=now,
                               output_state="committed", cleanup_state="in_progress")
    assert result["stage"] == "succeeded"
    assert result["output_state"] == "committed"
    assert result["cleanup_state"] == "in_progress"
    assert result["stage_seconds"] is None


def test_verifier_wait_does_not_use_submission_age():
    now = datetime.now(UTC)
    parent = SimpleNamespace(id=uuid4(), attempt=1, created_at=now-timedelta(seconds=80),
        finalized_at=now-timedelta(seconds=12), last_reconciled_at=now,
        pod_scheduled_at=None, pod_started_at=None, pod_terminated_at=None,
        materialization_committed_at=None)
    trial = SimpleNamespace(submitted_at=now-timedelta(days=2), finished_at=None,
                            scheduling_observation=None, result={"verifier_execution": {"state": "pending"}})
    progress = progress_response(trial, "verifying", None, [], parent=parent, admin=False, now=now)
    assert progress["stage"] == "verifying" and progress["detail"] == "execution_wait"
    assert progress["stage_seconds"] == 12
    assert "independent verifier" in progress["wait_message"]



def test_shared_image_clock_is_not_trial_wait_duration():
    now = datetime.now(UTC)
    trial = SimpleNamespace(submitted_at=now-timedelta(seconds=2), finished_at=None, scheduling_observation=None)
    progress = progress_response(trial, "image_preparation", None, [{"state": "running", "stage": "build",
        "stage_started_at": (now-timedelta(hours=1)).isoformat(), "observed_at": now.isoformat()}],
        admin=False, now=now)
    assert progress["detail"] == "build"
    assert progress["stage_seconds"] is None and progress["stage_started_at"] is None


def test_terminal_cleanup_has_independent_observation_freshness():
    now = datetime.now(UTC)
    trial = SimpleNamespace(submitted_at=now-timedelta(hours=1), finished_at=now-timedelta(minutes=5),
                            scheduling_observation=None)
    for observed, expected_stale in ((None, True), (now-timedelta(minutes=3), True), (now, False)):
        progress = progress_response(trial, "succeeded", None, [], admin=False, now=now,
            cleanup_state="pending", cleanup_observed_at=observed)
        assert progress["stage"] == "succeeded" and not progress["observation_stale"]
        assert progress["cleanup_observation_stale"] == expected_stale
