from copy import deepcopy

import pytest

from loom.execution_failure_diagnosis import execution_failure_diagnosis
from tests.unit.test_execution_resource_allocation import _plan


def _event(ordinal, *, reason="Error", restarts=1, previous=True, started="2026-09-23T21:06:14Z"):
    return {"ordinal": ordinal, "payload": {
        "job_uid": "job", "pod_uid": "pod", "reason": "SandboxRestarted",
        "normalized_state": "failed", "container_diagnostics": [{
            "name": "task-sandbox", "restart_count": restarts,
            "previous_termination" if previous else "current_termination": {
                "reason": reason, "exit_code": 137, "signal": 9,
                "started_at": started, "finished_at": "2026-09-23T21:19:33Z",
            },
        }],
    }}


def _diagnose(events):
    return execution_failure_diagnosis(events, plan=_plan()[2], job_uid="job", pod_uid="pod")


def test_late_oom_enriches_original_incarnation_without_trusting_exit_137():
    first = _event(1)
    assert _diagnose([first]) is None
    later = _event(3, reason="OOMKilled")
    cleanup = _event(4, reason="Error", restarts=2)
    events = [cleanup, later, first]
    unchanged = deepcopy(events)
    result = _diagnose(events)
    assert result["reason"] == "oom_killed"
    assert result["memory_limit_mib"] == 4096
    assert result["container_incarnation"] == 0
    assert result["evidence_ordinal"] == 3
    assert "4 GiB" in result["message"]
    assert events == unchanged


@pytest.mark.parametrize("mutation", ["pod", "job", "replacement", "started"])
def test_oom_of_another_identity_cannot_reclassify_original_failure(mutation):
    first = _event(1)
    later = _event(2, reason="OOMKilled")
    if mutation in {"pod", "job"}:
        later["payload"][mutation + "_uid"] = "other"
    elif mutation == "replacement":
        later["payload"]["container_diagnostics"][0]["restart_count"] = 2
    else:
        later["payload"]["container_diagnostics"][0]["previous_termination"]["started_at"] = "2026-09-23T21:20:00Z"
    assert _diagnose([first, later]) is None


@pytest.mark.parametrize("replacement_terminated", [False, True])
def test_initial_missing_last_state_can_be_enriched_and_diagnosis_uses_existing_report(replacement_terminated):
    from loom_service.diagnosis import build_trial_diagnosis

    first = _event(1)
    first["payload"]["container_diagnostics"][0].pop("previous_termination")
    if replacement_terminated:
        first["payload"]["container_diagnostics"][0]["current_termination"] = {
            "reason": "Error", "exit_code": 1, "started_at": "2026-09-23T21:19:34Z",
        }
    result = _diagnose([first, _event(2, reason="OOMKilled")])
    assert result is not None
    report = build_trial_diagnosis({
        "entity": {"type": "trial", "id": "trial"},
        "failure": {"reason_code": "trial.oom_killed", "platform_outcome": "failed"},
        "execution_failure": result,
    })
    assert report["summary"] == result["message"]
    assert report["primary_cause"]["attribution"] == "resource_limit"
    assert any("missing peak data is not zero" in e for e in report["evidence"])


def test_fixture_oom_uses_the_bound_fixture_limit_and_cannot_name_another_fixture():
    from tests.unit.test_task_fixtures import _plan as fixture_plan

    event = _event(1, reason="OOMKilled")
    event["payload"]["container_diagnostics"][0]["name"] = "fixture-server"
    result = execution_failure_diagnosis([event], plan=fixture_plan(), job_uid="job", pod_uid="pod")
    assert result is not None
    assert result["container_role"] == "fixture-server" and result["stage"] == "fixture"
    assert result["memory_limit_mib"] == 128
    event["payload"]["container_diagnostics"][0]["name"] = "fixture-other"
    assert execution_failure_diagnosis([event], plan=fixture_plan(), job_uid="job", pod_uid="pod") is None


@pytest.mark.parametrize("previous", [False, True])
@pytest.mark.parametrize("role", ["task-sandbox", "fixture-server"])
def test_delayed_original_oom_corrects_replacement_timestamp_in_first_observation(previous, role):
    # tw_100459: ordinal 5 attributed the replacement's short-lived Error to
    # lastState at restart_count=1; ordinals 8/9 later exposed the original OOM.
    first = _event(5, started="2026-09-23T21:19:34Z")
    ending = first["payload"]["container_diagnostics"][0]["previous_termination"]
    ending.update(exit_code=1, signal=None, finished_at="2026-09-23T21:19:34Z")
    unknown = deepcopy(first)
    unknown["ordinal"] = 7
    unknown["payload"]["container_diagnostics"][0]["current_termination"] = {
        "reason": "ContainerStatusUnknown", "exit_code": 137,
        "started_at": None, "finished_at": None,
    }
    assert _diagnose([first, unknown]) is None
    oom = _event(8, reason="OOMKilled", restarts=int(previous), previous=previous)
    oom["payload"].update(normalized_state="oom_killed",
                          reason="SandboxRestarted" if previous else "SandboxTerminated")
    if previous:
        oom["payload"]["container_diagnostics"][0]["current_termination"] = ending
    events = [unknown, oom, first]
    for event in events:
        event["payload"]["container_diagnostics"][0]["name"] = role
    plan = _plan()[2]
    if role == "fixture-server":
        from tests.unit.test_task_fixtures import _plan as fixture_plan

        plan = fixture_plan()
    unchanged = deepcopy(events)
    result = execution_failure_diagnosis(events, plan=plan, job_uid="job", pod_uid="pod")
    assert result is not None
    assert result["container_incarnation"] == 0
    assert result["started_at"] == "2026-09-23T21:06:14Z"
    assert result["terminated_at"] == "2026-09-23T21:19:33Z"
    assert result["evidence_ordinal"] == 8
    assert result["container_role"] == role
    assert result["stage"] == ("fixture" if role == "fixture-server" else "agent")
    assert result["memory_limit_mib"] == (128 if role == "fixture-server" else 4096)
    assert events == unchanged


def test_late_original_non_oom_prevents_misattributing_first_observed_replacement_oom():
    replacement = _event(1, reason="OOMKilled", started="2026-09-23T21:20:00Z")
    original = _event(2, reason="Error", restarts=0, previous=False)
    # Resolve identity over all available evidence before selecting an OOM.
    assert _diagnose([replacement, original]) is None
