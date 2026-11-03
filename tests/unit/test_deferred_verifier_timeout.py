"""Committed deadline evidence remains scoreable without losing its timeout."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom_control_plane import service_execution as execution
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.service_execution_materializer import _merge_verifier_execution
from tests.unit.test_execution_runtime_contract import _phase, _plan


def source(status="timed_out", role="attempt"):
    plan = _plan(
        execution_role=role,
        main=_phase("agent" if role == "attempt" else "verifier"),
        verifier_execution="separate_execution" if role == "attempt" else "skipped",
        verifier=None,
    )
    parent = SimpleNamespace(
        id=uuid4(),
        trial_id=uuid4(),
        execution_role=role,
        runtime_contract_json=plan.canonical_payload(),
        runtime_contract_sha256="sha256:" + "f" * 64,
        output_generation=1,
        resource_generation=1,
        generation=1,
        output_manifest_sha256="sha256:" + "a" * 64,
        output_marker_sha256="sha256:" + "b" * 64,
    )
    now = datetime.now(UTC)
    stream = {
        "path": "01-agent.stdout",
        "sha256": "sha256:" + "d" * 64,
        "bytes_seen": 0,
        "bytes_saved": 0,
        "truncated": False,
    }
    raw = {
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": parent.runtime_contract_sha256,
        **{
            key: getattr(plan, key)
            for key in (
                "candidate_sha",
                "task_revision_sha256",
                "command_identity_sha256",
                "execution_role",
                "task_image_ref",
                "runtime_image_ref",
                "runtime_binary_sha256",
                "execution_class_id",
            )
        },
        "container_roles": ["execution", "agent"],
        "status": status,
        "started_at": now,
        "finished_at": now + timedelta(seconds=1),
        "partial_evidence": status != "succeeded",
        "phases": [
            {
                "role": plan.main.role,
                "ordinal": 1,
                "started_at": now,
                "finished_at": now + timedelta(seconds=1),
                "exit_code": 124 if status == "timed_out" else 0,
                "timed_out": status == "timed_out",
                "stdout": stream,
                "stderr": {**stream, "path": "01-agent.stderr"},
            }
        ],
    }
    return parent, ExecutionRuntimeResultV1.model_validate(raw)


@pytest.mark.parametrize("safe", [True, False])
async def test_committed_timeout_handoff_is_only_queued_when_safe(monkeypatch, safe):
    parent, runtime = source()
    trial = SimpleNamespace(cancellation_requested_at=None, state="running")
    session = AsyncMock()
    session.execute.return_value = MagicMock(
        scalar_one_or_none=lambda: SimpleNamespace(payload_json=runtime.model_dump(mode="json"))
    )
    session.get.return_value = trial
    files = [("artifacts/workspace.tar", 12, "sha256:" + "c" * 64)] if safe else []
    monkeypatch.setattr(execution, "committed_handoff_files", AsyncMock(return_value=(files, [])))
    payload = await execution._committed_result_finalization_payload(session, lease=parent)
    assert payload["result"]["runtime_result"]["status"] == "timed_out"
    assert payload["trial_state"] == ("running" if safe else "failed")
    assert ("verifier_execution" in payload["result"]) is safe


async def test_cancelled_timeout_never_enters_grading(monkeypatch):
    parent, runtime = source()
    session = AsyncMock()
    session.execute.return_value = MagicMock(
        scalar_one_or_none=lambda: SimpleNamespace(payload_json=runtime.model_dump(mode="json"))
    )
    session.get.return_value = SimpleNamespace(
        cancellation_requested_at=datetime.now(UTC), state="running"
    )
    files = AsyncMock(return_value=([("artifacts/workspace.tar", 12, "sha256:" + "c" * 64)], []))
    monkeypatch.setattr(execution, "committed_handoff_files", files)
    payload = await execution._committed_result_finalization_payload(session, lease=parent)
    assert payload["trial_state"] == "cancelled"
    assert "verifier_execution" not in payload["result"]


def test_graded_timeout_preserves_timeout_and_numeric_zero():
    _, attempt = source()
    _, verifier = source("succeeded", "verifier")
    verifier = verifier.model_copy(update={"verifier_rewards": {"passed": 0.0}})
    result = {
        "reward": {"passed": 0.0},
        "verifier_execution": {
            "state": "committed",
            "runtime_result": verifier.model_dump(mode="json"),
        },
    }
    merged, _ = _merge_verifier_execution(
        attempt, result, {"result.json": verifier.model_dump_json().encode()}
    )
    assert merged.status == "timed_out"
    assert merged.verifier_rewards == {"passed": 0.0}
    assert merged.partial_evidence is True


@pytest.mark.parametrize("reason", ["max_nodes_reached", "provider_vcpu_quota_exceeded"])
async def test_capacity_wait_does_not_expire_committed_workspace(monkeypatch, reason):
    parent, _ = source()
    parent.deleted_at = datetime.now(UTC) - timedelta(hours=2)
    trial = SimpleNamespace(
        id=parent.trial_id,
        state="running",
        failure_reason=None,
        result={"verifier_execution": {"state": "pending", "parent_lease_id": str(parent.id)}},
    )
    session = AsyncMock()
    session.execute.return_value = MagicMock(all=lambda: [(parent, trial)])
    session.begin_nested = MagicMock(return_value=AsyncMock())
    reserve = AsyncMock(side_effect=ExecutionProvisioningBlockedError(reason))
    monkeypatch.setattr(scheduler, "_reserve_verifier_candidate", reserve)
    assert (
        await scheduler.reserve_next_verifier_executions(
            session, pool_id="pool", image_admission_keyring=MagicMock()
        )
        == []
    )
    assert trial.state == "running"
    assert trial.result["verifier_execution"]["state"] == "pending"
    child = SimpleNamespace(id=uuid4())
    reserve.side_effect = None
    reserve.return_value = child
    assert await scheduler.reserve_next_verifier_executions(
        session, pool_id="pool", image_admission_keyring=MagicMock()
    ) == [child]


async def test_setup_timeout_with_workspace_does_not_grade(monkeypatch):
    parent, runtime = source()
    raw = runtime.model_dump(mode="json")
    raw["phases"][0]["role"] = "setup"
    session = AsyncMock()
    session.execute.return_value = MagicMock(
        scalar_one_or_none=lambda: SimpleNamespace(payload_json=raw)
    )
    session.get.return_value = SimpleNamespace(cancellation_requested_at=None, state="running")
    files = AsyncMock(return_value=([("artifacts/workspace.tar", 12, "sha256:" + "c" * 64)], []))
    monkeypatch.setattr(execution, "committed_handoff_files", files)
    payload = await execution._committed_result_finalization_payload(session, lease=parent)
    assert payload["trial_state"] == "failed"
    assert "verifier_execution" not in payload["result"]
    files.assert_not_awaited()


async def test_child_score_preserves_parent_timeout_terminal_state():
    parent, attempt = source()
    child, verifier = source("succeeded", "verifier")
    child.parent_lease_id = parent.id
    verifier = verifier.model_copy(update={"verifier_rewards": {"passed": 0.0}})
    trial = SimpleNamespace(
        result={
            "runtime_result": attempt.model_dump(mode="json"),
            "verifier_execution": {"state": "pending", "parent_lease_id": str(parent.id)},
        }
    )
    session = AsyncMock()
    session.get.side_effect = [trial, parent]
    payload = await execution._verifier_finalization_payload(
        session, lease=child, runtime_result=verifier
    )
    assert payload["trial_state"] == "failed"
    assert payload["failure_reason"] == "timed_out"
    assert payload["result"]["reward"] == {"passed": 0.0}
    assert payload["result"]["runtime_result"]["status"] == "timed_out"
