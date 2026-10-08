"""Attempt-bound image evidence cannot borrow a completed result from a retry."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext
from loom.db.schema import Artifact, ServiceExecutionLease, Trial
from loom.pipeline.keys import canonical_digest
from tests.unit.test_execution_runtime_contract import _plan

_NOW = datetime(2026, 10, 8, tzinfo=UTC)


def _evidence():
    plan = _plan()
    trial = Trial(id=uuid4(), team_id=uuid4(), attempt_count=2, result={})
    lease = ServiceExecutionLease(
        id=uuid4(), trial_id=trial.id, team_id=trial.team_id, attempt=2,
        execution_role="attempt", execution_class_id=plan.execution_class_id,
        generation=7, resource_generation=3, runtime_contract_json=plan.canonical_payload(),
        runtime_contract_sha256=canonical_digest(plan.canonical_payload()),
        pod_started_at=None, output_commit_state="not_started",
    )
    return trial, lease


def _report(trial, lease):
    plan = _plan()
    lease.output_commit_state = "committed"
    lease.output_generation = lease.resource_generation
    lease.output_upload_session_id = uuid4()
    lease.output_manifest_sha256 = "sha256:" + "d" * 64
    lease.output_marker_sha256 = "sha256:" + "e" * 64
    trial.result = {
        "schema_version": "loom.service-execution-trial-result.v1",
        "output_manifest_sha256": lease.output_manifest_sha256,
        "output_marker_sha256": lease.output_marker_sha256,
        "runtime_result": {
            "schema_version": "loom.execution-runtime-result.v1",
            "runtime_contract_sha256": lease.runtime_contract_sha256,
            "candidate_sha": plan.candidate_sha,
            "task_revision_sha256": plan.task_revision_sha256,
            "command_identity_sha256": plan.command_identity_sha256,
            "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
            "task_image_ref": plan.task_image_ref, "runtime_image_ref": plan.runtime_image_ref,
            "runtime_binary_sha256": plan.runtime_binary_sha256,
            "execution_class_id": plan.execution_class_id, "status": "succeeded",
            "started_at": _NOW.isoformat(), "finished_at": (_NOW + timedelta(seconds=2)).isoformat(),
            "phases": [], "outputs": [], "partial_evidence": False,
        },
    }
    return Artifact(
        id=uuid4(), trial_id=trial.id, team_id=trial.team_id,
        control_producer_kind="service_execution", control_producer_id=lease.id,
        artifact_upload_session_id=lease.output_upload_session_id,
        provenance={
            "schema_version": "loom.service-execution-trial-bundle-provenance.v1",
            "lease_id": str(lease.id), "generation": lease.resource_generation,
            "runtime_contract_sha256": lease.runtime_contract_sha256,
            "candidate_sha": plan.candidate_sha, "task_revision_sha256": plan.task_revision_sha256,
            "command_identity_sha256": plan.command_identity_sha256,
        },
    )


def _read(trial, lease, artifacts=()):
    from loom_service.trial_execution_provenance import trial_execution_provenance

    return trial_execution_provenance(trial, lease, artifacts=artifacts).model_dump(mode="json")


def test_plan_images_are_not_reported_as_executed_until_start_is_observed():
    trial, lease = _evidence()
    planned = _read(trial, lease)
    assert planned["state"] == "planned"
    assert planned["image_source"] == "frozen_runtime_plan"
    assert planned["attempt"] == 2 and planned["resource_generation"] == 3
    assert planned["task_image_digest"] == "sha256:" + "a" * 64
    assert planned["runtime_image_digest"] == "sha256:" + "b" * 64
    assert planned["agent_image_digest"] is None
    assert planned["candidate_sha"] == "1" * 40
    lease.pod_started_at = _NOW
    started = _read(trial, lease)
    assert started["state"] == "execution_started"
    assert started["started_at"] == _NOW.isoformat()
    assert started["runtime_contract_sha256"] == lease.runtime_contract_sha256


def test_committed_result_requires_its_own_output_artifact():
    trial, lease = _evidence()
    artifact = _report(trial, lease)
    assert _read(trial, lease)["state"] == "planned"
    reported = _read(trial, lease, [artifact])
    assert reported["state"] == "runtime_reported"
    assert reported["started_at"] == _NOW.isoformat()


@pytest.mark.parametrize("drift", [
    "trial", "team", "lease", "generation", "upload", "contract", "candidate",
    "output_generation", "manifest", "marker", "image", "role", "malformed_result",
])
def test_drift_never_upgrades_frozen_plan_to_runtime_report(drift):
    trial, lease = _evidence()
    artifact = _report(trial, lease)
    if drift in {"trial", "team"}:
        setattr(artifact, f"{drift}_id", uuid4())
    elif drift == "lease":
        artifact.control_producer_id = uuid4()
    elif drift == "generation":
        artifact.provenance["generation"] = 1
    elif drift == "upload":
        artifact.artifact_upload_session_id = uuid4()
    elif drift in {"contract", "candidate"}:
        artifact.provenance["runtime_contract_sha256" if drift == "contract" else "candidate_sha"] = "bad"
    elif drift == "output_generation":
        lease.output_generation = 1
    elif drift in {"manifest", "marker"}:
        trial.result[f"output_{drift}_sha256"] = "sha256:" + "f" * 64
    elif drift == "image":
        trial.result["runtime_result"]["task_image_ref"] = "private@sha256:" + "f" * 64
    elif drift == "role":
        trial.result["runtime_result"]["execution_role"] = "verifier"
    else:
        trial.result["runtime_result"] = {"private": "secret"}
    assert _read(trial, lease, [artifact])["state"] == "planned"


def test_latest_retry_cannot_borrow_prior_attempt_result_even_with_same_plan():
    trial, old = _evidence()
    artifact = _report(trial, old)
    _, latest = _evidence()
    latest.trial_id, latest.team_id, latest.attempt = trial.id, trial.team_id, 3
    trial.attempt_count = 3
    assert _read(trial, latest, [artifact])["state"] == "planned"
    latest.pod_started_at = _NOW + timedelta(seconds=3)
    assert _read(trial, latest, [artifact])["state"] == "execution_started"


@pytest.mark.parametrize("broken", ["absent", "malformed", "digest", "foreign_team", "foreign_trial", "old_attempt", "verifier"])
def test_missing_untrusted_or_other_attempt_plan_is_unavailable(broken):
    trial, lease = _evidence()
    if broken == "absent":
        lease = None
    elif broken == "malformed":
        lease.runtime_contract_json = {"image": "secret"}
    elif broken == "digest":
        lease.runtime_contract_sha256 = "sha256:" + "0" * 64
    elif broken in {"foreign_team", "foreign_trial"}:
        setattr(lease, broken.removeprefix("foreign_") + "_id", uuid4())
    elif broken == "old_attempt":
        lease.attempt = 1
    else:
        lease.execution_role = "verifier"
    result = _read(trial, lease)
    assert result["state"] == "unavailable"
    assert result["task_image_digest"] is None
    assert "secret" not in str(result)


def test_projection_does_not_expose_private_plan_fields_or_registry_paths():
    trial, lease = _evidence()
    plan = _plan(task_image_ref="secret.private/team@sha256:" + "a" * 64,
                 agent_image_ref="secret.private/agent@sha256:" + "f" * 64)
    lease.runtime_contract_json = plan.canonical_payload()
    lease.runtime_contract_json["main"]["environment"] = {"LOOM_PRIVATE": "secret-value"}
    lease.runtime_contract_sha256 = canonical_digest(lease.runtime_contract_json)
    result = _read(trial, lease)
    assert result["agent_image_digest"] == "sha256:" + "f" * 64
    assert "secret" not in str(result)
    assert "environment" not in str(result)
    assert "argv" not in str(result)


@pytest.mark.parametrize("allowed_scope", [True, False])
async def test_trial_detail_denies_foreign_team_or_missing_read_scope_before_provenance(allowed_scope):
    from loom_service.routes.trials import get_trial

    trial, _ = _evidence()
    ctx = AuthContext(token_hash=b"x" * 32, type="team", scopes=["read:own"] if allowed_scope else [],
                      team_id=uuid4() if allowed_scope else trial.team_id, expires_at=None)
    session = AsyncMock()
    session.execute.return_value = MagicMock(scalar_one_or_none=lambda: trial)
    with pytest.raises(HTTPException) as error:
        await get_trial(SimpleNamespace(), (session, ctx), trial.id)
    assert error.value.status_code == 403
