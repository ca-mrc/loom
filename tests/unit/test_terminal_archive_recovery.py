"""Only a qualified deleted attempt and its exact verifier may be archived."""
from copy import copy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom_control_plane.pending_archive_recovery import RecoveryRefusedError
from loom_control_plane.terminal_archive_recovery import (
    TerminalArchiveRecoveryRequest,
    qualify_deleted,
)
from tests.unit.test_pending_archive_recovery import qualified as oracle_inputs


def qualified():
    old, parent, trial, _, now = oracle_inputs()
    child = copy(parent)
    child.id = uuid4()
    child.execution_role = "verifier"
    child.parent_lease_id = parent.id
    trial.result = {**trial.result, "verifier_execution": {"lease_id": str(child.id), "state": "committed",
        "output_manifest_sha256": child.output_manifest_sha256, "output_marker_sha256": child.output_marker_sha256}}
    request = TerminalArchiveRecoveryRequest(
        team_id=old.team_id, trial_id=old.trial_id, lease_id=old.lease_id, child_lease_id=child.id,
        artifact_id=old.artifact_id, upload_session_id=old.upload_session_id,
        child_upload_session_id=uuid4(), attempt=1, generation=1, child_generation=1,
        output_manifest_sha256=old.output_manifest_sha256, output_marker_sha256=old.output_marker_sha256,
        child_output_manifest_sha256=old.output_manifest_sha256, child_output_marker_sha256=old.output_marker_sha256,
        trial_state=trial.state, trial_result_sha256=digest_bytes(canonical_document(trial.result)),
        trial_config_sha256=old.trial_config_sha256, task_config_sha256=old.task_config_sha256,
        failure_reason=None, failure_message_sha256=digest_bytes(canonical_document(None)), finished_at=None,
        operation_id=uuid4(), cluster_id=old.cluster_id, namespace=old.namespace,
        installed_candidate=old.installed_candidate, candidate_sha=old.candidate_sha,
        image_ref=old.image_ref, installed_image_ref=old.installed_image_ref, schema_head="0175",
    )
    return request, parent, trial, child, now


@pytest.mark.parametrize("state", ["materializing", "failed", "cancelled"])
def test_deleted_attempt_allows_actual_terminal_outcome(state):
    _, parent, trial, child, _ = qualified()
    trial.state = state
    qualify_deleted(parent, trial, child)


@pytest.mark.parametrize("row,field,value", [
    ("parent", "cleanup_state", "pending"), ("parent", "deleted_at", None),
    ("parent", "finalized_at", None), ("parent", "revoked_at", None),
    ("parent", "materialization_state", "running"),
    ("parent", "materialization_claim_expires_at", datetime.now(UTC) - timedelta(seconds=1)),
    ("parent", "output_commit_state", "uploading"), ("parent", "output_generation", 2),
    ("parent", "source_cleanup_state", "retained"),
    ("child", "team_id", uuid4()), ("child", "trial_id", uuid4()), ("child", "attempt", 2),
    ("child", "parent_lease_id", uuid4()), ("child", "materialization_state", "running"),
    ("child", "materialization_claim_id", uuid4()), ("child", "cleanup_state", "pending"),
    ("trial", "attempt_count", 2), ("trial", "state", "running"), ("trial", "state", "succeeded"),
    ("trial", "result", {"verifier_execution": {"lease_id": str(uuid4()), "state": "committed"}}),
])
def test_active_changed_foreign_or_superseded_work_is_refused(row, field, value):
    _, parent, trial, child, _ = qualified()
    setattr({"parent": parent, "trial": trial, "child": child}[row], field, value)
    with pytest.raises(RecoveryRefusedError):
        qualify_deleted(parent, trial, child)


def test_job_is_pinned_to_existing_node_and_has_no_runtime_authority():
    from scripts.ops.nebius_terminal_archive_recovery import recovery_job

    from tests.ops.test_nebius_archive_recovery import inputs

    request, *_ = qualified()
    _, cp = inputs()
    cp["spec"]["template"]["spec"]["containers"][0]["image"] = request.installed_image_ref
    job = recovery_job(request, cp, node_name="existing-management-node")
    pod = job["spec"]["template"]["spec"]
    assert pod["nodeSelector"]["kubernetes.io/hostname"] == "existing-management-node"
    assert pod["automountServiceAccountToken"] is False
    assert job["spec"]["backoffLimit"] == 0
    container = pod["containers"][0]
    assert container["command"][2] == "loom_control_plane.terminal_archive_recovery"
    assert "MUST-NOT-COPY" not in str(container)
    with pytest.raises(ValueError, match="existing_control_plane_node_required"):
        recovery_job(request, cp, node_name="")


def test_inspection_program_executes_the_synchronous_qualifier():
    import asyncio
    import contextlib
    import io
    from types import SimpleNamespace

    from scripts.ops.nebius_terminal_archive_recovery import inspect_archive

    request, parent, trial, child, _ = qualified()
    trial.failure_reason = trial.failure_message = trial.finished_at = None
    parent.output_upload_session_id = request.upload_session_id
    child.output_upload_session_id = request.child_upload_session_id
    artifact = SimpleNamespace(id=request.artifact_id, team_id=parent.team_id, trial_id=parent.trial_id)
    upload = SimpleNamespace(team_id=parent.team_id, state="committed",
        canonical_manifest_json={"artifacts": [{"stored_files": [{"size_bytes": 3}]}]})

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def execute(self, *_): return None
        async def get(self, model, key, **kwargs):
            if model.__name__ == "ServiceExecutionLease":
                return parent if key == parent.id else child
            if model.__name__ == "Trial":
                return trial
            return upload
        async def scalar(self, *_): return artifact
        async def scalars(self, *_): return SimpleNamespace(one=lambda: "0175")

    class Engine:
        async def dispose(self): pass

    async def snapshot(*_, **__): return SimpleNamespace(config={"snapshot": "unchanged"})

    class API:
        def run(self, *args):
            program = args[-1]
            # Execute definitions and their actual binding/qualifier; mock only
            # transport/settings/database I/O at the supported inspection surface.
            definitions = program.split("try:\n asyncio.run(inspect())", 1)[0]
            namespace = {}
            exec(compile(definitions, "installed-inspection", "exec"), namespace)
            namespace["TerminalArchiveBinding"].model_rebuild(_types_namespace=namespace)
            assert callable(namespace["qualify_deleted"])
            namespace.update(ControlPlaneSettings=lambda: SimpleNamespace(db_engine_url="test", db_engine_connect_args={}),
                create_async_engine=lambda *_, **__: Engine(),
                async_sessionmaker=lambda *_, **__: SimpleNamespace(begin=Session),
                resolve_service_execution_task_snapshot=snapshot)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                asyncio.run(namespace["inspect"]())
            return output.getvalue()
    result = inspect_archive(API(), namespace=request.namespace, team_id=request.team_id, lease_id=request.lease_id)
    assert result["read_only"] and result["schema_head"] == "0175"
    assert result["binding"]["child_lease_id"] == str(child.id)
    assert "failure_message" not in result["binding"]


def test_publication_requires_the_exact_successful_native_image(monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from scripts.ops import nebius_terminal_archive_recovery as target

    request, *_ = qualified()
    monkeypatch.setattr(target, "select_publication", lambda _: {"status": "ready", "sha": request.candidate_sha, "artifact": "candidate"})
    monkeypatch.setattr(target, "candidate_follows", lambda *_: True)
    monkeypatch.setattr(target, "candidate_schema_head", lambda _: "0176")
    monkeypatch.setattr(target, "canonical_repository_name", lambda: "ca-mrc/loom")
    monkeypatch.setattr(target, "validate_identity", lambda *_, **__: None)
    def download(command, **_):
        destination = Path(command[command.index("--dir") + 1])
        (destination / "candidate.json").write_text(json.dumps({"candidate_sha": request.candidate_sha,
            "run_id": "123", "images": {"control_plane": {"image_ref": "wrong-image"}}}))
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(target.subprocess, "run", download)
    with pytest.raises(ValueError, match="published_image_changed"):
        target.qualify_publication(request, "123")


def test_terminal_job_reuses_uncertain_submission_readback_without_duplicate(tmp_path):
    from scripts.ops.nebius_archive_recovery import submit_once
    from scripts.ops.nebius_terminal_archive_recovery import recovery_job

    from tests.ops.test_nebius_archive_recovery import API, inputs

    request, *_ = qualified()
    _, cp = inputs()
    cp["spec"]["template"]["spec"]["containers"][0]["image"] = request.installed_image_ref
    job = recovery_job(request, cp, node_name="existing-management-node")
    api = API()
    api.lose_response = True
    with pytest.raises(RuntimeError, match="lost response"):
        submit_once(api, job, tmp_path / "evidence")
    api.lose_response = False
    assert submit_once(api, job, tmp_path / "evidence")["job_uid"] == "test-job-uid"
    assert len(api.created) == 1
