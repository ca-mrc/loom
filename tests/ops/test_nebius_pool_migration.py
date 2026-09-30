"""The protected parent retains all idle guards before staging closed authority."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_pool_registration import stage_pool_registration
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_registration import request as registration_request


def migration_request():
    from scripts.ops.nebius_pool_migration import PoolGuardTarget, PoolMigrationRequest

    registration = registration_request()
    targets = []
    for index, participant in enumerate(registration.spec.participants):
        namespace = f"loom-platform-{index}"
        controller = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {
            "namespace": namespace, "name": "loom-control-plane", "uid": str(uuid4()), "resourceVersion": "1"},
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "loom-control-plane"}},
                "template": {"metadata": {"labels": {"app": "loom-control-plane"}}, "spec": {
                    "containers": [{"name": "loom-control-plane", "image": "registry.example/cp@sha256:" + "c" * 64}]}}}}
        targets.append(PoolGuardTarget(participant_id=participant.participant_id,
            namespace=namespace, namespace_uid=uuid4(), controller=controller))
    return PoolMigrationRequest(registration=registration, guards=tuple(targets))


class MigrationAPI:
    """External boundary double; actual journal and registration stage stay real."""

    def __init__(self, request):
        self.request = request
        self.guards = {}
        self.actions = []
        self.registration = PhaseAPI(request.registration.binding)
        self.busy = None
        self.lost_reply = None
        self.commit_lost = True
        self.pending_registration = False

    def preflight(self, request):
        assert request == self.request

    def guard(self, target, action):
        self.actions.append((target.participant_id, action))
        owner = str(self.request.registration.spec.operation_id)
        if action == "observe":
            current = self.guards.get(target.participant_id)
            return {"status": "open" if current is None else "held" if current == owner else "skipped_locked"}
        assert action == "acquire"  # This stage has no release or writer activation.
        if self.busy == target.participant_id:
            return {"status": "skipped_busy"}
        if self.lost_reply == target.participant_id:
            if self.commit_lost:
                self.guards[target.participant_id] = owner
            raise RuntimeError("private-marker")
        self.guards[target.participant_id] = owner
        return {"status": "acquired"}

    def register(self, state_dir):
        assert set(self.guards) == {row.participant_id for row in self.request.guards}
        stage_pool_registration(request=self.request.registration, api=self.registration, state_dir=state_dir)
        if self.pending_registration:
            return None
        record = json.loads((state_dir / "stage.json").read_text())
        job, = [row for row in record["resources"].values() if row["desired"]["kind"] == "Job"]
        from loom_service.pool_management.capacity import digest

        spec = self.request.registration.spec
        return {"job_uid": job["uid"], "pod_uid": "b9fe1340-2941-4c37-835c-3a0f668dc25a",
            "registration": {"schema_version": "loom.pool-installation-receipt.v1", "operation_id": str(spec.operation_id),
                "pool_id": str(spec.pool_id), "installation_sha256": digest(spec.model_dump(mode="json")),
                "mode": "closed", "participants": 3, "machines": 5}}


def run(request, api, tmp_path):
    from scripts.ops.nebius_pool_migration import close_and_register_pool

    return close_and_register_pool(request=request, api=api, state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def test_all_participants_close_before_registration_and_replay_preserves_guards(tmp_path):
    request = migration_request()
    api = MigrationAPI(request)
    result = run(request, api, tmp_path)
    assert result["status"] == "pool_registered_closed"
    assert result["writer_migration_complete"] is False
    assert run(request, api, tmp_path) == result
    assert len([row for row in api.actions if row[1] == "acquire"]) == 3
    assert len(api.registration.creates) == 2
    record = json.loads((tmp_path / "state/migration.json").read_text())
    assert all(value == "held" for value in record["guards"].values())
    assert record["registration"]["proof"] is not None


def test_busy_environment_retains_earlier_guards_and_does_not_stage_registration(tmp_path):
    request = migration_request()
    api = MigrationAPI(request)
    api.busy = request.guards[1].participant_id
    assert run(request, api, tmp_path)["status"] == "pending_idle"
    assert len(api.guards) == 1 and not api.registration.creates
    api.busy = None
    assert run(request, api, tmp_path)["status"] == "pool_registered_closed"
    assert len(api.guards) == 3


@pytest.mark.parametrize("committed", [False, True])
def test_lost_guard_reply_requires_owner_readback_never_second_acquire(tmp_path, committed):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    request = migration_request()
    api = MigrationAPI(request)
    api.lost_reply = request.guards[0].participant_id
    api.commit_lost = committed
    if committed:
        assert run(request, api, tmp_path)["status"] == "pool_registered_closed"
    else:
        for _ in range(2):
            with pytest.raises(PoolMigrationError):
                run(request, api, tmp_path)
        assert not api.registration.creates
    assert api.actions.count((request.guards[0].participant_id, "acquire")) == 1


@pytest.mark.parametrize("damage", ["foreign_guard", "lost_guard", "lost_parent", "lost_child", "changed_candidate", "missing_participant"])
def test_drift_or_lost_recovery_evidence_cannot_reopen_any_phase(tmp_path, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    request = migration_request()
    api = MigrationAPI(request)
    run(request, api, tmp_path)
    original_creates = len(api.registration.creates)
    original_acquires = len([row for row in api.actions if row[1] == "acquire"])
    if damage == "foreign_guard":
        api.guards[request.guards[0].participant_id] = str(uuid4())
    elif damage == "lost_guard":
        api.guards.pop(request.guards[0].participant_id)
    elif damage == "lost_parent":
        (tmp_path / "state/migration.json").unlink()
    elif damage == "lost_child":
        (tmp_path / "state/registration/stage.json").unlink()
    elif damage == "changed_candidate":
        candidate = copy.deepcopy(request.registration.candidate)
        candidate["candidate_sha"] = "2" * 40
        request = replace(request, registration=replace(request.registration, candidate=candidate))
    elif damage == "missing_participant":
        request = replace(request, guards=request.guards[:-1])
    with pytest.raises(PoolMigrationError) as error:
        run(request, api, tmp_path)
    assert "private-marker" not in str(error.value)
    assert len(api.registration.creates) == original_creates
    assert len([row for row in api.actions if row[1] == "acquire"]) == original_acquires


def test_pending_job_reuses_same_registration_and_never_releases_guards(tmp_path):
    request = migration_request()
    api = MigrationAPI(request)
    api.pending_registration = True
    assert run(request, api, tmp_path)["status"] == "pending_registration"
    api.pending_registration = False
    assert run(request, api, tmp_path)["status"] == "pool_registered_closed"
    assert len(api.registration.creates) == 2
    assert len([row for row in api.actions if row[1] == "acquire"]) == 3
