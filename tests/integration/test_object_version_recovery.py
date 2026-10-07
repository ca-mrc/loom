"""Historical metadata repair uses real versioned S3, PostgreSQL and admin HTTP."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import event, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm.attributes import flag_modified

from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    ArtifactUploadSession,
    DataLifecycleAuthority,
    DataLifecycleGcAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    ServiceExecutionLease,
    Token,
    Trial,
)
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom_control_plane.app import _load_admin_secret_verifier, create_app
from loom_control_plane.config import ControlPlaneSettings
from tests.integration.test_service_execution_leases import _reserve, _seed_ready_trial
from tests.integration.test_token_admin import RAW_ADMIN_TOKEN, _write_admin_secret

URL = "/admin/object-version-recovery"
HEADERS = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}


def digest(value):
    return "sha256:" + hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


@pytest.fixture
async def recovery(monkeypatch, tmp_path, isolated_migration_postgres_url, shared_minio, request):
    cfg = shared_minio.get_config()
    bucket = "recovery-" + uuid4().hex
    s3 = boto3.client("s3", endpoint_url=f"http://{cfg['endpoint']}",
        aws_access_key_id=cfg["access_key"], aws_secret_access_key=cfg["secret_key"])
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    secret = tmp_path / "admin.toml"
    _write_admin_secret(secret)
    for key, value in {
        "LOOM_ENV": "development", "LOOM_NAMESPACE": "loom",
        "LOOM_CP_DB_URL": isolated_migration_postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": f"http://{cfg['endpoint']}",
        "LOOM_CP_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_CP_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_CP_LLM_GATEWAY_URL": "http://gateway.test/",
        "LOOM_CP_ADMIN_SECRET_FILE": str(secret),
        "LOOM_CP_ARTIFACTS_BUCKET": bucket, "LOOM_CP_TRAJECTORIES_BUCKET": bucket,
    }.items():
        monkeypatch.setenv(key, value)
    settings = ControlPlaneSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app.state.settings = settings
    app.state.session_factory = sessions
    app.state.minio_client = s3
    app.state.admin_secret_verifier = _load_admin_secret_verifier(settings)
    now = datetime.now(UTC)
    artifact_id, upload_id, authority_id = uuid4(), uuid4(), uuid4()
    objects, versions, bodies = [], [], []
    async with sessions() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        trial = await session.get(Trial, trial_id)
        result_body = b'{"reward":0}'
        result_sha = digest_bytes(result_body)
        stored_files = [{"file_index": 0, "relative_path": "result.json", "role": "semantic_document",
                         "archive_format": "none", "media_type": "application/json",
                         "size_bytes": len(result_body), "sha256": result_sha}]
        artifact_manifest = {"schema_version": "loom.artifact-manifest.v1", "artifact_id": str(artifact_id),
            "artifact_name": "trial_bundle", "artifact_type": "loom.trial-artifact-bundle.v1",
            "content_sha256": result_sha, "stored_size_bytes": len(result_body),
            "unpacked_size_bytes": len(result_body), "file_count": 1, "stored_files": stored_files,
            "lineage_artifact_ids": [], "lineage_digests": []}
        producer = {"commit_kind": "service_execution_output", "team_id": str(trial.team_id),
            "service_execution_lease_id": str(lease.id), "service_execution_generation": 1,
            "service_execution_role": "attempt", "runtime_contract_sha256": lease.runtime_contract_sha256,
            "candidate_sha": lease.runtime_contract_json["candidate_sha"],
            "task_revision_sha256": lease.runtime_contract_json["task_revision_sha256"],
            "command_identity_sha256": lease.runtime_contract_json["command_identity_sha256"],
            "input_lineage_artifact_ids": [], "input_lineage_digests": []}
        root_manifest = {"schema_version": "loom.artifact-commit-manifest.v1", "session_id": str(upload_id),
            "commit_kind": "service_execution_output", "producer_identity": producer,
            "artifacts": [{"artifact_id": str(artifact_id), "artifact_name": "trial_bundle",
                           "artifact_type": "loom.trial-artifact-bundle.v1",
                           "manifest_sha256": canonical_digest(artifact_manifest),
                           "content_sha256": result_sha, "stored_files": stored_files}],
            "total_bytes": len(result_body), "input_lineage_artifact_ids": [], "input_lineage_digests": [],
            "request_digest": "sha256:" + "e" * 64}
        source_fault = getattr(request.node, "callspec", SimpleNamespace(params={})).params.get("source_fault")
        if source_fault == "manifest":
            root_manifest = {}
        elif source_fault == "artifact":
            root_manifest["artifacts"][0]["artifact_id"] = str(uuid4())
        elif source_fault == "producer":
            producer["service_execution_lease_id"] = str(uuid4())
        elif source_fault == "runtime":
            producer["candidate_sha"] = "b" * 40
        prefix = f"trials/{trial.team_id}/{trial_id}/attempts/1/bundles/{artifact_id}/"
        trajectory_prefix = f"{trial.team_id}/{trial_id}/attempts/1/"
        for key, body in [(prefix + "files/result.json", result_body),
                          (prefix + "source/_manifest.json", canonical_document(root_manifest)),
                          (trajectory_prefix + "events.jsonl", b'{"seq":1}\n'),
                          (trajectory_prefix + "atif.json", b'{"steps":[]}')]:
            version = s3.put_object(Bucket=bucket, Key=key, Body=body)["VersionId"]
            objects.append(DataLifecycleObject(id=uuid4(), authority_id=authority_id,
                environment="development", namespace="loom", bucket=bucket, object_key=key,
                version_id=None, content_sha256=hashlib.sha256(body).hexdigest(),
                size_bytes=len(body), created_at=now))
            versions.append(version)
            bodies.append(body)
        file_rows = [{"relative_path": name, "media_type": "application/json",
                      "bucket": bucket, "key": obj.object_key, "version_id": None,
                      "sha256": "sha256:" + obj.content_sha256, "size_bytes": obj.size_bytes}
                     for name, obj in zip(("result.json", "source/_manifest.json"), objects[:2], strict=True)]
        storage = {"schema_version": "loom.canonical-trial-bundle-storage.v1", "attempt": 1,
                   "source_upload_session_id": str(upload_id),
                   "files": file_rows[:1], "source_evidence": file_rows[1:]}
        index = {"schema_version": "1", "trial_id": str(trial_id),
                 "team_id": str(trial.team_id), "task_id": trial.task_id, "attempt": 1,
                 "artifacts": copy.deepcopy(file_rows[:1])}
        for name, obj in zip(("trajectory", "atif"), objects[2:], strict=True):
            index.update({f"{name}_uri": f"s3://{bucket}/{obj.object_key}",
                          f"{name}_sha256": obj.content_sha256,
                          f"{name}_size_bytes": obj.size_bytes, f"{name}_version_id": None})
        if getattr(request.node, "callspec", SimpleNamespace(params={})).params.get("historical_fields"):
            for rows in (storage["files"], storage["source_evidence"], index["artifacts"]):
                for row in rows:
                    row.pop("version_id")
        if getattr(request.node, "callspec", SimpleNamespace(params={})).params.get("legacy_index_attempt"):
            index.pop("attempt")
        state = "running" if getattr(request.node, "callspec", SimpleNamespace(params={})).params.get(
            "change") == "nonterminal" else "failed"
        trial.state, trial.failure_reason, trial.result = state, "verifier_error", {"reward": 0}
        trial.trajectory_index = index
        lease.output_commit_state = lease.materialization_state = "committed"
        lease.output_upload_session_id, lease.output_generation = upload_id, 1
        lease.output_manifest_sha256 = canonical_digest(root_manifest)
        lease.output_marker_sha256 = canonical_digest({"schema_version": "loom.artifact-commit-marker.v1",
            "commit_kind": "service_execution_output", "manifest_sha256": lease.output_manifest_sha256,
            "session_id": str(upload_id)})
        lease.output_committed_at = lease.materialization_committed_at = now
        lease.materialization_attempts = 1
        lease.canonical_trajectory_sha256 = "sha256:" + objects[2].content_sha256
        lease.canonical_atif_sha256 = "sha256:" + objects[3].content_sha256
        session.add(ArtifactUploadSession(id=upload_id, team_id=trial.team_id,
            commit_kind="service_execution_output", service_execution_lease_id=lease.id,
            service_execution_generation=1, service_execution_role="attempt",
            service_execution_runtime_contract_sha256=lease.runtime_contract_sha256,
            service_execution_candidate_sha=producer["candidate_sha"],
            service_execution_task_revision_sha256=producer["task_revision_sha256"],
            service_execution_command_identity_sha256=producer["command_identity_sha256"],
            idempotency_key=str(upload_id), request_digest="sha256:" + "e" * 64,
            prefix=f"source/{upload_id}/", state="committed", expected_total_max_bytes=1024,
            expires_at=now + timedelta(days=1), committed_at=now, canonical_manifest_json=root_manifest,
            actual_total_bytes=len(result_body),
            manifest_sha256=lease.output_manifest_sha256,
            committed_marker_sha256=lease.output_marker_sha256))
        session.add(DataLifecycleAuthority(id=authority_id, environment="development",
            namespace="loom", team_id=trial.team_id, data_class="artifact", owner_kind="artifact",
            owner_id=str(artifact_id), pinned=True, created_at=now))
        await session.flush()
        session.add(Artifact(id=artifact_id, team_id=trial.team_id, trial_id=trial_id,
            artifact_type="loom.trial-artifact-bundle.v1", name="trial_bundle",
            control_producer_kind="service_execution", control_producer_id=lease.id,
            artifact_upload_session_id=upload_id, manifest_sha256=canonical_digest(artifact_manifest),
            stored_size_bytes=len(result_body), unpacked_size_bytes=len(result_body), file_count=1,
            content_hash=result_sha, storage=storage,
            artifact_metadata={"materialization_state": "committed"},
            lifecycle_authority_id=authority_id, created_at=now))
        session.add_all(objects)
        await session.commit()
    payload = {"operation_id": str(uuid4()), "trial_id": str(trial_id),
               "artifact_id": str(artifact_id), "expected_storage_sha256": digest(storage),
               "expected_index_sha256": digest(index),
               "objects": [{"registry_id": str(obj.id), "version_id": version}
                           for obj, version in zip(objects, versions, strict=True)]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://cp") as client:
        yield SimpleNamespace(client=client, app=app, sessions=sessions, s3=s3, bucket=bucket,
            payload=payload, engine=engine, storage=storage, index=index, objects=objects, versions=versions,
            bodies=bodies, lease_id=lease.id, upload_id=upload_id, authority_id=authority_id)
    await engine.dispose()


async def snapshot(r):
    async with r.sessions() as session:
        artifact = await session.get(Artifact, UUID(r.payload["artifact_id"]))
        trial = await session.get(Trial, UUID(r.payload["trial_id"]))
        rows = [await session.get(DataLifecycleObject, obj.id) for obj in r.objects]
        audits = list((await session.scalars(select(AdminAuditEvent))).all())
        return (artifact.storage, trial.trajectory_index, [row.version_id for row in rows],
                [(row.id, row.event_metadata) for row in audits],
                (trial.state, trial.failure_reason, trial.result, trial.attempt_count))


@pytest.mark.parametrize("historical_fields", [False, True])
@pytest.mark.parametrize("legacy_index_attempt", [False, True])
async def test_preview_apply_and_replay_repair_every_published_mirror(
    recovery, historical_fields, legacy_index_attempt,
):
    r = recovery
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["status"] == "preview"
    assert len(preview["plan"]["objects"]) == 4
    assert await snapshot(r) == before

    payload = {**r.payload, "apply": True, "plan_sha256": preview["plan_sha256"]}
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "applied"
    after = await snapshot(r)
    assert after[0]["files"][0]["version_id"] == r.versions[0]
    assert after[0]["source_evidence"][0]["version_id"] == r.versions[1]
    assert after[1]["artifacts"][0]["version_id"] == r.versions[0]
    assert after[1]["trajectory_version_id"] == r.versions[2]
    assert after[1]["atif_version_id"] == r.versions[3]
    assert ("attempt" not in after[1]) is legacy_index_attempt
    assert after[2] == r.versions
    assert len(after[3]) == 1
    assert str(after[3][0][0]) == r.payload["operation_id"]
    assert after[4] == before[4] == ("failed", "verifier_error", {"reward": 0}, 1)
    replay = await r.client.post(URL, headers=HEADERS, json=payload)
    assert replay.status_code == 200, replay.text
    assert replay.json()["status"] == "replayed"
    assert await snapshot(r) == after


@pytest.mark.parametrize("index_attempt", [None, True, False, 1.0, "1", 0, 2])
async def test_explicit_index_attempt_must_be_exact_integer(recovery, index_attempt):
    r = recovery
    async with r.sessions() as session:
        trial = await session.get(Trial, UUID(r.payload["trial_id"]))
        trial.trajectory_index = {**trial.trajectory_index, "attempt": index_attempt}
        flag_modified(trial, "trajectory_index")
        r.payload["expected_index_sha256"] = digest(trial.trajectory_index)
        await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "published_owner_conflict"
    assert await snapshot(r) == before


@pytest.mark.parametrize("storage_attempt", [True, 1.0])
async def test_legacy_index_requires_integer_canonical_storage_attempt(recovery, storage_attempt):
    r = recovery
    async with r.sessions() as session:
        artifact = await session.get(Artifact, UUID(r.payload["artifact_id"]))
        trial = await session.get(Trial, UUID(r.payload["trial_id"]))
        artifact.storage = {**artifact.storage, "attempt": storage_attempt}
        flag_modified(artifact, "storage")
        trial.trajectory_index = {key: value for key, value in trial.trajectory_index.items() if key != "attempt"}
        r.payload["expected_storage_sha256"] = digest(artifact.storage)
        r.payload["expected_index_sha256"] = digest(trial.trajectory_index)
        await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "published_owner_conflict"
    assert await snapshot(r) == before


@pytest.mark.parametrize("change", ["schema", "trial", "team", "task", "trajectory", "atif", "bundle"])
async def test_legacy_index_attempt_omission_preserves_identity_guards(recovery, change):
    r = recovery
    async with r.sessions() as session:
        trial = await session.get(Trial, UUID(r.payload["trial_id"]))
        artifact = await session.get(Artifact, UUID(r.payload["artifact_id"]))
        index = copy.deepcopy(trial.trajectory_index)
        index.pop("attempt")
        if change == "schema":
            index["schema_version"] = "2"
        elif change in {"trial", "team", "task"}:
            index[change + "_id"] = str(uuid4())
        elif change in {"trajectory", "atif"}:
            index[change + "_uri"] = index[change + "_uri"].replace("/attempts/1/", "/attempts/2/")
        else:
            storage = copy.deepcopy(artifact.storage)
            storage["files"][0]["key"] = storage["files"][0]["key"].replace("/attempts/1/", "/attempts/2/")
            artifact.storage = storage
            index["artifacts"] = copy.deepcopy(storage["files"])
            r.payload["expected_storage_sha256"] = digest(storage)
        trial.trajectory_index = index
        r.payload["expected_index_sha256"] = digest(index)
        await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


async def applying(r):
    preview = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert preview.status_code == 200, preview.text
    return {**r.payload, "apply": True, "plan_sha256": preview.json()["plan_sha256"]}


@pytest.mark.parametrize("change", ["attempt", "nonterminal", "upload", "unpinned", "foreign_scope",
                                   "registry_hash", "oversized", "competing", "gc_object", "gc_authority"])
async def test_owner_retention_registry_and_gc_guards(recovery, change):
    r = recovery
    async with r.sessions() as session:
        trial = await session.get(Trial, UUID(r.payload["trial_id"]))
        authority = await session.get(DataLifecycleAuthority, r.authority_id)
        obj = await session.get(DataLifecycleObject, r.objects[0].id)
        if change == "attempt":
            trial.attempt_count += 1
        elif change == "nonterminal":
            assert trial.state == "running"
        elif change == "upload":
            upload = await session.get(ArtifactUploadSession, r.upload_id)
            upload.service_execution_generation += 1
        elif change == "unpinned":
            authority.pinned, authority.expires_at = False, datetime.now(UTC) + timedelta(days=1)
        elif change == "foreign_scope":
            authority.namespace = "other"
        elif change == "registry_hash":
            obj.content_sha256 = "0" * 64
        elif change == "oversized":
            obj.size_bytes = 256 * 1024 * 1024 + 1
        elif change == "competing":
            session.add(DataLifecycleObject(authority_id=authority.id, environment="development",
                namespace="foreign", bucket=obj.bucket, object_key=obj.object_key, version_id="other",
                content_sha256=obj.content_sha256, size_bytes=obj.size_bytes, created_at=obj.created_at))
        else:
            run = DataLifecycleGcRun(id=uuid4(), environment="staging", namespace="loom",
                mutation_epoch_before=0, dry_run=False, requested_by="test", policy={}, inventory={})
            session.add(run)
            await session.flush()
            session.add(DataLifecycleGcItem(gc_run_id=run.id, object_id=obj.id, deletion_token=uuid4())
                if change == "gc_object" else DataLifecycleGcAuthority(
                    gc_run_id=run.id, authority_id=authority.id, deletion_token=uuid4()))
        await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("credential,status", [(None, 401), ("worker", 403), ("team", 403), ("rate_card", 401)])
async def test_only_singleton_administrator_can_recover(recovery, credential, status):
    r = recovery
    headers = {}
    if credential:
        token = "loom_w_" + uuid4().hex
        async with r.sessions() as session:
            trial = await session.get(Trial, UUID(r.payload["trial_id"]))
            session.add(Token(token_hash=hashlib.sha256(token.encode()).digest(),
                type="team" if credential == "team" else "worker",
                team_id=trial.team_id if credential == "team" else None,
                scopes=["admin:rate_cards"] if credential == "rate_card" else ["read:own"],
                issued_at=datetime.now(UTC)))
            await session.commit()
        headers = {"Authorization": f"Bearer {token}"}
    before = await snapshot(r)
    response = await r.client.post(URL, headers=headers, json=r.payload)
    assert response.status_code == status, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("change", ["duplicates", "null", "empty", "too_many", "no_plan", "extra"])
async def test_request_is_bounded_and_explicit(recovery, change):
    r = recovery
    payload = copy.deepcopy(r.payload)
    if change == "duplicates":
        payload["objects"].append(payload["objects"][0])
    elif change == "null":
        payload["objects"][0]["version_id"] = "null"
    elif change == "empty":
        payload["objects"] = []
    elif change == "too_many":
        payload["objects"] *= 9
    elif change == "no_plan":
        payload["apply"] = True
    else:
        payload["force"] = True
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 422, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("change", ["metadata", "registration"])
async def test_apply_rechecks_state_after_storage_verification(recovery, monkeypatch, change):
    r = recovery
    payload = await applying(r)
    recovery_service = r.app.state.object_version_recovery
    verify = recovery_service._verify

    async def change_after_verify(client, plan):
        await verify(client, plan)
        async with r.sessions() as session:
            if change == "metadata":
                trial = await session.get(Trial, UUID(r.payload["trial_id"]))
                trial.result = {"concurrent_update": True}
            else:
                obj = r.objects[0]
                session.add(DataLifecycleObject(authority_id=r.authority_id, environment="development",
                    namespace="loom", bucket=obj.bucket, object_key=obj.object_key,
                    version_id=r.versions[0], content_sha256=obj.content_sha256,
                    size_bytes=obj.size_bytes, created_at=obj.created_at))
            await session.commit()

    monkeypatch.setattr(recovery_service, "_verify", change_after_verify)
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 409, response.text
    after = await snapshot(r)
    assert after[:4] == before[:4]


async def test_audit_failure_rolls_back_all_published_and_registry_changes(recovery):
    r = recovery
    payload = await applying(r)
    before = await snapshot(r)

    def reject_audit(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO admin_audit_events"):
            raise IntegrityError("injected audit failure", {}, ValueError("secret must not escape"))

    event.listen(r.engine.sync_engine, "before_cursor_execute", reject_audit)
    try:
        response = await r.client.post(URL, headers=HEADERS, json=payload)
    finally:
        event.remove(r.engine.sync_engine, "before_cursor_execute", reject_audit)
    assert response.status_code == 409, response.text
    assert "secret" not in response.text
    assert await snapshot(r) == before


async def test_concurrent_apply_is_audited_once_and_replay_rejects_drift(recovery):
    r = recovery
    payload = await applying(r)
    responses = await asyncio.gather(*(r.client.post(URL, headers=HEADERS, json=payload) for _ in range(2)))
    assert {response.status_code for response in responses} == {200}
    assert {response.json()["status"] for response in responses} == {"applied", "replayed"}
    assert len((await snapshot(r))[3]) == 1
    changed = copy.deepcopy(payload)
    changed["objects"][0]["version_id"] = "different"
    collision = await r.client.post(URL, headers=HEADERS, json=changed)
    assert collision.status_code == 409, collision.text
    async with r.sessions() as session:
        lease = await session.get(ServiceExecutionLease, r.lease_id)
        lease.updated_at += timedelta(seconds=1)
        await session.commit()
    before = await snapshot(r)
    drift = await r.client.post(URL, headers=HEADERS, json=payload)
    assert drift.status_code == 409, drift.text
    assert drift.json()["detail"] == "replay_state_drift"
    assert await snapshot(r) == before


async def test_lock_fences_conflicting_registry_inserts(recovery, monkeypatch):
    from loom_control_plane import object_version_recovery as module

    r = recovery
    payload = await applying(r)
    load = module._load
    fenced = []

    async def insert_under_fence(session, request, *, locked):
        state = await load(session, request, locked=locked)
        if locked:
            async with r.sessions() as other:
                await other.execute(text("SET LOCAL lock_timeout = '50ms'"))
                obj = r.objects[0]
                other.add(DataLifecycleObject(authority_id=r.authority_id, environment="development",
                    namespace="loom", bucket=obj.bucket, object_key=obj.object_key,
                    version_id=r.versions[0], content_sha256=obj.content_sha256,
                    size_bytes=obj.size_bytes, created_at=obj.created_at))
                with pytest.raises(DBAPIError) as exc:
                    await other.flush()
                assert exc.value.orig.sqlstate == "55P03"
                fenced.append(True)
        return state

    monkeypatch.setattr(module, "_load", insert_under_fence)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    assert fenced == [True]


async def test_cancelled_verification_cannot_apply_later(recovery, monkeypatch):
    from loom_control_plane import object_version_recovery as module

    r = recovery
    payload = await applying(r)
    before = await snapshot(r)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    verify = module._verify_objects

    def delayed(client, plan):
        started.set()
        try:
            assert release.wait(10)
            verify(client, plan)
        finally:
            finished.set()

    monkeypatch.setattr(module, "_verify_objects", delayed)
    task = asyncio.create_task(r.client.post(URL, headers=HEADERS, json=payload))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 10)
    assert await snapshot(r) == before


@pytest.mark.parametrize("change", ["bytes", "version", "delete_marker", "mirror", "gc", "owner", "stale"])
async def test_conflicting_evidence_never_partially_repairs(recovery, change):
    r = recovery
    if change in {"bytes", "version"}:
        obj = r.objects[0]
        # Same length: metadata-only verification would incorrectly accept it.
        new = r.s3.put_object(Bucket=r.bucket, Key=obj.object_key, Body=b"x" * len(r.bodies[0]))
        if change == "bytes":
            r.s3.delete_object(Bucket=r.bucket, Key=obj.object_key, VersionId=r.versions[0])
            r.payload["objects"][0]["version_id"] = new["VersionId"]
    elif change == "delete_marker":
        r.s3.delete_object(Bucket=r.bucket, Key=r.objects[0].object_key)
    else:
        async with r.sessions() as session:
            artifact = await session.get(Artifact, UUID(r.payload["artifact_id"]))
            trial = await session.get(Trial, UUID(r.payload["trial_id"]))
            authority = await session.get(DataLifecycleAuthority, r.authority_id)
            if change == "mirror":
                index = copy.deepcopy(trial.trajectory_index)
                index["artifacts"][0]["version_id"] = r.versions[0]
                trial.trajectory_index = index
                r.payload["expected_index_sha256"] = digest(index)
            elif change == "gc":
                authority.state, authority.deletion_token = "deleting", uuid4()
            elif change == "owner":
                authority.owner_id = str(uuid4())
            else:
                artifact.storage = {**artifact.storage, "drift": True}
            await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("fault", ["missing_version_receipt", "incomplete_inventory", "sdk_error"])
async def test_storage_receipts_fail_closed_without_leaking_sdk_details(recovery, monkeypatch, fault):
    r = recovery
    payload = await applying(r)
    before = await snapshot(r)
    if fault == "incomplete_inventory":
        original = r.s3.list_object_versions

        def broken_inventory(**kwargs):
            result = original(**kwargs)
            result["IsTruncated"] = True
            result.pop("NextKeyMarker", None)
            result.pop("NextVersionIdMarker", None)
            return result

        monkeypatch.setattr(r.s3, "list_object_versions", broken_inventory)
    else:
        original = r.s3.get_object

        def broken_receipt(**kwargs):
            if fault == "sdk_error":
                raise ValueError("sensitive signed URL or credential")
            result = original(**kwargs)
            result.pop("VersionId")
            return result

        monkeypatch.setattr(r.s3, "get_object", broken_receipt)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 409, response.text
    assert "sensitive" not in response.text
    assert await snapshot(r) == before


async def test_inventory_pagination_does_not_confuse_prefix_neighbors_with_exact_key(recovery, monkeypatch):
    r = recovery
    r.s3.put_object(Bucket=r.bucket, Key=r.objects[0].object_key + ".neighbor", Body=b"unrelated")
    original = r.s3.list_object_versions

    def paginated(**kwargs):
        return original(**{**kwargs, "MaxKeys": 1})

    monkeypatch.setattr(r.s3, "list_object_versions", paginated)
    payload = await applying(r)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "applied"


@pytest.mark.parametrize("source_fault", ["manifest", "artifact", "producer", "runtime"])
async def test_corrupt_persisted_source_identity_is_rejected(recovery, source_fault):
    r = recovery
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("legacy", [False, True])
async def test_gc_claim_under_old_registry_uuid_cannot_delete_adopted_version(recovery, legacy):
    r = recovery
    obj = r.objects[0]
    old_id, old_authority, deletion_token, run_id = uuid4(), uuid4(), uuid4(), uuid4()
    inventory = {"objects": [{"id": str(old_id), "authority_id": str(old_authority),
        "bucket": obj.bucket, "object_key": obj.object_key, "version_id": r.versions[0],
        "content_sha256": obj.content_sha256, "size_bytes": obj.size_bytes}]} if legacy else {"schema_version": 2}
    async with r.sessions() as session:
        session.add(DataLifecycleGcRun(id=run_id, environment="staging", namespace="old",
            mutation_epoch_before=0, dry_run=False, requested_by="test", policy={}, inventory=inventory,
            state="failed"))
        await session.flush()
        # Exact snapshots intentionally survive deletion of their original rows.
        await session.execute(text("INSERT INTO data_lifecycle_gc_items "
            "(gc_run_id, object_id, deletion_token, authority_id, bucket, object_key, version_id, size_bytes) "
            "VALUES (:run, :object, :token, :authority, :bucket, :key, :version, :size)"),
            {"run": run_id, "object": old_id, "token": deletion_token,
             "authority": None if legacy else old_authority, "bucket": None if legacy else obj.bucket,
             "key": None if legacy else obj.object_key, "version": None if legacy else r.versions[0],
             "size": None if legacy else obj.size_bytes})
        await session.commit()
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


async def test_exact_version_recovery_needs_no_bucket_configuration_permission(recovery, monkeypatch):
    r = recovery

    def inaccessible_configuration(**kwargs):
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetBucketVersioning")

    monkeypatch.setattr(r.s3, "get_bucket_versioning", inaccessible_configuration)
    payload = await applying(r)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "applied"


def add_equivalent_versions(r, count=2, index=0):
    versions = [r.versions[index]]
    for _ in range(count - 1):
        versions.append(r.s3.put_object(
            Bucket=r.bucket, Key=r.objects[index].object_key, Body=r.bodies[index],
        )["VersionId"])
    r.payload["objects"][index].update(version_id=versions[-1], equivalent_version_ids=versions)
    return versions


@pytest.mark.parametrize("count", [2, 5, 8])
async def test_equivalent_versions_adopt_latest_with_complete_audit_and_replay(recovery, count):
    r = recovery
    versions = add_equivalent_versions(r, count)
    before = await snapshot(r)
    payload = await applying(r)
    assert await snapshot(r) == before
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    after = await snapshot(r)
    assert after[0]["files"][0]["version_id"] == versions[-1]
    assert after[1]["artifacts"][0]["version_id"] == versions[-1]
    assert after[2] == [versions[-1], *r.versions[1:]]
    assert after[4] == before[4]
    assert len(after[3]) == 1
    plan = after[3][0][1]["plan"]
    adopted = next(o for o in plan["objects"] if o["registry_id"] == str(r.objects[0].id))
    assert adopted["equivalent_version_ids"] == sorted(versions)
    assert adopted["version_id"] == versions[-1]
    # A set's order cannot create a new operation identity or invalidate its audit.
    payload["objects"][0]["equivalent_version_ids"].reverse()
    replay = await r.client.post(URL, headers=HEADERS, json=payload)
    assert replay.status_code == 200, replay.text
    assert replay.json()["status"] == "replayed"
    assert await snapshot(r) == after
    remaining = r.s3.list_object_versions(Bucket=r.bucket, Prefix=r.objects[0].object_key)
    assert {v["VersionId"] for v in remaining["Versions"]} == set(versions)
    assert not remaining.get("DeleteMarkers")


async def test_identical_versions_still_require_explicit_inventory(recovery):
    r = recovery
    add_equivalent_versions(r)
    r.payload["objects"][0].pop("equivalent_version_ids")
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert "ambiguous_stored_versions" in response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("fault", ["corrupt_older_copy", "nonlatest", "missing", "extra", "delete_marker"])
async def test_equivalent_inventory_rejects_conflicts_before_any_repair(recovery, fault):
    r = recovery
    versions = add_equivalent_versions(r)
    obj = r.objects[0]
    if fault == "corrupt_older_copy":
        bad = r.s3.put_object(Bucket=r.bucket, Key=obj.object_key, Body=b"x" * len(r.bodies[0]))["VersionId"]
        latest = r.s3.put_object(Bucket=r.bucket, Key=obj.object_key, Body=r.bodies[0])["VersionId"]
        r.payload["objects"][0].update(version_id=latest, equivalent_version_ids=[*versions, bad, latest])
    elif fault == "nonlatest":
        r.payload["objects"][0]["version_id"] = versions[0]
    elif fault == "missing":
        r.s3.delete_object(Bucket=r.bucket, Key=obj.object_key, VersionId=versions[0])
    elif fault == "extra":
        r.payload["objects"][0]["equivalent_version_ids"].append("nonexistent-version")
    else:
        r.s3.delete_object(Bucket=r.bucket, Key=obj.object_key)
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("when", ["after_preview", "during_reads"])
async def test_equivalent_inventory_drift_cannot_apply(recovery, monkeypatch, when):
    r = recovery
    add_equivalent_versions(r)
    payload = await applying(r)
    before = await snapshot(r)

    def add_copy():
        r.s3.put_object(Bucket=r.bucket, Key=r.objects[0].object_key, Body=r.bodies[0])

    if when == "after_preview":
        add_copy()
    else:
        original = r.s3.get_object
        remaining_reads = len(r.objects) + 1

        def change_during_reads(**kwargs):
            nonlocal remaining_reads
            result = original(**kwargs)
            remaining_reads -= 1
            # Registry rows are UUID ordered. Pick an earlier key deterministically
            # from the observed read order rather than assuming fixture order.
            if remaining_reads == 0:
                index = next(i for i, obj in enumerate(r.objects) if obj.object_key != kwargs["Key"])
                r.s3.put_object(Bucket=r.bucket, Key=r.objects[index].object_key, Body=r.bodies[index])
            return result

        monkeypatch.setattr(r.s3, "get_object", change_during_reads)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 409, response.text
    assert await snapshot(r) == before


async def test_all_equivalent_copies_count_toward_verification_budget(recovery, monkeypatch):
    import loom_control_plane.object_version_recovery as module

    r = recovery
    add_equivalent_versions(r)
    # Selected bytes fit exactly; the extra retained copy exceeds the budget.
    monkeypatch.setattr(module, "MAX_BYTES", sum(len(body) for body in r.bodies))
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 409, response.text
    assert "byte_limit_exceeded" in response.text
    assert await snapshot(r) == before


@pytest.mark.parametrize("invalid", [[], ["latest"], ["latest"] * 2,
    ["latest", "null"], ["latest", " null "], ["latest", ""], ["latest", 1],
    ["latest", "x" * 1025], ["latest", "bad\nversion"], ["a", "b"],
    ["latest", *[str(i) for i in range(8)]]])
async def test_equivalent_version_request_requires_complete_bounded_concrete_set(recovery, invalid):
    r = recovery
    r.payload["objects"][0].update(version_id="latest", equivalent_version_ids=invalid)
    before = await snapshot(r)
    response = await r.client.post(URL, headers=HEADERS, json=r.payload)
    assert response.status_code == 422, response.text
    assert await snapshot(r) == before


async def test_legacy_single_version_request_keeps_audited_identity(recovery):
    r = recovery
    payload = await applying(r)
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    after = await snapshot(r)
    legacy_identity = copy.deepcopy(r.payload)
    legacy_identity["objects"].sort(key=lambda obj: obj["registry_id"])
    audit = after[3][0][1]
    assert audit["request_sha256"] == digest(legacy_identity)
    assert audit["plan"]["request"] == legacy_identity
    assert all("equivalent_version_ids" not in obj for obj in audit["plan"]["objects"])
    # Explicit null is the same legacy mode, preserving old persisted receipts.
    for obj in payload["objects"]:
        obj["equivalent_version_ids"] = None
    response = await r.client.post(URL, headers=HEADERS, json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "replayed"


async def test_equivalent_versions_support_complete_paginated_inventory(recovery, monkeypatch):
    r = recovery
    add_equivalent_versions(r, 5)
    r.s3.put_object(Bucket=r.bucket, Key=r.objects[0].object_key + ".neighbor", Body=b"unrelated")
    original = r.s3.list_object_versions

    def paginated(**kwargs):
        return original(**{**kwargs, "MaxKeys": 1})

    monkeypatch.setattr(r.s3, "list_object_versions", paginated)
    response = await r.client.post(URL, headers=HEADERS, json=await applying(r))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "applied"
