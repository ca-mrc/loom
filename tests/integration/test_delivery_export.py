"""Delivery bundle export for completed batch families (#390)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import tarfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, delete, func, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    Batch,
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    LlmCall,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.delivery_export import build_canonical_trial_bundle_archive
from loom_service.delivery_export_errors import CorruptDeliveryObjectsError
from loom_service.delivery_export_tb2_v2 import SECRET_PATTERNS
from loom_service.trial_bundles import CanonicalTrialBundle, CanonicalTrialBundleFile, ObjectRef


def _assert_no_secret_patterns(text: str) -> None:
    """Match export-time secret scan; avoid naive `sk-` substring false positives."""
    for pattern in SECRET_PATTERNS:
        match = pattern.search(text)
        assert match is None, f"unexpected secret pattern {pattern.pattern!r}: {match!r}"


def _assert_complete_payload_checksums(tar: tarfile.TarFile) -> dict[str, str]:
    checksum_lines = (
        tar.extractfile("checksums/SHA256SUMS").read().decode().splitlines()  # type: ignore[union-attr]
    )
    entries: list[tuple[str, str]] = []
    for line in checksum_lines:
        digest, separator, path = line.partition("  ")
        assert separator == "  "
        assert len(digest) == 64
        assert path
        entries.append((path, digest))

    paths = [path for path, _digest in entries]
    assert len(paths) == len(set(paths)), "SHA256SUMS contains duplicate paths"
    expected_paths = {
        member.name
        for member in tar.getmembers()
        if member.isfile() and member.name != "checksums/SHA256SUMS"
    }
    assert set(paths) == expected_paths

    checksums = dict(entries)
    for path, expected_digest in checksums.items():
        body = tar.extractfile(path).read()  # type: ignore[union-attr]
        assert hashlib.sha256(body).hexdigest() == expected_digest

    if "task_bundles/manifest.json" in expected_paths:
        task_bundle_manifest = json.load(  # type: ignore[arg-type]
            tar.extractfile("task_bundles/manifest.json")
        )
        for item in task_bundle_manifest["files"]:
            assert checksums[item["archive_path"]] == item["sha256"]
    return checksums


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.head_errors: dict[tuple[str, str], str] = {}
        self.get_errors: dict[tuple[str, str], str] = {}
        self.heads: list[tuple[str, str]] = []
        self.put_body_types: list[type[object]] = []
        self.put_body_types_by_key: dict[tuple[str, str], type[object]] = {}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        self.heads.append((Bucket, Key))
        if code := self.head_errors.get((Bucket, Key)):
            raise ClientError(
                {"Error": {"Code": code, "Message": code}},
                "HeadObject",
            )
        body = self.objects.get((Bucket, Key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
        return {"ContentLength": len(body)}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        if code := self.get_errors.get((Bucket, Key)):
            raise ClientError(
                {"Error": {"Code": code, "Message": code}},
                "GetObject",
            )
        body = self.objects.get((Bucket, Key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "GetObject",
            )
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    def put_object(self, *, Bucket: str, Key: str, Body: object) -> dict[str, str]:  # noqa: N803
        self.put_body_types.append(type(Body))
        self.put_body_types_by_key[(Bucket, Key)] = type(Body)
        if isinstance(Body, bytes):
            data = Body
        elif isinstance(Body, str):
            data = Body.encode()
        else:
            read = getattr(Body, "read", None)
            if not callable(read):
                raise TypeError("Body must be bytes, str, or file-like")
            data = read()
        self.objects[(Bucket, Key)] = bytes(data)
        return {"ETag": hashlib.md5(bytes(data), usedforsecurity=False).hexdigest()}


def test_complete_trial_bundle_archive_is_deterministic_and_integrity_checked() -> None:
    client = _FakeS3Client()
    trial_id = uuid4()
    artifact_id = uuid4()
    payload = b"complete output\n"
    client.objects[("artifacts", "canonical/output.txt")] = payload
    bundle = CanonicalTrialBundle(
        artifact_id=artifact_id,
        trial_id=trial_id,
        task_id="benchmark/task-1",
        attempt=1,
        manifest_sha256="sha256:" + "a" * 64,
        content_sha256="sha256:" + "b" * 64,
        files=(
            CanonicalTrialBundleFile(
                relative_path="files/output.txt",
                ref=ObjectRef(
                    kind="trial_bundle",
                    trial_id=trial_id,
                    bucket="artifacts",
                    key="canonical/output.txt",
                ),
                size_bytes=len(payload),
                sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
                media_type="text/plain",
            ),
        ),
    )

    first = build_canonical_trial_bundle_archive(client=client, bundle=bundle)
    first_bytes = first.body.read()
    first.body.close()
    second = build_canonical_trial_bundle_archive(client=client, bundle=bundle)
    second_bytes = second.body.read()
    second.body.close()

    assert first.sha256 == second.sha256
    assert first_bytes == second_bytes
    with tarfile.open(fileobj=io.BytesIO(first_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        assert tar.extractfile("files/output.txt").read() == payload  # type: ignore[union-attr]
        manifest = json.load(tar.extractfile("bundle.json"))  # type: ignore[arg-type]
        assert manifest["trial_id"] == str(trial_id)
        assert "checksums/SHA256SUMS" in tar.getnames()

    client.objects[("artifacts", "canonical/output.txt")] = b"tampered output\n"
    with pytest.raises(CorruptDeliveryObjectsError):
        build_canonical_trial_bundle_archive(client=client, bundle=bundle)


@pytest.fixture
async def delivery_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[dict[str, object]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.minio_client = _FakeS3Client()
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    read_only_raw = f"loom_team_{uuid4().hex}"
    now = datetime.now(UTC)
    main_batch_id = uuid4()
    supplemental_batch_id = uuid4()
    targeted_batch_id = uuid4()
    task_ids = [f"source-useful-5003/task-{i:04d}" for i in range(1, 5)]

    selected_trials: dict[str, UUID] = {
        task_ids[0]: uuid4(),
        task_ids[1]: uuid4(),
        task_ids[2]: uuid4(),
        task_ids[3]: uuid4(),
    }
    failed_trial = uuid4()
    cancelled_trial = uuid4()
    still_failed_trial = uuid4()

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"Delivery Team {team_id.hex[:8]}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(read_only_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            )
        )
        for task_id in task_ids:
            source = None
            if task_id == task_ids[0]:
                source = f"s3://task-bundles/{task_id}/"
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="d" * 64,
                    config={"task": {"id": task_id, "name": task_id}},
                    source=source or "local",
                )
            )
        common_batch = {
            "team_id": team_id,
            "description": None,
            "purpose": "evaluation",
            "task_filter": {"subset_kind": "explicit", "task_ids": task_ids},
            "trial_config": {
                "agent_name": "opencode",
                "agent_model": {"provider": "yibu", "name": "glm-5.1-thinking"},
            },
            "state": "finished",
            "created_by_token_prefix": "test",
            "expected_trial_count": 4,
            "n_per_task": 1,
            "backend": "gb10",
            "combinations": [],
            "provider_model_id": "glm-5.1-thinking",
            "finished_at": now,
        }
        s.execute(
            insert(Batch).values(
                id=main_batch_id,
                name="source-useful-5003-opencode-glm51-gb10",
                result_status="partial_failed",
                created_at=now,
                **common_batch,
            )
        )
        s.execute(
            insert(Batch).values(
                id=supplemental_batch_id,
                name="source-useful-5003-primary32",
                result_status="partial_failed",
                rerun_of_batch_id=main_batch_id,
                created_at=now + timedelta(minutes=1),
                **common_batch,
            )
        )
        s.execute(
            insert(Batch).values(
                id=targeted_batch_id,
                name="source-useful-5003-http-parser-timeout1",
                result_status="succeeded",
                rerun_of_batch_id=main_batch_id,
                created_at=now + timedelta(minutes=2),
                **common_batch,
            )
        )

        def add_trial(
            *,
            trial_id: UUID,
            batch_id: UUID,
            task_id: str,
            state: str,
            reward: float | None,
            failure_reason: str | None = None,
            submitted_offset: int = 0,
        ) -> None:
            result = {"aggregate_reward": reward} if reward is not None else None
            trajectory_index = None
            if state == "succeeded":
                prefix = f"{team_id}/{trial_id}"
                trajectory_index = {
                    "trajectory_uri": f"s3://{settings.trajectories_bucket}/{prefix}/events.jsonl",
                    "atif_uri": f"s3://{settings.trajectories_bucket}/{prefix}/atif.json",
                }
                app.state.minio_client.objects[
                    (settings.trajectories_bucket, f"{prefix}/events.jsonl")
                ] = (
                    json.dumps(
                        {
                            "kind": "trial_end",
                            "seq": 1,
                            "emitted_at": now.isoformat(),
                            "trial_id": str(trial_id),
                            "step_id": "__trial__",
                            "final_state": "succeeded",
                            "reward": {"score": reward},
                            "failure_reason": None,
                        }
                    )
                    + "\n"
                ).encode()
                app.state.minio_client.objects[
                    (settings.trajectories_bucket, f"{prefix}/atif.json")
                ] = json.dumps(
                    {
                        "version": "1.7",
                        "trial_id": str(trial_id),
                        "metadata": {
                            "final_state": "succeeded",
                            "reward": {"score": reward},
                        },
                    }
                ).encode()
            s.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    batch_id=batch_id,
                    state=state,
                    failure_reason=failure_reason,
                    failure_message=failure_reason,
                    config=common_batch["trial_config"],
                    requires_caps={},
                    submitted_at=now + timedelta(seconds=submitted_offset),
                    started_at=now + timedelta(seconds=submitted_offset)
                    if state == "succeeded"
                    else None,
                    finished_at=now + timedelta(seconds=submitted_offset + 1),
                    sample_idx=0,
                    combination_idx=0,
                    result=result,
                    trajectory_index=trajectory_index,
                    provider_model_id="glm-5.1-thinking",
                    visibility="org",
                    share_status="shared",
                )
            )

        add_trial(
            trial_id=selected_trials[task_ids[0]],
            batch_id=main_batch_id,
            task_id=task_ids[0],
            state="succeeded",
            reward=1.0,
        )
        add_trial(
            trial_id=failed_trial,
            batch_id=main_batch_id,
            task_id=task_ids[1],
            state="failed",
            reward=None,
            failure_reason="gateway_error",
        )
        add_trial(
            trial_id=cancelled_trial,
            batch_id=main_batch_id,
            task_id=task_ids[2],
            state="cancelled",
            reward=None,
            failure_reason="cancelled",
        )
        add_trial(
            trial_id=selected_trials[task_ids[3]],
            batch_id=main_batch_id,
            task_id=task_ids[3],
            state="succeeded",
            reward=0.0,
        )
        add_trial(
            trial_id=selected_trials[task_ids[1]],
            batch_id=supplemental_batch_id,
            task_id=task_ids[1],
            state="succeeded",
            reward=0.0,
            submitted_offset=10,
        )
        add_trial(
            trial_id=still_failed_trial,
            batch_id=supplemental_batch_id,
            task_id=task_ids[2],
            state="failed",
            reward=None,
            failure_reason="retry_exhausted",
            submitted_offset=11,
        )
        add_trial(
            trial_id=selected_trials[task_ids[2]],
            batch_id=targeted_batch_id,
            task_id=task_ids[2],
            state="succeeded",
            reward=1.0,
            submitted_offset=20,
        )
        s.commit()

    try:
        yield {
            "app": app,
            "raw": raw,
            "read_only_raw": read_only_raw,
            "team_id": team_id,
            "main_batch_id": main_batch_id,
            "supplemental_batch_id": supplemental_batch_id,
            "targeted_batch_id": targeted_batch_id,
            "selected_trials": selected_trials,
            "failed_trial_id": failed_trial,
            "task_ids": task_ids,
            "settings": settings,
            "fake_s3": app.state.minio_client,
        }
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()
        sync_engine.dispose()


async def test_delivery_export_creates_5003_style_bundle_and_records_artifact(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    fake_s3 = delivery_setup["fake_s3"]
    bundle_task_id = task_ids[0]  # type: ignore[index]
    bundle_trial_id = selected_trials[bundle_task_id]  # type: ignore[index]
    bundle_artifact_id = uuid4()
    bundle_objects = {
        (
            "artifacts",
            f"trials/{bundle_trial_id}/bundles/{bundle_artifact_id}/files/artifacts/answer.txt",
        ): b"complete answer\n",
        (
            "artifacts",
            f"trials/{bundle_trial_id}/bundles/{bundle_artifact_id}/source/_manifest.json",
        ): b'{"schema_version":"loom.artifact-commit.v1"}',
    }
    fake_s3.objects.update(bundle_objects)  # type: ignore[attr-defined]
    sync_engine = create_engine(postgres_url)
    with sessionmaker(sync_engine)() as s:
        source_rows = []
        for index, ((bucket, key), payload) in enumerate(bundle_objects.items()):
            row = {
                "relative_path": (
                    "artifacts/answer.txt" if index == 0 else "source/_manifest.json"
                ),
                "size_bytes": len(payload),
                "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                "media_type": "application/json" if index else "text/plain",
                "bucket": bucket,
                "key": key,
            }
            source_rows.append(row)
        s.execute(
            insert(Artifact).values(
                id=bundle_artifact_id,
                artifact_type="loom.trial-artifact-bundle.v1",
                name="trial_bundle",
                team_id=delivery_setup["team_id"],
                trial_id=bundle_trial_id,
                control_producer_kind="service_execution",
                control_producer_id=uuid4(),
                content_hash="sha256:" + "1" * 64,
                storage={
                    "schema_version": "loom.canonical-trial-bundle-storage.v1",
                    "attempt": 0,
                    "files": [source_rows[0]],
                    "source_evidence": [source_rows[1]],
                },
                artifact_metadata={"materialization_state": "committed"},
                visibility="team",
                share_status="shared",
                safety_state="verified_internal",
                access_class="team_runtime",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )
        bundle_response = await ac.get(
            f"/api/v1/trials/{bundle_trial_id}/bundle/download",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert response.status_code == 201, response.text
    assert bundle_response.status_code == 200, bundle_response.text
    assert bundle_response.headers["content-type"] == "application/gzip"
    assert bundle_response.headers["cache-control"] == "private, no-store"
    assert bundle_response.headers["x-content-sha256"] == (
        "sha256:" + hashlib.sha256(bundle_response.content).hexdigest()
    )
    with tarfile.open(fileobj=io.BytesIO(bundle_response.content), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        assert tar.extractfile("files/artifacts/answer.txt").read() == b"complete answer\n"  # type: ignore[union-attr]
        assert "source/_manifest.json" in tar.getnames()
        assert "checksums/SHA256SUMS" in tar.getnames()
    body = response.json()
    assert body["status"] == "ready"
    assert len(body["sha256"]) == 64
    assert body["archive_filename"].endswith(".tar.gz")
    assert body["manifest"]["schema_version"] == "1"
    assert body["manifest"]["selection_rule"] == (
        "highest_priority_deliverable_by_task_sample_combination"
    )
    assert body["manifest"]["batch_family"] == {
        "main_batch_id": str(main_batch_id),
        "supplemental_batch_ids": [str(supplemental_batch_id), str(targeted_batch_id)],
    }
    assert body["manifest"]["task_count"] == 4
    assert body["manifest"]["trial_count"] == 4
    assert body["manifest"]["source_counts"] == {
        str(main_batch_id): 2,
        str(supplemental_batch_id): 1,
        str(targeted_batch_id): 1,
    }
    assert body["manifest"]["reward_distribution"] == {"0.0": 2, "1.0": 2}
    assert body["manifest"]["model_provider"] == "yibu"
    assert body["manifest"]["model_name"] == "glm-5.1-thinking"
    assert body["manifest"]["object_counts"] == {
        "atif": 4,
        "trajectory": 4,
        "trial_bundles": 1,
        "trial_bundle_files": 2,
    }
    assert body["manifest"]["archive_sha256"] == body["sha256"]

    # HEAD validation must happen before the export is declared ready.
    assert len(fake_s3.heads) == 10  # type: ignore[attr-defined]
    assert body["object_validation"]["checked"] == 10
    assert body["object_validation"]["missing"] == []

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    assert hashlib.sha256(archive_bytes).hexdigest() == body["sha256"]

    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        names = sorted(tar.getnames())
        assert "manifest.json" in names
        assert "summary.json" in names
        assert "ledger/resource_usage.jsonl" in names
        assert "ledger/trials.jsonl" in names
        assert "ledger/trials.csv" in names
        assert "checksums/SHA256SUMS" in names
        assert sum(name.startswith("trajectories/") for name in names) == 4
        assert sum(name.startswith("atif/") for name in names) == 4
        bundle_prefix = (
            f"trial_bundles/source-useful-5003-task-0001/{bundle_trial_id}"
        )
        assert tar.extractfile(f"{bundle_prefix}/files/artifacts/answer.txt").read() == (  # type: ignore[union-attr]
            b"complete answer\n"
        )
        bundle_manifest = json.load(  # type: ignore[arg-type]
            tar.extractfile(f"{bundle_prefix}/bundle.json")
        )
        assert bundle_manifest["artifact_id"] == str(bundle_artifact_id)
        assert [item["relative_path"] for item in bundle_manifest["files"]] == [
            "files/artifacts/answer.txt",
            "source/_manifest.json",
        ]
        manifest = json.load(tar.extractfile("manifest.json"))  # type: ignore[arg-type]
        assert manifest["task_count"] == 4
        assert "archive_sha256" not in manifest
        assert manifest["payload_checksums"] == {
            "algorithm": "sha256",
            "file": "checksums/SHA256SUMS",
            "scope": "archive payload files excluding checksums/SHA256SUMS",
        }
        checksums = tar.extractfile("checksums/SHA256SUMS").read().decode().splitlines()  # type: ignore[union-attr]
        manifest_checksum = hashlib.sha256(
            tar.extractfile("manifest.json").read()  # type: ignore[union-attr]
        ).hexdigest()
        assert f"{manifest_checksum}  manifest.json" in checksums
        ledger_lines = tar.extractfile("ledger/trials.jsonl").read().decode().splitlines()  # type: ignore[union-attr]
        ledger = [json.loads(line) for line in ledger_lines]
        assert [row["task_id"] for row in ledger] == sorted(task_ids)
        assert {row["selected_trial_id"] for row in ledger} == {
            str(value) for value in selected_trials.values()
        }
        csv_rows = list(
            csv.DictReader(
                io.StringIO(
                    tar.extractfile("ledger/trials.csv").read().decode(),  # type: ignore[union-attr]
                )
            )
        )
        assert len(csv_rows) == 4
        assert {row["selection_source"] for row in csv_rows} == {
            "main",
            "supplemental",
        }

    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as s:
            artifact = (
                s.execute(
                    select(Artifact).where(
                        Artifact.batch_id == main_batch_id,
                        Artifact.artifact_type == "trajectory_bundle",
                    )
                )
                .scalars()
                .one()
            )
            assert artifact.content_hash == f"sha256:{body['sha256']}"
            assert artifact.artifact_metadata["delivery_export"]["status"] == "ready"
            assert artifact.artifact_metadata["delivery_export"]["task_count"] == 4
            assert (
                artifact.artifact_metadata["delivery_export"]["manifest"]["archive_sha256"]
                == body["sha256"]
            )
            assert artifact.provenance["source_batch_ids"] == [
                str(main_batch_id),
                str(supplemental_batch_id),
                str(targeted_batch_id),
            ]
            authority = s.get(DataLifecycleAuthority, artifact.lifecycle_authority_id)
            assert authority is not None
            assert authority.data_class == "artifact"
            assert authority.owner_id == str(artifact.id)
            registered_objects = list(
                s.scalars(
                    select(DataLifecycleObject)
                    .where(DataLifecycleObject.authority_id == authority.id)
                    .order_by(DataLifecycleObject.object_key)
                )
            )
            assert len(registered_objects) == 2
            by_key = {row.object_key: row for row in registered_objects}
            archive_object = by_key[archive_key]
            assert archive_object.size_bytes == len(archive_bytes)
            assert archive_object.content_sha256 == body["sha256"]
            checksum_key = f"{archive_key}.sha256"
            checksum_bytes = fake_s3.objects[(body["storage"]["bucket"], checksum_key)]  # type: ignore[attr-defined]
            checksum_object = by_key[checksum_key]
            assert checksum_object.size_bytes == len(checksum_bytes)
            assert checksum_object.content_sha256 == hashlib.sha256(checksum_bytes).hexdigest()
            batch = s.get(Batch, main_batch_id)
            assert batch is not None
            assert batch.lifecycle_authority_id is not None
    finally:
        sync_engine.dispose()


async def test_delivery_export_rejects_selected_trial_result_state_drift(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    inconsistent_trial_id = next(iter(selected_trials.values()))

    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as session:
            trial = session.get(Trial, inconsistent_trial_id)
            assert trial is not None
            trial.result = {
                "state": "succeeded",
                "failure_reason": "verifier_error",
                "aggregate_reward": 1.0,
            }
            session.commit()

        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
            response = await ac.post(
                f"/api/v1/batches/{main_batch_id}/delivery-export",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "supplemental_batch_ids": [
                        str(supplemental_batch_id),
                        str(targeted_batch_id),
                    ]
                },
            )

        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["code"] == "delivery_export_terminal_state_mismatch"
        assert detail["inconsistent_trials"][0]["trial_id"] == str(inconsistent_trial_id)
        with sessionmaker(sync_engine)() as session:
            assert session.scalar(select(func.count()).select_from(Artifact)) == 0
    finally:
        sync_engine.dispose()


async def test_raw_harbor_tb2_delivery_export_streams_sample_compatible_bundle(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3 = delivery_setup["fake_s3"]
    app.state.settings.public_base_url = "https://yylx.world/dev"

    task_bundle_key = f"{task_ids[0]}/task.toml"
    fake_s3.objects[("task-bundles", task_bundle_key)] = (  # type: ignore[attr-defined]
        f'id = "{task_ids[0]}"\n'
    ).encode()
    fake_s3.objects[("task-bundles", f"{task_ids[0]}/instruction.md")] = (  # type: ignore[attr-defined]
        b"solve the task\n"
    )

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            captured_at_base = datetime.now(UTC)
            first_assistant_content = ""
            for index, task_id in enumerate(task_ids, start=1):
                trial_id = selected_trials[task_id]
                assistant_content = f"answer {index}"
                if index == 1:
                    assistant_content = json.dumps(
                        {
                            "state_analysis": "fresh shell",
                            "explanation": "inspect files",
                            "commands": [
                                {
                                    "keystrokes": "ls -la\n",
                                    "is_blocking": True,
                                    "timeout_sec": 5,
                                }
                            ],
                            "is_task_complete": False,
                        }
                    )
                    first_assistant_content = assistant_content
                conn.execute(
                    insert(LlmCall).values(
                        team_id=delivery_setup["team_id"],
                        trial_id=trial_id,
                        step_id=f"step-{index}",
                        dialect="openai_facade",
                        model="gpt-4o",
                        input_tokens=10,
                        output_tokens=5,
                        provider_extras={
                            "_loom_raw_provider_log": {
                                "schema_version": "1",
                                "ref": (
                                    f"llm_calls/raw-{index}/provider_extras/_loom_raw_provider_log"
                                ),
                                "request": {
                                    "body": {
                                        "messages": [
                                            {
                                                "role": "user",
                                                "content": f"prompt {index}",
                                            }
                                        ]
                                    }
                                },
                                "response": {
                                    "body": {
                                        "choices": [
                                            {
                                                "message": {
                                                    "role": "assistant",
                                                    "content": assistant_content,
                                                }
                                            }
                                        ]
                                    }
                                },
                            }
                        },
                        request_params={"status": "available", "parameters": {}},
                        cost_usd=0,
                        rate_card_hash="facade:operator-supplied",
                        captured_at=captured_at_base + timedelta(seconds=index),
                    )
                )
            conn.execute(
                insert(LlmCall).values(
                    team_id=delivery_setup["team_id"],
                    trial_id=selected_trials[task_ids[0]],
                    step_id="step-1-followup",
                    dialect="openai_facade",
                    model="gpt-4o",
                    input_tokens=12,
                    output_tokens=6,
                    provider_extras={
                        "_loom_raw_provider_log": {
                            "schema_version": "1",
                            "ref": ("llm_calls/raw-1b/provider_extras/_loom_raw_provider_log"),
                            "request": {
                                "body": {
                                    "messages": [
                                        {"role": "user", "content": "prompt 1"},
                                        {
                                            "role": "assistant",
                                            "content": first_assistant_content,
                                        },
                                        {"role": "user", "content": "total 1\n"},
                                    ]
                                }
                            },
                            "response": {
                                "body": {
                                    "choices": [
                                        {
                                            "message": {
                                                "role": "assistant",
                                                "content": "answer after ls",
                                            }
                                        }
                                    ]
                                }
                            },
                        }
                    },
                    request_params={"status": "available", "parameters": {}},
                    cost_usd=0,
                    rate_card_hash="facade:operator-supplied",
                    captured_at=captured_at_base + timedelta(seconds=100),
                )
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor-tb2-v1",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "raw-harbor-tb2-v1"
    assert body["manifest"]["export_profile"] == {
        "name": "raw-harbor-tb2",
        "version": "1",
        "source_of_truth": "provider_logs",
        "audit_spine": "loom_trajectory.jsonl",
    }
    assert body["manifest"]["object_counts"]["provider_logs"] == 5
    assert body["manifest"]["object_counts"]["task_bundle_files"] == 2
    assert body["archive_filename"].endswith("-raw-harbor-tb2-v1.tar.gz")
    assert body["download_url"].startswith("https://yylx.world/dev/api/v1/batches/")

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    assert hashlib.sha256(archive_bytes).hexdigest() == body["sha256"]
    # The archive body must be uploaded from a file-like spool, not held as one
    # fully materialized bytes object before put_object.
    assert fake_s3.put_body_types_by_key[(body["storage"]["bucket"], archive_key)] is not bytes  # type: ignore[attr-defined]

    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        names = set(tar.getnames())
        assert "manifest.json" in names
        assert "summary.json" in names
        assert "provider_logs/manifest.json" in names
        assert "derived/sft_messages.jsonl" in names
        assert f"task_bundles/{first_task}/task.toml" in names
        assert f"task_bundles/{first_task}/instruction.md" in names
        assert f"agent_runs/{first_task}/{first_trial}/execution_result.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/metrics.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/resource_usage.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/verifier_output.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/provider_logs_manifest.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/loom_trajectory.jsonl" in names
        manifest = json.load(tar.extractfile("manifest.json"))  # type: ignore[arg-type]
        assert manifest["mode"] == "raw-harbor-tb2-v1"
        assert manifest["layout"] == {
            "top_level_manifests": ["manifest.json", "summary.json"],
            "task_bundles": "task_bundles/<task_id>/...",
            "agent_runs": "agent_runs/<task_id>/<trial_id>/...",
            "derived": "derived/sft_messages.jsonl",
        }
        resource_usage = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/resource_usage.json"
            )
        )
        assert resource_usage["items"] == []
        assert resource_usage["aggregate"]["telemetry_status"] == "unavailable"
        provider_manifest = json.load(
            tar.extractfile("provider_logs/manifest.json")  # type: ignore[arg-type]
        )
        assert len(provider_manifest["logs"]) == 5
        first_provider_log = json.load(
            tar.extractfile(provider_manifest["logs"][0]["archive_path"])  # type: ignore[arg-type]
        )
        rendered_provider_log = json.dumps(first_provider_log)
        assert "prompt " in rendered_provider_log
        assert "state_analysis" in rendered_provider_log
        _assert_no_secret_patterns(rendered_provider_log)
        sft_lines = (
            tar.extractfile("derived/sft_messages.jsonl")  # type: ignore[union-attr]
            .read()
            .decode()
            .splitlines()
        )
        assert len(sft_lines) == 5
        first_sft_row = json.loads(sft_lines[0])
        assert first_sft_row["reward"] == 1.0
        assert first_sft_row["reward_positive"] is True
        assert first_sft_row["source"] == "provider_logs"
        assert first_sft_row["messages"] == [
            {"role": "user", "content": "prompt 1"},
            {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "analysis": "fresh shell",
                        "plan": "inspect files",
                        "commands": [{"keystrokes": "ls -la\n", "duration": 5}],
                        "task_complete": False,
                    }
                ),
            },
        ]
        history_sft_row = next(
            json.loads(line) for line in sft_lines if len(json.loads(line)["messages"]) == 4
        )
        history_assistant = history_sft_row["messages"][1]
        assert history_assistant["role"] == "assistant"
        assert json.loads(history_assistant["content"]) == {
            "analysis": "fresh shell",
            "plan": "inspect files",
            "commands": [{"keystrokes": "ls -la\n", "duration": 5}],
            "task_complete": False,
        }
        assert "state_analysis" not in history_assistant["content"]
        artifact_manifest = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json"
            )
        )
        assert {artifact["kind"]: artifact["path"] for artifact in artifact_manifest["artifacts"]}[
            "trajectory"
        ] == "trajectory.json"
        assert {artifact["kind"]: artifact["path"] for artifact in artifact_manifest["artifacts"]}[
            "agent_native_trajectory"
        ] == "loom_trajectory.jsonl"
        trajectory = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/trajectory.json"
            )
        )
        assert trajectory["schema_version"] == "ATIF-v1.7"
        assert trajectory["agent"]["name"] == "opencode"
        assert trajectory["agent"]["model_name"] == "gpt-4o"
        assert trajectory["steps"][0]["source"] == "user"
        assert trajectory["steps"][1]["source"] == "agent"
        assert trajectory["steps"][1]["message"] == "Analysis: fresh shell\nPlan: inspect files"
        assert trajectory["steps"][1]["observation"] == {"results": [{"content": "total 1\n"}]}
        assert trajectory["steps"][1]["tool_calls"] == [
            {
                "tool_call_id": "call-1-1",
                "function_name": "bash_command",
                "arguments": {"keystrokes": "ls -la\n", "duration": 5},
            }
        ]


async def test_raw_harbor_delivery_export_preserves_loom_native_trajectory(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3 = delivery_setup["fake_s3"]
    assistant_content = json.dumps(
        {
            "state_analysis": "fresh shell",
            "explanation": "inspect files",
            "commands": [{"keystrokes": "ls -la\n", "timeout_sec": 5}],
            "is_task_complete": False,
        }
    )

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            for index, task_id in enumerate(task_ids, start=1):
                conn.execute(
                    insert(LlmCall).values(
                        team_id=delivery_setup["team_id"],
                        trial_id=selected_trials[task_id],
                        step_id=f"raw-step-{index}",
                        dialect="openai_facade",
                        model="gpt-4o",
                        input_tokens=10,
                        output_tokens=5,
                        provider_extras={
                            "_loom_raw_provider_log": {
                                "schema_version": "1",
                                "request": {
                                    "body": {
                                        "messages": [
                                            {
                                                "role": "user",
                                                "content": f"prompt {index}",
                                            }
                                        ]
                                    }
                                },
                                "response": {
                                    "body": {
                                        "choices": [
                                            {
                                                "message": {
                                                    "role": "assistant",
                                                    "content": assistant_content,
                                                }
                                            }
                                        ]
                                    }
                                },
                            }
                        },
                        request_params={"status": "available", "parameters": {}},
                        cost_usd=0,
                        rate_card_hash="facade:operator-supplied",
                    )
                )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "raw-harbor"
    assert "export_profile" not in body["manifest"]
    assert body["archive_filename"].endswith("-raw-harbor.tar.gz")

    archive_key = body["storage"]["key"]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], archive_key)]  # type: ignore[attr-defined]
    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        names = set(tar.getnames())
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.jsonl" in names
        assert f"agent_runs/{first_task}/{first_trial}/trajectory.json" not in names
        assert f"agent_runs/{first_task}/{first_trial}/loom_trajectory.jsonl" not in names
        sft_lines = (
            tar.extractfile("derived/sft_messages.jsonl")  # type: ignore[union-attr]
            .read()
            .decode()
            .splitlines()
        )
        first_sft_row = json.loads(sft_lines[0])
        assert first_sft_row["messages"][1]["content"] == assistant_content
        assert "state_analysis" in first_sft_row["messages"][1]["content"]


async def test_delivery_export_create_requires_submit_scope(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["read_only_raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 403


async def test_delivery_export_fails_clearly_when_referenced_object_missing(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    missing_trial = selected_trials[task_ids[0]]
    del fake_s3.objects[  # type: ignore[attr-defined]
        (
            settings.trajectories_bucket,  # type: ignore[attr-defined]
            f"{delivery_setup['team_id']}/{missing_trial}/atif.json",
        )
    ]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )
    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["code"] == "delivery_export_objects_missing"
    assert body["detail"]["missing_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(missing_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": f"{delivery_setup['team_id']}/{missing_trial}/atif.json",
        }
    ]


async def test_delivery_export_fails_clearly_when_head_object_forbidden(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    forbidden_trial = selected_trials[task_ids[0]]
    forbidden_key = f"{delivery_setup['team_id']}/{forbidden_trial}/atif.json"
    fake_s3.head_errors[  # type: ignore[attr-defined]
        (settings.trajectories_bucket, forbidden_key)  # type: ignore[attr-defined]
    ] = "AccessDenied"

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_objects_unreadable"
    assert detail["unreadable_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(forbidden_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": forbidden_key,
            "operation": "HeadObject",
            "error_code": "AccessDenied",
        }
    ]


async def test_delivery_export_fails_clearly_when_get_object_forbidden(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials = delivery_setup["selected_trials"]
    task_ids = delivery_setup["task_ids"]
    settings = delivery_setup["settings"]
    fake_s3 = delivery_setup["fake_s3"]

    forbidden_trial = selected_trials[task_ids[0]]
    forbidden_key = f"{delivery_setup['team_id']}/{forbidden_trial}/atif.json"
    fake_s3.get_errors[  # type: ignore[attr-defined]
        (settings.trajectories_bucket, forbidden_key)  # type: ignore[attr-defined]
    ] = "AccessDenied"

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_objects_unreadable"
    assert detail["unreadable_objects"] == [
        {
            "kind": "atif",
            "trial_id": str(forbidden_trial),
            "bucket": settings.trajectories_bucket,  # type: ignore[attr-defined]
            "key": forbidden_key,
            "operation": "GetObject",
            "error_code": "AccessDenied",
        }
    ]
    assert not any(
        bucket == settings.artifacts_bucket and key.startswith("delivery-exports/")  # type: ignore[attr-defined]
        for bucket, key in fake_s3.objects  # type: ignore[attr-defined]
    )


async def test_delivery_export_rejects_unlinked_supplemental_batch(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    team_id = delivery_setup["team_id"]
    main_batch_id = delivery_setup["main_batch_id"]
    task_ids = delivery_setup["task_ids"]
    unlinked_batch_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            conn.execute(
                insert(Batch).values(
                    id=unlinked_batch_id,
                    team_id=team_id,
                    name="unlinked-supplemental",
                    description=None,
                    task_filter={"subset_kind": "explicit", "task_ids": task_ids},
                    trial_config={"agent_name": "opencode"},
                    state="finished",
                    result_status="succeeded",
                    created_at=now,
                    finished_at=now,
                    created_by_token_prefix="test",
                    expected_trial_count=0,
                    n_per_task=1,
                    backend="gb10",
                    combinations=[],
                )
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"supplemental_batch_ids": [str(unlinked_batch_id)]},
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_invalid_batch_family"
    assert "linked rerun" in detail["message"]


async def test_delivery_export_rejects_non_terminal_batch_family(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            conn.execute(update(Batch).where(Batch.id == main_batch_id).values(state="running"))
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ]
            },
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "delivery_export_invalid_batch_family"
    assert "terminal" in detail["message"]


async def test_delivery_export_rejects_unresolved_platform_failures(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"supplemental_batch_ids": [str(supplemental_batch_id)]},
        )
    assert response.status_code == 409
    body = response.json()
    assert body["detail"]["code"] == "delivery_export_unresolved_trials"
    assert body["detail"]["unresolved_trials"][0]["task_id"].startswith("source-useful-5003/")


async def test_delivery_export_explicit_trial_ids_skips_unresolved_siblings(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    # Main-batch succeeded attempts only — unresolved siblings remain on the family.
    trial_ids = [selected_trials[task_ids[0]], selected_trials[task_ids[3]]]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"selection": {"trial_ids": [str(tid) for tid in trial_ids]}},
        )
    assert response.status_code == 201, response.text
    body = response.json()
    manifest = body["manifest"]
    assert manifest["selection_rule"] == "explicit_trial_ids"
    assert manifest["trial_count"] == 2
    assert set(manifest["selection"]["selected_trial_ids"]) == {str(tid) for tid in trial_ids}
    assert manifest["selection"]["skipped_coordinates_count"] == 2
    assert len(manifest["selection"]["skipped_coordinates"]) == 2


async def test_delivery_export_explicit_trial_ids_rejects_unknown_and_ineligible(
    delivery_setup: dict[str, object],
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    failed_trial_id = delivery_setup["failed_trial_id"]

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        unknown = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"selection": {"trial_ids": [str(uuid4())]}},
        )
        ineligible = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={"selection": {"trial_ids": [str(failed_trial_id)]}},
        )
    assert unknown.status_code == 400
    assert unknown.json()["detail"]["code"] == "delivery_export_invalid_batch_family"
    assert ineligible.status_code == 409
    assert ineligible.json()["detail"]["code"] == "delivery_export_unresolved_trials"
    assert ineligible.json()["detail"]["ineligible_trials"][0]["trial_id"] == str(
        failed_trial_id
    )


def _seed_scored_timeout(
    delivery_setup: dict[str, object],
    postgres_url: str,
    *,
    defect: str | None = None,
) -> tuple[UUID, dict[str, object]]:
    """Persist the native timeout-to-verifier outcome without changing its failure."""
    task_id = delivery_setup["task_ids"][2]  # type: ignore[index]
    trial_id = delivery_setup["selected_trials"][task_id]  # type: ignore[index]
    settings: LoomServiceSettings = delivery_setup["settings"]  # type: ignore[assignment]
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    prefix = f"{delivery_setup['team_id']}/{trial_id}"
    reward = {"passed": 0.0}
    output = b'{"rewards":{"passed":0.0}}'
    digest = "sha256:" + hashlib.sha256(output).hexdigest()
    phases = [
        {"role": "agent", "exit_code": 124, "timed_out": True},
        {"role": "verifier", "exit_code": 0, "timed_out": False},
    ]
    outputs = [
        {
            "source_path": ".loom/verifier/output.json",
            "relative_path": "verifier/output.json",
            "kind": "verifier",
            "required": True,
            "state": "captured",
            "size_bytes": len(output),
            "sha256": digest,
        }
    ]
    if defect == "missing_verifier":
        phases.pop()
    elif defect == "failed_verifier":
        phases[-1]["exit_code"] = 1
    elif defect == "missing_output":
        outputs[0].update(state="missing", size_bytes=None, sha256=None)
    result: dict[str, object] = {
        # Native finalization persists state/reason on Trial, not this projection
        # (service_execution.finalize_committed_service_execution).
        "schema_version": "loom.service-execution-trial-result.v1",
        "aggregate_reward": 0.0,
        "reward": reward,
        "runtime_result": {
            "schema_version": "loom.execution-runtime-result.v1",
            "execution_role": "attempt",
            "status": "timed_out",
            "failure_reason": None,
            "phases": phases,
            "outputs": outputs,
            "verifier_rewards": reward,
            "partial_evidence": True,
        },
    }
    terminal = {
        "kind": "trial_end",
        "seq": 1,
        "emitted_at": datetime.now(UTC).isoformat(),
        "trial_id": str(trial_id),
        "step_id": "__trial__",
        "final_state": "failed",
        "reward": reward,
        "failure_reason": "timed_out",
    }
    metadata = {
        "final_state": "failed",
        "reward": reward,
        "error": {"failure_reason": "timed_out"},
    }
    if defect == "typed_wrong_reason":
        terminal["failure_reason"] = "verifier_error"
    elif defect == "atif_wrong_reason":
        metadata["error"] = {"failure_reason": "verifier_error"}
    fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/events.jsonl")] = (
        json.dumps(terminal) + "\n"
    ).encode()
    fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/atif.json")] = json.dumps(
        {"version": "1.7", "trial_id": str(trial_id), "metadata": metadata}
    ).encode()
    artifact_id = uuid4()
    key = f"trials/{trial_id}/bundles/{artifact_id}/files/verifier/output.json"
    fake_s3.objects[(settings.artifacts_bucket, key)] = output
    manifest_key = f"trials/{trial_id}/bundles/{artifact_id}/source/_manifest.json"
    manifest = b'{"schema_version":"loom.artifact-commit.v1"}'
    fake_s3.objects[(settings.artifacts_bucket, manifest_key)] = manifest
    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            conn.execute(
                update(Trial)
                .where(Trial.id == trial_id)
                .values(
                    state="failed",
                    failure_reason="timed_out",
                    failure_message="service execution runtime reported timed_out",
                    result=result,
                )
            )
            if defect != "missing_bundle":
                conn.execute(
                    insert(Artifact).values(
                        id=artifact_id,
                        artifact_type="loom.trial-artifact-bundle.v1",
                        name="trial_bundle",
                        team_id=delivery_setup["team_id"],
                        trial_id=trial_id,
                        control_producer_kind="service_execution",
                        control_producer_id=uuid4(),
                        content_hash="sha256:" + "1" * 64,
                        storage={
                            "schema_version": "loom.canonical-trial-bundle-storage.v1",
                            "attempt": 0,
                            "files": [
                                {
                                    "relative_path": "verifier/output.json",
                                    "size_bytes": len(output),
                                    "sha256": digest,
                                    "media_type": "application/json",
                                    "bucket": settings.artifacts_bucket,
                                    "key": key,
                                }
                            ],
                            "source_evidence": [
                                {
                                    "relative_path": "source/_manifest.json",
                                    "size_bytes": len(manifest),
                                    "sha256": "sha256:" + hashlib.sha256(manifest).hexdigest(),
                                    "media_type": "application/json",
                                    "bucket": settings.artifacts_bucket,
                                    "key": manifest_key,
                                }
                            ],
                        },
                        artifact_metadata={"materialization_state": "committed"},
                        visibility="team",
                        share_status="shared",
                        safety_state="verified_internal",
                        access_class="team_runtime",
                    )
                )
    finally:
        sync_engine.dispose()
    return trial_id, result


async def _export_complete_test_family(delivery_setup: dict[str, object]) -> httpx.Response:
    transport = httpx.ASGITransport(app=delivery_setup["app"])  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        return await client.post(
            f"/api/v1/batches/{delivery_setup['main_batch_id']}/delivery-export",
            headers={"Authorization": f"Bearer {delivery_setup['raw']}"},
            json={
                "supplemental_batch_ids": [
                    str(delivery_setup["supplemental_batch_id"]),
                    str(delivery_setup["targeted_batch_id"]),
                ]
            },
        )


async def test_delivery_export_includes_scored_timeout_without_rewriting_failure(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    trial_id, original_result = _seed_scored_timeout(delivery_setup, postgres_url)
    response = await _export_complete_test_family(delivery_setup)
    assert response.status_code == 201, response.text
    body = response.json()
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    archive = fake_s3.objects[(body["storage"]["bucket"], body["storage"]["key"])]
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        rows = [
            json.loads(line)
            for line in tar.extractfile("ledger/trials.jsonl").read().splitlines()  # type: ignore[union-attr]
        ]
        assert len(rows) == 4
        row = next(item for item in rows if item["selected_trial_id"] == str(trial_id))
        assert row["state"] == "failed"
        assert row["failure_reason"] == "timed_out"
        assert row["reward"] == 0.0
        assert sum(item["state"] == "succeeded" for item in rows) == 3
        csv_rows = list(
            csv.DictReader(
                io.StringIO(tar.extractfile("ledger/trials.csv").read().decode())  # type: ignore[union-attr]
            )
        )
        csv_row = next(item for item in csv_rows if item["selected_trial_id"] == str(trial_id))
        assert csv_row["state"] == "failed"
        assert csv_row["failure_reason"] == "timed_out"
        assert float(csv_row["reward"]) == 0.0
        terminal = json.loads(tar.extractfile(row["trajectory_file"]).read())  # type: ignore[union-attr]
        assert terminal["final_state"] == "failed"
        assert terminal["failure_reason"] == "timed_out"
        assert terminal["reward"] == {"passed": 0.0}
        atif = json.load(tar.extractfile(row["atif_file"]))  # type: ignore[arg-type]
        assert atif["metadata"]["final_state"] == "failed"
        assert atif["metadata"]["error"]["failure_reason"] == "timed_out"
        assert atif["metadata"]["reward"] == {"passed": 0.0}
        verifier_paths = [name for name in tar.getnames() if name.endswith("/verifier/output.json")]
        assert len(verifier_paths) == 1
        assert json.load(tar.extractfile(verifier_paths[0])) == {"rewards": {"passed": 0.0}}  # type: ignore[arg-type]
    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as session:
            trial = session.get(Trial, trial_id)
            assert trial is not None
            assert trial.state == "failed"
            assert trial.failure_reason == "timed_out"
            assert trial.result == original_result
    finally:
        sync_engine.dispose()


@pytest.mark.parametrize(
    "defect", ["missing_verifier", "failed_verifier", "missing_output", "missing_bundle"]
)
async def test_delivery_export_rejects_incomplete_scored_timeout(
    delivery_setup: dict[str, object],
    postgres_url: str,
    defect: str,
) -> None:
    _seed_scored_timeout(delivery_setup, postgres_url, defect=defect)
    response = await _export_complete_test_family(delivery_setup)
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == (
        "delivery_export_objects_missing"
        if defect == "missing_bundle"
        else "delivery_export_unresolved_trials"
    )


@pytest.mark.parametrize("defect", ["typed_wrong_reason", "atif_wrong_reason"])
async def test_delivery_export_rejects_timeout_terminal_evidence_disagreement(
    delivery_setup: dict[str, object],
    postgres_url: str,
    defect: str,
) -> None:
    _seed_scored_timeout(delivery_setup, postgres_url, defect=defect)
    response = await _export_complete_test_family(delivery_setup)
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "delivery_export_terminal_state_mismatch"


def _tb2_v2_events_jsonl(
    *,
    trial_id: UUID,
    artifact_hash: str,
    terminal_state: str = "succeeded",
) -> bytes:
    emitted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    common = {
        "trial_id": str(trial_id),
        "step_id": "main",
        "emitted_at": emitted_at,
    }
    lines = [
        {
            "seq": 1,
            "kind": "terminus2_runtime_provenance",
            **common,
            "parser_name": "json",
            "prompt_hash": "abc",
            "template_hashes": {},
            "harbor_compat_sha": "527d50deb63a5d279e8c20593c18a2cbc7f61f9e",
            "benchmark_provenance": None,
            "loom_runtime_revision": "1.0",
            "terminal_image_digest": None,
        },
        {
            "seq": 2,
            "kind": "llm_call",
            **common,
            "model": {"provider": "openai", "name": "gpt-4o"},
            "rate_card_hash": "h",
            "system_prompt": None,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": None,
            "tool_choice": None,
            "response": {"role": "assistant", "content": "{}"},
            "finish_reason": "stop",
            "input_tokens": 1,
            "cached_input_tokens": 0,
            "cache_write_tokens": 0,
            "output_tokens": 1,
            "thinking_tokens": 0,
            "provider_extras": {},
            "request_params": {"status": "available", "parameters": {}},
            "cost_usd_snapshot": 0.0,
            "duration_sec": 0.1,
            "streamed": False,
            "time_to_first_token_sec": None,
            "gateway_request_id": f"gw-{trial_id.hex[:8]}",
            "cache_keys": [],
            "attempt": 1,
        },
        {
            "seq": 3,
            "kind": "terminus2_turn",
            **common,
            "turn_id": f"turn-{trial_id.hex[:8]}",
            "turn_index": 0,
            "gateway_request_id": f"gw-{trial_id.hex[:8]}",
            "parse_state": "ok",
            "completion_state": "continue",
            "analysis": "",
            "plan": "",
            "raw_response_excerpt": "",
        },
        {
            "seq": 4,
            "kind": "terminus2_command",
            **common,
            "turn_id": f"turn-{trial_id.hex[:8]}",
            "command_batch_id": "batch-1",
            "command_id": "cmd-1",
            "index": 0,
            "keystrokes": "ls\n",
            "duration_sec": 0.1,
        },
        {
            "seq": 5,
            "kind": "terminus2_terminal_observation",
            **common,
            "turn_id": f"turn-{trial_id.hex[:8]}",
            "command_batch_id": "batch-1",
            "observation_id": "obs-1",
            "text": "New Terminal Output:\nfile.txt\n",
            "capture_source": "incremental",
            "byte_len": 10,
            "truncated": False,
            "completeness": "full",
            "content_hash": "abc",
            "redaction_applied": False,
            "is_aggregate": False,
        },
        {
            "seq": 6,
            "kind": "terminus2_artifact_ref",
            **common,
            "artifact_kind": "terminus_2.pane",
            "sandbox_path": "/app/.loom/agent/trajectory.json",
            "content_hash": artifact_hash,
            "size_bytes": 42,
            "share_policy": "restricted",
        },
        {
            "seq": 7,
            "kind": "trial_end",
            **common,
            "step_id": "__trial__",
            "final_state": terminal_state,
            "reward": {"passed": 1.0} if terminal_state == "succeeded" else None,
            "failure_reason": (None if terminal_state == "succeeded" else "verifier_error"),
        },
    ]
    return b"".join((json.dumps(line) + "\n").encode() for line in lines)


def _seed_tb2_v2_trial(
    *,
    conn: object,
    fake_s3: _FakeS3Client,
    settings: LoomServiceSettings,
    team_id: UUID,
    trial_id: UUID,
    task_id: str,
    terminal_state: str = "succeeded",
    atif_final_state: str = "succeeded",
) -> bytes:
    native = json.dumps(
        {
            "schema_version": "ATIF-v1.7",
            "steps": [{"observation": {"results": [{"content": "native\n"}]}}],
        }
    ).encode()
    artifact_hash = hashlib.sha256(native).hexdigest()
    prefix = f"{team_id}/{trial_id}"
    artifact_key = f"{prefix}/main/.loom/agent/trajectory.json"
    verifier_log = b"pytest: 3 passed\n"
    verifier_log_key = f"{prefix}/main/.loom/verifier/pytest.log"
    verifier_meta = (
        json.dumps(
            {
                "schema_version": "1",
                "truncated": False,
                "original_bytes": len(verifier_log),
                "kept_bytes": len(verifier_log),
                "return_code": 0,
                "script_path": "/app/environment/tb2-tests/run-tests.sh",
                "log_path": ".loom/verifier/pytest.log",
            }
        )
        + "\n"
    ).encode()
    verifier_meta_key = f"{verifier_log_key}.meta.json"
    verifier_output = b'{"rewards":{"passed":1.0}}'
    verifier_output_key = f"{prefix}/main/.loom/verifier/output.json"
    fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/events.jsonl")] = (
        _tb2_v2_events_jsonl(
            trial_id=trial_id,
            artifact_hash=artifact_hash,
            terminal_state=terminal_state,
        )
    )
    fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/atif.json")] = json.dumps(
        {
            "schema_version": "ATIF-v1.7",
            "steps": [],
            "metadata": {
                "final_state": atif_final_state,
                "reward": ({"passed": 1.0} if atif_final_state == "succeeded" else None),
                "error": (
                    None
                    if atif_final_state == "succeeded"
                    else {"failure_reason": "verifier_error"}
                ),
            },
        }
    ).encode()
    fake_s3.objects[(settings.artifacts_bucket, artifact_key)] = native
    fake_s3.objects[(settings.artifacts_bucket, verifier_log_key)] = verifier_log
    fake_s3.objects[(settings.artifacts_bucket, verifier_meta_key)] = verifier_meta
    fake_s3.objects[(settings.artifacts_bucket, verifier_output_key)] = verifier_output
    conn.execute(
        update(Trial)
        .where(Trial.id == trial_id)
        .values(
            config={
                "agent_name": "terminus-2",
                "agent_model": {"provider": "yibu", "name": "glm-5.1-thinking"},
            },
            trajectory_index={
                "trajectory_uri": (f"s3://{settings.trajectories_bucket}/{prefix}/events.jsonl"),
                "atif_uri": f"s3://{settings.trajectories_bucket}/{prefix}/atif.json",
                "artifacts": [
                    {
                        "step_name": "main",
                        "bucket": "artifacts",
                        "key": artifact_key,
                        "size": len(native),
                        "content_hash": f"sha256:{artifact_hash}",
                    },
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": verifier_log_key,
                        "size": len(verifier_log),
                        "content_hash": (f"sha256:{hashlib.sha256(verifier_log).hexdigest()}"),
                        "share_status": "shared",
                        "blocked_reason": None,
                    },
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": verifier_meta_key,
                        "size": len(verifier_meta),
                        "content_hash": (f"sha256:{hashlib.sha256(verifier_meta).hexdigest()}"),
                        "share_status": "shared",
                        "blocked_reason": None,
                    },
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": verifier_output_key,
                        "size": len(verifier_output),
                        "content_hash": (f"sha256:{hashlib.sha256(verifier_output).hexdigest()}"),
                        "share_status": "shared",
                        "blocked_reason": None,
                    },
                ],
            },
        )
    )
    return native


async def test_raw_harbor_tb2_v2_export_rejects_legacy_runtime_stream(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    settings: LoomServiceSettings = delivery_setup["settings"]  # type: ignore[assignment]
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    emitted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            for trial_id in selected_trials.values():
                prefix = f"{delivery_setup['team_id']}/{trial_id}"
                fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/events.jsonl")] = (
                    json.dumps(
                        {
                            "seq": 1,
                            "kind": "terminus2_runtime_provenance",
                            "step_id": "main",
                            "trial_id": str(trial_id),
                            "emitted_at": emitted_at,
                            "parser_name": "json",
                            "prompt_hash": "abc",
                            "template_hashes": {},
                            "harbor_compat_sha": ("527d50deb63a5d279e8c20593c18a2cbc7f61f9e"),
                            "benchmark_provenance": None,
                            "loom_runtime_revision": "1.0",
                        }
                    )
                    + "\n"
                    + json.dumps(
                        {
                            "seq": 2,
                            "kind": "agent_thought",
                            "step_id": "main",
                            "trial_id": str(trial_id),
                            "emitted_at": emitted_at,
                            "content": "legacy subprocess thought",
                        }
                    )
                    + "\n"
                    + json.dumps(
                        {
                            "seq": 3,
                            "kind": "trial_end",
                            "step_id": "__trial__",
                            "trial_id": str(trial_id),
                            "emitted_at": emitted_at,
                            "final_state": "succeeded",
                            "reward": {"score": 0.0},
                            "failure_reason": None,
                        }
                    )
                    + "\n"
                ).encode()
                conn.execute(
                    update(Trial)
                    .where(Trial.id == trial_id)
                    .values(
                        config={
                            "agent_name": "terminus-2",
                            "agent_model": {
                                "provider": "yibu",
                                "name": "glm-5.1-thinking",
                            },
                        }
                    )
                )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor-tb2-v2",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "legacy_runtime_stream"


@pytest.mark.parametrize("native_verifier", [False, True])
async def test_raw_harbor_tb2_v2_export_from_typed_events(
    native_verifier: bool,
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    settings: LoomServiceSettings = delivery_setup["settings"]  # type: ignore[assignment]
    team_id: UUID = delivery_setup["team_id"]  # type: ignore[assignment]
    app.state.settings.public_base_url = "https://yylx.world/dev"

    task_bundle_key = f"{task_ids[0]}/task.toml"
    fake_s3.objects[("task-bundles", task_bundle_key)] = (f'id = "{task_ids[0]}"\n').encode()
    fake_s3.objects[("task-bundles", f"{task_ids[0]}/instruction.md")] = b"solve the task\n"

    sync_engine = create_engine(postgres_url)
    native_by_trial: dict[UUID, bytes] = {}
    try:
        with sync_engine.begin() as conn:
            captured_at_base = datetime.now(UTC)
            for index, task_id in enumerate(task_ids, start=1):
                trial_id = selected_trials[task_id]
                native_by_trial[trial_id] = _seed_tb2_v2_trial(
                    conn=conn,
                    fake_s3=fake_s3,
                    settings=settings,
                    team_id=team_id,
                    trial_id=trial_id,
                    task_id=task_id,
                )
                if native_verifier:
                    from tests.unit.test_native_delivery_acceptance import native_trial
                    native_trial_row, native_store = native_trial(identity=trial_id, team=team_id, reward=1)
                    existing_index = conn.execute(select(Trial.trajectory_index).where(Trial.id == trial_id)).scalar_one()
                    existing_result = conn.execute(select(Trial.result).where(Trial.id == trial_id)).scalar_one()
                    artifact_rows = [row for row in existing_index["artifacts"] if "/.loom/verifier/" not in row["key"]]
                    artifact_rows.extend(native_trial_row.trajectory_index["artifacts"])
                    fake_s3.objects.update(native_store._objects)
                    conn.execute(update(Trial).where(Trial.id == trial_id).values(
                        attempt_count=1,
                        trajectory_index={**existing_index, "attempt": 1, "artifacts": artifact_rows},
                        result={**existing_result, "runtime_result": native_trial_row.result["runtime_result"]},
                    ))
                if index != 1:
                    continue
                conn.execute(
                    insert(LlmCall).values(
                        team_id=team_id,
                        trial_id=trial_id,
                        step_id="step-1",
                        dialect="openai_facade",
                        model="gpt-4o",
                        input_tokens=10,
                        output_tokens=5,
                        provider_extras={
                            "_loom_raw_provider_log": {
                                "schema_version": "1",
                                "request": {
                                    "body": {
                                        "messages": [
                                            {"role": "user", "content": "prompt 1"},
                                        ]
                                    }
                                },
                                "response": {
                                    "body": {
                                        "choices": [
                                            {
                                                "message": {
                                                    "role": "assistant",
                                                    "content": "answer 1",
                                                }
                                            }
                                        ]
                                    }
                                },
                            }
                        },
                        request_params={"status": "available", "parameters": {}},
                        cost_usd=0,
                        rate_card_hash="facade:operator-supplied",
                        captured_at=captured_at_base + timedelta(seconds=index),
                    )
                )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor-tb2-v2",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "raw-harbor-tb2-v2"
    assert body["manifest"]["export_profile"] == {
        "name": "raw-harbor-tb2",
        "version": "2",
        "source_of_truth": "harbor-checkpoint-bridge",
        "audit_spine": "loom_trajectory.jsonl",
        "model_input_trajectory": "model_input_trajectory.json",
        "execution_trajectory": "trajectory.json",
        "terminal_transcript": "terminal_transcript.jsonl",
    }
    assert body["archive_filename"].endswith("-raw-harbor-tb2-v2.tar.gz")

    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], body["storage"]["key"])]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        names = set(tar.getnames())
        assert "derived/sft_messages.jsonl" not in names
        assert f"agent_runs/{first_task}/{first_trial}/model_input_trajectory.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/terminal_transcript.jsonl" in names
        assert f"agent_runs/{first_task}/{first_trial}/native/harbor_trajectory.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/verifier/output.json" in names
        if native_verifier:
            assert f"agent_runs/{first_task}/{first_trial}/verifier/02-verifier.stdout" in names
            assert f"agent_runs/{first_task}/{first_trial}/verifier/runtime-result.json" in names
            assert f"agent_runs/{first_task}/{first_trial}/verifier/script.log.meta.json" not in names
        trajectory = json.load(
            tar.extractfile(f"agent_runs/{first_task}/{first_trial}/trajectory.json")  # type: ignore[arg-type]
        )
        assert trajectory["schema_version"] == "harbor-tb2-v2-projection"
        assert isinstance(trajectory["steps"][0]["observation"], str)
        assert trajectory["steps"][0]["observation"].startswith("New Terminal Output")
        model_input = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/model_input_trajectory.json"
            )
        )
        assert model_input["source_of_truth"] == "provider_logs"
        assert model_input["calls"][0]["messages"] == [
            {"role": "user", "content": "prompt 1"},
            {"role": "assistant", "content": "answer 1"},
        ]
        transcript_lines = (
            tar.extractfile(  # type: ignore[union-attr]
                f"agent_runs/{first_task}/{first_trial}/terminal_transcript.jsonl"
            )
            .read()
            .decode()
            .splitlines()
        )
        assert json.loads(transcript_lines[0])["text"].startswith("New Terminal Output")
        native = tar.extractfile(  # type: ignore[union-attr]
            f"agent_runs/{first_task}/{first_trial}/native/harbor_trajectory.json"
        ).read()
        assert native == native_by_trial[first_trial]
        artifact_manifest = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json"
            )
        )
        assert "verifier/output.json" in {
            row["path"]
            for row in artifact_manifest["artifacts"]
            if row.get("kind") == "verifier_artifact"
        }
        rendered = json.dumps(trajectory)
        # task_id values like `task-0001` contain `sk-` as a substring; use export scan patterns.
        _assert_no_secret_patterns(rendered)


def _openhands_events_jsonl(*, trial_id: UUID, artifact_hash: str) -> bytes:
    emitted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    common = {
        "trial_id": str(trial_id),
        "step_id": "main",
        "emitted_at": emitted_at,
    }
    lines = [
        {
            "seq": 1,
            "kind": "openhands_sdk_runtime_provenance",
            **common,
            "sdk_version": "1.34.0",
            "openhands_tools_version": "1.34.0",
            "loom_bridge_revision": "1.0",
        },
        {
            "seq": 2,
            "kind": "openhands_sdk_artifact_ref",
            **common,
            "artifact_kind": "openhands_sdk.events",
            "sandbox_path": ".loom/agent/openhands_sdk_events.json",
            "content_hash": artifact_hash,
            "size_bytes": 128,
            "share_policy": "restricted",
        },
        {
            "seq": 3,
            "kind": "trial_end",
            **common,
            "step_id": "__trial__",
            "final_state": "succeeded",
            "reward": {"score": 1.0},
            "failure_reason": None,
        },
    ]
    return b"".join((json.dumps(line) + "\n").encode() for line in lines)


def _seed_openhands_trial(
    *,
    conn: object,
    fake_s3: _FakeS3Client,
    settings: LoomServiceSettings,
    team_id: UUID,
    trial_id: UUID,
    task_id: str,
) -> bytes:
    native = json.dumps(
        [
            {
                "event_type": "ActionEvent",
                "tool_call_id": "call-1",
                "tool_name": "terminal",
                "reasoning_content": "inspect workspace",
                "thought": [],
                "tool_call": {
                    "function": {
                        "arguments": '{"command": "pwd"}',
                    }
                },
            }
        ]
    ).encode()
    artifact_hash = hashlib.sha256(native).hexdigest()
    prefix = f"{team_id}/{trial_id}"
    artifact_key = f"{prefix}/main/.loom/agent/openhands_sdk_events.json"
    fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/events.jsonl")] = (
        _openhands_events_jsonl(trial_id=trial_id, artifact_hash=artifact_hash)
    )
    fake_s3.objects[(settings.artifacts_bucket, artifact_key)] = native
    conn.execute(
        update(Trial)
        .where(Trial.id == trial_id)
        .values(
            config={
                "agent_name": "openhands-sdk",
                "agent_model": {"provider": "yibu", "name": "gpt-4o"},
            },
            trajectory_index={
                "trajectory_uri": (f"s3://{settings.trajectories_bucket}/{prefix}/events.jsonl"),
                "atif_uri": f"s3://{settings.trajectories_bucket}/{prefix}/atif.json",
                "artifacts": [
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": artifact_key,
                        "size": len(native),
                        "content_hash": f"sha256:{artifact_hash}",
                    },
                ],
            },
        )
    )
    return native


async def test_openhands_export_from_typed_events(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    settings: LoomServiceSettings = delivery_setup["settings"]  # type: ignore[assignment]
    team_id: UUID = delivery_setup["team_id"]  # type: ignore[assignment]

    task_bundle_key = f"{task_ids[0]}/task.toml"
    fake_s3.objects[("task-bundles", task_bundle_key)] = (f'id = "{task_ids[0]}"\n').encode()
    fake_s3.objects[("task-bundles", f"{task_ids[0]}/instruction.md")] = b"solve the task\n"

    sync_engine = create_engine(postgres_url)
    native_by_trial: dict[UUID, bytes] = {}
    try:
        with sync_engine.begin() as conn:
            for task_id in task_ids:
                trial_id = selected_trials[task_id]
                native_by_trial[trial_id] = _seed_openhands_trial(
                    conn=conn,
                    fake_s3=fake_s3,
                    settings=settings,
                    team_id=team_id,
                    trial_id=trial_id,
                    task_id=task_id,
                )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "openhands-export",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["manifest"]["mode"] == "openhands-export"
    assert body["manifest"]["export_profile"] == {
        "name": "openhands-export",
        "version": "1",
        "source_of_truth": "native/openhands_sdk_events.json",
        "audit_spine": "loom_trajectory.jsonl",
        "model_input_trajectory": "model_input_trajectory.json",
        "execution_trajectory": "trajectory.json",
    }
    assert body["archive_filename"].endswith("-openhands-export.tar.gz")

    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], body["storage"]["key"])]
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        names = set(tar.getnames())
        assert "derived/sft_messages.jsonl" not in names
        assert f"agent_runs/{first_task}/{first_trial}/model_input_trajectory.json" in names
        assert f"agent_runs/{first_task}/{first_trial}/native/openhands_sdk_events.json" in names
        trajectory = json.load(
            tar.extractfile(f"agent_runs/{first_task}/{first_trial}/trajectory.json")  # type: ignore[arg-type]
        )
        assert trajectory["schema_version"] == "openhands-export-projection"
        assert trajectory["events"][0]["reasoning_content"] == "inspect workspace"
        native = tar.extractfile(  # type: ignore[union-attr]
            f"agent_runs/{first_task}/{first_trial}/native/openhands_sdk_events.json"
        ).read()
        assert native == native_by_trial[first_trial]


async def test_raw_harbor_tb2_v1_packs_verifier_audit_artifacts(
    delivery_setup: dict[str, object],
    postgres_url: str,
) -> None:
    """#865: indexed .loom/verifier/** objects are embedded in the delivery tar."""
    app = delivery_setup["app"]
    raw = str(delivery_setup["raw"])
    main_batch_id = delivery_setup["main_batch_id"]
    supplemental_batch_id = delivery_setup["supplemental_batch_id"]
    targeted_batch_id = delivery_setup["targeted_batch_id"]
    selected_trials: dict[str, UUID] = delivery_setup["selected_trials"]  # type: ignore[assignment]
    task_ids: list[str] = delivery_setup["task_ids"]  # type: ignore[assignment]
    settings: LoomServiceSettings = delivery_setup["settings"]  # type: ignore[assignment]
    fake_s3: _FakeS3Client = delivery_setup["fake_s3"]  # type: ignore[assignment]
    team_id: UUID = delivery_setup["team_id"]  # type: ignore[assignment]

    first_task = task_ids[0]
    first_trial = selected_trials[first_task]
    log_body = b"--- stdout ---\npytest: 3 passed\n"
    meta_body = (
        json.dumps(
            {
                "schema_version": "1",
                "truncated": False,
                "original_bytes": len(log_body),
                "kept_bytes": len(log_body),
                "return_code": 0,
                "script_path": "/app/verifier/run.sh",
                "log_path": ".loom/verifier/script.log",
            }
        )
        + "\n"
    ).encode()
    log_key = f"{team_id}/{first_trial}/main/.loom/verifier/script.log"
    meta_key = f"{team_id}/{first_trial}/main/.loom/verifier/script.log.meta.json"
    output_body = b'{"rewards":{"passed":1.0}}'
    output_key = f"{team_id}/{first_trial}/main/.loom/verifier/output.json"
    fake_s3.objects[(settings.artifacts_bucket, log_key)] = log_body
    fake_s3.objects[(settings.artifacts_bucket, meta_key)] = meta_body
    fake_s3.objects[(settings.artifacts_bucket, output_key)] = output_body

    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as conn:
            prefix = f"{team_id}/{first_trial}"
            terminal_event = {
                "kind": "trial_end",
                "seq": 1,
                "emitted_at": datetime.now(UTC).isoformat(),
                "trial_id": str(first_trial),
                "step_id": "__trial__",
                "final_state": "succeeded",
                "reward": {"passed": 1.0},
                "failure_reason": None,
            }
            fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/events.jsonl")] = (
                json.dumps(terminal_event) + "\n"
            ).encode()
            fake_s3.objects[(settings.trajectories_bucket, f"{prefix}/atif.json")] = json.dumps(
                {
                    "version": "1.7",
                    "trial_id": str(first_trial),
                    "metadata": {
                        "final_state": "succeeded",
                        "reward": {"passed": 1.0},
                    },
                }
            ).encode()
            conn.execute(
                update(Trial)
                .where(Trial.id == first_trial)
                .values(
                    result={
                        "aggregate_reward": 1.0,
                        "reward": {"passed": 1.0},
                    },
                    trajectory_index={
                        "trajectory_uri": (
                            f"s3://{settings.trajectories_bucket}/{prefix}/events.jsonl"
                        ),
                        "atif_uri": (f"s3://{settings.trajectories_bucket}/{prefix}/atif.json"),
                        "artifacts": [
                            {
                                "step_name": "main",
                                "bucket": settings.artifacts_bucket,
                                "key": log_key,
                                "size": len(log_body),
                                "content_hash": (f"sha256:{hashlib.sha256(log_body).hexdigest()}"),
                                "share_status": "shared",
                                "blocked_reason": None,
                            },
                            {
                                "step_name": "main",
                                "bucket": settings.artifacts_bucket,
                                "key": meta_key,
                                "size": len(meta_body),
                                "content_hash": (f"sha256:{hashlib.sha256(meta_body).hexdigest()}"),
                                "share_status": "shared",
                                "blocked_reason": None,
                            },
                            {
                                "step_name": "main",
                                "bucket": settings.artifacts_bucket,
                                "key": output_key,
                                "size": len(output_body),
                                "content_hash": (
                                    f"sha256:{hashlib.sha256(output_body).hexdigest()}"
                                ),
                                "share_status": "shared",
                                "blocked_reason": None,
                            },
                        ],
                    },
                )
            )
            conn.execute(
                insert(LlmCall).values(
                    team_id=team_id,
                    trial_id=first_trial,
                    step_id="step-1",
                    dialect="openai_facade",
                    model="gpt-4o",
                    input_tokens=10,
                    output_tokens=5,
                    provider_extras={
                        "_loom_raw_provider_log": {
                            "schema_version": "1",
                            "ref": "llm_calls/raw-1/provider_extras/_loom_raw_provider_log",
                            "request": {
                                "body": {"messages": [{"role": "user", "content": "prompt 1"}]}
                            },
                            "response": {
                                "body": {
                                    "choices": [
                                        {
                                            "message": {
                                                "role": "assistant",
                                                "content": "answer 1",
                                            }
                                        }
                                    ]
                                }
                            },
                        }
                    },
                    request_params={"status": "available", "parameters": {}},
                    cost_usd=0,
                    rate_card_hash="facade:operator-supplied",
                    captured_at=datetime.now(UTC),
                )
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.post(
            f"/api/v1/batches/{main_batch_id}/delivery-export",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "mode": "raw-harbor-tb2-v1",
                "supplemental_batch_ids": [
                    str(supplemental_batch_id),
                    str(targeted_batch_id),
                ],
            },
        )
    assert response.status_code == 201, response.text
    body = response.json()
    archive_bytes = fake_s3.objects[(body["storage"]["bucket"], body["storage"]["key"])]

    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
        _assert_complete_payload_checksums(tar)
        log_member = f"agent_runs/{first_task}/{first_trial}/verifier/script.log"
        meta_member = f"agent_runs/{first_task}/{first_trial}/verifier/script.log.meta.json"
        output_member = f"agent_runs/{first_task}/{first_trial}/verifier/output.json"
        assert log_member in set(tar.getnames())
        assert meta_member in set(tar.getnames())
        assert output_member in set(tar.getnames())
        assert tar.extractfile(log_member).read() == log_body  # type: ignore[union-attr]
        assert tar.extractfile(output_member).read() == output_body  # type: ignore[union-attr]
        manifest = json.load(
            tar.extractfile(  # type: ignore[arg-type]
                f"agent_runs/{first_task}/{first_trial}/artifact_manifest.json"
            )
        )
        verifier_entries = [
            row for row in manifest["artifacts"] if row.get("kind") == "verifier_artifact"
        ]
        assert {row["path"] for row in verifier_entries} == {
            "verifier/script.log",
            "verifier/script.log.meta.json",
            "verifier/output.json",
        }
        log_entry = next(row for row in verifier_entries if row["path"] == "verifier/script.log")
        assert log_entry["truncated"] is False
        assert log_entry["share_status"] == "shared"
        assert log_entry["size_bytes"] == len(log_body)
        assert log_entry["content_hash"].startswith("sha256:")
        assert log_entry["step_name"] == "main"
