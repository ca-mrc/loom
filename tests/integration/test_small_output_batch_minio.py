"""Real TCP gateway, PostgreSQL and S3 publication for synthetic runtime output."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer

from loom.db.schema import ServiceExecutionLease
from loom.pipeline.artifact_commit import ArtifactCommitError, ArtifactCommitService, UploadAuthV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    record_execution_event,
)
from loom_control_plane.service_execution_output import ServiceExecutionOutputRouteService
from loom_llm_gateway.routes.service_execution import router
from tests.integration.pipeline_artifact_testkit import chunks, digest, plan
from tests.integration.test_gateway_model_concurrency import _tcp_gateway
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- pytest fixture
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)

pytestmark = [pytest.mark.docker, pytest.mark.usefixtures("_cleanup_service_execution_test_rows")]


@asynccontextmanager
async def _batch_upload(postgres_url, shared_minio, payloads):  # type: ignore[no-untyped-def]
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    config = shared_minio.get_config()
    store = MinioObjectStore(endpoint_url="http://" + config["endpoint"],
                             access_key=config["access_key"], secret_key=config["secret_key"])
    bucket = "small-output-batches"
    await store.ensure_bucket(bucket)
    try:
        async with sessions.begin() as db:
            now = datetime.now(UTC)
            trial_id, target = await _seed_ready_trial(db, now=now)
            lease = await _reserve(db, trial_id=trial_id, target=target, now=now)
        repository = SqlArtifactCommitRepository(session_factory=sessions, store=store, bucket=bucket)
        service = ArtifactCommitService(store=store, bucket=bucket, repository=repository)
        artifact_id = uuid4()
        grant = await service.prepare_session(
            producer=ServiceExecutionOutputRouteService._producer(lease),
            files=[plan(index=i, artifact_id=artifact_id,
                        path="result.json" if i == 0 else f"{i}.stdout", name="trial_bundle",
                        artifact_type="loom.trial-artifact-bundle.v1", producer="service",
                        role="semantic_document" if i == 0 else "payload", payload=body)
                   for i, body in enumerate(payloads)],
            idempotency_key="bounded-recovery", request_digest=digest(b"bounded-recovery"),
        )
        yield service, repository, store, grant, UploadAuthV1(upload_token=grant.upload_token)
    finally:
        await engine.dispose()


async def test_small_batch_recovers_a_part_accepted_before_response_loss(
    postgres_url: str, shared_minio: MinioContainer, monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = [b"first", b"second", b"third"]
    async with _batch_upload(postgres_url, shared_minio, payloads) as fixture:
        service, repository, store, grant, auth = fixture
        original = store.upload_part_stream
        calls = 0

        async def lose_part(upload, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal calls
            await original(upload, **kwargs)
            calls += 1
            if calls == 2:
                raise OSError("part accepted; response lost")

        monkeypatch.setattr(store, "upload_part_stream", lose_part)
        with pytest.raises(OSError):
            await service.write_small_files(
                session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth
            )
        partial = await repository.get(grant.upload_session_id)
        assert partial.files[0].receipts[1].size_bytes == len(payloads[0])
        assert partial.files[1].upload is not None and not partial.files[1].receipts
        result = await service.write_small_files(
            session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth
        )
        assert [file.sha256 for file in result] == [digest(body) for body in payloads]
        assert (await repository.get(grant.upload_session_id)).state == "uploaded"


async def test_small_batch_attaches_all_parts_before_completion_and_abort(
    postgres_url: str, shared_minio: MinioContainer, monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = [b"first", b"second", b"third"]
    async with _batch_upload(postgres_url, shared_minio, payloads) as fixture:
        service, repository, store, grant, auth = fixture
        original = store.complete_multipart_upload
        started, release = asyncio.Event(), asyncio.Event()

        async def pause_completion(upload):  # type: ignore[no-untyped-def]
            started.set()
            await release.wait()
            return await original(upload)

        monkeypatch.setattr(store, "complete_multipart_upload", pause_completion)
        task = asyncio.create_task(service.write_small_files(
            session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth,
        ))
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            durable = await repository.get(grant.upload_session_id)
            assert all(file.upload is not None and 1 in file.receipts for file in durable.files)
            await service.abort_session(session_id=grant.upload_session_id, auth=auth, reason="producer_abort")
            release.set()
            with pytest.raises(ArtifactCommitError, match="session_not_uploading"):
                await asyncio.wait_for(task, timeout=5)
            assert (await repository.get(grant.upload_session_id)).state == "aborted"
            await service.abort_session(session_id=grant.upload_session_id, auth=auth, reason="producer_abort")
            with pytest.raises(ArtifactCommitError, match="session_not_uploading"):
                await service.write_small_files(
                    session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth,
                )
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_disjoint_file_groups_merge_with_legacy_multipart_progress(
    postgres_url: str, shared_minio: MinioContainer, monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = [b"first", b"second", b"third", b"fourth", b"legacy"]
    async with _batch_upload(postgres_url, shared_minio, payloads) as fixture:
        service, repository, _store, grant, auth = fixture
        original_get = repository.get
        barrier, reads = asyncio.Barrier(3), 0

        async def simultaneous_snapshots(session_id):  # type: ignore[no-untyped-def]
            nonlocal reads
            state = await original_get(session_id)
            reads += 1
            if reads <= 3:
                await asyncio.wait_for(barrier.wait(), timeout=5)
            return state

        monkeypatch.setattr(repository, "get", simultaneous_snapshots)

        async def legacy() -> None:
            body = payloads[4]
            receipt = await service.write_part(
                session_id=grant.upload_session_id, file_index=4, part_number=1,
                content_length=len(body), content_sha256=digest(body), body=chunks(body), auth=auth,
            )
            await service.complete_file(
                session_id=grant.upload_session_id, file_index=4, ordered_parts=[receipt], auth=auth,
            )

        files = list(enumerate(payloads))
        await asyncio.gather(
            service.write_small_files(session_id=grant.upload_session_id, files=files[:2], auth=auth),
            service.write_small_files(session_id=grant.upload_session_id, files=files[2:4], auth=auth),
            legacy(),
        )
        state = await original_get(grant.upload_session_id)
        assert state.state == "uploaded"
        assert state.actual_total_bytes == sum(map(len, payloads))
        assert [file.verified.sha256 for file in state.files] == [digest(body) for body in payloads]


@pytest.mark.parametrize("lost_phase", ["receipts", "verified"])
async def test_small_batch_recovers_a_durable_group_with_lost_response(
    postgres_url: str, shared_minio: MinioContainer, monkeypatch: pytest.MonkeyPatch,
    lost_phase: str,
) -> None:
    payloads = [b"first", b"second", b"third"]
    async with _batch_upload(postgres_url, shared_minio, payloads) as fixture:
        service, repository, store, grant, auth = fixture
        original_save = repository.save_files
        original_part, original_complete = store.upload_part_stream, store.complete_multipart_upload
        lost = False
        parts = completions = 0

        async def save_then_lose_response(state, *, file_indexes):  # type: ignore[no-untyped-def]
            nonlocal lost
            await original_save(state, file_indexes=file_indexes)
            phase = "verified" if all(state.files[i].verified is not None for i in file_indexes) else "receipts"
            if not lost and phase == lost_phase:
                lost = True
                raise OSError("durable group response lost")

        async def part(upload, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal parts
            parts += 1
            return await original_part(upload, **kwargs)

        async def complete(upload):  # type: ignore[no-untyped-def]
            nonlocal completions
            completions += 1
            return await original_complete(upload)

        monkeypatch.setattr(repository, "save_files", save_then_lose_response)
        monkeypatch.setattr(store, "upload_part_stream", part)
        monkeypatch.setattr(store, "complete_multipart_upload", complete)
        with pytest.raises(OSError, match="durable group response lost"):
            await service.write_small_files(
                session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth,
            )
        await service.write_small_files(
            session_id=grant.upload_session_id, files=list(enumerate(payloads)), auth=auth,
        )
        assert parts == completions == len(payloads)
        state = await repository.get(grant.upload_session_id)
        assert state.state == "uploaded"
        assert [file.verified.sha256 for file in state.files] == [digest(body) for body in payloads]


async def test_small_batch_real_http_sql_s3_commit_and_generation_fence(
    postgres_url: str,
    shared_minio: MinioContainer,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    monkeypatch.setattr(logging.getLogger("loom.upload_observation"), "propagate", True)
    engine = create_async_engine(postgres_url)
    commits = 0

    @event.listens_for(engine.sync_engine, "commit")
    def count_commit(_connection: Connection) -> None:
        nonlocal commits
        commits += 1

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    config = shared_minio.get_config()
    store = MinioObjectStore(
        endpoint_url="http://" + config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
    )
    bucket = "small-output-batches"
    await store.ensure_bucket(bucket)
    try:
        now = datetime.now(UTC)
        async with sessions.begin() as db:
            trial_id, target = await _seed_ready_trial(db, now=now)
            lease = await _reserve(db, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                db, lease_id=lease.id, expected_generation=1, desired_state="start", now=now
            )
            await record_execution_event(
                db,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "fixture-job",
                    "pod_uid": "fixture-pod",
                    "pod_ip": "127.0.0.1",
                    "resource_version": "1",
                },
                observed_at=now,
            )
        payloads = {}
        document = _runtime_result_payload(lease, started_at=now)
        for phase in document["phases"]:
            for name in ("stdout", "stderr"):
                stream = phase[name]
                body = b"synthetic output\n" if name == "stdout" else b""
                payloads[stream["path"]] = body
                stream.update(
                    sha256=digest_bytes(body), bytes_seen=len(body), bytes_saved=len(body)
                )
        payloads = {"result.json": canonical_document(document), **dict(sorted(payloads.items()))}
        repository = SqlArtifactCommitRepository(
            session_factory=sessions, store=store, bucket=bucket
        )
        app = FastAPI()
        app.state.session_factory = sessions
        app.state.service_execution_output_service = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(store=store, bucket=bucket, repository=repository),
            session_factory=sessions,
        )
        app.include_router(router)
        identity = {"lease_id": str(lease.id), "generation": 1, "execution_role": "attempt"}
        async with (
            _tcp_gateway(app) as origin,
            httpx.AsyncClient(base_url=origin, timeout=20, trust_env=False) as client,
        ):
            response = await client.post(
                "/internal/service-execution/outputs/prepare",
                json={
                    **identity,
                    "schema_version": "loom.service-execution-output-prepare.v1",
                    "request_id": str(uuid4()),
                    "files": [
                        {
                            "relative_path": path,
                            "media_type": "application/octet-stream",
                            "size_bytes": len(body),
                            "sha256": digest_bytes(body),
                        }
                        for path, body in payloads.items()
                    ],
                },
            )
            assert response.status_code == 201, response.text
            grant = response.json()
            assert grant["small_file_batch"]["max_files"] == 16
            path = f"/internal/service-execution/outputs/{grant['upload_session_id']}"
            headers = {"X-Loom-Upload-Token": grant["upload_token"]}
            batch = {
                **identity,
                "schema_version": "loom.service-execution-small-files.v1",
                "files": [
                    {"file_index": i, "content_base64": base64.b64encode(body).decode()}
                    for i, body in enumerate(payloads.values())
                ],
            }
            before_batch = commits
            caplog.clear()
            response = await client.post(path + "/small-files", json=batch, headers=headers)
            assert response.status_code == 200, response.text
            assert commits - before_batch <= 2
            observations = [json.loads(record.message) for record in caplog.records
                            if record.name == "loom.upload_observation"]
            for stage in ("database_pool", "database"):
                saves = [value for value in observations if value["stage"] == stage
                         and value["operation"] == "save_files" and value["event"] == "finished"]
                assert len(saves) == 2
                assert all(value["outcome"] == "success" for value in saves)
            serialized = json.dumps(observations)
            for sensitive in (grant["upload_token"], "synthetic output", bucket):
                assert sensitive not in serialized
            assert [item["sha256"] for item in response.json()["files"]] == [
                digest_bytes(body) for body in payloads.values()
            ]
            response = await client.post(path + "/small-files", json=batch, headers=headers)
            assert response.status_code == 200, response.text
            response = await client.post(
                path + "/small-files", json={**batch, "generation": 2}, headers=headers
            )
            assert response.status_code == 409
            assert response.json()["detail"] == "execution_generation_fenced"
            response = await client.post(
                path + "/commit",
                json={**identity, "schema_version": "loom.service-execution-output-commit.v1"},
                headers=headers,
            )
            assert response.status_code == 200, response.text
            assert response.json()["manifest_sha256"].startswith("sha256:")
        async with sessions() as db:
            current = await db.get(ServiceExecutionLease, lease.id)
            assert current.output_commit_state == "committed"
        assert not store._client.list_multipart_uploads(Bucket=bucket).get("Uploads")
    finally:
        store.close()
        await engine.dispose()
