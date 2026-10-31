"""Real TCP gateway, PostgreSQL and S3 publication for synthetic runtime output."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer

from loom.db.schema import ServiceExecutionLease
from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    record_execution_event,
)
from loom_control_plane.service_execution_output import ServiceExecutionOutputRouteService
from loom_llm_gateway.routes.service_execution import router
from tests.integration.test_gateway_model_concurrency import _tcp_gateway
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- pytest fixture
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)

pytestmark = [pytest.mark.docker, pytest.mark.usefixtures("_cleanup_service_execution_test_rows")]


async def test_small_batch_real_http_sql_s3_commit_and_generation_fence(
    postgres_url: str,
    shared_minio: MinioContainer,
) -> None:
    engine = create_async_engine(postgres_url)
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
            response = await client.post(path + "/small-files", json=batch, headers=headers)
            assert response.status_code == 200, response.text
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
