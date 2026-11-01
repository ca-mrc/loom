"""Object-store latency must not consume the Gateway's shared DB pool."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from loom.db.schema import ServiceExecutionLease, Task, Trial
from loom.execution_runtime_contract import RuntimeTaskInputV1
from loom.pipeline.keys import digest_bytes
from loom.service_execution_materialization import (
    ServiceExecutionInputFileV1,
    ServiceExecutionInputManifestV1,
)
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_output import ServiceExecutionPeerV1
from loom_llm_gateway.routes import service_execution as routes
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)


class SlowManifestStore(FakeObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.reads = 0
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        if key == "task-inputs/manifest.json":
            self.reads += 1
            if self.reads == 2:
                self.blocked.set()
            await self.release.wait()
        return await super().get_object(bucket=bucket, key=key)


@pytest.mark.parametrize("path", ["manifest", "files/0"])
@pytest.mark.parametrize("drift", [False, True])
async def test_slow_input_reads_leave_connections_for_peer_authorization(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, path: str, drift: bool,
) -> None:
    engine = create_async_engine(postgres_url, pool_size=2, max_overflow=0, pool_timeout=0.1)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = SlowManifestStore()
    body = b"original task instruction\n"
    manifest = ServiceExecutionInputManifestV1(
        task_revision_sha256="sha256:" + "2" * 64,
        files=(ServiceExecutionInputFileV1(
            relative_path="instruction.md", size_bytes=len(body),
            sha256=digest_bytes(body), mode="0644",
        ),),
    )
    manifest_body = manifest.canonical_bytes()
    manifest_digest = digest_bytes(manifest_body)
    store.objects[("artifacts", "task-inputs/manifest.json")] = manifest_body
    store.objects[("artifacts", "task-bundles/example/instruction.md")] = body
    requests: list[asyncio.Task[httpx.Response]] = []
    try:
        now = datetime.now(UTC)
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.source = "s3://artifacts/task-bundles/example/"
            task.source_provenance = {"service_execution_input": {
                "schema_version": "loom.service-execution-input.v1",
                "manifest_uri": "s3://artifacts/task-inputs/manifest.json",
                "manifest_sha256": manifest_digest, "file_count": 1, "total_bytes": len(body),
            }}
            plan = _runtime_contract(now=now).model_copy(update={"task_input": RuntimeTaskInputV1(
                manifest_sha256=manifest_digest, file_count=1, total_bytes=len(body),
            )})
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now, runtime_contract=plan)
            await session.commit()

        # The native identity adapter is outside this test. Keep real ORM
        # checkout/expunge and the actual input snapshot/manifest validation.
        async def verified_pod(*args, **kwargs):  # type: ignore[no-untyped-def]
            return None

        async def authorize(session: AsyncSession, **kwargs) -> ServiceExecutionLease:  # type: ignore[no-untyped-def]
            persisted = await session.get(ServiceExecutionLease, kwargs["identity"].lease_id)
            assert persisted is not None
            return persisted

        monkeypatch.setattr(routes, "_verified_pod", verified_pod)
        monkeypatch.setattr(routes, "authorize_service_execution_peer", authorize)
        app = FastAPI()
        app.include_router(routes.router)
        app.state.session_factory = sessions
        app.state.artifact_store = store
        app.state.settings = SimpleNamespace(artifacts_bucket="artifacts")
        headers = {
            "X-Loom-Execution-Lease-Id": str(lease.id),
            "X-Loom-Execution-Generation": str(lease.resource_generation),
            "X-Loom-Execution-Role": "attempt",
        }
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            requests = [asyncio.create_task(client.get(
                "/internal/service-execution/inputs/" + path, headers=headers,
            )) for _ in range(2)]
            await asyncio.wait_for(store.blocked.wait(), timeout=5)
            if drift:
                store.objects[("artifacts", "task-inputs/manifest.json")] += b"\n"
            try:
                authorized = await routes._authorize(
                    Request({"type": "http", "app": app, "headers": [], "client": ("127.0.0.1", 1)}),
                    ServiceExecutionPeerV1(lease_id=lease.id, generation=lease.resource_generation, execution_role="attempt"),
                )
                assert authorized.id == lease.id
            finally:
                store.release.set()
                responses = await asyncio.gather(*requests)
            expected = manifest_body if path == "manifest" else body
            if drift:
                assert all(response.status_code == 409 for response in responses)
                assert all(response.json() == {"detail": "task_input_manifest_drift"} for response in responses)
            else:
                assert all(response.status_code == 200 and response.content == expected for response in responses)
                assert all(response.headers["X-Loom-Content-SHA256"] == digest_bytes(expected) for response in responses)
    finally:
        store.release.set()
        if requests:
            await asyncio.gather(*requests, return_exceptions=True)
        await engine.dispose()
