from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization, Trial, TrialTaskImageMaterialization
from loom.execution_runtime_contract import RuntimeTaskInputV1
from loom.pipeline.keys import digest_bytes
from loom.service_execution_materialization import (
    ServiceExecutionInputFileV1,
    ServiceExecutionInputManifestV1,
)
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_output import (
    read_service_execution_input,
    resolve_service_execution_input_source,
)
from loom_control_plane.service_execution_task_snapshot import (
    ServiceExecutionTaskSnapshotError,
    resolve_service_execution_task_snapshot,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)


async def test_prepared_snapshot_preserves_inputs_after_task_update_and_retirement(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    snapshot_id = uuid4()
    body = b"original task instruction\n"
    manifest = ServiceExecutionInputManifestV1(
        task_revision_sha256="sha256:" + "2" * 64,
        files=(ServiceExecutionInputFileV1(
            relative_path="instruction.md", size_bytes=len(body), sha256=digest_bytes(body), mode="0644",
        ),),
    )
    manifest_body = manifest.canonical_bytes()
    manifest_digest = digest_bytes(manifest_body)
    store = FakeObjectStore()
    store.objects[("artifacts", "original/manifest.json")] = manifest_body
    store.objects[("artifacts", "original/task/instruction.md")] = body
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.source = "s3://artifacts/original/task/"
            task.source_provenance = {"service_execution_input": {
                "schema_version": "loom.service-execution-input.v1",
                "manifest_uri": "s3://artifacts/original/manifest.json",
                "manifest_sha256": manifest_digest, "file_count": 1, "total_bytes": len(body),
            }}
            task.config = {
                "schema_version": "1", "task": {"id": task.id, "name": "Original task"},
                "environment": {
                    "os": "linux", "cpu_arch": "x86_64", "dockerfile": "environment/Dockerfile",
                    "cpus": 1, "memory_mb": 1024, "storage_mb": 2048, "tmpfs": ["/tmp"],
                    "baseline_network_policy": {"kind": "gateway-only"},
                    "network_policies_supported": ["gateway-only"],
                },
                "agent": {"name": "direct-completion"}, "verifier": {"name": "script"},
            }
            original_config = task.config
            plan = _runtime_contract(now=now).model_copy(update={"task_input": RuntimeTaskInputV1(
                manifest_sha256=manifest_digest, file_count=1, total_bytes=len(body),
            )})
            snapshot = TaskImageMaterialization(
                id=snapshot_id, materialization_key=uuid4().hex * 2, task_id=task.id,
                task_checksum="2" * 64, cpu_arch="x86_64", task_config=task.config,
                task_source=task.source, task_source_provenance=task.source_provenance,
                state="ready", registry_images={"task": plan.task_image_ref},
            )
            session.add(snapshot)
            await session.flush()
            association = TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=snapshot.id)
            session.add(association)
            plan = plan.model_copy(update={
                "task_image_materialization_id": snapshot.id, "agent_image_ref": plan.task_image_ref,
            })
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now, runtime_contract=plan)
            snapshot.state = "retiring"
            snapshot.registry_images = {}
            task.config = {"new_revision": True}
            task.source = "s3://artifacts/new/task/"
            task.source_provenance = {}
            await session.commit()

            for state in ("retiring", "retired"):
                snapshot.state = state
                await session.flush()
                source = await resolve_service_execution_input_source(session, lease=lease, artifacts_bucket="artifacts")
                resolved = await read_service_execution_input(source, store=store)
                assert resolved.manifest == manifest
                assert resolved.prefix == "original/task/"
                frozen = await resolve_service_execution_task_snapshot(session, lease=lease)
                assert frozen.config == original_config

            bad_lease = SimpleNamespace(
                trial_id=lease.trial_id, team_id=uuid4(), runtime_contract_json=lease.runtime_contract_json,
            )
            with pytest.raises(ServiceExecutionTaskSnapshotError, match="trial_mismatch"):
                await resolve_service_execution_task_snapshot(session, lease=bad_lease)
            bad_lease.team_id = lease.team_id

            for changed in (
                {"task_image_materialization_id": uuid4()},
                {"task_revision_sha256": "sha256:" + "9" * 64},
            ):
                bad_lease.runtime_contract_json = plan.model_copy(update=changed).canonical_payload()
                with pytest.raises(ServiceExecutionTaskSnapshotError, match="binding_mismatch"):
                    await resolve_service_execution_task_snapshot(session, lease=bad_lease)

            snapshot.task_id = "a-different-task"
            await session.flush()
            with pytest.raises(ServiceExecutionTaskSnapshotError, match="binding_mismatch"):
                await resolve_service_execution_task_snapshot(session, lease=lease)
            snapshot.task_id = task.id
            await session.delete(association)
            await session.flush()
            with pytest.raises(ServiceExecutionTaskSnapshotError, match="binding_mismatch"):
                await resolve_service_execution_task_snapshot(session, lease=lease)
    finally:
        try:
            # The shared execution fixture cleans its seed Trial/lease/Team.
            # This test additionally owns one frozen image prerequisite.
            async with sessions() as session, session.begin():
                await session.execute(delete(TrialTaskImageMaterialization).where(
                    TrialTaskImageMaterialization.materialization_id == snapshot_id,
                ))
                await session.execute(delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == snapshot_id,
                ))
        finally:
            await engine.dispose()
