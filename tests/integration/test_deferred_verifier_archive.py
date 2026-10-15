"""Archive real parent/verifier uploads together, through PostgreSQL and MinIO."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from minio.versioningconfig import ENABLED, VersioningConfig
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer

from loom.db.schema import Artifact, DataLifecycleObject, ServiceExecutionLease, Task, Trial
from loom.execution_contract import VerifierTopology
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import finalize_committed_service_execution
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
from loom_control_plane.service_execution_output import (
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
)
from tests.integration.test_service_execution_leases import (
    _complete_output_contract,
    _deferred_verifier_contract,
    _requirements,
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)
from tests.integration.test_service_execution_spool_materialization import (
    _store,
    independent_minio_endpoints,  # noqa: F401 -- expose the reusable pytest fixture
)


async def _upload_and_finalize(route, sessions, lease, plan, payloads, *, now, failed=False):
    """Use the real prepare/multipart/commit/finalize path for each execution."""
    result = _runtime_result_payload(lease, started_at=now)
    role = "agent" if lease.execution_role == "attempt" else "verifier"
    phase = next(item for item in result["phases"] if item["role"] == role)
    phase["ordinal"] = 1
    phase["exit_code"] = 1 if failed else 0
    for stream in ("stdout", "stderr"):
        body = (b"verifier failed\n" if failed else b"completed\n") if stream == "stdout" else b""
        path = phase[stream]["path"]
        payloads[path] = body
        phase[stream].update(sha256=digest_bytes(body), bytes_seen=len(body), bytes_saved=len(body))
    result.update(
        phases=[phase], container_roles=["execution", role],
        status="verifier_error" if failed else "succeeded", partial_evidence=failed,
        verifier_rewards=None if role == "agent" else {"passed": 0.0 if failed else 1.0},
        outputs=[{
            **item.model_dump(mode="json"), "state": "captured",
            "size_bytes": len(payloads[item.relative_path]),
            "sha256": digest_bytes(payloads[item.relative_path]),
        } for item in plan.output_declarations],
    )
    payloads = {"result.json": canonical_document(result), **dict(sorted(payloads.items()))}
    grant = await route.prepare(lease=lease, request=ServiceExecutionOutputPrepareV1(
        schema_version="loom.service-execution-output-prepare.v1", request_id=uuid4(),
        lease_id=lease.id, generation=lease.generation, execution_role=lease.execution_role,
        files=tuple(ServiceExecutionOutputFileV1(
            relative_path=path, media_type="application/octet-stream", size_bytes=len(body),
            sha256=digest_bytes(body),
        ) for path, body in payloads.items()),
    ))
    upload_id, token = UUID(grant["upload_session_id"]), grant["upload_token"]
    for index, payload in enumerate(payloads.values()):
        async def body(payload=payload):
            yield payload
        receipt = PartReceiptV1.model_validate(await route.put_part(
            lease=lease, session_id=upload_id, file_index=index, part_number=1,
            content_length=len(payload), content_sha256=digest_bytes(payload),
            upload_token=token, body=body(),
        ))
        await route.complete_file(lease=lease, session_id=upload_id, file_index=index,
                                  ordered_parts=(receipt,), upload_token=token)
    await route.commit(lease=lease, session_id=upload_id, upload_token=token)
    async with sessions() as session:
        current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
        current.observed_state = "finalizing"
        assert await finalize_committed_service_execution(
            session, lease_id=lease.id, observed_at=now + timedelta(seconds=4))
        current.desired_state = current.observed_state = "deleted"
        current.cleanup_state = "complete"
        current.deleted_at = now + timedelta(seconds=5)
        await session.commit()


async def _seed_deferred_archive(sessions, source_store, *, now, failed=False):
    """Real committed uploads, ready for ordinary or operator archival."""
    complete = _complete_output_contract(now=now)
    parent_plan = ExecutionRuntimePlanV1.model_validate({
        **complete.canonical_payload(), "verifier_execution": "separate_execution", "verifier": None,
        "output_declarations": [item.model_dump(mode="json") for item in complete.output_declarations
                                if item.kind != "verifier"],
    })
    child_plan = _deferred_verifier_contract(output_declarations=[
        item.model_dump(mode="json") for item in complete.output_declarations if item.kind == "verifier"])
    requirements = _requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION)
    route = ServiceExecutionOutputRouteService(
        service=ArtifactCommitService(store=source_store, bucket="artifacts",
            repository=SqlArtifactCommitRepository(session_factory=sessions, store=source_store, bucket="artifacts")),
        session_factory=sessions,
    )
    async with sessions() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        trial = await session.get(Trial, trial_id)
        task = await session.get(Task, trial.task_id)
        task.config = {
            "schema_version": "1", "task": {"id": task.id, "name": "Deferred archive"},
            "environment": {"os": "linux", "cpu_arch": "x86_64", "gpu_vendor": "none",
                "docker_image": parent_plan.task_image_ref, "network_policies_supported": ["gateway-only"],
                "baseline_network_policy": {"kind": "gateway-only"}},
            "agent": {"name": "direct-completion", "version": "1.0"},
            "verifier": {"name": "script", "env_mode": "shared"},
            "steps": [{"name": "main", "artifacts": ["answer.txt"]}],
        }
        trial.config = {"schema_version": "1", "agent_name": "direct-completion",
                        "agent_model": {"provider": "openai", "name": "gpt-5"}}
        parent = await _reserve(session, trial_id=trial_id, target=target, now=now,
                                requirements=requirements, runtime_contract=parent_plan)
        await session.commit()
    usage = {"rate_card_hash": "rate-v1", "gateway_request_id": "gateway-deferred", "finish_reason": "stop",
             "input_tokens": 1, "cached_input_tokens": 0, "cache_write_tokens": 0, "output_tokens": 1,
             "thinking_tokens": 0, "provider_extras": {}, "cost_usd": 0.01, "duration_sec": 1.0,
             "streamed": False, "time_to_first_token_sec": None, "attempt": 1}
    trace = canonical_document({"schema_version": "loom.service-execution-llm-call.v1", "turn": 0,
        "started_at": now.isoformat(), "finished_at": (now + timedelta(seconds=1)).isoformat(),
        "model": "openai/gpt-5", "request": {"messages": [{"role": "user", "content": "answer"}],
            "request_params": {}}, "response": {"role": "assistant", "content": "42"}, "usage": usage})
    await _upload_and_finalize(route, sessions, parent, parent_plan, {
        "artifacts/answer.txt": b"42\n", "trajectory/events.jsonl": trace,
        "accounting/usage.json": canonical_document({"schema_version": "loom.service-execution-usage.v1",
            "model": "openai/gpt-5", "call_count": 1, "calls": [usage], "totals": {
                key: usage[key] for key in ("input_tokens", "cached_input_tokens", "cache_write_tokens",
                                            "output_tokens", "thinking_tokens", "cost_usd", "duration_sec")}}),
    }, now=now)
    async with sessions() as session:
        child = await _reserve(session, trial_id=trial_id, target=target, now=now,
                               requirements=requirements, runtime_contract=child_plan, parent_lease_id=parent.id)
        await session.commit()
    await _upload_and_finalize(route, sessions, child, child_plan, {
        "verifier/output.json": canonical_document({"rewards": {"passed": 0.0 if failed else 1.0}}),
    }, now=now, failed=failed)
    return trial_id, parent, child


@pytest.mark.parametrize("failed", [False, True], ids=["succeeded", "failed-preserved"])
async def test_deferred_verifier_archive_from_committed_uploads(
    postgres_url: str, independent_minio_endpoints: tuple[MinioContainer, MinioContainer], failed: bool,  # noqa: F811
) -> None:
    spool, canonical = independent_minio_endpoints
    for container, buckets in ((spool, ("artifacts",)), (canonical, ("artifacts", "trajectories"))):
        for bucket in buckets:
            container.get_client().set_bucket_versioning(bucket, VersioningConfig(ENABLED))
    source_store, canonical_store = _store(spool), _store(canonical)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    materializer = ServiceExecutionMaterializer(
        session_factory=sessions, source_store=source_store, source_bucket="artifacts",
        canonical_store=canonical_store, artifacts_bucket="artifacts", trajectories_bucket="trajectories",
        source_retention_seconds=3600,
    )
    try:
        trial_id, parent, child = await _seed_deferred_archive(
            sessions, source_store, now=now, failed=failed)
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            before = copy.deepcopy((trial.state, trial.result, trial.failure_reason, trial.failure_message,
                                    trial.finished_at, trial.attempt_count))
            assert trial.state == ("failed" if failed else "materializing")
            assert (await session.get(ServiceExecutionLease, child.id)).materialization_attempts == 0
        sources = {obj.object_name: obj.version_id for obj in spool.get_client().list_objects(
            "artifacts", prefix=f"service-executions/{parent.team_id}/", recursive=True, include_version=True)}
        assert sources and all(sources.values())
        claim = await materializer.claim_one(lease_id=parent.id)
        assert claim is not None
        assert await materializer.claim_one(lease_id=child.id) is None
        result = await materializer._load_and_materialize(claim)
        assert result.final_trial_state == ("failed" if failed else "succeeded")
        assert await materializer._commit(claim, result)
        assert not await materializer._commit(claim, result)
        assert await materializer.claim_one(lease_id=parent.id) is None
        assert not await materializer.cleanup_source_once()
        async with sessions() as session:
            parent_row, child_row = [await session.get(ServiceExecutionLease, item.id) for item in (parent, child)]
            trial = await session.get(Trial, trial_id)
            assert trial.state == ("failed" if failed else "succeeded")
            assert trial.result == before[1] and trial.attempt_count == before[5] == 1
            if failed:
                assert (trial.failure_reason, trial.failure_message, trial.finished_at) == before[2:5]
            for row in (parent_row, child_row):
                assert row.materialization_state == "committed" and row.materialization_attempts == 1
                assert row.canonical_trajectory_sha256 == result.events_sha256
                assert row.canonical_atif_sha256 == result.atif_sha256
                assert row.materialization_claim_id is row.materialization_claim_expires_at is None
                assert row.source_cleanup_state == "retained"
                assert row.source_retain_until == parent_row.source_retain_until > now
            child_artifact = await session.scalar(select(Artifact).where(Artifact.control_producer_id == child.id))
            assert child_artifact.artifact_metadata["archived_with_lease_id"] == str(parent.id)
            assert child_artifact.artifact_metadata["materialization_state"] == "committed"
            artifact = await session.get(Artifact, result.artifact_id)
            files = artifact.storage["files"] + artifact.storage["source_evidence"]
            assert any(item["relative_path"].startswith("verifier-execution/") for item in files)
            for item in files:
                assert item["version_id"]
                response = canonical.get_client().get_object(item["bucket"], item["key"], version_id=item["version_id"])
                try:
                    assert digest_bytes(response.read()) == item["sha256"]
                finally:
                    response.close()
                    response.release_conn()
            objects = list(await session.scalars(select(DataLifecycleObject).where(
                DataLifecycleObject.authority_id == artifact.lifecycle_authority_id)))
            assert {item["key"] for item in files} <= {obj.object_key for obj in objects}
            for name, expected in (("trajectory", result.events_sha256), ("atif", result.atif_sha256)):
                index = trial.trajectory_index
                assert index[f"{name}_version_id"]
                key = index[f"{name}_uri"].removeprefix("s3://trajectories/")
                body = await canonical_store.get_object(bucket="trajectories", key=key)
                assert digest_bytes(body) == expected
            assert json.loads(result.atif_body)["metadata"]["final_state"] == trial.state
        assert sources == {obj.object_name: obj.version_id for obj in spool.get_client().list_objects(
            "artifacts", prefix=f"service-executions/{parent.team_id}/", recursive=True, include_version=True)}
    finally:
        await engine.dispose()
