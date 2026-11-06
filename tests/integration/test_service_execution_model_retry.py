"""A native model retry archives its predecessor before reserving a fresh lease."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Artifact,
    Batch,
    ServiceExecutionLease,
    Task,
    TeamQuota,
    Trial,
    TrialEvent,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    enqueue_execution_transition,
    finalize_committed_service_execution,
    model_retry_policy,
    record_execution_event,
)
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
from loom_control_plane.service_execution_output import (
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
)
from loom_control_plane.trial_cancellation import cancel_trial_under_authority
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _complete_output_contract,
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)


async def _commit_runtime_result(sessions, store, lease, now, reason="gateway_error", *, succeeded=False):
    async with sessions() as session:
        await enqueue_execution_transition(session, lease_id=lease.id,
            expected_generation=1, desired_state="start", now=now)
        await session.commit()
    result = _runtime_result_payload(lease, started_at=now)
    files = {}
    if succeeded:
        files = {
            "01-agent.stdout": b"", "01-agent.stderr": b"",
            "02-verifier.stdout": b"", "02-verifier.stderr": b"",
            "artifacts/answer.txt": b"42", "trajectory/events.jsonl": b"",
            "verifier/output.json": canonical_document({"rewards": {"passed": 1.0}}),
            "accounting/usage.json": canonical_document({
                "schema_version": "loom.service-execution-usage.v1", "model": "openai/test",
                "call_count": 0, "calls": [], "totals": {key: 0 for key in (
                    "input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens",
                    "thinking_tokens", "cost_usd", "duration_sec")},
            }),
        }
        for phase in result["phases"]:
            for stream in (phase["stdout"], phase["stderr"]):
                stream.update(bytes_seen=0, bytes_saved=0, sha256=digest_bytes(b""))
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        result.update(status="succeeded", verifier_rewards={"passed": 1.0}, outputs=[{
            **item.model_dump(mode="json"), "state": "captured",
            "size_bytes": len(files[item.relative_path]), "sha256": digest_bytes(files[item.relative_path]),
        } for item in plan.output_declarations])
    else:
        result.update(status="task_error", failure_reason=reason, phases=[], partial_evidence=True)
    files = {"result.json": canonical_document(result), **dict(sorted(files.items()))}
    route = ServiceExecutionOutputRouteService(
        service=ArtifactCommitService(store=store, bucket="artifacts",
            repository=SqlArtifactCommitRepository(session_factory=sessions, store=store, bucket="artifacts")),
        session_factory=sessions,
    )
    grant = await route.prepare(lease=lease, request=ServiceExecutionOutputPrepareV1(
        schema_version="loom.service-execution-output-prepare.v1", request_id=uuid4(),
        lease_id=lease.id, generation=1, execution_role="attempt",
        files=tuple(ServiceExecutionOutputFileV1(relative_path=path, media_type="application/json",
            size_bytes=len(payload), sha256=digest_bytes(payload)) for path, payload in files.items()),
    ))
    upload_id, token = UUID(grant["upload_session_id"]), grant["upload_token"]

    for index, payload in enumerate(files.values()):
        async def body(payload=payload):
            yield payload

        receipt = PartReceiptV1.model_validate(await route.put_part(lease=lease,
            session_id=upload_id, file_index=index, part_number=1, content_length=len(payload),
            content_sha256=digest_bytes(payload), upload_token=token, body=body()))
        await route.complete_file(lease=lease, session_id=upload_id, file_index=index,
            ordered_parts=(receipt,), upload_token=token)
    await route.commit(lease=lease, session_id=upload_id, upload_token=token)
    async with sessions() as session:
        current = await session.get(ServiceExecutionLease, lease.id)
        current.observed_state = "finalizing"
        assert await finalize_committed_service_execution(session, lease_id=lease.id, observed_at=now)
        await session.commit()


async def _delete(sessions, lease_id, now):
    async with sessions() as session:
        lease = await session.get(ServiceExecutionLease, lease_id)
        await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
            ordinal=lease.last_event_ordinal + 1, event_kind="deleted", payload={}, observed_at=now)
        await session.commit()


@pytest.mark.parametrize("fence", [None, "success", "policy", "retry_on", "team", "cancel", "cancel_after_copy", "cancel_queued", "ceiling_queued", "retain", "archive", "checkpoint", "batch_cancel", "pause"])
async def test_native_model_retry_waits_for_archive_cleanup_and_backoff(postgres_url, fence):
    await _exercise_native_model_retry(postgres_url, fence)


async def test_native_model_retry_policy_uses_restricted_actuator_role(
    platform_database, monkeypatch,  # noqa: F811 -- imported shared pytest fixture
):
    from loom import nebius_platform_bootstrap as bootstrap

    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    password = "restricted-native-retry-" + "a" * 32
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", password)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    url = make_url(platform_database).set(drivername="postgresql+psycopg")
    actuator = create_async_engine(url.set(username="loom_actuator", password=password))
    restricted = async_sessionmaker(actuator, expire_on_commit=False)
    try:
        async with restricted() as session:
            assert await session.scalar(text("SELECT current_user")) == "loom_actuator"
            assert not await session.scalar(text(
                "SELECT has_table_privilege(current_user, 'episode_checkpoints', 'SELECT')"
            ))
        # This policy is shared with PoolExecutionOutbox.accept_grant. Checkpoint
        # qualification belongs to the CP archive; mutable fences remain shared.
        await _exercise_native_model_retry(url, None, retry_policy_sessions=restricted)
    finally:
        await actuator.dispose()


async def _exercise_native_model_retry(postgres_url, fence, *, retry_policy_sessions=None):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = FakeObjectStore()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            task = await session.get(Task, trial.task_id)
            task.config = {"schema_version": "1", "task": {"id": task.id, "name": "Native retry"},
                "environment": {"os": "linux", "docker_image": "example.test/task@sha256:" + "a" * 64,
                    "network_policies_supported": ["gateway-only"], "baseline_network_policy": {"kind": "gateway-only"}},
                "agent": {"name": "direct-completion", "version": "1"},
                "verifier": {"name": "script", "env_mode": "shared"},
                "steps": [{"name": "main"}]}
            trial.config = {"agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "test"},
                "retry": {"max_attempts": 2, "backoff": {"base_sec": 30, "jitter": 0}}}
            session.add(TeamQuota(team_id=trial.team_id, max_attempts_ceiling=3))
            if fence in {"batch_cancel", "pause"}:
                batch = Batch(id=uuid4(), team_id=trial.team_id, name="retry-parent", task_filter={},
                    trial_config=trial.config, state="running", created_by_token_prefix="test")
                session.add(batch)
                await session.flush()
                trial.batch_id = batch.id
            first = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        await _commit_runtime_result(sessions, store, first, now)
        materializer = ServiceExecutionMaterializer(session_factory=sessions,
            source_store=store, source_bucket="artifacts", canonical_store=store,
            artifacts_bucket="artifacts", trajectories_bucket="trajectories")
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == "materializing"  # Terminal-to-queued is forbidden by PostgreSQL.
            if fence == "policy":
                trial.config = {**trial.config, "retry": {"max_attempts": 1}}
            elif fence == "retry_on":
                trial.config = {**trial.config, "retry": {"retry_on": []}}
            elif fence == "team":
                (await session.get(TeamQuota, trial.team_id)).max_attempts_ceiling = 1
            elif fence == "checkpoint":
                from loom_control_plane.terminus_recovery import (
                    reclaim_terminus_execution,
                    write_episode_checkpoint,
                )
                recovery = await reclaim_terminus_execution(session, trial_id=trial.id, step_id="agent")
                await write_episode_checkpoint(session, execution_id=recovery.agent_execution_id,
                    run_attempt_id=recovery.agent_run_attempt_id, episode=1, active_role="student",
                    last_call_ordinal=1, last_seq=1, tmux_session_id=None)
            elif fence == "batch_cancel":
                (await session.get(Batch, trial.batch_id)).state = "cancelling"
            elif fence == "pause":
                (await session.get(Batch, trial.batch_id)).admission_paused_at = now
            await session.commit()
        copied_claim = copied_result = None
        if fence == "cancel_after_copy":
            copied_claim = await materializer.claim_one()
            copied_result = await materializer._load_and_materialize(copied_claim)
        if fence in {"cancel", "cancel_after_copy"}:
            cancelled = await cancel_trial_under_authority(
                session_factory=sessions, trial_id=trial_id, team_id=first.team_id,
            )
            assert cancelled is not None and cancelled["state"] == "cancelled"
            replay = await cancel_trial_under_authority(
                session_factory=sessions, trial_id=trial_id, team_id=first.team_id,
            )
            assert replay is not None and replay["state"] == "cancelled"
        if fence == "retain":
            retained = await cancel_trial_under_authority(
                session_factory=sessions, trial_id=trial_id, team_id=first.team_id,
                preserve_retained=True,
            )
            assert retained is not None and retained["outcome"] == "retained"
        if copied_claim is not None:
            with pytest.raises(RuntimeError, match="cancelled during canonical derivation") as changed:
                await materializer._commit(copied_claim, copied_result)
            await materializer._retry(copied_claim, changed.value)
            async with sessions() as session:
                (await session.get(ServiceExecutionLease, first.id)).materialization_next_attempt_at = now
                await session.commit()
        if fence == "archive":
            from loom_control_plane.service_execution_materializer import (
                MaterializationIntegrityError,
            )
            claim = await materializer.claim_one()
            await materializer._unavailable(claim, MaterializationIntegrityError("source_object_missing"))
        else:
            assert await materializer.run_once()
        if fence in {"cancel", "cancel_after_copy"}:
            await _delete(sessions, first.id, now)
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            archived = await session.get(ServiceExecutionLease, first.id)
            artifact = await session.scalar(select(Artifact).where(Artifact.control_producer_id == first.id))
            if fence not in {None, "success", "pause", "cancel_queued", "ceiling_queued"}:
                assert trial.state in {"failed", "cancelled"}
                if fence in {"cancel", "cancel_after_copy"}:
                    assert trial.state == "cancelled" and archived.materialization_state == "committed"
                    assert trial.cancellation_observed_at >= trial.cancellation_requested_at
                    assert trial.result["runtime_result"]["failure_reason"] == "gateway_error"
                if fence == "retain":
                    assert trial.state == "failed" and archived.materialization_state == "committed"
                    assert trial.cancellation_retained_at is not None
                    assert trial.cancellation_requested_at is None
                    assert trial.trajectory_index["attempt"] == 1
                if fence == "checkpoint":
                    assert trial.state == "failed" and trial.failure_reason == "gateway_error"
                    assert archived.materialization_state == "committed"
                    assert trial.trajectory_index["attempt"] == 1
                    assert artifact is not None
                    assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)
                        .where(ServiceExecutionLease.trial_id == trial_id)) == 1
                assert trial.next_attempt_at is None
                return
            assert trial.state == "queued", (archived.materialization_error_code, archived.materialization_error_message)
            assert trial.attempt_count == 1
            assert trial.next_attempt_at >= now + timedelta(seconds=30)
            assert archived.materialization_state == "committed"
            assert artifact.storage["attempt"] == 1
            assert artifact.artifact_metadata["trajectory_index"]["attempt"] == 1
            assert trial.trajectory_index["attempt"] == 1
            # SSE cannot consume this predecessor's sequence while queued.
            from loom_service.routes.trajectory import _read_events_with_minio_fallback
            assert await _read_events_with_minio_fallback(session, None, trial=trial,
                bucket="trajectories", after_seq=-1, limit=100) == []
            assert await session.scalar(select(func.count()).select_from(TrialEvent).where(TrialEvent.trial_id == trial_id)) == 0
            retry_at = trial.next_attempt_at
            with pytest.raises(ServiceExecutionConflict):
                await _reserve(session, trial_id=trial_id, target=target, now=retry_at)
            await session.rollback()
        original_archive = dict(store.objects)
        if fence == "cancel_queued":
            assert await cancel_trial_under_authority(session_factory=sessions,
                trial_id=trial_id, team_id=first.team_id) is not None
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial.cancellation_requested_at is not None
                assert trial.cancellation_observed_at is None
        await _delete(sessions, first.id, now)
        if fence in {"cancel_queued", "ceiling_queued"}:
            if fence == "ceiling_queued":
                from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted
                async with sessions() as session:
                    (await session.get(TeamQuota, first.team_id)).max_attempts_ceiling = 1
                    await session.flush()
                    assert trial_id in await sweep_retry_exhausted(session)
                    await session.commit()
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial.state == ("cancelled" if fence == "cancel_queued" else "failed")
                if fence == "cancel_queued":
                    assert trial.failure_reason == "cancelled" and trial.next_attempt_at is None
                    assert trial.cancellation_observed_at >= trial.cancellation_requested_at
                assert trial.trajectory_index["attempt"] == 1
                assert all(store.objects[key] == body for key, body in original_archive.items())
            return
        if retry_policy_sessions is not None:
            async with retry_policy_sessions.begin() as session:
                trial = await session.get(Trial, trial_id, with_for_update=True)
                assert await model_retry_policy(session, trial=trial, reason="gateway_error") is not None
            async with sessions.begin() as session:
                (await session.get(TeamQuota, first.team_id)).max_attempts_ceiling = 1
            async with retry_policy_sessions.begin() as session:
                trial = await session.get(Trial, trial_id, with_for_update=True)
                assert await model_retry_policy(session, trial=trial, reason="gateway_error") is None
            async with sessions.begin() as session:
                (await session.get(TeamQuota, first.team_id)).max_attempts_ceiling = 3
        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict):
                await _reserve(session, trial_id=trial_id, target=target, now=retry_at - timedelta(seconds=1))
            await session.rollback()
        if fence == "pause":
            from loom_control_plane.service_execution import ServiceExecutionParentTerminalError
            async with sessions() as session:
                with pytest.raises(ServiceExecutionParentTerminalError):
                    await _reserve(session, trial_id=trial_id, target=target, now=retry_at)
                await session.rollback()
                trial = await session.get(Trial, trial_id)
                (await session.get(Batch, trial.batch_id)).admission_paused_at = None
                await session.commit()
        async def reserve_successor():
            async with sessions() as session:
                try:
                    successor = await _reserve(session, trial_id=trial_id, target=target, now=retry_at,
                        runtime_contract=_complete_output_contract(now=retry_at) if fence == "success" else None)
                    await session.commit()
                    return successor
                except ServiceExecutionConflict:
                    await session.rollback()
                    return None

        successors = [row for row in await asyncio.gather(reserve_successor(), reserve_successor()) if row is not None]
        assert len(successors) == 1
        second = successors[0]
        assert second.id != first.id and second.attempt == 2
        async with sessions() as session:
            assert (await session.get(Trial, trial_id)).trajectory_index is None
        await _commit_runtime_result(sessions, store, second, retry_at, reason="provider_transport_disconnect", succeeded=fence == "success")
        if fence == "success":
            # Successful compute keeps its existing archive-to-completion contract.
            assert await cancel_trial_under_authority(session_factory=sessions,
                trial_id=trial_id, team_id=first.team_id) is None
        await _delete(sessions, second.id, retry_at)
        assert await materializer.run_once()
        assert all(store.objects[key] == body for key, body in original_archive.items())
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == ("succeeded" if fence == "success" else "failed") and trial.attempt_count == 2
            assert trial.failure_reason == (None if fence == "success" else "provider_transport_disconnect")
            assert trial.trajectory_index["attempt"] == 2
            assert await session.scalar(select(func.count()).select_from(Artifact).where(Artifact.trial_id == trial_id)) == 2
            assert await session.scalar(select(func.count()).select_from(TrialEvent).where(TrialEvent.trial_id == trial_id)) > 0
            assert (await session.get(ServiceExecutionLease, first.id)).materialization_state == "committed"
            from loom_service.trial_bundles import canonical_bundle_from_artifact
            artifacts = list((await session.scalars(select(Artifact).where(Artifact.trial_id == trial_id))).all())
            selected = [bundle for artifact in artifacts if (bundle := canonical_bundle_from_artifact(artifact, trial=trial)) is not None]
            assert len(selected) == 1 and selected[0].attempt == 2
            end = await session.scalar(select(TrialEvent).where(TrialEvent.trial_id == trial_id, TrialEvent.kind == "trial_end"))
            assert end.payload["final_state"] == trial.state
            assert end.payload["failure_reason"] == trial.failure_reason
    finally:
        await engine.dispose()
