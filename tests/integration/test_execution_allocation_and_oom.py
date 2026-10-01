"""Database regressions for target allocation and late native OOM evidence."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import Batch, ExecutionCostReservation, ServiceExecutionLease, Task, Trial
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    ProbeV1,
    SidecarContainerV1,
    runtime_pod_resources,
)
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    record_execution_event,
)
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)


@pytest.mark.parametrize("node_share", [True, False])
async def test_scheduler_freezes_node_share_in_lease_and_finance(postgres_url, node_share):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
            from loom.service_execution_materialization import build_nebius_runtime_profile

            original = _runtime_contract(now=now)
            profile = build_nebius_runtime_profile(candidate_sha=original.candidate_sha,
                task_image_ref=original.task_image_ref, runtime_image_ref=original.runtime_image_ref,
                runtime_binary_sha256=original.runtime_binary_sha256, image_admission=original.image_admission)
            if node_share:
                profile = profile.model_copy(update={"resource_allocation_policy": "node-share-v1"})
            trial = await session.get(Trial, trial_id)
            batch = await session.get(Batch, trial.batch_id)
            batch.service_execution_runtime_profile = profile.model_dump(mode="json")
            trial.config = {"schema_version": "1", "agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "test", "source": "api"}}
            task = await session.get(Task, trial.task_id)
            config = {**task.config}
            config.pop("service_execution")
            config["environment"] = {k: v for k, v in config["environment"].items() if k != "tmpfs"}
            config["agent"] = {"name": "direct-completion"}
            config["verifier"] = {"name": "script", "args": {"script_path": "verifier/check.sh"}}
            config["steps"] = [{"name": "main", "instruction_file": "instruction.md", "artifacts": ["answer.txt"]}]
            task.config = config
            task.source_provenance = {"service_execution_input": {
                "schema_version": "loom.service-execution-input.v1",
                "manifest_uri": "s3://artifacts/task-inputs/task.json", "manifest_sha256": "sha256:" + "d" * 64,
                "file_count": 3, "total_bytes": 4096,
            }}
        async with sessions() as session, session.begin():
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            )
            assert lease is not None
            plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            # Original code reserves only 1024 MiB despite a larger node share.
            if node_share:
                assert plan.task_resources.memory_mib > 1024
                assert plan.node_resource_allocation.target_id == target.target_id
            else:
                assert plan.task_resources.memory_mib == 1024
                assert plan.node_resource_allocation is None
            total = runtime_pod_resources(plan)
            cost = await session.scalar(select(ExecutionCostReservation).where(
                ExecutionCostReservation.trial_id == trial_id,
            ))
            assert cost.requested_memory_mib == total.memory_mib
            assert cost.requested_cpu_millis == total.cpu_millis
            assert cost.requested_ephemeral_storage_mib == total.ephemeral_storage_mib
            assert lease.workload_requirements_json["memory_mib"] == 1024
    finally:
        await engine.dispose()


@pytest.mark.parametrize("already_failed", [False, True])
@pytest.mark.parametrize("replacement_timestamp", [False, True])
async def test_delayed_oom_replaces_generic_cause_but_preserves_outcome_and_output(
    postgres_url, already_failed, replacement_timestamp,
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            plan = _runtime_contract(now=now)
            agent_image = "registry.example/worker@sha256:" + "d" * 64
            sandboxes = [SidecarContainerV1(role_name=role, private_sandbox=True,
                image_ref=plan.task_image_ref, argv=("/loom/bin/loom-sandbox-runtime",),
                resources=plan.task_resources,
                startup_probe=ProbeV1(kind="exec", argv=("/bin/true",)),
                readiness_probe=ProbeV1(kind="exec", argv=("/bin/true",)))
                for role in ("task-sandbox", "verifier-sandbox")]
            plan = ExecutionRuntimePlanV1.model_validate({**plan.canonical_payload(),
                "agent_image_ref": agent_image, "sidecars": sandboxes,
                "image_admission": signed_image_admission_bundle(
                    (plan.task_image_ref, plan.runtime_image_ref, agent_image), now=now),
            })
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now, runtime_contract=plan)
            first = {
                "normalized_state": "failed", "job_uid": "job", "pod_uid": "pod",
                "reason": "SandboxRestarted", "message": "generic sandbox loss",
                "container_diagnostics": [{"name": "task-sandbox", "restart_count": 1}],
            }
            if replacement_timestamp:
                first["container_diagnostics"][0]["previous_termination"] = {
                    "reason": "Error", "exit_code": 1,
                    "started_at": (now + timedelta(seconds=1)).isoformat(),
                    "finished_at": (now + timedelta(seconds=1)).isoformat(),
                }
            await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                ordinal=1, event_kind="kubernetes_observed", payload=first, observed_at=now)
            if already_failed:
                for state in ("start", "finalize"):
                    await enqueue_execution_transition(session, lease_id=lease.id,
                        expected_generation=lease.generation, desired_state=state, now=now)
                await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                    ordinal=2, event_kind="finalized", observed_at=now,
                    payload={"trial_state": "failed", "failure_reason": "runtime_error",
                             "failure_message": "stop_processes transport_error"})
            trial = await session.get(Trial, trial_id)
            trial.trajectory_index = {"retained": "partial-native-trajectory"}
        async with sessions() as session, session.begin():
            late = {**first, "normalized_state": "oom_killed", "container_diagnostics": [{
                "name": "task-sandbox", "restart_count": 1,
                "previous_termination": {"reason": "OOMKilled", "exit_code": 137,
                    "started_at": (now - timedelta(minutes=1)).isoformat(),
                    "finished_at": now.isoformat()},
            }]}
            await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                ordinal=3, event_kind="kubernetes_observed", payload=late,
                observed_at=now + timedelta(seconds=10))
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current.error_code == "oom_killed"
            assert "OOMKilled" in current.error_message
            trial = await session.get(Trial, trial_id)
            assert trial.trajectory_index == {"retained": "partial-native-trajectory"}
            if already_failed:
                assert trial.state == "failed" and trial.failure_reason == "oom_killed"
                assert current.observed_state == "finalized"
            else:
                assert current.observed_state == "failed"
            # Cleanup from the replacement incarnation cannot erase the root.
            await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                ordinal=4, event_kind="failed", observed_at=now + timedelta(seconds=11),
                payload={"error_code": "transport_error", "error_message": "cleanup failed"})
            assert current.error_code == "oom_killed"
        if already_failed:
            from loom_cli.eval_cmd import _print_diagnosis_report
            from loom_service.app import register_api_routes
            from loom_service.dependencies import authed_session

            app = FastAPI()
            register_api_routes(app)
            app.state.settings = SimpleNamespace()

            async def authorized():
                async with sessions() as session:
                    yield session, AuthContext(token_hash=b"test", type="team",
                        scopes=["read:own"], team_id=trial.team_id, expires_at=None)

            app.dependency_overrides[authed_session] = authorized
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                detail = await client.get(f"/api/v1/trials/{trial_id}")
                debug = await client.get(f"/api/v1/trials/{trial_id}/debug")
                report = await client.get(f"/api/v1/trials/{trial_id}/diagnosis")
                usage = await client.get(f"/api/v1/trials/{trial_id}/resource-usage")
            assert detail.status_code == debug.status_code == report.status_code == usage.status_code == 200
            evidence = debug.json()["execution_failure"]
            assert evidence["memory_limit_mib"] == 1024
            assert evidence["supporting_events"][-1]["reason"] == "transport_error"
            assert detail.json()["failure_message"] == report.json()["summary"] == evidence["message"]
            assert debug.json()["failure"]["rerun_recommendation"] == "operator_approval"
            assert usage.json()["termination_failures"][0]["confirmed_oom_attempts"] == 1
            _print_diagnosis_report(report.json())
    finally:
        await engine.dispose()


@pytest.mark.parametrize("termination", [
    {"reason": "Error", "exit_code": 1},
    None,
])
async def test_non_oom_termination_keeps_runtime_cause(postgres_url, termination):
    from loom.execution_diagnosis_store import execution_failure_groups, read_execution_failure

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now,
                                   runtime_contract=_runtime_contract(now=now))
            for state in ("start", "finalize"):
                await enqueue_execution_transition(session, lease_id=lease.id,
                    expected_generation=lease.generation, desired_state=state, now=now)
            await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                ordinal=1, event_kind="finalized", observed_at=now,
                payload={"trial_state": "failed", "failure_reason": "runtime_error",
                         "failure_message": "controller exited"})
            container = {"name": "execution", "restart_count": 0}
            if termination is not None:
                container["current_termination"] = {
                    **termination, "started_at": now.isoformat(), "finished_at": now.isoformat(),
                }
            await record_execution_event(session, lease_id=lease.id, generation=lease.generation,
                ordinal=2, event_kind="kubernetes_observed", observed_at=now + timedelta(seconds=1),
                payload={"normalized_state": "failed", "job_uid": "job", "pod_uid": "pod",
                         "container_diagnostics": [container]})
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            diagnosis = await read_execution_failure(session, current)
            assert diagnosis is not None and diagnosis["reason"] != "oom_killed"
            assert current.error_code != "oom_killed"
            assert trial.failure_reason == "runtime_error"
            assert await execution_failure_groups(session, trial_id=trial_id) == []
    finally:
        await engine.dispose()
