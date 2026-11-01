"""Real PostgreSQL prerequisite admission without running a builder or model."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ExecutionBudgetPolicy,
    ExecutionCostReservation,
    LlmCall,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Task,
    TaskImageMaterialization,
    TeamQuota,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import workload_requirements_from_task
from loom.execution_runtime_contract import ExecutionRuntimePlanV1, runtime_pod_resources
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    compile_service_execution_plan,
)
from loom.task_image_materialization import (
    ensure_task_image_materializations,
    get_trial_task_image_execution_grant,
    resolve_prepared_task,
    task_image_materialization_key,
)
from loom_control_plane.service_execution import ServiceExecutionConflict, reserve_trial_execution
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.support.execution_image_admission import signed_image_admission_bundle

TASK_IMAGE = "registry.example/private-task@sha256:" + "f" * 64
CONTROLLER_IMAGE = "registry.example/controller@sha256:" + "9" * 64


async def _seed_preparing_trial(
    session: AsyncSession, *, now: datetime, state: str, fixture: bool = False, independent_verifier: bool = False,
) -> tuple[UUID, dict[str, UUID]]:
    trial_id, target = await fixtures._seed_ready_trial(session, now=now)
    await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
    trial = await session.get(Trial, trial_id)
    assert trial is not None
    task = await session.get(Task, trial.task_id)
    batch = await session.get(Batch, trial.batch_id)
    assert task is not None and batch is not None
    plan = fixtures._runtime_contract(now=now)
    profile = ServiceExecutionRuntimeProfileV1(
        candidate_sha=plan.candidate_sha,
        execution_class_id=plan.execution_class_id,
        task_image_ref=plan.task_image_ref,
        agent_image_ref=CONTROLLER_IMAGE,
        runtime_image_ref=plan.runtime_image_ref,
        runtime_binary_sha256=plan.runtime_binary_sha256,
        service_lifecycle_ready=fixture,
        image_admission=signed_image_admission_bundle(
            (plan.task_image_ref, CONTROLLER_IMAGE, plan.runtime_image_ref), now=now,
        ),
    )
    batch.service_execution_runtime_profile = profile.model_dump(mode="json")
    trial.config = {
        "agent_name": "terminus-2",
        "agent_model": {"provider": "openai", "name": "gpt-5", "source": "api"},
    }
    trial.requires_caps = {**trial.requires_caps, "cpu_arch": "any"}
    task.config = {
        "schema_version": "1",
        "task": {"id": task.id, "name": "Frozen Dockerfile task"},
        "environment": {
            "os": "linux", "cpu_arch": "any", "gpu_vendor": "none",
            "dockerfile": "environment/Dockerfile",
            "cpus": 1, "memory_mb": 1024, "storage_mb": 2048,
            "baseline_network_policy": {"kind": "gateway-only"},
            "network_policies_supported": ["gateway-only"],
        },
        "agent": {"name": "terminus-2"},
        "verifier": {"name": "script", "args": {"script_path": "verifier/check.sh"}},
        "steps": [{"name": "main", "instruction_file": "instruction.md"}],
    }
    if independent_verifier:
        task.config["verifier"]["environment"] = {
            **task.config["environment"], "dockerfile": "tests/Dockerfile",
        }
    if fixture:
        task.config["environment"].update({
            "docker_build_context": "environment",
            "sidecars": [{
                "name": "server", "fixture": True, "hostname": "fixture.example",
                "dockerfile": "fixtures/server/Dockerfile", "docker_build_context": "fixtures/server",
                "command": ["python3", "/server.py"], "ports": [23],
                "cpus": 0.1, "memory_mb": 128, "storage_mb": 128,
                "healthcheck": {"command": "true", "start_period_sec": 5,
                                "interval_sec": 2, "timeout_sec": 5, "retries": 15},
            }],
        })
    task.source = "s3://artifacts/frozen-task/"
    task.source_provenance = {"service_execution_input": {
        "schema_version": "loom.service-execution-input.v1",
        "manifest_uri": "s3://artifacts/frozen-task.json",
        "manifest_sha256": "sha256:" + "d" * 64,
        "file_count": 3, "total_bytes": 4096,
    }}
    rows = await ensure_task_image_materializations(session, task_row=task)
    assert {row.cpu_arch for row in rows} == {"x86_64"}
    # Historical ARM records may remain linked to frozen Trials. Insert one
    # explicitly so readiness/tamper checks still prove it cannot authorize an
    # x86 execution; new preparation must never enqueue another ARM build.
    legacy_arm = TaskImageMaterialization(
        id=uuid4(),
        materialization_key=task_image_materialization_key(
            task_id=task.id, task_checksum=task.checksum, cpu_arch="arm64",
        ),
        task_id=task.id,
        task_checksum=task.checksum,
        cpu_arch="arm64",
        task_config=deepcopy(task.config),
        task_source=task.source,
        task_source_provenance=deepcopy(task.source_provenance),
        state="failed",
    )
    session.add(legacy_arm)
    session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=legacy_arm.id))
    for row in rows:
        row.state = state
        session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=row.id))
    await session.execute(delete(ExecutionBudgetPolicy).where(
        ExecutionBudgetPolicy.scope_key.in_((target.logical_pool_id, target.target_id)),
    ))
    await session.commit()
    return trial_id, {**{row.cpu_arch: row.id for row in rows}, "arm64": legacy_arm.id}


async def _assert_no_execution(session: AsyncSession, trial_id: UUID) -> None:
    trial = await session.get(Trial, trial_id)
    assert trial is not None and trial.attempt_count == 0
    quota = await session.get(TeamQuota, trial.team_id)
    assert quota is not None and quota.in_flight_count == 0
    for model in (ServiceExecutionLease, ExecutionCostReservation, LlmCall):
        assert await session.scalar(select(func.count()).select_from(model).where(
            model.trial_id == trial_id,
        )) == 0


async def _clean_image_links(session: AsyncSession, trial_id: UUID | None) -> None:
    await session.rollback()
    if trial_id is not None:
        ids = list(await session.scalars(select(TrialTaskImageMaterialization.materialization_id)
                                        .where(TrialTaskImageMaterialization.trial_id == trial_id)))
        await session.execute(delete(TrialTaskImageMaterialization).where(
            TrialTaskImageMaterialization.trial_id == trial_id,
        ))
        await session.execute(delete(TaskImageMaterialization).where(
            TaskImageMaterialization.id.in_(ids),
        ))
        await session.commit()


@pytest.mark.parametrize("pending_state", ["queued", "claimed", "running"])
@pytest.mark.parametrize("task_rebuilt", [False, True], ids=["unchanged", "rebuilt"])
async def test_scheduler_waits_for_x86_image_then_reserves_frozen_revision(
    postgres_url: str, pending_state: str, task_rebuilt: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(session, now=now, state=pending_state)
            assert await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING, now=now,
            ) is None
            await session.commit()
            await _assert_no_execution(session, trial_id)
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "queued"
            # Unready images never consume the candidate wave or write a
            # polling backoff; becoming ready makes this Trial eligible now.
            assert trial.next_attempt_at is None
            x86 = await session.get(TaskImageMaterialization, ids["x86_64"])
            arm = await session.get(TaskImageMaterialization, ids["arm64"])
            assert x86 is not None and arm is not None
            x86.state = "ready"
            x86.registry_images = {"task": TASK_IMAGE}
            arm.state = "failed"
            if task_rebuilt:
                task = await session.get(Task, trial.task_id)
                assert task is not None
                changed_config = deepcopy(task.config)
                changed_config["environment"]["cpus"] = 1000
                task.config = changed_config
                task.checksum = "8" * 64
                task.source_provenance = {}
            await session.commit()
            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                now=now,
            )
            await session.commit()
            assert lease is not None
            plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            assert plan.task_image_materialization_id == ids["x86_64"]
            assert plan.task_image_ref == TASK_IMAGE
            assert plan.agent_image_ref == CONTROLLER_IMAGE
            assert {sandbox.image_ref for sandbox in plan.sidecars if sandbox.private_sandbox} == {TASK_IMAGE}
            assert plan.task_revision_sha256 == "sha256:" + "2" * 64
            assert plan.task_input is not None and plan.task_input.manifest_sha256 == "sha256:" + "d" * 64
            await session.refresh(trial)
            assert trial.attempt_count == 1
            assert await session.scalar(select(func.count()).select_from(LlmCall).where(
                LlmCall.trial_id == trial_id,
            )) == 0
    finally:
        async with sessions() as session:
            await _clean_image_links(session, trial_id)
        await engine.dispose()


@pytest.mark.parametrize("failure_reason", ["dockerfile_run_failed", "image_preparation_base_unsupported", "build_bound_node_unavailable"])
async def test_failed_x86_preparation_finishes_trial_without_an_attempt(postgres_url: str, failure_reason: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(session, now=now, state="queued")
            x86 = await session.get(TaskImageMaterialization, ids["x86_64"])
            arm = await session.get(TaskImageMaterialization, ids["arm64"])
            assert x86 is not None and arm is not None
            x86.state = "failed"
            x86.failure_reason = failure_reason
            arm.state = "ready"
            arm.registry_images = {"task": TASK_IMAGE}
            await session.commit()
            assert await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING, now=now,
            ) is None
            await session.commit()
            await _assert_no_execution(session, trial_id)
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "failed"
            assert trial.failure_reason == "task_image_build_failed"
            if failure_reason in {"image_preparation_base_unsupported", "build_bound_node_unavailable"}:
                from loom_service.failure_taxonomy import classify_trial_outcome
                assert trial.result["task_image_preparation_failure"]["reason"] == failure_reason
                classification = classify_trial_outcome(trial)
                assert classification["attribution"] == "platform"
                assert classification["requires_task_change"] is False
                assert classification["score_outcome"] == "unscored"
                if failure_reason == "build_bound_node_unavailable":
                    assert classification["rerun_recommendation"] == "operator_approval"
            assert trial.finished_at is not None
    finally:
        async with sessions() as session:
            await _clean_image_links(session, trial_id)
        await engine.dispose()


@pytest.mark.parametrize("tamper", ["materialization_id", "image", "revision", "cross_trial"])
async def test_direct_reservation_rejects_tampered_prepared_image_without_cost(
    postgres_url: str, tamper: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(session, now=now, state="queued")
            row = await session.get(TaskImageMaterialization, ids["x86_64"])
            assert row is not None
            row.state = "ready"
            row.registry_images = {"task": TASK_IMAGE}
            await session.commit()
            grant = await get_trial_task_image_execution_grant(
                session, trial_id=trial_id, cpu_arches=["x86_64"],
            )
            assert grant is not None
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            batch = await session.get(Batch, trial.batch_id)
            assert batch is not None
            task = TaskConfig.model_validate(grant.task_config)
            plan = compile_service_execution_plan(
                task=task,
                trial=TrialConfig.model_validate(trial.config),
                task_revision_sha256="sha256:" + grant.task_checksum,
                source_provenance=grant.task_source_provenance,
                profile=ServiceExecutionRuntimeProfileV1.model_validate(
                    batch.service_execution_runtime_profile,
                ),
                task_image_grant=grant,
            )
            target_id = await session.scalar(select(ServiceExecutionTarget.id))
            assert target_id is not None
            reserved_trial_id = trial_id
            if tamper == "materialization_id":
                plan = plan.model_copy(update={"task_image_materialization_id": ids["arm64"]})
            elif tamper == "image":
                changed_image = "registry.example/unrelated@sha256:" + "a" * 64
                plan = plan.model_copy(update={
                    "task_image_ref": changed_image,
                    "sidecars": tuple(sidecar.model_copy(update={"image_ref": changed_image})
                                      for sidecar in plan.sidecars),
                })
            elif tamper == "revision":
                plan = plan.model_copy(update={"task_revision_sha256": "sha256:" + "e" * 64})
            else:
                reserved_trial_id, _ = await fixtures._seed_ready_trial(session, now=now)
                await fixtures._configure_scheduler_trial(
                    session, trial_id=reserved_trial_id, now=now,
                )
            # Validate the caller's contract shape; rejection must come from the
            # persisted Trial association, not an invalid Pydantic fixture.
            plan = ExecutionRuntimePlanV1.model_validate(plan.model_dump(mode="json"))
            await session.commit()
            with pytest.raises(ServiceExecutionConflict, match="prepared image"):
                await reserve_trial_execution(
                    session, request_id=uuid4(), trial_id=reserved_trial_id,
                    execution_class_id=plan.execution_class_id, target_id=target_id,
                    requirements=workload_requirements_from_task(resolve_prepared_task(task, grant)),
                    runtime_contract=plan,
                    image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                    deadline_at=now + timedelta(seconds=3600), now=now,
                )
            # Do not roll back the rejection to hide accidental pre-check writes.
            await session.commit()
            await _assert_no_execution(session, trial_id)
            await _assert_no_execution(session, reserved_trial_id)
    finally:
        async with sessions() as session:
            await _clean_image_links(session, trial_id)
        await engine.dispose()


@pytest.mark.parametrize("tamper", [
    None, "image", "component", "hostname", "command", "resources", "probe",
    "missing", "prebuilt", "extra_image", "missing_image", "swapped_images",
])
async def test_fixture_reservation_requires_exact_frozen_components(
    postgres_url: str, tamper: str | None,
) -> None:
    """Plan shape alone must never authorize an arbitrary uploaded service image."""
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(session, now=now, state="queued", fixture=True)
            row = await session.get(TaskImageMaterialization, ids["x86_64"])
            assert row is not None
            row.state = "ready"
            row.registry_images = {"task": TASK_IMAGE, "sidecar:server":
                                   "registry.example/fixture@sha256:" + "a" * 64}
            await session.commit()
            grant = await get_trial_task_image_execution_grant(
                session, trial_id=trial_id, cpu_arches=["x86_64"],
            )
            assert grant is not None
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            batch = await session.get(Batch, trial.batch_id)
            assert batch is not None
            task = TaskConfig.model_validate(grant.task_config)
            plan = compile_service_execution_plan(
                task=task, trial=TrialConfig.model_validate(trial.config),
                task_revision_sha256="sha256:" + grant.task_checksum,
                source_provenance=grant.task_source_provenance,
                profile=ServiceExecutionRuntimeProfileV1.model_validate(batch.service_execution_runtime_profile),
                task_image_grant=grant,
            )
            payload = plan.canonical_payload()
            fixture = payload["sidecars"][0]
            if tamper == "image":
                fixture["image_ref"] = "registry.example/forged@sha256:" + "b" * 64
            elif tamper == "component":
                fixture.update(role_name="fixture-other", task_image_component="sidecar:other")
            elif tamper == "hostname":
                fixture["hostname"] = "other.example"
            elif tamper == "command":
                fixture["argv"] = ["/bin/false"]
            elif tamper == "resources":
                fixture["resources"]["cpu_millis"] = 1
            elif tamper == "probe":
                fixture["startup_probe"]["argv"] = ["/bin/true"]
            elif tamper == "missing":
                payload["sidecars"] = payload["sidecars"][1:]
            elif tamper == "prebuilt":
                raw = deepcopy(row.task_config)
                # Attaching the main image UUID must not authorize a prebuilt fixture.
                raw["environment"]["sidecars"][0].update(
                    dockerfile=None, docker_build_context=None, docker_image=fixture["image_ref"],
                )
                row.task_config = raw
                row.registry_images = {"task": TASK_IMAGE}
            elif tamper == "extra_image":
                row.registry_images = {**row.registry_images, "sidecar:other": fixture["image_ref"]}
            elif tamper == "missing_image":
                row.registry_images = {"task": TASK_IMAGE}
            elif tamper == "swapped_images":
                fixture["image_ref"] = TASK_IMAGE
            plan = ExecutionRuntimePlanV1.model_validate(payload)
            target_id = await session.scalar(select(ServiceExecutionTarget.id))
            assert target_id is not None
            await session.commit()
            args = dict(
                request_id=uuid4(), trial_id=trial_id, execution_class_id=plan.execution_class_id,
                target_id=target_id, requirements=workload_requirements_from_task(resolve_prepared_task(task, grant)),
                runtime_contract=plan, image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                deadline_at=now + timedelta(seconds=3600), now=now,
            )
            if tamper is not None:
                with pytest.raises(ServiceExecutionConflict, match=r"prepared fixture|not ready"):
                    await reserve_trial_execution(session, **args)
                await session.commit()
                await _assert_no_execution(session, trial_id)
            else:
                lease = await reserve_trial_execution(session, **args)
                await session.commit()
                assert lease.workload_requirements_json["sidecar_count"] == 1
                assert lease.runtime_contract_json["sidecars"][0]["image_ref"] == fixture["image_ref"]
                without_fixture = plan.model_copy(update={"sidecars": plan.sidecars[1:]})
                assert runtime_pod_resources(plan).cpu_millis == runtime_pod_resources(without_fixture).cpu_millis + 100
                await session.refresh(trial)
                assert trial.attempt_count == 1
    finally:
        async with sessions() as session:
            await _clean_image_links(session, trial_id)
        await engine.dispose()


@pytest.mark.parametrize("tamper", [
    None, "image", "source", "environment", "gpu", "target", "class", "candidate",
    "parent_requirements", "parent_runtime", "parent_route",
])
async def test_independent_verifier_reservation_preserves_agent_route(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch, tamper: str | None,
) -> None:
    """A distinct grading image must not be compared with the agent route's requirements."""
    from loom_control_plane import service_execution_scheduler as scheduler

    async def committed_files(*_args, **_kwargs):
        # Only object-store receipt material is supplied; compiler, immutable image
        # grant, scheduler selection, route and reservation all use the real DB.
        return [("artifacts/workspace.tar", 1, "sha256:" + "5" * 64)], ["owned/workspace.tar"]

    monkeypatch.setattr(scheduler, "committed_handoff_files", committed_files)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    grader_image = "registry.example/grader@sha256:" + "e" * 64
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(
                session, now=now, state="queued", independent_verifier=True,
            )
            row = await session.get(TaskImageMaterialization, ids["x86_64"])
            assert row is not None
            row.state = "ready"
            row.registry_images = {"task": TASK_IMAGE, "verifier": grader_image}
            await session.commit()
            parent = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING, now=now,
            )
            assert parent is not None
            await session.commit()
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            original_route = deepcopy(trial.execution_route_json)
            original_route_digest = trial.execution_route_sha256
            from loom.execution_contract import ExecutionRoutingReason
            from loom_control_plane.service_execution import _bind_kubernetes_execution_route
            agent_target = await session.get(ServiceExecutionTarget, parent.target_id)
            assert agent_target is not None
            with pytest.raises(ServiceExecutionConflict, match="route requirements drift"):
                _bind_kubernetes_execution_route(
                    trial=trial, target=agent_target, execution_class_id=parent.execution_class_id,
                    requirements_digest="sha256:" + "0" * 64,
                    routing_reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT, current_time=now,
                )
            await fixtures._hand_off_to_verifier(session, parent_id=parent.id, now=now)
            parent.cleanup_state = "complete"
            parent.cleanup_requested_at = now
            parent.cleanup_deadline_at = now + timedelta(seconds=30)
            parent.desired_state = "deleted"
            parent.observed_state = "deleted"
            parent.deleted_at = now
            await session.commit()
            if tamper is not None:
                from loom.verifier_runtime import verifier_execution_task

                child_plan, deadline = await scheduler.compile_verifier_candidate(
                    session, parent=parent, trial=trial, maximum_deadline_seconds=32400,
                    current_time=now + timedelta(seconds=5),
                )
                grant = await get_trial_task_image_execution_grant(
                    session, trial_id=trial_id, cpu_arches=["x86_64"],
                )
                assert grant is not None
                task = resolve_prepared_task(TaskConfig.model_validate(grant.task_config), grant)
                requirements = workload_requirements_from_task(
                    verifier_execution_task(task), TrialConfig.model_validate(trial.config),
                )
                target_id = parent.target_id
                expected = ""
                if tamper == "image":
                    changed_image = "registry.example/other@sha256:" + "7" * 64
                    child_plan = child_plan.model_copy(update={
                        "task_image_ref": changed_image,
                        "sidecars": tuple(s.model_copy(update={"image_ref": changed_image})
                                          if s.private_sandbox else s for s in child_plan.sidecars),
                    })
                    requirements = requirements.model_copy(update={"image_ref": changed_image})
                    expected = "prepared image"
                elif tamper == "source":
                    assert child_plan.task_input is not None
                    child_plan = child_plan.model_copy(update={
                        "task_input": child_plan.task_input.model_copy(update={"manifest_sha256": "sha256:" + "6" * 64}),
                    })
                    expected = "parent frozen identity drift"
                elif tamper == "environment":
                    import json

                    from loom.task_environment_handoff import RESOLVED_VERIFIER_ENVIRONMENT
                    env = dict(child_plan.main.environment)
                    grader = json.loads(env[RESOLVED_VERIFIER_ENVIRONMENT])
                    grader["workdir"] = "/different"
                    env[RESOLVED_VERIFIER_ENVIRONMENT] = json.dumps(grader)
                    child_plan = child_plan.model_copy(update={"main": child_plan.main.model_copy(update={"environment": env})})
                    expected = "verifier environment"
                elif tamper == "gpu":
                    requirements = requirements.model_copy(update={"gpu_vendor": "nvidia", "gpu_count": 1})
                    expected = "requirements do not match"
                elif tamper == "target":
                    _, foreign_target = await fixtures._seed_ready_trial(session, now=now)
                    target_row = await session.get(ServiceExecutionTarget, foreign_target.target_id)
                    assert target_row is not None
                    target_row.region = "different-region"
                    target_id = target_row.id
                    expected = "parent execution authority"
                elif tamper == "class":
                    from loom.execution_contract import NEBIUS_CPU_WEB_EXECUTION_CLASS_V1
                    _, wrong_class_target = await fixtures._seed_ready_trial(
                        session, now=now, execution_class=NEBIUS_CPU_WEB_EXECUTION_CLASS_V1,
                    )
                    child_plan = child_plan.model_copy(update={
                        "execution_class_id": NEBIUS_CPU_WEB_EXECUTION_CLASS_V1.class_id,
                    })
                    target_id = wrong_class_target.target_id
                    expected = "parent execution authority"
                elif tamper == "candidate":
                    child_plan = child_plan.model_copy(update={"candidate_sha": "8" * 40})
                    expected = "parent lease is not eligible"
                else:
                    from sqlalchemy.orm.attributes import set_committed_value
                    # The DB already forbids changing these persisted identities.
                    # Inject a corrupt loaded snapshot without UPDATE to verify
                    # the reservation boundary also rejects it before admission.
                    if tamper == "parent_requirements":
                        set_committed_value(parent, "workload_requirements_json", {
                            **parent.workload_requirements_json, "memory_mib": 8192,
                        })
                        expected = "parent frozen identity drift"
                    elif tamper == "parent_runtime":
                        altered = deepcopy(parent.runtime_contract_json)
                        altered["main"]["timeout_seconds"] += 1
                        set_committed_value(parent, "runtime_contract_json", altered)
                        expected = "parent frozen identity drift"
                    else:
                        set_committed_value(parent, "routing_decision_sha256", "sha256:" + "9" * 64)
                        expected = "parent lease binds a different execution route"
                child_plan = ExecutionRuntimePlanV1.model_validate(child_plan.canonical_payload())
                with pytest.raises(ServiceExecutionConflict, match=expected):
                    await reserve_trial_execution(
                        session, request_id=uuid4(), trial_id=trial_id,
                        execution_class_id=child_plan.execution_class_id, target_id=target_id,
                        requirements=requirements, runtime_contract=child_plan,
                        image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                        parent_lease_id=parent.id, deadline_at=deadline, now=now + timedelta(seconds=5),
                    )
                assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease).where(
                    ServiceExecutionLease.parent_lease_id == parent.id,
                )) == 0
                assert trial.execution_route_json == original_route
                assert trial.execution_route_sha256 == original_route_digest
                return
            reserved = await scheduler.reserve_next_verifier_executions(
                session, pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                now=now + timedelta(seconds=5),
            )
            assert len(reserved) == 1
            child = reserved[0]
            assert child.parent_lease_id == parent.id
            assert child.workload_requirements_json["image_ref"] == grader_image
            assert child.workload_requirements_sha256 != parent.workload_requirements_sha256
            assert {k: v for k, v in child.workload_requirements_json.items() if k != "image_ref"} == {
                k: v for k, v in parent.workload_requirements_json.items() if k != "image_ref"
            }
            assert child.routing_decision_sha256 == original_route_digest
            assert child.routing_generation == parent.routing_generation
            assert child.selected_pool_id == parent.selected_pool_id
            assert child.team_id == parent.team_id
            assert child.attempt == parent.attempt
            assert trial.execution_route_json == original_route
            assert trial.execution_route_sha256 == original_route_digest
            await session.commit()
            assert await scheduler.reserve_next_verifier_executions(
                session, pool_id="nebius-cpu",
                image_admission_keyring=fixtures.IMAGE_ADMISSION_KEYRING,
                now=now + timedelta(seconds=6),
            ) == []
    finally:
        async with sessions() as session:
            await _clean_image_links(session, trial_id)
        await engine.dispose()
