from __future__ import annotations

import asyncio
import io
import json
import tarfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import SecretStr
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from loom.auth import AuthContext, verify_step_jwt
from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    ArtifactUploadSession,
    Batch,
    DataLifecycleAuthority,
    DataLifecycleObject,
    ExecutionAdmissionPolicy,
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionCostReservationDebit,
    ExecutionNodeCostAllocation,
    ExecutionNodeCostRecord,
    ExecutionPriceSnapshot,
    ExecutionProvisioningAuthorization,
    ExecutionTargetPriceBinding,
    ServiceExecutionClass,
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionLeaseHistory,
    ServiceExecutionTarget,
    Task,
    Team,
    TeamQuota,
    Trial,
    TrialEvent,
    TrialResourceUsage,
    Worker,
)
from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    NEBIUS_CPU_WEB_EXECUTION_CLASS_V1,
    CapacityEvidenceKind,
    ExecutionAdapterKind,
    ExecutionClassV1,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionRoutingReason,
    ExecutionTargetV1,
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ExecutionRuntimeResultV1,
    ProbeV1,
    ProcessPhaseV1,
    RuntimeHandoffInputV1,
    RuntimeOutputDeclarationV1,
    RuntimeTaskInputV1,
    SidecarContainerV1,
)
from loom.models.networking import NetworkPolicy, resolve_effective_network_policy
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.service_execution_materialization import (
    ServiceExecutionInputFileV1,
    ServiceExecutionInputManifestV1,
    ServiceExecutionRuntimeProfileV1,
)
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.execution_capacity import (
    create_execution_capacity_observation,
    fetch_execution_capacity_status,
    reserve_execution_provisioning,
    upsert_execution_capacity_policy,
)
from loom_control_plane.execution_finance import (
    create_execution_price_snapshot,
    record_execution_node_cost,
    settle_execution_cost_reservation,
    upsert_execution_budget_policy,
    upsert_target_price_binding,
)
from loom_control_plane.scheduler.crash_detector import reclaim_expired_workers
from loom_control_plane.service_execution import (
    MAX_VERIFIER_RETRIES,
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    acknowledge_execution_command,
    claim_execution_commands,
    enqueue_execution_transition,
    execution_lease_projection,
    finalize_committed_service_execution,
    persist_execution_catalog,
    record_committed_runtime_result,
    record_execution_event,
    recover_execution_node_attribution,
    refresh_execution_target_health,
    request_trial_execution_cancellation,
    reserve_trial_execution,
    set_execution_target_health,
    verify_trial_execution_fence,
)
from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
    ServiceExecutionPeerV1,
    authorize_service_execution_peer,
    mint_service_execution_peer_token,
    resolve_service_execution_input,
)
from loom_control_plane.service_execution_scheduler import (
    reserve_next_service_execution,
    reserve_next_verifier_executions,
)
from loom_control_plane.trial_cancellation import cancel_trial_under_authority
from loom_execution_actuator.contracts import (
    ExecutionTerminationSummaryV1,
    KubernetesApiError,
    KubernetesJobInventory,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_execution_dispatch
from tests.execution_placement_fixtures import placement_fixture
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)


@pytest.fixture(autouse=True)
async def _cleanup_service_execution_test_rows(postgres_url: str):  # type: ignore[no-untyped-def]
    """Keep the session Postgres fixture compatible with legacy broad cleanup fixtures."""

    yield
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            owned_team_ids = select(Team.id).where(Team.name.like("service-execution-%"))
            owned_authority_ids = select(DataLifecycleAuthority.id).where(
                DataLifecycleAuthority.team_id.in_(owned_team_ids)
            )
            await session.execute(
                delete(DataLifecycleObject).where(
                    DataLifecycleObject.authority_id.in_(owned_authority_ids)
                )
            )
            await session.execute(
                delete(Artifact).where(Artifact.control_producer_kind == "service_execution")
            )
            await session.execute(
                delete(AdminAuditEvent).where(
                    AdminAuditEvent.action.in_(
                        (
                            "service_execution.step_token.minted",
                            "execution.node_attribution.recovered",
                        )
                    )
                )
            )
            owned_trials = select(Trial.id).join(Team).where(Team.name.like("service-execution-%"))
            owned_cost_reservations = select(ExecutionCostReservation.id).where(
                ExecutionCostReservation.trial_id.in_(owned_trials)
            )
            await session.execute(
                delete(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id.in_(
                        select(ServiceExecutionLease.id).where(
                            ServiceExecutionLease.trial_id.in_(owned_trials)
                        )
                    )
                )
            )
            await session.execute(
                delete(ExecutionNodeCostAllocation).where(
                    ExecutionNodeCostAllocation.cost_reservation_id.in_(owned_cost_reservations)
                )
            )
            await session.execute(
                delete(ExecutionNodeCostRecord).where(
                    ExecutionNodeCostRecord.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionCostReservationDebit).where(
                    ExecutionCostReservationDebit.reservation_id.in_(owned_cost_reservations)
                )
            )
            await session.execute(
                delete(ExecutionCostReservation).where(
                    ExecutionCostReservation.id.in_(owned_cost_reservations)
                )
            )
            await session.execute(delete(TrialResourceUsage).where(TrialResourceUsage.trial_id.in_(owned_trials)))
            await session.execute(
                delete(ServiceExecutionLease).where(
                    ServiceExecutionLease.trial_id.in_(owned_trials)
                )
            )
            await session.execute(delete(Trial).where(Trial.id.in_(owned_trials)))
            await session.execute(
                delete(Batch).where(
                    Batch.team_id.in_(select(Team.id).where(Team.name.like("service-execution-%")))
                )
            )
            await session.execute(delete(Task).where(Task.id.like("service-execution/%")))
            await session.execute(
                delete(ExecutionTargetPriceBinding).where(
                    ExecutionTargetPriceBinding.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionBudgetPolicy).where(
                    (ExecutionBudgetPolicy.scope_key == "nebius-cpu")
                    | (ExecutionBudgetPolicy.scope_key.like("nebius-staging-%"))
                )
            )
            await session.execute(
                delete(ExecutionCapacityObservation).where(
                    ExecutionCapacityObservation.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ExecutionCapacityPolicy).where(
                    ExecutionCapacityPolicy.target_id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id.like("nebius-staging-%")
                )
            )
            await session.execute(
                delete(ServiceExecutionClass).where(
                    ServiceExecutionClass.id.in_((
                        NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                        NEBIUS_CPU_WEB_EXECUTION_CLASS_V1.class_id,
                    ))
                )
            )
            await session.execute(
                delete(ExecutionPriceSnapshot).where(
                    ExecutionPriceSnapshot.source == "service-execution-test"
                )
            )
            await session.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id.in_(
                        select(Team.id).where(Team.name.like("service-execution-%"))
                    )
                )
            )
            await session.execute(
                delete(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.team_id.in_(owned_team_ids)
                )
            )
            await session.execute(delete(Team).where(Team.name.like("service-execution-%")))
    finally:
        await engine.dispose()


class _FakeKubernetesJobApi:
    def __init__(self, *, ambiguous_create: bool = False) -> None:
        self.jobs: dict[str, KubernetesJobObservation] = {}
        self.watch_events: list[KubernetesJobObservation] = []
        self.create_count = 0
        self.delete_count = 0
        self.ambiguous_create = ambiguous_create

    async def get_job(self, *, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        observation = self.jobs.get(job_name)
        assert observation is None or observation.namespace == namespace
        return observation

    async def create_job(
        self, *, namespace: str, manifest: dict[str, object]
    ) -> KubernetesJobObservation:
        metadata = manifest["metadata"]
        assert isinstance(metadata, dict)
        labels = metadata["labels"]
        annotations = metadata["annotations"]
        assert isinstance(labels, dict) and isinstance(annotations, dict)
        job_name = str(metadata["name"])
        self.create_count += 1
        observation = KubernetesJobObservation(
            namespace=namespace,
            job_name=job_name,
            lease_id=str(labels["loom.openai.com/lease-id"]),
            resource_generation=int(str(labels["loom.openai.com/generation"])),
            target_id=str(annotations["loom.openai.com/target-id"]),
            execution_unit_key=str(annotations["loom.openai.com/execution-unit-key"]),
            normalized_state=NormalizedJobState.PENDING,
            job_uid=f"job-uid-{job_name}",
            resource_version="1",
        )
        self.jobs[job_name] = observation
        if self.ambiguous_create:
            self.ambiguous_create = False
            raise KubernetesApiError("response lost", ambiguous=True)
        return observation

    async def delete_job(
        self,
        *,
        namespace: str,
        job_name: str,
        expected_uid: str,
        grace_period_seconds: int,
    ) -> None:
        assert grace_period_seconds >= 0
        current = self.jobs.get(job_name)
        if current is None:
            return
        assert current.namespace == namespace
        assert current.job_uid == expected_uid
        self.delete_count += 1
        del self.jobs[job_name]

    async def list_jobs(self, *, namespace: str, label_selector: str) -> KubernetesJobInventory:
        assert label_selector == "app.kubernetes.io/managed-by=loom-execution-actuator"
        return KubernetesJobInventory(
            tuple(item for item in self.jobs.values() if item.namespace == namespace)
        )

    async def watch_jobs(
        self,
        *,
        namespace: str,
        label_selector: str,
        resource_version: str | None,
        timeout_seconds: int,
    ) -> tuple[KubernetesJobObservation, ...]:
        del label_selector, resource_version, timeout_seconds
        events = tuple(item for item in self.watch_events if item.namespace == namespace)
        self.watch_events.clear()
        return events


def _target(suffix: str) -> ExecutionTargetV1:
    return ExecutionTargetV1(
        target_id=f"nebius-staging-{suffix}",
        logical_pool_id="nebius-cpu",
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        cluster_scope_id="nebius-eu-north1-shared",
        environment="staging",
        provider="nebius",
        region="eu-north1",
        failure_domain=f"eu-north1-{suffix}",
        data_residency="eu",
        namespace_name=f"loom-nebius-{suffix}",
        health_role="primary",
        health_check_id=f"nebius-staging-health-{suffix}",
        health_check_interval_seconds=10,
        health_stale_after_seconds=60,
    )


def _requirements(
    *,
    verifier_topology: VerifierTopology = VerifierTopology.IN_ATTEMPT,
    data_residency: str | None = None,
) -> WorkloadRequirementsV1:
    return WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        data_residency=data_residency,
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
        isolation_level=IsolationLevel.SHARED_KERNEL,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref="registry.example/loom/task@sha256:" + "a" * 64,
        sidecar_count=0,
        verifier_topology=verifier_topology,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )


def _runtime_contract(
    *,
    execution_class_id: str = NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
    execution_role: str = "attempt",
    verifier_execution: str = "in_attempt",
    now: datetime | None = None,
) -> ExecutionRuntimePlanV1:
    resources = ContainerResourcesV1(
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
    )
    task_image_ref = "registry.example/loom/task@sha256:" + "a" * 64
    runtime_image_ref = "registry.example/loom/runtime@sha256:" + "b" * 64
    return ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_role=execution_role,
        execution_class_id=execution_class_id,
        composition="init_payload",
        task_image_ref=task_image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256="sha256:" + "c" * 64,
        image_admission=signed_image_admission_bundle((task_image_ref, runtime_image_ref), now=now),
        task_resources=resources,
        workspace_mib=1024,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent" if execution_role == "attempt" else "verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution=verifier_execution,
        verifier=(
            ProcessPhaseV1(
                role="verifier",
                argv=("/bin/true",),
                working_directory="/workspace",
                timeout_seconds=60,
            )
            if verifier_execution == "in_attempt"
            else None
        ),
    )


def _deferred_verifier_contract(**updates: Any) -> ExecutionRuntimePlanV1:
    return ExecutionRuntimePlanV1.model_validate({
        **_runtime_contract(execution_role="verifier", verifier_execution="skipped").canonical_payload(),
        "task_input": {"manifest_sha256": "sha256:" + "4" * 64, "file_count": 1, "total_bytes": 1},
        "handoff_input": {"manifest_sha256": "sha256:" + "5" * 64, "file_count": 1, "total_bytes": 1},
        **updates,
    })


async def _hand_off_to_verifier(session: AsyncSession, *, parent_id: UUID, now: datetime) -> None:
    """The durable state a committed, finalized separate-mode attempt leaves behind."""
    parent = await session.get(ServiceExecutionLease, parent_id)
    assert parent is not None
    trial = await session.get(Trial, parent.trial_id)
    assert trial is not None
    plan = ExecutionRuntimePlanV1.model_validate(parent.runtime_contract_json)
    upload = ArtifactUploadSession(
        id=uuid4(),
        team_id=trial.team_id,
        commit_kind="service_execution_output",
        service_execution_lease_id=parent.id,
        service_execution_generation=parent.resource_generation,
        service_execution_role="attempt",
        service_execution_runtime_contract_sha256=parent.runtime_contract_sha256,
        service_execution_candidate_sha=plan.candidate_sha,
        service_execution_task_revision_sha256=plan.task_revision_sha256,
        service_execution_command_identity_sha256=plan.command_identity_sha256,
        idempotency_key=uuid4().hex,
        request_digest="sha256:" + "6" * 64,
        prefix=f"service-execution/{parent.id}/{uuid4().hex}/",
        state="committed",
        expected_total_max_bytes=1024,
        canonical_manifest_json={},
        manifest_sha256="sha256:" + "7" * 64,
        committed_marker_sha256="sha256:" + "8" * 64,
        expires_at=now + timedelta(hours=1),
        committed_at=now,
    )
    session.add(upload)
    await session.flush()
    parent.output_commit_state = "committed"
    parent.output_generation = parent.resource_generation
    parent.output_upload_session_id = upload.id
    parent.output_manifest_sha256 = upload.manifest_sha256
    parent.output_marker_sha256 = upload.committed_marker_sha256
    parent.output_committed_at = now
    parent.observed_state = "finalized"
    parent.finalized_at = now
    trial.state = "running"
    trial.result = {
        "schema_version": "loom.service-execution-trial-result.v1",
        "reward": None,
        "aggregate_reward": None,
        "verifier_execution": {"state": "pending", "parent_lease_id": str(parent.id)},
    }


def _complete_output_contract(*, now: datetime) -> ExecutionRuntimePlanV1:
    return _runtime_contract(now=now).model_copy(
        update={
            "output_declarations": (
                RuntimeOutputDeclarationV1(
                    source_path="answer.txt",
                    relative_path="artifacts/answer.txt",
                    kind="task_artifact",
                    required=True,
                ),
                RuntimeOutputDeclarationV1(
                    source_path=".loom/agent/trajectory.jsonl",
                    relative_path="trajectory/events.jsonl",
                    kind="trajectory",
                    required=True,
                ),
                RuntimeOutputDeclarationV1(
                    source_path=".loom/agent/usage.json",
                    relative_path="accounting/usage.json",
                    kind="usage",
                    required=True,
                ),
                RuntimeOutputDeclarationV1(
                    source_path=".loom/verifier/output.json",
                    relative_path="verifier/output.json",
                    kind="verifier",
                    required=True,
                ),
            )
        }
    )


def _runtime_result_payload(
    lease: ServiceExecutionLease,
    *,
    started_at: datetime,
) -> dict[str, object]:
    assert lease.runtime_contract_json is not None
    assert lease.runtime_contract_sha256 is not None
    plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
    phases: list[dict[str, object]] = []
    for ordinal, role in enumerate(("agent", "verifier"), start=1):
        phases.append(
            {
                "role": role,
                "ordinal": ordinal,
                "started_at": started_at.isoformat(),
                "finished_at": (started_at + timedelta(seconds=ordinal)).isoformat(),
                "exit_code": 0,
                "signal": None,
                "timed_out": False,
                "stdout": {
                    "path": f"{ordinal:02d}-{role}.stdout",
                    "sha256": "sha256:" + "4" * 64,
                    "bytes_seen": 10,
                    "bytes_saved": 10,
                    "truncated": False,
                },
                "stderr": {
                    "path": f"{ordinal:02d}-{role}.stderr",
                    "sha256": "sha256:" + "5" * 64,
                    "bytes_seen": 0,
                    "bytes_saved": 0,
                    "truncated": False,
                },
            }
        )
    return {
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": lease.runtime_contract_sha256,
        "candidate_sha": plan.candidate_sha,
        "task_revision_sha256": plan.task_revision_sha256,
        "command_identity_sha256": plan.command_identity_sha256,
        "execution_role": plan.execution_role,
        "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": plan.task_image_ref,
        "runtime_image_ref": plan.runtime_image_ref,
        "runtime_binary_sha256": plan.runtime_binary_sha256,
        "execution_class_id": plan.execution_class_id,
        "status": "succeeded",
        "started_at": started_at.isoformat(),
        "finished_at": (started_at + timedelta(seconds=3)).isoformat(),
        "phases": phases,
        "partial_evidence": False,
    }


async def _seed_ready_trial(
    session: AsyncSession,
    *,
    now: datetime,
    task_id: str | None = None,
    execution_class: ExecutionClassV1 = NEBIUS_CPU_EXECUTION_CLASS_V1,
    pool_origin: dict[str, Any] | None = None,
) -> tuple[UUID, ExecutionTargetV1]:
    suffix = uuid4().hex[:12]
    team_id = uuid4()
    trial_id = uuid4()
    task_id = task_id or f"service-execution/{suffix}"
    target = _target(suffix).model_copy(
        update={"execution_class_id": execution_class.class_id},
    )
    session.add_all(
        (
            Team(id=team_id, name=f"service-execution-{suffix}"),
            Task(
                id=task_id,
                checksum="b" * 64,
                config={"schema_version": "1", "task": {"id": task_id}},
            ),
            Trial(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={"agent_name": "test", "agent_model": None},
                requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                state="queued",
                attempt_count=0,
                pool_origin=pool_origin,
            ),
        )
    )
    await persist_execution_catalog(
        session,
        execution_class=execution_class,
        targets=(target,),
    )
    await set_execution_target_health(
        session,
        target_id=target.target_id,
        desired_state="active",
        observed_state="ready",
        health_status="healthy",
        observed_at=now,
    )
    price, _ = await create_execution_price_snapshot(
        session,
        provider="nebius",
        region=target.region,
        sku="test-node",
        source="service-execution-test",
        source_version=target.target_id,
        source_uri="https://example.test/nebius-price",
        effective_at=now - timedelta(days=1),
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=0,
        memory_gib_microusd_per_hour=0,
        ephemeral_storage_gib_microusd_per_hour=0,
    )
    await upsert_target_price_binding(
        session,
        target_id=target.target_id,
        price_snapshot_id=price.id,
        enabled=True,
        reason="service execution test",
        now=now,
    )
    for scope_kind, scope_key in (
        ("pool", target.logical_pool_id),
        ("target", target.target_id),
    ):
        await upsert_execution_budget_policy(
            session,
            scope_kind=scope_kind,  # type: ignore[arg-type]
            scope_key=scope_key,
            daily_limit_microusd=100_000_000,
            monthly_limit_microusd=1_000_000_000,
            per_attempt_limit_microusd=10_000_000,
            max_estimate_duration_seconds=7_200,
            emergency_stop=False,
            enabled=True,
            reason="service execution test",
            now=now,
        )
    await upsert_execution_capacity_policy(
        session,
        target_id=target.target_id,
        enabled=True,
        max_nodes=20,
        max_vcpu_millis=1_280_000,
        max_memory_mib=5_242_880,
        max_storage_mib=20_971_520,
        node_cpu_millis=64_000,
        node_memory_mib=262_144,
        node_storage_mib=1_048_576,
        max_pending_jobs=100,
        max_unschedulable_jobs=10,
        max_image_pull_backoff_jobs=10,
        max_create_per_minute=100,
        observation_max_age_seconds=900,
        reason="service execution test",
        now=now,
    )
    await create_execution_capacity_observation(
        session,
        target_id=target.target_id,
        source="service-execution-test",
        source_version=target.target_id,
        observed_at=now,
        provider_capacity_state="available",
        provider_capacity_reason=None,
        autoscaler_state="ready",
        autoscaler_reason=None,
        provider_quota_nodes=20,
        provider_quota_vcpu_millis=1_280_000,
        provider_quota_memory_mib=5_242_880,
        provider_quota_storage_mib=20_971_520,
        provider_used_nodes=1,
        provider_used_vcpu_millis=64_000,
        provider_used_memory_mib=262_144,
        provider_used_storage_mib=1_048_576,
        active_nodes=1,
        provisioned_vcpu_millis=64_000,
        provisioned_memory_mib=262_144,
        provisioned_storage_mib=1_048_576,
        allocatable_cpu_millis=64_000,
        allocatable_memory_mib=262_144,
        allocatable_storage_mib=1_048_576,
        requested_cpu_millis=0,
        requested_memory_mib=0,
        requested_storage_mib=0,
        pending_jobs=0,
        unschedulable_jobs=0,
        image_pull_backoff_jobs=0,
        pending_reasons={},
        placement=placement_fixture(target_id=target.target_id),
    )
    return trial_id, target


async def _task_effective_network_policy(
    session: AsyncSession, trial_id: UUID,
) -> NetworkPolicy | None:
    # Production requirements always freeze the task's resolved policy; fixtures
    # whose stored config is not a full TaskConfig keep the legacy omission.
    trial = await session.get(Trial, trial_id)
    task = await session.get(Task, trial.task_id) if trial is not None else None
    if trial is None or task is None:
        return None
    try:
        task_config = TaskConfig.model_validate(task.config)
        trial_config = TrialConfig.model_validate(trial.config) if trial.config else None
    except ValueError:
        return None
    return resolve_effective_network_policy(
        baseline=task_config.environment.baseline_network_policy,
        supported=task_config.environment.network_policies_supported,
        override=trial_config.baseline_network_policy_override if trial_config else None,
    )


async def _reserve(
    session: AsyncSession,
    *,
    trial_id: UUID,
    target: ExecutionTargetV1,
    now: datetime,
    request_id: UUID | None = None,
    requirements: WorkloadRequirementsV1 | None = None,
    runtime_contract: ExecutionRuntimePlanV1 | None = None,
    parent_lease_id: UUID | None = None,
    deadline_seconds: int = 3600,
) -> ServiceExecutionLease:
    if requirements is None:
        requirements = _requirements().model_copy(update={
            "effective_network_policy": await _task_effective_network_policy(session, trial_id),
        })
    return await reserve_trial_execution(
        session,
        request_id=request_id or uuid4(),
        trial_id=trial_id,
        execution_class_id=target.execution_class_id,
        target_id=target.target_id,
        requirements=requirements,
        runtime_contract=runtime_contract or _runtime_contract(
            now=now, execution_class_id=target.execution_class_id,
        ),
        image_admission_keyring=IMAGE_ADMISSION_KEYRING,
        parent_lease_id=parent_lease_id,
        deadline_at=now + timedelta(seconds=deadline_seconds),
        now=now,
    )


@pytest.mark.parametrize("state", ["claimed", "running", "queued"])
async def test_worker_reclaim_preserves_native_attempt_with_stale_worker_pointer(
    postgres_url: str, state: str,
) -> None:
    """Legacy worker recovery must never requeue a native current attempt."""
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    worker_id = uuid4()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            session.add(Worker(
                id=worker_id, hostname="stale-native-pointer", version="test",
                capabilities=[], registered_at=now - timedelta(minutes=10),
                last_seen_at=now - timedelta(minutes=5), status="active",
            ))
            await session.flush()
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.state = state
            trial.started_at = now - timedelta(minutes=1)
            trial.worker_id = worker_id
            await session.commit()
            generation = lease.generation
        async with sessions() as session, session.begin():
            assert await reclaim_expired_workers(session, expiry_sec=15) == 0
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            lease = await session.get(ServiceExecutionLease, lease.id)
            assert trial is not None and lease is not None
            assert trial.state == state
            assert trial.worker_id == worker_id
            assert trial.failure_reason is None
            assert trial.failure_message is None
            assert trial.next_attempt_at is None
            assert lease.generation == generation
    finally:
        async with sessions() as session, session.begin():
            await session.execute(update(Trial).where(Trial.worker_id == worker_id).values(worker_id=None))
            await session.execute(delete(Worker).where(Worker.id == worker_id))
        await engine.dispose()


async def test_default_catalog_upgrade_preserves_existing_class_identity(postgres_url: str) -> None:
    """A deployment must not add capabilities under the already persisted V1 ID."""
    from loom.execution_contract import ExecutionClassV1

    legacy = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    legacy.pop("supports_task_web_egress", None)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            await persist_execution_catalog(
                session, execution_class=ExecutionClassV1.model_validate(legacy), targets=(),
            )
            stored = await session.get(ServiceExecutionClass, "linux-amd64-cpu-pod-v1")
            assert stored is not None
            original_digest = stored.spec_sha256
            await persist_execution_catalog(
                session, execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1, targets=(),
            )
            assert stored.spec_json == legacy
            assert stored.spec_sha256 == original_digest
            # Different capabilities still require a different immutable ID.
            with pytest.raises(ServiceExecutionConflict, match="different content"):
                await persist_execution_catalog(
                    session, execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1.model_copy(
                        update={"supports_task_web_egress": True}), targets=(),
                )
    finally:
        await engine.dispose()


async def test_deployment_retirement_payload_commits_observed_target_health(
    postgres_url: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployer's actual payload must satisfy the migrated health constraint."""
    import io
    import urllib.request

    from scripts.ops.deploy_nebius_platform import TARGET_RETIRE_PROGRAM

    from loom_control_plane.routes.service_executions import TargetHealthBody

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    payloads = []
    try:
        async with sessions() as session, session.begin():
            _, target = await _seed_ready_trial(session, now=datetime.now(UTC))

        class Transport:
            def open(self, request, timeout):
                if request.method == "GET":
                    result = {"targets": [{"target_id": target.target_id, "desired_state": "active"}]}
                else:
                    payload = json.loads(request.data)
                    payloads.append(payload)
                    result = {"target_id": target.target_id, **payload}
                response = io.BytesIO(json.dumps(result).encode())
                response.status = 200
                return response

        monkeypatch.setattr(urllib.request, "build_opener", lambda *args: Transport())
        secret = tmp_path / "admin.toml"
        secret.write_text('[admin]\ntoken = "retirement-test"\n')
        namespace = {"__name__": "retirement_fixture"}
        exec(TARGET_RETIRE_PROGRAM, namespace)
        namespace["target_action"](
            "retire", target.target_id, "replacement-target", secret_file=secret, origin="http://fixture",
        )
        assert len(payloads) == 1
        body = TargetHealthBody.model_validate(payloads[0])
        async with sessions() as session, session.begin():
            await set_execution_target_health(session, target_id=target.target_id, **body.model_dump())
        async with sessions() as session:
            stored = await session.get(ServiceExecutionTarget, target.target_id)
            assert stored is not None
            assert stored.desired_state == stored.observed_state == "retired"
            assert stored.health_status == "unhealthy"
            assert stored.health_observed_at == body.observed_at
            assert stored.health_error_code == "target_replaced"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("operator_state", ["draining", "retired"])
async def test_actuator_refreshes_target_health_during_drift_without_reenabling_operator_state(
    postgres_url: str, operator_state: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            _trial_id, target = await _seed_ready_trial(session, now=now)
            await set_execution_target_health(
                session,
                target_id=target.target_id,
                desired_state=operator_state,
                observed_state="ready",
                health_status="healthy",
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        kubernetes = _FakeKubernetesJobApi()
        kubernetes.jobs["foreign-managed-job"] = KubernetesJobObservation(
            namespace=target.namespace_name,
            job_name="foreign-managed-job",
            lease_id=str(uuid4()),
            resource_generation=1,
            target_id=target.target_id,
            execution_unit_key=str(uuid4()),
            normalized_state=NormalizedJobState.PENDING,
            job_uid="foreign-uid",
            resource_version="1",
        )
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
            ),
            controller_id="target-health-refresh-test",
        )
        refreshed_at = now + timedelta(seconds=2)
        assert await actuator.reconcile_full_once(now=refreshed_at) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionTarget, target.target_id)
            assert persisted is not None
            assert persisted.desired_state == operator_state
            assert persisted.observed_state == "ready"
            assert persisted.health_status == "healthy"
            assert persisted.health_observed_at == refreshed_at
            with pytest.raises(
                ServiceExecutionConflict,
                match="health observation regressed",
            ):
                await refresh_execution_target_health(
                    session,
                    target_id=target.target_id,
                    observed_at=refreshed_at - timedelta(seconds=1),
                )
    finally:
        await engine.dispose()


async def test_reservation_persists_trial_lease_command_and_history_atomically(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    request_id = uuid4()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            await session.commit()
            lease_id = lease.id

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            persisted = await session.get(ServiceExecutionLease, lease_id)
            commands = (
                (
                    await session.execute(
                        select(ServiceExecutionCommand).where(
                            ServiceExecutionCommand.lease_id == lease_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            history = (
                await session.execute(
                    select(ServiceExecutionLeaseHistory).where(
                        ServiceExecutionLeaseHistory.lease_id == lease_id
                    )
                )
            ).scalar_one()
            cost_reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease_id
                    )
                )
            ).scalar_one()
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("claimed", 1)
            route = ExecutionRoutingDecisionV1.model_validate(trial.execution_route_json)
            assert route.reason == ExecutionRoutingReason.ADMIN_TARGET_BINDING
            assert route.selected_pool_id == "nebius-cpu"
            assert persisted is not None
            assert (persisted.attempt, persisted.generation) == (1, 1)
            assert persisted.routing_generation == route.generation
            assert persisted.selected_pool_id == route.selected_pool_id
            assert persisted.routing_decision_sha256 == trial.execution_route_sha256
            assert persisted.last_event_ordinal == 0
            assert len(commands) == 1
            assert (commands[0].command_type, commands[0].state) == ("create", "pending")
            assert commands[0].payload_json["selected_pool_id"] == "nebius-cpu"
            assert commands[0].payload_json["cost_reservation_id"] == str(cost_reservation.id)
            assert commands[0].payload_json["price_snapshot_id"] == str(
                cost_reservation.price_snapshot_id
            )
            assert commands[0].payload_json["estimated_cost_microusd"] == 3_600_000
            assert cost_reservation.estimated_cost_microusd == 3_600_000
            assert cost_reservation.requested_cpu_millis == 1_000
            assert cost_reservation.requested_memory_mib == 1_024
            assert cost_reservation.requested_ephemeral_storage_mib == 2_048
            assert history.snapshot_json["selected_pool_id"] == "nebius-cpu"
            projection = execution_lease_projection(persisted)
            assert projection["selected_pool_id"] == "nebius-cpu"
            assert projection["runtime_identity"]["image_admission_sha256"].startswith("sha256:")
            assert {
                item["image_ref"] for item in projection["runtime_identity"]["image_admissions"]
            } == {
                "registry.example/loom/task@sha256:" + "a" * 64,
                "registry.example/loom/runtime@sha256:" + "b" * 64,
            }

            replay = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            assert replay.id == lease_id

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="route can change only while queued"):
                await session.execute(
                    update(Trial)
                    .where(Trial.id == trial_id)
                    .values(execution_route_pool_name="gb10")
                )
                await session.commit()
            await session.rollback()

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="immutable identity changed"):
                await session.execute(
                    update(ServiceExecutionLease)
                    .where(ServiceExecutionLease.id == lease_id)
                    .values(selected_pool_id="gb10")
                )
                await session.commit()
            await session.rollback()
    finally:
        await engine.dispose()


async def _configure_scheduler_trial(
    session: AsyncSession,
    *,
    trial_id: UUID,
    now: datetime,
    batch_backend: str = "nebius",
    cpu_millis: int = 1000,
) -> None:
    trial = await session.get(Trial, trial_id)
    assert trial is not None
    session.add(TeamQuota(team_id=trial.team_id))
    batch_id = uuid4()
    session.add(
        Batch(
            id=batch_id,
            team_id=trial.team_id,
            name=f"service scheduler {batch_backend}",
            task_filter={},
            trial_config={},
            backend=batch_backend,
            state="submitted",
            created_by_token_prefix="test",
            expected_trial_count=1,
        )
    )
    trial.batch_id = batch_id
    task = await session.get(Task, trial.task_id)
    assert task is not None
    plan = _runtime_contract(now=now)
    plan = plan.model_copy(
        update={"task_resources": plan.task_resources.model_copy(update={"cpu_millis": cpu_millis})}
    )
    task.checksum = plan.task_revision_sha256.removeprefix("sha256:")
    task.config = {
        "schema_version": "1",
        "task": {"id": task.id, "name": "Service execution scheduler"},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "docker_image": plan.task_image_ref,
            "cpus": cpu_millis / 1000,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "tmpfs": ["/tmp"],
            "baseline_network_policy": {"kind": "gateway-only"},
            "network_policies_supported": ["gateway-only"],
        },
        "agent": {"name": "service-smoke"},
        "verifier": {"name": "script"},
        "service_execution": {
            "schema_version": "loom.task-service-execution.v1",
            "logical_pool_id": "nebius-cpu",
            "runtime_template": plan.model_dump(mode="json", exclude={"task_revision_sha256"}),
        },
    }
    trial.requires_caps = {
        "os": "linux",
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "network_policies": ["gateway-only"],
        "backend": "nebius",
        "worker_pool": "nebius-cpu",
    }


@pytest.mark.parametrize(
    ("batch_backend", "expects_lease"),
    [("nebius", True), ("docker", False)],
)
async def test_normal_scheduler_requires_explicit_nebius_backend(
    postgres_url: str,
    batch_backend: str,
    expects_lease: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(
                session, trial_id=trial_id, now=now, batch_backend=batch_backend
            )
            await session.execute(
                delete(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_key.in_((target.logical_pool_id, target.target_id))
                )
            )
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now,
            )
            await session.commit()
            if not expects_lease:
                assert lease is None
                return
            assert lease is not None
            assert lease.trial_id == trial_id
            assert lease.target_id == target.target_id
            assert lease.selected_pool_id == "nebius-cpu"
            assert lease.routing_reason == ExecutionRoutingReason.PREEXISTING_ASSIGNMENT
            assert lease.desired_state == "create"

        async with sessions() as session:
            persisted_trial = await session.get(Trial, trial_id)
            assert persisted_trial is not None
            assert persisted_trial.state == "claimed"
            assert persisted_trial.worker_id is None
            assert (
                await session.scalar(
                    select(func.count(ExecutionCostReservation.id)).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count(ExecutionCostReservationDebit.reservation_id)).where(
                        ExecutionCostReservationDebit.reservation_id
                        == select(ExecutionCostReservation.id)
                        .where(ExecutionCostReservation.lease_id == lease.id)
                        .scalar_subquery()
                    )
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("has_smaller_trial", [False, True])
async def test_scheduler_capacity_wait_rolls_back_and_skips_oversized_head(
    postgres_url: str,
    has_smaller_trial: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            blocked_id, target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(
                session, trial_id=blocked_id, now=now, cpu_millis=65_000
            )
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None
            blocked.submit_priority = 200
            team_id = blocked.team_id
            smaller_id = None
            if has_smaller_trial:
                smaller_id, _ = await _seed_ready_trial(session, now=now)
                await _configure_scheduler_trial(session, trial_id=smaller_id, now=now)
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now,
            )
            await session.commit()
            if smaller_id is None:
                assert lease is None
            else:
                assert lease is not None and lease.trial_id == smaller_id
                assert lease.attempt == 1
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ExecutionProvisioningAuthorization)
                        .where(ExecutionProvisioningAuthorization.lease_id == lease.id)
                    )
                    == 1
                )

        async with sessions() as session:
            blocked = await session.get(Trial, blocked_id)
            assert blocked is not None
            assert blocked.state == "queued" and blocked.attempt_count == 0
            assert blocked.claimed_at is None and blocked.started_at is None
            assert blocked.next_attempt_at == now + timedelta(seconds=15)
            quota = await session.get(TeamQuota, team_id)
            assert quota is not None and quota.in_flight_count == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionLease)
                    .where(ServiceExecutionLease.trial_id == blocked_id)
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ExecutionCostReservation)
                    .where(ExecutionCostReservation.team_id == team_id)
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ExecutionAdmissionReservation)
                    .where(ExecutionAdmissionReservation.team_id == team_id)
                )
                == 0
            )
            budget = await session.scalar(
                select(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_kind == "target",
                    ExecutionBudgetPolicy.scope_key == target.target_id,
                )
            )
            assert budget is not None
            if not has_smaller_trial:
                assert budget.daily_reserved_microusd == budget.monthly_reserved_microusd == 0
            # Correcting the task lets the same first attempt start after the
            # existing backoff, with a fresh execution deadline.
            task = await session.get(Task, blocked.task_id)
            assert task is not None
            config = json.loads(json.dumps(task.config))
            config["environment"]["cpus"] = 1
            config["service_execution"]["runtime_template"]["task_resources"]["cpu_millis"] = 1000
            task.config = config
            await session.commit()

        async with sessions() as session:
            assert (
                await reserve_next_service_execution(
                    session,
                    environment="staging",
                    pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    now=now + timedelta(seconds=14),
                )
                is None
            )
            recovered = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now + timedelta(seconds=15),
            )
            assert recovered is not None and recovered.trial_id == blocked_id
            assert recovered.attempt == 1
            assert recovered.deadline_at > now + timedelta(seconds=15 + 600)
            await session.commit()
    finally:
        await engine.dispose()


async def test_scheduler_capacity_scan_is_bounded(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, _ = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now, cpu_millis=65_000)
            first = await session.get(Trial, trial_id)
            assert first is not None
            trial_ids = [trial_id, *(uuid4() for _ in range(32))]
            for extra_id in trial_ids[1:]:
                session.add(
                    Trial(
                        id=extra_id,
                        team_id=first.team_id,
                        task_id=first.task_id,
                        batch_id=first.batch_id,
                        config=first.config,
                        requires_caps=first.requires_caps,
                        state="queued",
                        attempt_count=0,
                    )
                )
            await session.commit()
        async with sessions() as session:
            assert (
                await reserve_next_service_execution(
                    session,
                    environment="staging",
                    pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    now=now,
                )
                is None
            )
            await session.commit()
        async with sessions() as session:
            rows = (await session.scalars(select(Trial).where(Trial.id.in_(trial_ids)))).all()
            assert len(rows) == 33
            assert sum(row.next_attempt_at is not None for row in rows) == 32
            assert all(row.state == "queued" and row.attempt_count == 0 for row in rows)
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionLease)
                    .where(ServiceExecutionLease.trial_id.in_(trial_ids))
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("many_artifacts", [False, True])
async def test_scheduler_compiles_ordinary_task_from_deployment_profile(
    postgres_url: str, many_artifacts: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    plan = _runtime_contract(now=now)
    runtime_profile = ServiceExecutionRuntimeProfileV1(
        candidate_sha=plan.candidate_sha,
        execution_class_id=plan.execution_class_id,
        task_image_ref=plan.task_image_ref,
        runtime_image_ref=plan.runtime_image_ref,
        runtime_binary_sha256=plan.runtime_binary_sha256,
        image_admission=plan.image_admission,
        supports_task_artifact_inputs=many_artifacts,
    )
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(TeamQuota(team_id=trial.team_id))
            batch_id = uuid4()
            session.add(
                Batch(
                    id=batch_id,
                    team_id=trial.team_id,
                    name="automatic service scheduler",
                    task_filter={},
                    trial_config={},
                    backend="nebius",
                    service_execution_runtime_profile=runtime_profile.model_dump(mode="json"),
                    state="submitted",
                    created_by_token_prefix="test",
                    expected_trial_count=1,
                )
            )
            trial.batch_id = batch_id
            trial.config = {
                "schema_version": "1",
                "agent_name": "direct-completion",
                "agent_model": {
                    "provider": "openai",
                    "name": "gpt-5",
                    "source": "api",
                },
            }
            trial.requires_caps = {
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "network_policies": ["gateway-only"],
                "backend": "nebius",
                "worker_pool": "nebius-cpu",
            }
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.checksum = plan.task_revision_sha256.removeprefix("sha256:")
            task.config = {
                "schema_version": "1",
                "task": {"id": task.id, "name": "Automatic service execution"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "docker_image": plan.task_image_ref,
                    "cpus": 1,
                    "memory_mb": 1024,
                    "storage_mb": 2048,
                    "baseline_network_policy": {"kind": "gateway-only"},
                    "network_policies_supported": ["gateway-only"],
                },
                "agent": {"name": "direct-completion"},
                "verifier": {
                    "name": "script",
                    "args": {"script_path": "verifier/check.sh"},
                },
                "steps": [
                    {
                        "name": "main",
                        "instruction_file": "instruction.md",
                        "artifacts": ([f"out/part-{index:04}.json" for index in range(515)]
                                      if many_artifacts else ["answer.txt"]),
                    }
                ],
            }
            task.source_provenance = {
                "service_execution_input": {
                    "schema_version": "loom.service-execution-input.v1",
                    "manifest_uri": "s3://artifacts/task-inputs/task.json",
                    "manifest_sha256": "sha256:" + "d" * 64,
                    "file_count": 3,
                    "total_bytes": 4096,
                }
            }
            await session.execute(
                delete(ExecutionBudgetPolicy).where(
                    ExecutionBudgetPolicy.scope_key.in_((target.logical_pool_id, target.target_id))
                )
            )
            await session.commit()

        async with sessions() as session:
            lease = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now,
            )
            await session.commit()

        assert lease is not None
        persisted_plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        assert persisted_plan.task_input is not None
        assert persisted_plan.task_input.file_count == 3
        assert persisted_plan.main.environment["LOOM_TASK_MODEL"] == "openai/gpt-5"
        assert persisted_plan.main.argv[-1] == "direct-completion"
        if many_artifacts:
            assert persisted_plan.main.environment["LOOM_TASK_ARTIFACTS_FROM_INPUT"] == "1"
            assert "LOOM_TASK_ARTIFACTS_JSON" not in persisted_plan.main.environment
            assert sum(item.kind == "task_artifact" for item in persisted_plan.output_declarations) == 515
    finally:
        await engine.dispose()


async def test_nebius_budget_reservation_is_race_safe_across_service_leases(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first = await session.get(Trial, first_trial_id)
            assert first is not None
            second_trial_id = uuid4()
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first.team_id,
                    task_id=first.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                )
            )
            for scope_kind, scope_key in (
                ("pool", target.logical_pool_id),
                ("target", target.target_id),
            ):
                await upsert_execution_budget_policy(
                    session,
                    scope_kind=scope_kind,  # type: ignore[arg-type]
                    scope_key=scope_key,
                    daily_limit_microusd=3_600_000,
                    monthly_limit_microusd=3_600_000,
                    per_attempt_limit_microusd=3_600_000,
                    max_estimate_duration_seconds=3_600,
                    emergency_stop=False,
                    enabled=True,
                    reason="one paid attempt",
                    now=now,
                )
            await session.commit()

        async def reserve(trial_id: UUID) -> ServiceExecutionLease | str:
            async with sessions() as session:
                try:
                    lease = await _reserve(
                        session,
                        trial_id=trial_id,
                        target=target,
                        now=now,
                    )
                    await session.commit()
                    return lease
                except ServiceExecutionConflict as exc:
                    await session.rollback()
                    return str(exc)

        results = await asyncio.gather(reserve(first_trial_id), reserve(second_trial_id))
        leases = [result for result in results if isinstance(result, ServiceExecutionLease)]
        blockers = [result for result in results if isinstance(result, str)]
        assert len(leases) == 1
        assert blockers == ["execution_budget_pool_daily_limit_exceeded"]

        async with sessions() as session:
            policies = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {row.daily_reserved_microusd for row in policies} == {3_600_000}
            assert await session.scalar(select(func.count(ExecutionCostReservation.id))) == 1
    finally:
        await engine.dispose()


async def test_nebius_paid_execution_fails_closed_without_price_or_during_emergency_stop(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            binding = await session.get(ExecutionTargetPriceBinding, target.target_id)
            assert binding is not None
            binding.enabled = False
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match="execution_cost_price_binding_unavailable",
            ):
                await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.rollback()

        async with sessions() as session:
            binding = await session.get(ExecutionTargetPriceBinding, target.target_id)
            assert binding is not None
            binding.enabled = True
            pool_policy = (
                await session.execute(
                    select(ExecutionBudgetPolicy).where(
                        ExecutionBudgetPolicy.scope_kind == "pool",
                        ExecutionBudgetPolicy.scope_key == target.logical_pool_id,
                    )
                )
            ).scalar_one()
            pool_policy.emergency_stop = True
            pool_policy.reason = "provider emergency stop test"
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match="execution_budget_pool_emergency_stop",
            ):
                await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.rollback()
            assert await session.scalar(select(func.count(ExecutionCostReservation.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("pending", "execution_capacity_pending_limit_exceeded"),
        ("quota", "execution_capacity_provider_quota_nodes_exceeded"),
        ("physical", "execution_capacity_physical_capacity_unavailable"),
        ("stale", "execution_capacity_observation_stale"),
    ),
)
async def test_actuator_defers_create_with_distinct_capacity_blocker(
    postgres_url: str,
    case: str,
    expected_reason: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await upsert_execution_capacity_policy(
                session,
                target_id=target.target_id,
                enabled=True,
                max_nodes=20,
                max_vcpu_millis=1_280_000,
                max_memory_mib=5_242_880,
                max_storage_mib=20_971_520,
                node_cpu_millis=64_000,
                node_memory_mib=262_144,
                node_storage_mib=1_048_576,
                max_pending_jobs=1 if case == "pending" else 100,
                max_unschedulable_jobs=10,
                max_image_pull_backoff_jobs=10,
                max_create_per_minute=100,
                observation_max_age_seconds=10 if case == "stale" else 900,
                reason=f"{case} blocker test",
                now=now + timedelta(seconds=1),
            )
            quota_nodes = 1 if case == "quota" else 20
            quota_vcpu = 64_000 if case == "quota" else 1_280_000
            quota_memory = 262_144 if case == "quota" else 5_242_880
            quota_storage = 1_048_576 if case == "quota" else 20_971_520
            cluster_requested = 64_000 if case in {"quota", "physical"} else 0
            await create_execution_capacity_observation(
                session,
                target_id=target.target_id,
                placement=placement_fixture(
                    target_id=target.target_id,
                    quota_nodes=quota_nodes,
                    requested_cpu=cluster_requested,
                    requested_memory=262_144 if cluster_requested else 0,
                    requested_storage=1_048_576 if cluster_requested else 0,
                ),
                source="service-execution-test",
                source_version=f"{target.target_id}-{case}-blocker",
                observed_at=now + timedelta(seconds=1),
                provider_capacity_state=("insufficient" if case == "physical" else "available"),
                provider_capacity_reason=(
                    "provider reports no physical placement" if case == "physical" else None
                ),
                autoscaler_state="ready",
                autoscaler_reason=None,
                provider_quota_nodes=quota_nodes,
                provider_quota_vcpu_millis=quota_vcpu,
                provider_quota_memory_mib=quota_memory,
                provider_quota_storage_mib=quota_storage,
                provider_used_nodes=1,
                provider_used_vcpu_millis=64_000,
                provider_used_memory_mib=262_144,
                provider_used_storage_mib=1_048_576,
                active_nodes=1,
                provisioned_vcpu_millis=64_000,
                provisioned_memory_mib=262_144,
                provisioned_storage_mib=1_048_576,
                allocatable_cpu_millis=64_000,
                allocatable_memory_mib=262_144,
                allocatable_storage_mib=1_048_576,
                requested_cpu_millis=cluster_requested,
                requested_memory_mib=(262_144 if cluster_requested else 0),
                requested_storage_mib=(1_048_576 if cluster_requested else 0),
                pending_jobs=1 if case == "pending" else 0,
                unschedulable_jobs=0,
                image_pull_backoff_jobs=0,
                pending_reasons={"autoscaler_delay": 1} if case == "pending" else {},
            )
            await session.commit()

        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
                runtime_class_name="loom-sandbox",
            ),
            controller_id="capacity-blocker-test",
            command_lease_seconds=5,
        )
        actuator_time = now + timedelta(seconds=20 if case == "stale" else 2)
        assert await actuator.run_commands_once(now=actuator_time) == 1
        assert kubernetes.create_count == 0

        async with sessions() as session:
            command = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "create",
                    )
                )
            ).scalar_one()
            assert command.state == "pending"
            assert command.last_error_code == expected_reason
            assert (
                await session.scalar(
                    select(func.count(ExecutionProvisioningAuthorization.id)).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
                == 1
            )
            status = await fetch_execution_capacity_status(
                session,
                pool_id=target.logical_pool_id,
                now=actuator_time,
            )
            targets = status["targets"]
            assert isinstance(targets, list)
            target_status = targets[0]
            assert isinstance(target_status, dict)
            blockers = target_status["blockers"]
            assert isinstance(blockers, list)
            assert expected_reason in blockers
    finally:
        await engine.dispose()


async def test_provisioning_pending_limit_is_race_safe_before_claim(
    postgres_url: str,
) -> None:
    from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first_trial = await session.get(Trial, first_trial_id)
            assert first_trial is not None
            second_trial_id = uuid4()
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first_trial.team_id,
                    task_id=first_trial.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                )
            )
            policy = await session.get(ExecutionCapacityPolicy, target.target_id)
            assert policy is not None
            policy.max_pending_jobs = 1
            await session.commit()

        async def claim(trial_id: UUID) -> ServiceExecutionLease | str:
            try:
                async with sessions() as session, session.begin():
                    return await _reserve(session, trial_id=trial_id, target=target, now=now)
            except ExecutionProvisioningBlockedError as exc:
                return exc.reason

        results = await asyncio.gather(claim(first_trial_id), claim(second_trial_id))
        assert sum(isinstance(row, ServiceExecutionLease) for row in results) == 1
        assert "execution_capacity_pending_limit_exceeded" in results
        async with sessions() as session:
            trials = (
                (
                    await session.execute(
                        select(Trial).where(Trial.id.in_([first_trial_id, second_trial_id]))
                    )
                )
                .scalars()
                .all()
            )
            assert sorted(row.attempt_count for row in trials) == [0, 1]
            assert sum(row.state == "queued" for row in trials) == 1
            assert (
                await session.scalar(select(func.count(ExecutionProvisioningAuthorization.id))) == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("cleanup_node_field", [{}, {"node_name": None}])
@pytest.mark.parametrize("historical_recovery", [False, True])
async def test_provider_node_bill_allocates_requested_share_and_exposes_overhead(
    postgres_url: str,
    cleanup_node_field: dict[str, object],
    historical_recovery: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    # Billing intervals in this scenario must stay inside one UTC day, even
    # when the suite itself happens to run immediately before midnight.
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    pod_started = now + timedelta(seconds=10)
    pod_stopped = now + timedelta(seconds=110)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-finance",
                    "pod_uid": "pod-finance",
                    "resource_version": "1",
                    "node_name": "node-finance",
                    "scheduled_at": (pod_started - timedelta(seconds=1)).isoformat(),
                    "started_at": pod_started.isoformat(),
                },
                observed_at=pod_started,
            )
            await session.commit()

        async with sessions() as session:
            reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one()
            with pytest.raises(
                ValueError,
                match="without a persisted termination timestamp",
            ):
                await record_execution_node_cost(
                    session,
                    target_id=target.target_id,
                    price_snapshot_id=reservation.price_snapshot_id,
                    provider_record_id=f"premature-bill-{uuid4()}",
                    node_name="node-finance",
                    interval_started_at=pod_started,
                    interval_stopped_at=pod_started + timedelta(seconds=50),
                    node_cpu_millis=64_000,
                    node_memory_mib=262_144,
                    node_ephemeral_storage_mib=1_048_576,
                    provider_billed_microusd=1_000_000,
                    billing_source="nebius-invoice-export",
                    billing_source_version="invoice-2026-08",
                    observed_at=pod_started + timedelta(seconds=51),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "failed",
                    "job_uid": "job-finance",
                    "pod_uid": "pod-finance",
                    "resource_version": "2",
                    "node_name": "node-finance",
                    "scheduled_at": (pod_started - timedelta(seconds=1)).isoformat(),
                    "started_at": pod_started.isoformat(),
                    "terminated_at": pod_stopped.isoformat(),
                    "message": "test terminal",
                },
                observed_at=pod_stopped,
            )
            await session.commit()

        async with sessions() as session:
            reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert reservation.state == "awaiting_settlement"
            # The Pod disappears before its bill arrives. Sparse teardown
            # observations must retain the node that incurred the charge.
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=current.generation,
                desired_state="cancel",
                now=pod_stopped + timedelta(seconds=1),
            )
            if historical_recovery:
                # Old observations lost the node before the lease became immutable.
                current.node_name = None
                await session.flush()
            for ordinal, state in [(3, "terminating"), (4, "deleted")]:
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=current.generation,
                    ordinal=ordinal,
                    event_kind="kubernetes_observed",
                    payload={"normalized_state": state, **cleanup_node_field},
                    observed_at=pod_stopped + timedelta(seconds=ordinal),
                )
            await session.commit()

        async with sessions() as session:
            reservation = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one()
            if historical_recovery:
                recovered = await recover_execution_node_attribution(
                    session, lease_id=lease.id, apply=True
                )
                assert recovered["status"] == "recovered"
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.deleted_at is not None
            node_cost, created = await record_execution_node_cost(
                session,
                target_id=target.target_id,
                price_snapshot_id=reservation.price_snapshot_id,
                provider_record_id=f"bill-{uuid4()}",
                node_name="node-finance",
                interval_started_at=pod_started,
                interval_stopped_at=pod_stopped,
                node_cpu_millis=reservation.requested_cpu_millis * 4,
                node_memory_mib=reservation.requested_memory_mib * 4,
                node_ephemeral_storage_mib=(reservation.requested_ephemeral_storage_mib * 4),
                provider_billed_microusd=4_000_000,
                billing_source="nebius-invoice-export",
                billing_source_version="invoice-2026-08",
                observed_at=pod_stopped + timedelta(seconds=5),
            )
            assert created is True
            assert node_cost.allocated_microusd == 1_000_000
            assert node_cost.idle_system_fragmentation_microusd == 3_000_000
            settled = await settle_execution_cost_reservation(
                session,
                reservation_id=reservation.id,
                billing_complete_through=pod_stopped,
                now=pod_stopped + timedelta(seconds=6),
            )
            await session.commit()
            assert settled.state == "settled"
            assert settled.actual_allocated_microusd == 1_000_000

        async with sessions() as session:
            policies = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {row.daily_reserved_microusd for row in policies} == {0}
            assert {row.daily_settled_microusd for row in policies} == {4_000_000}
            allocation = (
                await session.execute(
                    select(ExecutionNodeCostAllocation).where(
                        ExecutionNodeCostAllocation.cost_reservation_id == reservation.id
                    )
                )
            ).scalar_one()
            assert allocation.dominant_resource_fraction_ppb == 250_000_000
            assert allocation.overlap_seconds == 100
            assert allocation.allocated_microusd == 1_000_000
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "scenario",
    [
        "valid",
        "missing_pod",
        "no_evidence",
        "conflicting_nodes",
        "wrong_pod",
        "wrong_job",
        "outside_generation",
        "missing_event_pod",
    ],
)
async def test_node_recovery_api_previews_audits_and_rejects_ambiguous_evidence(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
) -> None:
    import httpx

    from loom_control_plane.routes import admin

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            event, _ = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "pod_uid": "pod-recovery",
                    "job_uid": "job-recovery",
                    "node_name": "node-recovery",
                },
                observed_at=now,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                payload={"reason": "test recovery after deletion"},
                now=now,
            )
            lease.node_name = None  # Simulate the projection written by the old code.
            payload = dict(event.payload_json)
            if scenario == "missing_pod":
                lease.pod_uid = None
            elif scenario == "no_evidence":
                payload["node_name"] = None
            elif scenario == "wrong_pod":
                payload["pod_uid"] = "different-pod"
            elif scenario == "wrong_job":
                payload["job_uid"] = "different-job"
            elif scenario == "missing_event_pod":
                payload.pop("pod_uid")
            elif scenario == "outside_generation":
                event.generation = 3
            elif scenario == "conflicting_nodes":
                conflicting = {**payload, "node_name": "other-node"}
                session.add(
                    ServiceExecutionEvent(
                        id=uuid4(),
                        lease_id=lease.id,
                        generation=1,
                        ordinal=3,
                        event_kind="kubernetes_observed",
                        idempotency_key=str(uuid4()),
                        payload_json=conflicting,
                        payload_sha256=canonical_digest(conflicting),
                        observed_at=now,
                    )
                )
            event.payload_json = payload
            event.payload_sha256 = canonical_digest(payload)
            await session.flush()
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=2,
                ordinal=2,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "deleted"},
                observed_at=now,
            )
            await session.commit()
            before = (
                lease.generation,
                lease.last_event_ordinal,
                lease.observed_state,
                lease.pod_started_at,
                lease.pod_terminated_at,
            )

        async def verify_token(session, authorization, **kwargs):
            del session, kwargs
            if authorization is None:
                return None
            return AuthContext(
                token_hash=b"recovery-test",
                type="admin",
                scopes=["admin:worker_pools"] if authorization == "Bearer admin" else ["read:own"],
                team_id=None,
                expires_at=None,
            )

        monkeypatch.setattr(admin, "verify_bearer_token", verify_token)
        app = FastAPI()
        app.state.session_factory = sessions
        app.include_router(admin.router)
        url = f"/admin/execution-leases/{lease.id}/recover-node-attribution"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.post(url, json={"apply": True})).status_code == 403
            assert (
                await client.post(
                    url, json={"apply": True}, headers={"Authorization": "Bearer member"}
                )
            ).status_code == 403
            headers = {"Authorization": "Bearer admin", "x-request-id": "recovery-test"}
            assert (
                await client.post(url, json={"apply": "true"}, headers=headers)
            ).status_code == 422
            preview = await client.post(url, json={}, headers=headers)
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current is not None and current.node_name is None
                assert (
                    await session.scalar(
                        select(func.count(AdminAuditEvent.id)).where(
                            AdminAuditEvent.target_id == str(lease.id)
                        )
                    )
                    == 0
                )
            if scenario == "valid":
                assert preview.status_code == 200, preview.text
                assert preview.json()["status"] == "recoverable"
                assert preview.json()["evidence_event_ids"] == [str(event.id)]
                async with sessions() as session:
                    # The database exception must not permit other deleted-row edits.
                    with pytest.raises(DBAPIError, match="immutable outside materialization"):
                        await session.execute(
                            update(ServiceExecutionLease)
                            .where(ServiceExecutionLease.id == lease.id)
                            .values(node_name="node-recovery", last_event_ordinal=99)
                        )
                    await session.rollback()
                responses = await asyncio.gather(
                    *[client.post(url, json={"apply": True}, headers=headers) for _ in range(2)]
                )
                assert all(response.status_code == 200 for response in responses)
                assert sorted(response.json()["status"] for response in responses) == [
                    "recovered",
                    "unchanged",
                ]
            else:
                assert preview.status_code == 409, preview.text
                applied = await client.post(url, json={"apply": True}, headers=headers)
                assert applied.status_code == 409, applied.text
                async with sessions() as session:
                    # Direct SQL must reject the same unsupported attribution.
                    with pytest.raises(DBAPIError, match="immutable outside materialization"):
                        await session.execute(
                            update(ServiceExecutionLease)
                            .where(ServiceExecutionLease.id == lease.id)
                            .values(node_name="node-recovery")
                        )
                    await session.rollback()
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None
            assert current.node_name == ("node-recovery" if scenario == "valid" else None)
            assert (
                current.generation,
                current.last_event_ordinal,
                current.observed_state,
                current.pod_started_at,
                current.pod_terminated_at,
            ) == before
            audit = (
                await session.scalars(
                    select(AdminAuditEvent).where(
                        AdminAuditEvent.target_id == str(lease.id),
                        AdminAuditEvent.action == "execution.node_attribution.recovered",
                    )
                )
            ).all()
            assert len(audit) == (1 if scenario == "valid" else 0)
            if audit:
                assert audit[0].event_metadata["evidence_event_ids"] == [str(event.id)]
                assert audit[0].request_id == "recovery-test"
                for forbidden in (None, "different-node"):
                    with pytest.raises(DBAPIError, match="immutable outside materialization"):
                        await session.execute(
                            update(ServiceExecutionLease)
                            .where(ServiceExecutionLease.id == lease.id)
                            .values(node_name=forbidden)
                        )
                    await session.rollback()
    finally:
        await engine.dispose()


@pytest.mark.parametrize("changed_field", ["node_name", "pod_uid"])
async def test_kubernetes_node_attribution_rejects_identity_changes(
    postgres_url: str,
    changed_field: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "running", "pod_uid": "pod-a", "node_name": "node-a"},
                observed_at=now,
            )
            await session.commit()

        async with sessions() as session:
            # A delayed pre-scheduling observation is historical evidence only.
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "pending", "node_name": None},
                observed_at=now - timedelta(seconds=1),
            )
            await session.commit()
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None
            assert (current.node_name, current.pod_uid, current.observed_state) == (
                "node-a",
                "pod-a",
                "running",
            )

        async with sessions() as session:
            payload = {
                "normalized_state": "running",
                "pod_uid": "pod-a",
                "node_name": "node-a",
                changed_field: "different-identity",
            }
            with pytest.raises(ServiceExecutionConflict, match=f"{changed_field} changed"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=3,
                    event_kind="kubernetes_observed",
                    payload=payload,
                    observed_at=now + timedelta(seconds=1),
                )
            await session.rollback()

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None
            assert (current.node_name, current.pod_uid, current.last_event_ordinal) == (
                "node-a",
                "pod-a",
                2,
            )
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionEvent.id)).where(
                        ServiceExecutionEvent.lease_id == lease.id,
                    )
                )
                == 2
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "scope_kind",
    ["global", "environment", "region", "team", "batch", "execution_class", "pool"],
)
async def test_service_execution_enforces_each_scope_and_releases_on_terminal_event(
    postgres_url: str,
    scope_kind: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    policy_id = uuid4()
    batch_id = uuid4()
    try:
        async with sessions() as session:
            first_trial_id, target = await _seed_ready_trial(session, now=now)
            first_trial = await session.get(Trial, first_trial_id)
            assert first_trial is not None
            second_trial_id = uuid4()
            session.add(
                Batch(
                    id=batch_id,
                    team_id=first_trial.team_id,
                    name=f"service admission {scope_kind}",
                    task_filter={},
                    trial_config={},
                    state="submitted",
                    created_by_token_prefix="test",
                    expected_trial_count=2,
                )
            )
            first_trial.batch_id = batch_id
            session.add(
                Trial(
                    id=second_trial_id,
                    team_id=first_trial.team_id,
                    task_id=first_trial.task_id,
                    config={"agent": {"name": "test"}},
                    requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                    state="queued",
                    attempt_count=0,
                    batch_id=batch_id,
                )
            )
            scope_keys = {
                "global": "*",
                "environment": target.environment,
                "region": target.region,
                "team": str(first_trial.team_id),
                "batch": str(batch_id),
                "execution_class": NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
                "pool": target.logical_pool_id,
            }
            session.add(
                ExecutionAdmissionPolicy(
                    id=policy_id,
                    scope_kind=scope_kind,
                    scope_key=scope_keys[scope_kind],
                    max_concurrent=1,
                    enabled=True,
                    reason="service pool ceiling test",
                )
            )
            first = await _reserve(
                session,
                trial_id=first_trial_id,
                target=target,
                now=now,
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                ServiceExecutionConflict,
                match=f"execution_admission_{scope_kind}_ceiling_reached",
            ):
                await _reserve(
                    session,
                    trial_id=second_trial_id,
                    target=target,
                    now=now + timedelta(seconds=1),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=1,
                ordinal=1,
                event_kind="failed",
                payload={"failure_reason": "test_terminal"},
                observed_at=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            released = (
                await session.execute(
                    select(ExecutionAdmissionReservation).where(
                        ExecutionAdmissionReservation.owner_id == first.id
                    )
                )
            ).scalar_one()
            assert released.state == "released"
            released_cost = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == first.id
                    )
                )
            ).scalar_one()
            assert released_cost.state == "released"
            assert released_cost.release_reason == "lease_terminal_before_pod_start"
            second = await _reserve(
                session,
                trial_id=second_trial_id,
                target=target,
                now=now + timedelta(seconds=3),
            )
            assert second.id != first.id
            await session.commit()
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(ExecutionAdmissionPolicy).where(ExecutionAdmissionPolicy.id == policy_id)
            )
        await engine.dispose()


async def test_kubernetes_reservation_rejects_a_legacy_worker_route(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    requirements = _requirements()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            decision = ExecutionRoutingDecisionV1(
                generation=1,
                requirements_sha256=canonical_digest(requirements.model_dump(mode="json")),
                selected_pool_id="gb10",
                selected_adapter_kind=ExecutionAdapterKind.LEGACY_WORKER_CLAIM,
                reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
                decided_at=now,
                candidates=(
                    ExecutionRouteCandidateV1(
                        logical_pool_id="gb10",
                        adapter_kind=ExecutionAdapterKind.LEGACY_WORKER_CLAIM,
                        enabled=True,
                        healthy=True,
                        draining=False,
                        configured_slots=1,
                        active_slots=1,
                        occupied_slots=0,
                        pending_slots=0,
                        assigned_queued_slots=1,
                        available_slots=0,
                        capacity_evidence_kind=CapacityEvidenceKind.PREEXISTING_ASSIGNMENT,
                    ),
                ),
            )
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.execution_route_generation = decision.generation
            trial.execution_route_pool_name = decision.selected_pool_id
            trial.execution_route_json = decision.model_dump(mode="json")
            trial.execution_route_sha256 = canonical_digest(trial.execution_route_json)

            with pytest.raises(
                ServiceExecutionConflict,
                match="different execution authority",
            ):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now,
                    requirements=requirements,
                )
    finally:
        await engine.dispose()


async def test_kubernetes_reservation_rejects_target_residency_drift(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            with pytest.raises(ServiceExecutionConflict, match="violates data residency"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now,
                    requirements=_requirements(data_residency="ca"),
                )
    finally:
        await engine.dispose()


async def test_separate_verifier_is_a_parent_bound_execution_lease(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            parent = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=_runtime_contract(verifier_execution="separate_execution"),
            )
            await session.commit()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=parent.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "succeeded"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        verifier_requirements = _requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION)
        async with sessions() as session:
            # A pod that merely exited has not committed and handed off its workspace.
            with pytest.raises(ServiceExecutionConflict, match="verifier parent result is not ready"):
                await _reserve(
                    session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2),
                    requirements=verifier_requirements, runtime_contract=_deferred_verifier_contract(),
                    parent_lease_id=parent.id,
                )

        async with sessions() as session:
            await _hand_off_to_verifier(session, parent_id=parent.id, now=now + timedelta(seconds=1))
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="verifier parent cleanup is not complete"):
                await _reserve(
                    session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2),
                    requirements=verifier_requirements, runtime_contract=_deferred_verifier_contract(),
                    parent_lease_id=parent.id,
                )

        async with sessions() as session:
            parent_row = await session.get(ServiceExecutionLease, parent.id)
            parent_row.cleanup_state = "complete"
            parent_row.cleanup_requested_at = now
            parent_row.cleanup_deadline_at = now + timedelta(seconds=30)
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="requires a workspace handoff"):
                await _reserve(
                    session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2),
                    requirements=verifier_requirements,
                    runtime_contract=_deferred_verifier_contract(handoff_input=None),
                    parent_lease_id=parent.id,
                )

        async with sessions() as session:
            verifier = await _reserve(
                session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2),
                requirements=verifier_requirements, runtime_contract=_deferred_verifier_contract(),
                parent_lease_id=parent.id,
            )
            await session.commit()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            rows = (
                (
                    await session.execute(
                        select(ServiceExecutionLease)
                        .where(ServiceExecutionLease.trial_id == trial_id)
                        .order_by(ServiceExecutionLease.execution_role)
                    )
                )
                .scalars()
                .all()
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("running", 1)
            assert [item.execution_role for item in rows] == ["attempt", "verifier"]
            assert verifier.parent_lease_id == parent.id
            assert verifier.attempt == parent.attempt
            assert verifier.job_name.endswith("-v")
            assert rows[0].job_name.endswith("-a")

        async with sessions() as session:
            mismatched = _deferred_verifier_contract(candidate_sha="9" * 40)
            with pytest.raises(ServiceExecutionConflict, match="parent lease is not eligible"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now + timedelta(seconds=3),
                    requirements=_requirements(
                        verifier_topology=VerifierTopology.SEPARATE_EXECUTION
                    ),
                    runtime_contract=mismatched,
                    parent_lease_id=parent.id,
                )
    finally:
        await engine.dispose()


async def _separate_attempt_awaiting_verifier(
    sessions: async_sessionmaker[AsyncSession], *, now: datetime,
) -> tuple[UUID, ServiceExecutionLease, ExecutionTargetV1]:
    async with sessions() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        parent = await _reserve(
            session, trial_id=trial_id, target=target, now=now,
            requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
            runtime_contract=_runtime_contract(verifier_execution="separate_execution"),
        )
        await session.commit()
    async with sessions() as session:
        await _hand_off_to_verifier(session, parent_id=parent.id, now=now)
        row = await session.get(ServiceExecutionLease, parent.id)
        assert row is not None
        row.cleanup_state = "complete"
        row.cleanup_requested_at = now
        row.cleanup_deadline_at = now + timedelta(seconds=30)
        row.desired_state = "deleted"
        row.observed_state = "deleted"
        row.deleted_at = now
        await session.commit()
    return trial_id, parent, target


@pytest.fixture
def _stub_verifier_plan(monkeypatch: pytest.MonkeyPatch) -> list[ExecutionRuntimePlanV1]:
    import loom_control_plane.service_execution_scheduler as scheduler

    plans: list[ExecutionRuntimePlanV1] = []
    task = SimpleNamespace(verifier=SimpleNamespace(timeout_sec=60))

    async def snapshot(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(config={})

    async def files(*_args: Any, **_kwargs: Any) -> tuple[list[Any], list[str]]:
        return [], []

    def compile_plan(agent_plan: ExecutionRuntimePlanV1, *_args: Any, **kwargs: Any) -> ExecutionRuntimePlanV1:
        plan = _deferred_verifier_contract(handoff_input=kwargs["handoff_input"].model_dump(mode="json"))
        plans.append(plan)
        return plan

    monkeypatch.setattr(scheduler, "resolve_service_execution_task_snapshot", snapshot)
    monkeypatch.setattr(scheduler, "TaskConfig", SimpleNamespace(model_validate=lambda _config: task))
    monkeypatch.setattr(scheduler, "committed_handoff_files", files)
    monkeypatch.setattr(scheduler, "build_verifier_handoff_manifest", lambda **_kwargs: {})
    monkeypatch.setattr(scheduler, "verifier_handoff_input", lambda _manifest: RuntimeHandoffInputV1(
        manifest_sha256="sha256:" + "5" * 64, file_count=1, total_bytes=1,
    ))
    monkeypatch.setattr(scheduler, "compile_deferred_verifier_plan", compile_plan)
    return plans


async def test_scheduler_reserves_one_verifier_after_parent_cleanup(
    postgres_url: str, _stub_verifier_plan: list[ExecutionRuntimePlanV1],
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        trial_id, parent, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session:
            reserved = await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=5),
            )
            await session.commit()
        assert [item.parent_lease_id for item in reserved] == [parent.id]
        verifier = reserved[0]
        assert verifier.execution_role == "verifier"
        assert verifier.attempt == parent.attempt
        assert verifier.target_id == parent.target_id
        assert verifier.workload_requirements_sha256 == parent.workload_requirements_sha256

        # A repeated or concurrent pass observes the child and reserves nothing.
        async with sessions() as session:
            assert await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=6),
            ) == []
            rows = (await session.execute(
                select(ServiceExecutionLease).where(ServiceExecutionLease.trial_id == trial_id)
            )).scalars().all()
            assert sorted(item.execution_role for item in rows) == ["attempt", "verifier"]
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "running"
            assert trial.result["verifier_execution"]["state"] == "pending"
    finally:
        await engine.dispose()


async def _fail_verifier_natively(
    sessions: async_sessionmaker[AsyncSession], *, lease_id: UUID, at: datetime,
    historical_regression: bool = False,
) -> None:
    from loom_control_plane.service_execution import (
        finalize_failed_service_execution,
        record_kubernetes_observation,
    )

    async with sessions() as session, session.begin():
        lease = await session.get(ServiceExecutionLease, lease_id)
        assert lease is not None
        if historical_regression:
            await record_kubernetes_observation(
                session, lease_id=lease.id, generation=lease.generation, observed_at=at,
                payload={"normalized_state": "pending", "job_uid": f"verifier-{lease.verifier_retry}",
                         "resource_version": "created"},
            )
        failure, _ = await record_kubernetes_observation(
            session, lease_id=lease.id, generation=lease.generation, observed_at=at,
            payload={
                "normalized_state": "failed", "job_uid": lease.job_uid, "pod_uid": lease.pod_uid,
                "resource_version": f"verifier-failed-{lease.verifier_retry}",
                "started_at": (at - timedelta(seconds=1)).isoformat(),
                "reason": "BackoffLimitExceeded", "message": "verifier Job terminated",
            },
        )
        if historical_regression:
            await record_kubernetes_observation(
                session, lease_id=lease.id, generation=lease.generation,
                payload={**failure.payload_json, "normalized_state": "pending",
                         "resource_version": "delayed-pending", "reason": None, "message": None},
                observed_at=at + timedelta(seconds=6),
            )
            lease.observed_state = "creating"
            lease.error_class = lease.error_code = lease.error_message = None
            await session.flush()
            _, duplicate = await record_kubernetes_observation(
                session, lease_id=lease.id, generation=lease.generation,
                payload=failure.payload_json, observed_at=at + timedelta(minutes=5),
            )
            assert duplicate and lease.observed_state == "failed"
        assert await finalize_failed_service_execution(
            session, lease_id=lease.id, generation=lease.generation,
            observed_at=at + timedelta(minutes=5),
        )


async def _delete_lease(sessions: async_sessionmaker[AsyncSession], *, lease_id: UUID, at: datetime) -> None:
    async with sessions() as session, session.begin():
        row = await session.get(ServiceExecutionLease, lease_id)
        assert row is not None
        row.cleanup_state = "complete"
        row.cleanup_requested_at = row.cleanup_requested_at or at
        row.cleanup_deadline_at = row.cleanup_deadline_at or at + timedelta(seconds=30)
        row.desired_state = "deleted"
        row.observed_state = "deleted"
        row.deleted_at = at


@pytest.mark.parametrize("historical_regression", [False, True])
async def test_natively_failed_verifier_retries_on_a_new_lease_until_exhausted(
    postgres_url: str, _stub_verifier_plan: list[ExecutionRuntimePlanV1],
    historical_regression: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        trial_id, parent, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        clock = now + timedelta(seconds=5)
        verifiers: list[ServiceExecutionLease] = []
        for retry in range(MAX_VERIFIER_RETRIES + 1):
            async with sessions() as session:
                await set_execution_target_health(
                    session, target_id=target.target_id, desired_state="active", observed_state="ready",
                    health_status="healthy", observed_at=clock,
                )
                reserved = await reserve_next_verifier_executions(
                    session, pool_id=target.logical_pool_id,
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=clock,
                )
                await session.commit()
            assert [item.verifier_retry for item in reserved] == [retry]
            verifiers.append(reserved[0])
            await _fail_verifier_natively(
                sessions, lease_id=reserved[0].id, at=clock, historical_regression=historical_regression,
            )
            clock += timedelta(minutes=6)
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial is not None
                handoff = trial.result["verifier_execution"]
                if retry < MAX_VERIFIER_RETRIES:
                    assert trial.state == "running"
                    assert handoff["state"] == "pending"
                    assert handoff["retries"] == retry + 1
                    assert handoff["parent_lease_id"] == str(parent.id)
                    # The failed verifier still holds capacity until its pod is deleted.
                    assert await reserve_next_verifier_executions(
                        session, pool_id=target.logical_pool_id,
                        image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=clock,
                    ) == []
                else:
                    assert trial.state == "failed"
                    assert trial.failure_reason == "verifier_unavailable"
                    assert handoff["state"] == "unavailable"
            await _delete_lease(sessions, lease_id=reserved[0].id, at=clock)

        assert len({item.job_name for item in verifiers}) == len(verifiers)
        assert len({item.request_id for item in verifiers}) == len(verifiers)
        async with sessions() as session:
            assert await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=clock,
            ) == []
            costs = (await session.execute(
                select(ExecutionCostReservation.verifier_retry).where(
                    ExecutionCostReservation.trial_id == trial_id,
                    ExecutionCostReservation.execution_role == "verifier",
                ).order_by(ExecutionCostReservation.verifier_retry)
            )).scalars().all()
            assert costs == list(range(MAX_VERIFIER_RETRIES + 1))
    finally:
        await engine.dispose()


async def test_verifier_retry_is_immutable_and_attempts_cannot_carry_one(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        async with sessions() as session:
            with pytest.raises(DBAPIError, match="verifier retry identity is immutable"):
                await session.execute(
                    text("UPDATE execution_leases SET verifier_retry = 1 WHERE id = :id"), {"id": lease.id},
                )
    finally:
        await engine.dispose()


async def test_scheduler_skips_cancelled_handoff_and_fails_stale_one(
    postgres_url: str, _stub_verifier_plan: list[ExecutionRuntimePlanV1],
) -> None:
    import loom_control_plane.service_execution_scheduler as scheduler

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        cancelled_trial, _, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session:
            trial = await session.get(Trial, cancelled_trial)
            assert trial is not None
            trial.cancellation_requested_at = now
            await session.commit()
        async with sessions() as session:
            assert await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=5),
            ) == []
        assert _stub_verifier_plan == []

        stale_trial, _, stale_target = await _separate_attempt_awaiting_verifier(sessions, now=now)

        async def blocked(*_args: Any, **_kwargs: Any) -> ServiceExecutionLease:
            raise ServiceExecutionConflict("target is not ready")

        original = scheduler.reserve_trial_execution
        scheduler.reserve_trial_execution = blocked  # type: ignore[assignment]
        try:
            async with sessions() as session:
                # Within the handoff window an unavailable target is a wait.
                assert await reserve_next_verifier_executions(
                    session, pool_id=stale_target.logical_pool_id,
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(minutes=5),
                ) == []
                await session.commit()
                trial = await session.get(Trial, stale_trial)
                assert trial is not None and trial.state == "running"
            async with sessions() as session:
                assert await reserve_next_verifier_executions(
                    session, pool_id=stale_target.logical_pool_id,
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(minutes=31),
                ) == []
                await session.commit()
        finally:
            scheduler.reserve_trial_execution = original  # type: ignore[assignment]
        async with sessions() as session:
            trial = await session.get(Trial, stale_trial)
            assert trial is not None
            assert trial.state == "failed"
            assert trial.result["verifier_execution"]["state"] == "unavailable"
    finally:
        await engine.dispose()


async def test_runtime_result_identity_is_fenced_and_durably_reported(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="finalize",
                now=now,
            )
            await session.commit()

        payload = _runtime_result_payload(lease, started_at=now)
        async with sessions() as session:
            event, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="result_reported",
                payload=payload,
                observed_at=now + timedelta(seconds=3),
            )
            await session.commit()
            assert duplicate is False
            assert event.payload_sha256 == canonical_digest(payload)

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="identity"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="result_reported",
                    payload={**payload, "candidate_sha": "9" * 40},
                    observed_at=now + timedelta(seconds=4),
                )
    finally:
        await engine.dispose()


async def test_deferred_outbox_constraint_rolls_back_trial_reservation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            await session.commit()

        async with sessions() as session:
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.execute(
                delete(ServiceExecutionCommand).where(ServiceExecutionCommand.lease_id == lease.id)
            )
            with pytest.raises(DBAPIError, match=r"lacks durable .* command"):
                await session.commit()
            await session.rollback()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            lease_count = await session.scalar(
                select(func.count(ServiceExecutionLease.id)).where(
                    ServiceExecutionLease.trial_id == trial_id
                )
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("queued", 0)
            assert lease_count == 0
    finally:
        await engine.dispose()


async def test_command_redelivery_and_acknowledgement_are_replay_safe(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            first = await claim_execution_commands(
                session,
                consumer_id="actuator-a",
                target_id=target.target_id,
                limit=1,
                lease_seconds=5,
                now=now,
            )
            await session.commit()
            assert len(first) == 1
            assert first[0].delivery_count == 1

        async with sessions() as session:
            second = await claim_execution_commands(
                session,
                consumer_id="actuator-b",
                target_id=target.target_id,
                limit=1,
                lease_seconds=5,
                now=now + timedelta(seconds=6),
            )
            assert len(second) == 1
            assert second[0].id == first[0].id
            assert second[0].delivery_count == 2
            command = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
                now=now + timedelta(seconds=7),
            )
            await session.commit()
            assert command.state == "acknowledged"

        async with sessions() as session:
            replay = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
            )
            assert replay.id == second[0].id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await acknowledge_execution_command(
                    session,
                    command_id=second[0].id,
                    consumer_id="actuator-b",
                    acknowledgement={"provider_action": "different"},
                )
    finally:
        await engine.dispose()


async def test_events_are_replay_safe_and_lower_ordinals_do_not_regress_projection(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=1),
            )
            started, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert not duplicate
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            event_count = await session.scalar(
                select(func.count(ServiceExecutionEvent.id)).where(
                    ServiceExecutionEvent.lease_id == lease.id
                )
            )
            assert persisted is not None
            assert (persisted.observed_state, persisted.last_event_ordinal) == ("running", 2)
            assert event_count == 2
            replay, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert duplicate
            assert replay.id == started.id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="started",
                    payload={"runtime": "changed"},
                    observed_at=now + timedelta(seconds=2),
                )
    finally:
        await engine.dispose()


async def test_revocation_fences_old_generation_and_database_rejects_generation_skip(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            cancel = await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
            assert cancel.generation == 2

        async with sessions() as session:
            for surface in (
                "gateway",
                "artifact",
                "trajectory",
                "usage",
                "heartbeat",
                "result",
            ):
                with pytest.raises(ServiceExecutionFenceError):
                    await verify_trial_execution_fence(
                        session,
                        trial_id=trial_id,
                        lease_id=lease.id,
                        generation=1,
                        surface=surface,
                    )
            with pytest.raises(ServiceExecutionFenceError):
                await verify_trial_execution_fence(
                    session,
                    trial_id=trial_id,
                    lease_id=lease.id,
                    generation=2,
                    surface="heartbeat",
                )
            terminal = await verify_trial_execution_fence(
                session,
                trial_id=trial_id,
                lease_id=lease.id,
                generation=2,
                surface="cancelled",
                allow_terminal_event=True,
            )
            assert terminal is not None

            with pytest.raises(DBAPIError, match="advance monotonically by one"):
                await session.execute(
                    update(ServiceExecutionLease)
                    .where(ServiceExecutionLease.id == lease.id)
                    .values(generation=4)
                )
            await session.rollback()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.generation == 2
            assert persisted.revoked_at is not None
            assert persisted.cleanup_requested_at == now + timedelta(seconds=1)
            assert persisted.cleanup_deadline_at == now + timedelta(minutes=5, seconds=1)
    finally:
        await engine.dispose()


async def test_trial_cancellation_persists_service_execution_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            command = await request_trial_execution_cancellation(
                session,
                trial_id=trial_id,
                now=now + timedelta(seconds=1),
            )
            await session.commit()
            assert command is not None
            assert command.command_type == "cancel"
            assert command.generation == 2

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "cancel"
            assert persisted.generation == 2
            assert persisted.revoked_at == now + timedelta(seconds=1)
            assert persisted.cleanup_state == "pending"
            assert (
                await request_trial_execution_cancellation(
                    session,
                    trial_id=trial_id,
                    now=now + timedelta(seconds=2),
                )
                is None
            )
            await session.commit()
    finally:
        await engine.dispose()


async def test_retry_creates_a_new_attempt_and_finalization_is_idempotent(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            first = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=first.id,
                expected_generation=1,
                desired_state="retry",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="cleanup is not complete"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now + timedelta(seconds=2),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=2,
                ordinal=1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            second = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now + timedelta(seconds=3),
            )
            await session.commit()
            assert (second.attempt, second.generation) == (2, 1)
            assert second.id != first.id
            # Current task counts use the latest attempt, while cleanup keeps both.
            from loom_service.routes.batches import _batch_service_execution_summary
            from loom_service.routes.monitor import _service_execution_activity

            trial = await session.get(Trial, trial_id)
            summary = await _batch_service_execution_summary(session, [trial_id])
            activity = await _service_execution_activity(
                session, target_team=trial.team_id, filters={"task_id": trial.task_id},
            )
            assert summary["lease_count"] == 1
            assert activity["lease_count"] == 1
            assert sum(activity["lifecycle_stages"].values()) == 1
            assert sum(activity["source_cleanup_states"].values()) == 2

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=3),
            )
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="finalize",
                now=now + timedelta(seconds=4),
            )
            await session.commit()

        final_payload = {"trial_state": "succeeded", "result": {"reward": 1.0}}
        async with sessions() as session:
            event, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert not duplicate
            await session.commit()

        async with sessions() as session:
            replay, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert duplicate
            assert replay.id == event.id
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            assert trial.state == "succeeded"
            assert trial.result == {"reward": 1.0}
            assert trial.finished_at == now + timedelta(seconds=5)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("cached_trial", [False, True])
async def test_reservation_cannot_reopen_cancelled_trial(
    postgres_url: str,
    cached_trial: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            first = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=first.id,
                expected_generation=1,
                desired_state="retry",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=2,
                ordinal=1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=now,
            )
            await session.commit()
        async with sessions() as reserve_session:
            cached = await reserve_session.get(Trial, trial_id) if cached_trial else None
            cancelled = await cancel_trial_under_authority(
                session_factory=sessions,
                trial_id=trial_id,
                team_id=None,
            )
            assert cancelled is not None and cancelled["state"] == "cancelled"
            if cached is not None:
                assert cached.state == "queued"
            with pytest.raises(ServiceExecutionConflict, match="trial is not reservable"):
                await _reserve(reserve_session, trial_id=trial_id, target=target, now=now)
            await reserve_session.commit()
            trial = await reserve_session.get(Trial, trial_id, populate_existing=True)
            assert trial is not None and trial.state == "cancelled"
            assert trial.attempt_count == 1
            assert (
                await reserve_session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionLease)
                    .where(
                        ServiceExecutionLease.trial_id == trial_id,
                    )
                )
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("cached_trial", [False, True])
async def test_retry_cannot_reopen_cancelled_trial_after_timeout_reclaim(
    postgres_url: str,
    cached_trial: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC) - timedelta(minutes=10)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="timeout",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
        async with sessions() as retry_session:
            cached = await retry_session.get(Trial, trial_id) if cached_trial else None
            async with sessions() as session:
                assert (
                    await reclaim_expired_workers(
                        session,
                        expiry_sec=60,
                        claimed_without_start_expiry_sec=60,
                    )
                    == 0
                )
                # Native leases are excluded from legacy worker reclaim now.
                # Retain the regression for the historical queued state that
                # existed before that fence, as cancellation replay must repair it.
                historical = await session.get(Trial, trial_id)
                assert historical is not None
                historical.state = "queued"
                historical.failure_reason = "worker_lost_claim"
                await session.commit()
            cancelled = await cancel_trial_under_authority(
                session_factory=sessions,
                trial_id=trial_id,
                team_id=None,
            )
            assert cancelled is not None and cancelled["state"] == "cancelled"
            if cached is not None:
                assert cached.state == "claimed"
            before = await retry_session.get(ServiceExecutionLease, lease.id)
            assert before is not None and before.desired_state == "timeout"
            snapshot = (before.generation, before.revoked_at, before.cleanup_requested_at)
            with pytest.raises(ServiceExecutionConflict, match="terminal trial"):
                await enqueue_execution_transition(
                    retry_session,
                    lease_id=lease.id,
                    expected_generation=2,
                    desired_state="retry",
                    now=now + timedelta(seconds=2),
                )
            # A rejected command must not leave partial intent for a caller to commit.
            await retry_session.commit()
            await retry_session.refresh(before)
            assert before.desired_state == "timeout"
            assert (before.generation, before.revoked_at, before.cleanup_requested_at) == snapshot
            trial = await retry_session.get(Trial, trial_id, populate_existing=True)
            assert trial is not None and trial.state == "cancelled"
            assert trial.finished_at is not None
            assert (
                await retry_session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionCommand)
                    .where(
                        ServiceExecutionCommand.lease_id == lease.id,
                    )
                )
                == 2
            )  # create and timeout only
    finally:
        await engine.dispose()


@pytest.mark.parametrize("terminal_state", ["succeeded", "failed", "cancelled"])
@pytest.mark.parametrize("cached_trial", [False, True])
async def test_finalized_event_cannot_reopen_terminal_trial(
    postgres_url: str,
    terminal_state: str,
    cached_trial: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            for desired_state in ("start", "finalize"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state=desired_state,
                    now=now,
                )
            await session.commit()
        async with sessions() as event_session:
            cached = await event_session.get(Trial, trial_id) if cached_trial else None
            final_payload = {"trial_state": terminal_state, "result": {"reward": 0.0}}
            async with sessions() as session:
                event, duplicate = await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=1,
                    event_kind="finalized",
                    payload=final_payload,
                    observed_at=now,
                )
                assert not duplicate
                await session.commit()
            if cached is not None:
                assert cached.state == "claimed"
            with pytest.raises(ServiceExecutionConflict, match="terminal trial"):
                await record_execution_event(
                    event_session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="finalized",
                    payload={"trial_state": "materializing", "result": {"reward": 1.0}},
                    observed_at=now + timedelta(seconds=1),
                )
            await event_session.commit()
            trial = await event_session.get(Trial, trial_id, populate_existing=True)
            assert trial is not None and trial.state == terminal_state
            assert trial.finished_at == now
            assert trial.result == {"reward": 0.0}
            stored_lease = await event_session.get(
                ServiceExecutionLease,
                lease.id,
                populate_existing=True,
            )
            assert stored_lease is not None
            assert stored_lease.last_event_ordinal == 1
            assert stored_lease.finalized_at == now
            assert (
                await event_session.scalar(
                    select(func.count())
                    .select_from(ServiceExecutionEvent)
                    .where(
                        ServiceExecutionEvent.lease_id == lease.id,
                    )
                )
                == 1
            )
            replay, duplicate = await record_execution_event(
                event_session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now,
            )
            assert duplicate and replay.id == event.id
    finally:
        await engine.dispose()


@pytest.mark.parametrize("surface", ["retry", "failed"])
@pytest.mark.parametrize("cached_lease", [False, True])
async def test_service_execution_reloads_lease_before_projection(
    postgres_url: str,
    surface: str,
    cached_lease: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        async with sessions() as stale_session:
            cached = (
                await stale_session.get(ServiceExecutionLease, lease.id) if cached_lease else None
            )
            new_state = "cancel"
            async with sessions() as session:
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state=new_state,
                    now=now,
                )
                await session.commit()
            if cached is not None:
                assert cached.generation == 1
            with pytest.raises(ServiceExecutionFenceError, match="generation"):
                if surface == "retry":
                    await enqueue_execution_transition(
                        stale_session,
                        lease_id=lease.id,
                        expected_generation=1,
                        desired_state="retry",
                        now=now,
                    )
                else:
                    await record_execution_event(
                        stale_session,
                        lease_id=lease.id,
                        generation=1,
                        ordinal=1,
                        event_kind="failed",
                        payload={"error_code": "late_failure"},
                        observed_at=now,
                    )
            await stale_session.commit()
            current = await stale_session.get(
                ServiceExecutionLease, lease.id, populate_existing=True
            )
            assert current is not None and current.generation == 2
            assert current.desired_state == new_state
            trial = await stale_session.get(Trial, trial_id, populate_existing=True)
            assert trial is not None and trial.state == "claimed"
    finally:
        await engine.dispose()


async def test_finalized_projection_preserves_trial_key_share_compatibility(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            for desired_state in ("start", "finalize"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state=desired_state,
                    now=now,
                )
            await session.commit()
        async with sessions() as holder, sessions() as session:
            await holder.execute(
                select(Trial).where(Trial.id == trial_id).with_for_update(read=True, key_share=True)
            )
            # A reference insert holds KEY SHARE until commit. Finalization must
            # complete while that lock is still held; bound failures at the DB.
            await session.execute(text("SET LOCAL lock_timeout = '500ms'"))
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload={"trial_state": "materializing", "result": {"reward": 1.0}},
                observed_at=now,
            )
            await session.commit()
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "materializing"
    finally:
        await engine.dispose()


async def test_gateway_dispatch_is_rejected_immediately_after_generation_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            ctx = AuthContext(
                token_hash=b"",
                type="step_session",
                scopes=["llm:call"],
                team_id=lease.team_id,
                expires_at=now + timedelta(minutes=10),
                trial_id=trial_id,
                step_id="agent",
                provider_connection_id=None,
                provider_connection_id_bound=True,
                step_jwt_id=uuid4(),
                service_execution_lease_id=lease.id,
                service_execution_generation=1,
                service_execution_role="attempt",
                service_execution_runtime_contract_sha256=lease.runtime_contract_sha256,
                service_execution_candidate_sha="1" * 40,
                service_execution_task_revision_sha256="sha256:" + "2" * 64,
                service_execution_command_identity_sha256="sha256:" + "3" * 64,
            )
            # The broker can mint the Pod's step token as soon as the Actuator
            # has observed its identity, while the lease is still `creating`.
            # Dispatch must accept that same live authority during cold start.
            lease.observed_state = "creating"
            await session.flush()
            await authorize_trial_execution_dispatch(session, ctx)
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"provider_action": "created"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            await authorize_trial_execution_dispatch(session, ctx)
            for changed in (
                replace(ctx, provider_connection_id_bound=False),
                replace(ctx, service_execution_candidate_sha="9" * 40),
                replace(
                    ctx,
                    service_execution_runtime_contract_sha256="sha256:" + "9" * 64,
                ),
                replace(ctx, service_execution_command_identity_sha256="sha256:" + "9" * 64),
            ):
                with pytest.raises(HTTPException, match="service execution dispatch forbidden"):
                    await authorize_trial_execution_dispatch(session, changed)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                HTTPException,
                match="service execution dispatch forbidden",
            ) as exc_info:
                await authorize_trial_execution_dispatch(session, ctx)
            assert exc_info.value.status_code == 403
    finally:
        await engine.dispose()


async def test_service_step_token_freezes_identity_and_persists_audit(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane.routes import step_tokens

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    signing_key = "s" * 64
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        auth = AuthContext(
            token_hash=b"w" * 32,
            type="worker",
            scopes=["worker:report"],
            team_id=None,
            expires_at=None,
        )

        async def verified_auth(*args, **kwargs):  # type: ignore[no-untyped-def]
            del args, kwargs
            return auth

        monkeypatch.setattr(step_tokens, "verify_bearer_token", verified_auth)
        app = FastAPI()
        app.state.session_factory = sessions
        app.state.settings = SimpleNamespace(step_jwt_signing_key=SecretStr(signing_key))
        request = Request({"type": "http", "app": app, "headers": []})
        response = await step_tokens.issue_step_token(
            request,
            step_tokens._IssueStepTokenRequest(
                team_id=lease.team_id,
                trial_id=trial_id,
                step_id="agent",
                ttl_sec=600,
            ),
            principal=auth,
            execution_lease_id=lease.id,
            execution_generation=lease.generation,
        )

        ctx = verify_step_jwt(response.token, signing_key=signing_key)
        assert ctx.step_jwt_id is not None
        assert ctx.provider_connection_id_bound is True
        assert ctx.service_execution_lease_id == lease.id
        assert ctx.service_execution_generation == lease.generation
        assert ctx.service_execution_role == "attempt"
        assert ctx.service_execution_runtime_contract_sha256 == lease.runtime_contract_sha256
        async with sessions() as session:
            audit = (
                await session.execute(
                    select(AdminAuditEvent).where(
                        AdminAuditEvent.action == "service_execution.step_token.minted",
                        AdminAuditEvent.target_id == str(lease.id),
                    )
                )
            ).scalar_one()
            assert audit.event_metadata["step_jwt_id"] == str(ctx.step_jwt_id)
            assert audit.event_metadata["generation"] == lease.generation
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "terminal_state",
    [NormalizedJobState.SUCCEEDED, NormalizedJobState.FAILED],
)
@pytest.mark.parametrize("extra_artifacts", [
    0,
    # This case performs hundreds of real prepare/upload/complete transactions.
    # Keep the normal case's 60-second cap; allow the full inventory on CI CPUs.
    pytest.param(512, marks=pytest.mark.timeout(300)),
])
async def test_observed_pod_broker_commits_semantic_runtime_output(
    postgres_url: str,
    terminal_state: NormalizedJobState,
    extra_artifacts: int,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    signing_key = "b" * 64
    pod_ip = "10.24.7.19"
    runtime_contract = _complete_output_contract(now=now)
    extra_outputs = tuple(
        RuntimeOutputDeclarationV1(
            source_path=f"artifacts/part-{index:04}.json",
            relative_path=f"artifacts/part-{index:04}.json",
            kind="task_artifact", required=False,
        )
        for index in range(extra_artifacts)
    )
    runtime_contract = runtime_contract.model_copy(update={
        "output_declarations": (*runtime_contract.output_declarations, *extra_outputs),
    })
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                runtime_contract=runtime_contract,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-uid-1",
                    "pod_uid": "pod-uid-1",
                    "pod_ip": pod_ip,
                    "resource_version": "7",
                },
                observed_at=now,
            )
            await session.commit()

        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        async with sessions() as session:
            authorized = await authorize_service_execution_peer(
                session,
                peer_ip=pod_ip,
                identity=identity,
                purpose="token",
            )
            token, _expires_at, _token_id = await mint_service_execution_peer_token(
                session,
                lease=authorized,
                ttl_seconds=480,
                signing_key=signing_key,
                now=now,
            )
            await session.commit()
            session.expunge(authorized)
        token_context = verify_step_jwt(token, signing_key=signing_key)
        assert token_context.service_execution_lease_id == lease.id
        assert token_context.service_execution_generation == 1

        bundle_payloads = {
            "artifacts/answer.txt": b"42\n",
            "trajectory/events.jsonl": b'{"turn":0}\n',
            "accounting/usage.json": b'{"call_count":1}\n',
            "verifier/output.json": b'{"rewards":{"passed":1}}\n',
        }
        bundle_payloads.update({item.relative_path: b"{}" for item in extra_outputs})
        result_document = _runtime_result_payload(authorized, started_at=now)
        result_document.update(
            status="runtime_error",
            partial_evidence=True,
            phases=[],
            outputs=[
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured",
                    "size_bytes": len(bundle_payloads[declaration.relative_path]),
                    "sha256": digest_bytes(bundle_payloads[declaration.relative_path]),
                }
                for declaration in runtime_contract.output_declarations
            ],
            verifier_rewards={"passed": 1.0},
        )
        result_payload = canonical_document(result_document)
        if extra_artifacts:
            assert len(result_payload) > 65_536
        store = FakeObjectStore()
        repository = SqlArtifactCommitRepository(
            session_factory=sessions,
            store=store,
            bucket="artifacts",
        )
        route = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=store,
                bucket="artifacts",
                repository=repository,
            ),
            session_factory=sessions,
        )
        prepare = ServiceExecutionOutputPrepareV1(
            schema_version="loom.service-execution-output-prepare.v1",
            request_id=uuid4(),
            **identity.model_dump(),
            files=tuple(
                ServiceExecutionOutputFileV1(
                    relative_path=path,
                    media_type=(
                        "application/json"
                        if path.endswith(".json")
                        else "application/x-ndjson"
                        if path.endswith(".jsonl")
                        else "text/plain; charset=utf-8"
                    ),
                    size_bytes=len(payload),
                    sha256=digest_bytes(payload),
                )
                for path, payload in {
                    "result.json": result_payload,
                    **dict(sorted(bundle_payloads.items())),
                }.items()
            ),
        )
        grant = await route.prepare(lease=authorized, request=prepare)
        upload_session_id = UUID(grant["upload_session_id"])
        upload_token = str(grant["upload_token"])

        ordered_payloads = [result_payload, *[body for _, body in sorted(bundle_payloads.items())]]
        for file_index, payload in enumerate(ordered_payloads):

            async def body(payload: bytes = payload):  # type: ignore[no-untyped-def]
                yield payload

            receipt = PartReceiptV1.model_validate(
                await route.put_part(
                    lease=authorized,
                    session_id=upload_session_id,
                    file_index=file_index,
                    part_number=1,
                    content_length=len(payload),
                    content_sha256=digest_bytes(payload),
                    upload_token=upload_token,
                    body=body(),
                )
            )
            await route.complete_file(
                lease=authorized,
                session_id=upload_session_id,
                file_index=file_index,
                ordered_parts=(receipt,),
                upload_token=upload_token,
            )
        committed = await route.commit(
            lease=authorized,
            session_id=upload_session_id,
            upload_token=upload_token,
        )
        assert committed["state"] == "committed"
        assert committed["manifest_sha256"].startswith("sha256:")
        assert (
            await route.commit(
                lease=authorized,
                session_id=upload_session_id,
                upload_token=upload_token,
            )
            == committed
        )

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            artifact = (
                await session.execute(
                    select(Artifact).where(
                        Artifact.control_producer_kind == "service_execution",
                        Artifact.control_producer_id == lease.id,
                    )
                )
            ).scalar_one()
            assert persisted is not None
            assert persisted.output_commit_state == "committed"
            assert persisted.output_upload_session_id == upload_session_id
            assert persisted.desired_state == "finalize"
            assert artifact.content_hash == digest_bytes(result_payload)
            assert artifact.name == "trial_bundle"
            assert artifact.artifact_type == "loom.trial-artifact-bundle.v1"
            assert artifact.file_count == 5 + extra_artifacts
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            assert trial.trajectory_index is not None
            assert trial.trajectory_index["artifact_bundle_id"] == str(artifact.id)
            assert trial.trajectory_index["verifier_rewards"] == {"passed": 1.0}
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionEvent.id)).where(
                        ServiceExecutionEvent.lease_id == lease.id,
                        ServiceExecutionEvent.event_kind == "result_reported",
                    )
                )
                == 1
            )

        # Hundreds of real upload transactions can outlast the original clock.
        # Include the newly created finalize command in this actuator poll.
        now = datetime.now(UTC)
        terminal_observation = KubernetesJobObservation(
            namespace=target.namespace_name,
            job_name=lease.job_name,
            lease_id=str(lease.id),
            resource_generation=lease.resource_generation,
            target_id=target.target_id,
            execution_unit_key=str(lease.execution_unit_key),
            normalized_state=terminal_state,
            job_uid="job-uid-1",
            pod_uid="pod-uid-1",
            pod_ip=pod_ip,
            resource_version="8",
            terminated_at=now + timedelta(seconds=3),
            termination_summary=ExecutionTerminationSummaryV1(
                schema_version="loom.execution-termination-summary.v1",
                runtime_contract_sha256=authorized.runtime_contract_sha256,
                command_identity_sha256=result_document["command_identity_sha256"],
                execution_role="attempt",
                status="runtime_error",
                partial_evidence=True,
                phase_count=0,
                finished_at=now + timedelta(seconds=3),
                result_path="result.json",
                output_committed=True,
                output_upload_session_id=upload_session_id,
                output_manifest_sha256=committed["manifest_sha256"],
                output_marker_sha256=committed["committed_marker_sha256"],
            ),
        )
        kubernetes = _FakeKubernetesJobApi()
        kubernetes.jobs[lease.job_name] = terminal_observation
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
            ),
            controller_id="output-finalization-test",
        )
        completed = await actuator.run_commands_once(now=now + timedelta(seconds=4))
        async with sessions() as session:
            commands = (await session.scalars(select(ServiceExecutionCommand).where(
                ServiceExecutionCommand.lease_id == lease.id,
            ))).all()
            assert completed == 3, [
                (c.command_type, c.state, c.last_error_code, c.last_error_message) for c in commands
            ]

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert persisted is not None
            assert trial is not None
            assert persisted.desired_state == "delete_pending"
            assert persisted.observed_state == "finalized"
            assert trial.state == "failed"
            assert trial.failure_reason == "runtime_error"
            assert trial.result is not None
            assert trial.result["runtime_result"] == ExecutionRuntimeResultV1.model_validate(
                result_document,
            ).model_dump(mode="json")
            assert (
                await session.scalar(
                    select(func.count(ServiceExecutionCommand.id)).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "delete",
                    )
                )
                == 1
            )

        # A completed Job remains observable between durable finalization and
        # deletion. Replaying that Kubernetes state must not regress the lease
        # from ``finalized`` or erase its terminal timestamp.
        kubernetes.jobs[lease.job_name] = terminal_observation.model_copy(
            update={"resource_version": "9", "terminated_at": None}
        )
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=5)) == 0
        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.observed_state == "finalized"
            assert persisted.pod_terminated_at == now + timedelta(seconds=3)

        assert await actuator.run_commands_once(now=now + timedelta(seconds=5)) == 1
        assert kubernetes.delete_count == 1
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=6)) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "deleted"
            assert persisted.cleanup_state == "complete"

        async with sessions() as session:
            with pytest.raises(ServiceExecutionBrokerError, match="not_observed"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip="10.24.7.20",
                    identity=identity,
                    purpose="output",
                )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("source_task_id", [None, "nebius-acceptance/canonical-output"])
@pytest.mark.parametrize(
    ("rewards", "aggregate_reward"),
    [({"artifact_complete": 1.0}, 1.0), ({"passed": 0.0}, 0.0), ({"a": 0.0, "b": 1.0}, 0.5)],
)
async def test_materializer_commits_complete_bundle_after_execution_cleanup(
    postgres_url: str,
    source_task_id: str | None,
    rewards: dict[str, float] | None,
    aggregate_reward: float | None,
    runtime_status: str = "succeeded",
    late_cancellation: str | None = None,
    cancel_before_commit: bool = False,
    extra_artifacts: int = 0,
) -> None:
    class FailOnceSourceStore(FakeObjectStore):
        fail_next_delete: bool = True

        async def delete_object(self, *, bucket, key):  # type: ignore[no-untyped-def]
            if self.fail_next_delete:
                self.fail_next_delete = False
                raise OSError("temporary source object-store outage")
            await super().delete_object(bucket=bucket, key=key)

    class FailOnceCanonicalStore(FakeObjectStore):
        fail_next_copy: bool = True

        async def put_object_stream(self, *, bucket, key, body):  # type: ignore[no-untyped-def]
            if self.fail_next_copy and key.startswith("trials/"):
                self.fail_next_copy = False
                raise OSError("temporary canonical object-store outage")
            return await super().put_object_stream(bucket=bucket, key=key, body=body)

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    runtime_contract = _complete_output_contract(now=now)
    extra_outputs = tuple(RuntimeOutputDeclarationV1(
        source_path=f"artifacts/extra-{index:04}.txt",
        relative_path=f"artifacts/extra-{index:04}.txt",
        kind="task_artifact", required=False,
    ) for index in range(extra_artifacts))
    runtime_contract = runtime_contract.model_copy(update={
        "output_declarations": (*runtime_contract.output_declarations, *extra_outputs),
    })
    store = FailOnceSourceStore()
    canonical_store = FailOnceCanonicalStore()
    try:
        async with sessions() as session:
            catalog_task_id = (
                f"ts/{uuid4()}/nebius-canonical-acceptance/tasks/nebius-acceptance_canonical-output"
                if source_task_id is not None
                else None
            )
            trial_id, target = await _seed_ready_trial(session, now=now, task_id=catalog_task_id)
            trial = await session.get(Trial, trial_id)
            task = None if trial is None else await session.get(Task, trial.task_id)
            assert trial is not None and task is not None
            task.config = {
                "schema_version": "1",
                "task": {
                    "id": source_task_id or task.id,
                    "name": "Canonical materialization",
                },
                "environment": {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "docker_image": runtime_contract.task_image_ref,
                    "network_policies_supported": ["gateway-only"],
                    "baseline_network_policy": {"kind": "gateway-only"},
                },
                "agent": {"name": "direct-completion", "version": "1.0"},
                "verifier": {"name": "script", "env_mode": "shared"},
                "steps": [{"name": "main", "artifacts": ["answer.txt"]}],
            }
            trial.config = {
                "schema_version": "1",
                "agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "gpt-5"},
            }
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                runtime_contract=runtime_contract,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-materialize",
                    "pod_uid": "pod-materialize",
                    "pod_ip": "10.24.7.21",
                    "resource_version": "1",
                },
                observed_at=now,
            )
            await session.commit()
            session.expunge(lease)

        trace = (
            json.dumps(
                {
                    "schema_version": "loom.service-execution-llm-call.v1",
                    "turn": 0,
                    "started_at": now.isoformat(),
                    "finished_at": (now + timedelta(seconds=1)).isoformat(),
                    "model": "openai/gpt-5",
                    "request": {
                        "messages": [{"role": "user", "content": "answer"}],
                        "request_params": {},
                    },
                    "response": {"role": "assistant", "content": "42"},
                    "usage": {
                        "rate_card_hash": "rate-v1",
                        "gateway_request_id": "gateway-1",
                        "finish_reason": "stop",
                        "input_tokens": 1,
                        "cached_input_tokens": 0,
                        "cache_write_tokens": 0,
                        "output_tokens": 1,
                        "thinking_tokens": 0,
                        "provider_extras": {},
                        "cost_usd": 0.01,
                        "duration_sec": 1.0,
                        "streamed": False,
                        "time_to_first_token_sec": None,
                        "attempt": 1,
                    },
                },
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        trace_usage = json.loads(trace)["usage"]
        bundle_payloads = {
            "01-agent.stdout": b"42\n",
            "01-agent.stderr": b"",
            "02-verifier.stdout": b"passed\n",
            "02-verifier.stderr": b"",
            "artifacts/answer.txt": b"42\n",
            "trajectory/events.jsonl": trace,
            "accounting/usage.json": canonical_document(
                {
                    "schema_version": "loom.service-execution-usage.v1",
                    "model": "openai/gpt-5",
                    "call_count": 1,
                    "totals": {
                        "input_tokens": 1,
                        "cached_input_tokens": 0,
                        "cache_write_tokens": 0,
                        "output_tokens": 1,
                        "thinking_tokens": 0,
                        "cost_usd": 0.01,
                        "duration_sec": 1.0,
                    },
                    "calls": [trace_usage],
                }
            ),
            "verifier/output.json": canonical_document({"rewards": rewards}),
        }
        bundle_payloads.update({item.relative_path: b"extra\n" for item in extra_outputs})
        if cancel_before_commit:
            bundle_payloads.pop("verifier/output.json")
        result_document = _runtime_result_payload(lease, started_at=now)
        result_document["status"] = runtime_status
        result_document["partial_evidence"] = runtime_status != "succeeded"
        if runtime_status == "timed_out":
            phases = result_document["phases"]
            assert isinstance(phases, list)
            phases[0].update(exit_code=124, timed_out=True)
        result_document.update(
            outputs=[
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured" if declaration.relative_path in bundle_payloads else "missing",
                    "size_bytes": len(bundle_payloads[declaration.relative_path]) if declaration.relative_path in bundle_payloads else None,
                    "sha256": digest_bytes(bundle_payloads[declaration.relative_path]) if declaration.relative_path in bundle_payloads else None,
                }
                for declaration in runtime_contract.output_declarations
            ],
            verifier_rewards=rewards,
        )
        result_payload = canonical_document(result_document)
        repository = SqlArtifactCommitRepository(
            session_factory=sessions,
            store=store,
            bucket="artifacts",
        )
        route = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=store,
                bucket="artifacts",
                repository=repository,
            ),
            session_factory=sessions,
        )
        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        payloads = {"result.json": result_payload, **dict(sorted(bundle_payloads.items()))}
        prepare = ServiceExecutionOutputPrepareV1(
            schema_version="loom.service-execution-output-prepare.v1",
            request_id=uuid4(),
            **identity.model_dump(),
            files=tuple(
                ServiceExecutionOutputFileV1(
                    relative_path=path,
                    media_type=(
                        "application/json"
                        if path.endswith(".json")
                        else "application/x-ndjson"
                        if path.endswith(".jsonl")
                        else "text/plain; charset=utf-8"
                    ),
                    size_bytes=len(payload),
                    sha256=digest_bytes(payload),
                )
                for path, payload in payloads.items()
            ),
        )
        grant = await route.prepare(lease=lease, request=prepare)
        upload_session_id = UUID(grant["upload_session_id"])
        upload_token = str(grant["upload_token"])
        for file_index, payload in enumerate(payloads.values()):

            async def body(payload: bytes = payload):  # type: ignore[no-untyped-def]
                yield payload

            receipt = PartReceiptV1.model_validate(
                await route.put_part(
                    lease=lease,
                    session_id=upload_session_id,
                    file_index=file_index,
                    part_number=1,
                    content_length=len(payload),
                    content_sha256=digest_bytes(payload),
                    upload_token=upload_token,
                    body=body(),
                )
            )
            await route.complete_file(
                lease=lease,
                session_id=upload_session_id,
                file_index=file_index,
                ordered_parts=(receipt,),
                upload_token=upload_token,
            )
        if cancel_before_commit:
            async with sessions() as session:
                assert await request_trial_execution_cancellation(session, trial_id=trial_id, now=now)
                assert await request_trial_execution_cancellation(session, trial_id=trial_id, now=now) is None
                with pytest.raises(ServiceExecutionBrokerError, match="fenced"):
                    await authorize_service_execution_peer(session, peer_ip="10.24.7.21", identity=identity)
                await authorize_service_execution_peer(session, peer_ip="10.24.7.21", identity=identity, purpose="output")
                await session.commit()
            cancel_kube = _FakeKubernetesJobApi()
            cancel_kube.jobs[lease.job_name] = KubernetesJobObservation(
                namespace=target.namespace_name, job_name=lease.job_name, lease_id=str(lease.id),
                resource_generation=1, target_id=target.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                normalized_state=NormalizedJobState.RUNNING, job_uid="job-materialize",
                pod_uid="pod-materialize", pod_ip="10.24.7.21", resource_version="2",
            )
            cancel_actuator = ExecutionActuator(sessions=sessions, kubernetes=cancel_kube,
                target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
                controller_id="cancel-output-race")
            await cancel_actuator.run_commands_once(now=now)
            assert cancel_kube.delete_count == 1 and not cancel_kube.jobs
            await cancel_actuator.reconcile_full_once(now=now + timedelta(seconds=1))
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current is not None and current.output_commit_state == "uploading"
                assert current.deleted_at is None and current.cleanup_state == "pending"
        await route.commit(
            lease=lease,
            session_id=upload_session_id,
            upload_token=upload_token,
        )

        if cancel_before_commit:
            await cancel_actuator.reconcile_full_once(now=now + timedelta(seconds=4))
        elif late_cancellation is None:
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
                assert current is not None
                current.observed_state = "finalizing"
                assert await finalize_committed_service_execution(
                    session,
                    lease_id=current.id,
                    observed_at=now + timedelta(seconds=4),
                )
                await session.commit()
        else:
            # The result commits before cancellation revokes generation 1.
            # Archival must preserve it when the deleted Job is observed.
            async with sessions() as session:
                command = await request_trial_execution_cancellation(
                    session, trial_id=trial_id, now=now + timedelta(seconds=3)
                )
                assert command is not None and command.generation == 2
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current is not None and current.finalized_at is None
                if late_cancellation == "already_deleted":
                    # Reproduce the durable state written by the older controller.
                    current.desired_state = current.observed_state = "deleted"
                    current.cleanup_state = "complete"
                    current.deleted_at = now + timedelta(seconds=4)
                elif late_cancellation == "delete_event":
                    await record_execution_event(
                        session, lease_id=current.id, generation=2,
                        ordinal=current.last_event_ordinal + 1,
                        event_kind="deleted", payload={},
                        observed_at=now + timedelta(seconds=4),
                    )
                await session.commit()
            if late_cancellation == "before_deleted":
                pending_materializer = ServiceExecutionMaterializer(
                    session_factory=sessions, source_store=store, source_bucket="artifacts",
                    canonical_store=canonical_store, artifacts_bucket="artifacts",
                    trajectories_bucket="trajectories",
                )
                assert await pending_materializer.claim_one() is None
            actuator = ExecutionActuator(
                sessions=sessions,
                kubernetes=_FakeKubernetesJobApi(),
                target=ExecutionTargetRuntime(
                    target_id=target.target_id, namespace=target.namespace_name
                ),
                controller_id="late-cancellation-recovery",
            )
            await actuator.reconcile_full_once(now=now + timedelta(seconds=5))
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current is not None
                assert (current.finalized_at is None) == (late_cancellation == "already_deleted")
                assert current.desired_state == current.observed_state == "deleted"
                assert current.cleanup_state == "complete"
                assert current.generation == 2
            assert await actuator.reconcile_full_once(now=now + timedelta(seconds=6)) == 0
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            if late_cancellation != "already_deleted":
                assert trial.state == ("materializing" if runtime_status == "succeeded" else "cancelled" if cancel_before_commit else "failed")
                assert trial.result is not None
                assert trial.result["aggregate_reward"] == aggregate_reward
                assert trial.result["reward"] == rewards
                assert trial.result["runtime_result"] == ExecutionRuntimeResultV1.model_validate(
                    result_document
                ).model_dump(mode="json")
                projected_result = dict(trial.result)
            assert not await finalize_committed_service_execution(
                session, lease_id=current.id, observed_at=now + timedelta(seconds=5)
            )
            if late_cancellation != "already_deleted":
                assert trial.result == projected_result
            current.desired_state = "deleted"
            current.observed_state = "deleted"
            current.cleanup_state = "complete"
            if current.deleted_at is None:
                current.deleted_at = now + timedelta(seconds=5)
            await session.commit()

        materializer = ServiceExecutionMaterializer(
            session_factory=sessions,
            source_store=store,
            source_bucket="artifacts",
            canonical_store=canonical_store,
            artifacts_bucket="artifacts",
            trajectories_bucket="trajectories",
            source_retention_seconds=0,
        )
        if late_cancellation == "already_deleted":
            assert not await materializer.run_once(lease_id=uuid4())
            assert await materializer.run_once(lease_id=lease.id)
        else:
            assert await materializer.run_once()
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            assert current.materialization_state == "pending"
            assert current.materialization_error_code == "transient_materialization_error"
            assert trial.state == ("materializing" if runtime_status == "succeeded" else "cancelled" if cancel_before_commit else "failed")
            assert trial.result is not None
            assert trial.result["runtime_result"] == ExecutionRuntimeResultV1.model_validate(
                result_document
            ).model_dump(mode="json")
            projected_result = dict(trial.result)
            if late_cancellation == "already_deleted":
                assert current.finalized_at is None
                assert current.deleted_at == now + timedelta(seconds=4)
                assert current.desired_state == current.observed_state == "deleted"

            current.materialization_next_attempt_at = now
            await session.commit()
        materializer = ServiceExecutionMaterializer(
            session_factory=sessions,
            source_store=store,
            source_bucket="artifacts",
            canonical_store=canonical_store,
            artifacts_bucket="artifacts",
            trajectories_bucket="trajectories",
            source_retention_seconds=0,
        )
        assert await materializer.run_once()
        assert not await materializer.run_once()
        assert await materializer.cleanup_source_once()
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            assert current is not None
            assert current.source_cleanup_state == "retained"
            assert current.source_cleanup_attempts == 1
            assert current.source_cleanup_error_message == ("temporary source object-store outage")
            current.source_retain_until = now
            await session.commit()
        assert await materializer.cleanup_source_once()
        assert not await materializer.cleanup_source_once()

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            artifact = (
                await session.execute(
                    select(Artifact).where(Artifact.control_producer_id == lease.id)
                )
            ).scalar_one()
            events = list(
                (
                    await session.execute(
                        select(TrialEvent)
                        .where(TrialEvent.trial_id == trial_id)
                        .order_by(TrialEvent.seq)
                    )
                ).scalars()
            )
            lifecycle_objects = list(
                (
                    await session.execute(
                        select(DataLifecycleObject).where(
                            DataLifecycleObject.authority_id == artifact.lifecycle_authority_id
                        )
                    )
                ).scalars()
            )
            assert current is not None and trial is not None
            assert current.deleted_at is not None
            assert current.materialization_state == "committed", (
                current.materialization_error_code,
                current.materialization_error_message,
            )
            assert current.canonical_trajectory_sha256 is not None
            assert current.source_cleanup_state == "complete"
            assert current.source_cleanup_attempts == 2
            assert trial.state == ("succeeded" if runtime_status == "succeeded" else "cancelled" if cancel_before_commit else "failed")
            assert trial.failure_reason == ("cancelled" if cancel_before_commit else None if runtime_status == "succeeded" else runtime_status)
            assert trial.result == projected_result
            if extra_artifacts:
                # References remain resolvable after canonical storage replaced
                # the source registry and source-spool objects were deleted.
                stored_events = (await session.scalars(select(ServiceExecutionEvent).where(
                    ServiceExecutionEvent.lease_id == lease.id,
                    ServiceExecutionEvent.event_kind.in_(("result_reported", "finalized")),
                ))).all()
                assert stored_events
                for stored_event in stored_events:
                    assert stored_event.payload_json["schema_version"] == (
                        "loom.execution-event-payload-reference.v1"
                    )
                    full = json.loads(artifact.artifact_metadata["execution_event_payloads"][
                        stored_event.payload_json["payload_key"]
                    ])
                    replay, duplicate = await record_execution_event(
                        session, lease_id=lease.id, generation=stored_event.generation,
                        ordinal=stored_event.ordinal, event_kind=stored_event.event_kind,
                        payload=full, observed_at=now,
                    )
                    assert replay.id == stored_event.id and duplicate
                assert trial.result is not None
                assert trial.result["runtime_result"] == ExecutionRuntimeResultV1.model_validate(
                    result_document,
                ).model_dump(mode="json")
            from loom_service.routes.batches import _rollup_from_trials

            assert _rollup_from_trials([trial]) == aggregate_reward
            assert trial.trajectory_index is not None
            assert artifact.lifecycle_authority_id is not None
            assert len(lifecycle_objects) == len(artifact.storage["files"]) + 5
            assert all(event.lifecycle_authority_id is not None for event in events)
            assert trial.trajectory_index["atif_schema_version"] == "1.7"
            assert events[0].payload["task_id"] == trial.task_id
            persisted_task = await session.get(Task, trial.task_id)
            assert persisted_task is not None
            assert persisted_task.config["task"]["id"] == (source_task_id or trial.task_id)
            assert [event.kind for event in events] == [
                "trial_start",
                "step_start",
                "llm_call",
                "step_end",
                *([] if cancel_before_commit else ["verifier_start", "verifier_end"]),
                *([] if cancel_before_commit or runtime_status == "succeeded" else ["trial_error"]),
                "trial_end",
            ]
            assert events[-1].payload["reward"] == rewards
            assert events[-1].payload["final_state"] == trial.state
            if not cancel_before_commit:
                assert next(event for event in events if event.kind == "verifier_end").payload["result"]["rewards"] == rewards
            storage_files = artifact.storage["files"]
            source_evidence = artifact.storage["source_evidence"]
            assert [item["relative_path"] for item in source_evidence] == [
                "source/_manifest.json",
                "source/_COMMITTED",
                "source/_artifact_manifest.json",
            ]
            assert all(
                (item["bucket"], item["key"]) in canonical_store.objects for item in source_evidence
            )
            answer = next(
                item for item in storage_files if item["relative_path"] == "artifacts/answer.txt"
            )
            assert answer["key"].startswith(f"trials/{trial.team_id}/{trial.id}/attempts/1/")
            assert canonical_store.objects[("artifacts", answer["key"])] == b"42\n"
            trajectory_key = trial.trajectory_index["trajectory_uri"].removeprefix(
                "s3://trajectories/"
            )
            atif_key = trial.trajectory_index["atif_uri"].removeprefix("s3://trajectories/")
            assert b'"kind":"llm_call"' in canonical_store.objects[("trajectories", trajectory_key)]
            assert (
                json.loads(canonical_store.objects[("trajectories", atif_key)])["schema_version"]
                == "1.7"
            )
            assert (
                json.loads(canonical_store.objects[("trajectories", atif_key)])["metadata"][
                    "task_id"
                ]
                == trial.task_id
            )
            source_prefix = f"service-executions/{trial.team_id}/{lease.id}/1/output/"
            assert not any(
                bucket == "artifacts" and key.startswith(source_prefix)
                for bucket, key in store.objects
            )
        # Exercise the ordinary list API against the actual persisted result.
        # Authentication is supplied as this fixture's team; no external service runs.
        import httpx

        from loom_service.dependencies import authed_session
        from loom_service.routes.trials import router as trials_router

        app = FastAPI()
        app.include_router(trials_router, prefix="/api/v1")

        async def fixture_session():  # type: ignore[no-untyped-def]
            async with sessions() as session:
                yield (
                    session,
                    AuthContext(
                        token_hash=b"local-reward-projection",
                        type="team",
                        scopes=["read:own"],
                        team_id=lease.team_id,
                        expires_at=None,
                    ),
                )

        app.dependency_overrides[authed_session] = fixture_session
        if cancel_before_commit or extra_artifacts:
            class CanonicalClient:
                def get_object(self, *, Bucket, Key):  # type: ignore[no-untyped-def]  # noqa: N803
                    payload = canonical_store.objects[(Bucket, Key)]
                    return {"Body": io.BytesIO(payload), "ContentLength": len(payload)}
            app.state.minio_client = CanonicalClient()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/api/v1/trials")
            if cancel_before_commit or extra_artifacts:
                downloaded = await client.get(f"/api/v1/trials/{trial_id}/bundle/download")
                assert downloaded.status_code == 200, downloaded.text
                with tarfile.open(fileobj=io.BytesIO(downloaded.content), mode="r:gz") as archive:
                    raw = archive.extractfile("files/result.json")
                    assert raw is not None and json.load(raw)["status"] == runtime_status
                    assert "files/artifacts/answer.txt" in archive.getnames()
                    assert len([name for name in archive.getnames()
                                if name.startswith("files/artifacts/extra-")]) == extra_artifacts
                atif = json.loads(canonical_store.objects[("trajectories", atif_key)])
                if cancel_before_commit:
                    assert atif["metadata"]["final_state"] == "cancelled"
                    assert atif["metadata"].get("reward") is None
        assert response.status_code == 200, response.text
        item = next(item for item in response.json()["items"] if item["id"] == str(trial_id))
        assert item["aggregate_reward"] == aggregate_reward
        assert item["state"] == ("succeeded" if runtime_status == "succeeded" else "cancelled" if cancel_before_commit else "failed")
    finally:
        await engine.dispose()


@pytest.mark.parametrize("outcome", ["succeeded", "timed_out", "cancelled"])
async def test_large_runtime_result_survives_canonical_publication_and_download(
    postgres_url: str, outcome: str,
) -> None:
    await test_materializer_commits_complete_bundle_after_execution_cleanup(
        postgres_url=postgres_url, source_task_id=None,
        rewards=None if outcome == "cancelled" else {"passed": 0.0},
        aggregate_reward=None if outcome == "cancelled" else 0.0,
        runtime_status="timed_out" if outcome == "cancelled" else outcome,
        cancel_before_commit=outcome == "cancelled", extra_artifacts=256,
    )


@pytest.mark.parametrize("tamper", [
    "payload", "missing", "foreign_artifact", "foreign_lease", "foreign_trial",
    "foreign_team", "upload", "generation", "event_digest", "invalid_number",
])
async def test_large_runtime_event_reference_rejects_identity_drift(
    postgres_url: str, tamper: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await _hand_off_to_verifier(session, parent_id=lease.id, now=now)
            lease.finalized_at = None
            lease.observed_state = "finalizing"
            artifact = Artifact(
                id=uuid4(), artifact_type="loom.trial-artifact-bundle.v1", name="trial_bundle",
                team_id=lease.team_id, trial_id=trial_id,
                control_producer_kind="service_execution", control_producer_id=lease.id,
                content_hash="sha256:" + "1" * 64,
                artifact_upload_session_id=lease.output_upload_session_id,
                manifest_sha256="sha256:" + "2" * 64,
                file_count=1, stored_size_bytes=1, unpacked_size_bytes=1,
                provenance={"lease_id": str(lease.id), "generation": lease.resource_generation,
                            "runtime_contract_sha256": lease.runtime_contract_sha256},
            )
            session.add(artifact)
            document = _runtime_result_payload(lease, started_at=now)
            # Python's compact number representation fits; PostgreSQL's JSONB
            # representation expands the same valid finite numbers past 64 KiB.
            document["verifier_rewards"] = {f"score-{i}": 1e300 for i in range(256)}
            # JCS emits these valid floats as integer tokens. Storing JCS text
            # would parse them back as unsafe Python integers on replay.
            document["verifier_rewards"].update({
                "positive": 1e20, "negative": -1e20, "safe_boundary": float(2**53),
                "fractional_origin": 1.0000000000000001e18,
            })
            result = ExecutionRuntimeResultV1.model_validate(document)
            payload = result.model_dump(mode="json")
            assert len(canonical_document(payload)) < 65_536
            event = await record_committed_runtime_result(
                session, lease_id=lease.id, generation=lease.generation,
                runtime_result=result, observed_at=now,
            )
            await session.commit()
            event_id, artifact_id, lease_id = event.id, artifact.id, lease.id

        async with sessions() as session:
            event = await session.get(ServiceExecutionEvent, event_id)
            assert event is not None
            assert event.payload_json["schema_version"] == "loom.execution-event-payload-reference.v1"
            assert event.payload_sha256 == canonical_digest(event.payload_json)
            assert await session.scalar(text(
                "SELECT octet_length(payload_json::text) FROM execution_events WHERE id = :id",
            ), {"id": event_id}) < 65_536
            replay, duplicate = await record_execution_event(
                session, lease_id=lease_id, generation=event.generation, ordinal=event.ordinal,
                event_kind="result_reported", payload=payload, observed_at=now,
            )
            assert replay.id == event_id and duplicate
            assert (await record_committed_runtime_result(
                session, lease_id=lease_id, generation=event.generation,
                runtime_result=result, observed_at=now,
            )).id == event_id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await record_execution_event(
                    session, lease_id=lease_id, generation=event.generation, ordinal=event.ordinal,
                    event_kind="result_reported", payload={**payload, "verifier_rewards": {"x": 0}},
                    observed_at=now,
                )
            with pytest.raises(ServiceExecutionFenceError):
                await record_execution_event(
                    session, lease_id=lease_id, generation=999, ordinal=event.ordinal + 1,
                    event_kind="result_reported", payload=payload, observed_at=now,
                )
            assert await finalize_committed_service_execution(session, lease_id=lease_id, observed_at=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None and trial.state == "materializing"
            assert trial.result is not None
            assert ExecutionRuntimeResultV1.model_validate(
                trial.result["runtime_result"],
            ).model_dump(mode="json") == payload
            finalized = await session.scalar(select(ServiceExecutionEvent).where(
                ServiceExecutionEvent.lease_id == lease_id,
                ServiceExecutionEvent.event_kind == "finalized",
            ))
            assert finalized is not None
            assert finalized.payload_json["schema_version"] == "loom.execution-event-payload-reference.v1"
            artifact = await session.get(Artifact, artifact_id)
            assert artifact is not None
            finalized_payload = json.loads(artifact.artifact_metadata["execution_event_payloads"][
                finalized.payload_json["payload_key"]
            ])
            replay, duplicate = await record_execution_event(
                session, lease_id=lease_id, generation=finalized.generation,
                ordinal=finalized.ordinal, event_kind="finalized",
                payload=finalized_payload, observed_at=now,
            )
            assert replay.id == finalized.id and duplicate
            await session.commit()

        async with sessions() as session:
            artifact = await session.get(Artifact, artifact_id)
            event = await session.get(ServiceExecutionEvent, event_id)
            assert artifact is not None and event is not None
            if tamper == "payload":
                artifact.artifact_metadata = {"execution_event_payloads": {
                    event.payload_json["payload_key"]: json.dumps({**payload, "verifier_rewards": {"x": 0}}),
                }}
            elif tamper == "missing":
                artifact.artifact_metadata = {}
            elif tamper == "foreign_artifact":
                event.payload_json = {**event.payload_json, "artifact_id": str(uuid4())}
                event.payload_sha256 = canonical_digest(event.payload_json)
            elif tamper == "foreign_lease":
                artifact.control_producer_id = uuid4()
            elif tamper in {"foreign_trial", "foreign_team"}:
                foreign_id, _ = await _seed_ready_trial(session, now=now)
                foreign = await session.get(Trial, foreign_id)
                assert foreign is not None
                if tamper == "foreign_trial":
                    artifact.trial_id = foreign.id
                else:
                    artifact.team_id = foreign.team_id
            elif tamper == "upload":
                foreign_id, foreign_target = await _seed_ready_trial(session, now=now)
                foreign_lease = await _reserve(
                    session, trial_id=foreign_id, target=foreign_target, now=now,
                )
                await _hand_off_to_verifier(session, parent_id=foreign_lease.id, now=now)
                artifact.artifact_upload_session_id = foreign_lease.output_upload_session_id
            elif tamper == "generation":
                artifact.provenance = {**artifact.provenance, "generation": 999}
            elif tamper == "event_digest":
                event.payload_sha256 = "sha256:" + "f" * 64
            elif tamper == "invalid_number":
                artifact.artifact_metadata = {"execution_event_payloads": {
                    event.payload_json["payload_key"]: json.dumps({
                        **payload, "verifier_rewards": {"positive": 100000000000000000001},
                    }),
                }}
            await session.commit()
        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="execution event payload"):
                await record_execution_event(
                    session, lease_id=lease_id, generation=event.generation, ordinal=event.ordinal,
                    event_kind="result_reported", payload=payload, observed_at=now,
                )
    finally:
        await engine.dispose()


async def test_timeout_preserves_zero_verifier_reward_through_canonical_cleanup(
    postgres_url: str,
) -> None:
    await test_materializer_commits_complete_bundle_after_execution_cleanup(
        postgres_url=postgres_url,
        source_task_id=None,
        rewards={"passed": 0.0},
        aggregate_reward=0.0,
        runtime_status="timed_out",
    )


@pytest.mark.parametrize("late_cancellation", ["before_deleted", "delete_event", "already_deleted"])
@pytest.mark.parametrize("runtime_status", ["succeeded", "timed_out"])
async def test_late_cancellation_preserves_committed_result_through_archival(
    postgres_url: str, late_cancellation: str, runtime_status: str,
) -> None:
    await test_materializer_commits_complete_bundle_after_execution_cleanup(
        postgres_url=postgres_url,
        source_task_id=None,
        rewards={"passed": 0.0},
        aggregate_reward=0.0,
        runtime_status=runtime_status,
        late_cancellation=late_cancellation,
    )


async def test_service_execution_input_is_resolved_from_persisted_task_binding(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    store = FakeObjectStore()
    body = b"answer the question\n"
    manifest = ServiceExecutionInputManifestV1(
        task_revision_sha256="sha256:" + "2" * 64,
        files=(
            ServiceExecutionInputFileV1(
                relative_path="instruction.md",
                size_bytes=len(body),
                sha256=digest_bytes(body),
                mode="0644",
            ),
        ),
    )
    manifest_body = manifest.canonical_bytes()
    manifest_digest = digest_bytes(manifest_body)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.source = "s3://artifacts/task-bundles/example/"
            task.source_provenance = {
                "service_execution_input": {
                    "schema_version": "loom.service-execution-input.v1",
                    "manifest_uri": "s3://artifacts/task-inputs/example.json",
                    "manifest_sha256": manifest_digest,
                    "file_count": 1,
                    "total_bytes": len(body),
                }
            }
            plan = _runtime_contract(now=now).model_copy(
                update={
                    "task_input": RuntimeTaskInputV1(
                        manifest_sha256=manifest_digest,
                        file_count=1,
                        total_bytes=len(body),
                    )
                }
            )
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                runtime_contract=plan,
            )
            await session.commit()

        await store.put_object(
            bucket="artifacts",
            key="task-inputs/example.json",
            body=manifest_body,
        )
        await store.put_object(
            bucket="artifacts",
            key="task-bundles/example/instruction.md",
            body=body,
        )
        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            resolved = await resolve_service_execution_input(
                session,
                lease=persisted,
                store=store,
                artifacts_bucket="artifacts",
            )
            assert resolved.manifest == manifest
            assert resolved.prefix == "task-bundles/example/"

        await store.put_object(
            bucket="artifacts",
            key="task-inputs/example.json",
            body=manifest_body + b"\n",
        )
        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            with pytest.raises(ServiceExecutionBrokerError, match="task_input_manifest_drift"):
                await resolve_service_execution_input(
                    session,
                    lease=persisted,
                    store=store,
                    artifacts_bucket="artifacts",
                )
    finally:
        await engine.dispose()


async def test_revocation_fences_tokens_but_bounds_output_flush(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    pod_ip = "10.24.8.31"
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-uid-flush",
                    "pod_uid": "pod-uid-flush",
                    "pod_ip": pod_ip,
                },
                observed_at=now,
            )
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        async with sessions() as session:
            with pytest.raises(ServiceExecutionBrokerError, match="generation_fenced"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip=pod_ip,
                    identity=identity,
                    purpose="token",
                    now=now + timedelta(seconds=2),
                )
            output_lease = await authorize_service_execution_peer(
                session,
                peer_ip=pod_ip,
                identity=identity,
                purpose="output",
                now=now + timedelta(minutes=4),
            )
            assert output_lease.resource_generation == 1
            assert output_lease.generation == 2
            with pytest.raises(ServiceExecutionBrokerError, match="window_closed"):
                await authorize_service_execution_peer(
                    session,
                    peer_ip=pod_ip,
                    identity=identity,
                    purpose="output",
                    now=now + timedelta(minutes=5, seconds=1),
                )
    finally:
        await engine.dispose()


async def test_invalid_state_edges_event_bounds_and_payload_bounds_fail_closed(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="create -> finalize"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="finalize",
                )
            with pytest.raises(ServiceExecutionConflict, match="invalid for desired state"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=1,
                    event_kind="started",
                    payload={},
                    observed_at=now,
                )
            with pytest.raises(ServiceExecutionConflict, match="between 1 and 10000"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=10_001,
                    event_kind="created",
                    payload={},
                    observed_at=now,
                )

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="payload_bound"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="start",
                    payload={"oversized": "x" * 70_000},
                )
            await session.rollback()
    finally:
        await engine.dispose()


async def test_operator_projection_is_team_isolated(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane.routes import service_executions

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        app = FastAPI()
        app.state.session_factory = sessions
        request = Request({"type": "http", "app": app, "headers": []})

        async def wrong_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=uuid4(),
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", wrong_team_auth)
        with pytest.raises(HTTPException) as hidden:
            await service_executions.get_trial_execution(trial_id, request, None)
        assert (hidden.value.status_code, hidden.value.detail) == (404, "trial not found")

        async def owning_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=lease.team_id,
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", owning_team_auth)
        projection = await service_executions.get_trial_execution(trial_id, request, None)
        assert projection["trial_id"] == str(trial_id)
        assert projection["execution"]["lease_id"] == str(lease.id)
        assert len(projection["commands"]) == 1
        assert projection["events"] == []
        assert projection["history"]
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("state", "started", "exhausted", "expects_retry"),
    [
        (NormalizedJobState.PENDING, False, False, False),
        (NormalizedJobState.UNSCHEDULABLE, True, False, False),
        (NormalizedJobState.UNSCHEDULABLE, False, False, True),
        (NormalizedJobState.UNSCHEDULABLE, False, True, True),
    ],
)
async def test_actuator_requeues_only_unstarted_unschedulable_at_deadline(
    postgres_url: str,
    state: NormalizedJobState,
    started: bool,
    exhausted: bool,
    expects_retry: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            quota = await session.get(TeamQuota, trial.team_id)
            assert quota is not None
            quota.max_attempts_ceiling = 1 if exhausted else 3
            lease = await _reserve(
                session, trial_id=trial_id, target=target, now=now, deadline_seconds=60
            )
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="infra-recovery-test",
        )
        assert await actuator.run_commands_once(now=now) == 1
        observation = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": state,
                "resource_version": "2",
                "started_at": now if started else None,
                "message": "Insufficient cpu"
                if state == NormalizedJobState.UNSCHEDULABLE
                else None,
            }
        )
        kubernetes.jobs[lease.job_name] = observation
        await actuator.reconcile_full_once(now=now + timedelta(seconds=59))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.desired_state == "create"
        # Same resourceVersion still needs deadline recovery; replay-safe event
        # ingestion must not prevent this existing-clock decision.
        await actuator.reconcile_full_once(now=now + timedelta(seconds=60))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            assert (
                trial.attempt_count == 1 and trial.failure_reason is None and trial.result is None
            )
            if not expects_retry:
                assert current.desired_state == "create" and trial.state == "claimed"
                return
            assert current.desired_state == "retry" and current.generation == 2
            assert current.cleanup_state == "pending" and trial.state == "queued"
            authorization = await session.scalar(
                select(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id == lease.id
                )
            )
            assert authorization is not None and authorization.state != "released"
            assert trial.next_attempt_at == now + timedelta(seconds=75)
            assert current.error_code == (
                "infra_recovery_exhausted" if exhausted else "unschedulable"
            )
            assert (
                await reserve_next_service_execution(
                    session,
                    environment="staging",
                    pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                    now=now + timedelta(seconds=76),
                )
                is None
            )
        # Keep the existing output grace period and exact UID cleanup; no second
        # Job can be admitted while the old execution still owns resources.
        await actuator.reconcile_full_once(now=now + timedelta(seconds=76))
        await actuator.run_commands_once(now=now + timedelta(seconds=76))
        assert kubernetes.delete_count == 0
        await actuator.run_commands_once(now=now + timedelta(seconds=361))
        assert kubernetes.delete_count == 1 and not kubernetes.jobs
        await actuator.reconcile_full_once(now=now + timedelta(seconds=362))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.cleanup_state == "complete"
            authorization = await session.scalar(
                select(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id == lease.id
                )
            )
            assert authorization is not None and authorization.state == "released"
            recovered = await reserve_next_service_execution(
                session,
                environment="staging",
                pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING,
                now=now + timedelta(seconds=363),
            )
            if exhausted:
                assert recovered is None
                assert current.error_code == "infra_recovery_exhausted"
            else:
                assert recovered is not None and recovered.attempt == 2
                assert recovered.id != lease.id
            await session.commit()
    finally:
        await engine.dispose()


@pytest.mark.parametrize("ambiguous_create", [False, True], ids=["normal", "lost-response"])
async def test_actuator_create_cancel_restart_and_missing_reconcile_converge(
    postgres_url: str,
    ambiguous_create: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi(ambiguous_create=ambiguous_create)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        runtime = ExecutionTargetRuntime(
            target_id=target.target_id,
            namespace=target.namespace_name,
            runtime_class_name="loom-sandbox",
        )
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        assert kubernetes.create_count == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            create_command = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "create",
                    )
                )
            ).scalar_one()
            capacity_authorization = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert persisted is not None
            assert persisted.job_uid == f"job-uid-{lease.job_name}"
            assert persisted.observed_state == "creating"
            assert persisted.last_reconciled_at == now
            assert create_command.state == "acknowledged"
            assert capacity_authorization.state == "pending"
            assert capacity_authorization.decision_reason == "existing_allocatable"
            capacity_policy = await session.get(ExecutionCapacityPolicy, target.target_id)
            assert capacity_policy is not None
            capacity_policy.enabled = False
            await session.commit()

        async with sessions() as session:
            replayed_authorization = await reserve_execution_provisioning(
                session,
                lease_id=lease.id,
                now=now + timedelta(milliseconds=1),
            )
            assert replayed_authorization is not None
            assert replayed_authorization.id == capacity_authorization.id
            await session.rollback()

        restarted = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-b",
            command_lease_seconds=5,
        )
        assert await restarted.run_commands_once(now=now + timedelta(seconds=1)) == 0
        assert kubernetes.create_count == 1

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            await session.commit()

        assert await restarted.run_commands_once(now=now + timedelta(seconds=2)) == 1
        assert kubernetes.delete_count == 1
        assert kubernetes.jobs == {}
        assert await restarted.reconcile_full_once(now=now + timedelta(seconds=3)) == 1

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.desired_state == "deleted"
            assert persisted.observed_state == "deleted"
            assert persisted.cleanup_state == "complete"
            assert persisted.output_commit_state == "unavailable"
            assert persisted.output_unavailable_reason == "operator_cancelled"
            assert persisted.deleted_at == now + timedelta(seconds=3)
            capacity_authorization = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert capacity_authorization.state == "released"
            assert capacity_authorization.released_at is not None
    finally:
        await engine.dispose()


async def test_actuator_refuses_uid_reuse_and_never_deletes_foreign_scope(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        runtime = ExecutionTargetRuntime(
            target_id=target.target_id,
            namespace=target.namespace_name,
            runtime_class_name="loom-sandbox",
        )
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=runtime,
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        original = kubernetes.jobs[lease.job_name]
        kubernetes.watch_events.append(
            original.model_copy(update={"lease_id": str(uuid4()), "resource_version": "2"})
        )
        assert await actuator.watch_once(timeout_seconds=1) == 0
        assert actuator._watch_resource_version == "2"
        kubernetes.jobs[lease.job_name] = original.model_copy(update={"job_uid": "reused-uid"})

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
        assert await actuator.run_commands_once(now=now + timedelta(minutes=5, seconds=1)) == 1
        assert kubernetes.delete_count == 0
        async with sessions() as session:
            cancel = (
                await session.execute(
                    select(ServiceExecutionCommand).where(
                        ServiceExecutionCommand.lease_id == lease.id,
                        ServiceExecutionCommand.command_type == "cancel",
                    )
                )
            ).scalar_one()
            assert cancel.state == "dead_letter"
            assert cancel.last_error_code == "contract_error"

        foreign = KubernetesJobObservation(
            namespace=target.namespace_name,
            job_name="foreign-managed-job",
            lease_id=str(uuid4()),
            resource_generation=1,
            target_id=target.target_id,
            execution_unit_key=str(uuid4()),
            normalized_state=NormalizedJobState.RUNNING,
            job_uid="foreign-uid",
            resource_version="2",
        )
        kubernetes.jobs[foreign.job_name] = foreign
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=2)) >= 1
        assert kubernetes.jobs[foreign.job_name] == foreign
        assert kubernetes.delete_count == 0
    finally:
        await engine.dispose()


async def test_actuator_records_unavailable_before_accepting_an_already_absent_job(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    kubernetes = _FakeKubernetesJobApi()
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
                runtime_class_name="loom-sandbox",
            ),
            controller_id="actuator-a",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        kubernetes.jobs.clear()
        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        assert await actuator.run_commands_once(now=now + timedelta(seconds=1)) == 1
        async with sessions() as session:
            closed = await session.get(ServiceExecutionLease, lease.id)
            assert closed is not None
            assert closed.observed_state == "deleted"
            assert closed.output_commit_state == "unavailable"
            assert closed.output_unavailable_reason == "operator_cancelled"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("class_cpu_limit", [None, 4_500])
@pytest.mark.parametrize("independent_controller", [False, True])
@pytest.mark.parametrize("request_overrides", [False, True])
async def test_private_terminus_sandboxes_reserve_full_pod_resources(
    postgres_url: str,
    class_cpu_limit: int | None,
    independent_controller: bool,
    request_overrides: bool,
) -> None:
    """Admission and both reservations include the controller and private sandboxes."""
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    plan = _runtime_contract(now=now)
    resources = ContainerResourcesV1(
        cpu_millis=2_000, memory_mib=4_096, ephemeral_storage_mib=2_048
    )
    agent_image = "registry.example/loom/worker@sha256:" + "d" * 64
    sandboxes = []
    for role in ("task-sandbox", "verifier-sandbox"):
        socket = f"/loom/sandboxes/{role}/sandbox.sock"
        probe = ProbeV1(
            kind="exec", argv=("/loom/bin/loom-sandbox-runtime", "--check-socket", socket)
        )
        sandboxes.append(
            SidecarContainerV1(
                role_name=role,
                private_sandbox=True,
                image_ref=plan.task_image_ref,
                argv=("/loom/bin/loom-sandbox-runtime", "--socket", socket),
                resources=resources,
                startup_probe=probe,
                readiness_probe=probe,
            )
        )
    plan = ExecutionRuntimePlanV1.model_validate(
        {
            **plan.canonical_payload(),
            "agent_image_ref": agent_image,
            "task_resources": resources,
            "sidecars": sandboxes,
            "image_admission": signed_image_admission_bundle(
                (plan.task_image_ref, plan.runtime_image_ref, agent_image), now=now
            ),
        }
    )
    if independent_controller:
        plan = ExecutionRuntimePlanV1.model_validate({
            **plan.canonical_payload(),
            "controller_resources": resources.model_copy(
                update={"cpu_millis": 1_000, "memory_mib": 2_048}
            ),
        })
    if request_overrides:
        plan = ExecutionRuntimePlanV1.model_validate({
            **plan.canonical_payload(),
            "resource_requests": {
                "controller": {"cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 256},
                "task_sandbox": {"cpu_millis": 500, "memory_mib": 1024, "ephemeral_storage_mib": 512},
                "verifier_sandbox": {"cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 256},
            },
        })
    requirements = _requirements().model_copy(update={"cpu_millis": 2_000, "memory_mib": 4_096})
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.config = {
                "agent": {"name": "terminus-2"},
                "model": {"provider": "zhipu", "name": "glm-5.2"},
            }
            execution_class = await session.get(ServiceExecutionClass, plan.execution_class_id)
            assert execution_class is not None
            execution_class.spec_json = {
                **execution_class.spec_json,
                # Internal task/verifier isolation does not consume submitted
                # workload sidecar slots, but always consumes real resources.
                "maximum_sidecars": 0,
                "maximum_cpu_millis": class_cpu_limit,
            }
            await session.commit()

        async with sessions() as session:
            if class_cpu_limit is not None and not request_overrides:
                # The task's own 2 CPU declaration fits this class; its complete
                # 5 or 6 CPU Pod does not. No lease or reservation may survive.
                with pytest.raises(ServiceExecutionConflict, match="cpu_limit_exceeded"):
                    async with session.begin_nested():
                        await _reserve(
                            session,
                            trial_id=trial_id,
                            target=target,
                            now=now,
                            requirements=requirements,
                            runtime_contract=plan,
                        )
                await session.commit()
                trial = await session.get(Trial, trial_id)
                assert trial is not None and (trial.state, trial.attempt_count) == ("queued", 0)
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ServiceExecutionLease)
                        .where(ServiceExecutionLease.trial_id == trial_id)
                    )
                    == 0
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ExecutionCostReservation)
                        .where(ExecutionCostReservation.trial_id == trial_id)
                    )
                    == 0
                )
                return
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                requirements=requirements,
                runtime_contract=plan,
            )
            await session.commit()
            lease_id = lease.id

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease_id)
            assert persisted is not None
            assert persisted.workload_requirements_json["sidecar_count"] == 0
            assert persisted.workload_requirements_json["cpu_millis"] == 2_000
            assert persisted.workload_requirements_json["memory_mib"] == 4_096
            cost = (
                await session.execute(
                    select(ExecutionCostReservation).where(
                        ExecutionCostReservation.lease_id == lease_id
                    )
                )
            ).scalar_one()
            capacity = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease_id
                    )
                )
            ).scalar_one()
            admission = (
                await session.execute(
                    select(ExecutionAdmissionReservation).where(
                        ExecutionAdmissionReservation.owner_id == lease_id,
                        ExecutionAdmissionReservation.owner_kind == "service_execution_lease",
                    )
                )
            ).scalar_one()
            assert admission.state == "active"
            expected_cpu = 1_000 if request_overrides else (5_000 if independent_controller else 6_000)
            expected_memory = 2_048 if request_overrides else (10_240 if independent_controller else 12_288)
            expected_storage = 1_024 if request_overrides else 6_144
            for reserved in (cost, capacity):
                assert reserved.requested_cpu_millis == expected_cpu
                assert reserved.requested_memory_mib == expected_memory
            assert cost.requested_ephemeral_storage_mib == expected_storage
            assert capacity.requested_storage_mib == cost.requested_ephemeral_storage_mib
            pod = render_execution_job(persisted, target=ExecutionTargetRuntime(
                target_id=persisted.target_id, namespace=persisted.namespace_name,
            ), now=now)["spec"]["template"]["spec"]
            containers = [*pod["containers"], *pod["initContainers"][1:]]
            for dimension, suffix, expected in (
                ("cpu", "m", expected_cpu), ("memory", "Mi", expected_memory),
                ("ephemeral-storage", "Mi", expected_storage),
            ):
                assert sum(int(item["resources"]["requests"][dimension].removesuffix(suffix))
                           for item in containers) == expected
            assert pod["containers"][0]["resources"]["limits"]["cpu"] == (
                "1000m" if independent_controller else "2000m"
            )
            assert all(item["resources"]["limits"]["cpu"] == "2000m"
                       for item in pod["initContainers"][1:])
            trial = await session.get(Trial, trial_id)
            assert trial is not None and (trial.state, trial.attempt_count) == ("claimed", 1)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("runtime_status", ["cancelled", "task_error"])
async def test_cancel_during_upload_preserves_partial_canonical_evidence(
    postgres_url: str, runtime_status: str,
) -> None:
    await test_materializer_commits_complete_bundle_after_execution_cleanup(
        postgres_url=postgres_url, source_task_id=None, rewards=None, aggregate_reward=None,
        runtime_status=runtime_status, cancel_before_commit=True,
    )


async def test_cancelled_pod_without_output_closes_at_bounded_deadline(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kube = _FakeKubernetesJobApi()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        actuator = ExecutionActuator(sessions=sessions, kubernetes=kube,
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="bounded-cancel")
        await actuator.run_commands_once(now=now)
        kube.jobs[lease.job_name] = kube.jobs[lease.job_name].model_copy(update={
            "normalized_state": NormalizedJobState.RUNNING, "pod_uid": "cancel-pod",
            "pod_ip": "10.24.7.25", "resource_version": "2",
        })
        await actuator.reconcile_full_once(now=now)
        async with sessions() as session:
            await request_trial_execution_cancellation(session, trial_id=trial_id, now=now)
            await session.commit()
        await actuator.run_commands_once(now=now)
        assert kube.delete_count == 1 and not kube.jobs
        await actuator.reconcile_full_once(now=now + timedelta(seconds=299))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.output_commit_state == "not_started"
            assert current.cleanup_state == "pending" and current.deleted_at is None
        await actuator.reconcile_full_once(now=now + timedelta(seconds=300))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            assert current.cleanup_state == "complete" and current.deleted_at is not None
            assert current.output_unavailable_reason == "cleanup_deadline_elapsed"
            assert trial.state == "cancelled" and trial.attempt_count == 1
            assert trial.result["reward"] is None
            assert trial.result["output_unavailable_reason"] == "cleanup_deadline_elapsed"
    finally:
        await engine.dispose()
