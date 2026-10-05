"""Issue immutable collector scopes and retain exact physical observations.

These functions run inside the caller's management transaction and never perform
network I/O or commit. Collection happens between those transactions. New requests
do not invalidate a capture: only its exact represented Job receipts can discount
reservations later. A new registration requires a fresh capture.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCapture,
    NebiusPoolObservation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolParticipantV1, PoolWorkloadKind
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.contracts import (
    KubernetesCapacitySnapshot,
    ProviderCapacitySnapshot,
)
from loom_execution_capacity_collector.pool import (
    GatewayJobBinding,
    PoolEnvironmentBinding,
    PoolObservationScope,
    PoolPodClassifier,
)
from loom_service.pool_management.auth import PoolPrincipal, authorize_pool_machine
from loom_service.pool_management.locks import acquire_pool_mutation_lock

_CAPTURE_MAX_AGE = timedelta(minutes=5)


class PoolObservationError(ValueError):
    def __init__(self) -> None:
        super().__init__("pool_observation_unavailable")


@dataclass(frozen=True)
class IssuedPoolCapture:
    capture_id: UUID
    pool_id: UUID
    admission_epoch: int
    registration_sha256: str
    scope: PoolObservationScope
    created_at: datetime


@dataclass(frozen=True)
class RecordedPoolObservation:
    observation_id: UUID
    capture_id: UUID
    observation_sha256: str


def _digest(value: Any) -> str:
    return canonical_digest(value).removeprefix("sha256:")


async def _registration(session: AsyncSession, principal: PoolPrincipal) -> tuple[
    NebiusPoolBinding, tuple[PoolParticipantV1, ...], str,
]:
    # Take the exclusive pool lock before auth's read lock; upgrading concurrent
    # read locks later would deadlock. All pool writers take the mutation lock first.
    if any(isinstance(row, (NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest,
                            NebiusPoolCapture, NebiusPoolObservation))
           for row in session.new | session.dirty | session.deleted):
        raise PoolObservationError
    await acquire_pool_mutation_lock(session)
    pool = (await session.scalars(select(NebiusPoolBinding).where(
        NebiusPoolBinding.pool_id == principal.pool_id,
    ).with_for_update().execution_options(populate_existing=True))).one_or_none()
    await authorize_pool_machine(session, principal, role="observer", pool_id=principal.pool_id)
    if pool is None:
        raise PoolObservationError
    bindings, digest = await read_pool_registration(session, pool)
    return pool, bindings, digest


async def read_pool_registration(session: AsyncSession, pool: NebiusPoolBinding) -> tuple[tuple[PoolParticipantV1, ...], str]:
    """Read exact registration under the caller's mutation/pool locks."""
    if pool is None or pool.mode not in {"closed", "global"} or _digest(pool.binding_json) != pool.binding_sha256:
        raise PoolObservationError
    rows = list((await session.scalars(select(NebiusPoolParticipant).where(
        NebiusPoolParticipant.pool_id == pool.pool_id,
    ).order_by(NebiusPoolParticipant.participant_id).limit(10_001)
        .with_for_update(read=True).execution_options(populate_existing=True))).all())
    if len(rows) > 10_000:
        raise PoolObservationError
    bindings = []
    identities = []
    for row in rows:
        binding = PoolParticipantV1.model_validate(row.binding_json)
        if (_digest(row.binding_json) != row.binding_sha256
                or (binding.participant_id, binding.pool_id, binding.installation_id, binding.environment_id,
                    binding.incarnation, binding.binding_revision, binding.admission_epoch) != (
                    row.participant_id, pool.pool_id, pool.installation_id, row.environment_id,
                    row.incarnation, row.binding_revision, row.admission_epoch)
                or row.admission_epoch != pool.admission_epoch):
            raise PoolObservationError
        bindings.append(binding)
        identities.append({"participant_id": str(row.participant_id), "binding_sha256": row.binding_sha256,
                           "revision": row.binding_revision, "phase": row.phase})
    digest = _digest({"pool_id": str(pool.pool_id), "installation_id": str(pool.installation_id),
        "cluster_id": pool.cluster_id, "node_group_id": pool.node_group_id,
        "admission_epoch": pool.admission_epoch, "policy_revision": pool.policy_revision,
        "binding_sha256": pool.binding_sha256, "participants": identities})
    return tuple(bindings), digest


async def issue_pool_capture(session: AsyncSession, principal: PoolPrincipal) -> IssuedPoolCapture:
    """Persist a server-derived, secret-free scope; no caller receipt list."""
    try:
        with session.no_autoflush:
            pool, participants, registration_sha = await _registration(session, principal)
            by_id = {row.participant_id: row for row in participants}
            requests = list((await session.scalars(select(NebiusPoolRequest).where(
                NebiusPoolRequest.pool_id == pool.pool_id, NebiusPoolRequest.job_uid.is_not(None),
                NebiusPoolRequest.phase.in_(("observed", "cleanup_intent")),
            ).order_by(NebiusPoolRequest.request_id).limit(200_001)
                .execution_options(populate_existing=True))).all())
            if len(requests) > 200_000:
                raise PoolObservationError
            jobs = []
            for row in requests:
                participant = by_id[row.participant_id]
                if row.workload_kind not in {"trial", "verifier", "task_image_build", "application_image_build"}:
                    raise PoolObservationError
                if row.plan_json is None or _digest(row.plan_json) != row.plan_sha256:
                    raise PoolObservationError
                metadata = row.plan_json["job"]["metadata"]
                namespace = (participant.build_namespace if row.workload_kind in {"task_image_build", "application_image_build"}
                             else participant.execution_namespace)
                if row.namespace_uid != namespace.uid or metadata["namespace"] != namespace.name:
                    raise PoolObservationError
                participant.target(row.target_id, cast(PoolWorkloadKind, row.workload_kind))
                generation = row.generation
                prefix = ""
                if row.workload_kind == "task_image_build":
                    prefix = "task-image:"
                    request = PoolTaskImagePrepareV1.model_validate(row.request_json)
                    generation = request.build.expected_lease_epoch + 1
                    if (metadata["labels"]["loom.lease-epoch"] != str(generation)
                            or metadata["labels"]["loom.materialization-id"] != str(row.local_work_id)):
                        raise PoolObservationError
                elif row.workload_kind == "application_image_build":
                    application = PoolApplicationImagePrepareV1.model_validate(row.request_json)
                    generation = application.build.attempt
                    prefix = "application-image:"
                    if (generation != row.generation or application.build.build_id != row.local_work_id
                            or metadata["labels"]["loom.build-attempt"] != str(generation)
                            or metadata["labels"]["loom.application-build-id"] != str(row.local_work_id)):
                        raise PoolObservationError
                jobs.append(GatewayJobBinding.model_validate({
                    "reservation_id": row.request_id, "environment_id": participant.environment_id,
                    "incarnation": participant.incarnation, "namespace": namespace.name,
                    "job_name": metadata["name"], "job_uid": str(row.job_uid), "target_id": row.target_id,
                    "workload_kind": row.workload_kind, "generation": generation,
                    "lease_id": prefix + str(row.local_work_id),
                }))
            scope = PoolObservationScope(
                node_selector=pool.binding_json["node_selector"],
                environments=tuple(PoolEnvironmentBinding(
                    environment_id=row.environment_id, incarnation=row.incarnation,
                    execution_namespace=row.execution_namespace.name, build_namespace=row.build_namespace.name,
                    target_ids=tuple(target.target_id for target in row.targets),
                ) for row in participants), jobs=tuple(jobs),
            )
            capture_id = uuid4()
            created = (await session.execute(insert(NebiusPoolCapture).values(
                capture_id=capture_id, pool_id=pool.pool_id, admission_epoch=pool.admission_epoch,
                registration_sha256=registration_sha,
                scope_sha256=PoolPodClassifier(scope).fingerprint.removeprefix("sha256:"),
                scope_json=scope.model_dump(mode="json"),
            ).returning(NebiusPoolCapture.created_at))).scalar_one()
            return IssuedPoolCapture(capture_id, pool.pool_id, pool.admission_epoch, registration_sha, scope, created)
    except (ValueError, KeyError, TypeError):
        raise PoolObservationError from None


async def record_pool_observation(session: AsyncSession, principal: PoolPrincipal, *, capture_id: UUID,
                                  observed_at: datetime, provider: ProviderCapacitySnapshot,
                                  kubernetes: KubernetesCapacitySnapshot) -> RecordedPoolObservation:
    """Record one exact snapshot for a current server scope; replay never renews it."""
    try:
        with session.no_autoflush:
            pool, _, registration_sha = await _registration(session, principal)
            capture = (await session.scalars(select(NebiusPoolCapture).where(
                NebiusPoolCapture.capture_id == capture_id,
            ).execution_options(populate_existing=True))).one_or_none()
            if (capture is None or capture.pool_id != pool.pool_id
                    or capture.admission_epoch != pool.admission_epoch or capture.registration_sha256 != registration_sha
                    or observed_at.utcoffset() is None):
                raise PoolObservationError
            provider = ProviderCapacitySnapshot.model_validate(provider.model_dump())
            kubernetes = KubernetesCapacitySnapshot.model_validate(kubernetes.model_dump())
            scope = PoolObservationScope.model_validate(capture.scope_json)
            fingerprint = PoolPodClassifier(scope).fingerprint
            if (fingerprint.removeprefix("sha256:") != capture.scope_sha256
                    or kubernetes.source_versions.get("pool_scope") != fingerprint
                    or provider.node_group is None or provider.node_group.id != pool.node_group_id
                    or provider.node_count != provider.node_group.node_count):
                raise PoolObservationError
            known = {f"reservation:{job.reservation_id}" for job in scope.jobs}
            managed = [*(pod for node in kubernetes.nodes for pod in node.managed_pods), *kubernetes.pending_pods]
            # Foreign pending occupancy has a collector-generated non-reservation
            # identity. It stays charged, never discounts a durable reservation.
            represented = [pod for pod in managed if pod.lease_id.startswith("reservation:")]
            if (any(pod.lease_id not in known or pod.generation != 1 for pod in represented)
                    or len({pod.lease_id for pod in represented}) != len(represented)):
                raise PoolObservationError
            payload = {"schema_version": "loom.pool-observation.v1", "capture_id": str(capture_id),
                       "observed_at": observed_at.astimezone(UTC).isoformat(),
                       "provider": provider.model_dump(mode="json"), "kubernetes": kubernetes.model_dump(mode="json")}
            digest = _digest(payload)
            existing = (await session.scalars(select(NebiusPoolObservation).where(
                NebiusPoolObservation.capture_id == capture_id,
            ).execution_options(populate_existing=True))).one_or_none()
            if existing is not None:
                if existing.observation_sha256 != digest or existing.observation_json != payload:
                    raise PoolObservationError
                return RecordedPoolObservation(existing.observation_id, capture_id, digest)
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            if (observed_at < capture.created_at or observed_at > now + timedelta(seconds=60)
                    or observed_at > capture.created_at + _CAPTURE_MAX_AGE or now > capture.created_at + _CAPTURE_MAX_AGE):
                raise PoolObservationError
            observation_id = uuid4()
            await session.execute(insert(NebiusPoolObservation).values(
                observation_id=observation_id, pool_id=pool.pool_id, capture_id=capture_id,
                observed_at=observed_at, observation_sha256=digest, observation_json=payload,
            ))
            return RecordedPoolObservation(observation_id, capture_id, digest)
    except (ValueError, KeyError, TypeError):
        raise PoolObservationError from None
