"""Environment-owned execution proposals and exact post-grant claims.

Public operations commit before returning. Callers perform management HTTP only
between these operations; no method can create a Kubernetes resource.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import func, select, text, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import Batch, ServiceExecutionLease, ServiceExecutionTarget, Task, Trial
from loom.execution_contract import ExecutionRoutingReason
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.execution_resource_allocation import allocate_node_resources
from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_pool_contract import (
    PoolActivationV1,
    PoolParticipantV1,
    PoolReceiptV1,
    PoolRequestActionV1,
    PoolRequestKeyV1,
)
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.nebius_pool_workload import PoolExecutionPrepareV1, PoolExecutionWorkloadV1
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_digest
from loom.task_bundle_source_journal import require_task_bundle_transaction
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.pool_execution_handoff import execution_selection_snapshot
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    _execution_identity,
    enqueue_execution_transition,
    record_kubernetes_observation,
    reserve_trial_execution,
)
from loom_control_plane.service_execution_scheduler import (
    _SERVICE_TRIAL_BY_ID,
    ServiceExecutionConfigurationError,
    _compile_service_candidate,
)
from loom_execution_actuator.contracts import KubernetesJobObservation, NormalizedJobState
from loom_execution_actuator.pool_outbox import PoolHandoffError


@dataclass(frozen=True)
class PoolExecutionHandoff:
    request: PoolExecutionPrepareV1
    request_sha256: str
    phase: str
    reservation_id: UUID | None
    lease_id: UUID | None
    activation: PoolActivationV1 | None
    activated: PoolReceiptV1 | None
    released: PoolReceiptV1 | None

    @property
    def action(self) -> PoolRequestActionV1:
        return PoolRequestActionV1(pool_id=self.request.pool_id, request_key=self.request.key,
            admission_epoch=self.request.admission_epoch, request_sha256=self.request_sha256)


async def _clock(session: AsyncSession) -> datetime:
    return (await session.execute(select(func.clock_timestamp()))).scalar_one()  # type: ignore[no-any-return]


class PoolExecutionOutbox:
    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], participant: PoolParticipantV1,
        environment: str, logical_pool_id: str, image_admission_keyring: ImageAdmissionKeyring,
        maximum_deadline_seconds: int = 7200,
    ) -> None:
        self.sessions = sessions
        self.participant = PoolParticipantV1.model_validate_json(participant.model_dump_json())
        if not environment.strip() or not logical_pool_id.strip() or not 1 <= maximum_deadline_seconds <= 86400:
            raise PoolHandoffError
        self.environment, self.logical_pool_id = environment, logical_pool_id
        self.image_admission_keyring = image_admission_keyring
        self.maximum_deadline_seconds = maximum_deadline_seconds

    def _view(self, row: NebiusPoolExecutionOutbox) -> PoolExecutionHandoff:
        request = PoolExecutionPrepareV1.model_validate(row.request_json)
        if (request.key.participant_id != self.participant.participant_id
                or request.pool_id != self.participant.pool_id or request.key.local_work_id != row.lease_id
                or request.key.workload_kind != "trial" or request.key.generation != 1
                or row.request_sha256 != canonical_digest(row.request_json).removeprefix("sha256:")
                or request.origin.data_environment_id != self.participant.environment_id
                or row.selection_json["target"]["environment"] != self.environment
                or row.selection_json["target"]["logical_pool_id"] != self.logical_pool_id):
            raise PoolHandoffError
        return PoolExecutionHandoff(request, row.request_sha256, row.phase, row.reservation_id, row.attached_lease_id,
            PoolActivationV1.model_validate(row.activation_json) if row.activation_json is not None else None,
            PoolReceiptV1.model_validate(row.activated_json) if row.activated_json is not None else None,
            PoolReceiptV1.model_validate(row.released_json) if row.released_json is not None else None)

    async def _load(self, session: AsyncSession, key: PoolRequestKeyV1) -> NebiusPoolExecutionOutbox:
        await require_task_bundle_transaction(session)
        key = PoolRequestKeyV1.model_validate_json(key.model_dump_json())
        row = await session.get(NebiusPoolExecutionOutbox, key.local_work_id, with_for_update=True)
        if row is None or self._view(row).request.key != key:
            raise PoolHandoffError
        return row

    async def get(self, key: PoolRequestKeyV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            return self._view(await self._load(session, key))

    async def iter_pending(self, *, target_id: str, page_size: int = 100) -> AsyncIterator[PoolExecutionHandoff]:
        """Bounded keyset scan; no transaction remains open across reconciliation."""
        self.participant.target(target_id, "trial")
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise PoolHandoffError
        created, identity = NebiusPoolExecutionOutbox.created_at, NebiusPoolExecutionOutbox.lease_id
        pending = select(NebiusPoolExecutionOutbox).where(
            NebiusPoolExecutionOutbox.participant_id == self.participant.participant_id,
            NebiusPoolExecutionOutbox.request_json["target_id"].astext == target_id,
            NebiusPoolExecutionOutbox.phase.not_in(("cancelled", "released")),
        ).order_by(created, identity)
        async with self.sessions() as session:
            last = (await session.execute(pending.with_only_columns(created, identity)
                .order_by(None).order_by(created.desc(), identity.desc()).limit(1))).first()
        if last is None:
            return
        ceiling = (last[0], last[1])
        after: tuple[datetime, UUID] | None = None
        while True:
            query = pending.where(tuple_(created, identity) <= ceiling).limit(page_size)
            if after is not None:
                query = query.where(tuple_(created, identity) > after)
            async with self.sessions() as session:
                rows = list(await session.scalars(query))
            if not rows:
                return
            after = (rows[-1].created_at, rows[-1].lease_id)
            for row in rows:
                yield self._view(row)
            if after == ceiling:
                return

    def _current_binding(self, request: PoolExecutionPrepareV1) -> bool:
        return (request.admission_epoch == self.participant.admission_epoch
                and request.participant_revision == self.participant.binding_revision
                and any(target.target_id == request.target_id and "trial" in target.workload_kinds
                        for target in self.participant.targets))

    async def refresh_selection(self, key: PoolRequestKeyV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            request = self._view(row).request
            if row.phase == "selected":
                now = await _clock(session)
                candidate = (await session.execute(_SERVICE_TRIAL_BY_ID,
                    {"trial_id": row.trial_id, "pool_id": self.logical_pool_id, "now": now})).mappings().one_or_none()
                trial = await session.get(Trial, row.trial_id, populate_existing=True)
                target = await session.get(ServiceExecutionTarget, request.target_id, with_for_update=True)
                if (candidate is None or trial is None or target is None or not self._current_binding(request)
                        or request.deadline_at <= now or not await admission_open(session)
                        or execution_selection_snapshot(trial, candidate, target, request.execution.runtime) != row.selection_json):
                    row.phase = "cancel_pending"
                    await session.flush()
            return self._view(row)

    def _receipt(self, row: NebiusPoolExecutionOutbox, receipt: PoolReceiptV1) -> PoolReceiptV1:
        receipt = PoolReceiptV1.model_validate_json(receipt.model_dump_json())
        action = self._view(row).action
        if (receipt.pool_id != action.pool_id or receipt.request_key != action.request_key
                or receipt.admission_epoch != action.admission_epoch or receipt.request_sha256 != action.request_sha256
                or (row.reservation_id is not None and row.reservation_id != receipt.reservation_id)):
            raise PoolHandoffError
        return receipt

    async def propose(self, *, trial_id: UUID, target_id: str,
                      node_allocation: PoolNodeAllocationV1 | None = None) -> PoolExecutionHandoff | None:
        """Freeze actual compiled input without claiming an execution or budget."""
        self.participant.target(target_id, "trial")
        async with self.sessions.begin() as session:
            await require_task_bundle_transaction(session)
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 171))"),
                {"key": "pool-execution-selection:" + str(trial_id)})
            existing = await session.scalar(select(NebiusPoolExecutionOutbox).where(
                NebiusPoolExecutionOutbox.trial_id == trial_id,
                NebiusPoolExecutionOutbox.phase.not_in(("cancelled", "released"))).with_for_update())
            if existing is not None:
                view = self._view(existing)
                if view.request.target_id != target_id:
                    raise PoolHandoffError
                return view
            now = await _clock(session)
            candidate = (await session.execute(_SERVICE_TRIAL_BY_ID,
                {"trial_id": trial_id, "pool_id": self.logical_pool_id, "now": now})).mappings().one_or_none()
            if candidate is None or not await admission_open(session):
                raise PoolHandoffError
            trial = await session.get(Trial, trial_id, populate_existing=True)
            if trial is None:
                raise PoolHandoffError
            origin = PoolWorkOriginV1.model_validate(trial.pool_origin)
            pool_request_priority(self.participant, origin, workload_kind="trial")
            # Both proposal and attachment lock source -> target -> images.
            # The compiler locks prepared images; pin the target before it.
            target = await session.get(ServiceExecutionTarget, target_id, with_for_update=True, populate_existing=True)
            if target is None or target.spec_json["namespace_name"] != self.participant.execution_namespace.name:
                raise PoolHandoffError
            try:
                async with session.begin_nested():
                    compiled = await _compile_service_candidate(session, row=candidate, environment=self.environment,
                        pool_id=self.logical_pool_id, maximum_deadline_seconds=self.maximum_deadline_seconds,
                        current_time=now)
            except ServiceExecutionConfigurationError as error:
                # Roll back partial compilation but retain the source/Trial
                # locks acquired before the savepoint. A separate transaction
                # could fail a corrected candidate or race cancellation.
                await session.execute(update(Trial).where(Trial.id == trial_id, Trial.state == "queued",
                    Trial.cancellation_requested_at.is_(None)).values(state="failed",
                    failure_reason="service_execution_configuration_invalid", failure_message=str(error),
                    finished_at=now, next_attempt_at=None, scheduling_observation=None))
                return None
            if compiled is None:
                # Compilation may have finished an image-failed queued Trial.
                # Commit that existing terminal behavior without claiming work.
                return None
            if target_id not in {target.id for target in compiled.targets}:
                raise PoolHandoffError
            runtime = compiled.runtime_plan
            evidence = None
            if compiled.allocate_resources:
                if node_allocation is None:
                    raise PoolHandoffError
                evidence = PoolNodeAllocationV1.model_validate_json(node_allocation.model_dump_json())
                scope = PoolNodeAllocationRequestV1(pool_id=self.participant.pool_id,
                    participant_id=self.participant.participant_id, admission_epoch=self.participant.admission_epoch,
                    participant_revision=self.participant.binding_revision, target_id=target_id)
                if (evidence.scope != scope or evidence.observed_at > now + timedelta(seconds=60)
                        or evidence.valid_until <= now):
                    raise PoolHandoffError
                runtime = allocate_node_resources(runtime, target_id=target_id, usable_node=evidence.usable_node)
            # Compilation can freeze a legacy verifier default. Snapshot the
            # actual persisted input after that legitimate change, not old SQL.
            await session.flush()
            await session.refresh(trial)
            lease_id = uuid4()
            _, _, _, unit = _execution_identity(trial_id=trial.id, attempt=trial.attempt_count + 1,
                generation=1, execution_role="attempt", namespace_name=self.participant.execution_namespace.name,
                target_id=target_id)
            request = PoolExecutionPrepareV1(pool_id=self.participant.pool_id,
                admission_epoch=self.participant.admission_epoch, participant_revision=self.participant.binding_revision,
                key=PoolRequestKeyV1(participant_id=self.participant.participant_id, workload_kind="trial",
                    local_work_id=lease_id, generation=1), target_id=target_id, deadline_at=compiled.deadline_at,
                origin=origin, execution=PoolExecutionWorkloadV1(lease_generation=1, execution_unit_key=unit,
                    parent_lease_id=None, requirements=compiled.requirements, runtime=runtime, node_allocation=evidence))
            payload = request.model_dump(mode="json")
            row = NebiusPoolExecutionOutbox(lease_id=lease_id, trial_id=trial_id, pool_id=request.pool_id,
                participant_id=request.key.participant_id, request_json=payload,
                request_sha256=canonical_digest(payload).removeprefix("sha256:"),
                selection_json=execution_selection_snapshot(trial, candidate, target, runtime), phase="selected")
            session.add(row)
            await session.flush()
            return self._view(row)

    async def _current_claim(self, session: AsyncSession, row: NebiusPoolExecutionOutbox) -> bool:
        request = self._view(row).request
        if row.attached_lease_id is None or not self._current_binding(request):
            return False
        trial = await session.get(Trial, row.trial_id, with_for_update=True, populate_existing=True)
        if trial is None or trial.state != "claimed" or trial.cancellation_requested_at is not None:
            return False
        task = await session.get(Task, trial.task_id, with_for_update=True)
        batch = await session.get(Batch, trial.batch_id, with_for_update=True) if trial.batch_id is not None else None
        target = await session.get(ServiceExecutionTarget, request.target_id, with_for_update=True)
        lease = await session.get(ServiceExecutionLease, row.attached_lease_id, with_for_update=True)
        now = await _clock(session)
        if (task is None or batch is None or target is None or lease is None
                or lease.trial_id != trial.id or lease.attempt != trial.attempt_count
                or lease.desired_state != "create" or lease.generation != request.execution.lease_generation
                or lease.resource_generation != request.key.generation or lease.revoked_at is not None
                or lease.job_uid is not None or lease.pod_uid is not None or lease.deadline_at <= now
                or trial.execution_route_generation != lease.routing_generation
                or trial.execution_route_sha256 != lease.routing_decision_sha256
                or trial.execution_route_pool_name != lease.selected_pool_id
                or target.desired_state != "active" or target.observed_state != "ready"
                or target.health_status != "healthy" or target.health_observed_at is None
                or target.health_observed_at + timedelta(seconds=int(target.spec_json["health_stale_after_seconds"])) <= now
                or target.spec_json["namespace_name"] != self.participant.execution_namespace.name
                or not await admission_open(session)):
            return False
        snapshot = execution_selection_snapshot(trial, {
            "task_checksum": task.checksum, "task_config": task.config, "task_source_provenance": task.source_provenance,
            "legacy_separate_verifier_checksum": task.legacy_separate_verifier_checksum,
            "batch_runtime_profile": batch.service_execution_runtime_profile,
        }, target, request.execution.runtime)
        # These are the only selection fields deliberately advanced by claim.
        # Verify the current route against the lease above, then compare all
        # other source/configuration/identity fields to the retained proposal.
        for name in ("attempt_count", "route_generation", "route_pool", "route"):
            snapshot["trial"][name] = row.selection_json["trial"][name]
        return snapshot == row.selection_json

    async def begin_activation(self, key: PoolRequestKeyV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            if row.phase == "selected":
                raise PoolHandoffError
            if row.phase not in {"attached", "activation_pending"}:
                return self._view(row)
            saved, now = self._view(row), await _clock(session)
            if (not await self._current_claim(session, row)
                    or (saved.activation is not None and saved.activation.not_after <= now)):
                row.phase = "cancel_pending"
            else:
                if saved.activation is None:
                    row.activation_json = PoolActivationV1(action=saved.action,
                        not_after=min(now + timedelta(seconds=30), saved.request.deadline_at)).model_dump(mode="json")
                row.phase = "activation_pending"
            await session.flush()
            return self._view(row)

    async def confirm_activation(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            receipt = self._receipt(row, receipt)
            if (row.activation_json is None or receipt.plan_sha256 is None
                    or row.phase not in {"activation_pending", "cancel_pending", "active", "stop_pending", "released"}):
                raise PoolHandoffError
            if row.activated_json is not None:
                previous = PoolReceiptV1.model_validate(row.activated_json)
                if (previous.plan_sha256 != receipt.plan_sha256
                        or (previous.job_uid is not None and previous.job_uid != receipt.job_uid)):
                    raise PoolHandoffError
                return self._view(row)
            current = await self._current_claim(session, row)
            row.phase = ("active" if row.phase == "activation_pending" and current
                and receipt.phase in {"create_intent", "observed"} else "stop_pending")
            row.activated_json = receipt.model_dump(mode="json")
            await session.flush()
            return self._view(row)

    async def request_cancel(self, key: PoolRequestKeyV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            if row.phase in {"selected", "attached", "activation_pending", "active"}:
                row.phase = "stop_pending" if row.phase == "active" else "cancel_pending"
                await session.flush()
            return self._view(row)

    async def _cleanup_lease(self, session: AsyncSession, row: NebiusPoolExecutionOutbox) -> ServiceExecutionLease:
        saved = self._view(row)
        if saved.phase not in {"active", "stop_pending", "released"} or saved.activated is None or row.attached_lease_id is None:
            raise PoolHandoffError
        lease = await session.get(ServiceExecutionLease, row.attached_lease_id, with_for_update=True, populate_existing=True)
        if (lease is None or lease.trial_id != row.trial_id or lease.target_id != saved.request.target_id
                or lease.resource_generation != saved.request.key.generation
                or lease.execution_unit_key != saved.request.execution.execution_unit_key
                or lease.job_name != "loom-pool-" + saved.activated.reservation_id.hex):
            raise PoolHandoffError
        return lease

    async def revoke_stopped(self, key: PoolRequestKeyV1) -> None:
        """An accepted activation that lost its claim follows existing fenced cleanup."""
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            if row.phase != "stop_pending":
                return
            lease = await self._cleanup_lease(session, row)
            if lease.desired_state not in {"create", "start", "finalize"}:
                return
            trial = await session.get(Trial, row.trial_id, with_for_update=True, populate_existing=True)
            if lease.desired_state == "finalize":
                desired = "delete_pending"  # Existing transition requires durable output.
            elif (trial is not None and trial.attempt_count == lease.attempt
                    and trial.state in {"claimed", "running"} and trial.cancellation_requested_at is None):
                desired = "retry"
            else:
                desired = "cancel"
            await enqueue_execution_transition(session, lease_id=lease.id, expected_generation=lease.generation,
                desired_state=desired, now=await _clock(session))

    async def begin_stop(self, key: PoolRequestKeyV1) -> PoolStopV1 | None:
        """Retain cleanup consent from local revocation, before waiting for output."""
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            lease = await self._cleanup_lease(session, row)
            if row.stop_json is not None:
                return PoolStopV1.model_validate(row.stop_json)
            if lease.desired_state not in {"cancel", "retry", "timeout", "delete_pending"}:
                return None
            if lease.revoked_at is None or lease.cleanup_deadline_at is None:
                raise PoolHandoffError
            saved = self._view(row)
            assert saved.activated is not None and saved.activated.plan_sha256 is not None
            cause: Literal["completed", "failed", "cancelled", "lease_lost", "deadline"]
            if lease.desired_state == "cancel":
                cause = "cancelled"
            elif lease.desired_state == "timeout":
                cause = "deadline"
            else:
                cause = "completed" if lease.output_commit_state == "committed" else "failed"
            stop = PoolStopV1(action=saved.action, reservation_id=saved.activated.reservation_id,
                plan_sha256=saved.activated.plan_sha256, lease_generation=saved.request.execution.lease_generation,
                cause=cause, grace_deadline_at=min(lease.cleanup_deadline_at, await _clock(session) + timedelta(seconds=300)))
            row.stop_json, row.phase = stop.model_dump(mode="json"), "stop_pending"
            await session.flush()
            return stop

    @staticmethod
    def _output_evidence(lease: ServiceExecutionLease) -> dict[str, Any]:
        return {"lease_id": str(lease.id), "output_state": lease.output_commit_state,
            "output_generation": lease.output_generation,
            "upload_session_id": str(lease.output_upload_session_id) if lease.output_upload_session_id else None,
            "manifest_sha256": lease.output_manifest_sha256, "marker_sha256": lease.output_marker_sha256,
            "committed_at": lease.output_committed_at.isoformat() if lease.output_committed_at else None,
            "unavailable_reason": lease.output_unavailable_reason}

    async def begin_drain(self, key: PoolRequestKeyV1) -> PoolDrainV1 | None:
        """Freeze committed/unavailable output evidence; never close its window here."""
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            lease = await self._cleanup_lease(session, row)
            if row.stop_json is None:
                return None
            if lease.output_commit_state not in {"committed", "unavailable"}:
                return None
            if lease.output_generation != lease.resource_generation:
                raise PoolHandoffError
            output = self._output_evidence(lease)
            if row.drain_json is not None:
                if output != row.output_json:
                    raise PoolHandoffError
                return PoolDrainV1.model_validate(row.drain_json)
            stop = PoolStopV1.model_validate(row.stop_json)
            drain = PoolDrainV1(action=stop.action, reservation_id=stop.reservation_id,
                plan_sha256=stop.plan_sha256, lease_generation=stop.lease_generation,
                stop_sha256=canonical_digest(row.stop_json).removeprefix("sha256:"),
                output_generation=lease.resource_generation,
                output_state="committed" if lease.output_commit_state == "committed" else "unavailable",
                evidence_sha256=canonical_digest(output).removeprefix("sha256:"))
            row.output_json, row.drain_json = output, drain.model_dump(mode="json")
            await session.flush()
            return drain

    async def _finish_unstarted_claim(self, session: AsyncSession, row: NebiusPoolExecutionOutbox) -> None:
        if row.attached_lease_id is None:
            return
        trial = await session.get(Trial, row.trial_id, with_for_update=True)
        lease = await session.get(ServiceExecutionLease, row.attached_lease_id, with_for_update=True)
        if (trial is None or lease is None or lease.trial_id != trial.id or lease.attempt != trial.attempt_count
                or lease.job_uid is not None or lease.pod_uid is not None or lease.pod_started_at is not None
                or lease.output_commit_state not in {"not_started", "unavailable"}):
            raise PoolHandoffError
        now = await _clock(session)
        if lease.desired_state == "create":
            await enqueue_execution_transition(session, lease_id=lease.id, expected_generation=lease.generation,
                desired_state=("retry" if trial.state == "claimed" and trial.cancellation_requested_at is None else "cancel"), now=now)
        if lease.desired_state not in {"cancel", "retry", "timeout", "delete_pending"} or lease.revoked_at is None:
            raise PoolHandoffError
        # This path has the manager's exact cancelled_unstarted evidence, not
        # a local timeout or GET404. No runtime/output writer ever existed.
        lease.output_commit_state, lease.output_generation = "unavailable", lease.resource_generation
        lease.output_unavailable_reason = "pool_cancelled_unstarted"
        observation = KubernetesJobObservation(namespace=lease.namespace_name, job_name=lease.job_name,
            lease_id=str(lease.id), resource_generation=lease.resource_generation, target_id=lease.target_id,
            execution_unit_key=str(lease.execution_unit_key), normalized_state=NormalizedJobState.DELETED)
        await record_kubernetes_observation(session, lease_id=lease.id, generation=lease.generation,
            payload=observation.event_payload(), observed_at=now)

    async def confirm_release(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolExecutionHandoff:
        """Project deletion only with the exact manager's qualified cleanup receipt."""
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            receipt = self._receipt(row, receipt)
            saved = self._view(row)
            if (receipt.phase != "released" or row.phase not in {"stop_pending", "released"}
                    or saved.activated is None or receipt.plan_sha256 != saved.activated.plan_sha256
                    or (saved.activated.job_uid is not None and receipt.job_uid != saved.activated.job_uid)
                    or row.stop_json is None or row.drain_json is None or row.output_json is None):
                raise PoolHandoffError
            if row.released_json is not None:
                if row.released_json != receipt.model_dump(mode="json"):
                    raise PoolHandoffError
                return saved
            lease = await self._cleanup_lease(session, row)
            drain = PoolDrainV1.model_validate(row.drain_json)
            job_uid = str(receipt.job_uid) if receipt.job_uid is not None else None
            if (lease.desired_state not in {"cancel", "retry", "timeout", "delete_pending"}
                    or (lease.job_uid is not None and lease.job_uid != job_uid)
                    or lease.output_generation != lease.resource_generation
                    or self._output_evidence(lease) != row.output_json
                    or drain.stop_sha256 != canonical_digest(row.stop_json).removeprefix("sha256:")
                    or drain.evidence_sha256 != canonical_digest(row.output_json).removeprefix("sha256:")):
                raise PoolHandoffError
            observation = KubernetesJobObservation(namespace=lease.namespace_name, job_name=lease.job_name,
                lease_id=str(lease.id), resource_generation=lease.resource_generation, target_id=lease.target_id,
                execution_unit_key=str(lease.execution_unit_key), normalized_state=NormalizedJobState.DELETED,
                job_uid=job_uid, reason="PoolCleanupConfirmed")
            await record_kubernetes_observation(session, lease_id=lease.id, generation=lease.generation,
                payload=observation.event_payload(), observed_at=await _clock(session))
            row.phase, row.released_json = "released", receipt.model_dump(mode="json")
            await session.flush()
            return self._view(row)

    async def confirm_cancel(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolExecutionHandoff:
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            receipt = self._receipt(row, receipt)
            if row.phase not in {"cancel_pending", "cancelled"} or receipt.phase != "cancelled_unstarted":
                raise PoolHandoffError
            if row.phase == "cancelled":
                if row.cancelled_json != receipt.model_dump(mode="json"):
                    raise PoolHandoffError
                return self._view(row)
            if row.reservation_id is None:
                row.reservation_id, row.receipt_json = receipt.reservation_id, receipt.model_dump(mode="json")
            await self._finish_unstarted_claim(session, row)
            row.phase, row.cancelled_json = "cancelled", receipt.model_dump(mode="json")
            await session.flush()
            return self._view(row)

    async def accept_grant(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolExecutionHandoff:
        receipt = PoolReceiptV1.model_validate_json(receipt.model_dump_json())
        async with self.sessions.begin() as session:
            row = await self._load(session, key)
            saved = self._view(row)
            request = saved.request
            if (receipt.phase != "reserved" or receipt.pool_id != request.pool_id or receipt.request_key != request.key
                    or receipt.admission_epoch != request.admission_epoch or receipt.request_sha256 != row.request_sha256
                    or (row.reservation_id is not None and row.reservation_id != receipt.reservation_id)
                    or row.phase == "cancelled"):
                raise PoolHandoffError
            if row.attached_lease_id is not None:
                return saved
            row.reservation_id, row.receipt_json = receipt.reservation_id, receipt.model_dump(mode="json")
            if row.phase == "selected":
                row.phase = "grant_pending"
            await session.flush()
            now = await _clock(session)
            if (row.phase == "cancel_pending" or request.admission_epoch != self.participant.admission_epoch
                    or request.participant_revision != self.participant.binding_revision or request.deadline_at <= now):
                row.phase = "cancel_pending"
            else:
                try:
                    async with session.begin_nested():
                        self.participant.target(request.target_id, "trial")
                        candidate = (await session.execute(_SERVICE_TRIAL_BY_ID,
                            {"trial_id": row.trial_id, "pool_id": self.logical_pool_id, "now": now})).mappings().one_or_none()
                        if candidate is None:
                            raise PoolHandoffError
                        target = await session.get(ServiceExecutionTarget, request.target_id, with_for_update=True)
                        if target is None or target.spec_json["namespace_name"] != self.participant.execution_namespace.name:
                            raise PoolHandoffError
                        lease = await reserve_trial_execution(session, request_id=row.lease_id, trial_id=row.trial_id,
                            execution_class_id=request.execution.runtime.execution_class_id, target_id=request.target_id,
                            requirements=request.execution.requirements, runtime_contract=request.execution.runtime,
                            image_admission_keyring=self.image_admission_keyring,
                            routing_reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
                            deadline_at=request.deadline_at, now=now, pool_handoff_id=row.lease_id)
                        row.attached_lease_id, row.phase = lease.id, "attached"
                        await session.flush()
                except (ServiceExecutionConflict, ExecutionProvisioningBlockedError, PoolHandoffError):
                    row.phase = "cancel_pending"
            await session.flush()
            return self._view(row)
