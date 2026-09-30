"""Environment-owned execution proposals and exact post-grant claims.

Public operations commit before returning. Callers perform management HTTP only
between these operations; no method can create a Kubernetes resource.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import ServiceExecutionTarget, Trial
from loom.execution_contract import ExecutionRoutingReason
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_pool_contract import (
    PoolParticipantV1,
    PoolReceiptV1,
    PoolRequestActionV1,
    PoolRequestKeyV1,
)
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.nebius_pool_workload import PoolExecutionPrepareV1, PoolExecutionWorkloadV1
from loom.nebius_rollout_guard import admission_open
from loom.pipeline.keys import canonical_digest
from loom.task_bundle_source_journal import require_task_bundle_transaction
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.execution_resource_allocation import allocate_target_resources
from loom_control_plane.pool_execution_handoff import execution_selection_snapshot
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    _execution_identity,
    reserve_trial_execution,
)
from loom_control_plane.service_execution_scheduler import (
    _SERVICE_TRIAL_BY_ID,
    _compile_service_candidate,
)
from loom_execution_actuator.pool_outbox import PoolHandoffError


@dataclass(frozen=True)
class PoolExecutionHandoff:
    request: PoolExecutionPrepareV1
    request_sha256: str
    phase: str
    reservation_id: UUID | None
    lease_id: UUID | None

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
        return PoolExecutionHandoff(request, row.request_sha256, row.phase, row.reservation_id, row.attached_lease_id)

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

    async def propose(self, *, trial_id: UUID, target_id: str) -> PoolExecutionHandoff:
        """Freeze actual compiled input without claiming an execution or budget."""
        self.participant.target(target_id, "trial")
        async with self.sessions.begin() as session:
            await require_task_bundle_transaction(session)
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 171))"),
                {"key": "pool-execution-selection:" + str(trial_id)})
            existing = await session.scalar(select(NebiusPoolExecutionOutbox).where(
                NebiusPoolExecutionOutbox.trial_id == trial_id,
                NebiusPoolExecutionOutbox.phase != "cancelled").with_for_update())
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
            compiled = await _compile_service_candidate(session, row=candidate, environment=self.environment,
                pool_id=self.logical_pool_id, maximum_deadline_seconds=self.maximum_deadline_seconds, current_time=now)
            if compiled is None or target_id not in {target.id for target in compiled.targets}:
                raise PoolHandoffError
            runtime = (await allocate_target_resources(session, compiled.runtime_plan, target_id=target_id, now=now)
                       if compiled.allocate_resources else compiled.runtime_plan)
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
                    parent_lease_id=None, requirements=compiled.requirements, runtime=runtime))
            payload = request.model_dump(mode="json")
            row = NebiusPoolExecutionOutbox(lease_id=lease_id, trial_id=trial_id, pool_id=request.pool_id,
                participant_id=request.key.participant_id, request_json=payload,
                request_sha256=canonical_digest(payload).removeprefix("sha256:"),
                selection_json=execution_selection_snapshot(trial, candidate, target, runtime), phase="selected")
            session.add(row)
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
