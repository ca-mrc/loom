"""Durable native selection and exact-claim handoff; no network or Job authority.

Each public mutation owns and commits its transaction before returning. A caller
may then perform prepare/activate/cancel HTTP using the retained request/action.
Waiting and uncertain replies leave the same selection, without an attempt.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom.nebius_pool_contract import (
    PoolActivationV1,
    PoolParticipantV1,
    PoolReceiptV1,
    PoolRequestActionV1,
    PoolRequestKeyV1,
)
from loom.nebius_pool_task_image import PoolRegisteredBuildSourceV1, PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from loom.task_bundle_source_journal import require_task_bundle_transaction
from loom.task_image_materialization import admit_task_image_source
from loom_control_plane.task_image_materializations import (
    claim_task_image_materialization,
)
from loom_execution_actuator.pool_origins import preferred_task_image_origin


class PoolHandoffError(ValueError):
    def __init__(self) -> None:
        super().__init__("pool local handoff unavailable")


@dataclass(frozen=True)
class PoolBuildHandoff:
    request: PoolTaskImagePrepareV1
    request_sha256: str
    phase: str
    reservation_id: UUID | None
    attempt_id: UUID | None
    activation: PoolActivationV1 | None
    activated: PoolReceiptV1 | None

    @property
    def action(self) -> PoolRequestActionV1:
        return PoolRequestActionV1(pool_id=self.request.pool_id, request_key=self.request.key,
            admission_epoch=self.request.admission_epoch, request_sha256=self.request_sha256)


def _digest(request: PoolTaskImagePrepareV1) -> str:
    return canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:")


async def _clock(session: AsyncSession) -> datetime:
    return (await session.execute(select(func.clock_timestamp()))).scalar_one()  # type: ignore[no-any-return]


def _snapshot(row: TaskImageMaterialization) -> dict[str, Any]:
    return {name: getattr(row, name) for name in (
        "materialization_key", "task_id", "task_checksum", "cpu_arch", "task_config", "task_source",
        "task_source_provenance", "bundle_content_manifest_sha256")}


def _source_matches(request: PoolTaskImagePrepareV1, row: TaskImageMaterialization) -> bool:
    expected = request.build.claim_snapshot()
    actual = _snapshot(row)
    provenance = expected.pop("task_source_provenance")
    digest = request.build.source.registration.manifest.digest if isinstance(request.build.source, PoolRegisteredBuildSourceV1) else ""
    return (all(actual[name] == value for name, value in expected.items())
            and all(row.task_source_provenance.get(name) == value for name, value in provenance.items())
            and row.bundle_content_manifest_sha256 == digest)


class PoolBuildOutbox:
    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], participant: PoolParticipantV1,
                 logical_pool_id: str, builder_id: str) -> None:
        self.sessions = sessions
        self.participant = PoolParticipantV1.model_validate_json(participant.model_dump_json())
        if (not builder_id.strip() or len(builder_id) > 128 or not logical_pool_id.strip()
                or len(logical_pool_id) > 80):
            raise PoolHandoffError
        self.builder_id, self.logical_pool_id = builder_id, logical_pool_id

    def _key(self, key: PoolRequestKeyV1) -> PoolRequestKeyV1:
        key = PoolRequestKeyV1.model_validate_json(key.model_dump_json())
        if key.participant_id != self.participant.participant_id or key.workload_kind != "task_image_build":
            raise PoolHandoffError
        return key

    def _request(self, request: PoolTaskImagePrepareV1) -> PoolTaskImagePrepareV1:
        request = PoolTaskImagePrepareV1.model_validate_json(request.model_dump_json())
        self._key(request.key)
        if (request.pool_id != self.participant.pool_id or request.admission_epoch != self.participant.admission_epoch
                or request.participant_revision != self.participant.binding_revision
                or request.origin.data_environment_id != self.participant.environment_id):
            raise PoolHandoffError
        self.participant.target(request.target_id, request.key.workload_kind)
        return request

    def _view(self, row: NebiusPoolBuildOutbox) -> PoolBuildHandoff:
        # Current epoch/profile authorizes new claims, not access to retained
        # history. Recovery must still be able to cancel old unstarted grants.
        request = PoolTaskImagePrepareV1.model_validate(row.request_json)
        self._key(request.key)
        if (row.request_sha256 != _digest(request) or row.builder_id != self.builder_id
                or row.logical_pool_id != self.logical_pool_id or request.pool_id != self.participant.pool_id
                or request.origin.data_environment_id != self.participant.environment_id):
            raise PoolHandoffError
        return PoolBuildHandoff(request, row.request_sha256, row.phase, row.reservation_id, row.attempt_id,
            PoolActivationV1.model_validate(row.activation_json) if row.activation_json is not None else None,
            PoolReceiptV1.model_validate(row.activated_json) if row.activated_json is not None else None)

    @asynccontextmanager
    async def _transaction(self, key: PoolRequestKeyV1) -> AsyncIterator[tuple[AsyncSession, NebiusPoolBuildOutbox | None]]:
        key = self._key(key)
        async with self.sessions.begin() as session:
            await require_task_bundle_transaction(session)
            # Same materialization lock key even across different participants.
            # Order is local-selection advisory -> outbox -> materialization.
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 171))"),
                                  {"key": "pool-native-selection:" + str(key.local_work_id)})
            row = await session.scalar(select(NebiusPoolBuildOutbox).where(
                NebiusPoolBuildOutbox.participant_id == key.participant_id,
                NebiusPoolBuildOutbox.materialization_id == key.local_work_id,
                NebiusPoolBuildOutbox.generation == key.generation,
            ).with_for_update())
            if row is not None:
                self._view(row)
            yield session, row

    async def get(self, key: PoolRequestKeyV1) -> PoolBuildHandoff:
        async with self._transaction(key) as (_, row):
            if row is None:
                raise PoolHandoffError
            return self._view(row)

    async def pending(self, *, limit: int = 100) -> tuple[PoolBuildHandoff, ...]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise PoolHandoffError
        async with self.sessions() as session:
            rows = await session.scalars(select(NebiusPoolBuildOutbox).where(
                NebiusPoolBuildOutbox.participant_id == self.participant.participant_id,
                NebiusPoolBuildOutbox.phase != "cancelled",
            ).order_by(NebiusPoolBuildOutbox.created_at, NebiusPoolBuildOutbox.outbox_id).limit(limit))
            return tuple(self._view(row) for row in rows)

    async def _eligible(self, session: AsyncSession, request: PoolTaskImagePrepareV1,
                        row: TaskImageMaterialization) -> bool:
        try:
            self._request(request)
        except ValueError:
            return False
        now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        if (row.lease_epoch != request.build.expected_lease_epoch or row.attempt_count >= row.max_attempts
                or request.deadline_at <= now or not _source_matches(request, row)):
            return False
        available = ((row.state == "queued" and (row.next_attempt_at is None or row.next_attempt_at <= now))
            or (row.state in {"claimed", "running"} and row.lease_expires_at is not None and row.lease_expires_at <= now))
        return available and request.origin == await preferred_task_image_origin(session,
            materialization_id=row.id, participant=self.participant, logical_pool_id=self.logical_pool_id)

    async def remember(self, request: PoolTaskImagePrepareV1) -> PoolBuildHandoff:
        request = self._request(request)
        async with self._transaction(request.key) as (session, existing):
            if existing is not None:
                if existing.request_json != request.model_dump(mode="json") or existing.request_sha256 != _digest(request):
                    raise PoolHandoffError
                return self._view(existing)
            latest, live = (await session.execute(select(
                func.max(NebiusPoolBuildOutbox.generation),
                func.count().filter(NebiusPoolBuildOutbox.phase != "cancelled"),
            ).where(
                NebiusPoolBuildOutbox.materialization_id == request.key.local_work_id,
            ))).one()
            if live or (latest is not None and latest >= request.key.generation):
                raise PoolHandoffError
            selected = await session.get(TaskImageMaterialization, request.key.local_work_id, with_for_update=True)
            if selected is None or not await self._eligible(session, request, selected):
                raise PoolHandoffError
            from loom.nebius_rollout_guard import admission_open

            if not await admission_open(session):
                raise PoolHandoffError
            await admit_task_image_source(session, row=selected)
            row = NebiusPoolBuildOutbox(outbox_id=uuid4(), pool_id=request.pool_id,
                participant_id=request.key.participant_id, materialization_id=request.key.local_work_id,
                generation=request.key.generation, admission_epoch=request.admission_epoch,
                builder_id=self.builder_id, logical_pool_id=self.logical_pool_id,
                request_sha256=_digest(request), request_json=request.model_dump(mode="json"),
                selection_json=_snapshot(selected), phase="selected")
            session.add(row)
            await session.flush()
            return self._view(row)

    def _receipt(self, row: NebiusPoolBuildOutbox, receipt: PoolReceiptV1) -> PoolReceiptV1:
        receipt = PoolReceiptV1.model_validate_json(receipt.model_dump_json())
        action = self._view(row).action
        if (receipt.pool_id != action.pool_id or receipt.request_key != action.request_key
                or receipt.admission_epoch != action.admission_epoch or receipt.request_sha256 != action.request_sha256
                or (row.reservation_id is not None and row.reservation_id != receipt.reservation_id)):
            raise PoolHandoffError
        return receipt

    async def accept_grant(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolBuildHandoff:
        async with self._transaction(key) as (session, row):
            if row is None:
                raise PoolHandoffError
            receipt = self._receipt(row, receipt)
            if receipt.phase != "reserved" or row.phase == "cancelled":
                raise PoolHandoffError
            if row.attempt_id is not None:
                # A delayed prepare reply recovers identity, never rewinds a
                # later activation/cancellation phase or grants new consent.
                return self._view(row)
            request = self._view(row).request
            selected = await session.get(TaskImageMaterialization, key.local_work_id, with_for_update=True)
            attempt = None
            if (row.phase == "selected" and selected is not None and _snapshot(selected) == row.selection_json
                    and await self._eligible(session, request, selected)):
                try:
                    # Registered-source denial rolls back its own source pins too.
                    async with session.begin_nested():
                        claimed = await claim_task_image_materialization(session, builder_id=self.builder_id,
                            cpu_arch=request.build.cpu_arch, nebius_pool_id=self.logical_pool_id,
                            materialization_id=key.local_work_id, expected_lease_epoch=request.build.expected_lease_epoch)
                        if claimed is not None:
                            attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
                                TaskImageMaterializationAttempt.materialization_id == claimed.id,
                                TaskImageMaterializationAttempt.lease_epoch == claimed.lease_epoch))
                            if attempt is None:
                                raise PoolHandoffError
                except ValueError:
                    attempt = None
            if row.reservation_id is None:
                row.reservation_id, row.receipt_json = receipt.reservation_id, receipt.model_dump(mode="json")
            row.phase = "cancel_pending" if attempt is None else "attached"
            if attempt is not None:
                row.attempt_id, row.attempt_number, row.lease_epoch = attempt.id, attempt.attempt_number, attempt.lease_epoch
            await session.flush()
            return self._view(row)

    async def _claim_current(self, session: AsyncSession, outbox: NebiusPoolBuildOutbox, *,
                             recheck_origin: bool = True) -> TaskImageMaterialization | None:
        request = self._view(outbox).request
        try:
            self._request(request)
        except ValueError:
            return None
        row = await session.get(TaskImageMaterialization, outbox.materialization_id, with_for_update=True)
        attempt = await session.get(TaskImageMaterializationAttempt, outbox.attempt_id, with_for_update=True)
        now = await _clock(session)
        if (row is None or attempt is None or row.state != "claimed" or row.claimed_by != self.builder_id
                or row.lease_epoch != outbox.lease_epoch or row.attempt_count != outbox.attempt_number
                or row.lease_expires_at is None or row.lease_expires_at <= now or request.deadline_at <= now
                or _snapshot(row) != outbox.selection_json or not _source_matches(request, row)
                or attempt.native_build is not None or attempt.grant_id is not None):
            return None
        from loom.nebius_rollout_guard import admission_open

        origin = await preferred_task_image_origin(session, materialization_id=row.id,
            participant=self.participant, logical_pool_id=self.logical_pool_id)
        if (origin is None or (recheck_origin and origin != request.origin) or not await admission_open(session)):
            return None
        return row

    async def begin_activation(self, key: PoolRequestKeyV1) -> PoolBuildHandoff:
        """Commit bounded claim consent before HTTP; a replay must still qualify."""
        async with self._transaction(key) as (session, row):
            if row is None or row.phase == "selected":
                raise PoolHandoffError
            if row.phase not in {"attached", "activation_pending"}:
                return self._view(row)
            claim = await self._claim_current(session, row)
            now = await _clock(session)
            saved = self._view(row)
            if claim is None or (saved.activation is not None and saved.activation.not_after <= now):
                row.phase = "cancel_pending"
            else:
                if saved.activation is None:
                    assert claim.lease_expires_at is not None
                    row.activation_json = PoolActivationV1(action=saved.action,
                        not_after=min(claim.lease_expires_at, saved.request.deadline_at)).model_dump(mode="json")
                row.phase = "activation_pending"
            await session.flush()
            return self._view(row)

    async def confirm_activation(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolBuildHandoff:
        """Retain manager acceptance even after caller loss; never refund it."""
        async with self._transaction(key) as (session, row):
            if (row is None or row.activation_json is None
                    or row.phase not in {"activation_pending", "cancel_pending", "active", "stop_pending"}):
                raise PoolHandoffError
            receipt = self._receipt(row, receipt)
            if receipt.plan_sha256 is None:
                raise PoolHandoffError
            if row.activated_json is not None:
                previous = PoolReceiptV1.model_validate(row.activated_json)
                if (previous.plan_sha256 != receipt.plan_sha256
                        or (previous.job_uid is not None and previous.job_uid != receipt.job_uid)):
                    raise PoolHandoffError
                return self._view(row)
            current = await self._claim_current(session, row, recheck_origin=False)
            row.phase = ("active" if row.phase == "activation_pending" and current is not None
                and receipt.phase in {"create_intent", "observed"} else "stop_pending")
            row.activated_json = receipt.model_dump(mode="json")
            await session.flush()
            return self._view(row)

    async def request_cancel(self, key: PoolRequestKeyV1) -> PoolBuildHandoff:
        async with self._transaction(key) as (session, row):
            if row is None:
                raise PoolHandoffError
            if row.phase in {"selected", "attached", "activation_pending", "active"}:
                # Intent is not release: the manager may already have committed
                # activation after a lost reply. Only its terminal cancellation
                # receipt below can close/refund this retained local attempt.
                row.phase = "stop_pending" if row.phase == "active" else "cancel_pending"
                await session.flush()
            return self._view(row)

    async def _finish_unstarted_claim(self, session: AsyncSession, outbox: NebiusPoolBuildOutbox) -> None:
        """Retain a no-Job result and refund only this still-current claim.

        Called only with the exact manager cancelled_unstarted receipt, under
        the outbox lock. Unlike a live-build failure this may finish an expired
        lease; no native Job ever started, so do not invent publication evidence.
        Epochs/attempt identity never go backwards, matching native cancellation
        budget semantics. A superseding claim's budget belongs to that claim.
        """
        if outbox.attempt_id is None:
            return
        row = await session.get(TaskImageMaterialization, outbox.materialization_id, with_for_update=True)
        attempt = await session.get(TaskImageMaterializationAttempt, outbox.attempt_id, with_for_update=True)
        if row is None or attempt is None or attempt.native_build is not None or attempt.grant_id is not None:
            raise PoolHandoffError
        now: datetime = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        refundable = (row.lease_epoch == outbox.lease_epoch and row.claimed_by == self.builder_id
            and row.state == "claimed" and row.attempt_count == outbox.attempt_number
            and _snapshot(row) == outbox.selection_json)
        if refundable:
            row.attempt_count -= 1
            row.state, row.claimed_by, row.lease_expires_at = "queued", None, None
            row.next_attempt_at, row.finished_at = None, None
            row.failure_reason, row.failure_message = "build_cancelled", "Cancelled before global build activation"
            row.updated_at = now
        attempt.native_build = {"state": "cancelled_unstarted", "failure_reason": "build_cancelled",
            "failure_message": "Cancelled before global build activation",
            "pool_reservation_id": str(outbox.reservation_id), "retry_budget_refunded": refundable,
            "capacity_released_at": now.isoformat()}

    async def confirm_cancel(self, key: PoolRequestKeyV1, receipt: PoolReceiptV1) -> PoolBuildHandoff:
        async with self._transaction(key) as (session, row):
            if row is None or row.phase not in {"cancel_pending", "cancelled"}:
                raise PoolHandoffError
            receipt = self._receipt(row, receipt)
            if receipt.phase != "cancelled_unstarted":
                raise PoolHandoffError
            if row.phase == "cancelled":
                # Replay is evidence readback; never refund twice or modify a
                # newer lease that began after this cancellation committed.
                if row.cancelled_json != receipt.model_dump(mode="json"):
                    raise PoolHandoffError
                return self._view(row)
            if row.reservation_id is None:
                row.reservation_id, row.receipt_json = receipt.reservation_id, receipt.model_dump(mode="json")
            await self._finish_unstarted_claim(session, row)
            row.phase, row.cancelled_json = "cancelled", receipt.model_dump(mode="json")
            await session.flush()
            return self._view(row)
