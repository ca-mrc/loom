"""Short, lease-fenced transactions for personal builds in the common pool.

Only the common pool can attest capacity release. No method holds a SQL lock
across HTTP/Kubernetes I/O or acquires a pool lock after a build-history lock.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolActivationV1, PoolReceiptV1, PoolRequestActionV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.pipeline.keys import canonical_digest
from loom_service.application_management.build_dispatch import ApplicationBuildDispatch
from loom_service.application_management.build_publication import observed_publication
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.environment_management.registry import ManagementError

_TERMINAL = {"ready", "failed", "cancelled"}


def _digest(value: dict[str, Any]) -> str:
    return canonical_digest(value).removeprefix("sha256:")


@dataclass(frozen=True)
class ApplicationBuildLease:
    build_id: UUID
    attempt: int
    runner_epoch: int
    lease_token: UUID


@dataclass(frozen=True)
class ApplicationBuildState:
    phase: str
    desired_state: str
    now: datetime
    request: PoolApplicationImagePrepareV1 | None
    grant: PoolReceiptV1 | None
    activation: PoolActivationV1 | None
    activated: PoolReceiptV1 | None
    settlement: dict[str, Any] | None

    @property
    def action(self) -> PoolRequestActionV1:
        if self.request is None:
            raise ManagementError("application_build_not_dispatched")
        return PoolRequestActionV1(pool_id=self.request.pool_id, request_key=self.request.key,
            admission_epoch=self.request.admission_epoch, request_sha256=_digest(self.request.model_dump(mode="json")))

    @property
    def must_cancel(self) -> bool:
        return (self.desired_state == "cancelled" or (self.request is not None and self.request.deadline_at <= self.now)
            or (self.activated is None and self.activation is not None and self.activation.not_after <= self.now))


class ApplicationBuildJournal:
    def __init__(self, registry: ApplicationBuildRegistry):
        self.registry, self.dispatch = registry, ApplicationBuildDispatch(registry)
        self.session_factory = registry.session_factory

    def _scope(self) -> tuple[Any, ...]:
        scope = self.registry.binding.source
        return (NebiusApplicationBuild.installation_id == scope.installation_id,
            NebiusApplicationBuild.data_environment_id == scope.data_environment_id,
            NebiusApplicationBuild.cluster_id == scope.cluster_id,
            NebiusApplicationBuild.current_attempt == NebiusApplicationBuildAttempt.attempt)

    async def _locked(self, session: AsyncSession, build_id: UUID, attempt: int
                      ) -> tuple[NebiusApplicationBuild, NebiusApplicationBuildAttempt, datetime]:
        row = await session.scalar(select(NebiusApplicationBuild).join(NebiusApplicationBuildAttempt).where(
            *self._scope(), NebiusApplicationBuild.build_id == build_id,
            NebiusApplicationBuildAttempt.attempt == attempt).with_for_update(of=NebiusApplicationBuild))
        saved = await session.get(NebiusApplicationBuildAttempt, (build_id, attempt), with_for_update=True) if row else None
        if row is None or saved is None:
            raise ManagementError("application_build_attempt_unavailable")
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return row, saved, now

    async def _leased(self, session: AsyncSession, lease: ApplicationBuildLease
                      ) -> tuple[NebiusApplicationBuild, NebiusApplicationBuildAttempt, datetime]:
        row, saved, now = await self._locked(session, lease.build_id, lease.attempt)
        if (saved.phase in _TERMINAL or saved.runner_epoch != lease.runner_epoch or saved.lease_token != lease.lease_token
                or saved.lease_expires_at is None or saved.lease_expires_at <= now):
            raise ManagementError("stale_application_build_lease")
        return row, saved, now

    @staticmethod
    def _duration(seconds: int) -> timedelta:
        if type(seconds) is not int or not 3 <= seconds <= 300:
            raise ValueError("invalid_application_build_lease_duration")
        return timedelta(seconds=seconds)

    async def claim(self, build_id: UUID, *, attempt: int, lease_seconds: int = 60) -> ApplicationBuildLease | None:
        duration = self._duration(lease_seconds)
        async with self.session_factory.begin() as session:
            _, saved, now = await self._locked(session, build_id, attempt)
            if saved.phase in _TERMINAL or (saved.lease_expires_at is not None and saved.lease_expires_at > now):
                return None
            saved.runner_epoch += 1
            saved.lease_token, saved.lease_expires_at = uuid4(), now + duration
            return ApplicationBuildLease(build_id, attempt, saved.runner_epoch, saved.lease_token)

    async def renew(self, lease: ApplicationBuildLease, *, lease_seconds: int = 60) -> None:
        duration = self._duration(lease_seconds)
        async with self.session_factory.begin() as session:
            _, saved, now = await self._leased(session, lease)
            saved.lease_expires_at = now + duration

    async def release(self, lease: ApplicationBuildLease) -> None:
        async with self.session_factory.begin() as session:
            _, saved, _ = await self._leased(session, lease)
            saved.lease_token = saved.lease_expires_at = None

    async def pending(self, *, after: UUID | None = None, limit: int = 16) -> list[tuple[UUID, int]]:
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("invalid_application_build_scan_limit")
        async with self.session_factory() as session:
            query = select(NebiusApplicationBuild.build_id, NebiusApplicationBuild.current_attempt).join(
                NebiusApplicationBuildAttempt).where(*self._scope(),
                NebiusApplicationBuildAttempt.phase.not_in(_TERMINAL),
                NebiusApplicationBuildAttempt.lease_expires_at.is_(None)
                | (NebiusApplicationBuildAttempt.lease_expires_at <= func.clock_timestamp()))
            if after is not None:
                query = query.where(NebiusApplicationBuild.build_id > after)
            rows = (await session.execute(query.order_by(NebiusApplicationBuild.build_id).limit(limit))).all()
            return [(row[0], row[1]) for row in rows]

    @staticmethod
    def _view(row: NebiusApplicationBuild, saved: NebiusApplicationBuildAttempt, now: datetime) -> ApplicationBuildState:
        request = PoolApplicationImagePrepareV1.model_validate(saved.pool_request_json) if saved.pool_request_json else None
        if request is not None and (_digest(saved.pool_request_json or {}) != saved.pool_request_sha256
                or request.build.model_dump(mode="json") != saved.claim_json):
            raise ManagementError("application_build_history_conflict")
        return ApplicationBuildState(saved.phase, row.desired_state, now, request,
            PoolReceiptV1.model_validate(saved.grant_json) if saved.grant_json else None,
            PoolActivationV1.model_validate(saved.activation_json) if saved.activation_json else None,
            PoolReceiptV1.model_validate(saved.activated_json) if saved.activated_json else None,
            copy.deepcopy(saved.settlement_json))

    async def state(self, lease: ApplicationBuildLease) -> ApplicationBuildState:
        async with self.session_factory.begin() as session:
            return self._view(*(await self._leased(session, lease)))

    @staticmethod
    def _receipt(state: ApplicationBuildState, receipt: PoolReceiptV1) -> None:
        action = state.action
        if (receipt.pool_id, receipt.request_key, receipt.admission_epoch, receipt.request_sha256) != (
                action.pool_id, action.request_key, action.admission_epoch, action.request_sha256):
            raise ManagementError("application_build_receipt_conflict")
        # Snapshots may skip intermediate phases. Still reject rewinds and loss
        # or substitution of any previously committed identity/evidence.
        ranks = {"reserved": 0, "create_intent": 1, "observed": 2, "cleanup_intent": 3, "released": 4,
            "cancelled_unstarted": 4}
        previous = [state.grant, state.activated]
        if state.settlement is not None:
            previous.append(PoolNativeRuntimeV1.model_validate(state.settlement["runtime"]).receipt)
        for before in previous:
            if before is None:
                continue
            if before.reservation_id != receipt.reservation_id or ranks[receipt.phase] < ranks[before.phase]:
                raise ManagementError("application_build_receipt_conflict")
            for name in ("plan_sha256", "job_uid", "cleanup_observation_id"):
                value = getattr(before, name)
                if value is not None and value != getattr(receipt, name):
                    raise ManagementError("application_build_receipt_conflict")

    async def accept_grant(self, lease: ApplicationBuildLease, receipt: PoolReceiptV1) -> None:
        async with self.session_factory.begin() as session:
            row, saved, now = await self._leased(session, lease)
            self._receipt(self._view(row, saved, now), receipt)
            if saved.grant_json is None:
                saved.grant_json = receipt.model_dump(mode="json")

    async def begin_activation(self, lease: ApplicationBuildLease) -> PoolActivationV1 | None:
        async with self.session_factory.begin() as session:
            row, saved, now = await self._leased(session, lease)
            state = self._view(row, saved, now)
            if state.must_cancel:
                return None
            if saved.phase != "queued" or state.grant is None or state.request is None:
                raise ManagementError("application_build_activation_conflict")
            if state.activation is not None:
                return state.activation
            assert saved.lease_expires_at is not None
            activation = PoolActivationV1(action=state.action, not_after=min(saved.lease_expires_at, state.request.deadline_at))
            saved.activation_json = activation.model_dump(mode="json")
            return activation

    async def activated(self, lease: ApplicationBuildLease, receipt: PoolReceiptV1) -> None:
        async with self.session_factory.begin() as session:
            row, saved, now = await self._leased(session, lease)
            state = self._view(row, saved, now)
            self._receipt(state, receipt)
            if state.activation is None or receipt.plan_sha256 is None:
                raise ManagementError("application_build_activation_conflict")
            if saved.activated_json is None:
                saved.activated_json = receipt.model_dump(mode="json")
                saved.phase = "running"

    async def cancelled_unstarted(self, lease: ApplicationBuildLease, receipt: PoolReceiptV1 | None) -> None:
        async with self.session_factory.begin() as session:
            row, saved, now = await self._leased(session, lease)
            state = self._view(row, saved, now)
            if state.activated is not None:
                raise ManagementError("application_build_cancellation_conflict")
            if state.request is not None:
                if receipt is None or receipt.phase != "cancelled_unstarted":
                    raise ManagementError("application_build_cancellation_conflict")
                self._receipt(state, receipt)
                saved.terminal_receipt_json = receipt.model_dump(mode="json")
            elif receipt is not None or row.desired_state != "cancelled":
                raise ManagementError("application_build_cancellation_conflict")
            saved.phase = "cancelled" if row.desired_state == "cancelled" else "failed"
            saved.lease_token = saved.lease_expires_at = None

    async def observe(self, lease: ApplicationBuildLease, runtime: PoolNativeRuntimeV1,
                      observed: dict[str, Any] | None = None) -> ApplicationBuildState:
        async with self.session_factory.begin() as session:
            row, saved, now = await self._leased(session, lease)
            state = self._view(row, saved, now)
            self._receipt(state, runtime.receipt)
            request = state.request
            if (request is None or state.activated is None or runtime.target_id != request.target_id
                    or runtime.lease_epoch != request.build.attempt or runtime.deadline_at != request.deadline_at
                    or runtime.registry_repository != request.build.registry_repository):
                raise ManagementError("application_build_runtime_conflict")
            if state.settlement is not None:
                retained = PoolNativeRuntimeV1.model_validate(state.settlement["runtime"])
                if (runtime.namespace != retained.namespace or runtime.job_name != retained.job_name
                        or (retained.job_effect_id is not None and runtime.job_effect_id != retained.job_effect_id)):
                    raise ManagementError("application_build_runtime_conflict")
            if runtime.receipt.phase == "released":
                saved.terminal_receipt_json = runtime.receipt.model_dump(mode="json")
                saved.phase = ("cancelled" if row.desired_state == "cancelled" else
                    state.settlement["outcome"] if state.settlement is not None else "failed")
                saved.lease_token = saved.lease_expires_at = None
                return self._view(row, saved, now)
            if state.settlement is not None:
                return state
            outcome: Literal["pending", "ready", "failed", "cancelled"] = "pending"
            cause: Literal["completed", "failed", "cancelled", "deadline"] = "failed"
            publication = None
            if row.desired_state == "cancelled":
                outcome, cause = "cancelled", "cancelled"
            elif request.deadline_at <= now:
                outcome, cause = "failed", "deadline"
            elif runtime.receipt.phase not in {"create_intent", "observed"}:
                outcome = "failed"
            elif observed is not None:
                outcome, publication = observed_publication(request, runtime, observed)
                cause = "completed" if outcome == "ready" else "failed"
            if outcome != "pending":
                assert runtime.receipt.plan_sha256 is not None
                output = {"outcome": outcome, "cause": cause,
                    "publication": publication.model_dump(mode="json") if publication is not None else None}
                stop = PoolStopV1(action=state.action, reservation_id=runtime.receipt.reservation_id,
                    plan_sha256=runtime.receipt.plan_sha256, lease_generation=request.build.attempt, cause=cause,
                    grace_deadline_at=min(request.deadline_at, now + timedelta(seconds=30)))
                drain = PoolDrainV1(action=state.action, reservation_id=stop.reservation_id,
                    plan_sha256=stop.plan_sha256, lease_generation=stop.lease_generation,
                    stop_sha256=_digest(stop.model_dump(mode="json")), output_generation=request.build.attempt,
                    output_state="committed" if publication is not None else "unavailable", evidence_sha256=_digest(output))
                saved.settlement_json = {**output, "runtime": runtime.model_dump(mode="json"),
                    "stop": stop.model_dump(mode="json"), "drain": drain.model_dump(mode="json")}
                saved.phase = "settling"
            return self._view(row, saved, now)
