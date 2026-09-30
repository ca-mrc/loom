"""Native global-handoff consumer: results, heartbeat and durable stop/drain.

No local capacity admission or Kubernetes writes. All SQL commits finish before
management/Kubernetes I/O. Local output evidence commits before stop/drain HTTP;
neither a terminal result nor those replies frees the charged reservation.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
)
from loom.nebius_pool_contract import PoolRequestKeyV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.pipeline.keys import canonical_digest
from loom_control_plane.task_image_materializations import heartbeat_task_image_materialization
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_native_observation import qualify_native_observation
from loom_execution_actuator.pool_origins import preferred_task_image_origin
from loom_execution_actuator.pool_outbox import PoolHandoffError, _clock, _snapshot, _source_matches
from loom_execution_actuator.task_image_controller import (
    fail_native_build,
    record_native_build_result,
)

StopCause = Literal["completed", "failed", "cancelled", "lease_lost", "deadline"]
_LOG = logging.getLogger(__name__)


def _digest(value: dict[str, Any]) -> str:
    return canonical_digest(value).removeprefix("sha256:")


class PoolNativeBuildApi(Protocol):
    async def observe_pool(self, runtime: PoolNativeRuntimeV1, *, capture_logs: bool = False) -> dict[str, Any] | None: ...


class PoolNativeBuildController:
    def __init__(self, *, driver: PoolBuildDriver, kubernetes: PoolNativeBuildApi) -> None:
        self.driver, self.kubernetes = driver, kubernetes

    async def _finish(self, session: AsyncSession, saved: NebiusPoolBuildOutbox,
                       attempt: TaskImageMaterializationAttempt, runtime: PoolNativeRuntimeV1, *,
                       now: datetime, cause: StopCause) -> None:
        """Save one attempt's real evidence and fixed messages before any HTTP."""
        native = dict(attempt.native_build or {})
        saved.phase = "stop_pending"
        if "pool_stop" in native:
            return  # Lost replies replay the original grace, cause and evidence.
        images = (await session.execute(select(TaskImagePublicationEvidence.component,
            TaskImagePublicationEvidence.registry_image).where(
                TaskImagePublicationEvidence.materialization_attempt_id == attempt.id)
            .order_by(TaskImagePublicationEvidence.component, TaskImagePublicationEvidence.registry_image))).all()
        state: Literal["committed", "unavailable"] = "committed" if cause == "completed" and images else "unavailable"
        output = {"attempt_id": str(attempt.id), "lease_epoch": attempt.lease_epoch,
            "cause": cause, "output_state": state, "registry_images": [list(pair) for pair in images],
            "diagnostics": {name: native[name] for name in ("failure_reason", "failure_message", "builder_log",
                "job_conditions", "pod_uid", "pod_status", "scheduling", "phases", "observed_at") if name in native}}
        handoff = self.driver.outbox._view(saved)
        assert runtime.receipt.plan_sha256 is not None
        stop = PoolStopV1(action=handoff.action, reservation_id=runtime.receipt.reservation_id,
            plan_sha256=runtime.receipt.plan_sha256, lease_generation=runtime.lease_epoch, cause=cause,
            grace_deadline_at=min(runtime.deadline_at, now + timedelta(seconds=30)))
        drain = PoolDrainV1(action=handoff.action, reservation_id=stop.reservation_id,
            plan_sha256=stop.plan_sha256, lease_generation=runtime.lease_epoch,
            stop_sha256=_digest(stop.model_dump(mode="json")), output_generation=runtime.lease_epoch,
            output_state=state, evidence_sha256=_digest(output))
        attempt.native_build = {**native, "pool_output": output,
            "pool_stop": stop.model_dump(mode="json"), "pool_drain": drain.model_dump(mode="json")}

    async def _lifecycle(self, key: PoolRequestKeyV1) -> tuple[PoolStopV1, PoolDrainV1] | None:
        outbox = self.driver.outbox
        async with outbox._transaction(key) as (session, saved):
            if saved is None or saved.phase != "stop_pending" or saved.attempt_id is None:
                return None
            attempt = await session.get(TaskImageMaterializationAttempt, saved.attempt_id)
            if attempt is None or attempt.native_build is None:
                raise PoolHandoffError
            native = attempt.native_build
            stop, drain = PoolStopV1.model_validate(native["pool_stop"]), PoolDrainV1.model_validate(native["pool_drain"])
            handoff = outbox._view(saved)
            if (handoff.activated is None or stop.action != handoff.action or drain.action != handoff.action
                    or stop.reservation_id != saved.reservation_id or drain.reservation_id != stop.reservation_id
                    or stop.plan_sha256 != handoff.activated.plan_sha256 or drain.plan_sha256 != stop.plan_sha256
                    or stop.lease_generation != attempt.lease_epoch or drain.lease_generation != attempt.lease_epoch
                    or drain.output_generation != attempt.lease_epoch
                    or drain.stop_sha256 != _digest(native["pool_stop"])
                    or drain.evidence_sha256 != _digest(native["pool_output"])):
                raise PoolHandoffError
            return stop, drain

    async def _record(self, runtime: PoolNativeRuntimeV1, observed: dict[str, Any] | None = None) -> bool:
        """Requalify both sides of external reads; stale work retains stop intent."""
        outbox = self.driver.outbox
        runtime = PoolNativeRuntimeV1.model_validate_json(runtime.model_dump_json())
        async with outbox._transaction(runtime.receipt.request_key) as (session, saved):
            if saved is None or saved.phase not in {"active", "stop_pending"} or saved.attempt_id is None:
                raise PoolHandoffError
            handoff = outbox._view(saved)
            outbox._receipt(saved, runtime.receipt)
            request = handoff.request
            if (handoff.activated is None or runtime.receipt.plan_sha256 != handoff.activated.plan_sha256
                    or (handoff.activated.job_uid is not None and runtime.receipt.job_uid != handoff.activated.job_uid)
                    or runtime.target_id != request.target_id or runtime.lease_epoch != request.build.expected_lease_epoch + 1
                    or runtime.deadline_at != request.deadline_at):
                raise PoolHandoffError
            row = await session.get(TaskImageMaterialization, saved.materialization_id, with_for_update=True)
            attempt = await session.get(TaskImageMaterializationAttempt, saved.attempt_id, with_for_update=True)
            if (row is None or attempt is None or attempt.grant_id is not None
                    or attempt.lease_epoch != runtime.lease_epoch or attempt.builder_id != outbox.builder_id):
                raise PoolHandoffError
            identity = {"pool_reservation_id": str(runtime.receipt.reservation_id),
                "pool_plan_sha256": runtime.receipt.plan_sha256, "target_id": runtime.target_id,
                "namespace": runtime.namespace.name, "namespace_uid": str(runtime.namespace.uid),
                "job_name": runtime.job_name, "lease_epoch": runtime.lease_epoch,
                "deadline_at": runtime.deadline_at.isoformat(), "registry_repository": runtime.registry_repository}
            native = dict(attempt.native_build or {})
            if native and any(native.get(name) != want for name, want in identity.items()):
                raise PoolHandoffError
            uid = str(runtime.receipt.job_uid) if runtime.receipt.job_uid is not None else None
            effect = str(runtime.job_effect_id) if runtime.job_effect_id is not None else None
            if ((native.get("job_uid") is not None and native["job_uid"] != uid)
                    or (native.get("pool_effect_id") is not None and native["pool_effect_id"] != effect)):
                raise PoolHandoffError
            native.update(identity, job_uid=uid, pool_effect_id=effect)
            native.setdefault("state", "pending")
            attempt.native_build = native
            now = await _clock(session)
            current = (row.lease_epoch == attempt.lease_epoch and row.claimed_by == outbox.builder_id
                and row.state in {"claimed", "running"} and row.lease_expires_at is not None
                and row.lease_expires_at > now and row.attempt_count == attempt.attempt_number
                and _snapshot(row) == saved.selection_json and _source_matches(request, row))
            demand = current and await preferred_task_image_origin(session, materialization_id=row.id,
                participant=outbox.participant, logical_pool_id=outbox.logical_pool_id) is not None
            if (saved.phase == "stop_pending" or not current or not demand or runtime.deadline_at <= now
                    or runtime.receipt.phase not in {"create_intent", "observed"}):
                cause: StopCause = "lease_lost" if not current else "deadline" if runtime.deadline_at <= now else "cancelled"
                if "pool_stop" not in native and current:
                    await session.flush()
                    await fail_native_build(session, row,
                        "build_deadline_exceeded" if cause == "deadline" else "build_cancelled",
                        builder_id=outbox.builder_id, retryable=True)
                await self._finish(session, saved, attempt, runtime, now=now, cause=cause)
                await session.flush()
                return False
            await session.flush()
            await heartbeat_task_image_materialization(session, materialization_id=row.id,
                builder_id=outbox.builder_id, lease_epoch=attempt.lease_epoch)
            if observed is not None:
                qualify_native_observation(observed, runtime)
                await record_native_build_result(session, row, attempt, builder_id=outbox.builder_id,
                    observed=observed, registry_repository=runtime.registry_repository)
                if row.state not in {"claimed", "running"}:
                    await self._finish(session, saved, attempt, runtime, now=now,
                        cause="completed" if row.state == "ready" else "failed")
            await session.flush()
            return saved.phase == "active"

    async def _reconcile(self, key: PoolRequestKeyV1) -> None:
        handoff = await self.driver.advance(key)
        if handoff.phase not in {"active", "stop_pending"}:
            return
        runtime = await self.driver.management.native_runtime(handoff.action)
        if await self._record(runtime) and runtime.receipt.job_uid is not None:
            observed = await self.kubernetes.observe_pool(runtime)
            if observed is not None:
                await self._record(runtime, observed)
        lifecycle = await self._lifecycle(key)
        if lifecycle is not None:
            stop, drain = lifecycle
            await self.driver.management.stop(stop)
            await self.driver.management.drain(drain)

    async def run_once(self) -> None:
        first_error: Exception | None = None
        for pending in await self.driver.outbox.pending():
            try:
                await self._reconcile(pending.request.key)
            except Exception as error:
                if first_error is None:
                    first_error = error
                _LOG.warning("Global native build deferred materialization=%s error=%s",
                    pending.request.key.local_work_id, type(error).__name__)
        if first_error is not None:
            raise first_error
