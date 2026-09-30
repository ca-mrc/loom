"""Native global-handoff consumer: read results, heartbeat and retain stop intent.

No local capacity admission or Kubernetes writes. All SQL commits finish before
management/Kubernetes I/O. Stop/drain delivery and release require their separate
reconciliation; a terminal result never frees the charged global reservation.
"""
from __future__ import annotations

from typing import Any, Protocol

from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom_control_plane.task_image_materializations import heartbeat_task_image_materialization
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_native_observation import qualify_native_observation
from loom_execution_actuator.pool_origins import preferred_task_image_origin
from loom_execution_actuator.pool_outbox import PoolHandoffError, _clock, _snapshot, _source_matches
from loom_execution_actuator.task_image_controller import record_native_build_result


class PoolNativeBuildApi(Protocol):
    async def observe_pool(self, runtime: PoolNativeRuntimeV1, *, capture_logs: bool = False) -> dict[str, Any] | None: ...


class PoolNativeBuildController:
    def __init__(self, *, driver: PoolBuildDriver, kubernetes: PoolNativeBuildApi) -> None:
        self.driver, self.kubernetes = driver, kubernetes

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
                saved.phase = "stop_pending"
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
                    saved.phase = "stop_pending"
            await session.flush()
            return saved.phase == "active"

    async def run_once(self) -> None:
        for pending in await self.driver.outbox.pending():
            handoff = await self.driver.advance(pending.request.key)
            if handoff.phase not in {"active", "stop_pending"}:
                continue
            runtime = await self.driver.management.native_runtime(handoff.action)
            if not await self._record(runtime) or runtime.receipt.job_uid is None:
                continue
            observed = await self.kubernetes.observe_pool(runtime)
            if observed is not None:
                await self._record(runtime, observed)
