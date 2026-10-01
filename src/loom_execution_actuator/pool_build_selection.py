"""Select native demand without claiming an attempt or admitting capacity.

The read-only queue scan is a hint. The durable outbox requalifies source,
generation, origin and intake under its materialization lock before persisting.
No management or Kubernetes I/O occurs while these SQL sessions are open.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta

import rfc8785
from sqlalchemy import and_, case, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import (
    TaskBundleSource,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
)
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.nebius_rollout_guard import admission_open
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom_control_plane.task_image_materializations import nebius_task_image_consumers
from loom_execution_actuator.pool_origins import preferred_task_image_origin
from loom_execution_actuator.pool_outbox import (
    PoolBuildHandoff,
    PoolBuildOutbox,
    PoolHandoffError,
    _clock,
)

_LOG = logging.getLogger(__name__)


class PoolBuildSelector:
    def __init__(self, *, outbox: PoolBuildOutbox, target_id: str, deadline_seconds: int) -> None:
        outbox.participant.target(target_id, "task_image_build")
        if type(deadline_seconds) is not int or not 60 <= deadline_seconds <= 7200:
            raise PoolHandoffError
        self.outbox, self.target_id, self.deadline_seconds = outbox, target_id, deadline_seconds

    async def _request(self, session: AsyncSession, row: TaskImageMaterialization) -> PoolTaskImagePrepareV1 | None:
        participant = self.outbox.participant
        origin = await preferred_task_image_origin(session, materialization_id=row.id,
            participant=participant, logical_pool_id=self.outbox.logical_pool_id)
        if origin is None:
            return None
        source: dict[str, object]
        if row.bundle_content_manifest_sha256:
            retained = await session.get(TaskBundleSource, hashlib.sha256((row.task_source or "").encode()).hexdigest())
            if retained is None:
                raise PoolHandoffError
            registered = TaskBundleSourceSpecV1.model_validate_json(json.dumps(retained.spec_json))
            source = {"kind": "registered", "registration": registered.model_dump(mode="json")}
        else:
            source = {"kind": "legacy", "uri": row.task_source,
                "bundle_file_metadata_sha256": row.task_source_provenance.get("bundle_file_metadata_sha256"),
                "input_manifest": row.task_source_provenance.get("service_execution_input")}
        latest = await session.scalar(select(func.max(NebiusPoolBuildOutbox.generation)).where(
            NebiusPoolBuildOutbox.materialization_id == row.id))
        return PoolTaskImagePrepareV1.model_validate({
            "pool_id": participant.pool_id, "admission_epoch": participant.admission_epoch,
            "participant_revision": participant.binding_revision,
            "key": {"participant_id": participant.participant_id, "workload_kind": "task_image_build",
                "local_work_id": row.id, "generation": (latest or 0) + 1},
            "target_id": self.target_id, "origin": origin,
            "deadline_at": await _clock(session) + timedelta(seconds=self.deadline_seconds),
            "build": {"expected_lease_epoch": row.lease_epoch, "materialization_key": row.materialization_key,
                "task_id": row.task_id, "task_checksum": row.task_checksum, "cpu_arch": row.cpu_arch,
                "task_config_json": rfc8785.dumps(row.task_config).decode(), "source": source},
        })

    async def select_next(self) -> PoolBuildHandoff | None:
        row = TaskImageMaterialization
        consumers = nebius_task_image_consumers(row, pool_id=self.outbox.logical_pool_id)
        shared = consumers.where(Trial.pool_origin["kind"].astext == "environment",
            Trial.pool_origin["data_environment_id"].astext == str(self.outbox.participant.environment_id)).exists()
        live = exists().where(NebiusPoolBuildOutbox.materialization_id == row.id,
            NebiusPoolBuildOutbox.phase.not_in(("cancelled", "released")))
        legacy = exists().where(TaskImageMaterializationAttempt.materialization_id == row.id,
            TaskImageMaterializationAttempt.native_build.is_not(None),
            TaskImageMaterializationAttempt.native_build["capacity_released_at"].as_string().is_(None))
        async with self.outbox.sessions() as session:
            if not await admission_open(session):
                return None
            now = await _clock(session)
            query = select(row).where(consumers.exists(), ~live, ~legacy, row.cpu_arch == "x86_64",
                row.attempt_count < row.max_attempts,
                or_(and_(row.state == "queued", or_(row.next_attempt_at.is_(None), row.next_attempt_at <= now)),
                    and_(row.state.in_(("claimed", "running")), row.lease_expires_at <= now)),
            ).order_by(case((shared, 0), else_=1), row.created_at, row.id)
            rows = await session.stream_scalars(query, execution_options={"yield_per": 100})
            try:
                async for candidate in rows:
                    try:
                        request = await self._request(session, candidate)
                        if request is not None:
                            return await self.outbox.remember(request)
                    except ValueError as error:
                        # Source/mode errors and lost selection races never
                        # consume an attempt or hide the next useful candidate.
                        _LOG.warning("Global build selection deferred materialization=%s error=%s",
                            candidate.id, type(error).__name__)
            finally:
                await rows.close()
        return None
