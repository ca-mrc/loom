"""Local consumer provenance for shared builds, before management qualification."""
from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TaskImageMaterialization, Trial
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom_control_plane.task_image_materializations import nebius_task_image_consumers


async def preferred_task_image_origin(session: AsyncSession, *, materialization_id: UUID,
                                      participant: PoolParticipantV1, logical_pool_id: str) -> PoolWorkOriginV1 | None:
    """Choose the highest eligible class, oldest consumer within that class.

    Reuse the real native-demand query so cancelled/foreign/legacy work cannot
    promote a shared image. Unknown historical origins supply no class. This
    does not replace management's registered-application qualification or grant.
    A newly observed better origin invalidates an unstarted local selection;
    callers must cancel that request, not mutate its replay body or attempt.
    """
    query = (nebius_task_image_consumers(TaskImageMaterialization, pool_id=logical_pool_id)
        .with_only_columns(Trial.pool_origin)
        .where(TaskImageMaterialization.id == materialization_id)
        .order_by(Trial.submitted_at, Trial.id))
    origins = await session.stream_scalars(query, execution_options={"yield_per": 128})
    preferred, priority = None, 4
    try:
        async for raw in origins:
            try:
                origin = PoolWorkOriginV1.model_validate(raw)
                candidate_priority = pool_request_priority(participant, origin, workload_kind="task_image_build")
            except ValueError:
                continue
            if candidate_priority < priority:
                preferred, priority = origin, candidate_priority
            # Within one data environment, a qualified environment origin is
            # the best possible class. Rows are already ordered by age/UUID.
            if origin.kind == "environment":
                break
    finally:
        await origins.close()
    return preferred
