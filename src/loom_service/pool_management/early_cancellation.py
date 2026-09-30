"""Read retained pre-prepare cancellation under the caller's global mutation lock."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolCancellation
from loom.nebius_pool_contract import PoolReceiptV1, PoolRequestActionV1


async def read_early_cancellation(session: AsyncSession, action: PoolRequestActionV1) -> PoolReceiptV1 | None:
    key = action.request_key
    row = await session.scalar(select(NebiusPoolCancellation).where(
        NebiusPoolCancellation.participant_id == key.participant_id,
        NebiusPoolCancellation.workload_kind == key.workload_kind,
        NebiusPoolCancellation.local_work_id == key.local_work_id,
        NebiusPoolCancellation.generation == key.generation,
    ).execution_options(populate_existing=True))
    if row is None:
        return None
    if (row.pool_id != action.pool_id or row.admission_epoch != action.admission_epoch
            or row.request_sha256 != action.request_sha256):
        raise ValueError("pool_request_conflict")
    return PoolReceiptV1(reservation_id=row.cancellation_id, pool_id=row.pool_id,
        request_key=key, admission_epoch=row.admission_epoch, request_sha256=row.request_sha256,
        phase="cancelled_unstarted")
