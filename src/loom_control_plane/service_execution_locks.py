"""Keep Trial → execution lease ordering across cancellation and observations."""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionLease, Trial


async def lock_execution_trial(
    session: AsyncSession, lease_id: UUID, *, key_share: bool = False,
) -> None:
    """Lock the immutable parent before any lease lock or lease mutation.

    Cancellation, reservation and verifier scheduling already own the Trial
    first. Taking a lease first elsewhere lets an observer wait on that Trial
    while its owner waits on the lease. The subquery reads only the immutable
    foreign key; callers still reload and validate the lease under its lock.
    """
    await session.execute(
        select(Trial.id).where(
            Trial.id == select(ServiceExecutionLease.trial_id)
            .where(ServiceExecutionLease.id == lease_id).scalar_subquery()
        # Preserve existing strong authorization/mutation locks by default.
        # The established non-key finalization path opts into FK compatibility.
        ).with_for_update(key_share=key_share)
    )
