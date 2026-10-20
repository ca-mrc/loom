"""One management-wide serialization domain for all physical pool writers.

Take this before pool/participant/request row locks. This deliberately serializes
shared provider quotas across pools; no network I/O belongs inside the transaction.
Protected registration/epoch changes and gateway phase changes use the same lock.
"""
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

_POOL_MUTATION_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended('nebius-global-pool-mutation', 1915))")


async def acquire_pool_mutation_lock(session: AsyncSession) -> None:
    """A waiting writer must see all grants committed by the preceding writer."""
    isolation = await session.scalar(select(func.current_setting("transaction_isolation")))
    if isolation != "read committed":
        raise ValueError("pool_mutation_requires_read_committed")
    await session.execute(_POOL_MUTATION_LOCK)


async def require_pool_read_snapshot(session: AsyncSession) -> None:
    """Unlocked observation is permitted only in a coherent non-mutable snapshot."""
    if session.new or session.dirty or session.deleted:
        raise ValueError('pool_inspection_requires_read_only_snapshot')
    settings = (await session.execute(text("SELECT current_setting('transaction_read_only'), current_setting('transaction_isolation')"))).one()
    if tuple(settings) != ('on', 'repeatable read'):
        raise ValueError('pool_inspection_requires_read_only_snapshot')
