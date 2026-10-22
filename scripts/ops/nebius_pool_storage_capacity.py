"""Fixed manager-side aggregate proof while the execution gateway stays stopped.

Reuse the admission readers and their lock order, but always roll back. This is
not a write, registration or opening command. The connected parent qualifies the
manager process/database and supplies an exact, non-secret challenge.
"""
from __future__ import annotations

from scripts.ops.nebius_pool_startup_capacity import expected_startup_capacity

from loom_service.pool_management.installation import PoolInstallation


def expected_storage_capacity(spec: PoolInstallation) -> dict[str, str]:
    expected = expected_startup_capacity(spec)
    return {key: expected[key] for key in ('pool_id', 'installation_id', 'installation_sha256', 'registration_sha256')}


BOUND_CLOSED_STORAGE_CAPACITY_COMMAND = '''import asyncio, hmac, json, re, sys

async def probe():
    from uuid import UUID
    from sqlalchemy import func, select, text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from loom.db.nebius_pool_schema import NebiusPoolBinding
    from loom_service.config import LoomServiceSettings
    from loom_service.pool_management.capacity import read_connected_capacity
    from loom_service.pool_management.locks import acquire_pool_mutation_lock
    from loom_service.pool_management.observations import read_pool_registration
    if len(sys.argv) != 4 or any(re.fullmatch("[0-9a-f]{64}", value) is None for value in sys.argv[2:]):
        raise ValueError()
    pool_id = UUID(sys.argv[1])
    if not pool_id.int or str(pool_id) != sys.argv[1]:
        raise ValueError()
    settings = LoomServiceSettings()
    if settings.service_mode != "management":
        raise ValueError()
    engine = create_async_engine(settings.db_engine_url, isolation_level="READ COMMITTED")
    try:
        async with AsyncSession(engine) as session:
            try:
                await session.execute(text("SET LOCAL statement_timeout='10s'"))
                await session.execute(text("SET LOCAL lock_timeout='2s'"))
                await acquire_pool_mutation_lock(session)
                pool = await session.scalar(select(NebiusPoolBinding).where(
                    NebiusPoolBinding.pool_id == pool_id).with_for_update())
                if pool is None or pool.mode != "closed":
                    raise ValueError()
                _, registration = await read_pool_registration(session, pool)
                actual = {"pool_id": str(pool.pool_id), "installation_id": str(pool.installation_id),
                    "installation_sha256": pool.binding_json["installation_sha256"], "registration_sha256": registration}
                signature = hmac.new(bytes.fromhex(sys.argv[2]), json.dumps(actual, sort_keys=True,
                    separators=(",", ":")).encode(), "sha256").hexdigest()
                if not hmac.compare_digest(signature, sys.argv[3]):
                    raise ValueError()
                now = await session.scalar(select(func.clock_timestamp()))
                try:
                    capacities = await read_connected_capacity(session, pool_id, now)
                    if pool_id not in capacities:
                        raise ValueError()
                except ValueError as error:
                    if str(error) == "pool_observation_unavailable":
                        return {"status": "pending"}
                    raise
                return {"status": "qualified"}
            finally:
                await session.rollback()
    finally:
        await engine.dispose()

async def run():
    async with asyncio.timeout(25):
        return await probe()

try:
    print(json.dumps(asyncio.run(run())))
except Exception:
    print("Closed storage aggregate unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
