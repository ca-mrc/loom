"""Fixed non-mutating capacity qualification through the installed gateway.

Reuse admission's exact registration, observation freshness and physical quota
checks. Row/advisory locks use READ COMMITTED, not a SQL READ ONLY transaction;
only fixed reads are performed, and the transaction is always rolled back.
"""
from __future__ import annotations

from typing import Any

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation


def expected_startup_capacity(spec: PoolInstallation) -> dict[str, Any]:
    spec = PoolInstallation.model_validate(spec.model_dump())
    installation_sha256 = digest(spec.model_dump(mode='json'))
    binding = {'node_selector': spec.node_selector, 'admission': spec.admission.model_dump(),
        'quota_identities': {key: list(value) for key, value in spec.quota_identities.items()},
        'installation_sha256': installation_sha256, 'profile_catalog_sha256': digest(spec.profiles.model_dump(mode='json'))}
    registration = {'pool_id': str(spec.pool_id), 'installation_id': str(spec.installation_id),
        'cluster_id': spec.cluster_id, 'node_group_id': spec.node_group_id, 'admission_epoch': spec.admission_epoch,
        'policy_revision': spec.policy_revision, 'binding_sha256': digest(binding),
        'participants': [{'participant_id': str(row.participant_id), 'binding_sha256': digest(row.model_dump(mode='json')),
            'revision': row.binding_revision, 'phase': 'active'} for row in sorted(spec.participants, key=lambda row: row.participant_id)]}
    machine, = (row for row in spec.machines if row.role == 'gateway')
    return {'pool_id': str(spec.pool_id), 'installation_id': str(spec.installation_id),
        'admission_epoch': spec.admission_epoch, 'machine_id': str(machine.machine_id), 'token_sha256': machine.token_sha256,
        'installation_sha256': installation_sha256, 'registration_sha256': digest(registration)}


BOUND_POOL_CAPACITY_COMMAND = '''import asyncio, hashlib, hmac, json, re, sys

async def probe():
    if len(sys.argv) != 3 or any(re.fullmatch("[0-9a-f]{64}", value) is None for value in sys.argv[1:]):
        raise ValueError()
    from sqlalchemy import func, select, text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from loom_execution_capacity_collector.control_plane import read_owner_only_secret
    from loom_service.pool_management.__main__ import PoolGatewaySettings
    from loom_service.pool_management.auth import authorize_pool_machine, resolve_pool_machine
    from loom_service.pool_management.capacity import read_connected_capacity
    from loom_service.pool_management.locks import acquire_pool_mutation_lock
    from loom_service.pool_management.observations import read_pool_registration
    settings = PoolGatewaySettings()
    token = read_owner_only_secret(settings.bearer_token_file, maximum_bytes=512)
    engine = create_async_engine(settings.db_url, isolation_level="READ COMMITTED")
    try:
        async with AsyncSession(engine) as session:
            try:
                await session.execute(text("SET LOCAL statement_timeout='10s'"))
                await session.execute(text("SET LOCAL lock_timeout='2s'"))
                await acquire_pool_mutation_lock(session)
                principal = await resolve_pool_machine(session, "Bearer " + token)
                if principal is None or (
                        principal.role, principal.participant_id, principal.pool_id, principal.installation_id,
                        principal.machine_id, principal.pool_epoch, principal.pool_mode) != (
                        "gateway", None, settings.pool_id, settings.installation_id,
                        settings.machine_id, settings.admission_epoch, "closed"):
                    raise ValueError()
                await authorize_pool_machine(session, principal, role="gateway", pool_id=settings.pool_id)
                now = await session.scalar(select(func.clock_timestamp()))
                capacities = await read_connected_capacity(session, settings.pool_id, now)
                current = capacities[settings.pool_id]
                _, registration = await read_pool_registration(session, current.pool)
                actual = {"pool_id": str(current.pool.pool_id), "installation_id": str(current.pool.installation_id),
                    "admission_epoch": current.pool.admission_epoch, "machine_id": str(principal.machine_id),
                    "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
                    "installation_sha256": current.pool.binding_json["installation_sha256"], "registration_sha256": registration}
                signature = hmac.new(bytes.fromhex(sys.argv[1]),
                    json.dumps(actual, sort_keys=True, separators=(",", ":")).encode(), "sha256").hexdigest()
                if not hmac.compare_digest(signature, sys.argv[2]):
                    raise ValueError()
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
    print("Pool startup capacity unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
