"""Fixed capacity qualification and internal opening through the installed gateway.

Reuse admission's exact registration, observation freshness and physical quota
checks. Row/advisory locks use READ COMMITTED, not a SQL READ ONLY transaction.
The closed and active probes always roll back. The separate opening command repeats qualification
in its own transaction and commits only the closed-to-global mode transition.
Neither command exposes a caller-selected action or public activation endpoint.
Restored target-collector publication uses a separate read-only controller probe;
it checks publication health without taking admission locks or opening a pool.
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


_BOUND_POOL_CAPACITY_BODY = '''import asyncio, hashlib, hmac, json, re, sys

async def probe(*, activate, expected_mode):
    if (expected_mode not in ("closed", "global") or (activate and expected_mode != "closed")
            or len(sys.argv) != 3 or any(re.fullmatch("[0-9a-f]{64}", value) is None for value in sys.argv[1:])):
        raise ValueError()
    from datetime import timedelta
    from sqlalchemy import exists, func, select, text, update
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from loom.db.nebius_pool_schema import NebiusPoolBinding
    from loom.db.schema import Token
    from loom.pipeline.keys import MAX_SAFE_INTEGER
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
                        settings.machine_id, settings.admission_epoch, expected_mode):
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
                if activate:
                    if current.pool.policy_revision >= MAX_SAFE_INTEGER:
                        raise ValueError()
                    # Time can advance during qualification. Row locks retain
                    # authority, but expiry/freshness must still hold at write.
                    valid_token = exists(select(Token.token_hash).where(
                        Token.token_hash == principal.token_hash,
                        Token.issued_at <= func.clock_timestamp(),
                        Token.expires_at > func.clock_timestamp(),
                        Token.revoked_at.is_(None)))
                    statement = update(NebiusPoolBinding).where(
                        NebiusPoolBinding.pool_id == settings.pool_id,
                        NebiusPoolBinding.mode == "closed",
                        NebiusPoolBinding.policy_revision == current.pool.policy_revision,
                        valid_token,
                        *(func.clock_timestamp() <= item.observed_at + timedelta(
                            seconds=item.policy.observation_max_age_seconds) for item in capacities.values()),
                    ).values(mode="global").returning(NebiusPoolBinding.mode).execution_options(synchronize_session=False)
                    if (await session.execute(statement)).scalar_one() != "global":
                        raise ValueError()
                    await session.commit()
                    return {"status": "global"}
                return {"status": "qualified"}
            finally:
                await session.rollback()
    finally:
        await engine.dispose()

'''


def _command(*, activate: bool, active: bool = False) -> str:
    if activate and active:
        raise ValueError('opening requires the closed pool')
    failure = ("Pool activation unconfirmed; preserve recovery evidence" if activate else
        "Pool active capacity unqualified" if active else "Pool startup capacity unqualified")
    return _BOUND_POOL_CAPACITY_BODY + f'''
async def run():
    async with asyncio.timeout(25):
        return await probe(activate={activate!r}, expected_mode={'global' if active else 'closed'!r})

try:
    print(json.dumps(asyncio.run(run())))
except Exception:
    print({failure!r}, file=sys.stderr)
    raise SystemExit(1)
'''


BOUND_POOL_CAPACITY_COMMAND = _command(activate=False)
BOUND_POOL_ACTIVE_CAPACITY_COMMAND = _command(activate=False, active=True)
BOUND_POOL_ACTIVATION_COMMAND = _command(activate=True)


# Restored staging uses the retained target collector, not the pool collector.
# Run inside its qualified controller: only public scope/receipt and a nonce/HMAC
# are arguments. Use its real settings and existing domain readers, not a second
# admission implementation. A healthy collector can report exhausted capacity.
BOUND_TARGET_COLLECTOR_COMMAND = '''import asyncio, hmac, json, re, sys

async def probe():
    from datetime import datetime, timedelta
    from uuid import UUID
    from sqlalchemy import func, select, text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from loom.db.schema import ExecutionCapacityObservation, ExecutionCapacityPolicy
    from loom.execution_contract import ExecutionTargetV1
    from loom.pipeline.keys import canonical_digest
    from loom_control_plane.config import ControlPlaneSettings
    from loom_control_plane.execution_capacity import _latest_observation
    from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
    from loom_execution_capacity_collector.contracts import CapacityObservationReceipt, CapacityPlacement
    if (len(sys.argv) != 4 or len(sys.argv[1]) > 131072
            or any(re.fullmatch("[0-9a-f]{64}", value) is None for value in sys.argv[2:])):
        raise ValueError()
    expected = json.loads(sys.argv[1])
    if set(expected) != {"receipt", "targets", "source", "node_group_id", "quota_identities"}:
        raise ValueError()
    receipt = CapacityObservationReceipt.model_validate_json(json.dumps(expected["receipt"]))
    targets = [ExecutionTargetV1.model_validate(row) for row in expected["targets"]]
    wanted = {row.target_id: row.model_dump(mode="json") for row in targets}
    if (not 1 <= len(targets) <= 64 or len(wanted) != len(targets)
            or receipt.target_id not in wanted or receipt.source != expected["source"]
            or any(row.provider != "nebius" for row in targets)):
        raise ValueError()
    settings = ControlPlaneSettings()
    actual = hmac.new(bytes.fromhex(sys.argv[2]), settings.db_engine_url.encode(), "sha256").hexdigest()
    if not hmac.compare_digest(actual, sys.argv[3]):
        raise ValueError()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args,
        isolation_level="REPEATABLE READ")
    try:
        async with AsyncSession(engine) as session:
            try:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await session.execute(text("SET LOCAL statement_timeout='10s'"))
                await session.execute(text("SET LOCAL lock_timeout='2s'"))
                group = await resolve_capacity_targets(session, receipt.target_id)
                if group.owner.id != receipt.target_id or group.target_ids != wanted.keys():
                    raise ValueError()
                for member in group.members:
                    spec = ExecutionTargetV1.model_validate(member.spec_json).model_dump(mode="json")
                    if (spec != wanted[member.id] or canonical_digest(member.spec_json) != member.spec_sha256
                            or any(getattr(member, name) != spec[name] for name in (
                                "logical_pool_id", "execution_class_id", "schema_version", "environment",
                                "provider", "region", "failure_domain", "data_residency"))):
                        raise ValueError()
                policy = await session.get(ExecutionCapacityPolicy, group.owner.id)
                if policy is None or policy.observation_max_age_seconds <= 0:
                    raise ValueError()
                row = await session.get(ExecutionCapacityObservation, UUID(receipt.id))
                if row is None:
                    return {"status": "pending", "reason": "missing"}
                if (str(row.id) != receipt.id or any(getattr(row, name) != getattr(receipt, name) for name in (
                        "target_id", "source", "source_version", "observed_at", "provider_capacity_state",
                        "autoscaler_state", "observation_sha256"))):
                    raise ValueError()
                payload = row.observation_json
                if (row.provider != "nebius" or payload.get("schema_version") != "loom.execution-capacity-observation.v1"
                        or canonical_digest(payload) != row.observation_sha256 or not group.matches_observation_scope(payload)):
                    raise ValueError()
                # The immutable payload and its indexed/admission columns must
                # agree; checking only the receipt/hash would miss column drift.
                for name in row.__table__.columns.keys():
                    if name in {"id", "created_at", "observation_json", "observation_sha256"}:
                        continue
                    value = payload["pending_reasons" if name == "pending_reasons_json" else name]
                    if name == "observed_at":
                        value = datetime.fromisoformat(value)
                    if value != getattr(row, name):
                        raise ValueError()
                placement = CapacityPlacement.model_validate(payload["placement"])
                identities = {name: [quota.parent_id, quota.region, quota.service, quota.name, quota.unit]
                    for name, quota in placement.quota_resources.items()}
                if (placement.node_group.id != expected["node_group_id"]
                        or not {"nodes", "vcpu", "storage"}.issubset(identities)
                        or identities != expected["quota_identities"]):
                    raise ValueError()
                latest = await _latest_observation(session, group.owner.id)
                if latest is None or latest.id != row.id:
                    return {"status": "pending", "reason": "superseded"}
                now = await session.scalar(select(func.clock_timestamp()))
                if row.observed_at > now + timedelta(seconds=60):
                    return {"status": "pending", "reason": "future"}
                if now > row.observed_at + timedelta(seconds=policy.observation_max_age_seconds):
                    return {"status": "pending", "reason": "stale"}
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
    print("Target collector publication unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
