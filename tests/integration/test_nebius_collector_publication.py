"""Recovery must prove a current publication, not merely a successful Job."""
from __future__ import annotations

import asyncio
import copy
import hmac
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import PostgresDsn
from sqlalchemy import delete, select, update

from loom.db.schema import (
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ServiceExecutionTarget,
)
from loom.execution_contract import NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1
from loom_control_plane.execution_capacity import create_execution_capacity_observation
from loom_control_plane.service_execution import persist_execution_catalog
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.integration.test_service_execution_leases import _seed_ready_trial


@pytest.mark.parametrize('case', [
    'current', 'family_current', 'exhausted', 'zero_workers', 'stale', 'future', 'policy_age',
    'missing', 'superseded', 'receipt', 'checksum', 'scope', 'node_group',
    'quota', 'target_spec', 'target_index', 'wrong_database', 'wrong_source', 'policy_missing', 'write_attempt',
])
async def test_collector_publication_is_fresh_exact_and_read_only(sessions, tmp_path, case):
    from scripts.ops import nebius_pool_startup_capacity as commands

    assert hasattr(commands, 'BOUND_TARGET_COLLECTOR_COMMAND'), 'current target collector publication reader is missing'
    now = datetime.now(UTC)
    observed = now + timedelta(seconds=120) if case == 'future' else now - timedelta(
        seconds=1000 if case == 'stale' else 300 if case == 'policy_age' else 30)
    async with sessions.begin() as session:
        _, target = await _seed_ready_trial(session, now=observed - timedelta(seconds=1))
        row = (await session.scalars(select(ExecutionCapacityObservation).where(
            ExecutionCapacityObservation.target_id == target.target_id))).one()
        payload = copy.deepcopy(row.observation_json)
        payload.pop('schema_version')
        payload.pop('provider')
        payload.update(observed_at=observed, source_version='collector-regression')

        async def add_guest():
            guest = target.model_copy(update={'target_id': target.target_id + '-guest',
                'execution_class_id': NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1.class_id,
                'capacity_owner_target_id': target.target_id})
            await persist_execution_catalog(session, execution_class=NEBIUS_CPU_GUEST_EXECUTION_CLASS_V1, targets=(guest,))
            return guest.model_dump(mode='json')

        family = [target.model_dump(mode='json')]
        if case == 'family_current':
            family.append(await add_guest())
            payload['placement']['target_scope'] = {'schema_version': 'loom.execution-capacity-target-scope.v1',
                'owner_target_id': target.target_id, 'namespace_name': target.namespace_name,
                'target_ids': sorted(item['target_id'] for item in family)}
        if case == 'exhausted':
            payload['provider_capacity_state'] = 'insufficient'
            for quota in payload['placement']['quota_resources'].values():
                quota['used'] = quota['limit']
        elif case == 'zero_workers':
            payload['placement']['nodes'] = []
            payload['placement']['node_group']['node_count'] = 0
            for field in ('active_nodes', 'provisioned_vcpu_millis', 'provisioned_memory_mib',
                    'provisioned_storage_mib', 'allocatable_cpu_millis', 'allocatable_memory_mib',
                    'allocatable_storage_mib'):
                payload[field] = 0
            payload['node_states'] = {key: 0 for key in payload['node_states']}
        row, _ = await create_execution_capacity_observation(session, **payload)
        receipt = {key: getattr(row, key) for key in ('target_id', 'source', 'source_version',
            'provider_capacity_state', 'autoscaler_state', 'observation_sha256')}
        receipt.update(id=str(row.id), created=True, observed_at=observed.isoformat())
        expected = {'receipt': receipt, 'targets': family,
            'source': 'service-execution-test', 'node_group_id': target.target_id,
            'quota_identities': {name: [target.target_id, 'eu-north1', 'compute', name, unit]
                for name, unit in (('nodes', 'count'), ('vcpu', 'milli-vcpu'), ('memory', 'MiB'), ('storage', 'MiB'))}}
        if case == 'receipt':
            receipt['source_version'] = 'another-collection'
        elif case == 'checksum':
            receipt['observation_sha256'] = 'sha256:' + '0' * 64
        elif case == 'scope':
            # The family changes legitimately after this immutable observation.
            expected['targets'].append(await add_guest())
        elif case == 'node_group':
            expected['node_group_id'] = 'another-node-group'
        elif case == 'quota':
            expected['quota_identities']['nodes'][0] = 'another-quota-parent'
        elif case == 'wrong_source':
            expected['source'] = 'another-collector'
        elif case == 'target_spec':
            expected['targets'][0]['region'] = 'another-region'
        elif case == 'target_index':
            await session.execute(update(ServiceExecutionTarget).where(
                ServiceExecutionTarget.id == target.target_id).values(region='another-region'))
        elif case == 'policy_age':
            await session.execute(update(ExecutionCapacityPolicy).where(
                ExecutionCapacityPolicy.target_id == target.target_id).values(observation_max_age_seconds=60))
        elif case == 'policy_missing':
            await session.execute(delete(ExecutionCapacityPolicy).where(ExecutionCapacityPolicy.target_id == target.target_id))
        elif case == 'missing':
            receipt['id'] = str(uuid4())
        elif case == 'superseded':
            await create_execution_capacity_observation(session, **(payload | {'source_version': 'new-collection', 'observed_at': now}))

    async def retained():
        async with sessions() as session:
            rows = []
            for model in (ExecutionCapacityObservation, ExecutionCapacityPolicy, ServiceExecutionTarget):
                rows.append(tuple((await session.execute(select(model.__table__).order_by(*model.__table__.primary_key))).all()))
            return tuple(rows)

    before = await retained()
    url = str(PostgresDsn(sessions.kw['bind'].url.render_as_string(hide_password=False)))
    nonce = 'ab' * 32
    response = hmac.new(bytes.fromhex(nonce), url.encode(), 'sha256').hexdigest()
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    # The real settings' pooled URL takes precedence. Never connect to this
    # deliberately invalid direct URL or disclose either one in diagnostics.
    environment.update(LOOM_CP_DB_URL='postgresql+psycopg://private-wrong:private-secret@invalid.example/db',
        LOOM_CP_DB_URL_POOL=url, LOOM_CP_MINIO_ACCESS_KEY='private-fixture', LOOM_CP_MINIO_SECRET_KEY='private-fixture',
        LOOM_CP_STEP_JWT_SIGNING_KEY='private-fixture-signing-key')
    # Fault only the imported reader, not the command: the database must reject
    # a future accidental write rather than relying solely on code convention.
    prefix = '''from sqlalchemy import update
from loom.db.schema import ExecutionCapacityPolicy
from loom_control_plane import execution_capacity_targets as targets
original = targets.resolve_capacity_targets
async def writing_reader(session, target_id):
    await session.execute(update(ExecutionCapacityPolicy).values(reason="forbidden-write"))
    return await original(session, target_id)
targets.resolve_capacity_targets = writing_reader
''' if case == 'write_attempt' else ''
    result = await asyncio.to_thread(subprocess.run,
        [sys.executable, '-c', prefix + commands.BOUND_TARGET_COLLECTOR_COMMAND, json.dumps(expected), nonce,
            '0' * 64 if case == 'wrong_database' else response],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)
    if case in {'current', 'family_current', 'exhausted', 'zero_workers'}:
        assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b'')
    elif case in {'stale', 'future', 'policy_age', 'missing', 'superseded'}:
        assert result.returncode == 0 and result.stderr == b''
        assert json.loads(result.stdout) == {'status': 'pending', 'reason': 'stale' if case == 'policy_age' else case}
    else:
        assert (result.returncode, result.stdout, result.stderr) == (1, b'', b'Target collector publication unqualified\n')
    assert url.encode() not in result.stdout + result.stderr
    assert b'private-' not in result.stdout + result.stderr
    assert await retained() == before
