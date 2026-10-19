"""Protected commands bind exact input, target identity and deployment authority."""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.nebius_pool_lifecycle_schema import NebiusPoolLifecycleOperation, NebiusPoolParticipantLifecycle
from loom_service.pool_management.capacity import digest
from tests.integration.test_nebius_pool_participant_lifecycle import setup
from tests.integration.test_nebius_pool_registry import sessions as sessions


def administration():
    name = 'loom_service.pool_management.administration'
    assert importlib.util.find_spec(name), 'protected explicit-ID administration runner is missing'
    return importlib.import_module(name)


def authorization(module, participant, operation):
    return module.PoolAdministrationAuthorizationV2(
        schema_version='loom.pool-administration-authorization.v2', authorization_id=uuid4(),
        installation_id=participant.installation_id, pool_id=participant.pool_id,
        participant_id=participant.participant_id, environment_id=participant.environment_id,
        incarnation=participant.incarnation, authority=participant.environment_class,
        request_sha256=digest(operation.model_dump(mode='json')),
        issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(minutes=5))


async def test_protected_apply_replay_and_status_retain_original_intent(sessions):
    module = administration()
    _, spec, _, _, _, change = await setup(sessions)
    participant = spec.participants[2]
    operation = await change(participant, 'suspend', apply=False)
    grant = authorization(module, participant, operation)
    payload = operation.model_dump_json().encode()
    async with sessions.begin() as session:
        first = await module.administer(session, payload, grant, action='apply')
    async with sessions.begin() as session:
        assert await module.administer(session, payload, grant, action='apply') == first
    async with sessions.begin() as session:
        status = await module.administer(session, payload, grant, action='status')
    assert status['receipt']['state'] == status['current']['state'] == 'suspended'
    assert status['current']['operation_id'] == str(operation.operation_id)
    assert first['authorization_id'] == str(grant.authorization_id)


@pytest.mark.parametrize('damage', ['participant', 'environment', 'incarnation', 'installation', 'pool',
    'class', 'expired', 'request', 'unsigned-extra', 'duplicate-field'])
async def test_grant_substitution_is_refused_before_any_state_change(sessions, damage):
    module = administration()
    _, spec, _, _, _, change = await setup(sessions)
    participant = spec.participants[2]
    operation = await change(participant, 'suspend', apply=False)
    grant = authorization(module, participant, operation)
    payload = operation.model_dump_json().encode()
    if damage in {'participant', 'environment', 'incarnation', 'installation', 'pool'}:
        field = damage if damage == 'incarnation' else damage + '_id'
        grant = grant.model_copy(update={field: uuid4()})
    elif damage == 'class':
        grant = grant.model_copy(update={'authority': 'production'})
    elif damage == 'expired':
        grant = grant.model_copy(update={'issued_at': datetime.now(UTC) - timedelta(minutes=6),
            'expires_at': datetime.now(UTC) - timedelta(minutes=1)})
    elif damage == 'request':
        payload = operation.model_copy(update={'operation_id': uuid4()}).model_dump_json().encode()
    elif damage == 'unsigned-extra':
        payload = json.dumps(operation.model_dump(mode='json') | {'selected_secret': 'foreign'}).encode()
    else:
        payload = payload[:-1] + b', "action":"suspend"}'
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await module.administer(session, payload, grant, action='apply')
    async with sessions() as session:
        assert (await session.get(NebiusPoolParticipantLifecycle, participant.participant_id)).state == 'active'
        assert await session.get(NebiusPoolLifecycleOperation, operation.operation_id) is None


async def test_development_authority_cannot_apply_a_production_request_even_with_its_exact_digest(sessions):
    module = administration()
    _, spec, _, _, _, change = await setup(sessions)
    participant = spec.participants[0]
    operation = await change(participant, 'suspend', apply=False)
    grant = authorization(module, participant, operation).model_copy(update={'authority': 'development'})
    with pytest.raises(ValueError):
        async with sessions.begin() as session:
            await module.administer(session, operation.model_dump_json().encode(), grant, action='apply')


async def test_actual_installed_process_requires_pinned_authorization_and_reports_only_receipt(sessions, tmp_path):
    import hashlib

    module = administration()
    _, spec, _, _, _, change = await setup(sessions)
    participant = spec.participants[2]
    operation = await change(participant, 'suspend', apply=False)
    grant = authorization(module, participant, operation)
    request_file, grant_file = tmp_path / 'request.json', tmp_path / 'authorization.json'
    request_file.write_text(operation.model_dump_json())
    raw = grant.model_dump_json().encode()
    grant_file.write_bytes(raw)
    request_file.chmod(0o600)
    grant_file.chmod(0o600)
    env = dict(os.environ, LOOM_POOL_ADMINISTRATION_FILE=str(request_file),
        LOOM_POOL_ADMINISTRATION_AUTHORIZATION_FILE=str(grant_file),
        LOOM_POOL_ADMINISTRATION_AUTHORIZATION_SHA256=hashlib.sha256(raw).hexdigest(),
        LOOM_POOL_ADMINISTRATION_DB_URL=sessions.kw['bind'].url.render_as_string(hide_password=False),
        LOOM_POOL_ADMINISTRATION_ACTION='apply')
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-m', module.__name__],
        env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout
    value = json.loads(result.stdout)
    assert value['receipt']['operation_id'] == str(operation.operation_id) and value['receipt']['state'] == 'suspended'
    env['LOOM_POOL_ADMINISTRATION_AUTHORIZATION_SHA256'] = 'f' * 64
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-m', module.__name__],
        env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 1 and json.loads(result.stdout)['status'] == 'pool_administration_unavailable'
    async with sessions() as session:
        assert len(list(await session.scalars(select(NebiusPoolLifecycleOperation).where(
            NebiusPoolLifecycleOperation.operation_id == operation.operation_id)))) == 1
