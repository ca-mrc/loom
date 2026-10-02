"""The fixed startup probe uses real auth, capture, admission reads and PostgreSQL."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolCapture,
    NebiusPoolObservation,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
from loom_execution_capacity_collector.contracts import QuotaResource
from loom_execution_capacity_collector.pool import PoolPodClassifier
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.installation import PoolInstallation, register_installation
from tests.integration.test_nebius_pool_installation import installation
from tests.integration.test_nebius_pool_observation_registry import (
    capture_scope,
    publish,
    snapshots,
)
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def prepare_startup_capacity(sessions, tmp_path, damage=None, *, executable=False):
    from scripts.ops.nebius_pool_startup_capacity import expected_startup_capacity

    config, tokens = installation()
    if executable:
        for profile in config['profiles']['execution']:
            profile['runtime']['node_selector'] = dict(config['node_selector'])
        for profile in config['profiles']['task_images']:
            profile['target']['node_selector'] = dict(config['node_selector'])
    if damage == 'revision_exhausted':
        config['policy_revision'] = 2**53 - 1
    elif damage == 'revision_last_openable':
        config['policy_revision'] = 2**53 - 2
    elif damage == 'expires_during_validation':
        config['admission']['observation_max_age_seconds'] = 10
    spec = PoolInstallation.model_validate(config)
    async with sessions.begin() as session:
        await register_installation(session, spec)
    gateway, = (row for row in spec.machines if row.role == 'gateway')
    observer, = (row for row in spec.machines if row.role == 'observer')
    async with sessions() as session:
        principal = await resolve_pool_machine(session, 'Bearer ' + tokens[observer.machine_id])
    if damage != 'missing':
        capture = await capture_scope(sessions, principal)
        if damage == 'stale':
            # Immutable evidence can age; bootstrap an earlier issued capture
            # without waiting or bypassing its real observation acceptance path.
            capture = replace(capture, capture_id=uuid4(), created_at=datetime.now(UTC) - timedelta(seconds=120))
            async with sessions.begin() as session:
                await session.execute(insert(NebiusPoolCapture).values(capture_id=capture.capture_id,
                    pool_id=capture.pool_id, admission_epoch=capture.admission_epoch, registration_sha256=capture.registration_sha256,
                    scope_sha256=PoolPodClassifier(capture.scope).fingerprint.removeprefix('sha256:'),
                    scope_json=capture.scope.model_dump(mode='json'), created_at=capture.created_at))
        provider, kubernetes = snapshots(capture)
        quotas = {name: QuotaResource(parent_id=parts[0], region=parts[1], service=parts[2], name=parts[3], unit=parts[4],
            limit={'nodes': 2, 'vcpu': 8000, 'storage': 65536}[name], used=0) for name, parts in spec.quota_identities.items()}
        if damage == 'quota':
            quotas['nodes'] = quotas['nodes'].model_copy(update={'parent_id': 'foreign'})
        provider = provider.model_copy(update={'quota_resources': quotas})
        await publish(sessions, principal, capture, provider=provider, kubernetes=kubernetes,
            observed_at=capture.created_at + timedelta(seconds=1) if damage == 'stale' else None)
    async with sessions.begin() as session:
        if damage == 'participant':
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == spec.participants[0].participant_id).values(phase='fenced'))
        elif damage == 'mode':
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(mode='global'))
        elif damage in {'token_revoked', 'token_expired'}:
            values = ({'revoked_at': datetime.now(UTC)} if damage == 'token_revoked'
                else {'expires_at': datetime.now(UTC) - timedelta(seconds=1)})
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(gateway.token_sha256)).values(**values))
    token = tmp_path / 'machine-token'
    token.write_text('private-wrong-machine-token' if damage == 'wrong_token' else tokens[gateway.machine_id])
    token.chmod(0o600)
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update(LOOM_POOL_GATEWAY_DB_URL=sessions.kw['bind'].url.render_as_string(hide_password=False),
        LOOM_POOL_GATEWAY_POOL_ID=str(spec.pool_id), LOOM_POOL_GATEWAY_INSTALLATION_ID=str(spec.installation_id),
        LOOM_POOL_GATEWAY_MACHINE_ID=str(uuid4() if damage == 'wrong_machine' else gateway.machine_id),
        LOOM_POOL_GATEWAY_ADMISSION_EPOCH=str(spec.admission_epoch), LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE=str(token),
        LOOM_POOL_GATEWAY_KUBERNETES=json.dumps({'kind': 'projected_service_account', 'endpoint': 'https://must-not-contact.example.com',
            'ca_file': str(tmp_path / 'absent-ca'), 'token_file': str(tmp_path / 'absent-kubernetes-token')}))
    expected = expected_startup_capacity(spec)
    if damage == 'challenge':
        expected['registration_sha256'] = '0' * 64
    nonce = 'ab' * 32
    signature = hmac.new(bytes.fromhex(nonce), json.dumps(expected, sort_keys=True, separators=(',', ':')).encode(), 'sha256').hexdigest()
    return spec, tokens, environment, token, nonce, signature, gateway


@pytest.mark.parametrize('operation', ['probe', 'activate'])
@pytest.mark.parametrize('damage', [None, 'missing', 'stale', 'participant', 'mode', 'quota', 'token_revoked',
    'token_expired', 'wrong_machine', 'wrong_token', 'challenge', 'revision_exhausted'])
async def test_startup_capacity_probe_requires_fresh_exact_scope_and_current_gateway_authority(sessions, tmp_path, damage, operation):
    from scripts.ops import nebius_pool_startup_capacity as commands

    command = commands.BOUND_POOL_CAPACITY_COMMAND if operation == 'probe' else commands.BOUND_POOL_ACTIVATION_COMMAND
    spec, tokens, environment, token, nonce, signature, gateway = await prepare_startup_capacity(sessions, tmp_path, damage)

    async def retained_state():
        async with sessions() as session:
            pool = await session.get(NebiusPoolBinding, spec.pool_id)
            return (pool.mode, pool.policy_revision, pool.admission_epoch, pool.binding_sha256,
                await session.scalar(select(func.count()).select_from(NebiusPoolCapture)),
                await session.scalar(select(func.count()).select_from(NebiusPoolObservation)),
                await session.scalar(select(func.count()).select_from(NebiusPoolRequest)))

    before = await retained_state()
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', command, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)
    success = damage is None or (damage == 'revision_exhausted' and operation == 'probe')
    if not success:
        error = b'Pool startup capacity unqualified\n' if operation == 'probe' else b'Pool activation unconfirmed; preserve recovery evidence\n'
        assert result.returncode == 1 and result.stdout == b'' and result.stderr == error
    else:
        report = b'{"status": "qualified"}\n' if operation == 'probe' else b'{"status": "global"}\n'
        assert (result.returncode, result.stdout, result.stderr) == (0, report, b'')
        assert before[0] == 'closed' and before[-1] == 0
    assert all(raw not in (result.stdout + result.stderr).decode() for raw in tokens.values())
    assert 'private-' not in (result.stdout + result.stderr).decode()
    assert await retained_state() == (('global', *before[1:]) if success and operation == 'activate' else before)
    assert hashlib.sha256(token.read_bytes()).hexdigest() == (
        hashlib.sha256(b'private-wrong-machine-token').hexdigest() if damage == 'wrong_token' else gateway.token_sha256)


@pytest.mark.parametrize('expires', ['token', 'observation'])
async def test_opening_rechecks_expiry_at_write_after_real_capacity_validation(sessions, tmp_path, expires):
    from scripts.ops.nebius_pool_startup_capacity import BOUND_POOL_ACTIVATION_COMMAND

    spec, _, environment, _, nonce, signature, gateway = await prepare_startup_capacity(sessions, tmp_path, 'expires_during_validation')
    if expires == 'token':
        async with sessions.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(gateway.token_sha256)).values(
                expires_at=datetime.now(UTC) + timedelta(seconds=8)))
    # A test-only pause occurs after the unmodified production capacity read.
    # Its marker proves preflight succeeded while evidence was still current.
    pause = f'''import asyncio
from datetime import timedelta
from sqlalchemy import select, func
from loom.db.schema import Token
from loom_service.pool_management import capacity
original = capacity.read_connected_capacity
async def delayed(session, pool_id, now):
    result = await original(session, pool_id, now)
    print("validated", flush=True)
    current = result[pool_id]
    deadline = current.observed_at + timedelta(seconds=current.policy.observation_max_age_seconds)
    if {expires!r} == "token":
        deadline = await session.scalar(select(Token.expires_at).where(Token.token_hash == bytes.fromhex({gateway.token_sha256!r})))
    while await session.scalar(select(func.clock_timestamp())) <= deadline:
        await asyncio.sleep(0.02)
    return result
capacity.read_connected_capacity = delayed
'''
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', pause + BOUND_POOL_ACTIVATION_COMMAND, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)
    assert (result.returncode, result.stdout, result.stderr) == (1, b'validated\n', b'Pool activation unconfirmed; preserve recovery evidence\n')
    async with sessions() as session:
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'closed'
