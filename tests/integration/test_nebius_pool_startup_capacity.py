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


@pytest.mark.parametrize('damage', [None, 'missing', 'stale', 'participant', 'mode', 'quota', 'token_revoked',
    'wrong_machine', 'wrong_token', 'challenge'])
async def test_startup_capacity_probe_requires_fresh_exact_scope_and_current_gateway_authority(sessions, tmp_path, damage):
    from scripts.ops.nebius_pool_startup_capacity import (
        BOUND_POOL_CAPACITY_COMMAND,
        expected_startup_capacity,
    )

    config, tokens = installation()
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
        elif damage == 'token_revoked':
            await session.execute(update(Token).where(Token.token_hash == bytes.fromhex(gateway.token_sha256)).values(revoked_at=datetime.now(UTC)))
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

    async def retained_state():
        async with sessions() as session:
            pool = await session.get(NebiusPoolBinding, spec.pool_id)
            return (pool.mode, pool.admission_epoch, pool.binding_sha256,
                await session.scalar(select(func.count()).select_from(NebiusPoolCapture)),
                await session.scalar(select(func.count()).select_from(NebiusPoolObservation)),
                await session.scalar(select(func.count()).select_from(NebiusPoolRequest)))

    before = await retained_state()
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', BOUND_POOL_CAPACITY_COMMAND, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)
    if damage:
        assert result.returncode == 1 and result.stdout == b'' and result.stderr == b'Pool startup capacity unqualified\n'
    else:
        assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b'')
        assert before[0] == 'closed' and before[-1] == 0
    assert all(raw not in (result.stdout + result.stderr).decode() for raw in tokens.values())
    assert 'private-' not in (result.stdout + result.stderr).decode()
    assert await retained_state() == before
    assert hashlib.sha256(token.read_bytes()).hexdigest() == (
        hashlib.sha256(b'private-wrong-machine-token').hexdigest() if damage == 'wrong_token' else gateway.token_sha256)
