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


@pytest.mark.parametrize('operation', ['probe', 'activate', 'active-probe'])
@pytest.mark.parametrize('damage', [None, 'missing', 'stale', 'participant', 'mode', 'quota', 'token_revoked',
    'token_expired', 'wrong_machine', 'wrong_token', 'challenge', 'revision_exhausted'])
async def test_startup_capacity_probe_requires_fresh_exact_scope_and_current_gateway_authority(sessions, tmp_path, damage, operation):
    from scripts.ops import nebius_pool_startup_capacity as commands

    command = (commands.BOUND_POOL_ACTIVE_CAPACITY_COMMAND if operation == 'active-probe' else
        commands.BOUND_POOL_CAPACITY_COMMAND if operation == 'probe' else commands.BOUND_POOL_ACTIVATION_COMMAND)
    spec, tokens, environment, token, nonce, signature, gateway = await prepare_startup_capacity(sessions, tmp_path, damage)
    if operation == 'active-probe':
        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == spec.pool_id).values(
                mode='closed' if damage == 'mode' else 'global'))

    async def retained_state():
        async with sessions() as session:
            pool = await session.get(NebiusPoolBinding, spec.pool_id)
            return (pool.mode, pool.policy_revision, pool.admission_epoch, pool.binding_sha256,
                await session.scalar(select(func.count()).select_from(NebiusPoolCapture)),
                await session.scalar(select(func.count()).select_from(NebiusPoolObservation)),
                await session.scalar(select(func.count()).select_from(NebiusPoolRequest)))

    before = await retained_state()
    result, evidence = await asyncio.to_thread(_run_expiry_probe, command,
        environment=environment, tmp_path=tmp_path, nonce=nonce, signature=signature)
    success = damage is None or (damage == 'revision_exhausted' and operation != 'activate')
    if not success:
        error = (b'Pool active capacity unqualified\n' if operation == 'active-probe' else
            b'Pool startup capacity unqualified\n' if operation == 'probe' else b'Pool activation unconfirmed; preserve recovery evidence\n')
        assert result.returncode == 1 and result.stdout == b'' and result.stderr == error, evidence
    else:
        report = b'{"status": "global"}\n' if operation == 'activate' else b'{"status": "qualified"}\n'
        assert (result.returncode, result.stdout, result.stderr) == (0, report, b''), evidence
        assert before[0] == ('global' if operation == 'active-probe' else 'closed') and before[-1] == 0
    assert all(raw not in (result.stdout + result.stderr).decode() for raw in tokens.values())
    assert 'private-' not in (result.stdout + result.stderr).decode()
    assert await retained_state() == (('global', *before[1:]) if success and operation == 'activate' else before)
    assert hashlib.sha256(token.read_bytes()).hexdigest() == (
        hashlib.sha256(b'private-wrong-machine-token').hexdigest() if damage == 'wrong_token' else gateway.token_sha256)


async def test_active_capacity_probe_preserves_real_charged_work(sessions, tmp_path):
    from scripts.ops import nebius_pool_startup_capacity as commands

    from loom.nebius_pool_workload import PoolExecutionPrepareV1
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_activation_fence import open_pool
    from tests.integration.test_nebius_pool_registry import prepare, publish_placement
    from tests.unit.test_nebius_pool_execution_render import inputs

    assert hasattr(commands, 'BOUND_POOL_ACTIVE_CAPACITY_COMMAND'), 'read-only active capacity challenge is missing'
    prepared = await prepare_startup_capacity(sessions, tmp_path, executable=True)
    spec, tokens, environment, _, nonce, signature, _ = prepared
    assert (await open_pool(prepared, tmp_path)).returncode == 0
    participant = spec.participants[0]
    owner_machine, = (row for row in spec.machines if row.participant_id == participant.participant_id)
    observer_machine, = (row for row in spec.machines if row.role == 'observer')
    async with sessions() as session:
        owner = await resolve_pool_machine(session, 'Bearer ' + tokens[owner_machine.machine_id])
        observer = await resolve_pool_machine(session, 'Bearer ' + tokens[observer_machine.machine_id])
    placement = placement_fixture(target_id=spec.node_group_id, parent_id='parent', quota_nodes=2,
        node_cpu=4000, node_memory=8192, node_storage=32768)
    del placement['quota_resources']['memory']
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement))
    _, body = inputs()
    body.update(pool_id=spec.pool_id, key=body['key'] | {'participant_id': participant.participant_id},
        origin=body['origin'] | {'data_environment_id': participant.environment_id})
    grant = await prepare(sessions, owner, PoolExecutionPrepareV1.model_validate(body), spec.profiles.profiles())
    assert grant.phase == 'reserved'

    async def retained_state():
        async with sessions() as session:
            row = await session.get(NebiusPoolRequest, grant.reservation_id)
            pool = await session.get(NebiusPoolBinding, spec.pool_id)
            return pool.mode, pool.admission_epoch, pool.policy_revision, row.phase, row.renewed_at

    before = await retained_state()
    result, evidence = await asyncio.to_thread(_run_expiry_probe, commands.BOUND_POOL_ACTIVE_CAPACITY_COMMAND,
        environment=environment, tmp_path=tmp_path, nonce=nonce, signature=signature)
    assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b''), evidence
    assert await retained_state() == before and before[0] == 'global' and before[3] == 'reserved'


def _run_expiry_probe(command, *, environment, tmp_path, nonce, signature, timeout=35, traceback_after=30):
    """Retain child timing/stacks without changing its protocol or time budget."""
    diagnostic_path = tmp_path / 'startup-expiry-diagnostics.txt'
    prelude = f'''import asyncio, atexit, faulthandler, time
_diagnostic = open({str(diagnostic_path)!r}, "w")
_started = time.monotonic()
def _record(event):
    print(f"{{time.monotonic() - _started:.6f}} {{event}}", file=_diagnostic, flush=True)
_record("child_started")
faulthandler.dump_traceback_later({traceback_after!r}, file=_diagnostic)
atexit.register(lambda: _record("interpreter_exit"))
_original_run = asyncio.run
def _diagnosed_run(*args, **kwargs):
    _record("run_started")
    try:
        return _original_run(*args, **kwargs)
    except BaseException as exc:
        _record("run_failed:" + type(exc).__name__)
        raise
    finally:
        _record("run_finished")
asyncio.run = _diagnosed_run
'''
    try:
        result = subprocess.run([sys.executable, '-c', prelude + command, nonce, signature],
            env=environment, cwd=tmp_path, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        # subprocess.run has killed and reaped the child before this read.
        evidence = (diagnostic_path.read_text() if diagnostic_path.exists()
            else 'child did not create diagnostics')
        exc.add_note('Startup expiry child diagnostics:\n' + evidence)
        raise
    return result, diagnostic_path.read_text()


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
_record("pause_imports_finished")
original = capacity.read_connected_capacity
async def delayed(session, pool_id, now):
    _record("capacity_read_started")
    result = await original(session, pool_id, now)
    _record("capacity_validated")
    print("validated", flush=True)
    current = result[pool_id]
    deadline = current.observed_at + timedelta(seconds=current.policy.observation_max_age_seconds)
    if {expires!r} == "token":
        deadline = await session.scalar(select(Token.expires_at).where(Token.token_hash == bytes.fromhex({gateway.token_sha256!r})))
    database_now = await session.scalar(select(func.clock_timestamp()))
    _record("expiry_remaining_seconds:" + str((deadline - database_now).total_seconds()))
    while await session.scalar(select(func.clock_timestamp())) <= deadline:
        await asyncio.sleep(0.02)
    _record("expiry_reached")
    return result
capacity.read_connected_capacity = delayed
'''
    result, evidence = await asyncio.to_thread(_run_expiry_probe, pause + BOUND_POOL_ACTIVATION_COMMAND,
        environment=environment, tmp_path=tmp_path, nonce=nonce, signature=signature)
    assert (result.returncode, result.stdout, result.stderr) == (
        1, b'validated\n', b'Pool activation unconfirmed; preserve recovery evidence\n'), evidence
    # A generic refusal also covers timeouts. Prove the write's expiry guard
    # rejected its row instead of accepting an unrelated failure as success.
    assert 'expiry_reached' in evidence and 'run_failed:NoResultFound' in evidence, evidence
    async with sessions() as session:
        assert (await session.get(NebiusPoolBinding, spec.pool_id)).mode == 'closed'


def test_expiry_probe_timeout_retains_child_stack_and_phase(tmp_path):
    command = 'import time\nprint("validated", flush=True)\ntime.sleep(60)\n'
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        _run_expiry_probe(command, environment=dict(os.environ), tmp_path=tmp_path,
            nonce='ab' * 32, signature='cd' * 32, timeout=5, traceback_after=0.1)
    evidence = '\n'.join(getattr(caught.value, '__notes__', ()))
    assert 'child_started' in evidence
    assert 'Timeout (' in evidence and 'File "<string>"' in evidence
    assert 'ab' * 32 not in evidence and 'cd' * 32 not in evidence


def test_expiry_probe_records_exception_type_without_private_message(tmp_path):
    command = '''async def probe():
    raise RuntimeError("private-probe-database-value")
try:
    asyncio.run(probe())
except Exception:
    raise SystemExit(1)
'''
    result, evidence = _run_expiry_probe(command, environment=dict(os.environ), tmp_path=tmp_path,
        nonce='ab' * 32, signature='cd' * 32)
    assert result.returncode == 1
    assert result.stdout == result.stderr == b''
    assert 'run_failed:RuntimeError' in evidence
    assert 'interpreter_exit' in evidence
    assert 'private-probe-database-value' not in evidence
