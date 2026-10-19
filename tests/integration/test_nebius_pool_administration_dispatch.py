"""Public selectors cannot turn scoped installed administration into an operator CLI."""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import asdict, replace
from uuid import uuid4

import pytest

from loom.nebius_kubernetes import NebiusKubernetesConnection
from tests.integration.test_nebius_pool_administration import administration
from tests.integration.test_nebius_pool_administration_job import inputs
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.support.nebius_phase_api import PhaseAPI


def dispatcher():
    name = 'scripts.ops.nebius_pool_administration_dispatch'
    assert importlib.util.find_spec(name), 'generic scoped host dispatch is missing'
    return importlib.import_module(name)


async def prepared(sessions, tmp_path):
    module = dispatcher()
    job, selected, _ = await inputs(sessions)
    host = module.PoolAdministrationHostV2(runtime=job.runtime,
        scope=module.PoolAdministrationScopeV2(installation_id=selected.installation_id, pool_id=selected.pool_id,
            participant_id=selected.participant_id, environment_id=selected.environment_id,
            incarnation=selected.incarnation, authority=selected.environment_class),
        state_root=tmp_path / 'operations', anchor_root=tmp_path / 'anchors',
        connection=NebiusKubernetesConnection(endpoint='https://cluster.example',
            ca_file=tmp_path / 'ca.pem', credentials_file=tmp_path / 'credentials.json'))
    reference = module.retain_authorized_operation(host=host, request=job.request,
        authorization=job.authorization, evidence_sha256='b' * 64)
    return module, host, job, reference


def command(reference, action, **extra):
    return json.dumps({'schema_version': 'loom.pool-administration-command.v2', 'action': action,
        'reference': reference.model_dump(mode='json'), **extra}).encode()


async def test_prepare_apply_recover_status_only_use_scoped_retained_input(sessions, tmp_path):
    module, host, job, reference = await prepared(sessions, tmp_path)
    async with sessions.begin() as session:
        committed = await administration().administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')

    class API(PhaseAPI):
        def administration_report(self, state_dir):
            return {'job_uid': str(uuid4()), 'pod_uid': str(uuid4()), 'receipt': committed}

    api = API(host.runtime.binding)

    @contextmanager
    def connect(request):
        assert request.request == job.request and request.authorization == job.authorization
        assert request.runtime == host.runtime
        yield api

    response = module.dispatch_administration(command(reference, 'prepare'), action='prepare', host=host, connect=connect)
    assert response['status'] == 'prepared' and not api.creates
    result = module.dispatch_administration(command(reference, 'apply'), action='apply', host=host, connect=connect)
    assert result['status'] == 'complete' and result['proof']['receipt'] == committed
    assert len(api.creates) == 2
    recovered = module.dispatch_administration(command(reference, 'recover'), action='recover', host=host, connect=connect)
    assert recovered['status'] == 'complete' and len(api.creates) == 2
    observation_id = str(uuid4())
    observed = module.dispatch_administration(command(reference, 'status', observation_id=observation_id),
        action='status', host=host, connect=connect)
    assert observed['status'] == 'observed' and len(api.creates) == 3
    assert observed['action'] == 'status' and observed['observation_id'] == observation_id


@pytest.mark.parametrize('damage', ['participant', 'environment', 'incarnation', 'pool', 'request', 'authority', 'extra', 'duplicate',
    'host_scope', 'input', 'anchor', 'lost_input'])
async def test_substitution_and_lost_history_refuse_before_connecting(sessions, tmp_path, damage):
    module, host, _job, reference = await prepared(sessions, tmp_path)
    if damage in {'participant', 'environment', 'incarnation', 'pool', 'authority'}:
        key = {'participant': 'participant_id', 'environment': 'environment_id', 'incarnation': 'incarnation', 'pool': 'pool_id',
            'authority': 'authorization_id'}[damage]
        reference = reference.model_copy(update={key: uuid4()})
    elif damage == 'request':
        reference = reference.model_copy(update={'request_sha256': 'f' * 64})
    elif damage == 'host_scope':
        host = replace(host, scope=host.scope.model_copy(update={'authority': 'production'}))
    elif damage in {'input', 'anchor', 'lost_input'}:
        path = (host.anchor_root / (str(reference.authorization_id) + '.json') if damage == 'anchor'
            else host.state_root / str(reference.authorization_id) / 'authorized.json')
        if damage == 'lost_input':
            path.unlink()
        else:
            path.write_text('{}')
    content = command(reference, 'apply', **({'database_secret': 'production-db'} if damage == 'extra' else {}))
    if damage == 'duplicate':
        content = content[:-1] + b', "action":"apply"}'

    @contextmanager
    def connect(request):
        pytest.fail('unqualified input reached cluster authority')
        yield

    result = module.dispatch_administration(content, action='apply', host=host, connect=connect)
    assert result['status'] == 'blocked'
    assert set(result) == {'schema_version', 'status', 'stage'}


async def test_recovery_does_not_recreate_lost_job_journals(sessions, tmp_path):
    module, host, _job, reference = await prepared(sessions, tmp_path)

    @contextmanager
    def connect(request):
        pytest.fail('recover cannot create a missing application attempt')
        yield

    result = module.dispatch_administration(command(reference, 'recover'), action='recover', host=host, connect=connect)
    assert result['status'] == 'blocked'


async def test_apply_can_resume_interruption_after_start_but_cannot_recreate_lost_inputs(sessions, tmp_path, monkeypatch):
    module, host, job, reference = await prepared(sessions, tmp_path)
    async with sessions.begin() as session:
        committed = await administration().administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')
    class API(PhaseAPI):
        def administration_report(self, state_dir):
            return dict(job_uid=str(uuid4()), pod_uid=str(uuid4()), receipt=committed)
    api = API(host.runtime.binding)
    @contextmanager
    def connect(request):
        assert request == job
        yield api
    def interrupted(**kwargs):
        raise OSError('process interrupted before staging starts')
    with monkeypatch.context() as crash:
        crash.setattr(module, 'stage_administration_job', interrupted)
        assert module.dispatch_administration(command(reference, 'apply'), action='apply', host=host, connect=connect)['status'] == 'blocked'
    assert not api.creates
    result = module.dispatch_administration(command(reference, 'apply'), action='apply', host=host, connect=connect)
    assert result['status'] == 'complete'
    assert len(api.creates) == 2
    execution = host.state_root / str(reference.authorization_id) / 'execution'
    (execution / 'inputs').rename(execution / 'saved-inputs')
    try:
        assert module.dispatch_administration(command(reference, 'apply'), action='apply', host=host, connect=connect)['status'] == 'blocked'
        assert len(api.creates) == 2
    finally:
        (execution / 'saved-inputs').rename(execution / 'inputs')


async def test_status_before_apply_does_not_prevent_the_authorized_write(sessions, tmp_path):
    module, host, job, reference = await prepared(sessions, tmp_path)
    async with sessions.begin() as session:
        before = await administration().administer(session, job.request.model_dump_json().encode(), job.authorization, action='status')
    async with sessions.begin() as session:
        after = await administration().administer(session, job.request.model_dump_json().encode(), job.authorization, action='apply')

    class API(PhaseAPI):
        report = before

        def administration_report(self, state_dir):
            return dict(job_uid=str(uuid4()), pod_uid=str(uuid4()), receipt=self.report)

    api = API(host.runtime.binding)

    @contextmanager
    def connect(request):
        api.report = before if request.action == 'status' else after
        yield api

    status = module.dispatch_administration(command(reference, 'status', observation_id=str(uuid4())),
        action='status', host=host, connect=connect)
    assert status['proof']['receipt']['receipt'] is None
    applied = module.dispatch_administration(command(reference, 'apply'), action='apply', host=host, connect=connect)
    assert applied['status'] == 'complete', applied
    assert len(api.creates) == 3


async def test_actual_dispatch_process_requires_installed_configuration_hash(sessions, tmp_path):
    _module, host, _job, reference = await prepared(sessions, tmp_path)
    config = {'schema_version': 'loom.pool-administration-host.v2', 'runtime': asdict(host.runtime),
        'scope': host.scope.model_dump(mode='json'), 'state_root': str(host.state_root), 'anchor_root': str(host.anchor_root),
        'connection': host.connection.model_dump(mode='json')}
    path = tmp_path / 'host.json'
    raw = json.dumps(config).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    entry = 'import sys; from scripts.ops.nebius_pool_administration_dispatch import main; raise SystemExit(main(*sys.argv[1:]))'
    args = [sys.executable, '-c', entry, str(path), hashlib.sha256(raw).hexdigest(), 'prepare', command(reference, 'prepare').decode()]
    result = subprocess.run(args, env=dict(os.environ), capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 and json.loads(result.stdout)['status'] == 'prepared', result.stdout + result.stderr
    args[4] = 'f' * 64
    result = subprocess.run(args, env=dict(os.environ), capture_output=True, text=True, timeout=30)
    assert result.returncode == 1 and json.loads(result.stdout)['status'] == 'blocked'
