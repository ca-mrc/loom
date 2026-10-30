"""Live resource qualification follows actual retained dispatch, never wishful targets."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_participant_resource import _resource_path
from tests.ops.test_nebius_pool_participant_adapter import connection as _pool_connection
from tests.ops.test_nebius_pool_participant_creation import connection as _creation_connection
from tests.ops.test_nebius_pool_participant_creation import job_writer
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.ops.test_nebius_pool_participant_resource import connection as _resource_connection
from tests.ops.test_nebius_pool_participant_resource import file_writer

pool_connection = _pool_connection
creation_connection = _creation_connection
resource_connection = _resource_connection


def module():
    name = 'scripts.ops.nebius_pool_participant_observation'
    assert importlib.util.find_spec(name) is not None, 'phase-aware live resource observer is missing'
    return importlib.import_module(name)


def observer(retained, resources, *, after_read=None):
    instance = module().HTTPSParticipantResourceObserver(retained=retained,
        api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context())
    requests = []

    def respond(request):
        assert request.method == 'GET'
        requests.append(request.url.path)
        if request.url.path == '/api/v1/namespaces/kube-system':
            result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
                'uid': retained.release.migration.registration.binding.kube_system_uid}}
        else:
            # Snapshot matching fallbacks before either callback. Keep the other
            # keys so resource overrides retain their original insertion order.
            rows = {_key(row): copy.deepcopy(row) if _resource_path(row) == request.url.path else None
                for row in (*retained.inputs.current, retained.inputs.manager, retained.inputs.catalog)}
            rows.update(resources())
            result = next((row for row in rows.values() if row is not None and _resource_path(row) == request.url.path), None)
        if after_read is not None:
            after_read(request.url.path)
        return httpx.Response(404) if result is None else httpx.Response(200, json=result)

    instance.client.close()
    instance.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
    return instance, requests


@pytest.fixture
def observer_fallbacks(monkeypatch):
    """Exercise the mock HTTP responses independently of live qualification."""
    rows = [{'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': 'fixture', 'name': name},
        'data': {'value': name}} for name in ('first', 'second', 'manager', 'catalog')]
    retained = SimpleNamespace(inputs=SimpleNamespace(current=tuple(rows[:2]), manager=rows[2], catalog=rows[3],
        config={'kubernetes_api_server': 'https://kubernetes.invalid'}))
    monkeypatch.setattr(module(), 'HTTPSParticipantResourceObserver',
        lambda **kwargs: SimpleNamespace(client=httpx.Client()))
    return retained, rows


@pytest.mark.parametrize('name', ['first', 'manager', 'catalog', 'absent'])
def test_observer_http_copies_only_requested_fallback(observer_fallbacks, monkeypatch, name):
    retained, rows = observer_fallbacks
    live, requests = observer(retained, lambda: {})
    copied = []
    original = copy.deepcopy

    def tracked(value, *args, **kwargs):
        if any(value is row for row in rows):
            copied.append(value['metadata']['name'])
        return original(value, *args, **kwargs)

    monkeypatch.setattr(copy, 'deepcopy', tracked)
    path = '/api/v1/namespaces/fixture/configmaps/' + name
    with live.client:
        response = live.client.get(path)
    assert response.status_code == (404 if name == 'absent' else 200)
    if name != 'absent':
        assert response.json() == next(row for row in rows if row['metadata']['name'] == name)
    assert requests == [path]
    assert copied == ([] if name == 'absent' else [name])


def test_observer_http_snapshots_fallback_before_callbacks_and_reads_again(observer_fallbacks):
    retained, rows = observer_fallbacks
    selected = rows[0]
    events = []

    def resources():
        events.append('resources')
        selected['data']['value'] = 'resources'
        return {}

    def after_read(path):
        events.append('after_read')
        selected['data']['value'] = 'after_read'

    live, _ = observer(retained, resources, after_read=after_read)
    with live.client:
        first = live.client.get(_resource_path(selected))
        second = live.client.get(_resource_path(selected))
    assert first.json()['data']['value'] == 'first'
    assert second.json()['data']['value'] == 'after_read'
    assert events == ['resources', 'after_read', 'resources', 'after_read']


def test_observer_http_preserves_absence_before_resources_mutates_fallback(observer_fallbacks):
    retained, rows = observer_fallbacks
    path = '/api/v1/namespaces/fixture/configmaps/renamed'

    def resources():
        rows[0]['metadata']['name'] = 'renamed'
        return {}

    live, _ = observer(retained, resources)
    with live.client:
        assert live.client.get(path).status_code == 404
        assert live.client.get(path).json() == rows[0]


def test_observer_http_reads_overrides_each_time_and_preserves_after_read_alias(observer_fallbacks):
    retained, rows = observer_fallbacks
    override = copy.deepcopy(rows[0])
    state = {'current': override}

    def resources():
        return {_key(rows[0]): state['current']}

    def after_read(path):
        state['current']['data']['value'] += '-after'

    live, _ = observer(retained, resources, after_read=after_read)
    with live.client:
        first = live.client.get(_resource_path(rows[0]))
        state['current'] = copy.deepcopy(rows[0])
        state['current']['data']['value'] = 'replacement'
        second = live.client.get(_resource_path(rows[0]))
    assert first.json()['data']['value'] == 'first-after'
    assert second.json()['data']['value'] == 'replacement-after'
    assert rows[0]['data']['value'] == 'first'


def test_observer_http_keeps_fallback_key_order_for_overrides(observer_fallbacks):
    retained, rows = observer_fallbacks
    first, second = (copy.deepcopy(row) for row in rows[:2])
    first['metadata']['name'] = second['metadata']['name'] = 'shared-path'
    # An override retains its fallback key's position even when the fallback
    # itself does not match this GET. Reversed callback order must not win.
    live, _ = observer(retained, lambda: {_key(rows[1]): second, _key(rows[0]): first})
    with live.client:
        assert live.client.get(_resource_path(first)).json()['data']['value'] == 'first'


def test_observer_http_override_can_hide_matching_fallback(observer_fallbacks):
    retained, rows = observer_fallbacks
    live, _ = observer(retained, lambda: {_key(rows[0]): rows[1]})
    with live.client:
        assert live.client.get(_resource_path(rows[0])).status_code == 404


def evidence(retained):
    return {path: path.read_bytes() for root in (retained.state_dir, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()}


@pytest.fixture
def guard_connection(resource_connection, tmp_path, monkeypatch):
    """Only remote Kubernetes/SQL output is doubled; database checks stay real."""
    import base64
    import subprocess
    from urllib.parse import urlsplit

    from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
    from tests.ops.test_nebius_development_pool_retained import populate_database_backends

    state, connect = resource_connection
    retained = state.retained
    target, = (row for row in retained.inputs.previous.guards
        if row.participant_id == retained.release.refresh.participant_id)
    namespace = target.namespace
    kubeconfig = tmp_path / 'participant-kubeconfig'
    kubeconfig.write_text('private-test-config')
    kubeconfig.chmod(0o600)
    state.guard_status, state.guard_fault, state.guard_queries = 'held', None, []
    state.guard_calls = []
    state.guard_hook = None
    documents, template = {}, None
    database_key = f'StatefulSet:{namespace}:loom-postgres'
    service_key = f'Service:{namespace}:loom-postgres'
    reference, = (row['valueFrom']['secretKeyRef'] for row in target.controller['spec']['template']['spec']['containers'][0]['env']
        if row['name'] == 'LOOM_CP_DB_URL')
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': reference['name'], 'namespace': namespace,
        'uid': str(target.database.credential_uid), 'resourceVersion': target.database.credential_resource_version},
        'data': {reference['key']: base64.b64encode(
            f'postgresql+psycopg://loom_cp:private-test@loom-postgres.{namespace}.svc:5432/loom'.encode()).decode()}}

    def execute(command, **kwargs):
        nonlocal template
        assert command[:3] == ['/usr/bin/kubectl', '--kubeconfig', str(kubeconfig)]
        assert kwargs['timeout'] == 40 and kwargs['check'] is False
        position = next(index for index, value in enumerate(command) if value in {'get', 'exec'})
        args = command[position:]
        state.guard_calls.append(args)
        database = state.rows[database_key]
        documents[_resource_path(database)] = database
        documents[_resource_path(state.rows[service_key])] = state.rows[service_key]
        if template != database['spec']:
            # Model completed StatefulSet replacement, not two live primaries.
            documents.pop(f'/api/v1/namespaces/{namespace}/pods', None)
            populate_database_backends(documents)
            template = copy.deepcopy(database['spec'])
        if args[0] == 'exec':
            assert args[:-1] == ['exec', '-n', namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
            assert args[-1] == rollout_guard_observation_sql(owner=retained.guard_owner,
                candidate=retained.inputs.candidate['candidate_sha'])
            state.guard_queries.append(args[-1])
            value = {'status': state.guard_status}
            if state.guard_fault == 'extra-report':
                value['private'] = 'must-not-export'
            elif state.guard_fault == 'pod-restart':
                documents[f'/api/v1/namespaces/{namespace}/pods']['items'][0]['status']['containerStatuses'][0]['restartCount'] += 1
            elif state.guard_fault == 'guard-changed':
                state.guard_status = 'open'
            if state.guard_hook is not None:
                state.guard_hook()
        else:
            assert args[0] == 'get'
            kind, name = args[1:3]
            if kind == 'namespace':
                uid = retained.inputs.previous.registration.binding.kube_system_uid if name == 'kube-system' else str(target.namespace_uid)
                value = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
            elif kind == '--raw':
                value = copy.deepcopy(documents[urlsplit(name).path])
                if state.guard_fault == 'backend' and value['kind'] == 'EndpointSliceList':
                    value['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
            else:
                assert kind in {'statefulset', 'service', 'secret'}
                value = copy.deepcopy({'statefulset': database, 'service': state.rows[service_key], 'secret': secret}[kind])
                if state.guard_fault == 'credential' and kind == 'secret':
                    value['metadata']['uid'] = str(uuid4())
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', execute)
    return state, connect, {'kubeconfig': kubeconfig, 'executable': Path('/usr/bin/kubectl')}


def test_selected_guard_uses_release_owner_and_real_backend(guard_connection):
    state, _, options = guard_connection
    for phase, status, accepted in [('preflight', 'open', True), ('preflight', 'held', True),
        ('guarded', 'held', True), ('ready', 'held', True), ('released', 'open', True),
        ('guarded', 'open', False), ('ready', 'skipped_locked', False), ('released', 'held', False),
        ('preflight', 'skipped_locked', False)]:
        state.guard_queries.clear()
        state.guard_status = status
        with observer(state.retained, lambda: state.rows)[0] as live:
            before = evidence(state.retained)
            if accepted:
                assert live.observe_guard(phase=phase, **options) == status
                assert len(state.guard_queries) >= 2
            else:
                with pytest.raises(ValueError, match='preserve'):
                    live.observe_guard(phase=phase, **options)
            assert evidence(state.retained) == before
            assert state.writes == []


@pytest.mark.parametrize('staging_attachment_inputs', [2], indirect=True)
def test_guard_checks_receipts_without_reprojecting_each_sql_challenge(guard_connection, monkeypatch):
    state, _, options = guard_connection
    projections = []
    original = module()._ResourceView._pool

    def project(view):
        projections.append(None)
        return original(view)

    with observer(state.retained, lambda: state.rows)[0] as live:
        monkeypatch.setattr(module()._ResourceView, '_pool', project)
        assert live.observe_guard(phase='guarded', **options) == 'held'
    assert len(state.guard_queries) == 2
    # Two complete live observations each project before/after reads; one
    # additional projection pins receipts across the two SQL challenges.
    assert len(projections) <= 5
    assert state.writes == []


@pytest.mark.parametrize('staging_attachment_inputs', [2], indirect=True)
@pytest.mark.parametrize('damage', ['bytes', 'missing', 'extra', 'symlink', 'permissions', 'retained',
    'absent', 'directory'])
def test_guard_rechecks_receipts_and_inputs_before_next_sql_challenge(guard_connection, damage):
    state, connect, options = guard_connection
    file_writer(state, connect).apply_file('20-database.yaml')
    path = state.retained.state_dir / 'candidate-file-writes/dispatch.json'
    writes = list(state.writes)
    with observer(state.retained, lambda: state.rows)[0] as live:
        observations = []
        original = live.observe

        def observe_once(**kwargs):
            observations.append(None)
            return original(**kwargs)

        live.observe = observe_once

        def changed():
            if damage == 'bytes':
                path.write_bytes(path.read_bytes() + b'\n')
            elif damage == 'missing':
                path.unlink()
            elif damage == 'extra':
                private_state._atomic_json(path.parent / 'unexpected.json', {})
            elif damage == 'symlink':
                moved = path.with_suffix('.held')
                path.rename(moved)
                path.symlink_to(moved)
            elif damage == 'permissions':
                path.chmod(0o644)
            elif damage == 'absent':
                private_state._atomic_json(state.retained.anchor_dir /
                    (str(state.retained.reference.operation_id) + '-pool-effects.json'), {})
            elif damage == 'directory':
                path.parent.chmod(0o755)
            else:
                live.retained.inputs.candidate['candidate_sha'] = 'e' * 40

        state.guard_hook = changed
        with pytest.raises(ValueError, match='preserve'):
            live.observe_guard(phase='guarded', **options)
    assert len(observations) == 1, 'reject drift before another full live observation'
    assert len(state.guard_queries) == 1
    assert state.writes == writes


@pytest.mark.parametrize('staging_attachment_inputs', [2], indirect=True)
def test_guard_rejects_new_empty_retry_directory_during_final_sql_challenge(guard_connection):
    import hashlib

    state, _, options = guard_connection
    root = state.retained.state_dir / 'candidate-creations' / hashlib.sha256(b'pre-upgrade-backup').hexdigest()

    def changed():
        if len(state.guard_queries) == 2:
            (root / 'attempt-1').mkdir(mode=0o700, parents=True)
            root.chmod(0o700)
            root.parent.chmod(0o700)

    state.guard_hook = changed
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_guard(phase='guarded', **options)
    assert len(state.guard_queries) == 2
    assert state.writes == []


@pytest.mark.parametrize('damage', ['backend', 'credential', 'pod-restart', 'guard-changed', 'extra-report'])
def test_selected_guard_refuses_wrong_backend_and_late_changes(guard_connection, damage):
    state, _, options = guard_connection
    state.guard_fault = damage
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_guard(phase='guarded', **options)
    assert len(state.guard_queries) == (0 if damage in {'backend', 'credential'} else 1 if damage != 'guard-changed' else 2)
    assert state.writes == []


def test_selected_guard_follows_real_database_writer_without_recapturing_authority(guard_connection):
    state, connect, options = guard_connection
    with observer(state.retained, lambda: state.rows)[0] as live:
        def qualified(key):
            api = connect(key)
            original = api.qualify_resource

            def qualify(retained, selected):
                original(retained, selected)
                live.observe_guard(phase='guarded', preparing=selected, **options)

            api.qualify_resource = qualify
            return api

        file_writer(state, qualified).apply_file('20-database.yaml')
        assert len(state.writes) == 2
        assert live.observe_guard(phase='ready', **options) == 'held'
        state.guard_status = 'open'
        with pytest.raises(ValueError, match='preserve'):
            file_writer(state, qualified).apply_file('20-database.yaml')
        assert len(state.writes) == 2


@pytest.mark.parametrize('damage', ['source', 'kubeconfig', 'resource'])
def test_selected_guard_does_not_outlive_its_release_and_connection(guard_connection, damage):
    state, _, options = guard_connection

    def changed():
        if damage == 'resource':
            key = 'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-control-plane'
            state.rows[key]['spec']['replicas'] = 0
        else:
            path = options['kubeconfig'] if damage == 'kubeconfig' else state.retained.state_dir / 'inputs.json'
            path.write_bytes(path.read_bytes() + b'\n')

    state.guard_hook = changed
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_guard(phase='guarded', **options)
    assert state.writes == []


@pytest.mark.parametrize('kind', ['StatefulSet', 'Service'])
def test_selected_guard_rejects_recaptured_database_identity(guard_connection, operation_inputs, tmp_path, kind):
    from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout

    state, _, options = guard_connection
    inputs = copy.deepcopy(operation_inputs)
    namespace = state.retained.inputs.config['namespace']
    key = kind + ':' + namespace + ':loom-postgres'
    row, = (row for row in inputs['current'] if _key(row) == key)
    row['metadata']['uid'] = str(uuid4())
    inputs.update(state_root=tmp_path / 'recaptured', anchor_dir=tmp_path / 'recaptured-anchors')
    retained = retain_participant_rollout(**inputs)
    state.rows[key]['metadata']['uid'] = row['metadata']['uid']
    with observer(retained, lambda: state.rows)[0] as live:
        assert live.observe()[key]['metadata']['uid'] == row['metadata']['uid']
        with pytest.raises(ValueError, match='preserve'):
            live.observe_guard(phase='guarded', **options)
    assert state.guard_calls == [], 'a fresh capture must not replace the retained database identity'


@pytest.mark.timeout(180)
def test_selected_guard_blocks_real_pool_effects_after_ownership_loss(guard_connection, pool_connection):
    state, _, options = guard_connection
    pool_state, kube, connect = pool_connection
    assert state.retained == pool_state.retained
    with observer(state.retained, lambda: {**state.rows, **pool_state.api.resources})[0] as live:
        pool_state.qualify = lambda phase: live.observe_guard(phase=phase, **options)
        pool = connect()
        state.guard_status = 'open'
        pool.preflight(kube, state.retained.release)
        with pytest.raises(ValueError, match='preserve'):
            pool.activate(kube, state.retained.release)
        assert pool_state.effects == []
        state.guard_status = 'held'
        assert pool.activate(kube, state.retained.release) == 'ready'
        state.guard_status = 'skipped_locked'
        with pytest.raises(ValueError, match='preserve'):
            pool.finish(kube, state.retained.release)
        assert pool_state.effects == ['config', 'fence', 'stage', 'config', 'manager']
        state.guard_status = 'held'
        pool.finish(kube, state.retained.release)
        pool.read_capacity(kube, state.retained.release, observation_id=uuid4())
        before = evidence(pool_state.retained)
        assert live.observe()[_key(pool_state.manager)] == pool_state.manager
        assert evidence(pool_state.retained) == before
        assert pool_state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity']


def test_observation_is_read_only_and_keeps_unwritten_resources_at_original(resource_connection):
    state, _ = resource_connection
    with observer(state.retained, lambda: state.rows)[0] as live:
        before = evidence(state.retained)
        observed = live.observe()
        assert observed['Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service'] == state.rows[
            'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service']
        assert evidence(state.retained) == before
        key = 'Service:' + state.retained.inputs.config['namespace'] + ':loom-postgres'
        state.rows[key]['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


@pytest.mark.parametrize('failure', [None, 'lost-before'])
def test_observer_qualifies_real_resource_writer_during_preparation_and_recovery(resource_connection, failure):
    state, connect = resource_connection
    with observer(state.retained, lambda: state.rows)[0] as live:
        state.failure = failure

        def qualified(key):
            api = connect(key)
            original = api.qualify_resource

            def qualify(retained, selected):
                original(retained, selected)
                live.observe(preparing=selected)

            api.qualify_resource = qualify
            return api

        if failure is None:
            file_writer(state, qualified).apply_file('20-database.yaml')
            assert len(state.writes) == 2
        else:
            with pytest.raises(ValueError, match='preserve'):
                file_writer(state, qualified).apply_file('20-database.yaml')
            assert len(state.writes) == 1
        before = evidence(state.retained)
        assert live.observe()
        assert evidence(state.retained) == before


@pytest.mark.parametrize('damage', ['parent', 'anchor', 'child', 'unknown-journal', 'symlink-directory'])
def test_observer_rejects_missing_or_unrelated_dispatch_evidence(resource_connection, damage):
    state, connect = resource_connection
    file_writer(state, connect).apply_file('20-database.yaml')
    if damage == 'parent':
        (state.retained.state_dir / 'candidate-file-writes/dispatch.json').unlink()
    elif damage == 'anchor':
        next(state.retained.anchor_dir.glob('*-candidate-file-writes.json')).unlink()
    elif damage == 'child':
        next((state.retained.state_dir / 'candidate-resources').rglob('transition.json')).unlink()
    elif damage == 'symlink-directory':
        root = state.retained.state_dir / 'candidate-resources'
        moved = state.retained.state_dir / 'moved-resources'
        root.rename(moved)
        root.symlink_to(moved, target_is_directory=True)
    else:
        private_state._atomic_json(state.retained.state_dir / 'candidate-resources/foreign.json', {})
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


def test_job_retry_observation_selects_current_uid_and_preserves_predecessors(creation_connection):
    state, connect = creation_connection
    with observer(state.retained, lambda: state.external.resources)[0] as live:
        def qualified(key):
            api = connect(key)
            api.qualify_creation = lambda *_: live.observe(preparing=key)
            return api

        writer = job_writer(state, qualified)
        state.auto_outcome = 'failed'
        with pytest.raises(ValueError, match='preserve'):
            writer.run_job('pre-upgrade-backup', retry_failed_jobs=False)
        original = next(row for row in state.external.resources.values() if row['kind'] == 'Job')
        old_uid = original['metadata']['uid']
        state.auto_outcome = 'complete'
        result = job_writer(state, qualified).run_job('pre-upgrade-backup', retry_failed_jobs=True)
        assert result['job_uid'] != old_uid
        assert live.observe()[_key(original)]['metadata']['uid'] == result['job_uid']
        assert len(state.deletes) == 1
        assert len(list((state.retained.state_dir / 'candidate-creations').rglob('stage.json'))) == 2


def test_changed_receipt_during_live_reads_cannot_return_qualification(resource_connection):
    state, connect = resource_connection
    file_writer(state, connect).apply_file('20-database.yaml')
    path = state.retained.state_dir / 'candidate-file-writes/dispatch.json'
    changed = False

    def change(_):
        nonlocal changed
        if not changed:
            record = json.loads(path.read_bytes())
            record['started'] = []
            private_state._atomic_json(path, record)
            changed = True

    with observer(state.retained, lambda: state.rows, after_read=change)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


def test_unrecorded_creation_after_absence_read_is_rejected(creation_connection):
    state, _ = creation_connection
    document = next(row for row in state.retained.release.files['10-config-network.yaml']
        if row['metadata']['name'] == 'loom-feature-config')

    def appear(path):
        if path.endswith('/configmaps/loom-feature-config'):
            row = copy.deepcopy(document)
            row['metadata'].update(uid=str(uuid4()), resourceVersion='99')
            state.external.resources[_key(row)] = row

    with observer(state.retained, lambda: state.external.resources, after_read=appear)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


def test_pool_completion_receipt_must_identify_the_selected_participant(pool_connection):
    state, kube, connect = pool_connection
    assert connect().activate(kube, state.retained.release) == 'ready'
    path = state.retained.state_dir / 'pool-refresh/dispatch.json'
    record = json.loads(path.read_bytes())
    record['steps']['fence']['result']['refresh']['participant_id'] = str(uuid4())
    private_state._atomic_json(path, record)
    with observer(state.retained, lambda: state.api.resources)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


@pytest.mark.parametrize(('phase', 'expected'), [
    ('preflight', {'ready'}), ('preview', {'ready'}), ('config-only', {'ready'}), ('before', {'ready', 'fenced'}),
    ('after', {'staged'}), ('busy', {'ready'}), ('staged', {'staged'}), ('complete', {'complete'}),
])
def test_registry_alternatives_follow_actual_job_dispatch(pool_connection, phase, expected):
    """Preparing a ConfigMap is not permission for a registry state transition."""
    state, kube, connect = pool_connection
    pool = connect()
    if phase == 'config-only':
        def stop_after_config(_):
            if state.effects == ['config']:
                raise ValueError('interrupted before Job dispatch')

        state.qualify = stop_after_config
        with pytest.raises(ValueError, match='preserve'):
            pool.activate(kube, state.retained.release)
        assert state.effects == ['config']
    elif phase in {'preview', 'before'}:
        state.failure = phase
        with pytest.raises(ValueError, match='preserve'):
            pool.activate(kube, state.retained.release)
    elif phase != 'preflight':
        state.failure = 'after' if phase == 'after' else None
        state.busy = phase == 'busy'
        assert pool.activate(kube, state.retained.release) == ('busy' if state.busy else 'ready')
        if phase == 'complete':
            pool.finish(kube, state.retained.release)
    before = evidence(state.retained)
    assert module()._ResourceView(state.retained, None).registry_states == frozenset(expected)
    assert evidence(state.retained) == before


@pytest.mark.parametrize(('action', 'expected'), [('stage', {'fenced'}), ('open', {'staged'})])
def test_registry_pending_work_preserves_prior_state(pool_connection, action, expected):
    state, kube, connect = pool_connection

    def pending_report(_):
        for row in state.reports.values():
            if row['status'] == ('staged' if action == 'stage' else 'complete'):
                row['status'] = 'pending_work'

    state.qualify = pending_report
    pool = connect()
    with pytest.raises(ValueError, match='preserve'):
        pool.activate(kube, state.retained.release)
        assert action == 'open'
        pool.finish(kube, state.retained.release)
    assert module()._ResourceView(state.retained, None).registry_states == frozenset(expected)


@pytest.mark.parametrize('action', ['stage', 'open'])
def test_registry_rejects_later_dispatch_after_pending_work(pool_connection, action):
    """A successful Job can report pending work, not a committed transition."""
    state, kube, connect = pool_connection
    pool = connect()
    assert pool.activate(kube, state.retained.release) == 'ready'
    pool.finish(kube, state.retained.release)
    pool.read_capacity(kube, state.retained.release, observation_id=uuid4())
    path = state.retained.state_dir / 'pool-refresh/dispatch.json'
    record = json.loads(path.read_bytes())
    record['steps'][action]['result']['refresh']['status'] = 'pending_work'
    private_state._atomic_json(path, record)
    with observer(state.retained, lambda: state.api.resources)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()
