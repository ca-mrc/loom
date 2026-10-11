"""Live resource qualification follows actual retained dispatch, never wishful targets."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from uuid import uuid4

import httpx
import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_participant_resource import _resource_path
from tests.ops.test_nebius_pool_participant_adapter import connection as pool_connection
from tests.ops.test_nebius_pool_participant_creation import connection as creation_connection
from tests.ops.test_nebius_pool_participant_creation import job_writer
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import staging_attachment_inputs as staging_attachment_inputs
from tests.ops.test_nebius_pool_participant_resource import connection as resource_connection
from tests.ops.test_nebius_pool_participant_resource import file_writer


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
            rows = {_key(row): copy.deepcopy(row) for row in (*retained.inputs.current,
                retained.inputs.manager, retained.inputs.catalog)}
            rows.update(resources())
            result = next((row for row in rows.values() if _resource_path(row) == request.url.path), None)
        if after_read is not None:
            after_read(request.url.path)
        return httpx.Response(404) if result is None else httpx.Response(200, json=result)

    instance.client.close()
    instance.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
    return instance, requests


def evidence(retained):
    return {path: path.read_bytes() for root in (retained.state_dir, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()}


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


@pytest.mark.parametrize('damage', ['parent', 'anchor', 'child', 'unknown-journal'])
def test_observer_rejects_missing_or_unrelated_dispatch_evidence(resource_connection, damage):
    state, connect = resource_connection
    file_writer(state, connect).apply_file('20-database.yaml')
    if damage == 'parent':
        (state.retained.state_dir / 'candidate-file-writes/dispatch.json').unlink()
    elif damage == 'anchor':
        next(state.retained.anchor_dir.glob('*-candidate-file-writes.json')).unlink()
    elif damage == 'child':
        next((state.retained.state_dir / 'candidate-resources').rglob('transition.json')).unlink()
    else:
        private_state._atomic_json(state.retained.state_dir / 'candidate-resources/foreign.json', {})
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe()


def test_pool_adapter_uses_live_observer_through_catalog_and_job_transitions(pool_connection):
    state, kube, connect = pool_connection
    with observer(state.retained, lambda: state.api.resources)[0] as live:
        state.qualify = lambda _: live.observe()
        pool = connect()
        assert pool.activate(kube, state.retained.release) == 'ready'
        pool.finish(kube, state.retained.release)
        pool.read_capacity(kube, state.retained.release, observation_id=uuid4())
        before = evidence(state.retained)
        assert live.observe()[_key(state.manager)] == state.manager
        assert evidence(state.retained) == before
        assert state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity']


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
