"""Fixed candidate/backup creation retains exact UID lineage and uncertain intent."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops import nebius_pool_participant_operation as operation
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI


def module():
    name = 'scripts.ops.nebius_pool_participant_creation'
    assert importlib.util.find_spec(name) is not None, 'retained candidate and backup creation is missing'
    return importlib.import_module(name)


@pytest.fixture
def connection(operation_inputs):
    ns = operation_inputs['config']['namespace']
    operation_inputs['rendered_files']['10-config-network.yaml'].append({
        'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'loom-feature-config', 'namespace': ns},
        'data': {'mode': 'candidate-feature'}})
    retained = operation.retain_participant_rollout(**operation_inputs)
    external = PhaseAPI(retained.release.migration.registration.binding)
    external.key = _key
    state = SimpleNamespace(retained=retained, external=external, requests=[], allow=True, before_dispatch=[],
        pods=[], continuation=False, job_reads=0, replace_during_observation=False,
        deletes=[], delete_failure=None, retain_deleted_pods=False, deleting=False,
        auto_outcome=None,
        keys=[_key(retained.release.files[phase][0]) for phase in ('30-migrate.yaml', '50-configure.yaml')]
            + ['pre-upgrade-backup', 'ConfigMap:' + ns + ':loom-feature-config'])

    def qualify(current, key):
        assert current == retained and key in state.keys
        if not state.allow:
            raise ValueError('guard ownership unavailable')

    def connect(key):
        api = module().HTTPSParticipantCreationAPI(retained=retained, key=key,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_creation=qualify)

        def respond(message):
            state.requests.append(message)
            if message.url.path == '/api/v1/namespaces/' + ns + '/pods':
                assert message.method == 'GET' and dict(message.url.params) == {'limit': '1000'}
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                    'metadata': {'resourceVersion': '27', 'continue': 'more' if state.continuation else ''}, 'items': state.pods})
            prefix = '/api/v1/namespaces/' + ns + '/configmaps' if key.startswith('ConfigMap:') else (
                '/apis/batch/v1/namespaces/' + ns + '/jobs')
            if message.method == 'GET':
                assert message.url.path.startswith(prefix + '/')
                name = message.url.path.removeprefix(prefix + '/')
                kind = 'ConfigMap' if key.startswith('ConfigMap:') else 'Job'
                actual = external.resources.get(kind + ':' + ns + ':' + name)
                if kind == 'Job':
                    state.job_reads += 1
                    if state.replace_during_observation and state.job_reads == 2:
                        actual = copy.deepcopy(actual)
                        actual['metadata']['uid'] = str(uuid4())
                return httpx.Response(404) if actual is None else httpx.Response(200, json=actual)
            if message.method == 'DELETE':
                options = json.loads(message.content)
                kind = 'Job'
                name = message.url.path.removeprefix(prefix + '/')
                actual_key = kind + ':' + ns + ':' + name
                actual = external.resources[actual_key]
                assert options == {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'propagationPolicy': 'Foreground',
                    'preconditions': {'uid': actual['metadata']['uid'],
                        'resourceVersion': actual['metadata']['resourceVersion']}}
                journals = [json.loads(path.read_text()) for path in (retained.state_dir / 'candidate-creations').rglob('retries.json')]
                assert any(row['attempts'][-1]['status'] == 'delete_intent'
                    and row['attempts'][-1]['job_uid'] == actual['metadata']['uid'] for row in journals)
                state.deletes.append(options)
                if state.delete_failure == 'before':
                    raise httpx.ReadError('request unavailable')
                if state.deleting:
                    actual['metadata']['deletionTimestamp'] = '2026-10-11T00:00:00Z'
                    actual['metadata']['finalizers'] = ['foregroundDeletion']
                else:
                    del external.resources[actual_key]
                    if not state.retain_deleted_pods:
                        state.pods = []
                if state.delete_failure == 'after':
                    raise httpx.ReadError('response unavailable')
                return httpx.Response(202, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Success'})
            assert message.method == 'POST' and message.url.path == prefix
            doc = json.loads(message.content)
            if message.url.params:
                assert dict(message.url.params) == {'dryRun': 'All'}
                return httpx.Response(201, json=external.default_resource(doc))
            journals = [json.loads(path.read_text()) for path in (retained.state_dir / 'candidate-creations').rglob('stage.json')]
            pending, = (item for journal in journals for item in journal['resources'].values()
                if item['desired'] == doc)
            state.before_dispatch.append(copy.deepcopy(pending))
            try:
                external.create_resource(doc)
            finally:
                if state.auto_outcome is not None and doc['kind'] == 'Job' and _key(doc) in external.resources:
                    actual = external.resources[_key(doc)]
                    success = state.auto_outcome != 'failed'
                    actual['status'] = {'conditions': [{'type': 'Complete' if success else 'Failed', 'status': 'True'}],
                        'succeeded': int(success), 'failed': int(not success), 'active': 0}
                    pod = terminal_pod(actual, succeeded=success)
                    if state.auto_outcome == 'draining':
                        pod['status']['phase'] = 'Running'
                        pod['status']['containerStatuses'][0]['state'] = {'running': {}}
                    state.pods.append(pod)
            return httpx.Response(201, json=external.get_resource(doc))

        api.client.close()
        api.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        return api

    return state, connect


def terminal_pod(job, *, succeeded):
    template = copy.deepcopy(job['spec']['template'])
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'namespace': job['metadata']['namespace'], 'name': job['metadata']['name'] + '-owned',
        'uid': str(uuid4()), 'resourceVersion': '25', 'labels': template['metadata']['labels'],
        'annotations': template['metadata'].get('annotations', {}),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
            'uid': job['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': {**template['spec'], 'nodeName': 'system-0'}, 'status': {
            'phase': 'Succeeded' if succeeded else 'Failed',
            'containerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0 if succeeded else 1}}}
                for row in template['spec']['containers']],
            'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
                for row in template['spec'].get('initContainers', [])]}}


def fail_recorded_job(state, api, key):
    stage(state, api, key)
    job = state.external.resources[_key(api.document)]
    job['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}], 'active': 0, 'failed': 1}
    state.pods = [terminal_pod(job, succeeded=False)]
    return copy.deepcopy(job)


def job_writer(state, connect):
    from scripts.ops.nebius_pool_participant_resource import RetainedCandidateFileWriter

    return RetainedCandidateFileWriter(retained=state.retained, open_creation=connect,
        open_resource=lambda _: pytest.fail('unexpected candidate resource update'))


@pytest.mark.parametrize('selector', ['pre-upgrade-backup', '30-migrate.yaml', '50-configure.yaml'])
def test_connected_job_writer_executes_once_and_recovers_original_completion(connection, selector):
    state, connect = connection
    state.auto_outcome = 'complete'
    writer = job_writer(state, connect)
    result = writer.run_job(selector, retry_failed_jobs=False)
    assert result['status'] == 'complete'
    assert result['job_name'] == writer.job_name(selector)
    assert job_writer(state, connect).run_job(selector, retry_failed_jobs=False) == result
    assert len(state.external.creates) == 1 and state.deletes == []


def test_connected_job_writer_retries_only_an_existing_failed_attempt(connection):
    state, connect = connection
    writer = job_writer(state, connect)
    state.auto_outcome = 'failed'
    with pytest.raises(ValueError, match='preserve'):
        writer.run_job('30-migrate.yaml', retry_failed_jobs=True)
    assert len(state.external.creates) == 1 and state.deletes == [], 'a newly failed execution needs a new explicit retry'
    state.auto_outcome = 'complete'
    result = writer.run_job('30-migrate.yaml', retry_failed_jobs=True)
    assert result['status'] == 'complete'
    assert writer.run_job('30-migrate.yaml', retry_failed_jobs=False) == result
    assert len(state.external.creates) == 2 and len(state.deletes) == 1


def test_connected_job_writer_does_not_accept_complete_condition_with_live_processes(connection, monkeypatch):
    from scripts.ops import nebius_pool_participant_resource as resource

    state, connect = connection
    state.auto_outcome = 'draining'
    clock = [0]
    monkeypatch.setattr(resource, 'time', SimpleNamespace(monotonic=lambda: clock[0],
        sleep=lambda _: clock.__setitem__(0, 2000)), raising=False)
    with pytest.raises(ValueError, match='preserve'):
        job_writer(state, connect).run_job('pre-upgrade-backup', retry_failed_jobs=False)
    assert len(state.external.creates) == 1 and state.deletes == []


def test_connected_job_writer_cannot_resume_another_jobs_pending_retry(connection):
    state, connect = connection
    state.auto_outcome = 'failed'
    with pytest.raises(ValueError, match='preserve'):
        job_writer(state, connect).run_job('50-configure.yaml', retry_failed_jobs=False)
    state.delete_failure = 'before'
    with connect(state.keys[1]) as other:
        assert other.retry_failed_job() is False
    state.delete_failure = None
    deletes = len(state.deletes)
    with pytest.raises(ValueError, match='preserve'):
        job_writer(state, lambda _: connect(state.keys[1])).run_job('30-migrate.yaml', retry_failed_jobs=True)
    assert len(state.deletes) == deletes, 'factory scope mismatch must not delete another recorded Job'
    assert state.keys[1] in state.external.resources


@pytest.mark.parametrize('selected', [0, 2])
def test_failed_job_retry_preserves_attempt_history_and_uses_exact_delete_preconditions(connection, selected):
    state, connect = connection
    key = state.keys[selected]
    with connect(key) as first:
        original = fail_recorded_job(state, first, key)
        journal, = (state.retained.state_dir / 'candidate-creations').rglob('stage.json')
        original_bytes = journal.read_bytes()
        assert first.retry_failed_job() is True
        with pytest.raises(ValueError, match=r'changed|preserve'):
            first.create_resource(json.loads(original_bytes)['resources'][_key(original)]['desired'])
    with connect(key) as second:
        assert second.retry_pending() is False
        result = stage(state, second, key)
        replacement_uid, = result['resource_uids'].values()
        assert replacement_uid != original['metadata']['uid']
        assert second.document['metadata']['name'] == original['metadata']['name']
        assert second.document['spec'] == first.document['spec']
        assert second.job_outcome() is None
        assert stage(state, second, key) == result
    assert journal.read_bytes() == original_bytes
    assert len(list((state.retained.state_dir / 'candidate-creations').rglob('stage.json'))) == 2
    assert len(state.deletes) == 1 and len(state.external.creates) == 2


def test_multiple_failed_job_retries_preserve_every_predecessor(connection):
    state, connect = connection
    key = state.keys[0]
    history = {}
    retired_uids = []
    for _ in range(2):
        with connect(key) as api:
            original = fail_recorded_job(state, api, key)
            retired_uids.append(original['metadata']['uid'])
            history.update({path: path.read_bytes() for path in
                (state.retained.state_dir / 'candidate-creations').rglob('stage.json')})
            assert api.retry_failed_job() is True
    with connect(key) as api:
        result = stage(state, api, key)
        assert stage(state, api, key) == result
    assert all(path.read_bytes() == raw for path, raw in history.items())
    assert len(list((state.retained.state_dir / 'candidate-creations').rglob('stage.json'))) == 3
    assert len(set(retired_uids + list(result['resource_uids'].values()))) == 3
    assert len(state.external.creates) == 3 and len(state.deletes) == 2


@pytest.mark.parametrize('damage', ['predecessor', 'successor', 'retry-lineage'])
def test_missing_retry_history_cannot_recreate_a_job(connection, damage):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        fail_recorded_job(state, api, key)
        assert api.retry_failed_job() is True
    with connect(key) as api:
        stage(state, api, key)
    root, = (path for path in (state.retained.state_dir / 'candidate-creations').iterdir() if path.is_dir())
    # Only the test-owned journal is removed, simulating loss of retained evidence.
    path = {'predecessor': root / 'stage.json', 'successor': root / 'attempt-1' / 'stage.json',
        'retry-lineage': root / 'retries.json'}[damage]
    path.unlink()
    del state.external.resources[key]
    with pytest.raises(ValueError, match=r'preserve|participant creation'):
        with connect(key) as api:
            stage(state, api, key)
    assert len(state.external.creates) == 2 and len(state.deletes) == 1


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_retry_generation_create_remains_readback_only(connection, failure):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        fail_recorded_job(state, api, key)
        assert api.retry_failed_job() is True
    state.external.failure = failure
    for _ in range(2):
        with connect(key) as api:
            if failure == 'before':
                with pytest.raises(ValueError, match='preserve'):
                    stage(state, api, key)
            else:
                assert len(stage(state, api, key)['resource_uids']) == 1
    assert len(state.external.creates) == 2 and len(state.deletes) == 1


def test_paginated_cleanup_observation_cannot_authorize_recreation(connection):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        fail_recorded_job(state, api, key)
        state.retain_deleted_pods = True
        assert api.retry_failed_job() is False
        state.continuation = True
        state.pods = []
        with pytest.raises(ValueError, match='preserve'):
            api.reconcile_retry()
        with pytest.raises(ValueError, match='preserve'):
            stage(state, api, key)
        assert len(state.external.creates) == 1 and len(state.deletes) == 1
        state.continuation = False
        assert api.reconcile_retry() is True
    with connect(key) as api:
        stage(state, api, key)
    assert len(state.external.creates) == 2 and len(state.deletes) == 1


@pytest.mark.parametrize('selected', [0, 2])
def test_failed_job_that_never_started_a_pod_can_be_explicitly_retried(connection, selected):
    state, connect = connection
    key = state.keys[selected]
    with connect(key) as api:
        stage(state, api, key)
        job = state.external.resources[_key(api.document)]
        # Native Job deadline can expire while Pod admission is unavailable.
        job['status'] = {'conditions': [{'type': 'Failed', 'status': 'True', 'reason': 'DeadlineExceeded'}],
            'active': 0, 'succeeded': 0, 'failed': 0}
        assert state.pods == []
        assert api.job_outcome() == {'status': 'failed', 'job_uid': job['metadata']['uid'],
            'job_name': job['metadata']['name'], 'pod_uids': []}
        assert api.retry_failed_job() is True
    with connect(key) as api:
        stage(state, api, key)
    assert len(state.external.creates) == 2 and len(state.deletes) == 1


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_failed_job_retry_resolves_lost_delete_response_without_unconditional_delete(connection, failure):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        original = fail_recorded_job(state, api, key)
        state.delete_failure = failure
        assert api.retry_failed_job() is (failure == 'after')
        if failure == 'before':
            assert api.retry_pending() is True
            assert api.reconcile_retry() is False
            assert len(state.deletes) == 1, 'observing cleanup cannot redispatch deletion'
            state.delete_failure = None
            # A new explicit recovery can reissue only the original UID/version
            # conditional operation. The HTTP client itself never retries.
            assert api.retry_failed_job() is True
            assert state.deletes[0] == state.deletes[1]
    with connect(key) as api:
        stage(state, api, key)
    assert len(state.external.creates) == 2
    assert state.external.resources[_key(original)]['metadata']['uid'] != original['metadata']['uid']


@pytest.mark.parametrize('residue', ['deleting-job', 'owned-pod', 'unlabelled-owned-pod', 'partial-list'])
def test_retry_waits_for_old_job_and_all_descendants_to_disappear(connection, residue):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        original = fail_recorded_job(state, api, key)
        state.deleting = residue == 'deleting-job'
        state.retain_deleted_pods = True
        state.continuation = residue == 'partial-list'
        if residue == 'partial-list':
            with pytest.raises(ValueError, match='preserve'):
                api.retry_failed_job()
            assert state.deletes == []
            return
        assert api.retry_failed_job() is False
        if residue == 'unlabelled-owned-pod':
            state.pods[0]['metadata']['labels'] = {}
        assert api.reconcile_retry() is False
        with pytest.raises(ValueError, match='preserve'):
            stage(state, api, key)
        assert len(state.external.creates) == 1
        state.external.resources.pop(_key(original), None)
        state.pods = []
        assert api.reconcile_retry() is True
    with connect(key) as api:
        stage(state, api, key)
    assert len(state.deletes) == 1 and len(state.external.creates) == 2


@pytest.mark.parametrize('state_name', ['complete', 'active', 'replaced', 'version-changed', 'guard'])
def test_failed_job_retry_rejects_missing_authority_or_changed_attempt(connection, state_name):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        original = fail_recorded_job(state, api, key)
        if state_name in {'version-changed', 'replaced'}:
            state.delete_failure = 'before'
            assert api.retry_failed_job() is False
            state.delete_failure = None
        job = state.external.resources[_key(original)]
        if state_name == 'complete':
            job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
            state.pods = [terminal_pod(job, succeeded=True)]
        elif state_name == 'active':
            state.pods[0]['status']['phase'] = 'Running'
            state.pods[0]['status']['containerStatuses'][0]['state'] = {'running': {}}
        elif state_name == 'replaced':
            job['metadata']['uid'] = str(uuid4())
        elif state_name == 'version-changed':
            job['metadata']['resourceVersion'] = '999'
        else:
            state.allow = False
        count = len(state.deletes)
        with pytest.raises(ValueError, match='preserve'):
            api.retry_failed_job()
        assert len(state.deletes) == count and len(state.external.creates) == 1


@pytest.mark.parametrize('selected', [0, 2])
@pytest.mark.parametrize('outcome', ['pending', 'complete', 'failed', 'draining', 'success-without-pod'])
def test_job_observation_requires_terminal_owned_processes_without_writes(connection, selected, outcome):
    state, connect = connection
    with connect(state.keys[selected]) as api:
        stage(state, api, state.keys[selected])
        job, = state.external.resources.values()
        if outcome != 'pending':
            complete = outcome in {'complete', 'success-without-pod'}
            job['status'] = {'conditions': [{'type': 'Complete' if complete else 'Failed', 'status': 'True'}],
                'active': 0, 'succeeded': int(complete), 'failed': int(not complete)}
            state.pods = [terminal_pod(job, succeeded=complete)]
            if outcome == 'draining':
                state.pods[0]['status']['phase'] = 'Running'
                state.pods[0]['status']['containerStatuses'][0]['state'] = {'running': {}}
            elif outcome == 'success-without-pod':
                state.pods = []
        state.requests.clear()
        result = api.job_outcome()
        if outcome in {'pending', 'draining', 'success-without-pod'}:
            assert result is None
        else:
            assert result == {'status': outcome, 'job_uid': job['metadata']['uid'], 'job_name': job['metadata']['name'],
                'pod_uids': [state.pods[0]['metadata']['uid']]}
        assert all(row.method == 'GET' for row in state.requests)
        assert len(state.external.creates) == 1


@pytest.mark.parametrize('damage', ['pod-template', 'pod-owner', 'partial-list', 'job-replaced', 'no-success', 'duplicate-uid'])
def test_job_success_rejects_unqualified_execution_evidence(connection, damage):
    state, connect = connection
    with connect(state.keys[0]) as api:
        stage(state, api, state.keys[0])
        job, = state.external.resources.values()
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'active': 0, 'succeeded': 1}
        state.pods = [terminal_pod(job, succeeded=True)]
        if damage == 'pod-template':
            state.pods[0]['spec']['containers'][0]['image'] = 'foreign/image'
        elif damage == 'pod-owner':
            state.pods[0]['metadata']['ownerReferences'][0]['controller'] = False
        elif damage == 'partial-list':
            state.continuation = True
        elif damage == 'job-replaced':
            state.job_reads = 0
            state.replace_during_observation = True
        elif damage == 'no-success':
            state.pods[0]['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = 1
        else:
            state.pods.append(copy.deepcopy(state.pods[0]))
        state.requests.clear()
        with pytest.raises(ValueError, match='preserve'):
            api.job_outcome()
        assert all(row.method == 'GET' for row in state.requests)


def test_backup_success_accounts_for_backoff_pods_but_not_unrelated_running_services(connection):
    state, connect = connection
    with connect('pre-upgrade-backup') as api:
        stage(state, api, 'pre-upgrade-backup')
        job, = state.external.resources.values()
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}],
            'active': 0, 'succeeded': 1, 'failed': 1}
        failed, succeeded = terminal_pod(job, succeeded=False), terminal_pod(job, succeeded=True)
        failed['metadata']['name'] += '-failed'
        unrelated = copy.deepcopy(failed)
        unrelated['metadata'].update(name='ordinary-service', uid=str(uuid4()), labels={'app': 'loom-service'},
            ownerReferences=[])
        unrelated['status'] = {'phase': 'Running'}
        state.pods = [failed, unrelated, succeeded]
        result = api.job_outcome()
        assert result['status'] == 'complete'
        assert result['pod_uids'] == sorted([failed['metadata']['uid'], succeeded['metadata']['uid']])
        assert len(state.external.creates) == 1


def stage(state, api, key):
    return module().stage_participant_creation(retained=state.retained, key=key, api=api)


def test_new_candidate_and_original_backup_are_created_once_with_durable_uids(connection):
    state, connect = connection
    observed = []
    for key in state.keys:
        with connect(key) as api:
            result = stage(state, api, key)
            assert stage(state, api, key) == result
            observed.extend(result['resource_uids'].values())
    assert len(set(observed)) == 4
    assert len(state.external.creates) == 4
    assert all(item['status'] == 'create_intent' and item['uid'] is None for item in state.before_dispatch)
    backup, = (row for row in state.external.resources.values() if row['metadata']['name'].startswith('loom-predeploy-'))
    old, = (row for row in state.retained.inputs.current if row['kind'] == 'CronJob'
        and row['metadata']['name'] == 'loom-platform-backup')
    wanted = old['spec']['jobTemplate']['spec']
    assert backup['spec']['template']['spec'] == wanted['template']['spec']
    assert backup['spec']['activeDeadlineSeconds'] == 1800
    assert backup['metadata']['name'] == 'loom-predeploy-' + state.retained.reference.operation_id.hex
    assert backup['metadata'].get('ownerReferences', []) == []
    migration_key = state.keys[0]
    migration = state.external.resources[migration_key]
    assert migration['spec']['template']['spec']['containers'][0]['command'][-1] == 'database'
    assert migration['spec']['template']['spec']['containers'][0]['image'].endswith('@sha256:' + 'e' * 64)
    assert not any('status' in row for row in state.external.resources.values())  # Creation is not execution proof.


@pytest.mark.parametrize('failure', ['before', 'after'])
@pytest.mark.parametrize('selected', [0, 2])
def test_unknown_candidate_or_backup_create_is_only_read_back(connection, failure, selected):
    state, connect = connection
    key = state.keys[selected]
    state.external.failure = failure
    with connect(key) as api:
        for _ in range(2):
            if failure == 'before':
                with pytest.raises(ValueError, match='preserve'):
                    stage(state, api, key)
            else:
                assert len(stage(state, api, key)['resource_uids']) == 1
    assert len(state.external.creates) == 1


@pytest.mark.parametrize('damage', ['replaced', 'changed', 'missing', 'guard', 'anchor'])
def test_candidate_create_recovery_refuses_drift_without_recreating(connection, damage):
    state, connect = connection
    key = state.keys[0]
    with connect(key) as api:
        stage(state, api, key)
        row = state.external.resources[key]
        if damage == 'replaced':
            row['metadata']['uid'] = str(uuid4())
        elif damage == 'changed':
            row['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
        elif damage == 'missing':
            del state.external.resources[key]
        elif damage == 'guard':
            state.allow = False
        else:
            anchor, = state.retained.anchor_dir.glob('*.json')
            anchor.unlink()
        with pytest.raises(ValueError, match='preserve'):
            stage(state, api, key)
    assert state.external.creates == [key]


@pytest.mark.parametrize('outside', ['existing', 'secret', 'unknown', 'endpoint', 'payload'])
def test_create_connection_cannot_overwrite_or_accept_unretained_resources(connection, outside):
    state, connect = connection
    ns = state.retained.inputs.config['namespace']
    if outside == 'payload':
        with connect(state.keys[0]) as api:
            foreign = copy.deepcopy(state.retained.release.files['30-migrate.yaml'][0])
            foreign['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
            with pytest.raises(ValueError, match='participant creation'):
                api.create_resource(foreign)
        assert not state.requests
        return
    key = {'existing': 'ConfigMap:' + ns + ':loom-platform-config', 'secret': 'Secret:' + ns + ':loom-platform-db',
        'unknown': 'Job:' + ns + ':foreign', 'endpoint': state.keys[0]}[outside]
    with pytest.raises(ValueError, match='participant creation'):
        module().HTTPSParticipantCreationAPI(retained=state.retained, key=key,
            api_server='https://foreign.invalid' if outside == 'endpoint' else state.retained.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_creation=lambda *_: None)
