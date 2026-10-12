"""Concrete refresh Job execution keeps shared input and uncertain writes bound."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_participant_release import (
    catalog_inputs as catalog_inputs,
)
from tests.ops.test_nebius_pool_participant_release import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_participant_release import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_participant_release import (
    release_inputs as release_inputs,
)
from tests.ops.test_nebius_pool_participant_release import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom_service.pool_management.capacity import digest


def module():
    name = 'scripts.ops.nebius_pool_participant_job'
    assert importlib.util.find_spec(name) is not None, 'concrete participant refresh Job adapter is missing'
    return importlib.import_module(name)


def job_request(inputs, action='fence'):
    return module().PoolParticipantJobRequest(release=inputs['release'], manager=inputs['manager'],
        action=action, observation_id=uuid4() if action in {'observe', 'capacity'} else None)


def test_refresh_actions_share_one_input_and_keep_distinct_recorded_jobs(catalog_inputs, tmp_path):
    current = job_request(catalog_inputs)
    api = PhaseAPI(current.release.migration.registration.binding)
    jobs = []
    for action in ('observe', 'fence', 'stage', 'open', 'observe'):
        operation = replace(current, action=action, observation_id=uuid4() if action == 'observe' else None)
        first = module().stage_participant_job(request=operation, api=api, state_dir=tmp_path)
        assert module().stage_participant_job(request=operation, api=api, state_dir=tmp_path) == first
        jobs.append(first['job_name'])
    operation_id = current.release.refresh.after.operation_id.hex
    assert len(set(jobs)) == 5
    assert jobs[1:4] == ['loom-pool-refresh-' + operation_id + '-' + action for action in ('fence', 'stage', 'open')]
    assert [key for key in api.creates if key.startswith('ConfigMap:')] == ['ConfigMap:loom-pool-refresh-' + operation_id]
    assert len(api.creates) == 6
    assert len(list(tmp_path.rglob('stage.json'))) == 6


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_job_create_never_recreates_or_adopts(catalog_inputs, tmp_path, failure):
    current = job_request(catalog_inputs)

    class API(PhaseAPI):
        def create_resource(self, doc):
            self.failure = failure if doc['kind'] == 'Job' else None
            super().create_resource(doc)

    api = API(current.release.migration.registration.binding)
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ValueError, match='preserve'):
                module().stage_participant_job(request=current, api=api, state_dir=tmp_path)
        else:
            module().stage_participant_job(request=current, api=api, state_dir=tmp_path)
    assert len(api.creates) == 2
    with pytest.raises(ValueError, match='preserve'):
        module().stage_participant_job(request=current, api=api, state_dir=tmp_path / 'foreign-history')
    assert len(api.creates) == 2


def test_replaced_shared_input_blocks_later_action_before_job_create(catalog_inputs, tmp_path):
    current = job_request(catalog_inputs, 'observe')
    api = PhaseAPI(current.release.migration.registration.binding)
    module().stage_participant_job(request=current, api=api, state_dir=tmp_path)
    config, = (value for value in api.resources.values() if value['kind'] == 'ConfigMap')
    config['metadata']['uid'] = str(uuid4())
    with pytest.raises(ValueError, match='preserve'):
        module().stage_participant_job(request=replace(current, action='fence', observation_id=None), api=api, state_dir=tmp_path)
    assert len(api.creates) == 2


@pytest.mark.parametrize('outcome', ['complete', 'lost-before-create', 'lost-after-create'])
def test_concrete_https_adapter_stages_only_fixed_documents_without_write_retry(catalog_inputs, tmp_path, outcome):
    current = job_request(catalog_inputs)
    binding = current.release.migration.registration.binding
    external = PhaseAPI(binding)
    calls = []

    def respond(message):
        calls.append(message)
        path = message.url.path
        if path in ('/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + binding.namespace):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': namespace, 'uid': binding.kube_system_uid if namespace == 'kube-system' else binding.namespace_uid,
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if message.method == 'GET':
            collection, name = path.rsplit('/', 2)[-2:]
            kind = {'jobs': 'Job', 'configmaps': 'ConfigMap'}[collection]
            row = external.resources.get(kind + ':' + name)
            return httpx.Response(404) if row is None else httpx.Response(200, json=row)
        assert message.method == 'POST'
        document = json.loads(message.content)
        assert path == ('/api/v1' if document['kind'] == 'ConfigMap' else '/apis/batch/v1') + '/namespaces/' + binding.namespace + (
            '/configmaps' if document['kind'] == 'ConfigMap' else '/jobs')
        if message.url.params:
            assert dict(message.url.params) == {'dryRun': 'All'}
            return httpx.Response(201, json=external.default_resource(document))
        if document['kind'] == 'Job' and outcome == 'lost-before-create':
            raise httpx.ReadError('private-marker')
        external.create_resource(document)
        if document['kind'] == 'Job' and outcome == 'lost-after-create':
            raise httpx.ReadError('private-marker')
        return httpx.Response(201, json=external.resources[external.key(document)])

    api = module().HTTPSPoolParticipantJobAPI(request=current, api_server='https://cluster.example', ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(respond))
    with api:
        for _ in range(2):
            if outcome == 'lost-before-create':
                with pytest.raises(ValueError, match='preserve') as error:
                    module().stage_participant_job(request=current, api=api, state_dir=tmp_path)
                assert 'private-marker' not in str(error.value)
            else:
                result = module().stage_participant_job(request=current, api=api, state_dir=tmp_path)
                assert len(result['resource_uids']) == 2
                assert api.refresh_report(tmp_path) is None  # Creation alone is not SQL success.
    actual_writes = [message for message in calls if message.method == 'POST' and not message.url.params]
    assert len(actual_writes) == 2  # One ConfigMap attempt and one Job attempt, even on response loss.
    job = json.loads(actual_writes[-1].content)
    container, = job['spec']['template']['spec']['containers']
    assert container['command'] == ['python', '-m', 'loom_service.pool_management.participant_refresh', 'fence']
    assert container['image'] == current.manager['spec']['template']['spec']['containers'][0]['image']


@pytest.fixture
def refresh_live(catalog_inputs, tmp_path, request):
    action = getattr(request, 'param', 'fence')
    current = job_request(catalog_inputs, action)
    binding = current.release.migration.registration.binding
    fake = PhaseAPI(binding)
    module().stage_participant_job(request=current, api=fake, state_dir=tmp_path)
    job, = (row for row in fake.resources.values() if row['kind'] == 'Job')
    config, = (row for row in fake.resources.values() if row['kind'] == 'ConfigMap')
    name, uid = job['metadata']['name'], job['metadata']['uid']
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        **copy.deepcopy(job['spec']['template']['metadata']), 'namespace': binding.namespace,
        'name': name + '-abc', 'uid': str(uuid4()), 'ownerReferences': [{'apiVersion': 'batch/v1',
            'kind': 'Job', 'name': name, 'uid': uid, 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': copy.deepcopy(job['spec']['template']['spec']), 'status': {'phase': 'Succeeded',
            'containerStatuses': [{'name': 'register', 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}]}}
    refresh = current.release.refresh
    report = {'schema_version': 'loom.pool-participant-refresh-receipt.v1', 'operation_id': str(refresh.after.operation_id),
        'participant_id': str(refresh.participant_id), 'pool_id': str(refresh.after.pool_id),
        'status': {'observe': 'ready', 'fence': 'fenced', 'stage': 'staged', 'open': 'complete', 'capacity': 'complete'}[action],
        'before_sha256': digest(refresh.before.model_dump(mode='json')),
        'after_sha256': digest(refresh.after.model_dump(mode='json'))}
    if action == 'capacity':
        # Deliberately historical: this API verifies a receipt, not its usability
        # for today's guard release. The consuming parent must require freshness.
        observed = datetime(2026, 1, 1, tzinfo=UTC)
        report['capacity'] = {'checked_at': (observed + timedelta(seconds=1)).isoformat(),
            'valid_until': (observed + timedelta(seconds=refresh.after.admission.observation_max_age_seconds)).isoformat(),
            'selected_observation_id': str(uuid4()), 'selected_observed_at': observed.isoformat(),
            'connected_pools': 1, 'observations_sha256': 'e' * 64}
    state = SimpleNamespace(request=current, state_dir=tmp_path, job=job, config=config, pod=pod, report=report,
        log=None, calls=[], extra_pod=False, continuation=False, namespace_drift=False, final_drift=False)

    def respond(message):
        state.calls.append(message)
        assert message.method == 'GET'
        path = message.url.path
        if path in ('/api/v1/namespaces/kube-system', '/api/v1/namespaces/' + binding.namespace):
            namespace = path.rsplit('/', 1)[-1]
            identity = binding.kube_system_uid if namespace == 'kube-system' else binding.namespace_uid
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': namespace, 'uid': str(uuid4()) if state.namespace_drift else identity,
                'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if '/configmaps/' in path:
            return httpx.Response(200, json=state.config)
        if '/jobs/' in path:
            return httpx.Response(200, json=state.job)
        base = '/api/v1/namespaces/' + binding.namespace + '/pods'
        if path == base:
            assert dict(message.url.params) == {'labelSelector': 'batch.kubernetes.io/controller-uid=' + uid, 'limit': '2'}
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                'metadata': {'continue': 'next' if state.continuation else ''}, 'items': [state.pod] * (2 if state.extra_pod else 1)})
        if path.endswith('/log'):
            assert dict(message.url.params) == {'container': 'register', 'limitBytes': '16384', 'timestamps': 'false'}
            return httpx.Response(200, content=state.log if state.log is not None else json.dumps(state.report).encode())
        assert path == base + '/' + state.pod['metadata']['name']
        value = copy.deepcopy(state.pod)
        if state.final_drift:
            value['metadata']['uid'] = str(uuid4())
        return httpx.Response(200, json=value)

    api = module().HTTPSPoolParticipantJobAPI(request=current, api_server='https://cluster.example', ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(respond))
    return api, state


@pytest.mark.parametrize('refresh_live', ['observe', 'fence', 'stage', 'open', 'capacity'], indirect=True)
def test_refresh_receipt_binds_exact_action_job_pod_and_committed_result(refresh_live):
    api, state = refresh_live
    with api:
        proof = api.refresh_report(state.state_dir)
        assert proof == {'job_uid': state.job['metadata']['uid'], 'pod_uid': state.pod['metadata']['uid'],
            'refresh': state.report}
        assert api.refresh_report(state.state_dir) == proof  # Historical receipt, not fresh observation.


@pytest.mark.parametrize('refresh_live', ['capacity'], indirect=True)
@pytest.mark.parametrize('damage', ['missing', 'status', 'unknown-field', 'zero-id', 'bool-count', 'zero-count',
    'too-many', 'checksum', 'naive-time', 'negative-window', 'extended-window', 'future-observation'])
def test_capacity_receipt_rejects_malformed_or_unbounded_readiness(refresh_live, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = refresh_live
    proof = state.report['capacity']
    if damage == 'missing':
        del state.report['capacity']
    elif damage == 'status':
        state.report['status'] = 'staged'
    elif damage == 'unknown-field':
        proof['private_plan'] = 'private-marker'
    elif damage == 'zero-id':
        proof['selected_observation_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage in {'bool-count', 'zero-count', 'too-many'}:
        proof['connected_pools'] = {'bool-count': True, 'zero-count': 0, 'too-many': 1001}[damage]
    elif damage == 'checksum':
        proof['observations_sha256'] = 'not-a-fingerprint'
    elif damage == 'naive-time':
        proof['checked_at'] = '2026-01-01T00:00:01'
    elif damage == 'negative-window':
        proof['valid_until'] = proof['selected_observed_at']
    elif damage == 'extended-window':
        proof['valid_until'] = '2026-01-02T00:00:00+00:00'
    else:
        proof['selected_observed_at'] = '2026-01-02T00:00:00+00:00'
    with api, pytest.raises(ManagementStageError):
        api.refresh_report(state.state_dir)


@pytest.mark.parametrize('damage', ['pending', 'failed', 'replaced_job', 'config', 'extra_pod', 'continuation',
    'namespace_drift', 'owner', 'image', 'mount', 'privileged', 'restart', 'nonzero', 'report', 'checksum',
    'operation', 'participant', 'pool', 'wrong_action', 'log_secret', 'log_oversize', 'trailer', 'final_drift',
    'lost_input_history', 'lost_job_history'])
def test_unqualified_refresh_never_becomes_rollout_evidence(refresh_live, damage):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api, state = refresh_live
    if damage == 'pending':
        state.job['status'] = {}
    elif damage == 'failed':
        state.job['status']['conditions'] = [{'type': 'Failed', 'status': 'True'}]
    elif damage == 'replaced_job':
        state.job['metadata']['uid'] = str(uuid4())
    elif damage == 'config':
        state.config['data']['refresh.json'] = '{}'
    elif damage in {'extra_pod', 'continuation', 'namespace_drift', 'final_drift'}:
        setattr(state, damage, True)
    elif damage == 'owner':
        state.pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage in {'image', 'mount', 'privileged'}:
        container = state.pod['spec']['containers'][0]
        if damage == 'image':
            container['image'] = 'foreign:latest'
        elif damage == 'mount':
            state.pod['spec']['volumes'].append({'name': 'foreign', 'hostPath': {'path': '/'}})
        else:
            container['securityContext']['privileged'] = True
    elif damage in {'restart', 'nonzero'}:
        container = state.pod['status']['containerStatuses'][0]
        if damage == 'restart':
            container['restartCount'] = 1
        else:
            container['state']['terminated']['exitCode'] = 1
    elif damage in {'report', 'checksum', 'operation', 'participant', 'pool', 'wrong_action'}:
        key, value = {'report': ('private_plan', 'private-marker'), 'checksum': ('after_sha256', '0' * 64),
            'operation': ('operation_id', str(uuid4())), 'participant': ('participant_id', str(uuid4())),
            'pool': ('pool_id', str(uuid4())), 'wrong_action': ('status', 'complete')}[damage]
        state.report[key] = value
    elif damage == 'log_secret':
        state.log = b'private-marker'
    elif damage == 'log_oversize':
        state.log = b' ' * 16385
    elif damage == 'trailer':
        state.log = json.dumps(state.report).encode() + b'\nprivate-marker'
    elif damage == 'lost_input_history':
        (state.state_dir / 'inputs/stage.json').unlink()
    elif damage == 'lost_job_history':
        (state.state_dir / 'jobs' / state.job['metadata']['name'] / 'stage.json').unlink()
    with api:
        if damage == 'pending':
            assert api.refresh_report(state.state_dir) is None
        else:
            with pytest.raises(ManagementStageError) as error:
                api.refresh_report(state.state_dir)
            assert 'private-marker' not in str(error.value)
