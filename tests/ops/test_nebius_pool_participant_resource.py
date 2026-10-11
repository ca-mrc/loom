"""Candidate updates use retained originals and atomic UID/version preconditions."""
from __future__ import annotations

import copy
import importlib
import json
import ssl
from contextlib import nullcontext
from decimal import Decimal
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


def module():
    name = 'scripts.ops.nebius_pool_participant_resource'
    assert importlib.util.find_spec(name) is not None, 'retained candidate resource CAS is missing'
    return importlib.import_module(name)


@pytest.fixture
def connection(operation_inputs):
    for row in operation_inputs['current']:
        if row['kind'] == 'Service':
            row['spec'].update(clusterIP='10.0.0.7', clusterIPs=['10.0.0.7'],
                ipFamilies=['IPv4'], ipFamilyPolicy='SingleStack')
            if row['spec'].get('type') == 'LoadBalancer':
                row['spec']['ports'][0]['nodePort'] = 30443
        if row['kind'] == 'Namespace':
            row['spec'] = {'finalizers': ['kubernetes']}
    retained = operation.retain_participant_rollout(**operation_inputs)
    state = SimpleNamespace(retained=retained, rows={_key(row): copy.deepcopy(row) for row in retained.inputs.current},
        writes=[], requests=[], failure=None, qualifications=[], foreign=False)

    def qualify(current, key):
        assert current == retained
        state.qualifications.append(key)
        if state.foreign:
            raise ValueError('guard ownership lost')

    def connect(key):
        api = module().HTTPSParticipantResourceAPI(retained=retained, key=key,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_resource=qualify)

        def respond(message):
            state.requests.append(message)
            expected_paths = {
                'Namespace': '/api/v1/namespaces/',
                'ConfigMap': '/api/v1/namespaces/' + retained.inputs.config['namespace'] + '/configmaps/',
                'Service': '/api/v1/namespaces/' + retained.inputs.config['namespace'] + '/services/',
                'Deployment': '/apis/apps/v1/namespaces/' + retained.inputs.config['namespace'] + '/deployments/',
                'StatefulSet': '/apis/apps/v1/namespaces/' + retained.inputs.config['namespace'] + '/statefulsets/',
                'PersistentVolumeClaim': '/api/v1/namespaces/' + retained.inputs.config['namespace'] + '/persistentvolumeclaims/',
                'ValidatingAdmissionPolicy': '/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/',
                'ValidatingAdmissionPolicyBinding': '/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/',
            }
            row = state.rows[key]
            assert message.url.path == expected_paths[row['kind']] + row['metadata']['name']
            if message.method == 'GET':
                return httpx.Response(200, json=row)
            assert message.method == 'PATCH'
            assert message.headers['content-type'] == 'application/json-patch+json'
            patches = json.loads(message.content)
            assert patches[:2] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': row['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': row['metadata']['resourceVersion']},
            ]
            dry_run = bool(message.url.params)
            if dry_run:
                assert dict(message.url.params) == {'dryRun': 'All'}
            else:
                state.writes.append(key)
                if state.failure == 'conflict':
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
                if state.failure == 'lost-before':
                    raise httpx.ReadError('transport unavailable')
            value = copy.deepcopy(row)
            for item in patches[2:]:
                parts = item['path'].strip('/').split('/')
                owner = value
                for part in parts[:-1]:
                    owner = owner[part]
                if item['op'] == 'test':
                    assert owner[parts[-1]] == item['value']
                elif item['op'] == 'remove':
                    del owner[parts[-1]]
                else:
                    assert item['op'] in {'add', 'replace'}
                    owner[parts[-1]] = item['value']
            if state.failure == 'unsafe-default':
                value['spec']['template']['spec']['hostNetwork'] = True
            if not dry_run:
                value['metadata']['resourceVersion'] = str(int(row['metadata']['resourceVersion']) + 1)
                state.rows[key] = value
                if state.failure == 'lost-after':
                    raise httpx.ReadError('reply lost after commit')
            return httpx.Response(200, json=value)

        api.client.close()
        api.client = httpx.Client(base_url=retained.inputs.config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        return api

    return state, connect


def advance(state, api, key):
    return module().update_participant_resource(retained=state.retained, key=key, api=api)


def file_writer(state, connect):
    return module().RetainedCandidateFileWriter(retained=state.retained,
        open_resource=connect, open_creation=lambda _: pytest.fail('unexpected candidate creation'))


def test_candidate_file_writer_preserves_order_and_reads_back_repeated_dispatch(connection):
    state, connect = connection
    writer = file_writer(state, connect)
    writer.apply_file('20-database.yaml')
    writer.apply_file('20-database.yaml')
    ns = state.retained.inputs.config['namespace']
    assert state.writes == ['Service:' + ns + ':loom-postgres', 'StatefulSet:' + ns + ':loom-postgres']
    assert state.rows[state.writes[0]]['spec']['clusterIP'] == '10.0.0.7'


@pytest.mark.parametrize('kind', ['PersistentVolumeClaim', 'ResourceQuota'])
@pytest.mark.parametrize('different', [False, True])
def test_preserved_infrastructure_accepts_quantity_spellings_but_not_changed_amounts(operation_inputs, kind, different):
    original = next(row for row in operation_inputs['current'] if row['kind'] == kind)
    if kind == 'PersistentVolumeClaim':
        assert original['spec']['resources']['requests']['storage'] == '4Gi'
        original['spec']['resources']['requests']['storage'] = '8192Mi' if different else '4096Mi'
    else:
        value = original['spec']['hard']['requests.cpu']
        if value.endswith('m'):
            original['spec']['hard']['requests.cpu'] = str(Decimal(value[:-1]) / 1000 * (2 if different else 1))
        else:
            original['spec']['hard']['requests.cpu'] = str(Decimal(value) * 1000 * (2 if different else 1)) + 'm'
    retained = operation.retain_participant_rollout(**operation_inputs)

    def connect():
        return module().HTTPSParticipantResourceAPI(retained=retained, key=_key(original),
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_resource=lambda *_: None)

    if different:
        with pytest.raises(ValueError, match='unqualified'):
            connect()
    else:
        with connect() as api:
            assert api.targets[_key(original)]['spec'] == original['spec'], 'preserve the actual storage/quota allocation'


@pytest.mark.parametrize('changed', [False, True])
def test_candidate_file_writer_qualifies_unchanged_identity_policy_without_bootstrap(connection, changed):
    state, connect = connection
    policies = state.retained.release.files.get('00-task-identity-policy.yaml', [])
    if not policies:
        pytest.skip('participant has no private-root admission policy')
    writer = file_writer(state, connect)
    if changed:
        state.rows[_key(policies[0])]['spec']['failurePolicy'] = 'Ignore'
        with pytest.raises(ValueError, match='preserve'):
            writer.qualify_identity_policy()
    else:
        writer.qualify_identity_policy()
        assert len([row for row in state.requests if row.method == 'PATCH']) == 2
    assert state.writes == []
    assert all(row.method == 'GET' or dict(row.url.params) == {'dryRun': 'All'} for row in state.requests)


def test_candidate_file_writer_preserves_existing_storage_without_patch_or_allocation(connection):
    state, connect = connection
    key = 'PersistentVolumeClaim:' + state.retained.inputs.config['namespace'] + ':loom-web-tls'
    original = copy.deepcopy(state.rows[key])
    writer = file_writer(state, connect)
    writer.apply_file('40-services.yaml')
    writer.apply_file('40-services.yaml')
    assert state.rows[key] == original
    assert key not in state.writes
    with connect(key) as api:
        with pytest.raises(ValueError, match='preserve'):
            advance(state, api, key)
        with pytest.raises(ValueError, match='preserve'):
            api.patch_workload(key, original, api.targets[key])
    assert key not in state.writes


def test_candidate_file_writer_refuses_changed_storage_spec_before_application_writes(connection):
    state, connect = connection
    key = 'PersistentVolumeClaim:' + state.retained.inputs.config['namespace'] + ':loom-web-tls'
    state.rows[key]['spec']['resources']['requests']['storage'] = '900Gi'
    with pytest.raises(ValueError, match='preserve'):
        file_writer(state, connect).apply_file('40-services.yaml')
    assert state.writes == []


@pytest.mark.parametrize('failure', ['lost-before', 'lost-after'])
def test_candidate_file_writer_recovery_does_not_repeat_uncertain_patch(connection, failure):
    state, connect = connection
    writer = file_writer(state, connect)
    state.failure = failure
    if failure == 'lost-before':
        for _ in range(2):
            with pytest.raises(ValueError, match='preserve'):
                writer.apply_file('20-database.yaml')
        assert len(state.writes) == 1
    else:
        writer.apply_file('20-database.yaml')
        writer.apply_file('20-database.yaml')
        assert len(state.writes) == 2


@pytest.mark.parametrize('lost', ['child', 'parent', 'anchor'])
def test_candidate_file_writer_rejects_lost_dispatch_evidence(connection, lost):
    state, connect = connection
    writer = file_writer(state, connect)
    writer.apply_file('20-database.yaml')
    if lost == 'child':
        path = next((state.retained.state_dir / 'candidate-resources').rglob('transition.json'))
    elif lost == 'parent':
        path = state.retained.state_dir / 'candidate-file-writes' / 'dispatch.json'
    else:
        path = state.retained.anchor_dir / (str(state.retained.reference.operation_id) + '-candidate-file-writes.json')
    path.unlink()
    state.requests.clear()
    with pytest.raises(ValueError, match='preserve'):
        file_writer(state, connect).apply_file('20-database.yaml')
    assert len(state.writes) == 2
    assert not any(row.method != 'GET' for row in state.requests)


@pytest.mark.parametrize('filename', ['30-migrate.yaml', '50-configure.yaml', '00-task-identity-policy.yaml', '../20-database.yaml'])
def test_candidate_file_writer_cannot_claim_job_or_admission_bootstrap_authority(connection, filename):
    state, connect = connection
    with pytest.raises(ValueError, match='preserve'):
        file_writer(state, connect).apply_file(filename)
    assert state.requests == []


def test_candidate_file_writer_stages_new_objects_in_renderer_order(operation_inputs):
    from scripts.ops.nebius_management_stage import _qualified_defaulted
    from tests.ops.test_nebius_management_stage import PhaseAPI

    # Database file has an existing Service and StatefulSet; add a fixed new
    # ConfigMap. Only external API I/O is doubled; use both real journal engines.
    ns = operation_inputs['config']['namespace']
    operation_inputs['rendered_files']['20-database.yaml'].append({
        'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'loom-db-options', 'namespace': ns},
        'data': {'mode': 'feature'}})
    retained = operation.retain_participant_rollout(**operation_inputs)
    state = PhaseAPI(retained.release.migration.registration.binding)
    state.key = _key
    state.resources = {_key(row): copy.deepcopy(row) for row in retained.inputs.current}
    effects = []

    class Updates:
        def qualify(self):
            pass

        def read_workload(self, key):
            return copy.deepcopy(state.resources[key])

        def preview_workload(self, key, before, desired):
            return copy.deepcopy(desired)

        def patch_workload(self, key, before, desired):
            effects.append(key)
            state.resources[key] = {**copy.deepcopy(desired), 'metadata': {
                **copy.deepcopy(desired['metadata']), 'uid': before['metadata']['uid'], 'resourceVersion': '22'}}
            return True

        def workload_ready(self, key, expected):
            return _qualified_defaulted(expected, state.resources[key]) is not None

    def creations(key):
        effects.append(key)
        return nullcontext(state)

    writer = module().RetainedCandidateFileWriter(retained=retained,
        open_resource=lambda _: nullcontext(Updates()), open_creation=creations)
    writer.apply_file('20-database.yaml')
    assert effects == ['Service:' + ns + ':loom-postgres', 'StatefulSet:' + ns + ':loom-postgres',
        'ConfigMap:' + ns + ':loom-db-options']
    assert state.resources[effects[-1]]['data'] == {'mode': 'feature'}
    assert len(state.creates) == 1


def test_candidate_updates_cover_non_workload_resources_without_changing_allocations(connection):
    state, connect = connection
    namespace = state.retained.inputs.config['namespace']
    keys = ['Namespace:-:' + namespace, 'ConfigMap:' + namespace + ':loom-platform-config',
        'Service:' + namespace + ':loom-postgres', 'StatefulSet:' + namespace + ':loom-postgres',
        'Deployment:' + namespace + ':loom-service', 'Service:' + namespace + ':loom-web']
    originals = copy.deepcopy(state.rows)
    for key in keys:
        with connect(key) as api:
            assert advance(state, api, key) is True
            assert advance(state, api, key) is True
    assert state.writes == keys
    assert all(state.rows[key]['metadata']['uid'] == originals[key]['metadata']['uid'] for key in keys)
    assert state.rows[keys[0]]['spec']['finalizers'] == ['kubernetes']
    for key in (keys[2], keys[-1]):
        assert state.rows[key]['spec']['clusterIPs'] == ['10.0.0.7']
        assert state.rows[key]['spec']['ipFamilyPolicy'] == 'SingleStack'
    assert state.rows[keys[-1]]['spec']['type'] == 'LoadBalancer'
    assert state.rows[keys[-1]]['spec']['ports'][0]['nodePort'] == 30443
    profile = json.loads(state.rows[keys[1]]['data']['profile.json'])
    assert profile['candidate_sha'] == 'd' * 40
    assert all(row['image'].endswith('@sha256:' + 'e' * 64)
        for row in state.rows[keys[4]]['spec']['template']['spec']['containers'])
    assert set(state.rows) == set(originals)
    assert all(state.rows[key] == originals[key] for key in originals.keys() - set(keys))


@pytest.mark.parametrize('failure', ['lost-before', 'lost-after', 'conflict'])
def test_candidate_cas_only_retries_an_explicit_rejection(connection, failure):
    state, connect = connection
    key = 'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service'
    state.failure = failure
    with connect(key) as api:
        if failure == 'lost-before':
            for _ in range(2):
                with pytest.raises(ValueError, match='preserve'):
                    advance(state, api, key)
            assert state.writes == [key]
        elif failure == 'lost-after':
            assert advance(state, api, key) is True
            assert advance(state, api, key) is True
            assert state.writes == [key]
        else:
            assert advance(state, api, key) is False
            state.failure = None
            assert advance(state, api, key) is True
            assert state.writes == [key, key]


@pytest.mark.parametrize('damage', ['uid', 'original', 'anchor', 'guard', 'unsafe-default'])
def test_candidate_update_rejects_lost_authority_or_drift_before_write(connection, damage):
    state, connect = connection
    key = 'Deployment:' + state.retained.inputs.config['namespace'] + ':loom-service'
    with connect(key) as api:
        if damage == 'uid':
            state.rows[key]['metadata']['uid'] = str(uuid4())
        elif damage == 'original':
            state.rows[key]['spec']['replicas'] = 2
        elif damage == 'anchor':
            anchor, = state.retained.anchor_dir.glob('*.json')
            anchor.unlink()
        elif damage == 'guard':
            state.foreign = True
        else:
            state.failure = damage
        with pytest.raises(ValueError, match='preserve'):
            advance(state, api, key)
    assert not state.writes


@pytest.mark.parametrize('outside', ['secret', 'retired', 'manager', 'job', 'endpoint'])
def test_candidate_resource_transport_has_no_arbitrary_write_scope(connection, outside):
    state, _ = connection
    ns = state.retained.inputs.config['namespace']
    keys = {'secret': 'Secret:' + ns + ':loom-platform-db',
        'retired': 'CronJob:' + state.retained.inputs.config['execution_namespace'] + ':loom-execution-capacity-collector',
        'manager': _key(state.retained.inputs.manager),
        'job': _key(state.retained.release.files['30-migrate.yaml'][0]),
        'endpoint': 'Deployment:' + ns + ':loom-service'}
    with pytest.raises(ValueError, match='participant resource'):
        module().HTTPSParticipantResourceAPI(retained=state.retained, key=keys[outside],
            api_server='https://foreign.invalid' if outside == 'endpoint' else state.retained.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_resource=lambda *_: None)


def test_original_cannot_already_contain_this_release_write_marker(operation_inputs):
    """An unchanged before-image must never look like an uncertain committed write."""
    ns = operation_inputs['config']['namespace']
    key = 'Namespace:-:' + ns
    original, = (row for row in operation_inputs['current'] if _key(row) == key)
    original['metadata'].setdefault('annotations', {})['loom.nebius/participant-candidate-operation'] = str(operation_inputs['operation_id'])
    retained = operation.retain_participant_rollout(**operation_inputs)
    with pytest.raises(ValueError, match='participant resource'):
        module().HTTPSParticipantResourceAPI(retained=retained, key=key,
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            qualify_resource=lambda *_: None)
