"""Exercise the whole refresh parent with its real connected adapters.

Only external HTTPS, cloud prerequisite checks and object storage are doubled.
This is connected composition evidence, not actual Kubernetes runtime acceptance.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import ssl
import tomllib
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
from tests.ops.test_nebius_management_refresh_predecessor import checksum, load
from tests.ops.test_nebius_management_refresh_predecessor import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade_entry import private_upgrade as private_upgrade
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def connected_refresh(completed_upgrade, monkeypatch):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_material import _documents
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe
    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

    root = load(completed_upgrade[0])
    operation, _, _ = private_refresh(root)
    request = context(operation).request
    directory, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    binding = request.resources.binding
    app = request.resources.switch.render.after.installation.applications
    values = {_key(doc): copy.deepcopy(doc) for doc in completed_upgrade[2].store.resources.values()}
    for key, document in root.retained.items():
        values.setdefault(key, copy.deepcopy(document))
    bundle = json.loads((root.upgrade.original_state / 'bootstrap/material/material.json').read_text())
    for name, doc in _documents(bundle['material'], binding, bundle['operation_id']).items():
        doc['metadata']['uid'] = bundle['resources'][name]['uid']
        values[_key(doc)] = doc
    manager = copy.deepcopy(root.active)
    manager['metadata'].update(resourceVersion='30', generation=5)
    values[_key(manager)] = manager
    namespaces = {binding.namespace: binding.namespace_uid, 'kube-system': binding.kube_system_uid,
        app.shared.platform_namespace: request.resources.shared_namespace_uid}
    payload = b'PGDMP-connected-refresh-fixture'
    checksum = hashlib.sha256(payload).hexdigest()
    report = {'backup_key': binding.namespace + '/2026/09/29/230000-' + checksum[:12] + '.dump',
        'sha256': checksum, 'bytes': len(payload)}
    state = SimpleNamespace(root=root, request=request, directory=directory, anchor=anchor, values=values,
        manager=manager, namespaces=namespaces, calls=[], storage_calls=[], storage_options=[], public_calls=[],
        pods={}, logs=Counter(), fail_backup=False, fail_public=False, activation_probe_drift=False,
        operation=operation, fail_probe=None)
    kinds = {'configmaps': 'ConfigMap', 'secrets': 'Secret', 'serviceaccounts': 'ServiceAccount',
        'networkpolicies': 'NetworkPolicy', 'roles': 'Role', 'rolebindings': 'RoleBinding',
        'clusterroles': 'ClusterRole', 'clusterrolebindings': 'ClusterRoleBinding',
        'validatingadmissionpolicies': 'ValidatingAdmissionPolicy', 'validatingadmissionpolicybindings': 'ValidatingAdmissionPolicyBinding',
        'jobs': 'Job', 'cronjobs': 'CronJob', 'deployments': 'Deployment', 'services': 'Service', 'ingresses': 'Ingress',
        'statefulsets': 'StatefulSet', 'persistentvolumeclaims': 'PersistentVolumeClaim', 'persistentvolumes': 'PersistentVolume'}
    external = PhaseAPI(binding)
    rs_uid, manager_pod_uid = str(uuid4()), str(uuid4())

    def pod_for(job):
        uid = job['metadata']['uid']
        if uid not in state.pods:
            pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {**copy.deepcopy(job['spec']['template']['metadata']),
                'name': job['metadata']['name'] + '-abc', 'namespace': job['metadata']['namespace'], 'uid': str(uuid4()),
                'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
                    'uid': uid, 'controller': True}]}, 'spec': copy.deepcopy(job['spec']['template']['spec']),
                'status': {'phase': 'Succeeded', **{status: [{'name': row['name'], 'restartCount': 0,
                    'state': {'terminated': {'exitCode': 0}}} for row in job['spec']['template']['spec'].get(field, [])]
                    for field, status in [('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')]}}}
            state.pods[uid] = pod
        return state.pods[uid]

    def manager_workloads():
        count = manager['spec']['replicas']
        template = copy.deepcopy(manager['spec']['template'])
        template['metadata']['labels']['pod-template-hash'] = '1234567890'
        replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': 'loom-service-1234567890',
            'namespace': binding.namespace, 'uid': rs_uid, 'generation': manager['metadata']['generation'],
            'labels': {'app': 'loom-service', 'pod-template-hash': '1234567890'}, 'ownerReferences': [
                {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': 'loom-service',
                    'uid': manager['metadata']['uid'], 'controller': True}]},
            'spec': {'replicas': count, 'template': template,
                'selector': {'matchLabels': {'app': 'loom-service', 'pod-template-hash': '1234567890'}}},
            'status': {'observedGeneration': manager['metadata']['generation'], 'replicas': count,
                'readyReplicas': count, 'availableReplicas': count}}
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {**copy.deepcopy(template['metadata']),
            'name': 'loom-service-1234567890-abc', 'namespace': binding.namespace, 'uid': manager_pod_uid,
            'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
                'uid': rs_uid, 'controller': True}]}, 'spec': template['spec'], 'status': {'phase': 'Running',
            'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [
                {'name': 'loom-service', 'ready': True, 'state': {'running': {}}}],
            'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
                for row in template['spec'].get('initContainers', [])]}}
        return replica, [pod] if count else []

    def key_for(path):
        pieces = path.strip('/').split('/')
        ns = pieces[pieces.index('namespaces') + 1] if 'namespaces' in pieces else '-'
        return kinds[pieces[-2]] + ':' + ns + ':' + pieces[-1]

    def handler(message):
        state.calls.append(message)
        path = message.url.path
        if message.method == 'PATCH':
            assert path.endswith('/deployments/loom-service')
            patches = json.loads(message.content)
            updated = copy.deepcopy(manager)
            assert patches[:3] == [{'op': 'test', 'path': '/metadata/uid', 'value': manager['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': manager['metadata']['resourceVersion']},
                {'op': 'test', 'path': '/spec', 'value': manager['spec']}]
            for row in patches[3:]:
                node = updated
                keys = row['path'].strip('/').split('/')
                for key in keys[:-1]:
                    node = node[key]
                node[keys[-1]] = row['value']
            updated['metadata']['generation'] += 1
            updated['metadata']['resourceVersion'] = str(int(manager['metadata']['resourceVersion']) + 1)
            count = updated['spec']['replicas']
            updated['status'] = {'observedGeneration': updated['metadata']['generation'], 'replicas': count,
                'readyReplicas': count, 'updatedReplicas': count, 'availableReplicas': count}
            if not message.url.params:
                manager.clear()
                manager.update(updated)
            return httpx.Response(200, json=updated)
        if message.method == 'POST':
            document = json.loads(message.content)
            assert document['kind'] in {'ConfigMap', 'Job'}
            observed = external.default_resource(document)
            observed['metadata'].setdefault('uid', str(uuid4()))
            observed['metadata'].update(resourceVersion='77', generation=1)
            if not message.url.params:
                if document['kind'] == 'Job':
                    observed['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
                    if state.fail_probe and ('loom-refresh-' + state.fail_probe + '-') in document['metadata']['name']:
                        observed['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}], 'failed': 1}
                values[_key(observed)] = observed
            return httpx.Response(201, json=observed)
        assert message.method == 'GET'
        if path in {'/api/v1/namespaces/' + ns for ns in namespaces}:
            ns = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': ns, 'uid': namespaces[ns], 'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path.endswith(('/pods', '/replicasets')):
            label = message.url.params['labelSelector']
            if label == 'app=loom-service':
                replica, pods = manager_workloads()
                kind, items = ('ReplicaSet', [replica]) if path.endswith('/replicasets') else ('Pod', pods)
            else:
                uid = label.split('=', 1)[1]
                job = next(doc for doc in values.values() if doc['kind'] == 'Job' and doc['metadata']['uid'] == uid)
                kind, items = 'Pod', [pod_for(job)]
            return httpx.Response(200, json={'apiVersion': 'apps/v1' if kind == 'ReplicaSet' else 'v1',
                'kind': kind + 'List', 'metadata': {'resourceVersion': '77'}, 'items': items})
        if '/pods/' in path:
            name = path.split('/pods/', 1)[1].split('/')[0]
            pod = next(row for row in state.pods.values() if row['metadata']['name'] == name)
            if path.endswith('/log'):
                name = pod['metadata']['ownerReferences'][0]['name']
                state.logs[name] += 1
                if name.startswith('loom-refresh-backup-'):
                    return httpx.Response(200, content=json.dumps(report) + '\nNebius platform backup complete\n')
                config = values['ConfigMap:' + pod['metadata']['namespace'] + ':' + name]
                settings = json.loads(config['data']['probe.json'])
                result = {'schema': 'loom.nebius-management-refresh-probe.v1', 'status': 'qualified',
                    'mode': settings['mode'], 'revision': settings['expected_revision'], 'operations_checked':
                    int(state.activation_probe_drift and name.startswith('loom-refresh-manager-probe-') and state.logs[name] > 1)}
                return httpx.Response(200, json=result)
            return httpx.Response(200, json=pod)
        value = values.get(key_for(path))
        return httpx.Response(200, json=value) if value is not None else httpx.Response(404)

    real_transport = ManagementKubernetesTransport.__init__
    def transport(self, **kwargs):
        real_transport(self, **kwargs)
        self.client.close()
        self.client = httpx.Client(base_url=self.api_server, transport=httpx.MockTransport(handler))
    monkeypatch.setattr(ManagementKubernetesTransport, '__init__', transport)

    class Storage:
        def head_object(self, **kwargs):
            state.storage_calls.append(('HEAD', kwargs))
            assert kwargs == {'Bucket': root.deployment.backup_bucket, 'Key': report['backup_key']}
            return {'ContentLength': len(payload), 'Metadata': {'sha256': checksum}, 'ETag': 'fixed-etag'}

        def get_object(self, **kwargs):
            state.storage_calls.append(('GET', kwargs))
            assert kwargs == {'Bucket': root.deployment.backup_bucket, 'Key': report['backup_key'], 'IfMatch': 'fixed-etag'}
            return {'ContentLength': len(payload), 'ETag': 'fixed-etag', 'Body': io.BytesIO(b'corrupted' if state.fail_backup else payload)}

        def close(self):
            pass

        meta = SimpleNamespace(events=SimpleNamespace(register_first=lambda *_args: None))

    def storage_client(service, **kwargs):
        assert service == 's3'
        state.storage_options.append(kwargs)
        return Storage()
    monkeypatch.setattr('boto3.client', storage_client)

    def public_http(message):
        state.public_calls.append(message)
        assert message.method == 'GET'
        if state.fail_public:
            return httpx.Response(503)
        if message.url.path.endswith('/health/ready'):
            return httpx.Response(200, json={'status': 'ready', 'mode': 'management', 'postgres': 'ready', 'application_provisioner': 'ready'})
        if message.url.path == '/api/v1/tasks':
            return httpx.Response(404)
        expected = tomllib.loads(bundle['material']['loom-admin-secret']['secrets.toml'])['admin']['token']
        return httpx.Response(200, json={'items': [], 'next_cursor': None}) if message.headers.get('authorization') == 'Bearer ' + expected else httpx.Response(401)
    real_public = ManagementPublicProbe.__init__
    def public(self, **kwargs):
        real_public(self, **kwargs)
        self.client.close()
        self.client = httpx.Client(transport=httpx.MockTransport(public_http))
    monkeypatch.setattr(ManagementPublicProbe, '__init__', public)

    class Checks:
        def preflight(self, current):
            assert current.setup.deployment == state.request.resources.switch.render.after
            assert current.setup.candidate == state.request.resources.switch.render.candidate

        def public_route(self, current):
            assert current.setup.deployment.public_host == root.deployment.public_host

    with HTTPSManagementRefreshInstaller(request=request, original=root, predecessor=root, state_dir=directory,
        api_server=app.runtime.kubernetes.endpoint, ssl_context=ssl.create_default_context(), token='operator-only-token',
        runtime_ca_pem=None, checks=Checks()) as api:
        yield api, state


def run(connected):
    from scripts.ops.nebius_management_refresh_install import refresh_management

    api, state = connected
    return refresh_management(request=state.request, api=api, state_dir=state.directory, anchor_dir=state.anchor)


def writes(state):
    return [(message.method, message.url.path) for message in state.calls
        if message.method != 'GET' and not message.url.params]


@contextmanager
def successor_api(connected):
    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from tests.ops.test_nebius_management_refresh_supersession import successor

    old_api, state = connected
    state.fail_probe = 'shared-probe'
    with pytest.raises(ManagementRefreshInstallError):
        run(connected)
    selector = {'operation': state.operation, 'refresh_sha256': checksum(state.directory / 'refresh.json'),
        'switch_sha256': checksum(state.directory / 'switch/cutover.json')}
    operation, _ = successor(state.root, selector)
    bound = context(operation)
    state.request, state.operation = bound.request, operation
    state.directory, state.anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    state.fail_probe = None
    with HTTPSManagementRefreshInstaller(request=bound.request, original=bound.original, predecessor=bound.predecessor,
        superseded=bound.superseded, state_dir=state.directory, api_server=old_api.api_server,
        ssl_context=old_api.ssl_context, token=old_api.token, runtime_ca_pem=None, checks=old_api.checks) as api:
        yield api, state, bound


def test_connected_successor_requalifies_failed_job_and_completes_new_barriers(connected_refresh):
    with successor_api(connected_refresh) as (api, state, bound):
        frozen = {path: path.read_bytes() for path in bound.superseded.history}
        previous_calls = len(state.calls)
        api.preflight(state.request)
        assert all(message.method == 'GET' for message in state.calls[previous_calls:])
        assert run((api, state))['status'] == 'management_refreshed'
        assert state.manager['spec']['replicas'] == 1
        assert state.manager['metadata']['uid'] == bound.superseded.stopped['metadata']['uid']
        patches = [json.loads(row.content) for row in state.calls[previous_calls:]
            if row.method == 'PATCH' and not row.url.params]
        assert [row[4]['value'] for row in patches] == [0, 1]
        assert len(patches[0]) == 5
        assert {path: path.read_bytes() for path in frozen} == frozen
        recorded = writes(state)
        assert run((api, state))['status'] == 'management_refreshed'
        assert writes(state) == recorded


@pytest.mark.parametrize('damage', ['job_uid', 'pending', 'active', 'succeeded', 'terminating', 'status_type',
    'job_spec', 'config', 'old_history', 'stopped_marker', 'prerequisites'])
def test_connected_successor_rejects_unqualified_old_state_without_new_writes(connected_refresh, damage, monkeypatch):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    with successor_api(connected_refresh) as (api, state, bound):
        job = next(doc for doc in state.values.values()
            if doc['kind'] == 'Job' and doc['metadata']['uid'] == bound.superseded.failed_job_uid)
        if damage == 'job_uid':
            job['metadata']['uid'] = str(uuid4())
        elif damage == 'pending':
            job['status'] = {}
        elif damage in {'active', 'succeeded'}:
            job['status'][damage] = 1
        elif damage == 'terminating':
            job['metadata']['deletionTimestamp'] = '2026-09-30T04:00:00Z'
        elif damage == 'status_type':
            job['status']['active'] = False
        elif damage == 'job_spec':
            job['spec']['backoffLimit'] = 1
        elif damage == 'config':
            state.values['ConfigMap:' + job['metadata']['namespace'] + ':' + job['metadata']['name']]['data'] = {}
        elif damage == 'old_history':
            Path(bound.superseded.selector.operation['state_dir'], 'refresh.json').write_text('{}')
        elif damage == 'stopped_marker':
            state.manager['metadata']['annotations']['loom.nebius/management-refresh-id'] = str(uuid4())
        else:
            def fail(_request):
                raise ValueError('private-provider-payload')
            monkeypatch.setattr(api.checks, 'preflight', fail)
        prior = len(state.calls)
        with pytest.raises(ManagementRefreshInstallError):
            run((api, state))
        assert all(message.method == 'GET' for message in state.calls[prior:])
        assert not state.directory.exists()


def test_real_connected_parent_completes_and_replay_preserves_all_effects(connected_refresh):
    _, state = connected_refresh
    before = {path: path.read_bytes() for path in state.root.history}
    assert run(connected_refresh)['status'] == 'management_refreshed'
    recorded = writes(state)
    assert len([row for row in recorded if row[0] == 'PATCH']) == 2
    assert state.public_calls and state.storage_calls
    for options in state.storage_options:
        assert options['aws_access_key_id'] == state.root.upgrade.original.material['loom-platform-storage']['backup-access-key']
        assert options['config'].retries['total_max_attempts'] == 1
    receipt = (state.directory / 'completion.json').read_bytes()
    assert run(connected_refresh)['status'] == 'management_refreshed'
    assert writes(state) == recorded
    assert (state.directory / 'completion.json').read_bytes() == receipt
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize('drift', ['shared_namespace', 'retained_material', 'retained_volume', 'predecessor', 'manager_template'])
def test_preflight_rejects_drift_before_any_refresh_write(connected_refresh, drift):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, state = connected_refresh
    if drift == 'shared_namespace':
        state.namespaces[state.request.resources.switch.render.after.installation.applications.shared.platform_namespace] = str(uuid4())
    elif drift == 'predecessor':
        path = next(path for path in state.root.history if path.name == 'upgrade.json')
        path.write_text('{}')
    elif drift == 'manager_template':
        state.manager['spec']['template']['spec']['containers'][0]['image'] = 'foreign:latest'
    else:
        kind = 'Secret' if drift == 'retained_material' else 'PersistentVolume'
        row = next(value for value in state.values.values() if value['kind'] == kind)
        row['metadata']['uid'] = str(uuid4())
    with pytest.raises(ManagementRefreshInstallError):
        run(connected_refresh)
    assert not writes(state) and not state.storage_calls and not state.public_calls


@pytest.mark.parametrize('kind,stage', [('Secret', 'resource_inventory'), ('PersistentVolume', 'persistent_storage')])
def test_preflight_reports_the_failed_retained_check_without_writes(connected_refresh, kind, stage):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    api, state = connected_refresh
    document = next(row for row in state.values.values() if row['kind'] == kind)
    document['metadata']['uid'] = str(uuid4())
    with pytest.raises(ManagementRefreshInstallError) as error:
        api.preflight(state.request)
    assert error.value.stage == stage
    assert not writes(state) and not state.storage_calls and not state.public_calls


@pytest.mark.parametrize('detail,expected', [
    ('foundation', 'foundation'), ('shared_material', 'shared_material'),
    ('platform_capacity', 'platform_capacity'), ('publication', 'publication'),
    ('cloud_identity', 'cloud_identity'), ('public_route', 'public_route'),
    ('private-provider-payload', 'prerequisites'), (None, 'prerequisites'),
    (['private-provider-payload'], 'prerequisites'),
])
def test_preflight_keeps_only_closed_prerequisite_stages(connected_refresh, monkeypatch, detail, expected):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    api, state = connected_refresh

    def unavailable(_request):
        api.checks.diagnostic_stage = detail
        raise ManagementPrerequisiteError('private-provider-payload')

    monkeypatch.setattr(api.checks, 'preflight', unavailable)
    with pytest.raises(ManagementRefreshInstallError) as error:
        api.preflight(state.request)
    assert error.value.stage == expected
    assert 'private-provider-payload' not in str(error.value)
    assert not writes(state) and not state.storage_calls and not state.public_calls


def test_actual_capacity_failure_survives_retained_and_refresh_wrappers(connected_refresh, monkeypatch):
    from scripts.ops import nebius_management_refresh_entry as entry
    from scripts.ops.nebius_application_upgrade_prerequisites import ApplicationUpgradePrerequisites
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    api, state = connected_refresh
    checks = ApplicationUpgradePrerequisites(base=SimpleNamespace(inventory=lambda *args: []),
        settings=state.root.inputs.prerequisites)
    def preflight(request):
        checks.diagnostic_stage = 'platform_capacity'
        checks.platform_capacity(request)
    monkeypatch.setattr(checks, 'preflight', preflight)
    api.checks = checks
    @contextmanager
    def connected(_context, _operation):
        yield api
    monkeypatch.setattr(entry, 'connected_refresh_api', connected)
    with pytest.raises(ManagementRefreshInstallError) as error:
        entry.execute_refresh(SimpleNamespace(request=state.request), {'operation_id': str(uuid4())}, 'preflight')
    assert error.value.stage == 'refresh_platform_capacity'
    assert error.value.capacity == {'schema': 'loom.nebius-platform-capacity-diagnostic.v1',
        'stage': 'node_eligibility', 'kind': None, 'error_type': 'ManagementCapacityError', 'nodes': []}
    assert not writes(state) and not state.storage_calls and not state.public_calls


@pytest.mark.parametrize('failure', ['backup', 'activation_probe', 'public'])
def test_connected_barrier_failure_never_skips_proof_or_replays_effects(connected_refresh, failure):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, state = connected_refresh
    setattr(state, {'backup': 'fail_backup', 'activation_probe': 'activation_probe_drift', 'public': 'fail_public'}[failure], True)
    with pytest.raises(ManagementRefreshInstallError):
        run(connected_refresh)
    recorded = writes(state)
    assert len([row for row in recorded if row[0] == 'PATCH']) == (2 if failure == 'public' else 1)
    assert state.manager['spec']['replicas'] == (1 if failure == 'public' else 0)
    assert not (state.directory / 'completion.json').exists()
    if failure == 'backup':
        assert not any('/jobs' in path and 'migration' in message.content.decode() for message in state.calls
            for path in [message.url.path] if message.method == 'POST')
    if failure == 'public':
        state.fail_public = False
        assert run(connected_refresh)['status'] == 'management_refreshed'
        assert writes(state) == recorded


def test_foreign_resource_request_or_state_path_fails_before_connecting(connected_refresh):
    api, state = connected_refresh
    count = len(state.calls)
    with pytest.raises(RuntimeError):
        api.resources(replace(state.request.resources, shared_namespace_uid=str(uuid4())), 'config')
    with pytest.raises(RuntimeError):
        api.verify_probe(state.request, 'manager-probe', state.directory / 'foreign')
    with pytest.raises(RuntimeError):
        api.verify_backup(state.request, state.directory / 'foreign')
    assert len(state.calls) == count
