"""Fresh dev registration uses retained installation history, never legacy cutover."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
import ssl
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import rfc8785
from tests.integration.test_nebius_pool_installation import installation as pool_installation
from tests.ops.test_nebius_development_management_retained import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_retained import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_retained import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_retained import cloud as cloud
from tests.ops.test_nebius_development_management_retained import installation as installation
from tests.ops.test_nebius_development_management_retained import inventory as inventory
from tests.ops.test_nebius_development_management_retained import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_retained import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_retained import material as material
from tests.ops.test_nebius_development_management_retained import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_retained import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_retained import retained as retained
from tests.ops.test_nebius_development_management_retained import route as route
from tests.ops.test_nebius_development_management_retained import tls_material as tls_material
from tests.ops.test_nebius_management_stage import PhaseAPI


def module():
    name = 'scripts.ops.nebius_development_pool_registration'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development pool registration parent is missing')
    return importlib.import_module(name)


def configured(retained):
    from scripts.ops.nebius_development_management_retained import RetainedManagementReference

    from loom_service.pool_management.installation import PoolInstallation

    config, _ = pool_installation(('development',))
    config['installation_id'] = retained[1]['installation_id']
    foundation = retained[3].deployment.installation.foundation.platform_config
    config['cluster_id'] = foundation['cluster_id']
    config['node_group_id'] = foundation['execution_node_group_id']
    config['node_selector']['nebius.com/node-group-id'] = config['node_group_id']
    participant, = config['participants']
    participant['installation_id'] = config['installation_id']
    participant['environment_id'] = str(retained[3].deployment.installation.applications.shared.data_environment_id)
    participant['execution_namespace']['name'] = foundation['execution_namespace']
    participant['build_namespace']['name'] = foundation['execution_namespace'] + '-build'
    config['profiles']['execution'][0]['runtime']['namespace'] = participant['execution_namespace']['name']
    config['profiles']['execution'][0]['runtime']['node_selector'] = copy.deepcopy(config['node_selector'])
    build = config['profiles']['task_images'][0]
    build['target']['namespace'] = build['settings']['namespace'] = participant['build_namespace']['name']
    build['target']['node_selector'] = copy.deepcopy(config['node_selector'])
    return RetainedManagementReference.model_validate(retained[0]), PoolInstallation.model_validate(config)


class Server:
    """External Kubernetes transport only; parent, journals and verifier are real."""

    def __init__(self, request, resources):
        self.request = request
        self.store = PhaseAPI(request.registration.binding)
        self.store.resources = copy.deepcopy(resources)
        self.calls = []
        self.complete = False
        self.failure = None
        self.failure_kind = None
        self.pod = None
        self.damage_report = False
        self.drift_on_log = False

    def handle(self, message):
        self.calls.append(message)
        registration = self.request.registration
        binding = registration.binding
        path, method = message.url.path, message.method
        name = path.rsplit('/', 1)[-1]
        if path.startswith('/api/v1/namespaces/') and len(path.split('/')) == 5:
            assert method == 'GET'
            identities = {'kube-system': binding.kube_system_uid, binding.namespace: binding.namespace_uid,
                'loom-dev': str(self.request.retained.inputs.shared_namespace_uid)}
            for row in registration.spec.participants:
                for item in (row.execution_namespace, row.build_namespace):
                    identities[item.name] = str(item.uid)
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': name, 'uid': identities[name], 'labels': {
                    'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path.startswith('/api/v1/persistentvolumes/'):
            assert method == 'GET'
            value = self.store.resources.get('PersistentVolume:' + name)
            return httpx.Response(404) if value is None else httpx.Response(200, json=copy.deepcopy(value))
        assert '/namespaces/' + binding.namespace + '/' in path
        if method == 'POST':
            doc = json.loads(message.content)
            assert doc['kind'] in {'ConfigMap', 'Job'}
            assert doc['metadata']['name'].startswith('loom-pool-registration-')
            if message.url.params.get('dryRun') == 'All':
                return httpx.Response(201, json=self.store.default_resource(doc))
            self.store.failure = self.failure if self.failure_kind in {None, doc['kind']} else None
            try:
                self.store.create_resource(doc)
            except OSError:
                raise httpx.ReadTimeout('response lost') from None
            return httpx.Response(201, json=self.store.get_resource(doc))
        assert method == 'GET'
        if '/pods' in path:
            job, = [doc for doc in self.store.resources.values() if doc['kind'] == 'Job'
                    and doc['metadata']['name'].startswith('loom-pool-registration-')]
            if self.pod is None:
                self.pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
                    **copy.deepcopy(job['spec']['template']['metadata']), 'namespace': binding.namespace,
                    'name': job['metadata']['name'] + '-abc', 'uid': str(uuid4()),
                    'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
                        'uid': job['metadata']['uid'], 'controller': True}]},
                    'spec': copy.deepcopy(job['spec']['template']['spec']), 'status': {'phase': 'Succeeded',
                        'containerStatuses': [{'name': 'register', 'restartCount': 0,
                            'state': {'terminated': {'exitCode': 0}}}]}}
            if name == 'pods':
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {},
                    'items': [self.pod]})
            if name == 'log':
                if self.drift_on_log:
                    volume, = [doc for doc in self.store.resources.values() if doc['kind'] == 'PersistentVolume']
                    volume['spec']['csi']['volumeHandle'] = 'foreign-disk'
                spec = registration.spec
                report = {'schema_version': 'loom.pool-installation-receipt.v1', 'operation_id': str(spec.operation_id),
                    'pool_id': str(spec.pool_id), 'installation_sha256': hashlib.sha256(
                        rfc8785.dumps(spec.model_dump(mode='json')) + b'\n').hexdigest(),
                    'mode': 'global' if self.damage_report else 'closed', 'participants': 1, 'machines': 3}
                return httpx.Response(200, content=json.dumps(report).encode())
            assert name == self.pod['metadata']['name']
            return httpx.Response(200, json=self.pod)
        resource = path.split('/')[-2]
        kinds = {'secrets': 'Secret', 'services': 'Service', 'statefulsets': 'StatefulSet',
            'deployments': 'Deployment', 'configmaps': 'ConfigMap', 'jobs': 'Job',
            'persistentvolumeclaims': 'PersistentVolumeClaim'}
        value = copy.deepcopy(self.store.resources.get(kinds[resource] + ':' + name))
        if value is None:
            return httpx.Response(404)
        if value['kind'] == 'Job' and name.startswith('loom-pool-registration-') and self.complete:
            value['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
        return httpx.Response(200, json=value)


@pytest.fixture
def registered(retained):
    reference, spec = configured(retained)
    request = module().prepare_registration(reference=reference, spec=spec)
    live = importlib.import_module('scripts.ops.nebius_development_pool_registration_live')
    resources = {**retained[4].store.resources,
        **{'Secret:' + name: doc for name, doc in retained[4].bootstrap.secrets.secrets.items()}}
    server = Server(request, resources)
    api = live.HTTPSDevelopmentPoolRegistrationAPI(request=request,
        api_server=request.retained.inputs.operator_connection.endpoint, ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(server.handle))
    with api:
        yield SimpleNamespace(request=request, api=api, server=server)


def run(registered, *, execute=True):
    return module().register_development_pool(request=registered.request, api=registered.api, execute=execute)


def test_fresh_registration_requires_no_legacy_predecessor(retained):
    reference, spec = configured(retained)
    request = module().prepare_registration(reference=reference, spec=spec)
    assert request.registration.binding.namespace == 'loom-nebius-management-dev'
    assert request.registration.candidate == retained[3].candidate
    assert request.registration.spec.installation_id == spec.installation_id


def test_registration_preview_is_read_only_and_uses_independent_management(registered):
    result = run(registered, execute=False)
    assert result['status'] == 'development_pool_registration_preflight_qualified'
    assert not registered.server.store.creates
    assert all(call.method == 'GET' for call in registered.server.calls)
    assert not (Path(registered.request.retained.operation['anchor_dir']) / 'pool-registration.json').exists()


def test_registration_requires_committed_closed_receipt_and_replays_without_writes(registered):
    assert run(registered)['status'] == 'pending_registration'
    assert len(registered.server.store.creates) == 2
    job, = [doc for doc in registered.server.store.resources.values() if doc['kind'] == 'Job'
            and doc['metadata']['name'].startswith('loom-pool-registration-')]
    pod = job['spec']['template']['spec']
    assert job['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert pod['automountServiceAccountToken'] is False
    assert pod['containers'][0]['env'][1]['valueFrom']['secretKeyRef'] == {
        'name': 'loom-platform-db', 'key': 'admin-url'}
    registered.server.complete = True
    result = run(registered)
    assert result['status'] == 'development_pool_registered_closed'
    assert result['admission_open'] is False and result['writer_migration_complete'] is False
    before = len(registered.server.store.creates)
    assert run(registered) == result
    assert len(registered.server.store.creates) == before


@pytest.mark.parametrize('damage', ['Secret:loom-platform-db', 'Service:loom-postgres',
    'StatefulSet:loom-postgres', 'Deployment:loom-service'])
def test_database_or_manager_replacement_blocks_before_registration(registered, damage):
    registered.server.store.resources[damage]['metadata']['uid'] = str(uuid4())
    with pytest.raises(ValueError, match='development pool registration'):
        run(registered)
    assert not registered.server.store.creates


def test_rebound_database_credentials_cannot_redirect_registration(registered):
    registered.server.store.resources['Secret:loom-platform-db']['data']['admin-url'] = 'Zm9yZWlnbg=='
    with pytest.raises(ValueError, match='development pool registration'):
        run(registered)
    assert not registered.server.store.creates


@pytest.mark.parametrize('damage', ['Service:loom-postgres', 'StatefulSet:loom-postgres', 'Deployment:loom-service'])
def test_same_uid_database_or_manager_drift_blocks_registration(registered, damage):
    document = registered.server.store.resources[damage]
    if document['kind'] == 'Service':
        document['spec']['selector'] = {'app': 'foreign-database'}
    else:
        document['spec']['template']['spec']['containers'][0]['image'] = 'foreign.example.com/changed@sha256:' + 'a' * 64
    with pytest.raises(ValueError, match='development pool registration'):
        run(registered)
    assert not registered.server.store.creates


@pytest.mark.parametrize('damage', ['config', 'shared-secret', 'claim', 'volume', 'disk'])
def test_rebound_runtime_configuration_or_database_storage_blocks_registration(registered, damage):
    resources = registered.server.store.resources
    if damage == 'config':
        document, = [doc for doc in resources.values() if doc['kind'] == 'ConfigMap'
            and doc['metadata']['name'].startswith('loom-management-applications-')]
        value = json.loads(document['data']['installation.json'])
        value['applications']['shared']['data_environment_id'] = str(uuid4())
        document['data']['installation.json'] = json.dumps(value)
    elif damage == 'shared-secret':
        document, = [doc for doc in resources.values() if doc['kind'] == 'Secret'
            and doc['metadata']['name'].startswith('loom-applications-shared-')]
    elif damage == 'claim':
        document = resources['PersistentVolumeClaim:data-loom-postgres-0']
    else:
        document, = [doc for doc in resources.values() if doc['kind'] == 'PersistentVolume']
    if damage == 'disk':
        document['spec']['csi']['volumeHandle'] = 'foreign-disk'
    else:
        document['metadata']['uid'] = str(uuid4())
    with pytest.raises(ValueError, match='development pool registration'):
        run(registered)
    assert not registered.server.store.creates


def test_tls_successor_and_retired_initial_credentials_preserve_registration(registered, retained, monkeypatch):
    from scripts.ops.nebius_development_management_renewal import (
        RenewalRequest,
        renew_management_tls,
    )
    from tests.ops.test_nebius_development_management_renewal import RenewalAPI, new_material

    previous = registered.request
    renewal = RenewalRequest(retained=previous.retained, material=new_material(monkeypatch),
        operation_id=uuid4(), qualification_digest='sha256:' + 'b' * 64)
    renewal_api = RenewalAPI(retained[4].store)
    assert renew_management_tls(request=renewal, api=renewal_api, execute=True)[
        'status'] == 'development_management_tls_renewed'
    registered.server.store.resources.update(copy.deepcopy(renewal_api.store.resources))
    registered.server.store.resources['Ingress:loom-management'] = copy.deepcopy(renewal_api.ingress)
    inputs = retained[2]
    obsolete = {inputs['operator_connection']['credentials_file'], inputs['operator_connection']['ca_file'],
        inputs['operator_cloud_credentials'], *inputs['application_files'].values()}
    for path in map(Path, obsolete):
        path.unlink()
    registered.request = module().prepare_registration(reference=previous.reference, spec=previous.registration.spec)
    assert registered.request.registration == previous.registration
    registered.server.complete = True
    assert run(registered)['status'] == 'development_pool_registered_closed'
    assert len(registered.server.store.creates) == 2


@pytest.mark.parametrize('damage', ['installation', 'cluster', 'staging', 'namespace', 'shared-environment', 'node-group'])
def test_foreign_registration_scope_is_rejected(retained, damage):
    reference, spec = configured(retained)
    raw = spec.model_dump(mode='json')
    if damage == 'installation':
        raw['installation_id'] = str(uuid4())
        raw['participants'][0]['installation_id'] = raw['installation_id']
    elif damage == 'cluster':
        raw['cluster_id'] = 'foreign-cluster'
    elif damage == 'staging':
        raw['participants'][0]['environment_class'] = 'staging'
    elif damage == 'namespace':
        raw['participants'][0]['execution_namespace']['name'] = 'foreign-execution'
        raw['profiles']['execution'][0]['runtime']['namespace'] = 'foreign-execution'
    elif damage == 'shared-environment':
        raw['participants'][0]['environment_id'] = str(uuid4())
    else:
        raw['node_group_id'] = 'foreign-node-group'
        raw['node_selector']['nebius.com/node-group-id'] = raw['node_group_id']
        raw['profiles']['execution'][0]['runtime']['node_selector'] = copy.deepcopy(raw['node_selector'])
        raw['profiles']['task_images'][0]['target']['node_selector'] = copy.deepcopy(raw['node_selector'])
    with pytest.raises(ValueError, match='development pool registration'):
        module().prepare_registration(reference=reference, spec=type(spec).model_validate(raw))


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_create_never_starts_a_second_registration(registered, failure):
    registered.server.failure = failure
    if failure == 'before':
        for _ in range(2):
            with pytest.raises(ValueError):
                run(registered)
        assert len(registered.server.store.creates) == 1
    else:
        assert run(registered)['status'] == 'pending_registration'
        registered.server.failure = None
        registered.server.complete = True
        assert run(registered)['status'] == 'development_pool_registered_closed'
        assert len(registered.server.store.creates) == 2


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_job_create_is_readback_only_after_process_restart(registered, failure):
    from scripts.ops.nebius_development_pool_registration_live import (
        HTTPSDevelopmentPoolRegistrationAPI,
    )

    registered.server.failure_kind = 'Job'
    registered.server.failure = failure
    if failure == 'before':
        with pytest.raises(ValueError):
            run(registered)
    else:
        assert run(registered)['status'] == 'pending_registration'
    assert len(registered.server.store.creates) == 2
    previous = registered.request
    fresh = module().prepare_registration(reference=previous.reference, spec=previous.registration.spec)
    api = HTTPSDevelopmentPoolRegistrationAPI(request=fresh, api_server=registered.api.api_server,
        ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(registered.server.handle))
    registered.server.failure = None
    registered.server.complete = True
    with api:
        if failure == 'before':
            with pytest.raises(ValueError):
                module().register_development_pool(request=fresh, api=api, execute=True)
        else:
            assert module().register_development_pool(request=fresh, api=api, execute=True)[
                'status'] == 'development_pool_registered_closed'
    assert len(registered.server.store.creates) == 2


def test_database_drift_during_receipt_cannot_report_completion(registered):
    assert run(registered)['status'] == 'pending_registration'
    registered.server.complete = True
    registered.server.drift_on_log = True
    with pytest.raises(ValueError):
        run(registered)
    state = Path(registered.request.retained.operation['state_dir']).parent / 'pool-registration/registration.json'
    assert json.loads(state.read_text())['phase'] == 'started'
    assert len(registered.server.store.creates) == 2


@pytest.mark.parametrize('damage', ['anchor', 'parent', 'stage', 'original', 'receipt'])
def test_missing_or_changed_evidence_never_reopens_registration(registered, damage):
    run(registered)
    registered.server.complete = True
    run(registered)
    original = Path(registered.request.retained.operation['state_dir'])
    state = original.parent / 'pool-registration'
    if damage == 'anchor':
        (Path(registered.request.retained.operation['anchor_dir']) / 'pool-registration.json').unlink()
    elif damage == 'parent':
        (state / 'registration.json').unlink()
    elif damage == 'stage':
        (state / 'resources' / 'stage.json').unlink()
    elif damage == 'original':
        (original / 'installation.json').write_text('{}')
    else:
        registered.server.damage_report = True
    before = len(registered.server.store.creates)
    with pytest.raises(ValueError):
        run(registered)
    assert len(registered.server.store.creates) == before


def test_a_second_operation_cannot_create_another_pool_in_same_manager(registered):
    run(registered)
    request = registered.request
    changed = module().prepare_registration(reference=request.reference,
        spec=request.registration.spec.model_copy(update={'operation_id': uuid4()}))
    before = len(registered.server.store.creates)
    with pytest.raises(ValueError):
        module().register_development_pool(request=changed, api=registered.api, execute=True)
    assert len(registered.server.store.creates) == before
