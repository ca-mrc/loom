"""Connected dev pool preparation cannot become staging or runtime authority."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
import ssl
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from tests.integration.test_nebius_pool_installation import add_application_builder
from tests.ops.test_nebius_development_pool_registration import Server as RegistrationServer
from tests.ops.test_nebius_development_pool_registration import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_registration import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_registration import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_pool_registration import cloud as cloud
from tests.ops.test_nebius_development_pool_registration import configured
from tests.ops.test_nebius_development_pool_registration import installation as installation
from tests.ops.test_nebius_development_pool_registration import inventory as inventory
from tests.ops.test_nebius_development_pool_registration import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_registration import manager_entry as manager_entry
from tests.ops.test_nebius_development_pool_registration import material as material
from tests.ops.test_nebius_development_pool_registration import (
    platform_inputs as original_platform_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_registration import provider_checks as provider_checks
from tests.ops.test_nebius_development_pool_registration import retained as retained
from tests.ops.test_nebius_development_pool_registration import route as route
from tests.ops.test_nebius_development_pool_registration import tls_material as tls_material
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@pytest.fixture
def platform_inputs(request):
    config, candidate, profile = request.getfixturevalue('original_platform_inputs')
    profile['runtime_binary_sha256'] = 'sha256:' + 'c' * 64
    return config, candidate, profile


def module():
    name = 'scripts.ops.nebius_development_pool_intent'
    if importlib.util.find_spec(name) is None:
        pytest.fail('connected namespace-free development pool intent is missing')
    return importlib.import_module(name)


@pytest.fixture
def pool_inputs(retained, build_inputs):
    reference, spec = configured(retained)
    value = spec.model_dump(mode='json')
    config = retained[3].deployment.installation.foundation.platform_config
    candidate, profile = retained[3].candidate, retained[3].profile
    for execution in value['profiles']['execution']:
        execution.update(candidate_sha=candidate['candidate_sha'],
            runtime_image_ref=candidate['images']['execution_runtime']['image_ref'],
            runtime_binary_sha256=profile['runtime_binary_sha256'])
    for build in value['profiles']['task_images']:
        build['settings'].update(service_image=candidate['images']['service']['image_ref'],
            storage_endpoint=config['storage_endpoint'], storage_region=config['region'],
            source_bucket=config['buckets']['source'], registry_repository=build_inputs[0].registry_repository)
    recipe = build_inputs[0].recipe.model_copy(update={
        'schema_revision': retained[3].deployment.installation.applications.shared.schema_revision})
    value, _, _ = add_application_builder(value, recipe)
    value['node_selector'] = copy.deepcopy(value['profiles']['application_images'][0]['target']['node_selector'])
    for participant in value['participants']:
        for field in ('execution_namespace', 'build_namespace'):
            participant[field].pop('uid')
    tokens = {}
    for machine in value['machines']:
        token = 'private-pool-test-' + machine['machine_id']
        tokens[UUID(machine['machine_id'])] = token
        machine['token_sha256'] = hashlib.sha256(token.encode()).hexdigest()
    return reference, value, tokens


def prepare(pool_inputs):
    reference, catalog, tokens = pool_inputs
    return module().prepare_intent(reference=reference, catalog=catalog, tokens=tokens)


def test_complete_intent_binds_only_observed_namespace_uids(pool_inputs):
    intent = prepare(pool_inputs)
    row, = pool_inputs[1]['participants']
    names = [row[field]['name'] for field in ('execution_namespace', 'build_namespace')]
    uids = dict(zip(names, (str(uuid4()), str(uuid4())), strict=True))
    request = module().bind_namespaces(intent, uids)
    participant, = request.registration.spec.participants
    assert str(participant.execution_namespace.uid) == uids[names[0]]
    assert str(participant.build_namespace.uid) == uids[names[1]]
    assert request.registration.binding.namespace == 'loom-nebius-management-dev'
    assert len(request.registration.spec.machines) == 4
    assert all('uid' not in row[field] for field in ('execution_namespace', 'build_namespace'))
    assert not any(token in repr(intent) for token in pool_inputs[2].values())


@pytest.mark.parametrize('damage', ['missing-builder', 'publication', 'runtime-binary', 'service-image',
    'schema', 'source', 'physical-group', 'supplied-uid', 'token', 'extra-token', 'missing-task-profile'])
def test_incomplete_or_foreign_catalog_is_rejected_before_namespace_creation(pool_inputs, damage):
    reference, value, tokens = pool_inputs
    participant, = value['participants']
    if damage == 'missing-builder':
        value['profiles'].pop('application_images')
        participant['targets'] = [row for row in participant['targets'] if row['workload_kinds'] != ['application_image_build']]
        removed, = [row for row in value['machines'] if row.get('workload_scope') == 'application_builder']
        value['machines'].remove(removed)
        del tokens[UUID(removed['machine_id'])]
    elif damage == 'publication':
        value['profiles']['execution'][0]['candidate_sha'] = 'd' * 40
    elif damage == 'runtime-binary':
        value['profiles']['execution'][0]['runtime_binary_sha256'] = 'sha256:' + 'd' * 64
    elif damage == 'service-image':
        value['profiles']['task_images'][0]['settings']['service_image'] = 'registry.example/foreign@sha256:' + 'd' * 64
    elif damage == 'schema':
        value['profiles']['application_images'][0]['recipe']['schema_revision'] = '9999'
    elif damage == 'source':
        value['profiles']['task_images'][0]['settings']['source_bucket'] = 'foreign-source'
    elif damage == 'physical-group':
        value['node_group_id'] = 'foreign-group'
    elif damage == 'supplied-uid':
        participant['execution_namespace']['uid'] = str(uuid4())
    elif damage == 'token':
        tokens[next(iter(tokens))] = 'changed-private-token'
    elif damage == 'extra-token':
        tokens[uuid4()] = 'extra-private-token'
    else:
        value['profiles']['task_images'] = []
    with pytest.raises(ValueError, match='development pool intent') as caught:
        module().prepare_intent(reference=reference, catalog=value, tokens=tokens)
    assert not any(token in str(caught.value) for token in tokens.values())


@pytest.mark.parametrize('damage', ['missing', 'extra', 'duplicate', 'nil'])
def test_namespace_receipt_requires_exact_distinct_non_nil_identities(pool_inputs, damage):
    intent = prepare(pool_inputs)
    participant, = pool_inputs[1]['participants']
    names = [participant[field]['name'] for field in ('execution_namespace', 'build_namespace')]
    uids = {name: str(uuid4()) for name in names}
    if damage == 'missing':
        del uids[names[0]]
    elif damage == 'extra':
        uids['loom-nebius-platform'] = str(uuid4())
    elif damage == 'duplicate':
        uids[names[1]] = uids[names[0]]
    else:
        uids[names[0]] = str(UUID(int=0))
    with pytest.raises(ValueError, match='development pool namespace'):
        module().bind_namespaces(intent, uids)


def test_fixed_delivery_keeps_credentials_scoped_and_runtime_disabled(pool_inputs):
    intent = prepare(pool_inputs)
    namespaces = module().namespace_documents(intent)
    assert len(namespaces) == 2
    assert {row['kind'] for row in namespaces.values()} == {'Namespace'}
    assert all(row['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'
        for row in namespaces.values())
    uids = {row['metadata']['name']: str(uuid4()) for row in namespaces.values()}
    request = module().bind_namespaces(intent, uids)
    phases = module().delivery_documents(intent, request)
    assert set(phases) == {'material', 'configuration', 'workload'}
    assert {row['kind'] for row in phases['configuration'].values()} == {'ConfigMap', 'ServiceAccount'}
    gateway, = phases['workload'].values()
    assert gateway['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert gateway['spec']['replicas'] == 0
    assert gateway['spec']['template']['spec']['automountServiceAccountToken'] is False
    machines = {str(row.machine_id): row for row in request.registration.spec.machines}
    destinations = {}
    for secret in phases['material'].values():
        assert secret['immutable'] is True and set(secret['data']) == {'token'}
        identity = secret['metadata']['labels']['loom.nebius/pool-machine']
        destinations.setdefault(identity, set()).add(secret['metadata']['namespace'])
    for identity, machine in machines.items():
        assert destinations[identity] == ({'loom-nebius-management-dev'} if machine.role == 'gateway'
            or machine.workload_scope == 'application_builder' else
            ({'loom-dev', request.registration.spec.participants[0].execution_namespace.name}
             if machine.role == 'participant' else {request.registration.spec.participants[0].execution_namespace.name}))


class PoolServer(RegistrationServer):
    """Kubernetes wire boundary; parent, exact-document adapters and journals are real."""

    def __init__(self, intent, retained):
        resources = {**retained[4].store.resources,
            **{'Secret:' + name: doc for name, doc in retained[4].bootstrap.secrets.secrets.items()}}
        super().__init__(module()._preview(intent), resources)
        self.intent, self.namespaces, self.delivered = intent, {}, {}
        self.created, self.fail_target = [], None
        self.after_create = None
        self.after_delivery = None
        self.names = [row['metadata']['name'] for row in module().namespace_documents(intent).values()]
        self.bound_uids = None

    def handle(self, message):
        path, method = message.url.path, message.method
        names = self.names
        if path == '/api/v1/namespaces' or path in ['/api/v1/namespaces/' + name for name in names]:
            self.calls.append(message)
            if method == 'GET':
                actual = self.namespaces.get(path.rsplit('/', 1)[-1])
                return httpx.Response(404) if actual is None else httpx.Response(200, json=actual)
            assert method == 'POST' and path == '/api/v1/namespaces'
            doc = json.loads(message.content)
            assert doc['kind'] == 'Namespace' and doc['metadata']['name'] in names
            value = copy.deepcopy(doc)
            value['metadata'].update(uid=str(uuid4()), resourceVersion='1')
            value['metadata']['labels']['kubernetes.io/metadata.name'] = value['metadata']['name']
            value['spec'] = {'finalizers': ['kubernetes']}
            if message.url.params.get('dryRun') != 'All':
                target = ('Namespace', value['metadata']['name'])
                self.created.append(target)
                if self.fail_target == target and self.failure == 'before':
                    raise httpx.ReadTimeout('namespace request lost')
                self.namespaces[value['metadata']['name']] = value
                if self.after_create is not None:
                    self.after_create()
                if self.fail_target == target and self.failure == 'after':
                    raise httpx.ReadTimeout('namespace response lost')
            return httpx.Response(201, json=value)
        if len(self.namespaces) == 2:
            uids = {name: value['metadata']['uid'] for name, value in self.namespaces.items()}
            if self.bound_uids != uids:
                self.request = module().bind_namespaces(self.intent, uids)
                self.bound_uids = uids
        registration_name = 'loom-pool-registration-'
        if method == 'POST':
            doc = json.loads(message.content)
            if not doc['metadata']['name'].startswith(registration_name):
                self.calls.append(message)
                key = path.split('?')[0] + '/' + doc['metadata']['name']
                value = copy.deepcopy(doc)
                value['metadata'].update(uid=str(uuid4()), resourceVersion='1')
                if message.url.params.get('dryRun') != 'All':
                    target = (doc['kind'], doc['metadata']['name'])
                    self.created.append(target)
                    if self.fail_target == target and self.failure == 'before':
                        raise httpx.ReadTimeout('request lost')
                    assert key not in self.delivered
                    self.delivered[key] = value
                    if self.after_delivery is not None:
                        self.after_delivery(value)
                    if self.fail_target == target and self.failure == 'after':
                        raise httpx.ReadTimeout('response lost')
                return httpx.Response(201, json=value)
        if method == 'GET' and any(prefix in path for prefix in ('/secrets/loom-pool-machine-',
                '/configmaps/loom-pool-profiles-', '/serviceaccounts/loom-pool-gateway', '/deployments/loom-pool-gateway')):
            self.calls.append(message)
            value = self.delivered.get(path)
            return httpx.Response(404) if value is None else httpx.Response(200, json=value)
        response = super().handle(message)
        if path.endswith('/log') and response.status_code == 200:
            value = response.json()
            value['machines'] = 4
            return httpx.Response(200, json=value)
        return response


@pytest.fixture
def connected(pool_inputs, retained, monkeypatch):
    intent = prepare(pool_inputs)
    name = 'scripts.ops.nebius_development_pool_live'
    if importlib.util.find_spec(name) is None:
        pytest.fail('connected development pool installer is missing')
    live = importlib.import_module(name)
    server = PoolServer(intent, retained)
    real_client = httpx.Client

    def client(*args, **kwargs):
        kwargs.pop('transport').close()
        return real_client(*args, **kwargs, transport=httpx.MockTransport(server.handle))

    monkeypatch.setattr(httpx, 'Client', client)
    with live.HTTPSDevelopmentPoolInstallAPI(intent=intent,
            api_server=server.request.retained.inputs.operator_connection.endpoint, ssl_context=ssl.create_default_context()) as api:
        yield SimpleNamespace(intent=intent, api=api, server=server)


def install(connected, execute=True):
    return importlib.import_module('scripts.ops.nebius_development_pool_install').install_development_pool(
        intent=connected.intent, api=connected.api, execute=execute)


def test_connected_preflight_is_get_only_without_namespace_or_state_creation(connected):
    result = install(connected, execute=False)
    assert result['status'] == 'development_pool_preflight_qualified'
    assert not connected.server.created and not connected.server.store.creates
    assert all(message.method == 'GET' for message in connected.server.calls)
    root = Path(connected.server.request.retained.operation['state_dir']).parent
    assert not (root / 'pool-installation').exists()


def test_connected_install_waits_for_commit_then_delivers_only_disabled_runtime(connected):
    assert install(connected)['status'] == 'pending_registration'
    assert len(connected.server.namespaces) == 2
    assert len(connected.server.store.creates) == 2
    assert connected.server.delivered == {}
    connected.server.complete = True
    result = install(connected)
    assert result['status'] == 'development_pool_installed_closed'
    assert result['admission_open'] is False and result['writer_migration_complete'] is False
    snapshot = copy.deepcopy((connected.server.namespaces, connected.server.delivered))
    creates = list(connected.server.created)
    assert install(connected) == result
    assert (connected.server.namespaces, connected.server.delivered) == snapshot
    assert connected.server.created == creates
    assert len(connected.server.store.creates) == 2
    deployment, = [row for row in connected.server.delivered.values() if row['kind'] == 'Deployment']
    assert deployment['spec']['replicas'] == 0
    assert {row['kind'] for row in connected.server.delivered.values()} == {'Secret', 'ConfigMap', 'ServiceAccount', 'Deployment'}


@pytest.mark.parametrize('outcome', ['before', 'after'])
def test_namespace_uncertainty_never_repeats_create(connected, outcome):
    name = connected.intent.catalog['participants'][0]['execution_namespace']['name']
    connected.server.fail_target, connected.server.failure = ('Namespace', name), outcome
    if outcome == 'after':
        assert install(connected)['status'] == 'pending_registration'
        assert install(connected)['status'] == 'pending_registration'
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development pool installation'):
                install(connected)
    assert connected.server.created.count(('Namespace', name)) == 1


def test_existing_foreign_namespace_is_not_adopted(connected):
    name = connected.intent.catalog['participants'][0]['build_namespace']['name']
    connected.server.namespaces[name] = {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': name, 'uid': str(uuid4())}}
    for execute in (False, True):
        with pytest.raises(ValueError, match='development pool installation'):
            install(connected, execute=execute)
    assert not connected.server.created and not connected.server.store.creates


@pytest.mark.parametrize('damage', ['namespace-replaced', 'child-journal-missing', 'bad-receipt', 'db-replaced'])
def test_registration_boundary_rejects_drift_without_delivering_runtime(connected, damage):
    assert install(connected)['status'] == 'pending_registration'
    connected.server.complete = True
    if damage == 'namespace-replaced':
        next(iter(connected.server.namespaces.values()))['metadata']['uid'] = str(uuid4())
    elif damage == 'child-journal-missing':
        root = Path(connected.server.request.retained.operation['state_dir']).parent
        (root / 'pool-installation/namespaces/stage.json').unlink()
    elif damage == 'bad-receipt':
        connected.server.damage_report = True
    else:
        connected.server.store.resources['Secret:loom-platform-db']['metadata']['uid'] = str(uuid4())
    with pytest.raises(ValueError, match='development pool installation'):
        install(connected)
    assert not connected.server.delivered


def test_private_input_change_inside_phase_blocks_the_next_create(connected, tmp_path):
    path = tmp_path / 'private-inputs.json'
    path.write_bytes(b'original')
    path.chmod(0o600)
    connected.api.private_files = {path: b'original'}
    connected.server.after_create = lambda: path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='development pool installation'):
        install(connected)
    assert len(connected.server.created) == 1
    assert not connected.server.store.creates


def test_material_drift_during_later_runtime_create_cannot_complete(connected):
    connected.server.complete = True

    def drift(value):
        if value['kind'] == 'Deployment':
            secret = next(row for row in connected.server.delivered.values() if row['kind'] == 'Secret')
            secret['metadata']['uid'] = str(uuid4())

    connected.server.after_delivery = drift
    with pytest.raises(ValueError, match='development pool installation'):
        install(connected)


@pytest.mark.parametrize('kind', ['Secret', 'ConfigMap', 'ServiceAccount', 'Deployment'])
@pytest.mark.parametrize('outcome', ['before', 'after'])
def test_delivery_uncertainty_restarts_without_repeating_a_create(connected, kind, outcome):
    from scripts.ops.nebius_development_pool_live import HTTPSDevelopmentPoolInstallAPI

    assert install(connected)['status'] == 'pending_registration'
    connected.server.complete = True
    request = connected.server.request
    documents = module().delivery_documents(connected.intent, request)
    target = next((doc['kind'], doc['metadata']['name']) for phase in documents.values()
        for doc in phase.values() if doc['kind'] == kind)
    connected.server.fail_target, connected.server.failure = target, outcome
    for _ in range(2):
        # A new process/connection must consume the same disk state, not its last
        # in-memory request or the previous HTTP response.
        with HTTPSDevelopmentPoolInstallAPI(intent=prepare_intent_again(connected.intent),
                api_server=connected.api.api_server, ssl_context=ssl.create_default_context()) as fresh:
            current = SimpleNamespace(intent=connected.intent, api=fresh)
            if outcome == 'after':
                assert install(current)['status'] == 'development_pool_installed_closed'
            else:
                with pytest.raises(ValueError, match='development pool installation'):
                    install(current)
    # The environment machine intentionally goes to two different namespaces.
    assert connected.server.created.count(target) == (2 if kind == 'Secret'
        and outcome == 'after' and len([row for row in documents['material'].values()
            if row['metadata']['name'] == target[1]]) == 2 else 1)


def prepare_intent_again(intent):
    return module().prepare_intent(reference=intent.reference, catalog=copy.deepcopy(intent.catalog),
        tokens=dict(intent.tokens))


@pytest.fixture
def pool_entry(connected, monkeypatch):
    entry = importlib.import_module('scripts.ops.nebius_development_pool_entry')
    old = connected.server.request.retained
    operation_id = connected.intent.catalog['operation_id']
    owner = Path(old.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-development-pool' / old.binding.installation_id / operation_id
    root.mkdir(mode=0o700, parents=True)
    inputs = {'schema_version': 'loom.nebius-development-pool-inputs.v1',
        'retained': connected.intent.reference.model_dump(mode='json'), 'catalog': connected.intent.catalog,
        'tokens': {str(key): value for key, value in connected.intent.tokens.items()},
        'operator_connection': old.inputs.operator_connection.model_dump(mode='json')}
    raw = json.dumps(inputs)
    input_path = root / 'inputs.json'
    input_path.write_text(raw)
    input_path.chmod(0o600)
    operation = {'schema': 'loom.nebius-development-pool-operation.v1', 'source_sha': 'e' * 40,
        'installation_id': old.binding.installation_id, 'namespace': old.binding.namespace,
        'operation_id': operation_id, 'inputs_path': str(input_path), 'inputs_sha256': hashlib.sha256(raw.encode()).hexdigest()}
    path = root / 'operation.json'
    path.write_text(json.dumps(operation))
    path.chmod(0o600)
    source = root / 'development-pool-source.json'
    source.write_text(json.dumps({'source_sha': operation['source_sha'], 'source_archive_sha256': 'sha256:' + 'e' * 64}))
    source.chmod(0o600)
    monkeypatch.setattr(entry, 'SOURCE_RECORD', source)

    async def transport(_):
        return ssl.create_default_context(), 'operator-test-token'

    monkeypatch.setattr(entry, '_transport', transport)
    return entry, operation, inputs, path, connected


def test_private_entry_drives_real_connected_parent_and_exports_closed_result(pool_entry, capsys):
    entry, operation, _, path, connected = pool_entry
    assert entry.main(str(path), 'preflight') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_pool_preflight_qualified'
    assert entry.main(str(path), 'install') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'pending_registration'
    connected.server.complete = True
    assert entry.main(str(path), 'install') == 0
    value = json.loads(capsys.readouterr().out)
    assert value['status'] == 'development_pool_installed_closed'
    assert value['source_sha'] == operation['source_sha']
    assert value['admission_open'] is False and value['writer_migration_complete'] is False
    assert not any(token in json.dumps(value) for token in connected.intent.tokens.values())


@pytest.mark.parametrize('damage', ['hash', 'namespace', 'endpoint', 'operation-id', 'source', 'token'])
def test_private_entry_rejects_rebinding_before_transport(pool_entry, monkeypatch, capsys, damage):
    entry, operation, inputs, path, _ = pool_entry
    if damage == 'hash':
        operation['inputs_sha256'] = '0' * 64
    elif damage == 'namespace':
        operation['namespace'] = 'loom-nebius-platform'
    elif damage == 'endpoint':
        inputs['operator_connection']['endpoint'] = 'https://foreign.example.com'
    elif damage == 'operation-id':
        operation['operation_id'] = str(uuid4())
    elif damage == 'source':
        operation['source_sha'] = 'f' * 40
    else:
        inputs['tokens'][next(iter(inputs['tokens']))] = 'wrong-private-token'
    raw = json.dumps(inputs)
    Path(operation['inputs_path']).write_text(raw)
    if damage != 'hash':
        operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    path.write_text(json.dumps(operation))
    monkeypatch.setattr(entry, 'connected_api', lambda *_: pytest.fail('transport opened'))
    assert entry.main(str(path), 'install') == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'blocked'
