"""A shared handoff consumes both real completion chains, not asserted readiness."""
from __future__ import annotations

import copy
import hashlib
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_development_activation import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_activation import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_activation import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_activation import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_activation import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_activation import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_activation import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_activation import (
    connected as connected,
)
from tests.ops.test_nebius_development_activation import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_activation import (
    entry as entry,
)
from tests.ops.test_nebius_development_activation import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_activation import (
    installation as installation,
)
from tests.ops.test_nebius_development_activation import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_activation import (
    live as live,
)
from tests.ops.test_nebius_development_activation import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_activation import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_activation import (
    material as material,
)
from tests.ops.test_nebius_development_activation import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_activation import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_activation import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_activation import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_activation import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_activation import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_activation import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_activation import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_activation import (
    publication as publication,
)
from tests.ops.test_nebius_development_activation import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_activation import (
    retained as retained,
)
from tests.ops.test_nebius_development_activation import (
    route as route,
)
from tests.ops.test_nebius_development_activation import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_activation import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_development_pool_retained import (
    RuntimeParentAPI,
    database_runtime,
    runtime_install_request,
)
from tests.ops.test_nebius_development_pool_retained import (
    pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_pool_predecessor import finish_cutover

ROOT = Path(__file__).resolve().parents[2]
pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


@pytest.fixture
def original_platform_inputs():
    from tests.unit.test_nebius_platform_render import platform_inputs

    config, candidate, profile = platform_inputs.__wrapped__()
    config['cluster_id'] = 'mk8scluster-e00fixture'
    candidate.update(repository='ca-mrc/loom', source_ref='refs/heads/dev')
    return config, candidate, profile


@pytest.fixture
def staging_history(original_platform_inputs, retained, tmp_path):
    """Run the existing legacy installer/recovery in an independent fixture root.

    Only external I/O is doubled by those fixtures. Calling their builders
    explicitly avoids colliding legacy/dev fixture names or patching history
    readers in the behavior under test.
    """
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from tests.ops import test_nebius_management_install as install
    from tests.ops import test_nebius_pool_cutover_entry as entry
    from tests.ops.test_nebius_pool_runtime import runtime_inputs_for_environments

    base = tmp_path / 'legacy'
    base.mkdir()
    platform = copy.deepcopy(original_platform_inputs)
    management = entry.management_inputs.__wrapped__(platform)
    installed = install.installation.__wrapped__(management, entry.material.__wrapped__())
    bound = replace(installed[0].binding, kube_system_uid=retained[3].binding.kube_system_uid)
    installed = replace(installed[0], binding=bound), install.InstallationAPI(bound)
    with contextmanager(entry.checks.__wrapped__)(installed, entry.cloud.__wrapped__(), base) as checks:
        inputs = entry.entry_inputs.__wrapped__(base, checks, installed)
        application = entry.application_management_inputs.__wrapped__(management, platform)
        upgrade = entry.private_upgrade.__wrapped__(inputs, application, entry.application_material.__wrapped__(), base)
        completed = entry.completed_upgrade.__wrapped__(upgrade)
        runtime = runtime_inputs_for_environments(platform, management, ('development',))
        collector = entry.collector_inputs.__wrapped__(platform, runtime)
        retirement = entry.retirement_inputs.__wrapped__(runtime, platform)
        fencing = entry.fencing_inputs.__wrapped__(retirement)
        cutover = entry.cutover_inputs.__wrapped__(collector, retirement, fencing, runtime)
        with pytest.MonkeyPatch.context() as patch:
            database = entry.database_guard.__wrapped__(runtime, platform, base, patch)
            operation, _, root = entry.private_cutover.__wrapped__(completed, cutover, database)
            _, completion = finish_cutover(operation, legacy=True)
    return load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=completion['completion_sha256']), original=root)


@pytest.fixture
def staging_observation():
    return {}


@pytest.fixture
def published_source(publication, source_checkout, tmp_path):
    from tests.ops import test_nebius_development_preflight as preflight

    original = preflight.inputs

    def canonical(path):
        record, private, keyring = original(path)
        record['repository'] = 'ca-mrc/loom'
        return record, private, keyring

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(preflight, 'inputs', canonical)
        return preflight.published_source.__wrapped__(publication, source_checkout, tmp_path)


@pytest.fixture
def pool_inputs(request, retained, staging_history, staging_observation, original_platform_inputs):
    from scripts.ops.nebius_staging_pool_attachment import prepare_staging_pool_attachment

    from loom.nebius_platform_render import build_platform
    from loom.nebius_pool_contract import PoolParticipantV1

    reference, catalog, tokens = dev_pool_inputs.__wrapped__(request, retained)
    legacy = staging_history
    old, = legacy.context.inputs.installation.participants
    guard, = legacy.context.inputs.guards
    candidate = copy.deepcopy(retained[3].candidate)
    profile = copy.deepcopy(retained[3].profile)
    config = copy.deepcopy(original_platform_inputs[0])
    config.update(namespace=guard.namespace, execution_namespace=old.execution_namespace.name,
        target_id=old.targets[0].target_id, cluster_id=catalog['cluster_id'],
        execution_node_group_id=catalog['node_group_id'],
        task_image_builder={'registry_repository': 'cr.eu-north1.nebius.cloud/staging/task-images', 'max_concurrent': 2})
    rendered = build_platform(config, candidate, profile, catalog['profiles']['image_admission_keyring'], repo_root=ROOT)
    kinds = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
        'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
    rows = [row for group in rendered.values() for row in group if row['kind'] in kinds]
    pins = {**legacy.completion.workloads, _key(guard.database.statefulset): guard.database.statefulset,
        _key(guard.database.service): guard.database.service}
    namespaces = {guard.namespace: str(guard.namespace_uid), old.execution_namespace.name: str(old.execution_namespace.uid),
        old.build_namespace.name: str(old.build_namespace.uid)}
    for row in rows:
        prior = pins.get(_key(row), {}).get('metadata', {})
        row['metadata'].update(uid=prior.get('uid', str(uuid4())), resourceVersion='201')
        row['metadata'].setdefault('annotations', {}).update(prior.get('annotations', {}))
        if row['kind'] == 'Namespace':
            row['metadata']['uid'] = namespaces[row['metadata']['name']]
    staging_observation.update(schema_version='loom.nebius-pool-resource-observation.v1',
        cluster_id=config['cluster_id'], namespace=config['namespace'], candidate_sha=candidate['candidate_sha'], resources=rows,
        database_credential={'uid': str(guard.database.credential_uid), 'resource_version': '202'},
        actuator_credential={'uid': str(guard.database.actuator_credential_uid), 'resource_version': '203'})
    staged = old.model_dump(mode='json')
    staged.update(participant_id=str(uuid4()), pool_id=catalog['pool_id'], installation_id=catalog['installation_id'],
        environment_class='staging', legacy_catalog_environment='development', admission_epoch=catalog['admission_epoch'])
    staged['targets'][0]['workload_kinds'] = ['trial', 'verifier', 'task_image_build']
    attachment = prepare_staging_pool_attachment(participant=PoolParticipantV1.model_validate(staged),
        observation=staging_observation, candidate=candidate, cluster_id=catalog['cluster_id'],
        node_group_id=catalog['node_group_id'], repo_root=ROOT)
    for kind in ('execution', 'task_images'):
        catalog['profiles'][kind].extend(attachment.profiles.model_dump(mode='json')[kind])
    catalog['participants'].append(staged)
    machine = copy.deepcopy(next(row for row in catalog['machines'] if row['role'] == 'participant'))
    machine.update(machine_id=str(uuid4()), participant_id=staged['participant_id'])
    token = 'staging-test-token'
    machine['token_sha256'] = hashlib.sha256(token.encode()).hexdigest()
    catalog['machines'].append(machine)
    tokens[UUID(machine['machine_id'])] = token
    return reference, catalog, tokens


@pytest.fixture
def shared_runtime(completed_pool, publisher_cloud):
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
    from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication

    from loom_service.environment_management.candidates import ProtectedPublication
    from loom_service.environment_management.manager import CandidateBundle

    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    retained = request.database.manager.retained.request.retained.inputs
    candidate, profile = copy.deepcopy(retained.candidate), copy.deepcopy(retained.profile)
    source = PreparedDevelopmentSource(source_sha=candidate['candidate_sha'], source_archive_sha256=candidate['source_archive_sha256'])
    selected = ProtectedPublication(candidate_id=uuid4(), source_sha=source.source_sha, run_id=candidate['run_id'],
        run_attempt=1, artifact_id=23456, artifact_sha256='sha256:' + '7' * 64, pull_request=2399)
    baseline = DevelopmentRuntimePublication(source, selected, CandidateBundle(selected.candidate_id, candidate, profile))
    request = replace(request, database=database_runtime(completed_pool, publication=baseline))
    plan = runtime.prepare_runtime_install(request)
    api = RuntimeParentAPI(request, plan)
    api.database_complete = api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    return request, api


@pytest.fixture
def shared_operation(shared_runtime, staging_history, staging_observation, completed_pool, handoff):
    """A real source/history plan and GET-qualified database-bound migration."""
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_guard_bindings
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory

    request, parent = shared_runtime
    _, documents, handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    for row in documents.values():
        if row['kind'] == 'Secret':
            row['metadata'].setdefault('resourceVersion', '501')
    args = dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT)
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handler))
        bound = observe_shared_pool_guard_bindings(**args, development=observer)
    return args, bound


def _shared_entry_inputs(args, bindings):
    """Retain genuine runtime input/source and a separately captured shared operation."""
    import base64
    import json
    from dataclasses import asdict

    from scripts.ops import nebius_certificates as private_state

    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    owner = Path(manager.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-development-runtime' / manager.binding.installation_id / str(runtime.database.operation_id)
    root.mkdir(parents=True, mode=0o700)
    material = {}
    values = runtime.database.material[0]['data']
    for field, raw in (('collector_credential_file', runtime.collector_credential),
            ('registry_credential_file', runtime.registry_credential),
            ('actuator_password_file', base64.b64decode(values['actuator-password'])),
            ('batch_runner_token_file', base64.b64decode(values['batch-runner-token']))):
        path = root / (field + '.private')
        private_state._write_private(path, raw)
        material[field] = str(path)
    cache = None
    if runtime.cache_material is not None:
        cache = {}
        for index, (key, value) in enumerate(runtime.cache_material.items()):
            path = root / f'cache-{index}.private'
            private_state._write_private(path, value.encode())
            cache[key] = str(path)
    publication = runtime.database.manager.publication
    source = publication.source.model_dump(mode='json')
    private_state._atomic_json(root / 'development-runtime-source.json', source)
    retired = {'endpoint': runtime.database.foundation.inputs.config['kubernetes_api_server'],
        'ca_file': str(root / 'retired-operator-ca.pem'), 'credentials_file': str(root / 'retired-operator-key.json')}
    value = {'schema_version': 'loom.nebius-development-runtime-inputs.v1',
        'retained': runtime.database.reference.model_dump(mode='json'),
        'publication': publication.publication.model_dump(mode='json'),
        'candidate': publication.bundle.candidate, 'profile': publication.bundle.profile,
        'collector_scope': runtime.collector_scope.model_dump(mode='json'),
        'registry_scope': runtime.registry_scope.model_dump(mode='json'),
        'cache_files': cache, 'operator_connection': retired, **material}
    private_state._atomic_json(root / 'inputs.json', value)
    runtime_operation = {'schema': 'loom.nebius-development-runtime-operation.v1',
        'source_sha': source['source_sha'], 'installation_id': manager.binding.installation_id,
        'namespace': manager.binding.namespace, 'operation_id': str(runtime.database.operation_id),
        'inputs_path': str(root / 'inputs.json'), 'inputs_sha256': hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()}
    private_state._atomic_json(root / 'operation.json', runtime_operation)
    shared_root = owner / '.loom/nebius-shared-pool' / manager.binding.installation_id / str(bindings.migration.registration.spec.operation_id)
    shared_root.mkdir(parents=True, mode=0o700)
    private_state._atomic_json(shared_root / 'shared-pool-source.json', source)
    private_state._write_private(shared_root / 'operator-ca.pem', runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'].encode())
    private_state._write_private(shared_root / 'operator-key.json', b'private-new-shared-operator')
    private_state._atomic_json(shared_root / 'staging.json', args['observation'])
    dev, = (row for row in bindings.migration.guards if row.namespace == 'loom-dev')
    inputs = {'schema_version': 'loom.nebius-shared-pool-inputs.v1',
        'runtime': {'operation_path': str(root / 'operation.json'),
            'operation_sha256': hashlib.sha256((root / 'operation.json').read_bytes()).hexdigest()},
        'legacy': args['legacy'].selector.model_dump(mode='json'),
        'staging_observation_file': str(shared_root / 'staging.json'),
        'staging_observation_sha256': hashlib.sha256((shared_root / 'staging.json').read_bytes()).hexdigest(),
        'development_guard': json.loads(json.dumps(asdict(dev), default=str)),
        'operator_connection': {'endpoint': retired['endpoint'], 'ca_file': str(shared_root / 'operator-ca.pem'),
            'credentials_file': str(shared_root / 'operator-key.json')}}
    private_state._atomic_json(shared_root / 'inputs.json', inputs)
    operation = {'schema': 'loom.nebius-shared-pool-operation.v1', 'source_sha': source['source_sha'],
        'installation_id': manager.binding.installation_id, 'namespace': manager.binding.namespace,
        'operation_id': str(bindings.migration.registration.spec.operation_id),
        'inputs_path': str(shared_root / 'inputs.json'),
        'inputs_sha256': hashlib.sha256((shared_root / 'inputs.json').read_bytes()).hexdigest()}
    private_state._atomic_json(shared_root / 'operation.json', operation)
    return shared_root, operation, inputs, root, retired


def test_shared_private_entry_binds_retained_runtime_without_retired_operator_access(shared_operation, monkeypatch):
    import importlib.util

    from scripts.ops import nebius_certificates as private_state

    name = 'scripts.ops.nebius_shared_pool_entry'
    assert importlib.util.find_spec(name) is not None, 'protected shared-pool private entry is missing'
    module = __import__(name, fromlist=['load_inputs'])
    args, bindings = shared_operation
    root, operation, inputs, runtime_root, retired = _shared_entry_inputs(args, bindings)
    monkeypatch.setattr(module, 'SOURCE_RECORD', root / 'shared-pool-source.json')
    monkeypatch.setattr(module, '_transport', lambda *_: pytest.fail('intake must finish before credential exchange'))
    context = module.load_inputs(operation)
    assert context.runtime == args['runtime'] and context.legacy == args['legacy']
    assert context.bindings == bindings and context.observation == args['observation']
    assert all(path.read_bytes() == raw for path, raw in context.files.items())
    assert {root / 'inputs.json', root / 'staging.json', runtime_root / 'operation.json',
        runtime_root / 'inputs.json', runtime_root / 'development-runtime-source.json'} <= context.files.keys()
    assert all(not Path(retired[key]).exists() and Path(retired[key]) not in context.files
        for key in ('ca_file', 'credentials_file'))
    assert 'private-new-shared-operator' not in repr(context)
    original = {path: path.read_bytes() for path in context.files}
    for damage in ('source', 'runtime-source', 'snapshot', 'runtime-reference', 'dev-database', 'dev-controller',
            'staging-namespace', 'pool-operation', 'legacy', 'missing-runtime-child', 'non-private-key'):
        changed = copy.deepcopy(operation)
        altered = copy.deepcopy(inputs)
        try:
            if damage == 'source':
                changed['source_sha'] = 'f' * 40
            elif damage == 'runtime-source':
                private_state._atomic_json(runtime_root / 'development-runtime-source.json',
                    {'source_sha': 'f' * 40, 'source_archive_sha256': 'sha256:' + 'e' * 64})
            elif damage == 'snapshot':
                (root / 'staging.json').write_bytes((root / 'staging.json').read_bytes() + b'\n')
            elif damage == 'runtime-reference':
                altered['runtime']['operation_sha256'] = 'f' * 64
            elif damage == 'dev-database':
                altered['development_guard']['database']['credential_uid'] = str(uuid4())
            elif damage == 'dev-controller':
                altered['development_guard']['controller']['spec']['replicas'] = 0
            elif damage == 'staging-namespace':
                changed['namespace'] = 'loom-staging'
            elif damage == 'pool-operation':
                changed['operation_id'] = str(uuid4())
            elif damage == 'legacy':
                altered['legacy']['completion_sha256'] = 'f' * 64
            elif damage == 'missing-runtime-child':
                (bindings.plan.development.runtime.state_dir / 'start/transition.json').unlink()
            else:
                (root / 'operator-key.json').chmod(0o644)
            private_state._atomic_json(root / 'inputs.json', altered)
            changed['inputs_sha256'] = hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()
            with pytest.raises(ValueError, match='shared pool private inputs unqualified'):
                module.load_inputs(changed)
        finally:
            for path, raw in original.items():
                path.write_bytes(raw)
                path.chmod(0o600)
    # Old operation paths may reside in their authenticated immutable release;
    # an arbitrary same-content operation copied elsewhere is not a predecessor.
    copied = root / 'foreign/operation.json'
    copied.parent.mkdir(mode=0o700)
    copied.write_bytes((runtime_root / 'operation.json').read_bytes())
    copied.chmod(0o600)
    inputs['runtime']['operation_path'] = str(copied)
    private_state._atomic_json(root / 'inputs.json', inputs)
    operation['inputs_sha256'] = hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='shared pool private inputs unqualified'):
        module.load_inputs(operation)


@pytest.mark.timeout(420)
def test_shared_private_entry_dispatches_real_parent_and_preserves_uncertain_intent(
        shared_operation, shared_guard_transport, monkeypatch, capsys):
    """Entry, concrete parent and guard child are real; external observations/transport are controlled."""
    import json
    import ssl
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as concrete
    from scripts.ops import nebius_shared_pool_entry as entry

    assert callable(getattr(entry, 'main', None)), 'protected shared entry dispatch is missing'
    assert callable(getattr(entry, 'connected_api', None)), 'protected shared connection is missing'
    args, bindings = shared_operation
    guard, model = shared_guard_transport
    root, operation, _, _, _ = _shared_entry_inputs(args, bindings)
    monkeypatch.setattr(entry, 'SOURCE_RECORD', root / 'shared-pool-source.json')
    operator = root / 'operator-key.json'
    credentials, configs, failures = [], [], set()
    context = ssl.create_default_context(cadata=(root / 'operator-ca.pem').read_text())

    async def transport(connection):
        assert connection.endpoint == args['runtime'].database.foundation.inputs.config['kubernetes_api_server']
        assert connection.ca_file == root / 'operator-ca.pem' and connection.credentials_file == operator
        credentials.append(operator.read_bytes())
        if 'authentication' in failures:
            operator.write_bytes(b'changed-during-token-exchange')
        return context, 'test-operator-token'

    monkeypatch.setattr(entry, '_transport', transport)

    def checked(**options):
        if 'bindings' in options:
            assert options['bindings'] == bindings
        return SimpleNamespace(plan=bindings.plan, bindings=bindings, pending_workloads=(),
            pool_state='closed', guard_states={target.namespace: 'held' if target.namespace in model.held else 'open'
                for target in bindings.migration.guards}, input_digest=bindings.input_digest, authority={})

    async def publication(**options):
        assert options['request'] == args['runtime']

    async def provider(**options):
        assert options['operator_credentials'] == operator and options['runtime'] == args['runtime']
        return checked(**options)

    monkeypatch.setattr(concrete, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(concrete, 'observe_shared_pool_provider', provider)
    for name in ('observe_shared_pool_writers', 'observe_shared_pool_forward_writers',
            'observe_shared_pool_processes', 'observe_shared_pool_forward_processes'):
        monkeypatch.setattr(concrete, name, checked)
    real_api = concrete.HTTPSSharedPoolOperationAPI

    def api(**options):
        connection = real_api(**options)
        connection.client.close()
        connection.client = type(connection.client)(base_url=connection.api_server,
            transport=httpx.MockTransport(lambda request: httpx.Response(404)))
        guard.prefix = list(connection.guards.prefix)
        configs.append(Path(connection.prefix[connection.prefix.index('--kubeconfig') + 1]))
        return connection

    monkeypatch.setattr(entry, 'HTTPSSharedPoolOperationAPI', api)
    manager = args['runtime'].database.manager.retained.request.retained
    anchor = Path(manager.operation['anchor_dir'])
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    def run(action, code, status):
        assert entry.main(str(root / 'operation.json'), action) == code
        raw = capsys.readouterr().out
        assert 'private-new-shared-operator' not in raw and 'test-operator-token' not in raw
        value = json.loads(raw)
        assert value['status'] == status
        assert all(not path.exists() for path in configs), 'temporary connection leaked after entry exit'
        return value

    assert run('qualify', 0, 'tooling_qualified') == {'status': 'tooling_qualified'}
    assert not credentials
    assert run('forward-guards', 1, 'blocked') == {'status': 'blocked', 'stage': 'operation'}
    assert not credentials and not model.acquires and not state.exists()
    report = run('preflight', 0, 'shared_pool_preflight_qualified')
    assert {key: report[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')} == {
        key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
    assert report['admission_open'] is False and report['writer_migration_complete'] is False
    assert not state.exists() and not (anchor / 'shared-pool-handoff.json').exists() and not model.acquires
    # Selecting recovery without an existing handoff must never become install.
    assert run('recover', 1, 'blocked')['stage'] == 'handoff'
    assert not state.exists() and not model.acquires

    failures.add('authentication')
    assert run('install', 1, 'blocked')['stage'] == 'connection'
    assert not model.acquires and not state.exists()
    failures.clear()
    operator.write_bytes(b'private-new-shared-operator')
    # A changed pinned file is rejected even before another credential exchange.
    loaded = entry.load_inputs(operation)
    before = len(credentials)
    operator.write_bytes(b'changed-before-connect')
    with pytest.raises(ValueError):
        with entry.connected_api(loaded):
            pytest.fail('changed private input reached the handoff')
    assert len(credentials) == before
    operator.write_bytes(b'private-new-shared-operator')

    model.busy = bindings.migration.guards[0].namespace
    assert run('install', 0, 'pending')['phase'] == 'forward-guards'
    assert model.acquires == [model.busy] and not model.held
    model.busy, model.uncertain = None, bindings.migration.guards[0].namespace
    assert run('install', 1, 'blocked')['stage'] == 'handoff'
    attempted = list(model.acquires)
    assert run('install', 1, 'blocked')['stage'] == 'handoff'
    assert model.acquires == attempted
    assert json.loads((state / 'operation.json').read_bytes())['forward']['forward-guards']['status'] == 'started'


class SharedPhaseAPI:
    """Only the phase-adapter boundary is doubled in parent-journal tests.

    This models a child engine that writes intent before remote dispatch and
    reads an uncertain result on resumption. It does not prove live adapters.
    """
    def __init__(self, module):
        self.supported_phases = frozenset((*module.FORWARD_PHASES, *module.RECOVERY_PHASES))
        self.events = []
        self.qualifications = []
        self.preflights = 0
        self.block = None
        self.uncertain = None
        self.prepare_failure = None

    def preflight(self, bindings):
        self.preflights += 1

    def qualify(self, bindings, record, *, recovering):
        self.qualifications.append((record['outcome'], recovering))

    def prepare(self, phase, bindings, state):
        from scripts.ops import nebius_certificates as private_state

        if self.prepare_failure == (phase, 'before'):
            raise TimeoutError('simulated interruption before local child preparation')
        private_state._private_directory(state)
        if self.prepare_failure == (phase, 'directory'):
            raise TimeoutError('simulated interruption after private directory creation')
        path = state / 'receipt.json'
        if not path.exists():
            private_state._atomic_json(path, {'phase': phase, 'status': 'pending'})
        self.validate_prepared_phase(phase, state)
        if self.prepare_failure == (phase, 'after'):
            raise TimeoutError('simulated interruption after local child preparation')

    def validate_prepared_phase(self, phase, state):
        import json

        assert json.loads((state / 'receipt.json').read_bytes()) == {'phase': phase, 'status': 'pending'}

    def validate_phase(self, phase, state, *, complete):
        import json

        record = json.loads((state / 'receipt.json').read_bytes())
        assert record['phase'] == phase
        assert record['status'] in {'pending', 'applied'}
        if complete:
            assert record['status'] == 'applied'

    def advance(self, phase, bindings, state):
        import json

        from scripts.ops import nebius_certificates as private_state

        path = state / 'receipt.json'
        if path.exists() and json.loads(path.read_bytes())['status'] == 'applied':
            return True  # Readback settles the prior dispatch; no second write.
        private_state._private_directory(state)
        private_state._atomic_json(path, {'phase': phase, 'status': 'pending'})
        if phase == self.block:
            return False
        self.events.append(phase)
        private_state._atomic_json(path, {'phase': phase, 'status': 'applied'})
        if phase == self.uncertain:
            raise TimeoutError('simulated response loss after child commit')
        return True


def test_restored_collector_uses_completed_current_lineage_and_bound_publication(shared_operation, tmp_path, monkeypatch):
    """Real source attachment/lineage; only Kubernetes and the separately tested SQL read are doubled."""
    import importlib.util
    import json

    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER

    name = 'scripts.ops.nebius_shared_pool_recovery_collector'
    assert importlib.util.find_spec(name) is not None, 'restored collector publication has no completed-job consumer'
    module = __import__(name, fromlist=['observe_restored_staging_collector'])
    _, bindings = shared_operation
    attachment = bindings.plan.staging.attachment
    collector, = (copy.deepcopy(row) for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')
    controller = copy.deepcopy(attachment.guard.controller)
    for row in (collector, controller):
        row['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
    namespace = collector['metadata']['namespace']
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': 'collector-100', 'namespace': namespace,
        'uid': str(uuid4()), 'resourceVersion': '401', 'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'CronJob',
            'name': collector['metadata']['name'], 'uid': collector['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(collector['spec']['jobTemplate']['spec']),
        'status': {'succeeded': 1, 'completionTime': '2026-10-10T20:00:00Z', 'conditions': [{'type': 'Complete', 'status': 'True'}]}}
    generated = {prefix + key: value for prefix in ('', 'batch.kubernetes.io/') for key, value in (
        ('controller-uid', job['metadata']['uid']), ('job-name', job['metadata']['name']))}
    job['spec'].update(selector={'matchLabels': {'batch.kubernetes.io/controller-uid': job['metadata']['uid']}},
        completions=1, parallelism=1, completionMode='NonIndexed', manualSelector=False,
        podReplacementPolicy='TerminatingOrFailed', suspend=False)
    job['spec']['template']['metadata']['labels'].update(generated)
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'collector-100-abcde', 'namespace': namespace,
        'uid': str(uuid4()), 'resourceVersion': '402', **copy.deepcopy(job['spec']['template']['metadata']),
        'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': job['metadata']['name'],
            'uid': job['metadata']['uid'], 'controller': True}]}, 'spec': copy.deepcopy(job['spec']['template']['spec']),
        'status': {'phase': 'Succeeded'}}
    pod['spec']['nodeName'] = 'qualified-platform-node'
    pod['metadata']['labels']['topology.kubernetes.io/region'] = attachment.config['region']
    pod['spec'].setdefault('tolerations', []).extend({'key': 'node.kubernetes.io/' + key,
        'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300} for key in ('not-ready', 'unreachable'))
    for field, statuses in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
        pod['status'][statuses] = [{'name': row['name'], 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}
            for row in pod['spec'].get(field, [])]
    documents = {_key(row): copy.deepcopy(row) for row in attachment.resources}
    documents[_key(collector)] = collector
    for volume in collector['spec']['jobTemplate']['spec']['template']['spec']['volumes']:
        for source in volume.get('projected', {}).get('sources', []):
            if 'secret' in source:
                ref = source['secret']
                documents['Secret:' + namespace + ':' + ref['name']] = {'apiVersion': 'v1', 'kind': 'Secret',
                    'metadata': {'name': ref['name'], 'namespace': namespace, 'uid': str(uuid4()), 'resourceVersion': '403'},
                    'data': {item['key']: 'cHJpdmF0ZS10ZXN0' for item in ref['items']}}
    listing = {'jobs': {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '404'}, 'items': [job]},
        'pods': {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '405'}, 'items': [pod]}}
    receipt = {'id': str(uuid4()), 'created': True, 'target_id': attachment.config['target_id'],
        'source': 'nebius-kubernetes-capacity-collector', 'source_version': 'current-provider-kubernetes-digest',
        'observed_at': '2026-10-10T20:00:00Z', 'provider_capacity_state': 'insufficient',
        'autoscaler_state': 'ready', 'observation_sha256': 'sha256:' + 'a' * 64}
    kubeconfig = tmp_path / 'collector-kubeconfig'
    kubeconfig.write_text('private-test-config')
    kubeconfig.chmod(0o600)
    api = KubectlPoolGuardAPI(request=bindings.migration, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
    mode, calls, publications = {'case': None}, [], []

    def get(kind, name, selected=None):
        calls.append(('get', kind, name, selected))
        if kind == 'namespace' and name == 'kube-system':
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': bindings.migration.registration.binding.kube_system_uid}}
        wanted_kind = {'configmap': 'ConfigMap', 'secret': 'Secret', 'cronjob': 'CronJob',
            'deployment': 'Deployment', 'namespace': 'Namespace'}[kind]
        return copy.deepcopy(documents[wanted_kind + ':' + (selected or '-') + ':' + name])

    def run(args):
        calls.append(tuple(args))
        if args[0] == 'logs':
            assert args == ['logs', '-n', namespace, 'pod/' + pod['metadata']['name'], '-c', 'collector',
                '--limit-bytes=16384', '--timestamps=false']
            if mode['case'] == 'late_secret':
                next(row for row in documents.values() if row['kind'] == 'Secret')['metadata']['resourceVersion'] = 'changed'
            if mode['case'] == 'late_binding':
                attachment.config['region'] = 'changed'
            return copy.deepcopy(receipt)
        assert args[:2] == ['get', '--raw']
        kind = 'jobs' if args[2] == '/apis/batch/v1/namespaces/' + namespace + '/jobs?limit=1000' else 'pods'
        assert args[2] == ('/apis/batch/v1' if kind == 'jobs' else '/api/v1') + '/namespaces/' + namespace + '/' + kind + '?limit=1000'
        return copy.deepcopy(listing[kind])

    def publication(target, *, expected, publication):
        from scripts.ops.nebius_management_switch import _stable

        assert target == attachment.guard and _stable(expected) == _stable(controller)
        assert publication['receipt'] == receipt
        assert publication['source'] == 'nebius-kubernetes-capacity-collector'
        assert publication['node_group_id'] == bindings.migration.registration.spec.node_group_id
        assert publication['quota_identities'] == {key: list(value) for key, value in bindings.migration.registration.spec.quota_identities.items()}
        assert {row['target_id'] for row in publication['targets']} == {row.target_id for row in attachment.participant.targets}
        publications.append(copy.deepcopy(publication))
        if mode['case'] == 'late_pod':
            listing['pods']['items'][0]['status']['containerStatuses'][0]['restartCount'] = 1
        return mode['case'] != 'stale'

    monkeypatch.setattr(api, '_get', get)
    monkeypatch.setattr(api, '_run', run)
    monkeypatch.setattr(api, 'observe_collector_publication', publication)

    def observe():
        try:
            return module.observe_restored_staging_collector(bindings=bindings, guards=api, expected=collector, controller=controller)
        except ValueError as error:
            import traceback

            frames, cause = [], error
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('restored staging collector unqualified: ' + ', '.join(frames)) from None

    assert observe() is True
    assert len(publications) == 1
    baseline = copy.deepcopy((documents, listing, attachment.config))
    for case in ('missing', 'running', 'failed', 'old_template', 'stale', 'foreign_job', 'foreign_pod', 'duplicate',
            'truncated', 'pod_image', 'region', 'sidecar', 'config', 'missing_secret_key', 'empty_secret_key',
            'late_secret', 'late_pod', 'late_binding', 'suspended'):
        documents.clear()
        documents.update(copy.deepcopy(baseline[0]))
        listing.clear()
        listing.update(copy.deepcopy(baseline[1]))
        attachment.config.clear()
        attachment.config.update(copy.deepcopy(baseline[2]))
        mode['case'] = case
        if case == 'missing':
            listing['jobs']['items'], listing['pods']['items'] = [], []
        elif case in {'running', 'failed'}:
            listing['jobs']['items'][0]['status'] = {'active': 1} if case == 'running' else {
                'failed': 1, 'conditions': [{'type': 'Failed', 'status': 'True'}]}
        elif case == 'old_template':
            listing['jobs']['items'][0]['spec']['template']['spec']['containers'][0]['image'] = 'old/image'
        elif case in {'foreign_job', 'foreign_pod'}:
            listing['jobs' if case == 'foreign_job' else 'pods']['items'][0]['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
        elif case == 'duplicate':
            listing['pods']['items'].append(copy.deepcopy(listing['pods']['items'][0]))
        elif case == 'truncated':
            listing['pods']['metadata']['continue'] = 'more'
        elif case == 'pod_image':
            listing['pods']['items'][0]['spec']['containers'][0]['image'] = 'other/image'
        elif case == 'region':
            listing['pods']['items'][0]['metadata']['labels']['topology.kubernetes.io/region'] = 'wrong-region'
        elif case == 'sidecar':
            listing['pods']['items'][0]['spec']['containers'].append({'name': 'foreign', 'image': 'other/image'})
        elif case == 'config':
            documents['ConfigMap:' + namespace + ':loom-execution-capacity-collector']['data']['EXTRA_OVERRIDE'] = 'changed'
        elif case in {'missing_secret_key', 'empty_secret_key'}:
            secret = next(row for row in documents.values() if row['kind'] == 'Secret')
            secret['data'] = ({'unrelated': 'dW5yZWxhdGVk'} if case == 'missing_secret_key'
                else dict.fromkeys(secret['data'], ''))
        elif case == 'suspended':
            documents[_key(collector)]['spec']['suspend'] = True
        before = len(publications)
        if case in {'missing', 'running', 'failed', 'old_template', 'stale'}:
            assert observe() is False, case
        else:
            with pytest.raises(ValueError, match='restored staging collector unqualified'):
                observe()
        if case not in {'stale', 'late_pod', 'late_secret', 'late_binding'}:
            assert len(publications) == before, case
    assert all(row[0] in {'get', 'logs'} for row in calls)
    assert 'private-' not in json.dumps(publications)

    # The actual recovery callback connects collector health and process/SQL
    # challenges without supplying its own stop, restart or reopen authority.
    from scripts.ops.nebius_ingress_stage import _snapshot

    assert hasattr(module, 'SharedRecoveryRuntimeReadiness'), 'restored health is not connected to the restart callback'
    documents.clear()
    documents.update(copy.deepcopy(baseline[0]))
    listing.clear()
    listing.update(copy.deepcopy(baseline[1]))
    attachment.config.clear()
    attachment.config.update(copy.deepcopy(baseline[2]))
    mode['case'] = None
    controller_pod = {'metadata': {'uid': str(uuid4())}, 'status': {'containerStatuses': [{'restartCount': 0}]}}
    database_pod = {'metadata': {'uid': str(uuid4())}, 'status': {'containerStatuses': [{'restartCount': 0}]}}
    monkeypatch.setattr(api, '_database', lambda *_: copy.deepcopy(database_pod))
    monkeypatch.setattr(api, '_runtime', lambda *_, **kwargs: copy.deepcopy(controller_pod))
    readiness = module.SharedRecoveryRuntimeReadiness(bindings=bindings, guards=api)

    def callback(captured):
        desired = _snapshot(captured)
        desired['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        before = copy.deepcopy(desired)
        before['metadata']['uid'] = captured['metadata']['uid']
        before['spec']['suspend' if captured['kind'] == 'CronJob' else 'replicas'] = True if captured['kind'] == 'CronJob' else 0
        current = copy.deepcopy(desired)
        current['metadata']['uid'] = captured['metadata']['uid']
        return readiness(bindings.plan.development.runtime.request, 'recovery-restart', before, desired, current)

    assert callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')) is True
    mode['case'] = 'stale'
    assert callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob')) is False
    original_publication = api.observe_collector_publication
    def late_database(*args, **kwargs):
        result = original_publication(*args, **kwargs)
        database_pod['status']['containerStatuses'][0]['restartCount'] += 1
        return result
    monkeypatch.setattr(api, 'observe_collector_publication', late_database)
    for pending in (None, 'stale'):
        mode['case'] = pending
        with pytest.raises(ValueError, match='restored staging runtime unqualified'):
            callback(next(row for row in bindings.plan.staging.originals.values() if row['kind'] == 'CronJob'))
    monkeypatch.setattr(api, 'observe_collector_publication', original_publication)
    probes = []
    def probe(name, target, *, original, expected, **kwargs):
        assert target == attachment.guard and original == bindings.plan.staging.originals[_key(original)]
        assert expected['spec'] == original['spec']
        if name == mode['case']:
            raise ValueError('test-only failed process probe')
        probes.append((name, _key(original)))
    for method in ('database', 'legacy_settings', 'telemetry'):
        monkeypatch.setattr(api, 'qualify_runtime_' + method, lambda *args, method=method, **kwargs: probe(method, *args, **kwargs))
    for captured in bindings.plan.staging.originals.values():
        if captured['kind'] != 'Deployment' or captured['spec']['replicas'] == 0:
            continue
        mode['case'], probes[:] = None, []
        assert callback(captured) is True
        names = ['database', 'legacy_settings']
        if captured['metadata']['namespace'] == namespace:
            names.append('telemetry')
        assert probes == [(name, _key(captured)) for name in names]
        for name in names:
            mode['case'] = name
            with pytest.raises(ValueError, match='restored staging runtime unqualified'):
                callback(captured)


@pytest.fixture
def shared_guard_transport(shared_operation, tmp_path, monkeypatch):
    """Actual bound guard adapter; only kubectl's external transport is doubled."""
    import base64
    import hmac
    import json
    import subprocess
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlsplit

    from pydantic import PostgresDsn
    from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_GUARD_COMMAND,
        KubectlPoolGuardAPI,
        pool_runtime_role_sql,
    )
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        populate_runtime_pods,
    )

    _, bindings = shared_operation
    request = bindings.migration
    documents = {}

    def add(document):
        kind = document['kind']
        plural = {'Namespace': 'namespaces', 'Deployment': 'deployments', 'StatefulSet': 'statefulsets',
            'Service': 'services', 'Secret': 'secrets', 'ReplicaSet': 'replicasets'}[kind]
        prefix = '/api/v1' if document['apiVersion'] == 'v1' else '/apis/' + document['apiVersion']
        namespace = document['metadata'].get('namespace')
        path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + document['metadata']['name']
        documents[path] = copy.deepcopy(document)

    add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
        'uid': request.registration.binding.kube_system_uid}})
    urls = {}
    for target in request.guards:
        add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': target.namespace, 'uid': str(target.namespace_uid)}})
        for row in (target.controller, target.database.statefulset, target.database.service):
            add(row)
        env, = (row for row in target.controller['spec']['template']['spec']['containers'][0]['env']
            if row['name'] == 'LOOM_CP_DB_URL')
        reference = env['valueFrom']['secretKeyRef']
        urls[target.namespace] = f'postgresql+psycopg://loom_cp:private-test@loom-postgres.{target.namespace}.svc:5432/loom'
        add({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': reference['name'], 'namespace': target.namespace,
            'uid': str(target.database.credential_uid), 'resourceVersion': target.database.credential_resource_version},
            'data': {reference['key']: base64.b64encode(urls[target.namespace].encode()).decode()}})
    populate_runtime_pods(documents)
    populate_database_backends(documents)
    for row in list(documents.values()):
        if row['kind'] == 'ReplicaSetList':
            for replica in row['items']:
                add(replica)
    kubeconfig = tmp_path / 'shared-guard-kubeconfig'
    kubeconfig.write_text('private-test-config')
    kubeconfig.chmod(0o600)
    api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
    model = SimpleNamespace(held=set(), busy=None, uncertain=None, acquires=[], documents=documents,
        runtime_roles=set(), runtime_stages=[], runtime_reads=[], runtime_uncertain=False, runtime_lost_read=False,
        readiness_pages={}, readiness_calls=[], readiness_failure=None)

    def run(argv, **kwargs):
        assert argv[:len(api.prefix)] == api.prefix
        args = argv[len(api.prefix):]
        if args[0] == 'get':
            if args[1] == '--raw':
                url = urlsplit(args[2])
                value = copy.deepcopy(documents[url.path])
                selector = parse_qs(url.query).get('labelSelector')
                if selector:
                    label, wanted = selector[0].split('=', 1)
                    value['items'] = [row for row in value['items'] if row['metadata'].get('labels', {}).get(label) == wanted]
            else:
                kind, name = args[1:3]
                namespace = args[args.index('-n') + 1] if '-n' in args else None
                plural = {'namespace': 'namespaces', 'deployment': 'deployments', 'statefulset': 'statefulsets',
                    'service': 'services', 'secret': 'secrets', 'replicaset': 'replicasets'}[kind]
                prefix = '/apis/apps/v1' if kind in {'deployment', 'statefulset', 'replicaset'} else '/api/v1'
                value = documents[prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name]
        else:
            assert args[:2] == ['exec', '-n'] and args[6] == '--'
            namespace = args[2]
            if args[7] == 'psql':
                assert args[3:6] == ['pod/loom-postgres-0', '-c', 'loom-postgres']
                if args[-1] == rollout_guard_observation_sql(owner=str(request.registration.spec.operation_id),
                        candidate=request.registration.candidate['candidate_sha']):
                    value = {'status': 'held' if namespace in model.held else 'open'}
                elif (namespace, args[-1]) in model.readiness_pages:
                    assert namespace in model.held
                    model.readiness_calls.append(namespace)
                    if model.readiness_failure == namespace:
                        raise RuntimeError('simulated real SQL rejection of charged work')
                    value = copy.deepcopy(model.readiness_pages[namespace, args[-1]])
                else:
                    action, = (action for action in ('stage', 'observe') if args[-1] == pool_runtime_role_sql(
                        owner=str(request.registration.spec.operation_id), candidate=request.registration.candidate['candidate_sha'], action=action))
                    assert namespace in model.held  # Real SQL independently enforces the held idle guard.
                    if action == 'stage':
                        model.runtime_stages.append(namespace)
                        model.runtime_roles.add(namespace)
                        if model.runtime_uncertain:
                            model.runtime_uncertain, model.runtime_lost_read = False, True
                            raise TimeoutError('simulated lost committed runtime-role transaction')
                    else:
                        model.runtime_reads.append(namespace)
                        if model.runtime_lost_read:
                            model.runtime_lost_read = False
                            raise TimeoutError('simulated lost first runtime-role readback')
                        assert namespace in model.runtime_roles
                    value = {'status': 'staged' if action == 'stage' else 'qualified'}
            else:
                assert args[3:6] == ['pod/loom-control-plane-a12b34-test', '-c', 'loom-control-plane']
                assert args[7:11] == ['python', '-c', _BOUND_GUARD_COMMAND, 'acquire']
                assert args[11:13] == [str(request.registration.spec.operation_id), request.registration.candidate['candidate_sha']]
                assert hmac.new(bytes.fromhex(args[13]), str(PostgresDsn(urls[namespace])).encode(), 'sha256').hexdigest() == args[14]
                model.acquires.append(namespace)
                if namespace == model.uncertain:
                    raise TimeoutError('simulated unknown guard acquisition')
                if namespace == model.busy:
                    value = {'status': 'skipped_busy'}
                else:
                    model.held.add(namespace)
                    value = {'status': 'acquired'}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', run)
    return api, model


@pytest.mark.timeout(420)
def test_connected_shared_parent_gates_real_guard_acquisition_and_unknown_replies(shared_operation, shared_guard_transport,
        tmp_path, monkeypatch):
    """Concrete parent and real child/SQL binding; separately covered observation adapters are boundaries here."""
    import importlib.util
    import ssl
    from types import SimpleNamespace

    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_api'
    assert importlib.util.find_spec(name) is not None, 'concrete shared-pool parent is missing'
    module = __import__(name, fromlist=['HTTPSSharedPoolOperationAPI'])
    args, bindings = shared_operation
    guard, model = shared_guard_transport
    operator = tmp_path / 'operator-connection'
    operator.write_bytes(b'private-test-operator')
    operator.chmod(0o600)
    failures, seen = set(), set()

    def checked(gate, **options):
        seen.add(gate)
        if gate in failures:
            raise ValueError('simulated ' + gate + ' rejection')
        if 'bindings' in options:
            assert options['bindings'] == bindings
        return SimpleNamespace(plan=bindings.plan, bindings=bindings, pending_workloads=(),
            pool_state='closed', guard_states={target.namespace: 'held' if target.namespace in model.held else 'open'
                for target in bindings.migration.guards}, input_digest=bindings.input_digest, authority={})

    async def publication(**options):
        assert options['request'] == args['runtime']
        checked('publication')

    async def provider(**options):
        assert options['operator_credentials'] == operator and options['runtime'] == args['runtime']
        return checked('provider', **options)

    monkeypatch.setattr(module, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(module, 'observe_shared_pool_provider', provider)
    monkeypatch.setattr(module, 'observe_shared_pool_writers', lambda **options: checked('writers', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_writers', lambda **options: checked('writers', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_processes', lambda **options: checked('processes', **options))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_processes', lambda **options: checked('processes', **options))
    # Check absence through the real parent's HTTP reader, without a live API.
    import httpx

    def handle(request):
        assert request.method == 'GET'
        return httpx.Response(404)

    context = ssl.create_default_context(cadata=args['runtime'].database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    kwargs = dict(bindings=bindings, observation=args['observation'], repo_root=ROOT,
        api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
        ssl_context=context, token='operator-test', operator_credentials=operator,
        private_files={operator: operator.read_bytes()})

    def connect():
        connection = module.HTTPSSharedPoolOperationAPI(**kwargs)
        connection.client.close()
        connection.client = type(connection.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
        # Teach the external kubectl fixture the parent's freshly generated
        # private connection. Keep the actual parent-owned guard adapter intact.
        guard.prefix = list(connection.guards.prefix)
        return connection

    def run(api):
        try:
            return parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('connected parent failure at ' + ', '.join(frames)) from None

    with connect() as api:
        model.busy = bindings.migration.guards[0].namespace
        for failed in ('publication', 'provider', 'writers', 'processes'):
            failures.add(failed)
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
            failures.clear()
            assert not model.acquires and not model.held
        result = run(api)
        assert result['status'] == 'pending' and result['phase'] == 'forward-guards'
        assert seen == {'publication', 'provider', 'writers', 'processes'}
        assert model.acquires == [bindings.migration.guards[0].namespace]
        manager = args['runtime'].database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        # Intent with no response must not be redispatched by this parent.
        model.busy, model.uncertain = None, bindings.migration.guards[0].namespace
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        attempts = list(model.acquires)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert model.acquires == attempts
        for changed_phase in ('forward-drain', 'recovery-fence', '../forward-guards'):
            with pytest.raises(ValueError):
                api.advance(changed_phase, bindings, state / changed_phase)
        assert model.acquires == attempts
    # Reconstructing the concrete transport keeps the uncertain intent intact.
    with connect() as api:
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert model.acquires == attempts


def test_connected_shared_parent_forward_completion_requires_current_health(shared_operation, tmp_path, monkeypatch):
    """Real final child and parent; preceding effects and component observation boundaries are fixtures."""
    import ssl
    from types import SimpleNamespace

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_api as module
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    predecessor = SharedPhaseAPI(parent)
    predecessor.prepare_failure = ('forward-activate', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**args, bindings=bindings, api=predecessor, action='install')
    # Represent the already tested opening's completed remote effect, keeping
    # its actual semantic receipt and parent checksum validation in this test.
    activation = state / 'forward-activate'
    module.activation.prepare_shared_pool_activation(bindings=bindings, state_dir=activation)
    opened = _read(activation / 'activation.json')
    opened['opening'] = 'opened'
    for row in opened['guards'].values():
        row['release'] = 'released'
    private_state._atomic_json(activation / 'activation.json', opened)
    frame = _read(state / 'operation.json')
    frame['forward']['forward-activate'].update(status='complete', files=parent._files(activation))
    private_state._atomic_json(state / 'operation.json', frame)
    operator = tmp_path / 'forward-operator'
    operator.write_bytes(b'private-forward-test')
    operator.chmod(0o600)
    mode = {'damage': None}

    async def publication(**options):
        assert options['request'] == args['runtime']

    async def provider(**options):
        assert options['runtime'] == args['runtime']
        return SimpleNamespace(plan=bindings.plan)

    def processes(**options):
        assert options['bindings'] == bindings and options['state_dir'] == state
        if mode['damage'] == 'unhealthy':
            raise ValueError('simulated failed current process challenge')
        if mode['damage'] == 'receipt':
            path = state / 'forward-verify/verify.json'
            path.write_bytes(path.read_bytes() + b'\n')
        return module.SharedPoolForwardProcessObservation(bindings,
            'closed' if mode['damage'] == 'closed' else 'global',
            {target.namespace: 'held' if mode['damage'] == 'held' else 'open' for target in bindings.migration.guards},
            'revoked', ('Deployment:loom-dev:loom-service',) if mode['damage'] == 'pending' else (), {}, bindings.input_digest)

    monkeypatch.setattr(module, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(module, 'observe_shared_pool_provider', provider)
    monkeypatch.setattr(module, 'observe_shared_pool_forward_writers', lambda **_: module.SharedPoolWriterObservation(
        bindings.plan, '501', {}, {}, bindings.input_digest))
    monkeypatch.setattr(module, 'observe_shared_pool_forward_processes', processes)

    context = ssl.create_default_context(cadata=args['runtime'].database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    with module.HTTPSSharedPoolOperationAPI(bindings=bindings, observation=args['observation'], repo_root=ROOT,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
            token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}) as api:
        registry = api._registry()
        for phase in parent.FORWARD_PHASES[:-2]:
            def validate(*, state_dir, complete, bindings, prepared=False):
                if prepared:
                    predecessor.validate_prepared_phase(state_dir.name, state_dir)
                else:
                    predecessor.validate_phase(state_dir.name, state_dir, complete=complete)
            registry[phase] = ((lambda **_: pytest.fail('completed predecessor must not be prepared again'), validate),)
        monkeypatch.setattr(api, '_registry', lambda: registry)
        # Effective permissions are exercised with real rule reviews in the
        # connected recovery test; this test owns final phase/health composition.
        monkeypatch.setattr(api, '_permissions', lambda *_, **__: None)
        child = state / 'forward-verify'
        api.prepare('forward-verify', bindings, child)
        api.prepare('forward-verify', bindings, child)  # Interrupted local preparation is resumable.
        assert _read(child / 'verify.json')['status'] == 'prepared'
        frame['forward']['forward-verify']['status'] = 'started'
        private_state._atomic_json(state / 'operation.json', frame)
        with pytest.raises(ValueError):
            api.validate_prepared_phase('forward-verify', child)  # Started authority cannot masquerade as unstarted.
        raw = (child / 'verify.json').read_bytes()
        for damage in ('pending', 'closed', 'held', 'unhealthy', 'receipt'):
            mode['damage'] = damage
            with pytest.raises(ValueError):
                api.advance('forward-verify', bindings, child)
            assert _read(child / 'verify.json')['status'] == 'prepared'
            (child / 'verify.json').write_bytes(raw)
        mode['damage'] = None
        assert api.advance('forward-verify', bindings, child)
        assert _read(child / 'verify.json')['status'] == 'observed'
        result = parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='install')
        assert result['status'] == 'shared_pool_handoff_complete'
        assert result['admission_open'] and result['writer_migration_complete']
        retained = {path: path.read_bytes() for path in state.rglob('*') if path.is_file()}
        assert parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='preflight')['admission_open']
        mode['damage'] = 'unhealthy'
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**args, bindings=bindings, api=api, action='preflight')
        for phase in api.supported_phases:
            with pytest.raises(ValueError):
                api.advance(phase, bindings, state / phase)
        assert all(path.read_bytes() == value for path, value in retained.items())


@pytest.mark.timeout(420)
def test_shared_guard_child_retains_busy_and_unknown_acquisitions_without_registering_again(shared_operation, shared_guard_transport):
    import importlib.util

    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    name = 'scripts.ops.nebius_shared_pool_guards'
    assert importlib.util.find_spec(name) is not None, 'concrete shared guard child is missing'
    module = __import__(name, fromlist=['advance_shared_pool_guards'])
    args, bindings = shared_operation
    guards, model = shared_guard_transport

    class ConnectedGuardAPI(SharedPhaseAPI):
        guard_qualification_hook = None

        def qualify(self, bound, record, *, recovering):
            super().qualify(bound, record, recovering=recovering)
            if self.guard_qualification_hook is not None:
                self.guard_qualification_hook()

        def prepare(self, phase, bound, state):
            if phase == 'forward-guards':
                module.prepare_shared_pool_guards(bindings=bound, state_dir=state)
            else:
                super().prepare(phase, bound, state)

        def validate_prepared_phase(self, phase, state):
            if phase == 'forward-guards':
                record = module.validate_shared_pool_guards(bindings=bindings, state_dir=state, complete=False)
                assert set(record['guards'].values()) == {'prepared'}
            else:
                super().validate_prepared_phase(phase, state)

        def validate_phase(self, phase, state, *, complete):
            if phase == 'forward-guards':
                module.validate_shared_pool_guards(bindings=bindings, state_dir=state, complete=complete)
            else:
                super().validate_phase(phase, state, complete=complete)

        def advance(self, phase, bound, state):
            if phase == 'forward-guards':
                return module.advance_shared_pool_guards(bindings=bound, api=guards, state_dir=state,
                    qualify=lambda: self.qualify(bound, _read(state.parent / 'operation.json'), recovering=False))
            return super().advance(phase, bound, state)

    api = ConnectedGuardAPI(parent)
    api.block = 'forward-drain'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    first, second = (target.namespace for target in bindings.migration.guards)
    model.busy = second
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    assert model.held == {first} and model.acquires == [first, second]
    assert api.events == []
    # Disappearing evidence during a pre-dispatch qualification must not be
    # recreated by a later save. This is a separate controlled fault scenario.
    manager = args['runtime'].database.manager.retained.request.retained
    child = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff/forward-guards/guards.json'
    checkpoint = child.read_bytes()
    second_id = str(bindings.migration.guards[1].participant_id)
    def lose_guard_receipt():
        if child.exists() and _read(child)['guards'][second_id] == 'acquire_intent':
            api.guard_qualification_hook = None
            child.unlink()  # Only this test's temporary child journal.
    api.guard_qualification_hook = lose_guard_receipt
    model.busy = None
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.acquires == [first, second] and model.held == {first}
    assert not child.exists()
    # Restore the controlled fixture for the independent unknown-reply scenario;
    # production never rewinds an acquisition intent this way.
    child.write_bytes(checkpoint)
    child.chmod(0o600)
    model.busy, model.uncertain = None, second
    for _ in range(2):
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
    assert model.acquires == [first, second, second]
    assert api.events == []
    # A late committed acquisition is observed, not dispatched again.
    model.held.add(second)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-drain'
    assert model.acquires == [first, second, second]
    before = list(model.acquires)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-drain'
    assert model.acquires == before


@pytest.mark.timeout(420)
@pytest.mark.parametrize('direction', ['forward', 'recovery'])
def test_shared_parent_resumes_before_and_after_each_local_child_preparation(shared_operation, direction):
    import json

    from scripts.ops import nebius_shared_pool_operation as module

    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    phases, final = module.FORWARD_PHASES, 'shared_pool_handoff_complete'
    if direction == 'recovery':
        api.block = 'forward-wire'
        assert module.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        kwargs['action'] = 'recover'
        phases, final = module.RECOVERY_PHASES, 'staging_restored_dev_closed'
    prefix = list(api.events)
    for index, phase in enumerate(phases):
        for boundary in ('before', 'directory', 'after'):
            api.prepare_failure = (phase, boundary)
            with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
                module.run_shared_pool_operation(**kwargs)
            record = json.loads((state / 'operation.json').read_bytes())
            assert record[direction][phase]['status'] == 'prepared'
            assert (state / phase / 'receipt.json').exists() is (boundary == 'after')
            assert api.events == prefix + list(phases[:index])
    api.prepare_failure = None
    assert module.run_shared_pool_operation(**kwargs)['status'] == final
    assert api.events == prefix + list(phases)


@pytest.mark.timeout(420)
def test_shared_parent_anchors_all_phases_and_never_replays_completed_effects(shared_operation):
    import importlib.util
    import json

    name = 'scripts.ops.nebius_shared_pool_operation'
    assert importlib.util.find_spec(name) is not None, 'shared handoff phase controller is missing'
    module = __import__(name, fromlist=['run_shared_pool_operation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    marker = Path(manager.operation['anchor_dir']) / 'shared-pool-handoff.json'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    before = {path: path.read_bytes() for path in bindings.plan.history}
    assert module.run_shared_pool_operation(**kwargs, action='preflight')['status'] == 'shared_pool_preflight_qualified'
    assert api.events == [] and not state.exists() and not marker.exists()
    phases = api.supported_phases
    api.supported_phases -= {'recovery-restore-workloads'}
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    assert api.events == [] and not state.exists() and not marker.exists()
    api.supported_phases = phases
    api.uncertain = 'forward-wire'
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    journal = json.loads((state / 'operation.json').read_bytes())
    assert journal['forward']['forward-wire']['status'] == 'started'
    assert journal['forward']['forward-start']['status'] == 'prepared'
    assert marker.exists()
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'shared_pool_handoff_complete'
    assert api.events == [
        'forward-guards', 'forward-drain', 'forward-stop', 'forward-roles', 'forward-database',
        'forward-material', 'forward-wire', 'forward-start', 'forward-build-policy',
        'forward-gateway', 'forward-activate', 'forward-verify',
    ]
    writes = list(api.events)
    preflights = api.preflights
    assert module.run_shared_pool_operation(**kwargs, action='install')['admission_open'] is True
    qualified = module.run_shared_pool_operation(**kwargs, action='preflight')
    assert qualified['status'] == 'shared_pool_preflight_qualified'
    assert qualified['admission_open'] is True and qualified['writer_migration_complete'] is True
    assert api.events == writes and api.preflights == preflights
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='recover')
    child = state / 'forward-wire/receipt.json'
    raw = child.read_bytes()
    try:
        child.write_bytes(b'{}')
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            module.run_shared_pool_operation(**kwargs, action='install')
        assert api.events == writes
    finally:
        child.write_bytes(raw)
        child.chmod(0o600)
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.timeout(420)
def test_normal_release_predecessor_requires_completed_shared_history_without_live_effects(shared_operation):
    import hashlib

    from scripts.ops import nebius_shared_pool_operation as module

    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    anchor = Path(manager.operation['anchor_dir'])
    marker = anchor / 'shared-pool-handoff.json'
    api = SharedPhaseAPI(module)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    assert not state.exists() and not marker.exists() and not api.events

    api.block = 'forward-wire'
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'pending'
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    api.block = None
    assert module.run_shared_pool_operation(**kwargs, action='install')['status'] == 'shared_pool_handoff_complete'
    effects = (list(api.events), list(api.qualifications), api.preflights)
    proof = module.load_completed_shared_pool_operation(**kwargs)
    assert proof.migration == bindings.migration and proof.state_dir == state
    assert bindings.plan.history.items() <= proof.history.items()
    assert proof.history[marker] == hashlib.sha256(marker.read_bytes()).hexdigest()
    assert proof.history[state / 'operation.json'] == hashlib.sha256((state / 'operation.json').read_bytes()).hexdigest()
    for phase in module.FORWARD_PHASES:
        receipt = state / phase / 'receipt.json'
        assert proof.history[receipt] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert module.load_completed_shared_pool_operation(**kwargs) == proof
    assert effects == (api.events, api.qualifications, api.preflights)

    # Consume the actual completed registration in the normal-release renderer.
    # Kubernetes current state is projected from this handoff's real targets;
    # completion history alone is not presented as an observation of live Pods.
    from scripts.ops.nebius_pool_participant_release import prepare_staging_pool_release

    from loom.nebius_platform_render import build_platform

    attachment = bindings.plan.staging.attachment
    previous_files = build_platform(attachment.config, proof.migration.registration.candidate,
        attachment.runtime_profile.model_dump(mode='json'), proof.migration.registration.spec.profiles.image_admission_keyring,
        repo_root=ROOT)
    current = {_key(row): copy.deepcopy(row) for rows in previous_files.values() for row in rows}
    for row in current.values():
        row['metadata'].update(uid=str(uuid4()), resourceVersion='899')
    # Handoff observation covers selected processes/authority, not the complete
    # rollout inventory (notably ServiceAccounts). Preserve its known identities
    # within this synthetic full Kubernetes readback.
    current.update({_key(row): copy.deepcopy(row) for row in attachment.resources})
    for key, desired in {**bindings.plan.staging_targets, **bindings.plan.staging_role_targets}.items():
        observed = current[key]
        current[key] = copy.deepcopy(desired)
        current[key]['metadata'].update(uid=observed['metadata']['uid'], resourceVersion='900')
        if current[key]['kind'] == 'Deployment':
            current[key]['spec']['replicas'] = 1
    candidate = copy.deepcopy(proof.migration.registration.candidate)
    candidate['candidate_sha'] = 'd' * 40
    runtime_profile = attachment.runtime_profile.model_copy(update={'candidate_sha': candidate['candidate_sha']})
    release = prepare_staging_pool_release(previous=proof.migration, current=tuple(current.values()),
        config=attachment.config, candidate=candidate, runtime_profile=runtime_profile, operation_id=uuid4(),
        management_origin='https://' + args['runtime'].database.manager.deployment.public_host, repo_root=ROOT)
    assert release.refresh.before == proof.migration.registration.spec
    assert release.refresh.after.policy_revision == proof.migration.registration.spec.policy_revision + 1
    before_dev, = (row for row in proof.migration.registration.spec.participants if row.environment_class == 'development')
    after_dev, = (row for row in release.refresh.after.participants if row.environment_class == 'development')
    assert after_dev == before_dev

    for path in (marker, state / 'operation.json', state / 'forward-wire' / 'receipt.json'):
        original = path.read_bytes()
        try:
            path.write_bytes(b'{}')
            with pytest.raises(ValueError, match='completed shared pool history unavailable'):
                module.load_completed_shared_pool_operation(**kwargs)
        finally:
            path.write_bytes(original)
    recovery = anchor / 'shared-pool-recovery.json'
    recovery.write_bytes(b'{}')
    recovery.chmod(0o600)
    with pytest.raises(ValueError, match='completed shared pool history unavailable'):
        module.load_completed_shared_pool_operation(**kwargs)
    assert effects == (api.events, api.qualifications, api.preflights)


@pytest.mark.timeout(420)
def test_shared_parent_recovery_freezes_forward_history_and_is_terminal(shared_operation):
    import importlib.util
    import json

    name = 'scripts.ops.nebius_shared_pool_operation'
    assert importlib.util.find_spec(name) is not None, 'shared handoff recovery controller is missing'
    module = __import__(name, fromlist=['run_shared_pool_operation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    anchor = Path(manager.operation['anchor_dir'])
    api = SharedPhaseAPI(module)
    api.block = 'forward-wire'
    kwargs = {**args, 'bindings': bindings, 'api': api}
    before = {path: path.read_bytes() for path in bindings.plan.history}
    result = module.run_shared_pool_operation(**kwargs, action='install')
    assert result['status'] == 'pending' and result['phase'] == 'forward-wire'
    forward = json.loads((state / 'operation.json').read_bytes())['forward']
    api.uncertain = 'recovery-restore-workloads'
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='recover')
    assert (anchor / 'shared-pool-recovery.json').is_file()
    assert json.loads((state / 'operation.json').read_bytes())['forward'] == forward
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        module.run_shared_pool_operation(**kwargs, action='install')
    # Cancelling must freeze the pending child's bytes too, not only the
    # completed-child hashes. This altered receipt is individually well formed.
    pending = state / 'forward-wire/receipt.json'
    raw = pending.read_bytes()
    writes = list(api.events)
    try:
        pending.write_text(json.dumps({'phase': 'forward-wire', 'status': 'applied'}))
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            module.run_shared_pool_operation(**kwargs, action='recover')
        assert api.events == writes
    finally:
        pending.write_bytes(raw)
        pending.chmod(0o600)
    result = module.run_shared_pool_operation(**kwargs, action='recover')
    assert result['status'] == 'staging_restored_dev_closed' and result['admission_open'] is False
    assert result['writer_migration_complete'] is False
    assert [phase for phase in api.events if phase.startswith('recovery-')] == [
        'recovery-fence', 'recovery-drain', 'recovery-stop', 'recovery-revoke',
        'recovery-restore-workloads', 'recovery-restore-roles', 'recovery-restart',
        'recovery-reopen', 'recovery-verify',
    ]
    writes = list(api.events)
    assert module.run_shared_pool_operation(**kwargs, action='recover')['status'] == 'staging_restored_dev_closed'
    assert api.events == writes
    assert json.loads((state / 'operation.json').read_bytes())['forward'] == forward
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.timeout(420)
@pytest.mark.parametrize('connected_parent', [False, True], ids=['adapter', 'parent'])
def test_shared_parent_connects_current_staging_workload_transitions_without_replaying_unknown_patch(
        shared_operation, connected_parent, tmp_path, monkeypatch):
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as concrete
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_workloads'
    assert importlib.util.find_spec(name) is not None, 'shared current-staging workload child is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingWorkloads'])
    args, bindings = shared_operation
    runtime = args['runtime']
    original = copy.deepcopy(bindings.plan.staging.originals)
    rows, documents, writes = copy.deepcopy(original), {}, []
    mode = {'lose_reply': True, 'lose_read': False, 'delete_before_write': True, 'deleted_receipt': None}
    phases = {'forward-stop', 'forward-wire', 'forward-start'}

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            namespace = meta['namespace']
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'),
                    ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + namespace + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    refresh()

    def handle(message):
        path = message.url.path
        assert path in documents, 'workload child must not operate on other resources'
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated first readback loss')
            return httpx.Response(200, json=documents[path])
        assert message.method == 'PATCH'
        current = copy.deepcopy(documents[path])
        key = _key(current)
        changes = json.loads(message.content)
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': original[key]['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': current['spec']},
        ]
        for change in changes:
            target, field = (current['metadata'], change['path'].rsplit('/', 1)[-1]) if change['path'].startswith('/metadata/') else (current, 'spec')
            if change['op'] == 'test':
                assert target[field] == change['value']
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if message.url.params.get('dryRun') == 'All':
            return httpx.Response(200, json=current)
        assert not message.url.query
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        current['metadata']['generation'] += 1
        rows[key] = current
        writes.append(key)
        refresh()
        if mode['lose_reply']:
            mode['lose_reply'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated lost committed PATCH response')
        return httpx.Response(200, json=documents[path])

    with ExitStack() as stack:
        connection = None
        class ConnectedWorkloadsAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, state):
                if phase in phases:
                    if connection is not None:
                        connection.prepare(phase, bound, state)
                    else:
                        module.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=state)
                else:
                    super().prepare(phase, bound, state)

            def validate_prepared_phase(self, phase, state):
                if phase in phases:
                    if connection is not None:
                        connection.validate_prepared_phase(phase, state)
                    else:
                        module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state,
                            complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, state)

            def validate_phase(self, phase, state, *, complete):
                if phase in phases:
                    if connection is not None:
                        connection.validate_phase(phase, state, complete=complete)
                    else:
                        module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state, complete=complete)
                else:
                    super().validate_phase(phase, state, complete=complete)

            def advance(self, phase, bound, state):
                if phase not in phases:
                    return super().advance(phase, bound, state)
                if connection is not None:
                    return connection.advance(phase, bound, state)
                def qualify_runtime(request, child_phase, writing):
                    assert request == runtime and child_phase == phase
                    if writing and mode['delete_before_write']:
                        mode['delete_before_write'] = False
                        path = state / 'transition.json'
                        mode['deleted_receipt'] = path.read_bytes()
                        path.unlink()  # Fault injection in this test's own temporary child history.
                api = stack.enter_context(module.HTTPSSharedStagingWorkloads(bindings=bound, phase=phase,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify_runtime, observe_ready=lambda *_: True))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_staging_workloads(bindings=bound, phase=phase, api=api, state_dir=state)

        api = ConnectedWorkloadsAPI(parent)
        if connected_parent:
            # Keep the actual connected parent, child construction, phase
            # preparation/validation, CAS dispatch and readiness adapter. Only
            # earlier effects and separately exercised external observations
            # are boundaries in this workload-route regression.
            operator = tmp_path / 'workload-operator'
            operator.write_bytes(b'private-workload-test')
            operator.chmod(0o600)
            context = ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
            connection = stack.enter_context(concrete.HTTPSSharedPoolOperationAPI(bindings=bindings,
                observation=args['observation'], repo_root=ROOT,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
                token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}))
            registry = connection._registry()
            for phase in parent.FORWARD_PHASES:
                if phase not in phases:
                    def validate(*, state_dir, complete, bindings, prepared=False):
                        if prepared:
                            api.validate_prepared_phase(state_dir.name, state_dir)
                            return {'guards': {'fixture': 'prepared'}}
                        api.validate_phase(state_dir.name, state_dir, complete=complete)
                    registry[phase] = ((lambda **_: pytest.fail('earlier phase preparation is a fixture boundary'), validate),)
            monkeypatch.setattr(connection, '_registry', lambda: registry)
            monkeypatch.setattr(connection, '_permissions', lambda *_, **__: None)

            async def publication(**options):
                assert options['request'] == runtime

            async def provider(**options):
                assert options['runtime'] == runtime
                return SimpleNamespace(plan=bindings.plan)

            def processes(**options):
                assert options['bindings'] == bindings and options['state_dir'] == connection.shared_state
                return concrete.SharedPoolForwardProcessObservation(bindings, 'closed', {}, 'revoked', (), {}, bindings.input_digest)

            monkeypatch.setattr(concrete, 'qualify_runtime_publication', publication)
            monkeypatch.setattr(concrete, 'observe_shared_pool_provider', provider)
            monkeypatch.setattr(concrete, 'observe_shared_pool_forward_writers', lambda **_: concrete.SharedPoolWriterObservation(
                bindings.plan, '501', {}, {}, bindings.input_digest))
            monkeypatch.setattr(concrete, 'observe_shared_pool_forward_processes', processes)
            original_qualify = connection._child_qualify

            def qualify(request, phase, writing):
                original_qualify(request, phase, writing)
                if writing and mode['delete_before_write']:
                    mode['delete_before_write'] = False
                    path = connection.shared_state / phase / 'transition.json'
                    mode['deleted_receipt'] = path.read_bytes()
                    path.unlink()

            monkeypatch.setattr(connection, '_child_qualify', qualify)
            child_type = module.HTTPSSharedStagingWorkloads

            def child(**options):
                result = child_type(**options)
                client_type = type(result.client)
                result.client.close()
                result.client = client_type(base_url=result.api_server, transport=httpx.MockTransport(handle))
                return result

            monkeypatch.setattr(module, 'HTTPSSharedStagingWorkloads', child)
        api.block = 'forward-build-policy'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        manager = runtime.database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == [], 'lost child evidence must prevent a mutating PATCH'
        missing = state / 'forward-stop/transition.json'
        assert not missing.exists() and mode['deleted_receipt'] is not None
        missing.write_bytes(mode['deleted_receipt'])
        missing.chmod(0o600)
        with pytest.raises(ValueError, match='shared pool operation unconfirmed') as interrupted:
            parent.run_shared_pool_operation(**kwargs)
        if len(writes) != 1:
            import traceback

            frames, cause = [], interrupted.value
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('workload phase failed before intended transport fault: ' + ', '.join(frames), pytrace=False)
        record = json.loads((state / 'operation.json').read_bytes())
        assert record['forward']['forward-stop']['status'] == 'started'
        choices = module.shared_forward_staging_options(bindings=bindings, state_dir=state)
        assert len(choices[writes[0]]) == 2  # Both original and committed target, never a guessed outcome.
        assert tuple(_stable(row) for row in choices[writes[0]]) == (
            _stable(original[writes[0]]), _stable(bindings.plan.staging.stopped[writes[0]]))
        with pytest.raises(ValueError):
            module.HTTPSSharedStagingWorkloads(bindings=bindings, phase='forward-wire',
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=lambda *_: True)
        assert len(writes) == 1  # A later child cannot jump the incomplete stop phase.
        result = parent.run_shared_pool_operation(**kwargs)
        assert result['status'] == 'pending' and result['phase'] == 'forward-build-policy'
        deployments = {key for key, row in original.items() if row['kind'] == 'Deployment'}
        collector, = (key for key, row in original.items() if row['kind'] == 'CronJob')
        assert writes.count(collector) == 1  # Remains stopped; do not patch or start it again.
        assert set(writes) == set(original)
        assert all(writes.count(key) == 3 for key in deployments)
        assert rows[collector]['spec']['suspend'] is True
        for key in deployments:
            wanted = copy.deepcopy(bindings.plan.staging_targets[key])
            wanted['spec']['replicas'] = 1
            assert _stable(rows[key]) == _stable(wanted)
            assert rows[key]['metadata']['uid'] == original[key]['metadata']['uid']
        choices = module.shared_forward_staging_options(bindings=bindings, state_dir=state)
        assert all(len(options) == 1 and _stable(options[0]) == _stable(rows[key]) for key, options in choices.items())
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
        assert writes == before


@pytest.mark.timeout(420)
def test_shared_drain_checks_both_databases_and_retains_qualified_paged_backlog(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """Use real guards/history validators; saved drain is never current authority."""
    import importlib.util
    import json
    import shutil
    import ssl
    import subprocess
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
    from scripts.ops.nebius_pool_migration_guard import pool_cutover_readiness_sql
    from scripts.ops.nebius_pool_origin_history import pool_management_history_sql
    from scripts.ops.nebius_pool_startup_database import pool_startup_closed_sql
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    from loom.nebius_pool_priority import PoolWorkOriginV1
    from loom_service.pool_management.capacity import digest

    name = 'scripts.ops.nebius_shared_pool_drain'
    assert importlib.util.find_spec(name) is not None, 'shared real drain observation is missing'
    module = __import__(name, fromlist=['observe_closed_shared_pool_drain'])
    args, bindings = shared_operation
    runtime = args['runtime']
    guards, model = shared_guard_transport
    spec = bindings.migration.registration.spec
    model.held.update(target.namespace for target in bindings.migration.guards)
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    history_reports, read_history = {}, []
    backlog = {}
    for target in bindings.migration.guards:
        participant, = (row for row in spec.participants if row.participant_id == target.participant_id)
        count = 129 if participant.environment_class == 'development' else 1
        origins = tuple(PoolWorkOriginV1(data_environment_id=participant.environment_id,
            submission_id=UUID(int=index + 1), kind='environment', application=None) for index in range(count))
        rows = [{'key': 'batch:' + str(origin.submission_id), 'origin': origin.model_dump(mode='json'),
            'source_matches': True} for origin in origins]
        backlog[str(participant.participant_id)] = count
        logical_pool, = (row['value'] for row in target.controller['spec']['template']['spec']['containers'][0]['env']
            if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID')
        for offset in range(0, count, 128):
            after = rows[offset - 1]['key'] if offset else None
            query = pool_cutover_readiness_sql(owner=str(spec.operation_id),
                candidate=bindings.migration.registration.candidate['candidate_sha'], logical_pool_id=logical_pool, after=after)
            model.readiness_pages[target.namespace, query] = {'status': 'observed', 'schema_revision': '0175', 'rows': rows[offset:offset + 128]}
            page = origins[offset:offset + 128]
            history_reports[pool_management_history_sql(page)] = {'schema': 'loom.pool-management-history.v1',
                'schema_revision': '0175', 'read_only': True, 'rows': [
                    {'ordinal': index, 'origin': origin.model_dump(mode='json'), 'application': None, 'operation': None}
                    for index, origin in enumerate(page, start=1)]}
    _, documents, handler = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
    populate_database_backends(documents)
    monkeypatch.setattr(shutil, 'which', lambda _: '/usr/bin/kubectl')
    guard_run = subprocess.run
    mode = {'history_invalid': True, 'reopen_guard': False}
    closed_reads = []
    with HTTPSDevelopmentRuntimeProbes(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='private-test-kubernetes-token') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(handler))

        def run(argv, **kwargs):
            if argv[:len(guards.prefix)] == guards.prefix:
                return guard_run(argv, **kwargs)
            assert argv[:len(probes.prefix)] == probes.prefix
            command = argv[len(probes.prefix):]
            assert command[:17] == ['exec', '-n', manager.binding.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres',
                '--', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
            query = command[-1]
            if query == pool_startup_closed_sql(spec):
                closed_reads.append(query)
                value = {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'qualified': True}
            else:
                assert query in history_reports, 'only fixed read-only origin history belongs on the manager DB'
                read_history.append(query)
                value = copy.deepcopy(history_reports[query])
                if mode['history_invalid']:
                    value['rows'][0]['origin']['data_environment_id'] = str(uuid4())
                if mode['reopen_guard']:
                    mode['reopen_guard'] = False
                    model.held.remove(bindings.migration.guards[0].namespace)
            payload = json.dumps(value, indent=32).encode()
            if len(value.get('rows', [])) == 128:
                assert 65536 < len(payload) < 4 * 1024**2  # Legal JSON history exceeds the small process-probe cap.
            return SimpleNamespace(returncode=0, stdout=payload, stderr=b'')

        monkeypatch.setattr(subprocess, 'run', run)

        class ConnectedDrainAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, child):
                if phase == 'forward-drain':
                    module.prepare_shared_pool_drain(bindings=bound, state_dir=child)
                else:
                    super().prepare(phase, bound, child)

            def validate_prepared_phase(self, phase, child):
                if phase == 'forward-drain':
                    module.validate_shared_pool_drain(bindings=bindings, state_dir=child, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, child)

            def validate_phase(self, phase, child, *, complete):
                if phase == 'forward-drain':
                    module.validate_shared_pool_drain(bindings=bindings, state_dir=child, complete=complete)
                else:
                    super().validate_phase(phase, child, complete=complete)

            def advance(self, phase, bound, child):
                if phase != 'forward-drain':
                    return super().advance(phase, bound, child)
                return module.advance_shared_pool_drain(bindings=bound, guards=guards, development=probes,
                    state_dir=child, qualify=lambda: None)

        api = ConnectedDrainAPI(parent)
        api.block = 'forward-stop'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert read_history and closed_reads
        mode['history_invalid'] = False
        mode['reopen_guard'] = True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        model.held.update(target.namespace for target in bindings.migration.guards)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'
        record = json.loads((state / 'forward-drain/drain.json').read_bytes())
        assert record['pending_origins'] == backlog
        assert set(model.readiness_calls) == {target.namespace for target in bindings.migration.guards}
        assert set(read_history) == set(history_reports)  # Includes the 129th queued source, not only page one.
        before = (state / 'forward-drain/drain.json').read_bytes()
        for target in bindings.migration.guards:
            model.readiness_failure = target.namespace
            with pytest.raises(ValueError):
                module.observe_closed_shared_pool_drain(bindings=bindings, guards=guards, development=probes,
                    state_dir=state, qualify=lambda: None)
        assert (state / 'forward-drain/drain.json').read_bytes() == before
        assert model.runtime_stages == model.acquires == []


@pytest.mark.timeout(420)
def test_shared_material_child_preserves_dev_and_never_replays_uncertain_creates(shared_operation):
    """Real fixed stage engine/HTTPS transport, with only remote effects doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot

    name = 'scripts.ops.nebius_shared_pool_material'
    assert importlib.util.find_spec(name) is not None, 'shared staging material child is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingMaterial'])
    args, bindings = shared_operation
    runtime = args['runtime']
    fixed = {**bindings.plan.staging_material, **bindings.plan.staging_reader_authority}
    assert sorted(row['kind'] for row in fixed.values()) == ['ClusterRole', 'ClusterRoleBinding', 'Secret', 'Secret']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    directory = state / 'forward-material'
    receipt, initialized, dispatched = (directory / filename for filename in ('stage.json', 'material.json', 'dispatch.json'))
    stored, posts, defaults = {}, [], []
    mode = {'prepare_loss': True, 'default_loss': True, 'erase_stage': True,
        'saved_stage': None, 'reply_loss': True, 'read_loss': False}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def handle(message):
        if message.method == 'GET':
            assert message.url.path in paths, 'material stage must never read or rewrite installed dev/manager resources'
            key = paths[message.url.path]
            if mode['read_loss']:
                mode['read_loss'] = False
                raise httpx.ReadError('simulated lost create readback')
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        document = json.loads(message.content)
        key = _key(document)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        actual = copy.deepcopy(document)
        actual['metadata'].update(uid=str(uuid4()), resourceVersion='1')
        if message.url.params.get('dryRun') == 'All':
            defaults.append(key)
            if mode['default_loss']:
                mode['default_loss'] = False
                raise httpx.ReadError('simulated lost effect-free default preview')
            return httpx.Response(201, json=actual)
        assert not message.url.query and key not in stored
        assert json.loads(initialized.read_bytes())['dispatch'] is True and dispatched.is_file()
        assert json.loads(receipt.read_bytes())['resources'][key]['status'] == 'create_intent'
        posts.append(key)
        stored[key] = actual
        if mode['reply_loss']:
            mode['reply_loss'], mode['read_loss'] = False, True
            raise httpx.ReadError('simulated lost committed create reply')
        return httpx.Response(201, json=actual)

    with ExitStack() as stack:
        class ConnectedMaterialAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, child):
                if phase == 'forward-material':
                    module.prepare_shared_staging_material(bindings=bound, state_dir=child)
                    if mode['prepare_loss']:
                        mode['prepare_loss'] = False
                        raise TimeoutError('simulated interruption after local preparation')
                else:
                    super().prepare(phase, bound, child)

            def validate_prepared_phase(self, phase, child):
                if phase == 'forward-material':
                    module.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, child)

            def validate_phase(self, phase, child, *, complete):
                if phase == 'forward-material':
                    module.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=complete)
                else:
                    super().validate_phase(phase, child, complete=complete)

            def transport(self, bound):
                def qualify_runtime(request, phase, writing):
                    assert request == runtime and phase == 'forward-material'
                    if writing and mode['erase_stage'] and json.loads(initialized.read_bytes())['dispatch']:
                        mode['erase_stage'] = False
                        mode['saved_stage'] = receipt.read_bytes()
                        receipt.unlink()  # Fault injection in this test's private child evidence.
                api = stack.enter_context(module.HTTPSSharedStagingMaterial(bindings=bound,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify_runtime))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return api

            def advance(self, phase, bound, child):
                if phase != 'forward-material':
                    return super().advance(phase, bound, child)
                return module.advance_shared_staging_material(bindings=bound, api=self.transport(bound), state_dir=child)

        api = ConnectedMaterialAPI(parent)
        api.block = 'forward-wire'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert initialized.is_file() and not receipt.exists() and not dispatched.exists()
        assert defaults == posts == []  # Preparing the child cannot even invoke a dry-run.
        first = next(iter(fixed))
        stored[first] = copy.deepcopy(fixed[first])
        stored[first]['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert defaults == posts == []  # Existing material is never adopted.
        stored.clear()  # Reset only this controlled test transport's foreign-resource scenario.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert defaults == [first] and posts == [] and not receipt.exists()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == [] and not receipt.exists() and mode['saved_stage'] is not None
        assert json.loads(initialized.read_bytes())['dispatch'] is True and dispatched.is_file()
        receipt.write_bytes(mode['saved_stage'])
        receipt.chmod(0o600)
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == []  # A saved create intent without its object cannot be replayed.
        # Rewind only this controlled pre-effect fixture for the independent
        # lost-response scenario. Production cannot reset dispatch history.
        prepared = json.loads(mode['saved_stage'])
        for item in prepared['resources'].values():
            item['status'] = 'prepared'
        receipt.write_text(json.dumps(prepared))
        preparing = json.loads(initialized.read_bytes())
        preparing['dispatch'] = False
        initialized.write_text(json.dumps(preparing))
        dispatched.unlink()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert posts == [first] and set(stored) == {first}
        for path in (initialized, dispatched, receipt):
            content = path.read_bytes()
            path.unlink()
            with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
                parent.run_shared_pool_operation(**kwargs)
            assert posts == [first]
            path.write_bytes(content)
            path.chmod(0o600)
        with pytest.raises(ValueError):
            foreign = next(row for row in bindings.plan.development.runtime.resources.values() if row['kind'] == 'Secret')
            api.transport(bindings).create_resource(foreign)
        assert posts == [first]
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        assert posts == list(fixed) and set(stored) == set(fixed)
        for key, row in stored.items():
            actual = _snapshot(row)
            actual['metadata']['annotations'].pop('loom.nebius/management-stage-operation')
            if not actual['metadata']['annotations']:
                actual['metadata'].pop('annotations')
            assert actual == fixed[key]
        reader, = (row for row in stored.values() if row['kind'] == 'ClusterRole')
        assert reader['rules'][0]['verbs'] == ['get'] and reader['rules'][0]['resources'] == ['namespaces']
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-wire'
        assert posts == list(fixed)


@pytest.mark.timeout(420)
def test_shared_database_child_stages_only_staging_and_reads_unknown_commits(shared_operation, shared_guard_transport):
    """Real bound DB adapter and parent; no replay of dev's completed SQL stage."""
    import importlib.util
    import json

    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_database'
    assert importlib.util.find_spec(name) is not None, 'shared staging database child is missing'
    module = __import__(name, fromlist=['advance_shared_staging_database'])
    args, bindings = shared_operation
    database, model = shared_guard_transport
    model.held.update(target.namespace for target in bindings.migration.guards)
    staging = bindings.plan.staging.attachment.guard
    dev, = (target for target in bindings.migration.guards if target.participant_id != staging.participant_id)
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    receipt = state / 'forward-database/database.json'
    failure = {'delete': True, 'saved': None}
    # The DB-bound path must still work with the captured controller stopped.
    controller = model.documents['/apis/apps/v1/namespaces/' + staging.namespace + '/deployments/loom-control-plane']
    controller['spec']['replicas'] = 0

    class ConnectedDatabaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            if phase == 'forward-database':
                module.prepare_shared_staging_database(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-database':
                module.validate_shared_staging_database(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-database':
                module.validate_shared_staging_database(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase != 'forward-database':
                return super().advance(phase, bound, directory)

            def qualify():
                if failure['delete'] and json.loads(receipt.read_bytes())['status'] == 'intent':
                    failure['delete'] = False
                    failure['saved'] = receipt.read_bytes()
                    receipt.unlink()  # This test's private fault injection, never production repair.

            return module.advance_shared_staging_database(bindings=bound, api=database, state_dir=directory, qualify=qualify)

    api = ConnectedDatabaseAPI(parent)
    api.block = 'forward-material'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert not receipt.exists() and model.runtime_stages == [] and failure['saved'] is not None
    receipt.write_bytes(failure['saved'])
    receipt.chmod(0o600)
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.runtime_stages == []  # Missing outcome of saved intent never permits another stage.
    # Controlled fixture rewind to exercise an independent unknown-commit fault.
    # Production cannot reset an uncertain intent or recreate missing evidence.
    prepared = json.loads(failure['saved'])
    prepared['status'] = 'prepared'
    receipt.write_text(json.dumps(prepared))
    model.runtime_uncertain = True
    with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
        parent.run_shared_pool_operation(**kwargs)
    assert model.runtime_stages == [staging.namespace]
    assert json.loads(receipt.read_bytes())['status'] == 'intent'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    assert model.runtime_stages == [staging.namespace] and set(model.runtime_reads) == {staging.namespace}
    assert json.loads(receipt.read_bytes())['status'] == 'staged'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    assert model.runtime_stages == [staging.namespace]
    assert dev.namespace not in model.runtime_roles
    assert model.acquires == []  # No registration, guard replay/release, or controller exec.


@pytest.mark.timeout(420)
def test_shared_parent_restricts_current_roles_once_and_checks_foreign_effective_grants(shared_operation):
    """Catch repeated unknown PATCHes and false fencing from only two Role reads."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_management_switch import _stable

    name = 'scripts.ops.nebius_shared_pool_roles'
    assert importlib.util.find_spec(name) is not None, 'connected shared Role fencing is missing'
    module = __import__(name, fromlist=['HTTPSSharedStagingRoles'])
    args, bindings = shared_operation
    originals = bindings.plan.staging_role_originals
    roles = copy.deepcopy(originals)
    writes, attempts, reviews, qualifications = [], [], [], []
    mode = {'delete_receipt': True, 'delete_at_intent': False, 'saved_receipt': None, 'reject': True, 'lose_reply': True,
        'lose_read': False, 'extra_grant': True, 'incomplete': False, 'change_binding': False}
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    receipt = state / 'forward-roles/roles.json'
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    rolebindings = [copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] == 'RoleBinding']
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'name': 'foreign-reader', 'namespace': 'outside-pool', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'subjects': [{'kind': 'Group', 'apiGroup': 'rbac.authorization.k8s.io', 'name': 'system:authenticated'}],
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign-reader'}}
    rolebindings.append(foreign)
    original_bindings = copy.deepcopy(rolebindings)

    def handle(message):
        path = message.url.path
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            assert namespace in namespaces
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1',
                    'labels': {'loom.nebius/management-installation': identity.installation_id,
                        'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path in ('/apis/rbac.authorization.k8s.io/v1/rolebindings',
                '/apis/rbac.authorization.k8s.io/v1/clusterrolebindings'):
            assert message.method == 'GET' and 'Impersonate-User' not in message.headers
            kind = 'RoleBinding' if path.endswith('/rolebindings') else 'ClusterRoleBinding'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': kind + 'List', 'metadata': {'resourceVersion': '71'},
                'items': copy.deepcopy(rolebindings) if kind == 'RoleBinding' else []})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            account = message.headers['Impersonate-User']
            account_namespace = account.split(':')[2]
            assert set(message.headers.get_list('Impersonate-Group', split_commas=True)) == {
                'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'}
            namespace = json.loads(message.content)['spec']['namespace']
            reviews.append((account, namespace))
            rules = [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'list', 'watch']}]
            if namespace == 'outside-pool' and mode['extra_grant']:
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']})
            if mode['change_binding']:
                mode['change_binding'] = False
                foreign['metadata']['resourceVersion'] = '2'
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1',
                'kind': 'SelfSubjectRulesReview', 'spec': {}, 'status': {'incomplete': mode['incomplete'],
                    'resourceRules': rules, 'nonResourceRules': []}})
        assert 'Impersonate-User' not in message.headers
        parts = path.split('/')
        assert parts[:5] == ['', 'apis', 'rbac.authorization.k8s.io', 'v1', 'namespaces']
        key = 'Role:' + parts[5] + ':' + parts[7]
        assert parts[6] == 'roles' and key in originals
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated lost first Role readback')
            return httpx.Response(200, json=roles[key])
        assert message.method == 'PATCH' and not message.url.query
        changes = json.loads(message.content)
        current = copy.deepcopy(roles[key])
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': originals[key]['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/rules', 'value': current['rules']},
        ]
        assert [(row['op'], row['path']) for row in changes[3:]] == [
            ('add', '/metadata/annotations'), ('replace', '/rules')]
        assert json.loads(receipt.read_bytes())['roles'][key] == 'intent'
        attempts.append(key)
        if mode['reject']:
            mode['reject'] = False
            return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status',
                'status': 'Failure', 'code': 409, 'reason': 'Conflict'})
        current['metadata']['annotations'] = changes[3]['value']
        current['rules'] = changes[4]['value']
        current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
        roles[key] = current
        writes.append(key)
        if mode['lose_reply']:
            mode['lose_reply'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated lost committed Role PATCH reply')
        return httpx.Response(200, json=current)

    with ExitStack() as stack:
        class ConnectedRolesAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, directory):
                if phase == 'forward-roles':
                    module.prepare_shared_staging_roles(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-roles':
                    module.validate_shared_staging_roles(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-roles':
                    module.validate_shared_staging_roles(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase != 'forward-roles':
                    return super().advance(phase, bound, directory)

                def qualify(current, writing):
                    assert current == bindings
                    qualifications.append(writing)
                    if writing and (mode['delete_receipt'] or (mode['delete_at_intent']
                            and 'intent' in json.loads(receipt.read_bytes())['roles'].values())):
                        mode['delete_receipt'], mode['delete_at_intent'] = False, False
                        mode['saved_receipt'] = receipt.read_bytes()
                        receipt.unlink()  # Fault injection only in this test's private history.

                api = stack.enter_context(module.HTTPSSharedStagingRoles(bindings=bound,
                    api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=qualify))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_staging_roles(bindings=bound, api=api, state_dir=directory)

        api = ConnectedRolesAPI(parent)
        api.block = 'forward-database'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts and not receipt.exists() and mode['saved_receipt'] is not None
        # Restore this controlled pre-dispatch fixture to test other faults.
        # Production never recreates lost started history.
        prepared_checkpoint = mode['saved_receipt']
        receipt.write_bytes(prepared_checkpoint)
        receipt.chmod(0o600)
        mode['delete_at_intent'] = True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts and not receipt.exists()
        receipt.write_bytes(mode['saved_receipt'])
        receipt.chmod(0o600)
        # Even with restored intent, an unobserved effect is never redispatched.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts
        receipt.write_bytes(prepared_checkpoint)  # Controlled fixture rewind only.
        namespaces['kube-system'] = str(uuid4())
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert not attempts
        namespaces['kube-system'] = identity.kube_system_uid
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
        assert len(attempts) == 1 and writes == []  # Definite rejection permits a later retry.
        first = sorted(roles)[0]
        roles[first]['rules'].append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert len(attempts) == 1 and writes == []
        roles[first]['rules'].pop()
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert len(writes) == 1
        options = module.shared_forward_role_options(bindings=bindings, state_dir=state)
        assert len(options[writes[0]]) == 2  # Unknown commit retains both exact possibilities.
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert set(writes) == set(originals) and len(writes) == 2
        assert len(attempts) == 3  # No second dispatch of the unknown committed PATCH.
        assert any(namespace == 'outside-pool' for _, namespace in reviews)
        assert rolebindings == original_bindings
        mode['extra_grant'], mode['incomplete'] = False, True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        mode['incomplete'], mode['change_binding'] = False, True
        with pytest.raises(ValueError, match='shared pool operation unconfirmed'):
            parent.run_shared_pool_operation(**kwargs)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
        assert len(attempts) == 3 and any(qualifications)
        expected_subjects = set()
        for row in bindings.plan.staging.originals.values():
            pod = (row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob'
                else row['spec']['template']['spec'])
            expected_subjects.add('system:serviceaccount:' + row['metadata']['namespace'] + ':' + pod.get('serviceAccountName', 'default'))
        assert {subject for subject, namespace in reviews if namespace == 'outside-pool'} == expected_subjects
        assert all(_stable(roles[key]) == _stable(bindings.plan.staging_role_targets[key]) for key in originals)
        assert all(roles[key]['metadata']['uid'] == originals[key]['metadata']['uid'] for key in originals)
        assert all(len(choices) == 1 for choices in module.shared_forward_role_options(bindings=bindings, state_dir=state).values())
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
        assert len(attempts) == 3


@pytest.mark.timeout(420)
def test_shared_build_activation_requires_effective_policy_and_never_repeats_unknown_patch(
        shared_operation, shared_runtime, completed_pool, handoff):
    """Missing policy enforcement must never expose the rootless-build exception."""
    import importlib
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_build_activation'
    assert importlib.util.find_spec(name) is not None, 'shared build activation is not connected'
    module = importlib.import_module(name)
    args, bindings = shared_operation
    plan = bindings.plan.development
    original = plan.build_namespace_original
    namespace_path = '/api/v1/namespaces/' + original['metadata']['name']
    documents = {namespace_path: copy.deepcopy(original)}
    documents[namespace_path]['metadata']['resourceVersion'] = '801'
    for row in plan.build_policy.values():
        plural = 'validatingadmissionpolicies' if row['kind'] == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
        path = '/apis/admissionregistration.k8s.io/v1/' + plural + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
        documents[path]['metadata'].update(generation=1, resourceVersion='802')
        if row['kind'] == 'ValidatingAdmissionPolicy':
            documents[path]['status'] = {'observedGeneration': 1, 'typeChecking': {'expressionWarnings': []}}
            policy_path = path
    registration = bindings.migration.registration.binding
    documents['/api/v1/namespaces/kube-system'] = {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': 'kube-system', 'uid': registration.kube_system_uid}}
    calls, patches = [], []
    mode = {'deny': False, 'response_lost': True, 'conflict': False}

    def handler(request):
        calls.append((request.method, request.url.path, dict(request.url.params)))
        if request.method == 'GET':
            assert not request.url.params
            return httpx.Response(200, json=copy.deepcopy(documents[request.url.path]))
        if request.method == 'POST':
            assert request.url.path == namespace_path + '/pods' and dict(request.url.params) == {'dryRun': 'All'}
            pod = json.loads(request.content)
            assert pod['kind'] == 'Pod' and pod['metadata']['namespace'] == original['metadata']['name']
            assert pod['spec']['automountServiceAccountToken'] is False
            if mode.get('flap_before_patch'):
                mode['probe_count'] = mode.get('probe_count', 0) + 1
            if not mode['deny'] or (mode.get('flap_before_patch') and mode['probe_count'] == 2):
                return httpx.Response(201, json=pod)
            policy = documents[policy_path]['metadata']['name']
            reason = 'Only sequential prepare, build and publish phases are allowed.'
            message = 'denied by policy ' + (policy if mode['deny'] != 'foreign' else 'foreign-policy') + ': ' + reason
            return httpx.Response(403, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                'reason': 'Forbidden', 'code': 403, 'message': message})
        assert request.method == 'PATCH' and request.url.path == namespace_path and not request.url.params
        body = json.loads(request.content)
        current = documents[namespace_path]
        assert body[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': original['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': current['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/metadata/labels', 'value': original['metadata']['labels']},
        ]
        assert all(item['path'] in {'/metadata/uid', '/metadata/resourceVersion', '/metadata/labels',
            '/metadata/annotations'} for item in body)
        patches.append(body)
        if mode['conflict']:
            mode['conflict'] = False
            return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                'reason': 'Conflict', 'code': 409})
        for patch in body:
            if patch['op'] in {'add', 'replace'}:
                current['metadata'][patch['path'].split('/')[-1]] = copy.deepcopy(patch['value'])
        current['metadata']['resourceVersion'] = '803'
        if mode.get('lose_policy_after_patch'):
            mode['deny'] = False
        if mode['response_lost']:
            mode['response_lost'] = False
            raise httpx.ReadTimeout('simulated response loss after namespace commit')
        return httpx.Response(200, json=copy.deepcopy(current))

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, state):
            if phase == 'forward-build-policy':
                module.prepare_shared_build_activation(bindings=bound, state_dir=state)
            else:
                super().prepare(phase, bound, state)

        def validate_prepared_phase(self, phase, state):
            if phase == 'forward-build-policy':
                module.validate_shared_build_activation(bindings=bindings, state_dir=state, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, state)

        def validate_phase(self, phase, state, *, complete):
            if phase == 'forward-build-policy':
                module.validate_shared_build_activation(bindings=bindings, state_dir=state, complete=complete)
            else:
                super().validate_phase(phase, state, complete=complete)

        def advance(self, phase, bound, state):
            if phase != 'forward-build-policy':
                return super().advance(phase, bound, state)
            with module.HTTPSSharedBuildActivation(bindings=bound,
                    api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=lambda current, writing: None) as api:
                api.client.close()
                api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(handler))
                return module.advance_shared_build_activation(bindings=bound, api=api, state_dir=state)

    api = API(parent)
    api.block = 'forward-gateway'
    kwargs = dict(**args, bindings=bindings, api=api, action='install')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == [] and documents[namespace_path]['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'
    mode['deny'] = 'foreign'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert patches == []
    mode['deny'] = True
    for damage in ('warning', 'uid', 'selector'):
        saved = copy.deepcopy(documents[policy_path])
        if damage == 'warning':
            documents[policy_path]['status']['typeChecking']['expressionWarnings'] = [{'warning': 'invalid'}]
        elif damage == 'uid':
            documents[policy_path]['metadata']['uid'] = str(uuid4())
        else:
            documents[policy_path]['spec']['matchConditions'] = [{'name': 'skip', 'expression': 'false'}]
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        documents[policy_path] = saved
        assert patches == []
    documents[policy_path]['status']['observedGeneration'] = 0
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == []
    documents[policy_path]['status']['observedGeneration'] = 1
    mode['flap_before_patch'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert patches == []  # Known loss of enforcement before dispatch remains a safe wait.
    mode['flap_before_patch'] = False
    mode['conflict'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert len(patches) == 1
    mode['lose_policy_after_patch'] = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    assert len(patches) == 2  # Completion requires fresh enforcement after the PATCH too.
    mode['deny'] = True
    api.prepare_failure = ('forward-gateway', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert len(patches) == 2  # Rejected CAS retried; lost committed response was read back.
    assert documents[namespace_path]['metadata']['labels'] == plan.build_namespace_target['metadata']['labels']
    assert any(method == 'POST' for method, _, _ in calls)
    assert not any(method in {'DELETE', 'PUT'} for method, _, _ in calls)

    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory

    # The original installer remains strict. Only the anchored shared handoff
    # may recognize this exact successor; arbitrary label changes still fail.
    runtime = args['runtime']
    _, inventory, read = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
    inventory[namespace_path] = copy.deepcopy(documents[namespace_path])
    with HTTPSDevelopmentRuntimeObserver(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir)
        assert observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        inventory[namespace_path]['metadata']['labels']['pod-security.kubernetes.io/warn'] = 'privileged'
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        inventory[namespace_path] = copy.deepcopy(documents[namespace_path])
        receipt = Path(runtime.database.manager.retained.request.retained.operation['state_dir']).parent / 'shared-pool-handoff/forward-build-policy/build-policy.json'
        saved = receipt.read_bytes()
        receipt.unlink()
        with pytest.raises(ValueError):
            observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
        receipt.write_bytes(saved)
        receipt.chmod(0o600)
    from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    populate_runtime_pods(inventory)
    with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
        component, deployment, pod = probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)
        assert component == 'controller' and deployment['metadata']['name'] == 'loom-control-plane'
        assert pod['status']['phase'] == 'Running'
    api.prepare_failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    assert len(patches) == 2


@pytest.mark.timeout(600)
def test_shared_gateway_stages_exact_grants_then_starts_once_under_closed_parent(
        shared_operation, completed_pool, shared_runtime, handoff):
    """A grant or PATCH reply loss must not start a second writer or replay effects."""
    import importlib
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_build_activation as build_child
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import (
        populate_runtime_pods,
        runtime_http_inventory,
    )

    name = 'scripts.ops.nebius_shared_pool_gateway'
    assert importlib.util.find_spec(name) is not None, 'shared gateway child is missing'
    module = importlib.import_module(name)
    args, bindings = shared_operation
    runtime, plan = args['runtime'], bindings.plan.development
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'forward-gateway'
    fixed = plan.authority
    resources = {}
    gateway = copy.deepcopy(plan.gateway_original)
    gateway['metadata'].update(resourceVersion='901', generation=1)
    gateway_path = runtime_resource_path(gateway) + '/' + gateway['metadata']['name']
    resource_paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}
    creates, patches, reviews, challenges = [], [], [], []
    documents = {}
    mode = {'lose_create': True, 'lose_patch': True, 'lose_read': False,
        'ready': False, 'extra_permission': False, 'runtime_ready': False, 'prepare_loss': True}

    def refresh():
        namespace = gateway['metadata']['namespace']
        gateway['status'] = {'observedGeneration': gateway['metadata']['generation'],
            **{field: int(mode['ready'] and gateway['spec']['replicas'] == 1)
                for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
        documents.clear()
        documents[gateway_path] = copy.deepcopy(gateway)
        for version, plural, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod')):
            path = ('/api/v1' if version == 'v1' else '/apis/' + version) + '/namespaces/' + namespace + '/' + plural
            documents[path] = {'apiVersion': version, 'kind': kind + 'List',
                'metadata': {'resourceVersion': '902'}, 'items': []}
        if mode['ready']:
            populate_runtime_pods(documents)

    refresh()

    def handle(request):
        path = request.url.path
        if request.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated committed effect readback loss')
            if path in resource_paths:
                key = resource_paths[path]
                return httpx.Response(200, json=copy.deepcopy(resources[key])) if key in resources else httpx.Response(404)
            if path in documents:
                return httpx.Response(200, json=copy.deepcopy(documents[path]))
            collection = path.rsplit('/', 1)[-1]
            assert path == '/apis/rbac.authorization.k8s.io/v1/' + collection
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[collection]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '903'},
                'items': [copy.deepcopy(row) for row in resources.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert request.method == 'POST' and not request.url.query
            assert request.headers['Impersonate-User'] == 'system:serviceaccount:loom-nebius-management-dev:loom-pool-gateway'
            namespace = json.loads(request.content)['spec']['namespace']
            reviews.append(namespace)
            rules = [copy.deepcopy(rule) for row in resources.values()
                if row['kind'] == 'ClusterRole' or (row['kind'] == 'Role' and row['metadata']['namespace'] == namespace)
                for rule in row['rules']]
            if mode['extra_permission']:
                rules.append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {},
                'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': [], 'evaluationError': ''}})
        if request.method == 'POST':
            row = json.loads(request.content)
            key = _key(row)
            assert key in fixed and path == runtime_resource_path(fixed[key])
            row['metadata'].update(uid=str(uuid4()), resourceVersion='904')
            if request.url.params.get('dryRun') == 'All':
                return httpx.Response(201, json=row)
            assert not request.url.query and key not in resources
            assert json.loads((child / 'gateway.json').read_bytes())['dispatch'] is True
            assert json.loads((child / 'authority/stage.json').read_bytes())['resources'][key]['status'] == 'create_intent'
            assert gateway['spec']['replicas'] == 0
            creates.append(key)
            resources[key] = row
            if mode['lose_create']:
                mode['lose_create'], mode['lose_read'] = False, True
                raise httpx.ReadError('simulated committed grant response loss')
            return httpx.Response(201, json=row)
        assert request.method == 'PATCH' and path == gateway_path
        assert set(resources) == set(fixed) and reviews, 'gateway cannot start before effective fixed grants'
        changed = copy.deepcopy(gateway)
        changes = json.loads(request.content)
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': gateway['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': gateway['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': gateway['spec']},
        ]
        for change in changes:
            target, field = ((changed['metadata'], change['path'].rsplit('/', 1)[-1])
                if change['path'].startswith('/metadata/') else (changed, 'spec'))
            if change['op'] == 'test':
                assert target[field] == change['value']
            elif change['op'] == 'remove':
                target.pop(field)
            else:
                target[field] = change['value']
        if request.url.params.get('dryRun') == 'All':
            return httpx.Response(200, json=changed)
        assert not request.url.query
        assert json.loads((child / 'start/transition.json').read_bytes())['resources'][_key(gateway)]['status'] == 'intent'
        patches.append(_key(gateway))
        changed['metadata'].update(resourceVersion='905', generation=2)
        gateway.clear()
        gateway.update(changed)
        refresh()
        if mode['lose_patch']:
            mode['lose_patch'], mode['lose_read'] = False, True
            raise httpx.ReadError('simulated committed gateway response loss')
        return httpx.Response(200, json=copy.deepcopy(gateway))

    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            def prepare(self, phase, bound, directory):
                if phase == 'forward-activate':
                    return activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
                if phase == 'forward-build-policy':
                    return build_child.prepare_shared_build_activation(bindings=bound, state_dir=directory)
                if phase != 'forward-gateway':
                    return super().prepare(phase, bound, directory)
                module.prepare_shared_gateway(bindings=bound, state_dir=directory)
                if mode['prepare_loss']:
                    mode['prepare_loss'] = False
                    raise TimeoutError('simulated interruption after effect-free child preparation')

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-activate':
                    activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-activate':
                    activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-activate':
                    return False
                if phase == 'forward-build-policy':
                    # Only this preceding phase's remote effect is doubled;
                    # gateway and inventory readers consume its real receipt.
                    record = build_child.validate_shared_build_activation(bindings=bound, state_dir=directory, complete=False)
                    private_state._atomic_json(directory / 'build-policy.json', {**record, 'status': 'activated'})
                    return True
                if phase != 'forward-gateway':
                    return super().advance(phase, bound, directory)
                def ready(request, selected, original, expected, actual):
                    assert request == runtime and selected == 'forward-gateway'
                    assert _key(original) == _key(actual) == _key(plan.gateway_original)
                    challenges.append(actual['metadata']['uid'])
                    return mode['runtime_ready']
                transport = stack.enter_context(module.HTTPSSharedGateway(bindings=bound,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=ready))
                transport.client.close()
                transport.client = type(completed_pool[3].api.client)(base_url=transport.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_gateway(bindings=bound, api=transport, state_dir=directory)

        api = API(parent)
        api.block = 'forward-activate'
        kwargs = dict(**args, bindings=bindings, api=api, action='install')
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == patches == []
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == [next(iter(fixed))] and patches == []
        for path in (child / 'gateway.json', child / 'authority/stage.json', child / 'authority/dispatch.json', child / 'start/transition.json'):
            saved = path.read_bytes()
            path.unlink()
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**kwargs)
            assert len(creates) == 1 and patches == []
            path.write_bytes(saved)
            path.chmod(0o600)
        mode['extra_permission'] = True
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert creates == list(fixed) and patches == []
        mode['extra_permission'] = False
        with pytest.raises(ValueError) as startup_error:
            parent.run_shared_pool_operation(**kwargs)
        if not patches:
            import traceback

            frames, cause = [], startup_error.value
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('gateway failed before intended PATCH fault: ' + ', '.join(frames), pytrace=False)
        assert patches == [_key(gateway)]
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
        assert challenges == []  # No process challenge before actual rollout readiness.
        mode['ready'] = True
        refresh()
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
        assert challenges and patches == [_key(gateway)]
        reads, inventory, read = runtime_http_inventory(runtime, shared_runtime[1], completed_pool[3].server, handoff[2])
        namespace = copy.deepcopy(plan.build_namespace_target)
        namespace['metadata']['uid'] = plan.build_namespace_original['metadata']['uid']
        inventory['/api/v1/namespaces/' + namespace['metadata']['name']] = namespace
        inventory[gateway_path] = copy.deepcopy(gateway)
        with HTTPSDevelopmentRuntimeObserver(request=runtime,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                ssl_context=ssl.create_default_context()) as observer:
            for connection in (observer, observer.manager, observer.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir)  # Ordinary installation remains strict.
            observed = observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert observed[_key(gateway)] == gateway
            for damage in ('uid', 'image'):
                changed = inventory[gateway_path]
                if damage == 'uid':
                    changed['metadata']['uid'] = str(uuid4())
                else:
                    changed['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
                with pytest.raises(ValueError):
                    observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
                inventory[gateway_path] = copy.deepcopy(gateway)
            receipt = child / 'start/transition.json'
            saved = receipt.read_bytes()
            receipt.unlink()
            before = len(reads)
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert len(reads) == before
            receipt.write_bytes(saved)
            receipt.chmod(0o600)
            changed_during_read = []

            def changing_read(message):
                result = read(message)
                if not changed_during_read:
                    # Same parsed record, different retained bytes: semantic
                    # equality alone must not hide a concurrent journal write.
                    receipt.write_bytes(saved + b'\n')
                    changed_during_read.append(True)
                return result

            for connection in (observer, observer.manager, observer.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                    transport=httpx.MockTransport(changing_read))
            with pytest.raises(ValueError):
                observer.inspect(state_dir=plan.runtime.state_dir, _shared_handoff=bindings)
            assert changed_during_read
            receipt.write_bytes(saved)
            receipt.chmod(0o600)
        from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes

        populate_runtime_pods(inventory)
        with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
                ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            component, _, pod = probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)
            assert component == 'controller' and pod['status']['phase'] == 'Running'
        assert all(message.method == 'GET' for message in reads)
        mode['runtime_ready'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
        assert patches == [_key(gateway)] and creates == list(fixed)
        assert gateway['spec']['replicas'] == 1 and _stable(gateway) == _stable(plan.gateway_target)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
        assert patches == [_key(gateway)] and creates == list(fixed)
        assert {namespace for participant in bindings.migration.registration.spec.participants
            for namespace in (participant.execution_namespace.name, participant.build_namespace.name)} <= set(reviews)
        activation_path = child.parent / 'forward-activate/activation.json'
        activation_bytes = activation_path.read_bytes()
        with HTTPSSharedPoolRuntimeProbes(bindings=bindings,
                api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
                ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(read))
            assert probes._running('Deployment:loom-dev:loom-control-plane', plan.runtime.state_dir)[0] == 'controller'
            activation_path.unlink()
            before = len(reads)
            with pytest.raises(ValueError):
                probes.inspect(state_dir=plan.runtime.state_dir)
            assert len(reads) == before
            activation_path.write_bytes(activation_bytes)
            activation_path.chmod(0o600)
            changed = []

            def change_activation_during_read(message):
                response = read(message)
                if not changed:
                    activation_path.write_bytes(activation_bytes + b'\n')
                    changed.append(True)
                return response

            for connection in (probes, probes.manager, probes.foundation):
                connection.client.close()
                connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                    transport=httpx.MockTransport(change_activation_during_read))
            with pytest.raises(ValueError):
                probes.inspect(state_dir=plan.runtime.state_dir)
            assert changed
            activation_path.write_bytes(activation_bytes)
            activation_path.chmod(0o600)


@pytest.mark.timeout(420)
def test_shared_handoff_binds_real_completed_dev_and_current_staging(shared_runtime, staging_history, staging_observation):
    import base64

    from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
    from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff

    request, api = shared_runtime
    dev = load_completed_runtime(request)
    before = {**{path: path.read_bytes() for path in staging_history.history}, **dev.files}
    writes = copy.deepcopy(api.store.resources)
    result = prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    assert result.development.runtime == dev
    assert len(result.migration.guards) == 2
    assert {row.namespace for row in result.migration.guards} == {'loom-dev', staging_observation['namespace']}
    assert all(row['spec'].get('replicas', 0) == 0 for row in result.staging_targets.values())
    collector, = (row for row in result.staging_targets.values() if row['kind'] == 'CronJob')
    assert collector['spec']['suspend'] is True
    assert all(row['metadata']['namespace'] != 'loom-dev' for row in result.staging_targets.values())
    controller = result.staging_targets['Deployment:' + staging_observation['namespace'] + ':loom-control-plane']
    settings = {row['name']: row.get('value') for row in controller['spec']['template']['spec']['containers'][0]['env']}
    from loom.nebius_pool_settings import PoolRuntimeSettings

    bound = PoolRuntimeSettings.model_validate_json(settings['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON'])
    assert bound.participant.environment_class == 'staging' and bound.environment == 'development'
    assert bound.management_origin == 'https://' + request.database.manager.deployment.public_host
    participant = result.staging.attachment.participant
    execution, build = participant.execution_namespace.name, participant.build_namespace.name
    assert hasattr(result, 'staging_material'), 'handoff must derive staged credentials and role targets'
    assert len(result.staging_material) == 2
    assert {row['metadata']['namespace'] for row in result.staging_material.values()} == {staging_observation['namespace'], execution}
    assert all(row['kind'] == 'Secret' and row['immutable'] is True
        and base64.b64decode(row['data']['token']) == b'staging-test-token' for row in result.staging_material.values())
    roles = {f'Role:{execution}:loom-execution-actuator', f'Role:{build}:loom-task-image-builder'}
    assert set(result.staging_role_originals) == set(result.staging_role_targets) == roles
    for key in roles:
        original = result.staging_role_originals[key]
        assert original == next(row for row in staging_observation['resources'] if _key(row) == key)
        target = result.staging_role_targets[key]
        assert target['rules'] == [
            {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
            {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
            {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']},
        ]
        assert target['metadata']['annotations']['loom.nebius/shared-pool-handoff-operation'] == str(result.migration.registration.spec.operation_id)
        assert target['metadata'].get('labels') == original['metadata'].get('labels')
    assert {row['kind'] for row in result.staging_reader_authority.values()} == {'ClusterRole', 'ClusterRoleBinding'}
    reader, = (row for row in result.staging_reader_authority.values() if row['kind'] == 'ClusterRole')
    assert reader['rules'] == [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
        'resourceNames': [execution, build]}]
    binding, = (row for row in result.staging_reader_authority.values() if row['kind'] == 'ClusterRoleBinding')
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-execution-actuator', 'namespace': execution}]
    assert 'staging-test-token' not in repr(result)
    assert api.store.resources == writes
    assert all(path.read_bytes() == raw for path, raw in before.items())
    tokens = request.database.manager.retained.intent.tokens
    machine, = (row for row in result.migration.registration.spec.machines if row.participant_id == participant.participant_id)
    token = tokens[machine.machine_id]
    tokens[machine.machine_id] = 'wrong-machine-token'
    try:
        with pytest.raises(ValueError, match='shared pool handoff intent unqualified') as error:
            prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT)
        assert 'wrong-machine-token' not in str(error.value)
    finally:
        tokens[machine.machine_id] = token
    changed = copy.deepcopy(staging_observation)
    role = next(row for row in changed['resources'] if _key(row) in roles)
    role['metadata'].setdefault('annotations', {})['loom.nebius/shared-pool-handoff-operation'] = str(result.migration.registration.spec.operation_id)
    with pytest.raises(ValueError, match='shared pool handoff intent unqualified'):
        prepare_shared_pool_handoff(runtime=request, observation=changed, legacy=staging_history, repo_root=ROOT)
    assert api.store.resources == writes
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.timeout(420)
def test_shared_guard_binding_uses_live_dev_database_and_real_bound_guard_reader(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, monkeypatch, tmp_path):
    """An unbound dev guard must not fall back to the old controller-only path."""
    import base64
    import json
    import ssl
    import subprocess
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlsplit

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    request, parent = shared_runtime
    calls, documents, original_handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    populate_database_backends(documents)
    credential_path = '/api/v1/namespaces/loom-dev/secrets/loom-platform-db'
    actuator_secret = request.database.material[1]
    actuator_path = '/api/v1/namespaces/' + actuator_secret['metadata']['namespace'] + '/secrets/' + actuator_secret['metadata']['name']
    for path in (credential_path, actuator_path):
        documents[path]['metadata']['resourceVersion'] = '301'
    mode = {'late_version': False, 'reads': 0}

    def respond(message):
        if mode['late_version'] and message.url.path == credential_path:
            mode['reads'] += 1
            if mode['reads'] == 2:
                documents[credential_path]['metadata']['resourceVersion'] = '302'
        return original_handler(message)

    # Keep real history, HTTPS observer and guard implementation. Only their
    # external Kubernetes/psql transport is simulated.
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(respond))
        from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_guard_bindings

        def observe():
            return observe_shared_pool_guard_bindings(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=observer)

        bound = observe()
        guard, = (row for row in bound.migration.guards if row.namespace == 'loom-dev')
        database = guard.database
        assert database is not None
        assert str(database.credential_uid) == documents[credential_path]['metadata']['uid']
        assert database.credential_resource_version == '301'
        assert str(database.actuator_credential_uid) == documents[actuator_path]['metadata']['uid']
        assert database.actuator_credential_resource_version == '301'
        for kind, row in (('StatefulSet', database.statefulset), ('Service', database.service)):
            assert row['kind'] == kind and row['metadata']['namespace'] == 'loom-dev'
            assert row['metadata']['name'] == 'loom-postgres'
        staged, = (row for row in bound.migration.guards if row.namespace != 'loom-dev')
        assert staged == bound.plan.staging.attachment.guard
        kubeconfig = tmp_path / 'guard-kubeconfig'
        kubeconfig.write_text('test-only-private-kubeconfig')
        kubeconfig.chmod(0o600)
        commands = []

        def kubectl(command, **kwargs):
            commands.append(command)
            start = next(index for index, item in enumerate(command) if item in {'get', 'exec'})
            args = command[start:]
            if args[0] == 'exec':
                assert args[1:7] == ['-n', 'loom-dev', 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
                assert args[7] == 'psql'
                assert str(bound.migration.registration.spec.operation_id) in args[-1]
                assert bound.migration.registration.candidate['candidate_sha'] in args[-1]
                value = {'status': 'open'}
            elif args[1] == '--raw':
                url = urlsplit(args[2])
                value = copy.deepcopy(documents[url.path])
                query = parse_qs(url.query)
                if 'labelSelector' in query:
                    label, wanted = query['labelSelector'][0].split('=', 1)
                    value['items'] = [row for row in value['items'] if row['metadata']['labels'].get(label) == wanted]
            else:
                kind, name = args[1:3]
                namespace = args[args.index('-n') + 1] if '-n' in args else None
                if kind == 'namespace' and name == 'kube-system':
                    value = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                        'uid': bound.migration.registration.binding.kube_system_uid}}
                else:
                    plural = {'namespace': 'namespaces', 'secret': 'secrets', 'statefulset': 'statefulsets', 'service': 'services'}[kind]
                    prefix = '/apis/apps/v1' if kind == 'statefulset' else '/api/v1'
                    value = documents[prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name]
            return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode())

        with monkeypatch.context() as transport:
            transport.setattr(subprocess, 'run', kubectl)
            api = KubectlPoolGuardAPI(request=bound.migration, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
            assert api.guard(guard, 'observe') == {'status': 'open'}
        assert sum('exec' in row for row in commands) == 1
        assert calls and all(row.method == 'GET' for row in calls)
        assert base64.b64decode(documents[credential_path]['data']['control-plane-url']).decode() not in repr(bound)

        original = copy.deepcopy(documents[credential_path])
        for damage in ('uid', 'data'):
            if damage == 'uid':
                documents[credential_path]['metadata']['uid'] = str(uuid4())
            else:
                documents[credential_path]['data']['control-plane-url'] = base64.b64encode(b'foreign-database').decode()
            with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
                observe()
            documents[credential_path] = copy.deepcopy(original)
        mode.update(late_version=True, reads=0)
        with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
            observe()
        assert all(row.method == 'GET' for row in calls)
        mode['late_version'] = False
        documents[credential_path] = copy.deepcopy(original)
        child = bound.plan.development.runtime.state_dir / 'start' / 'transition.json'
        raw = child.read_bytes()
        child.unlink()
        before_reads = len(calls)
        try:
            with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
                observe()
            assert len(calls) == before_reads
        finally:
            child.write_bytes(raw)
            child.chmod(0o600)


@pytest.mark.timeout(600)
@pytest.mark.parametrize('forward_processes', [False, True, 'activation', 'recovery-databases', 'recovery-processes', 'process-observation'],
    ids=['initial', 'forward', 'activation', 'recovery-databases', 'recovery-processes', 'process-observation'])
def test_shared_process_preflight_reads_current_databases_and_retired_authority(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, monkeypatch, forward_processes):
    """Reject active old authority, failed real challenges and changed bindings.

    Only Kubernetes transport and remote process output are doubled. Omitting a
    required probe or using old staging templates must fail this connected test.
    """
    import base64
    import hmac
    import json
    import ssl
    import subprocess
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlsplit

    import httpx
    from pydantic import PostgresDsn
    from scripts.ops import nebius_shared_pool_handoff_live as module
    from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
    from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
    from scripts.ops.nebius_pool_activation_database import pool_activation_state_sql
    from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND
    from scripts.ops.nebius_pool_legacy_settings import (
        BOUND_LEGACY_SETTINGS_COMMAND,
        expected_legacy_runtime_settings,
    )
    from scripts.ops.nebius_pool_machine_database import (
        pool_machine_retirement_sql,
        pool_recovery_machine_state_sql,
    )
    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_DATABASE_COMMAND,
        _BOUND_TELEMETRY_COMMAND,
        pool_guard_activation_sql,
        pool_runtime_role_sql,
    )
    from scripts.ops.nebius_pool_recovery_database import (
        participant_recovery_drain_sql,
        pool_recovery_drain_sql,
    )
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )
    from scripts.ops.nebius_pool_startup_capacity import (
        BOUND_POOL_ACTIVATION_COMMAND,
        BOUND_POOL_ACTIVE_CAPACITY_COMMAND,
        BOUND_POOL_CAPACITY_COMMAND,
        expected_startup_capacity,
    )
    from scripts.ops.nebius_pool_startup_database import (
        pool_active_authority_sql,
        pool_startup_closed_sql,
    )
    from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        populate_runtime_pods,
        runtime_http_inventory,
    )
    from tests.ops.test_nebius_management_refresh_predecessor import history_credential

    from loom_service.pool_management.capacity import digest

    assert hasattr(module, 'observe_shared_pool_processes'), 'connected SQL/process preflight is missing'
    request, parent = shared_runtime
    calls, documents, fallback = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    plan = module.prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    attachment = plan.staging.attachment
    staging, dev = attachment.guard, next(row for row in plan.migration.guards if row.namespace == 'loom-dev')
    spec = plan.migration.registration.spec
    old = staging_history.context
    old_spec = old.inputs.installation
    old_namespace = old.original.upgrade.setup.binding.namespace
    manager_namespace = request.database.manager.retained.request.retained.binding.namespace

    def add(row):
        plural = {'Secret': 'secrets', 'Namespace': 'namespaces', 'Deployment': 'deployments', 'CronJob': 'cronjobs',
            'StatefulSet': 'statefulsets', 'Service': 'services', 'ReplicaSet': 'replicasets'}[row['kind']]
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        namespace = row['metadata'].get('namespace')
        path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
        return path

    for row in attachment.resources:
        if row['kind'] in {'Namespace', 'Deployment', 'StatefulSet', 'Service', 'CronJob'}:
            add(row)
    # Recover the old manager DB and credential from its genuine installation.
    journal = json.loads((old.original.upgrade.original_state / 'database/stage.json').read_bytes())
    for item in journal['resources'].values():
        row = copy.deepcopy(item['observed'])
        row['metadata']['uid'] = item['uid']
        add(row)
    old_secret_path = add(history_credential(old.original))
    add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': old_namespace,
        'uid': old.original.upgrade.setup.binding.namespace_uid}})
    add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
        'uid': plan.migration.registration.binding.kube_system_uid}})
    namespace_response = fallback(httpx.Request('GET', request.database.foundation.inputs.config['kubernetes_api_server']
        + '/api/v1/namespaces/' + manager_namespace))
    assert namespace_response.status_code == 200
    add(namespace_response.json())  # Share the full existing namespace fixture across HTTPS and kubectl.
    # Current staging secrets correspond to its captured UID/resourceVersions.
    for row in plan.staging.originals.values():
        if row['kind'] != 'Deployment':
            continue
        namespace = row['metadata']['namespace']
        actuator = namespace != staging.namespace
        variable = 'LOOM_EXECUTION_ACTUATOR_DB_URL' if actuator else (
            'LOOM_CP_DB_URL' if row['metadata']['name'] == 'loom-control-plane' else 'LOOM_SVC_DB_URL')
        env, = (env for env in row['spec']['template']['spec']['containers'][0]['env'] if env['name'] == variable)
        reference = env['valueFrom']['secretKeyRef']
        path = '/api/v1/namespaces/' + namespace + '/secrets/' + reference['name']
        if path not in documents:
            add({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'namespace': namespace, 'name': reference['name'],
                'uid': str(staging.database.actuator_credential_uid if actuator else staging.database.credential_uid),
                'resourceVersion': staging.database.actuator_credential_resource_version if actuator else staging.database.credential_resource_version},
                'data': {}})
        documents[path]['data'][reference['key']] = base64.b64encode(
            f'postgresql+psycopg://loom:private-test-only@loom-postgres.{staging.namespace}.svc:5432/loom'.encode()).decode()
    # Retained snapshots intentionally omit volatile versions, but live API
    # objects expose them. Both HTTPS and kubectl consume this same inventory.
    for row in documents.values():
        row['metadata'].setdefault('resourceVersion', '301')
    populate_runtime_pods(documents)
    populate_database_backends(documents)
    for row in documents.values():
        if row['kind'] == 'Deployment' and row['spec']['replicas'] == 0:
            row['metadata'].setdefault('generation', 1)
            row['status'] = {'observedGeneration': row['metadata']['generation']}
    for listing in list(documents.values()):
        if listing['kind'] == 'ReplicaSetList':
            for row in listing['items']:
                add(row)
        if listing['kind'] == 'PodList':
            for row in listing['items']:
                row['spec']['nodeName'] = 'platform-test'
    node_uid = str(uuid4())
    documents['/api/v1/nodes'] = {'apiVersion': 'v1', 'kind': 'NodeList', 'metadata': {'resourceVersion': '401'},
        'items': [{'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': 'platform-test', 'uid': node_uid, 'labels': {}}}]}

    def read(path):
        url = urlsplit(path)
        value = copy.deepcopy(documents[url.path])
        selector = parse_qs(url.query).get('labelSelector')
        if selector:
            label, wanted = selector[0].split('=', 1)
            value['items'] = [row for row in value['items'] if row['metadata'].get('labels', {}).get(label) == wanted]
        return value

    def handle(message):
        assert message.method == 'GET'
        if message.url.path in documents:
            calls.append(message)
            response = httpx.Response(200, json=read(str(message.url)))
            if mode.get('http_hook') is not None:
                mode['http_hook'](message)
            return response
        return fallback(message)

    commands, checked = [], set()
    mode = {'damage': None}
    roles = {(manager_namespace, 'loom-service'): 'manager', ('loom-dev', 'loom-service'): 'service',
        ('loom-dev', 'loom-control-plane'): 'controller',
        (next(row for row in spec.participants if row.participant_id == dev.participant_id).execution_namespace.name,
            'loom-execution-actuator'): 'actuator'}
    for row in plan.staging.originals.values():
        if row['kind'] == 'Deployment':
            namespace, name = row['metadata']['namespace'], row['metadata']['name']
            roles[namespace, name] = 'controller' if name == 'loom-control-plane' else 'service' if name == 'loom-service' else 'actuator'

    def execute(argv, **kwargs):
        start = next(index for index, item in enumerate(argv) if item in {'exec', 'get'})
        args = argv[start:]
        commands.append(args)
        if args[0] == 'get':
            if args[1] == '--raw':
                value = read(args[2])
            else:
                kind, name = args[1:3]
                namespace = args[args.index('-n') + 1] if '-n' in args else None
                plural = {'namespace': 'namespaces', 'deployment': 'deployments', 'replicaset': 'replicasets', 'cronjob': 'cronjobs',
                    'secret': 'secrets', 'service': 'services', 'statefulset': 'statefulsets'}[kind]
                prefix = '/apis/apps/v1' if kind in {'deployment', 'replicaset', 'statefulset'} else '/apis/batch/v1' if kind == 'cronjob' else '/api/v1'
                value = read(prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name)
                if (kind == 'deployment' and namespace == staging.namespace and mode.get('capacity_seen')
                        and mode['damage'] in {'late-manager-secret-after-gateway', 'late-manager-database-after-gateway'}):
                    if mode['damage'] == 'late-manager-secret-after-gateway':
                        documents['/api/v1/namespaces/' + manager_namespace + '/secrets/loom-platform-db']['metadata']['resourceVersion'] = 'late-final-rotation'
                    elif not mode.get('late_injected'):
                        pod, = (row for row in documents['/api/v1/namespaces/' + manager_namespace + '/pods']['items']
                            if row['metadata']['name'] == 'loom-postgres-0')
                        pod['status']['containerStatuses'][0]['restartCount'] += 1
                    mode['late_injected'] = True
        else:
            assert args[:2] == ['exec', '-n'] and args[4] == '-c' and args[6] == '--'
            namespace = args[2]
            if args[7] == 'psql':
                assert args[3] == 'pod/loom-postgres-0' and args[5] == 'loom-postgres'
                sql = args[-1]
                if namespace == old_namespace:
                    assert sql == pool_machine_retirement_sql(old_spec, action='observe')
                    value = {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(old_spec.operation_id),
                        'installation_sha256': digest(old_spec.model_dump(mode='json')),
                        'state': 'active' if mode['damage'] == 'old-active' else 'revoked'}
                    checked.add('old-revoked')
                elif namespace == manager_namespace:
                    if sql == pool_recovery_machine_state_sql(spec):
                        checked.add('recovery-machines')
                        value = {'schema': 'loom.pool-recovery-machine-state.v1', 'operation_id': str(spec.operation_id),
                            'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
                            'state': mode.get('machine_state', 'active')}
                        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
                    if sql == pool_recovery_drain_sql(spec):
                        assert mode['pool_state'] == 'fenced'
                        checked.add('pool-drain')
                        value = {'schema': 'loom.pool-recovery-drain.v1', 'operation_id': str(spec.operation_id),
                            'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
                            'counts': {'unstarted_requests': int(mode.get('pool_busy', False)), 'active_requests': 0,
                                'unconfirmed_creates': 0, 'unqualified_releases': 0}}
                        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
                    if sql == pool_activation_state_sql(spec):
                        value = {'schema': 'loom.pool-activation-state.v1', 'operation_id': str(spec.operation_id),
                            'installation_sha256': digest(spec.model_dump(mode='json')), 'state': mode.get('pool_state', 'closed')}
                        checked.add('pool-state')
                        if mode['damage'] == 'late-manager-database-at-final-state':
                            mode['final_state_reads'] = mode.get('final_state_reads', 0) + 1
                            if mode['final_state_reads'] == 3:
                                pod, = (row for row in documents['/api/v1/namespaces/' + manager_namespace + '/pods']['items']
                                    if row['metadata']['name'] == 'loom-postgres-0')
                                pod['status']['containerStatuses'][0]['restartCount'] += 1
                                mode['late_injected'] = True
                        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
                    if sql == pool_active_authority_sql(spec):
                        value = {'schema': 'loom.pool-active-authority.v1', 'operation_id': str(spec.operation_id),
                            'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
                            'qualified': mode.get('pool_state') == 'global' and mode['damage'] != 'new-not-active'}
                        checked.add('new-active')
                        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
                    assert sql == pool_startup_closed_sql(spec) and mode.get('pool_state', 'closed') == 'closed'
                    value = {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
                        'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
                        'qualified': mode['damage'] != 'new-not-closed'}
                    checked.add('new-closed')
                    if mode['damage'] in {'late-staging-restart', 'late-dev-restart', 'late-staging-template', 'late-stopped-pod'}:
                        mode['manager_reads'] = mode.get('manager_reads', 0) + 1
                        if mode['manager_reads'] == 2:
                            if mode['damage'] == 'late-stopped-pod':
                                path, pod = mode['late_pod']
                                documents[path]['items'].append(copy.deepcopy(pod))
                            elif mode['damage'] in {'late-staging-restart', 'late-dev-restart'}:
                                namespace = 'loom-dev' if mode['damage'] == 'late-dev-restart' else staging.namespace
                                pod, = (row for row in documents['/api/v1/namespaces/' + namespace + '/pods']['items']
                                    if row['metadata'].get('labels', {}).get('app') == 'loom-control-plane')
                                pod['status']['containerStatuses'][0]['restartCount'] += 1
                            else:
                                workload = documents['/apis/apps/v1/namespaces/' + staging.namespace + '/deployments/loom-control-plane']
                                workload['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
                else:
                    assert namespace in {'loom-dev', staging.namespace}
                    participant = next(row for row in plan.migration.guards if row.namespace == namespace)
                    if sql == rollout_guard_observation_sql(owner=str(spec.operation_id), candidate=request.database.foundation.inputs.candidate['candidate_sha']):
                        value = {'status': 'held' if mode['damage'] == 'guard-held' or namespace in mode.get('held', set()) else 'open'}
                        checked.add(('guard', namespace))
                    elif sql == pool_guard_activation_sql(spec.operation_id, participant.participant_id,
                            request.database.foundation.inputs.candidate['candidate_sha'], action='observe'):
                        value = {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(spec.operation_id),
                            'participant_id': str(participant.participant_id), 'status': 'foreign' if mode['damage'] == 'guard-foreign'
                            else mode.get('guard_states', {}).get(namespace, 'held' if namespace in mode.get('held', set()) else 'open')}
                        checked.add(('active-guard', namespace))
                    elif sql == participant_recovery_drain_sql(spec.operation_id, participant.participant_id,
                            request.database.foundation.inputs.candidate['candidate_sha']):
                        assert mode['guard_states'][namespace] == 'fenced'
                        checked.add(('participant-drain', namespace))
                        value = {'schema': 'loom.pool-participant-drain.v1', 'operation_id': str(spec.operation_id),
                            'participant_id': str(participant.participant_id),
                            'candidate_sha': request.database.foundation.inputs.candidate['candidate_sha'], 'read_only': True,
                            'counts': {'trials': int(namespace in mode.get('busy', set())), 'executions': 0,
                                'builds': 0, 'build_cleanup': 0, 'execution_outboxes': 0, 'build_outboxes': 0}}
                    elif sql == pool_guard_activation_sql(spec.operation_id, participant.participant_id,
                            request.database.foundation.inputs.candidate['candidate_sha'], action='release'):
                        assert forward_processes == 'activation' and mode['pool_state'] == 'global'
                        mode['effects'].append(('release', str(participant.participant_id)))
                        if mode.get('release_failure') == str(participant.participant_id):
                            raise TimeoutError('simulated unknown guard transaction')
                        mode['held'].remove(namespace)
                        value = {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(spec.operation_id),
                            'participant_id': str(participant.participant_id), 'status': 'open'}
                    else:
                        assert forward_processes and sql == pool_runtime_role_sql(owner=str(spec.operation_id),
                            candidate=request.database.foundation.inputs.candidate['candidate_sha'], action='inspect')
                        value = {'status': 'qualified'}
                        checked.add(('runtime-role', namespace))
            elif args[9] == _BOUND_TELEMETRY_COMMAND:
                assert args[10] == attachment.participant.execution_namespace.name
                assert args[-2:] == ['platform-test', node_uid]
                value = {'status': 'blocked', 'stage': 'counters'}
                checked.add('telemetry')
                if mode['damage'] == 'late-staging-actuator':
                    secret, = (row for row in documents.values() if row['kind'] == 'Secret'
                        and row['metadata']['uid'] == str(staging.database.actuator_credential_uid))
                    secret['metadata']['resourceVersion'] = 'late-rotation'
            elif args[9] == BOUND_POOL_ACTIVATION_COMMAND:
                assert forward_processes == 'activation' and args[7:9] == ['python', '-c']
                assert namespace == manager_namespace and args[5] == 'gateway' and len(args) == 12
                payload = json.dumps(expected_startup_capacity(spec), sort_keys=True, separators=(',', ':')).encode()
                assert hmac.new(bytes.fromhex(args[-2]), payload, 'sha256').hexdigest() == args[-1]
                assert mode.get('pool_state', 'closed') == 'closed' and len(mode['held']) == len(spec.participants)
                mode['effects'].append(('open', None))
                mode['pool_state'] = 'global'
                raise TimeoutError('simulated committed opening reply loss')
            elif args[9] in {BOUND_GATEWAY_KUBERNETES_COMMAND, BOUND_POOL_CAPACITY_COMMAND, BOUND_POOL_ACTIVE_CAPACITY_COMMAND}:
                assert args[7:9] == ['python', '-c'] and namespace == manager_namespace and args[5] == 'gateway'
                name = 'loom-pool-gateway'
                workload = documents['/apis/apps/v1/namespaces/' + namespace + '/deployments/' + name]
                machine, = (row for row in spec.machines if row.role == 'gateway')
                if args[9] == BOUND_GATEWAY_KUBERNETES_COMMAND:
                    assert len(args) == 13
                    settings = expected_pool_runtime_settings('gateway', workload, token_sha256=machine.token_sha256)
                    wanted = {key: settings[key] for key in ('pool_id', 'installation_id', 'machine_id', 'admission_epoch', 'kubernetes')}
                    wanted['namespaces'] = {ns.name: str(ns.uid) for participant in spec.participants
                        for ns in (participant.execution_namespace, participant.build_namespace)}
                    assert json.loads(args[10]) == sorted(wanted['namespaces'])
                    check = 'kubernetes'
                else:
                    assert len(args) == 12
                    active = mode.get('pool_state', 'closed') == 'global'
                    assert args[9] == (BOUND_POOL_ACTIVE_CAPACITY_COMMAND if active else BOUND_POOL_CAPACITY_COMMAND)
                    wanted = expected_startup_capacity(spec)
                    check = 'active-capacity' if active else 'capacity'
                payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
                assert hmac.new(bytes.fromhex(args[-2]), payload, 'sha256').hexdigest() == args[-1]
                checked.add((check, namespace, name))
                value = {'status': 'wrong' if mode['damage'] == 'gateway-' + check else 'qualified'}
                if check == 'capacity':
                    mode['capacity_seen'] = True
                    pods = documents['/api/v1/namespaces/' + namespace + '/pods']['items']
                    if mode['damage'] == 'late-gateway-secret':
                        documents['/api/v1/namespaces/' + namespace + '/secrets/loom-platform-db']['metadata']['resourceVersion'] = 'late-rotation'
                    elif mode['damage'] in {'late-gateway-restart', 'late-gateway-database-restart'}:
                        pod, = (row for row in pods if row['metadata']['name'] == (
                            'loom-postgres-0' if mode['damage'] == 'late-gateway-database-restart' else name + '-a12b34-test'))
                        pod['status']['containerStatuses'][0]['restartCount'] += 1
                    elif mode['damage'] == 'late-gateway-receipt':
                        path = mode['gateway_receipt']
                        path.write_bytes(path.read_bytes() + b'\n')
            else:
                assert args[7:9] == ['python', '-c']
                code, component, nonce, response = args[9:]
                name = args[3].removeprefix('pod/').removesuffix('-a12b34-test')
                role = 'gateway' if name == 'loom-pool-gateway' and namespace == manager_namespace else roles[namespace, name]
                workload = documents['/apis/apps/v1/namespaces/' + namespace + '/deployments/' + name]
                if code == _BOUND_DATABASE_COMMAND:
                    assert component == ('service' if role == 'manager' else role)
                    variable = {'service': 'LOOM_SVC_DB_URL', 'controller': 'LOOM_CP_DB_URL',
                        'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL', 'gateway': 'LOOM_POOL_GATEWAY_DB_URL'}[component]
                    env, = (row for row in workload['spec']['template']['spec']['containers'][0]['env'] if row['name'] == variable)
                    reference = env['valueFrom']['secretKeyRef']
                    secret = documents['/api/v1/namespaces/' + namespace + '/secrets/' + reference['name']]
                    url = base64.b64decode(secret['data'][reference['key']]).decode()
                    payload = (url if component in {'actuator', 'gateway'} else str(PostgresDsn(url))).encode()
                    assert url not in str(argv)
                    check = 'database'
                else:
                    assert component == role
                    staging_process = namespace in {staging.namespace, attachment.participant.execution_namespace.name}
                    if staging_process and not mode.get('shared_settings'):
                        assert code == BOUND_LEGACY_SETTINGS_COMMAND
                        settings = expected_legacy_runtime_settings(component, workload)
                    else:
                        assert code == BOUND_POOL_SETTINGS_COMMAND
                        participant_id = staging.participant_id if staging_process else dev.participant_id
                        machine, = (row for row in spec.machines if (row.role == 'gateway' if component == 'gateway'
                            else row.participant_id == participant_id and row.workload_scope == 'environment'))
                        settings = expected_pool_runtime_settings(component, workload,
                            token_sha256=machine.token_sha256 if component in {'controller', 'actuator', 'gateway'} else None,
                            catalog_sha256=hashlib.sha256(spec.profiles.model_dump_json().encode()).hexdigest() if component == 'manager' else None)
                    payload = json.dumps(settings, sort_keys=True, separators=(',', ':')).encode()
                    check = 'settings'
                assert hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest() == response
                checked.add((check, namespace, name))
                value = {'status': 'wrong' if (mode['damage'] == check + ':' + namespace
                    or (role == 'gateway' and mode['damage'] == 'gateway-' + check)) else 'qualified'}
                if mode['damage'] == 'late-old-secret':
                    documents[old_secret_path]['metadata']['resourceVersion'] = 'changed'
            assert args[7] in {'psql', 'python'}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    def execute_with_hook(argv, **kwargs):
        result = execute(argv, **kwargs)
        if mode.get('sql_hook') is not None and 'psql' in argv:
            mode['sql_hook'](argv)
        return result

    monkeypatch.setattr(subprocess, 'run', execute_with_hook)
    history = {path: path.read_bytes() for path in plan.history}
    with HTTPSDevelopmentRuntimeProbes(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='operator-test') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))

        def observe():
            return module.observe_shared_pool_processes(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=probes,
                kubeconfig=Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1]), executable=Path(probes.prefix[0]))

        try:
            result = observe()
        except ValueError as error:
            import traceback

            frames = []
            cause = error
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('process preflight failed at ' + ', '.join(frames), pytrace=False)
        assert result.bindings.plan == plan
        assert result.guard_states == {'loom-dev': 'open', staging.namespace: 'open'}
        assert result.retired_authority == 'revoked'
        assert result.telemetry == {'status': 'unavailable', 'checks': 1, 'unavailable': 1, 'reasons': ['counters']}
        assert checked == {'old-revoked', 'new-closed', 'telemetry', ('guard', 'loom-dev'), ('guard', staging.namespace),
            *((check, namespace, name) for namespace, name in roles for check in ('settings', 'database'))}
        assert 'private-test-only' not in repr(result)
        if forward_processes in {'recovery-databases', 'recovery-processes'}:
            _exercise_recovery_databases(bindings=result.bindings, documents=documents, mode=mode, checked=checked,
                commands=commands, probes=probes, process_observation=forward_processes == 'recovery-processes',
                args=dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT))
            return
        if forward_processes:
            with HTTPSSharedPoolRuntimeProbes(bindings=result.bindings,
                    api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
                    token='operator-test') as shared_probes:
                for connection in (shared_probes, shared_probes.manager, shared_probes.foundation):
                    connection.client.close()
                    connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                _exercise_forward_processes(bindings=result.bindings, development=shared_probes, documents=documents,
                    mode=mode, checked=checked, commands=commands, add=add, calls=calls,
                    activation_transport=forward_processes == 'activation',
                    observation_only=forward_processes == 'process-observation',
                    args=dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT))
            return
        baseline = copy.deepcopy(documents)
        for damage in ('old-active', 'new-not-closed', 'guard-held', 'settings:' + staging.namespace,
                'database:loom-dev', 'late-old-secret', 'late-staging-actuator'):
            mode['damage'] = damage
            with pytest.raises(ValueError, match='shared pool process preflight unqualified'):
                observe()
            documents.clear()
            documents.update(copy.deepcopy(baseline))
    assert calls and all(row.method == 'GET' for row in calls)
    assert all(row[0] in {'get', 'exec'} for row in commands)
    assert all(path.read_bytes() == raw for path, raw in history.items())


def _exercise_recovery_databases(*, bindings, documents, mode, checked, commands, probes, args, process_observation=False):
    """Real parent/child records and fixed SQL transports; earlier effects are doubled."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_reopening as reopening
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_pool_activation_database import pool_activation_state_sql

    name = 'scripts.ops.nebius_shared_pool_recovery_databases'
    assert importlib.util.find_spec(name) is not None, 'phase-bound recovery database observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_recovery_databases'])
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    staging = bindings.plan.staging.attachment.guard.namespace
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (reopening.prepare_shared_recovery_reopening, reopening.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (reopening.prepare_shared_recovery_verification, reopening.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            if phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-revoke'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                mode['machine_state'] = 'revoked'
                path = directory / 'gateway.json'
                record = _read(path)
                for row in record['roles'].values():
                    row['status'] = 'absent'
                private_state._atomic_json(path, record)
                return True
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                mode.update(pool_state='fenced', guard_states={row.namespace: 'fenced' for row in bound.migration.guards})
            elif phase in {'recovery-drain', 'recovery-verify'}:
                record['status'] = 'observed'
            elif phase == 'recovery-stop':
                for key, choices in stop._sources(bound, state).items():
                    before = _snapshot(choices[-1])
                    before['metadata']['uid'] = _uid(choices[-1])
                    record['resources'][key] = {'status': 'stopped', 'before': before,
                        'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
            elif phase in {'recovery-restore-workloads', 'recovery-restart'}:
                component = templates if phase == 'recovery-restore-workloads' else restart
                before, targets = component._inputs(bound, state)
                for key, row in record['resources'].items():
                    row.update(status='restored' if component is templates else 'started',
                        before_resource_version='2' if _stable(before[key]) != _stable(targets[key]) else None)
            elif phase == 'recovery-restore-roles':
                for row in record['roles'].values():
                    row.update(status='restored', before_resource_version='201')
            elif phase == 'recovery-reopen':
                record['status'] = 'released'
                mode['guard_states'][staging] = 'open'
            else:
                pytest.fail('unexpected phase: ' + phase)
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    api.block = 'forward-guards'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'

    if process_observation:
        _exercise_recovery_processes(kwargs=kwargs, state=state, api=api, documents=documents,
            probes=probes, mode=mode, checked=checked)
        return

    def observe():
        return module.observe_shared_pool_recovery_databases(bindings=bindings, state_dir=state,
            kubeconfig=Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1]), executable=Path(probes.prefix[0]))

    error = 'shared pool recovery database observation unqualified'
    with pytest.raises(ValueError, match=error):
        observe()  # No recovery marker; neither a caller flag nor forward history grants it.
    kwargs['action'], api.block = 'recover', 'recovery-fence'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    # A crashed successor must not make database recovery depend on app health.
    for document in documents.values():
        if document['kind'] == 'PodList':
            document['items'] = [row for row in document['items'] if row['metadata']['name'] == 'loom-postgres-0']
    checked.clear()
    first_command = len(commands)
    result = observe()
    assert result.pool_state == 'closed' and result.machine_authority == 'active' and result.retired_authority == 'revoked'
    assert result.guard_states == {'loom-dev': 'open', staging: 'open'}
    assert result.pool_drained is None and result.participant_drained == {'loom-dev': None, staging: None}
    assert checked == {'old-revoked', 'recovery-machines', 'pool-state',
        ('active-guard', 'loom-dev'), ('active-guard', staging), ('runtime-role', 'loom-dev')}

    fence_path = state / 'recovery-fence/fence.json'
    record = _read(fence_path)
    mode['pool_state'] = 'fenced'
    with pytest.raises(ValueError, match=error):
        observe()  # A remotely fenced state is not adopted before local intent.
    record['pool'] = 'intent'
    private_state._atomic_json(fence_path, record)
    assert observe().pool_state == 'fenced'
    mode['pool_state'] = 'closed'
    assert observe().pool_state == 'closed'
    record['pool'], mode['pool_state'] = 'fenced', 'fenced'
    private_state._atomic_json(fence_path, record)
    first_guard = bindings.migration.guards[0]
    mode['guard_states'] = {first_guard.namespace: 'fenced'}
    with pytest.raises(ValueError, match=error):
        observe()
    record['guards'][str(first_guard.participant_id)] = 'intent'
    private_state._atomic_json(fence_path, record)
    assert observe().guard_states[first_guard.namespace] == 'fenced'
    mode['guard_states'][first_guard.namespace] = 'open'
    assert observe().guard_states[first_guard.namespace] == 'open'
    record['guards'][str(first_guard.participant_id)] = 'fenced'
    private_state._atomic_json(fence_path, record)
    with pytest.raises(ValueError, match=error):
        observe()
    mode['guard_states'][first_guard.namespace] = 'fenced'
    api.block = 'recovery-drain'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    mode.update(pool_busy=True, busy={'loom-dev', staging})
    result = observe()
    assert result.pool_drained is False and result.participant_drained == {'loom-dev': False, staging: False}
    mode.update(pool_busy=False, busy=set())

    # Bracket the final SQL read, not just each database's own first query.
    baseline = copy.deepcopy(documents)
    for damage in ('old-active', 'guard-foreign', 'late-manager-database-at-final-state'):
        mode.update(damage=damage, final_state_reads=1)
        with pytest.raises(ValueError, match=error):
            observe()
        documents.clear()
        documents.update(copy.deepcopy(baseline))
    mode['damage'] = None
    namespaces = (bindings.migration.registration.binding.namespace,
        bindings.plan.staging.legacy.context.original.upgrade.setup.binding.namespace, 'loom-dev', staging)
    for namespace in namespaces:
        observed = {'queries': 0, 'injected': False}
        def restart_database(argv, observed=observed, namespace=namespace):
            if argv[-1] == pool_activation_state_sql(bindings.migration.registration.spec):
                observed['queries'] += 1
                if observed['queries'] == 2:
                    pod, = (row for row in documents['/api/v1/namespaces/' + namespace + '/pods']['items']
                        if row['metadata']['name'] == 'loom-postgres-0')
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
                    observed['injected'] = True
        mode['sql_hook'] = restart_database
        with pytest.raises(ValueError, match=error):
            observe()
        assert observed['injected']
        documents.clear()
        documents.update(copy.deepcopy(baseline))
    mode['sql_hook'] = None
    receipt_bytes = fence_path.read_bytes()
    mode['sql_hook'] = lambda argv: fence_path.write_bytes(receipt_bytes + b'\n')
    with pytest.raises(ValueError, match=error):
        observe()
    mode['sql_hook'] = None
    fence_path.write_bytes(receipt_bytes)

    api.block = 'recovery-revoke'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
    assert observe().machine_authority == 'active'
    mode['pool_busy'] = True
    with pytest.raises(ValueError, match=error):
        observe()  # A previous drain receipt is not current drain proof.
    mode['pool_busy'] = False
    mode['machine_state'] = 'revoked'
    with pytest.raises(ValueError, match=error):
        observe()
    machine_path = state / 'recovery-revoke/machines.json'
    private_state._atomic_json(machine_path, {**_read(machine_path), 'status': 'intent'})
    assert observe().machine_authority == 'revoked'
    mode['machine_state'] = 'active'
    assert observe().machine_authority == 'active'
    api.block = 'recovery-reopen'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-reopen'
    assert observe().machine_authority == 'revoked'
    mode['machine_state'] = 'active'
    with pytest.raises(ValueError, match=error):
        observe()
    mode['machine_state'] = 'revoked'
    mode['guard_states'][staging] = 'open'
    with pytest.raises(ValueError, match=error):
        observe()
    reopen_path = state / 'recovery-reopen/reopen.json'
    private_state._atomic_json(reopen_path, {**_read(reopen_path), 'status': 'intent'})
    mode['busy'] = {staging}
    result = observe()
    assert result.participant_drained == {'loom-dev': True, staging: None}
    mode['guard_states'][staging], mode['busy'] = 'fenced', set()
    assert observe().participant_drained == {'loom-dev': True, staging: True}
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    mode['busy'] = {staging}
    assert observe().participant_drained == {'loom-dev': True, staging: None}
    mode['busy'] = {'loom-dev', staging}
    with pytest.raises(ValueError, match=error):
        observe()
    mode['busy'] = {staging}
    # A late change to the caller's nested binding cannot reuse a copied proof.
    captured = bindings.plan.staging.originals
    saved = copy.deepcopy(captured)
    def mutate_binding(argv):
        next(iter(captured.values()))['metadata'].setdefault('annotations', {})['late-test'] = 'changed'
    mode['sql_hook'] = mutate_binding
    with pytest.raises(ValueError, match=error):
        observe()
    captured.clear()
    captured.update(saved)
    mode['sql_hook'] = None
    assert 'private-test-only' not in repr(result)
    assert all(row[0] == 'get' or (row[0] == 'exec' and row[7] == 'psql') for row in commands[first_command:])


def _exercise_recovery_processes(*, kwargs, state, api, documents, probes, mode, checked):
    """Real recovery history/readiness; external I/O and collector publication are explicit doubles."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_processes'
    assert importlib.util.find_spec(name) is not None, 'phase-bound recovery process observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_recovery_processes'])
    bindings = kwargs['bindings']
    staging = bindings.plan.staging.attachment.guard
    completed = bindings.plan.development.runtime
    roots = {**bindings.plan.staging.originals,
        **{key: completed.resources[key] for key in (*completed.plan.fixed['workloads'], *completed.plan.originals)},
        _key(bindings.plan.development.gateway_original): bindings.plan.development.gateway_original}
    # The fixture's original collector is active. Its lineage/publication reader
    # has a separate real callback regression; this test controls just that read.
    collector_calls = []
    def collection_health(**options):
        assert options['bindings'] == bindings and options['guards'].request == bindings.migration
        assert _key(options['expected']) in bindings.plan.staging.originals
        collector_calls.append(_key(options['expected']))
        if mode.get('process_hook') is not None:
            mode['process_hook']()
        return mode.get('collector_ready', True)

    def current(key):
        return documents[runtime_resource_path(roots[key]) + '/' + roots[key]['metadata']['name']]

    def replace(key, wanted):
        actual = current(key)
        identity = {field: actual['metadata'][field] for field in ('uid', 'resourceVersion')}
        generation = actual['metadata'].get('generation', 1) + 1
        actual.clear()
        actual.update(copy.deepcopy(wanted))
        actual['metadata'].update(**identity, generation=generation)
        actual['status'] = {'observedGeneration': generation}

    def descendants():
        # Model the controllers completing their actual desired state. Keep the
        # retained database Pods; the real runtime challenges must still find them.
        for row in documents.values():
            if row['kind'] == 'PodList':
                row['items'] = [pod for pod in row['items'] if pod['metadata']['name'] == 'loom-postgres-0']
            elif row['kind'] == 'ReplicaSetList':
                row['items'] = []
        populate_runtime_pods(documents)
        for row in tuple(documents.values()):
            if row['kind'] == 'PodList':
                for pod in row['items']:
                    pod['spec']['nodeName'] = 'platform-test'
            if row['kind'] == 'ReplicaSetList':
                for child in row['items']:
                    documents['/apis/apps/v1/namespaces/' + child['metadata']['namespace']
                        + '/replicasets/' + child['metadata']['name']] = copy.deepcopy(child)

    for row in roots.values():
        if row['kind'] == 'CronJob':
            namespace = row['metadata']['namespace']
            documents.setdefault('/apis/batch/v1/namespaces/' + namespace + '/jobs',
                {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '501'}, 'items': []})

    def observe():
        try:
            return module.observe_shared_pool_recovery_processes(bindings=bindings, state_dir=state,
                kubeconfig=Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1]), executable=Path(probes.prefix[0]))
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('shared pool recovery process observation unqualified: ' + ', '.join(frames)) from None

    error = 'shared pool recovery process observation unqualified'
    with pytest.raises(ValueError, match=error):
        observe()  # No recovery marker exists.
    kwargs['action'], api.block = 'recover', 'recovery-fence'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    for row in documents.values():
        if row['kind'] == 'PodList':
            row['items'] = [pod for pod in row['items'] if pod['metadata']['name'] == 'loom-postgres-0']
    checked.clear()
    assert observe().pending_workloads == ()  # Crashed apps cannot prevent fencing.
    assert not checked  # No application or SQL health challenge before restoration.
    api.block = 'recovery-stop'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    key = next(key for key in record['resources'] if roots[key]['kind'] == 'Deployment')
    # A later root cannot borrow pending permission from another root's intent.
    assert observe().pending_workloads == ()
    for selected in stop._sources(bindings, state):
        before = _snapshot(current(selected))
        before['metadata']['uid'] = _uid(current(selected))
        expected = stop._stopped(bindings, before)
        record['resources'][selected].update(status='intent', before=before,
            before_resource_version=current(selected)['metadata']['resourceVersion'], expected=expected)
        private_state._atomic_json(journal, record)
        assert observe().pending_workloads == (selected,)
        replace(selected, expected)
        descendants()
        assert observe().pending_workloads == ()
        record['resources'][selected]['status'] = 'stopped'
        private_state._atomic_json(journal, record)
    # A stale stopped status cannot mask a currently lingering Pod.
    namespace = roots[key]['metadata']['namespace']
    pod_path = '/api/v1/namespaces/' + namespace + '/pods'
    lingering = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'lingering', 'namespace': namespace,
        'uid': str(uuid4()), 'labels': copy.deepcopy(roots[key]['spec']['selector']['matchLabels'])},
        'spec': copy.deepcopy(roots[key]['spec']['template']['spec']), 'status': {'phase': 'Running'}}
    documents[pod_path]['items'].append(lingering)
    with pytest.raises(ValueError, match=error):
        observe()
    documents[pod_path]['items'].remove(lingering)
    api.block = 'recovery-restart'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-restart'
    originals, targets = restart._inputs(bindings, state)
    for selected, original in originals.items():
        replace(selected, original)
    descendants()
    assert observe().pending_workloads == ()
    journal = state / 'recovery-restart/restart.json'
    record = _read(journal)
    from scripts.ops import nebius_shared_pool_recovery_collector as collector

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(collector, 'observe_restored_staging_collector', collection_health)
        for selected, wanted in targets.items():
            changed = _stable(originals[selected]) != _stable(wanted)
            if changed:
                record['resources'][selected].update(status='intent',
                    before_resource_version=current(selected)['metadata']['resourceVersion'])
                private_state._atomic_json(journal, record)
                assert observe().pending_workloads == (selected,)
                replace(selected, wanted)
                if wanted['kind'] == 'Deployment':
                    assert observe().pending_workloads == (selected,)  # Spec alone is not Pod readiness.
                descendants()
                if wanted['kind'] == 'CronJob':
                    mode['collector_ready'] = False
                    assert observe().pending_workloads == (selected,)
                    mode['collector_ready'] = True
                assert observe().pending_workloads == ()
            record['resources'][selected].update(status='started', before_resource_version='701' if changed else None)
            private_state._atomic_json(journal, record)
        assert observe().pending_workloads == ()
        assert collector_calls and ('settings', staging.namespace, 'loom-control-plane') in checked
        assert ('database', staging.namespace, 'loom-service') in checked
        mode['collector_ready'] = False
        with pytest.raises(ValueError, match=error):
            observe()  # Completed restoration cannot borrow another phase's wait.
        mode['collector_ready'] = True
        before = copy.deepcopy(documents)
        def late_backend():
            pod, = (row for row in documents['/api/v1/namespaces/' + staging.namespace + '/pods']['items']
                if row['metadata']['name'] == 'loom-postgres-0')
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['process_hook'] = late_backend
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        documents.clear()
        documents.update(before)
        # A service restart during the earlier collector challenge is not
        # covered by that callback's controller/database checks. The aggregate
        # selected-process readback must reject the changed service too.
        before = copy.deepcopy(documents)
        def late_service():
            pod, = (row for row in documents['/api/v1/namespaces/' + staging.namespace + '/pods']['items']
                if row['metadata']['name'] == 'loom-service-a12b34-test')
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['process_hook'] = late_service
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        documents.clear()
        documents.update(before)
        api.block = None
        assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
        assert observe().pending_workloads == ()  # Terminal replay constructs no active child.
        raw = journal.read_bytes()
        mode['process_hook'] = lambda: journal.write_bytes(raw + b'\n')
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        journal.write_bytes(raw)
        assert observe().pending_workloads == ()
        captured = bindings.plan.staging.originals
        original = copy.deepcopy(captured)
        mode['process_hook'] = captured.clear
        try:
            with pytest.raises(ValueError, match=error):
                observe()  # Caller-owned nested bindings cannot change mid-read.
        finally:
            captured.update(original)
            mode['process_hook'] = None
        _exercise_connected_parent_terminal_recovery(kwargs=kwargs, state=state, documents=documents, mode=mode)


def _exercise_connected_parent_terminal_recovery(*, kwargs, state, documents, mode):
    """Real concrete replay, SQL/process readers and permission reviews; cloud/census I/O are controlled boundaries."""
    import json
    import ssl
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as module
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_shared_pool_handoff_live import (
        SharedPoolWriterObservation,
        _authority_roots,
    )

    bindings = kwargs['bindings']
    request = kwargs['runtime']
    authority = _authority_roots(bindings.plan)
    for row in authority.values():
        row['metadata'].setdefault('resourceVersion', '501')
    reviews = []

    def handle(message):
        if message.method == 'GET':
            plural = message.url.path.rsplit('/', 1)[-1]
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[plural]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '501'}, 'items': [row for row in authority.values() if row['kind'] == kind]})
        assert message.method == 'POST' and message.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
        principal = message.headers['Impersonate-User']
        account_namespace, account = principal.split(':')[2:]
        namespace = json.loads(message.content)['spec']['namespace']
        reviews.append((account_namespace, account, namespace))
        rules = []
        # Simulate Kubernetes' effective rights from this independent fixture
        # inventory; the production validator still sees actual review documents.
        for binding in authority.values():
            if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                continue
            if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
                continue
            if not any(subject['kind'] == 'ServiceAccount' and subject['name'] == account
                    and subject['namespace'] == account_namespace for subject in binding.get('subjects', [])):
                continue
            reference = binding['roleRef']
            selected = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
            rules.extend(copy.deepcopy(authority[selected]['rules']))
        if mode.get('parent_permission_failure') == account:
            rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['deletecollection']})
        return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {},
            'status': {'incomplete': False, 'evaluationError': '', 'resourceRules': rules, 'nonResourceRules': []}})

    async def publication(**options):
        assert options['request'] == request

    async def provider(**options):
        assert options['runtime'] == request and options['development'].shared_bindings == bindings
        return SimpleNamespace(plan=bindings.plan)

    def census(**options):
        assert options['bindings'] == bindings and options['state_dir'] == state
        return SharedPoolWriterObservation(bindings.plan, '501', {}, copy.deepcopy(authority), bindings.input_digest)

    def active_child(**options):
        pytest.fail('terminal replay must not construct active phase adapters')

    operator = state.parent / 'terminal-operator-connection'
    operator.write_bytes(b'private-terminal-operator')
    operator.chmod(0o600)
    context = ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(module, 'qualify_runtime_publication', publication)
        patch.setattr(module, 'observe_shared_pool_provider', provider)
        patch.setattr(module, 'observe_shared_pool_recovery_writers', census)
        for component, name in ((module.recovery_gateway, 'HTTPSSharedRecoveryGateway'),
                (module.recovery_roles, 'HTTPSSharedRecoveryRoles'), (module.restart, 'HTTPSSharedRecoveryRestart')):
            patch.setattr(component, name, active_child)
        with module.HTTPSSharedPoolOperationAPI(bindings=bindings, observation=kwargs['observation'], repo_root=ROOT,
                api_server=request.database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
                token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}) as api:
            api.client.close()
            api.client = type(api.client)(base_url=api.api_server, transport=httpx.MockTransport(handle))
            frame = _read(state / 'operation.json')
            def qualify():
                try:
                    api.qualify(bindings, frame, recovering=True)
                except ValueError as exc:
                    import traceback

                    frames, cause = [], exc
                    while cause is not None:
                        frames.extend(f'{Path(item.filename).name}:{item.lineno}:{item.name}'
                            for item in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('terminal parent failure at ' + ', '.join(frames)) from None
            qualify()
            assert reviews and any(account == 'loom-pool-gateway' for _, account, _ in reviews)
            assert any(account == 'loom-platform' for _, account, _ in reviews)
            for account in ('loom-pool-gateway', 'loom-platform'):
                mode['parent_permission_failure'] = account
                with pytest.raises(ValueError):
                    qualify()
            mode['parent_permission_failure'] = None
            for phase in api.supported_phases:
                with pytest.raises(ValueError):
                    api.advance(phase, bindings, state / phase)
            before = copy.deepcopy(documents)
            def late_dev_backend():
                pod, = (row for row in documents['/api/v1/namespaces/loom-dev/pods']['items']
                    if row['metadata']['name'] == 'loom-postgres-0')
                pod['status']['containerStatuses'][0]['restartCount'] += 1
            mode['process_hook'] = late_dev_backend
            try:
                with pytest.raises(ValueError):
                    qualify()  # All retained DBs must remain pinned across the process/collector reads.
            finally:
                mode['process_hook'] = None
                documents.clear()
                documents.update(before)


def _exercise_scoped_process_observation(*, development, documents, state, calls, catalog_path):
    """Bound API traffic without reusing live readiness or changed history."""
    runtime_state = development.shared_bindings.plan.development.runtime.state_dir
    key = 'Deployment:loom-dev:loom-control-plane'
    workload_path = '/apis/apps/v1/namespaces/loom-dev/deployments/loom-control-plane'

    def running():
        return development._running(key, runtime_state)

    before = len(calls)
    with development.process_observation(state_dir=runtime_state):
        first = running()
        assert running() == first
        development.qualify_runtime_settings(key=key, state_dir=runtime_state)
        development.qualify_runtime_database(key=key, state_dir=runtime_state)
    assert sum(call.url.path == catalog_path for call in calls[before:]) == 2
    assert sum(call.url.path == workload_path for call in calls[before:]) >= 12
    for path in (state / 'forward-guards/guards.json', runtime_state / 'installation.json'):
        raw = path.read_bytes()
        try:
            with pytest.raises(ValueError):
                with development.process_observation(state_dir=runtime_state):
                    running()
                    path.write_bytes(raw + b'\n')
                    running()  # Semantically equal receipt bytes are still changed.
        finally:
            path.write_bytes(raw)
            path.chmod(0o600)
        assert running()[0] == 'controller'  # Failed scopes never leak their view.
    for path in (catalog_path, workload_path):
        saved = copy.deepcopy(documents[path])
        try:
            with pytest.raises(ValueError):
                with development.process_observation(state_dir=runtime_state):
                    running()
                    documents[path]['metadata']['uid'] = str(uuid4())
                    if path == workload_path:
                        running()  # Target identity is checked on every nested read.
                    # Unrelated inventory drift must be rejected at scope exit.
        finally:
            documents[path] = saved
    original = copy.deepcopy(development.shared_bindings.plan.staging.originals)
    try:
        with pytest.raises(ValueError):
            with development.process_observation(state_dir=runtime_state):
                development.shared_bindings.plan.staging.originals.clear()
                running()
    finally:
        development.shared_bindings.plan.staging.originals.update(original)
    with development.process_observation(state_dir=runtime_state):
        with pytest.raises(ValueError):
            with development.process_observation(state_dir=runtime_state):
                pytest.fail('nested observation must not replace the active scope')
        assert running()[0] == 'controller'


def _exercise_forward_processes(*, bindings, development, documents, mode, checked, commands, add, calls, args,
        activation_transport=False, observation_only=False):
    """Real parent, process transports and readiness validators; no live effects."""
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_build_activation as build_child
    from scripts.ops import nebius_shared_pool_database as database
    from scripts.ops import nebius_shared_pool_gateway as gateway_child
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_workloads as workloads
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_stage import _comparison_snapshot, _stage_fixed_documents
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    from loom.nebius_platform_render import digest

    name = 'scripts.ops.nebius_shared_pool_forward_processes'
    assert importlib.util.find_spec(name) is not None, 'phase-bound shared process observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_forward_processes'])
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    class ProcessPhaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, child):
            if phase == 'forward-activate':
                if self.prepare_failure == (phase, 'before'):
                    raise ValueError('simulated preparation interruption')
                activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=child)
            elif phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=child)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=child)
            elif phase == 'forward-database':
                database.prepare_shared_staging_database(bindings=bound, state_dir=child)
            elif phase == 'forward-build-policy':
                build_child.prepare_shared_build_activation(bindings=bound, state_dir=child)
            elif phase == 'forward-gateway':
                gateway_child.prepare_shared_gateway(bindings=bound, state_dir=child)
            else:
                super().prepare(phase, bound, child)

        def validate_prepared_phase(self, phase, child):
            if phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-guards':
                value = guards.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=False)
                assert set(value['guards'].values()) == {'prepared'}
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase,
                    state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-database':
                database.validate_shared_staging_database(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-build-policy':
                build_child.validate_shared_build_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, child)

        def validate_phase(self, phase, child, *, complete):
            if phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=complete)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=child, complete=complete)
            elif phase == 'forward-database':
                database.validate_shared_staging_database(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-build-policy':
                build_child.validate_shared_build_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=complete)
            else:
                super().validate_phase(phase, child, complete=complete)

        def advance(self, phase, bound, child):
            if phase == 'forward-activate':
                record = json.loads((child / 'activation.json').read_bytes())
                return record['opening'] == 'opened' and all(row['release'] == 'released' for row in record['guards'].values())
            if phase == 'forward-guards':
                return set(json.loads((child / 'guards.json').read_bytes())['guards'].values()) == {'held'}
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                return all(row['status'] == 'applied' for row in json.loads((child / 'transition.json').read_bytes())['resources'].values())
            if phase == 'forward-database':
                return json.loads((child / 'database.json').read_bytes())['status'] == 'staged'
            if phase == 'forward-build-policy':
                return json.loads((child / 'build-policy.json').read_bytes())['status'] == 'activated'
            if phase == 'forward-gateway':
                return all(row['status'] == 'applied' for row in json.loads((child / 'start/transition.json').read_bytes())['resources'].values())
            return super().advance(phase, bound, child)

    api = ProcessPhaseAPI(parent)
    api.block = 'forward-roles'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    originals = bindings.plan.staging.originals
    for row in originals.values():
        if row['kind'] == 'CronJob':
            add(row)
        namespace = row['metadata']['namespace']
        documents.setdefault('/apis/batch/v1/namespaces/' + namespace + '/jobs',
            {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '1'}, 'items': []})

    def observe():
        try:
            return module.observe_shared_pool_forward_processes(bindings=bindings, state_dir=state, development=development,
                kubeconfig=Path(development.prefix[development.prefix.index('--kubeconfig') + 1]),
                executable=Path(development.prefix[0]))
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('forward process failure at ' + ', '.join(frames)) from None

    checked.clear()
    reads_before = len(calls)
    assert observe().pending_workloads == ()
    # Each whole inventory reads this unrelated retained ConfigMap. Repeating
    # the full inventory for every nested process challenge exhausted the
    # existing timeout and amplifies real API traffic as components increase.
    catalog_path = next(path for path, row in documents.items() if row['kind'] == 'ConfigMap')
    assert sum(call.url.path == catalog_path for call in calls[reads_before:]) <= 4
    assert {'new-closed', 'old-revoked', ('runtime-role', 'loom-dev')} <= checked
    if observation_only:
        saved = copy.deepcopy(documents)
        inventory_reads = 0
        def late_restart(message):
            nonlocal inventory_reads
            if message.url.path == catalog_path:
                inventory_reads += 1
                if inventory_reads == 4:
                    pod, = (row for row in documents['/api/v1/namespaces/' + manager.binding.namespace + '/pods']['items']
                        if row['metadata']['name'] == 'loom-postgres-0')
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['http_hook'] = late_restart
        try:
            with pytest.raises(ValueError):
                observe()  # The last inventory scan must precede the final DB identity check.
            assert inventory_reads == 4
        finally:
            mode['http_hook'] = None
            documents.clear()
            documents.update(saved)
        _exercise_scoped_process_observation(development=development, documents=documents,
            state=state, calls=calls, catalog_path=catalog_path)
        return
    guard_path = state / 'forward-guards/guards.json'
    guard_record = json.loads(guard_path.read_bytes())
    first = bindings.migration.guards[0]
    mode['held'] = {first.namespace}
    with pytest.raises(ValueError):
        observe()  # An unrecorded held guard is not owned by this operation.
    guard_record['guards'][str(first.participant_id)] = 'acquire_intent'
    private_state._atomic_json(guard_path, guard_record)
    assert observe().guard_states[first.namespace] == 'held'
    for target in bindings.migration.guards:
        mode['held'].add(target.namespace)
        guard_record['guards'][str(target.participant_id)] = 'held'
    private_state._atomic_json(guard_path, guard_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'
    assert observe().pending_workloads == ()

    def path_for(row):
        prefix = '/apis/apps/v1' if row['kind'] == 'Deployment' else '/apis/batch/v1'
        plural = 'deployments' if row['kind'] == 'Deployment' else 'cronjobs'
        return prefix + '/namespaces/' + row['metadata']['namespace'] + '/' + plural + '/' + row['metadata']['name']

    def replace_live(row, wanted):
        identity = {key: row['metadata'][key] for key in ('uid', 'resourceVersion')}
        generation = row['metadata'].get('generation', 1) + 1
        row.clear()
        row.update(copy.deepcopy(wanted))
        row['metadata'].update(**identity, generation=generation)
        row['status'] = {'observedGeneration': generation}

    stop_path = state / 'forward-stop/transition.json'
    stop_record = json.loads(stop_path.read_bytes())
    lingering = None
    for key, wanted in bindings.plan.staging.stopped.items():
        wanted = _comparison_snapshot(wanted)
        stop_record['resources'][key].update(status='intent', expected=wanted)
        private_state._atomic_json(stop_path, stop_record)
        current = documents[path_for(wanted)]
        replace_live(current, wanted)
        namespace = current['metadata']['namespace']
        if current['kind'] == 'Deployment':
            pod_path = '/api/v1/namespaces/' + namespace + '/pods'
            labels = current['spec']['selector']['matchLabels']
            selected = [pod for pod in documents[pod_path]['items']
                if all(pod['metadata'].get('labels', {}).get(k) == v for k, v in labels.items())]
            assert selected
            selected[0]['metadata']['deletionTimestamp'] = '2026-10-10T00:00:00Z'
            assert key in observe().pending_workloads  # Zero replicas alone is not drain.
            lingering = (key, pod_path, copy.deepcopy(selected[0]))
            documents[pod_path]['items'] = [pod for pod in documents[pod_path]['items'] if pod not in selected]
            for replica in documents['/apis/apps/v1/namespaces/' + namespace + '/replicasets']['items']:
                if any(owner['uid'] == current['metadata']['uid'] for owner in replica['metadata'].get('ownerReferences', [])):
                    replica['spec']['replicas'] = 0
                    replica['status'] = {'observedGeneration': replica['metadata']['generation']}
        stop_record['resources'][key]['status'] = 'applied'
        private_state._atomic_json(stop_path, stop_record)
    assert observe().pending_workloads == ()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
    before = len(commands)
    assert observe().pending_workloads == ()
    namespaces = {row['metadata']['namespace'] for row in originals.values()}
    assert not any(row[0] == 'exec' and row[2] in namespaces and row[7] == 'python' for row in commands[before:])
    key, pod_path, pod = lingering
    documents[pod_path]['items'].append(pod)
    with pytest.raises(ValueError):
        observe()  # Once stop is complete, new lingering processes are not a resumable wait.
    documents[pod_path]['items'].remove(pod)
    mode.update(damage='late-stopped-pod', manager_reads=0, late_pod=(pod_path, pod))
    with pytest.raises(ValueError):
        observe()  # A Pod appearing after its drain check invalidates the whole observation.
    documents[pod_path]['items'] = [row for row in documents[pod_path]['items'] if row['metadata']['uid'] != pod['metadata']['uid']]
    mode['damage'] = None
    api.block = 'forward-material'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
    db_path = state / 'forward-database/database.json'
    db_record = json.loads(db_path.read_bytes())
    db_record['status'] = 'staged'
    private_state._atomic_json(db_path, db_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    checked.clear()
    observe()
    assert ('runtime-role', bindings.plan.staging.attachment.guard.namespace) in checked
    api.block = 'forward-build-policy'
    for phase in ('forward-wire', 'forward-start'):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        if phase == 'forward-start':
            _, pod_path, pod = lingering
            documents[pod_path]['items'].append(pod)
            with pytest.raises(ValueError):
                observe()  # A still-stopped resource cannot become pending again during another resource's startup.
            documents[pod_path]['items'].remove(pod)
        journal = state / phase / 'transition.json'
        record = json.loads(journal.read_bytes())
        for key, item in record['resources'].items():
            current = documents[path_for(originals[key])]
            wanted = _comparison_snapshot(bindings.plan.staging_targets[key] if phase == 'forward-wire' else current)
            if phase == 'forward-start':
                wanted['spec']['replicas'] = originals[key]['spec']['replicas']
            item.update(status='intent', expected=wanted)
            private_state._atomic_json(journal, record)
            replace_live(current, wanted)
            if phase == 'forward-start':
                mode['shared_settings'] = True
                assert key in observe().pending_workloads
                runtime = {path_for(current): current}
                populate_runtime_pods(runtime)
                for path, value in runtime.items():
                    if value['kind'] in {'PodList', 'ReplicaSetList'}:
                        # This fixture reuses a deterministic hash: collect an
                        # old zero replica before creating its same-name successor.
                        names = {row['metadata']['name'] for row in value['items']}
                        documents[path]['items'] = [row for row in documents[path]['items'] if row['metadata']['name'] not in names]
                        documents[path]['items'].extend(value['items'])
                        for row in value['items']:
                            if row['kind'] == 'ReplicaSet':
                                add(row)
                            else:
                                row['spec']['nodeName'] = 'platform-test'
            assert key not in observe().pending_workloads
            item['status'] = 'applied'
            private_state._atomic_json(journal, record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    checked.clear()
    assert observe().pending_workloads == ()
    for row in originals.values():
        if row['kind'] == 'Deployment':
            assert ('settings', row['metadata']['namespace'], row['metadata']['name']) in checked
            assert ('database', row['metadata']['namespace'], row['metadata']['name']) in checked
    # The activation variant reuses the real transition setup, not the already
    # covered process fault matrices; its own faults exercise opening/release.
    for damage in (() if activation_transport else (
            'old-active', 'new-not-closed', 'settings:' + bindings.plan.staging.attachment.guard.namespace,
            'database:loom-dev', 'late-staging-actuator', 'late-staging-restart', 'late-dev-restart', 'late-staging-template')):
        baseline = copy.deepcopy(documents)
        mode['damage'] = damage
        mode['manager_reads'] = 0
        with pytest.raises(ValueError):
            observe()
        documents.clear()
        documents.update(baseline)
    mode['damage'] = None
    build_path = state / 'forward-build-policy/build-policy.json'
    build_record = json.loads(build_path.read_bytes())
    build_record['status'] = 'activated'
    private_state._atomic_json(build_path, build_record)
    plan = bindings.plan.development
    build_namespace = '/api/v1/namespaces/' + plan.build_namespace_original['metadata']['name']
    namespace = copy.deepcopy(plan.build_namespace_target)
    namespace['metadata']['uid'] = plan.build_namespace_original['metadata']['uid']
    documents[build_namespace] = namespace
    api.block = 'forward-gateway'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    checked.clear()
    assert observe().pending_workloads == ()
    gateway_key = _key(plan.gateway_original)
    gateway_path = path_for(plan.gateway_original)
    gateway = documents[gateway_path]
    original_gateway = copy.deepcopy(gateway)
    started = _comparison_snapshot(plan.gateway_target)
    replace_live(gateway, started)
    with pytest.raises(ValueError):
        observe()  # A planned but unrecorded start is not process authority.
    gateway.clear()
    gateway.update(original_gateway)
    lingering_gateway = {gateway_path: copy.deepcopy(gateway)}
    lingering_gateway[gateway_path]['spec']['replicas'] = 1
    populate_runtime_pods(lingering_gateway)
    pod_path = '/api/v1/namespaces/' + manager.binding.namespace + '/pods'
    lingering_pod, = lingering_gateway[pod_path]['items']
    documents[pod_path]['items'].append(lingering_pod)
    with pytest.raises(ValueError):
        observe()  # Zero replicas cannot excuse a live gateway descendant.
    documents[pod_path]['items'].remove(lingering_pod)
    fixed, stored = plan.authority, {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def grants(message):
        if message.method == 'GET':
            key = paths[message.url.path]
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        row = json.loads(message.content)
        key = _key(row)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        row['metadata'].update(uid=str(uuid4()), resourceVersion='501')
        if message.url.params.get('dryRun') != 'All':
            assert not message.url.query and key not in stored
            stored[key] = row
        return httpx.Response(201, json=row)

    with gateway_child.HTTPSSharedGateway(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None,
            observe_ready=lambda *_: False) as delivery:
        delivery.client.close()
        delivery.client = type(development.client)(base_url=delivery.api_server, transport=httpx.MockTransport(grants))
        _stage_fixed_documents(documents=fixed, revision=digest(fixed), phase='forward-gateway-authority',
            binding=delivery.binding, api=delivery, state_dir=state / 'forward-gateway/authority')
    start_path = state / 'forward-gateway/start/transition.json'
    start_record = json.loads(start_path.read_bytes())
    start_record['resources'][gateway_key].update(status='intent', expected=started)
    private_state._atomic_json(start_path, start_record)
    assert observe().pending_workloads == ()  # The PATCH may not have committed.
    replace_live(gateway, started)
    checked.clear()
    assert observe().pending_workloads == (gateway_key,)
    assert not any(isinstance(row, tuple) and row[-1] == 'loom-pool-gateway' for row in checked)
    runtime = {gateway_path: gateway}
    populate_runtime_pods(runtime)
    for path, value in runtime.items():
        if value['kind'] in {'PodList', 'ReplicaSetList'}:
            documents[path]['items'].extend(value['items'])
            for row in value['items']:
                if row['kind'] == 'ReplicaSet':
                    add(row)
                else:
                    row['spec']['nodeName'] = 'platform-test'
    checked.clear()
    assert observe().pending_workloads == ()
    assert {(check, manager.binding.namespace, 'loom-pool-gateway')
        for check in ('settings', 'database', 'kubernetes', 'capacity')} <= checked
    mode['gateway_receipt'] = start_path
    for damage in (() if activation_transport else ('gateway-settings', 'gateway-database', 'gateway-kubernetes', 'gateway-capacity',
            'late-gateway-secret', 'late-gateway-restart', 'late-gateway-database-restart', 'late-gateway-receipt',
            'late-manager-secret-after-gateway', 'late-manager-database-after-gateway', 'late-manager-database-at-final-state')):
        baseline, receipt = copy.deepcopy(documents), start_path.read_bytes()
        mode.update(damage=damage, capacity_seen=False, late_injected=False, final_state_reads=0)
        try:
            observe()
        except ValueError:
            pass
        else:
            pytest.fail(f'forward process observation accepted {damage}')
        if damage.startswith('late-manager-'):
            assert mode['late_injected'], damage
        documents.clear()
        documents.update(baseline)
        start_path.write_bytes(receipt)
        start_path.chmod(0o600)
    mode['damage'] = None
    start_record['resources'][gateway_key]['status'] = 'applied'
    private_state._atomic_json(start_path, start_record)
    api.prepare_failure = ('forward-activate', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert observe().pending_workloads == ()
    documents[gateway_path]['status']['readyReplicas'] = 0
    with pytest.raises(ValueError):
        observe()  # A completed startup cannot claim an in-flight readiness wait.
    documents[gateway_path]['status']['readyReplicas'] = 1
    api.prepare_failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    if activation_transport:
        _exercise_shared_activation_transport(bindings=bindings, development=development, state=state,
            mode=mode, checked=checked, parent=parent, kwargs=kwargs, api=api)
        return
    assert observe().pool_state == 'closed'
    mode['pool_state'] = 'global'
    with pytest.raises(ValueError):
        observe()  # A live open pool cannot qualify before its opening intent.
    mode['pool_state'] = 'closed'
    activation_path = state / 'forward-activate/activation.json'
    activation = json.loads(activation_path.read_bytes())
    activation['opening'] = 'intent'
    private_state._atomic_json(activation_path, activation)
    assert observe().pool_state == 'closed'
    mode['pool_state'] = 'global'
    checked.clear()
    assert observe().pool_state == 'global'
    assert 'new-active' in checked and 'new-closed' not in checked
    assert ('active-capacity', manager.binding.namespace, 'loom-pool-gateway') in checked
    activation['opening'] = 'opened'
    for target in bindings.migration.guards:
        key = str(target.participant_id)
        activation['guards'][key]['release'] = 'intent'
        private_state._atomic_json(activation_path, activation)
        assert observe().guard_states[target.namespace] == 'held'
        mode['held'].remove(target.namespace)
        assert observe().guard_states[target.namespace] == 'open'
        activation['guards'][key]['release'] = 'released'
        private_state._atomic_json(activation_path, activation)
    api.block = 'forward-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert observe().pool_state == 'global'
    for damage in ('new-not-active', 'guard-foreign', 'gateway-active-capacity', 'old-active'):
        mode['damage'] = damage
        with pytest.raises(ValueError):
            observe()
    mode['damage'] = None
    guard_path.unlink()
    before = len(commands)
    with pytest.raises(ValueError):
        observe()
    assert len(commands) == before


def _exercise_shared_activation_transport(*, bindings, development, state, mode, checked, parent, kwargs, api):
    """Actual process/SQL/guard transports under the shared opening controller.

    The preceding helper provides real child receipts and remote-I/O doubles.
    The parent's separate writer/provider qualification is a boundary double
    here; it must be connected in the full protected parent, not claimed by this
    test. An unhealthy actual runtime must fail even when that boundary passes.
    """
    import importlib.util

    from scripts.ops.nebius_shared_pool_activation import advance_shared_pool_activation

    name = 'scripts.ops.nebius_shared_pool_activation_live'
    assert importlib.util.find_spec(name) is not None, 'concrete shared opening transport is missing'
    module = __import__(name, fromlist=['SharedPoolActivationTransport'])
    qualification = []
    mode['effects'] = []

    def qualify():
        qualification.append('writers-provider')
        if mode.get('qualification_failure'):
            raise ValueError('simulated lost parent writer/provider qualification')

    transport = module.SharedPoolActivationTransport(bindings=bindings, development=development, qualify=qualify)
    child = state / 'forward-activate'
    roster = tuple(str(row.participant_id) for row in bindings.migration.guards)
    for call in (transport.open_pool, lambda: transport.release_guard(roster[0]),
            transport.fence_pool, lambda: transport.fence_guard(roster[0])):
        with pytest.raises(ValueError):
            call()
    assert mode['effects'] == []
    mode['qualification_failure'] = True
    with pytest.raises(ValueError):
        advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert qualification and mode['effects'] == []
    mode['qualification_failure'] = False
    mode['damage'] = 'gateway-capacity'
    with pytest.raises(ValueError):
        advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == []
    mode['damage'] = None
    mode['release_failure'] = roster[0]
    assert not advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['pool_state'] == 'global'
    assert mode['effects'] == [('open', None), ('release', roster[0])]
    # Unknown release remains observation-only even after the transport returns.
    assert not advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == [('open', None), ('release', roster[0])]
    mode['held'].remove(bindings.migration.guards[0].namespace)  # Delayed commit becomes observable.
    mode['release_failure'] = None
    checked.clear()
    assert advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == [('open', None), *(('release', key) for key in roster)]
    assert not mode['held'] and 'new-active' in checked and 'new-closed' not in checked
    assert any(isinstance(row, tuple) and row[0] == 'active-capacity' for row in checked)
    api.block = 'forward-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    with pytest.raises(ValueError):
        transport.open_pool()  # Completed child is no longer dispatch authority.
    assert mode['effects'] == [('open', None), *(('release', key) for key in roster)]


@pytest.mark.timeout(180)
def test_shared_activation_binds_own_parent_and_never_replays_unknown_writes(shared_operation):
    """Real phase journals; only preceding phases and remote transactions doubled.

    This checks shared-parent authority and one-dispatch ordering, not installed
    runtime readiness or the concrete database transport's SQL qualification.
    """
    import importlib.util
    import json

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent

    name = 'scripts.ops.nebius_shared_pool_activation'
    assert importlib.util.find_spec(name) is not None, 'shared-parent activation connection is missing'
    module = __import__(name, fromlist=['advance_shared_pool_activation'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'forward-activate'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    effects = []

    class Remote:
        migration = bindings.migration
        mode = 'closed'
        guards = dict.fromkeys(roster, 'held')
        failure = ('open', None, 'before')
        qualified = False
        damage = None
        ready = True

        def verify_retained(self):
            if not self.qualified:
                raise ValueError('simulated current authority loss')

        def qualify_shared_state(self):
            self.verify_retained()
            if not self.ready:
                raise ValueError('simulated unhealthy shared runtime')

        def read_workload(self, key):
            pytest.fail('shared activation must not reconstruct old startup workloads')

        def qualify_runtime(self):
            self.verify_retained()
            assert self.mode == 'closed' and set(self.guards.values()) == {'held'}

        def pool_state(self):
            if self.damage == 'receipt-bytes':
                path = child / 'activation.json'
                path.write_bytes(path.read_bytes() + b'\n')
            elif self.damage == 'receipt-loss':
                (child / 'activation.json').unlink()
            return self.mode

        def guard_state(self, participant):
            return self.guards[participant]

        def _write(self, operation, participant=None):
            record = json.loads((child / 'activation.json').read_bytes())
            frame = json.loads((state / 'operation.json').read_bytes())
            assert frame['forward']['forward-activate']['status'] == 'started'
            assert not (Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json').exists()
            if operation == 'open':
                assert record['opening'] == 'intent'
                assert self.mode == 'closed' and set(self.guards.values()) == {'held'}
            else:
                assert operation == 'release'
                assert self.mode == 'global' and record['opening'] == 'opened'
                assert record['guards'][participant] == {'release': 'intent', 'fence': 'prepared'}
                assert self.guards[participant] == 'held'
            effects.append((operation, participant))
            if self.failure == (operation, participant, 'before'):
                raise TimeoutError('simulated lost transaction before acknowledgement')
            if operation == 'open':
                self.mode = 'global'
            else:
                self.guards[participant] = 'open'
            if self.failure == (operation, participant, 'after'):
                raise TimeoutError('simulated committed transaction reply loss')

        def open_pool(self):
            self._write('open')

        def release_guard(self, participant):
            self._write('release', participant)

        def fence_pool(self):
            pytest.fail('forward child cannot acquire recovery authority')

        def fence_guard(self, participant):
            pytest.fail('forward child cannot acquire recovery authority')

    remote = Remote()

    class API(SharedPhaseAPI):
        interrupt_preparation = True

        def prepare(self, phase, bound, directory):
            if phase == 'forward-activate':
                module.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
                if self.interrupt_preparation:
                    self.interrupt_preparation = False
                    raise TimeoutError('simulated loss after local preparation')
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-activate':
                module.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-activate':
                module.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-activate':
                return module.advance_shared_pool_activation(bindings=bound, api=remote, state_dir=directory)
            return super().advance(phase, bound, directory)

    api = API(parent)
    api.block = 'forward-verify'
    kwargs = dict(**args, bindings=bindings, api=api, action='install')
    with pytest.raises(FileNotFoundError):
        module.prepare_shared_pool_activation(bindings=bindings, state_dir=child)
    assert not child.exists()
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    assert json.loads((child / 'activation.json').read_bytes())['opening'] == 'prepared'
    assert json.loads((state / 'operation.json').read_bytes())['forward']['forward-activate']['status'] == 'prepared'
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('closed',)
    assert view.guard_states == {target.namespace: ('held',) for target in bindings.migration.guards}
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []  # Preparation resumed, but live qualification failed.
    remote.qualified = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]  # An unknown opening is never replayed.
    assert module.shared_forward_activation_view(bindings=bindings, state_dir=state).pool_states == ('closed', 'global')
    remote.migration = bindings.plan.staging.legacy.context.request.fencing.retirement.migration
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Never observe or write through another installation's transport.
    remote.migration = bindings.migration
    retained = (child / 'activation.json').read_bytes()
    for damage in ('receipt-bytes', 'receipt-loss'):
        remote.damage = damage
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert effects == [('open', None)]
        (child / 'activation.json').write_bytes(retained)
        (child / 'activation.json').chmod(0o600)
    remote.damage = None
    for loss in ('missing', 'roster', 'released-before-opening', 'recovery'):
        record = json.loads(retained)
        if loss == 'missing':
            (child / 'activation.json').unlink()
        else:
            if loss == 'roster':
                del record['guards'][roster[0]]
            elif loss == 'released-before-opening':
                record['guards'][roster[0]]['release'] = 'released'
            else:
                record['cancellation'] = 'intent'
            private_state._atomic_json(child / 'activation.json', record)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert effects == [('open', None)]
        (child / 'activation.json').write_bytes(retained)
        (child / 'activation.json').chmod(0o600)
    remote.mode = 'global'  # A later read settles the first opening.
    remote.qualified = False
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Global alone is insufficient authority.
    remote.qualified = True
    remote.ready = False
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('open', None)]  # Valid history alone cannot release unhealthy runtimes.
    remote.ready = True
    remote.failure = ('release', roster[0], 'before')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None), ('release', roster[0])]
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('global',)
    assert view.guard_states == {target.namespace: (('held', 'open') if str(target.participant_id) == roster[0] else ('held',))
        for target in bindings.migration.guards}
    remote.guards[roster[0]] = 'open'
    remote.failure = ('release', roster[1], 'after')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    assert set(remote.guards.values()) == {'open'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    module.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=True)
    view = module.shared_forward_activation_view(bindings=bindings, state_dir=state)
    assert view.pool_states == ('global',) and view.guard_states == {target.namespace: ('open',) for target in bindings.migration.guards}
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'shared_pool_handoff_complete'
    assert module.shared_forward_activation_view(bindings=bindings, state_dir=state).frame['outcome'] == 'shared_pool_handoff_complete'


@pytest.mark.timeout(420)
@pytest.mark.parametrize('forward_census', [False, True, 'recovery', 'participant'],
    ids=['initial', 'forward', 'recovery', 'participant'])
def test_shared_workload_census_binds_current_roots_and_rejects_unaccounted_writers(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, shared_operation, forward_census,
        publisher_cloud):
    """A historical template or a stopped clone must not hide a live writer."""
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory
    from tests.ops.test_nebius_pool_cutover import writer_descendant

    request, parent = shared_runtime
    _, documents, _ = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    kinds = {'Deployment': 'deployments', 'StatefulSet': 'statefulsets', 'CronJob': 'cronjobs',
        'DaemonSet': 'daemonsets', 'ReplicaSet': 'replicasets', 'ReplicationController': 'replicationcontrollers',
        'Job': 'jobs', 'Pod': 'pods'}
    rbac_kinds = {'Role': 'roles', 'ClusterRole': 'clusterroles', 'RoleBinding': 'rolebindings',
        'ClusterRoleBinding': 'clusterrolebindings'}
    kinds.update(rbac_kinds)
    roots = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.request.platform_consumers,
        *staging_history.completion.workloads.values(), *documents.values(), *staging_observation['resources'])
        if row['kind'] in {'Deployment', 'StatefulSet', 'CronJob'}}
    inventory = {plural: [] for plural in kinds.values()}
    authority = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.original.retained.values(),
        *staging_history.context.predecessor.retained.values(), *documents.values(),
        *staging_observation['resources'], *staging_history.context.request.platform_authority.resources)
        if row['kind'] in rbac_kinds}
    gateway_stage = json.loads((Path(staging_history.context.operation['state_dir']) / 'authority/stage.json').read_bytes())
    retired_gateway_roles = set()
    for key, item in gateway_stage['resources'].items():
        row = copy.deepcopy(item['observed'])
        row['metadata']['uid'] = item['uid']
        if row['kind'] == 'Role':
            retired_gateway_roles.add(key)
            for rule in row['rules']:
                rule['verbs'] = [verb for verb in rule['verbs'] if verb not in {'create', 'delete'}]
        authority[key] = row
    assert retired_gateway_roles
    for row in authority.values():
        row['metadata']['resourceVersion'] = '401'
        inventory[kinds[row['kind']]].append(row)
    for row in roots.values():
        row['metadata']['resourceVersion'] = '401'
        inventory[kinds[row['kind']]].append(row)
    controller = roots['Deployment:' + staging_observation['namespace'] + ':loom-control-plane']
    replica = writer_descendant(controller, 'ReplicaSet')
    inventory['replicasets'].append(replica)
    inventory['pods'].append(writer_descendant(replica, 'Pod'))
    # Do not hide the installation's real, ownerless one-shot Jobs. They keep
    # their recorded UID/template; only API status and child Pods are supplied.
    jobs = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.original.retained.values(),
        *staging_history.context.predecessor.retained.values(), *documents.values()) if row['kind'] == 'Job'}
    management_jobs = [row for row in jobs.values() if row['metadata']['namespace'] == 'loom-nebius-management-dev']
    assert any(row['metadata']['name'].startswith('loom-management-migrate-') for row in management_jobs)
    assert any(row['metadata']['name'].startswith('loom-management-backup-') for row in management_jobs)
    for job in jobs.values():
        job['metadata']['resourceVersion'] = '401'
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'active': 0, 'succeeded': 1}
        pod = writer_descendant(job, 'Pod')
        pod['metadata']['labels'] = copy.deepcopy(job['spec']['template']['metadata']['labels'])
        pod['status'] = {'phase': 'Succeeded', **{states: [{'name': row['name'],
            'state': {'terminated': {'exitCode': 0}}} for row in pod['spec'].get(containers, [])]
            for containers, states in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses'))}}
        inventory['jobs'].append(job)
        inventory['pods'].append(pod)
    calls, mode = [], {'revision': '500'}

    def respond(message):
        calls.append(message)
        assert message.method == 'GET'
        if message.url.path == '/api/v1/namespaces/kube-system':
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': 'kube-system', 'uid': request.database.manager.retained.request.registration.binding.kube_system_uid}})
        plural = message.url.path.rsplit('/', 1)[-1]
        kind, = (kind for kind, name in kinds.items() if name == plural)
        version = message.url.params.get('resourceVersion')
        if plural != 'roles':
            assert version == '500' and message.url.params['resourceVersionMatch'] == 'Exact'
        if plural == 'pods' and mode.get('during_read') is not None:
            mode.pop('during_read')()
        return httpx.Response(200, json={'apiVersion': 'v1' if plural in {'pods', 'replicationcontrollers'}
            else 'batch/v1' if plural in {'jobs', 'cronjobs'} else 'rbac.authorization.k8s.io/v1'
            if kind in rbac_kinds else 'apps/v1', 'kind': kind + 'List',
            'metadata': {'resourceVersion': mode['revision'] if plural == 'pods' else '500', 'continue': ''},
            'items': inventory[plural]})

    with ManagementKubernetesTransport(api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as transport:
        transport.client.close()
        transport.client = type(completed_pool[3].api.client)(base_url=transport.api_server,
            transport=httpx.MockTransport(respond))
        if forward_census:
            _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode,
                recovery=forward_census == 'recovery', participant=forward_census == 'participant',
                publisher_cloud=publisher_cloud)
            return
        from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_writers

        def observe():
            return observe_shared_pool_writers(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, transport=transport)

        result = observe()
        assert result.resource_version == '500'
        assert result.workloads.keys() == roots.keys()
        assert result.workloads[_key(controller)] == controller
        gateway, = (row for row in staging_history.completion.workloads.values()
            if row['metadata']['name'] == 'loom-pool-gateway')
        assert result.workloads[_key(gateway)]['spec']['replicas'] == 0
        original = copy.deepcopy(inventory)
        # Completed Jobs and Pods may have been garbage-collected normally.
        # No absent historical object becomes a required deployment target.
        job_uids = {row['metadata']['uid'] for row in inventory['jobs']}
        inventory['jobs'] = []
        inventory['pods'] = [row for row in inventory['pods']
            if not any(owner['uid'] in job_uids for owner in row['metadata'].get('ownerReferences', []))]
        assert observe().workloads == result.workloads
        original_capture = copy.deepcopy(staging_observation['resources'])
        for damage in ('clone', 'cronjob', 'root_uid', 'old_template', 'revived_gateway', 'missing_root', 'revision',
                'foreign_writer', 'foreign_cron_writer', 'group_writer', 'foreign_subject', 'revived_gateway_role',
                'cross_namespace', 'missing_role', 'captured_writer', 'unrecorded_job', 'job_uid', 'job_template',
                'job_nonterminal', 'job_active', 'job_pod_running', 'job_pod_sidecar', 'job_pod_unowned',
                'job_absent_with_pod'):
            inventory.clear()
            inventory.update(copy.deepcopy(original))
            staging_observation['resources'] = copy.deepcopy(original_capture)
            if damage.startswith('job_') or damage == 'unrecorded_job':
                job = next(row for row in inventory['jobs'] if row['metadata']['namespace'] == 'loom-nebius-management-dev')
                pod, = (row for row in inventory['pods']
                    if row['metadata'].get('ownerReferences', [{}])[0].get('uid') == job['metadata']['uid'])
                if damage == 'unrecorded_job':
                    job['metadata']['name'] += '-unrecorded'
                    pod['metadata']['ownerReferences'][0]['name'] = job['metadata']['name']
                elif damage == 'job_uid':
                    uid = str(uuid4())
                    job['metadata']['uid'] = uid
                    job['spec']['selector']['matchLabels']['batch.kubernetes.io/controller-uid'] = uid
                    for labels in (job['spec']['template']['metadata']['labels'], pod['metadata']['labels']):
                        for label in ('controller-uid', 'batch.kubernetes.io/controller-uid'):
                            labels[label] = uid
                    pod['metadata']['ownerReferences'][0]['uid'] = uid
                elif damage == 'job_template':
                    job['spec']['template']['spec']['containers'][0]['command'] = ['sleep', 'infinity']
                elif damage == 'job_nonterminal':
                    job['status']['conditions'] = []
                elif damage == 'job_active':
                    job['status']['active'] = 1
                elif damage == 'job_pod_running':
                    pod['status']['phase'] = 'Running'
                elif damage == 'job_pod_sidecar':
                    pod['spec']['initContainers'] = [{'name': 'live-sidecar', 'restartPolicy': 'Always', 'image': 'test'}]
                    pod['status']['initContainerStatuses'] = [{'name': 'live-sidecar', 'state': {'running': {}}}]
                elif damage == 'job_absent_with_pod':
                    inventory['jobs'].remove(job)
                else:
                    pod['metadata']['ownerReferences'] = []
            elif damage == 'captured_writer':
                namespace = staging_history.context.inputs.installation.participants[0].execution_namespace.name
                clone = copy.deepcopy(controller)
                clone['metadata'].update(name='captured-foreign-writer', namespace=namespace, uid=str(uuid4()))
                clone['spec']['template']['spec']['serviceAccountName'] = 'captured-foreign-writer'
                role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
                    'metadata': {'name': 'captured-foreign-writer', 'namespace': namespace,
                        'uid': str(uuid4()), 'resourceVersion': '401'},
                    'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']}]}
                binding = {'apiVersion': role['apiVersion'], 'kind': 'RoleBinding',
                    'metadata': {'name': 'captured-foreign-writer', 'namespace': namespace,
                        'uid': str(uuid4()), 'resourceVersion': '401'},
                    'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'captured-foreign-writer'},
                    'subjects': [{'kind': 'ServiceAccount', 'name': 'captured-foreign-writer', 'namespace': namespace}]}
                # A capture is observation, not authority: adding the same
                # unrendered writer to both inputs must not legitimize it.
                for row in (clone, role, binding):
                    staging_observation['resources'].append(copy.deepcopy(row))
                    inventory[kinds[row['kind']]].append(row)
            elif damage in {'foreign_writer', 'foreign_cron_writer', 'group_writer', 'cross_namespace'}:
                role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
                    'metadata': {'name': 'unexpected-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
                    'rules': [{'apiGroups': ['batch'], 'resources': ['cronjobs' if damage == 'foreign_cron_writer' else 'jobs'],
                        'verbs': ['create']}]}
                binding = {'apiVersion': role['apiVersion'], 'kind': 'ClusterRoleBinding',
                    'metadata': {'name': 'unexpected-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
                    'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': 'unexpected-writer'},
                    'subjects': [{'kind': 'ServiceAccount', 'name': 'foreign-account', 'namespace': 'outside-pool'}]}
                if damage == 'group_writer':
                    binding['subjects'] = [{'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Group', 'name': 'system:authenticated'}]
                elif damage == 'cross_namespace':
                    binding['kind'] = 'RoleBinding'
                    binding['metadata']['namespace'] = 'outside-pool'
                    actuator, = (row for row in roots.values() if row['metadata']['name'] == 'loom-execution-actuator'
                        and row['metadata']['namespace'] == staging_history.context.inputs.installation.participants[0].execution_namespace.name)
                    binding['subjects'] = [{'kind': 'ServiceAccount', 'name': 'loom-execution-actuator',
                        'namespace': actuator['metadata']['namespace']}]
                inventory['clusterroles'].append(role)
                inventory[kinds[binding['kind']]].append(binding)
            elif damage == 'foreign_subject':
                binding = inventory['rolebindings'][0]
                binding['subjects'].append({'kind': 'ServiceAccount', 'name': 'foreign-account', 'namespace': 'outside-pool'})
            elif damage == 'revived_gateway_role':
                role = next(row for row in inventory['roles'] if _key(row) in retired_gateway_roles)
                role['rules'][0]['verbs'].append('create')
            elif damage == 'missing_role':
                inventory['roles'].pop()
            elif damage == 'clone':
                clone = copy.deepcopy(inventory['pods'][0])
                clone['metadata'].update(name='unexpected-writer', uid=str(uuid4()), ownerReferences=[])
                inventory['pods'].append(clone)
            elif damage == 'cronjob':
                rogue = copy.deepcopy(next(row for row in inventory['cronjobs']
                    if row['metadata']['name'] == 'loom-execution-capacity-collector'))
                rogue['metadata'].update(name='unexpected-schedule', uid=str(uuid4()))
                rogue['spec']['suspend'] = True
                rogue['spec']['jobTemplate']['spec']['template']['spec']['serviceAccountName'] = 'foreign-account'
                inventory['cronjobs'].append(rogue)
            elif damage == 'missing_root':
                inventory['deployments'] = [row for row in inventory['deployments'] if _key(row) != _key(controller)]
            elif damage == 'revision':
                mode['revision'] = '501'
            else:
                key = _key(gateway) if damage == 'revived_gateway' else _key(controller)
                row, = (row for row in inventory['deployments'] if _key(row) == key)
                if damage == 'root_uid':
                    row['metadata']['uid'] = str(uuid4())
                elif damage == 'old_template':
                    row['spec'] = copy.deepcopy(staging_history.completion.workloads[key]['spec'])
                    assert row['spec'] != controller['spec']
                else:
                    row['spec']['replicas'] = 1
            with pytest.raises(ValueError, match='shared pool writer census unqualified'):
                observe()
            mode['revision'] = '500'
        staging_observation['resources'] = original_capture
        assert calls and all(row.method == 'GET' for row in calls)


def _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode, *,
        recovery=False, participant=False, publisher_cloud=None):
    """Observe real parent/child receipts; remote transitions are fixture state.

    Accepting an unrecorded successor, an old template after completion, or a
    changed receipt during the census must fail independently of the mutator.
    """
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_gateway as gateway_child
    from scripts.ops import nebius_shared_pool_guards as guard_child
    from scripts.ops import nebius_shared_pool_material as material
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_roles as roles
    from scripts.ops import nebius_shared_pool_workloads as workloads
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_stage import _comparison_snapshot, _stage_fixed_documents

    from loom.nebius_platform_render import digest

    name = 'scripts.ops.nebius_shared_pool_forward_census'
    assert importlib.util.find_spec(name) is not None, 'phase-bound shared writer census is missing'
    module = __import__(name, fromlist=['observe_shared_pool_forward_writers'])
    if recovery:
        assert importlib.util.find_spec('scripts.ops.nebius_shared_pool_recovery_census') is not None, 'recovery-bound writer census is missing'
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    class CensusPhaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, child):
            if phase == 'forward-guards':
                guard_child.prepare_shared_pool_guards(bindings=bound, state_dir=child)
            elif phase == 'forward-activate':
                activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=child)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=child)
            elif phase == 'forward-roles':
                roles.prepare_shared_staging_roles(bindings=bound, state_dir=child)
            elif phase == 'forward-material':
                material.prepare_shared_staging_material(bindings=bound, state_dir=child)
            elif phase == 'forward-gateway':
                gateway_child.prepare_shared_gateway(bindings=bound, state_dir=child)
            else:
                super().prepare(phase, bound, child)

        def validate_prepared_phase(self, phase, child):
            if phase == 'forward-guards':
                guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=False)
            elif phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase,
                    state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-roles':
                roles.validate_shared_staging_roles(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-material':
                material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, child)

        def validate_phase(self, phase, child, *, complete):
            if phase == 'forward-guards':
                guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=child, complete=complete)
            elif phase == 'forward-roles':
                roles.validate_shared_staging_roles(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-material':
                material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=complete)
            else:
                super().validate_phase(phase, child, complete=complete)

        def advance(self, phase, bound, child):
            if phase == 'forward-guards':
                path = child / 'guards.json'
                record = json.loads(path.read_bytes())
                record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                private_state._atomic_json(path, record)
                return True
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                return all(row['status'] == 'applied' for row in json.loads((child / 'transition.json').read_bytes())['resources'].values())
            if phase == 'forward-roles':
                return all(status == 'restricted' for status in json.loads((child / 'roles.json').read_bytes())['roles'].values())
            if phase == 'forward-material':
                child_state = material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False)
                return child_state['stage'] is not None and all(row['status'] == 'created'
                    for row in child_state['stage']['resources'].values())
            if phase == 'forward-gateway':
                return all(row['status'] == 'applied' for row in json.loads((child / 'start/transition.json').read_bytes())['resources'].values())
            if phase == 'forward-activate':
                return getattr(self, 'activation_complete', False)
            return super().advance(phase, bound, child)

    api = CensusPhaseAPI(parent)
    api.block = 'forward-database'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'

    def observe():
        try:
            return module.observe_shared_pool_forward_writers(bindings=bindings, state_dir=state, transport=transport)
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('census failure at ' + ', '.join(frames)) from None

    initial = observe()
    assert initial.resource_version == '500'
    journal = state / 'forward-stop/transition.json'
    stopped = json.loads(journal.read_bytes())
    key = next(key for key, row in bindings.plan.staging.originals.items() if row['kind'] == 'Deployment')
    actual = next(row for row in inventory['deployments'] if _key(row) == key)
    original = copy.deepcopy(actual)
    expected = _comparison_snapshot(bindings.plan.staging.stopped[key])

    def replace_live(actual, wanted):
        identity = {field: actual['metadata'][field] for field in ('uid', 'resourceVersion')}
        actual.clear()
        actual.update(copy.deepcopy(wanted))
        actual['metadata'].update(identity)

    replace_live(actual, expected)
    with pytest.raises(ValueError):
        observe()  # No intent exists yet: matching a planned target is insufficient.
    replace_live(actual, original)
    stopped['resources'][key].update(status='intent', expected=expected)
    private_state._atomic_json(journal, stopped)
    assert observe().workloads[key] == original
    replace_live(actual, expected)
    assert observe().workloads[key] == actual  # A lost PATCH reply permits either recorded side.
    actual['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
    with pytest.raises(ValueError):
        observe()
    replace_live(actual, expected)
    raw = journal.read_bytes()
    journal.unlink()
    before = len(calls)
    with pytest.raises(ValueError):
        observe()
    assert len(calls) == before
    journal.write_bytes(raw)
    journal.chmod(0o600)
    mode['during_read'] = journal.unlink
    with pytest.raises(ValueError):
        observe()  # Started children are not covered by completed-child byte pins.
    journal.write_bytes(raw)
    journal.chmod(0o600)

    for key, target in bindings.plan.staging.stopped.items():
        wanted = _comparison_snapshot(target)
        stopped['resources'][key].update(status='applied', expected=wanted)
        row = next(row for row in inventory[kinds[target['kind']]] if _key(row) == key)
        replace_live(row, wanted)
    private_state._atomic_json(journal, stopped)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
    observe()
    key = min(bindings.plan.staging_role_originals)
    role = next(row for row in inventory['roles'] if _key(row) == key)
    original_role = copy.deepcopy(role)
    target_role = bindings.plan.staging_role_targets[key]
    role_path = state / 'forward-roles/roles.json'
    role_record = json.loads(role_path.read_bytes())
    replace_live(role, target_role)
    with pytest.raises(ValueError):
        observe()
    role_record['roles'][key] = 'intent'
    private_state._atomic_json(role_path, role_record)
    assert observe().authority[key] == role
    replace_live(role, original_role)
    assert observe().authority[key] == original_role
    for key, target in bindings.plan.staging_role_targets.items():
        role_record['roles'][key] = 'restricted'
        row = next(row for row in inventory['roles'] if _key(row) == key)
        replace_live(row, target)
    private_state._atomic_json(role_path, role_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
    observe()
    successor = copy.deepcopy(role)
    replace_live(role, original_role)
    with pytest.raises(ValueError):
        observe()  # Completion must not retain the old write grant as an alternative.
    replace_live(role, successor)
    reader = copy.deepcopy(next(iter(bindings.plan.staging_reader_authority.values())))
    reader['metadata'].update(uid=str(uuid4()), resourceVersion='401')
    inventory[kinds[reader['kind']]].append(reader)
    with pytest.raises(ValueError):
        observe()  # Even planned new authority is foreign until its retained create intent.
    inventory[kinds[reader['kind']]].remove(reader)

    api.block = 'forward-build-policy'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    observe()
    fixed = {**bindings.plan.staging_material, **bindings.plan.staging_reader_authority}
    stored = {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def material_request(message):
        if message.method == 'GET':
            key = paths[message.url.path]
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        row = json.loads(message.content)
        key = _key(row)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        row['metadata'].update(uid=str(uuid4()), resourceVersion='401')
        if message.url.params.get('dryRun') != 'All':
            assert not message.url.query and key not in stored
            stored[key] = row
            if key in bindings.plan.staging_reader_authority or key in bindings.plan.development.authority:
                inventory[kinds[row['kind']]].append(row)
                observe()  # The child still has create_intent, including an unobserved UID.
        return httpx.Response(201, json=row)

    with material.HTTPSSharedStagingMaterial(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: observe()) as delivery:
        delivery.client.close()
        delivery.client = type(transport.client)(base_url=delivery.api_server, transport=httpx.MockTransport(material_request))
        try:
            assert material.advance_shared_staging_material(bindings=bindings, api=delivery, state_dir=state / 'forward-material')
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('census material integration failed at ' + ', '.join(frames), pytrace=False)
    result = observe()
    assert set(bindings.plan.staging_reader_authority) <= result.authority.keys()
    for key in bindings.plan.staging_reader_authority:
        row = stored[key]
        original_uid = row['metadata']['uid']
        row['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError):
            observe()  # A completed create no longer permits a same-name replacement.
        row['metadata']['uid'] = original_uid

    for phase in ('forward-wire', 'forward-start'):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        observe()
        path = state / phase / 'transition.json'
        record = json.loads(path.read_bytes())
        for key, item in record['resources'].items():
            row = next(row for row in inventory['deployments'] if _key(row) == key)
            wanted = _comparison_snapshot(bindings.plan.staging_targets[key] if phase == 'forward-wire' else row)
            if phase == 'forward-start':
                wanted['spec']['replicas'] = bindings.plan.staging.originals[key]['spec']['replicas']
            item.update(status='intent', expected=wanted)
            private_state._atomic_json(path, record)
            replace_live(row, wanted)
            observe()
            item['status'] = 'applied'
            private_state._atomic_json(path, record)
        observe()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    observe()
    api.block = 'forward-gateway'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    observe()  # Started gateway child still permits only its stopped predecessor.
    gateway = bindings.plan.development.gateway_original
    gateway_key = _key(gateway)
    live_gateway = next(row for row in inventory['deployments'] if _key(row) == gateway_key)
    stopped_gateway = copy.deepcopy(live_gateway)
    started_gateway = _comparison_snapshot(bindings.plan.development.gateway_target)
    replace_live(live_gateway, started_gateway)
    before = len(calls)
    with pytest.raises(ValueError):
        observe()  # Planned startup alone is not retained dispatch intent.
    assert len(calls) > before
    replace_live(live_gateway, stopped_gateway)
    fixed = bindings.plan.development.authority
    untracked = copy.deepcopy(next(iter(fixed.values())))
    untracked['metadata'].update(uid=str(uuid4()), resourceVersion='401')
    inventory[kinds[untracked['kind']]].append(untracked)
    with pytest.raises(ValueError):
        observe()  # Even a source-qualified grant is forbidden before its intent.
    inventory[kinds[untracked['kind']]].remove(untracked)
    stored = {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}
    with gateway_child.HTTPSSharedGateway(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None,
            observe_ready=lambda *_: False) as delivery:
        delivery.client.close()
        delivery.client = type(transport.client)(base_url=delivery.api_server, transport=httpx.MockTransport(material_request))
        _stage_fixed_documents(documents=fixed, revision=digest(fixed), phase='forward-gateway-authority',
            binding=delivery.binding, api=delivery, state_dir=state / 'forward-gateway/authority')
    assert set(fixed) <= observe().authority.keys()
    for row in stored.values():
        original_uid = row['metadata']['uid']
        row['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError):
            observe()  # Created grants must keep their recorded UID.
        row['metadata']['uid'] = original_uid
    path = state / 'forward-gateway/start/transition.json'
    record = json.loads(path.read_bytes())
    record['resources'][gateway_key].update(status='intent', expected=started_gateway)
    private_state._atomic_json(path, record)
    assert observe().workloads[gateway_key] == stopped_gateway
    replace_live(live_gateway, started_gateway)
    assert observe().workloads[gateway_key] == live_gateway
    live_gateway['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
    with pytest.raises(ValueError):
        observe()
    replace_live(live_gateway, started_gateway)
    for receipt in ('gateway.json', 'authority/stage.json', 'authority/dispatch.json', 'start/transition.json'):
        selected = state / 'forward-gateway' / receipt
        raw = selected.read_bytes()
        selected.unlink()
        before = len(calls)
        with pytest.raises(ValueError):
            observe()
        assert len(calls) == before
        selected.write_bytes(raw)
        selected.chmod(0o600)
    raw = path.read_bytes()
    mode['during_read'] = path.unlink
    with pytest.raises(ValueError):
        observe()
    path.write_bytes(raw)
    path.chmod(0o600)
    record['resources'][gateway_key]['status'] = 'applied'
    private_state._atomic_json(path, record)
    observe()
    replace_live(live_gateway, stopped_gateway)
    with pytest.raises(ValueError):
        observe()  # Completed startup no longer admits the stopped predecessor.
    replace_live(live_gateway, started_gateway)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert observe().workloads[gateway_key] == live_gateway
    activation_path = state / 'forward-activate/activation.json'
    activation_bytes = activation_path.read_bytes()
    for mutation in ('bytes', 'missing'):
        def change(mutation=mutation):
            if mutation == 'bytes':
                activation_path.write_bytes(activation_bytes + b'\n')
            else:
                activation_path.unlink()
        mode['during_read'] = change
        with pytest.raises(ValueError):
            observe()
        activation_path.write_bytes(activation_bytes)
        activation_path.chmod(0o600)
    if participant:
        record = json.loads(activation_bytes)
        record['opening'] = 'opened'
        for row in record['guards'].values():
            row['release'] = 'released'
        private_state._atomic_json(activation_path, record)
        api.activation_complete = True
        assert parent.run_shared_pool_operation(**kwargs)['status'] == 'shared_pool_handoff_complete'
        _exercise_participant_writer_census(args, bindings, state, api, transport, inventory, kinds, mode, publisher_cloud)
        return
    if recovery:
        _exercise_recovery_writer_census(kwargs, state, transport, inventory, kinds, calls, mode, replace_live, CensusPhaseAPI)
        return
    api.block = 'recovery-fence'
    assert parent.run_shared_pool_operation(**{**kwargs, 'action': 'recover'})['phase'] == 'recovery-fence'
    before = len(calls)
    with pytest.raises(ValueError):
        observe()
    assert len(calls) == before


def _exercise_participant_writer_census(args, bindings, state, parent_api, transport, inventory, kinds, mode, publisher_cloud):
    """Normal release must retain the entire shared pool, not just its resources."""
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
    from scripts.ops.nebius_pool_participant_observation import HTTPSParticipantResourceObserver
    from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
    from scripts.ops.nebius_pool_participant_resource import (
        HTTPSParticipantResourceAPI,
        RetainedCandidateFileWriter,
        _resource_path,
    )
    from scripts.ops.nebius_shared_pool_operation import load_completed_shared_pool_operation
    from tests.ops.nebius_participant_external_fixture import (
        external_connection,
        publication_fixture,
    )

    from loom.nebius_platform_render import build_platform

    assert importlib.util.find_spec('scripts.ops.nebius_pool_participant_entry') is not None, 'protected release qualification is not composed'
    from scripts.ops.nebius_pool_participant_entry import ParticipantReleaseQualifier
    from scripts.ops import nebius_pool_participant_entry as participant_entry

    assert hasattr(participant_entry, 'connected_participant_release'), 'protected candidate and pool delivery are not composed'

    predecessor = load_completed_shared_pool_operation(**args, bindings=bindings, api=parent_api)
    attached = bindings.plan.staging.attachment
    spec = predecessor.migration.registration.spec
    old_files = build_platform(attached.config, predecessor.migration.registration.candidate,
        attached.runtime_profile.model_dump(mode='json'), spec.profiles.image_admission_keyring, repo_root=ROOT)
    current = {_key(row): copy.deepcopy(row) for rows in old_files.values() for row in rows}
    for row in current.values():
        row['metadata'].update(uid=str(uuid4()), resourceVersion='401')
    current.update({_key(row): copy.deepcopy(row) for row in attached.resources})
    for rows in inventory.values():
        for row in rows:
            if _key(row) in current:
                current[_key(row)] = copy.deepcopy(row)
    manager = next(row for row in inventory['deployments']
        if row['metadata']['namespace'] == predecessor.migration.registration.binding.namespace
        and row['metadata']['name'] == 'loom-service')
    catalog = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {
        'name': 'loom-pool-profiles-' + spec.operation_id.hex,
        'namespace': manager['metadata']['namespace'], 'uid': str(uuid4()), 'resourceVersion': '401'},
        'immutable': True, 'data': {'profiles.json': spec.profiles.model_dump_json()}}
    candidate = copy.deepcopy(predecessor.migration.registration.candidate)
    candidate.update(schema_version='loom.nebius-candidate.v1', candidate_sha='d' * 40,
        run_id=812, source_archive_sha256='sha256:' + 'a' * 64)
    profile = attached.runtime_profile.model_copy(update={'candidate_sha': candidate['candidate_sha']})
    publication, published_payload = publication_fixture(candidate, profile)
    release_inputs = dict(predecessor=predecessor, current=tuple(current.values()),
        config=attached.config, candidate=candidate, runtime_profile=profile, operation_id=uuid4(),
        management_origin='https://' + args['runtime'].database.manager.deployment.public_host, repo_root=ROOT,
        rendered_files=build_platform(attached.config, candidate, profile.model_dump(mode='json'),
            spec.profiles.image_admission_keyring, repo_root=ROOT),
        source=PreparedDevelopmentSource(source_sha=candidate['candidate_sha'],
            source_archive_sha256=candidate['source_archive_sha256']), publication=publication,
        manager=manager, catalog=catalog, state_root=state.parent / 'participant-releases',
        anchor_dir=state.parent / 'participant-anchors')
    retained = retain_participant_rollout(**release_inputs)
    resources = {**current, _key(manager): manager, _key(catalog): catalog}
    writes = []
    reviews = []

    def respond(message):
        if message.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            namespace, account = message.headers['Impersonate-User'].removeprefix('system:serviceaccount:').split(':')
            assert set(message.headers.get_list('Impersonate-Group')) == {
                'system:serviceaccounts', 'system:serviceaccounts:' + namespace, 'system:authenticated'}
            selected = json.loads(message.content)['spec']['namespace']
            reviews.append((namespace, account, selected))
            rules = []
            catalog = {_key(row): row for plural in ('roles', 'clusterroles') for row in inventory[plural]}
            for binding in (*inventory['rolebindings'], *inventory['clusterrolebindings']):
                if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != selected:
                    continue
                if not any(row['kind'] == 'ServiceAccount' and (row['namespace'], row['name']) == (namespace, account)
                        for row in binding.get('subjects', [])):
                    continue
                reference = binding['roleRef']
                key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(catalog[key]['rules']))
            if mode.get('effective_write'):
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['foreign-job']})
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'resourceRules': rules, 'nonResourceRules': [], 'incomplete': False, 'evaluationError': ''}})
        plural = message.url.path.rsplit('/', 1)[-1]
        if plural in {'rolebindings', 'clusterrolebindings'} and 'resourceVersion' not in message.url.params:
            assert message.method == 'GET'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': 'RoleBindingList' if plural == 'rolebindings' else 'ClusterRoleBindingList',
                'metadata': {'resourceVersion': '500', 'continue': ''}, 'items': inventory[plural]})
        if plural in kinds.values() or message.url.path == '/api/v1/namespaces/kube-system':
            return transport.client.send(message)
        row = next((row for row in resources.values() if _resource_path(row) == message.url.path), None)
        if message.method == 'GET':
            return httpx.Response(200, json=row) if row is not None else httpx.Response(404)
        assert message.method == 'PATCH' and row is not None
        target = copy.deepcopy(row)
        for patch in json.loads(message.content):
            parts = [part.replace('~1', '/').replace('~0', '~') for part in patch['path'].strip('/').split('/')]
            owner = target
            for part in parts[:-1]:
                owner = owner[part]
            if patch['op'] == 'test':
                assert owner[parts[-1]] == patch['value']
            elif patch['op'] == 'remove':
                del owner[parts[-1]]
            else:
                assert patch['op'] in {'add', 'replace'}
                owner[parts[-1]] = patch['value']
        if not message.url.params:
            writes.append(_key(row))
            target['metadata']['resourceVersion'] = str(int(row['metadata']['resourceVersion']) + 1)
            resources[_key(row)] = target
            if row['kind'] in kinds:
                inventory[kinds[row['kind']]] = [target if _key(item) == _key(row) else item
                    for item in inventory[kinds[row['kind']]]]
        else:
            assert dict(message.url.params) == {'dryRun': 'All'}
        return httpx.Response(200, json=target)

    def connect(api):
        api.client.close()
        api.client = type(transport.client)(base_url=transport.api_server, transport=httpx.MockTransport(respond))
        return api

    with connect(HTTPSParticipantResourceObserver(retained=retained, api_server=transport.api_server,
            ssl_context=ssl.create_default_context())) as observer:
        assert hasattr(observer, 'observe_writers'), 'normal rollout does not qualify the complete shared writer set'

        def observe(preparing=None):
            try:
                return observer.observe_writers(bindings=bindings, shared_state=state, preparing=preparing)
            except ValueError as error:
                import traceback

                frames, cause = [], error
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('participant census unqualified; preserve evidence; ' + ', '.join(frames)) from None

        result = observe()
        assert not writes
        assert result.workloads[_key(manager)] == manager
        assert any(row['metadata']['namespace'] == 'loom-dev' for row in result.workloads.values())
        mode['effective_write'] = True
        with pytest.raises(ValueError, match='preserve'):
            observe()
        mode.pop('effective_write')
        assert reviews, 'RBAC objects alone do not prove effective permissions'

        # A newly captured source snapshot cannot replace an already retained
        # actuator UID, even if all individual resource reads agree with it.
        changed = copy.deepcopy(release_inputs)
        changed['operation_id'] = uuid4()
        actuator = next(row for row in changed['current'] if row['kind'] == 'Deployment'
            and row['metadata']['namespace'] == attached.config['execution_namespace']
            and row['metadata']['name'] == 'loom-execution-actuator')
        actuator['metadata']['uid'] = str(uuid4())
        substituted = retain_participant_rollout(**changed)
        actuator_key = _key(actuator)
        saved = resources[actuator_key]
        resources[actuator_key] = actuator
        saved_inventory = copy.deepcopy(inventory['deployments'])
        inventory['deployments'] = [actuator if _key(row) == actuator_key else row for row in inventory['deployments']]
        try:
            with connect(HTTPSParticipantResourceObserver(retained=substituted, api_server=transport.api_server,
                    ssl_context=ssl.create_default_context())) as replaced:
                with pytest.raises(ValueError, match='preserve'):
                    replaced.observe_writers(bindings=bindings, shared_state=state)
        finally:
            resources[actuator_key] = saved
            inventory['deployments'] = saved_inventory

        def resource(key):
            return connect(HTTPSParticipantResourceAPI(retained=retained, key=key,
                api_server=transport.api_server, ssl_context=ssl.create_default_context(),
                qualify_resource=lambda _retained, selected: observe(selected)))

        writer = RetainedCandidateFileWriter(retained=retained, open_resource=resource,
            open_creation=lambda _: pytest.fail('unexpected creation'))
        writer.apply_file('20-database.yaml')
        key = 'StatefulSet:' + attached.config['namespace'] + ':loom-postgres'
        assert observe().workloads[key] == resources[key]
        assert len(writes) == 2
        snapshot = copy.deepcopy(inventory)
        for damage in ('cloned-controller', 'revived-collector', 'other-owner', 'extra-grant', 'old-template'):
            inventory.clear()
            inventory.update(copy.deepcopy(snapshot))
            if damage == 'cloned-controller':
                clone = copy.deepcopy(next(row for row in inventory['deployments']
                    if row['metadata']['namespace'] == attached.config['namespace'] and row['metadata']['name'] == 'loom-control-plane'))
                clone['metadata'].update(name='foreign-controller', uid=str(uuid4()))
                clone['spec']['replicas'] = 0
                inventory['deployments'].append(clone)
            elif damage == 'revived-collector':
                next(row for row in inventory['cronjobs'] if row['metadata']['namespace'] == attached.config['execution_namespace'])['spec']['suspend'] = False
            elif damage == 'other-owner':
                next(row for row in inventory['deployments'] if row['metadata']['namespace'] == 'loom-dev')['spec']['replicas'] = 0
            elif damage == 'extra-grant':
                next(row for row in inventory['roles'] if row['metadata']['namespace'] == attached.config['execution_namespace'])['rules'].append(
                    {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
            else:
                inventory['statefulsets'] = [current[key] if _key(row) == key else row for row in inventory['statefulsets']]
            with pytest.raises(ValueError, match='preserve'):
                observe()
            assert len(writes) == 2
        inventory.clear()
        inventory.update(snapshot)
        record = retained.state_dir / 'candidate-file-writes/dispatch.json'
        raw = record.read_bytes()
        mode['during_read'] = lambda: record.write_bytes(raw + b'\n')
        with pytest.raises(ValueError, match='preserve'):
            observe()
        record.write_bytes(raw)
        assert observe().workloads[key] == resources[key]

        # Exercise the real fence/catalog/manager/open/capacity children with
        # this same whole-pool qualifier, not the former resource-only callback.
        from tests.ops.test_nebius_pool_participant_adapter import connection as pool_connection

        with pytest.MonkeyPatch.context() as patch:
            import base64
            import hmac
            import subprocess
            from types import SimpleNamespace
            from urllib.parse import parse_qs, urlsplit

            from pydantic import PostgresDsn
            from scripts.ops.nebius_management_material import _documents as manager_material
            from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND
            from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
            from scripts.ops.nebius_pool_runtime_settings import (
                BOUND_POOL_SETTINGS_COMMAND,
                expected_pool_runtime_settings,
            )
            from scripts.ops.nebius_pool_startup_database import pool_participant_authority_sql
            from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
            from tests.ops.test_nebius_development_pool_retained import (
                populate_database_backends,
                populate_runtime_pods,
            )

            from loom_service.pool_management.capacity import digest

            patch.setattr(httpx, 'Client', type(transport.client))
            pool_state, kube, connect_pool = pool_connection.__wrapped__(release_inputs, patch)
            injected = set()
            manager_history = bindings.plan.development.runtime.request.database.manager.retained.request.retained
            material = json.loads(manager_history.files[Path(manager_history.operation['state_dir']) / 'bootstrap/material/material.json'])
            secret = manager_material(material['material'], manager_history.binding, material['operation_id'])['loom-platform-db']
            secret['metadata'].update(uid=material['resources']['loom-platform-db']['uid'], resourceVersion='701')
            resources[_key(secret)] = secret
            target = _gateway_history_target(bindings, secret)
            database_rows = {_resource_path(row): copy.deepcopy(row) for row in (target.database.statefulset, target.database.service)}
            populate_database_backends(database_rows)
            gateway = copy.deepcopy(next(row for row in inventory['deployments']
                if row['metadata']['namespace'] == target.namespace and row['metadata']['name'] == 'loom-pool-gateway'))
            gateway_rows = {_resource_path(gateway): gateway}
            populate_runtime_pods(gateway_rows)
            for plural in ('pods', 'replicasets'):
                inventory[plural][:] = [row for row in inventory[plural] if not (
                    row['metadata']['namespace'] == target.namespace
                    and row['metadata'].get('labels', {}).get('app.kubernetes.io/name') == 'loom-pool-gateway')]
                children = next(row['items'] for row in gateway_rows.values()
                    if row['kind'] == ('PodList' if plural == 'pods' else 'ReplicaSetList'))
                for row in children:
                    row['metadata']['resourceVersion'] = '501'
                inventory[plural].extend(children)
            gateway_pod, = next(row['items'] for row in gateway_rows.values() if row['kind'] == 'PodList')
            gateway_replica, = next(row['items'] for row in gateway_rows.values() if row['kind'] == 'ReplicaSetList')
            kubeconfig = state.parent / 'participant-registry-kubeconfig'
            kubeconfig.write_text('private-test-only')
            kubeconfig.chmod(0o600)
            registry_mode, registry_reads, manager_checks = {}, [], []

            def execute(command, **options):
                assert command[:3] == ['/usr/bin/kubectl', '--kubeconfig', str(kubeconfig)]
                assert options['timeout'] == 40 and options['check'] is False
                offset = next(index for index, value in enumerate(command) if value in {'get', 'exec'})
                selected = command[offset:]
                if selected[0] == 'exec' and selected[2:4] != [target.namespace, 'pod/loom-postgres-0']:
                    is_gateway = selected[3] == 'pod/' + gateway_pod['metadata']['name']
                    pod = gateway_pod if is_gateway else pool_state.children[1]['items'][0]
                    assert selected[:9] == ['exec', '-n', target.namespace, 'pod/' + pod['metadata']['name'],
                        '-c', 'gateway' if is_gateway else 'loom-service', '--', 'python', '-c']
                    script, component, nonce, challenge = selected[9:]
                    if script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        assert is_gateway
                        namespaces = {ns.name: str(ns.uid) for participant in retained.release.refresh.before.participants
                            for ns in (participant.execution_namespace, participant.build_namespace)}
                        assert json.loads(component) == sorted(namespaces)
                        machine, = (row for row in retained.release.refresh.before.machines if row.role == 'gateway')
                        wanted = expected_pool_runtime_settings('gateway', gateway, token_sha256=machine.token_sha256)
                        projection = {key: wanted[key] for key in ('pool_id', 'installation_id', 'machine_id', 'admission_epoch', 'kubernetes')}
                        payload = json.dumps({**projection, 'namespaces': namespaces}, sort_keys=True, separators=(',', ':')).encode()
                    elif script == _BOUND_DATABASE_COMMAND:
                        assert component == ('gateway' if is_gateway else 'service')
                        container, = pool_state.manager['spec']['template']['spec']['containers']
                        reference, = (row['valueFrom']['secretKeyRef'] for row in container['env'] if row['name'] == 'LOOM_SVC_DB_URL')
                        url = base64.b64decode(secret['data'][reference['key']]).decode()
                        payload = (url if is_gateway else str(PostgresDsn(url))).encode()
                    else:
                        assert script == BOUND_POOL_SETTINGS_COMMAND and component == ('gateway' if is_gateway else 'manager')
                        if is_gateway:
                            machine, = (row for row in retained.release.refresh.before.machines if row.role == 'gateway')
                            wanted = expected_pool_runtime_settings('gateway', gateway, token_sha256=machine.token_sha256)
                        else:
                            profiles = (retained.release.refresh.after if 'manager' in pool_state.effects else retained.release.refresh.before).profiles
                            wanted = expected_pool_runtime_settings('manager', pool_state.manager,
                                catalog_sha256=hashlib.sha256(profiles.model_dump_json().encode()).hexdigest())
                        payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
                    manager_checks.append(component)
                    assert challenge == hmac.new(bytes.fromhex(nonce), payload, hashlib.sha256).hexdigest()
                    result = {'status': 'unqualified' if registry_mode.get('manager-settings') else 'qualified'}
                    if is_gateway and registry_mode.get('gateway-kubernetes') and script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        result['status'] = 'unqualified'
                    if is_gateway and registry_mode.get('gateway-restart'):
                        pod['status']['containerStatuses'][0]['restartCount'] += 1
                    if is_gateway and registry_mode.get('gateway-credential') and script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        secret['metadata']['resourceVersion'] = '702'
                    if registry_mode.get('manager-restart'):
                        status = pod['status']['containerStatuses'][0]
                        status['restartCount'] = status.get('restartCount', 0) + 1
                    if registry_mode.get('manager-history'):
                        path = retained.state_dir / 'pool-refresh/dispatch.json'
                        path.write_bytes(path.read_bytes() + b'\n')
                    if registry_mode.get('manager-connection'):
                        kubeconfig.write_text('changed-test-connection')
                    if registry_mode.get('manager-credential') and component == 'manager':
                        secret['metadata']['resourceVersion'] = '702'
                elif selected[0] == 'exec':
                    assert selected[:-1] == ['exec', '-n', target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                        'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
                    assert selected[-1] == pool_participant_authority_sql(retained.release.refresh)
                    registry_reads.append(selected)
                    current_state = ('complete' if 'open' in pool_state.effects else 'staged' if 'stage' in pool_state.effects else
                        'fenced' if 'fence' in pool_state.effects else 'ready')
                    result = {'schema': 'loom.pool-participant-authority.v1',
                        'operation_id': str(retained.reference.operation_id), 'pool_id': str(spec.pool_id),
                        'before_sha256': digest(retained.release.refresh.before.model_dump(mode='json')),
                        'after_sha256': digest(retained.release.refresh.after.model_dump(mode='json')),
                        'read_only': True, 'state': registry_mode.get('state', current_state)}
                    if registry_mode.get('bad-report'):
                        result['operation_id'] = str(uuid4())
                    if registry_mode.get('history'):
                        path = retained.state_dir / 'pool-refresh/dispatch.json'
                        path.write_bytes(path.read_bytes() + b'\n')
                    if registry_mode.get('connection'):
                        kubeconfig.write_text('changed-test-connection')
                    if registry_mode.get('credential'):
                        secret['metadata']['resourceVersion'] = '702'
                    if registry_mode.get('restart'):
                        listing = next(row for row in database_rows.values() if row['kind'] == 'PodList')
                        listing['items'][0]['status']['containerStatuses'][0]['restartCount'] = len(registry_reads)
                elif selected[1] == 'namespace':
                    name = selected[2]
                    if name == 'kube-system':
                        uid = retained.inputs.previous.registration.binding.kube_system_uid
                    else:
                        assert name == target.namespace
                        uid = str(target.namespace_uid)
                    result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
                elif selected[1] == '--raw':
                    parsed = urlsplit(selected[2])
                    if parsed.path.endswith('/replicasets'):
                        result = copy.deepcopy(pool_state.children[0])
                        result['items'].append(copy.deepcopy(gateway_replica))
                    else:
                        result = copy.deepcopy(database_rows[parsed.path])
                    if result['kind'] == 'PodList':
                        result['items'].extend(copy.deepcopy(pool_state.children[1]['items']))
                        result['items'].append(copy.deepcopy(gateway_pod))
                    if 'labelSelector' in parse_qs(parsed.query):
                        label, value = parse_qs(parsed.query)['labelSelector'][0].split('=', 1)
                        result['items'] = [row for row in result['items'] if row['metadata']['labels'].get(label) == value]
                    if registry_mode.get('backend') and result['kind'] == 'EndpointSliceList':
                        result['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
                elif selected[1] in {'deployment', 'replicaset'}:
                    if selected[2] == gateway['metadata']['name']:
                        result = gateway
                    elif selected[2] == gateway_replica['metadata']['name']:
                        result = gateway_replica
                    else:
                        result = pool_state.manager if selected[1] == 'deployment' else pool_state.children[0]['items'][0]
                    assert selected[2:] == [result['metadata']['name'], '-n', target.namespace, '-o', 'json']
                    result = copy.deepcopy(result)
                    if registry_mode.get('manager-pending') and selected[1] == 'deployment':
                        result['status']['readyReplicas'] = 0
                else:
                    kind = selected[1]
                    assert kind in {'statefulset', 'service', 'secret'}
                    assert selected[2:] == ['loom-platform-db' if kind == 'secret' else 'loom-postgres', '-n', target.namespace, '-o', 'json']
                    result = (secret if kind == 'secret' else database_rows[_resource_path(
                        target.database.statefulset if kind == 'statefulset' else target.database.service)])
                return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

            patch.setattr(subprocess, 'run', execute)

            def registry():
                try:
                    return observer.observe_registry(bindings=bindings, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
                except ValueError as error:
                    import traceback

                    frames, cause = [], error
                    while cause is not None:
                        frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                            for frame in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('participant registry unqualified; preserve evidence; ' + ', '.join(frames)) from None

            assert registry() == 'ready'
            def manager_process(phase='guarded'):
                return observer.observe_manager_process(bindings=bindings, kubeconfig=kubeconfig,
                    executable=Path('/usr/bin/kubectl'), phase=phase)

            def gateway_process():
                try:
                    return observer.observe_gateway_process(bindings=bindings, shared_state=state,
                        kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
                except ValueError as error:
                    import traceback

                    frames, cause = [], error
                    while cause is not None:
                        frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                            for frame in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('gateway process unqualified; preserve evidence; ' + ', '.join(frames)) from None

            from tests.ops.test_nebius_pool_participant_processes import peer_process_connection

            selected_guard = {'status': 'held'}
            peer_process, peer_fault = peer_process_connection(observer=observer, bindings=bindings,
                shared_state=state, inventory=inventory, resources=resources, kubeconfig=kubeconfig,
                patch=patch, fallback=execute, selected_guard=selected_guard)
            peer_result = peer_process()
            assert peer_result['status'] == 'available'
            external, external_fault, external_mode, external_calls, provider_reads, collector_key, reader_key, operator = external_connection(
                observer=observer, bindings=bindings, shared_state=state, resources=resources,
                publisher_cloud=publisher_cloud, patch=patch, payload=published_payload)
            capacity = external()
            assert capacity.node_count == capacity.ready_node_count == capacity.target_node_count == 0
            assert manager_process() == ()
            assert gateway_process() is None
            registry_mode['manager-pending'] = True
            with pytest.raises(ValueError, match='preserve'):
                manager_process()
            registry_mode.clear()
            for foreign in ('fenced', 'staged', 'complete'):
                registry_mode['state'] = foreign
                with pytest.raises(ValueError, match='preserve'):
                    registry()
            registry_mode.clear()

            def qualify_pool(_phase):
                nonlocal injected
                resources.update(pool_state.api.resources)
                for row in pool_state.api.resources.values():
                    if row['kind'] in kinds:
                        collection = inventory[kinds[row['kind']]]
                        collection[:] = [other for other in collection if _key(other) != _key(row)] + [row]
                descendants = (*pool_state.children[0]['items'], *pool_state.children[1]['items'], *pool_state.pods.values())
                for plural in ('replicasets', 'pods'):
                    inventory[plural][:] = [row for row in inventory[plural] if row['metadata']['uid'] not in injected]
                injected = {row['metadata']['uid'] for row in descendants}
                for row in descendants:
                    inventory[kinds[row['kind']]].append(row)
                observe()
                registry()
                if _phase == 'ready':
                    assert manager_process('ready') == ()
                    assert gateway_process() is None
                    assert peer_process()['status'] == 'available'
                    external()

            pool_state.qualify = qualify_pool
            pool = connect_pool()
            assert pool.activate(kube, retained.release) == 'ready'
            assert registry() == 'staged'
            assert gateway_process() is None  # A fenced participant is not a gateway failure.
            registry_mode['manager-settings'] = True
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            registry_mode.clear()
            peer_fault['mode'] = 'settings'
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            peer_fault.clear()
            external_fault('checks')
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            external_fault(None)
            pool.finish(kube, retained.release)
            pool.read_capacity(kube, retained.release, observation_id=uuid4())
            assert pool_state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity']
            assert observe().workloads[_key(manager)] == pool_state.manager
            assert registry() == 'complete'
            assert manager_process('ready') == ()
            record = retained.state_dir / 'pool-refresh/dispatch.json'
            before = record.read_bytes()
            for damage in ('manager-settings', 'manager-restart', 'manager-history', 'manager-connection', 'manager-credential', 'backend'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    manager_process('ready')
                record.write_bytes(before)
                kubeconfig.write_text('private-test-only')
                secret['metadata']['resourceVersion'] = '701'
                registry_mode.clear()
            registry_mode['manager-pending'] = True
            assert manager_process() == (_key(manager),)
            with pytest.raises(ValueError, match='preserve'):
                manager_process('ready')
            registry_mode.clear()
            assert manager_process('released') == ()
            assert {'service', 'manager'} <= set(manager_checks)
            for damage in ('gateway-kubernetes', 'gateway-restart', 'gateway-credential'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    gateway_process()
                secret['metadata']['resourceVersion'] = '701'
                registry_mode.clear()
            assert gateway_process() is None
            for damage in ('database', 'pending', 'restart', 'credential', 'telemetry-binding', 'telemetry-restart'):
                peer_fault['mode'] = damage
                with pytest.raises(ValueError, match='preserve'):
                    peer_process()
                peer_fault.clear()
            peer_fault['mode'] = 'telemetry-unavailable'
            unavailable = peer_process()
            assert unavailable['status'] == 'unavailable'
            assert unavailable['checks'] >= 2 and unavailable['unavailable'] == unavailable['checks']
            assert unavailable['reasons'] == ['kubelet_network']
            peer_fault.clear()
            for damage in ('artifact', 'deleting', 'cluster'):
                external_fault(damage)
                with pytest.raises(ValueError, match='preserve'):
                    external()
            for condition in ('exhausted', 'scaling'):
                external_fault(condition)
                observed = external()
                if condition == 'exhausted':
                    assert observed.used_nodes == observed.quota_nodes
                else:
                    assert observed.autoscaler_state == 'scaling'
            external_fault(None)
            registry_keys = {key for key, row in resources.items() if row['kind'] == 'Secret'
                and row['metadata'].get('labels', {}).get('loom.nebius/development-material-purpose') == 'registry'}
            assert registry_keys
            collector_config, = (key for key, row in resources.items() if row['kind'] == 'ConfigMap'
                and row.get('data', {}).get('LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE') == 'pool')
            for key in (collector_key, reader_key, collector_config, *sorted(registry_keys)):
                original = copy.deepcopy(resources[key])
                reads_before = len(provider_reads)
                resources[key]['metadata']['uid'] = str(uuid4())
                with pytest.raises(ValueError, match='preserve'):
                    external()
                assert len(provider_reads) == reads_before
                resources[key] = original
            original_reader = copy.deepcopy(resources[reader_key])
            external_mode['hook'] = lambda: resources[reader_key]['metadata'].update(uid=str(uuid4()))
            with pytest.raises(ValueError, match='preserve'):
                external()
            resources[reader_key] = original_reader
            original_connection = observer.api_server
            external_mode['hook'] = lambda: setattr(observer, 'api_server', 'https://foreign.invalid')
            reads_before = len(provider_reads)
            with pytest.raises(ValueError, match='preserve'):
                external()
            assert len(provider_reads) == reads_before
            observer.api_server = original_connection
            original = operator.read_bytes()
            external_mode['hook'] = lambda: operator.write_bytes(original + b' ')
            with pytest.raises(ValueError, match='preserve'):
                external()
            operator.write_bytes(original)
            assert external_calls and provider_reads
            original_database_rows = copy.deepcopy(database_rows)
            for damage in ('backend', 'bad-report', 'history', 'connection', 'credential', 'restart'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    registry()
                record.write_bytes(before)
                kubeconfig.write_text('private-test-only')
                secret['metadata']['resourceVersion'] = '701'
                database_rows.clear()
                database_rows.update(copy.deepcopy(original_database_rows))
                registry_mode.clear()
            assert registry_reads and registry() == 'complete'

            # The production composed qualifier, consumed by the actual pool
            # adapter, must run all real observer families on this same world.
            # Only Kubernetes/HTTP/SDK transport is doubled above.
            from scripts.ops.nebius_pool_participant_completion import initialize_participant_head

            initialize_participant_head(initial=predecessor, state_root=retained.state_dir.parent,
                anchor_dir=retained.anchor_dir)
            kube = SimpleNamespace(kubeconfig=kubeconfig, context=None)
            gate = ParticipantReleaseQualifier(retained=retained, initial=predecessor, bindings=bindings,
                shared_state=state, observer=observer, kube=kube, kubeconfig=kubeconfig,
                executable=Path('/usr/bin/kubectl'), operator_credentials=operator,
                private_files={kubeconfig: kubeconfig.read_bytes(), operator: operator.read_bytes()})
            connected = connect_pool()
            connected.qualifier = gate
            effects = list(pool_state.effects)
            connected.under_guard(kube, retained.release)
            assert pool_state.effects == effects
            for damage in ('guard', 'publication', 'selected-process', 'connection'):
                if damage == 'guard':
                    selected_guard['status'] = 'foreign'
                elif damage == 'publication':
                    external_fault('checks')
                elif damage == 'selected-process':
                    peer_fault['mode'] = 'settings'
                else:
                    kubeconfig.write_text('foreign-connection')
                with pytest.raises(ValueError, match='preserve'):
                    connected.under_guard(kube, retained.release)
                assert pool_state.effects == effects
                selected_guard['status'] = 'held'
                external_fault(None)
                peer_fault.clear()
                kubeconfig.write_text('private-test-only')
            peer_fault['mode'] = 'telemetry-unavailable'
            connected.under_guard(kube, retained.release)
            peer_fault.clear()
            assert pool_state.effects == effects

            # Socket transport alone is replaced here: the protected connection
            # must construct the real qualifier, candidate APIs and pool adapter.
            # A lost guard must stop both existing-resource and Job access.
            patch.setattr(httpx, 'HTTPTransport', lambda **_: httpx.MockTransport(respond))
            with participant_entry.connected_participant_release(
                    retained=retained, initial=predecessor, expected=predecessor,
                    bindings=bindings, shared_state=state, kube=kube, kubeconfig=kubeconfig,
                    executable=Path('/usr/bin/kubectl'), operator_credentials=operator,
                    private_files={kubeconfig: kubeconfig.read_bytes(), operator: operator.read_bytes()},
                    ssl_context=ssl.create_default_context(), token='test-operator-token') as delivery:
                before_writes = list(writes)
                delivery.pool.under_guard(kube, retained.release)
                delivery.writer.apply_file('20-database.yaml')
                with delivery.writer.open_creation('pre-upgrade-backup') as api:
                    assert api.client.headers['Authorization'] == 'Bearer test-operator-token'
                    assert api.get_resource(api.document) is None
                selected_guard['status'] = 'foreign'
                with pytest.raises(ValueError, match='preserve'):
                    delivery.writer.apply_file('20-database.yaml')
                with delivery.writer.open_creation('pre-upgrade-backup') as api:
                    with pytest.raises(ValueError, match='preserve'):
                        api.get_resource(api.document)
                selected_guard['status'] = 'held'
                assert writes == before_writes and pool_state.effects == effects
                observer_client = delivery.qualifier.observer.client
            assert observer_client.is_closed


def _exercise_recovery_writer_census(kwargs, state, transport, inventory, kinds, calls, mode, replace_live, forward_api):
    """Real phase journals and cluster census; remote recovery effects are fixture state."""
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_census as census
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_reopening as reopening
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable

    bindings = kwargs['bindings']
    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (reopening.prepare_shared_recovery_reopening, reopening.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (reopening.prepare_shared_recovery_verification, reopening.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(forward_api):
        def prepare(self, phase, bound, directory):
            if phase in children:
                children[phase][0](bindings=bound, state_dir=directory)
            elif phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in children:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
            elif phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase in children:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-fence':
                path = directory / 'fence.json'
                record = _read(path)
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                private_state._atomic_json(path, record)
                return True
            if phase == 'recovery-drain':
                path = directory / 'drain.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'observed'})
                return True
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                return all(row['status'] == 'restricted' for row in _read(directory / 'gateway.json')['roles'].values())
            if phase in {'recovery-reopen', 'recovery-verify'}:
                record = _read(directory / children[phase][2])
                return record['status'] == ('released' if phase == 'recovery-reopen' else 'observed')
            if phase in children:
                record = _read(directory / children[phase][2])
                return all(row['status'] in {'stopped', 'restored', 'started'}
                    for row in record.get('resources', record.get('roles', {})).values())
            return super().advance(phase, bound, directory)

    def observe():
        return census.observe_shared_pool_recovery_writers(bindings=bindings, state_dir=state, transport=transport)

    def live(key):
        return next(row for rows in inventory.values() for row in rows if _key(row) == key)

    api = API(parent)
    api.block = 'recovery-fence'
    kwargs.update(api=api, action='recover')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
    initial = observe()
    assert initial.resource_version == '500'
    gateway_key = _key(bindings.plan.development.gateway_original)
    assert initial.workloads[gateway_key]['spec']['replicas'] == 1
    api.block = 'recovery-reopen'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    for index, key in enumerate(stop._sources(bindings, state)):
        actual = live(key)
        before = _snapshot(actual)
        before['metadata']['uid'] = _uid(actual)
        expected = stop._stopped(bindings, before)
        if index == 0:
            saved = copy.deepcopy(actual)
            replace_live(actual, expected)
            with pytest.raises(ValueError):
                observe()  # A planned recovery stop is forbidden before this child's intent.
            replace_live(actual, saved)
        record['resources'][key].update(status='intent', before=before,
            before_resource_version=actual['metadata']['resourceVersion'], expected=expected)
        private_state._atomic_json(journal, record)
        assert observe().workloads[key] == actual
        replace_live(actual, expected)
        assert observe().workloads[key] == actual
        record['resources'][key]['status'] = 'stopped'
        private_state._atomic_json(journal, record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
    assert observe().workloads[gateway_key]['spec']['replicas'] == 0
    journal = state / 'recovery-revoke/gateway.json'
    record = _read(journal)
    for index, key in enumerate(key for key in gateway._sources(bindings, state) if key in record['roles']):
        actual = live(key)
        expected = gateway._readonly(bindings, actual)
        saved = copy.deepcopy(actual)
        if index == 0:
            replace_live(actual, expected)
            with pytest.raises(ValueError):
                observe()
            replace_live(actual, saved)
        record['roles'][key].update(status='patch_intent', uid=_uid(actual),
            before_resource_version=actual['metadata']['resourceVersion'])
        private_state._atomic_json(journal, record)
        assert observe().authority[key] == actual
        replace_live(actual, expected)
        assert observe().authority[key] == actual
        record['roles'][key]['status'] = 'restricted'
        private_state._atomic_json(journal, record)
        if index == 0:
            replace_live(actual, saved)
            with pytest.raises(ValueError):
                observe()  # Completed retirement cannot admit the old gateway writer.
            replace_live(actual, expected)
    for phase, component, terminal in (
            ('recovery-restore-workloads', templates, 'restored'),
            ('recovery-restore-roles', roles, 'restored'),
            ('recovery-restart', restart, 'started')):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        observe()
        journal = state / phase / children[phase][2]
        record = _read(journal)
        targets = component._inputs(bindings, state)[1]
        for key in sorted(targets) if component is roles else targets:
            actual = live(key)
            expected = targets[key]
            row = record['roles' if component is roles else 'resources'][key]
            changed = _stable(actual) != _stable(expected)
            if changed:
                saved = copy.deepcopy(actual)
                replace_live(actual, expected)
                with pytest.raises(ValueError):
                    observe()
                replace_live(actual, saved)
                row.update(status='intent', before_resource_version=actual['metadata']['resourceVersion'])
                private_state._atomic_json(journal, record)
                observe()  # Uncertain intent permits only this before/after pair.
                replace_live(actual, expected)
                observed = observe()
                assert (observed.workloads if component is not roles else observed.authority)[key] == actual
            row.update(status=terminal, before_resource_version=actual['metadata']['resourceVersion'] if changed or component is roles else None)
            private_state._atomic_json(journal, record)
        observe()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-reopen'
    final = observe()
    for key, captured in bindings.plan.staging.originals.items():
        assert final.workloads[key]['spec'] == captured['spec']
    assert final.workloads[gateway_key]['spec']['replicas'] == 0
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    receipt = state / 'recovery-reopen/reopen.json'
    raw = receipt.read_bytes()
    mode['during_read'] = lambda: receipt.write_bytes(raw + b'\n')
    with pytest.raises(ValueError):
        observe()
    receipt.write_bytes(raw)
    # The whole-cluster algorithm must still run after recovery; fixed-root
    # equality alone would miss this external named Job writer.
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
        'metadata': {'name': 'unexpected-recovery-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
        'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']}]}
    foreign_binding = {'apiVersion': foreign['apiVersion'], 'kind': 'ClusterRoleBinding',
        'metadata': {'name': 'unexpected-recovery-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': foreign['metadata']['name']},
        'subjects': [{'kind': 'ServiceAccount', 'name': 'stranger', 'namespace': 'outside-pool'}]}
    inventory['clusterroles'].append(foreign)
    inventory['clusterrolebindings'].append(foreign_binding)
    with pytest.raises(ValueError):
        observe()
    inventory['clusterroles'].remove(foreign)
    inventory['clusterrolebindings'].remove(foreign_binding)
    observe()
    captured = next(iter(bindings.plan.staging.originals.values()))
    original_spec = copy.deepcopy(captured['spec'])
    mode['during_read'] = lambda: captured['spec'].update({'unapproved': True})
    try:
        with pytest.raises(ValueError):
            observe()  # Nested caller-owned plan objects cannot change mid-read.
    finally:
        captured['spec'] = original_spec
    # Terminal recovery retains the same read-only census authority, not the
    # forward view or permission to replay completed recovery mutations.
    private_state._atomic_json(receipt, {**_read(receipt), 'status': 'released'})
    api.block = 'recovery-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    observe()
    receipt = state / 'recovery-verify/verify.json'
    private_state._atomic_json(receipt, {**_read(receipt), 'status': 'observed'})
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    assert observe().workloads[gateway_key]['spec']['replicas'] == 0
    from scripts.ops.nebius_shared_pool_forward_census import observe_shared_pool_forward_writers

    with pytest.raises(ValueError):
        observe_shared_pool_forward_writers(bindings=bindings, state_dir=state, transport=transport)
    assert calls and all(call.method == 'GET' for call in calls)


@pytest.mark.timeout(240)
def test_shared_recovery_runtime_inventory_reaches_nested_probes_without_staging_scope(
        shared_operation, shared_runtime, completed_pool, handoff):
    """Real anchored stop receipts and inventory; only remote phase effects/HTTP are doubled."""
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_runtime_observation as runtime_observation
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from tests.ops.test_nebius_development_pool_retained import (
        populate_runtime_pods,
        runtime_http_inventory,
    )

    assert callable(getattr(runtime_observation, 'shared_recovery_runtime_view', None)), 'recovery runtime inventory is missing'
    args, bindings = shared_operation
    runtime, runtime_parent = shared_runtime
    completed = bindings.plan.development.runtime
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
    }

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-guards':
                record = children[phase][1](bindings=bindings, state_dir=directory, complete=False)
                assert set(record['guards'].values()) == {'prepared'}
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase in {'forward-guards', 'recovery-stop'}:
                return False
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
            else:
                assert phase == 'recovery-drain'
                record['status'] = 'observed'
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api}
    assert parent.run_shared_pool_operation(**kwargs, action='install')['phase'] == 'forward-guards'
    assert parent.run_shared_pool_operation(**kwargs, action='recover')['phase'] == 'recovery-stop'
    sources = stop._sources(bindings, state)
    dev_keys = [key for key in sources if key in completed.resources
        and completed.resources[key]['kind'] == 'Deployment'
        and completed.resources[key]['metadata']['name'] != 'loom-pool-gateway']
    assert len(dev_keys) >= 2
    stopped_key, nested_key = dev_keys[0], dev_keys[-1]
    calls, documents, handler = runtime_http_inventory(runtime, runtime_parent, completed_pool[3].server, handoff[2])
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    for key, choices in sources.items():
        original = choices[0]
        before = _snapshot(original)
        before['metadata']['uid'] = _uid(original)
        expected = stop._stopped(bindings, before)
        record['resources'][key].update(status='intent' if key == stopped_key else 'stopped',
            before=before, before_resource_version='500', expected=expected)
        if key == stopped_key:
            break
    private_state._atomic_json(journal, record)
    target = copy.deepcopy(record['resources'][stopped_key]['expected'])
    target['metadata']['uid'] = _uid(completed.resources[stopped_key])
    target_path = runtime_resource_path(target) + '/' + target['metadata']['name']
    assert target_path in documents
    documents[target_path] = target

    def connect(observer, read=handler):
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(read))

    with HTTPSDevelopmentRuntimeObserver(request=runtime,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        connect(observer)
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir)
        observed = observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        assert observed[stopped_key]['spec']['replicas'] == 0
        assert observed[nested_key]['spec']['replicas'] == 1
        raw = journal.read_bytes()
        # Removing intent must make this live recovery marker unauthorized.
        record['resources'][stopped_key] = stop._prepared()
        private_state._atomic_json(journal, record)
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        journal.write_bytes(raw)
        documents[target_path]['metadata']['uid'] = str(uuid4())
        with pytest.raises(ValueError):
            observer.inspect(state_dir=completed.state_dir, _shared_handoff=bindings)
        documents[target_path]['metadata']['uid'] = _uid(completed.resources[stopped_key])

    populate_runtime_pods(documents)
    with runtime_observation.HTTPSSharedPoolRuntimeProbes(bindings=bindings,
            api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'], token='fixture-token',
            ssl_context=ssl.create_default_context(cadata=runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])) as probes:
        connect(probes)
        _, current, pod = probes._running(nested_key, completed.state_dir)
        assert _key(current) == nested_key and pod['status']['phase'] == 'Running'
        changed = []

        def changing_read(message):
            response = handler(message)
            if not changed:
                journal.write_bytes(raw + b'\n')
                changed.append(True)
            return response

        connect(probes, changing_read)
        with pytest.raises(ValueError):
            probes._running(nested_key, completed.state_dir)
        assert changed
        journal.write_bytes(raw)
    assert calls and all(message.method == 'GET' for message in calls)
    staging_namespaces = {row['metadata']['namespace'] for row in bindings.plan.staging.originals.values()}
    assert all('/namespaces/' + namespace + '/' not in message.url.path
        for message in calls for namespace in staging_namespaces)


@pytest.mark.timeout(240)
@pytest.mark.parametrize('connection', ['protocol', 'database'])
@pytest.mark.parametrize('forward_point', ['partial-acquire', 'partial-release'])
def test_shared_recovery_fence_preserves_forward_history_and_observes_unknown_writes(
        shared_operation, forward_point, connection, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """Own recovery authority; only remote transactions and other phases doubled."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops.nebius_development_runtime_install import _read

    name = 'scripts.ops.nebius_shared_pool_recovery_fence'
    assert importlib.util.find_spec(name) is not None, 'shared terminal recovery fence is missing'
    module = __import__(name, fromlist=['advance_shared_recovery_fence'])
    live = None
    if connection == 'database':
        name = 'scripts.ops.nebius_shared_pool_recovery_fence_live'
        assert importlib.util.find_spec(name) is not None, 'fixed shared recovery transactions are missing'
        live = __import__(name, fromlist=['SharedPoolRecoveryFenceTransport'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-fence'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    effects = []

    class Remote:
        migration = bindings.migration
        mode = 'closed' if forward_point == 'partial-acquire' else 'global'
        local = dict.fromkeys(roster, 'open' if forward_point == 'partial-acquire' else 'held')
        local[roster[0]] = 'held' if forward_point == 'partial-acquire' else 'open'
        checks = 0
        damage = None

        def verify_retained(self):
            self.checks += 1

        def qualify_runtime(self):
            pytest.fail('terminal fencing must remain possible with an unhealthy gateway')

        def pool_state(self):
            if self.damage == 'receipt-bytes':
                path = child / 'fence.json'
                path.write_bytes(path.read_bytes() + b'\n')
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            assert _read(child / 'fence.json')['pool'] == 'intent'
            assert (Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json').exists()
            effects.append(('pool', None))
            raise TimeoutError('simulated unconfirmed pool fence')

        def fence_guard(self, participant):
            assert self.mode == 'fenced'
            assert _read(child / 'fence.json')['guards'][participant] == 'intent'
            effects.append(('guard', participant))
            if participant == roster[0]:
                self.local[participant] = 'fenced'
            raise TimeoutError('simulated committed or uncommitted guard fence reply loss')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    if connection == 'database':
        _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
            shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    class API(SharedPhaseAPI):
        interrupted = False
        fence_api = remote if connection == 'protocol' else None

        def prepare(self, phase, bound, directory):
            if phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
            elif phase == 'forward-activate':
                activation.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
            elif phase == 'recovery-fence':
                module.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
                if not self.interrupted:
                    self.interrupted = True
                    raise TimeoutError('simulated interrupted local preparation')
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase == 'forward-guards':
                assert set(guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=False)['guards'].values()) == {'prepared'}
            elif phase == 'forward-activate':
                activation.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            elif phase == 'recovery-fence':
                module.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'forward-activate':
                activation.validate_shared_pool_activation(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-fence':
                module.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-guards':
                path = directory / 'guards.json'
                record = _read(path)
                record['guards'].update(dict.fromkeys(roster, 'held') if forward_point == 'partial-release'
                    else {roster[0]: 'acquire_intent'})
                private_state._atomic_json(path, record)
                return forward_point == 'partial-release'
            if phase == 'forward-activate':
                path = directory / 'activation.json'
                record = _read(path)
                record['opening'] = 'opened'
                record['guards'][roster[0]]['release'] = 'intent'
                private_state._atomic_json(path, record)
                return False
            if phase == 'recovery-fence':
                if self.fence_api is None:
                    self.fence_api = live.SharedPoolRecoveryFenceTransport(bindings=bound,
                        kubeconfig=guard_api.kubeconfig, executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                return module.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
            return super().advance(phase, bound, directory)

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    expected = 'forward-guards' if forward_point == 'partial-acquire' else 'forward-activate'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == expected
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/*') if path.is_file()}
    with pytest.raises((ValueError, FileNotFoundError)):
        module.prepare_shared_recovery_fence(bindings=bindings, state_dir=child)
    assert not child.exists() and effects == []
    kwargs['action'] = 'recover'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert child.exists() and effects == []  # Only local preparation was interrupted.
    original = remote.local[roster[0]]
    remote.local[roster[0]] = 'foreign'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    remote.local[roster[0]] = original
    api.fence_api.migration = None
    checks = remote.checks
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert remote.checks == checks and effects == []
    api.fence_api.migration = bindings.migration
    previous = remote.mode
    remote.mode = 'global' if previous == 'closed' else 'closed'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    remote.mode = previous
    try:
        result = parent.run_shared_pool_operation(**kwargs)
    except Exception as error:
        import traceback
        cause = error
        while cause is not None:
            print(type(cause).__name__, [(row.name, row.lineno) for row in traceback.extract_tb(cause.__traceback__)])
            cause = cause.__context__
        raise
    assert result['phase'] == 'recovery-fence'
    assert effects == [('pool', None)]
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    assert effects == [('pool', None)]  # An unknown fence is never retried.
    receipt = child / 'fence.json'
    saved, checks = receipt.read_bytes(), remote.checks
    receipt.unlink()
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert remote.checks == checks and not receipt.exists()
    receipt.write_bytes(saved)
    receipt.chmod(0o600)
    remote.damage = 'receipt-bytes'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == [('pool', None)]
    receipt.write_bytes(saved)
    receipt.chmod(0o600)
    remote.damage = None
    remote.mode = 'fenced'  # A delayed pool-fence commit is now observable.
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    expected_effects = [('pool', None), *(('guard', key) for key in roster)]
    assert effects == expected_effects and remote.local[roster[0]] == 'fenced'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    assert effects == expected_effects
    remote.local[roster[1]] = 'fenced'
    api.block = 'recovery-drain'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    assert effects == expected_effects
    module.validate_shared_recovery_fence(bindings=bindings, state_dir=child, complete=True)
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    with pytest.raises(ValueError):
        module.advance_shared_recovery_fence(bindings=bindings, api=api.fence_api, state_dir=child)
    assert effects == expected_effects


def _shared_recovery_database_io(*, remote, bindings, guard_api, database_model,
        shared_runtime, completed_pool, handoff, monkeypatch):
    """Replace only external kubectl I/O, retaining real DB identity and SQL checks."""
    import json
    import subprocess
    from types import SimpleNamespace

    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
    )
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql
    from scripts.ops.nebius_pool_recovery_database import (
        participant_recovery_drain_sql,
        pool_recovery_drain_sql,
    )
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    from loom_service.pool_management.capacity import digest

    runtime, parent = shared_runtime
    _, inventory, _ = runtime_http_inventory(runtime, parent, completed_pool[3].server, handoff[2])
    namespace = bindings.migration.registration.binding.namespace
    documents = {path: row for path, row in inventory.items() if '/namespaces/' + namespace + '/' in path}
    documents['/api/v1/namespaces/' + namespace] = {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': namespace, 'uid': bindings.migration.registration.binding.namespace_uid}}
    for row in documents.values():
        if row['kind'] == 'Deployment':
            row['spec']['replicas'] = 0
            row['status'] = {'readyReplicas': 0}  # No healthy manager/gateway process is available.
    populate_database_backends(documents)
    database_model.documents.update(documents)
    spec = bindings.migration.registration.spec
    machine_queries = {pool_machine_retirement_sql(spec, action=action): action for action in ('observe', 'revoke')}
    fallback = subprocess.run

    def run(argv, **kwargs):
        assert argv[:len(guard_api.prefix)] == guard_api.prefix
        args = argv[len(guard_api.prefix):]
        if args[0] == 'get':
            return fallback(argv, **kwargs)
        assert args[:2] == ['exec', '-n'] and args[3:8] == ['pod/loom-postgres-0', '-c', 'loom-postgres', '--', 'psql']
        assert args[8:-2] == ['-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom']
        assert args[-2] == '-c'
        current_namespace, query = args[2], args[-1]
        if current_namespace == namespace:
            if query in machine_queries:
                assert remote.mode == 'fenced' and not any(remote.pool_counts.values())
                if machine_queries[query] == 'revoke':
                    remote.retire_machines()
                value = {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'state': remote.machines}
                if machine_queries[query] == 'observe':
                    remote.after_machine_observation()
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == pool_recovery_drain_sql(spec):
                remote.reads.append('pool')
                assert remote.mode == 'fenced'
                value = {'schema': 'loom.pool-recovery-drain.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'counts': remote.pool_counts}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == fence_pool_activation_sql(spec):
                remote.fence_pool()
            else:
                assert query == pool_activation_state_sql(spec)
            value = {'schema': 'loom.pool-activation-state.v1', 'operation_id': str(spec.operation_id),
                'installation_sha256': digest(spec.model_dump(mode='json')), 'state': remote.pool_state()}
        else:
            target, = (row for row in bindings.migration.guards if row.namespace == current_namespace)
            participant = str(target.participant_id)
            if query == pool_guard_recovery_release_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha']):
                remote.release_staging(participant)
                value = {'schema': 'loom.pool-recovery-release.v1', 'operation_id': str(spec.operation_id),
                    'participant_id': participant, 'candidate_sha': bindings.migration.registration.candidate['candidate_sha'],
                    'status': 'open'}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == participant_recovery_drain_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha']):
                remote.reads.append(participant)
                assert remote.local[participant] == 'fenced'
                value = {'schema': 'loom.pool-participant-drain.v1', 'operation_id': str(spec.operation_id),
                    'participant_id': participant, 'candidate_sha': bindings.migration.registration.candidate['candidate_sha'],
                    'read_only': True, 'counts': remote.local_counts[participant]}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == pool_guard_activation_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha'], action='fence'):
                remote.fence_guard(participant)
            else:
                assert query == pool_guard_activation_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha'], action='observe')
            value = {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(spec.operation_id),
                'participant_id': participant, 'status': remote.guard_state(participant)}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', run)


@pytest.mark.timeout(240)
def test_shared_recovery_drain_reads_all_databases_fresh_before_restoration(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch):
    """A prior zero cannot authorize stopping writers while cleanup is active."""
    import importlib.util

    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport

    name = 'scripts.ops.nebius_shared_pool_recovery_drain'
    assert importlib.util.find_spec(name) is not None, 'connected shared recovery drain is missing'
    drain = __import__(name, fromlist=['advance_shared_recovery_drain'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-drain'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)

    class Remote:
        mode = 'closed'
        damage = None

        def __init__(self):
            self.local = dict.fromkeys(roster, 'open')
            self.reads, self.effects = [], []
            self.pool_counts = {'unstarted_requests': 1, 'active_requests': 0, 'unconfirmed_creates': 0, 'unqualified_releases': 0}
            self.local_counts = {key: {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0,
                'execution_outboxes': 0, 'build_outboxes': 0} for key in roster}

        def verify_retained(self):
            if self.damage == 'authority':
                raise ValueError('simulated retained authority change')
            if self.damage == 'late-guard' and self.reads:
                self.local[roster[0]] = 'open'

        def pool_state(self):
            if self.damage == 'receipt' and self.reads:
                path = child / 'drain.json'
                path.write_bytes(path.read_bytes() + b'\n')
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.effects.append('pool')
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.effects.append(participant)
            self.local[participant] = 'fenced'

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    class API(SharedPhaseAPI):
        fence_api = None

        def prepare(self, phase, bound, directory):
            if phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
            elif phase == 'recovery-fence':
                fence.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
            elif phase == 'recovery-drain':
                drain.prepare_shared_recovery_drain(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-fence', 'recovery-drain'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-fence':
                fence.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-drain':
                drain.validate_shared_recovery_drain(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == 'forward-guards':
                return False
            if phase == 'recovery-fence':
                if self.fence_api is None:
                    self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                        executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
            if phase == 'recovery-drain':
                return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                    history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
            return super().advance(phase, bound, directory)

    api = API(parent)
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    api.block = 'recovery-stop'
    kwargs['action'] = 'recover'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    assert remote.effects == ['pool', *roster] and remote.reads == ['pool', *roster]
    receipt = child / 'drain.json'
    saved = receipt.read_bytes()
    for target, key in ((remote.pool_counts, 'unconfirmed_creates'),
            (remote.local_counts[roster[0]], 'execution_outboxes'), (remote.local_counts[roster[1]], 'build_cleanup')):
        remote.pool_counts['unstarted_requests'] = 0
        target[key] = 1
        remote.reads.clear()
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
        assert remote.reads == ['pool', *roster] and receipt.read_bytes() == saved
        target[key] = 0
    for damage in ('authority', 'receipt', 'late-guard'):
        remote.reads.clear()
        remote.damage = damage
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert remote.effects == ['pool', *roster]
        if damage == 'authority':
            assert remote.reads == []
        receipt.write_bytes(saved)
        receipt.chmod(0o600)
        remote.local[roster[0]] = 'fenced'
    remote.damage = None
    remote.reads.clear()
    api.block = 'recovery-stop'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=child, complete=True)
    remote.pool_counts['unconfirmed_creates'] = 1
    remote.reads.clear()
    assert drain.observe_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
        history=api.fence_api.history, state_dir=state, qualify=remote.verify_retained) is False
    assert remote.reads == ['pool', *roster] and remote.effects == ['pool', *roster]
    remote.pool_counts['unconfirmed_creates'] = 0
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=child, complete=True)
    with pytest.raises(ValueError):
        drain.advance_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
            history=api.fence_api.history, state_dir=child, qualify=remote.verify_retained)
    reads = list(remote.reads)
    with pytest.raises(ValueError):
        drain.observe_shared_recovery_drain(bindings=bindings, guards=api.fence_api.guards,
            history=api.fence_api.history, state_dir=state, qualify=remote.verify_retained)
    assert remote.reads == reads


@pytest.mark.timeout(600)
@pytest.mark.parametrize('forward_commit', [False, True])
def test_shared_recovery_stop_fences_delayed_forward_cas_without_replaying_unknown_stops(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, forward_commit):
    """Actual forward/recovery children and transports; only external I/O doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guard_child
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_workloads as forward
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_workloads'
    assert importlib.util.find_spec(name) is not None, 'connected terminal shared workload stop is missing'
    stop = __import__(name, fromlist=['HTTPSSharedRecoveryWorkloads'])
    assert callable(getattr(stop, 'observe_shared_recovery_workload_drain', None)), 'dependent recovery needs current process drain'
    args, bindings = shared_operation
    runtime = args['runtime']
    completed = bindings.plan.development.runtime
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-stop'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    roots = dict(bindings.plan.staging.originals)
    roots.update({key: completed.resources[key] for key in (*completed.plan.originals, *completed.plan.fixed['workloads'])})
    roots[_key(bindings.plan.development.gateway_original)] = bindings.plan.development.gateway_original
    rows, documents, writes = copy.deepcopy(roots), {}, []
    mode = {'forward': True, 'forward_delayed': None, 'forward_fault': True, 'busy_preview': True,
        'recovery_count': 0, 'deferred': None, 'lose_read': False, 'retain_pod': False}
    forward_phases = {'forward-stop', 'forward-wire', 'forward-start'}

    class Remote:
        mode = 'closed'

        def __init__(self):
            self.local = dict.fromkeys(roster, 'held')
            self.reads = []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            pass  # Full writer/provider composition is a separate required parent boundary.

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.local[participant] = 'fenced'

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            meta.setdefault('resourceVersion', '1')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    def apply(path, patches, *, persist):
        current = copy.deepcopy(documents[path])
        key = _key(current)
        assert patches[0] == {'op': 'test', 'path': '/metadata/uid', 'value': roots[key]['metadata']['uid']}
        assert patches[1]['op'] == 'test' and patches[1]['path'] == '/metadata/resourceVersion'
        for change in patches:
            parts = change['path'].strip('/').split('/')
            target = current
            for field in parts[:-1]:
                target = target[field]
            field = parts[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if persist:
            current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
            current['metadata']['generation'] += 1
            rows[key] = current
            writes.append(key)
            refresh()
        return httpx.Response(200, json=current)

    def handle(message):
        path = message.url.path
        assert path in documents, 'no database, backup or unrelated workload belongs to recovery stop'
        if message.method == 'GET':
            if mode['lose_read']:
                mode['lose_read'] = False
                raise httpx.ReadError('simulated unavailable stop readback')
            value = copy.deepcopy(documents[path])
            if mode['retain_pod'] and path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
                value['items'].append(copy.deepcopy(retained_pod))
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        patches = json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if not mode['forward'] and mode['busy_preview']:
                mode['busy_preview'] = False
                remote.pool_counts['active_requests'] = 1
            return apply(path, patches, persist=False)
        assert not message.url.query
        if mode['forward']:
            is_start = any(change.get('value', {}).get('replicas') == 1
                for change in patches if change['path'] == '/spec' and change['op'] == 'replace')
            if is_start and mode['forward_fault']:
                mode['forward_fault'] = False
                mode['forward_delayed'] = (path, patches)
                if forward_commit:
                    apply(path, patches, persist=True)
                    mode['lose_read'] = True
                raise httpx.ReadError('simulated uncertain forward start')
        else:
            assert remote.mode == 'fenced' and set(remote.local.values()) == {'fenced'}
            assert not any(remote.pool_counts.values())
            key = _key(documents[path])
            assert _read(child / 'stop.json')['resources'][key]['status'] == 'intent'
            mode['recovery_count'] += 1
            if mode['recovery_count'] == 2:
                mode['deferred'] = (path, patches)
                raise httpx.ReadError('simulated uncommitted stop reply loss')
            result = apply(path, patches, persist=True)
            if mode['recovery_count'] == 1:
                mode['lose_read'] = True
                raise httpx.ReadError('simulated committed stop reply loss')
            return result
        return apply(path, patches, persist=True)

    refresh()
    retained_pod = copy.deepcopy(next(pod for collection in documents.values() if collection['kind'] == 'PodList'
        for pod in collection['items']))
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            fence_api = None
            stop_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guard_child.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in forward_phases:
                    forward.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'recovery-fence':
                    fence.prepare_shared_recovery_fence(bindings=bound, state_dir=directory)
                elif phase == 'recovery-drain':
                    drain.prepare_shared_recovery_drain(bindings=bound, state_dir=directory)
                elif phase == 'recovery-stop':
                    stop.prepare_shared_recovery_workloads(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'recovery-stop':
                    stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', *forward_phases, 'recovery-fence', 'recovery-drain'}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in forward_phases:
                    forward.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'recovery-fence':
                    fence.validate_shared_recovery_fence(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-drain':
                    drain.validate_shared_recovery_drain(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-stop':
                    stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(roster, 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'recovery-fence':
                    if self.fence_api is None:
                        self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                            executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                    return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
                if phase == 'recovery-drain':
                    return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
                if phase not in {*forward_phases, 'recovery-stop'}:
                    return super().advance(phase, bound, directory)
                def qualify(request, selected, writing):
                    assert request == runtime and selected == phase
                def drained():
                    return drain.observe_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=state, qualify=remote.verify_retained)
                options = {'bindings': bound, 'api_server': runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    'ssl_context': ssl.create_default_context(), 'qualify_runtime': qualify,
                    'observe_ready': lambda *_: True}
                connection = stack.enter_context(stop.HTTPSSharedRecoveryWorkloads(**options, recovery_drained=drained)
                    if phase == 'recovery-stop' else forward.HTTPSSharedStagingWorkloads(**options, phase=phase))
                connection_type = type(connection.client)
                connection.client.close()
                connection.client = connection_type(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                if phase == 'recovery-stop':
                    self.stop_api = connection
                    return stop.advance_shared_recovery_workloads(bindings=bound, api=connection, state_dir=directory)
                return forward.advance_shared_staging_workloads(bindings=bound, phase=phase, api=connection, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-revoke'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert mode['forward_delayed'] is not None
        assert _read(state / 'operation.json')['forward']['forward-start']['status'] == 'started'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/*') if path.is_file()}
        mode['forward'] = False
        kwargs['action'] = 'recover'
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert writes == before
        assert all(row['status'] == 'prepared' for row in _read(child / 'stop.json')['resources'].values())
        remote.pool_counts['active_requests'] = 0
        # Missing/byte-altered frozen evidence must never be reconstructed by
        # recovery, even when its decoded JSON would still be equivalent.
        receipt = state / 'forward-start' / 'transition.json'
        retained = receipt.read_bytes()
        displaced = receipt.with_suffix('.held')
        receipt.rename(displaced)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == before
        displaced.rename(receipt)
        receipt.write_bytes(retained + b'\n')
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert writes == before
        receipt.write_bytes(retained)
        # A later root drifting must prevent any earlier root from being
        # stopped, not merely fail when iteration eventually reaches it.
        last = next(reversed(roots))
        original = copy.deepcopy(rows[last])
        for changed in ('uid', 'template'):
            if changed == 'uid':
                rows[last]['metadata']['uid'] = str(uuid4())
            else:
                rows[last]['spec']['template']['spec']['containers'][0]['image'] = 'unqualified.invalid/replacement:latest'
            refresh()
            with pytest.raises(ValueError):
                parent.run_shared_pool_operation(**kwargs)
            assert writes == before
            rows[last] = copy.deepcopy(original)
            refresh()
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert len(writes) == len(before) + 1
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert mode['deferred'] is not None and mode['recovery_count'] == 2
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        assert mode['recovery_count'] == 2  # Never replay the unresolved stop.
        assert apply(*mode['deferred'], persist=True).status_code == 200
        mode['retain_pod'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
        mode['retain_pod'] = False
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert set(writes[len(before):]) == set(roots)
        assert all(writes[len(before):].count(key) == 1 for key in roots)
        for row in rows.values():
            assert row['spec']['suspend' if row['kind'] == 'CronJob' else 'replicas'] == (True if row['kind'] == 'CronJob' else 0)
            assert row['metadata']['annotations'][stop.MARKER] == str(bindings.migration.registration.spec.operation_id)
        assert apply(*mode['forward_delayed'], persist=True).status_code == 409
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        stop.validate_shared_recovery_workloads(bindings=bindings, state_dir=child, complete=True)
        before = list(writes)
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert writes == before
        # A completed receipt proves past shutdown only. The following revoke
        # phase needs the same actual HTTP descendant checks and fresh SQL drain.
        def current_drain():
            return stop.observe_shared_recovery_workload_drain(bindings=bindings, api=api.stop_api, state_dir=child)
        assert current_drain() is True
        remote.pool_counts['active_requests'] = 1
        assert current_drain() is False
        remote.pool_counts['active_requests'] = 0
        mode['retain_pod'] = True
        assert current_drain() is False
        mode['retain_pod'] = False
        assert current_drain() is True
        with pytest.raises(ValueError):
            stop.advance_shared_recovery_workloads(bindings=bindings, api=api.stop_api, state_dir=child)
        assert writes == before


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_machine_revocation_uses_fixed_sql_and_current_stop_drain(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, committed):
    """Actual revoke/SQL/drain transports; prior stop effects and parent census doubled."""
    import importlib.util
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_machines'
    assert importlib.util.find_spec(name) is not None, 'shared recovery machine retirement is missing'
    machines = __import__(name, fromlist=['SharedRecoveryMachineTransport'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-revoke'
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    documents, http_calls = {}, []
    mode = {'retain_pod': False}
    retained_pod = None

    class Remote:
        mode = 'closed'
        machines = 'active'
        dispatches = 0
        busy_after_machine_read = False
        change_receipt_after_machine_read = False
        open_during_qualification = False

        def __init__(self):
            self.local = dict.fromkeys(roster, 'open')
            self.reads = []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            # The complete writer/provider parent is a separate integration
            # boundary; inject a race in its final callback, not a fake verdict.
            if self.open_during_qualification:
                self.open_during_qualification = False
                self.local[roster[0]] = 'open'

        def after_machine_observation(self):
            if self.busy_after_machine_read:
                self.busy_after_machine_read = False
                self.pool_counts['active_requests'] = 1
            if self.change_receipt_after_machine_read:
                self.change_receipt_after_machine_read = False
                receipt = child / 'machines.json'
                receipt.write_bytes(receipt.read_bytes() + b'\n')

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def fence_pool(self):
            self.mode = 'fenced'

        def fence_guard(self, participant):
            self.local[participant] = 'fenced'

        def retire_machines(self):
            assert _read(child / 'machines.json')['status'] == 'intent'
            assert not mode['retain_pod'] and set(self.local.values()) == {'fenced'}
            self.dispatches += 1
            if committed:
                self.machines = 'revoked'
            raise TimeoutError('simulated machine retirement reply loss')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)

    def inventory(rows):
        documents.clear()
        for original in rows.values():
            row = copy.deepcopy(original)
            meta = row['metadata']
            meta.update(generation=2, resourceVersion='2')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': 2, **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = row
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '2'}, 'items': []})
        populate_runtime_pods(documents)

    def handle(message):
        assert message.method == 'GET', 'machine revocation must not replay Kubernetes stop writes'
        http_calls.append(message.url.path)
        value = copy.deepcopy(documents[message.url.path])
        if mode['retain_pod'] and message.url.path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
            value['items'].append(copy.deepcopy(retained_pod))
        return httpx.Response(200, json=value)

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads),
        'recovery-revoke': (machines.prepare_shared_recovery_machines, machines.validate_shared_recovery_machines),
    }
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            fence_api = None
            machine_api = None
            machines_done = False

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                else:
                    children[phase][0](bindings=bound, state_dir=directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=False)
                else:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

            def advance(self, phase, bound, directory):
                nonlocal retained_pod
                if phase == self.block:
                    return False
                if phase == 'recovery-fence':
                    self.fence_api = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=guard_api.kubeconfig,
                        executable=Path(guard_api.prefix[0]), qualify=remote.verify_retained)
                    return fence.advance_shared_recovery_fence(bindings=bound, api=self.fence_api, state_dir=directory)
                if phase == 'recovery-drain':
                    return drain.advance_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                        history=self.fence_api.history, state_dir=directory, qualify=remote.verify_retained)
                if phase == 'recovery-stop':
                    # Deliberately double the earlier stop effects, not the
                    # current HTTP process-drain or SQL transaction under test.
                    sources = stop._sources(bound, state)
                    inventory({key: choices[0] for key, choices in sources.items()})
                    retained_pod = copy.deepcopy(next(pod for row in documents.values() if row['kind'] == 'PodList' for pod in row['items']))
                    record = _read(directory / 'stop.json')
                    stopped = {}
                    for key, choices in sources.items():
                        before = _snapshot(choices[0])
                        before['metadata']['uid'] = _uid(choices[0])
                        expected = stop._stopped(bound, before)
                        record['resources'][key] = {'status': 'stopped', 'before': before,
                            'before_resource_version': '1', 'expected': expected}
                        stopped[key] = copy.deepcopy(expected)
                        stopped[key]['metadata']['uid'] = _uid(before)
                    private_state._atomic_json(directory / 'stop.json', record)
                    inventory(stopped)
                    return True
                assert phase == 'recovery-revoke'
                if self.machine_api is None:
                    def current_drain():
                        return drain.observe_shared_recovery_drain(bindings=bound, guards=self.fence_api.guards,
                            history=self.fence_api.history, state_dir=state, qualify=remote.verify_retained)
                    observer = stack.enter_context(stop.HTTPSSharedRecoveryWorkloads(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: remote.verify_retained(),
                        recovery_drained=current_drain, observe_ready=lambda *_: True))
                    connection_type = type(observer.client)
                    observer.client.close()
                    observer.client = connection_type(base_url=observer.api_server, transport=httpx.MockTransport(handle))
                    self.machine_api = machines.SharedRecoveryMachineTransport(bindings=bound, workloads=observer,
                        guards=self.fence_api.guards, history=self.fence_api.history)
                self.machines_done = machines.advance_shared_recovery_machines(bindings=bound, api=self.machine_api, state_dir=directory)
                return False  # Gateway RBAC retirement is still required; never restore staging here.

        api = API(parent)
        api.block = 'forward-guards'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
        api.block = 'recovery-revoke'
        kwargs['action'] = 'recover'
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        api.block = None
        remote.pool_counts['active_requests'] = 1
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        remote.pool_counts['active_requests'] = 0
        mode['retain_pod'] = True
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        mode['retain_pod'] = False
        # Exercise the sealed transport independently of the controller's
        # earlier drain: work arriving after its machine read must remain a
        # known pre-dispatch wait, without publishing intent.
        remote.busy_after_machine_read = True
        assert api.machine_api.retire_machines(record_intent=lambda: pytest.fail('busy transport must not publish intent')) is False
        assert remote.dispatches == 0 and _read(child / 'machines.json')['status'] == 'prepared'
        remote.pool_counts['active_requests'] = 0
        remote.open_during_qualification = True
        with pytest.raises(ValueError):
            api.machine_api.machine_authority()
        remote.local[roster[0]] = 'fenced'
        receipt = child / 'machines.json'
        raw = receipt.read_bytes()
        remote.change_receipt_after_machine_read = True
        with pytest.raises(ValueError):
            api.machine_api.machine_authority()
        receipt.write_bytes(raw)
        displaced = receipt.with_suffix('.held')
        receipt.rename(displaced)
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)
        assert remote.dispatches == 0 and not receipt.exists()
        displaced.rename(receipt)
        remote.machines = 'revoked'
        with pytest.raises(ValueError):
            parent.run_shared_pool_operation(**kwargs)  # Cannot adopt unexplained revocation without this child's intent.
        assert remote.dispatches == 0
        remote.machines = 'active'
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 1 and api.machines_done is committed
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert remote.dispatches == 1  # Neither an unknown nor a completed transaction is reissued.
        remote.machines = 'revoked'  # The deferred transaction may now have committed.
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
        assert api.machines_done is True and remote.dispatches == 1
        machines.validate_shared_recovery_machines(bindings=bindings, state_dir=child, complete=True)
        assert http_calls and _read(state / 'operation.json')['recovery']['recovery-restore-workloads']['status'] == 'prepared'


@pytest.mark.timeout(600)
@pytest.mark.parametrize('winner', ['readonly', 'original', 'pending', 'binding'])
def test_shared_recovery_gateway_closes_partial_grants_without_replaying_unknown_writes(shared_operation, winner):
    """Real forward/recovery gateway HTTP; prior phase effects and machine readbacks doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_gateway as forward
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid

    name = 'scripts.ops.nebius_shared_pool_recovery_gateway'
    assert importlib.util.find_spec(name) is not None, 'partial shared gateway retirement is missing'
    gateway = __import__(name, fromlist=['HTTPSSharedRecoveryGateway'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-revoke'
    catalog = bindings.plan.development.authority
    role_keys = [key for key, row in catalog.items() if row['kind'] == 'Role']
    fault_key = next(key for key, row in catalog.items() if row['kind'] == 'RoleBinding'
        and row['metadata']['namespace'] == catalog[role_keys[0]]['metadata']['namespace']) if winner == 'binding' else role_keys[1]
    namespaces = {ns.name: str(ns.uid) for row in bindings.migration.registration.spec.participants
        for ns in (row.execution_namespace, row.build_namespace)}
    namespaces[manager.binding.namespace] = manager.binding.namespace_uid
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in catalog.items()}
    stored, effects, reviews = {}, [], []
    mode = {'forward': True, 'pending_original': None, 'busy_preview': True, 'busy': False,
        'pending_patch': None, 'patches': 0, 'barriers': 0, 'pending_barrier': None, 'foreign': True}
    foreign_role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
        'metadata': {'name': 'foreign-writer', 'namespace': 'foreign-gateway-test', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['named-job']}]}
    foreign_binding = {'apiVersion': foreign_role['apiVersion'], 'kind': 'RoleBinding',
        'metadata': {**foreign_role['metadata'], 'uid': str(uuid4())},
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign-writer'},
        'subjects': [{'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Group', 'name': 'system:authenticated'}]}

    def inventory():
        return [*stored.values(), *([foreign_role, foreign_binding] if mode['foreign'] and not mode['forward'] else [])]

    def conflict(reason='AlreadyExists'):
        return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'code': 409, 'reason': reason})

    def create(document, *, persist):
        key = _key(document)
        if key in stored:
            return conflict()
        result = copy.deepcopy(document)
        result['metadata'].update(uid=str(uuid4()), resourceVersion='1')
        if persist:
            stored[key] = result
            effects.append(('create', key))
        return httpx.Response(201, json=result)

    def patch(key, changes, *, persist):
        value = copy.deepcopy(stored[key])
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return conflict('Conflict')
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            stored[key] = value
            effects.append(('patch', key))
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        if message.method == 'GET':
            if path.startswith('/api/v1/namespaces/'):
                namespace = path.rsplit('/', 1)[-1]
                return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                    'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
            if path in paths:
                value = stored.get(paths[path])
                return httpx.Response(404) if value is None else httpx.Response(200, json=value)
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[path.rsplit('/', 1)[-1]]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '1', 'continue': ''}, 'items': [row for row in inventory() if row['kind'] == kind]})
        body = json.loads(message.content)
        if message.method == 'POST' and path.endswith('/selfsubjectrulesreviews'):
            assert message.headers['Impersonate-User'] == 'system:serviceaccount:' + manager.binding.namespace + ':loom-pool-gateway'
            namespace = body['spec']['namespace']
            reviews.append(namespace)
            rows, rules = {_key(row): row for row in inventory()}, []
            for binding in rows.values():
                if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                    continue
                if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
                    continue
                reference = binding['roleRef']
                key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                if key in rows:
                    rules.extend(copy.deepcopy(rows[key]['rules']))
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
        if message.method == 'POST':
            key = _key(body)
            assert key in catalog and path == runtime_resource_path(catalog[key])
            if message.url.params.get('dryRun') == 'All':
                return create(body, persist=False)
            if mode['forward']:
                if key == fault_key:
                    mode['pending_original'] = copy.deepcopy(body)
                    raise httpx.ReadError('simulated delayed forward grant creation')
                return create(body, persist=True)
            assert body['kind'] == 'Role' and key == fault_key and not mode['busy']
            assert _read(child / 'gateway.json')['roles'][key]['status'] == 'create_intent'
            mode['barriers'] += 1
            if winner == 'original':
                assert create(mode['pending_original'], persist=True).status_code == 201
                return create(body, persist=True)  # The original won: a known name conflict.
            if winner == 'pending':
                mode['pending_barrier'] = copy.deepcopy(body)
                raise httpx.ReadError('simulated uncommitted barrier reply loss')
            result = create(body, persist=True)
            assert result.status_code == 201
            raise httpx.ReadError('simulated committed barrier reply loss')
        assert message.method == 'PATCH' and paths[path] in role_keys and not mode['forward']
        key = paths[path]
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return patch(key, body, persist=False)
        assert not mode['busy'] and _read(child / 'gateway.json')['roles'][key]['status'] == 'patch_intent'
        mode['patches'] += 1
        if mode['patches'] == 1:
            mode['pending_patch'] = (key, body)
            raise httpx.ReadError('simulated uncommitted Role reduction')
        result = patch(key, body, persist=True)
        assert result.status_code == 200
        raise httpx.ReadError('simulated committed Role reduction reply loss')

    class MachineReadback:
        def __init__(self):
            self.bindings, self.state = bindings, child

        def drained(self):
            return not mode['busy']

        def machine_authority(self):
            return 'revoked'

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
    }
    workload_phases = {'forward-stop': 'stop', 'forward-wire': 'replace', 'forward-start': 'start'}
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            gateway_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in workload_phases:
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'forward-gateway':
                    forward.prepare_shared_gateway(bindings=bound, state_dir=directory)
                elif phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-gateway', *workload_phases, *children}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in workload_phases:
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    forward.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase in workload_phases:
                    from scripts.ops.nebius_management_stage import _comparison_snapshot

                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children:
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    else:
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
                    private_state._atomic_json(path, record)
                    return True
                if phase not in {'forward-gateway', 'recovery-revoke'}:
                    return super().advance(phase, bound, directory)
                options = {'bindings': bound, 'api_server': runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    'ssl_context': ssl.create_default_context()}
                if phase == 'forward-gateway':
                    connection = stack.enter_context(forward.HTTPSSharedGateway(**options,
                        qualify_runtime=lambda *_: None, observe_ready=lambda *_: False))
                elif self.gateway_api is None:
                    path = directory / 'machines.json'
                    record = _read(path)
                    record['status'] = 'revoked'  # This already-tested predecessor is an explicit double here.
                    private_state._atomic_json(path, record)
                    connection = self.gateway_api = stack.enter_context(gateway.HTTPSSharedRecoveryGateway(**options, machines=MachineReadback()))
                else:
                    connection = self.gateway_api
                if not hasattr(connection, 'test_transport_installed'):
                    connection_type = type(connection.client)
                    connection.client.close()
                    connection.client = connection_type(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                    connection.test_transport_installed = True
                if phase == 'forward-gateway':
                    return forward.advance_shared_gateway(bindings=bound, api=connection, state_dir=directory)
                return gateway.advance_shared_recovery_gateway(bindings=bound, api=connection, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restore-workloads'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}

        def run_operation():
            try:
                return parent.run_shared_pool_operation(**kwargs)
            except ValueError as exc:
                import traceback

                frames, cause = [], exc
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('gateway failure at ' + ', '.join(frames)) from None

        with pytest.raises(ValueError) as failure:
            run_operation()
        assert mode['pending_original'] is not None and role_keys[0] in stored, str(failure.value)
        original_uid = _uid(stored[role_keys[0]])
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        mode['forward'] = False
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 0 and mode['barriers'] == 0
        assert _read(child / 'gateway.json')['roles'][role_keys[0]]['status'] == 'original'
        mode['busy'] = False
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 1 and mode['pending_patch'] is not None
        assert run_operation()['phase'] == 'recovery-revoke'
        assert mode['patches'] == 1
        assert patch(*mode['pending_patch'], persist=True).status_code == 200
        if winner == 'pending':
            assert run_operation()['phase'] == 'recovery-revoke'
            assert mode['barriers'] == 1 and mode['pending_barrier'] is not None
            assert run_operation()['phase'] == 'recovery-revoke'
            assert mode['barriers'] == 1
            assert create(mode['pending_barrier'], persist=True).status_code == 201
        with pytest.raises(ValueError) as failure:
            run_operation()  # Real effective review must reject the extra named writer.
        assert 'foreign-gateway-test' in reviews, str(failure.value)
        assert _uid(stored[role_keys[0]]) == original_uid
        for key in role_keys:
            if key in stored:
                assert [rule['verbs'] for rule in stored[key]['rules']] == [['get'], ['get'], ['get', 'list']]
        mode['foreign'] = False
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        if winner == 'binding':
            assert create(mode['pending_original'], persist=True).status_code == 201
            api.gateway_api.verify_permissions()  # A late binding can now grant only the pinned read rights.
        else:
            assert create(mode['pending_original'], persist=True).status_code == 409
            assert mode['barriers'] == 1
        assert mode['patches'] == (2 if winner == 'original' else 1)
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert set(row['status'] for row in _read(child / 'gateway.json')['roles'].values()) <= {'restricted', 'absent'}
        before = list(effects)
        with pytest.raises(ValueError):
            gateway.advance_shared_recovery_gateway(bindings=bindings, api=api.gateway_api, state_dir=child)
        assert effects == before


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_restores_current_templates_closed_without_replaying_unknown_writes(shared_operation, committed):
    """Real template HTTP/CAS/descendant reads; predecessor phase and SQL effects doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_gateway as forward_gateway
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_stage import _comparison_snapshot
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_templates'
    assert importlib.util.find_spec(name) is not None, 'current-staging stopped template restoration is missing'
    templates = __import__(name, fromlist=['HTTPSSharedRecoveryTemplates'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restore-workloads'
    captured = copy.deepcopy(bindings.plan.staging.originals)
    rows, documents, effects = {}, {}, []
    mode = {'busy': False, 'busy_preview': True, 'retained_pod': False, 'pending': None, 'calls': 0}

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            meta.setdefault('resourceVersion', '1')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    rows.update(copy.deepcopy(captured))
    refresh()
    retained_pod = copy.deepcopy(next(pod for collection in documents.values() if collection['kind'] == 'PodList'
        for pod in collection['items']))

    def apply(path, changes, *, persist):
        value = copy.deepcopy(documents[path])
        key = _key(value)
        assert key in captured, 'restoration cannot change new dev/global, database or backup roots'
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(captured[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        assert value['spec']['suspend'] is True if value['kind'] == 'CronJob' else value['spec']['replicas'] == 0
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            value['metadata']['generation'] += 1
            rows[key] = value
            effects.append(key)
            refresh()
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        assert path in documents
        if message.method == 'GET':
            value = copy.deepcopy(documents[path])
            if mode['retained_pod'] and path == '/api/v1/namespaces/' + retained_pod['metadata']['namespace'] + '/pods':
                value['items'].append(copy.deepcopy(retained_pod))
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        changes = json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return apply(path, changes, persist=False)
        assert not mode['busy'] and not mode['retained_pod']
        key = _key(documents[path])
        assert _read(child / 'templates.json')['resources'][key]['status'] == 'intent'
        mode['calls'] += 1
        if mode['calls'] == 1:
            mode['pending'] = path, changes
            if committed:
                apply(path, changes, persist=True)
            raise httpx.ReadError('simulated uncertain template restoration')
        return apply(path, changes, persist=True)

    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
    }
    workload_phases = {'forward-stop': 'stop', 'forward-wire': 'replace', 'forward-start': 'start'}
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            template_api = None

            def prepare(self, phase, bound, directory):
                if phase == 'forward-guards':
                    guards.prepare_shared_pool_guards(bindings=bound, state_dir=directory)
                elif phase in workload_phases:
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'forward-gateway':
                    forward_gateway.prepare_shared_gateway(bindings=bound, state_dir=directory)
                elif phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-gateway', *workload_phases}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-guards':
                    guards.validate_shared_pool_guards(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in workload_phases:
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    forward_gateway.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-gateway':
                    return False  # Recovery precedes any new gateway grant.
                if phase == 'forward-guards':
                    path = directory / 'guards.json'
                    record = _read(path)
                    record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    private_state._atomic_json(path, record)
                    return True
                if phase in workload_phases:
                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restore-workloads':
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    else:
                        rows.clear()
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            expected = stop._stopped(bound, before)
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': expected}
                            rows[key] = {**copy.deepcopy(expected), 'metadata': {**expected['metadata'],
                                'uid': _uid(before), 'resourceVersion': '2'}}
                        refresh()
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase != 'recovery-restore-workloads':
                    return super().advance(phase, bound, directory)
                if self.template_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                    self.template_api = stack.enter_context(templates.HTTPSSharedRecoveryTemplates(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify,
                        recovery_drained=lambda: not mode['busy']))
                    connection_type = type(self.template_api.client)
                    self.template_api.client.close()
                    self.template_api.client = connection_type(base_url=self.template_api.api_server, transport=httpx.MockTransport(handle))
                return templates.advance_shared_recovery_templates(bindings=bound, api=self.template_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restore-roles'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}

        def run_operation():
            try:
                return parent.run_shared_pool_operation(**kwargs)
            except ValueError as exc:
                import traceback

                frames, cause = [], exc
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('template failure at ' + ', '.join(frames)) from None

        assert run_operation()['phase'] == 'forward-gateway'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        assert mode['calls'] == 0
        assert {row['status'] for row in _read(child / 'templates.json')['resources'].values()} == {'prepared'}
        untouched = {key: copy.deepcopy(row) for key, row in rows.items() if key not in captured}
        receipt = state / 'recovery-stop/stop.json'
        receipt_bytes = receipt.read_bytes()
        receipt.write_bytes(receipt_bytes + b'\n')
        try:
            with pytest.raises(ValueError):
                api.template_api.drained()
        finally:
            receipt.write_bytes(receipt_bytes)
        target_key = next(key for key in api.template_api.targets if captured[key]['kind'] == 'Deployment')
        target_before = copy.deepcopy(api.template_api.targets[target_key])
        api.template_api.targets[target_key]['spec']['replicas'] = 1
        try:
            with pytest.raises(ValueError):
                api.template_api.drained()
        finally:
            api.template_api.targets[target_key] = target_before
        assert mode['calls'] == 0
        mode['busy'], mode['retained_pod'] = False, True
        assert run_operation()['phase'] == 'recovery-restore-workloads'
        assert mode['calls'] == 0
        mode['retained_pod'] = False
        result = run_operation()
        assert mode['pending'] is not None
        if not committed:
            assert result['phase'] == 'recovery-restore-workloads'
            assert mode['calls'] == 1
            assert run_operation()['phase'] == 'recovery-restore-workloads' and mode['calls'] == 1
            assert apply(*mode['pending'], persist=True).status_code == 200
            result = run_operation()
        assert result['phase'] == 'recovery-restore-roles'
        assert effects and len(effects) == len(set(effects)) and set(effects) <= captured.keys()
        assert all(rows[key] == value for key, value in untouched.items())
        for key, before in captured.items():
            expected = copy.deepcopy(before['spec'])
            expected['suspend' if before['kind'] == 'CronJob' else 'replicas'] = True if before['kind'] == 'CronJob' else 0
            assert rows[key]['spec'] == expected
            assert _uid(rows[key]) == _uid(before)
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        calls = mode['calls']
        with pytest.raises(ValueError):
            templates.advance_shared_recovery_templates(bindings=bindings, api=api.template_api, state_dir=child)
        assert mode['calls'] == calls


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_restores_current_roles_with_delayed_forward_barrier(shared_operation, committed):
    """Real Role CAS/reviews; predecessor effects and SQL/process qualification doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_roles as forward
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_stage import _comparison_snapshot

    name = 'scripts.ops.nebius_shared_pool_recovery_roles'
    assert importlib.util.find_spec(name) is not None, 'current-staging Role restoration is missing'
    module = __import__(name, fromlist=['HTTPSSharedRecoveryRoles'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restore-roles'
    originals = copy.deepcopy(bindings.plan.staging_role_originals)
    roles = copy.deepcopy(originals)
    catalog = {_key(row): copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}}
    fixed_bindings = {key: row for key, row in catalog.items() if row['kind'].endswith('Binding')}
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'name': 'foreign', 'namespace': 'outside-pool', 'uid': str(uuid4()), 'resourceVersion': '1'},
        'subjects': [{'kind': 'Group', 'apiGroup': 'rbac.authorization.k8s.io', 'name': 'system:authenticated'}],
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'foreign'}}
    current_bindings = {**copy.deepcopy(fixed_bindings), _key(foreign): foreign}
    first = min(originals)
    inbound = copy.deepcopy(next(row for row in fixed_bindings.values()
        if row['roleRef']['kind'] == 'Role' and row['roleRef']['name'] == originals[first]['metadata']['name']))
    inbound['metadata'].update(name='foreign-inbound', uid=str(uuid4()))
    inbound['subjects'] = [{'kind': 'ServiceAccount', 'namespace': 'outside-pool', 'name': 'stranger'}]
    current_bindings[_key(inbound)] = inbound
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    mode = {'recovering': False, 'busy': False, 'busy_preview': True, 'late_fault': None, 'writing_calls': 0, 'extra': False, 'missing': False,
        'incomplete': False, 'gateway_writer': False, 'pending_forward': None, 'pending_restore': None, 'calls': 0}
    attempts, effects, reviews, qualification = [], [], [], []

    def apply(key, changes, *, persist):
        value = copy.deepcopy(roles[key])
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(originals[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        assert any(row['op'] == 'test' and row['path'] == '/rules' for row in changes)
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            roles[key] = value
            effects.append((mode['recovering'], key))
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
        if path in ('/apis/rbac.authorization.k8s.io/v1/rolebindings',
                '/apis/rbac.authorization.k8s.io/v1/clusterrolebindings'):
            assert message.method == 'GET'
            kind = 'RoleBinding' if path.endswith('/rolebindings') else 'ClusterRoleBinding'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '71'},
                'items': [copy.deepcopy(row) for row in current_bindings.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            account = message.headers['Impersonate-User']
            _, _, account_namespace, account_name = account.split(':')
            groups = {'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'}
            assert set(message.headers.get_list('Impersonate-Group', split_commas=True)) == groups
            namespace = json.loads(message.content)['spec']['namespace']
            reviews.append((account, namespace))
            rules = []
            for row in fixed_bindings.values():
                if row['kind'] == 'RoleBinding' and row['metadata']['namespace'] != namespace:
                    continue
                if not any(item['kind'] == 'ServiceAccount' and (item['namespace'], item['name']) == (account_namespace, account_name)
                        for item in row.get('subjects', [])):
                    continue
                reference = row['roleRef']
                key = reference['kind'] + ':' + (row['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(roles.get(key, catalog.get(key))['rules']))
            if namespace == 'outside-pool' and mode['extra']:
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']})
            if mode['missing']:
                rules = []
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': mode['incomplete'], 'resourceRules': rules, 'nonResourceRules': []}})
        assert 'Impersonate-User' not in message.headers
        for key, row in {**catalog, **bindings.plan.staging_reader_authority}.items():
            if path == runtime_resource_path(row) + '/' + row['metadata']['name']:
                if message.method == 'GET':
                    if not mode['recovering'] and committed and mode['pending_forward'] is not None:
                        raise httpx.ReadError('simulated lost committed forward readback')
                    value = roles.get(key, current_bindings.get(key, catalog.get(key)))
                    return httpx.Response(404) if value is None else httpx.Response(200, json=value)
                assert message.method == 'PATCH' and key in originals
                changes = json.loads(message.content)
                if message.url.params.get('dryRun') == 'All':
                    if mode['busy_preview']:
                        mode['busy_preview'], mode['busy'] = False, True
                    return apply(key, changes, persist=False)
                if not mode['recovering']:
                    assert _read(state / 'forward-roles/roles.json')['roles'][key] == 'intent'
                    assert mode['pending_forward'] is None
                    mode['pending_forward'] = key, changes
                    if committed:
                        apply(key, changes, persist=True)
                    raise httpx.ReadError('simulated uncertain forward Role reduction')
                assert not mode['busy'] and not mode['gateway_writer']
                assert _read(child / 'roles.json')['roles'][key]['status'] == 'intent'
                attempts.append(key)
                mode['calls'] += 1
                if mode['calls'] == 1:
                    mode['pending_restore'] = key, changes
                    if committed:
                        apply(key, changes, persist=True)
                    raise httpx.ReadError('simulated uncertain restoration')
                return apply(key, changes, persist=True)
        raise AssertionError('unexpected Role recovery request')

    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'forward-roles': (forward.prepare_shared_staging_roles, forward.validate_shared_staging_roles, 'roles.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (module.prepare_shared_recovery_roles, module.validate_shared_recovery_roles, 'roles.json'),
    }
    with ExitStack() as stack:
        def connect(transport):
            value = stack.enter_context(transport)
            connection_type = type(value.client)
            value.client.close()
            value.client = connection_type(base_url=value.api_server, transport=httpx.MockTransport(handle))
            return value

        class API(SharedPhaseAPI):
            role_api = None

            def prepare(self, phase, bound, directory):
                if phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'forward-stop':
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-guards':
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False)
                elif phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-stop', 'recovery-revoke'}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-stop':
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-roles':
                    connection = connect(forward.HTTPSSharedStagingRoles(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=lambda current, writing: None))
                    return forward.advance_shared_staging_roles(bindings=bound, api=connection, state_dir=directory)
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase == 'forward-stop':
                    _, targets = staging._inputs(bound, state, phase, staging._frame(bound, state))
                    path = directory / 'transition.json'
                    record = _read(path)
                    for key, row in record['resources'].items():
                        row.update(status='applied', expected=_comparison_snapshot(targets[key]))
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restore-roles':
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'forward-guards':
                        record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    elif phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    elif phase == 'recovery-stop':
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
                    elif phase == 'recovery-restore-workloads':
                        from scripts.ops.nebius_management_switch import _stable

                        before, targets = templates._inputs(bound, state)
                        for key, row in record['resources'].items():
                            row.update(status='restored', before_resource_version=
                                '2' if _stable(before[key]) != _stable(targets[key]) else None)
                    private_state._atomic_json(path, record)
                    return True
                if phase != 'recovery-restore-roles':
                    return super().advance(phase, bound, directory)
                if self.role_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                        qualification.append(writing)
                        if writing and mode['late_fault']:
                            mode['writing_calls'] += 1
                            if mode['writing_calls'] == 4:
                                mode[mode['late_fault']] = True

                    def gateway_denied():
                        if mode['gateway_writer']:
                            raise ValueError('simulated revived gateway write authority')

                    self.role_api = connect(module.HTTPSSharedRecoveryRoles(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify,
                        templates=SimpleNamespace(bindings=bound, state=state / 'recovery-restore-workloads', drained=lambda: not mode['busy']),
                        gateway=SimpleNamespace(bindings=bound, state=state / 'recovery-revoke', verify_permissions=gateway_denied)))
                return module.advance_shared_recovery_roles(bindings=bound, api=self.role_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-restart'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}

        def run_operation():
            try:
                return parent.run_shared_pool_operation(**kwargs)
            except ValueError as exc:
                import traceback

                frames, cause = [], exc
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('Role recovery failure at ' + ', '.join(frames)) from None

        with pytest.raises(ValueError) as forward_failure:
            run_operation()
        assert mode['pending_forward'] is not None, str(forward_failure.value)
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'], mode['recovering'] = 'recover', True
        with pytest.raises(ValueError) as inbound_failure:
            run_operation()  # Do not grant a restored fixed Role to a foreign subject.
        assert api.role_api is not None, str(inbound_failure.value)
        assert '_bindings' in str(inbound_failure.value)
        assert not attempts
        del current_bindings[_key(inbound)]
        mode['extra'] = True
        with pytest.raises(ValueError):
            run_operation()
        assert not attempts and any(namespace == 'outside-pool' for _, namespace in reviews)
        mode['extra'], mode['gateway_writer'] = False, True
        with pytest.raises(ValueError):
            run_operation()
        assert not attempts
        mode['gateway_writer'] = False
        assert run_operation()['phase'] == 'recovery-restore-roles'
        assert not attempts and mode['busy']
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        receipt = state / 'recovery-restore-workloads/templates.json'
        saved = receipt.read_bytes()
        receipt.write_bytes(saved + b'\n')
        try:
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
        finally:
            receipt.write_bytes(saved)
        target = copy.deepcopy(api.role_api.targets[first])
        api.role_api.targets[first]['rules'] = []
        try:
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
        finally:
            api.role_api.targets[first] = target
        mode['busy'], mode['late_fault'] = False, 'busy'
        assert run_operation()['phase'] == 'recovery-restore-roles'
        assert mode['busy'] and not attempts
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        mode['busy'], mode['late_fault'], mode['writing_calls'] = False, 'gateway_writer', 0
        with pytest.raises(ValueError):
            run_operation()
        assert mode['gateway_writer'] and not attempts
        assert {row['status'] for row in _read(child / 'roles.json')['roles'].values()} == {'prepared'}
        mode['late_fault'], mode['gateway_writer'] = None, False
        result = run_operation()
        assert mode['pending_restore'] is not None
        if not committed:
            assert result['phase'] == 'recovery-restore-roles' and len(attempts) == 1
            assert run_operation()['phase'] == 'recovery-restore-roles' and len(attempts) == 1
            assert apply(*mode['pending_restore'], persist=True).status_code == 200
            result = run_operation()
        assert result['phase'] == 'recovery-restart'
        assert attempts == sorted(originals)
        assert apply(*mode['pending_forward'], persist=True).status_code == 409
        for key, original in originals.items():
            assert roles[key]['rules'] == original['rules']
            assert roles[key]['metadata'].get('labels') == original['metadata'].get('labels')
            expected = {**original['metadata'].get('annotations', {}),
                stop.MARKER: str(bindings.migration.registration.spec.operation_id)}
            assert roles[key]['metadata']['annotations'] == expected
            assert _uid(roles[key]) == _uid(original)
        assert all(current_bindings[key] == row for key, row in fixed_bindings.items())
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert True in qualification
        for fault in ('missing', 'incomplete', 'extra'):
            mode[fault] = True
            with pytest.raises(ValueError):
                api.role_api.verify_permissions()
            mode[fault] = False
        api.role_api.verify_permissions()  # Completed recovery rights remain observable during restart.
        calls = len(attempts)
        with pytest.raises(ValueError):
            module.advance_shared_recovery_roles(bindings=bindings, api=api.role_api, state_dir=child)
        assert len(attempts) == calls


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_restarts_only_current_staging_with_real_pod_readiness(shared_operation, committed):
    """Real restart CAS/process reads; prior phase effects and SQL probes doubled."""
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops import nebius_shared_pool_workloads as staging
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_restart'
    assert importlib.util.find_spec(name) is not None, 'current-staging recovery restart is missing'
    module = __import__(name, fromlist=['HTTPSSharedRecoveryRestart'])
    args, bindings = shared_operation
    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-restart'
    captured = copy.deepcopy(bindings.plan.staging.originals)
    documents, rows = {}, {}
    mode = {'busy': False, 'busy_preview': True, 'late_busy': False, 'writing_calls': 0,
        'pending': None, 'unready': True, 'successor_pod': False, 'gateway_writer': False, 'roles_missing': False,
        'late_pod': None, 'bad_pod': None, 'challenge_reads': 0, 'unrelated_pod': False, 'late_unrelated': False}
    attempts, effects, challenges = [], [], []

    def refresh():
        documents.clear()
        for row in rows.values():
            meta = row['metadata']
            meta.setdefault('generation', 1)
            meta.setdefault('resourceVersion', '2')
            if row['kind'] == 'Deployment':
                row['status'] = {'observedGeneration': meta['generation'], **{field: row['spec']['replicas']
                    for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
            documents[runtime_resource_path(row) + '/' + meta['name']] = copy.deepcopy(row)
            for version, resource, kind in (('apps/v1', 'replicasets', 'ReplicaSet'), ('v1', 'pods', 'Pod'), ('batch/v1', 'jobs', 'Job')):
                prefix = '/api/v1' if version == 'v1' else '/apis/' + version
                documents.setdefault(prefix + '/namespaces/' + meta['namespace'] + '/' + resource,
                    {'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '1'}, 'items': []})
        populate_runtime_pods(documents)

    def apply(key, changes, *, persist):
        value = copy.deepcopy(rows[key])
        assert key in captured, 'restart cannot alter new dev/global, database or backup roots'
        assert changes[0] == {'op': 'test', 'path': '/metadata/uid', 'value': _uid(captured[key])}
        assert changes[1]['op'] == 'test' and changes[1]['path'] == '/metadata/resourceVersion'
        assert any(row['op'] == 'test' and row['path'] == '/spec' for row in changes)
        for change in changes:
            fields = change['path'].strip('/').split('/')
            target = value
            for field in fields[:-1]:
                target = target[field]
            field = fields[-1]
            if change['op'] == 'test':
                if target[field] != change['value']:
                    return httpx.Response(409, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure',
                        'code': 409, 'reason': 'Conflict'})
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'add', 'replace'}
                target[field] = change['value']
        assert value['spec'] == captured[key]['spec']
        assert value['metadata']['annotations'][stop.MARKER] == str(bindings.migration.registration.spec.operation_id)
        if persist:
            value['metadata']['resourceVersion'] = str(int(value['metadata']['resourceVersion']) + 1)
            value['metadata']['generation'] += 1
            rows[key] = value
            effects.append(key)
            refresh()
        return httpx.Response(200, json=value)

    def handle(message):
        path = message.url.path
        assert path in documents
        if message.method == 'GET':
            value = copy.deepcopy(documents[path])
            if value['kind'] == 'PodList':
                for pod in value['items']:
                    if mode['unready'] or (mode['bad_pod'] is not None and all(
                            pod['metadata'].get('labels', {}).get(label) == wanted
                            for label, wanted in rows[mode['bad_pod']]['spec']['selector']['matchLabels'].items())):
                        pod['status']['conditions'][0]['status'] = 'False'
                if mode['unrelated_pod'] and path == '/api/v1/namespaces/' + captured[target_key]['metadata']['namespace'] + '/pods':
                    value['items'].append({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'unrelated',
                        'namespace': captured[target_key]['metadata']['namespace'], 'uid': '00000000-0000-4000-8000-000000000778',
                        'labels': {'app': 'unrelated'}}, 'spec': {}, 'status': {'phase': 'Running'}})
            if value['kind'] == 'PodList' and mode['successor_pod']:
                root = next(row for key, row in rows.items() if key not in captured and row['kind'] == 'Deployment')
                if path == '/api/v1/namespaces/' + root['metadata']['namespace'] + '/pods':
                    value['items'].append({'apiVersion': 'v1', 'kind': 'Pod',
                        'metadata': {'name': 'retained-successor', 'namespace': root['metadata']['namespace'],
                            'uid': '00000000-0000-4000-8000-000000000777', 'labels': copy.deepcopy(root['spec']['selector']['matchLabels'])},
                        'spec': copy.deepcopy(root['spec']['template']['spec']), 'status': {'phase': 'Running'}})
            return httpx.Response(200, json=value)
        assert message.method == 'PATCH'
        key, changes = _key(documents[path]), json.loads(message.content)
        if message.url.params.get('dryRun') == 'All':
            if mode['busy_preview']:
                mode['busy_preview'], mode['busy'] = False, True
            return apply(key, changes, persist=False)
        assert not mode['busy'] and not mode['successor_pod'] and not mode['gateway_writer'] and not mode['roles_missing']
        assert _read(child / 'restart.json')['resources'][key]['status'] == 'intent'
        attempts.append(key)
        if len(attempts) == 1:
            mode['pending'] = key, changes
            if committed:
                apply(key, changes, persist=True)
            raise httpx.ReadError('simulated uncertain current-staging restart')
        return apply(key, changes, persist=True)

    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (module.prepare_shared_recovery_restart, module.validate_shared_recovery_restart, 'restart.json'),
    }
    with ExitStack() as stack:
        class API(SharedPhaseAPI):
            restart_api = None

            def prepare(self, phase, bound, directory):
                if phase in children:
                    children[phase][0](bindings=bound, state_dir=directory)
                elif phase == 'forward-stop':
                    staging.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=directory)
                elif phase == 'recovery-revoke':
                    machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                    gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
                else:
                    super().prepare(phase, bound, directory)

            def validate_prepared_phase(self, phase, directory):
                if phase in children and phase != 'forward-guards':
                    children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase in {'forward-guards', 'forward-stop', 'recovery-revoke'}:
                    self.validate_phase(phase, directory, complete=False)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase in children:
                    children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-stop':
                    staging.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=directory, complete=complete)
                elif phase == 'recovery-revoke':
                    machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                    gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
                if phase == 'forward-stop':
                    return False  # Test recovery before any forward workload dispatch.
                if phase == 'recovery-revoke':
                    path = directory / 'machines.json'
                    private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                    path = directory / 'gateway.json'
                    record = _read(path)
                    for row in record['roles'].values():
                        row['status'] = 'absent'
                    private_state._atomic_json(path, record)
                    return True
                if phase in children and phase != 'recovery-restart':
                    path = directory / children[phase][2]
                    record = _read(path)
                    if phase == 'forward-guards':
                        record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                    elif phase == 'recovery-fence':
                        record['pool'] = 'fenced'
                        record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                    elif phase == 'recovery-drain':
                        record['status'] = 'observed'
                    elif phase == 'recovery-stop':
                        for key, choices in stop._sources(bound, state).items():
                            before = _snapshot(choices[-1])
                            before['metadata']['uid'] = _uid(choices[-1])
                            expected = stop._stopped(bound, before)
                            record['resources'][key] = {'status': 'stopped', 'before': before,
                                'before_resource_version': '1', 'expected': expected}
                            rows[key] = {**copy.deepcopy(expected), 'metadata': {**expected['metadata'],
                                'uid': _uid(before), 'resourceVersion': '2'}}
                    elif phase == 'recovery-restore-workloads':
                        before, targets = templates._inputs(bound, state)
                        for key, row in record['resources'].items():
                            row.update(status='restored', before_resource_version=
                                '2' if _stable(before[key]) != _stable(targets[key]) else None)
                            rows[key] = {**copy.deepcopy(targets[key]), 'metadata': {**targets[key]['metadata'],
                                'uid': _uid(before[key]), 'resourceVersion': '3'}}
                    elif phase == 'recovery-restore-roles':
                        for row in record['roles'].values():
                            row.update(status='restored', before_resource_version='201')
                    private_state._atomic_json(path, record)
                    refresh()
                    return True
                if phase != 'recovery-restart':
                    return super().advance(phase, bound, directory)
                if self.restart_api is None:
                    def qualify(request, selected, writing):
                        assert request == runtime and selected == phase
                        if writing and mode['late_busy']:
                            mode['writing_calls'] += 1
                            if mode['writing_calls'] == 4:
                                mode['busy'] = True

                    def permissions(fault):
                        if mode[fault]:
                            raise ValueError('simulated changed recovery authority')

                    def ready(request, selected, before, desired, actual):
                        assert request == runtime and selected == phase
                        assert _key(before) in captured and actual['spec'] == desired['spec']
                        challenges.append(_key(actual))
                        if _key(actual) == mode['late_pod']:
                            mode['challenge_reads'] += 1
                            if mode['challenge_reads'] == 2:
                                if mode['late_unrelated']:
                                    mode['unrelated_pod'] = True
                                else:
                                    mode['bad_pod'] = _key(actual)
                        return True

                    self.restart_api = stack.enter_context(module.HTTPSSharedRecoveryRestart(bindings=bound,
                        api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                        ssl_context=ssl.create_default_context(), qualify_runtime=qualify, observe_ready=ready,
                        recovery_drained=lambda: not mode['busy'],
                        roles=SimpleNamespace(bindings=bound, state=state / 'recovery-restore-roles',
                            verify_permissions=lambda: permissions('roles_missing')),
                        gateway=SimpleNamespace(bindings=bound, state=state / 'recovery-revoke',
                            verify_permissions=lambda: permissions('gateway_writer'))))
                    connection_type = type(self.restart_api.client)
                    self.restart_api.client.close()
                    self.restart_api.client = connection_type(base_url=self.restart_api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_recovery_restart(bindings=bound, api=self.restart_api, state_dir=directory)

        api = API(parent)
        api.block = 'recovery-reopen'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}

        def run_operation():
            try:
                return parent.run_shared_pool_operation(**kwargs)
            except ValueError as exc:
                import traceback

                frames, cause = [], exc
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('restart failure at ' + ', '.join(frames)) from None

        assert run_operation()['phase'] == 'forward-stop'
        frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
        kwargs['action'] = 'recover'
        assert run_operation()['phase'] == 'recovery-restart'
        assert not attempts and mode['busy']
        untouched = {key: copy.deepcopy(row) for key, row in rows.items() if key not in captured}
        assert {row['status'] for row in _read(child / 'restart.json')['resources'].values()} <= {'prepared', 'started'}
        assert 'intent' not in {row['status'] for row in _read(child / 'restart.json')['resources'].values()}
        receipt = state / 'recovery-restore-roles/roles.json'
        saved = receipt.read_bytes()
        receipt.write_bytes(saved + b'\n')
        try:
            with pytest.raises(ValueError):
                api.restart_api.ready()
        finally:
            receipt.write_bytes(saved)
        target_key = next(key for key in captured if captured[key]['kind'] == 'Deployment')
        target = copy.deepcopy(api.restart_api.targets[target_key])
        api.restart_api.targets[target_key]['spec']['replicas'] = 2
        try:
            with pytest.raises(ValueError):
                api.restart_api.ready()
        finally:
            api.restart_api.targets[target_key] = target
        mode['busy'], mode['successor_pod'] = False, True
        assert run_operation()['phase'] == 'recovery-restart'
        assert not attempts
        mode['successor_pod'] = False
        for fault in ('gateway_writer', 'roles_missing'):
            mode[fault] = True
            with pytest.raises(ValueError):
                run_operation()
            assert not attempts
            mode[fault] = False
        mode['late_busy'] = True
        assert run_operation()['phase'] == 'recovery-restart'
        assert mode['busy'] and not attempts
        assert 'intent' not in {row['status'] for row in _read(child / 'restart.json')['resources'].values()}
        mode['busy'], mode['late_busy'] = False, False
        assert run_operation()['phase'] == 'recovery-restart'
        assert mode['pending'] is not None and len(attempts) == 1
        assert run_operation()['phase'] == 'recovery-restart' and len(attempts) == 1
        if not committed:
            assert apply(*mode['pending'], persist=True).status_code == 200
        # Actual unready Pods hold recovery before another root is started.
        assert run_operation()['phase'] == 'recovery-restart' and len(attempts) == 1
        mode['unready'] = False
        assert run_operation()['phase'] == 'recovery-reopen'
        changed = {key for key, value in captured.items()
            if value['spec']['suspend' if value['kind'] == 'CronJob' else 'replicas'] != (True if value['kind'] == 'CronJob' else 0)}
        assert set(attempts) == set(effects) == changed and len(attempts) == len(set(attempts))
        assert set(challenges) == captured.keys()
        for key, value in captured.items():
            assert rows[key]['spec'] == value['spec'] and _uid(rows[key]) == _uid(value)
            assert rows[key]['metadata']['annotations'] == {**value['metadata'].get('annotations', {}),
                stop.MARKER: str(bindings.migration.registration.spec.operation_id)}
        assert all(rows[key] == value for key, value in untouched.items())
        assert all(path.read_bytes() == raw for path, raw in frozen.items())
        assert api.restart_api.ready()
        mode['late_pod'] = next(key for key in reversed(api.restart_api.originals)
            if key in captured and captured[key]['kind'] == 'Deployment')
        assert api.restart_api.ready() is False
        assert mode['bad_pod'] is not None
        mode['bad_pod'], mode['challenge_reads'], mode['late_unrelated'] = None, 0, True
        assert api.restart_api.ready()  # Unrelated Pod activity is not a fixed-root readiness failure.
        assert mode['unrelated_pod']
        mode['late_pod'] = None
        mode['unready'] = True
        assert api.restart_api.ready() is False
        mode['unready'], mode['busy'] = False, True
        assert api.restart_api.ready()  # Later reopening observation is not an idle-work requirement.
        calls = len(attempts)
        with pytest.raises(ValueError):
            module.advance_shared_recovery_restart(bindings=bindings, api=api.restart_api, state_dir=child)
        assert len(attempts) == calls


def _exercise_completed_recovery_readers(bindings):
    """A fresh process may read completed authority without recreating old mutators."""
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_shared_pool_recovery_gateway import HTTPSSharedRecoveryGateway
    from scripts.ops.nebius_shared_pool_recovery_roles import HTTPSSharedRecoveryRoles
    from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER

    catalog = {_key(row): copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}}
    for key in bindings.plan.staging_role_originals:
        catalog[key]['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        catalog[key]['metadata']['resourceVersion'] = '202'
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    calls = []

    def handle(message):
        path = message.url.path
        calls.append((message.method, path))
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
        kinds = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole', 'clusterrolebindings': 'ClusterRoleBinding'}
        for resource, kind in kinds.items():
            if path == '/apis/rbac.authorization.k8s.io/v1/' + resource:
                assert message.method == 'GET'
                return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '71'}, 'items': [row for row in catalog.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            _, _, account_namespace, account = message.headers['Impersonate-User'].split(':')
            namespace = json.loads(message.content)['spec']['namespace']
            rules = []
            for row in catalog.values():
                if not row['kind'].endswith('Binding') or (row['kind'] == 'RoleBinding' and row['metadata']['namespace'] != namespace):
                    continue
                if not any(subject['kind'] == 'ServiceAccount' and (subject['namespace'], subject['name']) == (account_namespace, account)
                        for subject in row.get('subjects', [])):
                    continue
                reference = row['roleRef']
                key = reference['kind'] + ':' + (row['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(catalog[key]['rules']))
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
        assert message.method == 'GET', 'completed readers must not replay Kubernetes writes'
        value = next((row for row in catalog.values() if path == runtime_resource_path(row) + '/' + row['metadata']['name']), None)
        return httpx.Response(404) if value is None else httpx.Response(200, json=value)

    config = bindings.plan.development.runtime.request.database.foundation.inputs.config
    with HTTPSSharedRecoveryGateway(bindings=bindings, machines=None,
            api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context()) as gateway:
        gateway.client.close()
        gateway.client = type(gateway.client)(base_url=gateway.api_server, transport=httpx.MockTransport(handle))
        gateway.verify_permissions()
        with HTTPSSharedRecoveryRoles(bindings=bindings, templates=None, gateway=gateway,
                qualify_runtime=lambda *_: pytest.fail('completed reader must not call the former write-phase qualifier'),
                api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context()) as roles:
            roles.client.close()
            roles.client = type(roles.client)(base_url=roles.api_server, transport=httpx.MockTransport(handle))
            roles.verify_permissions()
            key = next(iter(bindings.plan.staging_role_originals))
            saved = catalog[key]['rules']
            catalog[key]['rules'] = []
            with pytest.raises(ValueError):
                roles.verify_permissions()  # Completed receipts cannot excuse current missing rights.
            catalog[key]['rules'] = saved
            with pytest.raises(ValueError):
                roles.restore_role(key, record_intent=lambda *_: pytest.fail('completed Role writer must not dispatch'))
        with pytest.raises(ValueError):
            gateway.qualify()  # Write-phase qualification still requires the active revoke phase.
    assert any(method == 'POST' for method, _ in calls)


@pytest.mark.timeout(600)
@pytest.mark.parametrize('committed', [False, True])
def test_shared_recovery_reopens_only_staging_once_and_requires_fresh_terminal_health(
        shared_operation, shared_guard_transport, shared_runtime, completed_pool, handoff, monkeypatch, committed):
    """Real reopening/DB adapters; prior effects and runtime/provider probes doubled."""
    import importlib.util
    from types import SimpleNamespace

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
    from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target

    name = 'scripts.ops.nebius_shared_pool_recovery_reopening'
    assert importlib.util.find_spec(name) is not None, 'staging-only recovery reopening is missing'
    module = __import__(name, fromlist=['SharedRecoveryReopeningTransport'])
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    child = state / 'recovery-reopen'
    staging_id = str(bindings.plan.staging.attachment.participant.participant_id)
    roster = tuple(str(target.participant_id) for target in bindings.migration.guards)
    dev_id, = (key for key in roster if key != staging_id)

    class Remote:
        mode = 'fenced'
        machines = 'revoked'
        healthy = False
        late_busy = False
        late_guard = False
        change_receipt = False

        def __init__(self):
            self.local = dict.fromkeys(roster, 'fenced')
            self.reads, self.dispatches = [], []
            self.pool_counts = dict.fromkeys(('unstarted_requests', 'active_requests', 'unconfirmed_creates', 'unqualified_releases'), 0)
            self.local_counts = {key: dict.fromkeys(('trials', 'executions', 'builds', 'build_cleanup',
                'execution_outboxes', 'build_outboxes'), 0) for key in roster}

        def verify_retained(self):
            pass  # Full-parent writer/provider checks are a separate integration boundary.

        def ready(self):
            if self.late_busy:
                self.late_busy = False
                self.local_counts[staging_id]['build_outboxes'] = 1
            if self.late_guard:
                self.late_guard = False
                self.local[dev_id] = 'open'
            if self.change_receipt:
                self.change_receipt = False
                receipt = state / 'recovery-restart/restart.json'
                receipt.write_bytes(receipt.read_bytes() + b'\n')
            return self.healthy

        def after_machine_observation(self):
            pass

        def pool_state(self):
            return self.mode

        def guard_state(self, participant):
            return self.local[participant]

        def release_staging(self, participant):
            assert participant == staging_id, 'recovery must never reopen the cancelled dev participant'
            assert _read(child / 'reopen.json')['status'] == 'intent'
            assert self.mode == 'fenced' and self.machines == 'revoked' and self.healthy
            assert set(self.local.values()) == {'fenced'}
            assert not any(self.pool_counts.values()) and not any(self.local_counts[participant].values())
            self.dispatches.append(participant)
            if committed:
                self.local[participant] = 'open'
                self.local_counts[participant]['trials'] = 1  # Newly admitted staging work is allowed.
            raise TimeoutError('simulated lost recovery release reply')

    remote = Remote()
    guard_api, database_model = shared_guard_transport
    _shared_recovery_database_io(remote=remote, bindings=bindings, guard_api=guard_api, database_model=database_model,
        shared_runtime=shared_runtime, completed_pool=completed_pool, handoff=handoff, monkeypatch=monkeypatch)
    credential = guard_api._get('secret', 'loom-platform-db', bindings.migration.registration.binding.namespace)
    history = KubectlPoolHistoryAPI(request=bindings.migration, target=_gateway_history_target(bindings, credential),
        kubeconfig=guard_api.kubeconfig, executable=Path(guard_api.prefix[0]))
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (module.prepare_shared_recovery_reopening, module.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (module.prepare_shared_recovery_verification, module.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(SharedPhaseAPI):
        reopen_api = None
        interrupt_verification = False
        completed_readers_checked = False

        def qualify(self, bound, record, *, recovering):
            super().qualify(bound, record, recovering=recovering)
            path = state / 'recovery-verify/verify.json'
            if (self.interrupt_verification and path.exists() and _read(path)['status'] == 'observed'
                    and record['recovery']['recovery-verify']['status'] == 'started'):
                self.interrupt_verification = False
                raise TimeoutError('simulated interruption after saving verification')

        def prepare(self, phase, bound, directory):
            if phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-revoke'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                path = directory / 'gateway.json'
                record = _read(path)
                for row in record['roles'].values():
                    row['status'] = 'absent'
                private_state._atomic_json(path, record)
                return True
            if phase in {'recovery-reopen', 'recovery-verify'}:
                if not self.completed_readers_checked:
                    _exercise_completed_recovery_readers(bound)
                    self.completed_readers_checked = True
                if self.reopen_api is None:
                    self.reopen_api = module.SharedRecoveryReopeningTransport(bindings=bound, guards=guard_api,
                        history=history, qualify=remote.verify_retained,
                        restart=SimpleNamespace(bindings=bound, state=state / 'recovery-restart', ready=remote.ready))
                function = module.advance_shared_recovery_reopening if phase == 'recovery-reopen' else module.advance_shared_recovery_verification
                return function(bindings=bound, api=self.reopen_api, state_dir=directory)
            # Earlier phase effects are doubled; their real validators still
            # qualify these journals. This is not a full-parent acceptance test.
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
            elif phase == 'recovery-drain':
                record['status'] = 'observed'
            elif phase == 'recovery-stop':
                for key, choices in stop._sources(bound, state).items():
                    before = _snapshot(choices[-1])
                    before['metadata']['uid'] = _uid(choices[-1])
                    record['resources'][key] = {'status': 'stopped', 'before': before,
                        'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
            elif phase in {'recovery-restore-workloads', 'recovery-restart'}:
                component = templates if phase == 'recovery-restore-workloads' else restart
                before, targets = component._inputs(bound, state)
                for key, row in record['resources'].items():
                    row.update(status='restored' if component is templates else 'started',
                        before_resource_version='2' if _stable(before[key]) != _stable(targets[key]) else None)
            elif phase == 'recovery-restore-roles':
                for row in record['roles'].values():
                    row.update(status='restored', before_resource_version='201')
            else:
                pytest.fail('unexpected predecessor phase: ' + phase)
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    api.block = 'forward-guards'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
    api.block, kwargs['action'] = 'recovery-verify', 'recover'
    try:
        result = parent.run_shared_pool_operation(**kwargs)
    except ValueError as exc:
        import traceback

        frames, cause = [], exc
        while cause is not None:
            frames.extend(f'{type(cause).__name__}:{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                for frame in traceback.extract_tb(cause.__traceback__))
            cause = cause.__context__
        raise AssertionError('reopening failed at ' + ', '.join(frames)) from None
    assert result['phase'] == 'recovery-reopen'
    assert not remote.dispatches and _read(child / 'reopen.json')['status'] == 'prepared'
    remote.healthy, remote.late_busy = True, True
    assert api.reopen_api.release_staging(record_intent=lambda: pytest.fail('busy release must not consume intent')) is False
    assert not remote.dispatches and _read(child / 'reopen.json')['status'] == 'prepared'
    remote.local_counts[staging_id]['build_outboxes'] = 0
    remote.late_guard = True
    with pytest.raises(ValueError):
        api.reopen_api.observe()
    remote.local[dev_id] = 'fenced'
    receipt = state / 'recovery-restart/restart.json'
    raw = receipt.read_bytes()
    remote.change_receipt = True
    try:
        with pytest.raises(ValueError):
            api.reopen_api.observe()
    finally:
        receipt.write_bytes(raw)
    receipt = child / 'reopen.json'
    displaced = receipt.with_suffix('.held')
    receipt.rename(displaced)
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert not receipt.exists() and not remote.dispatches
    displaced.rename(receipt)
    remote.local[staging_id] = 'open'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)  # An open guard cannot be adopted before intent.
    remote.local[staging_id] = 'fenced'
    for _ in range(2):
        result = parent.run_shared_pool_operation(**kwargs)
        assert result['phase'] == ('recovery-verify' if committed else 'recovery-reopen')
        assert remote.dispatches == [staging_id]
        api.reopen_api = None  # A reconstructed transport must not redispatch either outcome.
    remote.local[staging_id] = 'open'  # Resolve the deferred commit without replay.
    remote.local_counts[staging_id]['trials'] = 1
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    api.block, remote.healthy = None, False
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    assert _read(state / 'recovery-verify/verify.json')['status'] == 'prepared'
    remote.healthy = True
    remote.local_counts[dev_id]['build_cleanup'] = 1
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    remote.local_counts[dev_id]['build_cleanup'] = 0
    remote.machines = 'active'
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    remote.machines = 'revoked'
    api.interrupt_verification = True
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert _read(state / 'recovery-verify/verify.json')['status'] == 'observed'
    remote.healthy = False
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    remote.healthy = True
    remote.reads.clear()
    result = parent.run_shared_pool_operation(**kwargs)
    assert result['status'] == 'staging_restored_dev_closed' and result['admission_open'] is False
    assert remote.local == {staging_id: 'open', dev_id: 'fenced'}
    assert remote.mode == 'fenced' and remote.machines == 'revoked'
    assert remote.dispatches == [staging_id] and staging_id not in remote.reads
    assert 'pool' in remote.reads and dev_id in remote.reads
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    with pytest.raises(ValueError):
        module.advance_shared_recovery_reopening(bindings=bindings, api=api.reopen_api, state_dir=child)
    assert remote.dispatches == [staging_id]


@pytest.mark.timeout(420)
async def test_shared_provider_uses_installed_observer_and_retains_scale_zero(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff,
        publisher_cloud, monkeypatch, tmp_path):
    """The common-pool preflight must use actual observer IAM/pool readbacks."""
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_provider
    from tests.ops.test_nebius_development_pool_retained import (
        runtime_cloud_transport,
        runtime_http_inventory,
    )

    request, parent = shared_runtime
    calls, documents, handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    operator, sessions, reads, closed, mode = runtime_cloud_transport(request, publisher_cloud, monkeypatch, tmp_path)
    spec = request.database.manager.retained.request.registration.spec
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(handler))

        async def observe():
            return await observe_shared_pool_provider(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=observer, operator_credentials=operator)

        result = await observe()
        assert result.provider.node_group.id == spec.node_group_id
        assert result.provider.node_count == result.provider.ready_node_count == result.provider.target_node_count == 0
        assert result.provider.provider_capacity_state == 'available' and result.provider.autoscaler_state == 'ready'
        assert reads == [('quotas', request.collector_scope.account_id), ('nodegroup', request.collector_scope.account_id)]
        assert sessions == closed and not sessions[-1].path.exists()
        assert 'provider' not in repr(result) and all(row.method == 'GET' for row in calls)
        for state in ('exhausted', 'scaling'):
            mode['damage'] = state
            current = await observe()
            if state == 'exhausted':
                assert current.provider.used_nodes == current.provider.quota_nodes
            else:
                assert current.provider.autoscaler_state == 'scaling'

        for damage in ('deleting', 'cluster', 'credential'):
            mode['damage'] = damage
            if damage == 'credential':
                secret = next(row for row in documents.values() if row['kind'] == 'Secret'
                    and row.get('data', {}).get('credentials.json')
                    and row['metadata']['namespace'] == spec.participants[0].execution_namespace.name)
                previous_uid = secret['metadata']['uid']
                secret['metadata']['uid'] = str(uuid4())
                before = len(reads)
            with pytest.raises(ValueError, match='shared pool provider unqualified'):
                await observe()
            if damage == 'credential':
                assert len(reads) == before  # Fail before exchanging a mismatched installed identity.
                secret['metadata']['uid'] = previous_uid
            assert sessions == closed
        assert calls and all(row.method == 'GET' for row in calls)
