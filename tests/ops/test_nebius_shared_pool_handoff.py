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
    from scripts.ops.nebius_pool_migration_guard import _BOUND_GUARD_COMMAND, KubectlPoolGuardAPI
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
    model = SimpleNamespace(held=set(), busy=None, uncertain=None, acquires=[], documents=documents)

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
                assert args[-1] == rollout_guard_observation_sql(owner=str(request.registration.spec.operation_id),
                    candidate=request.registration.candidate['candidate_sha'])
                value = {'status': 'held' if namespace in model.held else 'open'}
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
def test_shared_parent_connects_current_staging_workload_transitions_without_replaying_unknown_patch(shared_operation):
    import importlib.util
    import json
    import ssl
    from contextlib import ExitStack

    import httpx
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
    mode = {'lose_reply': True, 'lose_read': False}
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
        class ConnectedWorkloadsAPI(SharedPhaseAPI):
            def prepare(self, phase, bound, state):
                if phase in phases:
                    module.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=state)
                else:
                    super().prepare(phase, bound, state)

            def validate_prepared_phase(self, phase, state):
                if phase in phases:
                    module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state,
                        complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, state)

            def validate_phase(self, phase, state, *, complete):
                if phase in phases:
                    module.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=state, complete=complete)
                else:
                    super().validate_phase(phase, state, complete=complete)

            def advance(self, phase, bound, state):
                if phase not in phases:
                    return super().advance(phase, bound, state)
                api = stack.enter_context(module.HTTPSSharedStagingWorkloads(bindings=bound, phase=phase,
                    api_server=runtime.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=lambda *_: True))
                connection_type = type(api.client)
                api.client.close()
                api.client = connection_type(base_url=api.api_server, transport=httpx.MockTransport(handle))
                return module.advance_shared_staging_workloads(bindings=bound, phase=phase, api=api, state_dir=state)

        api = ConnectedWorkloadsAPI(parent)
        api.block = 'forward-build-policy'
        kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
        manager = runtime.database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
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
def test_shared_process_preflight_reads_current_databases_and_retired_authority(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, monkeypatch):
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
    from scripts.ops.nebius_pool_legacy_settings import (
        BOUND_LEGACY_SETTINGS_COMMAND,
        expected_legacy_runtime_settings,
    )
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql
    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_DATABASE_COMMAND,
        _BOUND_TELEMETRY_COMMAND,
    )
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )
    from scripts.ops.nebius_pool_startup_database import pool_startup_closed_sql
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
        plural = {'Secret': 'secrets', 'Namespace': 'namespaces', 'Deployment': 'deployments',
            'StatefulSet': 'statefulsets', 'Service': 'services', 'ReplicaSet': 'replicasets'}[row['kind']]
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        namespace = row['metadata'].get('namespace')
        path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
        return path

    for row in attachment.resources:
        if row['kind'] in {'Namespace', 'Deployment', 'StatefulSet', 'Service'}:
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
    for row in documents.values():
        if row['kind'] == 'Secret':
            row['metadata'].setdefault('resourceVersion', '301')
    populate_runtime_pods(documents)
    populate_database_backends(documents)
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
            return httpx.Response(200, json=read(str(message.url)))
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
                plural = {'namespace': 'namespaces', 'deployment': 'deployments', 'replicaset': 'replicasets',
                    'secret': 'secrets', 'service': 'services', 'statefulset': 'statefulsets'}[kind]
                prefix = '/apis/apps/v1' if kind in {'deployment', 'replicaset', 'statefulset'} else '/api/v1'
                value = read(prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name)
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
                    assert sql == pool_startup_closed_sql(spec)
                    value = {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
                        'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
                        'qualified': mode['damage'] != 'new-not-closed'}
                    checked.add('new-closed')
                else:
                    assert namespace in {'loom-dev', staging.namespace}
                    assert sql == rollout_guard_observation_sql(owner=str(spec.operation_id), candidate=request.database.foundation.inputs.candidate['candidate_sha'])
                    value = {'status': 'held' if mode['damage'] == 'guard-held' else 'open'}
                    checked.add(('guard', namespace))
            elif args[9] == _BOUND_TELEMETRY_COMMAND:
                assert args[10] == attachment.participant.execution_namespace.name
                assert args[-2:] == ['platform-test', node_uid]
                value = {'status': 'blocked', 'stage': 'counters'}
                checked.add('telemetry')
                if mode['damage'] == 'late-staging-actuator':
                    secret, = (row for row in documents.values() if row['kind'] == 'Secret'
                        and row['metadata']['uid'] == str(staging.database.actuator_credential_uid))
                    secret['metadata']['resourceVersion'] = 'late-rotation'
            else:
                assert args[7:9] == ['python', '-c']
                code, component, nonce, response = args[9:]
                name = args[3].removeprefix('pod/').removesuffix('-a12b34-test')
                role = roles[namespace, name]
                workload = documents['/apis/apps/v1/namespaces/' + namespace + '/deployments/' + name]
                if code == _BOUND_DATABASE_COMMAND:
                    assert component == ('service' if role == 'manager' else role)
                    variable = {'service': 'LOOM_SVC_DB_URL', 'controller': 'LOOM_CP_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
                    env, = (row for row in workload['spec']['template']['spec']['containers'][0]['env'] if row['name'] == variable)
                    reference = env['valueFrom']['secretKeyRef']
                    secret = documents['/api/v1/namespaces/' + namespace + '/secrets/' + reference['name']]
                    url = base64.b64decode(secret['data'][reference['key']]).decode()
                    payload = (url if component == 'actuator' else str(PostgresDsn(url))).encode()
                    assert url not in str(argv)
                    check = 'database'
                else:
                    assert component == role
                    if namespace in {staging.namespace, attachment.participant.execution_namespace.name}:
                        assert code == BOUND_LEGACY_SETTINGS_COMMAND
                        settings = expected_legacy_runtime_settings(component, workload)
                    else:
                        assert code == BOUND_POOL_SETTINGS_COMMAND
                        machine, = (row for row in spec.machines if row.participant_id == dev.participant_id and row.workload_scope == 'environment')
                        settings = expected_pool_runtime_settings(component, workload,
                            token_sha256=machine.token_sha256 if component in {'controller', 'actuator'} else None,
                            catalog_sha256=hashlib.sha256(spec.profiles.model_dump_json().encode()).hexdigest() if component == 'manager' else None)
                    payload = json.dumps(settings, sort_keys=True, separators=(',', ':')).encode()
                    check = 'settings'
                assert hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest() == response
                checked.add((check, namespace, name))
                value = {'status': 'wrong' if mode['damage'] == check + ':' + namespace else 'qualified'}
                if mode['damage'] == 'late-old-secret':
                    documents[old_secret_path]['metadata']['resourceVersion'] = 'changed'
            assert args[7] in {'psql', 'python'}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', execute)
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


@pytest.mark.timeout(420)
def test_shared_workload_census_binds_current_roots_and_rejects_unaccounted_writers(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff):
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
