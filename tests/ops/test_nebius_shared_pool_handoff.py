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
                if phase == 'forward-build-policy':
                    return build_child.prepare_shared_build_activation(bindings=bound, state_dir=directory)
                if phase != 'forward-gateway':
                    return super().prepare(phase, bound, directory)
                module.prepare_shared_gateway(bindings=bound, state_dir=directory)
                if mode['prepare_loss']:
                    mode['prepare_loss'] = False
                    raise TimeoutError('simulated interruption after effect-free child preparation')

            def validate_prepared_phase(self, phase, directory):
                if phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                else:
                    super().validate_prepared_phase(phase, directory)

            def validate_phase(self, phase, directory, *, complete):
                if phase == 'forward-build-policy':
                    build_child.validate_shared_build_activation(bindings=bindings, state_dir=directory, complete=complete)
                elif phase == 'forward-gateway':
                    module.validate_shared_gateway(bindings=bindings, state_dir=directory, complete=complete)
                else:
                    super().validate_phase(phase, directory, complete=complete)

            def advance(self, phase, bound, directory):
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
@pytest.mark.parametrize('forward_processes', [False, True], ids=['initial', 'forward'])
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
    from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND
    from scripts.ops.nebius_pool_legacy_settings import (
        BOUND_LEGACY_SETTINGS_COMMAND,
        expected_legacy_runtime_settings,
    )
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql
    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_DATABASE_COMMAND,
        _BOUND_TELEMETRY_COMMAND,
        pool_runtime_role_sql,
    )
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )
    from scripts.ops.nebius_pool_startup_capacity import (
        BOUND_POOL_CAPACITY_COMMAND,
        expected_startup_capacity,
    )
    from scripts.ops.nebius_pool_startup_database import pool_startup_closed_sql
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
                    assert sql == pool_startup_closed_sql(spec)
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
                    if sql == rollout_guard_observation_sql(owner=str(spec.operation_id), candidate=request.database.foundation.inputs.candidate['candidate_sha']):
                        value = {'status': 'held' if mode['damage'] == 'guard-held' or namespace in mode.get('held', set()) else 'open'}
                        checked.add(('guard', namespace))
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
            elif args[9] in {BOUND_GATEWAY_KUBERNETES_COMMAND, BOUND_POOL_CAPACITY_COMMAND}:
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
                    wanted = expected_startup_capacity(spec)
                    check = 'capacity'
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
        if forward_processes:
            with HTTPSSharedPoolRuntimeProbes(bindings=result.bindings,
                    api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
                    ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
                    token='operator-test') as shared_probes:
                for connection in (shared_probes, shared_probes.manager, shared_probes.foundation):
                    connection.client.close()
                    connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
                _exercise_forward_processes(bindings=result.bindings, development=shared_probes, documents=documents,
                    mode=mode, checked=checked, commands=commands, add=add,
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


def _exercise_forward_processes(*, bindings, development, documents, mode, checked, commands, add, args):
    """Real parent, process transports and readiness validators; no live effects."""
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
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
            if phase == 'forward-guards':
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
            if phase == 'forward-guards':
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
            if phase == 'forward-guards':
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
    assert observe().pending_workloads == ()
    assert {'new-closed', 'old-revoked', ('runtime-role', 'loom-dev')} <= checked
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
    for damage in ('old-active', 'new-not-closed', 'settings:' + bindings.plan.staging.attachment.guard.namespace,
            'database:loom-dev', 'late-staging-actuator', 'late-staging-restart', 'late-dev-restart', 'late-staging-template'):
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
    for damage in ('gateway-settings', 'gateway-database', 'gateway-kubernetes', 'gateway-capacity',
            'late-gateway-secret', 'late-gateway-restart', 'late-gateway-database-restart', 'late-gateway-receipt',
            'late-manager-secret-after-gateway', 'late-manager-database-after-gateway'):
        baseline, receipt = copy.deepcopy(documents), start_path.read_bytes()
        mode.update(damage=damage, capacity_seen=False, late_injected=False)
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
    guard_path.unlink()
    before = len(commands)
    with pytest.raises(ValueError):
        observe()
    assert len(commands) == before


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
        mode = 'closed'
        guards = dict.fromkeys(roster, 'held')
        failure = ('open', None, 'before')
        qualified = False

        def verify_retained(self):
            if not self.qualified:
                raise ValueError('simulated current authority loss')

        def read_workload(self, key):
            pytest.fail('shared activation must not reconstruct old startup workloads')

        def qualify_runtime(self):
            self.verify_retained()
            assert self.mode == 'closed' and set(self.guards.values()) == {'held'}

        def pool_state(self):
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
        def prepare(self, phase, bound, directory):
            if phase == 'forward-activate':
                module.prepare_shared_pool_activation(bindings=bound, state_dir=directory)
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
    with pytest.raises(ValueError):
        module.prepare_shared_pool_activation(bindings=bindings, state_dir=child)
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert effects == []
    assert json.loads((child / 'activation.json').read_bytes())['opening'] == 'prepared'
    remote.qualified = True
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None)]  # An unknown opening is never replayed.
    retained = (child / 'activation.json').read_bytes()
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
    remote.failure = ('release', roster[0], 'before')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    remote.failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert effects == [('open', None), ('release', roster[0])]
    remote.guards[roster[0]] = 'open'
    remote.failure = ('release', roster[1], 'after')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    assert set(remote.guards.values()) == {'open'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert effects == [('open', None), *(('release', key) for key in roster)]
    module.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=True)


@pytest.mark.timeout(420)
@pytest.mark.parametrize('forward_census', [False, True], ids=['initial', 'forward'])
def test_shared_workload_census_binds_current_roots_and_rejects_unaccounted_writers(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, shared_operation, forward_census):
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
            _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode)
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


def _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode):
    """Observe real parent/child receipts; remote transitions are fixture state.

    Accepting an unrecorded successor, an old template after completion, or a
    changed receipt during the census must fail independently of the mutator.
    """
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_gateway as gateway_child
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
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    class CensusPhaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, child):
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
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
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
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
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
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
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                return all(row['status'] == 'applied' for row in json.loads((child / 'transition.json').read_bytes())['resources'].values())
            if phase == 'forward-roles':
                return all(status == 'restricted' for status in json.loads((child / 'roles.json').read_bytes())['roles'].values())
            if phase == 'forward-material':
                child_state = material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False)
                return child_state['stage'] is not None and all(row['status'] == 'created'
                    for row in child_state['stage']['resources'].values())
            if phase == 'forward-gateway':
                return False
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
    api.block = 'recovery-fence'
    assert parent.run_shared_pool_operation(**{**kwargs, 'action': 'recover'})['phase'] == 'recovery-fence'
    before = len(calls)
    with pytest.raises(ValueError):
        observe()
    assert len(calls) == before


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
