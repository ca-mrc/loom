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


@pytest.mark.timeout(420)
def test_shared_handoff_binds_real_completed_dev_and_current_staging(shared_runtime, staging_history, staging_observation):
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


@pytest.mark.timeout(420)
def test_shared_workload_census_binds_current_roots_and_rejects_unaccounted_writers(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff):
    """A historical template or a stopped clone must not hide a live writer."""
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
    roots = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.request.platform_consumers,
        *staging_history.completion.workloads.values(), *documents.values(), *staging_observation['resources'])
        if row['kind'] in {'Deployment', 'StatefulSet', 'CronJob'}}
    inventory = {plural: [] for plural in kinds.values()}
    for row in roots.values():
        row['metadata']['resourceVersion'] = '401'
        inventory[kinds[row['kind']]].append(row)
    controller = roots['Deployment:' + staging_observation['namespace'] + ':loom-control-plane']
    replica = writer_descendant(controller, 'ReplicaSet')
    inventory['replicasets'].append(replica)
    inventory['pods'].append(writer_descendant(replica, 'Pod'))
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
        if plural != 'deployments':
            assert version == '500' and message.url.params['resourceVersionMatch'] == 'Exact'
        return httpx.Response(200, json={'apiVersion': 'v1' if plural in {'pods', 'replicationcontrollers'}
            else 'batch/v1' if plural in {'jobs', 'cronjobs'} else 'apps/v1', 'kind': kind + 'List',
            'metadata': {'resourceVersion': mode['revision'] if plural == 'pods' else '500', 'continue': ''},
            'items': inventory[plural]})

    with ManagementKubernetesTransport(api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as transport:
        transport.client.close()
        transport.client = type(completed_pool[3].api.client)(base_url=transport.api_server,
            transport=httpx.MockTransport(respond))
        from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_workloads

        def observe():
            return observe_shared_pool_workloads(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, transport=transport)

        result = observe()
        assert result.resource_version == '500'
        assert result.workloads.keys() == roots.keys()
        assert result.workloads[_key(controller)] == controller
        gateway, = (row for row in staging_history.completion.workloads.values()
            if row['metadata']['name'] == 'loom-pool-gateway')
        assert result.workloads[_key(gateway)]['spec']['replicas'] == 0
        original = copy.deepcopy(inventory)
        for damage in ('clone', 'cronjob', 'root_uid', 'old_template', 'revived_gateway', 'missing_root', 'revision'):
            inventory.clear()
            inventory.update(copy.deepcopy(original))
            if damage == 'clone':
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
            with pytest.raises(ValueError, match='shared pool workload census unqualified'):
                observe()
            mode['revision'] = '500'
        assert calls and all(row.method == 'GET' for row in calls)
