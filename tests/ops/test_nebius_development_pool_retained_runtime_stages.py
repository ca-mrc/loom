"""Runtime successors consume closed pool evidence without replaying installation."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import json
import ssl
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from tests.ops.test_nebius_development_build_cloud import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud
from tests.ops.test_nebius_development_management_foundation import (
    development_inputs as original_development_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_management_foundation import entry as entry
from tests.ops.test_nebius_development_management_foundation import handoff as handoff
from tests.ops.test_nebius_development_management_foundation import live as live
from tests.ops.test_nebius_development_management_foundation import preflight as preflight
from tests.ops.test_nebius_development_management_foundation import publication as publication
from tests.ops.test_nebius_development_management_foundation import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_management_foundation import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_pool_install import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_install import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_pool_install import (
    connected as connected,
)
from tests.ops.test_nebius_development_pool_install import (
    installation as installation,
)
from tests.ops.test_nebius_development_pool_install import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_pool_install import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    manager_entry as original_manager_entry,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_install import (
    material as material,
)
from tests.ops.test_nebius_development_pool_install import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_inputs as original_pool_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_install import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    retained as retained,
)
from tests.ops.test_nebius_development_pool_install import (
    route as route,
)
from tests.ops.test_nebius_development_pool_install import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_development_pool_retained import RuntimeParentAPI as RuntimeParentAPI
from tests.ops.test_nebius_development_pool_retained import build_material as build_material
from tests.ops.test_nebius_development_pool_retained import build_policy as build_policy
from tests.ops.test_nebius_development_pool_retained import (
    catalog_runtime_stage as catalog_runtime_stage,
)
from tests.ops.test_nebius_development_pool_retained import completed_pool as completed_pool
from tests.ops.test_nebius_development_pool_retained import database_runtime as database_runtime
from tests.ops.test_nebius_development_pool_retained import development_inputs as development_inputs
from tests.ops.test_nebius_development_pool_retained import (
    foundation_publication as foundation_publication,
)
from tests.ops.test_nebius_development_pool_retained import manager_entry as manager_entry
from tests.ops.test_nebius_development_pool_retained import module as module
from tests.ops.test_nebius_development_pool_retained import network_runtime as network_runtime
from tests.ops.test_nebius_development_pool_retained import (
    parent_install_fixture as parent_install_fixture,
)
from tests.ops.test_nebius_development_pool_retained import pool_inputs as pool_inputs
from tests.ops.test_nebius_development_pool_retained import (
    populate_database_backends as populate_database_backends,
)
from tests.ops.test_nebius_development_pool_retained import (
    populate_runtime_pods as populate_runtime_pods,
)
from tests.ops.test_nebius_development_pool_retained import (
    protected_runtime_request as protected_runtime_request,
)
from tests.ops.test_nebius_development_pool_retained import publication_inputs as publication_inputs
from tests.ops.test_nebius_development_pool_retained import reference as reference
from tests.ops.test_nebius_development_pool_retained import (
    runtime_catalog_https as runtime_catalog_https,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_cloud_transport as runtime_cloud_transport,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_database_api as runtime_database_api,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_database_https as runtime_database_https,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_database_live as runtime_database_live,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_database_stage as runtime_database_stage,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_github_transport as runtime_github_transport,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_http_inventory as runtime_http_inventory,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_install_request as runtime_install_request,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_private_reader as runtime_private_reader,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_publication as runtime_publication,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_resources_api as runtime_resources_api,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_workload_options as runtime_workload_options,
)
from tests.ops.test_nebius_development_pool_retained import (
    runtime_workloads_api as runtime_workloads_api,
)
from tests.ops.test_nebius_development_pool_retained import system_kubectl as system_kubectl


@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_probe_finds_system_kubectl_without_trusting_ambient_path(
        completed_pool, publisher_cloud, monkeypatch, tmp_path, system_kubectl):
    from scripts.ops import nebius_development_runtime_probes as module

    _, request = runtime_install_request(completed_pool, publisher_cloud)
    system, untrusted = system_kubectl
    for ambient in ('protected', 'untrusted', 'missing-system-tool'):
        if ambient == 'missing-system-tool':
            (system / 'kubectl').unlink()
        monkeypatch.setenv('PATH', module.os.defpath if ambient == 'protected' else str(untrusted))
        arguments = dict(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(
                cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='operator-test')
        if ambient == 'missing-system-tool':
            with pytest.raises(ValueError, match='probe transport unavailable'):
                with module.HTTPSDevelopmentRuntimeProbes(**arguments):
                    pass
            continue
        with module.HTTPSDevelopmentRuntimeProbes(**arguments) as probes:
            assert probes.prefix[0] == str(system / 'kubectl')
            connection = Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1])
            assert connection.is_file() and connection.stat().st_mode & 0o077 == 0
        assert not connection.exists()



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_closed_database_probe_is_bound_readonly_and_independent_of_manager_process(
        completed_pool, publisher_cloud, monkeypatch, system_kubectl):
    from types import SimpleNamespace

    from loom_service.pool_management.capacity import digest

    _, request = runtime_install_request(completed_pool, publisher_cloud)
    name = 'scripts.ops.nebius_development_runtime_probes'
    if importlib.util.find_spec(name) is None:
        pytest.fail('concrete development runtime database probe is missing')
    module = importlib.import_module(name)
    pool = completed_pool[3].server
    namespace = request.database.manager.retained.request.retained.binding.namespace
    spec = request.database.manager.retained.request.registration.spec
    documents = copy.deepcopy(pool.store.resources)
    database = documents['StatefulSet:loom-postgres']
    database['status'].update(currentRevision='db-rev', updateRevision='db-rev', currentReplicas=1, updatedReplicas=1)
    service = documents['Service:loom-postgres']
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'loom-postgres-0', 'namespace': namespace,
        'uid': str(uuid4()), 'labels': {'app': 'loom-postgres', 'controller-revision-hash': 'db-rev'},
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': 'loom-postgres',
            'uid': database['metadata']['uid'], 'controller': True}]},
        'spec': copy.deepcopy(database['spec']['template']['spec']), 'status': {'phase': 'Running',
            'podIP': '10.20.0.2', 'podIPs': [{'ip': '10.20.0.2'}],
            'containerStatuses': [{'name': 'loom-postgres', 'ready': True, 'restartCount': 0}]}}
    pod['spec'].setdefault('volumes', []).append({'name': 'data',
        'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
    endpoints = {'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSliceList',
        'metadata': {'resourceVersion': '1'}, 'items': [{'metadata': {'namespace': namespace,
            'name': 'loom-postgres-test', 'uid': str(uuid4()),
            'labels': {'kubernetes.io/service-name': 'loom-postgres'}, 'ownerReferences': [
                {'apiVersion': 'v1', 'kind': 'Service', 'name': 'loom-postgres',
                    'uid': service['metadata']['uid'], 'controller': True}]},
            'addressType': 'IPv4', 'ports': [{'name': service['spec']['ports'][0]['name'], 'port': 5432}],
            'endpoints': [{'addresses': ['10.20.0.2'], 'conditions': {'ready': True},
                'targetRef': {'kind': 'Pod', 'namespace': namespace, 'name': 'loom-postgres-0', 'uid': pod['metadata']['uid']}}]}]}
    calls, executions = [], []
    mode = {'damage': None}

    def handle(message):
        calls.append(message)
        assert message.method == 'GET' and '/namespaces/loom-staging/' not in message.url.path
        path = message.url.path
        if path == '/api/v1/namespaces/' + namespace + '/pods':
            observed = copy.deepcopy(pod)
            if mode['damage'] == 'post_exec_pod' and executions:
                observed['metadata']['uid'] = str(uuid4())
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                'metadata': {'resourceVersion': '1'}, 'items': [observed]})
        if path == '/apis/discovery.k8s.io/v1/namespaces/' + namespace + '/endpointslices':
            value = copy.deepcopy(endpoints)
            if mode['damage'] == 'backend':
                value['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
            return httpx.Response(200, json=value)
        if path == '/apis/apps/v1/namespaces/' + namespace + '/statefulsets/loom-postgres':
            return httpx.Response(200, json=database)
        assert '/deployments/' not in path, 'closed SQL must not require a running manager'
        return pool.handle(message)

    def execute(argv, **kwargs):
        executions.append(argv)
        assert kwargs['env'] == {'PATH': module.os.defpath, 'LANG': 'C.UTF-8'}
        config = json.loads(Path(argv[argv.index('--kubeconfig') + 1]).read_text())
        assert config['clusters'][0]['cluster']['server'] == request.database.foundation.inputs.config['kubernetes_api_server']
        assert set(config['users'][0]['user']) == {'token'}
        assert config['users'][0]['user']['token'] == 'operator-test'
        command = argv[argv.index('exec'):]
        assert command[:7] == ['exec', '-n', namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
        assert command[7:17] == ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
        assert command[-1].startswith('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;')
        assert command[-1].endswith('ROLLBACK;\n') and 'UPDATE ' not in command[-1]
        assert 'operator-test' not in str(argv)
        report = {'schema': 'loom.pool-startup-closed.v1', 'operation_id': str(spec.operation_id),
            'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True,
            'qualified': mode['damage'] != 'report'}
        return SimpleNamespace(returncode=0, stdout=json.dumps(report).encode(), stderr=b'')

    monkeypatch.setattr(module.subprocess, 'run', execute)
    with module.HTTPSDevelopmentRuntimeProbes(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='operator-test') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
        probes.qualify_closed_pool()
        assert len(executions) == 1
        private_config = Path(executions[0][executions[0].index('--kubeconfig') + 1])
        assert private_config.stat().st_mode & 0o077 == 0
        for damage in ('backend', 'report', 'post_exec_pod'):
            mode['damage'] = damage
            executions.clear()
            with pytest.raises(ValueError, match='development runtime closed database unqualified'):
                probes.qualify_closed_pool()
            assert len(executions) == (0 if damage == 'backend' else 1)
    assert not private_config.exists()
    assert calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('probe_kind', ['settings', 'database'])
@pytest.mark.parametrize('connected', [None, True], indirect=True, ids=['dev-only', 'common-catalog'])
def test_runtime_process_settings_challenge_uses_actual_owned_pod_and_rejects_drift(
        completed_pool, publisher_cloud, handoff, monkeypatch, probe_kind, system_kubectl):
    import hmac
    from types import SimpleNamespace

    from scripts.ops import nebius_development_runtime_probes as module
    from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )

    runtime, request, parent, state, _ = parent_install_fixture(completed_pool, publisher_cloud)
    parent.database_complete = parent.catalog_complete = True
    runtime.install_development_runtime(request=request, api=parent, execute=True)
    method = 'qualify_runtime_' + probe_kind
    assert hasattr(module.HTTPSDevelopmentRuntimeProbes, method), 'concrete process probe is missing'
    calls, documents, original_handle = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    populate_runtime_pods(documents)
    if probe_kind == 'database':
        populate_database_backends(documents)

    def handle(message):
        if message.url.path.endswith('/pods') and message.url.params.get('labelSelector') == 'app=loom-postgres':
            calls.append(message)
            listing = copy.deepcopy(documents[message.url.path])
            listing['items'] = [row for row in listing['items'] if row['metadata']['labels'].get('app') == 'loom-postgres']
            return httpx.Response(200, json=listing)
        return original_handle(message)
    spec = request.database.manager.retained.request.registration.spec
    participant, = [row for row in spec.participants if row.environment_class == 'development']
    machine, = (row for row in spec.machines if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
    roles = {(request.database.manager.retained.request.retained.binding.namespace, 'loom-service'): 'manager',
        ('loom-dev', 'loom-service'): 'service', ('loom-dev', 'loom-control-plane'): 'controller',
        (participant.execution_namespace.name, 'loom-execution-actuator'): 'actuator'}
    commands, mode = [], {'damage': None}

    def execute(argv, **kwargs):
        commands.append(argv)
        command = argv[argv.index('exec'):]
        assert command[7:10] == ['python', '-c', BOUND_POOL_SETTINGS_COMMAND if probe_kind == 'settings' else _BOUND_DATABASE_COMMAND]
        component, nonce, response = command[10:]
        namespace = command[2]
        pod_name = command[3].removeprefix('pod/')
        pod, = (row for row in documents['/api/v1/namespaces/' + namespace + '/pods']['items'] if row['metadata']['name'] == pod_name)
        deployment_name = pod_name.removesuffix('-a12b34-test')
        role = roles[(namespace, deployment_name)]
        assert ('service' if role == 'manager' and probe_kind == 'database' else role) == component
        actual = documents['/apis/apps/v1/namespaces/' + namespace + '/deployments/' + deployment_name]
        if probe_kind == 'settings':
            wanted = expected_pool_runtime_settings(component, actual,
                token_sha256=machine.token_sha256 if component in {'controller', 'actuator'} else None,
                catalog_sha256=hashlib.sha256(spec.profiles.model_dump_json().encode()).hexdigest() if component == 'manager' else None)
            payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
        else:
            from pydantic import PostgresDsn

            variable = {'controller': 'LOOM_CP_DB_URL', 'service': 'LOOM_SVC_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
            entry, = (row for row in actual['spec']['template']['spec']['containers'][0]['env'] if row['name'] == variable)
            reference = entry['valueFrom']['secretKeyRef']
            secret = documents['/api/v1/namespaces/' + namespace + '/secrets/' + reference['name']]
            url = base64.b64decode(secret['data'][reference['key']]).decode()
            payload = (url if component == 'actuator' else str(PostgresDsn(url))).encode()
            assert url not in str(argv)
        assert hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest() == response
        if mode['damage'] == 'pod-replaced':
            pod['metadata']['uid'] = str(uuid4())
        elif mode['damage'] == 'process-restarted':
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        elif mode['damage'] == 'controller-lag':
            actual['status']['observedGeneration'] = 0
        return SimpleNamespace(returncode=0, stdout=b'{"status":"wrong"}' if mode['damage'] == 'report' else b'{"status":"qualified"}', stderr=b'')

    monkeypatch.setattr(module.subprocess, 'run', execute)
    with module.HTTPSDevelopmentRuntimeProbes(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='operator-test') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))
        for namespace, name in roles:
            getattr(probes, method)(key='Deployment:' + namespace + ':' + name, state_dir=state)
        assert len(commands) == 4
        assert len({row[-2] for row in commands}) == 4
        manager_key = 'Deployment:' + request.database.manager.retained.request.retained.binding.namespace + ':loom-service'
        for damage in ('pod-replaced', 'process-restarted', 'controller-lag', 'report'):
            original = copy.deepcopy(documents)
            mode['damage'] = damage
            with pytest.raises(ValueError, match='development runtime process ' + probe_kind + ' unqualified'):
                getattr(probes, method)(key=manager_key, state_dir=state)
            documents.clear()
            documents.update(original)
        count = len(commands)
        with pytest.raises(ValueError):
            getattr(probes, method)(key='Deployment:loom-staging:loom-service', state_dir=state)
        assert len(commands) == count
    assert calls and all(row.method == 'GET' for row in calls)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_https_fixed_phases_use_real_journal_and_never_emit_staging_or_writer_grants(completed_pool, publisher_cloud, tmp_path):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_stage import _stage_fixed_documents
    from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI

    from loom.nebius_platform_render import digest

    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan = runtime.prepare_runtime_install(request)
    binding = request.database.manager.retained.request.retained.binding
    store, calls, qualifications = PhaseAPI(binding), [], []
    store.key = _key
    def qualify(value, phase, writing):
        assert value == request
        qualifications.append((phase, writing))
    def handle(message):
        calls.append(message)
        if message.method == 'GET':
            return httpx.Response(404)
        document = json.loads(message.content)
        assert message.method == 'POST'
        assert 'staging' not in message.url.path
        assert all(set(rule['verbs']) <= {'get', 'list', 'watch'} for rule in document.get('rules', []))
        if message.url.query == b'dryRun=All':
            return httpx.Response(201, json=store.default_resource(document))
        store.create_resource(document)
        return httpx.Response(201, json=store.get_resource(document))
    for phase in ('authority', 'isolation'):
        documents = plan.fixed[phase]
        paths = {}
        with runtime_resources_api(request, phase, qualify) as api:
            def transport(message, paths=paths):
                if message.method == 'GET' and message.url.path in paths:
                    value = store.get_resource(paths[message.url.path])
                    return httpx.Response(200, json=value) if value else httpx.Response(404)
                if message.method == 'POST':
                    document = json.loads(message.content)
                    paths[message.url.path + '/' + document['metadata']['name']] = document
                return handle(message)
            api.client.close()
            api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(transport))
            args = dict(documents=documents, revision=digest(documents), phase='development-runtime-' + phase,
                binding=binding, api=api, state_dir=tmp_path / phase)
            _stage_fixed_documents(**args)
            before = list(store.creates)
            _stage_fixed_documents(**args)
            assert store.creates == before
    assert store.creates and qualifications and any(writing for _, writing in qualifications)
    assert any('/apis/rbac.authorization.k8s.io/v1/clusterroles' == call.url.path for call in calls)
    assert any('/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies' == call.url.path for call in calls)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_https_rejects_unbound_writes_before_transport(completed_pool, publisher_cloud):
    from scripts.ops.nebius_management_stage import _MARKER

    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan, calls = runtime.prepare_runtime_install(request), []
    with runtime_resources_api(request, 'authority', lambda *_: None) as api:
        api.client.close()
        def transport(message):
            calls.append(message)
            return httpx.Response(201, json=document)
        api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(transport))
        for damage in ('other-phase', 'foreign-namespace', 'write-verb', 'missing-marker'):
            document = copy.deepcopy(next(row for row in plan.fixed['authority'].values() if row['kind'] == 'Role'))
            if damage == 'other-phase':
                document = copy.deepcopy(next(iter(plan.fixed['material'].values())))
            elif damage == 'foreign-namespace':
                document['metadata']['namespace'] = 'loom-staging'
            elif damage == 'write-verb':
                document['rules'][0]['verbs'].append('create')
            if damage != 'missing-marker':
                document['metadata'].setdefault('annotations', {})[_MARKER] = str(uuid4())
            with pytest.raises(ValueError, match='outside fixed development runtime phase'):
                api.create_resource(document)
            assert not calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('late_status', [False, True])
def test_runtime_workload_https_binds_uid_version_and_spec_before_patch(completed_pool, publisher_cloud, late_status):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan, calls, qualifications = runtime.prepare_runtime_install(request), [], []
    key = 'Deployment:loom-dev:loom-control-plane'
    before = copy.deepcopy(plan.originals[key])
    before['metadata']['resourceVersion'] = '71'
    current = copy.deepcopy(before)
    current['metadata']['generation'] = 1
    current['status'] = {'observedGeneration': 1}
    desired = plan.stopped[key]
    def qualify(value, phase, writing):
        assert value == request and phase == 'stop'
        qualifications.append(writing)
    def ready(value, phase, original, expected, actual):
        assert value == request and phase == 'stop'
        assert original == plan.originals[key] and expected == desired
        assert actual == current
        return True
    def transport(message):
        calls.append(message)
        if message.url.path.endswith(('/replicasets', '/pods')):
            assert message.method == 'GET' and dict(message.url.params) == {'limit': '1000'}
            replicas = message.url.path.endswith('/replicasets')
            return httpx.Response(200, json={'apiVersion': 'apps/v1' if replicas else 'v1',
                'kind': 'ReplicaSetList' if replicas else 'PodList', 'metadata': {'resourceVersion': '75'}, 'items': []})
        assert message.url.path == '/apis/apps/v1/namespaces/loom-dev/deployments/loom-control-plane'
        if message.method == 'GET':
            if late_status and any(row.url.path.endswith('/pods') for row in calls):
                changed = copy.deepcopy(current)
                changed['status']['replicas'] = 1
                return httpx.Response(200, json=changed)
            return httpx.Response(200, json=current)
        assert message.method == 'PATCH'
        assert message.headers['content-type'] == 'application/json-patch+json'
        changes = json.loads(message.content)
        assert changes[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '71'},
            {'op': 'test', 'path': '/spec', 'value': before['spec']}]
        update = copy.deepcopy(current)
        for change in changes[3:]:
            assert change['op'] in {'add', 'replace'}
            if change['path'] == '/spec':
                update['spec'] = change['value']
            elif change['path'].startswith('/metadata/'):
                update['metadata'][change['path'].split('/')[-1]] = change['value']
            else:
                pytest.fail('unexpected workload mutation')
        if not message.url.query:
            current.update(update)
            current['metadata']['resourceVersion'] = '72'
        return httpx.Response(200, json=update)
    with runtime_workloads_api(request, qualify, ready) as api:
        api.client.close()
        api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(transport))
        assert api.read_workload(key) == current
        assert api.preview_workload(key, before, desired)['spec']['replicas'] == 0
        assert current['spec']['replicas'] == 1
        assert api.patch_workload(key, before, desired) is True
        assert api.workload_ready(key, desired) is (not late_status)
        assert current['spec']['replicas'] == 0
    assert len([row for row in calls if row.method == 'PATCH']) == 2
    assert len([row for row in calls if row.url.path.endswith(('/replicasets', '/pods'))]) == 2
    assert True in qualifications



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_workload_https_refuses_unbound_transition_before_io(completed_pool, publisher_cloud):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan, calls = runtime.prepare_runtime_install(request), []
    with runtime_workloads_api(request, lambda *_: None, lambda *_: True) as api:
        api.client.close()
        def transport(message):
            calls.append(message)
            return httpx.Response(200, json=before)
        api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(transport))
        for damage in ('target', 'uid', 'spec', 'foreign-key'):
            key = 'Deployment:loom-dev:loom-control-plane'
            before, desired = copy.deepcopy(plan.originals[key]), copy.deepcopy(plan.stopped[key])
            before['metadata']['resourceVersion'] = '71'
            if damage == 'target':
                desired['spec']['replicas'] = 1
            elif damage == 'uid':
                before['metadata']['uid'] = str(uuid4())
            elif damage == 'spec':
                before['spec']['replicas'] = 5
            else:
                key = 'Deployment:loom-staging:loom-control-plane'
            with pytest.raises(ValueError, match='development runtime workload update unconfirmed'):
                api.patch_workload(key, before, desired)
            assert not calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_workload_https_only_explicit_rejection_is_retryable(completed_pool, publisher_cloud):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    plan = runtime.prepare_runtime_install(request)
    key = 'Deployment:loom-dev:loom-control-plane'
    before, desired = copy.deepcopy(plan.originals[key]), copy.deepcopy(plan.stopped[key])
    before['metadata']['resourceVersion'] = '71'
    value = copy.deepcopy(desired)
    value['metadata'].update(uid=before['metadata']['uid'], resourceVersion='72')
    # Server defaults are accepted, not confused with an ambiguous write.
    value['spec']['revisionHistoryLimit'] = 10
    replies = [(200, value, True)]
    for status, reason in ((409, 'Conflict'), (422, 'Invalid')):
        rejection = {'apiVersion': 'v1', 'kind': 'Status', 'status': 'Failure', 'code': status, 'reason': reason}
        replies.extend(((status, rejection, False), (status, {**rejection, 'reason': 'Forbidden'}, None)))
    replies.extend(((503, value, None), (200, [], None), (200, {}, None)))
    for field in ('uid', 'security'):
        changed = copy.deepcopy(value)
        if field == 'uid':
            changed['metadata']['uid'] = str(uuid4())
        else:
            changed['spec']['template']['spec']['hostNetwork'] = True
        replies.append((200, changed, None))
    with runtime_workloads_api(request, lambda *_: None, lambda *_: True) as api:
        api.client.close()
        for status, body, expected in replies:
            calls = []
            def transport(message, status=status, body=body, calls=calls):
                calls.append(message)
                return httpx.Response(status, json=body)
            api.client = type(completed_pool[3].api.client)(base_url=api.api_server, transport=httpx.MockTransport(transport))
            if expected is None:
                with pytest.raises(ValueError, match='development runtime workload update unconfirmed'):
                    api.patch_workload(key, before, desired)
            else:
                assert api.patch_workload(key, before, desired) is expected
            assert len(calls) == 1
            api.client.close()



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_parent_connects_all_https_workload_phases_and_read_only_resume(completed_pool, publisher_cloud):
    from contextlib import ExitStack

    from scripts.ops.nebius_development_runtime_live import HTTPSDevelopmentRuntimeWorkloads

    runtime, request, api, _, _ = parent_install_fixture(completed_pool, publisher_cloud)
    calls = []
    rows = api.store.resources

    def observed(row):
        result = copy.deepcopy(row)
        result['metadata'].setdefault('generation', 1)
        result['metadata'].setdefault('resourceVersion', '1')
        if result['kind'] == 'Deployment':
            count = result['spec']['replicas']
            result['status'] = {'observedGeneration': result['metadata']['generation'], **{
                field: count for field in ('replicas', 'readyReplicas', 'updatedReplicas', 'availableReplicas')}}
        return result

    def collections(namespace, kind):
        items = []
        for key, row in rows.items():
            if row['kind'] != 'Deployment' or row['metadata']['namespace'] != namespace:
                continue
            parent = observed(row)
            # Stable IDs across reads; no production helper derives expectations.
            identity = UUID(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest())
            name = parent['metadata']['name'] + '-b876c5b4d'
            labels = {**parent['spec']['template']['metadata']['labels'], 'pod-template-hash': 'b876c5b4d'}
            count = parent['spec']['replicas']
            template = copy.deepcopy(parent['spec']['template'])
            template['metadata']['labels'] = labels
            if kind == 'ReplicaSet':
                items.append({'metadata': {'name': name, 'namespace': namespace, 'uid': str(identity), 'generation': 1,
                    'labels': labels, 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment',
                        'name': parent['metadata']['name'], 'uid': parent['metadata']['uid'], 'controller': True}]},
                    'spec': {'replicas': count, 'selector': {'matchLabels': {
                        **parent['spec']['selector']['matchLabels'], 'pod-template-hash': 'b876c5b4d'}}, 'template': template},
                    'status': {'observedGeneration': 1, 'replicas': count, 'readyReplicas': count, 'availableReplicas': count}})
            elif count:
                items.append({'metadata': {**template['metadata'], 'name': name + '-pod', 'namespace': namespace,
                    'uid': str(UUID(int=identity.int + 1)), 'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet',
                        'name': name, 'uid': str(identity), 'controller': True}]},
                    'spec': template['spec'], 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                        'containerStatuses': [{'name': container['name'], 'ready': True, 'state': {'running': {}}}
                            for container in template['spec']['containers']],
                        'initContainerStatuses': [{'name': container['name'], 'state': {'terminated': {'exitCode': 0}}}
                            for container in template['spec'].get('initContainers', [])]}})
        return {'apiVersion': 'apps/v1' if kind == 'ReplicaSet' else 'v1', 'kind': kind + 'List',
            'metadata': {'resourceVersion': '1'}, 'items': items}

    def transport(message):
        calls.append(message)
        parts = message.url.path.split('/')
        namespace, resource = parts[parts.index('namespaces') + 1:][:2]
        if resource in {'replicasets', 'pods'}:
            assert message.method == 'GET'
            return httpx.Response(200, json=collections(namespace, 'ReplicaSet' if resource == 'replicasets' else 'Pod'))
        key = ('Deployment' if resource == 'deployments' else 'CronJob') + ':' + namespace + ':' + parts[-1]
        current = observed(rows[key])
        if message.method == 'GET':
            return httpx.Response(200, json=current)
        assert message.method == 'PATCH'
        for change in json.loads(message.content):
            target, field = (current['metadata'], change['path'].rsplit('/', 1)[-1]) if change['path'].startswith('/metadata/') else (current, 'spec')
            if change['op'] == 'test':
                assert target[field] == change['value']
            elif change['op'] == 'remove':
                del target[field]
            else:
                assert change['op'] in {'replace', 'add'}
                target[field] = change['value']
        if not message.url.query:
            current['metadata']['resourceVersion'] = str(int(current['metadata']['resourceVersion']) + 1)
            current['metadata']['generation'] += 1
            rows[key] = observed(current)
        return httpx.Response(200, json=current)

    with ExitStack() as stack:
        def workloads(phase):
            api.visits.append(phase)
            client = stack.enter_context(HTTPSDevelopmentRuntimeWorkloads(request=request, phase=phase,
                api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
                ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None, observe_ready=lambda *_: True))
            client.client.close()
            client.client = type(completed_pool[3].api.client)(base_url=client.api_server, transport=httpx.MockTransport(transport))
            return client
        api.workloads = workloads
        api.database_complete = api.catalog_complete = True
        result = runtime.install_development_runtime(request=request, api=api, execute=True)
        assert result['status'] == 'development_runtime_installed_closed'
        assert [phase for phase in api.visits if phase in {'stop', 'replace', 'control', 'start'}] == ['stop', 'replace', 'control', 'start']
        writes = [row for row in calls if row.method == 'PATCH' and not row.url.query]
        assert len(writes) == 11  # Three stop, three replace, two control, three start.
        before = len(calls)
        assert runtime.install_development_runtime(request=request, api=api, execute=True) == result
        assert len(calls) == before



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_delivery_keeps_old_data_identity_and_separates_credentials(completed_pool):
    import asyncio

    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    calls = len(completed_pool[3].server.calls)
    prepared = database_runtime(completed_pool)
    manager, foundation = prepared.manager, prepared.foundation
    shared, worker = prepared.material
    config, job = prepared.database
    participant, = manager.retained.request.registration.spec.participants
    assert shared['metadata']['namespace'] == job['metadata']['namespace'] == config['metadata']['namespace'] == 'loom-dev'
    assert worker['metadata']['namespace'] == participant.execution_namespace.name
    assert shared['immutable'] is worker['immutable'] is config['immutable'] is True
    assert set(shared['data']) == {'actuator-password', 'batch-runner-token'}
    assert set(worker['data']) == {'actuator-url', 'ca.crt'}
    material = {key: base64.b64decode(value).decode() for key, value in worker['data'].items()}
    database = make_url(material['actuator-url'])
    assert (database.username, database.password, database.host, database.database) == (
        'loom_actuator', 'runtime-actuator-' + 'p' * 40, 'loom-postgres.loom-dev.svc', 'loom')
    assert dict(database.query) == {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}
    # The actuator consumes this exact Secret URL without driver normalization.
    # Engine construction is lazy: exercise its real async driver, no DB access.
    engine = create_async_engine(material['actuator-url'], pool_pre_ping=True)
    try:
        assert engine.dialect.is_async
    finally:
        asyncio.run(engine.dispose())
    original = foundation.bootstrap['material']['loom-platform-db']
    assert material['ca.crt'] == original['ca.crt']
    assert original['postgres-password'] not in repr(material)
    assert json.loads(config['data']['setup.json']) == {'namespace': 'loom-dev',
        'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'schema_revision': '0176'}
    pod = job['spec']['template']['spec']
    container, = pod['containers']
    assert container['command'] == ['python', '-m', 'loom.nebius_development_runtime_database']
    assert container['image'] == manager.publication.bundle.candidate['images']['service']['image_ref']
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never' and job['spec']['backoffLimit'] == 0
    assert not pod.get('initContainers')
    assert container['securityContext']['allowPrivilegeEscalation'] is False
    assert container['securityContext']['capabilities']['drop'] == ['ALL']
    env = {row['name']: row for row in container['env']}
    assert set(env) == {'LOOM_DEVELOPMENT_RUNTIME_CONFIG', 'LOOM_DB_URL', 'LOOM_DB_ACTUATOR_PASSWORD', 'LOOM_BATCH_RUNNER_TOKEN'}
    assert env['LOOM_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'admin-url'}
    for setting, key in [('LOOM_DB_ACTUATOR_PASSWORD', 'actuator-password'), ('LOOM_BATCH_RUNNER_TOKEN', 'batch-runner-token')]:
        assert env[setting]['valueFrom']['secretKeyRef'] == {'name': shared['metadata']['name'], 'key': key}
    assert {row['name'] for row in container['volumeMounts']} == {'db-ca', 'runtime-setup'}
    assert {row['name'] for row in pod['volumes']} == {'db-ca', 'runtime-setup'}
    ca, = [row for row in pod['volumes'] if row['name'] == 'db-ca']
    assert ca['secret'] == {'secretName': 'loom-platform-db', 'defaultMode': 0o440,
        'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}
    assert len(completed_pool[3].server.calls) == calls
    assert all(path.read_bytes() == raw for path, raw in foundation.files.items())



def test_runtime_database_delivery_refuses_invalid_material(completed_pool):
    # This protects the pre-write boundary, not merely the eventual SQL failure.
    name = 'scripts.ops.nebius_development_runtime_setup'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database delivery is missing')
    for changes in ({'operation_id': UUID(int=0)}, {'actuator_password': 'too-short'},
            {'batch_runner_token': 'wrong-kind-' + 'r' * 64}):
        with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
            database_runtime(completed_pool, **changes)



@pytest.mark.parametrize('manager_entry', ['foundation-old-schema', 'foundation-wrong-namespace',
    'foundation-wrong-ca_pem', 'foundation-wrong-secret_store_master_keys'], indirect=True)
def test_runtime_database_delivery_rejects_manager_foundation_mismatch(completed_pool):
    calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool)
    assert len(completed_pool[3].server.calls) == calls



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_retains_four_exact_creates_and_replays(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    first = runtime_database_stage(request, api, state)
    before = copy.deepcopy(api.resources)
    assert runtime_database_stage(request, api, state) == first
    assert api.resources == before
    name = 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f'
    assert api.creates == ['Secret:loom-dev:' + name, 'Secret:loom-nebius-dev-execution:' + name,
        'ConfigMap:loom-dev:' + name, 'Job:loom-dev:' + name]
    assert first['status'] == 'management_phase_staged'
    assert first['phase'] == 'development-runtime-database'
    assert len(first['resource_uids']) == 4
    assert 'runtime-actuator-' not in json.dumps(first)



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_runtime_database_stage_never_retries_uncertain_create(completed_pool, tmp_path, failure):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    api.failure = failure
    if failure == 'after':
        receipt = runtime_database_stage(request, api, state)
        assert runtime_database_stage(request, api, state) == receipt
        assert len(api.creates) == 4
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development runtime database stage unqualified'):
                runtime_database_stage(request, api, state)
        assert len(api.creates) == 1



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_refuses_caller_modified_job_before_writes(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    request.database[1]['spec']['template']['spec']['containers'][0]['command'] = ['sh', '-c', 'echo unapproved']
    with pytest.raises(ValueError, match='development runtime database stage unqualified'):
        runtime_database_stage(request, api, state)
    assert not api.creates and not state.exists()



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_proof_binds_job_operation_role_and_token(completed_pool, tmp_path):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'validate_database_runtime_proof'):
        pytest.fail('fixed development runtime database receipt validation is missing')
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    runtime_database_stage(request, api, state)
    job, = [row for row in api.resources.values() if row['kind'] == 'Job']
    proof = {'job_uid': job['metadata']['uid'], 'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b',
        'database': {'status': 'development_runtime_database_installed',
            'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'role': 'loom_actuator',
            'role_oid': 17000, 'token_sha256': hashlib.sha256(('loom_br_' + 'r' * 64).encode()).hexdigest()}}
    assert setup.validate_database_runtime_proof(request, state, proof) is None
    for field, value in [('status', 'pending'), ('operation_id', '27d6a159-4df3-4bbf-8722-897b2b3c619b'),
            ('role', 'postgres'), ('role_oid', True), ('role_oid', 0), ('role_oid', 2**32),
            ('token_sha256', '0' * 64), ('extra', 'not-allowed')]:
        changed = copy.deepcopy(proof)
        changed['database'][field] = value
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, changed)
    for field, value in [('job_uid', proof['pod_uid']), ('pod_uid', str(UUID(int=0))), ('extra', True)]:
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, {**proof, field: value})



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_waits_for_bound_sql_receipt_and_replays(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'runtime-database'
    with runtime_database_https(live) as api:
        runtime_database_stage(live.request, api, state)
        assert api.database_report(state) is None
        live.complete = True
        proof = api.database_report(state)
        assert proof['database']['role'] == 'loom_actuator'
        assert proof['database']['operation_id'] == 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f'
        before = list(live.store.creates)
        runtime_database_stage(live.request, api, state)
        assert api.database_report(state) == proof
        assert live.store.creates == before and len(before) == 4



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('damage', ['database', 'execution-namespace'])
def test_runtime_database_https_rejects_live_identity_drift_before_writes(runtime_database_live, tmp_path, damage):
    live = runtime_database_live
    if damage == 'database':
        live.foundation.bootstrap.secrets['loom-platform-db']['metadata']['uid'] = str(uuid4())
    else:
        live.pool.namespaces['loom-nebius-dev-execution']['metadata']['uid'] = str(uuid4())
    with runtime_database_https(live) as api, pytest.raises(ValueError, match='development runtime database stage unqualified'):
        runtime_database_stage(live.request, api, tmp_path / 'runtime-database')
    assert not live.store.creates
    assert all(message.method == 'GET' for message in live.calls)



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_rejects_receipt_and_late_drift(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'runtime-database'
    with runtime_database_https(live) as api:
        runtime_database_stage(live.request, api, state)
        live.complete = True
        first = api.database_report(state)
        live.report = {**first['database'], 'role': 'postgres'}
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        live.report = None
        live.pod['status']['containerStatuses'][0]['restartCount'] = 1
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        live.pod['status']['containerStatuses'][0]['restartCount'] = 0
        def drift():
            live.foundation.bootstrap.secrets['loom-platform-db']['metadata']['uid'] = str(uuid4())
        live.on_log = drift
        with pytest.raises(ValueError, match='development runtime database execution unqualified'):
            api.database_report(state)
        assert len(live.store.creates) == 4



@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_https_private_inputs_cannot_shadow_retained_evidence(runtime_database_live):
    live = runtime_database_live
    path = live.request.reference.operation_path
    changed = path.read_bytes() + b' '
    with runtime_database_https(live, private_files={path: changed}) as api:
        path.write_bytes(changed)
        with pytest.raises(ValueError, match='development runtime database prerequisites unqualified'):
            api.verify_identity(api.binding)
    assert not live.store.creates



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_shared_runtime_wires_real_predecessors_without_changing_artifact_provenance(completed_pool):
    name = 'scripts.ops.nebius_development_shared_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh shared development runtime preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_shared_runtime(database)
    service, controller = value.targets['service'], value.targets['control_plane']
    original = value.original
    for key, target, component in [('service', service, 'service'), ('control_plane', controller, 'control_plane')]:
        assert target['metadata']['uid'] == original[key]['metadata']['uid']
        assert target['metadata']['namespace'] == 'loom-dev'
        assert target['spec']['replicas'] == 0
        assert target['spec']['strategy'] == {'type': 'Recreate'}
        assert target['spec']['template']['spec']['containers'][0]['image'] == database.manager.publication.bundle.candidate['images'][component]['image_ref']
        old = {row['name']: row for row in original[key]['spec']['template']['spec']['volumes']}
        current = {row['name']: row for row in target['spec']['template']['spec']['volumes']}
        assert all(current[key] == row for key, row in old.items())
    api_env = {row['name']: row for row in service['spec']['template']['spec']['containers'][0]['env']}
    cp_env = {row['name']: row for row in controller['spec']['template']['spec']['containers'][0]['env']}
    assert api_env['LOOM_SVC_SERVICE_MODE']['value'] == 'application'
    assert api_env['LOOM_SVC_BATCH_RUNNER_CP_TOKEN']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f', 'key': 'batch-runner-token'}
    original_profile = database.manager.retained.request.retained.inputs.profile
    profile = json.loads(api_env['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value'])
    assert profile['candidate_sha'] == original_profile['candidate_sha']
    assert profile['candidate_sha'] != database.manager.publication.source.source_sha
    assert profile['runtime_image_ref'] == original_profile['runtime_image_ref']
    assert profile['image_admission'] == original_profile['image_admission']
    source = json.loads(api_env['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])
    assert source['kind'] == 'environment'
    assert source['data_environment_id'] == str(database.manager.deployment.installation.applications.shared.data_environment_id)
    pool = json.loads(cp_env['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
    assert pool['environment'] == 'development'
    assert pool['logical_pool_id'] == 'nebius-cpu'
    assert pool['management_origin'] == 'https://manage.example.com'
    assert pool['bearer_token_file'] == '/var/run/loom-pool-token/token'
    assert cp_env['LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED']['value'] == 'true'
    assert cp_env['LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED']['value'] == 'true'
    assert len(completed_pool[3].server.calls) == before_calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-bad-keyring', 'foundation-runtime-bad-target', 'foundation-runtime-bad-broker'], indirect=True)
def test_shared_runtime_rejects_incompatible_closed_catalog_before_writes(completed_pool):
    name = 'scripts.ops.nebius_development_shared_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh shared development runtime preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development shared runtime unqualified'):
        importlib.import_module(name).prepare_shared_runtime(database)
    assert len(completed_pool[3].server.calls) == before_calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_fresh_actuator_uses_catalog_and_only_read_authority(completed_pool, monkeypatch):
    name = 'scripts.ops.nebius_development_actuator_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh development actuator preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_actuator_runtime(database)
    spec = database.manager.retained.request.registration.spec
    participant, = spec.participants
    deployment = value.deployment
    assert 'uid' not in deployment['metadata']
    assert deployment['metadata']['namespace'] == participant.execution_namespace.name
    assert deployment['spec']['replicas'] == 0
    assert deployment['spec']['strategy'] == {'type': 'Recreate'}
    pod = deployment['spec']['template']['spec']
    container, = pod['containers']
    assert container['image'] == database.manager.publication.bundle.candidate['images']['execution_actuator']['image_ref']
    settings = {row['name']: row for row in container['env']}
    assert settings['LOOM_EXECUTION_ACTUATOR_DB_URL']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f', 'key': 'actuator-url'}
    assert settings['LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL']['value'] == 'http://loom-llm-gateway.loom-dev.svc.cluster.local:9100/internal/service-execution'
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL']['value'])['participant'] == participant.model_dump(mode='json')
    build, = spec.profiles.task_images
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER']['value']) == build.settings.model_dump(mode='json')
    assert json.loads(settings['LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR']['value']) == spec.profiles.execution[0].runtime.node_selector
    assert pod['nodeSelector'] == {'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}
    assert pod['securityContext']['runAsUser'] == 65532
    assert all(row['image'] == database.manager.publication.bundle.candidate['images']['service']['image_ref'] for row in pod['initContainers'])
    namespaces = {doc['metadata']['namespace'] for doc in value.authority if doc['kind'] == 'Role'}
    assert namespaces == {participant.execution_namespace.name, participant.build_namespace.name}
    rules = [rule for doc in value.authority for rule in doc.get('rules', [])]
    assert rules and all(set(rule['verbs']) <= {'get', 'list'} for rule in rules)
    assert {'apiGroups': [''], 'resources': ['nodes', 'nodes/stats'], 'verbs': ['get']} in rules
    assert not any('nodes/proxy' in rule['resources'] for rule in rules)
    assert all(doc.get('metadata', {}).get('namespace') != 'loom-staging' for doc in value.authority)
    from loom_execution_actuator.config import ExecutionActuatorSettings
    for row in container['env']:
        if 'value' in row:
            monkeypatch.setenv(row['name'], row['value'])
    database_ref = settings['LOOM_EXECUTION_ACTUATOR_DB_URL']['valueFrom']['secretKeyRef']
    worker_secret = database.material[1]
    assert database_ref['name'] == worker_secret['metadata']['name']
    database_url = base64.b64decode(worker_secret['data'][database_ref['key']]).decode()
    monkeypatch.setenv('LOOM_EXECUTION_ACTUATOR_DB_URL', database_url)
    parsed = ExecutionActuatorSettings(controller_id='dev-actuator-test')
    assert parsed.db_url == database_url
    assert parsed.global_pool.participant == participant
    assert parsed.task_image_builder == build.settings
    assert parsed.service_account_name == 'loom-execution-attempt'
    assert len(completed_pool[3].server.calls) == before_calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-bad-account'], indirect=True)
def test_fresh_actuator_rejects_foreign_task_service_account(completed_pool):
    from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
    database = database_runtime(completed_pool)
    with pytest.raises(ValueError, match='development actuator runtime unqualified'):
        prepare_actuator_runtime(database)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_job_binds_original_target_and_new_code_without_database_authority(completed_pool):
    name = 'scripts.ops.nebius_development_catalog_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development catalog Job preparation is missing')
    database = database_runtime(completed_pool)
    before_calls = len(completed_pool[3].server.calls)
    value = importlib.import_module(name).prepare_catalog_runtime(database)
    config, job = value.documents
    request = json.loads(config['data']['catalog.json'])
    execution, = database.manager.retained.request.registration.spec.profiles.execution
    assert request['execution_class'] == execution.execution_class.model_dump(mode='json')
    assert request['operation_id'] == str(database.operation_id)
    target, = request['topology']['targets']
    assert target['target_id'] == database.foundation.inputs.config['target_id']
    assert target['cluster_scope_id'] == database.foundation.inputs.config['cluster_scope_id']
    assert target['namespace_name'] == 'loom-nebius-dev-execution'
    assert target['environment'] == 'development'
    assert target['service_account_name'] == 'loom-execution-attempt'
    assert config['immutable'] is True
    assert config['metadata']['namespace'] == job['metadata']['namespace'] == 'loom-dev'
    assert job['spec']['backoffLimit'] == 0
    pod = job['spec']['template']['spec']
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never'
    container, = pod['containers']
    assert container['image'] == database.manager.publication.bundle.candidate['images']['service']['image_ref']
    assert container['command'] == ['python', '-m', 'loom.nebius_development_catalog']
    assert container['env'] == [{'name': 'LOOM_DEVELOPMENT_RUNTIME_CATALOG_CONFIG', 'value': '/var/run/loom-runtime-catalog/catalog.json'}]
    volumes = {row['name']: row for row in pod['volumes']}
    assert set(volumes) == {'runtime-catalog', 'admin-source', 'admin-owned'}
    assert volumes['admin-source']['secret']['secretName'] == 'loom-admin-secret'
    assert volumes['admin-source']['secret']['items'] == [{'key': 'secrets.toml', 'path': 'secrets.toml'}]
    assert volumes['admin-owned']['emptyDir']['medium'] == 'Memory'
    assert volumes['runtime-catalog']['configMap']['name'] == config['metadata']['name']
    initializer, = pod['initContainers']
    assert initializer['name'] == 'prepare-admin-secret'
    assert initializer['image'] == container['image']
    assert initializer['securityContext']['allowPrivilegeEscalation'] is False
    assert pod['securityContext']['runAsUser'] == 1000
    assert all(row['readOnly'] is True for row in container['volumeMounts'])
    assert len(completed_pool[3].server.calls) == before_calls



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_stage_creates_only_fixed_config_and_job_and_replays(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    first = catalog_runtime_stage(request, api, state)
    before = copy.deepcopy(api.resources)
    assert catalog_runtime_stage(request, api, state) == first
    assert api.resources == before
    name = 'loom-dev-catalog-aecc7407b7b84c388d1fbca5dca9840f'
    assert api.creates == ['ConfigMap:loom-dev:' + name, 'Job:loom-dev:' + name]
    assert first['phase'] == 'development-runtime-catalog'
    assert first['status'] == 'management_phase_staged'
    assert len(first['resource_uids']) == 2



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_catalog_stage_never_retries_uncertain_create(completed_pool, tmp_path, failure):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    api.failure = failure
    if failure == 'after':
        receipt = catalog_runtime_stage(request, api, state)
        assert catalog_runtime_stage(request, api, state) == receipt
        assert len(api.creates) == 2
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development catalog stage unqualified'):
                catalog_runtime_stage(request, api, state)
        assert len(api.creates) == 1



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_stage_refuses_modified_runtime_request_before_writes(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    request.database[1]['spec']['template']['spec']['serviceAccountName'] = 'foreign-authority'
    with pytest.raises(ValueError, match='development catalog stage unqualified'):
        catalog_runtime_stage(request, api, state)
    assert not api.creates and not state.exists()



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_proof_binds_exact_job_request_and_operation(completed_pool, tmp_path):
    import rfc8785
    from scripts.ops import nebius_development_catalog_runtime as catalog

    if not hasattr(catalog, 'validate_catalog_runtime_proof'):
        pytest.fail('fixed development catalog proof validation is missing')
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'catalog'
    catalog_runtime_stage(request, api, state)
    config, = [row for row in api.resources.values() if row['kind'] == 'ConfigMap']
    job, = [row for row in api.resources.values() if row['kind'] == 'Job']
    raw = json.loads(config['data']['catalog.json'])
    canonical = rfc8785.dumps(raw) + b'\n'
    proof = {'job_uid': job['metadata']['uid'], 'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b',
        'catalog': {'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f',
            'target_id': raw['topology']['targets'][0]['target_id'],
            'catalog_sha256': 'sha256:' + hashlib.sha256(canonical).hexdigest()}}
    assert catalog.validate_catalog_runtime_proof(request, state, proof) is None
    for field, value in [('operation_id', str(uuid4())), ('target_id', 'staging'),
            ('catalog_sha256', '0' * 64), ('extra', True)]:
        changed = copy.deepcopy(proof)
        changed['catalog'][field] = value
        with pytest.raises(ValueError, match='development catalog receipt unqualified'):
            catalog.validate_catalog_runtime_proof(request, state, changed)
    for field, value in [('job_uid', proof['pod_uid']), ('pod_uid', str(UUID(int=0))), ('extra', True)]:
        with pytest.raises(ValueError, match='development catalog receipt unqualified'):
            catalog.validate_catalog_runtime_proof(request, state, {**proof, field: value})
    journal = state / 'stage.json'
    saved = json.loads(journal.read_bytes())
    for row in saved['resources'].values():
        row['status'] = 'prepared'
    journal.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match='development catalog receipt unqualified'):
        catalog.validate_catalog_runtime_proof(request, state, proof)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_verifies_fixed_job_initializer_and_receipt(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        catalog_runtime_stage(live.request, api, state)
        assert api.catalog_report(state) is None
        live.complete = True
        proof = api.catalog_report(state)
        assert proof['catalog'] == live.report
        assert live.pod['status']['initContainerStatuses'][0]['name'] == 'prepare-admin-secret'
        assert api.catalog_report(state) == proof
        assert len(live.store.creates) == 2
        assert live.catalog_checks > 2



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_requires_parent_qualification_and_rejects_extra_authority(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        live.catalog_allowed = False
        with pytest.raises(ValueError, match='development catalog stage unqualified'):
            catalog_runtime_stage(live.request, api, state)
        assert not live.store.creates
        live.catalog_allowed = True
        foreign = copy.deepcopy(live.request.database[1])
        with pytest.raises(ValueError, match='resource outside fixed development runtime'):
            api.create_resource(foreign)
        assert not live.store.creates



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
def test_catalog_https_late_prerequisite_loss_cannot_report_completion(runtime_database_live, tmp_path):
    live, state = runtime_database_live, tmp_path / 'catalog'
    with runtime_catalog_https(live) as api:
        catalog_runtime_stage(live.request, api, state)
        live.complete = True
        live.on_log = lambda: setattr(live, 'catalog_allowed', False)
        with pytest.raises(ValueError, match='development catalog execution unqualified'):
            api.catalog_report(state)
        assert len(live.store.creates) == 2



@pytest.mark.parametrize('damage', ['anchor', 'parent', 'phase', 'incomplete', 'inputs', 'receipt'])
def test_missing_or_changed_completion_cannot_become_successor_authority(completed_pool, damage):
    loader = module()
    _, _, _, connected = completed_pool
    selector = reference(loader, completed_pool)
    original = Path(connected.server.request.retained.operation['state_dir']).parent
    state = original / 'pool-installation'
    parent_path = state / 'installation.json'
    parent = json.loads(parent_path.read_bytes())
    if damage == 'anchor':
        (Path(connected.server.request.retained.operation['anchor_dir']) / 'pool-installation.json').unlink()
    elif damage == 'parent':
        parent_path.unlink()
    elif damage == 'phase':
        (state / 'material/stage.json').unlink()
    elif damage == 'incomplete':
        parent['phases']['workload'] = {'status': 'started', 'sha256': None}
        parent_path.write_text(json.dumps(parent))
    elif damage == 'inputs':
        inputs_path = Path(completed_pool[0]['inputs_path'])
        inputs_path.write_bytes(inputs_path.read_bytes() + b' ')
    else:
        registration = original / 'pool-registration/registration.json'
        value = json.loads(registration.read_bytes())
        value['proof']['registration']['mode'] = 'global'
        registration.write_text(json.dumps(value))
        parent['phases']['registration']['sha256'] = hashlib.sha256(registration.read_bytes()).hexdigest()
        parent_path.write_text(json.dumps(parent))
    before = len(connected.server.calls)
    with pytest.raises(ValueError, match='retained development pool') as caught:
        loader.load_retained_pool(selector)
    assert len(connected.server.calls) == before
    assert not any(token in str(caught.value) for token in connected.intent.tokens.values())



@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_network_scopes_database_and_gateway_without_control_or_build_access(completed_pool):
    request = database_runtime(completed_pool)
    documents = network_runtime(request)
    policies = {row['metadata']['name']: row for row in documents}
    assert set(policies) == {'loom-execution-attempt-default-deny', 'loom-execution-attempt-egress',
        'development-runtime-postgres', 'development-runtime-gateway'}
    assert all(row['kind'] == 'NetworkPolicy' for row in documents)
    spec = request.manager.retained.request.registration.spec
    for name, app, port, selector in (
        ('postgres', 'loom-postgres', 5432, {'app.kubernetes.io/name': 'loom-execution-actuator'}),
        ('gateway', 'loom-llm-gateway', 9100, {'app.kubernetes.io/component': 'execution-unit'}),
    ):
        row = policies['development-runtime-' + name]
        assert row['metadata']['namespace'] == 'loom-dev'
        assert row['spec'] == {'podSelector': {'matchLabels': {'app': app}}, 'policyTypes': ['Ingress'],
            'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {
                'kubernetes.io/metadata.name': 'loom-nebius-dev-execution',
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/pool': str(spec.pool_id)}}, 'podSelector': {'matchLabels': selector}}],
                'ports': [{'protocol': 'TCP', 'port': port}]}]}
    deny = policies['loom-execution-attempt-default-deny']
    assert deny['spec']['ingress'] == deny['spec']['egress'] == []
    assert deny['metadata']['namespace'] == 'loom-nebius-dev-execution'
    egress = policies['loom-execution-attempt-egress']
    assert egress['metadata']['namespace'] == 'loom-nebius-dev-execution'
    assert egress['spec']['podSelector'] == {'matchLabels': {'app.kubernetes.io/component': 'execution-unit'}}
    dns, gateway = egress['spec']['egress']
    assert dns['to'][0]['namespaceSelector']['matchLabels'] == {'kubernetes.io/metadata.name': 'kube-system'}
    assert dns['ports'] == [{'protocol': 'UDP', 'port': 53}, {'protocol': 'TCP', 'port': 53}]
    assert gateway == {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'loom-dev'}},
        'podSelector': {'matchLabels': {'app': 'loom-llm-gateway'}}}], 'ports': [{'protocol': 'TCP', 'port': 9100}]}
    request.foundation.inputs.config['namespace'] = 'loom-nebius-staging'
    with pytest.raises(ValueError, match='development runtime network unqualified'):
        network_runtime(request)
    with pytest.raises(ValueError, match='development build policy unqualified'):
        build_policy(request)



@pytest.mark.parametrize('manager_entry', ['foundation-runtime-build'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_runtime_build_policy_is_bound_to_retained_namespace_without_psa_change(completed_pool):
    request = database_runtime(completed_pool)
    policies = build_policy(request)
    policy, binding = policies
    assert policy['kind'] == 'ValidatingAdmissionPolicy' and policy['spec']['failurePolicy'] == 'Fail'
    assert binding['kind'] == 'ValidatingAdmissionPolicyBinding'
    assert binding['spec']['validationActions'] == ['Deny']
    assert binding['spec']['matchResources'] == {'namespaceSelector': {
        'matchLabels': {'kubernetes.io/metadata.name': 'loom-nebius-dev-execution-build'}}}
    assert policy['metadata']['labels']['loom.nebius/development-runtime-operation'] == str(request.operation_id)
    assert all('namespace' not in row['metadata'] for row in policies)
