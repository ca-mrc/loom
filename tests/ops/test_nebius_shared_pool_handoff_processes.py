"""Shared handoff processes lifecycle regressions."""
from __future__ import annotations

from contextlib import nullcontext

from tests.ops.test_nebius_development_pool_retained import system_kubectl as system_kubectl
from tests.ops.test_nebius_shared_pool_handoff import (
    ROOT as ROOT,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    Path as Path,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _exercise_forward_processes as _exercise_forward_processes,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _exercise_recovery_databases as _exercise_recovery_databases,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_material as application_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    cloud as cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    connected as connected,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    copy as copy,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    dev_pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    entry as entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    handoff as handoff,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    hashlib as hashlib,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    installation as installation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    inventory as inventory,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    live as live,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    material as material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    preflight as preflight,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publication as publication,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    published_source as published_source,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytest as pytest,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytestmark as pytestmark,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    retained as retained,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    route as route,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_guard_transport as shared_guard_transport,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_operation as shared_operation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_runtime as shared_runtime,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_history as staging_history,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_observation as staging_observation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    uuid4 as uuid4,
)
from tests.support.nebius_shared_pool_history import captured_predecessor_history

pytestmark = [*pytestmark, pytest.mark.usefixtures('system_kubectl')]


# Hosted-runner activation took 587s including its independent fixture.
@pytest.mark.timeout(1200)
@pytest.mark.parametrize('staging_history', ['0174'], indirect=True)
@pytest.mark.parametrize('forward_processes', [False, True, 'activation', 'recovery-databases'],
    ids=['initial', 'forward', 'activation', 'recovery-databases'])
def test_shared_process_preflight_reads_current_databases_and_retired_authority(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, monkeypatch, forward_processes):
    """Reject active old authority, failed real challenges and changed bindings.

    Initial binding and concrete activation connect real predecessor files.
    Forward/recovery contracts capture those fixed disk inputs per case; current
    child journals, process probes and every fault selector remain exercised.
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
    physical = None
    if forward_processes in {'forward', 'recovery-processes'}:
        from scripts.ops import nebius_pool_storage_operation as storage
        from scripts.ops.nebius_development_runtime_live import runtime_resource_path
        from scripts.ops.nebius_shared_pool_entry import RetainedRuntimeReference
        from tests.ops.test_nebius_shared_pool_handoff import ClosedStorageAPI, _shared_entry_inputs

        original_args, original_bindings = shared_operation.__wrapped__(
            shared_runtime, staging_history, staging_observation, completed_pool, handoff)
        _, _, inputs, _, _ = _shared_entry_inputs(original_args, original_bindings)
        runtime = RetainedRuntimeReference.model_validate(inputs['runtime'])
        retained = storage.retain_closed_storage(runtime=runtime, operation_id=uuid4(),
            target_disk_type='NETWORK_SSD_NON_REPLICATED')
        storage_api = ClosedStorageAPI(retained)
        storage_api.collector.ready = storage_api.publication_ready = True
        assert storage.run_closed_storage(retained=retained, api=storage_api)
        physical = storage.load_completed_storage(runtime=runtime, reference=retained.reference)
        for row in (physical.resources.collector, physical.resources.configuration):
            documents[runtime_resource_path(row) + '/' + row['metadata']['name']] = copy.deepcopy(row)
    plan = module.prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT, storage=physical)
    attachment = plan.staging.attachment
    staging, dev = attachment.guard, next(row for row in plan.migration.guards if row.namespace == 'loom-dev')
    spec = plan.migration.registration.spec
    old = staging_history.context
    old_spec = old.inputs.installation
    old_namespace = old.original.upgrade.setup.binding.namespace
    old_revision = getattr(getattr(old.predecessor, 'resources', None), 'target_manager_revision', '0176')
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
                    assert sql == pool_machine_retirement_sql(old_spec, action='observe', manager_revision=old_revision)
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
                            # Physical correction leaves the mounted catalog
                            # bytes unchanged; round-tripping the registration
                            # can reorder maps without changing its meaning.
                            catalog_sha256=hashlib.sha256(documents['/api/v1/namespaces/' + manager_namespace
                                + '/configmaps/loom-pool-profiles-' + spec.operation_id.hex]['data']['profiles.json']
                                .encode()).hexdigest() if component == 'manager' else None)
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
    with HTTPSDevelopmentRuntimeProbes(request=request, storage=physical,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt']),
            token='operator-test') as probes:
        for connection in (probes, probes.manager, probes.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handle))

        def observe():
            return module.observe_shared_pool_processes(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=probes, storage=physical,
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
        history_io = (captured_predecessor_history(bindings=result.bindings, monkeypatch=monkeypatch,
            arguments=dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT))
            if forward_processes in (True, 'recovery-databases') else nullcontext())
        if forward_processes in {'recovery-databases', 'recovery-processes'}:
            with history_io:
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
                with history_io:
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
