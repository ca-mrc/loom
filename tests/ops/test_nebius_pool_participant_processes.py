"""Normal releases challenge the actual current participant processes."""
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import PostgresDsn
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_participant_resource import _resource_path, update_participant_resource
from tests.ops.test_nebius_pool_participant_observation import observer
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.ops.test_nebius_pool_participant_resource import connection as _resource_connection
from tests.ops.test_nebius_pool_participant_resource import file_writer

resource_connection = _resource_connection


@pytest.fixture
def process_connection(resource_connection, tmp_path, monkeypatch):
    from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        populate_runtime_pods,
    )

    state, connect = resource_connection
    retained = state.retained
    target, = (row for row in retained.inputs.previous.guards if row.participant_id == retained.release.refresh.participant_id)
    participant, = (row for row in retained.release.refresh.before.participants if row.participant_id == target.participant_id)
    machine, = (row for row in retained.release.refresh.before.machines
        if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
    keys = {f'Deployment:{target.namespace}:{name}' for name in ('loom-service', 'loom-control-plane')}
    keys.update(_key(row) for row in retained.release.files['60-execution.yaml'] if row['kind'] == 'Deployment')
    documents, templates, secrets = {}, {}, {}
    state.process_secrets = secrets
    state.process_fault, state.process_commands, state.pending = None, [], set()
    state.process_hook = None
    kubeconfig = tmp_path / 'process-kubeconfig'
    kubeconfig.write_text('private-test-only')
    kubeconfig.chmod(0o600)

    def sync():
        selected = {key: state.rows[key] for key in keys}
        selected.update({_key(row): state.rows[_key(row)] for row in (target.database.statefulset, target.database.service)})
        if templates != {key: row['spec'] for key, row in selected.items()}:
            documents.clear()
            documents.update({_resource_path(row): copy.deepcopy(row) for row in selected.values()})
            populate_runtime_pods(documents)
            populate_database_backends(documents)
            templates.clear()
            templates.update({key: copy.deepcopy(row['spec']) for key, row in selected.items()})
        for key in keys:
            row = selected[key]
            deployment = documents[_resource_path(row)]
            deployment['status']['readyReplicas'] = 0 if key in state.pending else 1
            namespace, name = row['metadata']['namespace'], row['metadata']['name']
            component = 'controller' if name == 'loom-control-plane' else 'service' if name == 'loom-service' else 'actuator'
            variable = {'controller': 'LOOM_CP_DB_URL', 'service': 'LOOM_SVC_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
            reference, = (item['valueFrom']['secretKeyRef'] for item in row['spec']['template']['spec']['containers'][0]['env']
                if item['name'] == variable)
            actuator = component == 'actuator'
            secret = secrets.setdefault((namespace, reference['name']), {'apiVersion': 'v1', 'kind': 'Secret',
                'metadata': {'namespace': namespace, 'name': reference['name'],
                    'uid': str(target.database.actuator_credential_uid if actuator else target.database.credential_uid),
                    'resourceVersion': target.database.actuator_credential_resource_version if actuator else target.database.credential_resource_version},
                'data': {}})
            secret['data'][reference['key']] = base64.b64encode(
                f'postgresql+psycopg://{component}:test-only@loom-postgres.{target.namespace}.svc:5432/loom'.encode()).decode()

    def execute(command, **options):
        assert command[:3] == ['/usr/bin/kubectl', '--kubeconfig', str(kubeconfig)]
        assert options['check'] is False and options['timeout'] == 40
        sync()
        index = next(index for index, value in enumerate(command) if value in {'get', 'exec'})
        args = command[index:]
        if args[0] == 'exec':
            assert args[1] == '-n' and args[4] == '-c' and args[6:9] == ['--', 'python', '-c']
            namespace = args[2]
            pod, = (row for row in documents[f'/api/v1/namespaces/{namespace}/pods']['items']
                if args[3] == 'pod/' + row['metadata']['name'])
            name = pod['metadata']['ownerReferences'][0]['name'].removesuffix('-a12b34')
            row = state.rows[f'Deployment:{namespace}:{name}']
            script, component, nonce, challenge = args[9:]
            state.process_commands.append((namespace, name, script, component))
            assert script in {_BOUND_DATABASE_COMMAND, BOUND_POOL_SETTINGS_COMMAND}
            if script == _BOUND_DATABASE_COMMAND:
                url = f'postgresql+psycopg://{component}:test-only@loom-postgres.{target.namespace}.svc:5432/loom'
                payload = (url if component == 'actuator' else str(PostgresDsn(url))).encode()
            else:
                wanted = expected_pool_runtime_settings(component, row,
                    token_sha256=None if component == 'service' else machine.token_sha256)
                payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
            assert challenge == hmac.new(bytes.fromhex(nonce), payload, hashlib.sha256).hexdigest()
            result = {'status': 'qualified'}
            if state.process_fault == ('database' if script == _BOUND_DATABASE_COMMAND else 'settings'):
                result['status'] = 'unqualified'
            if state.process_fault == 'restart':
                pod['status']['containerStatuses'][0]['restartCount'] += 1
            if state.process_hook is not None:
                state.process_hook()
        else:
            assert args[0] == 'get'
            kind, name = args[1:3]
            if kind == 'namespace':
                uid = {'kube-system': retained.inputs.previous.registration.binding.kube_system_uid,
                    target.namespace: str(target.namespace_uid), participant.execution_namespace.name: str(participant.execution_namespace.uid)}[name]
                result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
            elif kind == 'secret':
                result = copy.deepcopy(secrets[args[4], name])
                if state.process_fault == 'credential':
                    result['metadata']['resourceVersion'] = 'foreign'
            elif kind == '--raw':
                parsed = urlsplit(name)
                result = copy.deepcopy(documents[parsed.path])
                if 'labelSelector' in parse_qs(parsed.query):
                    label, value = parse_qs(parsed.query)['labelSelector'][0].split('=', 1)
                    result['items'] = [row for row in result['items'] if row['metadata']['labels'].get(label) == value]
                if state.process_fault == 'backend' and result['kind'] == 'EndpointSliceList':
                    result['items'][0]['endpoints'][0]['targetRef']['uid'] = '00000000-0000-0000-0000-000000000001'
            else:
                namespace = args[4]
                if kind == 'replicaset':
                    result, = (row for row in documents[f'/apis/apps/v1/namespaces/{namespace}/replicasets']['items']
                        if row['metadata']['name'] == name)
                else:
                    plural = {'deployment': 'deployments', 'statefulset': 'statefulsets', 'service': 'services'}[kind]
                    result = documents[('/api/v1' if kind == 'service' else '/apis/apps/v1') + f'/namespaces/{namespace}/{plural}/{name}']
        return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', execute)
    return state, connect, keys, {'kubeconfig': kubeconfig, 'executable': Path('/usr/bin/kubectl')}


def test_participant_processes_use_current_owned_pods_and_both_probes(process_connection):
    state, _, keys, options = process_connection
    with observer(state.retained, lambda: state.rows)[0] as live:
        assert live.observe_participant_processes(phase='preflight', **options) == ()
    assert {f'Deployment:{namespace}:{name}' for namespace, name, _, _ in state.process_commands} == keys
    assert len(state.process_commands) == 2 * len(keys)


@pytest.mark.parametrize('damage', ['database', 'settings', 'credential', 'backend', 'restart'])
def test_participant_processes_refuse_runtime_drift(process_connection, damage):
    state, _, _, options = process_connection
    state.process_fault = damage
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_participant_processes(phase='ready', **options)


def test_process_observation_follows_real_candidate_write_and_requires_final_readiness(process_connection):
    state, connect, _, options = process_connection
    key = f'Deployment:{state.retained.inputs.config["namespace"]}:loom-control-plane'
    with observer(state.retained, lambda: state.rows)[0] as live:
        state.pending.add(key)
        with pytest.raises(ValueError, match='preserve'):
            live.observe_participant_processes(phase='guarded', **options)
        state.pending.clear()
        file_writer(state, connect).apply_file('40-services.yaml')
        assert live.observe_participant_processes(phase='guarded', **options) == ()
        state.pending.add(key)
        assert live.observe_participant_processes(phase='guarded', **options) == (key,)
        with pytest.raises(ValueError, match='preserve'):
            live.observe_participant_processes(phase='ready', **options)
        state.pending.clear()
        assert live.observe_participant_processes(phase='ready', **options) == ()


@pytest.mark.parametrize('damage', ['connection', 'inputs', 'history'])
def test_participant_processes_reject_late_source_or_connection_change(process_connection, damage):
    state, _, _, options = process_connection
    path = (options['kubeconfig'] if damage == 'connection' else state.retained.state_dir / 'inputs.json'
        if damage == 'inputs' else Path(next(iter(state.retained.inputs.history))))
    original = path.read_bytes()

    def change():
        path.write_bytes(original + b'\n')
        state.process_hook = None

    state.process_hook = change
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_participant_processes(phase='ready', **options)


def test_participant_processes_reject_late_actuator_credential_change(process_connection):
    from scripts.ops.nebius_pool_runtime_settings import BOUND_POOL_SETTINGS_COMMAND

    state, _, keys, options = process_connection
    namespace = state.retained.inputs.config['namespace']
    actuators = [key for key in keys if not key.startswith(f'Deployment:{namespace}:')]

    def change():
        checked = [row for row in state.process_commands if row[2:] == (BOUND_POOL_SETTINGS_COMMAND, 'actuator')]
        if len(checked) == len(actuators):
            for (secret_namespace, _), secret in state.process_secrets.items():
                if secret_namespace != namespace:
                    secret['metadata']['resourceVersion'] = 'foreign-late-version'
            state.process_hook = None

    state.process_hook = change
    with observer(state.retained, lambda: state.rows)[0] as live:
        with pytest.raises(ValueError, match='preserve'):
            live.observe_participant_processes(phase='ready', **options)


def test_real_resource_writer_waits_for_process_qualification_before_next_write(process_connection):
    state, connect, _, options = process_connection
    namespace = state.retained.inputs.config['namespace']
    controller = f'Deployment:{namespace}:loom-control-plane'
    service = f'Deployment:{namespace}:loom-service'
    defer_service = True
    with observer(state.retained, lambda: state.rows)[0] as live:
        def qualify(retained, key):
            assert retained == state.retained
            if defer_service and service in state.writes:
                state.pending.add(service)
            pending = live.observe_participant_processes(phase='guarded', preparing=key, **options)
            if pending:
                raise ValueError('participant processes still pending')

        def connected(key):
            api = connect(key)
            api.qualify_resource = qualify
            return api

        writer = file_writer(state, connected)
        with pytest.raises(ValueError, match='preserve'):
            writer.apply_file('40-services.yaml')
        assert state.writes == [service]
        assert controller not in state.writes
        defer_service = False
        state.pending.clear()
        with connected(service) as api:
            assert update_participant_resource(retained=state.retained, key=service, api=api) is True
        assert state.writes == [service]  # Reconcile the applied child, never PATCH it again.
        assert live.observe_participant_processes(phase='ready', **options) == ()


def peer_process_connection(*, observer, bindings, shared_state, inventory, resources, kubeconfig, patch, fallback,
        selected_guard=None):
    """External transport only; the real census, readiness and probes stay connected."""
    from uuid import uuid4

    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_DATABASE_COMMAND,
        _BOUND_TELEMETRY_COMMAND,
    )
    from scripts.ops.nebius_pool_runtime_settings import (
        BOUND_POOL_SETTINGS_COMMAND,
        expected_pool_runtime_settings,
    )
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        populate_runtime_pods,
    )

    retained = observer.retained
    migration = retained.inputs.previous
    census = observer.observe_writers(bindings=bindings, shared_state=shared_state)
    documents, secrets, namespaces, owners, fault = {}, {}, {}, {}, {}
    for target in migration.guards:
        participant, = (row for row in migration.registration.spec.participants if row.participant_id == target.participant_id)
        namespaces.update({target.namespace: str(target.namespace_uid),
            participant.execution_namespace.name: str(participant.execution_namespace.uid)})
        for row in census.workloads.values():
            if row['metadata']['namespace'] not in {target.namespace, participant.execution_namespace.name}:
                continue
            if row['metadata']['namespace'] == target.namespace and row['metadata']['name'] not in {'loom-control-plane', 'loom-service'}:
                continue
            if row['kind'] == 'Deployment':
                documents[_resource_path(row)] = copy.deepcopy(row)
                owners[_key(row)] = target, participant
                actuator = row['metadata']['namespace'] == participant.execution_namespace.name
                component = 'actuator' if actuator else 'controller' if row['metadata']['name'] == 'loom-control-plane' else 'service'
                variable = {'controller': 'LOOM_CP_DB_URL', 'service': 'LOOM_SVC_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
                reference, = (item['valueFrom']['secretKeyRef'] for item in row['spec']['template']['spec']['containers'][0]['env']
                    if item['name'] == variable)
                secret = secrets.setdefault((row['metadata']['namespace'], reference['name']), {
                    'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'namespace': row['metadata']['namespace'],
                        'name': reference['name'],
                        'uid': str(target.database.actuator_credential_uid if actuator else target.database.credential_uid),
                        'resourceVersion': target.database.actuator_credential_resource_version if actuator else target.database.credential_resource_version},
                    'data': {}})
                secret['data'][reference['key']] = base64.b64encode(
                    f'postgresql+psycopg://{component}:test-only@loom-postgres.{target.namespace}.svc:5432/loom'.encode()).decode()
        for row in (target.database.statefulset, target.database.service):
            current = resources.get(_key(row), census.workloads.get(_key(row), row))
            documents[_resource_path(row)] = copy.deepcopy(current)
            resources.setdefault(_key(row), copy.deepcopy(current))
    populate_runtime_pods(documents)
    populate_database_backends(documents)
    for listing in documents.values():
        if listing['kind'] not in {'PodList', 'ReplicaSetList'}:
            continue
        plural = 'pods' if listing['kind'] == 'PodList' else 'replicasets'
        for row in listing['items']:
            row['metadata']['resourceVersion'] = '601'
            if row['kind'] == 'Pod':
                row['spec']['nodeName'] = 'platform-node'
            inventory[plural][:] = [other for other in inventory[plural]
                if (other['metadata']['namespace'], other['metadata']['name']) != (row['metadata']['namespace'], row['metadata']['name'])]
            inventory[plural].append(row)
    nodes = {'apiVersion': 'v1', 'kind': 'NodeList', 'metadata': {'resourceVersion': '602', 'continue': ''}, 'items': [
        {'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': 'platform-node', 'uid': str(uuid4()), 'resourceVersion': '601'}},
        {'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': 'worker-node', 'uid': str(uuid4()),
            'resourceVersion': '601', 'labels': dict(migration.registration.spec.node_selector)}}]}
    checked, telemetry = set(), set()

    def execute(command, **options):
        offset = next(index for index, value in enumerate(command) if value in {'get', 'exec'})
        args = command[offset:]
        namespace = args[args.index('-n') + 1] if '-n' in args else None
        parsed = urlsplit(args[2]) if args[:2] == ['get', '--raw'] else None
        if namespace not in namespaces and not (parsed and (parsed.path in documents or parsed.path == '/api/v1/nodes')):
            if args[:2] != ['get', 'namespace'] or args[2] not in namespaces:
                return fallback(command, **options)
        assert command[:3] == ['/usr/bin/kubectl', '--kubeconfig', str(kubeconfig)]
        assert options['check'] is False and options['timeout'] == 40
        if args[0] == 'exec':
            if selected_guard is not None and args[6:8] == ['--', 'psql']:
                from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql

                selected, = (row for row in migration.guards
                    if row.participant_id == retained.release.refresh.participant_id)
                assert args[:-1] == ['exec', '-n', selected.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                    'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
                assert args[-1] == rollout_guard_observation_sql(owner=retained.guard_owner,
                    candidate=retained.inputs.candidate['candidate_sha'])
                return SimpleNamespace(returncode=0, stdout=json.dumps({'status': selected_guard['status']}).encode(), stderr=b'')
            # No SQL/activity/idle probe is permitted for another owner's tasks.
            assert args[6:9] == ['--', 'python', '-c']
            pod, = (row for row in documents[f'/api/v1/namespaces/{namespace}/pods']['items']
                if args[3] == 'pod/' + row['metadata']['name'])
            name = pod['metadata']['ownerReferences'][0]['name'].removesuffix('-a12b34')
            key = f'Deployment:{namespace}:{name}'
            target, participant = owners[key]
            original = census.workloads[key]
            script = args[9]
            if script == _BOUND_TELEMETRY_COMMAND:
                selected, target_id, node_name, uid = args[10:]
                assert selected == namespace and namespace == participant.execution_namespace.name
                participant.target(target_id, 'trial')
                assert any(row['metadata']['name'] == node_name and row['metadata']['uid'] == uid for row in nodes['items'])
                telemetry.add((key, node_name))
                result = {'status': 'qualified', 'node_name': node_name, 'node_uid': uid}
                if fault.get('mode') in {'telemetry-binding', 'telemetry-unavailable'}:
                    result = {'status': 'blocked', 'stage': 'binding' if fault['mode'] == 'telemetry-binding' else 'kubelet_network'}
                if fault.get('mode') == 'telemetry-restart':
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
            else:
                assert script in {_BOUND_DATABASE_COMMAND, BOUND_POOL_SETTINGS_COMMAND}
                assert selected_guard is not None or target.participant_id != retained.release.refresh.participant_id
                component, nonce, challenge = args[10:]
                if script == _BOUND_DATABASE_COMMAND:
                    url = f'postgresql+psycopg://{component}:test-only@loom-postgres.{target.namespace}.svc:5432/loom'
                    payload = (url if component == 'actuator' else str(PostgresDsn(url))).encode()
                else:
                    machine, = (row for row in migration.registration.spec.machines
                        if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
                    wanted = expected_pool_runtime_settings(component, original,
                        token_sha256=None if component == 'service' else machine.token_sha256)
                    payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
                assert challenge == hmac.new(bytes.fromhex(nonce), payload, hashlib.sha256).hexdigest()
                checked.add((key, script))
                result = {'status': 'unqualified' if fault.get('mode') == (
                    'database' if script == _BOUND_DATABASE_COMMAND else 'settings') else 'qualified'}
                if fault.get('mode') == 'restart':
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
        elif args[1] == 'namespace':
            result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': args[2], 'uid': namespaces[args[2]]}}
        elif args[1] == 'secret':
            result = copy.deepcopy(secrets[namespace, args[2]])
            if fault.get('mode') == 'credential':
                result['metadata']['resourceVersion'] = 'foreign'
        elif parsed:
            result = copy.deepcopy(nodes if parsed.path == '/api/v1/nodes' else documents[parsed.path])
            if 'labelSelector' in parse_qs(parsed.query):
                label, value = parse_qs(parsed.query)['labelSelector'][0].split('=', 1)
                result['items'] = [row for row in result['items'] if row['metadata']['labels'].get(label) == value]
        elif args[1] == 'replicaset':
            result, = (row for row in documents[f'/apis/apps/v1/namespaces/{namespace}/replicasets']['items']
                if row['metadata']['name'] == args[2])
        else:
            plural = {'deployment': 'deployments', 'statefulset': 'statefulsets', 'service': 'services'}[args[1]]
            result = copy.deepcopy(documents[('/api/v1' if args[1] == 'service' else '/apis/apps/v1')
                + f'/namespaces/{namespace}/{plural}/{args[2]}'])
            if args[1] == 'deployment' and fault.get('mode') == 'pending':
                result['status']['readyReplicas'] = 0
        return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

    patch.setattr(subprocess, 'run', execute)

    def observe():
        checked.clear()
        telemetry.clear()
        result = observer.observe_peer_processes(bindings=bindings, shared_state=shared_state,
            kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
        peer_keys = {key for key, (target, _) in owners.items() if target.participant_id != retained.release.refresh.participant_id}
        actuator_keys = {key for key, (_, participant) in owners.items()
            if key.startswith(f'Deployment:{participant.execution_namespace.name}:')}
        assert {key for key, _ in checked} == peer_keys and len(checked) == 2 * len(peer_keys)
        assert {key for key, _ in telemetry} == actuator_keys and len(telemetry) == 2 * len(actuator_keys)
        assert result['checks'] == len(telemetry)
        return result

    return observe, fault
