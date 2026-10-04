"""Protected inspection retains only required shared inputs on the gateway."""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from uuid import UUID

import pytest


@pytest.fixture
def snapshot_objects():
    def obj(kind, name, uid, **fields):
        return {'apiVersion': 'v1' if kind != 'Deployment' else 'apps/v1', 'kind': kind,
            'metadata': {'name': name, 'namespace': 'loom-nebius-platform', 'uid': uid,
                'resourceVersion': '12'}, **fields}
    config = {'namespace': 'loom-nebius-platform', 'environment': 'development',
        'cluster_id': 'mk8scluster-test', 'kubernetes_api_server': 'https://api.example.test'}
    def encode(value):
        return base64.b64encode(value.encode()).decode()
    return {
        ('namespace', 'loom-nebius-platform'): obj('Namespace', 'loom-nebius-platform', '1' * 32),
        ('namespace', 'kube-system'): obj('Namespace', 'kube-system', '2' * 32),
        ('configmap', 'loom-platform-config'): obj('ConfigMap', 'loom-platform-config', '3' * 32,
            data={'environment.json': json.dumps(config), 'profile.json': json.dumps({'candidate_sha': 'a' * 40}),
                'keyring.json': json.dumps({'public-key': 'fixture-public-key'})}),
        ('deployment', 'loom-service'): obj('Deployment', 'loom-service', '4' * 32, spec={'private': 'unused-pod-data'}),
        ('secret', 'loom-platform-db'): obj('Secret', 'loom-platform-db', '5' * 32,
            data={'ca.crt': encode('fixture-ca'), 'admin-url': encode(
                'postgresql://postgres:DO-NOT-COPY@loom-postgres.loom-nebius-platform.svc:5432/loom'),
                'unused': encode('DO-NOT-COPY')}),
        ('secret', 'loom-platform-auth'): obj('Secret', 'loom-platform-auth', '6' * 32,
            data={'secret-store-master-key': encode('fixture-master-key'), 'jwt': encode('DO-NOT-COPY')}),
    }


def capture(snapshot_objects, tmp_path):
    from scripts.ops.nebius_application_snapshot import capture_shared_inputs

    def read(kind, name, namespace):
        assert namespace is None if kind == 'namespace' else namespace == 'loom-nebius-platform'
        return snapshot_objects[kind, name]
    return capture_shared_inputs(read=read, root=tmp_path, cluster_id='mk8scluster-test',
        namespace='loom-nebius-platform', namespace_uid='1' * 32, kube_system_uid='2' * 32)


def test_snapshot_is_private_bounded_and_does_not_export_credentials(snapshot_objects, tmp_path):
    result = capture(snapshot_objects, tmp_path)
    assert result['status'] == 'shared_inputs_observed'
    UUID(result['observation_id'])
    root = tmp_path / result['observation_id']
    assert root.stat().st_mode & 0o777 == 0o700
    assert {file.name for file in root.iterdir()} == {'binding.json', 'environment.json', 'runtime-profile.json',
        'keyring.json', 'database-name', 'ca.crt', 'secret-store-master-keys'}
    assert (root / 'database-name').read_text() == 'loom'
    assert (root / 'ca.crt').read_text() == 'fixture-ca'
    assert (root / 'secret-store-master-keys').read_text() == 'fixture-master-key'
    assert all(file.stat().st_mode & 0o777 == 0o600 for file in root.iterdir())
    assert 'DO-NOT-COPY' not in ''.join(file.read_text() for file in root.iterdir())
    assert 'fixture-' not in json.dumps(result) and 'DO-NOT-COPY' not in json.dumps(result)
    binding = json.loads((root / 'binding.json').read_text())
    assert binding['shared_config_uid'] == '3' * 32
    assert binding['shared_service_uid'] == '4' * 32
    assert binding['shared_database_uid'] == '5' * 32
    assert binding['shared_auth_uid'] == '6' * 32
    assert capture(snapshot_objects, tmp_path)['observation_id'] != result['observation_id']


@pytest.mark.parametrize('damage', ['namespace', 'cluster', 'environment', 'candidate', 'secret', 'database-route', 'deleting'])
def test_unqualified_snapshot_writes_nothing(snapshot_objects, tmp_path, damage):
    from scripts.ops.nebius_application_snapshot import SnapshotError

    cm = snapshot_objects['configmap', 'loom-platform-config']
    if damage == 'namespace':
        snapshot_objects['namespace', 'loom-nebius-platform']['metadata']['uid'] = '7' * 32
    elif damage in {'cluster', 'environment'}:
        config = json.loads(cm['data']['environment.json'])
        config['cluster_id' if damage == 'cluster' else 'environment'] = 'foreign'
        cm['data']['environment.json'] = json.dumps(config)
    elif damage == 'candidate':
        cm['data']['profile.json'] = '{"candidate_sha":"not-a-commit"}'
    elif damage == 'deleting':
        cm['metadata']['deletionTimestamp'] = '2026-09-28T00:00:00Z'
    else:
        data = snapshot_objects['secret', 'loom-platform-db']['data']
        data['ca.crt' if damage == 'secret' else 'admin-url'] = ('invalid!'
            if damage == 'secret' else base64.b64encode(b'postgresql://postgres:secret@foreign:5432/loom').decode())
    with pytest.raises(SnapshotError):
        capture(snapshot_objects, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_protected_inspection_snapshot_transport_never_requests_secret_payloads_in_actions(monkeypatch, tmp_path):
    from scripts.ops import nebius_management_preflight as preflight

    report = {'cluster_id': 'mk8scluster-test', 'namespace': 'loom-nebius-platform', 'namespaces': [
        {'name': 'loom-nebius-platform', 'uid': '1' * 32}, {'name': 'kube-system', 'uid': '2' * 32}]}
    for key, value in {'LOOM_DEPLOY_SSH_TARGET': 'codex@gateway.example.test',
            'LOOM_DEPLOY_SSH_KEY_FILE': str(tmp_path / 'key'),
            'LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE': str(tmp_path / 'known')}.items():
        monkeypatch.setenv(key, value)
    calls = []
    def execute(command, **kwargs):
        import subprocess
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, json.dumps({'status': 'shared_inputs_observed',
            'observation_id': '11111111-1111-4111-8111-111111111111', 'candidate_sha': 'a' * 40}), '')
    monkeypatch.setattr(preflight.subprocess, 'run', execute)
    result = preflight.prepare_shared_inputs(report, kubeconfig=Path('/private/kubeconfig'))
    assert result['candidate_sha'] == 'a' * 40
    command, options = calls[0]
    assert command[0] == 'ssh' and 'StrictHostKeyChecking=yes' in command
    assert command[-2] == 'codex@gateway.example.test'
    assert 'python3 -' in command[-1] and 'kubectl' not in command[-1]
    assert 'capture_shared_inputs' in options['input']
    assert options['timeout'] <= 240


def test_gateway_entry_uses_only_fixed_gets_and_existing_management_identity(pool_snapshot, monkeypatch, tmp_path, capsys):
    import subprocess

    from scripts.ops import nebius_application_snapshot as snapshot

    objects, collections, _, _, _ = pool_snapshot
    home = tmp_path / '.loom/nebius-management'
    home.mkdir(parents=True, mode=0o700)
    inputs = home / 'inputs.json'
    inputs.write_text(json.dumps({'binding': {'kube_system_uid': '2' * 32}, 'deployment': {'installation': {
        'foundation': {'platform_config_json': json.dumps({'namespace': 'loom-nebius-platform',
            'cluster_id': 'mk8scluster-test'})}}}}))
    inputs.chmod(0o600)
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setattr('sys.argv', ['snapshot', '--kubeconfig', str(tmp_path / 'kubeconfig'),
        '--cluster-id', 'mk8scluster-test', '--namespace', 'loom-nebius-platform',
        '--namespace-uid', '1' * 32, '--kube-system-uid', '2' * 32])
    calls = []
    def execute(command, **kwargs):
        assert command[:5] == ['kubectl', '--kubeconfig', str(tmp_path / 'kubeconfig'), '--request-timeout=30s', 'get']
        calls.append(command[5:])
        if command[5] == '--raw':
            path = command[6].split('?')[0]
            resource = path.rsplit('/', 1)[1]
            kind = next(name for name in ('Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
                'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding') if name.lower() + 's' == resource)
            namespace = path.split('/namespaces/')[1].split('/')[0] if '/namespaces/' in path else None
            row = collections[kind, namespace]
        else:
            namespace = command[command.index('-n') + 1] if '-n' in command else None
            row = objects[command[5], command[6], namespace]
        return subprocess.CompletedProcess(command, 0, json.dumps(row).encode(), b'')
    monkeypatch.setattr(snapshot.subprocess, 'run', execute)
    assert snapshot.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'shared_inputs_observed'
    assert (home / 'shared-input-observations' / result['observation_id'] / 'pool-resources.json').is_file()
    assert sum(row[0] == '--raw' for row in calls) == 23
    inputs.chmod(0o644)
    calls.clear()
    assert snapshot.main() == 1 and not calls
    assert capsys.readouterr().out.strip() == '{"status": "blocked"}'


@pytest.mark.parametrize('extra', ['secret-value', 'unexpected-field'])
def test_inspection_rejects_unqualified_remote_response(monkeypatch, tmp_path, extra):
    import subprocess

    from scripts.ops import nebius_management_preflight as preflight

    for key in ('LOOM_DEPLOY_SSH_KEY_FILE', 'LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE'):
        monkeypatch.setenv(key, str(tmp_path / 'private'))
    monkeypatch.setenv('LOOM_DEPLOY_SSH_TARGET', 'codex@gateway.example.test')
    response = {'status': 'shared_inputs_observed', 'observation_id': '11111111-1111-4111-8111-111111111111',
        'candidate_sha': 'a' * 40}
    if extra == 'secret-value':
        response['observation_id'] = 'DO-NOT-EXPORT'
    else:
        response['secret'] = 'DO-NOT-EXPORT'
    monkeypatch.setattr(preflight.subprocess, 'run', lambda *args, **kwargs:
        subprocess.CompletedProcess([], 0, json.dumps(response), ''))
    report = {'cluster_id': 'mk8scluster-test', 'namespace': 'loom-nebius-platform', 'namespaces': [
        {'name': 'loom-nebius-platform', 'uid': '1' * 32}, {'name': 'kube-system', 'uid': '2' * 32}]}
    with pytest.raises((preflight.DeploymentError, ValueError)):
        preflight.prepare_shared_inputs(report, kubeconfig=Path('/private/kubeconfig'))


@pytest.fixture
def pool_snapshot(snapshot_objects):
    """Only Kubernetes I/O is doubled; actual capture/validation/files are used."""
    shared, execution = 'loom-nebius-platform', 'loom-nebius-platform-execution'
    cm = snapshot_objects['configmap', 'loom-platform-config']
    config = json.loads(cm['data']['environment.json'])
    config['execution_namespace'] = execution
    cm['data']['environment.json'] = json.dumps(config)
    objects = {(kind, name, None if kind == 'namespace' else shared): copy.deepcopy(row)
        for (kind, name), row in snapshot_objects.items()}
    for (kind, _, _), row in objects.items():
        if kind == 'namespace':
            row['metadata'].pop('namespace')
    for index, name in enumerate((execution, execution + '-build'), 7):
        objects['namespace', name, None] = {'apiVersion': 'v1', 'kind': 'Namespace',
            'metadata': {'name': name, 'uid': str(UUID(int=index)), 'resourceVersion': '13'}}
    objects['secret', 'loom-execution-actuator-db', execution] = {'apiVersion': 'v1', 'kind': 'Secret',
        'metadata': {'name': 'loom-execution-actuator-db', 'namespace': execution,
            'uid': str(UUID(int=9)), 'resourceVersion': '14'}, 'data': {'db-url': 'DO-NOT-COPY'}}
    objects['secret', 'loom-execution-capacity-collector-nebius', execution] = {
        'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'name': 'loom-execution-capacity-collector-nebius', 'namespace': execution,
            'uid': str(UUID(int=10)), 'resourceVersion': '15'},
        'data': {'credentials.json': base64.b64encode(b'private-collector').decode()}}
    objects['secret', 'loom-platform-storage', shared] = {
        'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'name': 'loom-platform-storage', 'namespace': shared,
            'uid': str(UUID(int=12)), 'resourceVersion': '16'},
        'data': {key: base64.b64encode(value).decode() for key, value in {
            'source-access-key': b'private-source-access', 'source-secret-key': b'private-source-secret',
            'secret-key': b'private-data-secret'}.items()}}
    kinds = {'Deployment': 'apps/v1', 'StatefulSet': 'apps/v1', 'CronJob': 'batch/v1',
        'Service': 'v1', 'ConfigMap': 'v1', 'Role': 'rbac.authorization.k8s.io/v1',
        'RoleBinding': 'rbac.authorization.k8s.io/v1', 'ClusterRole': 'rbac.authorization.k8s.io/v1',
        'ClusterRoleBinding': 'rbac.authorization.k8s.io/v1'}
    collections = {(kind, ns): {'apiVersion': version, 'kind': kind + 'List',
        'metadata': {'resourceVersion': '22'}, 'items': []}
        for kind, version in kinds.items()
        for ns in ((None,) if kind.startswith('Cluster') else (shared, execution, execution + '-build'))}
    collections['Deployment', shared]['items'] = [copy.deepcopy(objects['deployment', 'loom-service', shared])]
    collections['ConfigMap', shared]['items'] = [copy.deepcopy(cm)]
    role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
        'metadata': {'name': 'sample-native', 'uid': str(UUID(int=11)), 'resourceVersion': '23'},
        'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']}]}
    collections['ClusterRole', None]['items'] = [role]
    calls = []

    def read(kind, name, ns):
        calls.append(('get', kind, name, ns))
        return copy.deepcopy(objects[kind, name, ns])

    def listing(kind, ns):
        calls.append(('list', kind, ns))
        return copy.deepcopy(collections[kind, ns])

    return objects, collections, calls, read, listing


def capture_pool(pool_snapshot, tmp_path, **overrides):
    from scripts.ops.nebius_application_snapshot import capture_shared_inputs

    _, _, _, read, listing = pool_snapshot
    return capture_shared_inputs(read=overrides.get('read', read), read_collection=overrides.get('read_collection', listing),
        root=tmp_path, cluster_id='mk8scluster-test', namespace='loom-nebius-platform',
        namespace_uid='1' * 32, kube_system_uid='2' * 32)


def test_pool_capture_preserves_actual_private_resources_not_credential_values(pool_snapshot, tmp_path):
    result = capture_pool(pool_snapshot, tmp_path)
    directory = tmp_path / result['observation_id']
    payload = json.loads((directory / 'pool-resources.json').read_bytes())
    assert payload['schema_version'] == 'loom.nebius-pool-resource-observation.v1'
    assert payload['cluster_id'] == 'mk8scluster-test'
    assert payload['candidate_sha'] == 'a' * 40
    assert payload['namespace'] == 'loom-nebius-platform'
    resources = {(row['kind'], row['metadata'].get('namespace'), row['metadata']['name']): row
        for row in payload['resources']}
    assert resources['Deployment', 'loom-nebius-platform', 'loom-service']['spec'] == {'private': 'unused-pod-data'}
    assert resources['ClusterRole', None, 'sample-native']['rules'] == [
        {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']}]
    assert payload['collector_credential'] == {'uid': str(UUID(int=10)), 'resource_version': '15',
        'sha256': '80e08e3e6e738e8067c908e490e2dd01209a038bb551805421e6219105a6a108'}
    assert payload['actuator_credential'] == {'uid': str(UUID(int=9)), 'resource_version': '14'}
    assert payload['database_credential'] == {'uid': '5' * 32, 'resource_version': '12'}
    import hashlib

    assert payload['application_source_credential'] == {'uid': str(UUID(int=12)), 'resource_version': '16',
        'sha256': hashlib.sha256(b'{"access-key":"private-source-access","secret-key":"private-source-secret"}').hexdigest()}
    assert not any(row['kind'] == 'Secret' for row in payload['resources'])
    raw = (directory / 'pool-resources.json').read_text()
    assert 'private-collector' not in raw and 'DO-NOT-COPY' not in raw
    assert all(value not in raw and base64.b64encode(value.encode()).decode() not in raw
        for value in ('private-source-access', 'private-source-secret', 'private-data-secret'))
    assert set(result) == {'status', 'observation_id', 'candidate_sha'}
    assert (directory / 'pool-resources.json').stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    assert len(payload['collections']) == 23
    old = (directory / 'pool-resources.json').read_bytes()
    assert capture_pool(pool_snapshot, tmp_path)['observation_id'] != result['observation_id']
    assert (directory / 'pool-resources.json').read_bytes() == old


@pytest.mark.parametrize('damage', ['missing', 'payload', 'type', 'stringData', 'whitespace', 'oversize', 'rotation'])
def test_source_capture_rejects_unqualified_or_rotated_material_before_writing(pool_snapshot, tmp_path, damage):
    from scripts.ops.nebius_application_snapshot import SnapshotError

    objects, _, _, read, _ = pool_snapshot
    key = 'secret', 'loom-platform-storage', 'loom-nebius-platform'
    source = objects[key]
    if damage == 'missing':
        del source['data']['source-secret-key']
    elif damage == 'payload':
        source['data']['source-secret-key'] = 'not-base64'
    elif damage == 'type':
        source['type'] = 'kubernetes.io/tls'
    elif damage == 'stringData':
        source['stringData'] = {'source-secret-key': 'override'}
    elif damage in {'whitespace', 'oversize'}:
        source['data']['source-secret-key'] = base64.b64encode(b'contains space' if damage == 'whitespace' else b'x' * 4097).decode()

    def changing_read(kind, name, namespace):
        row = read(kind, name, namespace)
        if damage == 'rotation' and (kind, name, namespace) == key:
            source['metadata']['resourceVersion'] = '17'
        return row

    with pytest.raises(SnapshotError):
        capture_pool(pool_snapshot, tmp_path, read=changing_read)
    assert not list(tmp_path.iterdir())


def test_pool_capture_inherits_missing_item_type_only_from_typed_collection(pool_snapshot, tmp_path):
    _, collections, _, _, _ = pool_snapshot
    for collection in collections.values():
        for row in collection['items']:
            row.pop('kind')
            row.pop('apiVersion')
    result = capture_pool(pool_snapshot, tmp_path)
    value = json.loads((tmp_path / result['observation_id'] / 'pool-resources.json').read_bytes())
    role, = (row for row in value['resources'] if row['kind'] == 'ClusterRole')
    assert role['apiVersion'] == 'rbac.authorization.k8s.io/v1'


@pytest.mark.parametrize('damage', ['foreign', 'duplicate', 'pagination', 'missing-version', 'wrong-kind',
    'deleting', 'nil-uid', 'oversize', 'too-many', 'collector-secret', 'config-drift', 'namespace-drift', 'item-type'])
def test_pool_capture_rejects_incomplete_or_changed_scope_without_writing(pool_snapshot, tmp_path, damage):
    from scripts.ops.nebius_application_snapshot import SnapshotError

    objects, collections, _, _, listing = pool_snapshot
    shared = 'loom-nebius-platform'
    collection = collections['Deployment', shared]
    item = collection['items'][0]
    if damage == 'foreign':
        item['metadata']['namespace'] = 'foreign'
    elif damage == 'duplicate':
        collection['items'].append(copy.deepcopy(item))
    elif damage == 'pagination':
        collection['metadata']['continue'] = 'next-page'
    elif damage == 'missing-version':
        del collection['metadata']['resourceVersion']
    elif damage == 'wrong-kind':
        collection['kind'] = 'List'
    elif damage == 'deleting':
        item['metadata']['deletionTimestamp'] = '2026-10-02T00:00:00Z'
    elif damage == 'nil-uid':
        item['metadata']['uid'] = str(UUID(int=0))
    elif damage == 'item-type':
        item['kind'] = 'Secret'
    elif damage == 'oversize':
        item['spec']['oversize'] = 'x' * (8 * 1024**2)
    elif damage == 'too-many':
        collection['items'] = [copy.deepcopy(item) for _ in range(1025)]
    elif damage == 'collector-secret':
        objects['secret', 'loom-execution-capacity-collector-nebius', shared + '-execution']['data']['extra'] = 'secret'

    def changed(kind, namespace):
        result = listing(kind, namespace)
        if damage == 'config-drift':
            objects['configmap', 'loom-platform-config', shared]['metadata']['resourceVersion'] = '99'
        elif damage == 'namespace-drift':
            objects['namespace', shared + '-execution', None]['metadata']['uid'] = str(UUID(int=99))
        return result

    with pytest.raises(SnapshotError) as error:
        capture_pool(pool_snapshot, tmp_path, read_collection=changed)
    assert 'private-' not in str(error.value) and not list(tmp_path.iterdir())
