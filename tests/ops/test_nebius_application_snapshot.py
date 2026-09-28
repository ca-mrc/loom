"""Protected inspection retains only required shared inputs on the gateway."""
from __future__ import annotations

import base64
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
    encode = lambda value: base64.b64encode(value.encode()).decode()
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
