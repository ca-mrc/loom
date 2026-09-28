"""The protected private entry connects upgrades without reusing bootstrap state."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_gateway import upgrade_operation
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_upgrade import UpgradeAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def private_upgrade(entry_inputs, application_management_inputs, application_material, tmp_path):
    from scripts.ops.nebius_management_entry import load_inputs
    from scripts.ops.nebius_management_install import install_management

    original_meta, _, _, installed = entry_inputs
    _, original, _ = load_inputs(original_meta)
    install_args = dict(request=original, api=installed, state_dir=Path(original_meta['state_dir']),
        anchor_dir=Path(original_meta['anchor_dir']))
    install_management(**install_args)
    for kind in ('StatefulSet', 'Job', 'Job', 'Deployment'):
        installed.complete(kind)
        result = install_management(**install_args)
    assert result['status'] == 'management_installed'
    root = tmp_path / 'nebius-management/upgrade'
    root.mkdir(mode=0o700)
    def private(name, value):
        path = root / name
        path.write_text(value)
        path.chmod(0o600)
        return str(path)
    deployment = original.deployment.model_dump(mode='json')
    config = original.deployment.installation.foundation.platform_config
    application = copy.deepcopy(application_management_inputs[0]['installation']['applications'])
    application['shared']['cluster_id'] = config['cluster_id']
    application['authority']['cluster_id'] = config['cluster_id']
    deployment['installation'].update(provider_runtime=None, applications=application)
    payload = {'schema_version': 'loom.nebius-management-upgrade-private-inputs.v1',
        'foundation_candidate': '3' * 40,
        'original_operation': original_meta, 'deployment': deployment, 'candidate': original.candidate,
        'profile': original.profile, 'binding': asdict(installed.store.binding), 'shared_namespace_uid': str(uuid4()),
        'material_files': {key: private(key, value) for key, value in asdict(application_material).items()},
        'prerequisites': {'candidate_id': str(uuid4()), **{key: str(uuid4()) for key in (
            'shared_config_uid', 'shared_database_uid', 'shared_auth_uid', 'shared_service_uid')},
            'bucket_ids': {key: 'bucket-' + key for key in ('artifacts', 'trajectories', 'source')},
            'cloud': {'tenant_id': config['quota_parent_id'], 'region': config['region'],
                'provisioning_project_id': application['storage']['project_id'], 'provisioning_account_id': 'account-manager',
                'provisioning_key_id': 'key-manager', 'provisioning_group_id': 'group-manager',
                'membership_group_id': 'group-membership', 'shared_project_id': config['project_id'],
                'data_group_id': application['storage']['data_group_id'], 'source_group_id': application['storage']['source_group_id']}}}
    private('inputs.json', json.dumps(payload))
    metadata = upgrade_operation(tmp_path) | {key: original_meta[key] for key in ('installation_id', 'namespace', 'candidate')}
    metadata['inputs_sha256'] = hashlib.sha256(Path(metadata['inputs_path']).read_bytes()).hexdigest()
    path = private('operation.json', json.dumps(metadata))
    return metadata, payload, Path(path), installed


def test_upgrade_entry_preserves_original_and_advances_real_composition(private_upgrade, monkeypatch, capsys):
    from scripts.ops import nebius_management_entry as entry

    metadata, payload, path, installed = private_upgrade
    inputs, request, original_inputs, _ = entry.load_upgrade_inputs(metadata)
    assert inputs.original_operation == payload['original_operation']
    assert original_inputs.deployment.installation.provider_runtime is not None
    assert request.setup.material.manager_password == 'm' * 48
    assert request.setup.deployment.installation.provider_runtime is None
    api = UpgradeAPI(installed, request.setup)
    original_bytes = {file: file.read_bytes() for directory in (request.original_state, request.original_anchor)
        for file in directory.rglob('*.json')}
    @contextmanager
    def connect(*args):
        yield api
    monkeypatch.setattr(entry, 'connected_upgrade_api', connect)
    monkeypatch.setattr(entry, 'connected_api', lambda *args: pytest.fail('legacy installer selected'))
    assert entry.main(str(path), 'preflight') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'preflight_qualified'
    assert not Path(metadata['state_dir']).exists()
    assert entry.main(str(path), 'install') == 0
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'pending' and report['phase'] == 'admission'
    assert all(file.read_bytes() == value for file, value in original_bytes.items())
    assert not api.switch.calls


@pytest.mark.parametrize('change', ['original_hash', 'wrong_original_path', 'missing_original_state',
    'wrong_namespace_uid', 'operator_material', 'candidate', 'unversioned'])
def test_bad_upgrade_input_never_opens_live_connection(private_upgrade, monkeypatch, capsys, change):
    from scripts.ops import nebius_management_entry as entry

    metadata, payload, path, _ = private_upgrade
    if change == 'original_hash':
        payload['original_operation']['inputs_sha256'] = '0' * 64
    elif change == 'wrong_original_path':
        payload['original_operation']['state_dir'] = str(path.parent / 'state')
    elif change == 'missing_original_state':
        Path(payload['original_operation']['state_dir'], 'installation.json').unlink()
    elif change == 'wrong_namespace_uid':
        payload['binding']['namespace_uid'] = str(uuid4())
    elif change == 'operator_material':
        old = json.loads(Path(payload['original_operation']['inputs_path']).read_bytes())
        payload['material_files']['cloud_credentials_json'] = old['operator_cloud_credentials']
    elif change == 'candidate':
        metadata['candidate'] = '0' * 40
    else:
        payload['schema_version'] = 'loom.nebius-management-private-inputs.v1'
    Path(metadata['inputs_path']).write_text(json.dumps(payload))
    metadata['inputs_sha256'] = hashlib.sha256(Path(metadata['inputs_path']).read_bytes()).hexdigest()
    path.write_text(json.dumps(metadata))
    monkeypatch.setattr(entry, 'connected_upgrade_api', lambda *args: pytest.fail('unqualified connection'), raising=False)
    assert entry.main(str(path), 'install') == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'blocked'
    assert not Path(metadata['state_dir']).exists()


def test_upgrade_qualifies_current_foundation_without_rewriting_historical_pins(private_upgrade, monkeypatch):
    from scripts.ops import nebius_management_entry as entry
    from scripts.ops.nebius_ingress_image import DIGEST
    from scripts.ops.nebius_ingress_operation import LiveIngressAPI

    metadata, _, _, _ = private_upgrade
    inputs, request, old, ingress = entry.load_upgrade_inputs(metadata)
    assert old.foundation_candidate == '2' * 40 and ingress['candidate'] == '1' * 40
    assert inputs.foundation_candidate == '3' * 40
    original_bytes = Path(inputs.original_operation['inputs_path']).read_bytes()
    # Only operator/network boundaries are replaced. Real connection assembly and
    # LiveIngressAPI.foundation consume the new pin and check the live profile.
    async def operator(connection):
        return ssl.create_default_context(), 'test-operator-token'
    monkeypatch.setattr(entry, '_operator_transport', operator)
    monkeypatch.setattr(entry.private_state, 'load_installation', lambda path: {})
    Path(old.operator_connection.ca_file).write_text(request.setup.material.ca_pem)
    Path(ingress['kubeconfig']).write_text('fixture-kubeconfig')
    Path(ingress['kubeconfig']).chmod(0o600)
    config = request.setup.deployment.installation.foundation.platform_config
    ingress['image'] = 'cr.eu-north1.nebius.cloud/registry/loom-shared-ingress@' + DIGEST
    ingress['binding'].update(namespace=config['namespace'], child_domain='dev.example.test')
    monkeypatch.setattr(LiveIngressAPI, 'verify_identity', lambda *args: None)
    monkeypatch.setattr(LiveIngressAPI, '_run', lambda *args: json.dumps({'clusters': [{
        'name': 'context-cluster-e00fixture', 'cluster': {'server': config['kubernetes_api_server'],
            'certificate-authority-data': 'fixture-ca'}}]}))
    monkeypatch.setattr(LiveIngressAPI, '_get', lambda *args: {
        'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'loom-platform-config',
            'namespace': config['namespace'], 'uid': str(uuid4()), 'resourceVersion': '1'},
        'data': {'environment.json': json.dumps(config), 'profile.json': json.dumps({'candidate_sha': '3' * 40})}})
    with entry.connected_upgrade_api(inputs, request, old, ingress) as api:
        assert api.checks.base.ingress.foundation().platform_config['cluster_id'] == config['cluster_id']
    assert Path(inputs.original_operation['inputs_path']).read_bytes() == original_bytes
