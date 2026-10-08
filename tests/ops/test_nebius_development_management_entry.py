"""Private entry connects fixed dev installation inputs, not a staging upgrade."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_prerequisites import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_management_prerequisites import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_management_route import route as route
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def manager_entry(provider_checks, route, installation, tmp_path, monkeypatch):
    from scripts.ops import nebius_development_management_entry as module

    api, request, _, _, _ = provider_checks
    root = tmp_path / '.loom/nebius-development-management' / request.binding.installation_id
    root.mkdir(mode=0o700, parents=True)
    (root.parent.parent / 'nebius-development-management-anchors').mkdir(mode=0o700)
    def private(path, raw):
        path.write_text(raw)
        path.chmod(0o600)
        return str(path)
    candidate = {**request.candidate, 'source_archive_sha256': 'sha256:' + 'f' * 64}
    issuer = root / 'issuer'
    issuer.mkdir(mode=0o700)
    certificate_id = str(uuid4())
    config = {'schema': 'loom.nebius-management-certificate-installation.v1', 'installation_id': certificate_id,
        'zone': 'example.com', 'management_host': request.deployment.public_host, 'email': None,
        'credential_file': str(root / 'dns-key-not-consumed'), 'state_dir': str(issuer)}
    config_path = private(root / 'certificate.json', json.dumps(config))
    private(issuer / 'installation.json', json.dumps(config))
    generation = hashlib.sha256(request.tls_material.chain.encode()).hexdigest()
    generation_path = issuer / 'generations' / generation
    generation_path.mkdir(mode=0o700, parents=True)
    private(generation_path / 'fullchain.pem', request.tls_material.chain)
    private(generation_path / 'privkey.pem', request.tls_material.key)
    certificate = {'config_path': config_path, 'installation_id': certificate_id, 'generation': generation}
    settings = api.settings
    # This fixture exercises private parsing and phase composition. Authenticated
    # publication selection is exercised by the concrete prerequisite tests.
    prerequisites = {'candidate_id': str(uuid4()),
        'foundation': {'operation_path': str(root / 'foundation/operation.json'), 'operation_sha256': 'a' * 64,
            'installation_input_digest': 'sha256:' + 'b' * 64, 'qualification_digest': 'sha256:' + 'c' * 64},
        'cloud': settings.cloud.model_dump(mode='json'), 'backup': settings.backup.model_dump(mode='json'),
        'backup_quota_name': settings.backup_quota_name, 'backup_quota_unit': settings.backup_quota_unit,
        'route': route[0].settings.model_dump(mode='json')}
    payload = {'schema_version': 'loom.nebius-development-management-private-inputs.v1',
        'binding': asdict(request.binding), 'shared_namespace_uid': request.shared_namespace_uid,
        'deployment': request.deployment.model_dump(mode='json'), 'candidate': candidate, 'profile': request.profile,
        'prerequisites': prerequisites, 'certificate': certificate,
        'operator_connection': {'endpoint': api.api_server, 'ca_file': private(root / 'operator-ca.pem', 'test-ca'),
            'credentials_file': str(api.operator_cloud_credentials)}, 'operator_cloud_credentials': str(api.operator_cloud_credentials),
        'material_files': {name: {key: private(root / (name + '-' + key), value) for key, value in data.items()}
            for name, data in request.material.items()},
        'application_files': {name: private(root / ('application-' + name), value)
            for name, value in asdict(request.application_material).items()}}
    raw = json.dumps(payload)
    operation = {'schema': 'loom.nebius-development-management-operation.v1', 'namespace': request.binding.namespace,
        'installation_id': request.binding.installation_id, 'source_sha': candidate['candidate_sha'],
        'candidate': candidate['candidate_sha'], 'inputs_path': private(root / 'inputs.json', raw),
        'inputs_sha256': hashlib.sha256(raw.encode()).hexdigest(), 'state_dir': str(root / 'state'),
        'anchor_dir': str(root.parent.parent / 'nebius-development-management-anchors' / root.name)}
    path = private(root / 'operation.json', json.dumps(operation))
    source_path = Path(private(root / 'development-management-source.json', json.dumps({
        'source_sha': candidate['candidate_sha'], 'source_archive_sha256': candidate['source_archive_sha256']})))
    monkeypatch.setattr(module, 'SOURCE_RECORD', source_path)
    return operation, payload, path, installation[1]


def test_private_entry_loads_exact_source_material_and_pinned_issuer_generation(manager_entry):
    from scripts.ops.nebius_development_management_entry import load_inputs

    operation, payload, _, _ = manager_entry
    inputs, request, files = load_inputs(operation)
    assert request.binding.namespace == 'loom-nebius-management-dev'
    assert request.shared_namespace_uid == payload['shared_namespace_uid']
    assert request.qualification_digest.startswith('sha256:')
    assert request.application_material.database_name == 'loom'
    assert request.tls_material.public_host == request.deployment.public_host
    assert inputs.operator_cloud_credentials in files
    assert not any(path.name in {'selected.json', 'dns-key-not-consumed'} for path in files)
    assert set(request.material) == {'loom-management-publications', 'loom-platform-storage'}


@pytest.mark.parametrize('selection', ['true', 'false', 0, 1, None])
def test_public_selection_requires_an_explicit_boolean(manager_entry, selection):
    from scripts.ops.nebius_development_management_entry import load_inputs

    operation, payload, _, api = manager_entry
    payload['shared_public_route'] = selection
    raw = json.dumps(payload)
    Path(operation['inputs_path']).write_text(raw)
    operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    with pytest.raises(ValueError, match='private inputs'):
        load_inputs(operation)
    assert api.store is None


@pytest.mark.parametrize('change', ['hash', 'source', 'namespace', 'alias', 'certificate-host', 'certificate-generation', 'public', 'symlink', 'source-upload'])
def test_invalid_entry_inputs_cannot_open_connection_or_create_state(manager_entry, change, monkeypatch, capsys):
    from scripts.ops import nebius_development_management_entry as module

    operation, payload, path, _ = manager_entry
    if change == 'hash':
        operation['inputs_sha256'] = '0' * 64
    elif change == 'source':
        payload['candidate']['source_archive_sha256'] = 'sha256:' + '0' * 64
    elif change == 'source-upload':
        payload['deployment']['installation']['applications']['runtime']['source_upload'] = {
            'credentials_file': '/var/run/loom-application-source-credentials/credentials.json',
            'spool_directory': '/run/loom-application-source/spool', 'max_inflight': 2,
        }
    elif change == 'namespace':
        operation['namespace'] = 'loom-nebius-management'
    elif change == 'alias':
        payload['application_files']['cloud_credentials_json'] = payload['operator_cloud_credentials']
    elif change == 'certificate-host':
        config_path = Path(payload['certificate']['config_path'])
        config = json.loads(config_path.read_text())
        config['management_host'] = 'foreign.example.com'
        config_path.write_text(json.dumps(config))
    elif change == 'certificate-generation':
        payload['certificate']['generation'] = '0' * 64
    elif change == 'public':
        Path(payload['application_files']['manager_password']).chmod(0o644)
    else:
        original = Path(payload['application_files']['manager_password'])
        alias = original.with_suffix('.link')
        alias.symlink_to(original)
        payload['application_files']['manager_password'] = str(alias)
    raw = json.dumps(payload)
    Path(operation['inputs_path']).write_text(raw)
    if change != 'hash':
        operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    Path(path).write_text(json.dumps(operation))
    monkeypatch.setattr(module, 'connected_api', lambda *args, **kwargs: pytest.fail('connection opened'))
    assert module.main(path, 'install') == 1
    assert json.loads(capsys.readouterr().out) == {'status': 'blocked', 'stage': 'operation' if change == 'namespace' else 'inputs'}
    assert not Path(operation['state_dir']).exists()


def test_entry_drives_real_installation_and_rejects_changed_identity_on_resume(manager_entry, monkeypatch, capsys):
    from scripts.ops import nebius_development_management_entry as module

    operation, payload, path, api = manager_entry
    @contextmanager
    def connected(*args, **kwargs):
        yield api
    monkeypatch.setattr(module, 'connected_api', connected)
    assert module.main(path, 'preflight') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_management_preflight_qualified'
    assert not Path(operation['state_dir']).exists()
    assert module.main(path, 'install') == 0
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'pending' and result['phase'] == 'database'
    writes = len(api.store.creates)
    assert module.main(path, 'install') == 0
    assert json.loads(capsys.readouterr().out) == result
    assert len(api.store.creates) == writes
    Path(payload['operator_cloud_credentials']).write_text('{"rotated":true}')
    assert module.main(path, 'install') == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'blocked'
    assert len(api.store.creates) == writes


@pytest.mark.parametrize('changed', [False, True])
def test_connection_uses_concrete_dev_api_and_rechecks_files_after_exchange(manager_entry, monkeypatch, changed):
    import ssl

    import certifi
    from scripts.ops import nebius_development_management_entry as module
    from scripts.ops.nebius_management_install import ManagementInstallError

    operation, payload, _, _ = manager_entry
    Path(payload['operator_connection']['ca_file']).write_bytes(Path(certifi.where()).read_bytes())
    inputs, request, files = module.load_inputs(operation)
    password = Path(payload['application_files']['manager_password'])
    async def exchange(connection):
        assert connection == inputs.operator_connection
        if changed:
            password.write_text('changed-at-exchange')
        return ssl.create_default_context(), 'short-lived-operator-token'
    # Token exchange is separately covered by the reused transport tests. Keep
    # the concrete prerequisites and live API composition below it real.
    monkeypatch.setattr(module, '_transport', exchange)
    if changed:
        with pytest.raises(ValueError, match='connection inputs changed'):
            with module.connected_api(inputs, request, files):
                pytest.fail('changed inputs yielded a live API')
    else:
        with module.connected_api(inputs, request, files) as api:
            assert api.development_checks.settings == inputs.prerequisites
            assert api.development_request == request and api.runtime_trust.get_ca_certs()
            password.write_text('changed-after-exchange')
            with pytest.raises(ManagementInstallError, match='private input changed'):
                api.bootstrap_api()


def test_unknown_entry_action_never_reads_material(monkeypatch, capsys):
    from scripts.ops import nebius_development_management_entry as module

    monkeypatch.setattr(module, '_private', lambda *args: pytest.fail('private input read'))
    assert module.main('/unused', 'recover-staging') == 1
    assert json.loads(capsys.readouterr().out) == {'status': 'blocked', 'stage': 'operation'}
