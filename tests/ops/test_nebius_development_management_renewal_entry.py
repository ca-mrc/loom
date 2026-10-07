"""Private renewal entry pins original installation and explicit issuer generation."""
from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_management_renewal import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_renewal import application_material as application_material
from tests.ops.test_nebius_development_management_renewal import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_renewal import cloud as cloud
from tests.ops.test_nebius_development_management_renewal import installation as installation
from tests.ops.test_nebius_development_management_renewal import inventory as inventory
from tests.ops.test_nebius_development_management_renewal import management_inputs as management_inputs
from tests.ops.test_nebius_development_management_renewal import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_renewal import material as material
from tests.ops.test_nebius_development_management_renewal import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_renewal import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_renewal import renewal as renewal
from tests.ops.test_nebius_development_management_renewal import retained as retained
from tests.ops.test_nebius_development_management_renewal import route as route
from tests.ops.test_nebius_development_management_renewal import tls_material as tls_material


def module():
    return importlib.import_module('scripts.ops.nebius_development_management_renewal_entry')


@pytest.fixture
def entry(renewal, retained, route, monkeypatch):
    request, api = renewal
    installation = request.retained
    owner = Path(installation.operation['inputs_path']).parents[2]
    root = owner / 'nebius-development-management-renewal' / installation.binding.installation_id / str(request.operation_id)
    root.mkdir(parents=True, mode=0o700)
    certificate = installation.inputs.certificate.model_dump(mode='json')
    config = json.loads(Path(certificate['config_path']).read_text())
    generation = hashlib.sha256(request.material.chain.encode()).hexdigest()
    folder = Path(config['state_dir']) / 'generations' / generation
    folder.mkdir(mode=0o700)
    for name, value in {'fullchain.pem': request.material.chain, 'privkey.pem': request.material.key}.items():
        (folder / name).write_text(value)
        (folder / name).chmod(0o600)
    certificate['generation'] = generation
    inputs = {'schema_version': 'loom.nebius-development-management-renewal-inputs.v1',
        'retained': retained[0], 'certificate': certificate,
        'operator_connection': installation.inputs.operator_connection.model_dump(mode='json'),
        'route': route[0].settings.model_dump(mode='json')}
    raw = json.dumps(inputs)
    (root / 'inputs.json').write_text(raw)
    (root / 'inputs.json').chmod(0o600)
    operation = {'schema': 'loom.nebius-development-management-renewal-operation.v1',
        'source_sha': 'd' * 40, 'installation_id': installation.binding.installation_id,
        'namespace': 'loom-nebius-management-dev', 'operation_id': str(request.operation_id),
        'inputs_path': str(root / 'inputs.json'), 'inputs_sha256': hashlib.sha256(raw.encode()).hexdigest()}
    path = root / 'operation.json'
    path.write_text(json.dumps(operation))
    path.chmod(0o600)
    source = root / 'development-management-renewal-source.json'
    source.write_text(json.dumps({'source_sha': operation['source_sha'], 'source_archive_sha256': 'sha256:' + 'e' * 64}))
    source.chmod(0o600)
    monkeypatch.setattr(module(), 'SOURCE_RECORD', source)
    return operation, inputs, path, api


def test_renewal_entry_uses_new_reviewed_source_but_original_installation_and_issuer(entry):
    operation, _, _, _ = entry
    inputs, request, files = module().load_inputs(operation)
    assert request.operation_id.hex == operation['operation_id'].replace('-', '')
    assert request.retained.operation['source_sha'] != operation['source_sha']
    assert request.retained.binding.installation_id == operation['installation_id']
    assert hashlib.sha256(request.material.chain.encode()).hexdigest() == inputs.certificate.generation
    assert all(path.name != 'selected.json' for path in files)
    assert request.material.chain != request.retained.tls['data']['tls.crt']


@pytest.mark.parametrize('damage', ['hash', 'source', 'issuer', 'certificate-path', 'generation', 'namespace', 'endpoint', 'binding', 'input-mode'])
def test_entry_rejects_rebinding_before_opening_transport(entry, monkeypatch, capsys, damage):
    operation, inputs, path, _ = entry
    if damage == 'hash':
        operation['inputs_sha256'] = '0' * 64
    elif damage == 'source':
        operation['source_sha'] = 'f' * 40
    elif damage == 'issuer':
        inputs['certificate']['installation_id'] = str(uuid4())
    elif damage == 'certificate-path':
        original = Path(inputs['certificate']['config_path'])
        clone = original.with_name('clone.json')
        clone.write_bytes(original.read_bytes())
        clone.chmod(0o600)
        inputs['certificate']['config_path'] = str(clone)
    elif damage == 'generation':
        inputs['certificate']['generation'] = '0' * 64
    elif damage == 'namespace':
        operation['namespace'] = 'loom-nebius-platform'
    elif damage == 'endpoint':
        inputs['operator_connection']['endpoint'] = 'https://foreign.example.com'
    elif damage == 'binding':
        operation['installation_id'] = str(uuid4())
    else:
        Path(operation['inputs_path']).chmod(0o644)
    raw = json.dumps(inputs)
    Path(operation['inputs_path']).write_text(raw)
    if damage != 'hash':
        operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    path.write_text(json.dumps(operation))
    monkeypatch.setattr(module(), 'connected_api', lambda *args: pytest.fail('transport opened'))
    assert module().main(str(path), 'renew') == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'blocked'


def test_entry_connects_real_renewal_and_replay_with_bounded_public_evidence(entry, monkeypatch, capsys):
    operation, _, path, api = entry
    @contextmanager
    def connected(inputs, request, files):
        assert request.qualification_digest.startswith('sha256:')
        assert files[path] == path.read_bytes()
        yield api
    monkeypatch.setattr(module(), 'connected_api', connected)
    assert module().main(str(path), 'preflight') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_management_tls_preflight_qualified'
    assert not api.patches
    for _ in range(2):
        assert module().main(str(path), 'renew') == 0
        visible = json.loads(capsys.readouterr().out)
        assert visible['status'] == 'development_management_tls_renewed'
        assert visible['source_sha'] == operation['source_sha']
        assert visible['operation_id'] == operation['operation_id']
        assert 'PRIVATE KEY' not in json.dumps(visible)
    assert len(api.patches) == 1


def test_entry_reports_definite_rejection_as_failure_without_exporting_private_error(entry, monkeypatch, capsys):
    _, _, path, api = entry
    api.failure = 'conflict'
    @contextmanager
    def connected(*args):
        yield api
    monkeypatch.setattr(module(), 'connected_api', connected)
    assert module().main(str(path), 'renew') == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'rejected'
