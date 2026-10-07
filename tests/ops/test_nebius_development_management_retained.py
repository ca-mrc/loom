"""Renewal consumes retained dev history, not expired initial delivery inputs."""
from __future__ import annotations

import hashlib
import importlib
import json
from datetime import timedelta
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_management_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_entry import application_material as application_material
from tests.ops.test_nebius_development_management_entry import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_entry import cloud as cloud
from tests.ops.test_nebius_development_management_entry import installation as installation
from tests.ops.test_nebius_development_management_entry import inventory as inventory
from tests.ops.test_nebius_development_management_entry import management_inputs as management_inputs
from tests.ops.test_nebius_development_management_entry import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_entry import material as material
from tests.ops.test_nebius_development_management_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_entry import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_entry import route as route
from tests.ops.test_nebius_development_management_entry import tls_material as tls_material


def module():
    return importlib.import_module('scripts.ops.nebius_development_management_retained')


@pytest.fixture
def retained(manager_entry):
    from scripts.ops.nebius_development_management_entry import load_inputs
    from scripts.ops.nebius_development_management_install import install_development_management

    operation, payload, path, api = manager_entry
    _, request, _ = load_inputs(operation)
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    for _ in range(8):
        try:
            result = install_development_management(request=request, api=api, state_dir=state, anchor_dir=anchor)
        except Exception as error:
            pytest.fail(f'initial fixture stage={getattr(error, "stage", None)} cause={error.__context__!r}')
        if result['status'] == 'development_management_installed':
            break
        phase = result['phase']
        if phase == 'application-admission':
            api.admit()
        else:
            api.complete({'database': 'StatefulSet', 'migration': 'Job',
                'application-database': 'Job', 'backup': 'Job', 'service': 'Deployment'}[phase])
    else:
        pytest.fail('fixture did not complete the real initial installer')
    parent = json.loads((state / 'installation.json').read_text())
    selector = {'operation_path': path, 'operation_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest(),
        'installation_input_digest': parent['input_digest'], 'qualification_digest': request.qualification_digest}
    return selector, operation, payload, request, api


def load(retained):
    return module().load_retained_management(module().RetainedManagementReference.model_validate(retained[0]))


def test_retained_manager_uses_original_uid_snapshot_and_no_new_renderer(retained, monkeypatch):
    from scripts.ops import nebius_development_management_install as installer

    monkeypatch.setattr(installer, 'render_installation', lambda *_: pytest.fail('old source rerendered'))
    before = len(retained[4].store.creates)
    result = load(retained)
    expected = retained[4].store.resources['Ingress:loom-management']
    assert result.binding.namespace == 'loom-nebius-management-dev'
    assert result.ingress['metadata']['uid'] == expected['metadata']['uid']
    assert result.ingress['spec'] == expected['spec']
    assert result.tls['metadata']['name'] == retained[3].deployment.public_tls_secret_name
    assert result.inputs.binding == retained[3].binding
    assert len(retained[4].store.creates) == before


def test_expired_original_certificate_and_retired_operator_files_do_not_prevent_retained_read(retained, monkeypatch):
    from scripts.ops import nebius_certificates as certificates
    from tests.ops.test_nebius_certificates import NOW

    material = retained[3].tls_material
    with pytest.raises(certificates.CertificateError, match='delivery window'):
        certificates._validate_certificate(material.chain.encode(), material.key.encode(),
            names=[material.public_host], now=NOW + timedelta(days=365), roots=[])
    monkeypatch.setattr(certificates, 'validate_management_certificate',
        lambda *_, **__: pytest.fail('expired initial certificate revalidated'))
    payload = retained[2]
    obsolete = [payload['operator_cloud_credentials'], *payload['application_files'].values(),
        payload['operator_connection']['ca_file'], payload['operator_connection']['credentials_file']]
    for path in {Path(value) for value in obsolete}:
        path.unlink()
    result = load(retained)
    assert result.ingress['metadata']['name'] == 'loom-management'
    assert not set(map(Path, obsolete)) & result.files.keys()


@pytest.mark.parametrize('damage', ['operation', 'anchor', 'parent', 'phase', 'incomplete', 'source-rebind', 'host-rebind'])
def test_missing_changed_or_rebound_history_is_rejected(retained, damage):
    selector, operation, payload, _, _ = retained
    state = Path(operation['state_dir'])
    if damage == 'operation':
        selector['operation_sha256'] = '0' * 64
    elif damage == 'anchor':
        (Path(operation['anchor_dir']) / (operation['installation_id'] + '.json')).unlink()
    elif damage == 'parent':
        (state / 'installation.json').unlink()
    elif damage == 'phase':
        (state / 'tls/stage.json').write_text('{}')
    elif damage == 'incomplete':
        path = state / 'installation.json'
        record = json.loads(path.read_text())
        del record['phases']['public']
        path.write_text(json.dumps(record))
    else:
        if damage == 'source-rebind':
            operation['source_sha'] = operation['candidate'] = 'e' * 40
            payload['candidate']['candidate_sha'] = payload['profile']['candidate_sha'] = 'e' * 40
        else:
            payload['deployment']['public_host'] = 'other.example.com'
        raw = json.dumps(payload).encode()
        Path(operation['inputs_path']).write_bytes(raw)
        operation['inputs_sha256'] = hashlib.sha256(raw).hexdigest()
        raw_operation = json.dumps(operation).encode()
        Path(selector['operation_path']).write_bytes(raw_operation)
        selector['operation_sha256'] = hashlib.sha256(raw_operation).hexdigest()
    with pytest.raises(ValueError, match='retained development management'):
        load(retained)
