"""Fresh dev source intake must deliver only its qualified shared source identity."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_management_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_entry import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_entry import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_entry import cloud as cloud
from tests.ops.test_nebius_development_management_entry import installation as installation
from tests.ops.test_nebius_development_management_entry import inventory as inventory
from tests.ops.test_nebius_development_management_entry import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_entry import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_entry import material as material
from tests.ops.test_nebius_development_management_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_entry import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_entry import route as route
from tests.ops.test_nebius_development_management_entry import tls_material as tls_material


def enable_source(deployment):
    raw = deployment.model_dump(mode='json')
    raw['installation']['applications']['runtime']['source_upload'] = {
        'credentials_file': '/var/run/loom-application-source-credentials/credentials.json',
        'spool_directory': '/run/loom-application-source/spool', 'max_inflight': 2,
    }
    return type(deployment).model_validate(raw)


def complete(request, api, state, anchor):
    from scripts.ops.nebius_development_management_install import install_development_management

    for _ in range(8):
        result = install_development_management(request=request, api=api, state_dir=state, anchor_dir=anchor)
        if result['status'] == 'development_management_installed':
            return result
        if result['phase'] == 'application-admission':
            api.admit()
        else:
            api.complete({'database': 'StatefulSet', 'migration': 'Job', 'application-database': 'Job',
                'backup': 'Job', 'service': 'Deployment'}[result['phase']])
    pytest.fail('source-enabled installation did not complete')


def source_inputs(manager_entry):
    from loom_service.environment_management.deployment import ManagementDeployment

    operation, payload, path, api = manager_entry
    payload['deployment'] = enable_source(ManagementDeployment.model_validate(payload['deployment'])).model_dump(mode='json')
    payload['source_files'] = {}
    for key, value in {'access-key': 'fixture-source-access', 'secret-key': 'fixture-source-secret'}.items():
        target = Path(path).parent / ('source-' + key)
        target.write_text(value)
        target.chmod(0o600)
        payload['source_files'][key] = str(target)
    save_inputs(operation, payload, path)
    return operation, payload, path, api


def save_inputs(operation, payload, path):
    raw = json.dumps(payload)
    Path(operation['inputs_path']).write_text(raw)
    operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    Path(path).write_text(json.dumps(operation))


def test_fresh_source_install_delivers_only_source_secret_before_manager_and_replays(manager_entry):
    from scripts.ops.nebius_development_management_entry import load_inputs
    from scripts.ops.nebius_development_management_retained import (
        RetainedManagementReference,
        load_retained_management,
    )

    operation, payload, path, api = source_inputs(manager_entry)
    _, request, files = load_inputs(operation)
    assert all(Path(item) in files for item in payload['source_files'].values())
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    result = complete(request, api, state, anchor)
    deployment = api.store.resources['Deployment:loom-service']
    pod = deployment['spec']['template']['spec']
    volume, = (row for row in pod['volumes'] if row['name'] == 'application-source-credentials')
    key = 'Secret:' + volume['secret']['secretName']
    secret = api.store.resources[key]
    assert secret['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert secret['immutable'] is True
    assert set(secret['data']) == {'credentials.json'}
    assert json.loads(base64.b64decode(secret['data']['credentials.json'])) == {
        'access-key': 'fixture-source-access', 'secret-key': 'fixture-source-secret'}
    assert api.store.creates.index(key) < api.store.creates.index('Deployment:loom-service')
    assert request.deployment.installation.applications.runtime.build is None
    assert request.deployment.pool_catalog_operation_id is None
    assert not any(row['name'] == 'pool-profiles' for row in pod['volumes'])
    before = list(api.store.creates)
    assert complete(request, api, state, anchor) == result
    assert api.store.creates == before
    parent = json.loads((state / 'installation.json').read_text())
    reference = RetainedManagementReference(operation_path=Path(path),
        operation_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest(),
        installation_input_digest=parent['input_digest'])
    # Renewal must use journaled material, not require retired original key files.
    for item in payload['source_files'].values():
        Path(item).unlink()
    retained = load_retained_management(reference)
    assert retained.inputs.deployment.installation.applications.runtime.source_upload is not None
    assert retained.binding.namespace == 'loom-nebius-management-dev'


@pytest.mark.parametrize('damage', ['missing', 'extra', 'alias', 'settings-absent', 'public', 'invalid'])
def test_private_source_input_mismatch_fails_before_state_or_connection(manager_entry, damage):
    from scripts.ops.nebius_development_management_entry import load_inputs

    operation, payload, path, api = source_inputs(manager_entry)
    if damage == 'missing':
        del payload['source_files']['secret-key']
    elif damage == 'extra':
        payload['source_files']['endpoint'] = payload['source_files']['access-key']
    elif damage == 'alias':
        payload['source_files']['secret-key'] = payload['application_files']['manager_password']
    elif damage == 'settings-absent':
        del payload['deployment']['installation']['applications']['runtime']['source_upload']
    elif damage == 'public':
        Path(payload['source_files']['secret-key']).chmod(0o644)
    else:
        Path(payload['source_files']['secret-key']).write_text('secret\nwith-whitespace')
    save_inputs(operation, payload, path)
    with pytest.raises(ValueError, match='private inputs'):
        load_inputs(operation)
    assert api.store is None
    assert not Path(operation['state_dir']).exists()


@pytest.mark.parametrize('foreign', [False, True])
async def test_source_intake_uses_exact_foundation_source_identity(provider_checks, monkeypatch, foreign):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, retained, _, _ = provider_checks
    values = retained.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
    material = {key: base64.b64decode(values['source-' + key]).decode() for key in ('access-key', 'secret-key')}
    if foreign:
        material['access-key'] = 'foreign-data-key'
    request = replace(request, deployment=enable_source(request.deployment), source_material=material)
    async def publications(*args):
        pass
    monkeypatch.setattr(api, 'publications', publications)
    if foreign:
        with pytest.raises(ManagementInstallError, match='cloud qualification failed'):
            await api.provider_and_publication(request, retained, 10240)
    else:
        await api.provider_and_publication(request, retained, 10240)


def test_recovery_cannot_change_frozen_source_material(installation, tmp_path):
    from scripts.ops.nebius_development_management_install import install_development_management
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    request = replace(request, deployment=enable_source(request.deployment),
        source_material={'access-key': 'fixture-access', 'secret-key': 'fixture-secret'})
    state, anchor = tmp_path / 'state', tmp_path / 'anchor'
    assert install_development_management(request=request, api=api, state_dir=state, anchor_dir=anchor)['phase'] == 'database'
    before = list(api.store.creates)
    changed = replace(request, source_material={**request.source_material, 'secret-key': 'replacement'})
    with pytest.raises(ManagementInstallError):
        install_development_management(request=changed, api=api, state_dir=state, anchor_dir=anchor)
    assert api.store.creates == before
