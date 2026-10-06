"""Original-source-generated output pins read-only historical reconstruction."""
from __future__ import annotations

import dataclasses
import hashlib
import json
from pathlib import Path

import pytest


@pytest.fixture
def historical_source_inputs():
    from loom_service.environment_management.deployment import ManagementDeployment
    from loom_service.pool_management.installation import PoolInstallation

    root = Path(__file__).resolve().parents[2]
    recorded = json.loads((root / 'tests/fixtures/nebius/source-delivery-v1.json').read_text())
    assert recorded['source_commit'] == '80c12bbd2509e62ebdcec83a0b68ccaed1c1c9f5'
    return dict(before=ManagementDeployment.model_validate(recorded['before']),
        pool=PoolInstallation.model_validate(recorded['pool']), active=recorded['active'],
        candidate=recorded['candidate'], profile=recorded['profile'], repo_root=root), recorded['rendered_sha256']


def test_historical_source_delivery_reproduces_original_source_bytes(historical_source_inputs):
    from scripts.ops.nebius_pool_application_history import render_legacy_source_delivery

    inputs, expected = historical_source_inputs
    before = inputs['before'].model_dump(mode='json')
    actual = render_legacy_source_delivery(**inputs)
    encoded = json.dumps(dataclasses.asdict(actual), sort_keys=True, separators=(',', ':'))
    assert hashlib.sha256(encoded.encode()).hexdigest() == expected
    assert inputs['before'].model_dump(mode='json') == before


def test_historical_reconstruction_does_not_change_current_rendering(historical_source_inputs):
    from scripts.ops.nebius_pool_application_delivery import render_application_build_delivery
    from scripts.ops.nebius_pool_application_history import render_legacy_source_delivery

    inputs, _ = historical_source_inputs
    current = render_application_build_delivery(**inputs)
    old = render_legacy_source_delivery(**inputs)
    assert current != old
    assert render_application_build_delivery(**inputs) == current
    config, = (row for row in current.configuration if row['kind'] == 'ConfigMap')
    source = json.loads(config['data']['installation.json'])['applications']['runtime']['source_upload']
    assert source['spool_directory'] == '/run/loom-application-source/spool'


@pytest.mark.parametrize('damage', ['foreign_env', 'wrong_source_path'])
def test_historical_projection_still_rejects_unqualified_inputs(historical_source_inputs, damage):
    from scripts.ops.nebius_pool_application_history import render_legacy_source_delivery

    inputs, _ = historical_source_inputs
    if damage == 'foreign_env':
        inputs['active']['spec']['template']['spec']['containers'][0]['env'].append(
            {'name': 'UNQUALIFIED_RUNTIME', 'value': '1'})
    else:
        from loom_service.application_management.installation import ApplicationSourceUploadSettings

        before = inputs['before']
        application = before.installation.applications
        source = ApplicationSourceUploadSettings(
            credentials_file=Path('/foreign/credentials'), spool_directory=Path('/foreign/spool'))
        application = application.model_copy(update={'runtime': application.runtime.model_copy(update={'source_upload': source})})
        inputs['before'] = before.model_copy(update={'installation': before.installation.model_copy(
            update={'applications': application})})
    with pytest.raises(ValueError):
        render_legacy_source_delivery(**inputs)
