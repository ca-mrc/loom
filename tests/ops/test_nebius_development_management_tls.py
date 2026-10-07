"""The dev manager's exact-host TLS Secret is installed once before its route."""
from __future__ import annotations

import base64
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_certificates import NOW, material
from tests.ops.test_nebius_management_stage import PhaseAPI


@pytest.fixture
def tls_material(monkeypatch):
    from scripts.ops import nebius_certificates as certificates
    from scripts.ops.nebius_development_management_tls import ManagementTLSMaterial

    chain, key, roots = material(names=('manage.example.com',))
    validate = certificates.validate_management_certificate

    def local_trust(chain, key, *, management_host):
        return validate(chain, key, management_host=management_host, now=NOW, roots=roots)

    # External trust/clock only; real chain, key and SAN validation still run.
    monkeypatch.setattr(certificates, 'validate_management_certificate', local_trust)
    return ManagementTLSMaterial('manage.example.com', chain.decode(), key.decode())


@pytest.fixture
def inputs(tls_material):
    from scripts.ops.nebius_management_material import ManagementBinding

    binding = ManagementBinding(str(uuid4()), 'loom-nebius-management-dev', str(uuid4()), str(uuid4()))
    return tls_material, binding, PhaseAPI(binding)


def run(inputs, state):
    from scripts.ops.nebius_development_management_tls import deliver_management_tls

    return deliver_management_tls(material=inputs[0], binding=inputs[1], api=inputs[2], state_dir=state)


def test_tls_delivery_is_exact_host_private_namespace_immutable_and_read_only_on_replay(inputs, tmp_path):
    result = run(inputs, tmp_path / 'tls')
    assert run(inputs, tmp_path / 'tls') == result
    assert len(inputs[2].creates) == 1
    secret, = inputs[2].resources.values()
    assert secret['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert secret['type'] == 'kubernetes.io/tls' and secret['immutable'] is True
    assert set(secret['data']) == {'tls.crt', 'tls.key'}
    assert base64.b64decode(secret['data']['tls.crt']).decode() == inputs[0].chain
    assert base64.b64decode(secret['data']['tls.key']).decode() == inputs[0].key
    assert 'PRIVATE KEY' not in json.dumps(result) and secret['data']['tls.key'] not in json.dumps(result)


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_tls_uncertain_create_is_readback_only(inputs, tmp_path, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].failure = failure
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ManagementStageError, match='unresolved'):
                run(inputs, tmp_path / 'tls')
        else:
            run(inputs, tmp_path / 'tls')
    assert len(inputs[2].creates) == 1


@pytest.mark.parametrize('change', ['namespace', 'host', 'key', 'replaced'])
def test_tls_binding_or_material_change_never_adopts_or_rotates(inputs, tmp_path, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    values = list(inputs)
    if change == 'replaced':
        run(inputs, tmp_path / 'tls')
        secret, = inputs[2].resources.values()
        secret['metadata']['uid'] = str(uuid4())
    elif change == 'namespace':
        values[1] = replace(inputs[1], namespace='loom-nebius-platform')
    elif change == 'host':
        values[0] = replace(inputs[0], public_host='foreign.example.com')
    else:
        values[0] = replace(inputs[0], key='private-unqualified-key')
    before = len(inputs[2].creates)
    with pytest.raises(ManagementStageError):
        run(values, tmp_path / 'tls')
    assert len(inputs[2].creates) == before
