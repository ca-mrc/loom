"""Actual renewal journals and TLS stage; only Kubernetes/HTTPS are external."""
from __future__ import annotations

import copy
import importlib
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_certificates import NOW
from tests.ops.test_nebius_certificates import material as certificate_material
from tests.ops.test_nebius_development_management_retained import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_retained import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_retained import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_retained import cloud as cloud
from tests.ops.test_nebius_development_management_retained import installation as installation
from tests.ops.test_nebius_development_management_retained import inventory as inventory
from tests.ops.test_nebius_development_management_retained import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_retained import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_retained import material as material
from tests.ops.test_nebius_development_management_retained import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_retained import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_retained import retained as retained
from tests.ops.test_nebius_development_management_retained import route as route
from tests.ops.test_nebius_development_management_retained import tls_material as tls_material


def module():
    return importlib.import_module('scripts.ops.nebius_development_management_renewal')


def new_material(monkeypatch):
    from scripts.ops import nebius_certificates as certificates
    from scripts.ops.nebius_development_management_tls import ManagementTLSMaterial

    chain, key, roots = certificate_material(names=('manage.example.com',))
    monkeypatch.setattr(certificates, 'validate_management_certificate',
        lambda chain, key, *, management_host: certificates._validate_certificate(
            chain, key, names=[management_host], now=NOW, roots=roots))
    return ManagementTLSMaterial('manage.example.com', chain.decode(), key.decode())


class RenewalAPI:
    def __init__(self, store):
        self.store = store
        self.ingress = store.resources['Ingress:loom-management']
        self.ingress['metadata'].update(resourceVersion='10', generation=1)
        self.patches = []
        self.failure = None
        self.public = True
        self.drift_on_public = False

    def verify(self):
        if self.failure == 'identity':
            raise ValueError('identity changed')

    def read_ingress(self):
        return copy.deepcopy(self.ingress)

    def qualify_route(self, expected):
        assert self.ingress['spec'] == expected['spec']
        # Status may advance during slow DNS/shared-ingress qualification.
        self.ingress['metadata']['resourceVersion'] = str(int(self.ingress['metadata']['resourceVersion']) + 1)

    @contextmanager
    def tls(self, material, binding):
        assert binding == self.store.binding
        yield self.store

    def preview(self, before, target):
        assert before['metadata']['resourceVersion'] == self.ingress['metadata']['resourceVersion']
        return copy.deepcopy(target)

    def patch(self, before, target):
        assert before['metadata']['resourceVersion'] == self.ingress['metadata']['resourceVersion']
        self.patches.append((copy.deepcopy(before), copy.deepcopy(target)))
        if self.failure == 'conflict':
            return False
        if self.failure == 'before':
            raise RuntimeError('lost before commit')
        version = str(int(before['metadata']['resourceVersion']) + 1)
        self.ingress = copy.deepcopy(target)
        self.ingress['metadata'].update(resourceVersion=version, generation=2)
        if self.failure == 'after':
            raise RuntimeError('lost after commit')
        return True

    def public_ready(self, target, fingerprint):
        assert len(fingerprint) == 64 and self.ingress['spec'] == target['spec']
        if self.drift_on_public:
            self.ingress['metadata']['uid'] = str(uuid4())
        return self.public


@pytest.fixture
def renewal(retained, monkeypatch):
    from scripts.ops.nebius_development_management_retained import (
        RetainedManagementReference,
        load_retained_management,
    )

    state = load_retained_management(RetainedManagementReference.model_validate(retained[0]))
    request = module().RenewalRequest(retained=state, material=new_material(monkeypatch),
        operation_id=uuid4(), qualification_digest='sha256:' + 'b' * 64)
    return request, RenewalAPI(retained[4].store)


def run(renewal, *, execute=True):
    return module().renew_management_tls(request=renewal[0], api=renewal[1], execute=execute)


def test_renewal_updates_only_tls_reference_then_proves_public_leaf_and_replays(renewal):
    request, api = renewal
    before = copy.deepcopy(api.ingress)
    writes = len(api.store.creates)
    result = run(renewal)
    assert result['status'] == 'development_management_tls_renewed'
    assert len(api.patches) == 1 and len(api.store.creates) == writes + 1
    assert api.patches[0][0]['metadata']['resourceVersion'] != '10'
    changed = copy.deepcopy(api.ingress['spec'])
    changed['tls'][0]['secretName'] = before['spec']['tls'][0]['secretName']
    assert changed == before['spec']
    assert api.ingress['metadata']['uid'] == before['metadata']['uid']
    assert run(renewal) == result
    assert len(api.patches) == 1 and len(api.store.creates) == writes + 1
    assert request.material.key not in json.dumps(result)


def test_preflight_never_creates_secret_or_renewal_state(renewal):
    request, api = renewal
    writes = len(api.store.creates)
    assert run(renewal, execute=False)['status'] == 'development_management_tls_preflight_qualified'
    assert not api.patches and len(api.store.creates) == writes
    assert not (Path(request.retained.operation['state_dir']).parent / 'tls-renewal').exists()
    assert not (Path(request.retained.operation['anchor_dir']) / 'tls-renewal.json').exists()


@pytest.mark.parametrize('failure', ['before', 'after', 'conflict'])
def test_uncertain_patch_is_readback_only_and_definite_conflict_is_distinct(renewal, failure):
    renewal[1].failure = failure
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(module().RenewalError, match='unresolved'):
                run(renewal)
        else:
            result = run(renewal)
            assert result['status'] == ('rejected' if failure == 'conflict' else 'development_management_tls_renewed')
    assert len(renewal[1].patches) == 1


def test_pending_public_propagation_can_resume_without_another_write(renewal):
    renewal[1].public = False
    assert run(renewal)['status'] == 'pending'
    renewal[1].public = True
    assert run(renewal)['status'] == 'development_management_tls_renewed'
    assert len(renewal[1].patches) == 1


def test_late_ingress_replacement_cannot_be_reported_complete(renewal):
    renewal[1].drift_on_public = True
    with pytest.raises(module().RenewalError):
        run(renewal)


@pytest.mark.parametrize('damage', ['identity', 'uid', 'host', 'private-file', 'invalid-key'])
def test_invalid_binding_or_material_never_creates_or_switches(renewal, damage):
    request, api = renewal
    if damage == 'identity':
        api.failure = 'identity'
    elif damage == 'uid':
        api.ingress['metadata']['uid'] = str(uuid4())
    elif damage == 'host':
        api.ingress['spec']['rules'][0]['host'] = 'staging.example.com'
    elif damage == 'private-file':
        path = Path(request.retained.operation['inputs_path'])
        path.write_text('{}')
    else:
        request = replace(request, material=replace(request.material, key='invalid-key'))
    writes = len(api.store.creates)
    with pytest.raises(module().RenewalError):
        run((request, api))
    assert not api.patches and len(api.store.creates) == writes


def test_second_generation_uses_completed_predecessor_not_initial_route(renewal, monkeypatch):
    request, api = renewal
    run(renewal)
    previous = copy.deepcopy(api.ingress)
    newer = replace(request, material=new_material(monkeypatch), operation_id=uuid4())
    assert run((newer, api))['status'] == 'development_management_tls_renewed'
    assert len(api.patches) == 2
    assert api.patches[1][0]['spec'] == previous['spec']
    with pytest.raises(module().RenewalError, match='superseded'):
        run(renewal)
    assert len(api.patches) == 2


def test_unknown_predecessor_blocks_new_operation(renewal):
    request, api = renewal
    api.failure = 'before'
    with pytest.raises(module().RenewalError):
        run(renewal)
    with pytest.raises(module().RenewalError):
        run((replace(request, operation_id=uuid4()), api))
    assert len(api.patches) == 1


@pytest.mark.parametrize('missing', ['anchor', 'journal'])
def test_lost_renewal_history_never_reopens_writes(renewal, missing):
    request, api = renewal
    run(renewal)
    operation = request.retained.operation
    path = (Path(operation['anchor_dir']) / 'tls-renewal.json' if missing == 'anchor'
        else Path(operation['state_dir']).parent / 'tls-renewal/renewal.json')
    path.unlink()
    with pytest.raises(module().RenewalError):
        run(renewal)
    assert len(api.patches) == 1
