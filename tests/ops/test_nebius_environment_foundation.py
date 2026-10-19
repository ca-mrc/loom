"""Protected foundation journal through actual providers; only remote APIs are fake."""
from __future__ import annotations

import base64
import copy
import importlib
import json
from uuid import uuid4

import httpx
import pytest

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_foundation import ROOT, declaration


def module():
    name = 'scripts.ops.nebius_environment_foundation'
    assert importlib.util.find_spec(name), 'protected foundation journal is missing'
    return importlib.import_module(name)


class Remote:
    def __init__(self, state):
        self.state, self.kubernetes, self.cloud, self.writes = state, {}, {}, []
        self.lose = None

    def assert_journaled(self):
        assert (self.state / 'plan.json').exists(), 'effect preceded frozen plan'
        assert list((self.state / 'steps').glob('*.intent.json')), 'effect preceded durable intent'

    def http(self, request):
        path = request.url.path
        if request.method == 'GET':
            return httpx.Response(200, json=self.kubernetes[path]) if path in self.kubernetes else httpx.Response(404)
        assert request.method == 'POST', 'foundation creation cannot patch or delete'
        self.assert_journaled()
        value = json.loads(request.content)
        path += '/' + value['metadata']['name']
        assert path not in self.kubernetes, 'duplicate create'
        value['metadata'].update(uid=str(uuid4()), generation=1, resourceVersion='1')
        if value['kind'] == 'Job':
            value['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}]}
        if value['kind'] in {'Deployment', 'StatefulSet'}:
            value['status'] = {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1,
                'updatedReplicas': 1, 'availableReplicas': 1}
        self.kubernetes[path] = value
        self.writes.append(('kubernetes', path))
        if self.lose == 'kubernetes':
            self.lose = None
            raise httpx.ReadTimeout('lost reply', request=request)
        return httpx.Response(201, json=value)

    @staticmethod
    def key(kind, expected):
        return kind, expected['metadata']['parent_id'], expected['metadata'].get('name', expected['spec'].get('member_id'))

    async def find(self, kind, expected):
        return copy.deepcopy(self.cloud.get(self.key(kind, expected)))

    async def create(self, kind, expected, *, idempotency_key):
        self.assert_journaled()
        key = self.key(kind, expected)
        assert key not in self.cloud, 'duplicate cloud create'
        value = copy.deepcopy(expected)
        value['metadata']['id'] = 'resource-' + str(uuid4())
        self.cloud[key] = value
        self.writes.append(('cloud', key))
        if self.lose == 'cloud':
            self.lose = None
            raise ProviderRetryError('lost_reply')
        return copy.deepcopy(value)

    async def access_key_secret(self, identity):
        assert any(row['metadata']['id'] == identity for row in self.cloud.values())
        return {'access-key': 'access-' + identity, 'secret-key': 'private-' + identity}


def prepare(state, inputs, *, kind='development'):
    return module().prepare_foundation(state_dir=state,
        declaration=declaration(inputs, kind=kind, backups=kind == 'production'), candidate=inputs[1],
        profile=inputs[2], keyring={}, repo_root=ROOT)


async def run(state, reference, remote):
    async with httpx.AsyncClient(base_url='https://kubernetes.test', transport=httpx.MockTransport(remote.http)) as http:
        return await module().reconcile_foundation(state_dir=state, reference=reference,
            kubernetes=KubernetesEnvironmentProvider(http), cloud=remote)


@pytest.mark.parametrize('kind', ['development', 'staging', 'production'])
async def test_foundation_creation_uses_own_resources_then_reobserves_without_reinstall(tmp_path, platform_inputs, kind):
    state = tmp_path / 'foundation'
    remote = Remote(state)
    reference = prepare(state, platform_inputs, kind=kind)
    observed = await run(state, reference, remote)
    assert observed.phase == 'resources_verified'
    assert observed.execution_enabled is False
    assert set(observed.namespace_uids) == {'app-red', 'work-red', 'build-red'}
    assert all(value in {doc['metadata']['uid'] for doc in remote.kubernetes.values()}
        for value in observed.namespace_uids.values())
    buckets = [doc for (resource, _, _), doc in remote.cloud.items() if resource == 'bucket']
    assert {doc['metadata']['name'] for doc in buckets} == (
        {'red-canonical', 'red-source', 'red-backup'} if kind == 'production' else {'red-canonical', 'red-source'})
    writes = copy.deepcopy(remote.writes)
    again = await run(state, reference, remote)
    assert again.identities == observed.identities
    assert remote.writes == writes
    assert not any(doc['kind'] in {'Deployment', 'Job'} and doc['metadata']['namespace'] in {'work-red', 'build-red'}
        for doc in remote.kubernetes.values())


@pytest.mark.parametrize('lost', ['cloud', 'kubernetes'])
async def test_restart_after_lost_reply_reuses_frozen_intent_and_does_not_duplicate(tmp_path, platform_inputs, lost):
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    remote.lose = lost
    with pytest.raises(ProviderRetryError):
        await run(state, reference, remote)
    assert not (state / 'observed.json').exists()
    completed = await run(state, reference, remote)
    assert completed.phase == 'resources_verified'
    assert len(remote.writes) == len(set(map(repr, remote.writes)))


async def test_foreign_namespace_is_not_adopted_or_overwritten(tmp_path, platform_inputs):
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    remote.kubernetes['/api/v1/namespaces/app-red'] = {
        'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'app-red', 'uid': 'foreign'}}
    with pytest.raises(ProviderBlockedError, match='identity_conflict'):
        await run(state, reference, remote)
    assert remote.writes == []
    assert not (state / 'observed.json').exists()


@pytest.mark.parametrize('damage', ['namespace', 'secret', 'cloud', 'plan', 'material'])
async def test_completed_receipt_never_masks_current_resource_or_input_drift(tmp_path, platform_inputs, damage):
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    await run(state, reference, remote)
    writes = copy.deepcopy(remote.writes)
    if damage == 'namespace':
        remote.kubernetes['/api/v1/namespaces/app-red']['metadata']['uid'] = 'replaced'
    elif damage == 'secret':
        del remote.kubernetes['/api/v1/namespaces/app-red/secrets/loom-platform-db']
    elif damage == 'cloud':
        next(iter(remote.cloud.values()))['metadata']['id'] = 'replaced'
    elif damage == 'plan':
        content = json.loads((state / 'plan.json').read_text())
        content['declaration']['registration']['kind'] = 'production'
        (state / 'plan.json').write_text(json.dumps(content))
    else:
        (state / 'material.json').unlink()
    with pytest.raises((ProviderBlockedError, ValueError)):
        await run(state, reference, remote)
    assert remote.writes == writes


async def test_exact_import_is_read_only_preserves_credentials_and_rejects_replaced_uid(tmp_path, platform_inputs):
    subject = module()
    source = tmp_path / 'source'
    reference = prepare(source, platform_inputs)
    remote = Remote(source)
    observed = await run(source, reference, remote)
    destination = tmp_path / 'import'
    imported = subject.prepare_foundation_import(state_dir=destination, source_dir=source,
        source_reference=reference, source_observation=observed, operation_id=uuid4())
    writes = copy.deepcopy(remote.writes)
    proof = await run(destination, imported, remote)
    assert proof.identities == observed.identities
    assert proof.operation_id != observed.operation_id
    assert remote.writes == writes
    secret = remote.kubernetes['/api/v1/namespaces/app-red/secrets/loom-platform-db']
    material = json.loads((source / 'material.json').read_text())
    assert base64.b64decode(secret['data']['service-password']).decode() == material['loom-platform-db']['service-password']
    secret['metadata']['uid'] = 'recreated'
    with pytest.raises(ProviderBlockedError, match='identity_conflict'):
        await run(destination, imported, remote)
    assert remote.writes == writes
