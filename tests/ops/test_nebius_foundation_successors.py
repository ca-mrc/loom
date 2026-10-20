"""Authorized runtime successors do not rewrite creation or data lineage."""
from __future__ import annotations

import copy
import json
from uuid import uuid4

import httpx
import pytest
from scripts.ops import nebius_environment_foundation as foundation
from scripts.ops.nebius_ingress_stage import _snapshot
from tests.ops.test_nebius_environment_foundation import Remote, prepare, run
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_candidate_catalog import resolve

from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError


@pytest.mark.parametrize('damage', [None, 'uid', 'unapproved', 'data', 'create'])
async def test_runtime_readback_keeps_exact_successors_and_original_data(tmp_path, platform_inputs, damage):
    state = tmp_path / 'foundation'
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    original = await run(state, reference, remote)
    receipt = (state / 'observed.json').read_bytes()
    writes = copy.deepcopy(remote.writes)
    api = remote.kubernetes['/apis/apps/v1/namespaces/app-red/deployments/loom-service']
    api['spec']['replicas'] = 0
    api['status']['readyReplicas'] = 0
    expected = _snapshot(api)
    expected['metadata']['uid'] = api['metadata']['uid']
    successors = {'k8s:Deployment:app-red:loom-service': (expected,)}
    if damage == 'uid':
        api['metadata']['uid'] = expected['metadata']['uid'] = str(uuid4())
    elif damage == 'unapproved':
        api['spec']['template']['spec']['containers'][0]['image'] = 'foreign/image'
    elif damage == 'data':
        database = remote.kubernetes['/apis/apps/v1/namespaces/app-red/statefulsets/loom-postgres']
        successors['k8s:StatefulSet:app-red:loom-postgres'] = (copy.deepcopy(database),)
    async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as http:
        async def observe():
            return await foundation.reconcile_foundation(state_dir=state, reference=reference,
                kubernetes=KubernetesEnvironmentProvider(http), cloud=remote,
                observe_only=damage != 'create', _runtime_successors=successors)
        if damage is None:
            observed = await observe()
            assert observed.identities == original.identities
            assert observed.phase == 'runtime_bindings_verified'
            assert observed.execution_enabled is None, 'resource readback cannot assert current admission state'
            # No supplied successor can hide replacement of a stable Secret.
            remote.kubernetes['/api/v1/namespaces/app-red/secrets/loom-platform-db']['metadata']['uid'] = str(uuid4())
            with pytest.raises((ValueError, ProviderBlockedError)):
                await observe()
        else:
            with pytest.raises((ValueError, ProviderBlockedError)):
                await observe()
    assert remote.writes == writes
    assert (state / 'observed.json').read_bytes() == receipt


async def test_release_configmap_successor_preserves_nonpublication_configuration(tmp_path, platform_inputs, publication):
    state = tmp_path / 'foundation'
    bundle = await resolve(publication)
    platform_inputs = platform_inputs[0], bundle.candidate, bundle.profile
    reference = prepare(state, platform_inputs)
    remote = Remote(state)
    await run(state, reference, remote)
    config = remote.kubernetes['/api/v1/namespaces/app-red/configmaps/loom-platform-config']
    profile = json.loads(config['data']['profile.json'])
    profile['runtime_binary_sha256'] = 'sha256:' + 'e' * 64
    config['data']['profile.json'] = json.dumps(profile)
    successor = _snapshot(config)
    successor['metadata']['uid'] = config['metadata']['uid']
    async with httpx.AsyncClient(base_url='https://api.cluster.test', transport=httpx.MockTransport(remote.http)) as http:
        async def observe():
            return await foundation.reconcile_foundation(state_dir=state, reference=reference,
                kubernetes=KubernetesEnvironmentProvider(http), cloud=remote, observe_only=True,
                _runtime_successors={'k8s:ConfigMap:app-red:loom-platform-config': (successor,)})
        assert (await observe()).phase == 'runtime_bindings_verified'
        successor['data']['config.json'] = '{}'
        with pytest.raises(ValueError, match='legacy_foundation_successor_forbidden'):
            await observe()
