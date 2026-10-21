"""The v2 provider path retains exact ownership, read-only import and shared buckets."""
from __future__ import annotations

import copy
import json
from dataclasses import replace

import httpx
import pytest

from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError
from loom_service.environment_management.steps import ProvisioningStep
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_cloud_provider import CloudApi
from tests.unit.test_nebius_environment_foundation import declaration
from tests.unit.test_nebius_environment_kubernetes_provider import context, namespace


@pytest.mark.parametrize('present', [False, True])
async def test_kubernetes_import_observes_exact_uid_and_cannot_create(present):
    ctx = context()
    step = namespace()
    ctx.identities[step.key] = 'approved-uid'
    actual = KubernetesEnvironmentProvider._expected(ctx, step)
    actual['metadata']['uid'] = 'approved-uid'
    writes = []

    def api(request):
        if request.method != 'GET':
            writes.append(request.method)
        return httpx.Response(200, json=actual) if present else httpx.Response(404)

    async with httpx.AsyncClient(base_url='https://kubernetes.test', transport=httpx.MockTransport(api)) as http:
        provider = KubernetesEnvironmentProvider(http)
        if present:
            assert await provider.apply(ctx, step, create=False) == 'approved-uid'
            actual['metadata']['uid'] = 'replacement-uid'
            with pytest.raises(ProviderBlockedError, match='identity_conflict'):
                await provider.apply(ctx, step, create=False)
        else:
            with pytest.raises(ProviderBlockedError, match='missing'):
                await provider.apply(ctx, step, create=False)
        assert writes == []


async def test_import_cannot_use_names_without_exact_retained_provider_identities():
    ctx, api = context(), CloudApi()
    step = ProvisioningStep('iam:canonical:group', 'credentials', {'purpose': 'canonical', 'action': 'group'})
    ctx.config.update(project_id='project-test', quota_parent_id='tenant-test')
    with pytest.raises(ProviderBlockedError, match='identity_missing'):
        await NebiusEnvironmentCloudProvider(api).apply(ctx, step, create=False)
    assert api.created == []
    async with httpx.AsyncClient(base_url='https://kubernetes.test', transport=httpx.MockTransport(
            lambda request: pytest.fail('identity-free import performed IO'))) as http:
        with pytest.raises(ProviderBlockedError, match='identity_missing'):
            await KubernetesEnvironmentProvider(http).apply(ctx, namespace(), create=False)


async def test_v2_bucket_uses_explicit_names_without_changing_v1_authority(platform_inputs):
    desired = declaration(platform_inputs)
    ctx = replace(context(), registration=desired.registration.model_dump(mode='json'), config=desired.platform_config)
    ctx.identities['iam:canonical:group'] = 'exact-canonical-group'
    api = CloudApi()
    step = ProvisioningStep('bucket:artifacts', 'object_bucket', {'purpose': 'artifacts', 'name': 'red-canonical'})
    provider = NebiusEnvironmentCloudProvider(api)
    identity = await provider.apply(ctx, step)
    assert api.created[0][1]['metadata']['name'] == 'red-canonical'
    assert api.created[0][1]['spec']['bucket_policy'] == {'rules': [
        {'group_id': 'exact-canonical-group', 'paths': ['*'], 'roles': ['storage.object-editor']}]}
    ctx.identities[step.key] = identity
    assert await provider.apply(ctx, step, create=False) == identity
    api.rows.clear()
    with pytest.raises(ProviderBlockedError, match='missing'):
        await provider.apply(ctx, step, create=False)
    assert len(api.created) == 1
    with pytest.raises(ProviderBlockedError, match='name_mismatch'):
        await provider.apply(ctx, replace(step, payload={'purpose': 'artifacts', 'name': 'foreign'}))


async def test_kubernetes_create_success_rejects_replacement_before_readback():
    """A create reply alone cannot substitute for authenticated observed state."""
    ctx, calls = context(), []
    stored = None

    def api(request):
        nonlocal stored
        calls.append(request.method)
        if request.method == 'POST':
            stored = json.loads(request.content)
            stored['metadata']['uid'] = 'actual-uid'
            reply = copy.deepcopy(stored)
            reply['metadata']['uid'] = 'unconfirmed-reply-uid'
            return httpx.Response(201, json=reply)
        return httpx.Response(200, json=stored) if stored else httpx.Response(404)

    async with httpx.AsyncClient(base_url='https://kubernetes.test', transport=httpx.MockTransport(api)) as http:
        # V1 also benefits from readback, while its intent/receipt bytes stay intact.
        with pytest.raises(ProviderBlockedError, match='identity_conflict'):
            await KubernetesEnvironmentProvider(http).apply(ctx, namespace())
    assert calls == ['GET', 'POST', 'GET']


@pytest.mark.parametrize('damage', ['owner', 'init_container', 'host_network', 'privileged', 'service_account',
    'command', 'environment_source', 'secret_volume', 'container_security_context'])
def test_controller_readback_rejects_added_execution_authority(damage):
    expected = {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'service'},
        'spec': {'template': {'spec': {'containers': [{'name': 'service', 'image': 'fixed',
            'securityContext': {'allowPrivilegeEscalation': False}}], 'automountServiceAccountToken': False}}}}
    actual = copy.deepcopy(expected)
    actual['metadata']['uid'] = 'retained'
    pod = actual['spec']['template']['spec']
    if damage == 'owner':
        actual['metadata']['ownerReferences'] = [{'uid': 'foreign-owner'}]
    elif damage == 'init_container':
        pod['initContainers'] = [{'name': 'unreviewed', 'image': 'foreign'}]
    elif damage == 'host_network':
        pod['hostNetwork'] = True
    elif damage == 'privileged':
        pod['containers'][0]['securityContext']['privileged'] = True
    elif damage == 'service_account':
        pod['serviceAccountName'] = 'foreign'
    elif damage == 'command':
        pod['containers'][0]['command'] = ['unapproved-command']
    elif damage == 'environment_source':
        pod['containers'][0]['envFrom'] = [{'secretRef': {'name': 'foreign'}}]
    elif damage == 'secret_volume':
        pod['volumes'] = [{'name': 'foreign', 'secret': {'secretName': 'foreign'}}]
    else:
        expected['spec']['template']['spec']['containers'][0].pop('securityContext')
        pod['containers'][0]['securityContext'] = {'privileged': True}
    with pytest.raises(ProviderBlockedError, match='identity_conflict'):
        KubernetesEnvironmentProvider._identity(actual, expected, 'retained')
