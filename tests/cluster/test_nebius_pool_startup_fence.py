"""Actual Kubernetes CAS ordering; only disposable workloads are created here."""
from __future__ import annotations

import asyncio
import copy
import os
import threading
from uuid import uuid4

import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1', reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
async def test_real_startup_fence_blocks_delayed_patch_and_preserves_templates():
    from kubernetes.client.exceptions import ApiException
    from scripts.ops.nebius_pool_startup_fence import startup_fence_patches

    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        namespace = 'startup-fence'
        await asyncio.to_thread(core.create_namespace, {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}})

        async def call(path, method, body=None, *, ready=None):
            def execute():
                if ready is not None:
                    ready.wait(timeout=10)
                return core.api_client.call_api(path, method,
                    body=body, response_type='object', auth_settings=['BearerToken'], _return_http_data_only=True,
                    header_params={'Content-Type': 'application/json-patch+json' if method == 'PATCH' else 'application/json'})
            return await asyncio.to_thread(execute)

        for kind in ('Deployment', 'CronJob'):
            for winner in ('fence', 'start', 'concurrent'):
                name = kind.lower() + '-' + winner
                template = {'metadata': {'labels': {'app': name}}, 'spec': {
                    'containers': [{'name': 'test', 'image': 'busybox:1.36', 'command': ['sleep', '600']}],
                    'restartPolicy': 'Always' if kind == 'Deployment' else 'Never'}}
                document = {'apiVersion': 'apps/v1' if kind == 'Deployment' else 'batch/v1', 'kind': kind,
                    'metadata': {'name': name, 'namespace': namespace, 'annotations': {'fixture': 'preserved'}},
                    'spec': {'replicas': 0, 'selector': {'matchLabels': {'app': name}}, 'template': template} if kind == 'Deployment'
                        else {'schedule': '0 0 1 1 *', 'suspend': True, 'jobTemplate': {'spec': {'template': template}}}}
                collection = '/apis/' + document['apiVersion'] + '/namespaces/' + namespace + '/' + ('deployments' if kind == 'Deployment' else 'cronjobs')
                original = await call(collection, 'POST', document)
                path, operation = collection + '/' + name, uuid4()
                field = 'replicas' if kind == 'Deployment' else 'suspend'
                # A controller can update status immediately after creation.
                # Refresh only after an explicit API rejection, never a timeout.
                for attempt in range(10):
                    before = await call(path, 'GET')
                    pending = [
                        {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': before['metadata']['resourceVersion']},
                        {'op': 'test', 'path': '/spec', 'value': copy.deepcopy(before['spec'])},
                        {'op': 'replace', 'path': '/spec/' + field, 'value': 1 if kind == 'Deployment' else False}]
                    barrier = startup_fence_patches(original, before, operation)
                    actual_winner = winner
                    if winner == 'concurrent':
                        ready = threading.Barrier(2)
                        replies = await asyncio.gather(call(path, 'PATCH', barrier, ready=ready),
                            call(path, 'PATCH', pending, ready=ready), return_exceptions=True)
                        successes = [(label, reply) for label, reply in zip(('fence', 'start'), replies, strict=True)
                            if not isinstance(reply, BaseException)]
                        assert all(not isinstance(reply, BaseException) or
                            (isinstance(reply, ApiException) and reply.status in (409, 422)) for reply in replies)
                        assert len(successes) <= 1, 'both original-version CAS requests committed'
                        if successes:
                            actual_winner, settled = successes[0]
                            break
                        assert attempt < 9
                        continue
                    try:
                        settled = await call(path, 'PATCH', barrier if winner == 'fence' else pending)
                        break
                    except ApiException as error:
                        assert error.status in (409, 422) and attempt < 9
                assert settled['metadata']['uid'] == before['metadata']['uid']
                assert settled['metadata']['resourceVersion'] != before['metadata']['resourceVersion']
                with pytest.raises(ApiException) as error:
                    await call(path, 'PATCH', pending if actual_winner == 'fence' else barrier)
                assert error.value.status in (409, 422)
                current = await call(path, 'GET')
                assert current['spec'] == settled['spec']
                assert current['metadata']['annotations']['fixture'] == 'preserved'
                if actual_winner == 'fence':
                    assert current['metadata']['annotations']['loom.nebius/pool-startup-fence'] == str(operation)
                    assert current['spec'] == before['spec']
                else:
                    assert 'loom.nebius/pool-startup-fence' not in current['metadata']['annotations']
                    expected = copy.deepcopy(before['spec'])
                    expected[field] = 1 if kind == 'Deployment' else False
                    assert current['spec'] == expected
    finally:
        await asyncio.to_thread(container.stop)
