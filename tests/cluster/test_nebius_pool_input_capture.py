"""Private input observation against real typed Kubernetes list responses."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import ssl
import time

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_application_snapshot import pool_snapshot as pool_snapshot
from tests.ops.test_nebius_application_snapshot import snapshot_objects as snapshot_objects

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(120)
async def test_private_capture_observes_actual_workloads_and_roles_without_secret_exports(pool_snapshot, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_application_snapshot import _POOL_COLLECTIONS, capture_shared_inputs

    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        apps = client.AppsV1Api(core.api_client)
        shared, execution = 'loom-nebius-platform', 'loom-nebius-platform-execution'
        namespaces = {}
        for name in (shared, execution, execution + '-build'):
            value = await asyncio.to_thread(core.create_namespace, {'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': name}})
            namespaces[name] = value.metadata.uid
        kube_system = await asyncio.to_thread(core.read_namespace, 'kube-system')
        objects, _, _, _, _ = pool_snapshot
        deployment_uid = None
        for (kind, name, namespace), source in objects.items():
            if kind == 'namespace':
                continue
            document = copy.deepcopy(source)
            document['metadata'] = {'name': name, 'namespace': namespace}
            if kind == 'deployment':
                document['spec'] = {'replicas': 0, 'selector': {'matchLabels': {'app': name}},
                    'template': {'metadata': {'labels': {'app': name}}, 'spec': {
                        'containers': [{'name': 'service', 'image': 'busybox:1.36'}]}}}
                created = await asyncio.to_thread(apps.create_namespaced_deployment, namespace, document)
                deployment_uid = created.metadata.uid
            elif kind == 'configmap':
                await asyncio.to_thread(core.create_namespaced_config_map, namespace, document)
            else:
                if name == 'loom-execution-actuator-db':
                    document['data']['db-url'] = 'cHJpdmF0ZQ=='
                await asyncio.to_thread(core.create_namespaced_secret, namespace, document)
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)

        # API/namespace readiness precedes bootstrap RBAC on a fresh K3s.
        # Capture is a point-in-time observation, not a bootstrap waiter: make
        # the real role asserted below a fixture prerequisite before capturing.
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        deadline = time.monotonic() + 30
        while True:
            try:
                admin_role = await asyncio.to_thread(rbac.read_cluster_role, 'cluster-admin')
                if admin_role.rules:
                    break
            except client.ApiException as error:
                if error.status != 404:
                    raise
            assert time.monotonic() < deadline, 'disposable cluster-admin bootstrap role did not appear'
            await asyncio.sleep(0.1)

        def capture():
            with httpx.Client(base_url=configuration.host, verify=tls, trust_env=False, timeout=15) as http:
                def read(kind, name, namespace):
                    version = 'apps/v1' if kind == 'deployment' else 'v1'
                    path = ('/api/v1' if version == 'v1' else '/apis/' + version)
                    path += ('' if namespace is None else '/namespaces/' + namespace)
                    response = http.get(path + '/' + kind + 's/' + name)
                    response.raise_for_status()
                    return response.json()

                def listing(kind, namespace):
                    version, resource = _POOL_COLLECTIONS[kind]
                    path = '/api/v1' if version == 'v1' else '/apis/' + version
                    path += ('' if namespace is None else '/namespaces/' + namespace)
                    response = http.get(path + '/' + resource + '?limit=1025')
                    response.raise_for_status()
                    return response.json()

                return capture_shared_inputs(read=read, read_collection=listing, root=tmp_path,
                    cluster_id='mk8scluster-test', namespace=shared, namespace_uid=namespaces[shared],
                    kube_system_uid=kube_system.metadata.uid)

        result = await asyncio.to_thread(capture)
        root = tmp_path / result['observation_id']
        snapshot = json.loads((root / 'pool-resources.json').read_bytes())
        deployment, = (row for row in snapshot['resources'] if row['kind'] == 'Deployment')
        assert deployment['metadata']['uid'] == deployment_uid
        assert deployment['spec']['replicas'] == 0
        assert any(row['kind'] == 'ClusterRole' and row['metadata']['name'] == 'cluster-admin'
            and row['metadata']['uid'] == admin_role.metadata.uid
            and row['rules'] for row in snapshot['resources'])
        assert all(row['metadata'].get('namespace') in {None, shared, execution, execution + '-build'}
            and row['kind'] != 'Secret' for row in snapshot['resources'])
        assert 'private-collector' not in (root / 'pool-resources.json').read_text()
        source = await asyncio.to_thread(core.read_namespaced_secret, 'loom-platform-storage', shared)
        assert snapshot['application_source_credential']['uid'] == source.metadata.uid
        assert snapshot['application_source_credential']['resource_version'] == source.metadata.resource_version
        assert 'private-source' not in (root / 'pool-resources.json').read_text()
        assert set(result) == {'status', 'observation_id', 'candidate_sha'}
        assert (root / 'pool-resources.json').stat().st_mode & 0o777 == 0o600
        assert not (await asyncio.to_thread(core.list_namespaced_pod, shared)).items
    finally:
        await asyncio.to_thread(container.stop)
