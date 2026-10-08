"""The manager must qualify its own dynamic disk, not a staging or cloned disk."""
from __future__ import annotations

import copy
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_install import run
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def manager_disk(installation, cloud, tmp_path, monkeypatch):
    from nebius.api.nebius.compute import v1 as compute
    from nebius.api.nebius.iam import v1 as iam
    from nebius.sdk import SDK
    from scripts.ops.nebius_development_cloud import DevelopmentCloudScope
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_development_management_prerequisites import (
        HTTPSDevelopmentManagementPrerequisites,
    )

    request, store = installation
    run(installation, tmp_path)
    store.complete('StatefulSet')
    binding, rendered = store.store.binding, render_installation(request)
    rows = store.store.resources
    controller = rows['StatefulSet:loom-postgres']
    claim = rows['PersistentVolumeClaim:data-loom-postgres-0']
    volume = rows['PersistentVolume:' + claim['spec']['volumeName']]
    claim['metadata']['creationTimestamp'] = '2026-10-07T12:00:01Z'
    volume['metadata'].update(creationTimestamp='2026-10-07T12:00:03Z',
        annotations={'pv.kubernetes.io/provisioned-by': 'compute.csi.nebius.com'})
    volume['spec'].update(accessModes=['ReadWriteOnce'],
        csi={'driver': 'compute.csi.nebius.com', 'volumeHandle': 'computedisk-test'})
    spec = copy.deepcopy(controller['spec']['template']['spec'])
    spec.setdefault('volumes', []).append({'name': 'data', 'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
    spec.update(nodeName='computeinstance-test', hostname='loom-postgres-0', subdomain='loom-postgres')
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'loom-postgres-0',
        'namespace': binding.namespace, 'uid': str(uuid4()), 'ownerReferences': [{
            'apiVersion': 'apps/v1', 'kind': 'StatefulSet', 'name': 'loom-postgres',
            'uid': controller['metadata']['uid'], 'controller': True}]}, 'spec': spec}
    node = {'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': 'computeinstance-test', 'uid': str(uuid4())},
        'spec': {'providerID': 'nebius://computeinstance-test'}}
    docs = {'/api/v1/namespaces/kube-system': {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': 'kube-system', 'uid': binding.kube_system_uid}},
        '/api/v1/namespaces/' + binding.namespace: store.bootstrap.namespace,
        '/api/v1/namespaces/' + binding.namespace + '/pods/loom-postgres-0': pod,
        '/api/v1/nodes/computeinstance-test': node,
        '/api/v1/namespaces/' + binding.namespace + '/persistentvolumeclaims/data-loom-postgres-0': claim,
        '/api/v1/persistentvolumes/' + claim['spec']['volumeName']: volume,
        '/apis/apps/v1/namespaces/' + binding.namespace + '/statefulsets/loom-postgres': controller}
    calls = []
    def handle(message):
        assert message.method == 'GET'
        calls.append(message.url.path)
        return httpx.Response(200, json=docs[message.url.path])
    # Use the real fixed adapter with only its external HTTP boundary replaced.
    original = httpx.Client
    monkeypatch.setattr(httpx, 'Client', lambda **kwargs: original(**{**kwargs, 'transport': httpx.MockTransport(handle)}))
    credential = tmp_path / 'operator.json'
    credential.write_text('{}')
    credential.chmod(0o600)
    api = HTTPSDevelopmentManagementPrerequisites(settings=SimpleNamespace(), operator_cloud_credentials=credential,
        api_server=request.deployment.installation.foundation.platform_config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), token='operator')
    retained = SimpleNamespace(inputs=SimpleNamespace(settings=SimpleNamespace(cloud=DevelopmentCloudScope.model_validate(cloud.scope))))
    monkeypatch.setattr(api, 'foundation', lambda selected: retained)
    closes, change = [], {}
    async def close(self):
        closes.append(True)
        if change.get('node'):
            node['metadata']['uid'] = str(uuid4())
        if change.get('identity'):
            credential.write_text('{"changed":true}')
    monkeypatch.setattr(SDK, '__init__', lambda self, **kwargs: None)
    monkeypatch.setattr(SDK, 'close', close)
    monkeypatch.setattr(compute, 'DiskServiceClient', lambda sdk: cloud.clients['disks'])
    monkeypatch.setattr(iam, 'ProjectServiceClient', lambda sdk: cloud.clients['projects'])
    receipt = {'status': 'management_storage_verified', 'pvc_uid': claim['metadata']['uid'], 'pv_uid': volume['metadata']['uid']}
    with api:
        yield SimpleNamespace(api=api, request=request, binding=binding, rendered=rendered, receipt=receipt,
            claim=claim, volume=volume, pod=pod, node=node, controller=controller, cloud=cloud,
            calls=calls, closes=closes, change=change)


def qualify(selected):
    selected.api.qualify_storage(selected.request, selected.binding, selected.rendered, selected.receipt)


def test_manager_storage_uses_actual_bound_disk_and_pod_node_without_writes(manager_disk):
    qualify(manager_disk)
    assert ('get', 'computedisk-test') in manager_disk.cloud.calls
    assert manager_disk.closes == [True]
    assert not any('loom-nebius-platform' in path or '/namespaces/loom-dev/' in path for path in manager_disk.calls)


@pytest.mark.parametrize('change', ['pvc-uid', 'pv-uid', 'clone', 'driver', 'pod-owner', 'pod-volume', 'node-provider',
    'disk-project', 'disk-source', 'disk-attachment', 'late-node', 'late-identity'])
def test_storage_drift_blocks_migration_and_closes_provider_connection(manager_disk, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    state = manager_disk
    if change == 'pvc-uid':
        state.claim['metadata']['uid'] = str(uuid4())
    elif change == 'pv-uid':
        state.volume['metadata']['uid'] = str(uuid4())
    elif change == 'clone':
        state.claim['spec']['dataSource'] = {'kind': 'PersistentVolumeClaim', 'name': 'old'}
    elif change == 'driver':
        state.volume['spec']['csi']['driver'] = 'foreign.csi'
    elif change == 'pod-owner':
        state.pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif change == 'pod-volume':
        state.pod['spec']['volumes'][-1]['persistentVolumeClaim']['claimName'] = 'foreign'
    elif change == 'node-provider':
        state.node['spec']['providerID'] = 'nebius://computeinstance-other'
    elif change.startswith('disk-'):
        disk = state.cloud.rows['computedisk-test'][1]
        section, key, value = {'disk-project': ('metadata', 'parent_id', 'project-foreign'),
            'disk-source': ('spec', 'source_snapshot_id', 'snapshot-old'),
            'disk-attachment': ('status', 'read_write_attachment', 'computeinstance-foreign')}[change]
        disk[section][key] = value
    else:
        state.change[change.removeprefix('late-')] = True
    with pytest.raises(ManagementInstallError):
        qualify(state)
    assert state.closes == ([True] if change.startswith(('disk-', 'late-')) else [])
