"""Disposable API defaulting for fresh namespaces and the stopped pool gateway."""
from __future__ import annotations

import base64
import os
import ssl
from uuid import uuid4

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.integration.test_nebius_pool_installation import installation

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
def test_real_namespace_defaults_and_stopped_gateway_replay_without_authority(tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_development_pool_live import _PhaseAPI
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI, _stage_fixed_documents
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

    from loom.nebius_platform_render import _namespace, digest
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_gateway

    class DisposableAPI(_PhaseAPI):
        # This test isolates the real API's defaulting/allocation. The retained
        # manager/PVC verifier is covered by the connected installation tests.
        def _private_inputs(self):
            pass

        def verify_identity(self, binding):
            HTTPSManagementStageAPI.verify_identity(self, binding)

    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster)
        config = yaml.safe_load(cluster.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        endpoint = 'https://127.0.0.1:' + str(cluster.get_exposed_port(6443))
        context = ssl.create_default_context(cadata=base64.b64decode(
            config['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = config['users'][0]['user']
        cert, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        cert.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        context.load_cert_chain(cert, key)
        identity = str(uuid4())
        manager = core.create_namespace({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': 'loom-nebius-management-dev', 'labels': {'loom.nebius/management-installation': identity,
                'pod-security.kubernetes.io/enforce': 'restricted'}}})
        binding = ManagementBinding(identity, manager.metadata.name, manager.metadata.uid,
            core.read_namespace('kube-system').metadata.uid)
        catalog, _ = installation(('development',))
        catalog['installation_id'] = identity
        catalog['participants'][0]['installation_id'] = identity
        participant = catalog['participants'][0]
        documents = {_key(doc): doc for doc in [_namespace(participant[field]['name'])
            for field in ('execution_namespace', 'build_namespace')]}

        def stage(documents, name):
            api = DisposableAPI.__new__(DisposableAPI)
            api.binding, api.documents = binding, documents
            ManagementKubernetesTransport.__init__(api, api_server=endpoint, ssl_context=context)
            with api:
                return _stage_fixed_documents(documents=documents, revision=digest(documents), phase=name,
                    binding=binding, api=api, state_dir=tmp_path / name)

        namespaces = stage(documents, 'namespaces')
        assert stage(documents, 'namespaces') == namespaces
        for field in ('execution_namespace', 'build_namespace'):
            namespace = core.read_namespace(participant[field]['name'])
            assert namespace.metadata.labels['pod-security.kubernetes.io/enforce'] == 'restricted'
            participant[field]['uid'] = namespace.metadata.uid
        spec = PoolInstallation.model_validate(catalog)
        gateway = render_gateway(spec, namespace=binding.namespace,
            service_image='registry.example/service@sha256:' + 'b' * 64, kubernetes_endpoint=endpoint)
        for phase in ('configuration', 'workload'):
            docs = {_key(doc): doc for doc in gateway[phase]}
            receipt = stage(docs, phase)
            assert stage(docs, phase) == receipt
        deployment = client.AppsV1Api(core.api_client).read_namespaced_deployment('loom-pool-gateway', binding.namespace)
        assert deployment.spec.replicas == 0
        assert core.list_namespaced_pod(binding.namespace).items == []
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        for namespace in (binding.namespace, participant['execution_namespace']['name'], participant['build_namespace']['name']):
            assert rbac.list_namespaced_role_binding(namespace).items == []
    finally:
        cluster.stop()
