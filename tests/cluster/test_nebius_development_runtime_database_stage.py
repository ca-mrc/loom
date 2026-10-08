"""Actual API defaulting of runtime DB material; no fixture-image execution."""
from __future__ import annotations

import base64
import os
import ssl
from uuid import uuid4

import httpx
import pytest
import yaml
from httpx import Client as HTTPClient

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_development_pool_retained import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_retained import build_inputs as build_inputs
from tests.ops.test_nebius_development_pool_retained import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_pool_retained import cloud as cloud
from tests.ops.test_nebius_development_pool_retained import completed_pool as completed_pool
from tests.ops.test_nebius_development_pool_retained import connected as connected
from tests.ops.test_nebius_development_pool_retained import database_runtime
from tests.ops.test_nebius_development_pool_retained import development_inputs as development_inputs
from tests.ops.test_nebius_development_pool_retained import entry as entry
from tests.ops.test_nebius_development_pool_retained import handoff as handoff
from tests.ops.test_nebius_development_pool_retained import installation as installation
from tests.ops.test_nebius_development_pool_retained import inventory as inventory
from tests.ops.test_nebius_development_pool_retained import live as live
from tests.ops.test_nebius_development_pool_retained import management_inputs as management_inputs
from tests.ops.test_nebius_development_pool_retained import manager_entry as manager_entry
from tests.ops.test_nebius_development_pool_retained import material as material
from tests.ops.test_nebius_development_pool_retained import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_retained import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_pool_retained import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_pool_retained import pool_entry as pool_entry
from tests.ops.test_nebius_development_pool_retained import pool_inputs as pool_inputs
from tests.ops.test_nebius_development_pool_retained import preflight as preflight
from tests.ops.test_nebius_development_pool_retained import provider_checks as provider_checks
from tests.ops.test_nebius_development_pool_retained import publication as publication
from tests.ops.test_nebius_development_pool_retained import published_source as published_source
from tests.ops.test_nebius_development_pool_retained import retained as retained
from tests.ops.test_nebius_development_pool_retained import route as route
from tests.ops.test_nebius_development_pool_retained import source_checkout as source_checkout
from tests.ops.test_nebius_development_pool_retained import tls_material as tls_material

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True, ids=['private-only'])
def test_runtime_database_exact_resources_use_native_defaults_and_replay(completed_pool, tmp_path, monkeypatch):
    from kubernetes import client
    from scripts.ops.nebius_development_runtime_database_live import (
        HTTPSDevelopmentRuntimeDatabaseAPI,
    )
    from scripts.ops.nebius_development_runtime_setup import database_runtime_documents
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import (
        HTTPSManagementStageAPI,
        _defaulted,
        _stage_fixed_documents,
    )
    from scripts.ops.nebius_management_supplied import _defaulted as secret_defaulted
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

    from loom.nebius_platform_render import digest

    class DisposableAPI(HTTPSDevelopmentRuntimeDatabaseAPI):
        # Isolate real API defaulting here; the original resource qualification
        # and completion reader are exercised by the connected HTTP regressions.
        def _private_inputs(self):
            pass

        def verify_identity(self, binding):
            HTTPSManagementStageAPI.verify_identity(self, binding)

        def __exit__(self, *args):
            ManagementKubernetesTransport.__exit__(self, *args)

    prepared = database_runtime(completed_pool)
    documents = database_runtime_documents(prepared)
    # The connected predecessor fixture uses an external API double. Only this
    # native allocation check replaces it with an authenticated disposable API.
    monkeypatch.setattr(httpx, 'Client', HTTPClient)
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
        for namespace in ('loom-dev', 'loom-nebius-dev-execution'):
            core.create_namespace({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': namespace, 'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
            core.create_namespaced_resource_quota(namespace, {'apiVersion': 'v1', 'kind': 'ResourceQuota',
                'metadata': {'name': 'no-fixture-execution'}, 'spec': {'hard': {'pods': '0'}}})
        api = DisposableAPI.__new__(DisposableAPI)
        api.binding, api.documents = binding, documents
        ManagementKubernetesTransport.__init__(api, api_server=endpoint, ssl_context=context)

        def default(api, document):
            return (secret_defaulted if document['kind'] == 'Secret' else _defaulted)(api, document)

        def stage():
            return _stage_fixed_documents(documents=documents, revision=digest(documents),
                phase='development-runtime-database', binding=binding, api=api,
                state_dir=tmp_path / 'stage', default_document=default)

        with api:
            receipt = stage()
            assert stage() == receipt
        name = 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f'
        shared = core.read_namespaced_secret(name, 'loom-dev')
        worker = core.read_namespaced_secret(name, 'loom-nebius-dev-execution')
        assert shared.immutable is worker.immutable is True
        assert set(shared.data) == {'actuator-password', 'batch-runner-token'}
        assert set(worker.data) == {'actuator-url', 'ca.crt'}
        job = client.BatchV1Api(core.api_client).read_namespaced_job(name, 'loom-dev')
        assert job.spec.backoff_limit == 0
        assert job.spec.template.spec.automount_service_account_token is False
        assert job.spec.template.spec.restart_policy == 'Never'
        assert job.spec.template.spec.containers[0].command == ['python', '-m', 'loom.nebius_development_runtime_database']
        assert core.list_namespaced_pod('loom-dev').items == []
        assert len(receipt['resource_uids']) == 4
    finally:
        cluster.stop()


@pytest.mark.timeout(180)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True, ids=['private-only'])
def test_stopped_actuator_effective_permissions_on_native_api(completed_pool):
    from kubernetes import client
    from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_management_stage import _qualified_defaulted

    prepared = database_runtime(completed_pool)
    runtime = prepare_actuator_runtime(prepared)
    participant, = prepared.manager.retained.request.registration.spec.participants
    execution, build = participant.execution_namespace.name, participant.build_namespace.name
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        apps = client.AppsV1Api(core.api_client)
        authorization = client.AuthorizationV1Api(core.api_client)
        for namespace in (execution, build, 'loom-staging'):
            core.create_namespace({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': namespace, 'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
            core.create_namespaced_resource_quota(namespace, {'apiVersion': 'v1', 'kind': 'ResourceQuota',
                'metadata': {'name': 'no-fixture-execution'}, 'spec': {'hard': {'pods': '0'}}})
        methods = {'ServiceAccount': core.create_namespaced_service_account,
            'Role': rbac.create_namespaced_role, 'RoleBinding': rbac.create_namespaced_role_binding,
            'ClusterRole': rbac.create_cluster_role, 'ClusterRoleBinding': rbac.create_cluster_role_binding}
        for document in runtime.authority:
            namespace = document['metadata'].get('namespace')
            if namespace:
                methods[document['kind']](namespace, document)
            else:
                methods[document['kind']](document)
        deployment = apps.create_namespaced_deployment(execution, runtime.deployment)
        actual = core.api_client.sanitize_for_serialization(deployment)
        _qualified_defaulted(runtime.deployment, _snapshot(actual))
        assert deployment.spec.replicas == 0
        assert core.list_namespaced_pod(execution).items == []
        identity = 'system:serviceaccount:' + execution + ':loom-execution-actuator'
        for namespace, group, resource, subresource, verb, name, allowed in (
            (execution, 'batch', 'jobs', None, 'get', 'example', True),
            (build, 'batch', 'jobs', None, 'get', 'example', True),
            (build, '', 'pods', 'log', 'get', 'example', True),
            (None, '', 'namespaces', None, 'get', execution, True),
            (None, '', 'nodes', 'stats', 'get', 'example', True),
            (None, '', 'nodes', 'proxy', 'get', 'example', False),
            (execution, 'batch', 'jobs', None, 'create', 'example', False),
            (build, 'batch', 'jobs', None, 'delete', 'example', False),
            (execution, '', 'secrets', None, 'get', 'example', False),
            ('loom-staging', 'batch', 'jobs', None, 'get', 'example', False),
            (None, '', 'namespaces', None, 'get', 'loom-staging', False),
        ):
            review = authorization.create_subject_access_review({'apiVersion': 'authorization.k8s.io/v1',
                'kind': 'SubjectAccessReview', 'spec': {'user': identity, 'resourceAttributes': {
                    'namespace': namespace, 'group': group, 'resource': resource,
                    'subresource': subresource, 'verb': verb, 'name': name}}})
            assert review.status.allowed is allowed, (namespace, resource, subresource, verb)
    finally:
        cluster.stop()
