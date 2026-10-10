"""Actual suspended dev collector defaults and effective read-only permissions."""
from __future__ import annotations

import os

import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_development_collector_runtime import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_collector_runtime import build_inputs as build_inputs
from tests.ops.test_nebius_development_collector_runtime import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_collector_runtime import cloud as cloud
from tests.ops.test_nebius_development_collector_runtime import completed_pool as completed_pool
from tests.ops.test_nebius_development_collector_runtime import connected as connected
from tests.ops.test_nebius_development_collector_runtime import database_runtime
from tests.ops.test_nebius_development_collector_runtime import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import entry as entry
from tests.ops.test_nebius_development_collector_runtime import handoff as handoff
from tests.ops.test_nebius_development_collector_runtime import installation as installation
from tests.ops.test_nebius_development_collector_runtime import inventory as inventory
from tests.ops.test_nebius_development_collector_runtime import live as live
from tests.ops.test_nebius_development_collector_runtime import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import manager_entry as manager_entry
from tests.ops.test_nebius_development_collector_runtime import material as material
from tests.ops.test_nebius_development_collector_runtime import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_collector_runtime import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_collector_runtime import pool_entry as pool_entry
from tests.ops.test_nebius_development_collector_runtime import pool_inputs as pool_inputs
from tests.ops.test_nebius_development_collector_runtime import preflight as preflight
from tests.ops.test_nebius_development_collector_runtime import provider_checks as provider_checks
from tests.ops.test_nebius_development_collector_runtime import publication as publication
from tests.ops.test_nebius_development_collector_runtime import published_source as published_source
from tests.ops.test_nebius_development_collector_runtime import quota_damage as quota_damage
from tests.ops.test_nebius_development_collector_runtime import retained as retained
from tests.ops.test_nebius_development_collector_runtime import route as route
from tests.ops.test_nebius_development_collector_runtime import (
    runtime_pool_inputs as runtime_pool_inputs,
)
from tests.ops.test_nebius_development_collector_runtime import source_checkout as source_checkout
from tests.ops.test_nebius_development_collector_runtime import tls_material as tls_material

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_suspended_collector_has_only_effective_observation_authority(completed_pool):
    from kubernetes import client
    from scripts.ops.nebius_development_collector_runtime import prepare_collector_runtime
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_management_stage import _qualified_defaulted

    runtime = prepare_collector_runtime(database_runtime(completed_pool))
    namespace = 'loom-nebius-dev-execution'
    cluster = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(cluster)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        batch = client.BatchV1Api(core.api_client)
        authorization = client.AuthorizationV1Api(core.api_client)
        core.create_namespace({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': namespace, 'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}})
        core.create_namespaced_resource_quota(namespace, {'apiVersion': 'v1', 'kind': 'ResourceQuota',
            'metadata': {'name': 'no-fixture-execution'}, 'spec': {'hard': {'pods': '0'}}})
        methods = {'ServiceAccount': core.create_namespaced_service_account,
            'ClusterRole': rbac.create_cluster_role, 'ClusterRoleBinding': rbac.create_cluster_role_binding}
        for document in runtime.authority:
            if document['metadata'].get('namespace'):
                methods[document['kind']](namespace, document)
            else:
                methods[document['kind']](document)
        core.create_namespaced_config_map(namespace, runtime.configuration)
        cron = batch.create_namespaced_cron_job(namespace, runtime.cronjob)
        actual = core.api_client.sanitize_for_serialization(cron)
        _qualified_defaulted(runtime.cronjob, _snapshot(actual))
        assert cron.spec.suspend is True and cron.spec.concurrency_policy == 'Forbid'
        assert batch.list_namespaced_job(namespace).items == []
        assert core.list_namespaced_pod(namespace).items == []
        identity = 'system:serviceaccount:' + namespace + ':loom-execution-capacity-collector'
        for group, resource, subresource, verb, allowed in (
            ('', 'nodes', None, 'list', True), ('', 'pods', None, 'list', True),
            ('apps', 'daemonsets', None, 'list', True), ('', 'nodes', None, 'patch', False),
            ('', 'nodes', 'proxy', 'get', False), ('', 'secrets', None, 'get', False),
            ('', 'pods', None, 'create', False), ('batch', 'jobs', None, 'create', False),
        ):
            review = authorization.create_subject_access_review({'apiVersion': 'authorization.k8s.io/v1',
                'kind': 'SubjectAccessReview', 'spec': {'user': identity, 'resourceAttributes': {
                    'group': group, 'resource': resource, 'subresource': subresource, 'verb': verb}}})
            assert review.status.allowed is allowed, (group, resource, subresource, verb)
    finally:
        cluster.stop()
