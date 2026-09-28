"""Protected setup connects shared access without rendering another data stack."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def render_setup(inputs):
    from loom_service.application_management.deployment import render_application_setup
    from loom_service.environment_management.deployment import ManagementDeployment

    deployment, candidate, profile = inputs
    return render_application_setup(ManagementDeployment.model_validate(deployment),
        candidate=candidate, profile=profile, repo_root=ROOT)


def test_fixed_sql_job_uses_shared_admin_route_and_only_setup_credentials(application_management_inputs):
    before = copy.deepcopy(application_management_inputs)
    phases = render_setup(application_management_inputs)
    assert set(phases) == {'admission', 'permissions', 'network', 'database', 'retirement'}
    docs = phases['database']
    assert [doc['kind'] for doc in docs] == ['ConfigMap', 'Job']
    config, job = docs
    shared = before[0]['installation']['applications']['shared']
    assert job['metadata']['namespace'] == config['metadata']['namespace'] == shared['platform_namespace']
    assert config['immutable'] is True
    assert json.loads(config['data']['setup.json']) == {'namespace': shared['platform_namespace'],
        'data_environment_id': shared['data_environment_id'], 'schema_revision': '0159'}
    spec = job['spec']
    assert spec['backoffLimit'] == 0 and spec['activeDeadlineSeconds'] == 600
    assert 'ttlSecondsAfterFinished' not in spec
    pod = spec['template']['spec']
    assert pod['restartPolicy'] == 'Never' and pod['automountServiceAccountToken'] is False
    assert pod['serviceAccountName'] == 'loom-platform' and not pod.get('initContainers')
    assert len(pod['containers']) == 1
    container = pod['containers'][0]
    assert container['image'] == before[1]['images']['service']['image_ref']
    assert container['command'] == ['python', '-m', 'loom.nebius_application_database_install']
    env = {row['name']: row for row in container['env']}
    assert set(env) == {'LOOM_APPLICATION_SETUP_CONFIG', 'LOOM_DB_URL', 'LOOM_APPLICATION_MANAGER_PASSWORD'}
    assert env['LOOM_APPLICATION_SETUP_CONFIG']['value'] == '/var/run/loom-application-setup/setup.json'
    assert env['LOOM_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'admin-url'}
    suffix = job['metadata']['name'].removeprefix('loom-applications-setup-')
    assert len(suffix) == 12
    assert env['LOOM_APPLICATION_MANAGER_PASSWORD']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-applications-manager-' + suffix, 'key': 'password'}
    volumes = {volume['name']: volume for volume in pod['volumes']}
    assert set(volumes) == {'application-setup', 'db-ca'}
    assert volumes['db-ca']['secret']['secretName'] == 'loom-platform-db'
    assert volumes['db-ca']['secret']['items'] == [{'key': 'ca.crt', 'path': 'ca.crt'}]
    assert volumes['application-setup']['configMap']['name'] == config['metadata']['name']
    assert container['securityContext']['allowPrivilegeEscalation'] is False
    assert container['securityContext']['capabilities']['drop'] == ['ALL']
    assert container['resources']['requests'] == {'cpu': '100m', 'memory': '256Mi', 'ephemeral-storage': '256Mi'}
    assert all(doc['kind'] not in {'Secret', 'Namespace', 'PersistentVolumeClaim', 'StatefulSet'}
               for phase in phases.values() for doc in phase)
    assert application_management_inputs == before


def test_admission_can_be_qualified_before_bootstrap_grant(application_management_inputs):
    phases = render_setup(application_management_inputs)
    assert {doc['kind'] for doc in phases['admission']} == {
        'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding', 'ClusterRole'}
    assert {doc['kind'] for doc in phases['permissions']} == {'ClusterRoleBinding', 'Role', 'RoleBinding'}
    for doc in phases['permissions']:
        if 'subjects' in doc:
            assert doc['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-application-provisioner',
                'namespace': 'loom-nebius-management'}]


def test_management_sql_network_does_not_expand_personal_service_access(application_management_inputs):
    policies = render_setup(application_management_inputs)['network']
    assert len(policies) == 4
    management = next(doc for doc in policies if doc['metadata']['name'].endswith('-manager-postgres'))
    assert management['spec'] == {'podSelector': {'matchLabels': {'app': 'loom-postgres'}},
        'policyTypes': ['Ingress'], 'ingress': [{'from': [{
            'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'loom-nebius-management',
                'loom.nebius/management-installation': '30000000-0000-4000-8000-000000000001'}},
            'podSelector': {'matchLabels': {'app': 'loom-service'}}}],
            'ports': [{'protocol': 'TCP', 'port': 5432}]}]}
    for purpose, workload, port in [('postgres', 'loom-postgres', 5432), ('control-plane', 'loom-control-plane', 8080),
                                    ('gateway', 'loom-llm-gateway', 9100)]:
        doc = next(doc for doc in policies if doc is not management and doc['metadata']['name'].endswith('-' + purpose))
        assert doc['spec']['podSelector'] == {'matchLabels': {'app': workload}}
        rule = doc['spec']['ingress'][0]
        assert rule['ports'] == [{'protocol': 'TCP', 'port': port}]
        assert len(rule['from']) == 1
        assert {row['key'] for row in rule['from'][0]['namespaceSelector']['matchExpressions']} == {
            'loom.nebius/application-id', 'loom.nebius/incarnation'}


def test_setup_rejects_legacy_runtime_without_application_binding(management_inputs):
    with pytest.raises(ValueError, match='application'):
        render_setup(management_inputs)


def test_retirement_fence_blocks_only_legacy_management_pod_creation(application_management_inputs):
    policy, binding = render_setup(application_management_inputs)['retirement']
    assert policy['kind'] == 'ValidatingAdmissionPolicy'
    assert binding['kind'] == 'ValidatingAdmissionPolicyBinding'
    assert binding['spec']['policyName'] == policy['metadata']['name']
    assert binding['spec']['validationActions'] == ['Deny']
    assert policy['spec']['failurePolicy'] == 'Fail'
    assert policy['spec']['matchConstraints']['resourceRules'] == [
        {'operations': ['CREATE'], 'apiGroups': [''], 'apiVersions': ['v1'], 'resources': ['pods']}]
    assert policy['spec']['validations'] == [{'expression':
        "request.namespace != 'loom-nebius-management' || !has(object.spec.serviceAccountName) || "
        "object.spec.serviceAccountName != 'loom-management-provisioner'",
        'message': 'legacy management process is retired', 'reason': 'Forbidden'}]
