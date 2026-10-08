"""Fresh dev capacity observation is catalog-bound and initially suspended."""
from __future__ import annotations

import importlib

import pytest
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
from tests.ops.test_nebius_development_pool_retained import pool_inputs as runtime_pool_inputs  # noqa: F401
from tests.ops.test_nebius_development_pool_retained import preflight as preflight
from tests.ops.test_nebius_development_pool_retained import provider_checks as provider_checks
from tests.ops.test_nebius_development_pool_retained import publication as publication
from tests.ops.test_nebius_development_pool_retained import published_source as published_source
from tests.ops.test_nebius_development_pool_retained import retained as retained
from tests.ops.test_nebius_development_pool_retained import route as route
from tests.ops.test_nebius_development_pool_retained import source_checkout as source_checkout
from tests.ops.test_nebius_development_pool_retained import tls_material as tls_material


@pytest.fixture
def quota_damage(request):
    return getattr(request, 'param', None)


@pytest.fixture
def pool_inputs(request, retained, quota_damage):
    reference, value, tokens = request.getfixturevalue('runtime_pool_inputs')
    config = retained[3].deployment.installation.foundation.platform_config
    for identity in value['quota_identities'].values():
        identity[0] = config['quota_parent_id']
    if quota_damage:
        index, replacement = {'parent': (0, 'tenant-foreign'), 'region': (1, 'eu-west1'),
            'service': (2, 'foreign-service')}[quota_damage]
        value['quota_identities']['vcpu'][index] = replacement
    return reference, value, tokens


def prepare(request):
    name = 'scripts.ops.nebius_development_collector_runtime'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fresh suspended development pool collector is missing')
    return importlib.import_module(name).prepare_collector_runtime(request)


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
def test_collector_binds_closed_pool_observer_and_ignores_ambient_authority(completed_pool, monkeypatch):
    request = database_runtime(completed_pool)
    monkeypatch.setenv('LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID', 'staging-group')
    monkeypatch.setenv('LOOM_EXECUTION_CAPACITY_COLLECTOR_KUBERNETES_ENDPOINT', 'https://staging.invalid')
    value = prepare(request)
    spec = request.manager.retained.request.registration.spec
    observer, = [row for row in spec.machines if row.role == 'observer']
    assert value.credential_secret_name == 'loom-dev-collector-aecc7407b7b84c388d1fbca5dca9840f'
    data = value.configuration['data']
    prefix = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_'
    assert data[prefix + 'COLLECTION_MODE'] == 'pool'
    assert data[prefix + 'POOL_ID'] == str(spec.pool_id)
    assert data[prefix + 'NEBIUS_PROJECT_ID'] == 'project-test'
    assert data[prefix + 'NEBIUS_QUOTA_PARENT_ID'] == 'tenant-test'
    assert data[prefix + 'NEBIUS_REGION'] == 'eu-north1'
    assert data[prefix + 'NEBIUS_NODE_GROUP_ID'] == request.foundation.inputs.config['execution_node_group_id']
    assert prefix + 'KUBERNETES_ENDPOINT' not in data
    assert data[prefix + 'MANAGEMENT_URL'] == 'https://' + request.manager.deployment.public_host
    for kind, identity in spec.quota_identities.items():
        assert data[prefix + 'QUOTA_' + kind.upper() + '_NAME'] == identity[3]
        assert data[prefix + 'QUOTA_' + kind.upper() + '_UNIT'] == identity[4]
    assert value.configuration['immutable'] is True
    cron = value.cronjob
    assert cron['spec']['suspend'] is True
    assert cron['spec']['concurrencyPolicy'] == 'Forbid'
    assert 'uid' not in cron['metadata']
    assert cron['metadata']['namespace'] == 'loom-nebius-dev-execution'
    pod = cron['spec']['jobTemplate']['spec']['template']['spec']
    assert pod['serviceAccountName'] == 'loom-execution-capacity-collector'
    assert pod['nodeSelector'] == {'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}
    container, = pod['containers']
    initializer, = pod['initContainers']
    expected_image = request.manager.publication.bundle.candidate['images']['execution_actuator']['image_ref']
    assert container['image'] == initializer['image'] == expected_image
    assert container['command'] == ['python', '-m', 'loom_execution_capacity_collector']
    assert container['envFrom'] == [{'configMapRef': {'name': value.configuration['metadata']['name']}}]
    files = {item['name']: item['value'] for item in container['env']}
    assert files == {prefix + 'NEBIUS_CREDENTIALS_FILE': '/var/run/loom-owned/credentials/nebius-credentials.json',
        prefix + 'MANAGEMENT_BEARER_TOKEN_FILE': '/var/run/loom-owned/credentials/control-plane-token'}
    projected, = [row for row in pod['volumes'] if row['name'] == 'projected-credentials']
    assert projected['projected']['sources'] == [
        {'secret': {'name': value.credential_secret_name,
            'items': [{'key': 'credentials.json', 'path': 'nebius-credentials.json'}]}},
        {'secret': {'name': 'loom-pool-machine-' + observer.machine_id.hex,
            'items': [{'key': 'token', 'path': 'control-plane-token'}]}}]
    roles = [row for row in value.authority if row['kind'] == 'ClusterRole']
    assert len(roles) == 1
    assert roles[0]['rules'] == [
        {'apiGroups': [''], 'resources': ['nodes', 'pods'], 'verbs': ['get', 'list']},
        {'apiGroups': ['apps'], 'resources': ['daemonsets'], 'verbs': ['get', 'list']}]
    assert {row['kind'] for row in value.authority} == {'ServiceAccount', 'ClusterRole', 'ClusterRoleBinding'}
    binding, = [row for row in value.authority if row['kind'] == 'ClusterRoleBinding']
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-execution-capacity-collector',
        'namespace': 'loom-nebius-dev-execution'}]
    assert binding['roleRef']['name'] == roles[0]['metadata']['name']


@pytest.mark.parametrize('manager_entry', ['foundation-runtime'], indirect=True)
@pytest.mark.parametrize('retained', [False], indirect=True)
@pytest.mark.parametrize('quota_damage', ['parent', 'region', 'service'], indirect=True)
def test_collector_rejects_foreign_or_inconsistent_quota_scope(completed_pool, quota_damage):
    with pytest.raises(ValueError, match='development collector runtime unqualified'):
        prepare(database_runtime(completed_pool))
