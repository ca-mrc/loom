"""Build publication cannot inherit project-wide operator credentials."""
from __future__ import annotations

import hashlib
import importlib
from datetime import UTC, datetime

import pytest
from nebius.api.nebius.registry import v1 as registry
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud


def module():
    name = 'scripts.ops.nebius_development_build_cloud'
    if importlib.util.find_spec(name) is None:
        pytest.fail('registry-scoped native build credential qualification is missing')
    return importlib.import_module(name)


@pytest.fixture
def publisher_cloud(collector_cloud):
    cloud = collector_cloud
    cloud.scope.update(registry_id='registry-builds', registry_fqdn='cr.eu-north1.nebius.cloud/builds')
    cloud.rows['registry-builds'] = (registry.Registry, {'metadata': {
        'id': 'registry-builds', 'parent_id': 'project-children'},
        'status': {'state': 'ACTIVE', 'registry_fqdn': 'cr.eu-north1.nebius.cloud/builds'}})
    cloud.permits['group-manager'][0]['spec'] = {'resource_id': 'registry-builds', 'role': 'editor'}
    cloud.clients['registries'] = cloud.clients['accounts']
    cloud.repositories = ('cr.eu-north1.nebius.cloud/builds/task-images',
        'cr.eu-north1.nebius.cloud/builds/application-images')
    return cloud


async def qualify(cloud):
    api = module()
    return await api.qualify_registry_cloud(sdk=None,
        scope=api.DevelopmentRegistryCloudScope.model_validate(cloud.scope), config=cloud.config,
        credential=cloud.credential, repositories=cloud.repositories, clients=cloud.clients,
        now=datetime(2026, 10, 8, 12, tzinfo=UTC))


async def test_registry_editor_is_bound_to_both_catalog_repositories_without_project_writes(publisher_cloud):
    cloud = publisher_cloud
    proof = await qualify(cloud)
    assert proof == {'account_id': 'serviceaccount-manager', 'key_id': 'authpublickey-manager',
        'credential_sha256': hashlib.sha256(cloud.credential).hexdigest(), 'registry_id': 'registry-builds'}
    assert ('get', 'registry-builds') in cloud.calls


@pytest.mark.parametrize('damage', ['project-editor', 'registry-admin', 'extra-membership',
    'foreign-project', 'foreign-registry', 'foreign-region', 'prefix-confusion', 'traversal',
    'empty-repositories', 'suspended', 'wrong-fqdn', 'wrong-key'])
async def test_native_publisher_rejects_operator_scope_or_unbound_repository(publisher_cloud, damage):
    cloud = publisher_cloud
    permit = cloud.permits['group-manager'][0]['spec']
    row = cloud.rows['registry-builds'][1]
    if damage == 'project-editor':
        permit['resource_id'] = 'project-children'
    elif damage == 'registry-admin':
        permit['role'] = 'admin'
    elif damage == 'extra-membership':
        cloud.groups['serviceaccount-manager'].append('group-backup')
    elif damage == 'foreign-project':
        row['metadata']['parent_id'] = 'project-foreign'
    elif damage == 'foreign-registry':
        cloud.repositories = ('cr.eu-north1.nebius.cloud/foreign/task-images',)
    elif damage == 'foreign-region':
        cloud.scope['registry_fqdn'] = 'cr.us-central1.nebius.cloud/builds'
    elif damage == 'prefix-confusion':
        cloud.repositories = ('cr.eu-north1.nebius.cloud/buildsevil/task-images',)
    elif damage == 'traversal':
        cloud.repositories = ('cr.eu-north1.nebius.cloud/builds/../foreign/task-images',)
    elif damage == 'empty-repositories':
        cloud.repositories = ()
    elif damage == 'suspended':
        row['status']['state'] = 'SUSPENDED'
    elif damage == 'wrong-fqdn':
        row['status']['registry_fqdn'] = 'cr.eu-north1.nebius.cloud/foreign'
    else:
        cloud.rows['authpublickey-manager'][1]['spec']['account']['service_account']['id'] = 'serviceaccount-backup'
    with pytest.raises(ValueError, match='development build registry unqualified') as error:
        await qualify(cloud)
    assert 'PRIVATE KEY' not in str(error.value)
