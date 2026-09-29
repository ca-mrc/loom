"""Application credentials may manage two shared memberships, not their project."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from nebius.api.nebius.iam import v1
from nebius.api.nebius.storage import v1 as storage
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud


@pytest.fixture
def application_cloud(cloud):
    scope = {key: value for key, value in cloud.scope.items() if not key.startswith('backup_')}
    scope.update(shared_project_id='project-shared', membership_group_id='group-membership',
        data_group_id='group-data', source_group_id='group-source')
    project = copy.deepcopy(cloud.rows['project-children'][1])
    project['metadata']['id'] = 'project-shared'
    cloud.rows['project-shared'] = v1.Container, project
    for group, parent in (('membership', 'tenant-test'), ('data', 'project-shared'), ('source', 'project-shared')):
        cloud.rows['group-' + group] = v1.Group, {'metadata': {'id': 'group-' + group, 'parent_id': parent}}
        cloud.permits['group-' + group] = []
    cloud.groups['serviceaccount-manager'].append('group-membership')
    cloud.permits['group-membership'] = [
        {'metadata': {'id': 'permit-' + group, 'parent_id': 'group-membership'},
         'spec': {'resource_id': 'group-' + group, 'role': 'admin'}} for group in ('data', 'source')]
    cloud.clients['groups'] = SimpleNamespace(get=cloud.clients['accounts'].get)
    buckets = {}
    for name, group in (('artifacts', 'data'), ('trajectories', 'data'), ('source', 'source')):
        identity = 'bucket-' + name
        buckets[identity] = 'shared-' + name
        cloud.rows[identity] = storage.Bucket, {
            'metadata': {'id': identity, 'parent_id': 'project-shared', 'name': buckets[identity]},
            'spec': {'bucket_policy': {'rules': [{'group_id': 'group-' + group,
                'paths': ['*'], 'roles': ['storage.object-editor']}]},
                'versioning_policy': 'DISABLED' if name == 'source' else 'ENABLED'},
            'status': {'state': 'ACTIVE', 'suspension_state': 'NOT_SUSPENDED', 'region': 'eu-north1'}}
    async def listing(request, **kwargs):
        assert request.parent_id == 'project-shared' and not request.page_token
        assert kwargs == {'timeout': 30, 'retries': 0}
        return storage.ListBucketsResponse.from_json(json.dumps({'items': [
            row for cls, row in cloud.rows.values() if cls == storage.Bucket and row['metadata']['parent_id'] == 'project-shared']}))
    cloud.clients['buckets'].list = listing
    return cloud, scope, buckets


async def qualify(fixture):
    from scripts.ops.nebius_application_cloud_scope import (
        ApplicationCloudScope,
        qualify_application_cloud,
    )

    cloud, scope, buckets = fixture
    return await qualify_application_cloud(sdk=None, scope=ApplicationCloudScope.model_validate(scope),
        credentials_json=cloud.material['loom-management-cloud']['credentials.json'],
        data_buckets={key: value for key, value in buckets.items() if key != 'bucket-source'},
        source_bucket=('bucket-source', 'shared-source'), clients=cloud.clients,
        now=datetime(2026, 9, 28, tzinfo=UTC))


async def test_application_scope_qualifies_cross_project_group_authority_without_writes(application_cloud):
    await qualify(application_cloud)
    cloud = application_cloud[0]
    assert ('get', 'group-data') in cloud.calls and ('get', 'group-source') in cloud.calls
    assert ('member_of', 'serviceaccount-manager') in cloud.calls
    assert all(call[0] in {'get', 'member_of', 'permits'} for call in cloud.calls)


@pytest.mark.parametrize('groups', [('data',), ('source',), ('data', 'source')])
async def test_shared_object_groups_may_belong_to_the_same_tenant(application_cloud, groups):
    cloud, _, _ = application_cloud
    for group in groups:
        cloud.rows['group-' + group][1]['metadata']['parent_id'] = 'tenant-test'
    await qualify(application_cloud)
    assert all(call[0] in {'get', 'member_of', 'permits'} for call in cloud.calls)


@pytest.mark.parametrize('mutation', ['missing_membership', 'foundation_admin', 'tenant_admin', 'wrong_group_scope',
    'extra_membership', 'wrong_key', 'inactive', 'foreign_group', 'foreign_tenant', 'group_project_permit', 'missing_bucket',
    'extra_bucket', 'wrong_bucket_parent', 'public_bucket', 'wrong_policy', 'missing_group_permit'])
async def test_unqualified_shared_authority_fails_without_provider_or_secret_diagnostics(application_cloud, mutation):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    cloud, scope, _ = application_cloud
    if mutation == 'missing_membership':
        cloud.groups['serviceaccount-manager'].remove('group-membership')
    elif mutation in {'foundation_admin', 'tenant_admin'}:
        cloud.permits['group-membership'][0]['spec']['resource_id'] = (
            'project-shared' if mutation == 'foundation_admin' else 'tenant-test')
    elif mutation == 'wrong_group_scope':
        cloud.rows['group-membership'][1]['metadata']['parent_id'] = 'project-children'
    elif mutation == 'extra_membership':
        cloud.groups['serviceaccount-manager'].append('group-data')
    elif mutation == 'wrong_key':
        scope['provisioning_key_id'] = 'another-key'
    elif mutation == 'inactive':
        cloud.rows['serviceaccount-manager'][1]['status']['active'] = False
    elif mutation == 'foreign_group':
        cloud.rows['group-data'][1]['metadata']['parent_id'] = 'project-other'
    elif mutation == 'foreign_tenant':
        cloud.rows['group-data'][1]['metadata']['parent_id'] = 'tenant-other'
    elif mutation == 'group_project_permit':
        cloud.permits['group-data'] = [copy.deepcopy(cloud.permits['group-manager'][0])]
    elif mutation == 'missing_bucket':
        del cloud.rows['bucket-artifacts']
    elif mutation == 'extra_bucket':
        extra = copy.deepcopy(cloud.rows['bucket-artifacts'][1])
        extra['metadata']['id'] = 'bucket-other'
        cloud.rows['bucket-other'] = storage.Bucket, extra
    elif mutation == 'wrong_bucket_parent':
        cloud.rows['bucket-artifacts'][1]['metadata']['parent_id'] = 'project-other'
    elif mutation == 'public_bucket':
        cloud.rows['bucket-artifacts'][1]['status']['anonymous_access_enabled'] = True
    elif mutation == 'wrong_policy':
        cloud.rows['bucket-artifacts'][1]['spec']['bucket_policy']['rules'][0]['roles'] = ['admin']
    else:
        cloud.permits['group-membership'].pop()
    with pytest.raises(ManagementCloudScopeError) as error:
        await qualify(application_cloud)
    assert 'PRIVATE KEY' not in str(error.value) and 'never-print' not in str(error.value)
