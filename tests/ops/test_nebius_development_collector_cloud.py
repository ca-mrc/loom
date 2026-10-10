"""Collector qualification uses real credentials/SDK shapes and read-only RPCs."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from datetime import UTC, datetime

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from tests.ops.test_nebius_management_cloud_scope import cloud as original_cloud  # noqa: F401


def module():
    name = 'scripts.ops.nebius_development_collector_cloud'
    if importlib.util.find_spec(name) is None:
        pytest.fail('collector-only cloud credential qualification is missing')
    return importlib.import_module(name)


@pytest.fixture
def collector_cloud(request):
    cloud = request.getfixturevalue('original_cloud')
    # Keep the existing real RSA/SDK fixture; change only its authority from
    # project administration to the Terraform collector's tenant viewer role.
    cloud.scope = {'tenant_id': 'tenant-test', 'region': 'eu-north1', 'project_id': 'project-children',
        'account_id': 'serviceaccount-manager', 'group_id': 'group-manager', 'key_id': 'authpublickey-manager'}
    cloud.config = {'namespace': 'loom-dev', 'environment': 'development', 'project_id': 'project-children',
        'quota_parent_id': 'tenant-test', 'region': 'eu-north1'}
    cloud.rows['group-manager'][1]['metadata']['parent_id'] = 'tenant-test'
    cloud.clients['groups'] = cloud.clients['accounts']
    cloud.permits['group-manager'][0]['spec'] = {'resource_id': 'tenant-test', 'role': 'viewer'}
    cloud.credential = cloud.material['loom-management-cloud']['credentials.json'].encode()
    return cloud


async def qualify(cloud):
    runtime = module()
    return await runtime.qualify_collector_cloud(sdk=None,
        scope=runtime.DevelopmentCollectorCloudScope.model_validate(cloud.scope),
        config=cloud.config, credential=cloud.credential, clients=cloud.clients,
        now=datetime(2026, 10, 8, 12, tzinfo=UTC))


@pytest.mark.parametrize('expiry', [True, False])
async def test_only_tenant_viewer_with_matching_active_key_is_qualified(collector_cloud, expiry):
    cloud = collector_cloud
    if not expiry:
        cloud.rows['authpublickey-manager'][1]['spec'].pop('expires_at')
    proof = await qualify(cloud)
    assert proof == {'account_id': 'serviceaccount-manager', 'key_id': 'authpublickey-manager',
        'credential_sha256': hashlib.sha256(cloud.credential).hexdigest()}
    assert {identity for _, identity in cloud.calls} == {
        'project-children', 'serviceaccount-manager', 'group-manager', 'authpublickey-manager'}
    assert 'PRIVATE KEY' not in json.dumps(proof)


@pytest.mark.parametrize('damage', ['staging', 'foreign-project', 'foreign-tenant', 'wrong-region',
    'inactive-project', 'inactive-account', 'extra-membership', 'group-parent', 'editor',
    'extra-permit', 'wrong-permit-parent', 'wrong-key-account', 'revoked-key', 'expiring-key',
    'wrong-public-key', 'wrong-credential-subject', 'unknown-credential-field', 'external-private-key',
    'deleting-group', 'provider-error'])
async def test_collector_rejects_broader_or_unbound_cloud_authority(collector_cloud, damage):
    cloud = collector_cloud
    project = cloud.rows['project-children'][1]
    group = cloud.rows['group-manager'][1]
    key = cloud.rows['authpublickey-manager'][1]
    permit = cloud.permits['group-manager'][0]
    if damage == 'staging':
        cloud.config.update(namespace='loom-nebius-platform', environment='staging')
    elif damage == 'foreign-project':
        cloud.config['project_id'] = 'project-foreign'
    elif damage == 'foreign-tenant':
        cloud.config['quota_parent_id'] = 'tenant-foreign'
    elif damage == 'wrong-region':
        project['status']['region'] = 'us-central1'
    elif damage == 'inactive-project':
        project['status']['suspension_state'] = 'SUSPENDED'
    elif damage == 'inactive-account':
        cloud.rows['serviceaccount-manager'][1]['status']['active'] = False
    elif damage == 'extra-membership':
        cloud.groups['serviceaccount-manager'].append('group-backup')
    elif damage == 'group-parent':
        group['metadata']['parent_id'] = 'project-children'
    elif damage == 'editor':
        permit['spec']['role'] = 'editor'
    elif damage == 'extra-permit':
        extra = copy.deepcopy(permit)
        extra['metadata']['id'] = 'permit-editor'
        extra['spec'] = {'resource_id': 'project-children', 'role': 'editor'}
        cloud.permits['group-manager'].append(extra)
    elif damage == 'wrong-permit-parent':
        permit['metadata']['parent_id'] = 'group-foreign'
    elif damage == 'wrong-key-account':
        key['spec']['account']['service_account']['id'] = 'serviceaccount-backup'
    elif damage == 'revoked-key':
        key['status']['state'] = 'INACTIVE'
    elif damage == 'expiring-key':
        key['spec']['expires_at'] = '2026-10-08T12:05:00Z'
    elif damage == 'wrong-public-key':
        key['spec']['data'] = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    elif damage in {'wrong-credential-subject', 'unknown-credential-field', 'external-private-key'}:
        value = json.loads(cloud.credential)
        if damage == 'wrong-credential-subject':
            value['subject-credentials'].update(iss='serviceaccount-backup', sub='serviceaccount-backup')
        elif damage == 'unknown-credential-field':
            value['operator-token'] = 'never-print-test-token'
        else:
            value['subject-credentials']['private-key'] = '/private/operator/key.pem'
        cloud.credential = json.dumps(value).encode()
    elif damage == 'deleting-group':
        group['metadata']['deletion_timestamp'] = '2026-10-08T11:59:00Z'
    else:
        async def fail(*args, **kwargs):
            raise RuntimeError('never-print-test-token PRIVATE KEY')
        cloud.clients['projects'].get = fail
    with pytest.raises(ValueError, match='development collector cloud unqualified') as error:
        await qualify(cloud)
    assert 'never-print' not in str(error.value) and 'PRIVATE KEY' not in str(error.value)
