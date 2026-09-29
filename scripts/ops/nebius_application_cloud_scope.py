"""Qualify application IAM against existing development groups without writes.

Nebius group-scoped access permits also cover memberships. A tenant-owned group
can grant this narrow cross-project authority; provisioning-project admin alone
cannot. No grant here permits managing the shared foundation project or buckets.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Self

from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_management_cloud_scope import (
    ManagementCloudScopeError,
    _active_key,
    _pages,
    _ProviderId,
    _read,
    _require,
    _resource,
)

from loom_service.environment_management.candidates import _json


class ApplicationCloudScope(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r'^[a-z]+-[a-z]+[0-9]+$')
    provisioning_project_id: _ProviderId
    provisioning_account_id: _ProviderId
    provisioning_group_id: _ProviderId
    provisioning_key_id: _ProviderId
    shared_project_id: _ProviderId
    membership_group_id: _ProviderId
    data_group_id: _ProviderId
    source_group_id: _ProviderId

    @model_validator(mode='after')
    def separate(self) -> Self:
        if (self.shared_project_id == self.provisioning_project_id or len({self.provisioning_group_id,
                self.membership_group_id, self.data_group_id, self.source_group_id}) != 4):
            raise ValueError('application cloud authorities must be distinct')
        return self


async def qualify_application_cloud(*, sdk: Any, scope: ApplicationCloudScope, credentials_json: str,
        data_buckets: dict[str, str], source_bucket: tuple[str, str],
        clients: dict[str, Any] | None = None, now: datetime | None = None) -> None:
    """Read-only exact account/key, grants and shared object-policy qualification."""
    from nebius.api.nebius.iam import v1
    from nebius.api.nebius.storage import v1 as storage
    from nebius.base.service_account.credentials_file import ServiceAccountCredentials

    try:
        async with asyncio.timeout(120):
            api: dict[str, Any] = clients if clients is not None else {
                'projects': v1.ProjectServiceClient(sdk), 'accounts': v1.ServiceAccountServiceClient(sdk),
                'groups': v1.GroupServiceClient(sdk), 'memberships': v1.GroupMembershipServiceClient(sdk),
                'permits': v1.AccessPermitServiceClient(sdk), 'public_keys': v1.AuthPublicKeyServiceClient(sdk),
                'buckets': storage.BucketServiceClient(sdk)}
            subject = ServiceAccountCredentials.from_json(_json(credentials_json.encode())).subject_credentials
            _require(subject.sub == scope.provisioning_account_id and subject.kid == scope.provisioning_key_id)
            for identity in (scope.provisioning_project_id, scope.shared_project_id):
                project = await _read(api['projects'].get, v1.GetProjectRequest(id=identity))
                _resource(project, identity, scope.tenant_id)
                _require(project['status']['container_state'] == 'ACTIVE'
                    and project['status']['suspension_state'] == 'NONE'
                    and project['spec']['region'] == project['status']['region'] == scope.region)
            account = await _read(api['accounts'].get, v1.GetServiceAccountRequest(id=subject.sub))
            _resource(account, scope.provisioning_account_id, scope.provisioning_project_id)
            _require(account['status']['active'] is True)
            memberships = await _pages(api['memberships'].list_member_of, v1.ListMemberOfRequest, subject_id=subject.sub)
            _require({row['metadata']['id'] for row in memberships} == {scope.provisioning_group_id, scope.membership_group_id})
            expected_groups = {
                scope.provisioning_group_id: ({scope.provisioning_project_id}, {scope.provisioning_project_id}),
                scope.membership_group_id: ({scope.tenant_id}, {scope.data_group_id, scope.source_group_id}),
                scope.data_group_id: ({scope.shared_project_id, scope.tenant_id}, set()),
                scope.source_group_id: ({scope.shared_project_id, scope.tenant_id}, set())}
            for identity, (parents, resources) in expected_groups.items():
                group = await _read(api['groups'].get, v1.GetGroupRequest(id=identity))
                parent = group['metadata']['parent_id']
                _require(parent in parents)
                _resource(group, identity, parent)
                for membership in memberships:
                    if membership['metadata']['id'] == identity:
                        # ListMemberOf omits Group.Get status/counters. Compare
                        # authority identity, not these unequal API projections.
                        _resource(membership, identity, parent)
                permits = await _pages(api['permits'].list, v1.ListAccessPermitRequest, parent_id=identity)
                _require(len(permits) == len(resources))
                observed = set()
                for permit in permits:
                    _require(permit['metadata']['parent_id'] == identity and not permit['metadata'].get('deletion_timestamp')
                        and set(permit['spec']) == {'resource_id', 'role'} and permit['spec']['role'] == 'admin')
                    observed.add(permit['spec']['resource_id'])
                _require(observed == resources)
            key = await _read(api['public_keys'].get, v1.GetAuthPublicKeyRequest(id=subject.kid))
            _resource(key, scope.provisioning_key_id, scope.provisioning_project_id)
            _active_key(key, account=scope.provisioning_account_id, now=now or datetime.now(UTC))
            registered = serialization.load_pem_public_key(key['spec']['data'].encode()).public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            actual = subject.parse_private_key().public_key().public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            _require(registered == actual)
            source_id, source_name = source_bucket
            _require(bool(data_buckets) and len(data_buckets) <= 2 and source_id not in data_buckets
                and len(set(data_buckets.values()) | {source_name}) == len(data_buckets) + 1)
            expected = {**{identity: (name, scope.data_group_id) for identity, name in data_buckets.items()},
                source_id: (source_name, scope.source_group_id)}
            buckets = await _pages(api['buckets'].list, storage.ListBucketsRequest, parent_id=scope.shared_project_id)
            _require(set(expected) <= {row['metadata']['id'] for row in buckets})
            for bucket in buckets:
                identity = bucket['metadata']['id']
                _resource(bucket, identity, scope.shared_project_id)
                rules = bucket.get('spec', {}).get('bucket_policy', {}).get('rules', [])
                shared_rules = [rule for rule in rules if rule.get('group_id') in {scope.data_group_id, scope.source_group_id}]
                if identity not in expected:
                    _require(not shared_rules)
                    continue
                name, group_id = expected[identity]
                _require(bucket['metadata']['name'] == name and bucket['status']['state'] == 'ACTIVE'
                    and bucket['status']['suspension_state'] == 'NOT_SUSPENDED' and bucket['status']['region'] == scope.region
                    and not bucket['status'].get('anonymous_access_enabled')
                    and not bucket['status'].get('deleted_at') and not bucket['status'].get('purge_at')
                    and shared_rules == [{'group_id': group_id, 'paths': ['*'], 'roles': ['storage.object-editor']}])
    except Exception:
        raise ManagementCloudScopeError('application cloud authority unqualified') from None
