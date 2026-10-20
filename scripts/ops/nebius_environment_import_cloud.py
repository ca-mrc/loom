"""Exact read-only cloud qualification for an installed legacy environment."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_management_cloud_scope import (
    _active_key,
    _pages,
    _ProviderId,
    _read,
    _require,
    _resource,
)


class ImportedCloudSubjectV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    account_id: _ProviderId
    group_id: _ProviderId
    key_id: _ProviderId
    buckets: dict[_ProviderId, str] = Field(min_length=1, max_length=2)


class ImportedCloudScopeV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r'^[a-z]+-[a-z]+[0-9]+$')
    compute_project_id: _ProviderId
    object_project_id: _ProviderId
    database_disk_id: _ProviderId
    subjects: dict[Literal['canonical', 'source', 'backup'], ImportedCloudSubjectV2]

    @model_validator(mode='after')
    def separate(self) -> Self:
        if set(self.subjects) not in ({'canonical', 'source'}, {'canonical', 'source', 'backup'}):
            raise ValueError('environment_import_cloud_subjects_invalid')
        for field in ('account_id', 'group_id', 'key_id'):
            if len({getattr(row, field) for row in self.subjects.values()}) != len(self.subjects):
                raise ValueError('environment_import_cloud_subjects_invalid')
        ids = [identity for row in self.subjects.values() for identity in row.buckets]
        names = [name for row in self.subjects.values() for name in row.buckets.values()]
        if len(set(ids)) != len(ids) or len(set(names)) != len(names):
            raise ValueError('environment_import_cloud_buckets_invalid')
        return self


async def qualify_imported_cloud(*, sdk: Any, scope: ImportedCloudScopeV2, config: dict[str, Any],
        material: dict[str, str], clients: dict[str, Any] | None = None) -> None:
    """Authenticate exact subjects, projects, bucket policies and retained keys.

    Import grants no create/delete rights and does not reuse provisioning's
    name-based lookups. Enumerating all bucket policies detects credentials that
    unexpectedly gained access to unrelated data after the installed snapshot.
    """
    from nebius.api.nebius.compute import v1 as compute
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.storage import v1 as storage

    try:
        scope = ImportedCloudScopeV2.model_validate(scope.model_dump())
        _require(config['project_id'] == scope.compute_project_id and config['quota_parent_id'] == scope.tenant_id
            and config['region'] == scope.region)
        keys: set[str] = set()
        for purpose, subject in scope.subjects.items():
            expected = ({config['buckets'][name] for name in ('artifacts', 'trajectories')} if purpose == 'canonical'
                else {config['buckets'][purpose]})
            _require(set(subject.buckets.values()) == expected)
            prefix = '' if purpose == 'canonical' else purpose + '-'
            keys.update((prefix + 'access-key', prefix + 'secret-key'))
        _require(set(material) == keys and all(isinstance(value, str) and 0 < len(value.encode()) <= 65536
            for value in material.values()))
        async with asyncio.timeout(180):
            api: dict[str, Any] = clients if clients is not None else {
                'projects': v1.ProjectServiceClient(sdk), 'accounts': v1.ServiceAccountServiceClient(sdk),
                'groups': v1.GroupServiceClient(sdk), 'memberships': v1.GroupMembershipServiceClient(sdk),
                'permits': v1.AccessPermitServiceClient(sdk), 'access_keys': v2.AccessKeyServiceClient(sdk),
                'buckets': storage.BucketServiceClient(sdk), 'disks': compute.DiskServiceClient(sdk)}
            for identity in {scope.compute_project_id, scope.object_project_id}:
                row = await _read(api['projects'].get, v1.GetProjectRequest(id=identity))
                _resource(row, identity, scope.tenant_id)
                _require(row['status']['container_state'] == 'ACTIVE' and row['status']['suspension_state'] == 'NONE'
                    and row['spec']['region'] == row['status']['region'] == scope.region)
            disk = await _read(api['disks'].get, compute.GetDiskRequest(id=scope.database_disk_id))
            _resource(disk, scope.database_disk_id, scope.compute_project_id)
            _require(disk['status']['state'] == 'READY' and not disk['status'].get('reconciling')
                and not disk['status'].get('managed_by') and not disk['status'].get('read_only_attachments'))
            for purpose, subject in scope.subjects.items():
                row = await _read(api['accounts'].get, v1.GetServiceAccountRequest(id=subject.account_id))
                _resource(row, subject.account_id, scope.object_project_id)
                _require(row['status']['active'] is True)
                memberships = await _pages(api['memberships'].list_member_of, v1.ListMemberOfRequest, subject_id=subject.account_id)
                _require(len(memberships) == 1)
                _resource(memberships[0], subject.group_id, scope.object_project_id)
                row = await _read(api['groups'].get, v1.GetGroupRequest(id=subject.group_id))
                _resource(row, subject.group_id, scope.object_project_id)
                _require(not await _pages(api['permits'].list, v1.ListAccessPermitRequest, parent_id=subject.group_id))
                prefix = '' if purpose == 'canonical' else purpose + '-'
                row = await _read(api['access_keys'].get_by_aws_id,
                    v2.GetAccessKeyByAwsIdRequest(aws_access_key_id=material[prefix + 'access-key']))
                _resource(row, subject.key_id, scope.object_project_id)
                _active_key(row, account=subject.account_id, now=datetime.now(UTC))
                _require(row['status']['aws_access_key_id'] == material[prefix + 'access-key'])
            expected_buckets = {identity: (name, subject.group_id) for subject in scope.subjects.values()
                for identity, name in subject.buckets.items()}
            buckets = await _pages(api['buckets'].list, storage.ListBucketsRequest, parent_id=scope.object_project_id)
            _require(set(expected_buckets) <= {row['metadata']['id'] for row in buckets})
            groups = {row.group_id for row in scope.subjects.values()}
            for row in buckets:
                identity = row['metadata']['id']
                _resource(row, identity, scope.object_project_id)
                rules = [rule for rule in row.get('spec', {}).get('bucket_policy', {}).get('rules', [])
                    if rule.get('group_id') in groups]
                if identity not in expected_buckets:
                    _require(not rules)
                    continue
                name, group = expected_buckets[identity]
                _require(row['metadata']['name'] == name and row['status']['state'] == 'ACTIVE'
                    and row['status']['suspension_state'] == 'NOT_SUSPENDED' and row['status']['region'] == scope.region
                    and not row['status'].get('anonymous_access_enabled') and not row['status'].get('deleted_at')
                    and not row['status'].get('purge_at')
                    and rules == [{'group_id': group, 'paths': ['*'], 'roles': ['storage.object-editor']}])
    except Exception:
        raise ValueError('environment_import_cloud_unqualified') from None
