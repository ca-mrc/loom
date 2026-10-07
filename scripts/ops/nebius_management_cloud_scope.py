"""Read-only qualification of pinned management IAM and backup authority.

The private protected installer owns the observing SDK, not a tenant caller.
Nothing here creates resources, obtains backup secrets or delivers credentials.
Bucket access is also tested with the supplied S3 credential by the entrypoint.
"""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom_service.environment_management.candidates import _json


class ManagementCloudScopeError(RuntimeError):
    """Sanitized diagnostic; SDK errors and credential material must stay private."""


_ProviderId = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,128}$")]


class ManagementBackupScope(BaseModel):
    """Object-only recovery identity, independent of the provisioning mode."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r"^[a-z]+-[a-z]+[0-9]+$")
    backup_project_id: _ProviderId
    backup_account_id: _ProviderId
    backup_group_id: _ProviderId
    backup_bucket_id: _ProviderId
    backup_key_id: _ProviderId


class ManagementCloudScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r"^[a-z]+-[a-z]+[0-9]+$")
    provisioning_project_id: _ProviderId
    provisioning_account_id: _ProviderId
    provisioning_group_id: _ProviderId
    provisioning_key_id: _ProviderId
    backup_project_id: _ProviderId
    backup_account_id: _ProviderId
    backup_group_id: _ProviderId
    backup_bucket_id: _ProviderId
    backup_key_id: _ProviderId

    @model_validator(mode="after")
    def separate_authorities(self) -> ManagementCloudScope:
        for field in ("project_id", "account_id", "group_id"):
            if getattr(self, "provisioning_" + field) == getattr(self, "backup_" + field):
                raise ValueError("management provisioning and backup require separate authority")
        return self


def _require(value: bool) -> None:
    if not value:
        raise ManagementCloudScopeError("management cloud authority unqualified")


async def _read(method: Any, request: Any) -> dict[str, Any]:
    result = await method(request, timeout=30, retries=0)
    return dict(json.loads(result.to_json(preserving_proto_field_name=True)))


async def _pages(method: Any, request_type: Any, **fields: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    token = ""
    seen: set[str] = set()
    for _ in range(20):
        page = await _read(method, request_type(**fields, page_size=100, page_token=token))
        items = page.get("items", [])
        _require(isinstance(items, list) and len(items) <= 100)
        rows.extend(items)
        token = page.get("next_page_token", "")
        if not token:
            identities = [row["metadata"]["id"] for row in rows]
            _require(len(identities) == len(set(identities)))
            return rows
        _require(isinstance(token, str) and token not in seen)
        seen.add(token)
    raise ManagementCloudScopeError("management cloud inventory unqualified")


def _resource(row: dict[str, Any], identity: str, parent: str) -> None:
    meta = row["metadata"]
    _require(meta["id"] == identity and meta["parent_id"] == parent
             and not meta.get("deletion_timestamp"))


def _active_key(row: dict[str, Any], *, account: str, now: datetime) -> None:
    _require(row["spec"]["account"] == {"service_account": {"id": account}} and row["status"]["state"] == "ACTIVE")
    # A key that can expire during bootstrap is not a usable installation input.
    expires = datetime.fromisoformat(row["spec"]["expires_at"].replace("Z", "+00:00"))
    _require(expires > now + timedelta(minutes=10))


async def qualify_cloud_material(*, sdk: Any, scope: ManagementCloudScope, material: dict[str, dict[str, str]],
                                 bucket_name: str, backup_bytes: int, clients: dict[str, Any] | None = None,
                                 now: datetime | None = None) -> dict[str, str]:
    """Prove exact, non-inherited project admin and separate object-only backup.

    `clients`/`now` support deterministic transport tests. Installed callers use
    SDK clients and the actual clock. Every read is bounded with no RPC retries.
    A failed or incomplete observation is never permission to proceed.
    """
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.storage import v1 as storage
    from nebius.base.service_account.credentials_file import ServiceAccountCredentials

    try:
        async with asyncio.timeout(120):
            observed_at = now or datetime.now(UTC)
            api: dict[str, Any] = clients if clients is not None else {
                "projects": v1.ProjectServiceClient(sdk), "accounts": v1.ServiceAccountServiceClient(sdk),
                "memberships": v1.GroupMembershipServiceClient(sdk), "permits": v1.AccessPermitServiceClient(sdk),
                "public_keys": v1.AuthPublicKeyServiceClient(sdk), "access_keys": v2.AccessKeyServiceClient(sdk),
                "buckets": storage.BucketServiceClient(sdk),
            }
            credentials = ServiceAccountCredentials.from_json(_json(material["loom-management-cloud"]["credentials.json"].encode()))
            subject = credentials.subject_credentials
            _require(subject.sub == scope.provisioning_account_id and subject.kid == scope.provisioning_key_id)
            public = subject.parse_private_key().public_key().public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            for prefix in ("provisioning",):
                project, account, group = (getattr(scope, prefix + "_" + field) for field in ("project_id", "account_id", "group_id"))
                container = await _read(api["projects"].get, v1.GetProjectRequest(id=project))
                _resource(container, project, scope.tenant_id)
                _require(container["status"]["container_state"] == "ACTIVE"
                         and container["status"]["suspension_state"] == "NONE"
                         and container["spec"]["region"] == container["status"]["region"] == scope.region)
                row = await _read(api["accounts"].get, v1.GetServiceAccountRequest(id=account))
                _resource(row, account, project)
                _require(row["status"]["active"] is True)
                groups = await _pages(api["memberships"].list_member_of, v1.ListMemberOfRequest, subject_id=account)
                _require(len(groups) == 1)
                _resource(groups[0], group, project)
                # Nebius group members are accounts, not groups. ListMemberOf
                # accepts only account subjects. Check all account memberships
                # above; never query a fictional nested-group relationship.
                permits = await _pages(api["permits"].list, v1.ListAccessPermitRequest, parent_id=group)
                _require(len(permits) == 1 and permits[0]["metadata"]["parent_id"] == group
                         and permits[0]["spec"] == {"resource_id": project, "role": "admin"})
            row = await _read(api["public_keys"].get, v1.GetAuthPublicKeyRequest(id=subject.kid))
            _resource(row, scope.provisioning_key_id, scope.provisioning_project_id)
            _active_key(row, account=scope.provisioning_account_id, now=observed_at)
            registered = serialization.load_pem_public_key(row["spec"]["data"].encode()).public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            _require(registered == public)
            backup = await qualify_backup_material(sdk=sdk,
                scope=ManagementBackupScope.model_validate(scope.model_dump(include=set(ManagementBackupScope.model_fields))),
                material=material['loom-platform-storage'], bucket_name=bucket_name, backup_bytes=backup_bytes,
                clients=api, now=observed_at)
            return {"provisioning_account_id": scope.provisioning_account_id, **backup}
    except Exception:
        raise ManagementCloudScopeError("management cloud authority unqualified") from None


async def qualify_backup_material(*, sdk: Any, scope: ManagementBackupScope, material: dict[str, str],
                                  bucket_name: str, backup_bytes: int, clients: dict[str, Any] | None = None,
                                  now: datetime | None = None) -> dict[str, str]:
    """Read-only recovery qualification shared by legacy and application managers."""
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.storage import v1 as storage

    try:
        async with asyncio.timeout(120):
            api: dict[str, Any] = clients if clients is not None else {
                'projects': v1.ProjectServiceClient(sdk), 'accounts': v1.ServiceAccountServiceClient(sdk),
                'memberships': v1.GroupMembershipServiceClient(sdk), 'permits': v1.AccessPermitServiceClient(sdk),
                'access_keys': v2.AccessKeyServiceClient(sdk), 'buckets': storage.BucketServiceClient(sdk)}
            observed_at = now or datetime.now(UTC)
            project, account, group = scope.backup_project_id, scope.backup_account_id, scope.backup_group_id
            container = await _read(api['projects'].get, v1.GetProjectRequest(id=project))
            _resource(container, project, scope.tenant_id)
            _require(container['status']['container_state'] == 'ACTIVE' and container['status']['suspension_state'] == 'NONE'
                and container['spec']['region'] == container['status']['region'] == scope.region)
            row = await _read(api['accounts'].get, v1.GetServiceAccountRequest(id=account))
            _resource(row, account, project)
            _require(row['status']['active'] is True)
            groups = await _pages(api['memberships'].list_member_of, v1.ListMemberOfRequest, subject_id=account)
            _require(len(groups) == 1)
            _resource(groups[0], group, project)
            # Object access belongs to one bucket policy, never project grants.
            _require(not await _pages(api['permits'].list, v1.ListAccessPermitRequest, parent_id=group))
            _require(set(material) == {'backup-access-key', 'backup-secret-key'}
                and all(isinstance(value, str) and 0 < len(value.encode()) <= 65536 for value in material.values()))
            aws_id = material["backup-access-key"]
            row = await _read(api["access_keys"].get_by_aws_id, v2.GetAccessKeyByAwsIdRequest(aws_access_key_id=aws_id))
            _resource(row, scope.backup_key_id, scope.backup_project_id)
            _active_key(row, account=scope.backup_account_id, now=observed_at)
            _require(row["status"]["aws_access_key_id"] == aws_id)
            bucket = await _read(api["buckets"].get, storage.GetBucketRequest(id=scope.backup_bucket_id))
            _resource(bucket, scope.backup_bucket_id, scope.backup_project_id)
            _require(bucket["metadata"]["name"] == bucket_name and bucket["status"]["state"] == "ACTIVE"
                     and bucket["status"]["suspension_state"] == "NOT_SUSPENDED" and bucket["status"]["region"] == scope.region
                     and not bucket["status"].get("anonymous_access_enabled")
                     and not bucket["status"].get("deleted_at") and not bucket["status"].get("purge_at")
                     and bucket["spec"]["versioning_policy"] == "ENABLED")
            _require(type(backup_bytes) is int and backup_bytes > 0)
            limit = int(bucket["spec"].get("max_size_bytes", 0))
            sizes = [int(counters.get(section, {}).get(field, 0))
                     for counters in bucket["status"].get("counters", [])
                     for section, fields in (("counters", ("simple_objects_size", "multipart_objects_size", "inflight_parts_size")),
                                             ("non_current_counters", ("simple_objects_size", "multipart_objects_size")))
                     for field in fields]
            _require(limit >= 0 and all(size >= 0 for size in sizes)
                     and (limit == 0 or limit - sum(sizes) >= backup_bytes))
            # Initial recovery evidence is retained. Do not silently accept
            # lifecycle deletion/transition policy before restore qualification.
            _require(all(rule.get("status") == "DISABLED" for rule in
                         bucket["spec"].get("lifecycle_configuration", {}).get("rules", [])))
            rules = bucket["spec"]["bucket_policy"]["rules"]
            _require(len(rules) == 1 and rules[0] == {
                         "group_id": scope.backup_group_id, "paths": ["*"], "roles": ["storage.object-editor"]})
            # Project groups can authorize resources in that project. Absence
            # of IAM permits is insufficient: another bucket policy could
            # reference the same group. Inspect the complete project inventory.
            buckets = await _pages(api["buckets"].list, storage.ListBucketsRequest, parent_id=scope.backup_project_id)
            _require(sum(item["metadata"]["id"] == scope.backup_bucket_id for item in buckets) == 1)
            for item in buckets:
                _require(item["metadata"]["parent_id"] == scope.backup_project_id)
                if item["metadata"]["id"] == scope.backup_bucket_id:
                    _require(item == bucket)
                else:
                    _require(not any(rule.get("group_id") == scope.backup_group_id
                                     for rule in item.get("spec", {}).get("bucket_policy", {}).get("rules", [])))
            return {"backup_bucket_id": scope.backup_bucket_id, "backup_key_id": scope.backup_key_id}
    except Exception:
        raise ManagementCloudScopeError("management cloud authority unqualified") from None
