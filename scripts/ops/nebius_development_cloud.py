"""Read-only provider qualification consumed by the private dev installer.

Operator SDK credentials never become runtime material. The two runtime subjects
have object-only authority, not project provisioning or backup permissions. Actual
S3 secret possession is probed separately by the connected installer.
"""
from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_management_cloud_scope import (
    _active_key,
    _pages,
    _ProviderId,
    _read,
    _require,
    _resource,
)

_BucketName = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")]


class DevelopmentCloudError(RuntimeError):
    """Closed diagnostic; never emit provider payloads or supplied credentials."""


class DevelopmentCloudScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r"^[a-z]+-[a-z]+[0-9]+$")
    compute_project_id: _ProviderId
    object_project_id: _ProviderId
    data_account_id: _ProviderId
    data_group_id: _ProviderId
    data_key_id: _ProviderId
    source_account_id: _ProviderId
    source_group_id: _ProviderId
    source_key_id: _ProviderId
    data_buckets: dict[_ProviderId, _BucketName] = Field(min_length=1, max_length=2)
    source_bucket_id: _ProviderId
    source_bucket_name: _BucketName
    storage_quota_name: str = Field(min_length=1, max_length=255)
    storage_quota_unit: Literal["byte", "bytes", "B"]
    disk_type: Literal["NETWORK_SSD", "NETWORK_HDD", "NETWORK_SSD_NON_REPLICATED", "NETWORK_SSD_IO_M3"]

    @model_validator(mode="after")
    def separate(self) -> Self:
        if (any(getattr(self, "data_" + field) == getattr(self, "source_" + field)
                for field in ("account_id", "group_id", "key_id"))
                or self.source_bucket_id in self.data_buckets
                or len(set(self.data_buckets.values()) | {self.source_bucket_name}) != len(self.data_buckets) + 1):
            raise ValueError("development data and source authorities must be separate")
        return self


async def _project(client: Any, identity: str, scope: DevelopmentCloudScope) -> None:
    from nebius.api.nebius.iam import v1

    row = await _read(client.get, v1.GetProjectRequest(id=identity))
    _resource(row, identity, scope.tenant_id)
    _require(row["status"]["container_state"] == "ACTIVE" and row["status"]["suspension_state"] == "NONE"
        and row["spec"]["region"] == row["status"]["region"] == scope.region)


async def qualify_development_cloud(*, sdk: Any, scope: DevelopmentCloudScope, config: dict[str, Any],
                                    material: dict[str, str], pending_storage_mib: int,
                                    clients: dict[str, Any] | None = None, now: datetime | None = None) -> None:
    """Check configured project/buckets, actual AWS-key subjects and disk demand."""
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.quotas import v1 as quotas
    from nebius.api.nebius.storage import v1 as storage

    try:
        scope = DevelopmentCloudScope.model_validate(scope.model_dump())
        _require(config["namespace"] == "loom-dev" and config["environment"] == "development"
            and config["project_id"] == scope.compute_project_id and config["quota_parent_id"] == scope.tenant_id
            and config["region"] == scope.region
            and set(scope.data_buckets.values()) == {config["buckets"][name] for name in ("artifacts", "trajectories")}
            and scope.source_bucket_name == config["buckets"]["source"]
            and set(material) == {"access-key", "secret-key", "source-access-key", "source-secret-key"}
            and all(isinstance(value, str) and 0 < len(value.encode()) <= 65536 for value in material.values())
            and type(pending_storage_mib) is int and pending_storage_mib >= 0)
        async with asyncio.timeout(180):
            api: dict[str, Any] = clients if clients is not None else {
                "projects": v1.ProjectServiceClient(sdk), "accounts": v1.ServiceAccountServiceClient(sdk),
                "groups": v1.GroupServiceClient(sdk), "memberships": v1.GroupMembershipServiceClient(sdk),
                "permits": v1.AccessPermitServiceClient(sdk), "access_keys": v2.AccessKeyServiceClient(sdk),
                "buckets": storage.BucketServiceClient(sdk), "quotas": quotas.QuotaAllowanceServiceClient(sdk)}
            for project in {scope.compute_project_id, scope.object_project_id}:
                await _project(api["projects"], project, scope)
            for name, credential in (("data", "access-key"), ("source", "source-access-key")):
                account, group, key = (getattr(scope, name + "_" + field) for field in ("account_id", "group_id", "key_id"))
                row = await _read(api["accounts"].get, v1.GetServiceAccountRequest(id=account))
                _resource(row, account, scope.object_project_id)
                _require(row["status"]["active"] is True)
                memberships = await _pages(api["memberships"].list_member_of, v1.ListMemberOfRequest, subject_id=account)
                _require(len(memberships) == 1)
                _resource(memberships[0], group, scope.object_project_id)
                row = await _read(api["groups"].get, v1.GetGroupRequest(id=group))
                _resource(row, group, scope.object_project_id)
                _require(not await _pages(api["permits"].list, v1.ListAccessPermitRequest, parent_id=group))
                row = await _read(api["access_keys"].get_by_aws_id, v2.GetAccessKeyByAwsIdRequest(aws_access_key_id=material[credential]))
                _resource(row, key, scope.object_project_id)
                _active_key(row, account=account, now=now or datetime.now(UTC))
                _require(row["status"]["aws_access_key_id"] == material[credential])
            expected = {**{identity: (name, scope.data_group_id) for identity, name in scope.data_buckets.items()},
                scope.source_bucket_id: (scope.source_bucket_name, scope.source_group_id)}
            buckets = await _pages(api["buckets"].list, storage.ListBucketsRequest, parent_id=scope.object_project_id)
            _require(set(expected) <= {row["metadata"]["id"] for row in buckets})
            for row in buckets:
                identity = row["metadata"]["id"]
                _resource(row, identity, scope.object_project_id)
                rules = [rule for rule in row.get("spec", {}).get("bucket_policy", {}).get("rules", [])
                    if rule.get("group_id") in {scope.data_group_id, scope.source_group_id}]
                if identity not in expected:
                    _require(not rules)
                    continue
                name, group = expected[identity]
                _require(row["metadata"]["name"] == name and row["status"]["state"] == "ACTIVE"
                    and row["status"]["suspension_state"] == "NOT_SUSPENDED" and row["status"]["region"] == scope.region
                    and not row["status"].get("anonymous_access_enabled") and not row["status"].get("deleted_at")
                    and not row["status"].get("purge_at")
                    and rules == [{"group_id": group, "paths": ["*"], "roles": ["storage.object-editor"]}])
            quota = await _read(api["quotas"].get_by_name, quotas.GetByNameRequest(
                parent_id=scope.tenant_id, name=scope.storage_quota_name, region=scope.region))
            _require(bool(quota["metadata"].get("id")) and quota["metadata"]["parent_id"] == scope.tenant_id
                and quota["metadata"]["name"] == scope.storage_quota_name and quota["spec"]["region"] == scope.region
                and quota["status"]["state"] == "STATE_ACTIVE" and quota["status"]["service"] == "compute"
                and quota["status"]["unit"] == scope.storage_quota_unit
                and quota["status"]["usage_state"] in {"USAGE_STATE_USED", "USAGE_STATE_NOT_USED"})
            limit, used = int(quota["spec"].get("limit", 0)), int(quota["status"].get("usage", 0))
            _require(min(limit, used) >= 0 and limit - used >= pending_storage_mib * 1024**2)
    except Exception:
        raise DevelopmentCloudError("development cloud identity or disk headroom unqualified") from None


def _timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError()
    return result


async def qualify_development_disk(*, sdk: Any, scope: DevelopmentCloudScope, disk_id: str,
                                  capacity_bytes: int, instance_id: str, claim_created_at: str,
                                  volume_created_at: str, clients: dict[str, Any] | None = None) -> None:
    """Qualify the disk named by a separately UID-bound dynamic PVC/PV observation.

    Timestamps and instance ID come from live Kubernetes readback, not an owner
    request. Trusted CSI creates the empty disk between its claim and PV; provider
    project region, source, capacity and attachment are checked independently.
    """
    from nebius.api.nebius.compute import v1 as compute
    from nebius.api.nebius.iam import v1

    try:
        scope = DevelopmentCloudScope.model_validate(scope.model_dump())
        _require(re.fullmatch(r"computedisk-[a-z0-9]+", disk_id) is not None
            and re.fullmatch(r"computeinstance-[a-z0-9]+", instance_id) is not None
            and type(capacity_bytes) is int and capacity_bytes > 0)
        async with asyncio.timeout(90):
            api: dict[str, Any] = clients if clients is not None else {
                "projects": v1.ProjectServiceClient(sdk), "disks": compute.DiskServiceClient(sdk)}
            await _project(api["projects"], scope.compute_project_id, scope)
            row = await _read(api["disks"].get, compute.GetDiskRequest(id=disk_id))
            _resource(row, disk_id, scope.compute_project_id)
            created = _timestamp(row["metadata"]["created_at"])
            _require(_timestamp(claim_created_at) <= created <= _timestamp(volume_created_at))
            spec, status = row["spec"], row["status"]
            sizes = [int(spec[key]) * factor for key, factor in (
                ("size_bytes", 1), ("size_kibibytes", 1024), ("size_mebibytes", 1024**2), ("size_gibibytes", 1024**3)) if key in spec]
            _require(sizes == [capacity_bytes] and int(status["size_bytes"]) == capacity_bytes
                and spec["type"] == scope.disk_type and status["state"] == "READY"
                and not any(spec.get(field) for field in ("source_image_id", "source_image_family", "source_snapshot_id"))
                and not status.get("source_image_id") and not status.get("reconciling")
                and not status.get("managed_by") and not status.get("read_only_attachments")
                and status.get("read_write_attachment", "") in {"", instance_id})
    except Exception:
        raise DevelopmentCloudError("development provider disk unqualified") from None
