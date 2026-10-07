"""Dev installer qualifies real SDK message shapes; only provider RPC is faked."""
from __future__ import annotations

import copy
import importlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from nebius.api.nebius.compute import v1 as compute
from nebius.api.nebius.iam import v1, v2
from nebius.api.nebius.quotas import v1 as quotas
from nebius.api.nebius.storage import v1 as storage


def module():
    return importlib.import_module("scripts.ops.nebius_development_cloud")


@pytest.fixture
def cloud():
    scope = {"tenant_id": "tenant-test", "region": "eu-north1", "compute_project_id": "project-compute",
        "object_project_id": "project-dev-data", "data_account_id": "serviceaccount-data",
        "data_group_id": "group-data", "data_key_id": "accesskey-data", "source_account_id": "serviceaccount-source",
        "source_group_id": "group-source", "source_key_id": "accesskey-source",
        "data_buckets": {"bucket-data": "loom-dev-data"}, "source_bucket_id": "bucket-source",
        "source_bucket_name": "loom-dev-source", "storage_quota_name": "compute-network-ssd",
        "storage_quota_unit": "byte", "disk_type": "NETWORK_SSD"}
    config = {"namespace": "loom-dev", "environment": "development", "project_id": "project-compute",
        "quota_parent_id": "tenant-test", "region": "eu-north1", "buckets": {
            "artifacts": "loom-dev-data", "trajectories": "loom-dev-data", "source": "loom-dev-source", "backup": "unused"}}
    material = {"access-key": "aws-data", "secret-key": "private-data-secret",
        "source-access-key": "aws-source", "source-secret-key": "private-source-secret"}
    rows = {}
    for project in ("project-compute", "project-dev-data"):
        rows[project] = (v1.Container, {"metadata": {"id": project, "parent_id": "tenant-test"},
            "spec": {"region": "eu-north1"}, "status": {"region": "eu-north1", "container_state": "ACTIVE", "suspension_state": "NONE"}})
    groups, permits = {}, {}
    for name in ("data", "source"):
        account, group, key, bucket = (prefix + name for prefix in ("serviceaccount-", "group-", "accesskey-", "bucket-"))
        rows[account] = (v1.ServiceAccount, {"metadata": {"id": account, "parent_id": "project-dev-data"}, "status": {"active": True}})
        rows[group] = (v1.Group, {"metadata": {"id": group, "parent_id": "project-dev-data"}})
        rows["aws-" + name] = (v2.AccessKey, {"metadata": {"id": key, "parent_id": "project-dev-data"},
            "spec": {"account": {"service_account": {"id": account}}, "expires_at": "2027-01-01T00:00:00Z"},
            "status": {"state": "ACTIVE", "aws_access_key_id": "aws-" + name}})
        rows[bucket] = (storage.Bucket, {"metadata": {"id": bucket, "parent_id": "project-dev-data", "name": "loom-dev-" + name},
            "spec": {"bucket_policy": {"rules": [{"group_id": group, "paths": ["*"], "roles": ["storage.object-editor"]}]}},
            "status": {"state": "ACTIVE", "suspension_state": "NOT_SUSPENDED", "region": "eu-north1"}})
        groups[account], permits[group] = [group], []
    rows["compute-network-ssd"] = (quotas.QuotaAllowance, {"metadata": {
        "id": "quota-storage", "parent_id": "tenant-test", "name": "compute-network-ssd"},
        "spec": {"region": "eu-north1", "limit": str(100 * 1024**3)},
        "status": {"state": "STATE_ACTIVE", "usage_state": "USAGE_STATE_USED", "service": "compute",
            "unit": "byte", "usage": str(80 * 1024**3)}})
    rows["computedisk-test"] = (compute.Disk, {"metadata": {"id": "computedisk-test", "parent_id": "project-compute",
        "name": "csi-provisioned", "created_at": "2026-10-07T12:00:02Z"},
        "spec": {"size_gibibytes": "10", "type": "NETWORK_SSD"},
        "status": {"state": "READY", "size_bytes": str(10 * 1024**3), "read_write_attachment": "computeinstance-test"}})
    calls = []

    async def get(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        identity = getattr(request, "id", None) or getattr(request, "aws_access_key_id", None) or request.name
        if isinstance(request, quotas.GetByNameRequest):
            assert request.parent_id == "tenant-test" and request.region == "eu-north1"
        calls.append(("get", identity))
        cls, document = rows[identity]
        return cls.from_json(json.dumps(document))

    async def member_of(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0} and not request.page_token
        return v1.ListMemberOfResponse.from_json(json.dumps({"items": [rows[name][1] for name in groups[request.subject_id]]}))

    async def list_permits(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0} and not request.page_token
        return v1.ListAccessPermitResponse.from_json(json.dumps({"items": permits[request.parent_id]}))

    async def list_buckets(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0} and not request.page_token
        assert request.parent_id == "project-dev-data"
        return storage.ListBucketsResponse.from_json(json.dumps({"items": [row for cls, row in rows.values() if cls is storage.Bucket]}))

    clients = {name: SimpleNamespace(get=get) for name in ("projects", "accounts", "groups", "disks")}
    clients.update(access_keys=SimpleNamespace(get_by_aws_id=get), memberships=SimpleNamespace(list_member_of=member_of),
        permits=SimpleNamespace(list=list_permits), buckets=SimpleNamespace(list=list_buckets), quotas=SimpleNamespace(get_by_name=get))
    return SimpleNamespace(scope=scope, config=config, material=material, rows=rows, groups=groups, permits=permits,
        clients=clients, calls=calls)


async def qualify(cloud, demand=10 * 1024):
    return await module().qualify_development_cloud(sdk=None,
        scope=module().DevelopmentCloudScope.model_validate(cloud.scope), config=cloud.config,
        material=cloud.material, pending_storage_mib=demand, clients=cloud.clients,
        now=datetime(2026, 10, 7, 12, tzinfo=UTC))


async def test_dev_data_and_source_identities_need_no_backup_or_provisioning_grants(cloud):
    await qualify(cloud)
    assert ("get", "aws-data") in cloud.calls and ("get", "aws-source") in cloud.calls
    assert ("get", "compute-network-ssd") in cloud.calls


@pytest.mark.parametrize("change", ["staging", "wrong-region", "suspended", "wrong-key", "expired-key", "inactive-account",
    "extra-membership", "admin-grant", "public-bucket", "wrong-bucket-role", "wrong-bucket-name", "extra-bucket-grant",
    "quota-exhausted", "quota-wrong-region", "quota-wrong-unit"])
async def test_cloud_mismatch_or_broad_data_authority_blocks_installation(cloud, change):
    key = cloud.rows["aws-data"][1]
    bucket = cloud.rows["bucket-data"][1]
    quota = cloud.rows["compute-network-ssd"][1]
    if change == "staging":
        cloud.config["namespace"] = "loom-nebius-platform"
    elif change == "wrong-region":
        cloud.rows["project-compute"][1]["status"]["region"] = "us-central1"
    elif change == "suspended":
        cloud.rows["project-dev-data"][1]["status"]["suspension_state"] = "SUSPENDED"
    elif change == "wrong-key":
        key["spec"]["account"]["service_account"]["id"] = "serviceaccount-staging"
    elif change == "expired-key":
        key["spec"]["expires_at"] = "2026-10-07T12:00:00Z"
    elif change == "inactive-account":
        cloud.rows["serviceaccount-data"][1]["status"]["active"] = False
    elif change == "extra-membership":
        cloud.groups["serviceaccount-data"].append("group-source")
    elif change == "admin-grant":
        cloud.permits["group-data"] = [{"metadata": {"id": "permit-admin", "parent_id": "group-data"},
            "spec": {"role": "admin", "resource_id": "project-dev-data"}}]
    elif change == "public-bucket":
        bucket["status"]["anonymous_access_enabled"] = True
    elif change == "wrong-bucket-role":
        bucket["spec"]["bucket_policy"]["rules"][0]["roles"] = ["storage.admin"]
    elif change == "wrong-bucket-name":
        bucket["metadata"]["name"] = "staging-data"
    elif change == "extra-bucket-grant":
        extra = copy.deepcopy(bucket)
        extra["metadata"].update(id="bucket-foreign", name="staging-data")
        cloud.rows["bucket-foreign"] = storage.Bucket, extra
    elif change == "quota-exhausted":
        quota["status"]["usage"] = str(99 * 1024**3)
    elif change == "quota-wrong-region":
        quota["spec"]["region"] = "us-central1"
    else:
        quota["status"]["unit"] = "count"
    with pytest.raises(module().DevelopmentCloudError) as error:
        await qualify(cloud)
    assert "private-" not in str(error.value)


async def disk(cloud):
    await module().qualify_development_disk(sdk=None, scope=module().DevelopmentCloudScope.model_validate(cloud.scope),
        disk_id="computedisk-test", capacity_bytes=10 * 1024**3, instance_id="computeinstance-test",
        claim_created_at="2026-10-07T12:00:00Z", volume_created_at="2026-10-07T12:00:03Z", clients=cloud.clients)


async def test_bound_disk_is_empty_fresh_correct_project_region_size_and_attachment(cloud):
    await disk(cloud)
    assert ("get", "computedisk-test") in cloud.calls
    assert ("get", "project-compute") in cloud.calls


@pytest.mark.parametrize("change", ["project", "region", "old", "future", "snapshot", "image", "capacity", "type",
    "broken", "reconciling", "foreign-attachment", "read-only-attachment", "instance-owned"])
async def test_provider_disk_cannot_be_reused_cloned_foreign_or_unready(cloud, change):
    row = cloud.rows["computedisk-test"][1]
    if change == "project":
        row["metadata"]["parent_id"] = "project-foreign"
    elif change == "region":
        cloud.rows["project-compute"][1]["status"]["region"] = "us-central1"
    elif change in {"old", "future"}:
        row["metadata"]["created_at"] = "2026-10-07T" + ("11:59:59Z" if change == "old" else "12:00:04Z")
    elif change == "snapshot":
        row["spec"]["source_snapshot_id"] = "computedisksnapshot-old"
    elif change == "image":
        row["spec"]["source_image_id"] = "computeimage-old"
    elif change == "capacity":
        row["status"]["size_bytes"] = str(9 * 1024**3)
    elif change == "type":
        row["spec"]["type"] = "NETWORK_HDD"
    elif change == "broken":
        row["status"]["state"] = "BROKEN"
    elif change == "reconciling":
        row["status"]["reconciling"] = True
    elif change == "foreign-attachment":
        row["status"]["read_write_attachment"] = "computeinstance-foreign"
    elif change == "read-only-attachment":
        row["status"]["read_only_attachments"] = ["computeinstance-foreign"]
    else:
        row["status"]["managed_by"] = "computeinstance-test"
    with pytest.raises(module().DevelopmentCloudError):
        await disk(cloud)
