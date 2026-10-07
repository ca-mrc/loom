"""Actual SDK message shapes; only cloud RPCs are replaced by a read-only store."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from nebius.api.nebius.iam import v1, v2
from nebius.api.nebius.storage import v1 as storage


@pytest.fixture
def cloud():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    scope = {"tenant_id": "tenant-test", "region": "eu-north1", "provisioning_project_id": "project-children",
             "provisioning_account_id": "serviceaccount-manager", "provisioning_group_id": "group-manager",
             "provisioning_key_id": "authpublickey-manager", "backup_project_id": "project-backups",
             "backup_account_id": "serviceaccount-backup", "backup_group_id": "group-backup",
             "backup_bucket_id": "bucket-management", "backup_key_id": "accesskey-backup"}
    credential = {"subject-credentials": {"alg": "RS256", "private-key": private,
                  "kid": "authpublickey-manager", "iss": "serviceaccount-manager", "sub": "serviceaccount-manager"}}
    material = {"loom-management-cloud": {"credentials.json": json.dumps(credential)},
                "loom-management-publications": {"token": "never-print-test-token"},
                "loom-platform-storage": {"backup-access-key": "aws-backup", "backup-secret-key": "never-print-secret"}}
    rows = {}
    for name, account, group in (("children", "manager", "manager"), ("backups", "backup", "backup")):
        project = "project-" + name
        rows[project] = (v1.Container, {"metadata": {"id": project, "parent_id": "tenant-test"},
            "spec": {"region": "eu-north1"}, "status": {"container_state": "ACTIVE", "suspension_state": "NONE", "region": "eu-north1"}})
        rows["serviceaccount-" + account] = (v1.ServiceAccount, {
            "metadata": {"id": "serviceaccount-" + account, "parent_id": project}, "status": {"active": True}})
        rows["group-" + group] = (v1.Group, {"metadata": {"id": "group-" + group, "parent_id": project}})
    rows["authpublickey-manager"] = (v1.AuthPublicKey, {
        "metadata": {"id": "authpublickey-manager", "parent_id": "project-children"},
        "spec": {"account": {"service_account": {"id": "serviceaccount-manager"}}, "data": public,
                 "expires_at": "2027-01-01T00:00:00Z"}, "status": {"state": "ACTIVE"}})
    rows["aws-backup"] = (v2.AccessKey, {
        "metadata": {"id": "accesskey-backup", "parent_id": "project-backups"},
        "spec": {"account": {"service_account": {"id": "serviceaccount-backup"}}, "expires_at": "2027-01-01T00:00:00Z"},
        "status": {"state": "ACTIVE", "aws_access_key_id": "aws-backup"}})
    rows["bucket-management"] = (storage.Bucket, {
        "metadata": {"id": "bucket-management", "name": "loom-management-backup", "parent_id": "project-backups"},
        "spec": {"versioning_policy": "ENABLED", "bucket_policy": {"rules": [{
            "group_id": "group-backup", "paths": ["*"], "roles": ["storage.object-editor"]}]}},
        "status": {"state": "ACTIVE", "suspension_state": "NOT_SUSPENDED", "region": "eu-north1"}})
    groups = {"serviceaccount-manager": ["group-manager"], "serviceaccount-backup": ["group-backup"]}
    permits = {"group-manager": [{"metadata": {"id": "permit-manager", "parent_id": "group-manager"},
                "spec": {"resource_id": "project-children", "role": "admin"}}], "group-backup": []}
    bucket_ids = ["bucket-management"]
    calls = []

    async def get(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        identity = getattr(request, "id", None) or request.aws_access_key_id
        calls.append(("get", identity))
        cls, document = rows[identity]
        return cls.from_json(json.dumps(document))

    async def member_of(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        # Nebius ListMemberOf accepts account subjects, never a group ID.
        if request.subject_id not in groups:
            raise ValueError("unsupported membership subject")
        calls.append(("member_of", request.subject_id))
        assert not request.page_token
        return v1.ListMemberOfResponse.from_json(json.dumps({
            "items": [rows[identity][1] for identity in groups[request.subject_id]]}))

    async def list_permits(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        calls.append(("permits", request.parent_id))
        assert not request.page_token
        return v1.ListAccessPermitResponse.from_json(json.dumps({"items": permits[request.parent_id]}))

    async def list_buckets(request, **kwargs):
        assert kwargs == {"timeout": 30, "retries": 0}
        assert request.parent_id == "project-backups" and not request.page_token
        return storage.ListBucketsResponse.from_json(json.dumps({"items": [rows[identity][1] for identity in bucket_ids]}))

    clients = {name: SimpleNamespace(get=get) for name in ("projects", "accounts", "public_keys", "buckets")}
    clients.update(memberships=SimpleNamespace(list_member_of=member_of), permits=SimpleNamespace(list=list_permits),
                   access_keys=SimpleNamespace(get_by_aws_id=get))
    clients["buckets"].list = list_buckets
    return SimpleNamespace(scope=scope, material=material, rows=rows, groups=groups, permits=permits, clients=clients,
                           calls=calls, bucket_ids=bucket_ids)


async def qualify(cloud):
    from scripts.ops.nebius_management_cloud_scope import (
        ManagementCloudScope,
        qualify_cloud_material,
    )

    return await qualify_cloud_material(sdk=None, scope=ManagementCloudScope.model_validate(cloud.scope),
        material=cloud.material, bucket_name="loom-management-backup", clients=cloud.clients,
        backup_bytes=10 * 1024**3,
        now=datetime(2026, 9, 24, tzinfo=UTC))


@pytest.mark.asyncio
async def test_exact_project_provisioner_and_object_only_backup_are_qualified(cloud):
    result = await qualify(cloud)
    assert result["provisioning_account_id"] == "serviceaccount-manager"
    assert result["backup_bucket_id"] == "bucket-management"
    assert result["backup_key_id"] == "accesskey-backup"
    assert "never-print" not in json.dumps(result) and "PRIVATE KEY" not in json.dumps(result)


@pytest.mark.asyncio
async def test_backup_qualification_needs_no_legacy_provisioner_key_or_grants(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementBackupScope, qualify_backup_material

    scope = ManagementBackupScope.model_validate({key: value for key, value in cloud.scope.items()
        if key in {'tenant_id', 'region'} or key.startswith('backup_')})
    result = await qualify_backup_material(sdk=None, scope=scope,
        material=cloud.material['loom-platform-storage'], bucket_name='loom-management-backup',
        backup_bytes=10 * 1024**3, clients=cloud.clients, now=datetime(2026, 9, 24, tzinfo=UTC))
    assert result == {'backup_bucket_id': 'bucket-management', 'backup_key_id': 'accesskey-backup'}
    assert {identity for _, identity in cloud.calls} <= {
        'project-backups', 'serviceaccount-backup', 'group-backup', 'aws-backup', 'bucket-management'}


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['extra_group', 'iam_grant', 'wrong_key', 'cross_bucket', 'full_bucket'])
async def test_independent_backup_checks_reject_unqualified_scope_or_headroom(cloud, change):
    from scripts.ops.nebius_management_cloud_scope import (
        ManagementBackupScope,
        ManagementCloudScopeError,
        qualify_backup_material,
    )

    scope = ManagementBackupScope.model_validate({key: value for key, value in cloud.scope.items()
        if key in {'tenant_id', 'region'} or key.startswith('backup_')})
    if change == 'extra_group':
        cloud.groups['serviceaccount-backup'].append('group-manager')
    elif change == 'iam_grant':
        cloud.permits['group-backup'] = copy.deepcopy(cloud.permits['group-manager'])
    elif change == 'wrong_key':
        cloud.rows['aws-backup'][1]['spec']['account']['service_account']['id'] = 'serviceaccount-manager'
    elif change == 'cross_bucket':
        cloud.rows['other-bucket'] = (storage.Bucket, copy.deepcopy(cloud.rows['bucket-management'][1]))
        cloud.rows['other-bucket'][1]['metadata'].update(id='other-bucket', name='other-bucket')
        cloud.bucket_ids.append('other-bucket')
    else:
        cloud.rows['bucket-management'][1]['spec']['max_size_bytes'] = str(1024)
    with pytest.raises(ManagementCloudScopeError):
        await qualify_backup_material(sdk=None, scope=scope,
            material=cloud.material['loom-platform-storage'], bucket_name='loom-management-backup',
            backup_bytes=10 * 1024**3, clients=cloud.clients, now=datetime(2026, 9, 24, tzinfo=UTC))


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["tenant_admin", "extra_group", "tenant_group", "wrong_public_key", "wrong_subject",
    "wrong_kid", "key_expired", "key_inactive", "account_inactive", "project_suspended", "wrong_region", "backup_admin",
    "backup_key_subject", "backup_key_expired", "backup_key_inactive", "bucket_public", "bucket_wrong_group", "bucket_broad_role",
    "bucket_wrong_project", "bucket_no_versioning", "bucket_extra_rule"])
async def test_broad_mismatched_or_unusable_cloud_authority_is_rejected(cloud, mutation):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    key = cloud.rows["authpublickey-manager"][1]
    backup = cloud.rows["aws-backup"][1]
    bucket = cloud.rows["bucket-management"][1]
    if mutation == "tenant_admin":
        cloud.permits["group-manager"][0]["spec"]["resource_id"] = "tenant-test"
    elif mutation == "extra_group":
        cloud.groups["serviceaccount-manager"].append("group-backup")
    elif mutation == "tenant_group":
        cloud.rows["group-manager"][1]["metadata"]["parent_id"] = "tenant-test"
    elif mutation == "wrong_public_key":
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        key["spec"]["data"] = other.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    elif mutation in {"wrong_subject", "wrong_kid"}:
        credentials = json.loads(cloud.material["loom-management-cloud"]["credentials.json"])
        credentials["subject-credentials"]["sub" if mutation == "wrong_subject" else "kid"] = "other"
        cloud.material["loom-management-cloud"]["credentials.json"] = json.dumps(credentials)
    elif mutation == "key_expired":
        key["spec"]["expires_at"] = "2026-09-23T00:00:00Z"
    elif mutation == "key_inactive":
        key["status"]["state"] = "INACTIVE"
    elif mutation == "account_inactive":
        cloud.rows["serviceaccount-manager"][1]["status"]["active"] = False
    elif mutation == "project_suspended":
        cloud.rows["project-children"][1]["status"]["suspension_state"] = "SUSPENDED"
    elif mutation == "wrong_region":
        cloud.rows["project-children"][1]["status"]["region"] = "us-central1"
    elif mutation == "backup_admin":
        cloud.permits["group-backup"] = [copy.deepcopy(cloud.permits["group-manager"][0])]
    elif mutation == "backup_key_subject":
        backup["spec"]["account"]["service_account"]["id"] = "serviceaccount-manager"
    elif mutation == "backup_key_expired":
        backup["spec"]["expires_at"] = "2026-09-23T00:00:00Z"
    elif mutation == "backup_key_inactive":
        backup["status"]["state"] = "INACTIVE"
    elif mutation == "bucket_public":
        bucket["status"]["anonymous_access_enabled"] = True
    elif mutation == "bucket_wrong_group":
        bucket["spec"]["bucket_policy"]["rules"][0]["group_id"] = "group-manager"
    elif mutation == "bucket_broad_role":
        bucket["spec"]["bucket_policy"]["rules"][0]["roles"] = ["admin"]
    elif mutation == "bucket_wrong_project":
        bucket["metadata"]["parent_id"] = "project-children"
    elif mutation == "bucket_no_versioning":
        bucket["spec"]["versioning_policy"] = "DISABLED"
    elif mutation == "bucket_extra_rule":
        bucket["spec"]["bucket_policy"]["rules"].append({"group_id": "group-manager", "paths": ["*"], "roles": ["storage.object-editor"]})
    with pytest.raises(ManagementCloudScopeError) as error:
        await qualify(cloud)
    assert "never-print" not in str(error.value) and "PRIVATE KEY" not in str(error.value)


@pytest.mark.asyncio
async def test_later_membership_pages_are_checked_and_repeated_tokens_fail_closed(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    async def paged(request, **kwargs):
        return v1.ListMemberOfResponse.from_json(json.dumps({"items": [cloud.rows["group-manager"][1]], "next_page_token": "again"}))

    cloud.clients["memberships"].list_member_of = paged
    with pytest.raises(ManagementCloudScopeError):
        await qualify(cloud)


@pytest.mark.asyncio
async def test_rpc_errors_do_not_expose_secret_bearing_provider_diagnostics(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    async def failed(request, **kwargs):
        raise RuntimeError("never-print-secret")

    cloud.clients["projects"].get = failed
    with pytest.raises(ManagementCloudScopeError, match="unqualified") as error:
        await qualify(cloud)
    assert "never-print" not in str(error.value)


@pytest.mark.asyncio
async def test_backup_group_cannot_also_access_another_bucket(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    other = copy.deepcopy(cloud.rows["bucket-management"][1])
    other["metadata"].update(id="bucket-other", name="foreign-data")
    cloud.rows["bucket-other"] = (storage.Bucket, other)
    cloud.bucket_ids.append("bucket-other")
    with pytest.raises(ManagementCloudScopeError):
        await qualify(cloud)


@pytest.mark.asyncio
async def test_bucket_inventory_must_include_the_selected_backup_identity(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    cloud.bucket_ids.clear()
    with pytest.raises(ManagementCloudScopeError):
        await qualify(cloud)


@pytest.mark.asyncio
async def test_broad_permit_on_later_page_is_not_ignored(cloud):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    async def pages(request, **kwargs):
        if not request.page_token:
            return v1.ListAccessPermitResponse.from_json(json.dumps({"items": cloud.permits[request.parent_id], "next_page_token": "second"}))
        return v1.ListAccessPermitResponse.from_json(json.dumps({"items": [{
            "metadata": {"id": "permit-foreign", "parent_id": request.parent_id},
            "spec": {"role": "admin", "resource_id": "tenant-test"}}]}))

    cloud.clients["permits"].list = pages
    with pytest.raises(ManagementCloudScopeError):
        await qualify(cloud)


@pytest.mark.asyncio
async def test_typed_provider_readback_not_an_invented_key_prefix_identifies_the_key(cloud):
    cloud.scope["provisioning_key_id"] = "publickey-e00example"
    cls, document = cloud.rows.pop("authpublickey-manager")
    document["metadata"]["id"] = "publickey-e00example"
    cloud.rows["publickey-e00example"] = cls, document
    credentials = json.loads(cloud.material["loom-management-cloud"]["credentials.json"])
    credentials["subject-credentials"]["kid"] = "publickey-e00example"
    cloud.material["loom-management-cloud"]["credentials.json"] = json.dumps(credentials)
    assert (await qualify(cloud))["provisioning_account_id"] == "serviceaccount-manager"


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["tiny", "used", "versions", "parts", "negative", "deleting", "expiration"])
async def test_backup_bucket_requires_room_for_a_complete_dump_and_retains_it(cloud, mutation):
    from scripts.ops.nebius_management_cloud_scope import ManagementCloudScopeError

    bucket = cloud.rows["bucket-management"][1]
    bucket["spec"]["max_size_bytes"] = str(20 * 1024**3)
    if mutation == "tiny":
        bucket["spec"]["max_size_bytes"] = "1"
    elif mutation == "negative":
        bucket["spec"]["max_size_bytes"] = "-1"
    elif mutation == "deleting":
        bucket["status"]["deleted_at"] = "2026-09-25T00:00:00Z"
    elif mutation == "expiration":
        bucket["spec"]["lifecycle_configuration"] = {"rules": [{
            "id": "expire", "status": "ENABLED", "expiration": {"days": 1}}]}
    else:
        fields = {"used": ("counters", "simple_objects_size"),
                  "versions": ("non_current_counters", "multipart_objects_size"),
                  "parts": ("counters", "inflight_parts_size")}
        section, field = fields[mutation]
        bucket["status"]["counters"] = [{"storage_class": "STANDARD", section: {field: str(11 * 1024**3)}}]
    with pytest.raises(ManagementCloudScopeError):
        await qualify(cloud)


@pytest.mark.asyncio
async def test_backup_bucket_exact_headroom_includes_all_version_and_part_counters(cloud):
    bucket = cloud.rows["bucket-management"][1]
    bucket["spec"]["max_size_bytes"] = str(20 * 1024**3)
    bucket["status"]["counters"] = [{"storage_class": "STANDARD",
        "counters": {"simple_objects_size": str(2 * 1024**3), "multipart_objects_size": str(3 * 1024**3),
                     "inflight_parts_size": str(1 * 1024**3)},
        "non_current_counters": {"simple_objects_size": str(1 * 1024**3), "multipart_objects_size": str(3 * 1024**3)}}]
    assert (await qualify(cloud))["backup_bucket_id"] == "bucket-management"
