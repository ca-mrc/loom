from __future__ import annotations

import base64
import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.task_image_archives import (
    archive_write_intents,
    publish_image_archive,
    validate_archive_receipt,
)


def archive_claim():
    claim = {
        "id": str(uuid4()),
        "lease_epoch": 2,
        "source_bucket": "artifacts",
        "task_config": {
            "environment": {"native_compose": {"services": [{"name": "main"}, {"name": "db"}]}}
        },
    }
    claim["archive_write_intents"] = archive_write_intents(claim)
    return claim


class VersionedStorage:
    def __init__(self):
        self.versioning = "Enabled"
        self.objects = {}
        self.calls = []

    def get_bucket_versioning(self, **kwargs):
        return {"Status": self.versioning}

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        data = kwargs["Body"].read()
        version = str(uuid4())
        self.objects[version] = {
            "ContentLength": len(data),
            "Metadata": kwargs["Metadata"],
            "VersionId": version,
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(data).digest()).decode(),
        }
        return {"VersionId": version}

    def head_object(self, **kwargs):
        return self.objects[kwargs["VersionId"]]


def test_archive_publish_reuses_exact_issued_key_version_and_native_checksum(tmp_path):
    client = VersionedStorage()
    claim = archive_claim()
    path = tmp_path / "image.tar"
    path.write_bytes(b"prepared-oci-transport")
    image = "registry.example/image@sha256:" + "a" * 64
    archive = publish_image_archive(client, claim, component="task", image_ref=image, archive=path)
    assert archive.object.object_key == claim["archive_write_intents"]["task"]["object_key"]
    assert archive.object.content_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert (
        client.calls[0]["Metadata"]["loom-image-write-id"]
        == claim["archive_write_intents"]["task"]["write_id"]
    )
    assert client.calls[0]["ChecksumAlgorithm"] == "SHA256"
    assert (
        validate_archive_receipt(
            claim, {"task": image}, {"task": archive.model_dump(mode="json")}, complete=False
        )["task"]
        == archive
    )
    with pytest.raises(ValueError, match="issued components"):
        validate_archive_receipt(claim, {"task": image}, {"task": archive.model_dump(mode="json")})
    with pytest.raises(ValueError, match="native attempt"):
        validate_archive_receipt(
            claim,
            {"task": image.replace("a" * 64, "b" * 64)},
            {"task": archive.model_dump(mode="json")},
            complete=False,
        )


@pytest.mark.parametrize("damage", ["versioning", "foreign-key", "missing-intent"])
def test_archive_publication_fails_before_unjournaled_or_unversioned_put(tmp_path, damage):
    client = VersionedStorage()
    claim = archive_claim()
    path = tmp_path / "image.tar"
    path.write_bytes(b"oci")
    if damage == "versioning":
        client.versioning = "Suspended"
    if damage == "foreign-key":
        claim["archive_write_intents"]["task"]["object_key"] = "foreign/key"
    if damage == "missing-intent":
        del claim["archive_write_intents"]["task"]
    with pytest.raises(ValueError):
        publish_image_archive(
            client,
            claim,
            component="task",
            image_ref="registry/image@sha256:" + "a" * 64,
            archive=path,
        )
    assert not client.calls


@pytest.mark.asyncio
async def test_source_broker_checks_exact_lease_grant_and_prepared_service(monkeypatch):
    import loom_control_plane.service_execution_output as module
    from loom.execution_contract import nebius_guest_execution_class
    from loom.task_image_archives import PreparedImageArchiveV1
    from tests.unit.test_compose_image_inputs import _lease_for, prepared_plan

    task, trial, plan = prepared_plan()
    lease = _lease_for(plan, task, trial)
    grant = SimpleNamespace(
        materialization_id=plan.task_image_materialization_id,
        task_config=task.model_dump(mode="json"),
        image_archives={},
    )
    for name, service in [
        ("task", task.environment.native_compose.services[0]),
        ("compose:database", task.environment.native_compose.services[1]),
    ]:
        grant.image_archives[name] = PreparedImageArchiveV1.model_validate(
            {
                "image_ref": service.image,
                "object": {
                    "bucket": "artifacts",
                    "object_key": "owned/" + name.replace(":", "-"),
                    "version_id": "v1",
                    "content_sha256": "a" * 64,
                    "size_bytes": 3,
                },
            }
        )

    class Session:
        async def get(self, *args):
            return SimpleNamespace(spec_json=nebius_guest_execution_class().model_dump(mode="json"))

    async def resolve(*args, **kwargs):
        return grant

    monkeypatch.setattr(
        "loom_control_plane.service_execution_task_snapshot.resolve_service_execution_image_grant", resolve
    )
    monkeypatch.setattr("loom.task_image_materialization.resolve_prepared_task", lambda *_: task)
    result = await module.resolve_service_execution_image_archives(
        Session(), lease=lease, artifacts_bucket="artifacts"
    )
    assert set(result) == {"main", "database"}
    grant.materialization_id = uuid4()
    with pytest.raises(module.ServiceExecutionBrokerError, match="grant_drift"):
        await module.resolve_service_execution_image_archives(
            Session(), lease=lease, artifacts_bucket="artifacts"
        )
