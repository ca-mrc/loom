"""Real versioned storage for prepared OCI transport and interrupted writes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from starlette.requests import Request

from loom.task_image_archives import publish_image_archive
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.task_image_archive_retention import (
    delete_archive_version,
    recover_archive_versions,
)
from tests.support.minio_tls import minio_tls  # noqa: F401
from tests.unit.test_prepared_image_archives import archive_claim

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.mark.parametrize("metadata_case", ["native", "nebius-title"])
async def test_exact_prepared_archive_survives_latest_overwrite_and_receipt_loss(
    minio_tls, tmp_path, monkeypatch, metadata_case, request  # noqa: F811
):
    from loom_llm_gateway.routes import service_execution as routes

    origin, credentials, _, admin, _ = minio_tls

    # Keep real versioned PUT/HEAD/GET/delete and SDK checksum handling; replay
    # the observed Nebius HEAD metadata representation at the SDK boundary.
    def title_case_metadata(parsed, **_kwargs):
        if "Metadata" in parsed:
            parsed["Metadata"] = {key.title(): value for key, value in parsed["Metadata"].items()}

    if metadata_case == "nebius-title":
        admin.meta.events.register("after-call.s3.HeadObject", title_case_metadata)
        request.addfinalizer(
            lambda: admin.meta.events.unregister("after-call.s3.HeadObject", title_case_metadata)
        )
    bucket = "prepared-images-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    claim = archive_claim()
    claim["source_bucket"] = bucket
    archive = tmp_path / "prepared.tar"
    archive.write_bytes(b"immutable prepared OCI bytes")
    image = "registry.example/task@sha256:" + "a" * 64
    # Simulate process death immediately after successful PUT: the caller loses
    # its receipt; recovery is derived solely from the pre-PUT native claim.
    publish_image_archive(admin, claim, component="task", image_ref=image, archive=archive)
    recovered = recover_archive_versions(admin, claim, bucket=bucket)
    assert len(recovered) == 1
    expected = recovered[0]
    admin.put_object(
        Bucket=bucket,
        Key=expected.object.object_key,
        Body=b"foreign latest version",
        Metadata={"loom-image-write-id": "unrelated"},
    )
    store = MinioObjectStore(
        endpoint_url=origin, access_key=credentials.access_key, secret_key=credentials.secret_key
    )
    store._client.close()
    store._client = admin

    async def authorized(*args, **kwargs):
        return {"main": expected}

    monkeypatch.setattr(routes, "_prepared_image_archives", authorized)
    app = SimpleNamespace(state=SimpleNamespace(artifact_store=store))
    request = Request({"type": "http", "app": app, "headers": []})
    response = await routes.get_prepared_image_archive("main", request, uuid4(), 1, "attempt")
    result = b"".join([chunk async for chunk in response.body_iterator])
    assert result == archive.read_bytes()
    # Exact retirement is restartable and never deletes the foreign latest key.
    await asyncio.to_thread(delete_archive_version, admin, expected)
    await asyncio.to_thread(delete_archive_version, admin, expected)
    assert (
        admin.get_object(Bucket=bucket, Key=expected.object.object_key)["Body"].read()
        == b"foreign latest version"
    )
    assert not recover_archive_versions(admin, claim, bucket=bucket)
