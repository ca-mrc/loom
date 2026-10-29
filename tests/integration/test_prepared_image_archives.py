"""Real versioned storage for prepared OCI transport and interrupted writes."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import boto3
import pytest
from botocore.config import Config
from starlette.requests import Request

from loom.task_image_archives import publish_image_archive
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.task_image_archive_retention import (
    delete_archive_version,
    recover_archive_versions,
)
from tests.support.minio_tls import minio_tls  # noqa: F401
from tests.unit.test_nebius_task_image_runtime import (
    _eight_component_archive_claim,
    _write_native_oci,
    source_bundle,  # noqa: F401
)
from tests.unit.test_prepared_image_archives import archive_claim

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def test_eight_component_publication_keeps_all_real_archive_versions_in_termination_transport(
    minio_tls, source_bundle, tmp_path, monkeypatch,  # noqa: F811
):
    from loom.task_image_archives import validate_archive_receipt
    from loom_execution_actuator import task_image_runtime as runtime
    from loom_execution_actuator.image_publication_receipt import decode_publication_receipt
    from loom_execution_actuator.task_image_controller import _publication_document
    from tests.unit.test_nebius_task_image_controller import receipt_job

    origin, credentials, _, admin, cert_file = minio_tls
    claim = _eight_component_archive_claim(source_bundle)
    bucket = claim["source_bucket"]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    storage = boto3.client("s3", endpoint_url=origin, aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key, region_name="us-east-1", verify=str(cert_file),
        config=Config(signature_version="s3v4", proxies={}, retries={"max_attempts": 0}))
    monkeypatch.setattr(runtime, "_client", lambda *_args: storage)
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    components = runtime.derive_task_image_build_components(claim["task_config"])
    for index, component in enumerate(components):
        _write_native_oci(work / component.oci_output_path, runtime_config={"User": "0:0", "WorkingDir": "/app",
            "Env": ["HOME=/root", "PATH=/usr/bin", f"PUBLIC_COMPONENT={index}"]})
    copies = []

    def copy(argv, **_kwargs):
        copies.append(argv)
        Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + format(len(copies), "064x"))
        return subprocess.CompletedProcess(argv, 0)

    # Versioned PUT/HEAD/checksums are real. The registry process boundary is
    # stubbed; this test does not claim a live registry push or task execution.
    monkeypatch.setattr(runtime.subprocess, "run", copy)
    receipt_path = tmp_path / "termination-message"
    try:
        receipt = runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_path)
        wire = receipt_path.read_text()
        assert len(copies) == len(receipt["registry_images"]) == 8
        assert len(receipt["image_archives"]) == 7 and len(receipt["image_runtime_metadata"]) == 1
        assert len(json.dumps(receipt).encode()) > 4000 and len(wire.encode()) <= 4000
        assert decode_publication_receipt(wire) == receipt
        _, job = receipt_job(message=wire)
        decoded = _publication_document(job, materialization_id=UUID(claim["id"]), lease_epoch=claim["lease_epoch"])
        assert decoded == receipt
        assert len(validate_archive_receipt(claim, decoded["registry_images"], decoded["image_archives"])) == 7
        assert len(admin.list_object_versions(Bucket=bucket).get("Versions", [])) == 7
        decoded["image_archives"]["task"]["object"]["object_key"] = "foreign/other.tar"
        with pytest.raises(ValueError, match="native attempt"):
            validate_archive_receipt(claim, decoded["registry_images"], decoded["image_archives"])
    finally:
        versions = admin.list_object_versions(Bucket=bucket)
        for item in versions.get("Versions", []) + versions.get("DeleteMarkers", []):
            admin.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
        admin.delete_bucket(Bucket=bucket)


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
