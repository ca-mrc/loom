"""API admission uses frozen source bytes and creates no work on incompatibility."""
import hashlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from botocore.exceptions import ClientError
from pydantic import SecretStr
from sqlalchemy import func, select

from loom.db.schema import Batch, Task, TaskImageMaterialization, Trial
from loom.service_execution_materialization import ServiceExecutionInputManifestV1
from loom_service.storage import create_minio_client
from tests.integration.test_service_batch_resource_requests import (
    native_resource_batch as native_resource_batch,
)
from tests.integration.test_service_batches_crud import camp_setup as camp_setup


@pytest.mark.parametrize("base,corrupt,storage_error,expected", [
    ("opensuse/leap:15.5", False, None, 201),
    ("opensuse/leap:15.6", False, None, 400),
    ("debian:bookworm", True, None, 400),
    ("debian:bookworm", False, "SlowDown", 503),
    ("debian:bookworm", False, "InvalidAccessKeyId", 400),
])
async def test_submission_checks_primary_image_before_creating_work(native_resource_batch, shared_minio, monkeypatch, base, corrupt, storage_error, expected):
    f = native_resource_batch
    app = f["app"]
    storage = shared_minio.get_config()
    settings = app.state.settings.model_copy(update={
        "minio_endpoint": f"http://{storage['endpoint']}",
        "minio_access_key": SecretStr(storage["access_key"]),
        "minio_secret_key": SecretStr(storage["secret_key"]),
    })
    app.state.settings = settings
    client = create_minio_client(settings, endpoint_url=settings.minio_endpoint)
    bucket = settings.artifacts_bucket
    if not any(row["Name"] == bucket for row in client.list_buckets()["Buckets"]):
        client.create_bucket(Bucket=bucket)
    task_id = f["task_ids"][0]
    prefix = f"admission-fixture/{task_id.rsplit('/', 1)[-1]}/"
    dockerfile = f"FROM {base}\nWORKDIR /app\n".encode()
    manifest = ServiceExecutionInputManifestV1(task_revision_sha256="sha256:" + "c" * 64,
        files=[dict(relative_path="environment/Dockerfile", mode="0644", size_bytes=len(dockerfile),
                    sha256="sha256:" + hashlib.sha256(dockerfile).hexdigest())])
    manifest_bytes = manifest.canonical_bytes()
    objects = {prefix + "manifest.json": manifest_bytes,
               prefix + "task/environment/Dockerfile": dockerfile + (b"# changed\n" if corrupt else b"")}
    for key, body in objects.items():
        client.put_object(Bucket=bucket, Key=key, Body=body)
    if storage_error is not None:
        failing_client = SimpleNamespace(get_object=Mock(side_effect=ClientError(
            {"Error": {"Code": storage_error}}, "GetObject")), close=Mock())
        monkeypatch.setattr("loom_service.task_image_admission.create_minio_client", lambda *a, **kw: failing_client)
    try:
        with f["sessions"]() as session:
            row = session.get(Task, task_id)
            task_config = deepcopy(row.config)
            task_config["environment"].pop("docker_image", None)
            task_config["environment"]["dockerfile"] = "environment/Dockerfile"
            row.config = task_config
            row.source = f"s3://{bucket}/{prefix}task/"
            row.source_provenance = {"service_execution_input": dict(
                manifest_uri=f"s3://{bucket}/{prefix}manifest.json",
                manifest_sha256="sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
                file_count=1, total_bytes=len(dockerfile))}
            session.commit()
            before = [session.scalar(select(func.count()).select_from(table))
                      for table in (Batch, Trial, TaskImageMaterialization)]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as http:
            result = await http.post("/api/v1/batches", headers={"Authorization": "Bearer " + f["raw"]}, json=f["payload"])
        assert result.status_code == expected, result.text
        with f["sessions"]() as session:
            if expected != 201:
                after = [session.scalar(select(func.count()).select_from(table))
                         for table in (Batch, Trial, TaskImageMaterialization)]
                assert after == before
                reason = ("image_preparation_source_unavailable" if storage_error == "SlowDown"
                          else "image_preparation_source_denied" if storage_error is not None
                          else "image_preparation_input_invalid" if corrupt
                          else "image_preparation_base_unsupported")
                assert result.json()["detail"]["rejection_reasons"][task_id] == [reason]
            row = session.get(Task, task_id)
            assert row.config == task_config and row.checksum == "c" * 64
    finally:
        for key in objects:
            client.delete_object(Bucket=bucket, Key=key)
        client.close()
