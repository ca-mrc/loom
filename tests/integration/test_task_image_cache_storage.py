"""Cache safety against the real S3-compatible backend used in integration tests."""

import json
import subprocess
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_nebius_task_image_runtime import _write_native_oci, source_bundle  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.mark.parametrize("transfer", ["blobs", "tar"])
@pytest.mark.parametrize("oversized", [False, True])
def test_publication_preserves_cache_budget_with_real_storage(
    shared_minio, tmp_path, monkeypatch, capsys, source_bundle, transfer, oversized,  # noqa: F811
):
    config = shared_minio.get_config()

    def client():
        return boto3.client(
            "s3", endpoint_url="http://" + config["endpoint"],
            aws_access_key_id=config["access_key"], aws_secret_access_key=config["secret_key"],
            region_name="us-east-1",
        )

    admin = client()
    cache = client()
    bucket = "cache-budget-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    try:
        admin.put_object(Bucket=bucket, Key="retained", Body=b"existing")
        claim, _ = source_bundle
        claim.update(cache_bucket=bucket, cache_transfer=transfer)
        (tmp_path / "secrets/cache").mkdir(parents=True)
        monkeypatch.setattr(runtime, "_client", lambda *_args: cache)
        work = tmp_path / "work"
        (work / "oci").mkdir(parents=True)
        _write_native_oci(work / "oci/0000.tar")
        directory = work / "cache-out/0"
        directory.mkdir(parents=True)
        with (directory / "layer").open("wb") as output:
            # Sparse metadata-only overflow: exercises the real unchanged1GiB
            # boundary without allocating/uploading a GiB in this test.
            output.truncate(runtime._CACHE_BYTES + 1 if oversized else 32)
        registry_copies = []

        def copy(argv, **_kwargs):
            registry_copies.append(argv)
            Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + "b" * 64)
            return subprocess.CompletedProcess(argv, 0)

        # Storage is real; registry-copy admission remains the unit-tested
        # subprocess boundary. This does not claim a real registry push.
        monkeypatch.setattr(runtime.subprocess, "run", copy)
        receipt = runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
        assert receipt["registry_images"] == {"task": "registry.example/tasks@sha256:" + "b" * 64}
        assert len(registry_copies) == 1
        objects = {item["Key"] for item in admin.list_objects_v2(Bucket=bucket)["Contents"]}
        assert admin.get_object(Bucket=bucket, Key="retained")["Body"].read() == b"existing"
        if oversized:
            assert objects == {"retained"}
            assert '"reason":"byte_budget_exceeded"' in capsys.readouterr().out
        else:
            assert any(key.startswith("task-build-cache/") for key in objects)
            assert "byte_budget_exceeded" not in capsys.readouterr().out
    finally:
        for item in admin.list_objects_v2(Bucket=bucket).get("Contents", []):
            admin.delete_object(Bucket=bucket, Key=item["Key"])
        admin.delete_bucket(Bucket=bucket)
        admin.close()
        cache.close()


def test_unsupported_conditional_write_keeps_cache_append_only(shared_minio, tmp_path, capsys):
    config = shared_minio.get_config()
    client = boto3.client(
        "s3", endpoint_url="http://" + config["endpoint"],
        aws_access_key_id=config["access_key"], aws_secret_access_key=config["secret_key"],
        region_name="us-east-1",
    )
    bucket = "cache-" + uuid4().hex
    client.create_bucket(Bucket=bucket)
    try:
        # This pinned old MinIO silently ignores If-None-Match, rather than
        # returning NotImplemented. It must never be trusted as a mutex.
        assert not runtime._supports_conditional_cache_write(client, bucket)
        legacy_key = f"task-build-cache/{'b' * 64}/0.tar"
        with runtime.cache_request_counts(client, "probe"):
            client.put_object(Bucket=bucket, Key=legacy_key, Body=b"retained")
            client.head_object(Bucket=bucket, Key=legacy_key)
            client.list_objects_v2(Bucket=bucket)
            with client.get_object(Bucket=bucket, Key=legacy_key)["Body"] as body:
                assert body.read() == b"retained"
        events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert events[-1]["request_attempts"] == {
            "PutObject": 1, "HeadObject": 1, "ListObjectsV2": 1, "GetObject": 1,
        }
        runtime.trim_cache(client, bucket, runtime._CACHE_TOTAL_BYTES)
        assert client.get_object(Bucket=bucket, Key=legacy_key)["Body"].read() == b"retained"
        directory = tmp_path / "cache"
        directory.mkdir()
        (directory / "layer").write_bytes(b"layer")
        claim = {"cache_bucket": bucket, "materialization_key": "a" * 64}
        runtime._publish_cache_blobs(client, claim, index=0, cache_dir=directory)
        response = client.get_object(Bucket=bucket, Key=runtime._v2_manifest_key("a" * 64, 0))
        manifest = json.loads(response["Body"].read())
        for entry in manifest["files"]:
            blob = client.get_object(Bucket=bucket, Key=runtime._v2_blob_key(entry["sha256"]))
            assert len(blob["Body"].read()) == entry["size"]
        assert "conditional_write_unsupported" in capsys.readouterr().out
    finally:
        for item in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])
        client.delete_bucket(Bucket=bucket)
        client.close()
