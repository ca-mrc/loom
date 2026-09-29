"""Cache safety against the real S3-compatible backend used in integration tests."""

import json
from uuid import uuid4

import boto3

from loom_execution_actuator import task_image_runtime as runtime


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
