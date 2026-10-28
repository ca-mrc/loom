"""A logical-limit cache must survive tar overhead through real TLS storage."""
from uuid import uuid4

import pytest

from loom_execution_actuator import task_image_runtime as runtime
from tests.support.minio_tls import minio_tls  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def test_exact_logical_cache_tar_export_storage_download_and_import(minio_tls, tmp_path, monkeypatch):  # noqa: F811
    _, _, _, client, _ = minio_tls
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 512)
    cache = tmp_path / "cache"
    long_name = "long-" + "p" * 120
    (cache / long_name).mkdir(parents=True)
    (cache / long_name / ("é" * 60)).write_bytes(b"a")
    (cache / "layer").write_bytes(b"b" * 511)
    (cache / "empty").mkdir()
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    assert archive.stat().st_size > runtime._CACHE_BYTES
    bucket = "cache-tar-" + uuid4().hex
    key = f"task-build-cache/{'a' * 64}/0.tar"
    client.create_bucket(Bucket=bucket)
    try:
        client.upload_file(str(archive), bucket, key)
        assert runtime._try_import_legacy_tar(client, {"cache_bucket": bucket}, source="exact",
            key="a" * 64, index=0, work=tmp_path / "work", archive_parent=tmp_path / "download")
        restored = tmp_path / "work/cache-in/0"
        assert (restored / long_name / ("é" * 60)).read_bytes() == b"a"
        assert (restored / "layer").read_bytes() == b"b" * 511
        assert (restored / "empty").is_dir()
    finally:
        client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)
