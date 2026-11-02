"""Cache safety against the real S3-compatible backend used in integration tests."""

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from uuid import uuid4

import boto3
import pytest
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom_execution_actuator import task_image_runtime as runtime
from tests.support.minio_images import MINIO_TLS_IMAGE, prepare_test_image
from tests.unit.test_nebius_task_image_runtime import (  # noqa: F401
    _v2_cache_objects,
    _write_native_oci,
    source_bundle,
)

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.fixture(scope="module")
def conditional_minio():
    # The existing pinned 2025 fixture implements native conditional PUT. Keep
    # the old 2022 fixture below to cover servers which silently ignore it.
    with MinioContainer(prepare_test_image(MINIO_TLS_IMAGE)).waiting_for(
        HttpWaitStrategy(9000, "/minio/health/cluster"),
    ) as server:
        yield server


@pytest.fixture(params=["conditional_minio", "shared_minio"])
def cache_backend(request):
    return request.getfixturevalue(request.param), request.param == "conditional_minio"


@pytest.mark.parametrize("transfer", ["blobs", "tar"])
@pytest.mark.parametrize("oversized", [False, True])
def test_publication_preserves_cache_budget_with_real_storage(
    cache_backend, tmp_path, monkeypatch, capsys, source_bundle, transfer, oversized,  # noqa: F811
):
    server, supports_condition = cache_backend
    config = server.get_config()

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
            # Sparse metadata-only overflow: exercises the real 8 GiB
            # boundary without allocating/uploading that cache in this test.
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
        elif supports_condition:
            assert any(key.startswith("task-build-cache/") for key in objects)
            assert "byte_budget_exceeded" not in capsys.readouterr().out
        else:
            assert objects == {"retained"}
            assert '"reason":"conditional_write_unsupported"' in capsys.readouterr().out
    finally:
        for item in admin.list_objects_v2(Bucket=bucket).get("Contents", []):
            admin.delete_object(Bucket=bucket, Key=item["Key"])
        admin.delete_bucket(Bucket=bucket)
        admin.close()
        cache.close()


def test_unsupported_conditional_write_defers_cache_without_unbounded_growth(shared_minio, tmp_path, capsys):
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
        objects = {item["Key"] for item in client.list_objects_v2(Bucket=bucket).get("Contents", [])}
        assert objects == {legacy_key}
        assert "conditional_write_unsupported" in capsys.readouterr().out
    finally:
        for item in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])
        client.delete_bucket(Bucket=bucket)
        client.close()



def test_conditional_storage_reuses_published_blobs_and_batches_collection(conditional_minio, tmp_path, capsys):
    config = conditional_minio.get_config()
    client = boto3.client(
        "s3", endpoint_url="http://" + config["endpoint"],
        aws_access_key_id=config["access_key"], aws_secret_access_key=config["secret_key"],
        region_name="us-east-1",
    )
    bucket = "cache-batch-" + uuid4().hex
    client.create_bucket(Bucket=bucket)
    try:
        assert runtime._supports_conditional_cache_write(client, bucket)
        directory = tmp_path / "cache"
        directory.mkdir()
        (directory / "layer").write_bytes(b"shared layer")
        for index in range(8):
            with runtime.cache_request_counts(client, "publication", publication=index):
                runtime._publish_cache_blobs(
                    client, {"cache_bucket": bucket, "materialization_key": f"{index:064x}"},
                    index=0, cache_dir=directory,
                )
        events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        publications = [row for row in events if row.get("loom_task_image_stage") == "publication"]
        assert len(publications) == 8
        assert publications[0]["request_attempts"]["ListObjectsV2"] == 1
        assert all(row["request_attempts"].get("ListObjectsV2", 0) == 0 for row in publications[1:])
        transfers = [row for row in events if row.get("loom_task_image_stage") == "cache_export"
                     and row["event"] == "transfer"]
        assert len(transfers) == 8
        assert sum(row["uploaded_blob_bytes"] for row in transfers) == len(b"shared layer")
        assert all(row["reused_blob_bytes"] == len(b"shared layer") for row in transfers[1:])
        runtime._materialize_cache_blobs(
            client, {"cache_bucket": bucket}, materialization_key=f"{7:064x}",
            index=0, destination=tmp_path / "imported",
        )
        assert (tmp_path / "imported/layer").read_bytes() == b"shared layer"
        runtime.trim_cache(client, bucket, 0)
        state = json.loads(client.get_object(Bucket=bucket, Key=runtime._CACHE_GC_CHECKPOINT)["Body"].read())
        assert state["publications"] == state["debt_bytes"] == 0
        for index in range(8):
            manifest = json.loads(client.get_object(
                Bucket=bucket, Key=runtime._v2_manifest_key(f"{index:064x}", 0),
            )["Body"].read())
            for entry in manifest["files"]:
                client.head_object(Bucket=bucket, Key=runtime._v2_blob_key(entry["sha256"]))
    finally:
        for item in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=item["Key"])
        client.delete_bucket(Bucket=bucket)
        client.close()


def test_pressure_gc_can_race_reader_without_installing_partial_cache(conditional_minio, tmp_path):
    config = conditional_minio.get_config()
    def connect():
        return boto3.client('s3', endpoint_url='http://' + config['endpoint'],
            aws_access_key_id=config['access_key'], aws_secret_access_key=config['secret_key'], region_name='us-east-1')
    admin, reader = connect(), connect()
    bucket = 'cache-reader-race-' + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    ready, resume = Event(), Event()
    claim = {'cache_bucket': bucket, 'materialization_key': 'a' * 64, 'cache_gc_policy': 'budget-pressure'}
    directory = tmp_path / 'export'
    directory.mkdir()
    (directory / 'first').write_bytes(b'first verified bytes')
    (directory / 'second').write_bytes(b'second verified bytes')
    original = reader.get_object
    calls = 0
    def pause_second_blob(**kwargs):
        nonlocal calls
        if '/blobs/' in kwargs['Key']:
            calls += 1
            if calls == 2:
                ready.set()
                assert resume.wait(15)
        return original(**kwargs)
    reader.get_object = pause_second_blob
    verified = {}
    try:
        runtime._publish_cache_blobs(admin, claim, index=0, cache_dir=directory)
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(runtime._try_import_cache, reader, claim, index=0,
                work=tmp_path / 'work', verified=verified)
            try:
                assert ready.wait(15), 'reader did not reach the second blob'
                # GC completes while the unlocked reader is paused, including
                # recent roots. The reader must cold-fallback transactionally.
                runtime.trim_cache(admin, bucket, 0, total_bytes=0, gc_policy='budget-pressure')
                assert not admin.list_objects_v2(Bucket=bucket, Prefix='task-build-cache/v2/').get('Contents')
            finally:
                resume.set()
            pending.result(timeout=15)
        assert verified == {} and not (tmp_path / 'work/cache-in/0').exists()
        assert list((tmp_path / 'work/cache-in').iterdir()) == []
        # A later valid publication remains usable; no stale partial aliases.
        runtime._publish_cache_blobs(admin, claim, index=0, cache_dir=directory)
        runtime._try_import_cache(reader, claim, index=0, work=tmp_path / 'work', verified=verified)
        assert (tmp_path / 'work/cache-in/0/second').read_bytes() == b'second verified bytes'
        assert len(verified) == 2
    finally:
        resume.set()
        for item in admin.list_objects_v2(Bucket=bucket).get('Contents', []):
            admin.delete_object(Bucket=bucket, Key=item['Key'])
        admin.delete_bucket(Bucket=bucket)
        reader.close()
        admin.close()


def test_real_storage_excludes_gc_and_other_publishers_during_preaccount(
    conditional_minio, tmp_path, monkeypatch, capsys,
):
    config = conditional_minio.get_config()

    def client():
        return boto3.client(
            "s3", endpoint_url="http://" + config["endpoint"],
            aws_access_key_id=config["access_key"], aws_secret_access_key=config["secret_key"],
            region_name="us-east-1",
        )

    admin, publisher = client(), client()
    bucket = "cache-concurrent-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    directory = tmp_path / "cache"
    directory.mkdir()
    (directory / "layer").write_bytes(b"shared layer")
    entered, release = Event(), Event()
    original_checkpoint = runtime._write_cache_checkpoint

    def pause_after_durable_accounting(*args):
        original_checkpoint(*args)
        if not entered.is_set():
            entered.set()
            assert release.wait(10), "test publisher was not released"

    try:
        runtime._publish_cache_blobs(
            admin, {"cache_bucket": bucket, "materialization_key": "a" * 64},
            index=0, cache_dir=directory,
        )
        monkeypatch.setattr(runtime, "_write_cache_checkpoint", pause_after_durable_accounting)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                runtime._publish_cache_blobs, publisher,
                {"cache_bucket": bucket, "materialization_key": "b" * 64},
                index=0, cache_dir=directory,
            )
            try:
                assert entered.wait(10), "publisher did not reach its durable checkpoint"
                runtime.trim_cache(admin, bucket, 0)
                runtime._publish_cache_blobs(
                    admin, {"cache_bucket": bucket, "materialization_key": "c" * 64},
                    index=0, cache_dir=directory,
                )
                # Neither the collector nor a second publisher can mutate cache
                # data while the first publication is between account and write.
                objects = {item["Key"] for item in admin.list_objects_v2(Bucket=bucket)["Contents"]}
                assert runtime._CACHE_MUTATION_LOCK in objects
                assert runtime._v2_manifest_key("a" * 64, 0) in objects
                assert runtime._v2_manifest_key("b" * 64, 0) not in objects
                assert runtime._v2_manifest_key("c" * 64, 0) not in objects
            finally:
                release.set()
            future.result(timeout=10)
        runtime.trim_cache(admin, bucket, 0)
        runtime._materialize_cache_blobs(
            admin, {"cache_bucket": bucket}, materialization_key="b" * 64,
            index=0, destination=tmp_path / "imported-concurrent",
        )
        assert (tmp_path / "imported-concurrent/layer").read_bytes() == b"shared layer"
        state = json.loads(admin.get_object(Bucket=bucket, Key=runtime._CACHE_GC_CHECKPOINT)["Body"].read())
        assert state["publications"] == state["debt_bytes"] == 0
        assert "cache_mutation_busy" in capsys.readouterr().out
    finally:
        release.set()
        for item in admin.list_objects_v2(Bucket=bucket).get("Contents", []):
            admin.delete_object(Bucket=bucket, Key=item["Key"])
        admin.delete_bucket(Bucket=bucket)
        admin.close()
        publisher.close()


@pytest.mark.parametrize('budget', [800, 10000])
def test_real_storage_resumes_mid_list_page_and_graph_with_no_retained_lock(
    conditional_minio, monkeypatch, budget,
):
    config = conditional_minio.get_config()
    admin = boto3.client('s3', endpoint_url='http://' + config['endpoint'],
        aws_access_key_id=config['access_key'], aws_secret_access_key=config['secret_key'], region_name='us-east-1')
    bucket = 'cache-resume-' + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    objects = {}
    for index in range(7):
        objects.update(_v2_cache_objects(f'{index:064x}', 0, {'layer': b'shared' * 16}))
    class Paged:
        def __getattr__(self, name):
            return getattr(admin, name)
        def get_paginator(self, name):
            paginator = admin.get_paginator(name)
            class Pages:
                def paginate(self, **kwargs):
                    return paginator.paginate(**kwargs, PaginationConfig={'PageSize': 2})
            return Pages()
    try:
        for key, body in objects.items():
            admin.put_object(Bucket=bucket, Key=key, Body=body)
        monkeypatch.setattr(runtime, '_CACHE_GC_MAX_OBJECTS', 5)
        monkeypatch.setattr(runtime, '_CACHE_GC_MAX_MANIFESTS', 2)
        monkeypatch.setattr(runtime, '_CACHE_GC_ROOT_BATCH', 2)
        phases = set()
        for _ in range(200):
            runtime.trim_cache(Paged(), bucket, 0, total_bytes=budget, gc_policy='budget-pressure')
            state = runtime._read_cache_checkpoint(admin, bucket)
            current = {row['Key']: row['Size'] for page in admin.get_paginator('list_objects_v2').paginate(Bucket=bucket)
                       for row in page.get('Contents', [])}
            assert runtime._CACHE_MUTATION_LOCK not in current
            if state['version'] == 1:
                break
            phases.add(state['scan']['phase'])
            assert state['inventory_complete'] is False and 'retry_after' not in state
        else:
            pytest.fail('real S3 continuation did not converge')
        expected = {key: size for key, size in current.items()
                    if runtime._V2_MANIFEST.fullmatch(key) or runtime._V2_BLOB.fullmatch(key)}
        assert state['retained_bytes'] == sum(expected.values()) <= budget
        assert not any(key.startswith(runtime._CACHE_GC_CONTROL) for key in current)
        roots = [key for key in expected if runtime._V2_MANIFEST.fullmatch(key)]
        assert len(roots) == (5 if budget == 800 else 7)
        assert any(runtime._V2_BLOB.fullmatch(key) for key in expected)
        assert 'list' in phases
        if budget == 800:
            assert phases & {'load', 'refs'}
    finally:
        for page in admin.get_paginator('list_objects_v2').paginate(Bucket=bucket):
            for row in page.get('Contents', []):
                admin.delete_object(Bucket=bucket, Key=row['Key'])
        admin.delete_bucket(Bucket=bucket)
        admin.close()
