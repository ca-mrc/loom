"""Real TLS MinIO evidence for late version recovery, not durable admission."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from loom.data_lifecycle_gc import RegisteredObject
from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.task_bundle_source_storage import (
    S3TaskBundleVersionInventory,
    TaskBundleObjectIntentV1,
    write_task_bundle_source_object,
)
from loom.trajectory.storage import MinioObjectStore
from tests.support.minio_tls import minio_tls  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


async def test_exact_version_reader_keeps_original_after_replacement_and_delete(minio_tls):  # noqa: F811
    module = importlib.import_module("loom.trajectory.immutable_object")
    admin = minio_tls[3]
    bucket = "exact-reader-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    original = admin.put_object(Bucket=bucket, Key="file", Body=b"abc")["VersionId"]
    admin.put_object(Bucket=bucket, Key="file", Body=b"replacement")
    admin.delete_object(Bucket=bucket, Key="file")
    before = admin.list_object_versions(Bucket=bucket)
    destination = io.BytesIO()
    await asyncio.to_thread(
        module.read_verified_object_version,
        admin,
        module.ImmutableObjectVersion(
            bucket=bucket, object_key="file", version_id=original,
            content_sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            size_bytes=3,
        ),
        max_bytes=3, destination=destination,
    )
    assert destination.getvalue() == b"abc"
    after = admin.list_object_versions(Bucket=bucket)
    assert after["Versions"] == before["Versions"]
    assert after["DeleteMarkers"] == before["DeleteMarkers"]


def _intent(bucket, key):
    return TaskBundleObjectIntentV1(
        id=uuid4(),
        bucket=bucket,
        object_key=key,
        content_sha256=hashlib.sha256(b"abc").hexdigest(),
        size_bytes=3,
    )


def _store(fixture):
    origin, credentials, _, admin, _ = fixture
    store = MinioObjectStore(
        endpoint_url=origin,
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
        operation_attempts=2,
        operation_timeout=2,
    )
    store._client.close()
    # Reuse the fixture's owned TLS CA configuration, never disable verification.
    store._client = admin
    return store


def _registered(intent, version):
    return RegisteredObject(
        id=uuid4(),
        authority_id=intent.id,
        environment="test",
        namespace="source-fixture",
        bucket=intent.bucket,
        object_key=intent.object_key,
        version_id=version,
        content_sha256=intent.content_sha256,
        size_bytes=intent.size_bytes,
        state="deleting",
    )


def _scan(reader, intent):
    batch = reader.scan_batch(intent)
    assert batch.observed_end
    return batch.versions


async def test_real_minio_strong_write_refuses_disabled_and_suspended_versioning(minio_tls):  # noqa: F811
    store = _store(minio_tls)
    admin = minio_tls[3]
    bucket = "source-versioning-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    intent = _intent(bucket, "bundle/file")
    with pytest.raises(ValueError, match="versioning"):
        await write_task_bundle_source_object(store, intent, b"abc")
    assert "Contents" not in admin.list_objects_v2(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    result = await write_task_bundle_source_object(store, intent, b"abc")
    assert result.version_id not in {None, "null"}
    versions = await asyncio.to_thread(_scan, S3TaskBundleVersionInventory(admin), intent)
    assert versions == (result,)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    with pytest.raises(ValueError, match="versioning"):
        await write_task_bundle_source_object(store, _intent(bucket, "other/file"), b"abc")
    # Ordinary legacy writes remain supported in a suspended bucket.
    assert await store.put_object(bucket=bucket, key="legacy/file", body=b"legacy")
    response = admin.get_object(Bucket=bucket, Key="legacy/file")["Body"]
    try:
        assert response.read() == b"legacy"
    finally:
        response.close()


async def test_late_retry_version_is_recovered_after_empty_scan_without_deleting_new_publish(
    minio_tls,  # noqa: F811
):
    store = _store(minio_tls)
    admin = minio_tls[3]
    bucket = "source-late-write-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    old, new = _intent(bucket, "bundle/task.toml"), _intent(bucket, "bundle/task.toml")
    started, release, completed = threading.Event(), threading.Event(), threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    late_write = None

    class DelayedClient:
        def get_bucket_versioning(self, **kwargs):
            return admin.get_bucket_versioning(**kwargs)

        def put_object(self, **kwargs):
            nonlocal late_write
            late_write = executor.submit(self.finish_put, kwargs)
            started.set()
            # The caller times out while the server-side write remains pending.
            # Inject that outcome without imposing a 100 ms deadline on real S3 I/O.
            raise TimeoutError("injected timeout before delayed PUT completes")

        def finish_put(self, kwargs):
            try:
                assert release.wait(30), "fixture did not release delayed PUT"
                return admin.put_object(**kwargs)
            finally:
                completed.set()

        def close(self):
            # A closed client cannot undo a request the server already accepted.
            # Keep this fixture-owned wrapper alive until its delayed write joins.
            pass

    store._client = DelayedClient()
    store._build_client = lambda: admin
    inventory, deleter = S3TaskBundleVersionInventory(admin), S3ExactObjectDeleter(admin)
    try:
        known = await write_task_bundle_source_object(store, old, b"abc")
        assert started.is_set() and not completed.is_set()
        # Only the retry's version is visible. Retire it and observe an empty scan.
        assert await asyncio.to_thread(_scan, inventory, old) == (known,)
        await asyncio.to_thread(deleter.delete_exact, _registered(old, known.version_id))
        assert await asyncio.to_thread(_scan, inventory, old) == ()
        replacement = await write_task_bundle_source_object(store, new, b"abc")
        assert replacement.version_id != known.version_id
        release.set()
        assert await asyncio.to_thread(completed.wait, 10)
        late = await asyncio.to_thread(_scan, inventory, old)
        assert len(late) == 1 and late[0].version_id not in {
            known.version_id,
            replacement.version_id,
        }
        assert (
            admin.head_object(Bucket=bucket, Key=old.object_key)["VersionId"] == late[0].version_id
        )
        await asyncio.to_thread(deleter.delete_exact, _registered(old, late[0].version_id))
        assert await asyncio.to_thread(_scan, inventory, old) == ()
        assert await asyncio.to_thread(_scan, inventory, new) == (replacement,)
        assert (
            admin.head_object(Bucket=bucket, Key=new.object_key)["VersionId"]
            == replacement.version_id
        )
    finally:
        release.set()
        try:
            if late_write is not None:
                await asyncio.to_thread(late_write.result, 10)
        finally:
            executor.shutdown(wait=True)


async def test_real_minio_resumes_bounded_inventory_across_foreign_versions_and_delete_marker(
    minio_tls,  # noqa: F811
):
    from loom.task_bundle_source_storage import TaskBundleInventoryCursorV1

    store = _store(minio_tls)
    admin = minio_tls[3]
    bucket = "source-pagination-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    ours, foreign = _intent(bucket, "bundle/task.toml"), _intent(bucket, "bundle/task.toml")
    expected = set()
    for intent in (ours, foreign, ours, foreign, ours):
        receipt = await write_task_bundle_source_object(store, intent, b"abc")
        if intent == ours:
            expected.add(receipt.version_id)
    # A delete marker is not an own data version and must not be fetched as one.
    marker = admin.delete_object(Bucket=bucket, Key=ours.object_key)["VersionId"]
    cursor, observed, seen = None, set(), set()
    for _ in range(10):
        reader = S3TaskBundleVersionInventory(admin, max_versions=1, max_read_bytes=3)
        batch = await asyncio.to_thread(reader.scan_batch, ours, cursor=cursor)
        observed.update(item.version_id for item in batch.versions)
        if batch.observed_end:
            break
        encoded = batch.continuation.model_dump_json()
        assert encoded not in seen
        seen.add(encoded)
        cursor = TaskBundleInventoryCursorV1.model_validate_json(encoded)
    else:
        pytest.fail("bounded inventory did not reach an observational end")
    assert len(seen) == 5
    assert observed == expected and marker not in observed
