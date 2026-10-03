"""Byte preservation and cleanup at the real store's S3 transport boundary."""
from __future__ import annotations

import asyncio
import io
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.exceptions import ConnectionClosedError

from loom.trajectory.storage import MinioObjectStore


class StreamS3:
    def __init__(self, *, fail_first=False, block_first=False):
        self.meta = SimpleNamespace(events=Mock())
        self.objects = {}
        self.parts = {}
        self.fail_first = fail_first
        self.block_first = block_first
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.late_body = None
        self.uploads = 0
        self.active_uploads = set()
        self.block_create = False
        self.aborted = threading.Event()

    def close(self):
        pass

    def consume(self, body):
        reader = io.BytesIO(body) if isinstance(body, bytes) else body
        if self.fail_first or self.block_first:
            block = self.block_first
            self.fail_first = self.block_first = False
            prefix = reader.read(2)
            if block:
                self.entered.set()
                try:
                    assert self.release.wait(5)
                    self.late_body = prefix + reader.read()
                finally:
                    self.finished.set()
            raise ConnectionClosedError(endpoint_url="http://test-s3")
        return reader.read()

    def put_object(self, *, Bucket, Key, Body, **kwargs):  # noqa: N803
        data = self.consume(Body)
        assert len(data) <= 8 * 1024**2, "unbounded single-request upload"
        self.objects[Bucket, Key] = data
        return {}

    def create_multipart_upload(self, **kwargs):
        if self.block_create:
            self.entered.set()
            assert self.release.wait(5)
        self.uploads += 1
        self.active_uploads.add(str(self.uploads))
        return {"UploadId": str(self.uploads)}

    def upload_part(self, *, UploadId, PartNumber, Body, **kwargs):  # noqa: N803
        data = self.consume(Body)
        assert len(data) <= 8 * 1024**2, "unbounded multipart request"
        self.parts[UploadId, PartNumber] = data
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
        rows = MultipartUpload["Parts"]
        assert [r["PartNumber"] for r in rows] == list(range(1, len(rows) + 1))
        assert all(len(self.parts[UploadId, r["PartNumber"]]) >= 5 * 1024**2 for r in rows[:-1])
        self.objects[Bucket, Key] = b"".join(self.parts.pop((UploadId, r["PartNumber"])) for r in rows)
        self.active_uploads.remove(UploadId)
        return {}

    def abort_multipart_upload(self, *, UploadId, **kwargs):  # noqa: N803
        self.parts = {k: v for k, v in self.parts.items() if k[0] != UploadId}
        self.active_uploads.discard(UploadId)
        self.aborted.set()


def make_store(monkeypatch, client):
    monkeypatch.setattr("loom.trajectory.storage.boto3.client", lambda **kwargs: client)
    return MinioObjectStore(endpoint_url="http://test-s3", access_key="test", secret_key="test")


class VersionlessCompletionS3(StreamS3):
    """S3-compatible completion omits versions; HEAD still returns identity."""

    def __init__(self, *, head_version="version-owned", replace=False, completion_version=None):
        super().__init__()
        self.head_version = head_version
        self.replace = replace
        self.completion_version = completion_version
        self.upload_metadata = {}
        self.metadata = {}
        self.head_calls = 0

    def create_multipart_upload(self, **kwargs):
        response = super().create_multipart_upload(**kwargs)
        self.upload_metadata[response["UploadId"]] = dict(kwargs.get("Metadata", {}))
        return response

    def complete_multipart_upload(self, **kwargs):
        super().complete_multipart_upload(**kwargs)
        self.metadata[kwargs["Bucket"], kwargs["Key"]] = self.upload_metadata[kwargs["UploadId"]]
        if self.replace:
            # Even identical bytes/ETag cannot bind a different writer's object.
            self.metadata[kwargs["Bucket"], kwargs["Key"]] = {"foreign-write": "other"}
        response = {"ETag": '"same-content-etag"'}
        if self.completion_version is not None:
            response["VersionId"] = self.completion_version
        return response

    def head_object(self, *, Bucket, Key):  # noqa: N803
        self.head_calls += 1
        assert self.completion_version is None, "explicit write versions need no current HEAD"
        response = {"ContentLength": len(self.objects[Bucket, Key]),
                    "ETag": '"same-content-etag"', "Metadata": self.metadata[Bucket, Key]}
        if self.head_version is not None:
            response["VersionId"] = self.head_version
        return response


@pytest.mark.parametrize("version", ["version-owned", None], ids=["versioned", "unversioned"])
async def test_stream_completion_recovers_only_its_own_readback_version(monkeypatch, version):
    backend = VersionlessCompletionS3(head_version=version)
    store = make_store(monkeypatch, backend)
    payload = b"same-upload" * (1024**2)
    result = await store.put_object_stream_with_metadata(bucket="b", key="k", body=chunks(payload))
    assert result.version_id == version
    assert result.uri == "s3://b/k" and backend.objects["b", "k"] == payload
    assert backend.head_calls == 1
    assert not backend.active_uploads


async def test_stream_completion_rejects_readback_from_competing_same_content_write(monkeypatch):
    backend = VersionlessCompletionS3(replace=True)
    store = make_store(monkeypatch, backend)
    with pytest.raises(ValueError, match="identity"):
        await store.put_object_stream_with_metadata(bucket="b", key="k", body=chunks(b"v" * (9 * 1024**2)))
    assert backend.head_calls == 1


@pytest.mark.parametrize("version", ["", " padded ", 3, False, {}])
async def test_stream_completion_rejects_malformed_readback_version(monkeypatch, version):
    backend = VersionlessCompletionS3(head_version=version)
    store = make_store(monkeypatch, backend)
    with pytest.raises(ValueError, match="malformed VersionId"):
        await store.put_object_stream_with_metadata(bucket="b", key="k", body=chunks(b"v" * (9 * 1024**2)))


async def test_stream_completion_preserves_explicit_version_without_head(monkeypatch):
    backend = VersionlessCompletionS3(completion_version="completed-version")
    store = make_store(monkeypatch, backend)
    result = await store.put_object_stream_with_metadata(bucket="b", key="k", body=chunks(b"v" * (9 * 1024**2)))
    assert result.version_id == "completed-version" and backend.head_calls == 0


async def chunks(*values):
    for value in values:
        yield value


@pytest.mark.parametrize("size", [7, 9 * 1024**2], ids=["small", "multipart"])
@pytest.mark.parametrize("version", [None, "", " padded ", 3, False, {}])
async def test_stream_write_rejects_malformed_version_evidence(monkeypatch, size, version):
    class MalformedVersionS3(StreamS3):
        def put_object(self, **kwargs):
            super().put_object(**kwargs)
            return {"VersionId": version}

        def complete_multipart_upload(self, **kwargs):
            super().complete_multipart_upload(**kwargs)
            return {"VersionId": version}

    store = make_store(monkeypatch, MalformedVersionS3())
    with pytest.raises(ValueError, match="malformed VersionId"):
        await store.put_object_stream_with_metadata(bucket="b", key="k", body=chunks(b"v" * size))


@pytest.mark.parametrize("payload", [b"complete payload", b"x" * (9 * 1024**2)], ids=["small", "multipart"])
async def test_stream_retry_preserves_bytes_after_partial_read(monkeypatch, payload):
    client = StreamS3(fail_first=True)
    store = make_store(monkeypatch, client)
    await store.put_object_stream(bucket="b", key="k", body=chunks(payload))
    assert client.objects["b", "k"] == payload


async def test_stream_timeout_does_not_share_or_close_a_late_reader(monkeypatch):
    client = StreamS3(block_first=True)
    store = make_store(monkeypatch, client)
    store._operation_timeout = 0.1
    task = asyncio.create_task(store.put_object_stream(bucket="b", key="k", body=chunks(b"complete payload")))
    try:
        assert await asyncio.to_thread(client.entered.wait, 5)
        await task
    finally:
        client.release.set()
        assert await asyncio.to_thread(client.finished.wait, 5)
    assert client.objects["b", "k"] == b"complete payload"
    assert client.late_body == b"complete payload"


@pytest.mark.parametrize("size", [0, 7, 8 * 1024**2, 8 * 1024**2 + 3, 17 * 1024**2])
async def test_stream_preserves_bytes_with_bounded_s3_requests(monkeypatch, size):
    client = StreamS3()
    store = make_store(monkeypatch, client)
    payload = (b"0123456789abcdef" * ((size + 15) // 16))[:size]
    await store.put_object_stream(bucket="b", key="k", body=chunks(payload[:7], b"", payload[7:]))
    assert client.objects["b", "k"] == payload
    assert not client.parts


@pytest.mark.parametrize("failure", [ValueError, asyncio.CancelledError])
async def test_stream_failure_aborts_partial_upload_and_preserves_prior_object(monkeypatch, failure):
    client = StreamS3()
    client.objects["b", "k"] = b"previous complete object"
    store = make_store(monkeypatch, client)

    async def source():
        yield b"x" * (9 * 1024**2)
        assert client.parts, "stream should upload bounded parts before source ends"
        raise failure("interrupted source")

    with pytest.raises(failure, match="interrupted source"):
        await store.put_object_stream(bucket="b", key="k", body=source())
    assert not client.parts
    assert client.objects["b", "k"] == b"previous complete object"


@pytest.mark.parametrize("payload", [b"abcdefgh", b"abcd" * (3 * 1024**2)], ids=["small", "multipart"])
async def test_stream_accepts_bytes_like_chunks_without_changing_bytes(monkeypatch, payload):
    client = StreamS3()
    store = make_store(monkeypatch, client)
    await store.put_object_stream(bucket="b", key="k", body=chunks(
        bytearray(b"begin"), memoryview(payload).cast("I"), b"end",
    ))
    assert client.objects["b", "k"] == b"begin" + payload + b"end"


async def test_stream_refuses_exhausted_part_inventory_and_aborts(monkeypatch):
    client = StreamS3()
    store = make_store(monkeypatch, client)
    monkeypatch.setattr("loom.trajectory.storage._S3_MAX_UPLOAD_PARTS", 2)
    with pytest.raises(ValueError, match="part limit"):
        await store.put_object_stream(bucket="b", key="k", body=chunks(b"x" * (16 * 1024**2 + 1)))
    assert not client.objects and not client.parts


async def test_stream_rejects_non_bytes_after_started_upload_and_aborts(monkeypatch):
    client = StreamS3()
    store = make_store(monkeypatch, client)
    with pytest.raises(TypeError, match="bytes-like"):
        await store.put_object_stream(bucket="b", key="k", body=chunks(b"x" * (9 * 1024**2), "bad"))
    assert not client.objects and not client.parts


@pytest.mark.parametrize("cancel", [True, False], ids=["cancelled", "timed-out"])
async def test_stream_reclaims_upload_created_after_initiation_was_abandoned(monkeypatch, cancel):
    client = StreamS3()
    client.block_create = True
    store = make_store(monkeypatch, client)
    store._operation_timeout = 0.1
    task = asyncio.create_task(store.put_object_stream(
        bucket="b", key="k", body=chunks(b"x" * (9 * 1024**2)),
    ))
    try:
        assert await asyncio.to_thread(client.entered.wait, 5)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
    finally:
        client.release.set()
    assert await asyncio.to_thread(client.aborted.wait, 5)
    assert not client.objects and not client.active_uploads
