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
        self.uploads += 1
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
        return {}

    def abort_multipart_upload(self, *, UploadId, **kwargs):  # noqa: N803
        self.parts = {k: v for k, v in self.parts.items() if k[0] != UploadId}


def make_store(monkeypatch, client):
    monkeypatch.setattr("loom.trajectory.storage.boto3.client", lambda **kwargs: client)
    return MinioObjectStore(endpoint_url="http://test-s3", access_key="test", secret_key="test")


async def chunks(*values):
    for value in values:
        yield value


async def test_stream_retry_preserves_bytes_after_partial_read(monkeypatch):
    client = StreamS3(fail_first=True)
    store = make_store(monkeypatch, client)
    await store.put_object_stream(bucket="b", key="k", body=chunks(b"complete payload"))
    assert client.objects["b", "k"] == b"complete payload"


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
