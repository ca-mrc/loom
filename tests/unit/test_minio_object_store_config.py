import asyncio
import threading
import time
from types import SimpleNamespace

import botocore.handlers
import pytest
from botocore.exceptions import ClientError

from loom.trajectory.storage import MinioObjectStore, _remove_expect_header


class _IntentWriteClient:
    def __init__(self, *, status="Enabled", version="version-1"):
        self.status = status
        self.version = version
        self.calls = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def get_bucket_versioning(self, **kwargs):
        self.calls.append(("versioning", kwargs))
        self.entered.set()
        assert self.release.wait(10)
        return {"Status": self.status} if self.status is not None else {}

    def put_object(self, **kwargs):
        self.calls.append(("put", kwargs))
        return {"VersionId": self.version} if self.version is not None else {}


@pytest.mark.parametrize("status", [None, "", "Suspended", "invalid"])
async def test_strong_put_refuses_non_enabled_versioning_before_any_write(status):
    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    client = _IntentWriteClient(status=status)
    store._client = client
    with pytest.raises(ValueError, match="versioning"):
        await store.put_object_with_metadata(
            bucket="sources", key="task/file", body=b"abc",
            metadata={"loom-source-write-id": "intent-1"}, require_versioning=True,
        )
    assert [name for name, _ in client.calls] == ["versioning"]


@pytest.mark.parametrize("version", [None, "null"])
async def test_strong_put_refuses_unversioned_receipt_without_legacy_regression(version):
    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    client = _IntentWriteClient(version=version)
    store._client = client
    with pytest.raises(ValueError, match="immutable object version"):
        await store.put_object_with_metadata(
            bucket="sources", key="task/file", body=b"abc", require_versioning=True,
        )
    result = await store.put_object_with_metadata(bucket="sources", key="legacy/file", body=b"abc")
    assert result.version_id == version
    assert "Metadata" not in client.calls[-1][1]


async def test_strong_put_snapshots_metadata_before_async_preflight():
    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    client = _IntentWriteClient()
    client.release.clear()
    store._client = client
    metadata = {"loom-source-write-id": "intent-1"}
    task = asyncio.create_task(store.put_object_with_metadata(
        bucket="sources", key="task/file", body=b"abc", metadata=metadata, require_versioning=True,
    ))
    try:
        assert await asyncio.to_thread(client.entered.wait, 5)
        metadata["loom-source-write-id"] = "changed"
    finally:
        client.release.set()
        result = await task
    assert result.version_id == "version-1"
    assert client.calls[-1][1]["Metadata"] == {"loom-source-write-id": "intent-1"}


@pytest.mark.parametrize("metadata", [{"Bad-Key": "v"}, {"key": "bad\nvalue"}, {"key": "x" * 2049}])
async def test_put_rejects_invalid_intent_metadata_before_io(metadata):
    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    client = _IntentWriteClient()
    store._client = client
    with pytest.raises(ValueError, match="metadata"):
        await store.put_object_with_metadata(bucket="sources", key="task/file", body=b"abc", metadata=metadata)
    assert client.calls == []


async def test_strong_write_rechecks_versioning_on_retry_with_the_same_intent_metadata():
    from botocore.exceptions import ConnectionClosedError

    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    first, second = _IntentWriteClient(), _IntentWriteClient(status="Suspended")
    attempts = []

    def fail_write(**kwargs):
        attempts.append(kwargs)
        raise ConnectionClosedError(endpoint_url="http://disposable-invalid")

    first.put_object = fail_write
    store._client = first
    store._build_client = lambda: second
    with pytest.raises(ValueError, match="versioning"):
        await store.put_object_with_metadata(bucket="sources", key="task/file", body=b"abc",
            metadata={"loom-source-write-id": "intent-1"}, require_versioning=True)
    assert attempts[0]["Metadata"] == {"loom-source-write-id": "intent-1"}
    assert [name for name, _ in second.calls] == ["versioning"]


@pytest.mark.parametrize("backend", ["fake", "local"])
async def test_nonversioned_backends_refuse_strong_write_without_creating_objects(backend, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_cli.local_object_store import LocalDiskObjectStore

    store = FakeObjectStore() if backend == "fake" else LocalDiskObjectStore(root=tmp_path)
    with pytest.raises(ValueError, match="versioning"):
        await store.put_object_with_metadata(bucket="sources", key="task/file", body=b"abc", require_versioning=True)
    if backend == "fake":
        assert store.objects == {}
    else:
        assert list(tmp_path.iterdir()) == []


def test_minio_object_store_uses_import_safe_s3_client_defaults() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    config = store._client.meta.config

    assert config.max_pool_connections >= 64
    assert config.tcp_keepalive is True
    assert config.connect_timeout <= 10
    assert config.read_timeout <= 30


def test_remove_expect_header_drops_botocore_continue_handshake() -> None:
    params = {"headers": {"Expect": "100-continue", "Content-Type": "text/plain"}}

    _remove_expect_header(model=object(), params=params)

    assert params == {"headers": {"Content-Type": "text/plain"}}


class _FakeEvents:
    def __init__(self) -> None:
        self.unregistered: list[tuple[str, object]] = []
        self.registered_last: list[tuple[str, object]] = []

    def unregister(self, event_name: str, handler: object) -> None:
        self.unregistered.append((event_name, handler))

    def register_last(self, event_name: str, handler: object) -> None:
        self.registered_last.append((event_name, handler))


def test_minio_client_disables_botocore_expect_continue_handler() -> None:
    events = _FakeEvents()
    client = SimpleNamespace(meta=SimpleNamespace(events=events))

    MinioObjectStore._configure_client_events(client)

    assert (
        "before-call.s3",
        botocore.handlers.add_expect_header,
    ) in events.unregistered
    assert ("before-call.s3", _remove_expect_header) in events.registered_last


class _SlowPutClient:
    def __init__(self) -> None:
        self.closed = False
        self.put_calls = 0

    def put_object(self, **_kwargs: object) -> None:
        self.put_calls += 1
        time.sleep(0.2)

    def close(self) -> None:
        self.closed = True


async def test_owned_store_close_is_idempotent_and_prevents_reopening_or_new_io(monkeypatch):
    store = MinioObjectStore(endpoint_url="http://127.0.0.1:9000", access_key="test", secret_key="test")
    client = store._client
    closed = []
    close = client.close

    def close_client():
        closed.append(client)
        close()

    monkeypatch.setattr(client, "close", close_client)
    store.close()
    store.close()
    assert closed == [client]
    with pytest.raises(RuntimeError, match="closed"):
        await store.put_object(bucket="source", key="x", body=b"x")
    store._replace_client(client)
    assert store._client is client and closed == [client]


class _FastPutClient:
    def __init__(self) -> None:
        self.put_calls = 0

    def put_object(self, **_kwargs: object) -> None:
        self.put_calls += 1


class _VersionedWriteClient:
    def put_object(self, **_kwargs: object) -> dict[str, str]:
        return {"VersionId": "put-version-123"}

    def complete_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
        return {"VersionId": "multipart-version-456"}


class _UnversionedWriteClient:
    def put_object(self, **_kwargs: object) -> dict[str, str]:
        return {}

    def complete_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
        return {}


class _MalformedVersionWriteClient:
    def __init__(self, version_id: object) -> None:
        self.version_id = version_id

    def put_object(self, **_kwargs: object) -> dict[str, object]:
        return {"VersionId": self.version_id}


@pytest.mark.asyncio
async def test_minio_write_metadata_preserves_returned_object_versions() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _VersionedWriteClient()

    put_result = await store.put_object_with_metadata(
        bucket="artifacts",
        key="team/trial/result.txt",
        body=b"result",
    )
    upload = SimpleNamespace(
        bucket="trajectories",
        key="team/trial/events.jsonl",
        upload_id="upload-1",
        parts=[(1, "etag-1")],
    )
    multipart_result = await store.complete_multipart_upload_with_metadata(upload)

    assert put_result.uri == "s3://artifacts/team/trial/result.txt"
    assert put_result.version_id == "put-version-123"
    assert multipart_result.uri == "s3://trajectories/team/trial/events.jsonl"
    assert multipart_result.version_id == "multipart-version-456"


@pytest.mark.asyncio
async def test_minio_write_metadata_preserves_unversioned_response() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _UnversionedWriteClient()

    put_result = await store.put_object_with_metadata(
        bucket="artifacts",
        key="team/trial/result.txt",
        body=b"result",
    )
    upload = SimpleNamespace(
        bucket="trajectories",
        key="team/trial/events.jsonl",
        upload_id="upload-1",
        parts=[(1, "etag-1")],
    )
    multipart_result = await store.complete_multipart_upload_with_metadata(upload)

    assert put_result.version_id is None
    assert multipart_result.version_id is None


@pytest.mark.parametrize("version_id", [None, "", " surrounding ", 7, False, {}])
@pytest.mark.asyncio
async def test_minio_write_metadata_rejects_malformed_version_response(
    version_id: object,
) -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _MalformedVersionWriteClient(version_id)

    with pytest.raises(ValueError, match="malformed VersionId"):
        await store.put_object_with_metadata(
            bucket="artifacts",
            key="team/trial/result.txt",
            body=b"result",
        )


@pytest.mark.asyncio
async def test_put_object_reconnects_and_retries_once_after_client_timeout() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        operation_timeout=0.01,
    )
    slow_client = _SlowPutClient()
    fast_client = _FastPutClient()
    store._client = slow_client
    store._build_client = lambda: fast_client  # type: ignore[method-assign]

    uri = await store.put_object(bucket="benchmarks", key="task/file.txt", body=b"x")

    assert uri == "s3://benchmarks/task/file.txt"
    assert slow_client.put_calls == 1
    assert slow_client.closed is True
    assert fast_client.put_calls == 1


class _BucketClient:
    def __init__(self, *, head_code: str | None = None) -> None:
        self.head_code = head_code
        self.head_calls: list[str] = []
        self.create_calls: list[str] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self.head_calls.append(Bucket)
        if self.head_code is not None:
            raise ClientError(
                {"Error": {"Code": self.head_code}},
                "HeadBucket",
            )

    def create_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self.create_calls.append(Bucket)


@pytest.mark.asyncio
async def test_ensure_bucket_noops_when_bucket_exists() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    client = _BucketClient()
    store._client = client

    await store.ensure_bucket("trajectories")

    assert client.head_calls == ["trajectories"]
    assert client.create_calls == []


@pytest.mark.asyncio
async def test_ensure_bucket_creates_missing_bucket() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    client = _BucketClient(head_code="NoSuchBucket")
    store._client = client

    await store.ensure_bucket("trajectories")

    assert client.head_calls == ["trajectories"]
    assert client.create_calls == ["trajectories"]
