from __future__ import annotations

from pathlib import Path

from botocore.exceptions import ClientError, ConnectionClosedError

from loom.trajectory.storage import MinioObjectStore


class _FakeEvents:
    def unregister(self, *_args: object, **_kwargs: object) -> None:
        return None

    def register_last(self, *_args: object, **_kwargs: object) -> None:
        return None


class _FakeMeta:
    events = _FakeEvents()


class _FakeS3Client:
    meta = _FakeMeta()

    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _PrefixPaginator:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803
        yield {
            "Contents": [
                {"Key": key}
                for key in self._keys
                if key.startswith(Prefix)
            ],
        }


class _PrefixDownloadClient(_FakeS3Client):
    def __init__(
        self,
        name: str,
        *,
        keys: list[str],
        fail_once_key: str | None = None,
    ) -> None:
        super().__init__(name)
        self._keys = keys
        self._fail_once_key = fail_once_key
        self.downloads: list[str] = []

    def get_paginator(self, name: str) -> _PrefixPaginator:
        assert name == "list_objects_v2"
        return _PrefixPaginator(self._keys)

    def download_file(self, bucket: str, key: str, path: str) -> None:
        self.downloads.append(key)
        if self._fail_once_key == key:
            self._fail_once_key = None
            raise ConnectionClosedError(endpoint_url="http://minio:9000")
        Path(path).write_text(f"{bucket}/{key}")


def _store_with_clients(monkeypatch, clients: list[_FakeS3Client]) -> MinioObjectStore:  # type: ignore[no-untyped-def]
    created: list[_FakeS3Client] = []

    def fake_boto3_client(**_kwargs: object) -> _FakeS3Client:
        client = clients[len(created)]
        created.append(client)
        return client

    monkeypatch.setattr("loom.trajectory.storage.boto3.client", fake_boto3_client)
    store = MinioObjectStore(
        endpoint_url="http://minio:9000",
        access_key="ak",
        secret_key="sk",
        operation_attempts=2,
        operation_timeout=1,
    )
    assert created == [clients[0]]
    return store


async def test_run_client_call_retries_connection_closed_with_fresh_client(
    monkeypatch,
) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)
    attempts: list[str] = []

    def call(client: _FakeS3Client) -> str:
        attempts.append(client.name)
        if client.name == "first":
            raise ConnectionClosedError(endpoint_url="http://minio:9000")
        return "ok"

    result = await store._run_client_call("put_object", call)

    assert result == "ok"
    assert attempts == ["first", "second"]
    assert clients[0].closed is True


async def test_run_client_call_retries_transient_s3_500(monkeypatch) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)
    attempts: list[str] = []

    def call(client: _FakeS3Client) -> str:
        attempts.append(client.name)
        if client.name == "first":
            raise ClientError(
                {
                    "Error": {"Code": "InternalError", "Message": "try later"},
                    "ResponseMetadata": {"HTTPStatusCode": 500},
                },
                "PutObject",
            )
        return "ok"

    result = await store._run_client_call("put_object", call)

    assert result == "ok"
    assert attempts == ["first", "second"]
    assert clients[0].closed is True


async def test_run_client_call_does_not_retry_permanent_s3_403(monkeypatch) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)

    def call(_client: _FakeS3Client) -> str:
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "PutObject",
        )

    try:
        await store._run_client_call("put_object", call)
    except ClientError:
        pass
    else:
        raise AssertionError("expected permanent ClientError")

    assert clients[0].closed is False
    assert clients[1].closed is False


async def test_download_prefix_retries_only_failed_object_with_fresh_client(
    monkeypatch,
    tmp_path,
) -> None:
    keys = ["bench/task/a.txt", "bench/task/b.txt"]
    clients = [
        _PrefixDownloadClient(
            "first",
            keys=keys,
            fail_once_key="bench/task/b.txt",
        ),
        _PrefixDownloadClient("second", keys=keys),
    ]
    store = _store_with_clients(monkeypatch, clients)

    count = await store.download_prefix(
        bucket="benchmarks",
        prefix="bench/task/",
        out_dir=tmp_path,
    )

    assert count == 2
    assert clients[0].downloads == ["bench/task/a.txt", "bench/task/b.txt"]
    assert clients[1].downloads == ["bench/task/b.txt"]
    assert (tmp_path / "a.txt").read_text() == "benchmarks/bench/task/a.txt"
    assert (tmp_path / "b.txt").read_text() == "benchmarks/bench/task/b.txt"
