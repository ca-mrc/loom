"""LocalDiskObjectStore writes the same shapes the FakeObjectStore /
MinioObjectStore expose, but keyed under <root>/<bucket>/<key> on the
host filesystem. The Trial uses it to land trajectories + ATIF docs."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.local_object_store import LocalDiskObjectStore


@pytest.mark.asyncio
async def test_put_get_roundtrip(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    uri = await store.put_object(bucket="traj", key="t1/events.jsonl", body=b"hi")
    assert uri == "s3://traj/t1/events.jsonl"
    assert (tmp_path / "traj" / "t1" / "events.jsonl").read_bytes() == b"hi"
    got = await store.get_object(bucket="traj", key="t1/events.jsonl")
    assert got == b"hi"


@pytest.mark.asyncio
async def test_multipart_streaming(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    upload = await store.create_multipart_upload(bucket="b", key="k")
    await store.upload_part(upload, part_number=1, body=b"hello ")
    await store.upload_part(upload, part_number=2, body=b"world")
    uri = await store.complete_multipart_upload(upload)
    assert uri == "s3://b/k"
    assert (tmp_path / "b" / "k").read_bytes() == b"hello world"


@pytest.mark.asyncio
async def test_get_missing_raises_keyerror(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    with pytest.raises(KeyError):
        await store.get_object(bucket="x", key="y")


@pytest.mark.asyncio
async def test_abort_multipart_drops_parts(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    upload = await store.create_multipart_upload(bucket="b", key="k")
    await store.upload_part(upload, part_number=1, body=b"x")
    await store.abort_multipart_upload(upload)
    assert not (tmp_path / "b" / "k").exists()


@pytest.mark.asyncio
async def test_presign_returns_file_url(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    url = await store.presign_put(bucket="b", key="k", expires_sec=60)
    assert url.startswith("file://")


async def test_unversioned_reads_work_but_exact_versions_never_fall_back_to_latest(tmp_path: Path) -> None:
    store = LocalDiskObjectStore(root=tmp_path)
    await store.put_object(bucket="b", key="k", body=b"old")
    await store.put_object(bucket="b", key="k", body=b"latest")
    assert (await store.stat_object(bucket="b", key="k", version_id=None)).content_length == 6
    assert b"".join([chunk async for chunk in store.stream_object(bucket="b", key="k", version_id=None)]) == b"latest"
    with pytest.raises(ValueError, match="does not support versioned reads"):
        await store.stat_object(bucket="b", key="k", version_id="old-version")
    with pytest.raises(ValueError, match="does not support versioned reads"):
        _ = [chunk async for chunk in store.stream_object(bucket="b", key="k", version_id="old-version")]
