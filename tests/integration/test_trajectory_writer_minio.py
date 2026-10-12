"""End-to-end TrajectoryWriter against a real MinIO via testcontainers."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from minio.versioningconfig import ENABLED, VersioningConfig
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom.models.trajectory import StepStartEvent
from loom.trajectory.storage import MinioObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom_benchmark_tool.upload import upload_task_dir
from loom_llm_gateway.harness_cache import HarnessCacheEntry, read_entry, store_entry
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.support.minio_images import prepare_test_image


@pytest.fixture(scope="module")
def minio() -> Iterator[MinioContainer]:
    with MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster")) as m:
        yield m


@pytest.fixture
def store(minio: MinioContainer) -> MinioObjectStore:
    cfg = minio.get_config()
    return MinioObjectStore(
        endpoint_url=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
    )


def _event(seq: int) -> StepStartEvent:
    return StepStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="main",
        seq=seq,
        instruction_excerpt=f"event {seq}",
    )


@pytest.mark.parametrize("versioned", [False, True], ids=["unversioned", "versioned"])
@pytest.mark.parametrize("size", [0, 17, 9 * 1024**2], ids=["empty", "small", "multipart"])
async def test_stream_write_returns_exact_object_version(
    store: MinioObjectStore, minio: MinioContainer, versioned: bool, size: int,
) -> None:
    bucket = f"stream-version-{uuid4().hex}"
    client = minio.get_client()
    client.make_bucket(bucket)
    if versioned:
        client.set_bucket_versioning(bucket, VersioningConfig(ENABLED))
    payload = b"v" * size

    async def chunks():
        yield payload[:11]
        yield payload[11:]

    receipt = await store.put_object_stream_with_metadata(bucket=bucket, key="output", body=chunks())
    actual = client.stat_object(bucket, "output")
    assert receipt.uri == f"s3://{bucket}/output"
    assert bool(actual.version_id) is versioned
    assert receipt.version_id == actual.version_id
    assert await store.get_object(bucket=bucket, key="output") == payload


async def test_trajectory_writer_writes_to_minio(
    tmp_path: Path,
    store: MinioObjectStore,
    minio: MinioContainer,
) -> None:
    bucket_name = "trajectories"
    client = minio.get_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    local = tmp_path / "events.jsonl"
    key = f"team/{uuid4()}/events.jsonl"
    writer = TrajectoryWriter(
        local_path=local,
        store=store,
        bucket=bucket_name,
        key=key,
        flush_event_count=2,
        flush_bytes=10_000_000,
        flush_sec=3600,
    )
    async with writer:
        for i in range(5):
            await writer.append(_event(i))

    fetched = await store.get_object(bucket=bucket_name, key=key)
    assert fetched.count(b"\n") == 5


async def test_minio_object_store_ensure_bucket_creates_missing_bucket(
    store: MinioObjectStore,
    minio: MinioContainer,
) -> None:
    bucket_name = f"trajectories-{uuid4().hex}"
    client = minio.get_client()
    assert not client.bucket_exists(bucket_name)

    await store.ensure_bucket(bucket_name)

    assert client.bucket_exists(bucket_name)


async def test_concurrent_cache_publication_keeps_immutable_archive_and_digest(store: MinioObjectStore) -> None:
    bucket = f"reference-{uuid4().hex}"
    await store.ensure_bucket(bucket)
    entry = HarnessCacheEntry(archive_key="cache/archive", digest_key="cache/reference", max_bytes=1024)
    payloads = [b"first", b"second"]
    ready = asyncio.Event()
    arrivals = 0

    async def body(payload):
        nonlocal arrivals
        arrivals += 1
        if arrivals == 2:
            ready.set()
        await asyncio.wait_for(ready.wait(), timeout=5)
        yield payload

    accepted = await asyncio.gather(*[
        store_entry(store, bucket=bucket, entry=entry, body=body(payload), declared_size=len(payload),
                    declared_digest="sha256:" + hashlib.sha256(payload).hexdigest())
        for payload in payloads
    ])
    assert all(accepted)
    published = await read_entry(store, bucket=bucket, entry=entry)
    assert published is not None
    archive = await store.get_object(bucket=bucket, key=published.archive_key)
    assert archive in payloads
    assert published.digest == "sha256:" + hashlib.sha256(archive).hexdigest()
    assert not await store_entry(store, bucket=bucket, entry=entry, body=body(b"later"), declared_size=5,
                                 declared_digest="sha256:" + hashlib.sha256(b"later").hexdigest())
    assert await read_entry(store, bucket=bucket, entry=entry) == published


async def test_minio_bundle_roundtrip_restores_executable_mode(
    tmp_path: Path,
    store: MinioObjectStore,
) -> None:
    bucket_name = f"bundle-modes-{uuid4().hex}"
    await store.ensure_bucket(bucket_name)
    source = tmp_path / "source"
    source.mkdir()
    (source / "task.toml").write_text("[task]\nid='mode-test'\n")
    verifier = source / "verifier" / "run.sh"
    verifier.parent.mkdir()
    verifier.write_text("#!/bin/sh\nexit 0\n")
    verifier.chmod(0o755)

    await upload_task_dir(
        store=store,
        bucket=bucket_name,
        prefix="task/",
        task_dir=source,
    )
    out = tmp_path / "out"
    count = await store.download_prefix(
        bucket=bucket_name,
        prefix="task/",
        out_dir=out,
    )

    assert count == 2
    assert (out / "verifier" / "run.sh").stat().st_mode & 0o777 == 0o755
