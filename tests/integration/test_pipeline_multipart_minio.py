from __future__ import annotations

import hashlib

import pytest
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom.pipeline.artifact_commit import ArtifactCommitService
from loom.trajectory.storage import MinioObjectStore
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.integration.pipeline_artifact_testkit import final_producer, plan, upload_all
from tests.support.minio_images import prepare_test_image

pytestmark = pytest.mark.docker


async def test_real_minio_multipart_commit_and_readback() -> None:
    with MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).waiting_for(
        HttpWaitStrategy(9000, "/minio/health/cluster")
    ) as container:
        config = container.get_config()
        store = MinioObjectStore(
            endpoint_url="http://" + config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
        )
        await store.ensure_bucket("artifacts")
        service = ArtifactCommitService(store=store, bucket="artifacts")
        _grant, committed = await upload_all(
            service,
            producer=final_producer(),
            planned=[plan(payload=b'{"ok":true}\n')],
            payloads=[b'{"ok":true}\n'],
        )
        assert committed.state == "committed_ready"


async def test_streamed_canonical_objects_round_trip_and_abort_in_real_minio() -> None:
    with MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).waiting_for(
        HttpWaitStrategy(9000, "/minio/health/cluster")
    ) as container:
        config = container.get_config()
        store = MinioObjectStore(
            endpoint_url="http://" + config["endpoint"],
            access_key=config["access_key"], secret_key=config["secret_key"],
        )
        await store.ensure_bucket("artifacts")
        payload = b"0123456789abcdef" * (1024**2) + b"last part"

        async def source():
            yield payload[:17]
            yield payload[17:]

        await store.put_object_stream(bucket="artifacts", key="workspace", body=source())
        digest = hashlib.sha256()
        async for chunk in store.stream_object(bucket="artifacts", key="workspace", chunk_size=1024**2):
            digest.update(chunk)
        assert digest.digest() == hashlib.sha256(payload).digest()
        assert (await store.stat_object(bucket="artifacts", key="workspace")).content_length == len(payload)

        async def broken_source():
            yield payload
            raise ValueError("source interrupted")

        with pytest.raises(ValueError, match="source interrupted"):
            await store.put_object_stream(bucket="artifacts", key="workspace", body=broken_source())
        assert await store.get_object(bucket="artifacts", key="workspace") == payload
        assert not store._client.list_multipart_uploads(Bucket="artifacts").get("Uploads")
