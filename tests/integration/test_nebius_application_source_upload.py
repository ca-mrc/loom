"""Owner source intents persist through real PostgreSQL transactions/restarts."""
from __future__ import annotations

import asyncio
import hashlib
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from loom.db.schema_startup import service_schema_head
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_application_source_archive import archive_bytes
from tests.unit.test_application_source_archive import source as source
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def upload_registry(factory, **changes):
    from loom.application_source_upload import ApplicationSourceUploadBindingV1
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    binding = ApplicationSourceUploadBindingV1(
        installation_id=uuid4(), data_environment_id=uuid4(), cluster_id="cluster-1",
        source_bucket="shared-source", **changes,
    )
    return ApplicationSourceRegistry(factory, binding=binding)


def intent(**changes):
    from loom.application_source_upload import ApplicationSourceUploadRequestV1

    return ApplicationSourceUploadRequestV1.model_validate({
        "source_digest": "sha256:" + "a" * 64, "archive_sha256": "b" * 64,
        "archive_size_bytes": 10240, "base_commit": "c" * 40,
    } | changes)


async def source_chunks(body):
    for offset in range(0, len(body), 173):
        yield body[offset:offset + 173]


async def test_verified_upload_stores_exact_shared_bytes_for_separate_owners(environment_registry, source, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model)
    request = intent(source_digest=model.digest, archive_sha256=hashlib.sha256(body).hexdigest(),
                     archive_size_bytes=len(body))
    first, second = await asyncio.gather(*[
        registry.create(principal=owner, request=request, idempotency_key="same-source") for owner in (alice, bob)
    ])
    store = FakeObjectStore()
    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    uploader = ApplicationSourceUploader(registry, store, spool_directory=spool)
    results = await asyncio.gather(*[
        uploader.upload(receipt.upload_id, principal=owner, body=source_chunks(body))
        for receipt, owner in ((first, alice), (second, bob))
    ])
    assert first.upload_id != second.upload_id
    assert all(row.phase == "source_verified" for row in results)
    assert store.objects == {("shared-source", "application-sources/v1/sha256/" + request.archive_sha256 + ".tar"): body}
    assert not list(spool.iterdir())
    for result, owner in zip(results, (alice, bob), strict=True):
        assert await registry.status(result.upload_id, principal=owner) == result


@pytest.mark.parametrize("cancel_waiter", [False, True])
async def test_upload_reception_does_not_multiply_memory_heavy_verification(
    environment_registry, source, tmp_path, monkeypatch, cancel_waiter,
):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management import source_upload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    body = archive_bytes(source[1])
    request = intent(source_digest=source[1].digest, archive_sha256=hashlib.sha256(body).hexdigest(),
                     archive_size_bytes=len(body))
    receipts = [await registry.create(principal=alice, request=request, idempotency_key=f"parallel-{i}")
                for i in range(3)]
    received, release = asyncio.Event(), asyncio.Event()
    reception_count, active, peak = 0, 0, 0
    entered = []
    original = source_upload._verify_off_loop

    async def held(stream, receipt, directory):
        nonlocal active, peak
        entered.append(receipt.upload_id)
        active += 1
        peak = max(peak, active)
        try:
            await release.wait()
            await original(stream, receipt, directory)
        finally:
            active -= 1

    async def receive():
        nonlocal reception_count
        yield body
        reception_count += 1
        if reception_count == 3:
            received.set()

    monkeypatch.setattr(source_upload, "_verify_off_loop", held)
    store = FakeObjectStore()
    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    uploader = source_upload.ApplicationSourceUploader(registry, store, spool_directory=spool, max_inflight=3)
    tasks = [asyncio.create_task(uploader.upload(row.upload_id, principal=alice, body=receive())) for row in receipts]
    cancelled = None
    try:
        await asyncio.wait_for(received.wait(), timeout=5)
        # All three streams finish, but only two verifier calls may retain
        # parsed manifests. The spy holds timing, not the real validation result.
        assert peak == 2
        if cancel_waiter:
            cancelled, = (index for index, row in enumerate(receipts) if row.upload_id not in entered)
            tasks[cancelled].cancel()
    finally:
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    for index, result in enumerate(results):
        if index == cancelled:
            assert isinstance(result, asyncio.CancelledError)
            assert await registry.status(receipts[index].upload_id, principal=alice) == receipts[index]
            assert receipts[index].upload_id not in entered
        else:
            assert result.phase == "source_verified"
    assert peak == 2 and active == 0 and not list(spool.iterdir())
    if cancelled is not None:
        result = await uploader.upload(receipts[cancelled].upload_id, principal=alice, body=source_chunks(body))
        assert result.phase == "source_verified"


@pytest.mark.parametrize("damage", ["truncated", "extra", "hash", "archive", "source_digest"])
async def test_invalid_upload_never_writes_or_completes(environment_registry, source, tmp_path, damage):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model, damage="name" if damage == "archive" else None)
    request = intent(source_digest=model.digest if damage != "source_digest" else "sha256:" + "e" * 64,
                     archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body))
    receipt = await registry.create(principal=alice, request=request, idempotency_key="invalid")
    if damage == "truncated":
        body = body[:-1]
    elif damage == "extra":
        body += b"!"
    elif damage == "hash":
        body = body[:-1] + b"!"
    store = FakeObjectStore()
    with pytest.raises(ManagementError, match="application_source_invalid"):
        await ApplicationSourceUploader(registry, store, spool_directory=tmp_path).upload(
            receipt.upload_id, principal=alice, body=source_chunks(body))
    assert store.objects == {}
    assert await registry.status(receipt.upload_id, principal=alice) == receipt


async def test_foreign_owner_is_rejected_before_consuming_upload(environment_registry, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    receipt = await registry.create(principal=alice, request=intent(), idempotency_key="private")
    consumed = []
    async def body():
        consumed.append(True)
        yield b"private-source"
    store = FakeObjectStore()
    with pytest.raises(ManagementError, match="application_source_forbidden"):
        await ApplicationSourceUploader(registry, store, spool_directory=tmp_path).upload(
            receipt.upload_id, principal=bob, body=body())
    assert consumed == [] and store.objects == {}
    assert await registry.status(receipt.upload_id, principal=alice) == receipt


async def test_upload_admission_and_cancel_release_private_spool(environment_registry, source, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model)
    receipt = await registry.create(principal=alice, request=intent(source_digest=model.digest,
        archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body)), idempotency_key="bounded")
    entered = asyncio.Event()
    async def stalled():
        entered.set()
        yield body[:100]
        await asyncio.Event().wait()
    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    store = FakeObjectStore()
    uploader = ApplicationSourceUploader(registry, store, spool_directory=spool, max_inflight=1)
    active = asyncio.create_task(uploader.upload(receipt.upload_id, principal=alice, body=stalled()))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        with pytest.raises(ManagementError, match="application_source_capacity_exhausted"):
            await uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body))
        assert store.objects == {}
    finally:
        active.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active
    assert not list(spool.iterdir())
    assert await registry.status(receipt.upload_id, principal=alice) == receipt
    assert (await uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body))).phase == "source_verified"


async def test_stalled_upload_expires_without_storage_or_db_completion(environment_registry, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    receipt = await registry.create(principal=alice, request=intent(), idempotency_key="slow")
    async def stalled():
        yield b"part"
        await asyncio.Event().wait()
    store = FakeObjectStore()
    with pytest.raises(ManagementError, match="application_source_reception_timeout"):
        await ApplicationSourceUploader(registry, store, spool_directory=tmp_path,
            receive_timeout_seconds=0.01).upload(receipt.upload_id, principal=alice, body=stalled())
    assert store.objects == {}
    assert await registry.status(receipt.upload_id, principal=alice) == receipt


async def test_cancel_during_verification_retains_admission_and_spool_until_worker_finishes(
    environment_registry, source, tmp_path, monkeypatch,
):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management import source_upload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model)
    receipt = await registry.create(principal=alice, request=intent(source_digest=model.digest,
        archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body)), idempotency_key="verify-cancel")
    entered, release = threading.Event(), threading.Event()
    original = source_upload.extract_application_source_archive
    def held(stream, **kwargs):
        result = original(stream, **kwargs)
        entered.set()
        assert release.wait(5), "verification was not released"
        assert not stream.closed
        return result
    monkeypatch.setattr(source_upload, "extract_application_source_archive", held)
    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    store = FakeObjectStore()
    uploader = source_upload.ApplicationSourceUploader(registry, store, spool_directory=spool, max_inflight=1)
    active = asyncio.create_task(uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body)))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        for _ in range(2):
            active.cancel()
            await asyncio.sleep(0)
        assert not active.done()
        with pytest.raises(ManagementError, match="application_source_capacity_exhausted"):
            await uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body))
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await active
    assert not list(spool.iterdir()) and store.objects == {}
    assert await registry.status(receipt.upload_id, principal=alice) == receipt


async def test_stalled_storage_readback_is_bounded_and_closed(environment_registry, source, tmp_path):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    class Store(FakeObjectStore):
        closed = False
        async def stream_object(self, **kwargs):
            try:
                yield b""
                await asyncio.Event().wait()
            finally:
                self.closed = True
    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model)
    receipt = await registry.create(principal=alice, request=intent(source_digest=model.digest,
        archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body)), idempotency_key="slow-store")
    store = Store()
    with pytest.raises(ManagementError, match="application_source_storage_unverified"):
        await ApplicationSourceUploader(registry, store, spool_directory=tmp_path,
            storage_timeout_seconds=0.01).upload(receipt.upload_id, principal=alice, body=source_chunks(body))
    assert store.closed is True
    assert await registry.status(receipt.upload_id, principal=alice) == receipt


@pytest.mark.parametrize("failure", ["before", "after", "corrupt"])
async def test_storage_reply_is_not_proof_and_uncertain_write_is_only_observed(
    environment_registry, source, tmp_path, failure,
):
    from loom.trajectory.storage import FakeObjectStore
    from loom_service.application_management.source_upload import ApplicationSourceUploader

    class Store(FakeObjectStore):
        writes = 0
        async def put_object_stream(self, *, bucket, key, body):
            self.writes += 1
            if failure == "before":
                raise OSError("private-storage-detail")
            result = await super().put_object_stream(bucket=bucket, key=key, body=body)
            if failure == "after":
                raise OSError("private-storage-detail")
            self.objects[(bucket, key)] = b"wrong stored content"
            return result

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    _, model = source
    body = archive_bytes(model)
    receipt = await registry.create(principal=alice, request=intent(source_digest=model.digest,
        archive_sha256=hashlib.sha256(body).hexdigest(), archive_size_bytes=len(body)), idempotency_key="store")
    store = Store()
    uploader = ApplicationSourceUploader(registry, store, spool_directory=tmp_path)
    if failure == "after":
        assert (await uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body))).phase == "source_verified"
    else:
        with pytest.raises(ManagementError, match="application_source_storage_unverified") as error:
            await uploader.upload(receipt.upload_id, principal=alice, body=source_chunks(body))
        assert "private-storage-detail" not in str(error.value)
        assert await registry.status(receipt.upload_id, principal=alice) == receipt
    assert store.writes == 1


async def test_concurrent_owner_upload_replay_survives_registry_restart(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    replies = await asyncio.gather(*[
        registry.create(principal=alice, request=intent(), idempotency_key="source-1") for _ in range(3)
    ])
    first = replies[0]
    assert all(reply == first for reply in replies)
    assert first.phase == "awaiting_source"
    assert first.source_digest == "sha256:" + "a" * 64
    assert first.archive_sha256 == "b" * 64
    assert first.base_commit == "c" * 40
    restored = ApplicationSourceRegistry(factory, binding=registry.binding)
    assert await restored.status(first.upload_id, principal=alice) == first
    assert await restored.create(principal=alice, request=intent(), idempotency_key="source-1") == first
    other = await registry.create(principal=bob, request=intent(), idempotency_key="source-1")
    assert other.upload_id != first.upload_id
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationSourceUpload)) == 2
        rows = list(await session.scalars(select(NebiusApplicationSourceUpload)))
        assert {row.object_key for row in rows} == {"application-sources/v1/sha256/" + "b" * 64 + ".tar"}


@pytest.mark.parametrize("change", [
    {"source_digest": "sha256:" + "d" * 64}, {"archive_sha256": "e" * 64},
    {"archive_size_bytes": 20480}, {"base_commit": None},
])
async def test_upload_idempotency_refuses_changed_source_intent(environment_registry, change):
    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="same-key")
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, request=intent(**change), idempotency_key="same-key")
    assert await registry.status(first.upload_id, principal=alice) == first


async def test_upload_owner_team_installation_and_mutation_scope_are_enforced(environment_registry):
    from loom_service.application_management.source_registry import ApplicationSourceRegistry

    _, factory, (alice, bob), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    for stranger in (bob, replace(alice, team_id=uuid4())):
        with pytest.raises(ManagementError, match="application_source_forbidden"):
            await registry.status(first.upload_id, principal=stranger)
        with pytest.raises(ManagementError, match="application_source_forbidden"):
            await registry.complete(first.upload_id, principal=stranger)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=replace(alice, team_id=uuid4()), request=intent(), idempotency_key="source")
    read_only = replace(alice, scopes=["read:own"])
    assert await registry.status(first.upload_id, principal=read_only) == first
    with pytest.raises(ManagementError, match="environment_scope_required"):
        await registry.complete(first.upload_id, principal=read_only)
    alternate = ApplicationSourceRegistry(factory, binding=registry.binding.model_copy(update={"installation_id": uuid4()}))
    with pytest.raises(ManagementError, match="application_source_forbidden"):
        await alternate.status(first.upload_id, principal=alice)
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await alternate.create(principal=alice, request=intent(), idempotency_key="source")


async def test_verified_source_receipt_is_idempotent_and_not_a_ready_build(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    replies = await asyncio.gather(*[registry.complete(first.upload_id, principal=alice) for _ in range(3)])
    assert all(reply == replies[0] for reply in replies)
    assert replies[0].phase == "source_verified"
    assert set(replies[0].model_dump()) == {
        "schema_version", "upload_id", "source_digest", "archive_sha256", "archive_size_bytes",
        "base_commit", "phase", "expires_at",
    }
    async with factory() as session:
        row = await session.get(NebiusApplicationSourceUpload, first.upload_id)
        assert row.verified_at >= row.created_at
        assert row.expires_at - row.created_at == timedelta(seconds=3600)


async def test_upload_database_retains_immutable_identity_and_verified_receipt(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    for change in ({"archive_sha256": "d" * 64}, {"object_key": "other"},
                   {"owner_user_id": uuid4()}, {"expires_at": first.expires_at + timedelta(seconds=1)}):
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(update(NebiusApplicationSourceUpload).where(
                    NebiusApplicationSourceUpload.upload_id == first.upload_id).values(**change))
    verified = await registry.complete(first.upload_id, principal=alice)
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(update(NebiusApplicationSourceUpload).where(
                NebiusApplicationSourceUpload.upload_id == first.upload_id
            ).values(phase="awaiting_source", verified_at=None))
    assert await registry.status(first.upload_id, principal=alice) == verified
    async with factory() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == service_schema_head()


async def test_expiry_uses_database_clock_without_erasing_completed_source(environment_registry):
    from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload

    _, factory, (alice, _), _ = environment_registry
    registry = upload_registry(factory)
    first = await registry.create(principal=alice, request=intent(), idempotency_key="source")
    assert await registry.for_upload(first.upload_id, principal=alice) == first
    # Seed historical rows in the disposable DB, without a wall-clock sleep or
    # weakening the production trigger's immutable timestamp contract.
    earlier = datetime.now(UTC) - timedelta(hours=2)
    async with factory.begin() as session:
        current = await session.get(NebiusApplicationSourceUpload, first.upload_id)
        original = {name: getattr(current, name) for name in NebiusApplicationSourceUpload.__table__.columns.keys()}
        expired, completed = uuid4(), uuid4()
        for identity, key in ((expired, "expired"), (completed, "completed")):
            session.add(NebiusApplicationSourceUpload(**(original | {
                "upload_id": identity, "idempotency_key": key,
                "created_at": earlier, "expires_at": earlier + timedelta(hours=1),
            })))
        await session.flush()
        await session.execute(update(NebiusApplicationSourceUpload).where(
            NebiusApplicationSourceUpload.upload_id == completed,
        ).values(phase="source_verified", verified_at=earlier + timedelta(minutes=1)))
    expired_status = await registry.status(expired, principal=alice)
    for operation in (registry.for_upload, registry.complete):
        with pytest.raises(ManagementError, match="application_source_expired"):
            await operation(expired, principal=alice)
    assert await registry.status(expired, principal=alice) == expired_status
    completed_status = await registry.status(completed, principal=alice)
    assert completed_status.phase == "source_verified"
    assert await registry.for_upload(completed, principal=alice) == completed_status
    assert await registry.complete(completed, principal=alice) == completed_status
