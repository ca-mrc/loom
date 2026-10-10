"""Compose real PostgreSQL admission with pinned TLS MinIO source recovery."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import (
    publish_task_bundle_source,
    release_task_bundle_reference,
    retire_task_bundle_source,
)
from loom.task_bundle_source_publisher import TaskBundleSourcePublisher, TaskBundleSourceRecovery
from loom.task_bundle_source_storage import (
    S3TaskBundleVersionInventory,
    write_task_bundle_source_object,
)
from tests.integration.test_task_bundle_source_journal import NOW
from tests.integration.test_task_bundle_source_journal import journal as journal
from tests.integration.test_task_bundle_source_storage import _store
from tests.support.minio_tls import minio_tls as minio_tls
from tests.unit.test_task_bundle_registration import _bundle

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.mark.parametrize("source_registration_mode", ["legacy", "versioned-v1"])
async def test_local_publisher_excludes_stale_transport_sidecar_from_authored_inputs(
    isolated_migration_postgres_url, tmp_path, minio_tls, source_registration_mode,
):
    import hashlib
    import json

    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.schema import Task
    from loom.models.task_checksum import task_checksum
    from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME
    from loom_cli.local_benchmark_publish import publish_local_benchmark
    from loom_cli.local_benchmark_validate import validate_local_benchmark
    from tests.integration.test_local_benchmark_publish import _write_layout

    root = tmp_path / "local-benchmark"
    _write_layout(root)
    task_dir = root / "tasks" / "alpha"
    (task_dir / ".authored").write_bytes(b"authored dotfile\n")
    nested = task_dir / "inputs" / BUNDLE_FILE_METADATA_NAME
    nested.parent.mkdir()
    nested.write_bytes(b"authored nested file\n")
    nested.chmod(0o755)
    authored_checksum = task_checksum(task_dir)
    authored = {
        path.relative_to(task_dir).as_posix(): path.read_bytes()
        for path in task_dir.rglob("*") if path.is_file()
    }
    stale_metadata = b'{"files":{},"schema_version":1}'
    (task_dir / BUNDLE_FILE_METADATA_NAME).write_bytes(stale_metadata)
    assert validate_local_benchmark(root).task_count == 1
    bucket = "publisher-sidecar-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)

    stats = await publish_local_benchmark(
        root, db_url=isolated_migration_postgres_url, object_store=store, bucket=bucket,
        source_registration_mode=source_registration_mode,
    )
    assert stats.inserted == 1
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            task = await session.get(Task, "team-evals/alpha")
            binding = task.source_provenance["service_execution_input"]
            prefix = task.source.removeprefix(f"s3://{bucket}/")
            manifest_bytes = await store.get_object(
                bucket=bucket, key=binding["manifest_uri"].removeprefix(f"s3://{bucket}/"),
            )
            manifest = json.loads(manifest_bytes)
            restored = tmp_path / "restored"
            count = await store.download_prefix(bucket=bucket, prefix=prefix, out_dir=restored)
            assert {
                "catalog_checksum": task.checksum,
                "manifest_checksum": manifest["task_revision_sha256"],
                "manifest_paths": [item["relative_path"] for item in manifest["files"]],
                "binding_file_count": binding["file_count"],
                "binding_total_bytes": binding["total_bytes"],
            } == {
                "catalog_checksum": authored_checksum,
                "manifest_checksum": "sha256:" + authored_checksum,
                "manifest_paths": sorted(authored),
                "binding_file_count": len(authored),
                "binding_total_bytes": sum(map(len, authored.values())),
            }
            assert binding["manifest_sha256"] == "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
            assert task_checksum(restored) == authored_checksum
            assert count == len(authored)
            for item in manifest["files"]:
                body = authored[item["relative_path"]]
                assert item["sha256"] == "sha256:" + hashlib.sha256(body).hexdigest()
                assert (restored / item["relative_path"]).read_bytes() == body
            assert (restored / "inputs" / BUNDLE_FILE_METADATA_NAME).stat().st_mode & 0o111
            assert not (restored / BUNDLE_FILE_METADATA_NAME).exists()
    finally:
        await engine.dispose()
    assert (task_dir / BUNDLE_FILE_METADATA_NAME).read_bytes() == stale_metadata
    for name, body in authored.items():
        assert (task_dir / name).read_bytes() == body


def _prepare(tmp_path, minio_tls, *, versioning=True):
    bucket = "journal-source-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    if versioning:
        admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    task_dir = _bundle(tmp_path)
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(task_dir, task_id="benchmark/" + uuid4().hex),
        bucket=bucket,
    )
    return task_dir, spec, _store(minio_tls)


async def _publish(journal, ticket, owner="catalog"):
    async with journal.begin() as session:
        return await publish_task_bundle_source(
            session,
            incarnation_id=ticket.incarnation_id,
            reference_kind="catalog",
            owner_id=owner,
            now=NOW,
        )


@pytest.mark.parametrize("factory_isolation", ["READ COMMITTED", "SERIALIZABLE", "AUTOCOMMIT"])
async def test_publisher_commits_intent_before_put_and_attaches_only_with_catalog_transaction(
    journal,
    tmp_path,
    minio_tls,
    monkeypatch,
    factory_isolation,
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls)
    original = store.put_object_with_metadata
    observed = []

    async def checked_put(**kwargs):
        intent_id = UUID(kwargs["metadata"]["loom-source-write-id"])
        async with journal.begin() as session:
            # A separate committed view sees the issuance, and can acquire the
            # source lock: no source transaction is held through this PUT.
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            assert (
                await session.scalar(
                    text("SELECT id FROM task_bundle_sources WHERE id=:id FOR UPDATE"),
                    {"id": spec.id},
                )
                == spec.id
            )
            assert (
                await session.scalar(
                    text("SELECT issued_at FROM task_bundle_source_writes WHERE id=:id"),
                    {"id": intent_id},
                )
                == NOW
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM task_bundle_source_references WHERE source_id=:id"),
                    {"id": spec.id},
                )
                == 0
            )
        observed.append(intent_id)
        return await original(**kwargs)

    monkeypatch.setattr(store, "put_object_with_metadata", checked_put)
    writer_sessions = async_sessionmaker(
        journal.kw["bind"].execution_options(isolation_level=factory_isolation),
        expire_on_commit=False,
    )
    publisher = TaskBundleSourcePublisher(writer_sessions, store, clock=lambda: NOW)
    ticket = await publisher.prepare(spec, task_dir)
    assert observed == [intent.id for intent in ticket.intents]
    async with journal() as session:
        await publish_task_bundle_source(
            session,
            incarnation_id=ticket.incarnation_id,
            reference_kind="catalog",
            owner_id="rolled-back",
            now=NOW,
        )
        await session.rollback()
    assert await _publish(journal, ticket) == spec
    # An available ticket is reuse, not another PUT or an implicit catalog pin.
    replay = await publisher.prepare(spec, task_dir)
    assert replay.available and replay.incarnation_id == ticket.incarnation_id
    assert len(observed) == len(ticket.intents)
    async with journal.begin() as session:
        await release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog"
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW
        )
    with pytest.raises(ValueError, match="retiring"):
        await _publish(journal, replay)
    replacement = await publisher.prepare(spec, task_dir)
    assert replacement.incarnation_id != ticket.incarnation_id
    assert await _publish(journal, replacement) == spec


async def test_lost_receipt_recovers_after_restart_and_delayed_old_cleanup_preserves_republication(
    journal,
    tmp_path,
    minio_tls,
    monkeypatch,
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls)
    original = store.put_object_with_metadata
    upload_id, lost = uuid4(), []

    async def lost_response(**kwargs):
        lost.append(await original(**kwargs))
        raise TimeoutError("storage accepted PUT but caller lost its receipt")

    monkeypatch.setattr(store, "put_object_with_metadata", lost_response)
    with pytest.raises(TimeoutError, match="lost its receipt"):
        await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(
            spec, task_dir, upload_id=upload_id
        )
    async with journal.begin() as session:
        intent_id = await session.scalar(
            text(
                "SELECT id FROM task_bundle_source_writes WHERE incarnation_id=:id AND issued_at IS NOT NULL"
            ),
            {"id": upload_id},
        )
        assert (
            await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_versions WHERE write_id=:id"),
                {"id": intent_id},
            )
            == 0
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=upload_id, now=NOW + timedelta(hours=2)
        )
    admin = minio_tls[3]
    reader = S3TaskBundleVersionInventory(admin, max_versions=1)
    deleter = S3ExactObjectDeleter(admin)
    recovery = TaskBundleSourceRecovery(
        journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
    )
    assert await recovery.inventory_batch(intent_id)
    monkeypatch.setattr(store, "put_object_with_metadata", original)
    newer = await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(
        spec, task_dir
    )
    await _publish(journal, newer)
    # Crash after the external deletion but before committing absence. Restart
    # finishes that exact claim instead of deleting whatever is currently at key.
    first = True
    original_delete = deleter.delete_exact

    def lose_delete_response(item):
        nonlocal first
        original_delete(item)
        if first:
            first = False
            raise TimeoutError("delete committed without response")

    monkeypatch.setattr(deleter, "delete_exact", lose_delete_response)
    with pytest.raises(TimeoutError, match="delete committed"):
        await recovery.delete_batch(upload_id)
    recovery = TaskBundleSourceRecovery(
        journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
    )
    assert await recovery.delete_batch(upload_id) == 1
    # An empty observation must not discard the old intent: model a still-live
    # retry arriving after the old version is gone and the replacement is ready.
    assert await recovery.inventory_batch(intent_id)
    async with journal.begin() as session:
        from loom.task_bundle_source_journal import task_bundle_inventory_checkpoint

        intent, _, _ = await task_bundle_inventory_checkpoint(session, intent_id=intent_id)
    late = await write_task_bundle_source_object(
        store, intent, spec.read_object(task_dir, intent.object_key)
    )
    for _ in range(10):
        recovery = TaskBundleSourceRecovery(
            journal, reader, deleter, clock=lambda: NOW + timedelta(hours=2)
        )
        if await recovery.inventory_batch(intent_id):
            break
    else:
        pytest.fail("restarted bounded inventory never reached its observational end")
    assert late.version_id != lost[0].version_id
    assert await recovery.delete_batch(upload_id) == 1
    for planned in newer.intents:
        recovered = S3TaskBundleVersionInventory(admin).scan_batch(planned)
        assert len(recovered.versions) == 1
        body = admin.get_object(Bucket=planned.bucket, Key=planned.object_key)["Body"]
        try:
            assert body.read() == spec.read_object(task_dir, planned.object_key)
        finally:
            body.close()


async def test_shared_global_manifest_keeps_separately_owned_versions(journal, tmp_path, minio_tls):
    task_dir, first_spec, store = _prepare(tmp_path, minio_tls)
    second_spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(task_dir, task_id="other/" + uuid4().hex),
        bucket=first_spec.bucket,
    )
    publisher = TaskBundleSourcePublisher(journal, store, clock=lambda: NOW)
    first, second = (
        await publisher.prepare(first_spec, task_dir),
        await publisher.prepare(second_spec, task_dir),
    )
    assert first.intents[-1].object_key == second.intents[-1].object_key
    await _publish(journal, first)
    await _publish(journal, second)
    async with journal.begin() as session:
        await release_task_bundle_reference(
            session, source_id=first_spec.id, reference_kind="catalog", owner_id="catalog"
        )
        assert await retire_task_bundle_source(
            session, incarnation_id=first.incarnation_id, now=NOW
        )
    admin = minio_tls[3]
    recovery = TaskBundleSourceRecovery(
        journal, S3TaskBundleVersionInventory(admin), S3ExactObjectDeleter(admin), clock=lambda: NOW
    )
    assert await recovery.delete_batch(first.incarnation_id) == len(first.intents)
    assert await _publish(journal, second, owner="still-readable") == second_spec
    for intent in second.intents:
        assert len(S3TaskBundleVersionInventory(admin).scan_batch(intent).versions) == 1


async def test_unversioned_publication_leaves_only_unreachable_recovery_intents(
    journal, tmp_path, minio_tls
):
    task_dir, spec, store = _prepare(tmp_path, minio_tls, versioning=False)
    with pytest.raises(ValueError, match="versioning"):
        await TaskBundleSourcePublisher(journal, store, clock=lambda: NOW).prepare(spec, task_dir)
    async with journal() as session:
        assert (
            await session.scalar(
                text("SELECT state FROM task_bundle_source_incarnations WHERE source_id=:id"),
                {"id": spec.id},
            )
            == "uploading"
        )
        assert (
            await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_references WHERE source_id=:id"),
                {"id": spec.id},
            )
            == 0
        )
    assert "Contents" not in minio_tls[3].list_objects_v2(Bucket=spec.bucket)


async def test_versioned_local_producer_registers_authored_identity_and_reuses_images(
    isolated_migration_postgres_url,
    tmp_path,
    minio_tls,
):
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.schema import Benchmark, Task, TaskImageMaterialization
    from loom_cli.local_benchmark_publish import publish_local_benchmark
    from tests.integration.test_local_benchmark_publish import _write_layout

    root = tmp_path / "local-benchmark"
    _write_layout(root)
    task_dir = root / "tasks" / "alpha"
    config = task_dir / "task.toml"
    config.write_text(
        config.read_text().replace(
            'docker_image = "python:3.11-alpine"', 'cpu_arch = "any"\ndockerfile = "Dockerfile"'
        )
    )
    (task_dir / "Dockerfile").write_text("FROM scratch\n")
    bucket = "local-source-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)
    arguments = dict(
        db_url=isolated_migration_postgres_url,
        object_store=store,
        bucket=bucket,
        source_registration_mode="versioned-v1",
    )
    first = await publish_local_benchmark(root, **arguments)
    assert first.inserted == 1 and first.uploaded_objects > 0
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            task = await session.get(Task, "team-evals/alpha")
            benchmark = await session.get(Benchmark, "team-evals")
            assert task.config["task"]["id"] == task.id
            assert task.source_provenance["bundle_task_identity"]["bundle_task_id"] == "alpha"
            assert task.source.startswith(benchmark.upstream_locator)
            rows = tuple(
                await session.scalars(
                    select(TaskImageMaterialization).where(
                        TaskImageMaterialization.task_id == task.id
                    )
                )
            )
            assert {row.cpu_arch for row in rows} == {"x86_64"}
            assert all(row.state == "queued" and row.task_source == task.source for row in rows)
            identities = {row.id for row in rows}
            registered_versions = await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_versions")
            )
            object_key = task.source.removeprefix(f"s3://{bucket}/") + "task.toml"
        body = admin.get_object(Bucket=bucket, Key=object_key)["Body"]
        try:
            assert body.read() == config.read_bytes()
        finally:
            body.close()
        repeated = await publish_local_benchmark(root, **arguments)
        assert repeated.unchanged == 1 and repeated.uploaded_objects == 0
        async with factory() as session:
            assert set(await session.scalars(select(TaskImageMaterialization.id))) == identities
            assert (
                await session.scalar(text("SELECT count(*) FROM task_bundle_source_versions"))
                == registered_versions
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("failure_boundary", ["upload", "catalog"])
async def test_versioned_local_publication_failure_keeps_recovery_not_partial_catalog(
    isolated_migration_postgres_url, tmp_path, minio_tls, monkeypatch, failure_boundary,
):
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.schema import Benchmark, Task, TaskImageMaterialization
    from loom_cli import local_benchmark_source_publish as producer
    from loom_cli.local_benchmark_publish import publish_local_benchmark
    from tests.integration.test_local_benchmark_publish import _write_layout

    root = tmp_path / "local-benchmark"
    _write_layout(root)
    bucket = "local-failure-" + uuid4().hex
    task_dir = root / "tasks" / "alpha"
    config = task_dir / "task.toml"
    config.write_text(config.read_text().replace(
        'docker_image = "python:3.11-alpine"', 'cpu_arch = "any"\ndockerfile = "Dockerfile"',
    ))
    (task_dir / "Dockerfile").write_text("FROM scratch\n")
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    put = store.put_object_with_metadata
    observed = []

    async def checked_put(**kwargs):
        # The separate observer must see no catalog admission or locks while
        # upload is in progress. A lost response still leaves exact versions.
        async with sessions.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            assert await session.scalar(select(Benchmark.id).with_for_update()) is None
            assert await session.scalar(select(Task.id).with_for_update()) is None
        receipt = await put(**kwargs)
        observed.append(receipt)
        if failure_boundary == "upload":
            raise RuntimeError("lost upload response")
        return receipt

    publish = producer.publish_task_bundle_catalog

    async def rejected_catalog(*args, **kwargs):
        await publish(*args, **kwargs)
        raise RuntimeError("catalog commit aborted")

    monkeypatch.setattr(store, "put_object_with_metadata", checked_put)
    if failure_boundary == "catalog":
        monkeypatch.setattr(producer, "publish_task_bundle_catalog", rejected_catalog)
    try:
        with pytest.raises(RuntimeError, match=r"lost upload response|catalog commit aborted"):
            await publish_local_benchmark(
                root, db_url=isolated_migration_postgres_url, object_store=store,
                bucket=bucket, source_registration_mode="versioned-v1",
            )
        assert observed
        async with sessions() as session:
            for model in (Benchmark, Task, TaskImageMaterialization):
                assert await session.scalar(select(model.id)) is None
            assert await session.scalar(text("SELECT count(*) FROM task_bundle_sources")) == 1
            assert await session.scalar(
                text("SELECT count(*) FROM task_bundle_source_references"),
            ) == 0
            assert await session.scalar(
                text("SELECT state FROM task_bundle_source_incarnations"),
            ) == "uploading"
        for receipt in observed:
            key = receipt.uri.removeprefix(f"s3://{bucket}/")
            assert admin.head_object(
                Bucket=bucket, Key=key, VersionId=receipt.version_id,
            )["ContentLength"] >= 0
    finally:
        await engine.dispose()


async def test_versioned_nebius_profile_binds_adapted_bytes_without_bucket_admin(
    isolated_migration_postgres_url, tmp_path, minio_tls, monkeypatch,
):
    from sqlalchemy.ext.asyncio import create_async_engine

    from loom.db.schema import Task
    from loom.nebius_terminus_ingest import preflight_nebius_terminus_admission
    from loom_cli.local_benchmark_publish import publish_local_benchmark
    from tests.integration.test_local_benchmark_publish import _write_harbor_layout

    root = tmp_path / "harbor"
    _write_harbor_layout(root)
    authored = root / "tasks" / "harbor-sample" / "task.toml"
    original = authored.read_bytes()
    bucket = "profile-" + uuid4().hex
    admin = minio_tls[3]
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    store = _store(minio_tls)

    async def denied_bucket_admin(bucket):
        raise AssertionError("publication must not require bucket administration")

    monkeypatch.setattr(store, "ensure_bucket", denied_bucket_admin)
    args = dict(db_url=isolated_migration_postgres_url, object_store=store, bucket=bucket,
                source_registration_mode="versioned-v1", execution_profile="nebius-terminus")
    first = await publish_local_benchmark(root, **args)
    assert first.inserted == 1 and first.execution_profile == "nebius-terminus"
    assert authored.read_bytes() == original
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            task = await session.get(Task, "harbor-nebius-profile/harbor-sample")
            assert not preflight_nebius_terminus_admission(task.config, task.source_provenance)
            assert task.config["environment"]["cpu_arch"] == "x86_64"
            assert task.source_provenance["bundle_task_identity"]["bundle_task_id"] != task.id
        again = await publish_local_benchmark(root, **args)
        assert again.unchanged == 1 and again.uploaded_objects == 0
    finally:
        await engine.dispose()
