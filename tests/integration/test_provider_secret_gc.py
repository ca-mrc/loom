"""Real PostgreSQL retirement, ownership, transaction and reference-race coverage."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Base, ProviderConnection, Secret, Team
from loom.security.secret_store import LocalEncryptedSecretStore, SecretNotFoundError
from loom_service.provider_secret_gc import (
    collect_provider_secrets,
    retire_provider_secret,
    run_loop,
)
from tests.integration.test_nebius_application_effect_migration import operation

KEY = bytes(range(32))
OLD = datetime.now(UTC) - timedelta(days=2)


@pytest_asyncio.fixture
async def factory(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def seed(session, *, retired=None, deleted=False, namespace=None, connection=True):
    team_id = uuid4()
    session.add(Team(id=team_id, name=str(team_id)))
    await session.flush()
    ref = await LocalEncryptedSecretStore(session, master_key=KEY).put(
        namespace=namespace or f"team:{team_id}", value="test-provider-key",
    )
    await session.execute(update(Secret).where(Secret.ref == ref).values(provider_retired_at=retired))
    if connection:
        session.add(ProviderConnection(
            team_id=team_id, display_name=str(uuid4()), provider_type="openai-compatible",
            base_url="https://api.example.com/", upstream_host="api.example.com",
            encrypted_api_key_ref=ref, created_by="test", deleted_at=OLD if deleted else None,
        ))
        await session.flush()
    return ref, team_id


@pytest.mark.asyncio
async def test_grace_ownership_active_shared_and_discovery(factory):
    async with factory.begin() as session:
        expired, _ = await seed(session, retired=OLD, deleted=True)
        active, _ = await seed(session, retired=OLD)
        recent, _ = await seed(session, retired=datetime.now(UTC), deleted=True)
        await session.execute(update(Secret).where(Secret.ref == recent).values(created_at=OLD))
        discovered, _ = await seed(session, deleted=True)
        unknown, _ = await seed(session, connection=False)
        foreign, _ = await seed(session, namespace="infra:credential", deleted=True)
        shared, _ = await seed(session, retired=OLD, deleted=True)
        extra, _ = await seed(session)
        owner = await session.scalar(select(ProviderConnection).where(ProviderConnection.encrypted_api_key_ref == extra))
        owner.encrypted_api_key_ref = shared
    async with factory.begin() as session:
        assert await collect_provider_secrets(session) == 1
    async with factory() as session:
        assert await session.get(Secret, expired) is None
        assert await session.get(Secret, active) is not None
        for ref in (recent, discovered, unknown, foreign, shared):
            assert await session.get(Secret, ref) is not None
        assert (await session.get(Secret, discovered)).provider_retired_at > OLD + timedelta(days=1)
        assert (await session.get(Secret, shared)).provider_retired_at > OLD + timedelta(days=1)
        assert (await session.get(Secret, unknown)).provider_retired_at is None
        assert (await session.get(Secret, foreign)).provider_retired_at is None


@pytest.mark.asyncio
async def test_retirement_and_collection_rollback(factory):
    async with factory.begin() as session:
        ref, team = await seed(session, deleted=True)
    async with factory() as session:
        await retire_provider_secret(session, ref=ref, team_id=uuid4())
        assert (await session.get(Secret, ref)).provider_retired_at is None
        await retire_provider_secret(session, ref=ref, team_id=team)
        await session.rollback()
        assert (await session.get(Secret, ref)).provider_retired_at is None
    async with factory.begin() as session:
        await session.execute(update(Secret).where(Secret.ref == ref).values(provider_retired_at=OLD))
    async with factory() as session:
        assert await collect_provider_secrets(session) == 1
        await session.rollback()
        assert await LocalEncryptedSecretStore(session, master_key=KEY).get(ref) == "test-provider-key"


@pytest.mark.asyncio
async def test_bounded_overlapping_collectors_and_rewrap(factory):
    async with factory.begin() as session:
        refs = [(await seed(session, retired=OLD, deleted=True))[0] for _ in range(4)]
    async with factory() as rewrapper, factory() as first, factory() as second:
        await LocalEncryptedSecretStore(rewrapper, master_key=KEY).rewrap(refs[0], new_master_key=KEY)
        assert await collect_provider_secrets(first, batch_size=2) == 2
        assert await collect_provider_secrets(second, batch_size=2) == 1
        await first.commit()
        await second.commit()
        assert await LocalEncryptedSecretStore(rewrapper, master_key=KEY).get(refs[0]) == "test-provider-key"
        await rewrapper.commit()
    async with factory.begin() as session:
        assert await collect_provider_secrets(session) == 1
        with pytest.raises(SecretNotFoundError):
            await LocalEncryptedSecretStore(session, master_key=KEY).rewrap(refs[0], new_master_key=KEY)


@pytest.mark.asyncio
async def test_attachment_wins_and_collector_skips_exact_locked_secret(factory):
    async with factory.begin() as session:
        retired, _ = await seed(session, retired=OLD, deleted=True)
        active, _ = await seed(session)
    async with factory() as writer, factory() as collector:
        await writer.execute(update(ProviderConnection).where(
            ProviderConnection.encrypted_api_key_ref == active,
        ).values(encrypted_api_key_ref=retired))
        assert await collect_provider_secrets(collector) == 0
        await collector.commit()
        await writer.commit()
        assert await collect_provider_secrets(collector) == 0
        await collector.commit()
        assert await collector.get(Secret, retired) is not None


@pytest.mark.asyncio
async def test_collection_wins_attachment_waits_then_rejects_missing(factory):
    async with factory.begin() as session:
        retired, _ = await seed(session, retired=OLD, deleted=True)
        active, _ = await seed(session)
    async with factory() as collector, factory() as writer:
        collector_pid = await collector.scalar(text("SELECT pg_backend_pid()"))
        writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
        assert await collect_provider_secrets(collector) == 1
        async with factory() as observer:
            # Force an activity snapshot before the writer reaches its lock wait.
            # pg_blocking_pids reads live lock state even with this stale snapshot.
            await observer.scalar(text("SELECT count(*) FROM pg_stat_activity"))
            attachment = asyncio.create_task(writer.execute(update(ProviderConnection).where(
                ProviderConnection.encrypted_api_key_ref == active,
            ).values(encrypted_api_key_ref=retired)))
            try:
                async with asyncio.timeout(5):
                    while not await observer.scalar(text(
                        "SELECT :collector = ANY(pg_blocking_pids(:writer))",
                    ), {"collector": collector_pid, "writer": writer_pid}):
                        assert not attachment.done()
                        await asyncio.sleep(0.01)
                assert not attachment.done()
                await collector.commit()
                with pytest.raises(IntegrityError, match="local provider secret does not exist"):
                    await asyncio.wait_for(attachment, timeout=3)
            finally:
                # Release the blocker before unwinding a still-running writer.
                await collector.rollback()
                if not attachment.done():
                    attachment.cancel()
                await asyncio.gather(attachment, return_exceptions=True)
                await writer.rollback()


@pytest.mark.asyncio
async def test_cancellation_rolls_back_open_pass(factory, monkeypatch):
    import loom_service.provider_secret_gc as gc

    async with factory.begin() as session:
        ref, _ = await seed(session, retired=OLD, deleted=True)
    deleted = asyncio.Event()

    async def paused_pass(session):
        await collect_provider_secrets(session)
        deleted.set()
        await asyncio.Future()

    monkeypatch.setattr(gc, "collect_provider_secrets", paused_pass)
    task = asyncio.create_task(run_loop(session_factory=factory))
    await asyncio.wait_for(deleted.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with factory.begin() as session:
        assert await session.get(Secret, ref) is not None
        assert await collect_provider_secrets(session) == 1


def test_migration_downgrade_upgrade_and_reference_inventory(isolated_migration_postgres_url):
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", isolated_migration_postgres_url.replace("%", "%%"))
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert "provider_retired_at" in {c["name"] for c in inspect(engine).get_columns("secrets")}
        command.downgrade(cfg, "0155")
        assert "provider_retired_at" not in {c["name"] for c in inspect(engine).get_columns("secrets")}
        command.upgrade(cfg, "0156")
        with engine.connect() as conn:
            actual = set(conn.execute(text("""
                SELECT tgrelid::regclass::text FROM pg_trigger
                WHERE tgname LIKE '%_provider_secret_attachment'
            """)).scalars())
        # This is migration0156's historical inventory, not today's ORM schema.
        assert actual == {"provider_connections", "dev_instances", "task_image_build_projections",
                          "task_image_build_session_generations", "pipeline_stage_runs"}
        indexes = {index["name"] for index in inspect(engine).get_indexes("secrets")}
        assert "secrets_provider_retired_idx" in indexes
    finally:
        engine.dispose()


def test_current_secret_consumers_have_attachment_integrity(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.connect() as connection:
            guarded = {(table, field.decode()) for table, args in connection.execute(text("""
                SELECT tgrelid::regclass::text, tgargs FROM pg_trigger
                WHERE tgname LIKE '%_provider_secret_attachment'
            """)) for field in bytes(args).split(b"\0") if field}
        inspector = inspect(engine)
        for table in Base.metadata.tables.values():
            for constraint in inspector.get_foreign_keys(table.name):
                if constraint["referred_table"] == "secrets" and constraint["referred_columns"] == ["ref"]:
                    guarded.update((table.name, column) for column in constraint["constrained_columns"])
        expected = {(table.name, column.name) for table in Base.metadata.tables.values() for column in table.columns
                    if column.name.endswith("secret_ref") or column.name in {"secret_refs", "encrypted_api_key_ref"}}
        assert guarded == expected
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_retired_secret_with_application_material_reference_is_retained(factory):
    from loom.db.schema import NebiusApplicationMaterial

    async with factory.begin() as session:
        ref, _ = await seed(session, retired=OLD, deleted=True)
        owner = await (await session.connection()).run_sync(operation)
        session.add(NebiusApplicationMaterial(operation_id=owner, secret_ref=ref))
    # Native FK protection alone prevents deletion but would abort every GC pass.
    # The collector must recognize this consumer and still reclaim another key.
    async with factory.begin() as session:
        unreferenced, _ = await seed(session, retired=OLD, deleted=True)
    async with factory.begin() as session:
        assert await collect_provider_secrets(session) == 1
    async with factory() as session:
        assert await session.get(Secret, ref) is not None
        assert await session.get(Secret, unreferenced) is None


@pytest.mark.asyncio
async def test_historical_pipeline_reference_retained_and_new_array_attachment_guarded(factory):
    async with factory.begin() as session:
        ref, team = await seed(session, retired=OLD, deleted=True)
        run_id, stage_id = uuid4(), uuid4()
        digest = "sha256:" + "a" * 64
        await session.execute(text("""
            INSERT INTO pipeline_runs (
                id, team_id, submission_policy, recipe_name, recipe_version, recipe_digest,
                graph_spec_json, graph_spec_digest, parameters_json, parameters_digest,
                resolved_inputs_json, budget_json, request_digest, idempotency_key
            ) VALUES (
                :id, :team, 'ordinary', 'secret-gc-test', 1, :digest,
                '{}'::jsonb, :digest, '{}'::jsonb, :digest,
                '[]'::jsonb, '{}'::jsonb, :digest, :idempotency
            )
        """), {"id": run_id, "team": team, "digest": digest, "idempotency": str(run_id)})
        await session.execute(text("""
            INSERT INTO pipeline_stage_runs (
                id, pipeline_run_id, node_key, shard_key, node_kind, state,
                resource_profile_json, resource_profile_digest, failure_policy, secret_refs, finished_at
            ) VALUES (
                :id, :run, 'historical', 'singleton', 'container', 'cancelled',
                '{}'::jsonb, :digest, 'fail_run', ARRAY[:ref], now()
            )
        """), {"id": stage_id, "run": run_id, "digest": digest, "ref": ref})
    async with factory.begin() as session:
        assert await collect_provider_secrets(session) == 0
        assert await session.get(Secret, ref) is not None
    async with factory.begin() as session:
        with pytest.raises(IntegrityError, match="local provider secret does not exist"):
            async with session.begin_nested():
                await session.execute(text("""
                    UPDATE pipeline_stage_runs SET secret_refs = ARRAY[:ref, :missing] WHERE id = :id
                """), {"ref": ref, "missing": f"loom://team:{team}/{uuid4()}", "id": stage_id})
        # Unrelated schemes continue to work, and historical refs need no revalidation.
        await session.execute(text("""
            UPDATE pipeline_stage_runs SET secret_refs = ARRAY[:ref, 'k8s://ns/name'] WHERE id = :id
        """), {"ref": ref, "id": stage_id})


@pytest.mark.asyncio
async def test_read_committed_required(factory):
    async with factory() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        with pytest.raises(RuntimeError, match="READ COMMITTED"):
            await collect_provider_secrets(session)


def test_attachment_guard_does_not_grant_ciphertext_access(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    role = "secret_ref_writer_" + uuid4().hex
    team, connection_id = uuid4(), uuid4()
    ref = f"loom://team:{team}/{uuid4()}"
    try:
        # Transactional role creation is rolled back, preserving cluster roles.
        with engine.connect() as connection, connection.begin() as transaction:
            connection.execute(text(f'CREATE ROLE "{role}"'))
            connection.execute(text(f'GRANT USAGE ON SCHEMA public TO "{role}"'))
            connection.execute(text(f'GRANT INSERT ON provider_connections TO "{role}"'))
            connection.execute(text("INSERT INTO teams(id, name) VALUES (:id, :name)"),
                               {"id": team, "name": str(team)})
            connection.execute(text("""
                INSERT INTO secrets(ref, ciphertext, nonce, master_key_version)
                VALUES (:ref, 'ciphertext', 'nonce', 1)
            """), {"ref": ref})
            connection.execute(text(f'SET LOCAL ROLE "{role}"'))
            connection.execute(text("""
                INSERT INTO provider_connections(id, team_id, provider_type, display_name,
                    base_url, upstream_host, encrypted_api_key_ref, created_by)
                VALUES (:id, :team, 'openai-compatible', 'restricted-writer',
                    'https://example.com/', 'example.com', :ref, 'test')
            """), {"id": connection_id, "team": team, "ref": ref})
            assert not connection.execute(text("""
                SELECT has_table_privilege(current_user, 'public.secrets', 'SELECT')
            """)).scalar_one()
            transaction.rollback()
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_reactivation_requires_existing_credential(factory):
    async with factory.begin() as session:
        retired, _ = await seed(session, retired=OLD, deleted=True)
    async with factory.begin() as session:
        assert await collect_provider_secrets(session) == 1
    async with factory.begin() as session:
        # Historical audit updates remain possible after credential collection.
        await session.execute(update(ProviderConnection).where(
            ProviderConnection.encrypted_api_key_ref == retired,
        ).values(status="disabled"))
        with pytest.raises(IntegrityError, match="local provider secret does not exist"):
            async with session.begin_nested():
                await session.execute(update(ProviderConnection).where(
                    ProviderConnection.encrypted_api_key_ref == retired,
                ).values(deleted_at=None))


@pytest.mark.asyncio
async def test_collector_lock_timeout_rolls_back_without_blocking_writer(factory):
    from sqlalchemy.exc import DBAPIError

    async with factory.begin() as session:
        ref, _ = await seed(session, retired=OLD, deleted=True)
    async with factory() as blocker, factory() as collector:
        await blocker.execute(text("LOCK TABLE secrets IN ACCESS EXCLUSIVE MODE"))
        with pytest.raises(DBAPIError, match="lock timeout"):
            await asyncio.wait_for(collect_provider_secrets(collector), timeout=3)
        await collector.rollback()
        await blocker.rollback()
        assert await collector.get(Secret, ref) is not None
        assert await collect_provider_secrets(collector) == 1
        await collector.commit()
