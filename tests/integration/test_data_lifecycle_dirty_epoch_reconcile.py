from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event

import pytest
from sqlalchemy import create_engine, text

import loom.data_lifecycle_dirty_epoch_reconcile as dirty_epoch
from loom.data_lifecycle_dirty_epoch_reconcile import (
    DirtyEpochReconcileError,
    SqlAlchemyDirtyEpochReconciler,
)
from loom.data_lifecycle_gc import GcScope

SCOPE = GcScope(environment="staging", namespace="loom-staging")
NOW = datetime(2026, 8, 7, 21, 0, tzinfo=UTC)


def _reset(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM staging_mutation_epoch_events"))
        connection.execute(text("DELETE FROM staging_mutation_epochs"))
        connection.execute(text("DELETE FROM data_lifecycle_objects"))
        connection.execute(text("DELETE FROM data_lifecycle_authorities"))
        connection.execute(text("UPDATE alembic_version SET version_num='0075'"))


def _insert_dirty_authority(engine, owner_id: str) -> str:
    with engine.begin() as connection:
        return str(
            connection.execute(
            text(
                "INSERT INTO data_lifecycle_authorities "
                "(environment,namespace,data_class,owner_kind,owner_id,pinned,state,metadata) "
                    "VALUES "
                    "('staging','loom-staging','system','system',:owner_id,true,'active','{}') "
                    "RETURNING id"
            ),
            {"owner_id": owner_id},
            ).scalar_one()
        )


def test_digest_approved_dirty_epoch_reconciliation_is_atomic(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    try:
        _reset(engine)
        _insert_dirty_authority(engine, "dirty-reconcile-fixture")
        reconciler = SqlAlchemyDirtyEpochReconciler(engine, now=lambda: NOW)
        plan = reconciler.inventory(scope=SCOPE)
        assert plan.applicable

        with pytest.raises(DirtyEpochReconcileError, match="digest does not match"):
            reconciler.apply(
                plan=plan,
                approved_inventory_digest="0" * 64,
                request_id="req-dirty-epoch-fixture",
            )

        state = reconciler.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-dirty-epoch-fixture",
        )
        assert state.epoch == 1
        assert state.evidence_sha256 == plan.inventory_digest

        with engine.connect() as connection:
            epoch = connection.execute(
                text(
                    "SELECT epoch,reason,request_id,evidence_sha256 "
                    "FROM staging_mutation_epochs"
                )
            ).one()
            event = connection.execute(
                text(
                    "SELECT epoch,mutation_class,request_id,evidence_sha256 "
                    "FROM staging_mutation_epoch_events"
                )
            ).one()
        expected = (1, "object_rewrite", "req-dirty-epoch-fixture", plan.inventory_digest)
        assert tuple(epoch) == expected
        assert tuple(event) == expected
    finally:
        engine.dispose()


def test_dirty_epoch_reconciliation_rechecks_inventory_under_lock(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    try:
        _reset(engine)
        _insert_dirty_authority(engine, "dirty-reconcile-baseline")
        reconciler = SqlAlchemyDirtyEpochReconciler(engine, now=lambda: NOW)
        plan = reconciler.inventory(scope=SCOPE)
        _insert_dirty_authority(engine, "dirty-reconcile-drift")

        with pytest.raises(DirtyEpochReconcileError, match="inventory drifted"):
            reconciler.apply(
                plan=plan,
                approved_inventory_digest=plan.inventory_digest,
                request_id="req-dirty-epoch-drift",
            )

        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT count(*) FROM staging_mutation_epochs")
            ).scalar_one() == 0
    finally:
        engine.dispose()


def test_dirty_epoch_reconciliation_distinguishes_null_and_empty_version_ids(
    isolated_migration_postgres_url: str,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    try:
        _reset(engine)
        authority_id = _insert_dirty_authority(engine, "dirty-reconcile-version")
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_objects "
                    "(authority_id,environment,namespace,bucket,object_key,version_id,"
                    "content_sha256,size_bytes,created_at,state) VALUES "
                    "(:authority_id,'staging','loom-staging','artifacts','exact-key',NULL,"
                    ":digest,1,:created_at,'active')"
                ),
                {
                    "authority_id": authority_id,
                    "digest": "a" * 64,
                    "created_at": NOW,
                },
            )
        reconciler = SqlAlchemyDirtyEpochReconciler(engine, now=lambda: NOW)
        plan = reconciler.inventory(scope=SCOPE)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE data_lifecycle_objects SET version_id='' WHERE object_key='exact-key'")
            )

        with pytest.raises(DirtyEpochReconcileError, match="inventory drifted"):
            reconciler.apply(
                plan=plan,
                approved_inventory_digest=plan.inventory_digest,
                request_id="req-dirty-version-drift",
            )
    finally:
        engine.dispose()


def test_dirty_epoch_reconciliation_serializes_schema_revision_updates(
    isolated_migration_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(isolated_migration_postgres_url)
    release_inventory = Event()
    entered_locked_inventory = Event()
    writer_started = Event()
    try:
        _reset(engine)
        _insert_dirty_authority(engine, "dirty-reconcile-schema-lock")
        reconciler = SqlAlchemyDirtyEpochReconciler(engine, now=lambda: NOW)
        plan = reconciler.inventory(scope=SCOPE)
        original_inventory = dirty_epoch._inventory_connection

        def held_inventory(connection, *, scope):  # type: ignore[no-untyped-def]
            entered_locked_inventory.set()
            assert release_inventory.wait(timeout=5)
            return original_inventory(connection, scope=scope)

        monkeypatch.setattr(dirty_epoch, "_inventory_connection", held_inventory)

        def update_revision() -> None:
            with engine.begin() as connection:
                writer_started.set()
                connection.execute(text("UPDATE alembic_version SET version_num='0076'"))

        with ThreadPoolExecutor(max_workers=2) as executor:
            apply_future = executor.submit(
                reconciler.apply,
                plan=plan,
                approved_inventory_digest=plan.inventory_digest,
                request_id="req-dirty-schema-lock",
            )
            assert entered_locked_inventory.wait(timeout=5)
            update_future = executor.submit(update_revision)
            assert writer_started.wait(timeout=5)
            time.sleep(0.2)
            assert not update_future.done()
            release_inventory.set()
            assert apply_future.result(timeout=5).epoch == 1
            update_future.result(timeout=5)
    finally:
        release_inventory.set()
        _reset(engine)
        engine.dispose()
