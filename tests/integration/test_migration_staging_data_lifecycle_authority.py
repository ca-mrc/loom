"""Migration 0069 adds fail-closed staging lifecycle authority."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from loom.data_lifecycle import (
    DataClass,
    OwnerKind,
    StagingCapacity,
    staging_capacity_policy_digest,
)
from loom.data_lifecycle_capacity import StagingCapacityEvidence
from loom.data_lifecycle_capacity_sql import SqlAlchemyStagingCapacityStore
from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcScope,
    LifecycleGcExecutionError,
    ObservedObject,
    RegisteredObject,
    build_gc_plan,
    execute_gc,
    resume_gc,
)
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal, _copy_uuid_rows
from loom.data_lifecycle_inventory_sql import SqlAlchemyLifecycleInventory
from loom.data_lifecycle_legacy_sql import SqlAlchemyLegacyClassifier
from loom.data_lifecycle_registry import RuntimeLifecycleScope, ensure_lifecycle_authority
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    ProtectedMutationClass,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


def _cfg(postgres_url: str) -> Config:
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "database" / "migrations" / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture(scope="module")
def postgres_url_at_0065() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        command.upgrade(_cfg(url), "0065")
        yield url


def test_upgrade_adds_authority_journal_and_nullable_execution_links(
    postgres_url_at_0065: str,
) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        command.upgrade(cfg, "head")
        with engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'data_lifecycle_%'"
                    )
                )
            }
            linked = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.columns "
                        "WHERE table_schema='public' "
                        "AND column_name='lifecycle_authority_id'"
                    )
                )
            }
            mutation_tables = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public' AND table_name LIKE 'staging_mutation_%'"
                    )
                )
            }
    finally:
        engine.dispose()

    assert tables == {
        "data_lifecycle_authorities",
        "data_lifecycle_objects",
        "data_lifecycle_gc_runs",
        "data_lifecycle_gc_items",
        "data_lifecycle_gc_authorities",
    }
    assert linked == {
        "batches",
        "trials",
        "llm_calls",
        "trial_events",
        "trial_resource_usage",
        "service_execution_live_events",
        "artifacts",
        "execution_leases",
    }
    assert mutation_tables == {
        "staging_mutation_epochs",
        "staging_mutation_epoch_events",
    }


def test_constraints_reject_unbounded_or_cross_environment_authority(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO data_lifecycle_authorities "
                        "(environment, namespace, data_class, owner_kind, owner_id, pinned) "
                        "VALUES ('staging','loom-staging','trial','trial','bad',false)"
                    )
                )
        with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                conn.execute(
                    text(
                        "INSERT INTO staging_mutation_epochs "
                        "(environment, namespace, epoch, reason) "
                        "VALUES ('production','loom-prod',0,'bad')"
                    )
                )
    finally:
        engine.dispose()


def test_capacity_store_publishes_only_newer_exact_evidence(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    newer_capacity = StagingCapacity(12, 34, 56, 78)
    older_capacity = StagingCapacity(99, 99, 99, 99)
    newer_at = datetime(2026, 7, 20, 0, 5, tzinfo=UTC)
    try:
        store = SqlAlchemyStagingCapacityStore(engine)
        store.publish(
            StagingCapacityEvidence(
                namespace="loom-staging",
                capacity=newer_capacity,
                policy_sha256=staging_capacity_policy_digest(),
                evidence_sha256=newer_capacity.evidence_digest,
                observed_at=newer_at,
            )
        )
        with pytest.raises(RuntimeError, match="lost freshness authority"):
            store.publish(
                StagingCapacityEvidence(
                    namespace="loom-staging",
                    capacity=older_capacity,
                    policy_sha256=staging_capacity_policy_digest(),
                    evidence_sha256=older_capacity.evidence_digest,
                    observed_at=newer_at - timedelta(minutes=1),
                )
            )
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT namespace,object_count,bytes_used,disk_free_percent,"
                    "inode_free_percent,evidence_sha256,observed_at "
                    "FROM staging_lifecycle_capacity WHERE environment='staging'"
                )
            ).one()
        assert tuple(row) == (
            "loom-staging",
            12,
            34,
            56,
            78,
            newer_capacity.evidence_digest,
            newer_at,
        )
    finally:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM staging_lifecycle_capacity"))
        engine.dispose()


def test_gc_plan_copy_exceeds_postgres_parameter_limit_without_broadening(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    identifiers = [UUID(int=value) for value in range(1, 70_001)]
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_copy_scale_test "
                    "(authority_id uuid PRIMARY KEY) ON COMMIT DROP"
                )
            )
            _copy_uuid_rows(
                connection,
                "COPY gc_copy_scale_test (authority_id) FROM STDIN",
                identifiers,
            )
            count = connection.execute(text("SELECT count(*) FROM gc_copy_scale_test")).scalar_one()
        assert count == len(identifiers)
    finally:
        engine.dispose()


def test_epoch_compare_and_swap_updates_authority_and_appends_exact_event(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    occurred_at = datetime(2026, 7, 20, tzinfo=UTC)
    advance = MutationEpochAdvance(
        environment="staging",
        namespace="loom-staging",
        expected_epoch=0,
        mutation_class=ProtectedMutationClass.LIFECYCLE_GC,
        request_id="req-gc0000000",
        evidence_sha256="a" * 64,
        occurred_at=occurred_at,
    )
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            state = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(conn),
                advance,
            )
        with engine.connect() as conn:
            epoch_row = conn.execute(
                text(
                    "SELECT epoch, reason, request_id, evidence_sha256 "
                    "FROM staging_mutation_epochs WHERE environment='staging'"
                )
            ).one()
            event_row = conn.execute(
                text(
                    "SELECT epoch, mutation_class, request_id, evidence_sha256 "
                    "FROM staging_mutation_epoch_events WHERE environment='staging'"
                )
            ).one()
        assert state.epoch == 1
        assert tuple(epoch_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)
        assert tuple(event_row) == (1, "lifecycle_gc", "req-gc0000000", "a" * 64)

        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="stale"):
                advance_mutation_epoch(SqlAlchemyMutationEpochStore(conn), advance)
    finally:
        engine.dispose()


def test_sql_gc_resume_advances_from_intervening_mutation_epoch(
    postgres_url_at_0065: str,
) -> None:
    command.upgrade(_cfg(postgres_url_at_0065), "head")
    engine = create_engine(postgres_url_at_0065)
    authority_id = uuid4()
    empty_authority_id = uuid4()
    object_id = uuid4()
    now = datetime(2026, 7, 20, 1, tzinfo=UTC)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO staging_mutation_epochs "
                "(environment, namespace, epoch, reason) "
                "VALUES ('staging','loom-staging',0,'bootstrap') "
                "ON CONFLICT (environment) DO NOTHING"
            )
        )
        epoch = connection.execute(
            text("SELECT epoch FROM staging_mutation_epochs WHERE environment='staging'")
        ).scalar_one()
    plan = build_gc_plan(
        scope=GcScope(environment="staging", namespace="loom-staging"),
        mutation_epoch=epoch,
        now=now,
        authorities=[
            AuthorityInventory(
                id=authority_id,
                environment="staging",
                namespace="loom-staging",
                owner_kind="trial",
                owner_id="integration-trial",
                expires_at=now - timedelta(days=1),
                pinned=False,
                state="active",
            ),
            AuthorityInventory(
                id=empty_authority_id,
                environment="staging",
                namespace="loom-staging",
                owner_kind="trial",
                owner_id="integration-empty-trial",
                expires_at=now - timedelta(days=1),
                pinned=False,
                state="active",
            ),
        ],
        objects=[
            RegisteredObject(
                id=object_id,
                authority_id=authority_id,
                environment="staging",
                namespace="loom-staging",
                bucket="loom-staging-artifacts",
                object_key="integration/trial.json",
                version_id="version-1",
                content_sha256="b" * 64,
                size_bytes=7,
                state="active",
            )
        ],
    )

    class NoBusinessRows:
        def delete_exact(self, connection, authority_ids) -> None:
            assert authority_ids == tuple(sorted((authority_id, empty_authority_id)))

    class Deleter:
        deleted = False
        verify = False

        def delete_exact(self, item) -> None:
            assert item.id == object_id
            self.deleted = True

        def exact_absent(self, item) -> bool:
            return self.verify and self.deleted and item.id == object_id

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_authorities "
                    "(id, environment, namespace, data_class, owner_kind, owner_id, "
                    "created_at, expires_at, pinned, state) VALUES "
                    "(:id,'staging','loom-staging','trial','trial','integration-trial',"
                    ":created_at,:expires_at,false,'active')"
                ),
                {
                    "id": authority_id,
                    "created_at": now - timedelta(days=8),
                    "expires_at": now - timedelta(days=1),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_authorities "
                    "(id, environment, namespace, data_class, owner_kind, owner_id, "
                    "created_at, expires_at, pinned, state) VALUES "
                    "(:id,'staging','loom-staging','trial','trial','integration-empty-trial',"
                    ":created_at,:expires_at,false,'active')"
                ),
                {
                    "id": empty_authority_id,
                    "created_at": now - timedelta(days=8),
                    "expires_at": now - timedelta(days=1),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_objects "
                    "(id, authority_id, environment, namespace, bucket, object_key, "
                    "version_id, content_sha256, size_bytes, created_at, state) VALUES "
                    "(:id,:authority_id,'staging','loom-staging','loom-staging-artifacts',"
                    "'integration/trial.json','version-1',:sha,7,:created_at,'active')"
                ),
                {
                    "id": object_id,
                    "authority_id": authority_id,
                    "sha": "b" * 64,
                    "created_at": now - timedelta(days=8),
                },
            )
        journal = SqlAlchemyGcJournal(engine, metadata_purger=NoBusinessRows())
        deleter = Deleter()
        with pytest.raises(LifecycleGcExecutionError, match="still present"):
            execute_gc(
                plan=plan,
                requested_by="qianyi",
                journal=journal,
                object_deleter=deleter,
                dry_run=False,
                request_id="req-gcinteg00",
                completed_at=now,
            )
        with engine.connect() as connection:
            run_id, inventory = connection.execute(
                text(
                    "SELECT id, inventory FROM data_lifecycle_gc_runs "
                    "WHERE inventory->>'inventory_digest'=:digest"
                ),
                {"digest": plan.inventory_digest},
            ).one()
            item_evidence = connection.execute(
                text(
                    "SELECT authority_id, bucket, object_key, version_id, "
                    "content_sha256, size_bytes FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND object_id=:object_id"
                ),
                {"run_id": run_id, "object_id": object_id},
            ).one()
            authority_evidence = tuple(
                connection.execute(
                    text(
                        "SELECT authority_id FROM data_lifecycle_gc_authorities "
                        "WHERE gc_run_id=:run_id ORDER BY authority_id"
                    ),
                    {"run_id": run_id},
                ).scalars()
            )
        assert inventory == {
            "schema_version": 2,
            "environment": "staging",
            "namespace": "loom-staging",
            "mutation_epoch": epoch,
            "planned_at": now.isoformat(),
            "inventory_digest": plan.inventory_digest,
            "authority_count": 2,
            "object_count": 1,
            "bytes_total": 7,
            "blockers": [],
        }
        assert tuple(item_evidence) == (
            authority_id,
            "loom-staging-artifacts",
            "integration/trial.json",
            "version-1",
            "b" * 64,
            7,
        )
        assert authority_evidence == tuple(sorted((authority_id, empty_authority_id)))
        with engine.begin() as connection:
            intervening = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(connection),
                MutationEpochAdvance(
                    environment="staging",
                    namespace="loom-staging",
                    expected_epoch=epoch,
                    mutation_class=ProtectedMutationClass.ROLLOUT_APPLY,
                    request_id="req-rollout-intervening",
                    evidence_sha256="c" * 64,
                    occurred_at=now + timedelta(minutes=1),
                ),
            )
        assert intervening.epoch == epoch + 1

        class RaceAfterLoadJournal:
            def __init__(self, delegate: SqlAlchemyGcJournal) -> None:
                self.delegate = delegate

            def load_resume(self, exact_run_id):
                snapshot = self.delegate.load_resume(exact_run_id)
                with engine.begin() as connection:
                    raced = advance_mutation_epoch(
                        SqlAlchemyMutationEpochStore(connection),
                        MutationEpochAdvance(
                            environment="staging",
                            namespace="loom-staging",
                            expected_epoch=snapshot.completion_mutation_epoch,
                            mutation_class=ProtectedMutationClass.ROLLOUT_APPLY,
                            request_id="req-rollout-after-load",
                            evidence_sha256="d" * 64,
                            occurred_at=now + timedelta(minutes=2),
                        ),
                    )
                assert raced.epoch == epoch + 2
                return snapshot

            def __getattr__(self, name):
                return getattr(self.delegate, name)

        deleter.verify = True
        with pytest.raises(LifecycleGcExecutionError, match="stale"):
            resume_gc(
                run_id=run_id,
                request_id="req-gcresume-raced",
                completed_at=now + timedelta(minutes=3),
                journal=RaceAfterLoadJournal(journal),
                object_deleter=deleter,
            )
        with engine.connect() as connection:
            raced_run = connection.execute(
                text(
                    "SELECT state, mutation_epoch_before, mutation_epoch_after "
                    "FROM data_lifecycle_gc_runs WHERE id=:id"
                ),
                {"id": run_id},
            ).one()
            raced_item_state = connection.execute(
                text(
                    "SELECT state FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:id AND object_id=:object_id"
                ),
                {"id": run_id, "object_id": object_id},
            ).scalar_one()
            raced_lifecycle_events = connection.execute(
                text(
                    "SELECT count(*) FROM staging_mutation_epoch_events "
                    "WHERE environment='staging' AND namespace='loom-staging' "
                    "AND request_id='req-gcresume-raced'"
                )
            ).scalar_one()
        assert tuple(raced_run) == ("failed", epoch, None)
        assert raced_item_state == "metadata_deleted"
        assert raced_lifecycle_events == 0

        result = resume_gc(
            run_id=run_id,
            request_id="req-gcresume-intervening",
            completed_at=now + timedelta(minutes=4),
            journal=journal,
            object_deleter=deleter,
        )
        with engine.connect() as connection:
            run = connection.execute(
                text(
                    "SELECT state, mutation_epoch_before, mutation_epoch_after "
                    "FROM data_lifecycle_gc_runs WHERE id=:id"
                ),
                {"id": result.run_id},
            ).one()
            item_state = connection.execute(
                text(
                    "SELECT state FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:id AND object_id=:object_id"
                ),
                {"id": result.run_id, "object_id": object_id},
            ).scalar_one()
            authority_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_authorities WHERE id IN (:a,:b)"),
                {"a": authority_id, "b": empty_authority_id},
            ).scalar_one()
            completion_event = connection.execute(
                text(
                    "SELECT mutation_class, request_id, evidence_sha256 "
                    "FROM staging_mutation_epoch_events "
                    "WHERE environment='staging' AND namespace='loom-staging' "
                    "AND epoch=:epoch"
                ),
                {"epoch": epoch + 3},
            ).one()
            completion_request_count = connection.execute(
                text(
                    "SELECT count(*) FROM staging_mutation_epoch_events "
                    "WHERE environment='staging' AND namespace='loom-staging' "
                    "AND request_id='req-gcresume-intervening'"
                )
            ).scalar_one()
        assert tuple(run) == ("completed", epoch, epoch + 3)
        assert item_state == "metadata_deleted"
        assert authority_count == 0
        assert tuple(completion_event) == (
            ProtectedMutationClass.LIFECYCLE_GC,
            "req-gcresume-intervening",
            plan.inventory_digest,
        )
        assert completion_request_count == 1
    finally:
        engine.dispose()


def test_gc_mark_rejects_exact_object_identity_drift(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    authority_id = uuid4()
    object_id = uuid4()
    now = datetime(2026, 7, 20, 1, 30, tzinfo=UTC)
    try:
        with engine.begin() as connection:
            epoch = connection.execute(
                text("SELECT epoch FROM staging_mutation_epochs WHERE environment='staging'")
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_authorities "
                    "(id, environment, namespace, data_class, owner_kind, owner_id, "
                    "created_at, expires_at, pinned, state) VALUES "
                    "(:id,'staging','loom-staging','artifact','artifact','drift-artifact',"
                    ":created_at,:expires_at,false,'active')"
                ),
                {
                    "id": authority_id,
                    "created_at": now - timedelta(days=8),
                    "expires_at": now - timedelta(days=1),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_objects "
                    "(id, authority_id, environment, namespace, bucket, object_key, "
                    "content_sha256, size_bytes, created_at, state) VALUES "
                    "(:id,:authority_id,'staging','loom-staging','loom-staging-artifacts',"
                    "'integration/drift.json',:sha,8,:created_at,'active')"
                ),
                {
                    "id": object_id,
                    "authority_id": authority_id,
                    "sha": "c" * 64,
                    "created_at": now - timedelta(days=8),
                },
            )
        stale_plan = build_gc_plan(
            scope=GcScope(environment="staging", namespace="loom-staging"),
            mutation_epoch=epoch,
            now=now,
            authorities=[
                AuthorityInventory(
                    id=authority_id,
                    environment="staging",
                    namespace="loom-staging",
                    owner_kind="artifact",
                    owner_id="drift-artifact",
                    expires_at=now - timedelta(days=1),
                    pinned=False,
                    state="active",
                )
            ],
            objects=[
                RegisteredObject(
                    id=object_id,
                    authority_id=authority_id,
                    environment="staging",
                    namespace="loom-staging",
                    bucket="loom-staging-artifacts",
                    object_key="integration/drift.json",
                    version_id=None,
                    content_sha256="c" * 64,
                    size_bytes=7,
                    state="active",
                )
            ],
        )
        with pytest.raises(RuntimeError, match="object mark count"):
            SqlAlchemyGcJournal(engine).begin_apply(
                plan=stale_plan,
                requested_by="qianyi",
                deletion_token=uuid4(),
            )
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT state FROM data_lifecycle_objects WHERE id=:id"),
                    {"id": object_id},
                ).scalar_one()
                == "active"
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM data_lifecycle_gc_runs "
                        "WHERE inventory->>'inventory_digest'=:digest"
                    ),
                    {"digest": stale_plan.inventory_digest},
                ).scalar_one()
                == 0
            )
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM data_lifecycle_objects WHERE id=:id"), {"id": object_id}
            )
            connection.execute(
                text("DELETE FROM data_lifecycle_authorities WHERE id=:id"),
                {"id": authority_id},
            )
        engine.dispose()


def test_sql_inventory_is_read_only_and_binds_unclassified_counts(
    postgres_url_at_0065: str,
) -> None:
    engine = create_engine(postgres_url_at_0065)
    scope = GcScope(environment="staging", namespace="loom-staging")
    try:
        with engine.connect() as connection:
            gc_runs_before = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_runs")
            ).scalar_one()
        snapshot = SqlAlchemyLifecycleInventory(engine).load(scope=scope)
        plan = snapshot.build_plan(now=datetime(2026, 7, 20, 2, tzinfo=UTC))
        with engine.connect() as connection:
            gc_runs_after = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_gc_runs")
            ).scalar_one()
    finally:
        engine.dispose()

    assert snapshot.scope == scope
    assert dict(snapshot.unclassified_rows) == {
        "artifacts": 0,
        "batches": 0,
        "llm_calls": 0,
        "trial_events": 0,
        "trial_resource_usage": 0,
        "service_execution_live_events": 0,
        "trials": 0,
    }
    assert plan.mutation_epoch == snapshot.mutation_epoch
    assert gc_runs_after == gc_runs_before


def test_execution_authority_registration_is_transactional_and_idempotent(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    owner_id = f"trial-{uuid4()}"
    created_at = datetime(2026, 7, 20, 3, tzinfo=UTC)
    engine = create_engine(postgres_url_at_0065)
    async_engine = create_async_engine(postgres_url_at_0065)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"lifecycle-{team_id}"},
        )

    async def _register() -> tuple[object, object]:
        sessions = async_sessionmaker(async_engine, expire_on_commit=False)
        spec = RuntimeLifecycleScope(
            environment="staging", namespace="loom-staging"
        ).authority_spec(
            team_id=team_id,
            data_class=DataClass.TRIAL,
            owner_kind=OwnerKind.TRIAL,
            owner_id=owner_id,
            created_at=created_at,
        )
        async with sessions.begin() as session:
            first = await ensure_lifecycle_authority(session, spec=spec)
            second = await ensure_lifecycle_authority(session, spec=spec)
        return first, second

    try:
        first, second = asyncio.run(_register())
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT environment,namespace,team_id,data_class,owner_kind,owner_id,"
                    "created_at,expires_at,pinned,state FROM data_lifecycle_authorities "
                    "WHERE id=:id"
                ),
                {"id": first},
            ).one()
        assert first == second
        assert tuple(row) == (
            "staging",
            "loom-staging",
            team_id,
            "trial",
            "trial",
            owner_id,
            created_at,
            created_at + timedelta(days=7),
            False,
            "active",
        )
    finally:
        asyncio.run(async_engine.dispose())
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM data_lifecycle_authorities WHERE owner_id=:owner_id"),
                {"owner_id": owner_id},
            )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_artifact_classification_is_digest_approved_and_epoch_bound(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    artifact_id = uuid4()
    created_at = datetime(2026, 7, 12, 4, tzinfo=UTC)
    object_body = b"legacy artifact body"
    object_sha = hashlib.sha256(object_body).hexdigest()
    engine = create_engine(postgres_url_at_0065)

    class Inspector:
        def inspect(self, *, bucket, object_key, version_id):
            assert bucket == "loom-staging-artifacts"
            assert object_key == f"teams/{team_id}/artifacts/{artifact_id}.json"
            assert version_id is None
            return None, object_sha, len(object_body)

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash, storage, created_at) "
                    "VALUES (:id,'benchmark','legacy',:team_id,:content_hash,"
                    "CAST(:storage AS jsonb),:created_at)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "content_hash": f"sha256:{object_sha}",
                    "storage": json.dumps(
                        {
                            "bucket": "loom-staging-artifacts",
                            "key": f"teams/{team_id}/artifacts/{artifact_id}.json",
                            "size_bytes": len(object_body),
                        }
                    ),
                    "created_at": created_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(engine, Inspector())
        scope = GcScope(environment="staging", namespace="loom-staging")
        plan = classifier.inventory(
            scope=scope,
            planned_at=datetime(2026, 7, 20, 4, tzinfo=UTC),
        )
        plan.require_applicable()
        assert [row.row_id for row in plan.rows] == [artifact_id]
        assert plan.objects[0].content_sha256 == object_sha
        assert plan.authorities[0].data_class is DataClass.BENCHMARK
        assert plan.authorities[0].pinned is True
        assert plan.authorities[0].expires_at is None

        with pytest.raises(RuntimeError, match="digest"):
            classifier.apply(
                plan=plan,
                approved_inventory_digest="0" * 64,
                request_id="req-legacybad0",
                applied_at=datetime(2026, 7, 20, 4, 1, tzinfo=UTC),
            )

        state = classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacygood",
            applied_at=datetime(2026, 7, 20, 4, 2, tzinfo=UTC),
        )
        with engine.connect() as connection:
            artifact_authority = connection.execute(
                text("SELECT lifecycle_authority_id FROM artifacts WHERE id=:id"),
                {"id": artifact_id},
            ).scalar_one()
            registered = connection.execute(
                text(
                    "SELECT authority_id, content_sha256, size_bytes "
                    "FROM data_lifecycle_objects WHERE authority_id=:id"
                ),
                {"id": artifact_authority},
            ).one()
            pinned_authority = connection.execute(
                text(
                    "SELECT data_class, owner_kind, pinned, expires_at "
                    "FROM data_lifecycle_authorities WHERE id=:id"
                ),
                {"id": artifact_authority},
            ).one()
        assert state.epoch == plan.mutation_epoch + 1
        assert tuple(registered) == (artifact_authority, object_sha, len(object_body))
        assert tuple(pinned_authority) == ("benchmark", "benchmark", True, None)
    finally:
        with engine.begin() as connection:
            authority_ids = list(
                connection.execute(
                    text(
                        "SELECT lifecycle_authority_id FROM artifacts "
                        "WHERE id=:id AND lifecycle_authority_id IS NOT NULL"
                    ),
                    {"id": artifact_id},
                ).scalars()
            )
            connection.execute(text("DELETE FROM artifacts WHERE id=:id"), {"id": artifact_id})
            if authority_ids:
                connection.execute(
                    text(
                        "DELETE FROM data_lifecycle_objects WHERE authority_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": authority_ids},
                )
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": authority_ids},
                )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_supplemental_catalog_object_is_exact_and_pinned(
    postgres_url_at_0065: str,
) -> None:
    command.upgrade(_cfg(postgres_url_at_0065), "head")
    team_id = uuid4()
    artifact_id = uuid4()
    created_at = datetime(2026, 7, 12, 4, 15, tzinfo=UTC)
    primary_key = f"teams/{team_id}/artifacts/{artifact_id}.json"
    catalog_key = f"tasksets/user/{team_id}/alpha/manifest.yaml"
    bodies = {primary_key: b"artifact", catalog_key: b"manifest"}
    engine = create_engine(postgres_url_at_0065)
    created_authority_ids: list[UUID] = []

    class Inspector:
        def inspect(self, *, bucket, object_key, version_id):
            assert bucket == "loom-staging-artifacts"
            assert version_id is None
            body = bodies[object_key]
            return None, hashlib.sha256(body).hexdigest(), len(body)

    class Inventory:
        def load(self, *, buckets):
            assert tuple(sorted(buckets)) == (
                "loom-staging-artifacts",
                "loom-staging-trajectories",
            )
            return tuple(
                ObservedObject(
                    bucket="loom-staging-artifacts",
                    object_key=key,
                    version_id=None,
                    size_bytes=len(body),
                    last_modified=created_at,
                )
                for key, body in bodies.items()
            )

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-supplemental-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash, storage, created_at) "
                    "VALUES (:id,'evidence_bundle','legacy',:team_id,:content_hash,"
                    "CAST(:storage AS jsonb),:created_at)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "content_hash": f"sha256:{hashlib.sha256(b'artifact').hexdigest()}",
                    "storage": json.dumps(
                        {
                            "bucket": "artifacts",
                            "key": primary_key,
                            "size_bytes": len(b"artifact"),
                        }
                    ),
                    "created_at": created_at,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO task_sets "
                    "(id,owning_team_id,slug,display_name,status,intents,manifest_blob_uri,"
                    "created_at,updated_at) VALUES "
                    "(:id,:team_id,'alpha','Alpha','ready',ARRAY['evaluation'],:uri,"
                    ":created_at,:created_at)"
                ),
                {
                    "id": f"ts/{team_id}/alpha",
                    "team_id": team_id,
                    "uri": f"s3://loom-staging-artifacts/{catalog_key}",
                    "created_at": created_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(
            engine,
            Inspector(),
            object_inventory=Inventory(),
            bucket_aliases={
                "artifacts": "loom-staging-artifacts",
                "trajectories": "loom-staging-trajectories",
            },
        )
        plan = classifier.inventory(
            scope=GcScope(environment="staging", namespace="loom-staging"),
            planned_at=datetime(2026, 7, 20, 4, 15, tzinfo=UTC),
        )
        plan.require_applicable()
        assert len(plan.objects) == 2
        assert {item.object_key for item in plan.objects} == {primary_key, catalog_key}
        catalog_authority = next(
            item for item in plan.authorities if item.owner_id.startswith("taskset:")
        )
        created_authority_ids = [
            item.id
            for item in plan.authorities
            if item.owner_id in {str(artifact_id), f"taskset:ts/{team_id}/alpha"}
        ]
        assert len(created_authority_ids) == 2
        assert catalog_authority.data_class is DataClass.CATALOG
        assert catalog_authority.team_id == team_id
        assert catalog_authority.pinned is True

        classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacy-supplemental",
            applied_at=datetime(2026, 7, 20, 4, 16, tzinfo=UTC),
        )
        with engine.connect() as connection:
            registered = connection.execute(
                text(
                    "SELECT a.data_class,a.pinned,o.object_key FROM data_lifecycle_objects o "
                    "JOIN data_lifecycle_authorities a ON a.id=o.authority_id "
                    "ORDER BY o.object_key"
                )
            ).all()
        assert ("catalog", True, catalog_key) in map(tuple, registered)
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE artifacts SET lifecycle_authority_id=NULL WHERE id=:id"),
                {"id": artifact_id},
            )
            if created_authority_ids:
                connection.execute(
                    text(
                        "DELETE FROM data_lifecycle_objects WHERE authority_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": created_authority_ids},
                )
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": created_authority_ids},
                )
            connection.execute(
                text("DELETE FROM task_sets WHERE owning_team_id=:team_id"),
                {"team_id": team_id},
            )
            connection.execute(text("DELETE FROM artifacts WHERE id=:id"), {"id": artifact_id})
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_orphan_llm_call_receives_explicit_ephemeral_authority(
    postgres_url_at_0065: str,
) -> None:
    call_id = uuid4()
    team_id = uuid4()
    missing_trial_id = uuid4()
    captured_at = datetime(2026, 7, 12, 4, tzinfo=UTC)
    engine = create_engine(postgres_url_at_0065)

    class NoObjectInspector:
        def inspect(self, *, bucket, object_key, version_id):
            raise AssertionError("an orphan LLM call has no object authority")

    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-orphan-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO llm_calls "
                    "(id,team_id,trial_id,step_id,model,dialect,input_tokens,output_tokens,"
                    "provider_extras,cost_usd,rate_card_hash,captured_at) VALUES "
                    "(:id,:team_id,:trial_id,'orphan-step','model','openai',1,1,'{}',"
                    "0.0,:rate_card_hash,:captured_at)"
                ),
                {
                    "id": call_id,
                    "team_id": team_id,
                    "trial_id": missing_trial_id,
                    "rate_card_hash": "d" * 64,
                    "captured_at": captured_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(engine, NoObjectInspector())
        plan = classifier.inventory(
            scope=GcScope(environment="staging", namespace="loom-staging"),
            planned_at=datetime(2026, 7, 20, 4, 30, tzinfo=UTC),
            expire_created_before=datetime(2026, 7, 19, tzinfo=UTC),
        )
        plan.require_applicable()
        assert plan.blockers == ()
        assert [(row.table, row.row_id, row.owner_kind) for row in plan.rows] == [
            ("llm_calls", call_id, OwnerKind.ORPHAN)
        ]
        authority = plan.authorities[0]
        assert authority.data_class is DataClass.EVENT
        assert authority.owner_kind is OwnerKind.ORPHAN
        assert authority.owner_id == f"llm-call:{call_id}"
        assert authority.team_id == team_id
        assert authority.created_at == captured_at
        assert authority.expires_at == datetime(2026, 7, 19, tzinfo=UTC)

        classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacy-orphan",
            applied_at=datetime(2026, 7, 20, 4, 31, tzinfo=UTC),
        )
        with engine.connect() as connection:
            linked_authority = connection.execute(
                text("SELECT lifecycle_authority_id FROM llm_calls WHERE id=:id"),
                {"id": call_id},
            ).scalar_one()
            authority_row = connection.execute(
                text(
                    "SELECT team_id,data_class,owner_kind,owner_id,pinned,expires_at "
                    "FROM data_lifecycle_authorities WHERE id=:id"
                ),
                {"id": linked_authority},
            ).one()
        assert tuple(authority_row) == (
            team_id,
            "event",
            "orphan",
            f"llm-call:{call_id}",
            False,
            datetime(2026, 7, 19, tzinfo=UTC),
        )
    finally:
        with engine.begin() as connection:
            authority_ids = list(
                connection.execute(
                    text(
                        "SELECT lifecycle_authority_id FROM llm_calls "
                        "WHERE id=:id AND lifecycle_authority_id IS NOT NULL"
                    ),
                    {"id": call_id},
                ).scalars()
            )
            connection.execute(text("DELETE FROM llm_calls WHERE id=:id"), {"id": call_id})
            for authority_id in authority_ids:
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id=:id"),
                    {"id": authority_id},
                )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_legacy_absent_object_is_bound_as_explicit_authority_evidence(
    postgres_url_at_0065: str,
) -> None:
    team_id = uuid4()
    artifact_id = uuid4()
    created_at = datetime(2026, 7, 12, 5, tzinfo=UTC)
    engine = create_engine(postgres_url_at_0065)

    class Inspector:
        def inspect(self, *, bucket, object_key, version_id):
            assert bucket == "loom-staging-artifacts"
            assert object_key == f"teams/{team_id}/artifacts/{artifact_id}.json"
            assert version_id is None
            return None

    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": f"legacy-absent-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash, storage, created_at) "
                    "VALUES (:id,'evidence_bundle','legacy-absent',:team_id,"
                    "'pending:legacy-unhashed',CAST(:storage AS jsonb),:created_at)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "storage": json.dumps(
                        {
                            "bucket": "artifacts",
                            "key": f"teams/{team_id}/artifacts/{artifact_id}.json",
                            "size_bytes": 0,
                        }
                    ),
                    "created_at": created_at,
                },
            )
        classifier = SqlAlchemyLegacyClassifier(
            engine,
            Inspector(),
            bucket_aliases={"artifacts": "loom-staging-artifacts"},
        )
        scope = GcScope(environment="staging", namespace="loom-staging")
        plan = classifier.inventory(
            scope=scope,
            planned_at=datetime(2026, 7, 20, 5, tzinfo=UTC),
        )
        plan.require_applicable()
        assert plan.objects == ()
        assert [item.row_id for item in plan.absent_objects] == [artifact_id]

        state = classifier.apply(
            plan=plan,
            approved_inventory_digest=plan.inventory_digest,
            request_id="req-legacyabsent",
            applied_at=datetime(2026, 7, 20, 5, 1, tzinfo=UTC),
        )
        with engine.connect() as connection:
            artifact_authority = connection.execute(
                text("SELECT lifecycle_authority_id FROM artifacts WHERE id=:id"),
                {"id": artifact_id},
            ).scalar_one()
            registered_count = connection.execute(
                text("SELECT count(*) FROM data_lifecycle_objects WHERE authority_id=:id"),
                {"id": artifact_authority},
            ).scalar_one()
            authority = connection.execute(
                text(
                    "SELECT data_class, pinned, metadata FROM data_lifecycle_authorities "
                    "WHERE id=:id"
                ),
                {"id": artifact_authority},
            ).one()
        assert state.epoch == plan.mutation_epoch + 1
        assert registered_count == 0
        assert authority.data_class == "artifact"
        assert authority.pinned is False
        assert authority.metadata == {
            "classification": "legacy-staging-v2",
            "expire_created_before": None,
            "inventory_digest": plan.inventory_digest,
            "object_state": "verified_absent",
        }
    finally:
        with engine.begin() as connection:
            authority_ids = list(
                connection.execute(
                    text(
                        "SELECT lifecycle_authority_id FROM artifacts "
                        "WHERE id=:id AND lifecycle_authority_id IS NOT NULL"
                    ),
                    {"id": artifact_id},
                ).scalars()
            )
            connection.execute(text("DELETE FROM artifacts WHERE id=:id"), {"id": artifact_id})
            if authority_ids:
                connection.execute(
                    text(
                        "DELETE FROM data_lifecycle_objects WHERE authority_id IN :ids"
                    ).bindparams(bindparam("ids", expanding=True)),
                    {"ids": authority_ids},
                )
                connection.execute(
                    text("DELETE FROM data_lifecycle_authorities WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": authority_ids},
                )
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
        engine.dispose()


def test_downgrade_refuses_to_discard_lifecycle_data(postgres_url_at_0065: str) -> None:
    cfg = _cfg(postgres_url_at_0065)
    engine = create_engine(postgres_url_at_0065)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment, namespace, epoch, reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
        with pytest.raises(Exception, match="deployment data remains"):
            command.downgrade(cfg, "0065")
    finally:
        engine.dispose()
