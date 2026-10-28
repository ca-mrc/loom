"""Regression coverage for shared integration-database cleanup helpers."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, insert, select, text, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from loom.db.schema import DataLifecycleAuthority, Team, Trial
from tests.integration import conftest as integration_conftest
from tests.integration.gateway_db import (
    delete_gateway_trial,
    delete_lifecycle_authorities,
    delete_teams_and_quotas,
    insert_gateway_trial,
)
from tests.support import postgres_databases


def test_delete_teams_and_quotas_is_scoped_to_owned_ids() -> None:
    session = MagicMock(spec=Session)
    owned_team_ids = (uuid4(), uuid4())

    delete_teams_and_quotas(session, owned_team_ids)

    statements = [call.args[0] for call in session.execute.call_args_list]
    assert len(statements) == 2
    assert all(statement.whereclause is not None for statement in statements)
    assert all(
        list(statement.compile().params.values()) == [list(owned_team_ids)]
        for statement in statements
    )

    session.reset_mock()
    delete_teams_and_quotas(session, ())
    session.execute.assert_not_called()


def test_gateway_trial_cleanup_preserves_cross_scope_and_team_sentinels(
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    sentinel_team_id = uuid4()
    trial_id = uuid4()
    trial_authority_id = uuid4()
    event_authority_id = uuid4()
    cross_scope_id = uuid4()
    cross_team_id = uuid4()
    now = datetime.now(UTC)

    with session_factory() as session:
        session.execute(
            insert(Team),
            (
                {"id": team_id, "name": f"gateway-cleanup-{team_id}"},
                {
                    "id": sentinel_team_id,
                    "name": f"gateway-cleanup-sentinel-{sentinel_team_id}",
                },
            ),
        )
        task_id = insert_gateway_trial(
            session,
            team_id=team_id,
            trial_id=trial_id,
        )
        authority_base = {
            "namespace": "loom",
            "owner_kind": "trial",
            "owner_id": str(trial_id),
            "created_at": now,
            "expires_at": None,
            "pinned": True,
            "state": "active",
        }
        session.execute(
            insert(DataLifecycleAuthority),
            (
                {
                    **authority_base,
                    "id": trial_authority_id,
                    "environment": "development",
                    "team_id": team_id,
                    "data_class": "trial",
                },
                {
                    **authority_base,
                    "id": event_authority_id,
                    "environment": "development",
                    "team_id": team_id,
                    "data_class": "event",
                },
                {
                    **authority_base,
                    "id": cross_scope_id,
                    "environment": "production",
                    "namespace": "production-sentinel",
                    "team_id": sentinel_team_id,
                    "data_class": "trial",
                },
                {
                    **authority_base,
                    "id": cross_team_id,
                    "environment": "development",
                    "team_id": sentinel_team_id,
                    "data_class": "artifact",
                    "owner_kind": "artifact",
                    "owner_id": "cross-team-sentinel",
                },
            ),
        )
        session.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(lifecycle_authority_id=trial_authority_id)
        )
        session.commit()

    try:
        with session_factory() as session:
            delete_lifecycle_authorities(
                session,
                bindings=(("artifact", "artifact", "cross-team-sentinel", team_id),),
            )
            delete_gateway_trial(session, trial_id=trial_id, task_id=task_id)
            session.commit()

        with session_factory() as session:
            remaining_ids = set(session.scalars(select(DataLifecycleAuthority.id)))
        assert trial_authority_id not in remaining_ids
        assert event_authority_id not in remaining_ids
        assert cross_scope_id in remaining_ids
        assert cross_team_id in remaining_ids
    finally:
        with session_factory() as session:
            session.execute(
                delete(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.id.in_((cross_scope_id, cross_team_id))
                )
            )
            session.execute(delete(Team).where(Team.id.in_((team_id, sentinel_team_id))))
            session.commit()
        engine.dispose()


class _FailingDropConnection:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        self._statements.append("terminate")

    def exec_driver_sql(self, statement: str) -> None:
        self._statements.append(statement)
        if statement.startswith("DROP DATABASE"):
            raise RuntimeError("synthetic drop failure")


class _FailingDropEngine:
    def __init__(self) -> None:
        self.dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
        self.statements: list[str] = []
        self.disposed = False

    def connect(self) -> _FailingDropConnection:
        return _FailingDropConnection(self.statements)

    def dispose(self) -> None:
        self.disposed = True


def test_isolated_migration_fixture_disposes_engine_when_drop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_engine = _FailingDropEngine()
    monkeypatch.setattr(
        postgres_databases,
        "create_engine",
        lambda *_args, **_kwargs: admin_engine,
    )
    fixture_factory: Any = (
        integration_conftest.isolated_migration_postgres_url.__wrapped__  # type: ignore[attr-defined]
    )
    fixture: Generator[str, None, None] = fixture_factory(
        "postgresql+psycopg://test:test@localhost/source",
    )

    isolated_url = next(fixture)
    assert "/loom_migration_" in isolated_url
    with pytest.raises(RuntimeError, match="synthetic drop failure"):
        fixture.close()

    assert admin_engine.disposed is True
    assert any(statement.startswith("CREATE DATABASE") for statement in admin_engine.statements)
    assert any(statement.startswith("DROP DATABASE") for statement in admin_engine.statements)


def test_migration_template_runs_history_once_and_isolates_clones(
    postgres_cluster_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # This test owns a complete template migration; it needs only the server,
    # not another fully migrated application database before its counted chain.
    run = postgres_databases.subprocess.run
    upgrades = 0

    def counted_upgrade(args: Any, **kwargs: Any) -> Any:
        nonlocal upgrades
        if "alembic" in args:
            upgrades += 1
        return run(args, **kwargs)

    monkeypatch.setattr(postgres_databases.subprocess, "run", counted_upgrade)

    def template_factory(url: str) -> Any:
        return postgres_databases.isolated_migration_database(
            url, template_name="template0", prepare_template=True
        )
    clone_factory = integration_conftest.isolated_migration_postgres_url.__wrapped__
    names: list[str] = []
    admin = create_engine(make_url(postgres_cluster_url).set(database="postgres"))
    try:
        with ExitStack() as stack:
            template = stack.enter_context(closing(template_factory(postgres_cluster_url)))
            template_url = next(template)
            template_name = make_url(template_url).database
            assert template_name is not None
            names.append(template_name)
            with admin.connect() as connection:
                assert (
                    connection.scalar(
                        text("SELECT datallowconn FROM pg_database WHERE datname = :name"),
                        {"name": template_name},
                    )
                    is False
                )

            versions = []
            for _ in range(2):
                clone = stack.enter_context(closing(clone_factory(template_url)))
                clone_url = next(clone)
                clone_name = make_url(clone_url).database
                assert clone_name is not None and clone_name not in names
                names.append(clone_name)
                engine = create_engine(clone_url)
                try:
                    with engine.begin() as connection:
                        versions.append(
                            connection.scalar(text("SELECT version_num FROM alembic_version"))
                        )
                        assert (
                            connection.scalar(
                                text("SELECT count(*) FROM task_image_publication_jobs")
                            )
                            == 0
                        )
                        assert (
                            connection.scalar(text("SELECT to_regclass('public.clone_sentinel')"))
                            is None
                        )
                        connection.exec_driver_sql("CREATE TABLE clone_sentinel (value integer)")
                        connection.exec_driver_sql("INSERT INTO clone_sentinel VALUES (1)")
                finally:
                    engine.dispose()
            expected_head = ScriptDirectory(
                str(Path(__file__).resolve().parents[2] / "database" / "migrations")
            ).get_current_head()
            assert versions == [expected_head, expected_head]
            assert upgrades == 1
        with admin.connect() as connection:
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM pg_database WHERE datname = ANY(:names)"),
                    {"names": names},
                )
                == 0
            )
    finally:
        admin.dispose()


def test_failed_migration_template_setup_removes_only_its_owned_database(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    names: list[str] = []

    def failed_upgrade(_args: Any, **kwargs: Any) -> None:
        name = make_url(kwargs["env"]["LOOM_DB_URL"]).database
        assert name is not None
        names.append(name)
        raise RuntimeError("synthetic template migration failure")

    monkeypatch.setattr(postgres_databases.subprocess, "run", failed_upgrade)

    def factory(url: str) -> Any:
        return postgres_databases.isolated_migration_database(
            url, template_name="template0", prepare_template=True
        )
    with closing(factory(postgres_url)) as fixture:
        with pytest.raises(RuntimeError, match="synthetic template migration failure"):
            next(fixture)
    assert len(names) == 1
    admin = create_engine(make_url(postgres_url).set(database="postgres"))
    try:
        with admin.connect() as connection:
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM pg_database WHERE datname = :name"),
                    {"name": names[0]},
                )
                == 0
            )
            assert (
                connection.scalar(
                    text("SELECT count(*) FROM pg_database WHERE datname = :name"),
                    {"name": make_url(postgres_url).database},
                )
                == 1
            )
    finally:
        admin.dispose()
