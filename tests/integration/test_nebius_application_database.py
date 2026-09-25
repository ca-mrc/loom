"""Real shared-data access, generation fencing and connection retirement."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from secrets import token_urlsafe
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.nebius_application_database import (
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
    install_application_database_access,
)


@pytest.fixture(scope="module")
def access_postgres():
    with PostgresContainer("postgres:16") as container:
        yield make_url(container.get_connection_url()).set(drivername="postgresql")


@pytest.fixture
def database_access(access_postgres):
    database, manager, data_id = "app_" + uuid4().hex, "mgr_" + uuid4().hex, uuid4()
    manager_password = token_urlsafe(48)
    with psycopg.connect(access_postgres.render_as_string(hide_password=False), autocommit=True) as root:
        root.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        root.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
            sql.Identifier(manager), sql.Literal(manager_password)))
    admin_url = access_postgres.set(database=database).render_as_string(hide_password=False)
    manager_url = access_postgres.set(database=database, username=manager,
                                     password=manager_password).render_as_string(hide_password=False)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute("CREATE TABLE public.alembic_version(version_num text PRIMARY KEY)")
        admin.execute("INSERT INTO public.alembic_version VALUES ('test_revision')")
        admin.execute("CREATE TABLE public.shared_records(id bigserial PRIMARY KEY, value text NOT NULL)")
        install_application_database_access(admin, data_environment_id=data_id, manager_role=manager)
        with psycopg.connect(manager_url, autocommit=True) as manager_connection:
            yield admin, manager_url, ApplicationDatabaseAccess(manager_connection, data_id), data_id


def login(manager_url, role, password):
    url = make_url(manager_url).set(username=role, password=password)
    return psycopg.connect(url.render_as_string(hide_password=False), autocommit=True, connect_timeout=2)


def test_two_apps_share_dml_without_schema_or_provisioning_authority(database_access):
    admin, url, access, _ = database_access
    passwords = [token_urlsafe(48), token_urlsafe(48)]
    roles = [access.grant(uuid4(), uuid4(), 1, password) for password in passwords]
    runtime = admin.execute("SELECT runtime_role FROM loom_application_access.binding").fetchone()[0]
    with login(url, roles[0], passwords[0]) as alice, login(url, roles[1], passwords[1]) as bob:
        alice.execute("INSERT INTO public.shared_records(value) VALUES ('alice')")
        assert bob.execute("SELECT value FROM public.shared_records").fetchall() == [("alice",)]
        bob.execute("UPDATE public.shared_records SET value='bob'")
        assert alice.execute("SELECT value FROM public.shared_records").fetchall() == [("bob",)]
        for statement in (
            "CREATE TABLE public.forbidden(id integer)",
            "TRUNCATE public.shared_records",
            "UPDATE public.alembic_version SET version_num='forbidden'",
            "SELECT * FROM loom_application_access.applications",
            "CREATE ROLE forbidden LOGIN",
            sql.SQL("SET ROLE {}").format(sql.Identifier(runtime)),
            sql.SQL("SET ROLE {}").format(sql.Identifier(roles[1])),
            "SELECT loom_application_access.revoke_access(NULL,NULL,NULL,1)",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                alice.execute(statement)
    with psycopg.connect(url, autocommit=True) as manager:
        for statement in ("CREATE ROLE bypass", "SELECT * FROM public.shared_records",
                          "DELETE FROM loom_application_access.applications"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                manager.execute(statement)


def test_retirement_fences_delayed_grants_and_retires_only_own_connections(database_access):
    admin, url, access, _ = database_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password)
    other_password = token_urlsafe(48)
    other_role = access.grant(uuid4(), uuid4(), 1, other_password)
    with login(url, role, password) as old, login(url, other_role, other_password) as other:
        old.execute("INSERT INTO public.shared_records(value) VALUES ('retained')")
        with pytest.raises(ApplicationDatabaseAccessError, match="not_retired"):
            access.drain(app, incarnation, 1)
        with pytest.raises(ApplicationDatabaseAccessError, match="previous_access_active"):
            access.grant(app, incarnation, 2, token_urlsafe(48))
        access.revoke(app, incarnation, 1)
        access.revoke(app, incarnation, 1)  # Lost acknowledgement is safe to replay.
        with pytest.raises(ApplicationDatabaseAccessError, match="retired"):
            access.grant(app, incarnation, 1, password)
        with pytest.raises(psycopg.OperationalError):
            login(url, role, password)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            old.execute("INSERT INTO public.shared_records(value) VALUES ('late')")
        assert access.drain(app, incarnation, 1)
        with pytest.raises(psycopg.OperationalError):
            old.execute("SELECT 1")
        assert other.execute("SELECT value FROM public.shared_records").fetchall() == [("retained",)]
    new_password = token_urlsafe(48)
    new_role = access.grant(app, incarnation, 2, new_password)
    assert new_role != role
    with login(url, new_role, new_password) as successor:
        assert successor.execute("SELECT value FROM public.shared_records").fetchall() == [("retained",)]
    assert admin.execute("SELECT version_num FROM public.alembic_version").fetchone() == ("test_revision",)


def test_revoke_before_grant_and_identity_mismatch_fail_closed(database_access):
    _, _, access, data_id = database_access
    app, incarnation = uuid4(), uuid4()
    access.revoke(app, incarnation, 3)
    assert access.drain(app, incarnation, 3)
    for generation in (1, 2, 3):
        with pytest.raises(ApplicationDatabaseAccessError, match="retired"):
            access.grant(app, incarnation, generation, token_urlsafe(48))
    with pytest.raises(ApplicationDatabaseAccessError, match="incarnation"):
        access.grant(app, uuid4(), 4, token_urlsafe(48))
    with pytest.raises(ApplicationDatabaseAccessError, match="identity"):
        ApplicationDatabaseAccess(access.connection, uuid4()).grant(uuid4(), uuid4(), 1, token_urlsafe(48))
    for generation in (0, -1, True, 2**63):
        with pytest.raises(ValueError):
            access.grant(app, incarnation, generation, token_urlsafe(48))
    with pytest.raises(ValueError):
        access.grant(UUID(int=0), incarnation, 4, token_urlsafe(48))
    assert data_id.int != 0


def test_grant_replay_does_not_rotate_unknown_or_changed_credentials(database_access):
    _, url, access, _ = database_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password)
    assert access.grant(app, incarnation, 1, password) == role
    with pytest.raises(ApplicationDatabaseAccessError, match="credential"):
        access.grant(app, incarnation, 1, token_urlsafe(48))
    changed = token_urlsafe(48)
    with login(url, role, password) as client:
        client.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(role), sql.Literal(changed)))
    with pytest.raises(ApplicationDatabaseAccessError, match="credential"):
        access.grant(app, incarnation, 1, password)
    access.revoke(app, incarnation, 1)  # Credential drift must not prevent revocation.
    assert access.drain(app, incarnation, 1)


def test_concurrent_grant_replay_has_one_role_and_revocation_wins(database_access):
    _, url, access, data_id = database_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    def grant():
        with psycopg.connect(url, autocommit=True) as connection:
            return ApplicationDatabaseAccess(connection, data_id).grant(app, incarnation, 1, password)
    with ThreadPoolExecutor(max_workers=2) as pool:
        roles = list(pool.map(lambda _: grant(), range(2)))
    assert roles[0] == roles[1]
    def revoke():
        with psycopg.connect(url, autocommit=True) as connection:
            ApplicationDatabaseAccess(connection, data_id).revoke(app, incarnation, 1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        granting, revoking = pool.submit(grant), pool.submit(revoke)
        revoking.result()
        try:
            assert granting.result() == roles[0]
        except ApplicationDatabaseAccessError as exc:
            assert "retired" in str(exc)
    with pytest.raises(ApplicationDatabaseAccessError, match="retired"):
        grant()
    assert access.drain(app, incarnation, 1)


def test_access_calls_require_commit_boundary_and_public_dml_cannot_defeat_revocation(database_access):
    admin, url, access, data_id = database_access
    with psycopg.connect(url) as transaction:
        with pytest.raises(ValueError, match="autocommit"):
            ApplicationDatabaseAccess(transaction, data_id).revoke(uuid4(), uuid4(), 1)
    admin.execute("GRANT INSERT ON public.shared_records TO PUBLIC")
    with pytest.raises(ApplicationDatabaseAccessError, match="public_privileges"):
        access.grant(uuid4(), uuid4(), 1, token_urlsafe(48))


def test_install_replay_preserves_credentials_and_rejects_wrong_binding(database_access):
    admin, url, access, data_id = database_access
    manager = make_url(url).username
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password)
    install_application_database_access(admin, data_environment_id=data_id, manager_role=manager)
    assert access.grant(app, incarnation, 1, password) == role
    with pytest.raises(ApplicationDatabaseAccessError, match="binding"):
        install_application_database_access(admin, data_environment_id=uuid4(), manager_role=manager)
