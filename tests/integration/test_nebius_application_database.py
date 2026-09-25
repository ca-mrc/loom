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
        with pytest.raises(ApplicationDatabaseAccessError, match="previous_access_active"):
            access.grant(app, incarnation, 2, token_urlsafe(48))
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


def test_concurrent_grant_replay_has_one_role_and_revocation_wins(database_access, monkeypatch):
    from loom import nebius_application_database as implementation
    original_failure = implementation._failure
    diagnostics = []
    def diagnose(exc):
        diagnostics.append((type(exc).__name__, exc.sqlstate, exc.diag.constraint_name))
        return original_failure(exc)
    monkeypatch.setattr(implementation, "_failure", diagnose)
    _, url, access, data_id = database_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    def grant():
        with psycopg.connect(url, autocommit=True) as connection:
            return ApplicationDatabaseAccess(connection, data_id).grant(app, incarnation, 1, password)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(grant) for _ in range(2)]
        for result in results:
            if result.exception() is not None:
                pytest.fail(f"concurrent grant failed: {diagnostics}", pytrace=False)
        roles = [result.result() for result in results]
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


@pytest.mark.parametrize("drift", ["runtime_privilege", "runtime_replacement", "manager_replacement",
                                  "runtime_owner", "runtime_schema", "runtime_ddl", "runtime_migration"])
def test_provider_rejects_replaced_or_privileged_shared_identities(database_access, drift):
    admin, url, access, data_id = database_access
    runtime = admin.execute("SELECT runtime_role FROM loom_application_access.binding").fetchone()[0]
    if drift == "runtime_privilege":
        admin.execute(sql.SQL("ALTER ROLE {} CREATEDB").format(sql.Identifier(runtime)))
    elif drift == "runtime_replacement":
        admin.execute(sql.SQL("DROP OWNED BY {}; DROP ROLE {}; CREATE ROLE {} NOLOGIN NOINHERIT").format(
            *[sql.Identifier(runtime)] * 3))
    elif drift == "manager_replacement":
        manager = make_url(url).username
        admin.execute(sql.SQL("DROP OWNED BY {}; DROP ROLE {}; CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
            *[sql.Identifier(manager)] * 3, sql.Literal(make_url(url).password)))
        admin.execute(sql.SQL("GRANT USAGE ON SCHEMA loom_application_access TO {}; GRANT EXECUTE ON FUNCTION loom_application_access.grant_access(uuid,uuid,uuid,bigint,text) TO {}").format(
            *[sql.Identifier(manager)] * 2))
        with psycopg.connect(url, autocommit=True) as replacement:
            with pytest.raises(ApplicationDatabaseAccessError, match="identity"):
                ApplicationDatabaseAccess(replacement, data_id).grant(uuid4(), uuid4(), 1, token_urlsafe(48))
        return
    else:
        statement = {
            "runtime_owner": "ALTER TABLE public.shared_records OWNER TO {}",
            "runtime_schema": "GRANT CREATE ON SCHEMA public TO {}",
            "runtime_ddl": "GRANT TRUNCATE ON public.shared_records TO {}",
            "runtime_migration": "GRANT UPDATE ON public.alembic_version TO {}",
        }[drift]
        admin.execute(sql.SQL(statement).format(sql.Identifier(runtime)))
    with pytest.raises(ApplicationDatabaseAccessError, match="identity"):
        access.grant(uuid4(), uuid4(), 1, token_urlsafe(48))


def test_retirement_does_not_adopt_replaced_generation_role(database_access):
    admin, _, access, _ = database_access
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password)
    admin.execute(sql.SQL("DROP OWNED BY {}; DROP ROLE {}; CREATE ROLE {} LOGIN").format(
        *[sql.Identifier(role)] * 3))
    with pytest.raises(ApplicationDatabaseAccessError, match="role_identity"):
        access.revoke(app, incarnation, 1)
    assert admin.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (role,)).fetchone() == (True,)
    assert admin.execute("SELECT retired_through FROM loom_application_access.applications WHERE application_id=%s", (app,)).fetchone() == (0,)


@pytest.mark.parametrize("permission", ["table", "column", "schema", "runtime_owner"])
def test_retirement_never_proves_a_login_with_residual_data_privileges_is_safe(database_access, permission):
    admin, _, access, _ = database_access
    app, incarnation = uuid4(), uuid4()
    role = access.grant(app, incarnation, 1, token_urlsafe(48))
    target = role
    if permission == "runtime_owner":
        target = admin.execute("SELECT runtime_role FROM loom_application_access.binding").fetchone()[0]
    statement = {
        "table": "GRANT SELECT ON public.shared_records TO {}",
        "column": "GRANT SELECT (value) ON public.shared_records TO {}",
        "schema": "GRANT CREATE ON SCHEMA public TO {}",
        "runtime_owner": "ALTER TABLE public.shared_records OWNER TO {}",
    }[permission]
    admin.execute(sql.SQL(statement).format(sql.Identifier(target)))
    with pytest.raises(ApplicationDatabaseAccessError, match="role_identity"):
        access.revoke(app, incarnation, 1)


def test_actual_application_schema_supports_shared_reads_without_migration_authority(isolated_migration_postgres_url):
    admin_url = make_url(isolated_migration_postgres_url).set(drivername="postgresql")
    manager, manager_password, data_id = "mgr_" + uuid4().hex, token_urlsafe(48), uuid4()
    manager_url = admin_url.set(username=manager, password=manager_password).render_as_string(hide_password=False)
    with psycopg.connect(admin_url.render_as_string(hide_password=False), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
            sql.Identifier(manager), sql.Literal(manager_password)))
        install_application_database_access(admin, data_environment_id=data_id, manager_role=manager)
        with psycopg.connect(manager_url, autocommit=True) as manager_connection:
            access = ApplicationDatabaseAccess(manager_connection, data_id)
            password, app, incarnation = token_urlsafe(48), uuid4(), uuid4()
            role = access.grant(app, incarnation, 1, password)
            with login(manager_url, role, password) as client:
                assert client.execute("SELECT version_num FROM public.alembic_version").fetchone() == ("0162",)
                for table in ("teams", "users", "tasks", "trials", "tokens", "secrets"):
                    assert client.execute(sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(table))).fetchone() is not None
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    client.execute("UPDATE public.alembic_version SET version_num='forbidden'")
            access.revoke(app, incarnation, 1)
            assert access.drain(app, incarnation, 1)


def test_raw_manager_calls_cannot_use_stale_snapshots_or_leak_secrets(database_access):
    _, url, _, data_id = database_access
    app, incarnation, secret = uuid4(), uuid4(), token_urlsafe(48)
    with psycopg.connect(url, autocommit=True) as connection:
        access = ApplicationDatabaseAccess(connection, data_id)
        connection.execute("SET default_transaction_isolation='repeatable read'")
        with pytest.raises(ApplicationDatabaseAccessError, match="isolation") as error:
            access.grant(app, incarnation, 1, secret)
        assert secret not in str(error.value)
        connection.execute("SET default_transaction_isolation='read committed'")
        access.grant(app, incarnation, 1, secret)
        with pytest.raises(ApplicationDatabaseAccessError, match="credential") as error:
            access.grant(app, incarnation, 1, token_urlsafe(48))
        assert secret not in str(error.value)
