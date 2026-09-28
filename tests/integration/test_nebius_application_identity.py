"""Shared identity enrollment uses real migrated tables and ordinary manager SQL."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from secrets import token_urlsafe
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.nebius_application_database import (
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
    install_application_database_access,
)
from tests.integration.test_nebius_application_database import login
from tests.integration.test_nebius_application_schema import migration_access as migration_access


def principal(**changes):
    from loom.nebius_application_identity import ApplicationPrincipalV1

    values = dict(user_id=uuid4(), team_id=uuid4(), username="Alice", username_normalized="alice",
                  display_name="Alice Example", team_name="Example team", role="member")
    return ApplicationPrincipalV1(**(values | changes))


def enroll(access, app, incarnation, owner, *, generation=1, schema="0166"):
    from loom.nebius_application_identity import ApplicationDatabaseIdentity

    return ApplicationDatabaseIdentity(access).enroll(
        app, incarnation, generation, schema_revision=schema, principal=owner)


def granted(access):
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    role = access.grant(app, incarnation, 1, password, schema_revision="0166")
    return app, incarnation, role, password


@pytest.mark.parametrize("role", ["owner", "member", "viewer"])
def test_initial_enrollment_preserves_actual_role_without_secrets_or_admin(migration_access, role):
    admin, _, access, _ = migration_access
    owner = principal(role=role)
    app, incarnation, _, _ = granted(access)
    assert enroll(access, app, incarnation, owner) == role
    assert admin.execute("SELECT username,username_normalized,display_name,status,password_hash,email,is_platform_admin FROM public.users WHERE id=%s", (owner.user_id,)).fetchone() == (
        "Alice", "alice", "Alice Example", "active", None, None, False)
    assert admin.execute("SELECT name,disabled_at,public_registration_enabled FROM public.teams WHERE id=%s", (owner.team_id,)).fetchone() == ("Example team", None, False)
    assert admin.execute("SELECT role FROM public.team_memberships WHERE team_id=%s AND user_id=%s", (owner.team_id, owner.user_id)).fetchone() == (role,)


def test_replay_preserves_shared_profiles_password_and_authorization(migration_access):
    admin, _, access, _ = migration_access
    owner = principal(role="owner")
    app, incarnation, _, _ = granted(access)
    assert enroll(access, app, incarnation, owner) == "owner"
    admin.execute("UPDATE public.users SET username='renamed',username_normalized='renamed',display_name='Updated',password_hash='retained-hash' WHERE id=%s", (owner.user_id,))
    admin.execute("UPDATE public.teams SET name='Updated team',submissions_paused_at=now() WHERE id=%s", (owner.team_id,))
    admin.execute("UPDATE public.team_memberships SET role='viewer' WHERE team_id=%s AND user_id=%s", (owner.team_id, owner.user_id))
    assert enroll(access, app, incarnation, owner) == "viewer"
    sibling, sibling_incarnation, _, _ = granted(access)
    assert enroll(access, sibling, sibling_incarnation, owner) == "viewer"
    assert admin.execute("SELECT username,display_name,password_hash FROM public.users WHERE id=%s", (owner.user_id,)).fetchone() == ("renamed", "Updated", "retained-hash")
    assert admin.execute("SELECT name,submissions_paused_at IS NOT NULL FROM public.teams WHERE id=%s", (owner.team_id,)).fetchone() == ("Updated team", True)


@pytest.mark.parametrize("removed", ["user", "team", "membership"])
def test_removed_shared_identity_cannot_be_recreated_by_another_application(migration_access, removed):
    admin, _, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, owner)
    if removed == "user":
        admin.execute("DELETE FROM public.users WHERE id=%s", (owner.user_id,))
        # A new team must not bypass retained user history.
        owner = principal(user_id=owner.user_id, team_name="Another team")
    elif removed == "team":
        admin.execute("DELETE FROM public.teams WHERE id=%s", (owner.team_id,))
        owner = principal(team_id=owner.team_id, username="Bob", username_normalized="bob")
    else:
        admin.execute("DELETE FROM public.team_memberships WHERE team_id=%s AND user_id=%s", (owner.team_id, owner.user_id))
    sibling, sibling_incarnation, _, _ = granted(access)
    before = admin.execute("SELECT (SELECT count(*) FROM public.users),(SELECT count(*) FROM public.teams)").fetchone()
    with pytest.raises(ApplicationDatabaseAccessError, match="principal"):
        enroll(access, sibling, sibling_incarnation, owner)
    assert admin.execute("SELECT (SELECT count(*) FROM public.users),(SELECT count(*) FROM public.teams)").fetchone() == before
    assert admin.execute("SELECT count(*) FROM loom_application_access.principal_enrollments").fetchone() == (1,)


@pytest.mark.parametrize("change", ["disabled_user", "disabled_team", "privileged_user", "pending_user"])
def test_enrollment_never_repairs_disabled_or_privileged_shared_identity(migration_access, change):
    admin, _, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, owner)
    statements = {
        "disabled_user": "UPDATE public.users SET disabled_at=now() WHERE id=%s",
        "disabled_team": "UPDATE public.teams SET disabled_at=now() WHERE id=%s",
        "privileged_user": "UPDATE public.users SET is_platform_admin=true WHERE id=%s",
        "pending_user": "UPDATE public.users SET status='pending_setup' WHERE id=%s",
    }
    admin.execute(statements[change], (owner.team_id if change == "disabled_team" else owner.user_id,))
    before = admin.execute("SELECT row_to_json(u),row_to_json(t) FROM public.users u,public.teams t WHERE u.id=%s AND t.id=%s", (owner.user_id, owner.team_id)).fetchone()
    with pytest.raises(ApplicationDatabaseAccessError, match="principal"):
        enroll(access, app, incarnation, owner)
    assert admin.execute("SELECT row_to_json(u),row_to_json(t) FROM public.users u,public.teams t WHERE u.id=%s AND t.id=%s", (owner.user_id, owner.team_id)).fetchone() == before


@pytest.mark.parametrize("collision", ["username", "team"])
def test_name_collision_cannot_adopt_another_uuid_or_leave_partial_identity(migration_access, collision):
    admin, _, access, _ = migration_access
    owner = principal()
    first, first_incarnation, _, _ = granted(access)
    enroll(access, first, first_incarnation, owner)
    other = principal(team_name="Another team") if collision == "username" else principal(username="Bob", username_normalized="bob")
    app, incarnation, _, _ = granted(access)
    with pytest.raises(ApplicationDatabaseAccessError):
        enroll(access, app, incarnation, other)
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (other.user_id,)).fetchone() == (0,)
    assert admin.execute("SELECT count(*) FROM public.teams WHERE id=%s", (other.team_id,)).fetchone() == (0,)
    assert admin.execute("SELECT count(*) FROM loom_application_access.principal_enrollments").fetchone() == (1,)


def test_missing_membership_between_existing_identities_is_not_assumed_new(migration_access):
    admin, _, access, _ = migration_access
    owner = principal()
    admin.execute("INSERT INTO public.users(id,username,username_normalized,status) VALUES (%s,%s,%s,'active')", (owner.user_id, owner.username, owner.username_normalized))
    admin.execute("INSERT INTO public.teams(id,name) VALUES (%s,%s)", (owner.team_id, owner.team_name))
    app, incarnation, _, _ = granted(access)
    with pytest.raises(ApplicationDatabaseAccessError, match="principal"):
        enroll(access, app, incarnation, owner)
    assert admin.execute("SELECT count(*) FROM public.team_memberships WHERE user_id=%s", (owner.user_id,)).fetchone() == (0,)


@pytest.mark.parametrize("invalid", ["no_grant", "generation", "incarnation", "data", "retired", "schema"])
def test_enrollment_requires_exact_live_grant_and_schema(migration_access, invalid):
    admin, _, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    generation, schema = 1, "0166"
    if invalid == "no_grant":
        app, incarnation = uuid4(), uuid4()
    elif invalid == "generation":
        generation = 2
    elif invalid == "incarnation":
        incarnation = uuid4()
    elif invalid == "data":
        access = ApplicationDatabaseAccess(access.connection, uuid4())
    elif invalid == "retired":
        access.revoke(app, incarnation, 1)
    else:
        schema = "0165"
    with pytest.raises(ApplicationDatabaseAccessError):
        enroll(access, app, incarnation, owner, generation=generation, schema=schema)
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (owner.user_id,)).fetchone() == (0,)


def test_application_owner_mapping_is_immutable_across_generation(migration_access):
    admin, _, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, owner)
    access.revoke(app, incarnation, 1)
    assert access.drain(app, incarnation, 1)
    access.grant(app, incarnation, 2, token_urlsafe(48), schema_revision="0166")
    other = principal(username="Bob", username_normalized="bob", team_name="Other team")
    with pytest.raises(ApplicationDatabaseAccessError, match="principal"):
        enroll(access, app, incarnation, other, generation=2)
    assert enroll(access, app, incarnation, owner, generation=2) == "member"
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (other.user_id,)).fetchone() == (0,)


def test_parallel_applications_share_one_identity_without_escalation(migration_access):
    admin, url, access, _ = migration_access
    owner = principal(role="viewer")
    apps = [granted(access)[:2] for _ in range(4)]

    def start(pair):
        with psycopg.connect(url, autocommit=True) as connection:
            return enroll(ApplicationDatabaseAccess(connection, access.data_environment_id), *pair, owner)

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert list(workers.map(start, apps)) == ["viewer"] * 4
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (owner.user_id,)).fetchone() == (1,)
    assert admin.execute("SELECT count(*) FROM loom_application_access.principal_enrollments").fetchone() == (4,)


def test_ordinary_api_cannot_enroll_or_modify_private_provenance(migration_access):
    _, url, access, _ = migration_access
    owner = principal()
    app, incarnation, role, password = granted(access)
    enroll(access, app, incarnation, owner)
    with login(url, role, password) as api:
        with pytest.raises(ApplicationDatabaseAccessError):
            enroll(ApplicationDatabaseAccess(api, access.data_environment_id), app, incarnation, owner)
        for statement in ("SELECT * FROM loom_application_access.principal_enrollments",
                          "DELETE FROM loom_application_access.principal_identities"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                api.execute(statement)


@pytest.mark.parametrize("drift", ["routine", "table", "partial", "grant"])
def test_installer_refuses_identity_drift_without_overwriting_it(migration_access, drift):
    admin, url, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, owner)
    manager = make_url(url).username
    if drift == "routine":
        definition = admin.execute("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE pronamespace='loom_application_access'::regnamespace AND proname='enroll_principal'").fetchone()[0]
        admin.execute(definition.replace("BEGIN", "BEGIN RAISE EXCEPTION 'changed';", 1))
    elif drift == "table":
        admin.execute("ALTER TABLE loom_application_access.principal_enrollments ADD COLUMN unexpected text")
    elif drift == "partial":
        admin.execute("DROP TABLE loom_application_access.principal_identities")
    else:
        admin.execute(sql.SQL("GRANT SELECT ON loom_application_access.principal_enrollments TO {}").format(sql.Identifier(manager)))
    before = admin.execute("SELECT to_regclass('loom_application_access.principal_identities'),(SELECT count(*) FROM loom_application_access.principal_enrollments)").fetchone()
    with pytest.raises(ApplicationDatabaseAccessError, match=r"drift|private_authority"):
        install_application_database_access(admin, data_environment_id=access.data_environment_id, manager_role=manager)
    assert admin.execute("SELECT to_regclass('loom_application_access.principal_identities'),(SELECT count(*) FROM loom_application_access.principal_enrollments)").fetchone() == before


def test_installer_replay_preserves_enrollment_and_bounded_execute(migration_access):
    admin, url, access, _ = migration_access
    owner = principal()
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, owner)
    install_application_database_access(admin, data_environment_id=access.data_environment_id, manager_role=make_url(url).username)
    assert enroll(access, app, incarnation, owner) == "member"
    assert admin.execute("SELECT count(*) FROM loom_application_access.principal_enrollments").fetchone() == (1,)


@pytest.mark.parametrize("change", [dict(username="Bad name"), dict(username_normalized="ALICE"),
                                    dict(role="admin"), dict(team_name=" "), dict(user_id="not-a-uuid"),
                                    dict(password_hash="must-not-be-copied"), dict(is_platform_admin=True)])
def test_principal_rejects_malformed_or_privileged_input(change):
    with pytest.raises(ValueError):
        principal(**change)


def test_raw_grant_and_enrollment_share_transaction_and_rollback(migration_access):
    admin, _, access, _ = migration_access
    owner = principal()
    app, incarnation, password = uuid4(), uuid4(), token_urlsafe(48)
    with pytest.raises(RuntimeError, match="rollback"):
        with access.connection.transaction():
            access.connection.execute("SELECT loom_application_access.grant_access_at_schema(%s,%s,%s,1,%s,'0166')",
                                      (access.data_environment_id, app, incarnation, password))
            assert access.connection.execute("SELECT loom_application_access.enroll_principal(%s,%s,%s,1,'0166',%s,%s,%s,%s,%s,%s,%s)",
                                             (access.data_environment_id, app, incarnation, owner.user_id, owner.team_id,
                                              owner.username, owner.username_normalized, owner.display_name, owner.team_name, owner.role)).fetchone() == ("member",)
            raise RuntimeError("rollback")
    assert admin.execute("SELECT count(*) FROM public.users WHERE id=%s", (owner.user_id,)).fetchone() == (0,)
    assert admin.execute("SELECT count(*) FROM loom_application_access.applications WHERE application_id=%s", (app,)).fetchone() == (0,)


@pytest.mark.parametrize("object_kind", ["TABLES", "FUNCTIONS"])
def test_installer_cannot_inherit_unrelated_default_privileges(migration_access, object_kind):
    admin, url, access, _ = migration_access
    # Simulate adding this extension to a prior installation, under a role with
    # unsafe defaults. Its private provenance and enrollment must stay private.
    admin.execute("DROP FUNCTION loom_application_access.enroll_principal(uuid,uuid,uuid,bigint,text,uuid,uuid,text,text,text,text,text)")
    admin.execute("DROP TABLE loom_application_access.principal_enrollments,loom_application_access.principal_identities")
    unrelated = "unrelated_" + uuid4().hex
    admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(unrelated)))
    privilege = "SELECT" if object_kind == "TABLES" else "EXECUTE"
    admin.execute(sql.SQL("ALTER DEFAULT PRIVILEGES GRANT {} ON {} TO {}").format(
        sql.SQL(privilege), sql.SQL(object_kind), sql.Identifier(unrelated)))
    with pytest.raises(ApplicationDatabaseAccessError, match="private_authority"):
        install_application_database_access(admin, data_environment_id=access.data_environment_id, manager_role=make_url(url).username)
    assert admin.execute("SELECT to_regclass('loom_application_access.principal_enrollments')").fetchone() == (None,)


@pytest.mark.parametrize("privilege", ["SELECT", "UPDATE"])
def test_installer_refuses_column_access_to_retained_identity_history(migration_access, privilege):
    admin, url, access, _ = migration_access
    app, incarnation, _, _ = granted(access)
    enroll(access, app, incarnation, principal())
    admin.execute(sql.SQL("GRANT {}(identity_id) ON loom_application_access.principal_identities TO {}").format(
        sql.SQL(privilege), sql.Identifier(make_url(url).username)))
    # These grants live in pg_attribute.attacl, not the table's relacl. They
    # allow the ordinary manager to read or corrupt the deletion tombstones.
    if privilege == "SELECT":
        assert len(access.connection.execute("SELECT identity_id FROM loom_application_access.principal_identities").fetchall()) == 2
    else:
        # Prove the writable capability but roll back the test's corruption.
        with pytest.raises(RuntimeError, match="rollback"):
            with access.connection.transaction():
                assert access.connection.execute("UPDATE loom_application_access.principal_identities SET identity_id=%s", (uuid4(),)).rowcount == 2
                raise RuntimeError("rollback")
    with pytest.raises(ApplicationDatabaseAccessError, match="private_authority"):
        install_application_database_access(admin, data_environment_id=access.data_environment_id, manager_role=make_url(url).username)


@pytest.mark.parametrize("direction", ["public_parent", "public_child"])
def test_installer_refuses_inheritance_across_private_provenance(migration_access, direction):
    admin, url, access, _ = migration_access
    if direction == "public_parent":
        admin.execute("CREATE TABLE public.identity_parent(kind text NOT NULL,identity_id uuid NOT NULL,first_application_id uuid NOT NULL)")
        admin.execute("ALTER TABLE loom_application_access.principal_identities INHERIT public.identity_parent")
    else:
        admin.execute("CREATE TABLE public.identity_child() INHERITS (loom_application_access.principal_identities)")
    with pytest.raises(ApplicationDatabaseAccessError, match="installation_drift"):
        install_application_database_access(admin, data_environment_id=access.data_environment_id, manager_role=make_url(url).username)
    # Rejected installation must not expand API authority to the public table.
    table = "identity_parent" if direction == "public_parent" else "identity_child"
    assert admin.execute("SELECT has_table_privilege(runtime_oid,%s,'SELECT,INSERT,UPDATE,DELETE') FROM loom_application_access.binding", ("public." + table,)).fetchone() == (False,)
