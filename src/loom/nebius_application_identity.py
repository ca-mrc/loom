"""Bounded shared identity enrollment; never copy credentials or admin authority.

Retained private provenance is independent of public user/team deletion. Personal
application retirement must not delete these shared identities or their history.
"""
from __future__ import annotations

from typing import Any, Literal, Self, cast
from uuid import UUID

import psycopg
from psycopg import sql
from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_application_database import (
    _REVISION,
    ApplicationDatabaseAccess,
    ApplicationDatabaseAccessError,
    _failure,
    _idle,
)
from loom.nebius_application_schema import APPLICATION_SCHEMA_LOCK

MembershipRole = Literal["owner", "member", "viewer"]
_ENROLLMENT_LOCK = 0x4C4F4F4D494445


class ApplicationPrincipalV1(BaseModel):
    """Current management identity, not an authentication token or public input."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    user_id: UUID
    team_id: UUID
    username: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$")
    username_normalized: str
    display_name: str | None = Field(default=None, max_length=1024)
    team_name: str = Field(min_length=1, max_length=1024)
    role: MembershipRole

    @model_validator(mode="after")
    def _canonical(self) -> Self:
        if (not self.user_id.int or not self.team_id.int
                or self.username_normalized != self.username.casefold()
                or not self.team_name.strip()
                or any("\0" in value for value in (self.team_name, self.display_name or ""))):
            raise ValueError("invalid application principal")
        return self


_ARGUMENTS = "p_data uuid,p_app uuid,p_incarnation uuid,p_generation bigint,p_schema text,p_user uuid,p_team uuid,p_username text,p_normalized text,p_display text,p_team_name text,p_role text"
_TYPES = "uuid,uuid,uuid,bigint,text,uuid,uuid,text,text,text,text,text"
_ENROLL = f"""
DECLARE
    v_user record; v_team record; v_access record; v_mapping record; v_binding record;
    v_new_user boolean := false; v_new_team boolean := false; v_role text;
BEGIN
    IF pg_catalog.current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'application_database_isolation';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared({APPLICATION_SCHEMA_LOCK});
    IF p_schema IS NULL OR p_schema !~ '^[a-zA-Z0-9_]{{1,64}}$'
       OR (SELECT count(*) FROM public.alembic_version)<>1
       OR NOT EXISTS (SELECT 1 FROM public.alembic_version WHERE version_num=p_schema) THEN
        RAISE EXCEPTION 'application_database_schema_mismatch';
    END IF;
    PERFORM loom_application_access.lock_application(p_data,p_app,p_incarnation,p_generation);
    SELECT * INTO STRICT v_binding FROM loom_application_access.binding;
    SELECT * INTO v_access FROM loom_application_access.generations
        WHERE application_id=p_app AND generation=p_generation;
    IF NOT FOUND OR v_access.retired
       OR (SELECT retired_through FROM loom_application_access.applications WHERE application_id=p_app)>=p_generation THEN
        RAISE EXCEPTION 'application_database_principal_access';
    END IF;
    PERFORM loom_application_access.require_role(v_access.role_name,v_access.role_oid,v_binding.runtime_oid);
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_authid WHERE oid=v_access.role_oid AND rolcanlogin
                      AND pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(rolpassword,'UTF8')),'hex')=v_access.verifier_sha256)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=v_binding.runtime_oid AND member=v_access.role_oid) THEN
        RAISE EXCEPTION 'application_database_principal_access';
    END IF;
    IF p_user IS NULL OR p_team IS NULL
       OR p_user='00000000-0000-0000-0000-000000000000'::uuid
       OR p_team='00000000-0000-0000-0000-000000000000'::uuid
       OR p_username IS NULL OR p_username COLLATE "C" !~ '^[A-Za-z0-9][A-Za-z0-9_.-]{{1,63}}$'
       OR p_normalized IS DISTINCT FROM pg_catalog.lower(p_username COLLATE "C")
       OR p_team_name IS NULL OR pg_catalog.length(pg_catalog.btrim(p_team_name))=0
       OR pg_catalog.length(p_team_name)>1024 OR pg_catalog.length(p_display)>1024
       OR p_role IS NULL OR p_role NOT IN ('owner','member','viewer') THEN
        RAISE EXCEPTION 'application_database_principal_invalid';
    END IF;
    -- One short local transaction, not a scheduling/capacity authority. Follow
    -- the platform-admin User -> Team -> membership row-lock ordering.
    PERFORM pg_catalog.pg_advisory_xact_lock({_ENROLLMENT_LOCK});
    SELECT * INTO v_mapping FROM loom_application_access.principal_enrollments WHERE application_id=p_app;
    IF FOUND AND (v_mapping.user_id<>p_user OR v_mapping.team_id<>p_team) THEN
        RAISE EXCEPTION 'application_database_principal_conflict';
    END IF;
    SELECT * INTO v_user FROM public.users WHERE id=p_user FOR UPDATE;
    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM loom_application_access.principal_identities WHERE kind='user' AND identity_id=p_user) THEN
            RAISE EXCEPTION 'application_database_principal_removed';
        END IF;
        INSERT INTO public.users(id,username,username_normalized,display_name,status,is_platform_admin)
            VALUES(p_user,p_username,p_normalized,p_display,'active',false) RETURNING * INTO v_user;
        v_new_user := true;
    END IF;
    IF v_user.status<>'active' OR v_user.disabled_at IS NOT NULL OR v_user.is_platform_admin THEN
        RAISE EXCEPTION 'application_database_principal_disabled';
    END IF;
    SELECT * INTO v_team FROM public.teams WHERE id=p_team FOR UPDATE;
    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM loom_application_access.principal_identities WHERE kind='team' AND identity_id=p_team) THEN
            RAISE EXCEPTION 'application_database_principal_removed';
        END IF;
        INSERT INTO public.teams(id,name) VALUES(p_team,p_team_name) RETURNING * INTO v_team;
        v_new_team := true;
    END IF;
    IF v_team.disabled_at IS NOT NULL THEN
        RAISE EXCEPTION 'application_database_principal_disabled';
    END IF;
    SELECT role INTO v_role FROM public.team_memberships WHERE user_id=p_user AND team_id=p_team FOR UPDATE;
    IF NOT FOUND THEN
        IF NOT (v_new_user OR v_new_team) THEN
            RAISE EXCEPTION 'application_database_principal_membership_missing';
        END IF;
        INSERT INTO public.team_memberships(user_id,team_id,role) VALUES(p_user,p_team,p_role);
        v_role := p_role;
    END IF;
    IF v_role NOT IN ('owner','member','viewer') THEN
        RAISE EXCEPTION 'application_database_principal_invalid';
    END IF;
    INSERT INTO loom_application_access.principal_identities(kind,identity_id,first_application_id)
        VALUES('user',p_user,p_app),('team',p_team,p_app) ON CONFLICT DO NOTHING;
    INSERT INTO loom_application_access.principal_enrollments(application_id,user_id,team_id)
        VALUES(p_app,p_user,p_team) ON CONFLICT DO NOTHING;
    RETURN v_role;
END
"""

_TABLES = {
    "principal_identities": (
        [("kind", "text"), ("identity_id", "uuid"), ("first_application_id", "uuid")],
        ["FOREIGN KEY (first_application_id) REFERENCES loom_application_access.applications(application_id)",
         "PRIMARY KEY (kind, identity_id)"],
    ),
    "principal_enrollments": (
        [("application_id", "uuid"), ("user_id", "uuid"), ("team_id", "uuid")],
        ["FOREIGN KEY (application_id) REFERENCES loom_application_access.applications(application_id)",
         "PRIMARY KEY (application_id)"],
    ),
}


def _install_identity(connection: psycopg.Connection[Any], manager_role: str) -> None:
    """Called only inside the protected installer's exclusive-lock transaction."""
    present = connection.execute("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace='loom_application_access'::regnamespace AND relname IN ('principal_identities','principal_enrollments') ORDER BY relname").fetchall()
    observed = connection.execute("""SELECT p.prosrc,p.prosecdef,p.proconfig,p.prorettype::regtype::text,
        pg_catalog.oidvectortypes(p.proargtypes),p.proargnames,p.pronargdefaults,p.proowner=n.nspowner,
        l.lanname,p.provolatile,p.proisstrict,p.proretset,p.prokind
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_catalog.pg_language l ON l.oid=p.prolang
        WHERE n.nspname='loom_application_access' AND p.proname='enroll_principal'""").fetchall()
    expected = [(_ENROLL, True, ["search_path=pg_catalog, pg_temp"], "text", _TYPES.replace(",", ", "),
                 [part.split()[0] for part in _ARGUMENTS.split(",")], 0, True, "plpgsql", "v", False, False, "f")]
    if not present and not observed:
        connection.execute("""
            CREATE TABLE loom_application_access.principal_identities(
                kind text NOT NULL,identity_id uuid NOT NULL,
                first_application_id uuid NOT NULL REFERENCES loom_application_access.applications,
                PRIMARY KEY(kind,identity_id));
            CREATE TABLE loom_application_access.principal_enrollments(
                application_id uuid PRIMARY KEY REFERENCES loom_application_access.applications,
                user_id uuid NOT NULL,team_id uuid NOT NULL);
        """)
        connection.execute(sql.SQL("CREATE FUNCTION loom_application_access.enroll_principal({}) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS {}").format(sql.SQL(_ARGUMENTS), sql.Literal(_ENROLL)))
    elif present != [("principal_enrollments",), ("principal_identities",)] or observed != expected:
        raise ApplicationDatabaseAccessError("application_database_installation_drift")
    for name, (columns, constraints) in _TABLES.items():
        relation = "loom_application_access." + name
        actual = connection.execute("""SELECT a.attname,a.atttypid::regtype::text,a.attnotnull,
            a.atthasdef,a.attidentity,a.attgenerated,a.attisdropped
            FROM pg_catalog.pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 ORDER BY a.attnum""", (relation,)).fetchall()
        rules = connection.execute("SELECT pg_catalog.pg_get_constraintdef(oid),convalidated,condeferrable FROM pg_catalog.pg_constraint WHERE conrelid=%s::regclass ORDER BY 1", (relation,)).fetchall()
        safe = connection.execute("""SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity
            AND NOT c.relforcerowsecurity AND NOT c.relispartition AND NOT c.relhasrules
            AND c.relowner=n.nspowner AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
            FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE c.oid=%s::regclass""", (relation,)).fetchone()
        if (actual != [(column, kind, True, False, "", "", False) for column, kind in columns]
                or rules != [(rule, True, False) for rule in constraints] or safe != (True,)):
            raise ApplicationDatabaseAccessError("application_database_installation_drift")
    connection.execute(sql.SQL("REVOKE ALL ON FUNCTION loom_application_access.enroll_principal({}) FROM PUBLIC; GRANT EXECUTE ON FUNCTION loom_application_access.enroll_principal({}) TO {}").format(
        sql.SQL(_TYPES), sql.SQL(_TYPES), sql.Identifier(manager_role)))


class ApplicationDatabaseIdentity:
    def __init__(self, access: ApplicationDatabaseAccess):
        self.access = access

    def enroll(self, application_id: UUID, incarnation: UUID, generation: int, *,
               schema_revision: str, principal: ApplicationPrincipalV1) -> MembershipRole:
        access = self.access
        _idle(access.connection)
        if (any(not isinstance(value, UUID) or not value.int for value in (access.data_environment_id, application_id, incarnation))
                or type(generation) is not int or not 1 <= generation <= 2**63 - 1
                or not isinstance(schema_revision, str) or _REVISION.fullmatch(schema_revision) is None):
            raise ValueError("invalid application principal access identity")
        principal = ApplicationPrincipalV1.model_validate(principal.model_dump())
        try:
            result = access.connection.execute("SELECT loom_application_access.enroll_principal(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (access.data_environment_id, application_id, incarnation, generation, schema_revision,
                 principal.user_id, principal.team_id, principal.username, principal.username_normalized,
                 principal.display_name, principal.team_name, principal.role)).fetchone()
        except psycopg.Error as exc:
            raise _failure(exc) from None
        if result is None or result[0] not in {"owner", "member", "viewer"}:
            raise ApplicationDatabaseAccessError("application_database_result_invalid")
        return cast(MembershipRole, result[0])
