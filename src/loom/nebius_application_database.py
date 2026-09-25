"""Protected shared-development SQL access, separate from management journals.

Installation requires the shared database administrator. Only the dedicated
manager login may call the installed routines; personal APIs receive individual
ordinary logins. This module neither installs live resources nor releases Pods,
object credentials or application reservations.
"""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

_SCHEMA = "loom_application_access"
_PASSWORD = re.compile(r"[A-Za-z0-9_-]{48,128}")


class ApplicationDatabaseAccessError(RuntimeError):
    """Bounded provider failure, without SQL text or credential material."""


# All references are qualified; SECURITY DEFINER routines never resolve an
# ordinary caller's public/temp relation or function before pg_catalog.
_PUBLIC_SAFE = """
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n,
            LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))) a
        WHERE n.nspname='public' AND a.grantee=0 AND a.privilege_type='CREATE'
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace,
            LATERAL pg_catalog.aclexplode(c.relacl) a
        WHERE n.nspname='public' AND a.grantee=0
            AND a.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','USAGE')
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute t JOIN pg_catalog.pg_class c ON c.oid=t.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace,
            LATERAL pg_catalog.aclexplode(t.attacl) a
        WHERE n.nspname='public' AND a.grantee=0 AND a.privilege_type IN ('SELECT','INSERT','UPDATE')
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace,
            LATERAL pg_catalog.aclexplode(COALESCE(p.proacl, pg_catalog.acldefault('f',p.proowner))) a
        WHERE n.nspname='public' AND p.prosecdef AND a.grantee=0 AND a.privilege_type='EXECUTE'
            AND p.prorettype NOT IN ('pg_catalog.trigger'::pg_catalog.regtype,
                                    'pg_catalog.event_trigger'::pg_catalog.regtype)
    ) THEN
        RAISE EXCEPTION 'application_database_public_privileges';
    END IF;
"""

_LOCK = """
DECLARE v_binding loom_application_access.binding%ROWTYPE; v_incarnation uuid;
BEGIN
    SELECT * INTO STRICT v_binding FROM loom_application_access.binding;
    IF session_user<>v_binding.manager_role OR p_data IS DISTINCT FROM v_binding.data_environment_id
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE oid=v_binding.manager_oid
                      AND rolname=session_user AND NOT rolsuper AND NOT rolcreatedb
                      AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_authid WHERE oid=v_binding.runtime_oid
                      AND rolname=v_binding.runtime_role AND NOT rolcanlogin AND rolpassword IS NULL
                      AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole
                      AND NOT rolreplication AND NOT rolbypassrls AND NOT rolinherit)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members
                  WHERE member IN (v_binding.runtime_oid,v_binding.manager_oid))
       OR v_binding.database_oid<>(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
       OR v_binding.system_identifier<>(SELECT system_identifier::text FROM pg_catalog.pg_control_system())
       OR p_app IS NULL OR p_incarnation IS NULL OR p_generation IS NULL OR p_generation<1
       OR p_app='00000000-0000-0000-0000-000000000000'::uuid
       OR p_incarnation='00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION 'application_database_identity';
    END IF;
    IF pg_catalog.current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'application_database_isolation';
    END IF;
    INSERT INTO loom_application_access.applications(application_id,incarnation)
        VALUES(p_app,p_incarnation) ON CONFLICT DO NOTHING;
    SELECT incarnation INTO v_incarnation FROM loom_application_access.applications
        WHERE application_id=p_app FOR UPDATE;
    IF NOT FOUND OR v_incarnation<>p_incarnation THEN
        RAISE EXCEPTION 'application_database_incarnation';
    END IF;
END
"""

_ROLE_SAFE = """
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_authid WHERE oid=p_oid AND rolname=p_role
        AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication
        AND NOT rolbypassrls AND NOT rolinherit)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=p_oid
                  OR member=p_oid AND (roleid<>p_runtime OR admin_option OR set_option OR NOT inherit_option))
       OR pg_catalog.has_schema_privilege(p_oid,'public','CREATE')
       OR pg_catalog.has_database_privilege(p_oid,current_database(),'CREATE')
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                  WHERE n.nspname='public' AND c.relowner IN (p_oid,p_runtime))
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace,
                  LATERAL pg_catalog.aclexplode(c.relacl) a WHERE n.nspname='public' AND a.grantee=p_oid)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_attribute t JOIN pg_catalog.pg_class c ON c.oid=t.attrelid
                  JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace,
                  LATERAL pg_catalog.aclexplode(t.attacl) a WHERE n.nspname='public' AND a.grantee=p_oid) THEN
        RAISE EXCEPTION 'application_database_role_identity';
    END IF;
END
"""

_GRANT = """
DECLARE
    v_binding loom_application_access.binding%ROWTYPE;
    v_access loom_application_access.generations%ROWTYPE;
    v_role text; v_oid oid; v_hash text; v_verifier text; v_runtime oid;
BEGIN
    PERFORM loom_application_access.lock_application(p_data,p_app,p_incarnation,p_generation);
""" + _PUBLIC_SAFE + """
    IF p_password IS NULL OR p_password !~ '^[A-Za-z0-9_-]{48,128}$' THEN
        RAISE EXCEPTION 'application_database_credential';
    END IF;
    IF (SELECT retired_through FROM loom_application_access.applications WHERE application_id=p_app)>=p_generation THEN
        RAISE EXCEPTION 'application_database_retired';
    END IF;
    SELECT * INTO STRICT v_binding FROM loom_application_access.binding;
    v_runtime := pg_catalog.to_regrole(v_binding.runtime_role)::oid;
    v_hash := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p_password,'UTF8')),'hex');
    SELECT * INTO v_access FROM loom_application_access.generations
        WHERE application_id=p_app AND generation=p_generation;
    IF FOUND THEN
        PERFORM loom_application_access.require_role(v_access.role_name,v_access.role_oid,v_runtime);
        SELECT rolpassword INTO v_verifier FROM pg_catalog.pg_authid WHERE oid=v_access.role_oid AND rolcanlogin;
        IF v_access.retired OR v_access.password_sha256<>v_hash OR v_verifier IS NULL
           OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_verifier,'UTF8')),'hex')<>v_access.verifier_sha256
           OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members
                          WHERE roleid=v_runtime AND member=v_access.role_oid) THEN
            RAISE EXCEPTION 'application_database_credential';
        END IF;
        RETURN v_access.role_name;
    END IF;
    PERFORM pg_catalog.pg_stat_clear_snapshot();
    IF EXISTS (SELECT 1 FROM loom_application_access.generations WHERE application_id=p_app AND NOT retired)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a JOIN loom_application_access.generations g
                  ON g.role_oid=a.usesysid WHERE g.application_id=p_app) THEN
        RAISE EXCEPTION 'application_database_previous_access_active';
    END IF;
    v_role := 'lap_' || replace(p_incarnation::text,'-','') || '_g' || p_generation::text;
    IF pg_catalog.to_regrole(v_role) IS NOT NULL THEN
        RAISE EXCEPTION 'application_database_role_identity';
    END IF;
    PERFORM pg_catalog.set_config('password_encryption','scram-sha-256',true);
    EXECUTE pg_catalog.format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',v_role,p_password);
    EXECUTE pg_catalog.format('GRANT %I TO %I WITH INHERIT TRUE, SET FALSE, ADMIN FALSE',v_binding.runtime_role,v_role);
    SELECT oid,rolpassword INTO STRICT v_oid,v_verifier FROM pg_catalog.pg_authid WHERE rolname=v_role;
    INSERT INTO loom_application_access.generations
        (application_id,generation,role_name,role_oid,password_sha256,verifier_sha256)
        VALUES(p_app,p_generation,v_role,v_oid,v_hash,
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_verifier,'UTF8')),'hex'));
    RETURN v_role;
END
"""

_REVOKE = """
DECLARE v_binding loom_application_access.binding%ROWTYPE; v_access record; v_runtime oid;
BEGIN
    PERFORM loom_application_access.lock_application(p_data,p_app,p_incarnation,p_generation);
    SELECT * INTO STRICT v_binding FROM loom_application_access.binding;
    v_runtime := pg_catalog.to_regrole(v_binding.runtime_role)::oid;
    FOR v_access IN SELECT * FROM loom_application_access.generations
        WHERE application_id=p_app AND generation<=p_generation ORDER BY generation LOOP
        PERFORM loom_application_access.require_role(v_access.role_name,v_access.role_oid,v_runtime);
        EXECUTE pg_catalog.format('ALTER ROLE %I NOLOGIN PASSWORD NULL',v_access.role_name);
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE roleid=v_runtime AND member=v_access.role_oid) THEN
            EXECUTE pg_catalog.format('REVOKE %I FROM %I',v_binding.runtime_role,v_access.role_name);
        END IF;
    END LOOP;
    UPDATE loom_application_access.generations SET retired=true
        WHERE application_id=p_app AND generation<=p_generation;
    UPDATE loom_application_access.applications SET retired_through=greatest(retired_through,p_generation)
        WHERE application_id=p_app;
END
"""

_DRAIN = """
DECLARE v_access record; v_pid integer; v_runtime oid;
BEGIN
    PERFORM loom_application_access.lock_application(p_data,p_app,p_incarnation,p_generation);
    IF (SELECT retired_through FROM loom_application_access.applications WHERE application_id=p_app)<p_generation THEN
        RAISE EXCEPTION 'application_database_not_retired';
    END IF;
""" + _PUBLIC_SAFE + """
    SELECT pg_catalog.to_regrole(runtime_role)::oid INTO STRICT v_runtime FROM loom_application_access.binding;
    FOR v_access IN SELECT * FROM loom_application_access.generations
        WHERE application_id=p_app AND generation<=p_generation LOOP
        PERFORM loom_application_access.require_role(v_access.role_name,v_access.role_oid,v_runtime);
        IF NOT v_access.retired OR EXISTS (SELECT 1 FROM pg_catalog.pg_authid WHERE oid=v_access.role_oid
                                          AND (rolcanlogin OR rolpassword IS NOT NULL))
           OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=v_access.role_oid) THEN
            RAISE EXCEPTION 'application_database_not_retired';
        END IF;
        FOR v_pid IN SELECT pid FROM pg_catalog.pg_stat_activity WHERE usesysid=v_access.role_oid LOOP
            IF NOT pg_catalog.pg_terminate_backend(v_pid,5000) THEN RETURN false; END IF;
        END LOOP;
    END LOOP;
    PERFORM pg_catalog.pg_stat_clear_snapshot();
    RETURN NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a
        JOIN loom_application_access.generations g ON g.role_oid=a.usesysid
        WHERE g.application_id=p_app AND g.generation<=p_generation);
END
"""

_ROUTINES = {
    "lock_application": ("p_data uuid,p_app uuid,p_incarnation uuid,p_generation bigint", "void", _LOCK),
    "require_role": ("p_role text,p_oid oid,p_runtime oid", "void", _ROLE_SAFE),
    "grant_access": ("p_data uuid,p_app uuid,p_incarnation uuid,p_generation bigint,p_password text", "text", _GRANT),
    "revoke_access": ("p_data uuid,p_app uuid,p_incarnation uuid,p_generation bigint", "void", _REVOKE),
    "drain_access": ("p_data uuid,p_app uuid,p_incarnation uuid,p_generation bigint", "boolean", _DRAIN),
}


def _failure(exc: psycopg.Error) -> ApplicationDatabaseAccessError:
    message = exc.diag.message_primary or ""
    if re.fullmatch(r"application_database_[a-z_]{1,48}", message) is None:
        message = "application_database_operation_failed"
    return ApplicationDatabaseAccessError(message)


def _idle(connection: psycopg.Connection[Any]) -> None:
    if not connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("application database access requires an idle autocommit connection")
    if connection.info.server_version // 10000 not in {16, 17}:
        raise ValueError("application database access requires PostgreSQL16 or17")


def install_application_database_access(
    connection: psycopg.Connection[Any], *, data_environment_id: UUID, manager_role: str,
) -> None:
    """Install/reapply trusted shared-side grants; never adopt another binding.

    The protected caller selects the development DB and provisions the dedicated
    ordinary manager login. This routine does not grant that login general DDL.
    Private authority is retained independently of application Alembic migrations.
    """
    _idle(connection)
    if (not isinstance(data_environment_id, UUID) or not data_environment_id.int
            or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", manager_role) is None):
        raise ValueError("invalid shared application database installation")
    runtime = "loom_app_runtime_" + data_environment_id.hex
    try:
        with connection.transaction():
            if connection.execute("SELECT current_user=session_user AND rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user").fetchone() != (True,):
                raise ApplicationDatabaseAccessError("application_database_administrator_required")
            if connection.execute("SELECT NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls AND rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=%s", (manager_role,)).fetchone() != (True,):
                raise ApplicationDatabaseAccessError("application_database_manager_identity")
            expected = (data_environment_id, manager_role, runtime)
            namespace = connection.execute("SELECT pg_catalog.to_regnamespace(%s)", (_SCHEMA,)).fetchone()
            if namespace == (None,):
                connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(sql.Identifier(runtime)))
                connection.execute("""
                    CREATE SCHEMA loom_application_access;
                    REVOKE ALL ON SCHEMA loom_application_access FROM PUBLIC;
                    CREATE TABLE loom_application_access.binding(
                        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                        data_environment_id uuid NOT NULL, manager_role text NOT NULL,
                        manager_oid oid NOT NULL, runtime_role text NOT NULL, runtime_oid oid NOT NULL,
                        database_oid oid NOT NULL, system_identifier text NOT NULL);
                    CREATE TABLE loom_application_access.applications(
                        application_id uuid PRIMARY KEY, incarnation uuid UNIQUE NOT NULL,
                        retired_through bigint NOT NULL DEFAULT 0 CHECK(retired_through>=0));
                    CREATE TABLE loom_application_access.generations(
                        application_id uuid NOT NULL REFERENCES loom_application_access.applications,
                        generation bigint NOT NULL CHECK(generation>0), role_name text UNIQUE NOT NULL,
                        role_oid oid UNIQUE NOT NULL, password_sha256 text NOT NULL, verifier_sha256 text NOT NULL,
                        retired boolean NOT NULL DEFAULT false, PRIMARY KEY(application_id,generation));
                """)
                connection.execute("INSERT INTO loom_application_access.binding(data_environment_id,manager_role,runtime_role,manager_oid,runtime_oid,database_oid,system_identifier) SELECT %s,%s,%s,pg_catalog.to_regrole(%s)::oid,pg_catalog.to_regrole(%s)::oid,d.oid,s.system_identifier::text FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_control_system() s WHERE d.datname=current_database()", (*expected, manager_role, runtime))
                for name, (arguments, result, body) in _ROUTINES.items():
                    connection.execute(sql.SQL("CREATE FUNCTION {}.{}({}) RETURNS {} LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS {}").format(
                        sql.Identifier(_SCHEMA), sql.Identifier(name), sql.SQL(arguments), sql.SQL(result), sql.Literal(body)))
                connection.execute("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA loom_application_access FROM PUBLIC")
                connection.execute(sql.SQL("GRANT USAGE ON SCHEMA loom_application_access TO {}").format(sql.Identifier(manager_role)))
                for name in ("grant_access", "revoke_access", "drain_access"):
                    arguments = "uuid,uuid,uuid,bigint" + (",text" if name == "grant_access" else "")
                    connection.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {}.{}({}) TO {}").format(
                        sql.Identifier(_SCHEMA), sql.Identifier(name), sql.SQL(arguments), sql.Identifier(manager_role)))
            observed = connection.execute("SELECT data_environment_id,manager_role,runtime_role FROM loom_application_access.binding WHERE manager_oid=pg_catalog.to_regrole(manager_role)::oid AND runtime_oid=pg_catalog.to_regrole(runtime_role)::oid AND database_oid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()) AND system_identifier=(SELECT system_identifier::text FROM pg_catalog.pg_control_system())").fetchone()
            if observed != expected:
                raise ApplicationDatabaseAccessError("application_database_binding")
            for name, (_, _, body) in _ROUTINES.items():
                if connection.execute("SELECT prosrc,prosecdef,proconfig FROM pg_catalog.pg_proc WHERE pronamespace=pg_catalog.to_regnamespace(%s) AND proname=%s", (_SCHEMA, name)).fetchall() != [(body, True, ["search_path=pg_catalog, pg_temp"])]:
                    raise ApplicationDatabaseAccessError("application_database_installation_drift")
            connection.execute(sql.SQL("DO {} ").format(sql.Literal("BEGIN " + _PUBLIC_SAFE + " END")))
            connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}; GRANT USAGE ON SCHEMA public TO {}").format(
                sql.Identifier(connection.info.dbname), sql.Identifier(runtime), sql.Identifier(runtime)))
            # Do not inherit loom_service: its historical grants include writes
            # to alembic_version. Migrations reapply these bounded shared grants.
            connection.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}").format(
                sql.Identifier(runtime), sql.Identifier(runtime), sql.Identifier(runtime)))
            tables = connection.execute("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','v') AND relname<>'alembic_version'").fetchall()
            for (table,) in tables:
                connection.execute(sql.SQL("GRANT INSERT,UPDATE,DELETE ON {}.{} TO {}").format(
                    sql.Identifier("public"), sql.Identifier(table), sql.Identifier(runtime)))
    except psycopg.Error as exc:
        raise _failure(exc) from None


class ApplicationDatabaseAccess:
    """One auto-committed operation per call; never expose provider SQL errors."""

    def __init__(self, connection: psycopg.Connection[Any], data_environment_id: UUID):
        self.connection, self.data_environment_id = connection, data_environment_id

    def _call(self, name: str, app: UUID, incarnation: UUID, generation: int, password: str | None = None) -> Any:
        _idle(self.connection)
        if (any(not isinstance(value, UUID) or not value.int for value in (self.data_environment_id, app, incarnation))
                or type(generation) is not int or not 1 <= generation <= 2**63 - 1):
            raise ValueError("invalid shared application database identity")
        arguments: list[Any] = [self.data_environment_id, app, incarnation, generation]
        if name == "grant_access":
            if not isinstance(password, str) or _PASSWORD.fullmatch(password) is None:
                raise ValueError("invalid shared application database credential")
            arguments.append(password)
        try:
            result = self.connection.execute(sql.SQL("SELECT {}.{}({})").format(
                sql.Identifier(_SCHEMA), sql.Identifier(name), sql.SQL(",").join(sql.Placeholder() for _ in arguments)), arguments).fetchone()
        except psycopg.Error as exc:
            raise _failure(exc) from None
        if result is None:
            raise ApplicationDatabaseAccessError("application_database_result_missing")
        return result[0]

    def grant(self, application_id: UUID, incarnation: UUID, generation: int, password: str) -> str:
        role = self._call("grant_access", application_id, incarnation, generation, password)
        if not isinstance(role, str):
            raise ApplicationDatabaseAccessError("application_database_result_invalid")
        return role

    def revoke(self, application_id: UUID, incarnation: UUID, through_generation: int) -> None:
        self._call("revoke_access", application_id, incarnation, through_generation)

    def drain(self, application_id: UUID, incarnation: UUID, through_generation: int) -> bool:
        result = self._call("drain_access", application_id, incarnation, through_generation)
        if type(result) is not bool:
            raise ApplicationDatabaseAccessError("application_database_result_invalid")
        return result
