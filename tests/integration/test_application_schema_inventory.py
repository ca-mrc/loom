"""Read-only application shape evidence; never ownership-transfer authority."""

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url

from loom.application_schema_inventory import (
    ApplicationSchemaInventoryError,
    read_application_schema_inventory,
)
from tests.integration.test_application_migration_authority import (
    application_migration_roles,  # noqa: F401
)


def _connect(url: str) -> str:
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


@pytest.fixture
def inventory_database(postgres_url: str) -> Iterator[str]:
    source = make_url(postgres_url)
    assert source.database is not None
    database = f"app_inventory_{uuid4().hex}"
    with psycopg.connect(_connect(postgres_url), autocommit=True) as admin:
        admin.execute(
            psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                psycopg.sql.Identifier(database), psycopg.sql.Identifier(source.database)
            )
        )
    try:
        yield source.set(database=database).render_as_string(hide_password=False)
    finally:
        with psycopg.connect(_connect(postgres_url), autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(database)
                )
            )


def test_inventory_matches_independent_actual_migrations_and_restores_search_path(
    postgres_url: str,
    application_migration_roles: tuple[Config, str, str, str],  # noqa: F811
) -> None:
    cfg, target_url, owner, _migrator = application_migration_roles
    command.upgrade(cfg, "head")
    with psycopg.connect(_connect(postgres_url)) as reference:
        reference.execute("SET TRANSACTION READ ONLY")
        baseline = read_application_schema_inventory(
            reference, role_bindings={str(make_url(postgres_url).username): "application-owner"}
        )
    with psycopg.connect(_connect(target_url)) as target:
        target.execute("SET TRANSACTION READ ONLY")
        target.execute("SET LOCAL search_path = public, pg_catalog")
        observed = read_application_schema_inventory(
            target, role_bindings={owner: "application-owner"}
        )
        assert target.execute("SHOW search_path").fetchone() == ("public, pg_catalog",)
        assert target.execute("SHOW transaction_read_only").fetchone() == ("on",)
    assert len(observed.objects) > 200
    assert observed.differences_from(baseline) == ()
    assert observed.sha256 == baseline.sha256


@pytest.mark.parametrize(
    ("setup", "mutation", "kind"),
    [
        ("", "ALTER FUNCTION public.trials_inflight_delta() SECURITY DEFINER", "routine"),
        ("", "ALTER FUNCTION public.trials_inflight_delta() SET search_path=public", "routine"),
        ("", "ALTER FUNCTION public.trials_inflight_delta() COST 17", "routine"),
        (
            "",
            "CREATE OR REPLACE FUNCTION public.trials_inflight_delta() RETURNS trigger "
            "LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
            "routine",
        ),
        ("", "ALTER TABLE public.trials ALTER COLUMN attempt_count SET DEFAULT 999", "column"),
        (
            "",
            "ALTER TABLE public.tasks ADD CONSTRAINT inventory_check CHECK (length(id)>0)",
            "constraint",
        ),
        ("", "ALTER TABLE public.trials DISABLE TRIGGER trials_inflight_count", "trigger"),
        ("", "ALTER TABLE public.trials DISABLE TRIGGER ALL", "trigger"),
        ("", "GRANT TRIGGER ON public.trials TO PUBLIC", "relation"),
        ("", "GRANT UPDATE (state) ON public.trials TO PUBLIC", "column"),
        ("", "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC", "default_acl"),
        ("", "CREATE RULE inventory_rule AS ON DELETE TO public.teams DO INSTEAD NOTHING", "rule"),
        ("", "CREATE POLICY inventory_policy ON public.teams USING (true)", "policy"),
        ("", "ALTER TABLE public.teams ENABLE ROW LEVEL SECURITY", "relation"),
        (
            "CREATE TYPE public.inventory_enum AS ENUM ('a','b')",
            "ALTER TYPE public.inventory_enum ADD VALUE 'c'",
            "type",
        ),
        (
            "CREATE DOMAIN public.inventory_domain AS integer CONSTRAINT positive CHECK (VALUE>0)",
            "ALTER DOMAIN public.inventory_domain DROP CONSTRAINT positive",
            "constraint",
        ),
        (
            "CREATE SEQUENCE public.inventory_sequence",
            "ALTER SEQUENCE public.inventory_sequence INCREMENT 3 CACHE 99",
            "sequence",
        ),
        (
            "CREATE SCHEMA inventory_foreign; CREATE TABLE inventory_foreign.parent (id integer); "
            "CREATE TABLE public.inventory_child (id integer)",
            "ALTER TABLE public.inventory_child INHERIT inventory_foreign.parent",
            "inheritance",
        ),
        (
            "CREATE TABLE public.inventory_index (id integer, value integer); "
            "CREATE INDEX inventory_index_idx ON public.inventory_index(id)",
            "DROP INDEX public.inventory_index_idx; "
            "CREATE INDEX inventory_index_idx ON public.inventory_index(value)",
            "index",
        ),
    ],
)
def test_inventory_detects_real_catalog_drift(
    inventory_database: str, setup: str, mutation: str, kind: str
) -> None:
    bindings = {str(make_url(inventory_database).username): "application-owner"}
    with psycopg.connect(_connect(inventory_database)) as connection:
        if setup:
            connection.execute(setup)
        else:
            connection.execute("SELECT 1")
        baseline = read_application_schema_inventory(connection, role_bindings=bindings)
        connection.execute(mutation)
        observed = read_application_schema_inventory(connection, role_bindings=bindings)
        differences = observed.differences_from(baseline)
        assert any(item.kind == kind for item in differences), differences
        assert observed.sha256 != baseline.sha256


def test_inventory_excludes_rows_sequence_cursors_statistics_and_unrelated_foreign_objects(
    inventory_database: str,
) -> None:
    bindings = {str(make_url(inventory_database).username): "application-owner"}
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("CREATE SEQUENCE public.inventory_sequence")
        baseline = read_application_schema_inventory(connection, role_bindings=bindings)
        connection.execute("SELECT nextval('public.inventory_sequence')")
        connection.execute(
            "INSERT INTO public.teams(id,name) VALUES (%s, 'inventory-team')", (uuid4(),)
        )
        connection.execute("ANALYZE public.teams")
        connection.execute("CREATE SCHEMA inventory_foreign")
        connection.execute("CREATE TABLE inventory_foreign.untouched (id integer)")
        connection.execute("INSERT INTO inventory_foreign.untouched VALUES (7)")
        observed = read_application_schema_inventory(connection, role_bindings=bindings)
        assert observed == baseline
        assert connection.execute("SELECT id FROM inventory_foreign.untouched").fetchone() == (7,)


def test_role_aliases_never_rewrite_sql_bodies(inventory_database: str) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(
            "CREATE FUNCTION public.inventory_literal() RETURNS text LANGUAGE sql AS $$SELECT 'alice'$$"
        )
        baseline = read_application_schema_inventory(connection, role_bindings={"alice": "owner"})
        connection.execute(
            "CREATE OR REPLACE FUNCTION public.inventory_literal() RETURNS text LANGUAGE sql AS $$SELECT 'bob'$$"
        )
        observed = read_application_schema_inventory(connection, role_bindings={"bob": "owner"})
        assert observed.differences_from(baseline)


@pytest.mark.parametrize("enabled", [True, False])
def test_catalog_refuses_databasewide_event_triggers_outside_public(inventory_database, enabled):
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("CREATE SCHEMA inventory_foreign")
        connection.execute(
            "CREATE FUNCTION inventory_foreign.event_policy() RETURNS event_trigger LANGUAGE plpgsql "
            "AS $$BEGIN RAISE EXCEPTION 'private callback must not execute'; END$$"
        )
        baseline = read_application_schema_inventory(connection, role_bindings={})
        connection.execute(
            "CREATE EVENT TRIGGER inventory_event_policy ON ddl_command_start "
            "WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION inventory_foreign.event_policy()"
        )
        if not enabled:
            connection.execute("ALTER EVENT TRIGGER inventory_event_policy DISABLE")
        with pytest.raises(ApplicationSchemaInventoryError, match="event trigger"):
            read_application_schema_inventory(connection, role_bindings={})
        connection.execute("DROP EVENT TRIGGER inventory_event_policy")
        assert read_application_schema_inventory(connection, role_bindings={}) == baseline


def test_catalog_bootstrap_cannot_call_a_public_shadow_function(inventory_database: str) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(
            "CREATE FUNCTION public.set_config(text,text,boolean) RETURNS text LANGUAGE plpgsql "
            "AS $$BEGIN RAISE EXCEPTION 'public shadow must not execute'; END$$"
        )
        connection.execute("SET LOCAL search_path=public,pg_catalog")
        read_application_schema_inventory(connection, role_bindings={})
        assert connection.execute("SHOW search_path").fetchone() == ("public, pg_catalog",)


def test_catalog_cannot_read_a_temporary_shadow_view(inventory_database: str) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(
            "CREATE FUNCTION public.inventory_namespace(name) RETURNS name LANGUAGE plpgsql "
            "AS $$BEGIN RAISE EXCEPTION 'temporary catalog view must not execute'; END$$"
        )
        baseline = read_application_schema_inventory(connection, role_bindings={})
        connection.execute(
            "CREATE TEMP VIEW pg_namespace AS SELECT oid, "
            "public.inventory_namespace(nspname) AS nspname, nspowner, nspacl "
            "FROM pg_catalog.pg_namespace"
        )
        assert read_application_schema_inventory(connection, role_bindings={}) == baseline
        assert connection.execute("SHOW search_path").fetchone() == ('"$user", public',)


def test_catalog_never_queries_a_replaced_migration_version_relation(
    inventory_database: str,
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("SELECT 1")
        baseline = read_application_schema_inventory(connection, role_bindings={})
        connection.execute("DROP TABLE public.alembic_version")
        connection.execute(
            "CREATE FUNCTION public.inventory_version() RETURNS varchar LANGUAGE plpgsql "
            "AS $$BEGIN RAISE EXCEPTION 'version view must not execute'; END$$"
        )
        connection.execute(
            "CREATE VIEW public.alembic_version AS SELECT public.inventory_version() AS version_num"
        )
        observed = read_application_schema_inventory(connection, role_bindings={})
        assert any(d.kind == "relation" for d in observed.differences_from(baseline))


def test_catalog_deparse_is_session_independent_and_restores_settings(
    inventory_database: str,
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(
            "CREATE TABLE public.inventory_timestamp (value timestamptz "
            "DEFAULT '2026-01-01 01:02:03+00'::timestamptz)"
        )
        baseline = read_application_schema_inventory(connection, role_bindings={})
        connection.execute("SET LOCAL quote_all_identifiers=on")
        connection.execute("SET LOCAL timezone='America/Toronto'")
        observed = read_application_schema_inventory(connection, role_bindings={})
        assert observed == baseline
        assert connection.execute("SHOW quote_all_identifiers").fetchone() == ("on",)
        assert connection.execute("SHOW timezone").fetchone() == ("America/Toronto",)


def test_catalog_excludes_fast_default_row_storage(inventory_database: str) -> None:
    with psycopg.connect(_connect(inventory_database), autocommit=True) as connection:
        connection.execute(
            "INSERT INTO public.teams(id,name) VALUES (%s,'fast-default')", (uuid4(),)
        )
        connection.execute(
            "ALTER TABLE public.teams ADD COLUMN inventory_default integer NOT NULL DEFAULT 7"
        )
        with connection.transaction():
            baseline = read_application_schema_inventory(connection, role_bindings={})
            assert connection.execute(
                "SELECT atthasmissing FROM pg_attribute WHERE attrelid='public.teams'::regclass "
                "AND attname='inventory_default'"
            ).fetchone() == (True,)
        connection.execute("VACUUM FULL public.teams")
        with connection.transaction():
            observed = read_application_schema_inventory(connection, role_bindings={})
            assert connection.execute(
                "SELECT atthasmissing FROM pg_attribute WHERE attrelid='public.teams'::regclass "
                "AND attname='inventory_default'"
            ).fetchone() == (False,)
            assert observed == baseline


@pytest.mark.parametrize(
    "usage", ["public", "foreign_column", "foreign_default", "foreign_index", "unrelated"]
)
def test_catalog_refuses_non_native_type_output_before_deparsing(
    inventory_database: str, usage: str
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("SELECT 1")
        baseline = read_application_schema_inventory(connection, role_bindings={})
        schema = "public" if usage == "public" else "inventory_foreign"
        if schema != "public":
            connection.execute("CREATE SCHEMA inventory_foreign")
        connection.execute(
            f"CREATE TYPE {schema}.inventory_custom; "
            f"CREATE FUNCTION {schema}.inventory_custom_in(cstring) RETURNS {schema}.inventory_custom "
            "AS 'int4in' LANGUAGE internal IMMUTABLE STRICT; "
            f"CREATE FUNCTION {schema}.inventory_custom_out({schema}.inventory_custom) RETURNS cstring "
            "AS 'int4out' LANGUAGE internal IMMUTABLE STRICT; "
            f"CREATE TYPE {schema}.inventory_custom (INPUT={schema}.inventory_custom_in, "
            f"OUTPUT={schema}.inventory_custom_out, INTERNALLENGTH=4, PASSEDBYVALUE, ALIGNMENT=int4)"
        )
        if usage == "unrelated":
            assert read_application_schema_inventory(connection, role_bindings={}) == baseline
            return
        if usage == "foreign_index":
            connection.execute(
                "CREATE FUNCTION inventory_foreign.inventory_predicate(integer, "
                "inventory_foreign.inventory_custom) RETURNS boolean LANGUAGE plpgsql "
                "IMMUTABLE AS $$BEGIN RETURN true; END$$; "
                "CREATE TABLE public.inventory_index_type(id integer); "
                "CREATE INDEX inventory_index_type_idx ON public.inventory_index_type(id) "
                "WHERE inventory_foreign.inventory_predicate(id, "
                "'7'::inventory_foreign.inventory_custom)"
            )
        elif usage == "foreign_default":
            connection.execute(
                "CREATE FUNCTION public.inventory_default(value inventory_foreign.inventory_custom "
                "DEFAULT '7'::inventory_foreign.inventory_custom) RETURNS integer LANGUAGE sql "
                "AS $$SELECT 7$$"
            )
        else:
            connection.execute(
                f"CREATE TABLE public.inventory_custom_table (value {schema}.inventory_custom "
                f"DEFAULT '7'::{schema}.inventory_custom)"
            )
        with pytest.raises(ApplicationSchemaInventoryError, match="type output"):
            read_application_schema_inventory(connection, role_bindings={})
        assert connection.execute("SHOW search_path").fetchone() == ('"$user", public',)


@pytest.mark.parametrize("method", ["index", "table"])
def test_catalog_refuses_non_native_access_method_before_deparsing(
    inventory_database: str, method: str
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("CREATE SCHEMA inventory_foreign")
        if method == "index":
            connection.execute(
                "CREATE FUNCTION inventory_foreign.inventory_handler(internal) "
                "RETURNS index_am_handler AS 'hashhandler' LANGUAGE internal; "
                "CREATE ACCESS METHOD inventory_am TYPE INDEX "
                "HANDLER inventory_foreign.inventory_handler; "
                "CREATE OPERATOR CLASS inventory_foreign.inventory_ops "
                "FOR TYPE integer USING inventory_am AS "
                "OPERATOR 1 = (integer,integer), "
                "FUNCTION 1 pg_catalog.hashint4(integer); "
                "CREATE TABLE public.inventory_am_table(id integer); "
                "CREATE INDEX inventory_am_index ON public.inventory_am_table "
                "USING inventory_am(id inventory_foreign.inventory_ops)"
            )
        else:
            connection.execute(
                "CREATE FUNCTION inventory_foreign.inventory_handler(internal) "
                "RETURNS table_am_handler AS 'heap_tableam_handler' LANGUAGE internal; "
                "CREATE ACCESS METHOD inventory_am TYPE TABLE "
                "HANDLER inventory_foreign.inventory_handler; "
                "CREATE TABLE public.inventory_am_table(id integer) USING inventory_am"
            )
        with pytest.raises(ApplicationSchemaInventoryError, match="access method"):
            read_application_schema_inventory(connection, role_bindings={})


def test_catalog_read_error_restores_transaction_local_settings(inventory_database: str) -> None:
    from unittest.mock import patch

    import loom.application_schema_inventory as inventory_module

    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute("SET LOCAL search_path=public,pg_catalog")
        connection.execute("SET LOCAL timezone='America/Toronto'")
        with patch.object(inventory_module, "_CATALOG_SQL", "SELECT 1/0 WHERE %s IS NOT NULL"):
            with pytest.raises(ApplicationSchemaInventoryError, match="observation failed"):
                read_application_schema_inventory(connection, role_bindings={})
        assert connection.execute("SHOW search_path").fetchone() == ("public, pg_catalog",)
        assert connection.execute("SHOW timezone").fetchone() == ("America/Toronto",)
        assert connection.execute("SELECT 7").fetchone() == (7,)


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
def test_catalog_refuses_old_snapshot_isolation(inventory_database: str, isolation: str) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
        connection.execute("SET LOCAL search_path=public,pg_catalog")
        connection.execute("SELECT 1")
        with pytest.raises(ApplicationSchemaInventoryError, match="READ COMMITTED"):
            read_application_schema_inventory(connection, role_bindings={})
        assert connection.execute("SHOW search_path").fetchone() == ("public, pg_catalog",)
        assert connection.execute("SELECT 7").fetchone() == (7,)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE OPERATOR public.=== (LEFTARG=integer,RIGHTARG=integer,FUNCTION=pg_catalog.int4eq)",
        "CREATE OPERATOR FAMILY public.inventory_family USING btree",
        "CREATE OPERATOR CLASS public.inventory_ops FOR TYPE integer USING btree AS "
        "OPERATOR 1 < (integer,integer), FUNCTION 1 pg_catalog.btint4cmp(integer,integer)",
        "CREATE COLLATION public.inventory_collation (provider=libc,locale='C')",
        "CREATE TEXT SEARCH CONFIGURATION public.inventory_search (COPY=pg_catalog.english)",
        "CREATE TEXT SEARCH DICTIONARY public.inventory_dictionary (TEMPLATE=pg_catalog.simple)",
    ],
)
def test_catalog_refuses_unhandled_namespaced_objects(
    inventory_database: str, statement: str
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(statement)
        with pytest.raises(ApplicationSchemaInventoryError, match=r"unsupported.*schema object"):
            read_application_schema_inventory(connection, role_bindings={})


@pytest.mark.parametrize("object_sql", ["TABLE public.teams", "SCHEMA public"])
def test_catalog_refuses_extension_managed_application_objects(
    inventory_database: str, object_sql: str
) -> None:
    with psycopg.connect(_connect(inventory_database)) as connection:
        connection.execute(f"ALTER EXTENSION plpgsql ADD {object_sql}")
        with pytest.raises(ApplicationSchemaInventoryError, match="extension"):
            read_application_schema_inventory(connection, role_bindings={})


def test_inventory_observes_actual_guard_migrated_application_read_only(
    capacity_guard_database: dict[str, object],
) -> None:
    admin_url = capacity_guard_database["admin_url"]
    guard_owner = capacity_guard_database["owner_role"]
    assert isinstance(admin_url, str) and isinstance(guard_owner, str)
    with psycopg.connect(_connect(admin_url)) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        inventory = read_application_schema_inventory(
            connection, role_bindings={guard_owner: "guard-owner"}
        )
        assert any(
            o.kind == "trigger" and "capacity_guard_lock_trial_writer" in o.identity
            for o in inventory.objects
        )
        assert any(
            o.kind == "routine" and "loom_drop_trial_writer_triggers" in o.identity
            for o in inventory.objects
        )
