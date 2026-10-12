"""Bounded application DDL credentials on disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.asyncio
async def test_application_migrator_journals_oid_before_login_and_retires_surviving_owner_session(transfer_database):  # noqa: F811
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        recorded = []
        def persist(identity):
            assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone() == (False, None)
            recorded.append(identity)
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=persist)
        assert recorded == [identity]
        expires = datetime.now(UTC) + timedelta(minutes=45)
        try:
            for _ in range(2):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expires)
            with psycopg.connect(transfer_database[0], user=name, password=password, autocommit=True) as active:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                active.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(target.successor_role)))
                active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                for _ in range(2):
                    seal_application_migrator(peer, **authority, identity=identity)
                assert active.execute("SELECT current_user").fetchone() == (target.successor_role,)
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
                retire_application_migrator(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)
                with pytest.raises(psycopg.OperationalError):
                    active.execute("SELECT 1")
            assert peer.execute("SELECT relowner::bigint FROM pg_class WHERE relname='app_migrator_probe'").fetchone() == (target.successor_oid,)
            assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["closed", "expired", "long-lease", "oid", "credential", "expiry", "membership", "role-setting"])
async def test_migrator_arming_refuses_changed_admission_or_saved_authority(transfer_database, drift):  # noqa: F811
    from dataclasses import replace

    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=lambda _: None)
        expiry = datetime.now(UTC) + timedelta(minutes=45)
        try:
            if drift in {"credential", "expiry"}:
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            if drift == "closed":
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
            elif drift == "expired":
                expiry = datetime.now(UTC) - timedelta(seconds=1)
            elif drift == "long-lease":
                expiry = datetime.now(UTC) + timedelta(hours=2)
            elif drift == "oid":
                identity = replace(identity, role_oid=identity.role_oid + 1)
            elif drift == "credential":
                password += "changed"
            elif drift == "expiry":
                expiry += timedelta(seconds=1)
            elif drift == "membership":
                peer.execute(sql.SQL("GRANT pg_read_all_data TO {}").format(sql.Identifier(name)))
            elif drift == "role-setting":
                peer.execute(sql.SQL("ALTER ROLE {} SET role TO {}").format(sql.Identifier(name), sql.Identifier(target.successor_role)))
            with pytest.raises(RuntimeError):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            assert peer.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (name,)).fetchone() == (drift in {"credential", "expiry"},)
        finally:
            peer.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM {}; DROP ROLE {}").format(
                sql.Identifier(target.database), sql.Identifier(name), sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["journal", "guard", "collision"])
async def test_migrator_creation_never_adopts_ambient_role_or_outlives_failed_journal(transfer_database, failure):  # noqa: F811
    from loom.application_migrator_provision import create_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        name = "app_migrator_" + uuid4().hex
        if failure == "collision":
            peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(name)))
        recorded = []
        def persist(identity):
            recorded.append(identity)
            if failure == "journal":
                raise RuntimeError("journal publication failed")
            if failure == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
        try:
            with pytest.raises(RuntimeError):
                create_application_migrator(peer, target=args["target"], coordination_guard=args["coordination_guard"],
                    provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
                    migrator_role=name, persist_identity=persist)
            assert bool(peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone()) == (failure == "collision")
            assert bool(recorded) == (failure != "collision")
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


# Keep this historical guard profile below the retirement boundary; the refusal
# regression below separately verifies that it cannot silently advance to head.
@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("target_revision", ["0142", "0146", "0147", "0148", "0149", "0150", "0151", "0156", "0170"])
async def test_actual_baseline_upgrade_preserves_separated_runtime_authority(transfer_database, monkeypatch, target_revision):  # noqa: F811
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.engine import make_url

    from loom.application_completed_authority import observe_completed_application_authority
    from loom.application_migrator_admission import (
        close_application_migrator_admission,
        reopen_application_migrator_admission,
    )
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        team, trial, task = uuid4(), uuid4(), "handoff-reward-" + uuid4().hex
        peer.execute("INSERT INTO teams(id,name) VALUES (%s,'handoff reward')", (team,))
        peer.execute("INSERT INTO tasks(id,config,checksum) VALUES (%s,'{}',%s)", (task, "a" * 64))
        peer.execute("""INSERT INTO trials(id,team_id,task_id,config,requires_caps,state,result)
            VALUES (%s,%s,%s,'{}','{}','succeeded',
            '{"schema_version":"loom.service-execution-trial-result.v1","reward":{"passed":1.0},
              "runtime_result":{"verifier_rewards":{"passed":1.0},"status":"succeeded"}}')""", (trial, team, task))
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        if target_revision in {"0147", "0148", "0149", "0150", "0151", "0156", "0170"}:
            from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility
            guard_owner = next(name for name, binding in args["role_bindings"].items() if binding == "guard-owner")
            metadata_query = """SELECT p.oid,p.proowner,p.proacl,p.prosecdef,p.proconfig,
                r.rolcanlogin,r.rolinherit,r.rolsuper,r.rolcreatedb,r.rolcreaterole,
                r.rolreplication,r.rolbypassrls,r.rolpassword
                FROM pg_proc p JOIN pg_authid r ON r.oid=p.proowner
                WHERE p.oid='loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)'::regprocedure"""
            original_metadata = peer.execute(metadata_query).fetchone()
            retained = []
            ensure_guard_claim_compatibility(peer, **authority, guard_owner=guard_owner,
                retained=None, persist=lambda value: retained.append(dict(value)))
            ensure_guard_claim_compatibility(peer, **authority, guard_owner=guard_owner,
                retained=retained[0], persist=lambda _: pytest.fail("replay must retain original inputs"))
            assert peer.execute(metadata_query).fetchone() == original_metadata
            assert peer.execute("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version").fetchone() == ("guard_0030",)
        identity = create_application_migrator(peer, **authority, migrator_role="app_migrator_" + uuid4().hex,
            persist_identity=lambda _: None)
        password = uuid4().hex
        arm_application_migrator(peer, **authority, identity=identity, password=password,
            expires_at=datetime.now(UTC) + timedelta(minutes=45))
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "database/migrations/alembic.ini"))
        config.set_main_option("script_location", str(root / "database" / "migrations"))
        url = make_url(transfer_database[0]).set(drivername="postgresql+psycopg", username=identity.role_name, password=password)
        config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
        monkeypatch.setenv("LOOM_DB_OWNER_ROLE", target.successor_role)
        try:
            command.upgrade(config, target_revision)
            assert peer.execute("SELECT version_num FROM public.alembic_version").fetchone() == (target_revision,)
            assert peer.execute("SELECT result->>'aggregate_reward' FROM trials WHERE id=%s", (trial,)).fetchone() == (
                "1.0" if target_revision in {"0146", "0147", "0148", "0149", "0150", "0151", "0156", "0170"} else None,)
            observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=identity)
            if target_revision in {"0156", "0170"}:
                # The reviewed trigger must be recognized after its introducing
                # migration; a same-named body or execution-context change must not.
                definition = peer.execute(
                    "SELECT pg_get_functiondef('public.guard_provider_secret_attachment()'::regprocedure)"
                ).fetchone()[0]
                for mutation in (
                    "CREATE OR REPLACE FUNCTION public.guard_provider_secret_attachment() RETURNS trigger "
                    "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN RETURN NEW; END $$",
                    "ALTER FUNCTION public.guard_provider_secret_attachment() SET search_path=public",
                    "ALTER FUNCTION public.guard_provider_secret_attachment() STABLE",
                ):
                    try:
                        peer.execute(mutation)
                        with pytest.raises(RuntimeError, match="elevated definer authority changed"):
                            observe_completed_application_authority(
                                peer, target=target, runtime_password=args["password"], successor=identity,
                            )
                    finally:
                        peer.execute(definition)
                observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=identity)
        finally:
            seal_application_migrator(maintenance, **authority, identity=identity)
            close_application_migrator_admission(maintenance, **authority, identity=identity)
            retire_application_migrator(maintenance, **authority, migrator_role=identity.role_name, migrator_oid=identity.role_oid)
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        observe_completed_application_authority(peer, target=target, runtime_password=args["password"])


def _retirement_boundary_state(peer, *, role_bindings, migrator):
    """Observe the disposable target, including objects the failed DDL could lose."""
    from loom.application_schema_inventory import read_application_schema_inventory

    with peer.transaction():
        public = read_application_schema_inventory(peer, role_bindings=role_bindings)
    return {
        "public_catalog": public,
        "application_revision": peer.execute("SELECT version_num FROM public.alembic_version").fetchall(),
        "guard_revision": peer.execute(
            "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
        ).fetchall(),
        "guard_schema": peer.execute(
            "SELECT oid,nspowner,nspacl FROM pg_namespace WHERE nspname='loom_capacity_guard'"
        ).fetchall(),
        "guard_relations": peer.execute("""
            SELECT c.oid,c.relname,c.relowner,c.relacl FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='loom_capacity_guard' ORDER BY c.oid
        """).fetchall(),
        "guard_routines": peer.execute("""
            SELECT p.oid, p.proowner, p.proacl, pg_get_functiondef(p.oid)
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='loom_capacity_guard' ORDER BY p.oid
        """).fetchall(),
        "roles": peer.execute(
            "SELECT to_jsonb(r) FROM pg_authid r WHERE rolname=ANY(%s) ORDER BY oid",
            ([*role_bindings, migrator.role_name],),
        ).fetchall(),
        "memberships": peer.execute("SELECT to_jsonb(m) FROM pg_auth_members m ORDER BY oid").fetchall(),
        "records": {
            table: peer.execute(sql.SQL("SELECT to_jsonb(t) FROM {} t ORDER BY id").format(sql.Identifier(table))).fetchall()
            for table in ("teams", "tasks", "trials")
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("start_revision", ["0134", "0170"])
async def test_historical_guard_retirement_refusal_preserves_schema_data_and_authority(
    transfer_database, monkeypatch, start_revision,  # noqa: F811
):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.engine import make_url

    from loom.application_completed_authority import observe_completed_application_authority
    from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility
    from loom.application_migrator_admission import (
        close_application_migrator_admission,
        reopen_application_migrator_admission,
    )
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        team, trial, task = uuid4(), uuid4(), "retirement-preserved-" + uuid4().hex
        peer.execute("INSERT INTO teams(id,name) VALUES (%s,'retirement preserved')", (team,))
        peer.execute("INSERT INTO tasks(id,config,checksum) VALUES (%s,'{}',%s)", (task, "a" * 64))
        peer.execute("""INSERT INTO trials(id,team_id,task_id,config,requires_caps,state,result)
            VALUES (%s,%s,%s,'{}','{}','succeeded',
            '{"schema_version":"loom.service-execution-trial-result.v1","reward":{"passed":1.0},
              "runtime_result":{"verifier_rewards":{"passed":1.0},"status":"succeeded"}}')""", (trial, team, task))
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        ensure_guard_claim_compatibility(peer, **authority,
            guard_owner=next(n for n, a in args["role_bindings"].items() if a == "guard-owner"),
            retained=None, persist=lambda _: None)
        identity = create_application_migrator(peer, **authority,
            migrator_role="app_migrator_" + uuid4().hex, persist_identity=lambda _: None)
        password = uuid4().hex
        arm_application_migrator(peer, **authority, identity=identity, password=password,
            expires_at=datetime.now(UTC) + timedelta(minutes=45))
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "database/migrations/alembic.ini"))
        url = make_url(transfer_database[0]).set(drivername="postgresql+psycopg", username=identity.role_name, password=password)
        config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
        monkeypatch.setenv("LOOM_DB_OWNER_ROLE", target.successor_role)
        try:
            if start_revision == "0170":
                command.upgrade(config, start_revision)
            roles = {**args["role_bindings"], target.successor_role: "sealed-application-owner"}
            before = _retirement_boundary_state(peer, role_bindings=roles, migrator=identity)
            assert before["application_revision"] == [(start_revision,)]
            assert before["guard_revision"] == [("guard_0030",)]
            # Both a direct historical upgrade and a retry at the last compatible
            # revision must retain installed authority instead of bypassing 0171.
            for destination in ("0171", "head"):
                with pytest.raises(RuntimeError, match="legacy retirement requires dependent SQL routine disposition"):
                    command.upgrade(config, destination)
                assert _retirement_boundary_state(peer, role_bindings=roles, migrator=identity) == before
                observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=identity)
                assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            seal_application_migrator(maintenance, **authority, identity=identity)
            close_application_migrator_admission(maintenance, **authority, identity=identity)
            retire_application_migrator(maintenance, **authority, migrator_role=identity.role_name, migrator_oid=identity.role_oid)
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        observe_completed_application_authority(peer, target=target, runtime_password=args["password"])


@pytest.mark.asyncio
async def test_migration_recovery_waits_for_exact_prior_peer_and_never_adopts_unrecorded_role(transfer_database):  # noqa: F811
    from loom.application_migrator_provision import create_application_migrator
    from loom.application_migrator_recovery import (
        observe_application_migration_backend,
        observe_application_migrator_role,
        require_application_migration_peer_retired,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        with psycopg.connect(transfer_database[0], autocommit=True) as creator:
            backend = observe_application_migration_backend(creator, **authority, maintenance=False)
            with pytest.raises(RuntimeError, match="previous peer"):
                require_application_migration_peer_retired(maintenance, **authority, backend=backend)
        require_application_migration_peer_retired(maintenance, **authority, backend=backend)
        name = "app_migrator_" + uuid4().hex
        assert not observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=None)
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=lambda _: None)
        try:
            with pytest.raises(RuntimeError, match="unrecorded"):
                observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=None)
            assert observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)
        finally:
            peer.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))
        assert not observe_application_migrator_role(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)




@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("failure", ["journal", "readback", "guard"])
async def test_claim_compatibility_failure_keeps_replay_bound_to_original_function(transfer_database, failure):  # noqa: F811
    from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
            guard_owner=next(n for n, a in args["role_bindings"].items() if a == "guard-owner"))
        query = "SELECT pg_get_functiondef('loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)'::regprocedure)"
        before = peer.execute(query).fetchone()
        retained = []
        def persist(binding):
            retained.append(dict(binding))
            assert peer.execute(query).fetchone() == before
            if failure == "journal":
                raise RuntimeError("durable journal failed")
            if failure == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
        class LostReadback:
            def __getattr__(self, name):
                return getattr(peer, name)
            def execute(self, statement, *args, **kwargs):
                rendered = statement.as_string(peer) if isinstance(statement, sql.Composable) else statement
                if rendered.startswith("SELECT pg_catalog.pg_get_functiondef(oid),"):
                    raise RuntimeError("rewrite readback lost")
                return peer.execute(statement, *args, **kwargs)
        with pytest.raises(RuntimeError):
            ensure_guard_claim_compatibility(LostReadback() if failure == "readback" else peer,
                **authority, retained=None, persist=persist)
        assert peer.execute(query).fetchone() == before
        assert len(retained) == 1
        if failure != "guard":
            for _ in range(2):
                ensure_guard_claim_compatibility(peer, **authority, retained=retained[0],
                    persist=lambda _: pytest.fail("must not replace retained binding"))
            assert "guard_0033: refundable admission compatibility" in peer.execute(query).fetchone()[0]
        assert peer.execute("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version").fetchone() == ("guard_0030",)


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
@pytest.mark.parametrize("drift", ["owner", "security", "search-path", "identity", "digest", "malformed-digest"])
async def test_claim_compatibility_refuses_changed_private_authority(transfer_database, drift):  # noqa: F811
    from loom.application_guard_claim_compatibility import ensure_guard_claim_compatibility

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        authority = dict(target=args["target"], coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
            guard_owner=next(n for n, a in args["role_bindings"].items() if a == "guard-owner"))
        retained = []
        ensure_guard_claim_compatibility(peer, **authority, retained=None, persist=lambda value: retained.append(dict(value)))
        function = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
        if drift == "owner":
            authority["guard_owner"] = "unrelated_guard_owner"
        elif drift == "security":
            peer.execute(sql.SQL("ALTER FUNCTION " + function + " SECURITY INVOKER"))
        elif drift == "search-path":
            peer.execute(sql.SQL("ALTER FUNCTION " + function + " SET search_path=public"))
        elif drift == "identity":
            retained[0]["function_oid"] += 1
        elif drift == "digest":
            retained[0]["after_sha256"] = "0" * 64
        elif drift == "malformed-digest":
            retained[0]["before_sha256"] = []
        with pytest.raises(RuntimeError, match="guard compatibility"):
            ensure_guard_claim_compatibility(peer, **authority, retained=retained[0],
                persist=lambda _: pytest.fail("refusal must not rewrite saved authority"))
