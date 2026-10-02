"""Exercise the real actuator command/trigger path with its bootstrap DB role."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import (
    ExecutionAdmissionPolicy,
    ExecutionAdmissionReservation,
    ExecutionBudgetPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    TeamQuota,
    Trial,
)
from loom_control_plane.execution_admission import upsert_execution_admission_policy
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_execution_actuator.contracts import NormalizedJobState
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401
from tests.integration.test_service_execution_leases import (
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


@pytest.fixture
def global_role_database(platform_database):  # noqa: F811 -- imported shared fixture
    from tests.integration.conftest import _isolated_migration_database

    url = make_url(platform_database).set(drivername="postgresql+psycopg").render_as_string(hide_password=False)
    for database in _isolated_migration_database(url, template_name="template0", prepare_template=False):
        yield make_url(database).set(drivername="postgresql").render_as_string(hide_password=False)


@pytest.mark.parametrize("kind", ["execution", "build", "registered-build"])
async def test_global_handoff_runs_as_restricted_actuator_after_guarded_role_stage(
    global_role_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    kind: str,
) -> None:
    from scripts.ops import nebius_pool_migration_guard as migration

    from loom.nebius_rollout_guard import acquire, release
    from tests.integration.test_nebius_pool_build_outbox import grant as build_grant
    from tests.integration.test_nebius_pool_build_outbox import local_setup, outbox
    from tests.integration.test_nebius_pool_execution_outbox import grant as execution_grant
    from tests.integration.test_nebius_pool_execution_outbox import setup
    from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: global_role_database)
    monkeypatch.setenv("LOOM_DB_URL", global_role_database)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", "restricted-role-password-" + "a" * 32)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    url = make_url(global_role_database).set(drivername="postgresql+psycopg")
    admin = create_async_engine(url)
    worker = create_async_engine(url.set(username="loom_actuator", password="restricted-role-password-" + "a" * 32))
    scheduler = create_async_engine(url.set(username="loom_control_plane", password="restricted-role-password-" + "a" * 32))
    owners = async_sessionmaker(admin, expire_on_commit=False)
    actuators = async_sessionmaker(worker, expire_on_commit=False)
    schedulers = async_sessionmaker(scheduler, expire_on_commit=False)
    owner, candidate = str(uuid4()), "a" * 40
    try:
        with psycopg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False), autocommit=True) as db:
            for held_owner, held_candidate in ((None, None), (str(uuid4()), candidate), (owner, "f" * 40)):
                if held_owner is not None:
                    db.execute("INSERT INTO nebius_rollout_guard(id,owner,candidate_sha) VALUES(1,%s,%s)",
                        (held_owner, held_candidate))
                with pytest.raises(psycopg.errors.RaiseException, match="guard unqualified"):
                    db.execute(migration.pool_runtime_role_sql(owner=owner, candidate=candidate, action="stage"), prepare=False)
                db.rollback()
                assert db.execute("SELECT has_table_privilege('loom_actuator','nebius_pool_execution_outbox','INSERT')").fetchone() == (False,)
                db.execute("DELETE FROM nebius_rollout_guard")
        async with owners.begin() as session:
            assert (await acquire(session, owner=owner, candidate=candidate))["status"] == "acquired"
        with psycopg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False), autocommit=True) as db:
            if kind == "build":
                db.execute("DELETE FROM alembic_version")
                with pytest.raises(psycopg.errors.RaiseException, match="not closed and idle"):
                    db.execute(migration.pool_runtime_role_sql(owner=owner, candidate=candidate, action="stage"), prepare=False)
                db.rollback()
                db.execute("INSERT INTO alembic_version(version_num) VALUES('0173')")
            with db.cursor() as cursor:
                cursor.execute(migration.pool_runtime_role_sql(owner=owner, candidate=candidate, action="stage"), prepare=False)
                reports = []
                while True:
                    if cursor.description:
                        reports.extend(cursor.fetchall())
                    if not cursor.nextset():
                        break
            assert reports == [({"status": "staged"},)]
            if kind == "execution":
                db.execute("GRANT UPDATE(mode) ON nebius_pool_bindings TO loom_actuator")
                for action in ("observe", "stage"):
                    with pytest.raises(psycopg.errors.RaiseException, match="management authority unqualified"):
                        db.execute(migration.pool_runtime_role_sql(owner=owner, candidate=candidate, action=action), prepare=False)
                    db.rollback()
                db.execute("REVOKE UPDATE(mode) ON nebius_pool_bindings FROM loom_actuator")
            if kind == "registered-build":
                db.execute("REVOKE INSERT ON task_bundle_source_references FROM loom_actuator")
                db.execute("GRANT INSERT(source_id) ON task_bundle_source_references TO loom_actuator")
                with pytest.raises(psycopg.errors.RaiseException, match="source journal authority unqualified"):
                    db.execute(migration.pool_runtime_role_sql(owner=owner, candidate=candidate, action="observe"), prepare=False)
                db.rollback()
                db.execute("REVOKE INSERT(source_id) ON task_bundle_source_references FROM loom_actuator")
                db.execute("GRANT INSERT ON task_bundle_source_references TO loom_actuator")
        async with owners.begin() as session:
            await release(session, owner=owner, candidate=candidate)
        if kind == "execution":
            journal, trial_id, target = await setup(owners)
            journal.sessions = schedulers
            proposal = await journal.propose(trial_id=trial_id, target_id=target.target_id)
            receipt = execution_grant(proposal)
            attached = await journal.accept_grant(proposal.request.key, receipt)
            assert attached.phase == "attached"
            from loom_execution_actuator.pool_execution_outbox import PoolExecutionOutbox

            journal = PoolExecutionOutbox(sessions=actuators, participant=journal.participant,
                environment="staging", logical_pool_id="nebius-cpu", image_admission_keyring=IMAGE_ADMISSION_KEYRING)
            key = proposal.request.key
        else:
            participant, request, trial_id = await local_setup(owners)
            journal = outbox(actuators, participant)
            if kind == "registered-build":
                from sqlalchemy import update

                from loom.db.schema import Task, TrialTaskImageMaterialization
                from loom.task_image_materialization import ensure_task_image_materializations
                from tests.integration.test_nebius_pool_build_selection import selector
                from tests.integration.test_task_bundle_source_journal import (
                    _module,
                    _publish,
                    _receipts,
                    _spec,
                    _upload,
                )

                spec = _spec(tmp_path)
                ticket = await _upload(owners, spec)
                await _receipts(owners, ticket)
                await _publish(owners, ticket)
                async with owners.begin() as session:
                    task = Task(id=spec.catalog_task_id, checksum=spec.manifest.task_checksum,
                        config=spec.task_config, source=spec.source_uri, source_provenance=spec.provenance)
                    session.add(task)
                    await session.flush()
                    image = (await ensure_task_image_materializations(session, task_row=task))[0]
                    await session.execute(update(Trial).where(Trial.id == trial_id).values(task_id=spec.catalog_task_id))
                    session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image.id))
                    await session.flush()
                    await _module().release_task_bundle_reference(session, source_id=spec.id,
                        reference_kind="materialization", owner_id=str(image.id))
                saved = await selector(journal).select_next()
                assert saved is not None and saved.request.build.source.kind == "registered"
                request = saved.request
            else:
                assert (await journal.remember(request)).phase == "selected"
            receipt = build_grant(request)
            attached = await journal.accept_grant(request.key, receipt)
            assert attached.phase == "attached"
            key = request.key
        pending = await journal.begin_activation(key)
        assert pending.phase == "activation_pending" and pending.activation is not None
        active = await journal.confirm_activation(key, receipt.model_copy(update={
            "phase": "create_intent", "plan_sha256": "d" * 64}))
        assert active.phase == "active"
        assert await journal.get(key) == active
        # The runtime can lock its source but cannot alter task content, batch
        # identity/origin, credentials, global authority, or erase either journal.
        with psycopg.connect(url.set(drivername="postgresql", username="loom_actuator",
                password="restricted-role-password-" + "a" * 32).render_as_string(hide_password=False)) as db:
            assert db.execute("SELECT current_user").fetchone() == ("loom_actuator",)
            db.commit()
            for statement in (
                "UPDATE tasks SET config='{}'::jsonb",
                "UPDATE batches SET service_execution_runtime_profile='{}'::jsonb",
                "DELETE FROM nebius_pool_execution_outbox",
                "DELETE FROM nebius_pool_build_outbox",
                "UPDATE nebius_pool_bindings SET mode='global'",
                "SELECT * FROM nebius_pool_machine_credentials",
                "UPDATE task_bundle_sources SET spec_json='{}'::jsonb",
                "UPDATE task_bundle_source_incarnations SET state='retired'",
                "INSERT INTO task_bundle_source_writes DEFAULT VALUES",
                "DELETE FROM task_bundle_source_references",
                "DELETE FROM tokens",
                "CREATE ROLE global_actuator_escape",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    db.execute(statement)
                db.rollback()
    finally:
        await worker.dispose()
        await scheduler.dispose()
        await admin.dispose()


@pytest.mark.parametrize("infra_retry", [False, True])
async def test_restricted_actuator_creates_reconciles_and_releases_without_policy_authority(
    platform_database: str,  # noqa: F811 -- imported shared pytest fixture
    monkeypatch: pytest.MonkeyPatch,
    infra_retry: bool,
) -> None:
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", "restricted-role-password-" + "a" * 32)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    url = make_url(platform_database).set(drivername="postgresql+psycopg")
    admin_engine = create_async_engine(url)
    actuator_engine = create_async_engine(
        url.set(username="loom_actuator", password="restricted-role-password-" + "a" * 32)
    )
    control_plane_engine = create_async_engine(
        url.set(username="loom_control_plane", password="restricted-role-password-" + "a" * 32)
    )
    control_plane = async_sessionmaker(control_plane_engine, expire_on_commit=False)
    admins = async_sessionmaker(admin_engine, expire_on_commit=False)
    restricted = async_sessionmaker(actuator_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    try:
        # Setup uses the owner; all actuator commands and reconciliation below
        # connect as the real restricted login, including invoker-rights triggers.
        async with admins() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(TeamQuota(team_id=trial.team_id))
            await upsert_execution_admission_policy(
                session,
                scope_kind="pool",
                scope_key="nebius-cpu",
                max_concurrent=4,
                enabled=True,
                reason="restricted-role test",
                now=now,
            )
            await session.commit()
        # Scheduling now reserves capacity before publishing the create command;
        # exercise that transaction using the real Control Plane login as well.
        async with control_plane() as session:
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()
        async with restricted() as session:
            assert (
                await session.execute(text("SELECT current_user"))
            ).scalar_one() == "loom_actuator"
        actuator = ExecutionActuator(
            sessions=restricted,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id,
                namespace=target.namespace_name,
                runtime_class_name="loom-sandbox",
            ),
            controller_id="restricted-actuator",
        )
        assert await actuator.run_commands_once(now=now) == 1
        assert kubernetes.create_count == 1
        assert await actuator.run_commands_once(now=now + timedelta(seconds=1)) == 0
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=1)) == 0
        async with admins() as session:
            authorization = (
                await session.execute(
                    select(ExecutionProvisioningAuthorization).where(
                        ExecutionProvisioningAuthorization.lease_id == lease.id
                    )
                )
            ).scalar_one()
            assert authorization.state == "pending"
            policy = (await session.execute(select(ExecutionAdmissionPolicy))).scalar_one()
            assert policy.active_count == 1
            budgets = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(budgets) == 2 and all(row.daily_reserved_microusd > 0 for row in budgets)
            if not infra_retry:
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="cancel",
                    now=now + timedelta(seconds=2),
                )
            await session.commit()
        if infra_retry:
            kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
                update={
                    "normalized_state": NormalizedJobState.UNSCHEDULABLE,
                    "resource_version": "2",
                }
            )
            await actuator.reconcile_full_once(now=lease.deadline_at)
            async with admins() as session:
                retry_trial = await session.get(Trial, trial_id)
                assert retry_trial is not None and retry_trial.state == "queued"
                assert retry_trial.attempt_count == 1 and retry_trial.failure_reason is None
                assert retry_trial.next_attempt_at == lease.deadline_at + timedelta(seconds=15)
            now = lease.deadline_at + timedelta(minutes=5)
        assert await actuator.run_commands_once(now=now + timedelta(seconds=2)) == 1
        assert kubernetes.delete_count == 1
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=3)) == 1
        assert await actuator.reconcile_full_once(now=now + timedelta(seconds=4)) == 0
        async with admins() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None and persisted.observed_state == "deleted"
            assert persisted.cleanup_state == "complete"
            assert (
                await session.execute(select(ExecutionAdmissionPolicy.active_count))
            ).scalar_one() == 0
            assert (
                await session.execute(
                    select(ExecutionAdmissionReservation.state).where(
                        ExecutionAdmissionReservation.trial_id == trial_id
                    )
                )
            ).scalar_one() == "released"
            assert (
                await session.execute(
                    select(ExecutionCostReservation.state).where(
                        ExecutionCostReservation.lease_id == lease.id
                    )
                )
            ).scalar_one() == "released"
            budgets = (
                (
                    await session.execute(
                        select(ExecutionBudgetPolicy).where(
                            ExecutionBudgetPolicy.scope_key.in_(
                                (target.logical_pool_id, target.target_id)
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert all(
                row.daily_reserved_microusd == row.monthly_reserved_microusd == 0 for row in budgets
            )
        # Row locks and trigger counters do not grant authority to rewrite
        # policy settings, identities, prices, observations or credentials.
        restricted_url = url.set(
            drivername="postgresql",
            username="loom_actuator",
            password="restricted-role-password-" + "a" * 32,
        )
        with psycopg.connect(restricted_url.render_as_string(hide_password=False)) as connection:
            # The restricted actuator itself completes cancellation and releases
            # the counter; an infrastructure retry remains queued and also releases it.
            assert connection.execute(
                "SELECT in_flight_count FROM team_quotas WHERE team_id=%s", (trial.team_id,)
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT state FROM trials WHERE id=%s", (trial_id,)
            ).fetchone() == ("queued" if infra_retry else "cancelled",)
            connection.commit()
            for statement in (
                "UPDATE execution_capacity_policies SET max_nodes=21",
                "UPDATE execution_capacity_policies SET enabled=false",
                "UPDATE execution_admission_policies SET max_concurrent=99",
                "UPDATE execution_admission_policies SET scope_key='other-pool'",
                "UPDATE execution_budget_policies SET daily_limit_microusd=1",
                "UPDATE execution_budget_policies SET emergency_stop=false",
                "UPDATE execution_target_price_bindings SET enabled=false",
                "UPDATE execution_price_snapshots SET base_microusd_per_hour=0",
                "UPDATE execution_capacity_observations SET provider_quota_nodes=999",
                "DELETE FROM execution_capacity_policies",
                "DELETE FROM tokens",
                "DELETE FROM users",
                "UPDATE team_quotas SET max_attempts_ceiling=99",
                "UPDATE team_quotas SET team_id='11111111-1111-1111-1111-111111111111'",
                "CREATE ROLE actuator_escape",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    connection.execute(statement)
                connection.rollback()
    finally:
        await actuator_engine.dispose()
        await control_plane_engine.dispose()
        await admin_engine.dispose()
