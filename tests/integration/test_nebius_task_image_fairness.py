"""Waiting native builds keep their place across real capacity transactions."""

import asyncio
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from time import perf_counter
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import event, func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Task,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    Trial,
)
from loom.execution_contract import ExecutionRoutingReason
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    reserve_execution_provisioning,
)
from loom_control_plane.execution_capacity_wait import read_execution_capacity_waits
from loom_control_plane.service_execution import (
    _bind_kubernetes_execution_route,
    set_execution_target_health,
)
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_controller import (
    NativeTaskImageController,
    NativeTaskImageSettings,
)
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_nebius_task_image_claims import _seed, claim_setup  # noqa: F401
from tests.integration.test_nebius_task_image_controller import (
    FakeKube,
    rows,
    seed_image,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _configure_scheduler_trial,
    _requirements,
    _reserve,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


def _healthy_placement(*, sampled_at: datetime | None = None, **kwargs: Any) -> dict[str, Any]:
    """This file models healthy nodes; preserve each test's later damage."""
    placement = placement_fixture(**kwargs)
    stamp = (sampled_at or datetime.now(UTC)).isoformat()
    for node in placement["nodes"]:
        node.update(name=node["uid"], draining=False,
                    io_pressure={"sampled_at": stamp, "full_avg10": 0.0})
    return placement


@pytest.fixture
async def native_build_setup(claim_setup):  # noqa: F811
    sessions, team_id = claim_setup
    kube = FakeKube()
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        trial_id, target = await _seed_ready_trial(session, now=now)
        occupied = _healthy_placement(
            target_id=target.target_id, nodes=1, used_nodes=1,
            quota_nodes=1, requested_cpu=64_000,
        )
        await _record(session, target.target_id, now + timedelta(milliseconds=1), occupied)
    controller = NativeTaskImageController(
        sessions=sessions, kubernetes=kube,
        target=ExecutionTargetRuntime(target_id=target.target_id, namespace="executions"),
        settings=NativeTaskImageSettings(
            namespace="test-builds", service_image="registry.example/service@sha256:" + "b" * 64,
            storage_endpoint="https://storage.example", storage_region="eu-north1", source_bucket="tasks",
            registry_repository="registry.example/tasks", registry_auth_kind="docker-config",
            cpu_millis=64_000,
        ),
    )
    image_id, builder_trial_id = await seed_image(sessions, team_id)
    return controller, sessions, kube, image_id, builder_trial_id, trial_id, target, now


@pytest.fixture
async def waiting_build(native_build_setup):
    controller, sessions, kube, image_id, _, _, target, _ = native_build_setup
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 0, 0)
    assert not attempts and kube.ensure_calls == 0
    async with sessions() as session:
        wait = await session.get(TaskImageCapacityWait, target.target_id)
        assert wait is not None and wait.materialization_id == image_id
        assert wait.expires_at - wait.renewed_at == timedelta(seconds=120)
    return native_build_setup


async def _free(sessions, target, now, *, quota_nodes=1):
    async with sessions() as session, session.begin():
        free = _healthy_placement(
            target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=quota_nodes,
            sampled_at=now,
        )
        await _record(session, target.target_id, now + timedelta(seconds=1), free)


async def _schedule_ready_execution(sessions, at):
    async with sessions() as session, session.begin():
        return await reserve_next_service_execution(session, environment="staging", pool_id="nebius-cpu",
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=at)


@pytest.fixture
async def waiting_execution(native_build_setup):
    _, sessions, _, _, _, trial_id, _, now = native_build_setup
    async with sessions() as session, session.begin():
        await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
    assert await _schedule_ready_execution(sessions, now) is None
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        assert trial.state == "queued" and trial.attempt_count == 0
        assert trial.scheduling_observation["reason"].startswith("execution_capacity_")
    return native_build_setup


async def test_new_trial_cannot_take_last_capacity_a_waiting_builder_needs(waiting_build):
    controller, sessions, kube, image_id, _, trial_id, target, now = waiting_build
    # The occupied allocation drains. A trial arrives before the next builder
    # reconcile: serialization alone lets it jump ahead on every iteration.
    await _free(sessions, target, now)
    for index in range(3):
        with pytest.raises(ExecutionProvisioningBlockedError):
            async with sessions() as session, session.begin():
                await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2 + index))

    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state in {"claimed", "running"}
    assert row.attempt_count == len(attempts) == kube.ensure_calls == 1
    assert attempts[0].native_build["capacity_reserved_at"]
    async with sessions() as session:
        assert await session.get(TaskImageCapacityWait, target.target_id) is None


async def test_execution_waiting_before_successive_build_heads_gets_released_capacity(native_build_setup):
    controller, sessions, kube, image_id, builder_trial_id, trial_id, target, now = native_build_setup
    async with sessions() as session, session.begin():
        await _configure_scheduler_trial(session, trial_id=trial_id, now=now)
        builder_team_id = (await session.get(Trial, builder_trial_id)).team_id

    async def schedule(at):
        async with sessions() as session, session.begin():
            return await reserve_next_service_execution(session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=at)

    assert await schedule(now) is None
    async with sessions() as session:
        queued = await session.get(Trial, trial_id)
        assert queued.state == "queued" and queued.attempt_count == 0
        assert queued.scheduling_observation["reason"].startswith("execution_capacity_")
        first_wait = datetime.fromisoformat(queued.scheduling_observation["observed_at"])
        for model in (ServiceExecutionLease, ExecutionCostReservation):
            assert await session.scalar(select(func.count()).select_from(model).where(model.trial_id == trial_id)) == 0

    lease = None
    for index in range(2):
        await controller.run_once()
        async with sessions() as session:
            wait = await session.get(TaskImageCapacityWait, target.target_id)
            assert wait is not None and wait.materialization_id == image_id
            assert wait.first_waited_at > first_wait
        await _free(sessions, target, now + timedelta(seconds=index * 20))
        lease = await schedule(now + timedelta(seconds=(index + 1) * 20))
        if lease is not None:
            break
        # A later build must not acquire and replenish the only headroom ahead
        # of this older, ready execution on every capacity-release cycle.
        await controller.run_once()
        _, attempts = await rows(sessions, image_id)
        assert len(attempts) == 1 and attempts[0].native_build["capacity_reserved_at"]
        async with sessions() as session, session.begin():
            (await session.get(Trial, builder_trial_id)).cancellation_requested_at = datetime.now(UTC)
        await controller._reconcile(attempts[0].id)
        _, attempts = await rows(sessions, image_id)
        assert attempts[0].native_build["capacity_released_at"] and not kube.jobs
        if index == 0:
            image_id, builder_trial_id = await seed_image(sessions, builder_team_id)
            async with sessions() as session, session.begin():
                await _record(session, target.target_id, now + timedelta(seconds=20), _healthy_placement(
                    target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1, requested_cpu=64000))
    assert lease is not None, "younger native build heads repeatedly bypassed the waiting execution"
    assert lease.trial_id == trial_id and lease.attempt == 1


@pytest.mark.parametrize("invalidated", ["cancelled", "input", "route", "attempt", "expired",
                                      "target_environment", "target_region", "parent_cancelled", "parent_finished"])
async def test_invalid_execution_wait_cannot_hold_builder_capacity(waiting_execution, invalidated):
    controller, sessions, _, _, _, trial_id, target, now = waiting_execution
    async with sessions() as session, session.begin():
        trial = await session.get(Trial, trial_id)
        if invalidated == "cancelled":
            trial.cancellation_requested_at = now
        elif invalidated.startswith("parent_"):
            (await session.get(Batch, trial.batch_id)).state = invalidated.removeprefix("parent_")
        elif invalidated == "input":
            (await session.get(Task, trial.task_id)).checksum = "f" * 64
        elif invalidated == "route":
            _bind_kubernetes_execution_route(trial=trial,
                target=await session.get(ServiceExecutionTarget, target.target_id),
                execution_class_id=target.execution_class_id,
                requirements_digest=canonical_digest(_requirements().model_dump(mode="json")),
                routing_reason=ExecutionRoutingReason.ADMIN_TARGET_BINDING, current_time=now)
        elif invalidated == "attempt":
            trial.attempt_count += 1
        elif invalidated in {"target_environment", "target_region"}:
            persisted_target = await session.get(ServiceExecutionTarget, target.target_id)
            if invalidated == "target_environment":
                persisted_target.environment = "development"
            else:
                persisted_target.region = "eu-west1"
    await _free(sessions, target, now)
    claim_at = now + timedelta(seconds=121 if invalidated == "expired" else 2)
    async with sessions() as session, session.begin():
        if invalidated == "expired":
            await set_execution_target_health(session, target_id=target.target_id,
                desired_state="active", observed_state="ready", health_status="healthy", observed_at=claim_at)
            # This case advances the explicit admission clock by121s. Model an
            # ordinary fresh collector epoch at that same clock, independently
            # of the deliberately expired execution-wait hint.
            await _record(session, target.target_id, claim_at, _healthy_placement(
                target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1,
                sampled_at=claim_at,
            ))
        # Use the real native claim/render/admission path with an explicit
        # clock so expiry needs no sleeps or patched production clock.
        attempt = await controller._prepare_claim(session)
        assert attempt is not None
        native = await reserve_native_task_image_capacity(session, attempt_id=attempt.id,
            now=claim_at)
        assert native["capacity_reserved_at"]


async def test_released_capacity_remains_admissible_after_slow_fixture_work(waiting_execution, monkeypatch):
    """Slow SQL/setup must not put the test's collector ahead of admission."""
    *_, now = waiting_execution

    class SlowWallClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (now + timedelta(seconds=30)).astimezone(tz)

    monkeypatch.setitem(globals(), "datetime", SlowWallClock)
    await test_invalid_execution_wait_cannot_hold_builder_capacity(waiting_execution, "parent_finished")


async def test_actual_spare_quota_admits_build_and_waiting_execution(waiting_execution):
    controller, sessions, _, image_id, _, trial_id, target, now = waiting_execution
    await _free(sessions, target, now, quota_nodes=2)
    await controller.run_once()
    _, attempts = await rows(sessions, image_id)
    assert len(attempts) == 1 and attempts[0].native_build["capacity_reserved_at"]
    lease = await _schedule_ready_execution(sessions, now + timedelta(seconds=20))
    assert lease is not None and lease.trial_id == trial_id


async def test_only_one_execution_waiting_head_per_target_is_projected(waiting_execution):
    controller, sessions, _, image_id, _, trial_id, target, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        another_id = uuid4()
        session.add(Trial(id=another_id, team_id=original.team_id, task_id=original.task_id,
            batch_id=original.batch_id, config=original.config, requires_caps=original.requires_caps,
            state="queued", attempt_count=0))
    assert await _schedule_ready_execution(sessions, now + timedelta(milliseconds=1)) is None
    async with sessions() as session:
        another = await session.get(Trial, another_id)
        assert another.scheduling_observation["reason"].startswith("execution_capacity_")
    # The real 63 CPU build and one 1 CPU execution head fit this 64 CPU
    # allocation. Projecting every queued execution would incorrectly deny it.
    controller.settings = controller.settings.model_copy(update={"cpu_millis": 63000})
    await _free(sessions, target, now)
    await controller.run_once()
    _, attempts = await rows(sessions, image_id)
    assert len(attempts) == 1 and attempts[0].native_build["capacity_reserved_at"]


async def _copy_execution_wait(session, original, *, first=None, trial_id=None, **changes):
    observation = deepcopy(original.scheduling_observation)
    if first is not None:
        observation["capacity_wait"]["first_waited_at"] = first
    observation["capacity_wait"].update(changes)
    trial = Trial(id=trial_id or uuid4(), team_id=original.team_id, task_id=original.task_id,
        batch_id=original.batch_id, config=original.config, requires_caps=original.requires_caps,
        state="queued", attempt_count=0, next_attempt_at=original.next_attempt_at,
        scheduling_observation=observation)
    session.add(trial)
    await session.flush()
    return trial


async def _copy_many_waits(session, original, count):
    await session.execute(insert(Trial), [{
        "id": uuid4(), "team_id": original.team_id, "task_id": original.task_id,
        "batch_id": original.batch_id, "config": original.config, "requires_caps": original.requires_caps,
        "state": "queued", "attempt_count": 0, "next_attempt_at": original.next_attempt_at,
        "scheduling_observation": original.scheduling_observation,
    } for _ in range(count)])


async def test_execution_head_reader_bounds_backlog_payloads(waiting_execution):
    _, sessions, _, _, _, trial_id, _, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        await _copy_many_waits(session, original, 999)
    decoded = 0

    def decode(value):
        nonlocal decoded
        result = json.loads(value)
        if isinstance(result, dict) and "capacity_wait" in result:
            decoded += 1
        return result

    # Count actual driver JSON deserialization, not a particular SQL spelling.
    engine = create_async_engine(sessions.kw["bind"].url, json_deserializer=decode)
    try:
        async with async_sessionmaker(engine)() as session:
            heads, _ = await read_execution_capacity_waits(session, now=now + timedelta(seconds=1))
        assert len(heads) == 1
        assert decoded <= 100, "younger backlog payloads crossed the database boundary after one head was sufficient"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("invalid", ["expired", "malformed", "fingerprint"])
async def test_execution_head_reader_batches_invalid_backlog(waiting_execution, invalid, record_property):
    _, sessions, _, _, _, trial_id, _, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        observation = deepcopy(original.scheduling_observation)
        if invalid == "expired":
            observation["capacity_wait"]["expires_at"] = (now - timedelta(seconds=1)).isoformat()
        elif invalid == "malformed":
            observation["capacity_wait"]["first_waited_at"] = "malformed"
        else:
            observation["capacity_wait"]["input_fingerprint"] = "stale"
        original.scheduling_observation = observation
        await _copy_many_waits(session, original, 9999)
    decoded = statements = 0

    def decode(value):
        nonlocal decoded
        result = json.loads(value)
        if isinstance(result, dict) and "capacity_wait" in result:
            decoded += 1
        return result

    def count(*args):
        nonlocal statements
        statements += 1

    engine = create_async_engine(sessions.kw["bind"].url, json_deserializer=decode)
    event.listen(engine.sync_engine, "before_cursor_execute", count)
    try:
        started = perf_counter()
        async with async_sessionmaker(engine)() as session:
            heads, _ = await read_execution_capacity_waits(session, now=now + timedelta(seconds=1))
        record_property("reader_budget", json.dumps({"population": invalid, "hints": 10000,
            "elapsed_ms": round((perf_counter() - started) * 1000, 3),
            "sql_events": statements, "decoded_wait_payloads": decoded}))
        assert heads == []
        if invalid == "expired":
            assert statements <= 2 and decoded == 0
        else:
            assert statements <= 205, "invalid hints caused a database round trip per queued Trial"
            assert decoded == (10000 if invalid == "fingerprint" else 0)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("invalid", ["expired", "first", "expiry", "fingerprint", "cancelled", "resources",
                                    "null_target", "list_target", "number_target"])
async def test_execution_head_reader_advances_past_invalid_oldest(waiting_execution, invalid):
    _, sessions, _, _, _, trial_id, _, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        younger = await _copy_execution_wait(session, original)
        observation = deepcopy(original.scheduling_observation)
        hint = observation["capacity_wait"]
        hint["first_waited_at"] = (now - timedelta(seconds=2)).isoformat()
        if invalid == "expired":
            hint["expires_at"] = (now - timedelta(seconds=1)).isoformat()
        elif invalid == "first":
            hint["first_waited_at"] = "2026-99-99T25:90:90+00:00"
        elif invalid == "expiry":
            hint["expires_at"] = "malformed"
        elif invalid == "fingerprint":
            hint["input_fingerprint"] = "stale"
        elif invalid == "cancelled":
            original.cancellation_requested_at = now
        elif invalid.endswith("_target"):
            hint["target_id"] = {"null_target": None, "list_target": [], "number_target": 3}[invalid]
        else:
            hint["resources"] = {**hint["resources"], "cpu_millis": 99999999}
        original.scheduling_observation = observation
        expected = younger.id
    async with sessions() as session:
        heads, _ = await read_execution_capacity_waits(session, now=now + timedelta(seconds=1))
    assert [head.trial_id for head in heads] == [expected]


@pytest.mark.parametrize("format", ["offset", "python_iso", "tie"])
async def test_execution_head_reader_uses_actual_age_and_uuid_ties(waiting_execution, format):
    _, sessions, _, _, _, trial_id, _, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        earlier = now - timedelta(seconds=1)
        if format == "offset":
            first = earlier.astimezone(timezone(timedelta(hours=5))).isoformat()
        elif format == "python_iso":
            # fromisoformat accepts a separator PostgreSQL does not parse.
            first = earlier.isoformat().replace("T", "X")
            assert await session.scalar(select(func.pg_input_is_valid(first, "timestamptz"))) is False
        else:
            first = original.scheduling_observation["capacity_wait"]["first_waited_at"]
        another = await _copy_execution_wait(session, original, first=first)
        expected = min(original.id, another.id) if format == "tie" else another.id
    async with sessions() as session:
        heads, _ = await read_execution_capacity_waits(session, now=now + timedelta(seconds=1))
    assert [head.trial_id for head in heads] == [expected]


async def test_execution_head_reader_compares_entire_python_only_prefix(waiting_execution):
    _, sessions, _, _, _, trial_id, _, now = waiting_execution
    # Reverse actual age relative to UUID order across the100-row page boundary.
    ids = sorted(uuid4() for _ in range(101))
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        for index, current in enumerate(ids):
            first = (now - timedelta(seconds=1, microseconds=index)).isoformat().replace("T", "X")
            await _copy_execution_wait(session, original, first=first, trial_id=current)
    async with sessions() as session:
        heads, _ = await read_execution_capacity_waits(session, now=now + timedelta(seconds=1))
    assert [head.trial_id for head in heads] == [ids[-1]]


async def test_execution_head_reader_retains_nonhead_claim_order(waiting_execution, monkeypatch):
    _, sessions, _, _, _, trial_id, target, now = waiting_execution
    async with sessions() as session, session.begin():
        original = await session.get(Trial, trial_id)
        older = await _copy_execution_wait(session, original, first=(now - timedelta(seconds=1)).isoformat())
        older_id = older.id
        own_first = datetime.fromisoformat(original.scheduling_observation["capacity_wait"]["first_waited_at"])
    await _free(sessions, target, now, quota_nodes=2)
    import loom_control_plane.execution_capacity_wait as waits
    observed = []

    async def read(*args, **kwargs):
        result = await read_execution_capacity_waits(*args, **kwargs)
        observed.append(result)
        return result

    monkeypatch.setattr(waits, "read_execution_capacity_waits", read)
    async with sessions() as session, session.begin():
        lease = await reserve_next_service_execution(session, environment="staging", pool_id="nebius-cpu",
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=30))
        assert lease is not None and lease.trial_id == trial_id
    assert any([head.trial_id for head in heads] == [older_id]
               and own == (own_first, f"execution-wait:{trial_id}") for heads, own in observed)


async def test_execution_wait_renewal_keeps_first_age_and_protects_matching_backoff(waiting_execution):
    controller, sessions, kube, image_id, _, trial_id, target, now = waiting_execution
    async with sessions() as session:
        first = (await session.get(Trial, trial_id)).scheduling_observation["capacity_wait"]["first_waited_at"]
    await controller.run_once()  # The builder starts waiting after the execution.
    assert await _schedule_ready_execution(sessions, now + timedelta(seconds=15)) is None
    async with sessions() as session:
        trial = await session.get(Trial, trial_id)
        assert trial.scheduling_observation["capacity_wait"]["first_waited_at"] == first
        assert trial.next_attempt_at == now + timedelta(seconds=30)
    await _free(sessions, target, now + timedelta(seconds=15))
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.attempt_count == 0 and not attempts and kube.ensure_calls == 0
    lease = await _schedule_ready_execution(sessions, now + timedelta(seconds=30))
    assert lease is not None and lease.trial_id == trial_id


@pytest.mark.parametrize("invalidated", ["expired", "cancelled", "epoch", "shape", "pool"])
async def test_abandoned_or_invalid_wait_cannot_hold_trial_capacity(waiting_build, invalidated):
    _, sessions, _, image_id, builder_trial_id, trial_id, target, now = waiting_build
    await _free(sessions, target, now)
    async with sessions() as session, session.begin():
        if invalidated == "cancelled":
            (await session.get(Trial, builder_trial_id)).cancellation_requested_at = now
        elif invalidated == "epoch":
            (await session.get(TaskImageMaterialization, image_id)).lease_epoch += 1
        elif invalidated == "shape":
            (await session.get(ExecutionCapacityPolicy, target.target_id)).node_cpu_millis = 63_000
        elif invalidated == "pool":
            (await session.get(ServiceExecutionTarget, target.target_id)).logical_pool_id = "unrelated-pool"
    claim_time = now + timedelta(seconds=130 if invalidated == "expired" else 2)
    async with sessions() as session, session.begin():
        if invalidated == "expired":
            await set_execution_target_health(
                session, target_id=target.target_id, desired_state="active", observed_state="ready",
                health_status="healthy", observed_at=claim_time,
            )
        assert await _reserve(session, trial_id=trial_id, target=target, now=claim_time)


async def test_wait_renewal_preserves_order_without_attempts_or_creates(waiting_build):
    controller, sessions, kube, image_id, _, _, target, _ = waiting_build
    async with sessions() as session:
        original = await session.get(TaskImageCapacityWait, target.target_id)
        first, expiry = original.first_waited_at, original.expires_at
    await controller.run_once()
    async with sessions() as session:
        renewed = await session.get(TaskImageCapacityWait, target.target_id)
        assert renewed.first_waited_at == first and renewed.expires_at >= expiry
        assert not (await session.scalars(select(ExecutionProvisioningAuthorization)
                    .where(ExecutionProvisioningAuthorization.target_id == target.target_id))).all()
    row, attempts = await rows(sessions, image_id)
    assert row.attempt_count == row.lease_epoch == kube.ensure_calls == 0 and not attempts


async def test_real_spare_headroom_can_admit_trial_without_displacing_wait(waiting_build):
    controller, sessions, _, _, _, trial_id, target, now = waiting_build
    await _free(sessions, target, now, quota_nodes=2)
    async with sessions() as session, session.begin():
        lease = await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2))
        authorization = await session.scalar(select(ExecutionProvisioningAuthorization)
                                             .where(ExecutionProvisioningAuthorization.lease_id == lease.id))
        assert authorization.incremental_nodes == 0
        assert authorization.decision_reason == "existing_allocatable"
    # An already reserved operation can still start if a later fence exists.
    # Revalidation must not treat waiting state as prior acquired authority.
    async with sessions() as session, session.begin():
        assert await reserve_execution_provisioning(
            session, lease_id=lease.id, now=now + timedelta(seconds=3), revalidate_existing=True,
        )
    await controller.run_once()


async def test_wait_reuses_compatible_native_allocatable_history(waiting_build):
    _, sessions, _, _, _, trial_id, target, now = waiting_build
    async with sessions() as session, session.begin():
        current = _healthy_placement(
            target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1,
        )
        current["template_samples"] = []
        await _record(session, target.target_id, now + timedelta(seconds=1), current)
    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target,
                           now=now + timedelta(seconds=2))


@pytest.mark.parametrize("occupant", [
    "managed", "foreign", "duplicate", "overdrawn", "unready", "unschedulable", "deleting",
])
async def test_wait_can_use_observed_managed_drain_without_cold_sample(native_build_setup, occupant):
    controller, sessions, kube, _, _, trial_id, target, now = native_build_setup
    occupied = _healthy_placement(target_id=target.target_id, nodes=1, used_nodes=1,
                                 quota_nodes=1, requested_cpu=64_000)
    occupied["template_samples"] = []
    occupied["node_group"]["template"]["preset"] = "new-template-without-sample"
    node = occupied["nodes"][0]
    node["used_pod_slots"] = 1
    if occupant != "foreign":
        node["managed_pods"] = [{"uid": "occupied-pod", "lease_id": "occupied-lease", "generation": 1,
                                 "requests": node["requested"].copy()}]
    if occupant == "duplicate":
        node["managed_pods"] *= 2
        node["used_pod_slots"] = 2
    elif occupant == "overdrawn":
        node["managed_pods"][0]["requests"]["cpu_millis"] += 1
    elif occupant == "unready":
        node["ready"] = False
    elif occupant in {"unschedulable", "deleting"}:
        node[occupant] = True
    async with sessions() as session, session.begin():
        await _record(session, target.target_id, now + timedelta(milliseconds=2), occupied)
    await controller.run_once()
    async with sessions() as session:
        wait = await session.get(TaskImageCapacityWait, target.target_id)
        assert (wait is not None) == (occupant == "managed")
    if occupant != "managed":
        return  # No evidence that unknown resident/DaemonSet overhead can drain.
    free = _healthy_placement(target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1)
    free["template_samples"] = []
    free["node_group"]["template"]["preset"] = "new-template-without-sample"
    async with sessions() as session, session.begin():
        await _record(session, target.target_id, now + timedelta(seconds=1), free)
    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2))
    await controller.run_once()
    assert kube.ensure_calls == 1


@pytest.mark.parametrize("domain", ["shared", "independent", "shared-disk", "impossible-policy"])
async def test_wait_protects_shared_native_quota_but_not_an_independent_domain(waiting_build, domain):
    _, sessions, _, _, _, _, first, now = waiting_build
    async with sessions() as session, session.begin():
        trial_id, second = await _seed_ready_trial(session, now=now)
        for target in (first, second):
            placement = _healthy_placement(
                target_id=target.target_id, nodes=0, used_nodes=0, quota_nodes=1,
                parent_id="wait-shared" if domain in {"shared", "impossible-policy"} else target.target_id,
            )
            if domain == "shared-disk":
                placement["quota_resources"]["storage"]["parent_id"] = "wait-shared-ssd"
            if domain == "impossible-policy" and target == first:
                placement["node_group"]["raw_node"]["cpu_millis"] = 128_000
            await _record(session, target.target_id, now + timedelta(seconds=1), placement)
        if domain == "impossible-policy":
            (await session.get(ExecutionCapacityPolicy, first.target_id)).max_vcpu_millis = 64_000
    if domain in {"shared", "shared-disk"}:
        with pytest.raises(ExecutionProvisioningBlockedError, match="provider_quota"):
            async with sessions() as session, session.begin():
                await _reserve(session, trial_id=trial_id, target=second,
                               now=now + timedelta(seconds=2))
    else:
        async with sessions() as session, session.begin():
            assert await _reserve(session, trial_id=trial_id, target=second,
                                  now=now + timedelta(seconds=2))


@pytest.mark.parametrize("limit", ["max_pending_jobs", "max_create_per_minute"])
async def test_wait_protects_a_scarce_admission_slot_without_consuming_it(waiting_build, limit):
    controller, sessions, kube, _, _, trial_id, target, now = waiting_build
    await _free(sessions, target, now, quota_nodes=2)
    async with sessions() as session, session.begin():
        setattr(await session.get(ExecutionCapacityPolicy, target.target_id), limit, 1)
    with pytest.raises(ExecutionProvisioningBlockedError, match=r"pending_limit|create_rate"):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2))
    await controller.run_once()
    assert kube.ensure_calls == 1


async def test_existing_trial_authority_precedes_a_later_wait(native_build_setup):
    controller, sessions, _, _, _, trial_id, target, now = native_build_setup
    await _free(sessions, target, now)
    async with sessions() as session, session.begin():
        lease = await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2))
    await controller.run_once()
    async with sessions() as session, session.begin():
        assert await session.get(TaskImageCapacityWait, target.target_id) is not None
        assert await reserve_execution_provisioning(
            session, lease_id=lease.id, now=now + timedelta(seconds=3), revalidate_existing=True,
        )


@pytest.mark.parametrize("admitted", [False, True])
async def test_failed_outer_commit_keeps_claim_and_fence_atomic(waiting_build, admitted):
    controller, sessions, kube, image_id, _, _, target, now = waiting_build
    if admitted:
        await _free(sessions, target, now)
    async with sessions() as session:
        before = await session.get(TaskImageCapacityWait, target.target_id)
        original = (before.first_waited_at, before.renewed_at, before.expires_at)

    def refuse_outer_commit(session):
        if not session.in_nested_transaction():
            raise RuntimeError("simulated outer commit failure")

    class RefuseCommitSession(AsyncSession):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            event.listen(self.sync_session, "before_commit", refuse_outer_commit)

    controller.sessions = async_sessionmaker(
        sessions.kw["bind"], class_=RefuseCommitSession, expire_on_commit=False,
    )
    with pytest.raises(RuntimeError, match="simulated outer commit failure"):
        await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "queued" and row.lease_epoch == row.attempt_count == 0
    assert not attempts and kube.ensure_calls == 0
    async with sessions() as session:
        after = await session.get(TaskImageCapacityWait, target.target_id)
        assert (after.first_waited_at, after.renewed_at, after.expires_at) == original


async def test_concurrent_controller_replicas_consume_one_wait_once(waiting_build):
    controller, sessions, kube, image_id, _, _, target, now = waiting_build
    await _free(sessions, target, now)
    await asyncio.gather(controller.run_once(), controller.run_once())
    row, attempts = await rows(sessions, image_id)
    assert row.attempt_count == len(attempts) == kube.ensure_calls == 1


@pytest.fixture
async def two_waiting_builds(waiting_build):
    first, sessions, _, image_id, _, _, target, now = waiting_build
    async with sessions() as session, session.begin():
        _, second_target = await _seed_ready_trial(session, now=now)
        image = await session.get(TaskImageMaterialization, image_id)
        first_trial = await session.scalar(select(Trial).where(Trial.task_id == image.task_id))
        second_image, _ = await _seed(
            session, first_trial.team_id,
            snapshot_values={
                "task_config": image.task_config,
                "task_source": image.task_source,
            },
        )
        for current in (target, second_target):
            occupied = _healthy_placement(
                target_id=current.target_id, nodes=0, used_nodes=1, quota_nodes=1,
                parent_id="ordered-waits",
            )
            await _record(session, current.target_id, now + timedelta(milliseconds=2), occupied)
    second_kube = FakeKube()
    second = NativeTaskImageController(
        sessions=sessions, kubernetes=second_kube,
        target=ExecutionTargetRuntime(target_id=second_target.target_id, namespace="executions"),
        settings=first.settings,
    )
    # Both targets serve the supported automatic pool. Model another claimant
    # holding the oldest row: SKIP LOCKED lets the second controller consider
    # the younger build without inventing an unsupported Dockerfile binding.
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    async with sessions() as session, session.begin():
        assert await session.get(TaskImageCapacityWait, second_target.target_id) is not None
        for current in (target, second_target):
            free = _healthy_placement(target_id=current.target_id, nodes=0, used_nodes=0,
                                     quota_nodes=1, parent_id="ordered-waits")
            await _record(session, current.target_id, now + timedelta(seconds=1), free)
    return waiting_build, second, second_kube, second_image, second_target


async def test_older_waiting_builder_wins_without_mutual_wait(two_waiting_builds):
    waiting_build, second, second_kube, second_image, _ = two_waiting_builds
    first, sessions, first_kube, image_id, builder_trial_id, _, _, now = waiting_build
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    assert second_kube.ensure_calls == 0
    await first.run_once()
    assert first_kube.ensure_calls == 1
    assert (await rows(sessions, second_image))[0].attempt_count == 0
    # The younger builder progresses after actual UID-fenced cleanup releases
    # the older reservation, not merely after its materialization is cancelled.
    async with sessions() as session, session.begin():
        (await session.get(Trial, builder_trial_id)).cancellation_requested_at = now
    _, attempts = await rows(sessions, image_id)
    await first._reconcile(attempts[0].id)
    assert first_kube.delete_calls and not first_kube.jobs
    await second.run_once()
    assert second_kube.ensure_calls == 1
    assert (await rows(sessions, second_image))[0].attempt_count == 1


@pytest.mark.parametrize("changed", ["epoch", "resources"])
async def test_changed_claim_cannot_inherit_an_old_waiting_priority(two_waiting_builds, changed):
    waiting_build, second, second_kube, _, second_target = two_waiting_builds
    first, sessions, first_kube, image_id, _, _, target, _ = waiting_build
    if changed == "epoch":
        async with sessions() as session, session.begin():
            (await session.get(TaskImageMaterialization, image_id)).lease_epoch += 1
    else:
        first.settings = first.settings.model_copy(update={"cpu_millis": 63_000})
    await first.run_once()
    assert first_kube.ensure_calls == 0
    async with sessions() as session:
        renewed = await session.get(TaskImageCapacityWait, target.target_id)
        other = await session.get(TaskImageCapacityWait, second_target.target_id)
        assert renewed.first_waited_at > other.first_waited_at
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    assert second_kube.ensure_calls == 1


async def test_admission_pass_starts_one_build_per_node_until_its_io_feedback(native_build_setup, monkeypatch):
    # #2701: batching leaves I/O pacing with the native capacity path. A node
    # with an unobserved new start cannot take another in the same pass.
    controller, sessions, kube, image_id, builder_trial_id, _, target, now = native_build_setup
    controller.settings = controller.settings.model_copy(update={"cpu_millis": 1000})
    async with sessions() as session:
        team_id = (await session.get(Trial, builder_trial_id)).team_id
    second_id, _ = await seed_image(sessions, team_id)
    await _free(sessions, target, now)
    cycles = []
    monkeypatch.setattr("loom_execution_actuator.task_image_controller.record_build_cycle",
                        lambda controller, **cycle: cycles.append(cycle))
    await controller.run_once()
    # A blocked, validated head becomes the target's wait rather than an error.
    assert (cycles[-1]["claimed"], cycles[-1]["outcome"]) == (1, "idle")
    assert kube.ensure_calls == 1
    states = {image: (await rows(sessions, image))[0].state for image in (image_id, second_id)}
    assert sorted(states.values()) == ["queued", "running"]
    async with sessions() as session:
        wait = await session.get(TaskImageCapacityWait, target.target_id)
        assert wait is not None and states[wait.materialization_id] == "queued"
        assert wait.reason == "execution_capacity_native_io_feedback_pending"
