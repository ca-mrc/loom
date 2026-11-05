"""Completed work gets a released slot before renewable new-attempt heads."""

from copy import deepcopy
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, event, select

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_control_plane import execution_capacity_wait
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    admit_capacity_resources,
)
from loom_control_plane.execution_capacity_wait import ExecutionCapacityWait
from loom_control_plane.service_execution import set_execution_target_health
from loom_execution_capacity_collector.contracts import ResourceTotals
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_deferred_verifier_target_wait import (
    _reserve,
)
from tests.integration.test_deferred_verifier_target_wait import (
    independent_handoff as _independent_handoff,
)
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable database cleanup
)

independent_handoff = _independent_handoff


async def _released_slot(handoff, *, free_cpu=3000):
    h = handoff
    async with h.sessions() as session, session.begin():
        authorization = await session.scalar(select(ExecutionProvisioningAuthorization).where(
            ExecutionProvisioningAuthorization.lease_id == h.parent.id,
        ))
        if authorization.state != "released":
            authorization.state = "released"
            authorization.released_at = h.now
        policy = await session.get(ExecutionCapacityPolicy, h.target.target_id)
        policy.max_nodes = 1
        policy.max_create_per_minute = 100
        await set_execution_target_health(session, target_id=h.target.target_id,
            desired_state="active", observed_state="ready", health_status="healthy", observed_at=h.now)
        placement = placement_fixture(target_id=h.target.target_id, node_cpu=31900,
            requested_cpu=31900 - free_cpu, nodes=1, used_nodes=1, quota_nodes=1)
        await _record(session, h.target.target_id, h.now + timedelta(microseconds=1), placement)


async def test_verifier_uses_one_released_three_cpu_slot_before_older_attempt_head(
    independent_handoff, monkeypatch,
):
    h = independent_handoff
    await _released_slot(h)
    ordinary = ExecutionCapacityWait(uuid4(), h.target.target_id,
        ResourceTotals(cpu_millis=3000, memory_mib=6144, storage_mib=20480),
        h.now - timedelta(hours=2))

    async def validated_ordinary_head(*_args, **_kwargs):
        # Isolate the already validated ordinary head: the real PostgreSQL
        # reservation still renders the verifier and checks all hard limits.
        return [ordinary], None

    monkeypatch.setattr(execution_capacity_wait, "read_execution_capacity_waits", validated_ordinary_head)
    children = await _reserve(h)
    assert len(children) == 1, "an older new-agent hint must not consume the verifier's released slot"
    async with h.sessions() as session:
        cost = await session.scalar(select(ExecutionCostReservation).where(
            ExecutionCostReservation.lease_id == children[0].id,
        ))
        assert cost.requested_cpu_millis <= 3000
        trial = await session.get(Trial, h.trial_id)
        assert trial.state == "running"


async def test_repeated_capacity_denial_keeps_released_parent_age_and_exact_retry(independent_handoff):
    h = independent_handoff
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        first = dict(trial.scheduling_observation["capacity_wait"])
        assert first["execution_role"] == "verifier"
        assert first["parent_lease_id"] == str(h.parent.id)
        assert first["verifier_retry"] == 0
        assert first["first_waited_at"] == h.parent.deleted_at.isoformat()
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        assert len(heads) == 1 and heads[0].execution_role == "verifier"
    h.now += timedelta(seconds=10)
    assert await _reserve(h) == []
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        second = trial.scheduling_observation["capacity_wait"]
        assert second["first_waited_at"] == first["first_waited_at"]
        assert second["renewed_at"] != first["renewed_at"]
        assert second["expires_at"] > first["expires_at"]
        assert trial.result["verifier_execution"]["state"] == "pending"


@pytest.mark.parametrize("invalid", ["cancelled", "retry", "parent", "source", "target", "expired"])
async def test_ineligible_verifier_hint_does_not_hold_new_capacity(independent_handoff, invalid):
    h = independent_handoff
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    async with h.sessions() as session, session.begin():
        trial = await session.get(Trial, h.trial_id)
        assert trial.scheduling_observation["capacity_wait"]["execution_role"] == "verifier"
        if invalid == "cancelled":
            trial.cancellation_requested_at = h.now
        elif invalid == "retry":
            trial.result = {**trial.result, "verifier_execution": {
                **trial.result["verifier_execution"], "retries": 1, "pending_since": h.now.isoformat(),
            }}
        elif invalid == "parent":
            observation = dict(trial.scheduling_observation)
            observation["capacity_wait"] = {**observation["capacity_wait"], "parent_lease_id": str(uuid4())}
            trial.scheduling_observation = observation
        elif invalid == "source":
            trial.config = {**trial.config, "agent_timeout_multiplier": 2}
        elif invalid == "target":
            target = await session.get(ServiceExecutionTarget, h.target.target_id)
            target.desired_state = "disabled"
    at = h.now + timedelta(seconds=121) if invalid == "expired" else h.now
    async with h.sessions() as session:
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=at)
        assert not heads


async def test_verifier_wait_disappears_after_child_admission(independent_handoff):
    h = independent_handoff
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    h.now += timedelta(seconds=10)
    await _released_slot(h)
    children = await _reserve(h)
    assert len(children) == 1
    async with h.sessions() as session:
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        assert not heads, "a real child authorization must replace its parent's waiting hint"


async def test_new_attempt_yields_to_validated_verifier_head_when_slot_opens(independent_handoff):
    h = independent_handoff
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    h.now += timedelta(seconds=10)
    await _released_slot(h)
    with pytest.raises(ExecutionProvisioningBlockedError, match="max_nodes_exceeded"):
        async with h.sessions() as session, session.begin():
            target = await session.get(ServiceExecutionTarget, h.target.target_id)
            await admit_capacity_resources(session, target=target, demand_id="new-attempt",
                resources=ResourceTotals(cpu_millis=3000, memory_mib=1024, storage_mib=2048),
                current_time=h.now)
    assert len(await _reserve(h)) == 1


async def test_new_verifier_retry_renews_its_own_pending_age(independent_handoff):
    h = independent_handoff
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    h.now += timedelta(seconds=10)
    async with h.sessions() as session, session.begin():
        trial = await session.get(Trial, h.trial_id)
        trial.result = {**trial.result, "verifier_execution": {
            **trial.result["verifier_execution"], "retries": 1, "pending_since": h.now.isoformat(),
        }}
    assert await _reserve(h) == []
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        payload = trial.scheduling_observation["capacity_wait"]
        assert payload["verifier_retry"] == 1
        assert payload["first_waited_at"] == h.now.isoformat()
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        assert len(heads) == 1 and heads[0].verifier_retry == 1


async def _copy_waiting_verifier(session, handoff, *, deleted_at, invalid=None,
                                 trial_id=None, parent_id=None):
    """Copy a released disposable parent, never an executable authorization."""
    source = await session.get(Trial, handoff.trial_id)
    parent = await session.get(ServiceExecutionLease, handoff.parent.id)
    row = dict((await session.execute(execution_capacity_wait._verifier_candidate_rows().where(
        Trial.id == source.id,
    ))).mappings().one())
    trial_id, parent_id = trial_id or uuid4(), parent_id or uuid4()
    trial_values = {column.name: deepcopy(getattr(source, column.name))
                    for column in Trial.__table__.columns}
    parent_values = {column.name: deepcopy(getattr(parent, column.name))
                     for column in ServiceExecutionLease.__table__.columns}
    handoff_result = {'state': 'pending', 'parent_lease_id': str(parent_id)}
    row.update(id=trial_id, verifier_parent_id=parent_id, verifier_handoff=handoff_result,
               verifier_deleted_at=deleted_at)
    payload = {**source.scheduling_observation['capacity_wait'], 'parent_lease_id': str(parent_id),
        'verifier_retry': 0, 'first_waited_at': deleted_at.isoformat(),
        'input_fingerprint': execution_capacity_wait.execution_wait_fingerprint(row)}
    if invalid == 'stale':
        payload['input_fingerprint'] = 'stale'
    elif invalid == 'malformed':
        payload['first_waited_at'] = 'invalid timestamp'
    trial_values.update(id=trial_id, idempotency_key=str(uuid4()),
        result={**source.result, 'verifier_execution': handoff_result},
        scheduling_observation={**source.scheduling_observation, 'capacity_wait': payload})
    parent_values.update(id=parent_id, trial_id=trial_id, request_id=uuid4(),
        provider_scope_key=uuid4().hex, job_name=uuid4().hex, execution_unit_key=uuid4(),
        deleted_at=deleted_at)
    session.add(Trial(**trial_values))
    await session.flush()
    session.add(ServiceExecutionLease(**parent_values))
    for link in (await session.scalars(select(TrialTaskImageMaterialization).where(
        TrialTaskImageMaterialization.trial_id == source.id,
    ))).all():
        session.add(TrialTaskImageMaterialization(trial_id=trial_id,
            materialization_id=link.materialization_id))
    await session.flush()
    handoff.backlog_trial_ids.append(trial_id)
    return trial_id


@pytest.fixture
async def verifier_backlog(independent_handoff):
    h = independent_handoff
    h.backlog_trial_ids = []
    try:
        yield h
    finally:
        # The owner fixture deletes its image materializations before the
        # generic lease/trial cleanup; remove these extra references first.
        async with h.sessions() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.trial_id.in_(h.backlog_trial_ids),
            ))


async def test_verifier_head_read_queries_stay_bounded_with_large_valid_backlog(verifier_backlog):
    h = verifier_backlog
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    async with h.sessions() as session, session.begin():
        for index in range(128):
            await _copy_waiting_verifier(session, h,
                deleted_at=h.parent.deleted_at + timedelta(seconds=index + 1))
        assert len((await session.execute(execution_capacity_wait._verifier_candidate_rows())).all()) == 129
    statements = []

    def capture(_conn, _cursor, statement, _parameters, _context, _many):
        if statement.startswith('SELECT'):
            statements.append(statement)

    async with h.sessions() as session, session.begin():
        from loom_control_plane.execution_capacity import _CAPACITY_ADMISSION_LOCK

        await session.execute(_CAPACITY_ADMISSION_LOCK)
        event.listen(session.bind.sync_engine, 'before_cursor_execute', capture)
        try:
            heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        finally:
            event.remove(session.bind.sync_engine, 'before_cursor_execute', capture)
    assert len(heads) == 1 and heads[0].trial_id == h.trial_id
    assert len(statements) <= 24, 'one target head must not validate every younger verifier under the capacity lock'


async def test_verifier_head_order_uses_retry_age_and_skips_invalid_prefix(verifier_backlog):
    h = verifier_backlog
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    async with h.sessions() as session, session.begin():
        for index in range(105):
            await _copy_waiting_verifier(session, h,
                deleted_at=h.parent.deleted_at - timedelta(seconds=index + 1),
                invalid='stale' if index else 'malformed')
        older_waiter = await _copy_waiting_verifier(session, h,
            deleted_at=h.parent.deleted_at + timedelta(minutes=10))
        trial = await session.get(Trial, h.trial_id)
        trial.result = {**trial.result, 'verifier_execution': {
            **trial.result['verifier_execution'], 'retries': 1,
            'pending_since': (h.now - timedelta(seconds=10)).isoformat(),
        }}
    assert await _reserve(h) == []
    async with h.sessions() as session:
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        assert len(heads) == 1 and heads[0].trial_id == older_waiter
        assert heads[0].first_waited_at == h.parent.deleted_at + timedelta(minutes=10)


async def test_equal_age_verifier_head_uses_parent_identity_tiebreaker(verifier_backlog):
    h = verifier_backlog
    await _released_slot(h, free_cpu=0)
    assert await _reserve(h) == []
    async with h.sessions() as session, session.begin():
        first_parent = await _copy_waiting_verifier(session, h, deleted_at=h.parent.deleted_at,
            trial_id=UUID(int=2**128 - 1), parent_id=UUID(int=1))
    async with h.sessions() as session:
        heads, _ = await execution_capacity_wait.read_execution_capacity_waits(session, now=h.now)
        assert len(heads) == 1 and heads[0].trial_id == first_parent

