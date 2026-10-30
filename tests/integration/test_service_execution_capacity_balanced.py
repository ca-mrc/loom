"""Opt-in mixed-region placement preserves the real reservation transaction."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionProvisioningAuthorization,
    ServiceExecutionLease,
)
from loom_control_plane.execution_admission import upsert_execution_admission_policy
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_nebius_task_image_capacity import native_setup as _native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.integration.test_service_execution_regions import _observe
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING

native_setup = _native_setup


@pytest.mark.parametrize("case", ["legacy", "balanced", "released_capacity", "stale_primary", "global_limit"])
async def test_two_healthy_regions_mix_only_when_enabled_and_preserve_global_limit(postgres_url, monkeypatch, case):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    original = fixtures._target
    try:
        async with sessions() as session, session.begin():
            first = await fixtures._seed_ready_trial(session, now=now)
            with monkeypatch.context() as patch:
                patch.setattr(fixtures, "_target", lambda suffix: original(suffix).model_copy(update={
                    "region": "eu-west1", "health_role": "secondary", "failure_domain": "west", "cluster_scope_id": "west-cluster",
                }))
                second = await fixtures._seed_ready_trial(session, now=now)
            for trial_id, _target in (first, second):
                await fixtures._configure_scheduler_trial(session, trial_id=trial_id, now=now)
            await _observe(session, first[1], now + timedelta(seconds=1), quota=20)
            await _observe(session, second[1], now + timedelta(seconds=19), quota=20)
            if case == "stale_primary":
                from loom.db.schema import ExecutionCapacityPolicy

                policy = await session.get(ExecutionCapacityPolicy, first[1].target_id)
                policy.observation_max_age_seconds = 10
            if case == "global_limit":
                await upsert_execution_admission_policy(session, scope_kind="global", scope_key="*",
                    max_concurrent=1, enabled=True, reason="bounded test", now=now)
        chosen = []
        for offset in (20, 21):
            async with sessions() as session, session.begin():
                lease = await reserve_next_service_execution(session, environment="staging", pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=offset),
                    placement_policy="environment-local-health-first" if case == "legacy" else "environment-local-capacity-balanced")
                if lease is not None:
                    chosen.append(lease.target_id)
                    if case == "released_capacity":
                        # Once physical authority is released, its retained
                        # dispatch time must still break the next idle tie.
                        authorization = await session.scalar(select(ExecutionProvisioningAuthorization).where(
                            ExecutionProvisioningAuthorization.lease_id == lease.id))
                        authorization.state = "released"
                        authorization.released_at = now + timedelta(seconds=offset)
        if case in {"balanced", "released_capacity"}:
            assert set(chosen) == {first[1].target_id, second[1].target_id}
        elif case == "global_limit":
            assert len(chosen) == 1
        else:
            assert chosen == [first[1].target_id if case == "legacy" else second[1].target_id] * 2
        async with sessions() as session:
            own = select(ServiceExecutionLease.id).where(ServiceExecutionLease.trial_id.in_([first[0], second[0]]))
            assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease).where(
                ServiceExecutionLease.id.in_(own))) == len(chosen)
            assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization).where(
                ExecutionProvisioningAuthorization.lease_id.in_(own))) == len(chosen)
            assert await session.scalar(select(func.count()).select_from(ExecutionAdmissionReservation).where(
                ExecutionAdmissionReservation.owner_id.in_(own))) == len(chosen)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("load_kind", ["guest", "native", "released_native"])
async def test_ordering_reads_guest_and_native_owner_occupancy_without_reserving(native_setup, monkeypatch, load_kind):
    from loom.db.schema import ServiceExecutionTarget, TaskImageMaterializationAttempt
    from loom_control_plane.execution_capacity_ordering import order_targets_by_capacity
    from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
    from tests.integration.test_execution_shared_capacity import _family, _guest_reserve
    from tests.integration.test_nebius_task_image_capacity import _native

    sessions, owned = native_setup
    now = datetime.now(UTC)
    original = fixtures._target
    async with sessions() as session, session.begin():
        north, guest, _placement = await _family(session, now)
        with monkeypatch.context() as patch:
            patch.setattr(fixtures, "_target", lambda suffix: original(suffix).model_copy(update={
                "region": "eu-west1", "health_role": "secondary", "failure_domain": "west", "cluster_scope_id": "west-cluster",
            }))
            west = await fixtures._seed_ready_trial(session, now=now)
        await _observe(session, west[1], now + timedelta(seconds=1), quota=20)
        if load_kind == "guest":
            await _guest_reserve(session, guest, now + timedelta(seconds=2))
        else:
            attempt_id, _ = await _native(session, owned, north, now, cpu=8000)
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
            if load_kind == "released_native":
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
                attempt.native_build = {**attempt.native_build,
                    "capacity_reserved_at": (now - timedelta(minutes=3)).isoformat(),
                    "capacity_released_at": (now - timedelta(minutes=2)).isoformat()}
        await session.flush()
        before = await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization))
        targets = [await session.get(ServiceExecutionTarget, pair[1].target_id) for pair in (north, west)]
        ordered = await order_targets_by_capacity(session, targets, now=now + timedelta(seconds=3))
        assert ordered[0].id == west[1].target_id
        assert await session.scalar(select(func.count()).select_from(ExecutionProvisioningAuthorization)) == before
        assert not session.new and not session.dirty
