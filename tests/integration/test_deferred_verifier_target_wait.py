"""Real PG deferred grading waits for fresh target health without losing output."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionAdmissionReservation,
    ExecutionCostReservation,
    ServiceExecutionClass,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    TaskImageMaterialization,
    Trial,
)
from loom.execution_contract import ExecutionTargetV1
from loom_control_plane import service_execution_scheduler as scheduler
from loom_control_plane.service_execution import set_execution_target_health
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_image_readiness import (
    TASK_IMAGE,
    _clean_image_links,
    _seed_preparing_trial,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- disposable PG cleanup
)
from tests.integration.test_service_execution_regions import _observe
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


@pytest.fixture
async def independent_handoff(postgres_url: str, monkeypatch: pytest.MonkeyPatch):
    async def committed_files(*_args, **_kwargs):
        # Supply the object-store receipt only; compile, frozen image grant,
        # target health, admission and reservation use the real database.
        return [("artifacts/workspace.tar", 1, "sha256:" + "5" * 64)], ["owned/workspace.tar"]

    monkeypatch.setattr(scheduler, "committed_handoff_files", committed_files)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    started = now - timedelta(minutes=45)
    trial_id = None
    execution_class_id = None
    try:
        async with sessions() as session:
            trial_id, images = await _seed_preparing_trial(
                session, now=started, state="queued", independent_verifier=True,
            )
            image = await session.get(TaskImageMaterialization, images["x86_64"])
            image.state = "ready"
            image.registry_images = {
                "task": TASK_IMAGE, "verifier": "registry.example/grader@sha256:" + "e" * 64,
            }
            await session.commit()
            parent = await scheduler.reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=started,
            )
            assert parent is not None
            execution_class_id = parent.execution_class_id
            await fixtures._hand_off_to_verifier(session, parent_id=parent.id, now=started)
            parent.cleanup_state = "complete"
            parent.cleanup_requested_at = started
            parent.cleanup_deadline_at = started + timedelta(seconds=30)
            parent.desired_state = "deleted"
            parent.observed_state = "deleted"
            parent.deleted_at = started
            target = await session.get(ServiceExecutionTarget, parent.target_id)
            target_spec = ExecutionTargetV1.model_validate(target.spec_json)
            # Capacity is current and sufficient; only the target health is old.
            await _observe(session, target_spec, now, quota=20)
            await session.commit()
        yield SimpleNamespace(
            sessions=sessions, trial_id=trial_id, parent=parent, target=target_spec, now=now,
        )
    finally:
        async with sessions() as session:
            if execution_class_id is not None:
                execution_class = await session.get(ServiceExecutionClass, execution_class_id)
                if execution_class is not None:
                    execution_class.enabled = True
                    await session.commit()
            await _clean_image_links(session, trial_id)
        await engine.dispose()


async def _reserve(handoff):
    async with handoff.sessions() as session:
        result = await scheduler.reserve_next_verifier_executions(
            session, pool_id=handoff.target.logical_pool_id,
            image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=handoff.now,
            parent_lease_id=handoff.parent.id,
        )
        await session.commit()
        return result


async def _assert_no_verifier_reservation(handoff):
    async with handoff.sessions() as session:
        children = select(ServiceExecutionLease.id).where(
            ServiceExecutionLease.parent_lease_id == handoff.parent.id,
        )
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease).where(
            ServiceExecutionLease.id.in_(children),
        )) == 0
        assert await session.scalar(select(func.count()).select_from(ExecutionCostReservation).where(
            ExecutionCostReservation.trial_id == handoff.trial_id,
            ExecutionCostReservation.execution_role == "verifier",
        )) == 0
        assert await session.scalar(select(func.count()).select_from(ExecutionAdmissionReservation).where(
            ExecutionAdmissionReservation.owner_id.in_(children),
        )) == 0


@pytest.mark.parametrize("readiness", ["stale", "unhealthy", "not_ready", "unobserved"])
async def test_old_independent_handoff_waits_for_health_then_reserves_exactly_once(
    independent_handoff, readiness: str,
):
    h = independent_handoff
    async with h.sessions() as session:
        target = await session.get(ServiceExecutionTarget, h.target.target_id)
        if readiness == "unhealthy":
            target.health_status = "unhealthy"
        elif readiness == "not_ready":
            target.observed_state = "unknown"
        elif readiness == "unobserved":
            target.health_status = "unknown"
            target.health_observed_at = None
        await session.commit()
    for _ in range(2):
        assert await _reserve(h) == []
        async with h.sessions() as session:
            trial = await session.get(Trial, h.trial_id)
            assert trial.state == "running"
            assert trial.failure_reason is None
            assert trial.result["verifier_execution"]["state"] == "pending"
            parent = await session.get(ServiceExecutionLease, h.parent.id)
            assert parent.output_commit_state == "committed"
            assert parent.output_manifest_sha256 == h.parent.output_manifest_sha256
            target = await session.get(ServiceExecutionTarget, h.target.target_id)
            assert target.spec_json["health_stale_after_seconds"] == h.target.health_stale_after_seconds
            if readiness == "stale":
                assert target.health_observed_at + timedelta(
                    seconds=target.spec_json["health_stale_after_seconds"],
                ) <= h.now
        await _assert_no_verifier_reservation(h)

    async with h.sessions() as session:
        await set_execution_target_health(
            session, target_id=h.target.target_id, desired_state="active",
            observed_state="ready", health_status="healthy", observed_at=h.now,
        )
        await session.commit()
    # Replay/concurrent passes still create just one child and one reservation.
    passes = await asyncio.gather(_reserve(h), _reserve(h))
    children = [child for result in passes for child in result]
    assert len(children) == 1
    assert children[0].parent_lease_id == h.parent.id
    assert await _reserve(h) == []
    async with h.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease).where(
            ServiceExecutionLease.parent_lease_id == h.parent.id,
        )) == 1
        assert await session.scalar(select(func.count()).select_from(ExecutionCostReservation).where(
            ExecutionCostReservation.lease_id == children[0].id,
        )) == 1
        assert await session.scalar(select(func.count()).select_from(ExecutionAdmissionReservation).where(
            ExecutionAdmissionReservation.owner_id == children[0].id,
        )) == 1


@pytest.mark.parametrize("invalid", ["disabled", "class_disabled", "region", "residency"])
async def test_old_handoff_without_compatible_configuration_still_fails(
    independent_handoff, monkeypatch: pytest.MonkeyPatch, invalid: str,
):
    h = independent_handoff
    async with h.sessions() as session:
        target = await session.get(ServiceExecutionTarget, h.target.target_id)
        if invalid == "residency":
            # Catalog targets are constrained to EU. Exercise an incompatible
            # typed workload requirement without inventing an invalid DB row.
            project = scheduler.workload_requirements_from_task
            monkeypatch.setattr(scheduler, "workload_requirements_from_task", lambda *args, **kwargs:
                project(*args, **kwargs).model_copy(update={"data_residency": "ca"}))
        elif invalid == "class_disabled":
            execution_class = await session.get(ServiceExecutionClass, target.execution_class_id)
            execution_class.enabled = False
        else:
            target.desired_state = "disabled"
            if invalid == "region":
                _, foreign = await fixtures._seed_ready_trial(session, now=h.now)
                alternative = await session.get(ServiceExecutionTarget, foreign.target_id)
                alternative.region = "different-region"
        await session.commit()
    assert await _reserve(h) == []
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        assert trial.state == "failed"
        assert trial.failure_reason == "verifier_unavailable"
        assert trial.result["verifier_execution"]["state"] == "unavailable"
    await _assert_no_verifier_reservation(h)


async def test_cancelled_old_handoff_does_not_resume_after_health_refresh(independent_handoff):
    h = independent_handoff
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        trial.cancellation_requested_at = h.now
        await set_execution_target_health(
            session, target_id=h.target.target_id, desired_state="active",
            observed_state="ready", health_status="healthy", observed_at=h.now,
        )
        await session.commit()
    assert await _reserve(h) == []
    await _assert_no_verifier_reservation(h)


async def test_invalid_workspace_does_not_become_a_target_health_wait(
    independent_handoff, monkeypatch: pytest.MonkeyPatch,
):
    h = independent_handoff

    async def missing_archive(*_args, **_kwargs):
        return [], []

    monkeypatch.setattr(scheduler, "committed_handoff_files", missing_archive)
    assert await _reserve(h) == []
    async with h.sessions() as session:
        trial = await session.get(Trial, h.trial_id)
        assert trial.state == "failed"
        assert trial.failure_reason == "verifier_unavailable"
        assert "verifier handoff archive was not committed" in trial.failure_message
    await _assert_no_verifier_reservation(h)
