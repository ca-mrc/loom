"""Preparation is visible before an execution lease exists."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete

from loom.db.schema import (
    ServiceExecutionClass,
    ServiceExecutionTarget,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_control_plane.service_execution import persist_execution_catalog
from tests.integration.test_service_execution_leases import NEBIUS_CPU_EXECUTION_CLASS_V1, _target
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401


async def test_shared_image_progress_and_filter_before_execution(trials_setup):  # noqa: F811
    app, token, _, ids = trials_setup
    image_id = uuid4()
    seed_id = ids[0]
    ids = [uuid4(), uuid4()]
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, seed_id)
        trial = Trial(id=ids[0], task_id=seed.task_id, team_id=seed.team_id,
                      state="queued", config=seed.config, requires_caps={"backend": "nebius"})
        session.add(trial)
        await session.flush()
        session.add(TaskImageMaterialization(
            id=image_id, materialization_key=uuid4().hex * 2, task_id=trial.task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={}, state="queued",
        ))
        await session.flush()
        other = Trial(id=ids[1], task_id=seed.task_id, team_id=seed.team_id,
                      state="queued", config=seed.config, requires_caps={"backend": "nebius"})
        session.add(other)
        await session.flush()
        session.add_all([TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image_id)
                         for trial_id in ids])
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc",
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            detail = await client.get(f"/api/v1/trials/{ids[0]}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["progress"]["stage"] == "image_preparation"
            listed = await client.get("/api/v1/trials", params={"state": "stage:image_preparation"})
            assert {item["id"] for item in listed.json()["items"]} == {str(i) for i in ids}
            assert listed.json()["items"][0]["progress"]["stage"] == "image_preparation"
            summary = await client.get("/api/v1/monitor/summary", params={"view": "trials"})
            assert summary.status_code == 200, summary.text
            assert summary.json()["progress"]["stages"]["image_preparation"] == 2
            assert summary.json()["progress"]["images"]["states"]["queued"] == 1
            assert summary.json()["queue"]["status"] != "blocked"
            async with app.state.session_factory() as session, session.begin():
                trial = await session.get(Trial, ids[0])
                trial.state = "cancelled"
            # A shared cache that remains queued cannot override terminal history.
            detail = await client.get(f"/api/v1/trials/{ids[0]}")
            assert detail.json()["progress"]["stage"] == "cancelled"
            async with app.state.session_factory() as session, session.begin():
                image = await session.get(TaskImageMaterialization, image_id)
                image.state = "ready"
            reused = await client.get(f"/api/v1/trials/{ids[1]}")
            assert reused.json()["progress"]["stage"] == "execution_wait"
            cancelled = await client.get(f"/api/v1/trials/{ids[0]}")
            assert cancelled.json()["progress"]["stage"] == "cancelled"
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.materialization_id == image_id))
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.id == image_id))
            await session.execute(delete(Trial).where(Trial.id.in_(ids)))


@pytest.mark.parametrize(("reason", "expected"), [
    ("execution_capacity_native_io_feedback_pending",
     "Waiting for disk I/O measurements from an earlier image build before admitting more work."),
    ("execution_capacity_bound_node_unavailable",
     "A reserved node cannot currently accept this workload. Waiting for capacity recovery."),
    ("execution_capacity_password=private",
     "Waiting for execution capacity; detailed admission diagnostics are unavailable."),
])
async def test_capacity_wait_reasons_reach_trial_detail_and_list(trials_setup, reason, expected):  # noqa: F811
    app, token, _, seed_ids = trials_setup
    image_id, trial_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    execution_class = NEBIUS_CPU_EXECUTION_CLASS_V1.model_copy(update={"class_id": "progress-" + uuid4().hex})
    target = _target(uuid4().hex[:12]).model_copy(update={"execution_class_id": execution_class.class_id})
    async with app.state.session_factory() as session, session.begin():
        await persist_execution_catalog(session, execution_class=execution_class, targets=(target,))
        seed = await session.get(Trial, seed_ids[0])
        trial = Trial(id=trial_id, task_id=seed.task_id, team_id=seed.team_id,
                      state="queued", config=seed.config, requires_caps={"backend": "nebius"},
                      scheduling_observation={"reason": reason, "observed_at": now.isoformat(),
                                              "message": "password=private", "node_name": "other-private-node"})
        session.add(trial)
        session.add(TaskImageMaterialization(
            id=image_id, materialization_key=uuid4().hex * 2, task_id=seed.task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={}, state="queued", lease_epoch=0,
        ))
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image_id))
        session.add(TaskImageCapacityWait(
            target_id=target.target_id, materialization_id=image_id, lease_epoch=0, pool_id=target.logical_pool_id,
            cpu_millis=1000, memory_mib=1024, storage_mib=2048, reason=reason,
            first_waited_at=now, renewed_at=now, expires_at=now + timedelta(seconds=120),
        ))
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc",
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            for stage in ("image_preparation", "execution_wait"):
                detail = await client.get(f"/api/v1/trials/{trial_id}")
                assert detail.status_code == 200, detail.text
                progress = detail.json()["progress"]
                assert progress["stage"] == stage and progress["wait_message"] == expected
                assert "private" not in str(progress)
                listed = await client.get("/api/v1/trials", params={"state": f"stage:{stage}"})
                assert listed.status_code == 200, listed.text
                item = next(item for item in listed.json()["items"] if item["id"] == str(trial_id))
                assert item["progress"]["wait_message"] == expected
                if stage == "image_preparation":
                    assert progress["detail"] == "capacity_wait"
                    # An earlier lease epoch must not explain the current build.
                    async with app.state.session_factory() as session, session.begin():
                        image = await session.get(TaskImageMaterialization, image_id)
                        image.lease_epoch = 1
                    stale = await client.get(f"/api/v1/trials/{trial_id}")
                    assert stale.json()["progress"]["detail"] == "queued"
                    assert stale.json()["progress"]["wait_message"] is None
                    async with app.state.session_factory() as session, session.begin():
                        image = await session.get(TaskImageMaterialization, image_id)
                        image.state = "ready"
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TaskImageCapacityWait).where(TaskImageCapacityWait.materialization_id == image_id))
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.materialization_id == image_id))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id == image_id))
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target.target_id))
            await session.execute(delete(ServiceExecutionClass).where(ServiceExecutionClass.id == execution_class.class_id))
