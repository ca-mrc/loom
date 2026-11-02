"""Pipeline counts and filters share current attempt/verifier and tenant authority."""
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import VerifierTopology
from loom_control_plane.task_image_materializations import retry_task_image_materialization
from loom_service.monitor_filters import apply_trial_monitor_filters
from loom_service.trial_progress import load_trial_progress, progress_summary
from tests.integration import test_service_execution_leases as execution
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401


async def test_image_stage_counts_drilldown_pagination_and_tenant_scope(trials_setup):  # noqa: F811
    app, token, team_id, seed_ids = trials_setup
    images = [uuid4(), uuid4()]
    own_ids = [uuid4(), uuid4(), uuid4()]
    foreign_id, foreign_team = uuid4(), uuid4()
    now = datetime.now(UTC)
    batch_id = uuid4()
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, seed_ids[0])
        session.add(Team(id=foreign_team, name="private-other-team"))
        session.add(Batch(id=batch_id, team_id=team_id, name="pipeline named batch", task_filter={},
                          trial_config={}, expected_trial_count=3, created_by_token_prefix="test", state="running"))
        await session.flush()
        session.add_all([Trial(id=trial_id, team_id=team_id, task_id=seed.task_id, batch_id=batch_id,
                               config=seed.config, requires_caps={"backend": "nebius"}, state="queued")
                         for trial_id in own_ids])
        session.add(Trial(id=foreign_id, team_id=foreign_team, task_id=seed.task_id,
                          config=seed.config, requires_caps={}, state="queued"))
        session.add_all([TaskImageMaterialization(id=image_id, materialization_key=uuid4().hex*2,
            task_id=seed.task_id, task_checksum=checksum*64, cpu_arch="x86_64", task_config={},
            state=state, attempt_count=attempts, lease_epoch=epoch, failure_reason=reason)
            for image_id, checksum, state, attempts, epoch, reason in [
                (images[0], "a", "running", 1, 1, None),
                (images[1], "b", "queued", 1, 1, "build_publish_failed"),
            ]])
        await session.flush()
        session.add(TaskImageMaterializationAttempt(materialization_id=images[0], attempt_number=1,
            lease_epoch=1, builder_id="pipeline-builder", claimed_at=now-timedelta(seconds=50),
            native_build={"observed_at": now.isoformat(), "phases": [
                {"name": "build", "state": {"running": {"startedAt": (now-timedelta(seconds=20)).isoformat()}}},
                {"name": "publish", "state": {"waiting": {"reason": "PodInitializing"}}}],
                "configmap": {"data": {"claim.json": "secret-not-for-api"}},
                "job": {"private_endpoint": "secret-not-for-api"}}))
        session.add_all([TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=image_id)
                        for trial_id, image_id in [(own_ids[0], images[0]), (own_ids[1], images[0]),
                                                  (own_ids[2], images[1]), (foreign_id, images[0])]])
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc",
                headers={"Authorization": f"Bearer {token}"}) as client:
            summary = await client.get("/api/v1/monitor/summary", params={"view": "trials", "batch_id": str(batch_id)})
            assert summary.status_code == 200, summary.text
            progress = summary.json()["progress"]
            assert progress["stages"]["image_preparation"] == 3
            assert progress["images"]["image_count"] == 2
            assert progress["images"]["stages"] == {"build": 1, "retry_wait": 1}
            listing = await client.get("/api/v1/monitor/images", params={"stage": "build"})
            assert listing.status_code == 200, listing.text
            item, = listing.json()["items"]
            assert item["affected_trials"] == item["waiting_trials"] == 2
            assert item["stage"] == "build" and item["stage_seconds"] >= 20
            assert item["region"] is None and not item["observation_stale"]
            assert "secret" not in str(listing.json()) and str(foreign_id) not in str(listing.json())
            retry = (await client.get("/api/v1/monitor/images", params={"stage": "retry_wait"})).json()["items"][0]
            assert "publication failed" in retry["wait_message"]
            assert retry["stage_seconds"] is None and retry["observation_missing"]
            page1 = (await client.get("/api/v1/monitor/images", params={"limit": 1})).json()
            assert page1["next_cursor"]
            page2 = (await client.get("/api/v1/monitor/images", params={"limit": 1, "cursor": page1["next_cursor"]})).json()
            assert len({page1["items"][0]["id"], page2["items"][0]["id"]}) == 2
            assert page2["next_cursor"] is None
            assert (await client.get("/api/v1/monitor/images", params={"team_id": str(foreign_team)})).status_code == 403
            rows = await client.get("/api/v1/trials", params={"batch_q": "pipeline named batch", "image_id": str(images[0])})
            assert {item["id"] for item in rows.json()["items"]} == {str(i) for i in own_ids[:2]}
            drill_summary = (await client.get("/api/v1/monitor/summary", params={"view": "trials",
                "batch_q": "pipeline named batch", "image_id": str(images[0])})).json()["progress"]
            assert drill_summary["trial_count"] == 2 and drill_summary["images"]["image_count"] == 1
            # Admin retry advances the epoch before old native resources are
            # released. The old reservation still blocks admission and must be
            # visible as one unique image cleanup job, not a clean queue row.
            async with app.state.session_factory() as session, session.begin():
                image = await session.get(TaskImageMaterialization, images[0])
                old = await session.scalar(select(TaskImageMaterializationAttempt).where(
                    TaskImageMaterializationAttempt.materialization_id == images[0]))
                image.state = "failed"
                old.native_build = {**old.native_build, "state": "cleaning",
                    "capacity_reserved_at": now.isoformat(),
                    "observed_at": (now-timedelta(minutes=3)).isoformat()}
                await session.flush()
                retried = await retry_task_image_materialization(session, materialization_id=images[0])
                assert retried.state == "queued" and retried.lease_epoch == 2
            cleanup_response = await client.get("/api/v1/monitor/images", params={"stage": "cleanup"})
            assert cleanup_response.status_code == 200, cleanup_response.text
            cleanup, = cleanup_response.json()["items"]
            assert cleanup["id"] == str(images[0]) and cleanup["affected_trials"] == 2
            assert cleanup["state"] == "queued" and cleanup["resources_released"] is False
            assert cleanup["stage_seconds"] is None and cleanup["observation_stale"]
            assert "Previous image attempt" in cleanup["wait_message"]
            cleanup_summary = (await client.get("/api/v1/monitor/summary", params={"view": "trials",
                "batch_id": str(batch_id)})).json()["progress"]["images"]
            assert cleanup_summary["image_count"] == 2 and cleanup_summary["stages"]["cleanup"] == 1
            async with app.state.session_factory() as session, session.begin():
                old = await session.scalar(select(TaskImageMaterializationAttempt).where(
                    TaskImageMaterializationAttempt.materialization_id == images[0]))
                old.native_build = {**old.native_build, "capacity_released_at": now.isoformat()}
            released = (await client.get("/api/v1/monitor/images", params={"image_id": str(images[0])})).json()["items"][0]
            assert released["stage"] == "queued" and released["stage_seconds"] is None

    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(TrialTaskImageMaterialization.materialization_id.in_(images)))
            await session.execute(delete(TaskImageMaterializationAttempt).where(TaskImageMaterializationAttempt.materialization_id.in_(images)))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id.in_(images)))
            await session.execute(delete(Trial).where(Trial.id.in_([*own_ids, foreign_id])))
            await session.execute(delete(Batch).where(Batch.id == batch_id))
            await session.execute(delete(Team).where(Team.id == foreign_team))


async def test_current_attempt_verifier_retry_and_terminal_cleanup_projection(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        trial_id, parent, target = await execution._separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            waiting = (await load_trial_progress(session, [trial]))[trial_id]
            assert waiting["stage"] == "verifying" and waiting["detail"] == "execution_wait"
            assert waiting["output_state"] == "committed" and waiting["cleanup_state"] == "complete"
            verifier = await execution._reserve(session, trial_id=trial_id, target=target,
                now=now+timedelta(seconds=1), parent_lease_id=parent.id,
                requirements=execution._requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=execution._deferred_verifier_contract())
            await session.flush()
            startup = (await load_trial_progress(session, [trial]))[trial_id]
            assert startup["stage"] == "verifying" and startup["detail"] == "starting"
            verifier.observed_state = "running"
            verifier.pod_started_at = now
            verifier.last_reconciled_at = now
            await session.flush()
            live = (await load_trial_progress(session, [trial]))[trial_id]
            assert live["stage"] == "verifying" and live["detail"] == "running"
            assert live["region"] == target.region
            assert not live["observation_stale"]
            summary = await progress_summary(session, select(Trial.id).where(Trial.id == trial_id))
            assert summary["stages"]["verifying"] == 1 and summary["trial_count"] == 1
            # A newer retry identity is awaiting admission; old running child cannot explain it.
            trial.result = {**trial.result, "verifier_execution": {
                "state": "pending", "parent_lease_id": str(parent.id), "retries": 1}}
            await session.flush()
            retry = (await load_trial_progress(session, [trial]))[trial_id]
            assert retry["stage"] == "verifying" and retry["detail"] == "execution_wait"
            # A new Trial attempt has no current lease: old generation is not current execution.
            savepoint = await session.begin_nested()
            trial.state = "claimed"
            trial.attempt_count += 1
            await session.flush()
            new_attempt = (await load_trial_progress(session, [trial]))[trial_id]
            assert new_attempt["stage"] == "unknown" and new_attempt["output_state"] == "unknown"
            assert new_attempt["observation_missing"] and new_attempt["region"] is None
            await savepoint.rollback()
            await session.refresh(trial)
            # A committed result remains terminal even when old resources are blocked.
            trial.state, trial.finished_at = "succeeded", now
            verifier.cleanup_state = "blocked"
            verifier.last_reconciled_at = now-timedelta(minutes=3)
            verifier.cleanup_requested_at = now
            verifier.cleanup_deadline_at = now+timedelta(seconds=30)
            await session.flush()
            terminal = (await load_trial_progress(session, [trial]))[trial_id]
            assert terminal["stage"] == "succeeded" and terminal["cleanup_state"] == "blocked"
            assert terminal["cleanup_observation_stale"] and not terminal["observation_stale"]
            scoped = apply_trial_monitor_filters(select(Trial.id), target_team=trial.team_id, state="cleanup:blocked")
            assert trial_id in (await session.scalars(scoped)).all()
            await session.rollback()
    finally:
        await engine.dispose()


async def test_real_reserved_and_creating_attempt_stages(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await execution._seed_ready_trial(session, now=now)
            lease = await execution._reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            assert lease.observed_state == "reserved"
            for state in ("reserved", "creating"):
                lease.observed_state = state
                await session.flush()
                progress = (await load_trial_progress(session, [trial]))[trial_id]
                assert progress["stage"] == "starting"
                assert progress["stage_started_at"] == lease.created_at.isoformat()
            lease.output_commit_state = "unavailable"
            lease.output_generation = lease.resource_generation
            lease.output_unavailable_reason = "runtime_output_missing"
            await session.flush()
            missing_output = (await load_trial_progress(session, [trial]))[trial_id]
            assert missing_output["output_state"] == "unavailable"
            filtered = apply_trial_monitor_filters(select(Trial.id), target_team=trial.team_id,
                                                   state="output:unavailable")
            assert trial_id in (await session.scalars(filtered)).all()
            await session.rollback()
    finally:
        await engine.dispose()
