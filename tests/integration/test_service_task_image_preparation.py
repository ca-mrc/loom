"""Ordinary trial readers can diagnose builds without an execution bundle."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401


@pytest.mark.parametrize("cancelled", [False, True])
async def test_pre_execution_diagnostics_and_bundle_unavailable(trials_setup, cancelled):  # noqa: F811
    app, token, _team_id, trial_ids = trials_setup
    materialization_id, attempt_id = uuid4(), uuid4()
    async with app.state.session_factory() as session, session.begin():
        trial = await session.get(Trial, trial_ids[0])
        trial.state = "cancelled" if cancelled else "failed"
        trial.finished_at = datetime.now(UTC)
        row = TaskImageMaterialization(
            id=materialization_id,
            materialization_key=uuid4().hex * 2,
            task_id=trial.task_id,
            task_checksum="a" * 64,
            cpu_arch="x86_64",
            task_config={},
            state="queued" if cancelled else "failed",
            attempt_count=2,
            lease_epoch=3,
            failure_reason="build_cancelled" if cancelled else "build_build_failed",
            failure_message="secret=private-build-password",
        )
        session.add(row)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=row.id))
        session.add(
            TaskImageMaterializationAttempt(
                id=attempt_id,
                materialization_id=row.id,
                attempt_number=2,
                lease_epoch=3,
                builder_id="nebius:primary",
                claimed_at=datetime.now(UTC),
                native_build={
                    "builder_log": "sensitive-private-build-log",
                    "phases": [{"name": "build", "state": {"terminated": {"exitCode": 37}}}],
                    "capacity_released_at": "2026-09-13T00:07:00Z",
                },
            )
        )
        session.add(
            TaskImageMaterializationAttempt(
                id=uuid4(),
                materialization_id=row.id,
                attempt_number=1,
                lease_epoch=2,
                builder_id="nebius:primary",
                claimed_at=datetime.now(UTC),
                native_build={
                    "phases": [{"name": "build", "state": {"terminated": {"exitCode": 99}}}],
                },
            )
        )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc"
        ) as client:
            headers = {"Authorization": f"Bearer {token}"}
            detail = await client.get(f"/api/v1/trials/{trial_ids[0]}", headers=headers)
            assert detail.status_code == 200, detail.text
            assert "task_environment_preparation" in detail.json()
            preparation = detail.json()["task_environment_preparation"]
            assert len(preparation) == 1
            assert preparation[0]["failure_reason"] == row.failure_reason
            assert preparation[0]["phases"][0]["exit_code"] == 37
            assert preparation[0]["resources_released"] is True
            assert "private-build" not in detail.text
            unrelated = await client.get(f"/api/v1/trials/{trial_ids[1]}", headers=headers)
            assert unrelated.status_code == 200
            assert unrelated.json()["task_environment_preparation"] == []
            bundle = await client.get(
                f"/api/v1/trials/{trial_ids[0]}/bundle/download", headers=headers
            )
            assert bundle.status_code == 409
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(
                delete(TrialTaskImageMaterialization).where(
                    TrialTaskImageMaterialization.materialization_id == materialization_id
                )
            )
            await session.execute(
                delete(TaskImageMaterializationAttempt).where(
                    TaskImageMaterializationAttempt.materialization_id == materialization_id
                )
            )
            await session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.id == materialization_id
                )
            )


async def test_publish_diagnostic_is_visible_without_builder_secrets(trials_setup):  # noqa: F811
    app, token, _team_id, trial_ids = trials_setup
    materialization_id, attempt_id = uuid4(), uuid4()
    async with app.state.session_factory() as session, session.begin():
        trial = await session.get(Trial, trial_ids[0])
        trial.state = "failed"
        trial.finished_at = datetime.now(UTC)
        row = TaskImageMaterialization(
            id=materialization_id, materialization_key=uuid4().hex * 2, task_id=trial.task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={}, state="failed",
            attempt_count=3, lease_epoch=4, failure_reason="build_publish_failed",
            failure_message="secret=private-build-password registry.private/team/image",
        )
        session.add(row)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=row.id))
        session.add(TaskImageMaterializationAttempt(
            id=attempt_id, materialization_id=row.id, attempt_number=3, lease_epoch=4,
            builder_id="nebius:primary", claimed_at=datetime.now(UTC),
            native_build={
                "builder_log": "sensitive-private-build-log",
                "phases": [{"name": "publish", "state": {"terminated": {"exitCode": 1}},
                            "platform_diagnostic": {
                                "exit_code": 1, "summary": "writing blob: unauthorized",
                                "argv": "skopeo --authfile /secret",
                            }}],
                "capacity_released_at": "2026-10-28T07:56:03Z",
            },
        ))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
            detail = await client.get(
                f"/api/v1/trials/{trial_ids[0]}", headers={"Authorization": f"Bearer {token}"},
            )
            assert detail.status_code == 200, detail.text
            preparation = detail.json()["task_environment_preparation"][0]
            assert preparation["failure_reason"] == "build_publish_failed"
            assert preparation["diagnostic"] == {
                "phase": "publish", "exit_code": 1, "summary": "writing blob: unauthorized",
            }
            assert "private-build" not in detail.text
            assert "registry.private" not in detail.text and "/secret" not in detail.text
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(
                TrialTaskImageMaterialization.materialization_id == materialization_id))
            await session.execute(delete(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.materialization_id == materialization_id))
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.id == materialization_id))


async def test_node_loss_diagnostics_preserve_recovery_lifecycle_and_authorization(trials_setup):  # noqa: F811
    app, token, _team_id, trial_ids = trials_setup
    mid, aid, trial_id = uuid4(), uuid4(), uuid4()
    retry_at = datetime.now(UTC) + timedelta(seconds=30)
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, trial_ids[0])
        trial = Trial(id=trial_id, task_id=seed.task_id, team_id=seed.team_id,
                      submitted_by_user_id=seed.submitted_by_user_id, state="queued",
                      config=seed.config, requires_caps={"backend": "nebius"})
        session.add(trial)
        row = TaskImageMaterialization(
            id=mid, materialization_key=uuid4().hex * 2, task_id=trial.task_id,
            task_checksum="a" * 64, cpu_arch="x86_64", task_config={}, state="queued",
            attempt_count=1, lease_epoch=2, failure_reason="build_bound_node_unavailable",
            failure_message="secret=do-not-return private-node", next_attempt_at=retry_at,
        )
        session.add(row)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=mid))
        session.add(TaskImageMaterializationAttempt(
            id=aid, materialization_id=mid, attempt_number=1, lease_epoch=2,
            builder_id="nebius:primary", claimed_at=datetime.now(UTC), native_build={
                "state": "cleaning", "builder_log": "private-build-log", "io_binding": {"node_name": "private-node"},
            },
        ))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
            headers = {"Authorization": f"Bearer {token}"}
            assert (await client.get(f"/api/v1/trials/{trial_id}")).status_code in {401, 403}
            for released in (False, True):
                if released:
                    async with app.state.session_factory() as session, session.begin():
                        attempt = await session.get(TaskImageMaterializationAttempt, aid)
                        attempt.native_build = {**attempt.native_build, "state": "released",
                            "capacity_released_at": datetime.now(UTC).isoformat()}
                response = await client.get(f"/api/v1/trials/{trial_id}", headers=headers)
                assert response.status_code == 200, response.text
                preparation = response.json()["task_environment_preparation"][0]
                assert preparation["failure_reason"] == "build_bound_node_unavailable"
                assert preparation["attribution"] == "platform" and preparation["retryable"] is True
                assert preparation["state"] == "queued" and preparation["resources_released"] is released
                assert datetime.fromisoformat(preparation["next_attempt_at"]) == retry_at
                assert not any(value in response.text for value in ("do-not-return", "private-node", "private-build-log"))
            unrelated = await client.get(f"/api/v1/trials/{trial_ids[1]}", headers=headers)
            assert unrelated.json()["task_environment_preparation"] == []
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(TrialTaskImageMaterialization).where(TrialTaskImageMaterialization.materialization_id == mid))
            await session.execute(delete(TaskImageMaterializationAttempt).where(TaskImageMaterializationAttempt.materialization_id == mid))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id == mid))
            await session.execute(delete(Trial).where(Trial.id == trial_id))
