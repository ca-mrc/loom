"""A real snapshot conflict crosses the CP boundary and terminates batch fanout."""

import copy
import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import delete, insert, select

from loom.db.schema import Batch, Task, TaskImageMaterialization, Token, Trial
from loom.task_image_materialization import task_image_materialization_key
from loom_control_plane.routes.trials import router
from loom_service.batch_runner import run_once
from tests.integration.test_batch_runner_e2e import runner_setup  # noqa: F401


async def test_actual_snapshot_conflict_finishes_batch_without_retries(runner_setup, monkeypatch):  # noqa: F811
    session_factory, _, team_id, task_ids, _ = runner_setup
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    task_id = task_ids[0]
    checksum = "a" * 64
    materialization_id = uuid4()
    token = f"loom_w_{uuid4().hex}"
    async with session_factory() as session:
        task = await session.get(Task, task_id)
        config = copy.deepcopy(task.config)
        config["environment"] = {
            "os": "linux", "cpu_arch": "x86_64", "dockerfile": "Dockerfile",
        }
        config["verifier"]["env_mode"] = "separate"
        task.config = config
        task.checksum = checksum
        task.source_provenance = {}
        snapshot = copy.deepcopy(config)
        snapshot["verifier"]["env_mode"] = "shared"
        session.add(TaskImageMaterialization(
            id=materialization_id,
            materialization_key=task_image_materialization_key(
                task_id=task_id, task_checksum=checksum, cpu_arch="x86_64",
            ),
            task_id=task_id, task_checksum=checksum, cpu_arch="x86_64",
            task_config=snapshot, task_source=task.source, task_source_provenance={},
            state="queued",
        ))
        batch = Batch(
            team_id=team_id, name="snapshot-conflict", backend="docker",
            task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
            trial_config={"agent_name": "terminus-2", "agent_model": {"name": "test-model", "provider": "test"}, "retry": {}}, state="submitted",
            created_by_token_prefix="abcdef12", expected_trial_count=1,
        )
        session.add(batch)
        await session.execute(insert(Token).values(
            token_hash=hashlib.sha256(token.encode()).digest(), type="worker",
            scopes=["submit:batch"], issued_at=datetime.now(UTC),
        ))
        await session.commit()
        batch_id = batch.id

    app = FastAPI()
    app.include_router(router)
    app.state.session_factory = session_factory
    app.state.settings = SimpleNamespace()
    responses = []

    async def capture_response(response):
        await response.aread()
        responses.append(response)

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://cp",
            event_hooks={"response": [capture_response]},
        ) as client:
            for _ in range(2):
                await run_once(
                    session_factory=session_factory, http_client=client,
                    batch_size=10, submit_rate_per_sec=100,
                    cp_authorization=f"Bearer {token}",
                )

        assert [response.status_code for response in responses] == [409]
        detail = responses[0].json()["detail"]
        assert detail["reason"] == "task_image_snapshot_conflict"
        assert detail["task_id"] == task_id
        async with session_factory() as session:
            batch = await session.get(Batch, batch_id)
            assert batch.state == "finished"
            assert batch.result_status == "all_failed"
            assert batch.failure_reason == "fanout_submit_failed"
            assert batch.expected_trial_count == 0
            assert batch.finished_at is not None
            assert len(batch.fanout_errors) == 1
            assert batch.fanout_errors[0]["status_code"] == 409
            assert "task_image_snapshot_conflict" in batch.fanout_errors[0]["detail"]
            assert (await session.scalars(select(Trial).where(Trial.batch_id == batch_id))).all() == []
            image = await session.get(TaskImageMaterialization, materialization_id)
            task = await session.get(Task, task_id)
            assert image.task_config["verifier"]["env_mode"] == "shared"
            assert task.config["verifier"]["env_mode"] == "separate"
    finally:
        async with session_factory() as session:
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.id == materialization_id,
            ))
            await session.commit()
