"""Real public submission paths stamp their process, including cloned/reused work."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.schema import Batch, Trial
from tests.integration.test_service_batches_crud import camp_setup as camp_setup
from tests.integration.test_service_run_library import run_library_setup as run_library_setup
from tests.unit.test_nebius_pool_submission_source import source


def configure(app, body):
    # Model copy replaces only protected deployment configuration, never HTTP input.
    app.state.settings = app.state.settings.model_copy(update={"pool_submission_source_json": json.dumps(body)})


async def origin(app, identity):
    async with app.state.session_factory() as session:
        return (await session.get(Batch, UUID(identity))).pool_origin


async def test_public_batch_records_exact_application_origin_and_ignores_payload_priority(camp_setup):
    app, token, _ = camp_setup
    installed = source()
    configure(app, installed)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post("/api/v1/batches", headers={"Authorization": "Bearer " + token}, json={
            "name": "origin-test", "purpose": "evaluation", "task_filter": {"license": "MIT"}, "trial_config": {"agent": {"name": "oracle"}},
            "pool_origin": source("environment"), "priority": "production"})
    assert response.status_code == 201, response.text
    identity = response.json()["batch_id"]
    stored = await origin(app, identity)
    assert stored == {"schema_version": "loom.pool-work-origin.v1", "submission_id": identity,
        "data_environment_id": installed["data_environment_id"], "kind": "application", "application": installed["application"]}
    # A later deployment cannot rewrite already queued work's release identity.
    configure(app, source())
    assert await origin(app, identity) == stored


@pytest.mark.parametrize("operation", ["clone", "reuse"])
async def test_shared_run_reuse_stamps_current_personal_application_not_parent(run_library_setup, operation):
    setup = run_library_setup
    app = setup["app"]
    installed = source()
    configure(app, installed)
    if operation == "clone":
        path = f"/api/v1/run-library/batches/{setup['batch_shared']}/clone-config"
        body = {"name": "origin clone", "provider_connection_id": str(setup["conn_b"])}
    else:
        path = f"/api/v1/run-library/trials/{setup['trial_shared']}/artifacts/reuse"
        body = {"name": "origin reuse", "key": setup["safe_key"], "provider_connection_id": str(setup["conn_b"])}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post(path, json=body, headers={"Authorization": "Bearer " + str(setup["raw_b"])})
    assert response.status_code == 201, response.text
    identity = response.json()["batch_id"]
    stored = await origin(app, identity)
    assert stored["submission_id"] == identity and stored["kind"] == "application"
    assert stored["application"] == installed["application"]
    assert stored["data_environment_id"] == installed["data_environment_id"]


async def test_missing_template_is_unknown_not_silently_shared(camp_setup):
    app, token, _ = camp_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post("/api/v1/batches", headers={"Authorization": "Bearer " + token}, json={
            "name": "legacy-origin", "purpose": "evaluation", "task_filter": {"license": "MIT"}, "trial_config": {"agent": {"name": "oracle"}},
            "pool_origin": {"submission_id": str(uuid4()), "kind": "environment"}})
    assert response.status_code == 201, response.text
    assert await origin(app, response.json()["batch_id"]) is None
    async with app.state.session_factory() as session:
        assert (await session.scalars(select(Batch))).first() is not None


async def test_failed_case_rerun_binds_the_current_application_release(camp_setup):
    app, token, team_id = camp_setup
    installed = source()
    configure(app, installed)
    parent_id = uuid4()
    async with app.state.session_factory.begin() as session:
        session.add(Batch(id=parent_id, team_id=team_id, name="rerun-origin", task_filter={"task_ids": ["local/mit-0"]},
            trial_config={"agent_name": "oracle"}, state="finished", created_by_token_prefix="test-origin",
            expected_trial_count=1, n_per_task=1, result_status="partial_failed", finished_at=datetime.now(UTC)))
        await session.flush()
        session.add(Trial(id=uuid4(), batch_id=parent_id, team_id=team_id, task_id="local/mit-0", state="failed",
            failure_reason="gateway_error", failure_message="gateway HTTP 503", config={}, requires_caps={},
            submitted_at=datetime.now(UTC), sample_idx=0, combination_idx=0))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post(f"/api/v1/batches/{parent_id}/rerun-failed", headers={"Authorization": "Bearer " + token})
    assert response.status_code == 201, response.text
    identity = response.json()["batch_id"]
    stored = await origin(app, identity)
    assert stored["submission_id"] == identity and stored["application"] == installed["application"]
