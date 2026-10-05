"""Ordinary Trial detail and SQL summaries agree on cancelled execution status."""

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Batch, ExecutionCapacityObservation, Token, Trial, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_execution_capacity_placement import _record as _record_capacity
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


@pytest.mark.parametrize(
    "trial_state,expected_stage", [("cancelled", "cancelled"), ("failed", "output_unavailable")]
)
async def test_cancelled_output_unavailable_agrees_across_public_projections(
    postgres_url: str, trial_state: str, expected_stage: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    user_id, batch_id = uuid4(), uuid4()
    raw_token = f"test-display-{uuid4()}"
    token_hash = hashlib.sha256(raw_token.encode()).digest()
    settings = LoomServiceSettings(
        _env_file=None, db_url=postgres_url, minio_endpoint="http://minio:9000",
        minio_access_key="test", minio_secret_key="test",
        control_plane_url="http://cp/", gateway_url="http://gw/",
    )
    app = create_app(settings)
    app.state.settings = settings
    app.state.session_factory = sessions
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(User(
                id=user_id, username=f"display-{user_id}",
                username_normalized=f"display-{user_id}", status="active",
                is_platform_admin=False,
            ))
            session.add(Batch(
                id=batch_id, team_id=trial.team_id, name="display parity",
                task_filter={}, trial_config={}, expected_trial_count=1,
                created_by_token_prefix="test", state="finished", backend="nebius",
            ))
            await session.flush()
            session.add(Token(
                token_hash=token_hash, type="team", scopes=["read:own"],
                team_id=trial.team_id, created_by_user_id=user_id, issued_at=now,
            ))
            trial.batch_id = batch_id
            trial.state = trial_state
            trial.finished_at = now
            lease.output_commit_state = "unavailable"
            lease.output_generation = lease.resource_generation
            lease.output_unavailable_reason = (
                "operator_cancelled" if trial_state == "cancelled" else "output_marker_missing"
            )
            lease.desired_state = "deleted"
            lease.observed_state = "deleted"
            lease.cleanup_state = "complete"
            lease.cleanup_requested_at = now
            lease.cleanup_deadline_at = now + timedelta(minutes=5)
            lease.deleted_at = now
            observation = await session.scalar(select(ExecutionCapacityObservation).where(
                ExecutionCapacityObservation.target_id == target.target_id,
            ).order_by(ExecutionCapacityObservation.observed_at.desc()).limit(1))
            assert observation is not None
            await _record_capacity(session, target.target_id, datetime.now(UTC), {
                **observation.observation_json["placement"],
                "nodes": [], "pending_pods": [], "build_concurrency_limit": 7,
            })
            await session.commit()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://service",
            headers={"Authorization": f"Bearer {raw_token}"},
        ) as client:
            detail = await client.get(f"/api/v1/trials/{trial_id}")
            batch = await client.get(f"/api/v1/batches/{batch_id}")
            monitor = await client.get(
                "/api/v1/monitor/summary", params={"view": "trials", "batch_id": str(batch_id)},
            )
            placement = await client.get("/api/v1/monitor/placement", params={"target_id": target.target_id})
            forbidden = await client.get("/api/v1/monitor/placement", params={
                "target_id": target.target_id, "team_id": str(uuid4()),
            })
        assert placement.status_code == 200, placement.text
        assert placement.json()["available"] is True
        assert placement.json()["build_concurrency_limit"] == 7
        assert placement.json()["nodes"] == [] and placement.json()["pending_builds"] == 0
        assert forbidden.status_code == 403
        for response in (detail, batch, monitor):
            assert response.status_code == 200, response.text
        materialization = detail.json()["materialization"]
        summary = batch.json()["service_execution_summary"]
        activity = monitor.json()["service_execution"]["activity"]
        # The unavailable output remains factual; only the lifecycle precedence changes.
        assert detail.json()["state"] == trial_state
        assert materialization["output_commit_state"] == "unavailable"
        assert materialization["state"] == "not_started"
        assert materialization["canonical_ready"] is False
        assert materialization["error"]["code"] == (
            "operator_cancelled" if trial_state == "cancelled" else "output_marker_missing"
        )
        assert "no canonical bundle" in materialization["error"]["message"]
        assert summary["output_commit_states"] == {"unavailable": 1}
        assert summary["materialization_states"] == {"not_started": 1}
        assert summary["canonical_ready_count"] == 0
        assert summary["execution_states"] == {lease.observed_state: 1}
        assert activity["materialization"]["states"]["not_started"] == 1
        assert {
            "trial_api": materialization["lifecycle_stage"],
            "batch_sql": {k: v for k, v in summary["lifecycle_stages"].items() if v},
            "monitor_sql": {k: v for k, v in activity["lifecycle_stages"].items() if v},
        } == {
            "trial_api": expected_stage,
            "batch_sql": {expected_stage: 1},
            "monitor_sql": {expected_stage: 1},
        }
    finally:
        async with sessions() as session:
            await session.execute(delete(Token).where(Token.token_hash == token_hash))
            await session.execute(delete(User).where(User.id == user_id))
            await session.commit()
        await engine.dispose()
