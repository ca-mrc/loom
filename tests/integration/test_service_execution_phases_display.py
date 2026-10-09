"""Trial detail and SQL summaries agree on a separate-mode trial's phases."""

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Batch, Token, Trial, User
from loom.execution_contract import VerifierTopology
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


async def test_separate_trial_exposes_agent_wait_and_verifier_phases(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    user_id, batch_id = uuid4(), uuid4()
    raw_token = f"test-phases-{uuid4()}"
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
        trial_id, parent, target = await fixtures._separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(User(
                id=user_id, username=f"phases-{user_id}",
                username_normalized=f"phases-{user_id}", status="active",
                is_platform_admin=False,
            ))
            session.add(Batch(
                id=batch_id, team_id=trial.team_id, name="phase display",
                task_filter={}, trial_config={}, expected_trial_count=1,
                created_by_token_prefix="test", state="running", backend="nebius",
            ))
            await session.flush()
            session.add(Token(
                token_hash=token_hash, type="team", scopes=["read:own"],
                team_id=trial.team_id, created_by_user_id=user_id, issued_at=now,
            ))
            trial.batch_id = batch_id
            await session.commit()

        client_kwargs = {
            "transport": httpx.ASGITransport(app=app), "base_url": "http://service",
            "headers": {"Authorization": f"Bearer {raw_token}"},
        }
        async with httpx.AsyncClient(**client_kwargs) as client:
            waiting = (await client.get(f"/api/v1/trials/{trial_id}")).json()
        provenance = waiting["execution_provenance"]
        # This fixture commits handoff bookkeeping without a runtime result or
        # container start observation. Completion alone cannot prove execution.
        assert provenance["state"] == "planned"
        assert provenance["lease_id"] == str(parent.id)
        assert provenance["task_image_digest"] == "sha256:" + "a" * 64
        assert provenance["runtime_image_digest"] == "sha256:" + "b" * 64
        assert "registry" not in str(provenance)
        phases = waiting["execution_phases"]
        assert phases["verifier_execution"] == "separate_execution"
        assert [item["phase"] for item in phases["phases"]] == ["agent", "awaiting_verifier"]
        assert phases["phases"][1]["state"] == "pending"
        assert phases["phases"][1]["reserved_seconds"] == 0.0
        assert waiting["materialization"]["lifecycle_stage"] == "verifying"

        async with sessions() as session:
            verifier = await fixtures._reserve(
                session, trial_id=trial_id, target=target, now=now + timedelta(seconds=5),
                requirements=fixtures._requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
                runtime_contract=fixtures._deferred_verifier_contract(), parent_lease_id=parent.id,
            )
            await session.commit()

        async with httpx.AsyncClient(**client_kwargs) as client:
            detail = await client.get(f"/api/v1/trials/{trial_id}")
            batch = await client.get(f"/api/v1/batches/{batch_id}")
            monitor = await client.get(
                "/api/v1/monitor/summary", params={"view": "trials", "batch_id": str(batch_id)},
            )
        for response in (detail, batch, monitor):
            assert response.status_code == 200, response.text
        assert detail.json()["execution_provenance"] == provenance
        phases = detail.json()["execution_phases"]
        assert [item["phase"] for item in phases["phases"]] == ["agent", "awaiting_verifier", "verifier"]
        agent, wait, child = phases["phases"]
        assert agent["lease_id"] == str(parent.id)
        assert child["lease_id"] == str(verifier.id)
        assert wait["state"] == "complete"
        assert phases["handoff_storage_bytes"] == 1
        # Team tokens see resources and time, never execution prices.
        assert all(item["estimated_cost_microusd"] is None for item in phases["phases"])
        assert phases["reservation_overlap_seconds"] == 0.0
        summary = batch.json()["service_execution_summary"]
        activity = monitor.json()["service_execution"]["activity"]
        assert {
            "trial_api": detail.json()["materialization"]["lifecycle_stage"],
            "batch_sql": {k: v for k, v in summary["lifecycle_stages"].items() if v},
            "monitor_sql": {k: v for k, v in activity["lifecycle_stages"].items() if v},
        } == {
            "trial_api": "verifying",
            "batch_sql": {"verifying": 1},
            "monitor_sql": {"verifying": 1},
        }
    finally:
        async with sessions() as session:
            await session.execute(delete(Token).where(Token.token_hash == token_hash))
            await session.execute(delete(User).where(User.id == user_id))
            await session.commit()
        await engine.dispose()
