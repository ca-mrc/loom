"""Legacy and unified trial claims must honor the optional mainland gateway capability."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial, Worker
from tests.integration.test_trial_claim_compatibility import _claim_trial


@pytest.mark.parametrize("unified", [False, True], ids=["legacy", "unified"])
async def test_mainland_proxy_trial_waits_for_capable_worker(postgres_url, unified):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, trial_id, worker_id = uuid4(), uuid4(), uuid4()
    task_id = f"mainland-claim/{uuid4()}"
    pool = f"mainland-proxy-{uuid4()}"
    capability_digest = "sha256:" + "d" * 64
    token_hash = b"mainland-proxy-worker"
    now = datetime.now(UTC)
    caps = [
        {
            "backend": "docker",
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "network_policies": ["public"],
        }
    ]
    try:
        async with sessions() as session, session.begin():
            await session.execute(insert(Team).values(id=team_id, name=f"mainland-{team_id}"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="d" * 64,
                    config={"schema_version": "1"},
                )
            )
            await session.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname=f"mainland-{worker_id}",
                    version="test",
                    pool_name=pool,
                    capabilities=caps,
                    supported_work_kinds=["trial"],
                    capability_snapshot_digest=capability_digest,
                    capability_snapshot_json={
                        "schema_version": "loom.worker-capabilities.v1",
                        "cpu_arch": "x86_64",
                        "container_runtime_features": [],
                    },
                    auth_token_hash=token_hash,
                    max_concurrent=1,
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                )
            )
            await session.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={"agent_name": "oracle", "agent_model": None, "mainland_proxy": True},
                    requires_caps={
                        "backend": "docker",
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                        "worker_pool": pool,
                        "mainland_proxy": True,
                    },
                    state="queued",
                )
            )
        async with sessions() as session, session.begin():
            assert (
                await _claim_trial(
                    session,
                    unified=unified,
                    worker_id=worker_id,
                    capability_digest=capability_digest,
                    token_hash=token_hash,
                )
                is None
            )
        async with sessions() as session, session.begin():
            await session.execute(
                update(Worker)
                .where(Worker.id == worker_id)
                .values(
                    capabilities=[{**caps[0], "mainland_proxy": True}],
                )
            )
        async with sessions() as session, session.begin():
            claimed = await _claim_trial(
                session,
                unified=unified,
                worker_id=worker_id,
                capability_digest=capability_digest,
                token_hash=token_hash,
            )
            assert claimed is not None
            assert claimed["id"] == trial_id
            assert claimed["config"]["mainland_proxy"] is True
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(Worker).where(Worker.id == worker_id))
            await session.execute(delete(Task).where(Task.id == task_id))
            await session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            await session.execute(delete(Team).where(Team.id == team_id))
        await engine.dispose()
