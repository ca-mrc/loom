"""Kill an in-flight local runner, recover diagnostics, and reap only its sandbox."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import multiprocessing
import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import docker
import httpx
import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.oracle import OracleAgent
from loom.db.schema import Task, TeamQuota, Token, Trial, Worker
from loom.driver.docker import DockerDriver
from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.retry_exhausted_sweeper import sweep_retry_exhausted
from loom_control_plane.scheduler.crash_detector import reclaim_expired_workers
from loom_worker.control_plane_client import HttpControlPlaneClient
from loom_worker.orphan_containers import cleanup_orphan_sandbox_containers
from loom_worker.trial_runner import LocalTrialRunner
from tests._trial_config_defaults import stub_trial_config
from tests.integration.test_service_trials_read import trials_setup  # noqa: F401

pytestmark = pytest.mark.docker
logger = logging.getLogger(__name__)


class _PassVerifier:
    name = "pass"

    async def verify(self, **kwargs):
        return VerifierResult(rewards={"passed": 1.0})


def _run_local_worker(
    db_url: str, token: str, worker_id: UUID, trial_id: UUID, team_id: UUID,
    task_config: dict, task_dir: str, identity: str, container_name: str,
) -> None:
    # A fresh process owns its event loop, CP client and PostgreSQL connections.
    # ASGI transports use the real authenticated CP routes, without fixed ports
    # or a shared Compose installation.
    os.environ["LOOM_ENV"] = "development"
    os.environ["LOOM_LOCAL_EXECUTION"] = "1"

    async def run() -> None:
        engine = create_async_engine(db_url)
        settings = ControlPlaneSettings(
            _env_file=None, db_url=db_url, minio_access_key="test", minio_secret_key="test",
        )
        app = create_app(settings)
        app.state.settings = settings
        app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://cp",
        ) as http:
            cp = HttpControlPlaneClient(base_url="http://cp", token=token, _client=http)

            async def patch(state, failure_reason, failure_message=None):
                return await cp.patch_state(
                    trial_id=trial_id, worker_id=worker_id, state=state,
                    failure_reason=failure_reason, failure_message=failure_message,
                )

            runner = LocalTrialRunner(
                trial_id=trial_id, team_id=team_id, attempt_count=1,
                task_config=TaskConfig.model_validate(task_config),
                task_checksum="0" * 64, task_dir=Path(task_dir),
                trial_config=stub_trial_config(),
                driver_factory=lambda: DockerDriver(image="alpine:3.20", container_name=container_name),
                agent_factory=lambda path, _gw, _model, _name: OracleAgent(task_dir=path, trial_id=trial_id),
                verifier_factory=_PassVerifier,
                object_store=FakeObjectStore(), gateway_client=FakeLLMGatewayClient(scripted=[]),
                local_trajectory_root=Path(task_dir).parent / "trajectories",
                state_patch_callback=patch,
                runtime_identity_labels=(("loom.sandbox", identity),),
                container_memory_mib=128, container_cpus=0.5, container_pids=64,
            )
            await runner.run()
        await engine.dispose()

    asyncio.run(run())


async def test_killed_runner_exposes_failure_and_cleans_only_owned_orphan(
    trials_setup, tmp_path: Path,  # noqa: F811
) -> None:
    app, team_token, _, seeded_trials = trials_setup
    trial_id, worker_id = uuid4(), uuid4()
    identity = f"worker-death-{trial_id.hex}"
    container_name = f"loom-death-{trial_id.hex}"
    control_names = (f"{container_name}-active", f"{container_name}-foreign")
    worker_token = f"test-worker-{uuid4().hex}"
    token_hash = hashlib.sha256(worker_token.encode()).digest()
    now = datetime.now(UTC)
    task_dir = tmp_path / "task"
    (task_dir / "solution").mkdir(parents=True)
    (task_dir / "instruction.md").write_text("Wait for the worker-death fault injection.\n")
    (task_dir / "solution" / "solve.sh").write_text(
        "#!/bin/sh\nprintf ready > /workspace/solver-started\nexec sleep 120\n",
    )
    task_config = {
        "schema_version": "1", "task": {"id": f"local/death-{trial_id.hex}", "name": "worker death"},
        "environment": {"os": "linux", "docker_image": "alpine:3.20"},
        "agent": {"name": "oracle"}, "verifier": {"name": "pass"}, "steps": [{"name": "main"}],
    }
    async with app.state.session_factory() as session, session.begin():
        seed = await session.get(Trial, seeded_trials[0])
        team_id = seed.team_id
        quota = await session.get(TeamQuota, team_id)
        if quota is None:
            session.add(TeamQuota(team_id=team_id, max_attempts_ceiling=1))
        else:
            quota.max_attempts_ceiling = 1
        session.add(Worker(id=worker_id, hostname="fault-injection", version="test", capabilities=[],
                           registered_at=now, last_seen_at=now, status="active"))
        session.add(Token(token_hash=token_hash, type="worker", scopes=["worker:report"], issued_at=now))
        session.add(Task(id=task_config["task"]["id"], checksum="0" * 64, config=task_config))
        await session.flush()
        session.add(Trial(
            id=trial_id, team_id=team_id, task_id=task_config["task"]["id"],
            submitted_by_user_id=seed.submitted_by_user_id, state="claimed", worker_id=worker_id,
            claimed_at=now, attempt_count=1, requires_caps={},
            config={"agent_name": "oracle", "agent_model": None, "retry": {"max_attempts": 1, "retry_on": []}},
        ))

    process = multiprocessing.get_context("spawn").Process(target=_run_local_worker, args=(
        str(app.state.settings.db_url), worker_token, worker_id, trial_id, team_id,
        task_config, str(task_dir), identity, container_name,
    ))
    client = docker.from_env()
    controls = []
    orphan = None
    process.start()
    logger.info("worker-death fixture started child=%s container=%s", process.pid, container_name)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc",
            headers={"Authorization": f"Bearer {team_token}"},
        ) as user:
            async with asyncio.timeout(20):
                while True:
                    assert process.is_alive(), f"runner exited before kill: {process.exitcode}"
                    try:
                        orphan = await asyncio.to_thread(client.containers.get, container_name)
                        if orphan.status != "running":
                            await asyncio.sleep(0.05)
                            continue
                        marker = await asyncio.to_thread(orphan.exec_run, "cat /workspace/solver-started")
                    except docker.errors.NotFound:
                        pass
                    else:
                        if marker.exit_code == 0 and marker.output == b"ready":
                            break
                    await asyncio.sleep(0.05)
            running = await user.get(f"/api/v1/trials/{trial_id}")
            logger.info("worker-death solver marker observed: %s", running.json()["state"])
            assert running.status_code == 200, running.text
            assert running.json()["state"] == "running"
            assert running.json()["started_at"] is not None
            assert running.json()["finished_at"] is None
            assert orphan.attrs["HostConfig"]["Memory"] == 128 * 1024 * 1024
            assert orphan.attrs["HostConfig"]["NanoCpus"] == 500_000_000
            assert orphan.attrs["HostConfig"]["PidsLimit"] == 64

            process.kill()
            await asyncio.to_thread(process.join, 5)
            logger.info("worker-death injected signal completed")
            assert process.exitcode == -signal.SIGKILL  # Injected death, not OOM evidence.
            await asyncio.to_thread(orphan.reload)
            assert orphan.status == "running"

            # Advance only this disposable worker's heartbeat age. Recovery uses
            # its normal expiry/backoff/exhaustion policy, with no second attempt.
            async with app.state.session_factory() as session, session.begin():
                worker = await session.get(Worker, worker_id)
                worker.last_seen_at = datetime.now(UTC) - timedelta(seconds=60)
            async with app.state.session_factory() as session, session.begin():
                assert await reclaim_expired_workers(session, expiry_sec=15) == 1
            reclaimed = (await user.get(f"/api/v1/trials/{trial_id}")).json()
            logger.info("worker-death reclaimed: %s", reclaimed["state"])
            assert reclaimed["state"] == "queued"
            assert reclaimed["failure_reason"] == "worker_lost_claim"
            assert str(worker_id) in reclaimed["failure_message"]
            async with app.state.session_factory() as session, session.begin():
                assert await sweep_retry_exhausted(session) == [trial_id]
            failed = (await user.get(f"/api/v1/trials/{trial_id}")).json()
            logger.info("worker-death exhausted: %s", failed["state"])
            assert failed["state"] == "failed"
            assert failed["failure_reason"] == "retry_exhausted"
            assert failed["failure_message"] == reclaimed["failure_message"]
            assert failed["finished_at"] is not None
            listed = await user.get("/api/v1/trials", params={"state": "failed"})
            item = next(item for item in listed.json()["items"] if item["id"] == str(trial_id))
            assert item["failure_message"] == failed["failure_message"]

            for control_name, control_trial, control_identity in (
                (control_names[0], seeded_trials[1], identity),
                (control_names[1], trial_id, f"foreign-{identity}"),
            ):
                controls.append(await asyncio.to_thread(
                    client.containers.run, "alpine:3.20", ["sleep", "120"], detach=True,
                    name=control_name,
                    labels={"loom.trial_id": str(control_trial), "loom.sandbox": control_identity},
                ))
            states = {trial_id: failed["state"], seeded_trials[1]: "running"}
            removed = await asyncio.to_thread(
                cleanup_orphan_sandbox_containers, docker_client=client,
                state_lookup=states.__getitem__, sandbox_identity=identity,
            )
            logger.info("worker-death orphan cleanup completed")
            assert removed == [trial_id]
            with pytest.raises(docker.errors.NotFound):
                await asyncio.to_thread(client.containers.get, container_name)
            for control in controls:
                await asyncio.to_thread(control.reload)
                assert control.status == "running"
    finally:
        logger.info("worker-death fixture cleanup starting")
        if process.is_alive():
            process.kill()
        await asyncio.to_thread(process.join, 5)
        # Creation may have succeeded before its reply was read or the child
        # was killed. Recover only the exact unique names assigned above.
        for name in (container_name, *control_names):
            try:
                container = await asyncio.to_thread(client.containers.get, name)
                assert container.labels["loom.sandbox"] in (identity, f"foreign-{identity}")
                await asyncio.to_thread(container.remove, force=True)
            except docker.errors.NotFound:
                pass
        client.close()
        async with app.state.session_factory() as session, session.begin():
            trial = await session.get(Trial, trial_id)
            trial.worker_id = None
            await session.flush()
            await session.execute(delete(Worker).where(Worker.id == worker_id))
            await session.execute(delete(Token).where(Token.token_hash == token_hash))
