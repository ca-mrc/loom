"""Automatic owner builds use real pool SQL/HTTP and cleanup-qualified results."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_pool_schema import NebiusPoolMachineCredential, NebiusPoolRequest
from loom.db.schema import Token
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.integration.test_nebius_pool_application_build_admission import setup_application_pool
from tests.integration.test_nebius_pool_build_runtime import Reader, gateway_job
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_registry import machine
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@contextmanager
def observe_worker_waits(*, delay_seconds=50):
    """Expose suspended awaits before pytest's unchanged 60-second deadline.

    Thread stacks only show the asyncio selector for this failure. Emit code
    locations and cancellation counts, never task reprs, SQL or local values.
    A loop timer adds no task to cancel or await during worker cleanup.
    """
    loop = asyncio.get_running_loop()
    started = loop.time()

    def snapshot():
        try:
            tasks = []
            for task in list(asyncio.all_tasks(loop))[:64]:
                awaitable, chain, seen = task.get_coro(), [], set()
                while awaitable is not None and id(awaitable) not in seen and len(seen) < 64:
                    seen.add(id(awaitable))
                    frame = getattr(awaitable, "cr_frame", None) or getattr(awaitable, "gi_frame", None)
                    if frame is not None:
                        chain.append({"file": frame.f_code.co_filename,
                            "line": frame.f_lineno, "function": frame.f_code.co_name})
                    awaitable = getattr(awaitable, "cr_await", None) or getattr(awaitable, "gi_yieldfrom", None)
                tasks.append({"cancelling": task.cancelling(), "await_chain": chain})
            print(json.dumps({"event": "slow_application_build_worker_test",
                "elapsed_seconds": round(loop.time() - started, 3), "tasks": tasks}),
                file=sys.stderr, flush=True)
        except Exception:
            # Best-effort evidence must not replace the original test failure.
            pass

    timer = loop.call_later(delay_seconds, snapshot)
    try:
        yield
    finally:
        timer.cancel()


# A pool operation still running after this many seconds is observed before the
# client's 5-second request budget cancels it (#2603).
SLOW_POOL_OPERATION_SECONDS = 3.0

_BACKEND_WAITS = text("""
    SELECT a.pid,
           a.state,
           a.wait_event_type,
           a.wait_event,
           round(extract(epoch FROM clock_timestamp() - a.xact_start)::numeric, 3) AS xact_age_seconds,
           round(extract(epoch FROM clock_timestamp() - a.state_change)::numeric, 3) AS state_age_seconds,
           cardinality(pg_blocking_pids(a.pid)) AS blocked_by,
           EXISTS (SELECT 1 FROM pg_locks l
                    WHERE l.pid = a.pid AND l.locktype = 'advisory' AND l.granted) AS holds_advisory
      FROM pg_stat_activity a
     WHERE a.datname = current_database()
       AND a.pid <> pg_backend_pid()
       AND a.backend_type = 'client backend'
     ORDER BY a.pid
""")


async def database_waits(factory):
    """Every other backend of the test database: what it waits on, how long its
    transaction has been open, how many backends block it, and whether it holds
    an advisory lock (all pool writers share one). No query text or values."""
    engine = create_async_engine(
        factory.kw["bind"].url.render_as_string(hide_password=False), poolclass=NullPool,
    )
    try:
        async with asyncio.timeout(1.5):
            async with engine.connect() as connection:
                rows = (await connection.execute(_BACKEND_WAITS)).mappings().all()
        return [{key: (float(value) if key.endswith("_seconds") and value is not None else value)
                 for key, value in row.items()} for row in rows]
    finally:
        await engine.dispose()


@asynccontextmanager
async def observe_slow_pool_operation(factory, operation):
    """Print database and event-loop state if a pool operation outlives
    SLOW_POOL_OPERATION_SECONDS; pytest shows it only for a failed test.

    `loop_lag_seconds` is how late the observation timer fired: a large value
    means the event loop was starved, not that a statement was waiting.
    """
    loop = asyncio.get_running_loop()
    due = loop.time() + SLOW_POOL_OPERATION_SECONDS

    async def observe():
        await asyncio.sleep(SLOW_POOL_OPERATION_SECONDS)
        evidence = {"event": "slow_build_pool_operation", "operation": operation,
                    "loop_lag_seconds": round(loop.time() - due, 3)}
        try:
            evidence["backends"] = await database_waits(factory)
        except Exception as error:
            # Best-effort evidence must not replace the original test failure.
            evidence["backends_unavailable"] = type(error).__name__
        print(json.dumps(evidence, default=str))

    observer = asyncio.create_task(observe())
    try:
        yield
    finally:
        if observer.done():
            pass
        elif loop.time() >= due:
            # The snapshot is being taken; let it finish rather than lose it.
            await asyncio.gather(observer, return_exceptions=True)
        else:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)


@pytest.fixture
async def worker_timeout_diagnostics():
    with observe_worker_waits():
        yield


@asynccontextmanager
async def setup_worker(environment_registry, build_inputs, tmp_path, *, lose_reply=None, **changes):
    from loom_service.app import create_app
    from loom_service.application_management.build_journal import ApplicationBuildJournal
    from loom_service.application_management.build_worker import ApplicationBuildWorker
    from loom_service.config import LoomServiceSettings

    factory, principals, requests, profiles, _, _, _, registry = await setup_application_pool(
        environment_registry, build_inputs, **changes)
    raw = "application_worker_" + uuid4().hex
    hashed = hashlib.sha256(raw.encode()).digest()
    async with factory.begin() as session:
        await session.execute(insert(Token).values(token_hash=hashed, type="pool_machine", scopes=[],
            issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(hours=1)))
        await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=hashed,
            machine_id=principals[0].machine_id, credential_epoch=principals[0].credential_epoch))
    token = tmp_path / "builder-token"
    token.write_text(raw)
    token.chmod(0o600)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory, app.state.pool_profiles = factory, profiles

    class LostReply(httpx.AsyncBaseTransport):
        lost = False
        async def handle_async_request(self, request):
            operation = request.url.path.rsplit("/", 1)[-1]
            started = monotonic()
            outcome = "pending"
            try:
                async with observe_slow_pool_operation(factory, operation):
                    response = await httpx.ASGITransport(app=app).handle_async_request(request)
                outcome = str(response.status_code)
            except BaseException as error:
                outcome = type(error).__name__
                raise
            finally:
                # Captured on failure only; omit credentials, identities and bodies.
                print(f"build pool operation={operation} elapsed={monotonic() - started:.3f}s outcome={outcome}")
            if lose_reply and request.url.path.endswith("/" + lose_reply) and not self.lost:
                self.lost = True
                await response.aclose()
                print(f"build pool operation={operation} injected_lost_reply=true")
                raise httpx.ReadTimeout("response lost after commit")
            return response

    async with httpx.AsyncClient(transport=LostReply()) as http:
        management = client(http, token)
        journal, reader = ApplicationBuildJournal(registry), Reader()
        worker = ApplicationBuildWorker(journal, management, reader)
        yield factory, registry, requests, journal, worker, reader
        await management.close()


async def saved(factory, request):
    async with factory() as session:
        return await session.get(NebiusApplicationBuildAttempt, (request.build.build_id, request.build.attempt))


def finish(job, request):
    claim = request.build
    receipt = {"schema_version": "loom.application-image-publication.v1",
        **claim.model_dump(mode="json", include={"build_id", "attempt", "upload_id", "installation_id",
            "owner_user_id", "owner_team_id", "data_environment_id", "cluster_id"}),
        "source_digest": claim.source.source_digest, "recipe_digest": claim.recipe.digest,
        "schema_revision": claim.recipe.schema_revision, "cpu_arch": claim.recipe.cpu_arch,
        "registry_images": {name: claim.registry_repository + "@sha256:" + digit * 64
            for name, digit in (("service", "a"), ("web", "b"))}}
    job["status"] = {"succeeded": 1, "conditions": [{"type": "Complete", "status": "True"}]}
    job["pods"][0]["status"] = {"phase": "Succeeded",
        "initContainerStatuses": [{"name": name, "restartCount": 0, "state": {"terminated": {"exitCode": 0}}}
            for name in ("prepare", "build")],
        "containerStatuses": [{"name": "publish", "restartCount": 0, "state": {"terminated": {
            "exitCode": 0, "message": json.dumps(receipt)}}}]}
    return receipt


async def observed_build(factory, request):
    row = await saved(factory, request)
    job = await gateway_job(factory, SimpleNamespace(request=request, reservation_id=UUID(row.grant_json["reservation_id"])))
    return job


async def cleanup(factory, request):
    """The fixed gateway's trusted absence boundary, not a worker release shortcut."""
    from loom_service.pool_management.cleanup import PoolCleanupJournal
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal

    row = await saved(factory, request)
    gateway = await machine(factory, request.pool_id, role="gateway")
    journal = PoolCleanupJournal(PoolGatewayJournal(factory))
    snapshot = await journal.prepare(gateway, UUID(row.grant_json["reservation_id"]))
    return await journal.finalize(gateway, snapshot, pod_list_resource_version="91")


async def test_worker_only_marks_ready_after_exact_publication_and_pool_cleanup(
    environment_registry, build_inputs, tmp_path, worker_timeout_diagnostics,
):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, registry, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        first = await saved(factory, request)
        assert first.phase == "running" and first.activation_json is not None and first.activated_json is not None
        reader.job = await observed_build(factory, request)
        publication = finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "settling" and row.settlement_json["publication"] == publication
        assert row.terminal_receipt_json is None
        async with factory() as session:
            pool = await session.get(NebiusPoolRequest, UUID(row.grant_json["reservation_id"]))
            assert pool.phase == "cleanup_intent" and pool.cleanup_observation_id is None
            assert pool.stop_json is not None and pool.drain_json is not None
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "ready" and row.terminal_receipt_json["cleanup_observation_id"]
        assert row.lease_token is None
        status = await registry.status(request.build.build_id, principal=environment_registry[2][0])
        assert status.phase == "ready" and status.attempt == 1


@pytest.mark.parametrize("damage", ["owner", "attempt", "recipe", "repository", "partial", "restart", "publisher-exit"])
async def test_invalid_or_untrusted_publication_is_failed_only_after_cleanup(environment_registry, build_inputs, tmp_path, damage):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        receipt = finish(reader.job, request)
        publisher = reader.job["pods"][0]["status"]["containerStatuses"][0]
        if damage == "owner":
            receipt["owner_user_id"] = str(uuid4())
        elif damage == "attempt":
            receipt["attempt"] += 1
        elif damage == "recipe":
            receipt["recipe_digest"] = "sha256:" + "f" * 64
        elif damage == "repository":
            receipt["registry_images"]["web"] = "foreign.example/web@sha256:" + "f" * 64
        elif damage == "partial":
            receipt["schema_version"] = "loom.application-image-publication-progress.v1"
        elif damage == "restart":
            publisher["restartCount"] = 1
        else:
            publisher["state"]["terminated"]["exitCode"] = 1
        publisher["state"]["terminated"]["message"] = json.dumps(receipt)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "settling" and row.settlement_json["publication"] is None
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        assert (await saved(factory, request)).phase == "failed"


async def test_cancellation_during_external_read_discards_late_result_but_keeps_charge(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        async def cancelled():
            async with factory.begin() as session:
                await session.execute(update(NebiusApplicationBuild).where(NebiusApplicationBuild.build_id == request.build.build_id)
                    .values(desired_state="cancelled"))
        reader.during_read = cancelled
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "settling" and row.settlement_json["publication"] is None
        assert row.settlement_json["outcome"] == "cancelled"
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        assert (await saved(factory, request)).phase == "cancelled"


async def test_concurrent_workers_recover_lost_activation_without_new_attempt_or_consent(environment_registry, build_inputs, tmp_path):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    async with setup_worker(environment_registry, build_inputs, tmp_path, lose_reply="activate") as (factory, _, requests, _, worker, _):
        request = requests[0]
        with pytest.raises(PoolRequestUnconfirmedError):
            await worker.reconcile_once(request.build.build_id, attempt=1)
        before = await saved(factory, request)
        assert before.phase == "queued" and before.activation_json is not None
        await asyncio.gather(*(worker.reconcile_once(request.build.build_id, attempt=1) for _ in range(3)))
        after = await saved(factory, request)
        assert after.phase == "running" and after.activation_json == before.activation_json
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1
            assert await session.scalar(select(func.count()).select_from(NebiusApplicationBuildAttempt)
                .where(NebiusApplicationBuildAttempt.build_id == request.build.build_id)) == 1
