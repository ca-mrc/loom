"""Recovery boundaries use real build history, machine auth and pool lifecycle."""
from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_application_build_worker import (
    cleanup,
    finish,
    observed_build,
    saved,
    setup_worker,
)
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def cancel(factory, build_id):
    async with factory.begin() as session:
        await session.execute(update(NebiusApplicationBuild).where(NebiusApplicationBuild.build_id == build_id)
            .values(desired_state="cancelled"))


async def test_waiting_cancel_retains_request_until_real_pool_tombstone(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path, occupied_cpu=3000) as (factory, _, requests, _, worker, _):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        before = await saved(factory, request)
        assert before.phase == "queued" and before.grant_json is None
        await cancel(factory, request.build.build_id)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        after = await saved(factory, request)
        assert after.phase == "cancelled" and after.pool_request_json == before.pool_request_json
        assert after.terminal_receipt_json["phase"] == "cancelled_unstarted"
        async with factory() as session:
            pool = await session.get(NebiusPoolRequest, after.terminal_receipt_json["reservation_id"])
            assert pool.phase == "cancelled_unstarted" and pool.plan_json is None


async def test_unsubmitted_cancellation_never_creates_pool_demand(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, registry, requests, _, worker, _):
        build = await registry.create(principal=environment_registry[2][0], upload_id=requests[0].build.upload_id,
            idempotency_key="cancel-before-dispatch")
        await cancel(factory, build.build_id)
        await worker.reconcile_once(build.build_id, attempt=1)
        async with factory() as session:
            row = await session.get(NebiusApplicationBuildAttempt, (build.build_id, 1))
            assert row.phase == "cancelled" and row.pool_request_json is None and row.terminal_receipt_json is None
            assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_expired_worker_cannot_record_result_or_clear_successor_lease(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, journal, worker, _):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        job = await observed_build(factory, request)
        publication = finish(job, request)
        old = await journal.claim(request.build.build_id, attempt=1)
        runtime = await worker.management.native_runtime((await journal.state(old)).action)
        async with factory.begin() as session:
            await session.execute(update(NebiusApplicationBuildAttempt).where(
                NebiusApplicationBuildAttempt.build_id == old.build_id).values(
                    lease_expires_at=func.clock_timestamp() - timedelta(seconds=1)))
        current = await journal.claim(old.build_id, attempt=1)
        assert current.runner_epoch > old.runner_epoch and current.lease_token != old.lease_token
        for operation in (journal.observe(old, runtime, job), journal.renew(old), journal.release(old)):
            with pytest.raises(ManagementError, match="stale_application_build_lease"):
                await operation
        row = await saved(factory, request)
        assert row.lease_token == current.lease_token and row.settlement_json is None
        await journal.observe(current, runtime, job)
        assert (await saved(factory, request)).settlement_json["publication"] == publication
        await journal.release(current)


@pytest.mark.parametrize("operation", ["stop", "drain"])
async def test_restart_replays_exact_cleanup_after_lost_reply(environment_registry, build_inputs, tmp_path, operation):
    async with setup_worker(environment_registry, build_inputs, tmp_path, lose_reply=operation) as (factory, _, requests, journal, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        with pytest.raises(PoolRequestUnconfirmedError):
            await worker.reconcile_once(request.build.build_id, attempt=1)
        before = await saved(factory, request)
        assert before.phase == "settling" and before.terminal_receipt_json is None
        restarted = type(worker)(type(journal)(journal.registry), worker.management, reader)
        await restarted.reconcile_once(request.build.build_id, attempt=1)
        after = await saved(factory, request)
        assert after.settlement_json == before.settlement_json and after.runner_epoch > before.runner_epoch
        await cleanup(factory, request)
        await restarted.reconcile_once(request.build.build_id, attempt=1)
        assert (await saved(factory, request)).phase == "ready"


async def test_late_cancellation_cannot_turn_saved_output_into_ready_release(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        publication = finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await cancel(factory, request.build.build_id)
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "cancelled" and row.settlement_json["publication"] == publication


@pytest.mark.parametrize("field", ["grant_json", "activation_json", "activated_json", "settlement_json"])
async def test_sql_cannot_erase_saved_build_evidence(environment_registry, build_inputs, tmp_path, field):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, _, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(update(NebiusApplicationBuildAttempt).where(
                    NebiusApplicationBuildAttempt.build_id == request.build.build_id).values({field: None}))
        assert getattr(await saved(factory, request), field) is not None


async def test_successor_attempt_requires_terminal_cleanup(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, _, worker, _):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await cancel(factory, request.build.build_id)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(update(NebiusApplicationBuild).where(
                    NebiusApplicationBuild.build_id == request.build.build_id).values(current_attempt=2))
        assert (await saved(factory, request)).phase == "settling"


@pytest.mark.parametrize("damage", ["namespace_uid", "namespace_name", "effect"])
async def test_cleanup_cannot_substitute_retained_runtime_identity(environment_registry, build_inputs, tmp_path, damage):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, journal, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await cleanup(factory, request)
        lease = await journal.claim(request.build.build_id, attempt=1)
        runtime = await worker.management.native_runtime((await journal.state(lease)).action)
        if damage == "effect":
            runtime = runtime.model_copy(update={"job_effect_id": uuid4()})
        else:
            runtime = runtime.model_copy(update={"namespace": runtime.namespace.model_copy(update={
                "uid" if damage == "namespace_uid" else "name": uuid4() if damage == "namespace_uid" else "foreign"})})
        with pytest.raises(ManagementError, match="application_build_runtime_conflict"):
            await journal.observe(lease, runtime)
        assert (await saved(factory, request)).phase == "settling"
        await journal.release(lease)


async def test_keyset_scan_does_not_repeat_waiting_first_owner(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (_, _, requests, journal, _, _):
        first = await journal.pending(limit=1)
        second = await journal.pending(after=first[0][0], limit=1)
        assert {first[0][0], second[0][0]} == {request.build.build_id for request in requests}
        assert await journal.pending(after=second[0][0], limit=1) == []
        leases = await asyncio.gather(*(journal.claim(identity, attempt=attempt) for identity, attempt in first + second))
        assert await journal.pending() == []
        await asyncio.gather(*(journal.release(lease) for lease in leases))


async def test_slow_read_is_heartbeated_and_drained_before_lease_release(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, journal, worker, reader):
        request = requests[0]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        reader.job = await observed_build(factory, request)
        finish(reader.job, request)
        entered, blocked = asyncio.Event(), asyncio.Event()
        lease_at_read_exit = []

        async def slow_read():
            entered.set()
            try:
                await blocked.wait()
            finally:
                lease_at_read_exit.append((await saved(factory, request)).lease_token)

        reader.during_read = slow_read
        short = type(worker)(journal, worker.management, reader, lease_seconds=3, reconcile_timeout=10)
        work = asyncio.create_task(short.reconcile_once(request.build.build_id, attempt=1))
        try:
            async with asyncio.timeout(8):
                await entered.wait()
                original = await saved(factory, request)
                while True:
                    renewed = await saved(factory, request)
                    if renewed.lease_expires_at > original.lease_expires_at:
                        break
                    await asyncio.sleep(0.05)
                assert renewed.lease_token == original.lease_token and renewed.phase == "running"
        finally:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
        assert len(lease_at_read_exit) == 1 and lease_at_read_exit[0] is not None
        row = await saved(factory, request)
        assert row.lease_token is None and row.settlement_json is None


async def test_expired_consent_without_activation_cancels_reservation(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, journal, worker, _):
        request = requests[0]
        lease = await journal.claim(request.build.build_id, attempt=1, lease_seconds=3)
        receipt = await worker.management.prepare(request)
        await journal.accept_grant(lease, receipt)
        consent = await journal.begin_activation(lease)
        await journal.release(lease)
        async with asyncio.timeout(8):
            while True:
                async with factory() as session:
                    if await session.scalar(select(func.clock_timestamp())) >= consent.not_after:
                        break
                await asyncio.sleep(0.05)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        row = await saved(factory, request)
        assert row.phase == "failed" and row.activated_json is None
        assert row.activation_json == consent.model_dump(mode="json")
        assert row.terminal_receipt_json["phase"] == "cancelled_unstarted"


async def test_automatic_loop_reaches_second_owner_and_shutdown_leaves_no_lease(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path, max_nodes=3) as (factory, _, requests, _, worker, _):
        running = asyncio.create_task(worker.run(concurrency=1, poll_seconds=1))
        try:
            async with asyncio.timeout(15):
                while True:
                    rows = [await saved(factory, request) for request in requests]
                    if all(row.phase == "running" for row in rows):
                        break
                    if running.done():
                        running.result()
                    await asyncio.sleep(0.05)
                assert worker.healthy
                async with factory() as session:
                    assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 2
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        assert not worker.healthy
        stopped = [await saved(factory, request) for request in requests]
        assert all(row.lease_token is None for row in stopped)


@pytest.mark.parametrize("boundary", ["claim", "release"])
@pytest.mark.parametrize("cancel_count", [1, 2])
async def test_shutdown_drains_committed_claim_and_lease_release(
    environment_registry, build_inputs, tmp_path, monkeypatch, boundary, cancel_count,
):
    """Shutdown cannot abandon a committed lease, even during its cleanup."""
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, _, requests, journal, worker, _):
        request = requests[0]
        entered, proceed = asyncio.Event(), asyncio.Event()
        initial = await saved(factory, request)
        original = getattr(journal, boundary)

        async def delayed(*args, **kwargs):
            if boundary == "claim":
                result = await original(*args, **kwargs)
                entered.set()
                await proceed.wait()
                return result
            entered.set()
            await proceed.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(journal, boundary, delayed)
        task = asyncio.create_task(worker.reconcile_once(request.build.build_id, attempt=1))
        try:
            async with asyncio.timeout(10):
                await entered.wait()
                assert (await saved(factory, request)).lease_token is not None
                for _ in range(cancel_count):
                    task.cancel()
                    await asyncio.sleep(0)
        finally:
            proceed.set()
            async with asyncio.timeout(10):
                results = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError)
        row = await saved(factory, request)
        assert row.lease_token is None and row.lease_expires_at is None
        if boundary == "claim":
            assert row.phase == "queued" and row.pool_request_json == initial.pool_request_json
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_explicit_concurrent_retry_runs_new_pool_generation_after_old_cleanup(environment_registry, build_inputs, tmp_path):
    async with setup_worker(environment_registry, build_inputs, tmp_path) as (factory, registry, requests, _, worker, _):
        request = requests[0]
        owner, _ = environment_registry[2]
        await worker.reconcile_once(request.build.build_id, attempt=1)
        await registry.cancel(request.build.build_id, principal=owner, attempt=1)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        with pytest.raises(ManagementError, match="application_build_cleanup_required"):
            await registry.retry(request.build.build_id, principal=owner, attempt=1)
        await cleanup(factory, request)
        await worker.reconcile_once(request.build.build_id, attempt=1)
        replies = await asyncio.gather(*(registry.retry(request.build.build_id, principal=owner, attempt=1) for _ in range(3)))
        assert all(reply.attempt == 2 for reply in replies)
        await worker.reconcile_once(request.build.build_id, attempt=2)
        async with factory() as session:
            old, new = [await session.get(NebiusApplicationBuildAttempt, (request.build.build_id, number)) for number in (1, 2)]
            assert old.phase == "cancelled" and old.terminal_receipt_json["phase"] == "released"
            assert new.phase == "running" and new.pool_request_json["key"]["generation"] == 2
            assert new.grant_json["reservation_id"] != old.grant_json["reservation_id"]
            rows = (await session.scalars(select(NebiusPoolRequest).order_by(NebiusPoolRequest.generation))).all()
            assert [(row.generation, row.phase) for row in rows] == [(1, "released"), (2, "create_intent")]
