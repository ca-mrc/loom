"""A manager-confirmed cleanup closes exactly one local build handoff."""
from __future__ import annotations

import asyncio
import copy
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError

from loom.db.nebius_pool_outbox_schema import NebiusPoolBuildOutbox
from loom.db.schema import TaskImageMaterialization, TaskImageMaterializationAttempt
from loom_execution_actuator.pool_build_driver import PoolBuildDriver
from loom_execution_actuator.pool_build_runtime import PoolNativeBuildController
from tests.integration.test_nebius_pool_build_driver import selected
from tests.integration.test_nebius_pool_build_outbox import counts, outbox
from tests.integration.test_nebius_pool_build_runtime import Reader, finish, local_rows
from tests.integration.test_nebius_pool_kubernetes import KubernetesAPI
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI, pod
from tests.integration.test_nebius_pool_registry import machine


@asynccontextmanager
async def completed(sessions, tmp_path, *, outcome="success", started=True):
    from loom_service.pool_management.gateway_journal import PoolGatewayJournal
    from loom_service.pool_management.kubernetes import KubernetesPoolGateway

    app, token, participant, request, journal = await selected(sessions, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        driver = PoolBuildDriver(outbox=journal, management=client(http, token))
        handoff = await driver.advance(request.key)
        runtime = await driver.management.native_runtime(handoff.action)
        api = KubernetesAPI(runtime.namespace.uid)
        async with httpx.AsyncClient(base_url="https://kubernetes.example",
                transport=httpx.MockTransport(InventoryAPI(api, []))) as kube:
            gateway = KubernetesPoolGateway(PoolGatewayJournal(sessions), kube)
            principal = await machine(sessions, request.pool_id, role="gateway")
            job = None
            if started:
                for kind in ("ConfigMap", "Job"):
                    await gateway.create(principal, handoff.reservation_id, kind=kind)
                job = copy.deepcopy(next(item for item in api.objects.values() if item["kind"] == "Job"))
                job["pods"] = [pod(job)]
            if outcome in {"success", "invalid"}:
                finish(job, request, invalid=outcome == "invalid")
            elif outcome == "cancelled":
                await journal.request_cancel(request.key)
            else:
                async with sessions.begin() as session:
                    values = ({"lease_epoch": 2, "claimed_by": "successor"} if outcome == "superseded"
                              else {"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)})
                    await session.execute(update(TaskImageMaterialization).values(**values))
            reader = Reader(job)
            controller = PoolNativeBuildController(driver=driver, kubernetes=reader)
            await controller.run_once()
            assert (await journal.get(request.key)).phase == "stop_pending"
            # Real gateway journal + identity checks at the external HTTP boundary.
            if started:
                for kind in ("Job", "ConfigMap"):
                    await gateway.delete(principal, handoff.reservation_id, kind=kind)
            receipt = await gateway.verify_cleanup(principal, handoff.reservation_id)
            yield participant, request, journal, driver, reader, receipt


@pytest.mark.parametrize("outcome", ["success", "invalid", "cancelled", "expired", "superseded"])
async def test_restart_reads_manager_release_without_replaying_results_or_budget(sessions, tmp_path, outcome):
    async with completed(sessions, tmp_path, outcome=outcome) as (participant, request, _journal, driver, reader, receipt):
        before, attempt = await local_rows(sessions, request)
        native = copy.deepcopy(attempt.native_build)
        reads = reader.calls
        recovered = outbox(sessions, participant)
        controller = PoolNativeBuildController(driver=PoolBuildDriver(outbox=recovered, management=driver.management), kubernetes=reader)
        await asyncio.gather(controller.run_once(), controller.run_once())
        released = await recovered.get(request.key)
        assert released.phase == "released" and released.released == receipt
        assert released.attempt_id == attempt.id and not await recovered.pending()
        after, done = await local_rows(sessions, request)
        assert (after.state, after.lease_epoch, after.attempt_count, after.claimed_by, after.registry_images,
                after.failure_reason, after.next_attempt_at) == (
            before.state, before.lease_epoch, before.attempt_count, before.claimed_by, before.registry_images,
            before.failure_reason, before.next_attempt_at)
        assert done.native_build["state"] == "released" and done.native_build["capacity_released_at"]
        assert {key: value for key, value in done.native_build.items() if key not in {"state", "capacity_released_at"}} == {
            key: value for key, value in native.items() if key != "state"}
        await controller.run_once()
        assert reader.calls == reads
        assert (await local_rows(sessions, request))[1].native_build == done.native_build


async def test_released_handoff_allows_new_selection_and_preserves_old_replay(sessions, tmp_path):
    async with completed(sessions, tmp_path, outcome="cancelled") as (participant, request, journal, driver, reader, receipt):
        following = request.model_copy(update={"key": request.key.model_copy(update={"generation": request.key.generation + 1}),
            "build": request.build.model_copy(update={"expected_lease_epoch": 1})})
        with pytest.raises(ValueError):
            await journal.remember(following)
        await PoolNativeBuildController(driver=driver, kubernetes=reader).run_once()
        released = await journal.get(request.key)
        assert (await journal.remember(following)).phase == "selected"
        assert (await driver.advance(following.key)).phase == "active"
        assert await counts(sessions, request.key.local_work_id) == (2, 1, 2)
        assert await outbox(sessions, participant).confirm_release(request.key, receipt) == released
        assert await counts(sessions, request.key.local_work_id) == (2, 1, 2)
        assert [item.request.key for item in await journal.pending()] == [following.key]


async def test_release_without_dispatched_job_preserves_cancel_refund_and_empty_uid(sessions, tmp_path):
    async with completed(sessions, tmp_path, outcome="cancelled", started=False) as (_, request, journal, driver, reader, receipt):
        assert receipt.job_uid is None and receipt.cleanup_observation_id is not None
        await PoolNativeBuildController(driver=driver, kubernetes=reader).run_once()
        assert (await journal.get(request.key)).released == receipt and not reader.calls
        assert await counts(sessions, request.key.local_work_id) == (1, 0, 1)


async def test_delayed_runtime_and_activation_readbacks_cannot_reopen_released_handoff(sessions, tmp_path):
    async with completed(sessions, tmp_path) as (_, request, journal, driver, reader, receipt):
        runtime = await driver.management.native_runtime((await journal.get(request.key)).action)
        stale = runtime.model_copy(update={"receipt": receipt.model_copy(update={
            "phase": "cleanup_intent", "cleanup_observation_id": None})})
        released = await journal.confirm_release(request.key, receipt)
        before = (await local_rows(sessions, request))[1].native_build
        assert not await PoolNativeBuildController(driver=driver, kubernetes=reader)._record(stale)
        assert await journal.confirm_activation(request.key, stale.receipt) == released
        assert (await local_rows(sessions, request))[1].native_build == before


@pytest.mark.parametrize("damage", ["pool", "reservation", "key", "epoch", "digest", "plan", "job", "no-proof", "not-released"])
async def test_release_must_match_retained_attempt_and_manager_identity(sessions, tmp_path, damage):
    async with completed(sessions, tmp_path) as (_, request, journal, _, _, receipt):
        changes = {"pool": {"pool_id": uuid4()}, "reservation": {"reservation_id": uuid4()},
            "key": {"request_key": request.key.model_copy(update={"generation": request.key.generation + 1})},
            "epoch": {"admission_epoch": receipt.admission_epoch + 1}, "digest": {"request_sha256": "b" * 64},
            "plan": {"plan_sha256": "c" * 64}, "job": {"job_uid": uuid4()},
            "no-proof": {"cleanup_observation_id": None},
            "not-released": {"phase": "cleanup_intent", "cleanup_observation_id": None}}[damage]
        with pytest.raises(ValueError):
            await journal.confirm_release(request.key, receipt.model_copy(update=changes))
        assert (await journal.get(request.key)).phase == "stop_pending"
        assert not (await local_rows(sessions, request))[1].native_build.get("capacity_released_at")


@pytest.mark.parametrize("damage", ["missing-stop", "missing-drain", "output", "epoch", "reservation"])
async def test_release_requires_exact_saved_local_stop_drain_and_output(sessions, tmp_path, damage):
    async with completed(sessions, tmp_path) as (_, request, journal, _, _, receipt):
        async with sessions.begin() as session:
            attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
            native = copy.deepcopy(attempt.native_build)
            if damage.startswith("missing-"):
                native.pop("pool_" + damage.removeprefix("missing-"))
            elif damage == "output":
                native["pool_output"]["registry_images"] = []
            elif damage == "epoch":
                native["lease_epoch"] = 99
            else:
                native["pool_reservation_id"] = str(uuid4())
            attempt.native_build = native
        with pytest.raises(ValueError):
            await journal.confirm_release(request.key, receipt)
        assert (await journal.get(request.key)).phase == "stop_pending"


async def test_release_attempt_and_terminal_receipt_roll_back_together(sessions, tmp_path):
    async with completed(sessions, tmp_path) as (_, request, journal, _, _, receipt):
        async with sessions.begin() as session:
            await session.execute(text("CREATE FUNCTION fail_local_release() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN IF NEW.phase = 'released' THEN RAISE EXCEPTION 'injected disk failure'; END IF; RETURN NEW; END $$"))
            await session.execute(text("CREATE TRIGGER fail_local_release BEFORE UPDATE ON nebius_pool_build_outbox "
                "FOR EACH ROW EXECUTE FUNCTION fail_local_release()"))
        with pytest.raises(DBAPIError):
            await journal.confirm_release(request.key, receipt)
        assert (await journal.get(request.key)).phase == "stop_pending"
        assert not (await local_rows(sessions, request))[1].native_build.get("capacity_released_at")
        async with sessions.begin() as session:
            await session.execute(text("DROP TRIGGER fail_local_release ON nebius_pool_build_outbox"))
        assert (await journal.confirm_release(request.key, receipt)).phase == "released"


async def test_sql_retains_release_identity_and_forbids_reopening(sessions, tmp_path):
    async with completed(sessions, tmp_path) as (_, request, journal, _, _, receipt):
        await journal.confirm_release(request.key, receipt)
        with pytest.raises(ValueError):
            await journal.confirm_release(request.key, receipt.model_copy(update={"cleanup_observation_id": uuid4()}))
        async with sessions.begin() as session:
            for values in ({"phase": "stop_pending", "released_json": None},
                           {"released_json": receipt.model_copy(update={"cleanup_observation_id": uuid4()}).model_dump(mode="json")}):
                with pytest.raises(DBAPIError):
                    async with session.begin_nested():
                        await session.execute(update(NebiusPoolBuildOutbox).values(**values))


async def test_lost_release_readback_keeps_local_slot_until_exact_retry(sessions, tmp_path):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    async with completed(sessions, tmp_path) as (_, request, journal, driver, reader, receipt):
        class Lost(httpx.AsyncBaseTransport):
            async def handle_async_request(self, incoming):
                raise httpx.ReadTimeout("readback lost", request=incoming)

        token = tmp_path / "lost-readback-token"
        token.write_text("test-token")
        token.chmod(0o600)
        async with httpx.AsyncClient(transport=Lost()) as http:
            with pytest.raises(PoolRequestUnconfirmedError):
                await PoolNativeBuildController(driver=PoolBuildDriver(outbox=journal,
                    management=client(http, token)), kubernetes=reader).run_once()
        assert (await journal.get(request.key)).phase == "stop_pending"
        await PoolNativeBuildController(driver=driver, kubernetes=reader).run_once()
        assert (await journal.get(request.key)).released == receipt
