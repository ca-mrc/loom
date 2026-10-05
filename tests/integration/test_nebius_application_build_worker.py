"""Automatic owner builds use real pool SQL/HTTP and cleanup-qualified results."""
from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, insert, select, update

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
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


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
            response = await httpx.ASGITransport(app=app).handle_async_request(request)
            if lose_reply and request.url.path.endswith("/" + lose_reply) and not self.lost:
                self.lost = True
                await response.aclose()
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


async def test_worker_only_marks_ready_after_exact_publication_and_pool_cleanup(environment_registry, build_inputs, tmp_path):
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
