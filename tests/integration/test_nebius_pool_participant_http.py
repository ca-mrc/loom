"""The real management routes expose only dedicated participant handoffs."""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_schema import NebiusPoolRequest
from loom.db.schema import Token
from loom.nebius_pool_contract import PoolActivationV1, PoolRequestActionV1
from loom.pipeline.keys import canonical_digest
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_nebius_pool_build_admission import mixed_setup
from tests.integration.test_nebius_pool_build_outbox import counts, local_setup, outbox
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def setup(sessions, tmp_path, *, credential_role="participant", **changes):
    participants, principals, executions, builds, profiles, _ = await mixed_setup(sessions, **changes)
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management",
        db_url="postgresql+asyncpg://unused:unused@localhost/unused"))
    app.state.session_factory, app.state.pool_profiles = sessions, profiles
    # Enroll a real credential for the same participant machine; retain the
    # production dedicated binding/role checks rather than overriding auth.
    from sqlalchemy import insert

    from loom.db.nebius_pool_schema import NebiusPoolMachine, NebiusPoolMachineCredential

    raw = "loom_pool_" + uuid4().hex + uuid4().hex
    token_hash = hashlib.sha256(raw.encode()).digest()
    async with sessions.begin() as session:
        old = await session.get(Token, principals[0].token_hash)
        machine_id = principals[0].machine_id
        if credential_role != "participant":
            machine_id = uuid4()
            await session.execute(insert(NebiusPoolMachine).values(machine_id=machine_id,
                pool_id=participants[0].pool_id, participant_id=None, role=credential_role,
                credential_epoch=1, phase="active"))
        await session.execute(insert(Token).values(token_hash=token_hash, type="pool_machine", scopes=[],
            issued_at=old.issued_at, expires_at=old.expires_at))
        await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=token_hash,
            machine_id=machine_id, credential_epoch=principals[0].credential_epoch))
    token = tmp_path / "participant-token"
    token.write_text(raw)
    token.chmod(0o600)
    return app, raw, token, participants, executions, builds


def action(request):
    return PoolRequestActionV1(pool_id=request.pool_id, request_key=request.key,
        admission_epoch=request.admission_epoch,
        request_sha256=canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"))


def client(http, token):
    from loom_execution_actuator.pool_client import PoolClient

    return PoolClient(origin="https://management.example", bearer_token_file=token, timeout_seconds=5, client=http)


async def test_actual_participant_client_and_routes_prepare_replay_activate_and_status(sessions, tmp_path):
    app, _, token, _, executions, _ = await setup(sessions, tmp_path)
    request = executions[0]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        first = await management.prepare(request)
        assert first.phase == "reserved"
        assert await management.prepare(request) == first
        activation = PoolActivationV1(action=action(request), not_after=request.deadline_at)
        activated = await management.activate(activation)
        assert activated.phase == "create_intent" and activated.reservation_id == first.reservation_id
        assert await management.status(action(request)) == activated
        assert await management.activate(activation) == activated
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1
        row = await session.get(NebiusPoolRequest, first.reservation_id)
        assert row.plan_json["job"]["kind"] == "Job" and row.phase == "create_intent"


async def test_native_client_waits_without_attempt_then_cancels_exact_unstarted_selection(sessions, tmp_path):
    app, _, token, _, _, builds = await setup(sessions, tmp_path, occupied_cpu=3000)
    request = builds[0]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        waiting = await management.prepare(request)
        assert waiting.phase == "waiting" and not hasattr(waiting, "reservation_id")
        assert (await management.status(action(request))).phase == "waiting"
        cancelled = await management.cancel_unstarted(action(request))
        assert cancelled.phase == "cancelled_unstarted"
        assert await management.cancel_unstarted(action(request)) == cancelled
        assert await management.prepare(request) == cancelled


async def test_durable_local_selection_through_real_management_http_claims_only_after_grant(sessions, tmp_path):
    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    _, local_request, _ = await local_setup(sessions, environment_id=participants[0].environment_id)
    participant = participants[0]
    request = local_request.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": local_request.key.model_copy(update={"participant_id": participant.participant_id}),
        "origin": local_request.origin.model_copy(update={"data_environment_id": participant.environment_id})})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        management = client(http, token)
        reserved = await management.prepare(request)
        assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
        attached = await journal.accept_grant(request.key, reserved)
        assert attached.phase == "attached"
        # Fresh objects simulate process loss after both durable commits.
        replay = await client(http, token).prepare((await outbox(sessions, participant).get(request.key)).request)
        assert replay == reserved
        assert await journal.accept_grant(request.key, replay) == attached
        pending = await journal.begin_activation(request.key)
        assert (await management.activate(pending.activation)).phase == "create_intent"
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)


@pytest.mark.parametrize("damage", ["ordinary", "admin", "worker", "revoked", "foreign-pool", "foreign-participant",
                                    "observer", "gateway", "old-epoch"])
async def test_participant_routes_reject_other_authority_without_creating_demand(sessions, tmp_path, damage):
    app, raw, _, participants, executions, _ = await setup(sessions, tmp_path,
        credential_role=damage if damage in {"observer", "gateway"} else "participant")
    request = executions[0]
    if damage in {"admin", "worker", "revoked"}:
        async with sessions.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == hashlib.sha256(raw.encode()).digest()).values(
                **({"type": damage} if damage != "revoked" else {"revoked_at": datetime.now(UTC)})))
    elif damage == "ordinary":
        raw = "ordinary-user-token"
    elif damage == "foreign-pool":
        request = request.model_copy(update={"pool_id": uuid4()})
    elif damage == "foreign-participant":
        request = request.model_copy(update={"key": request.key.model_copy(update={"participant_id": participants[1].participant_id})})
    elif damage == "old-epoch":
        from loom.db.nebius_pool_schema import NebiusPoolMachine

        async with sessions.begin() as session:
            await session.execute(update(NebiusPoolMachine).where(
                NebiusPoolMachine.participant_id == participants[0].participant_id).values(credential_epoch=2))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        response = await http.post(f"/internal/pools/v1/{request.pool_id}/prepare", json=request.model_dump(mode="json"),
                                   headers={"Authorization": "Bearer " + raw})
    assert response.status_code in {401, 403}
    assert raw not in response.text
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_conflicting_replay_and_missing_installed_profiles_do_not_open_admission(sessions, tmp_path):
    app, raw, token, _, executions, _ = await setup(sessions, tmp_path)
    request = executions[0]
    path = f"/internal/pools/v1/{request.pool_id}/prepare"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.example") as http:
        assert (await client(http, token).prepare(request)).phase == "reserved"
        changed = request.model_dump(mode="json")
        changed["origin"]["submission_id"] = str(uuid4())
        assert (await http.post(path, json=changed, headers={"Authorization": "Bearer " + raw})).status_code == 409
        del app.state.pool_profiles
        new = request.model_dump(mode="json")
        new["key"]["local_work_id"] = str(uuid4())
        assert (await http.post(path, json=new, headers={"Authorization": "Bearer " + raw})).status_code == 503
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1


async def test_participant_routes_absent_from_owner_application_apis():
    from fastapi import FastAPI

    from loom_service.app import register_api_routes

    app = FastAPI()
    register_api_routes(app, management=False, include_local_execution=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://owner.example") as http:
        for operation in ("prepare", "activate", "status", "cancel-unstarted"):
            assert (await http.post(f"/internal/pools/v1/{uuid4()}/{operation}", json={})).status_code == 404


@pytest.mark.parametrize("lost_operation", ["prepare", "activate"])
async def test_lost_committed_http_reply_recovers_the_same_grant_and_local_attempt(sessions, tmp_path, lost_operation):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    app, _, token, participants, _, _ = await setup(sessions, tmp_path)
    _, selected, _ = await local_setup(sessions, environment_id=participants[0].environment_id)
    participant = participants[0]
    request = selected.model_copy(update={"pool_id": participant.pool_id,
        "admission_epoch": participant.admission_epoch, "participant_revision": participant.binding_revision,
        "key": selected.key.model_copy(update={"participant_id": participant.participant_id}),
        "origin": selected.origin.model_copy(update={"data_environment_id": participant.environment_id})})
    journal = outbox(sessions, participant)
    await journal.remember(request)
    calls = []

    class LostReply(httpx.AsyncBaseTransport):
        def __init__(self):
            self.inner = httpx.ASGITransport(app=app)
            self.lost = False

        async def handle_async_request(self, incoming):
            calls.append(incoming.url.path.rsplit("/", 1)[-1])
            response = await self.inner.handle_async_request(incoming)
            if calls[-1] == lost_operation and not self.lost:
                self.lost = True
                assert response.status_code == 200
                await response.aclose()
                raise httpx.ReadError("reply lost after management commit")
            return response

    async with httpx.AsyncClient(transport=LostReply()) as http:
        management = client(http, token)
        if lost_operation == "prepare":
            with pytest.raises(PoolRequestUnconfirmedError):
                await management.prepare(request)
            assert calls == ["prepare"]
            assert (await journal.get(request.key)).phase == "selected"
            assert await counts(sessions, request.key.local_work_id) == (0, 0, 0)
        reserved = await client(http, token).prepare(request)
        attached = await journal.accept_grant(request.key, reserved)
        pending = await journal.begin_activation(request.key)
        if lost_operation == "activate":
            with pytest.raises(PoolRequestUnconfirmedError):
                await management.activate(pending.activation)
            assert calls == ["prepare", "activate"]
            assert (await client(http, token).status(attached.action)).phase == "create_intent"
        else:
            assert (await management.activate(pending.activation)).phase == "create_intent"
        assert await outbox(sessions, participant).accept_grant(request.key, reserved) == pending
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1
        assert (await session.get(NebiusPoolRequest, reserved.reservation_id)).phase == "create_intent"
    assert await counts(sessions, request.key.local_work_id) == (1, 1, 1)
