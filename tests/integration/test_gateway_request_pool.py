"""Real PostgreSQL + actual hosted JWT HTTP facade, mocked provider transport."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import mint_step_jwt
from loom.db.schema import (
    GatewayDispatchReceipt,
    LlmCall,
    LlmCallIntent,
    ModelSwitchPlan,
    ProviderConnection,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
    Secret,
    ServiceExecutionLease,
    TeamQuota,
    TerminusAgentExecution,
    TerminusAgentRunAttempt,
    Trial,
)
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway import dispatch_audit
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


@pytest.fixture
async def pool_setup(monkeypatch: pytest.MonkeyPatch, postgres_url: str):
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode())
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 10)
    settings = GatewaySettings(
        _env_file=None,
        llm_retry_base_backoff_sec=0,
        llm_retry_jitter_sec=0,
        llm_retry_max_attempts=3,
    )
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    root, _second, _third = ids = [uuid4() for _ in range(3)]
    now = datetime.now(UTC)
    async with sessions() as session:
        trial_id, target = await _seed_ready_trial(session, now=now)
        lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        trial = await session.get(Trial, trial_id)
        team_id = lease.team_id
        trial.config = {
            "agent_name": "terminus-2",
            "agent_model": {"provider": "openai", "name": "student"},
            "multi_model": {
                "enabled": True,
                "policy": "student_teacher_student",
                "secondary_model": {"provider": "openai", "name": "teacher"},
            },
        }
        trial.state = "running"
        lease.observed_state = "running"
        session.add(TeamQuota(team_id=team_id))
        store = LocalEncryptedSecretStore(session)
        for i, connection_id in enumerate(ids):
            ref = await store.put(namespace=f"team:{team_id}", value=f"test-credential-{i}")
            session.add(
                ProviderConnection(
                    id=connection_id,
                    team_id=team_id,
                    provider_type="openai-compatible",
                    display_name=f"pool-{i}",
                    base_url=f"https://provider-{i}.example.test/v1",
                    upstream_host=f"provider-{i}.example.test",
                    encrypted_api_key_ref=ref,
                    status="valid",
                    pricing_source="operator-supplied",
                    pricing_data={"input_usd_per_1m": (i + 1) * 10, "output_usd_per_1m": 0},
                    allowed_models=["student", "teacher"],
                    created_by="test",
                )
            )
        await session.flush()
        for connection_id in ids:
            for name in ["student", "teacher"]:
                session.add(
                    ProviderModelCache(
                        provider_connection_id=connection_id,
                        model_id=name,
                        last_preflight_status="valid",
                    )
                )
        trial.provider_connection_id = root
        session.add(
            ProviderRequestPool(
                connection_id=root,
                required_models=["student", "teacher"],
                attempt_timeout_sec=0.1,
                cooldown_sec=30,
            )
        )
        await session.flush()
        for i, connection_id in enumerate(ids):
            session.add(
                ProviderRequestPoolMember(
                    pool_connection_id=root,
                    connection_id=connection_id,
                    ordinal=i,
                    quota_group="az" if i == 0 else "yibu",
                )
            )
        await session.commit()
    captures: dict[str, Any] = {"requests": [], "handler": None}

    async def provider(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)
        if captures["handler"]:
            response = await captures["handler"](request)
            if response is not None:
                return response
        model = json.loads(request.content)["model"]
        return httpx.Response(
            200,
            json={
                "id": "completion",
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "answer"},
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
            },
        )

    apps = []
    for _ in range(2):
        app = create_app(settings)
        app.state.settings = settings
        app.state.session_factory = sessions
        app.state.rate_card_cache = RateCardCache(session_factory=sessions, ttl_sec=60)
        app.state.upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app.state.egress_client_pool = EgressClientPool(
            upstream_client=app.state.upstream_client,
            proxy_url="",
            upstream_timeout_sec=200,
        )
        apps.append(app)

    def token(deadline: datetime | None = None) -> str:
        return mint_step_jwt(
            team_id=team_id,
            trial_id=trial_id,
            step_id="agent",
            ttl_sec=300,
            signing_key=settings.step_jwt_signing_key.get_secret_value(),
            provider_connection_id=root,
            provider_connection_id_bound=True,
            step_jwt_id=uuid4(),
            attempt_deadline_wall_clock=deadline or now + timedelta(seconds=120),
            service_execution_lease_id=lease.id,
            service_execution_generation=lease.generation,
            service_execution_role="attempt",
            service_execution_runtime_contract_sha256=lease.runtime_contract_sha256,
            service_execution_candidate_sha="1" * 40,
            service_execution_task_revision_sha256="sha256:" + "2" * 64,
            service_execution_command_identity_sha256="sha256:" + "3" * 64,
        )

    data = dict(
        apps=apps,
        sessions=sessions,
        ids=ids,
        root=root,
        trial_id=trial_id,
        team_id=team_id,
        captures=captures,
        jwt=token(),
        token=token,
        settings=settings,
        lease_id=lease.id,
    )
    try:
        yield data
    finally:
        for app in apps:
            await app.state.egress_client_pool.aclose()
            await app.state.upstream_client.aclose()
        async with sessions() as session:
            await session.execute(delete(LlmCall).where(LlmCall.trial_id == trial_id))
            await session.execute(
                update(Trial).where(Trial.id == trial_id).values(provider_connection_id=None)
            )
            await session.execute(
                delete(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == trial_id)
            )
            await session.execute(
                delete(ProviderRequestPool).where(ProviderRequestPool.connection_id == root)
            )
            await session.execute(delete(ProviderConnection).where(ProviderConnection.id.in_(ids)))
            await session.execute(
                delete(Secret).where(Secret.ref.startswith(f"loom://team:{team_id}/"))
            )
            await session.commit()
        await engine.dispose()


async def post(
    p: dict,
    body: dict | None = None,
    *,
    replica: int = 0,
    jwt: str | None = None,
    path: str = "/openai/v1/chat/completions",
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=p["apps"][replica]), base_url="http://gateway.test"
    ) as client:
        return await client.post(
            path,
            headers={"Authorization": "Bearer " + (jwt or p["jwt"])},
            json=body
            or {"model": "student", "messages": [{"role": "user", "content": "first observation"}]},
        )


async def receipts(p: dict):
    async with p["sessions"]() as session:
        return list(
            (
                await session.scalars(
                    select(GatewayDispatchReceipt)
                    .where(
                        GatewayDispatchReceipt.trial_id == p["trial_id"],
                    )
                    .order_by(GatewayDispatchReceipt.admitted_at)
                )
            ).all()
        )


async def test_round_robin_shared_by_replicas_and_physical_member_cost(pool_setup: dict) -> None:
    p = pool_setup
    for replica in [0, 1, 0, 1, 0, 1]:
        response = await post(p, replica=replica)
        assert response.status_code == 200, response.text
    assert [r.url.host for r in p["captures"]["requests"]] == [
        f"provider-{i}.example.test" for i in [0, 1, 2, 0, 1, 2]
    ]
    rows = await receipts(p)
    assert [r.provider_connection_id for r in rows] == p["ids"] * 2
    assert all(r.pool_connection_id == p["root"] for r in rows)
    async with p["sessions"]() as session:
        calls = list(
            (
                await session.scalars(
                    select(LlmCall)
                    .where(
                        LlmCall.trial_id == p["trial_id"],
                    )
                    .order_by(LlmCall.captured_at)
                )
            ).all()
        )
    assert sorted(round(float(c.cost_usd), 6) for c in calls) == [
        0.001,
        0.001,
        0.002,
        0.002,
        0.003,
        0.003,
    ]
    assert all(
        c.provider_extras["_loom_request_pool"]["pool_connection_id"] == str(p["root"])
        for c in calls
    )


async def test_timeout_spare_success_preserves_history_and_marks_uncertain_cost(
    pool_setup: dict,
) -> None:
    p = pool_setup

    async def fail_first(request):
        if request.url.host == "provider-0.example.test":
            raise httpx.ReadTimeout("test-credential-0 must never enter logs", request=request)

    p["captures"]["handler"] = fail_first
    body = {
        "model": "student",
        "messages": [
            {"role": "user", "content": "initial"},
            {"role": "assistant", "content": "completed prior turn"},
            {"role": "user", "content": "terminal observation"},
        ],
    }
    response = await post(p, body)
    assert response.status_code == 200, response.text
    sent = p["captures"]["requests"]
    assert [r.url.host for r in sent] == ["provider-0.example.test", "provider-1.example.test"]
    assert sent[0].content == sent[1].content
    assert sent[0].headers["X-Request-ID"] != sent[1].headers["X-Request-ID"]
    rows = await receipts(p)
    assert rows[0].request_id == rows[1].request_id
    assert [r.provider_outcome for r in rows] == ["transport_error", "response_received"]
    assert rows[1].failover_reason == "upstream_timeout"
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
        assert call.attempt == 2 and float(call.cost_usd) == pytest.approx(0.002)
        assert call.provider_extras["_loom_request_pool"]["possible_duplicate_inference"] is True
        assert "test-credential-0" not in json.dumps(call.provider_extras)
        assert "test-credential-1" not in json.dumps(call.provider_extras)
    for model in ["teacher", "student"]:
        body["messages"].extend(
            [
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "next observation"},
            ]
        )
        body["model"] = model
        assert (await post(p, body)).status_code == 200
    assert [json.loads(r.content)["model"] for r in sent] == [
        "student",
        "student",
        "teacher",
        "student",
    ]


@pytest.mark.parametrize(
    "addition",
    [
        {"stream": True},
        {"previous_response_id": "prior"},
        {"conversation": "account-local"},
        {"file_id": "account-local"},
        {"extra_body": {"previous_response_id": "prior"}},
        {"extra_body": {"tools": [{"type": "web_search"}]}},
        {"tools": [{"type": "web_search"}]},
        {"messages": [{"role": "user", "content": [{"type": "file", "file_id": "account-local"}]}]},
    ],
)
async def test_stateful_and_streaming_rejected_without_dispatch(
    pool_setup: dict, addition: dict
) -> None:
    body = {"model": "student", "messages": [{"role": "user", "content": "hi"}], **addition}
    response = await post(pool_setup, body)
    assert response.status_code == 400, response.text
    assert not pool_setup["captures"]["requests"]
    assert not await receipts(pool_setup)


async def test_responses_and_generic_jwt_cannot_bypass_pool(pool_setup: dict) -> None:
    p = pool_setup
    response = await post(p, {"model": "student", "input": "hello"}, path="/openai/v1/responses")
    assert response.status_code == 400, response.text
    platform = await post(
        p,
        {"model": "student", "messages": [{"role": "user", "content": "hi"}]},
        path="/v1/messages",
    )
    assert platform.status_code == 400, platform.text
    generic = mint_step_jwt(
        team_id=p["team_id"],
        trial_id=p["trial_id"],
        step_id="agent",
        ttl_sec=300,
        signing_key=p["settings"].step_jwt_signing_key.get_secret_value(),
        provider_connection_id=p["root"],
    )
    assert (await post(p, jwt=generic)).status_code == 403
    assert not p["captures"]["requests"]


@pytest.mark.parametrize("explicit", [False, True])
async def test_only_explicit_invalid_credential_is_quarantined(
    pool_setup: dict, explicit: bool
) -> None:
    p = pool_setup

    async def auth_failure(request):
        if request.url.host == "provider-0.example.test":
            return httpx.Response(
                401,
                json={
                    "error": {
                        "code": "invalid_api_key" if explicit else "unknown_proxy_failure",
                        "message": "bad test-credential-0",
                    }
                },
            )

    p["captures"]["handler"] = auth_failure
    response = await post(p)
    assert response.status_code == (200 if explicit else 401), response.text
    assert "test-credential-0" not in response.text
    async with p["sessions"]() as session:
        member = await session.get(ProviderRequestPoolMember, (p["root"], p["root"]))
        assert member.enabled is (not explicit)
    assert len(p["captures"]["requests"]) == (2 if explicit else 1)


async def test_shared_quota_429_cools_both_yibu_members_and_honors_retry_after(
    pool_setup: dict,
) -> None:
    p = pool_setup
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.next_member = 1
        await session.commit()

    async def quota_failure(request):
        if request.url.host == "provider-1.example.test":
            return httpx.Response(429, headers={"Retry-After": "0.05"}, json={"error": "quota"})

    p["captures"]["handler"] = quota_failure
    started = asyncio.get_running_loop().time()
    response = await post(p)
    assert response.status_code == 200, response.text
    assert asyncio.get_running_loop().time() - started >= 0.05
    assert [r.url.host for r in p["captures"]["requests"]] == [
        "provider-1.example.test",
        "provider-0.example.test",
    ]
    async with p["sessions"]() as session:
        for member_id in p["ids"][1:]:
            member = await session.get(ProviderRequestPoolMember, (p["root"], member_id))
            assert member.cooldown_until > datetime.now(UTC)


async def test_concurrent_requests_rotate_without_any_loom_concurrency_cap(
    pool_setup: dict,
) -> None:
    p = pool_setup
    released = asyncio.Event()
    started = asyncio.Event()
    active = 0
    maximum = 0

    async def block(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if active == 12:
            started.set()
        await released.wait()
        active -= 1

    p["captures"]["handler"] = block
    async with p["sessions"]() as session:
        await session.execute(update(ProviderRequestPool).values(attempt_timeout_sec=10))
        await session.commit()
    calls = [asyncio.create_task(post(p, replica=i % 2)) for i in range(12)]
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert maximum == 12
        assert len(p["captures"]["requests"]) == 12
        rows = await receipts(p)
        assert all(
            sum(r.provider_connection_id == member for r in rows) == 4 for member in p["ids"]
        )
    finally:
        released.set()
        responses = await asyncio.gather(*calls)
    assert all(response.status_code == 200 for response in responses)
    for request in p["captures"]["requests"]:
        index = int(request.url.host.split(".")[0].split("-")[-1])
        assert request.headers["Authorization"] == f"Bearer test-credential-{index}"


async def test_signed_deadline_stops_without_spare_dispatch(pool_setup: dict) -> None:
    p = pool_setup
    async with p["sessions"]() as session:
        await session.execute(update(ProviderRequestPool).values(attempt_timeout_sec=10))
        await session.commit()

    async def slow(request):
        await asyncio.sleep(2)

    p["captures"]["handler"] = slow
    response = await post(p, jwt=p["token"](datetime.now(UTC) + timedelta(seconds=0.3)))
    assert response.status_code == 504, response.text
    assert "agent_timeout" in response.text
    assert len(p["captures"]["requests"]) == 1
    assert (await receipts(p))[0].provider_outcome == "deadline"


async def test_exhausted_timeout_records_last_member_attempt_and_redacts_errors(
    pool_setup: dict,
    caplog: pytest.LogCaptureFixture,
) -> None:
    p = pool_setup
    caplog.set_level("INFO")

    async def timeout(request):
        raise httpx.ReadTimeout("test-credential-0 test-credential-1 test-credential-2")

    p["captures"]["handler"] = timeout
    response = await post(p)
    assert response.status_code == 504, response.text
    assert len(p["captures"]["requests"]) == 3
    assert all(f"test-credential-{i}" not in response.text + caplog.text for i in range(3))
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
        assert call.attempt == 3
        assert call.provider_extras["_loom_request_pool"]["provider_connection_id"] == str(
            p["ids"][2]
        )
        assert call.provider_extras["_loom_request_pool"]["possible_duplicate_inference"] is True
    assert [row.provider_connection_id for row in await receipts(p)] == p["ids"]


async def test_live_lease_revocation_stops_timeout_failover(pool_setup: dict) -> None:
    p = pool_setup

    async def revoked(request):
        async with p["sessions"]() as session:
            await session.execute(
                update(ServiceExecutionLease)
                .where(
                    ServiceExecutionLease.id == p["lease_id"],
                )
                .values(revoked_at=datetime.now(UTC))
            )
            await session.commit()
        raise httpx.ReadTimeout("simulated upstream timeout")

    p["captures"]["handler"] = revoked
    response = await post(p)
    assert response.status_code == 403, response.text
    assert len(p["captures"]["requests"]) == 1


async def test_cancellation_stops_without_failover(pool_setup: dict) -> None:
    p = pool_setup
    entered = asyncio.Event()

    async def wait(request):
        entered.set()
        await asyncio.Event().wait()

    p["captures"]["handler"] = wait
    task = asyncio.create_task(post(p))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(p["captures"]["requests"]) == 1
    # ASGI repeated cancellation may interrupt the bounded observation write.
    # The durable admission remains uncertain rather than claiming completion.
    assert (await receipts(p))[0].provider_outcome in {"cancelled", "admitted"}


@pytest.mark.parametrize("change", ["disabled", "invalid", "rejected_model", "hidden_teacher"])
async def test_live_member_eligibility_rechecked(pool_setup: dict, change: str) -> None:
    p = pool_setup
    async with p["sessions"]() as session:
        if change == "disabled":
            member = await session.get(ProviderRequestPoolMember, (p["root"], p["root"]))
            member.enabled = False
        elif change == "invalid":
            await session.execute(
                update(ProviderConnection)
                .where(
                    ProviderConnection.id == p["root"],
                )
                .values(status="invalid")
            )
        else:
            model = await session.get(ProviderModelCache, (p["root"], "teacher"))
            if change == "rejected_model":
                model.last_preflight_status = "failed"
                model.last_preflight_error_code = "model_not_found"
                model.last_preflight_http_status = 404
            else:
                model.visible = False
        await session.commit()
    response = await post(p)
    assert response.status_code == 200, response.text
    assert p["captures"]["requests"][0].url.host == "provider-1.example.test"


async def test_correlated_student_teacher_student_history_survives_timeout(
    pool_setup: dict,
) -> None:
    p = pool_setup
    async with p["sessions"]() as session:
        session.add(
            ModelSwitchPlan(
                trial_id=p["trial_id"],
                mix_mode="student_teacher_student",
                k1=2,
                k2=3,
                teacher_episodes=1,
                seed="fixture",
                prng_version="fixture",
                student_model_snapshot={"provider": "openai", "name": "student"},
                teacher_model_snapshot={"provider": "openai", "name": "teacher"},
                provider_connection_id=p["root"],
            )
        )
        await session.commit()
    failed = False

    async def once(request):
        nonlocal failed
        if not failed:
            failed = True
            raise httpx.ReadTimeout("current call timed out")

    p["captures"]["handler"] = once
    execution_id, run_attempt_id = uuid4(), uuid4()
    async with p["sessions"]() as session:
        session.add(
            TerminusAgentExecution(id=execution_id, trial_id=p["trial_id"], step_id="agent")
        )
        await session.flush()
        session.add(
            TerminusAgentRunAttempt(id=run_attempt_id, execution_id=execution_id, attempt_number=1)
        )
        await session.commit()
    call_ids = []
    history = [{"role": "user", "content": "initial terminal observation"}]
    for episode, (model, role) in enumerate(
        [
            ("student", "student"),
            ("teacher", "teacher"),
            ("student", "student"),
        ],
        start=1,
    ):
        call_id = uuid4()
        call_ids.append(call_id)
        body = {
            "model": model,
            "messages": list(history),
            "loom_client_call_id": str(call_id),
            "loom_agent_execution_id": str(execution_id),
            "loom_agent_run_attempt_id": str(run_attempt_id),
            "loom_episode": episode,
            "loom_call_ordinal": episode,
            "loom_requested_model": "openai/" + model,
            "loom_role": role,
        }
        response = await post(p, body)
        assert response.status_code == 200, response.text
        history.extend(
            [
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": f"terminal observation {episode}"},
            ]
        )
    sent = p["captures"]["requests"]
    assert len(sent) == 4 and sent[0].content == sent[1].content
    assert all(not any(k.startswith("loom_") for k in json.loads(r.content)) for r in sent)
    assert json.loads(sent[-1].content)["messages"] == history[:-2]
    async with p["sessions"]() as session:
        calls = list(
            (
                await session.scalars(
                    select(LlmCall)
                    .where(
                        LlmCall.trial_id == p["trial_id"],
                    )
                    .order_by(LlmCall.captured_at)
                )
            ).all()
        )
        intents = list(
            (
                await session.scalars(
                    select(LlmCallIntent).where(
                        LlmCallIntent.trial_id == p["trial_id"],
                    )
                )
            ).all()
        )
        assert [c.client_call_id for c in calls] == call_ids
        assert [c.role for c in calls] == ["student", "teacher", "student"]
        assert all(c.correlation_status == "correlated" for c in calls)
        assert len(intents) == 3
        plan = await session.scalar(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == p["trial_id"])
        )
        assert plan.provider_connection_id == p["root"]
