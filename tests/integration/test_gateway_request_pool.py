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
from sqlalchemy import delete, event, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.auth import mint_step_jwt
from loom.db.schema import (
    GatewayDispatchQueue,
    GatewayDispatchReceipt,
    LlmCall,
    LlmCallIntent,
    ModelSwitchPlan,
    PriceCatalog,
    ProviderConnection,
    ProviderConnectionTimeoutHealth,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolBinding,
    ProviderRequestPoolGroupCap,
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
from loom_llm_gateway.quota_queue import decide_group_queue
from loom_llm_gateway.rate_card import RateCardCache
from loom_llm_gateway.request_pool import PoolDispatchAudit
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

    def token(deadline: datetime | None = None, *, trial_id=trial_id, lease=lease) -> str:
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
        target=target,
        trial_ids=[trial_id],
    )
    try:
        yield data
    finally:
        for app in apps:
            await app.state.egress_client_pool.aclose()
            await app.state.upstream_client.aclose()
        async with sessions() as session:
            await session.execute(delete(LlmCall).where(LlmCall.trial_id.in_(data["trial_ids"])))
            await session.execute(
                update(Trial).where(Trial.id.in_(data["trial_ids"])).values(provider_connection_id=None)
            )
            await session.execute(
                delete(ModelSwitchPlan).where(ModelSwitchPlan.trial_id.in_(data["trial_ids"]))
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
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    request_headers = {"Authorization": "Bearer " + (jwt or p["jwt"])}
    if headers:
        request_headers.update(headers)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=p["apps"][replica]), base_url="http://gateway.test"
    ) as client:
        return await client.post(
            path,
            headers=request_headers,
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


async def _another_pool_trial(p: dict) -> dict:
    now = datetime.now(UTC)
    trial_id = uuid4()
    async with p["sessions"]() as session:
        original = await session.get(Trial, p["trial_id"])
        assert original is not None
        trial = Trial(
            id=trial_id, team_id=original.team_id, task_id=original.task_id,
            config=original.config, requires_caps=original.requires_caps,
            state="queued", attempt_count=0, provider_connection_id=p["root"],
        )
        session.add(trial)
        await session.flush()
        lease = await _reserve(session, trial_id=trial_id, target=p["target"], now=now)
        trial.state = "running"
        lease.observed_state = "running"
        await session.commit()
    p["trial_ids"].append(trial_id)
    return {**p, "trial_id": trial_id, "lease_id": lease.id,
            "jwt": p["token"](trial_id=trial_id, lease=lease)}


async def test_distinct_sticky_trials_admit_without_waiting_for_the_other_policy(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = pool_setup
    second = await _another_pool_trial(p)
    assert (await post(p)).status_code == 200
    assert (await post(second, replica=1)).status_code == 200
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 1)
    entered, release = asyncio.Event(), asyncio.Event()
    original = PoolDispatchAudit._eligible

    async def hold_first(self, *args, **kwargs):
        if self.ctx.trial_id == p["trial_id"]:
            entered.set()
            await release.wait()
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(PoolDispatchAudit, "_eligible", hold_first)
    first_request = asyncio.create_task(post(p))
    other_request = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        other_request = asyncio.create_task(post(second, replica=1))
        response = await asyncio.wait_for(other_request, 3)
        assert response.status_code == 200, response.text
        # The first transaction is still inside policy, before receipt commit.
        assert not first_request.done()
        assert not release.is_set()
    finally:
        release.set()
        results = await asyncio.gather(
            first_request, *([other_request] if other_request is not None else []),
        )
    assert all(response.status_code == 200 for response in results)
    assert [row.provider_connection_id for row in await receipts(p)] == [p["ids"][0]] * 2
    assert [row.provider_connection_id for row in await receipts(second)] == [p["ids"][1]] * 2
    async with p["sessions"]() as session:
        assert (await session.get(ProviderRequestPool, p["root"])).next_member == 2


async def test_concurrent_first_assignments_preserve_round_robin(pool_setup: dict) -> None:
    p = pool_setup
    second = await _another_pool_trial(p)
    responses = await asyncio.gather(post(p), post(second, replica=1))
    assert all(response.status_code == 200 for response in responses)
    async with p["sessions"]() as session:
        bindings = list((await session.scalars(select(ProviderRequestPoolBinding).where(
            ProviderRequestPoolBinding.pool_connection_id == p["root"],
        ))).all())
        assert len(bindings) == 2
        assert {binding.member_connection_id for binding in bindings} == set(p["ids"][:2])
        assert (await session.get(ProviderRequestPool, p["root"])).next_member == 2


@pytest.mark.parametrize("change", ["disable", "member"])
async def test_sticky_admission_keeps_pool_writers_excluded(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    p = pool_setup
    assert (await post(p)).status_code == 200
    entered, release, writer_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = PoolDispatchAudit._eligible
    writer_pid = None

    async def hold_first(self, *args, **kwargs):
        entered.set()
        await release.wait()
        return await original(self, *args, **kwargs)

    async def edit_pool():
        nonlocal writer_pid
        async with p["sessions"]() as session:
            writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            writer_started.set()
            pool = await session.scalar(select(ProviderRequestPool).where(
                ProviderRequestPool.connection_id == p["root"],
            ).with_for_update())
            if change == "disable":
                pool.enabled = False
            else:
                member = await session.get(ProviderRequestPoolMember, (p["root"], p["root"]))
                member.enabled = False
            await session.commit()

    monkeypatch.setattr(PoolDispatchAudit, "_eligible", hold_first)
    request = asyncio.create_task(post(p))
    writer = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        writer = asyncio.create_task(edit_pool())
        await asyncio.wait_for(writer_started.wait(), 3)
        async with asyncio.timeout(3), p["sessions"]() as session:
            while not await session.scalar(text("SELECT cardinality(pg_blocking_pids(:pid))"),
                                          {"pid": writer_pid}):
                await asyncio.sleep(0.01)
        assert not writer.done()
    finally:
        release.set()
        response, *_ = await asyncio.gather(request, *([writer] if writer is not None else []))
    assert response.status_code == 200, response.text
    monkeypatch.setattr(PoolDispatchAudit, "_eligible", original)
    if change == "member":
        assert (await post(p)).status_code == 200
        assert (await receipts(p))[-1].provider_connection_id == p["ids"][1]
        async with p["sessions"]() as session:
            assert (await session.get(ProviderRequestPool, p["root"])).next_member == 1
    else:
        async with p["sessions"]() as session:
            assert (await session.get(ProviderRequestPool, p["root"])).enabled is False


async def test_pool_disabled_after_sticky_hint_does_not_dispatch(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = pool_setup
    assert (await post(p)).status_code == 200
    original = AsyncSession.scalar
    disabled = False

    async def disable_after_hint(session, statement, *args, **kwargs):
        nonlocal disabled
        result = await original(session, statement, *args, **kwargs)
        if (not disabled and result == p["trial_id"]
                and str(statement).startswith("SELECT provider_request_pool_bindings.trial_id")):
            disabled = True
            async with p["sessions"]() as other:
                await other.execute(update(ProviderRequestPool).where(
                    ProviderRequestPool.connection_id == p["root"],
                ).values(enabled=False))
                await other.commit()
        return result

    monkeypatch.setattr(AsyncSession, "scalar", disable_after_hint)
    response = await post(p)
    assert response.status_code == 409, response.text
    assert disabled
    assert len(await receipts(p)) == 1
    assert len(p["captures"]["requests"]) == 1


async def test_deleted_sticky_hint_restarts_exclusive_assignment(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = pool_setup
    assert (await post(p)).status_code == 200
    original = AsyncSession.scalar
    deleted_hint = False

    async def remove_after_hint(session, statement, *args, **kwargs):
        nonlocal deleted_hint
        result = await original(session, statement, *args, **kwargs)
        if (not deleted_hint and result == p["trial_id"]
                and str(statement).startswith("SELECT provider_request_pool_bindings.trial_id")):
            deleted_hint = True
            async with p["sessions"]() as other:
                await other.execute(delete(ProviderRequestPoolBinding).where(
                    ProviderRequestPoolBinding.pool_connection_id == p["root"],
                    ProviderRequestPoolBinding.trial_id == p["trial_id"],
                ))
                await other.commit()
        return result

    monkeypatch.setattr(AsyncSession, "scalar", remove_after_hint)
    response = await post(p)
    assert response.status_code == 200, response.text
    assert deleted_hint
    rows = await receipts(p)
    assert len(rows) == 2
    assert rows[-1].provider_connection_id == p["ids"][1]
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert binding.member_connection_id == p["ids"][1]
        assert (await session.get(ProviderRequestPool, p["root"])).next_member == 2


@pytest.mark.parametrize(("error_type", "phase"), [
    (httpx.ConnectTimeout, "connect"), (httpx.ReadTimeout, "read"),
    (httpx.WriteTimeout, "write"), (httpx.PoolTimeout, "pool"),
    (httpx.TimeoutException, "unknown"),
])
async def test_pool_timeout_preserves_only_safe_transport_phase(
    pool_setup: dict, caplog: pytest.LogCaptureFixture, error_type, phase: str,
) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
    secret = "private-timeout-endpoint-and-credential"

    async def timeout(request):
        raise error_type(secret, request=request)

    p["captures"]["handler"] = timeout
    response = await post(p)
    assert response.status_code == 504
    rows = await receipts(p)
    assert len(rows) == 1 and rows[0].provider_outcome == "transport_error"
    messages = [record.getMessage() for record in caplog.records
                if record.getMessage().startswith("request_pool_upstream_timeout ")]
    assert len(messages) == 1
    assert f"phase={phase}" in messages[0]
    assert f"receipt_id={rows[0].id}" in messages[0]
    assert f"request_id={rows[0].request_id}" in messages[0]
    assert f"provider_connection_id={p['root']}" in messages[0]
    assert secret not in caplog.text
    assert "test-credential-" not in messages[0]
    assert "provider-0.example.test" not in messages[0]


async def test_trial_stays_on_one_member_across_replicas(pool_setup: dict) -> None:
    p = pool_setup
    for replica in [0, 1, 0, 1, 0, 1]:
        response = await post(p, replica=replica)
        assert response.status_code == 200, response.text
    assert [r.url.host for r in p["captures"]["requests"]] == ["provider-0.example.test"] * 6
    rows = await receipts(p)
    assert [r.provider_connection_id for r in rows] == [p["ids"][0]] * 6
    assert all(r.pool_connection_id == p["root"] for r in rows)
    async with p["sessions"]() as session:
        calls = list(
            (
                await session.scalars(
                    select(LlmCall)
                    .where(LlmCall.trial_id == p["trial_id"])
                    .order_by(LlmCall.captured_at)
                )
            ).all()
        )
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
    assert binding is not None and binding.member_connection_id == p["ids"][0]
    assert [round(float(c.cost_usd), 6) for c in calls] == [0.001] * 6
    assert all(
        c.provider_extras["_loom_request_pool"]["pool_connection_id"] == str(p["root"])
        for c in calls
    )


async def test_timeout_moves_binding_before_504_and_the_spare_keeps_history(
    pool_setup: dict,
) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.

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
    timed_out = await post(p, body)
    assert timed_out.status_code == 504, timed_out.text
    assert "test-credential-0" not in timed_out.text
    rows = await receipts(p)
    assert len(rows) == 1
    assert rows[0].provider_outcome == "transport_error"
    assert rows[0].failover_reason == "upstream_timeout"
    assert rows[0].provider_connection_id == p["ids"][0]
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
    assert binding is not None and binding.member_connection_id == p["ids"][1]
    assert call.attempt == 1
    pool_meta = call.provider_extras["_loom_request_pool"]
    assert pool_meta["possible_duplicate_inference"] is True
    assert pool_meta["provider_connection_id"] == str(p["ids"][0])
    assert "test-credential-0" not in json.dumps(call.provider_extras)
    response = await post(p, body)
    assert response.status_code == 200, response.text
    sent = p["captures"]["requests"]
    assert [r.url.host for r in sent] == ["provider-0.example.test", "provider-1.example.test"]
    assert sent[0].content == sent[1].content
    assert sent[0].headers["X-Request-ID"] != sent[1].headers["X-Request-ID"]
    for model in ["teacher", "student"]:
        body["messages"].extend(
            [
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "next observation"},
            ]
        )
        body["model"] = model
        assert (await post(p, body)).status_code == 200
    assert [r.url.host for r in sent[1:]] == ["provider-1.example.test"] * 3
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
    assert response.status_code == 401, response.text
    assert "test-credential-0" not in response.text
    async with p["sessions"]() as session:
        member = await session.get(ProviderRequestPoolMember, (p["root"], p["root"]))
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert member.enabled is True
        assert binding is not None and binding.member_connection_id == p["ids"][0]
    assert len(p["captures"]["requests"]) == 1


async def test_quota_429_retries_the_same_member_and_does_not_switch(
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
    assert response.status_code == 429, response.text
    assert asyncio.get_running_loop().time() - started >= 0.05
    assert [r.url.host for r in p["captures"]["requests"]] == ["provider-1.example.test"] * 3
    async with p["sessions"]() as session:
        for member_id in p["ids"]:
            member = await session.get(ProviderRequestPoolMember, (p["root"], member_id))
            assert member.cooldown_until is None
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert binding is not None and binding.member_connection_id == p["ids"][1]


async def test_concurrent_requests_stay_on_the_bound_member_without_a_cap(
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
        assert [row.provider_connection_id for row in rows] == [p["ids"][0]] * 12
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


async def test_one_timeout_returns_504_and_moves_the_binding(
    pool_setup: dict,
    caplog: pytest.LogCaptureFixture,
) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
    caplog.set_level("INFO")

    async def timeout(request):
        raise httpx.ReadTimeout("test-credential-0 test-credential-1 test-credential-2")

    p["captures"]["handler"] = timeout
    response = await post(p)
    assert response.status_code == 504, response.text
    assert len(p["captures"]["requests"]) == 1
    assert all(f"test-credential-{i}" not in response.text + caplog.text for i in range(3))
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert call.attempt == 1
        assert call.provider_extras["_loom_request_pool"]["provider_connection_id"] == str(
            p["ids"][0]
        )
        assert call.provider_extras["_loom_request_pool"]["possible_duplicate_inference"] is True
        assert binding is not None and binding.member_connection_id == p["ids"][1]
    assert [row.provider_connection_id for row in await receipts(p)] == [p["ids"][0]]


async def test_timeout_spare_skips_keys_in_the_same_quota_group(pool_setup: dict) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.next_member = 1
        await session.commit()

    async def timeout(request):
        if request.url.host == "provider-1.example.test":
            raise httpx.ReadTimeout("upstream")

    p["captures"]["handler"] = timeout
    assert (await post(p)).status_code == 504
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
    assert binding is not None and binding.member_connection_id == p["ids"][0]
    assert (await post(p)).status_code == 200
    assert [r.url.host for r in p["captures"]["requests"]] == [
        "provider-1.example.test",
        "provider-0.example.test",
    ]


async def test_timeout_keeps_the_binding_when_only_same_quota_group_keys_remain(
    pool_setup: dict,
) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.next_member = 1
        root_member = await session.get(ProviderRequestPoolMember, (p["root"], p["root"]))
        root_member.enabled = False
        await session.commit()

    async def timeout(request):
        raise httpx.ReadTimeout("upstream")

    p["captures"]["handler"] = timeout
    assert (await post(p)).status_code == 504
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
    assert binding is not None and binding.member_connection_id == p["ids"][1]
    assert [r.url.host for r in p["captures"]["requests"]] == ["provider-1.example.test"]


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
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
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
        if episode == 1:
            assert response.status_code == 504, response.text
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
    assert [request.url.host for request in sent] == [
        "provider-0.example.test",
        "provider-1.example.test",
        "provider-1.example.test",
        "provider-1.example.test",
    ]
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
        assert [c.client_call_id for c in calls if c.rate_card_hash != "failed-upstream"] == call_ids
        assert [c.role for c in calls if c.rate_card_hash != "failed-upstream"] == [
            "student",
            "teacher",
            "student",
        ]
        assert all(
            c.correlation_status == "correlated"
            for c in calls
            if c.rate_card_hash != "failed-upstream"
        )
        assert len(intents) == 3
        plan = await session.scalar(
            select(ModelSwitchPlan).where(ModelSwitchPlan.trial_id == p["trial_id"])
        )
        assert plan.provider_connection_id == p["root"]


async def test_slow_success_is_not_cut_off_by_the_stored_attempt_timeout(pool_setup: dict) -> None:
    p = pool_setup

    async def slow(request):
        await asyncio.sleep(0.3)

    p["captures"]["handler"] = slow
    response = await post(p)
    assert response.status_code == 200, response.text
    assert len(p["captures"]["requests"]) == 1
    assert p["captures"]["requests"][0].url.host == "provider-0.example.test"


@pytest.mark.parametrize("member_index", [1, 2], ids=["yibu-gen1", "yibu-gen2"])
async def test_yibu_physical_member_catalog_and_call_price_snapshot(
    pool_setup: dict, member_index: int,
) -> None:
    """Synthetic catalog prices exercise attribution, not current supplier rates."""
    p = pool_setup
    catalog_id = f"test:yibu:{uuid4()}"
    prices = {
        "student": {
            "input_usd_per_1m": 10.0, "output_usd_per_1m": 20.0,
            "cache_read_usd_per_1m": 1.0, "cache_write_usd_per_1m": None,
        },
    }
    async with p["sessions"]() as session:
        session.add(PriceCatalog(
            id=catalog_id, name="Synthetic Yibu supplier catalog", supplier_id="yibuapi",
            source_url="https://yibuapi.com/api/pricing", prices=prices, revision=7,
            source_metadata={"group": "default", "pricing_version": "fixture-v7"},
        ))
        member = await session.get(ProviderConnection, p["ids"][member_index])
        member.pricing_config = {
            "pricing_mode": "catalog", "supplier_id": "yibuapi", "catalog_id": catalog_id,
        }
        session.add(ProviderRequestPoolBinding(
            pool_connection_id=p["root"], trial_id=p["trial_id"],
            member_connection_id=member.id,
        ))
        await session.commit()

    async def provider_with_cache(request):
        return httpx.Response(200, json={
            "model": "student", "choices": [],
            "usage": {
                "prompt_tokens": 100, "completion_tokens": 20,
                "prompt_tokens_details": {"cached_tokens": 40},
            },
        })

    p["captures"]["handler"] = provider_with_cache
    try:
        assert (await post(p)).status_code == 200
        async with p["sessions"]() as session:
            catalog = await session.get(PriceCatalog, catalog_id)
            catalog.prices = {"student": {**prices["student"], "input_usd_per_1m": 99.0}}
            catalog.revision = 8
            await session.commit()
        assert (await post(p)).status_code == 200
        async with p["sessions"]() as session:
            calls = list((await session.scalars(
                select(LlmCall).where(LlmCall.trial_id == p["trial_id"])
                .order_by(LlmCall.captured_at),
            )).all())
        assert len(calls) == 2
        assert [float(call.cost_usd) for call in calls] == pytest.approx([0.00104, 0.00638])
        for call, revision in zip(calls, [7, 8], strict=True):
            pool = call.provider_extras["_loom_request_pool"]
            basis = call.provider_extras["_loom_price_basis"]
            assert pool["provider_connection_id"] == str(p["ids"][member_index])
            assert basis["catalog_id"] == catalog_id
            assert basis["supplier_id"] == "yibuapi"
            assert basis["revision"] == revision
            assert basis["currency"] == "USD"
            assert basis["unit"] == "usd_per_1m_tokens"
            assert basis["source_model"] == "student"
            assert basis["input_includes_cache"] is True
        assert calls[0].provider_extras["_loom_price_basis"]["prices"]["input_usd_per_1m"] == 10
        assert calls[0].rate_card_hash != calls[1].rate_card_hash
    finally:
        async with p["sessions"]() as session:
            await session.execute(delete(PriceCatalog).where(PriceCatalog.id == catalog_id))
            await session.commit()


async def test_retry_budget_expiry_does_not_rotate_or_suppress_a_provider(pool_setup: dict) -> None:
    p = pool_setup
    p["settings"].llm_retry_budget_sec = 1.0
    calls = 0
    cancelled = False

    async def hold_retry(request):
        nonlocal calls, cancelled
        calls += 1
        if calls == 1:
            return httpx.Response(503)
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    p["captures"]["handler"] = hold_retry
    async with asyncio.timeout(5):
        response = await post(p)
    assert response.status_code == 504, response.text
    assert "gateway retry budget exhausted" in response.text
    assert calls == 2
    assert cancelled
    rows = await receipts(p)
    assert [row.provider_outcome for row in rows] == ["response_received", "transport_error"]
    assert all(row.failover_reason is None for row in rows)
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        health = await session.get(ProviderConnectionTimeoutHealth, p["ids"][0])
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
    assert binding is not None and binding.member_connection_id == p["ids"][0]
    assert health is None
    assert call.provider_extras["_loom_failure_category"] == "gateway_retry_budget_exhausted"


async def test_received_retry_success_keeps_usage_when_later_budget_observation_is_expired(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_llm_gateway.retry import send_with_retry
    from loom_llm_gateway.routes import facade_openai

    p = pool_setup
    p["settings"].llm_retry_budget_sec = 60.0
    clock = [0.0]
    calls = 0

    async def retry_with_clock(*args, **kwargs):
        return await send_with_retry(*args, **kwargs, now=lambda: clock[0])

    async def respond(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            clock[0] = 59.0
            return httpx.Response(503)
        clock[0] = 61.0
        return None  # Fixture supplies a successful response with real usage.

    monkeypatch.setattr(facade_openai, "send_with_retry", retry_with_clock)
    p["captures"]["handler"] = respond
    response = await post(p)
    assert response.status_code == 200, response.text
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
    assert call is not None
    assert call.rate_card_hash != "failed-upstream"
    assert call.input_tokens == 100
    assert call.output_tokens == 20
    assert float(call.cost_usd) == pytest.approx(0.001)
    assert call.attempt == 2
    assert [row.provider_outcome for row in await receipts(p)] == [
        "response_received", "response_received",
    ]


async def test_timeout_burst_suppresses_the_member_for_the_next_assignment(
    pool_setup: dict,
) -> None:
    p = pool_setup
    p["settings"].llm_retry_max_attempts = 1  # Isolate one-attempt rotation/observation.
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.timeout_burst_count = 1
        await session.commit()

    async def timeout(request):
        raise httpx.ReadTimeout("upstream")

    p["captures"]["handler"] = timeout
    assert (await post(p)).status_code == 504
    async with p["sessions"]() as session:
        health = await session.get(ProviderConnectionTimeoutHealth, p["ids"][0])
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert health is not None and health.suppressed_until is not None
        assert health.suppressed_until > datetime.now(UTC)
        assert binding is not None and binding.member_connection_id == p["ids"][1]
        await session.delete(binding)
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.next_member = 0
        await session.commit()
    p["captures"]["handler"] = None
    assert (await post(p)).status_code == 200
    assert p["captures"]["requests"][-1].url.host == "provider-1.example.test"


@pytest.mark.parametrize("status", [400, 500])
async def test_non_timeout_errors_stay_on_the_bound_member(pool_setup: dict, status: int) -> None:
    p = pool_setup

    async def fail(request):
        return httpx.Response(status, json={"error": "same-member"})

    p["captures"]["handler"] = fail
    response = await post(p)
    assert response.status_code == status, response.text
    expected = 1 if status == 400 else 3
    assert [request.url.host for request in p["captures"]["requests"]] == [
        "provider-0.example.test"
    ] * expected
    async with p["sessions"]() as session:
        binding = await session.get(ProviderRequestPoolBinding, (p["root"], p["trial_id"]))
        assert binding is not None and binding.member_connection_id == p["ids"][0]


async def test_broker_pause_header_reaches_group_queue_admission(pool_setup: dict) -> None:
    """The header the broker sets is what turns queue admission on.

    A pool with the queue enabled still uses today's path until the
    authenticated request carries exactly ``X-Loom-Pause-Capable: 1``.
    Missing caps then fail closed inside group admission, which is how this
    test shows the queue path ran. With caps, the same header admits.
    """

    p = pool_setup
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        assert pool is not None
        pool.queue_enabled = True
        await session.commit()
    paused = {"X-Loom-Pause-Capable": "1"}
    response = await post(p, headers=paused)
    assert response.status_code == 503, response.text
    assert response.json()["detail"] == "provider_queue_unconfigured"
    assert p["captures"]["requests"] == []

    for value in (None, "true", "1 "):
        p["captures"]["requests"].clear()
        extra = None if value is None else {"X-Loom-Pause-Capable": value}
        response = await post(p, headers=extra)
        assert response.status_code == 200, (value, response.text)
        assert len(p["captures"]["requests"]) == 1

    async with p["sessions"]() as session:
        session.add(ProviderRequestPoolGroupCap(
            pool_connection_id=p["root"], quota_group="az", max_in_flight=1,
        ))
        session.add(ProviderRequestPoolGroupCap(
            pool_connection_id=p["root"], quota_group="yibu", max_in_flight=1,
        ))
        await session.commit()
    p["captures"]["requests"].clear()
    response = await post(p, headers=paused)
    assert response.status_code == 200, response.text
    assert len(p["captures"]["requests"]) == 1


async def test_group_stall_lookup_does_not_scan_completed_dispatch_history(pool_setup: dict) -> None:
    """Completed calls reset stall age; accumulated history must not exhaust admission."""
    p = pool_setup
    now = datetime.now(UTC)
    async with p["sessions"]() as session:
        session.add_all([
            ProviderRequestPoolGroupCap(pool_connection_id=p["root"], quota_group=group,
                                        max_in_flight=1)
            for group in ("az", "yibu")
        ])
        # Both yibu keys share one quota. Their completed history is absent from
        # the partial in-flight index, but its latest entry still resets stall age.
        await session.execute(text("""
            INSERT INTO gateway_dispatch_receipts
                (id, request_id, dispatch_ordinal, attempt, team_id, trial_id,
                 step_id, provider_connection_id, dialect, purpose, admitted_at,
                 provider_outcome, gateway_outcome)
            SELECT gen_random_uuid(), gen_random_uuid(), 1, 1, :team, :trial,
                   'agent', CASE WHEN n % 2 = 0 THEN CAST(:second AS uuid)
                                 ELSE CAST(:third AS uuid) END,
                   'facade_openai', 'model_call', :now - n * interval '1 second',
                   'response_received', 'completed'
              FROM generate_series(1, 30000) AS n
        """), {"team": p["team_id"], "trial": p["trial_id"], "second": p["ids"][1],
                "third": p["ids"][2], "now": now})
        session.add(GatewayDispatchReceipt(
            id=uuid4(), request_id=uuid4(), dispatch_ordinal=1, attempt=1,
            team_id=p["team_id"], trial_id=p["trial_id"], step_id="agent",
            provider_connection_id=p["ids"][1], dialect="facade_openai", purpose="model_call",
            admitted_at=now - timedelta(hours=1),
            attempt_deadline_wall_clock=now + timedelta(minutes=5),
        ))
        await session.commit()
        await session.execute(text("ANALYZE gateway_dispatch_receipts"))
        members = list((await session.scalars(select(ProviderRequestPoolMember).where(
            ProviderRequestPoolMember.pool_connection_id == p["root"],
        ).order_by(ProviderRequestPoolMember.ordinal))).all())
        statements = []
        engine = p["sessions"].kw["bind"]

        def capture(_conn, _cursor, statement, parameters, _context, _many):
            if "max(gateway_dispatch_receipts.admitted_at)" in statement:
                statements.append((statement, parameters))

        event.listen(engine.sync_engine, "before_cursor_execute", capture)
        try:
            choice = await decide_group_queue(
                session, pool_id=p["root"], request_id=uuid4(), bound_group="yibu",
                bound_connection_id=p["ids"][1], attempt=1, stall_sec=200,
                upstream_timeout_sec=200, members=members,
            )
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture)
        assert choice.kind == "wait"  # Recent COMPLETED receipt prevents false stall.
        assert len(statements) == 1
        connection = await session.connection()
        statement, parameters = statements[0]
        plan = (await connection.exec_driver_sql(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters,
        )).scalar_one()[0]["Plan"]

        def receipt_scans(node):
            if node.get("Relation Name") == "gateway_dispatch_receipts":
                yield node
            for child in node.get("Plans", []):
                yield from receipt_scans(child)

        scans = list(receipt_scans(plan))
        assert scans
        assert all(scan["Node Type"] in {"Index Scan", "Index Only Scan"} for scan in scans), plan
        assert sum(scan["Actual Rows"] * scan["Actual Loops"] for scan in scans) <= 2, plan


@pytest.mark.parametrize("pooled", [False, True])
async def test_capacity_lookup_does_not_scan_expired_admitted_receipts(
    pool_setup: dict, pooled: bool,
) -> None:
    """Unknown completion history must not turn a free slot into an audit timeout."""
    p = pool_setup
    now = datetime.now(UTC)
    async with p["sessions"]() as session:
        await session.execute(update(ProviderConnection).where(
            ProviderConnection.id == p["root"],
        ).values(max_in_flight=1))
        if pooled:
            await session.execute(update(ProviderRequestPool).where(
                ProviderRequestPool.connection_id == p["root"],
            ).values(queue_enabled=True))
            session.add_all([
                ProviderRequestPoolGroupCap(pool_connection_id=p["root"], quota_group=group,
                                            max_in_flight=1)
                for group in ("az", "yibu")
            ])
        await session.execute(text("""
            INSERT INTO gateway_dispatch_receipts
                (id, request_id, dispatch_ordinal, attempt, team_id, trial_id,
                 step_id, provider_connection_id, dialect, purpose, admitted_at,
                 attempt_deadline_wall_clock)
            SELECT gen_random_uuid(), gen_random_uuid(), 1, 1, :team, :trial,
                   'agent', :connection, 'facade_openai', 'model_call',
                   :now - interval '1 day' - n * interval '1 second',
                   CASE WHEN n % 2 = 0 THEN :now - interval '1 hour' ELSE NULL END
              FROM generate_series(1, 6000) AS n
        """), {"team": p["team_id"], "trial": p["trial_id"],
                "connection": p["root"], "now": now})
        await session.commit()
        await session.execute(text("ANALYZE gateway_dispatch_receipts"))
    statements = []
    engine = p["sessions"].kw["bind"]

    def capture(_conn, _cursor, statement, parameters, _context, _many):
        if "count(" in statement and "gateway_dispatch_receipts" in statement:
            statements.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        response = await post(p, headers={"X-Loom-Pause-Capable": "1"})
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert response.status_code == 200, response.text
    assert len(p["captures"]["requests"]) == 1
    assert statements
    async with engine.connect() as connection:
        for statement, parameters in statements:
            plan = (await connection.exec_driver_sql(
                "EXPLAIN (ANALYZE, FORMAT JSON) " + statement, parameters,
            )).scalar_one()[0]["Plan"]

            def receipt_scans(node):
                if node.get("Relation Name") == "gateway_dispatch_receipts":
                    yield node
                for child in node.get("Plans", []):
                    yield from receipt_scans(child)

            scans = list(receipt_scans(plan))
            assert scans, plan
            visited = sum((scan["Actual Rows"] + scan.get("Rows Removed by Filter", 0))
                          * scan["Actual Loops"] for scan in scans)
            assert visited <= 4, plan


async def test_group_queue_waits_and_drains_across_replicas_with_the_normal_audit_budget(
    pool_setup: dict, monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = pool_setup
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 1.0)
    released = asyncio.Event()
    active = {"az": 0, "yibu": 0}
    maximum = dict(active)

    async def block(request):
        group = "az" if request.url.host == "provider-0.example.test" else "yibu"
        active[group] += 1
        maximum[group] = max(maximum[group], active[group])
        try:
            await released.wait()
        finally:
            active[group] -= 1

    p["captures"]["handler"] = block
    async with p["sessions"]() as session:
        pool = await session.get(ProviderRequestPool, p["root"])
        pool.queue_enabled = True
        session.add_all([
            ProviderRequestPoolGroupCap(pool_connection_id=p["root"], quota_group=group,
                                        max_in_flight=1)
            for group in active
        ])
        await session.commit()
    trials = [p] + [await _another_pool_trial(p) for _ in range(3)]
    calls = [asyncio.create_task(post(trial, replica=i % 2,
                                    headers={"X-Loom-Pause-Capable": "1"}))
             for i, trial in enumerate(trials)]
    try:
        async with asyncio.timeout(5):
            while True:
                async with p["sessions"]() as session:
                    waiting = await session.scalar(select(func.count()).select_from(
                        GatewayDispatchQueue,
                    ).where(GatewayDispatchQueue.pool_connection_id == p["root"],
                            GatewayDispatchQueue.closed_at.is_(None)))
                if waiting and all(active.values()):
                    break
                await asyncio.sleep(0.02)
        assert len(p["captures"]["requests"]) == 2
    finally:
        released.set()
        responses = await asyncio.wait_for(asyncio.gather(*calls), 10)
    assert [response.status_code for response in responses] == [200] * 4
    assert maximum == {"az": 1, "yibu": 1}
    async with p["sessions"]() as session:
        rows = list((await session.scalars(select(GatewayDispatchQueue).where(
            GatewayDispatchQueue.pool_connection_id == p["root"],
        ))).all())
    assert rows and all(row.closed_at is not None for row in rows)
    assert all(row.outcome in {"admitted", "overflow_admitted"} for row in rows)


async def test_in_flight_cap_is_shared_by_gateway_replicas(pool_setup: dict) -> None:
    p = pool_setup
    released = asyncio.Event()
    active = 0
    maximum = 0

    async def block(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await released.wait()
        active -= 1

    p["captures"]["handler"] = block
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == p["ids"][0])
            .values(max_in_flight=2)
        )
        await session.commit()
    calls = [asyncio.create_task(post(p, replica=i % 2)) for i in range(6)]
    try:
        async with asyncio.timeout(3):
            while maximum < 2 or len(p["captures"]["requests"]) < 2:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)
        assert maximum == 2
        assert len(p["captures"]["requests"]) == 2
    finally:
        released.set()
        responses = await asyncio.gather(*calls)
    assert all(response.status_code == 200 for response in responses)
    assert maximum == 2


async def test_queue_wait_returns_429_without_calling_the_provider(pool_setup: dict) -> None:
    p = pool_setup
    p["settings"].connection_queue_max_wait_sec = 0
    released = asyncio.Event()
    entered = asyncio.Event()

    async def block(request):
        entered.set()
        await released.wait()

    p["captures"]["handler"] = block
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == p["ids"][0])
            .values(max_in_flight=1)
        )
        await session.commit()
    held = asyncio.create_task(post(p, replica=0))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        limited = await post(p, replica=1)
        assert limited.status_code == 429, limited.text
        assert limited.json()["detail"] == "provider_connection_concurrency_limited"
        assert limited.headers["retry-after"] == "1"
        assert len(p["captures"]["requests"]) == 1
    finally:
        released.set()
        await held


async def test_stale_admitted_receipt_does_not_consume_a_slot(pool_setup: dict) -> None:
    p = pool_setup
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == p["ids"][0])
            .values(max_in_flight=1)
        )
        session.add(
            GatewayDispatchReceipt(
                request_id=uuid4(),
                dispatch_ordinal=1,
                attempt=1,
                team_id=p["team_id"],
                trial_id=p["trial_id"],
                step_id="agent",
                provider_connection_id=p["ids"][0],
                dialect="facade_openai",
                purpose="model_call",
                provider_outcome="admitted",
                admitted_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        await session.commit()
    response = await post(p)
    assert response.status_code == 200, response.text
    assert len(p["captures"]["requests"]) == 1


async def test_open_admitted_receipt_with_a_future_deadline_holds_the_slot(
    pool_setup: dict,
) -> None:
    p = pool_setup
    p["settings"].connection_queue_max_wait_sec = 0
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == p["ids"][0])
            .values(max_in_flight=1)
        )
        session.add(
            GatewayDispatchReceipt(
                request_id=uuid4(),
                dispatch_ordinal=1,
                attempt=1,
                team_id=p["team_id"],
                trial_id=p["trial_id"],
                step_id="agent",
                provider_connection_id=p["ids"][0],
                dialect="facade_openai",
                purpose="model_call",
                provider_outcome="admitted",
                admitted_at=datetime.now(UTC) - timedelta(hours=1),
                attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(minutes=10),
            )
        )
        await session.commit()
    response = await post(p)
    assert response.status_code == 429, response.text
    assert response.json()["detail"] == "provider_connection_concurrency_limited"
    assert not p["captures"]["requests"]


async def test_gateway_retries_rotated_member_in_the_same_request(pool_setup: dict) -> None:
    p = pool_setup

    async def timeout_first_member(request):
        if request.url.host == "provider-0.example.test":
            raise httpx.ReadTimeout("synthetic provider read timeout")

    p["captures"]["handler"] = timeout_first_member
    response = await post(p)
    assert response.status_code == 200, response.text
    sent = p["captures"]["requests"]
    assert [request.url.host for request in sent] == [
        "provider-0.example.test", "provider-1.example.test",
    ]
    assert [request.headers["Authorization"] for request in sent] == [
        "Bearer test-credential-0", "Bearer test-credential-1",
    ]
    assert json.loads(sent[0].content)["messages"] == json.loads(sent[1].content)["messages"]
    rows = await receipts(p)
    assert [row.attempt for row in rows] == [1, 2]
    assert [row.provider_outcome for row in rows] == ["transport_error", "response_received"]
    assert [row.provider_connection_id for row in rows] == p["ids"][:2]
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
    assert call.attempt == 2
    assert call.input_tokens == 100
    assert float(call.cost_usd) == pytest.approx(0.002)
    assert call.provider_extras["_loom_request_pool"]["provider_connection_id"] == str(p["ids"][1])
    assert call.provider_extras["_loom_request_pool"]["possible_duplicate_inference"] is True


async def test_gateway_pool_failover_uses_the_same_total_attempt_limit(pool_setup: dict) -> None:
    p = pool_setup

    async def always_timeout(request):
        raise httpx.ReadTimeout("synthetic provider read timeout")

    p["captures"]["handler"] = always_timeout
    response = await post(p)
    assert response.status_code == 504, response.text
    assert len(p["captures"]["requests"]) == p["settings"].llm_retry_max_attempts == 3
    rows = await receipts(p)
    assert [row.attempt for row in rows] == [1, 2, 3]
    assert all(row.provider_outcome == "transport_error" for row in rows)
    async with p["sessions"]() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == p["trial_id"]))
    assert call.attempt == 3
