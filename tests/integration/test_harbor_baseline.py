"""Real PostgreSQL/SecretStore HTTP tests; upstream model calls are mocked."""

from __future__ import annotations

import asyncio
import json
import os
import socket
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from sqlalchemy import Text, delete, insert, select, update
from sqlalchemy.exc import IntegrityError

from loom.auth import AuthContext
from loom.db.schema import (
    AdminAuditEvent,
    HarborBaselineDispatch,
    HarborBaselineSession,
    LlmCall,
    ProviderConnection,
    ProviderModelCache,
)
from loom_service.dependencies import authed_session
from loom_service.routes import harbor_baselines
from tests.integration.test_gateway_facade_openai import facade_setup  # noqa: F401


@pytest.fixture
async def baseline_setup(facade_setup):  # noqa: F811
    gateway, _, team_id, _, connection_id, captures = facade_setup
    async with gateway.state.session_factory() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == connection_id)
            .values(
                status="valid",
                pricing_config={
                    "pricing_mode": "custom",
                    "custom_pricing": {
                        "gpt-4o": {
                            "input_usd_per_1m": 5.0,
                            "output_usd_per_1m": 15.0,
                            "cache_read_usd_per_1m": 2.0,
                        }
                    },
                },
            )
        )
        await session.execute(
            insert(ProviderModelCache).values(
                provider_connection_id=connection_id,
                model_id="gpt-4o",
                context_length=4000,
                last_preflight_status=None,
                visible=True,
                upstream_present=True,
            )
        )
        await session.commit()
    service = FastAPI()
    service.include_router(harbor_baselines.router, prefix="/api/v1")
    context = AuthContext(b"fixture", "team", ["submit", "read:own"], team_id, None)

    async def authenticated():
        async with gateway.state.session_factory() as session:
            yield session, context

    service.dependency_overrides[authed_session] = authenticated
    create = {
        "label": "20-task-pilot",
        "provider_connection_id": str(connection_id),
        "model": "gpt-4o",
        "ttl_seconds": 600,
        "max_calls": 2,
        "max_output_tokens": 100,
        "max_total_tokens": 10000,
        "budget_usd": "1.00",
    }
    try:
        yield gateway, service, create, captures, context
    finally:
        async with gateway.state.session_factory() as session:
            ids = select(HarborBaselineSession.id).where(HarborBaselineSession.team_id == team_id)
            await session.execute(
                delete(AdminAuditEvent).where(
                    AdminAuditEvent.target_type == "harbor_baseline",
                    AdminAuditEvent.target_id.in_(
                        select(HarborBaselineSession.id.cast(Text)).where(
                            HarborBaselineSession.team_id == team_id
                        )
                    ),
                )
            )
            await session.execute(delete(LlmCall).where(LlmCall.baseline_session_id.in_(ids)))
            await session.execute(
                delete(HarborBaselineDispatch).where(
                    HarborBaselineDispatch.baseline_session_id.in_(ids)
                )
            )
            await session.execute(
                delete(HarborBaselineSession).where(HarborBaselineSession.team_id == team_id)
            )
            await session.execute(
                delete(ProviderModelCache).where(
                    ProviderModelCache.provider_connection_id == connection_id
                )
            )
            await session.commit()


async def grant(service, create):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        response = await client.post("/api/v1/harbor-baselines", json=create)
        assert response.status_code == 201, response.text
        return response.json()


async def call(gateway, token, **changes):
    payload = {"model": "gpt-4o", "messages": [{"role": "user", "content": "hello"}], **changes}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway), base_url="http://test"
    ) as client:
        return await client.post(
            "/harbor-baseline/v1/chat/completions",
            json=payload,
            headers={"Authorization": "Bearer " + token},
        )


async def test_create_call_accounting_and_quota(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    response = await call(gateway, info["token"])
    assert response.status_code == 200
    upstream = captures["requests"][0]
    assert upstream.headers["Authorization"] == "Bearer sk-upstream-XYZ"
    assert "max_completion_tokens" in upstream.content.decode()
    assert info["token"] not in response.text and "sk-upstream" not in response.text
    async with gateway.state.session_factory() as session:
        rows = (
            await session.scalars(select(LlmCall).where(LlmCall.baseline_session_id == info["id"]))
        ).all()
        assert len(rows) == 1 and rows[0].trial_id is None and rows[0].execution_attempt_id is None
        assert rows[0].input_tokens == 100 and rows[0].output_tokens == 50
        assert rows[0].cost_usd == Decimal("0.001250")
    assert (await call(gateway, info["token"])).status_code == 200
    exhausted = await call(gateway, info["token"])
    assert exhausted.status_code == 429 and len(captures["requests"]) == 2
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        status = await client.get("/api/v1/harbor-baselines/" + info["id"])
        assert "token" not in status.json()
        assert status.json()["tokens_reserved"] == 300


async def test_model_tenant_and_expiry_boundaries(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    assert (await call(gateway, info["token"], model="gpt-other")).status_code == 403
    assert (await call(gateway, info["token"], tools=[])).status_code == 400
    assert (await call(gateway, "loom_api_not-a-baseline")).status_code == 401
    async with gateway.state.session_factory() as session:
        await session.execute(
            update(HarborBaselineSession)
            .where(HarborBaselineSession.id == info["id"])
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.commit()
    assert (await call(gateway, info["token"])).status_code == 401
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        bad = await client.post(
            "/api/v1/harbor-baselines", json={**create, "team_id": str(uuid4())}
        )
        assert bad.status_code == 404
    assert not captures["requests"]


async def test_unknown_usage_keeps_reservation_and_disables_grant(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    captures["response"] = httpx.Response(200, json={"choices": []})
    result = await call(gateway, info["token"])
    assert result.status_code == 502
    assert (await call(gateway, info["token"])).status_code == 409
    async with gateway.state.session_factory() as session:
        row = await session.get(HarborBaselineSession, info["id"])
        assert row.tokens_reserved == 4100
        assert row.cost_reserved_usd == Decimal("0.021500")
        assert row.blocked_reason == "baseline_usage_unavailable"
    assert len(captures["requests"]) == 1


async def test_price_unavailable_and_revocation(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        revoked = await client.delete("/api/v1/harbor-baselines/" + info["id"])
        assert revoked.status_code == 204
        assert (await call(gateway, info["token"])).status_code == 401
        async with gateway.state.session_factory() as session:
            await session.execute(
                update(ProviderConnection)
                .where(ProviderConnection.id == create["provider_connection_id"])
                .values(pricing_config={"pricing_mode": "usage_only"})
            )
            await session.commit()
        unpriced = await client.post("/api/v1/harbor-baselines", json=create)
        assert unpriced.status_code == 409
    assert not captures["requests"]


@pytest.mark.parametrize("budget", [{"max_total_tokens": 4100}, {"budget_usd": "0.022000"}])
async def test_worst_case_reservation_stops_before_next_dispatch(baseline_setup, budget):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, {**create, **budget})
    assert (await call(gateway, info["token"], max_tokens=100)).status_code == 200
    assert (await call(gateway, info["token"])).status_code == 429
    assert len(captures["requests"]) == 1
    sent = json.loads(captures["requests"][0].content)
    assert sent["max_completion_tokens"] == 100 and "max_tokens" not in sent


async def test_concurrent_calls_cannot_spend_same_reservation(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    entered, finish = asyncio.Event(), asyncio.Event()
    original = gateway.state.upstream_client

    async def handler(request):
        captures["requests"].append(request)
        entered.set()
        await finish.wait()
        return captures["response"]

    blocked_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway.state.egress_client_pool.upstream_client = blocked_client
    try:
        first = asyncio.create_task(call(gateway, info["token"]))
        await asyncio.wait_for(entered.wait(), timeout=3)
        competing = await call(gateway, info["token"])
        assert competing.status_code == 409 and len(captures["requests"]) == 1
        finish.set()
        assert (await first).status_code == 200
    finally:
        finish.set()
        gateway.state.egress_client_pool.upstream_client = original
        await blocked_client.aclose()


async def test_provider_overrun_and_connection_revocation_fail_closed(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    body = captures["response"].json()
    body["usage"]["completion_tokens"] = 101
    captures["response"] = httpx.Response(200, json=body)
    assert (await call(gateway, info["token"])).status_code == 502
    assert (await call(gateway, info["token"])).status_code == 409
    second = await grant(service, create)
    async with gateway.state.session_factory() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == create["provider_connection_id"])
            .values(deleted_at=datetime.now(UTC))
        )
        await session.commit()
    assert (await call(gateway, second["token"])).status_code == 404
    assert len(captures["requests"]) == 1


async def test_call_attribution_constraint_rejects_missing_or_multiple_subjects(baseline_setup):
    gateway, service, create, _, _ = baseline_setup
    info = await grant(service, create)
    assert (await call(gateway, info["token"])).status_code == 200
    async with gateway.state.session_factory() as session:
        for changes in ({"baseline_session_id": None}, {"trial_id": uuid4()}):
            with pytest.raises(IntegrityError, match="llm_calls_exactly_one_subject_check"):
                async with session.begin_nested():
                    await session.execute(
                        update(LlmCall)
                        .where(LlmCall.baseline_session_id == info["id"])
                        .values(**changes)
                    )


async def test_call_evidence_uses_independent_subject(baseline_setup):
    gateway, service, create, _, _ = baseline_setup
    info = await grant(service, create)
    assert (await call(gateway, info["token"])).status_code == 200
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        evidence = await client.get("/api/v1/harbor-baselines/" + info["id"] + "/calls")
    assert evidence.status_code == 200
    row = evidence.json()["items"][0]
    assert row["outcome"] == "completed" and row["input_tokens"] == 100
    assert "sk-upstream" not in evidence.text and info["token"] not in evidence.text


@pytest.mark.skipif(
    not os.environ.get("LOOM_HARBOR_SMOKE_PYTHON"), reason="pinned Harbor interpreter is opt-in"
)
async def test_native_harbor_chat_request_through_gateway(baseline_setup, tmp_path):
    """Actual pinned Harbor+LiteLLM -> HTTP -> DB reservation -> mocked upstream."""
    gateway, service, create, captures, _ = baseline_setup
    async with gateway.state.session_factory() as session:
        await session.execute(
            update(ProviderModelCache)
            .where(ProviderModelCache.provider_connection_id == create["provider_connection_id"])
            .values(model_id="gpt-5.4")
        )
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == create["provider_connection_id"])
            .values(pricing_config={"pricing_mode": "usage_only"})
        )
        await session.commit()
    info = await grant(service, {**create, "model": "gpt-5.4", "budget_usd": None})
    response = captures["response"].json()
    response["model"] = "gpt-5.4"
    captures["response"] = httpx.Response(200, json=response)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(gateway, log_level="error", lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started
        program = r"""
import asyncio, os, tempfile
from pathlib import Path
from harbor.agents.terminus_2.terminus_2 import Terminus2
from harbor.llms.lite_llm import LiteLLM
# Reproduce the pre-fix constructor failure against real Harbor, no model call.
with tempfile.TemporaryDirectory() as directory:
    try:
        Terminus2(logs_dir=Path(directory), model_name="openai/gpt-5.4",
                  llm_kwargs={"reasoning_effort": "high", "api_key": "fixture"})
    except TypeError as exc:
        assert "reasoning_effort" in str(exc) and "multiple" in str(exc)
    else:
        raise AssertionError("expected duplicate reasoning keyword failure")
    fixed = Terminus2(logs_dir=Path(directory), model_name="openai/gpt-5.4",
                      reasoning_effort="high", llm_kwargs={"max_completion_tokens": 100})
    assert fixed._llm._reasoning_effort == "high"
async def main():
    llm = LiteLLM(model_name="openai/gpt-5.4", api_base=os.environ["BASELINE_BASE"],
                  reasoning_effort="high", max_completion_tokens=100)
    result = await llm.call("hello")
    assert result.content == "hello"
asyncio.run(main())
print("real Harbor constructor regression and native HTTP chat passed")
"""
        env = {
            **os.environ,
            "OPENAI_API_KEY": info["token"],
            "BASELINE_BASE": f"http://127.0.0.1:{port}/harbor-baseline/v1",
            "LITELLM_LOCAL_MODEL_COST_MAP": "True",
        }
        process = await asyncio.create_subprocess_exec(
            os.environ["LOOM_HARBOR_SMOKE_PYTHON"],
            "-c",
            program,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        # Never print subprocess output on failure: SDK diagnostics can contain a bearer.
        assert process.returncode == 0, (
            "native Harbor smoke failed; inspect sanitized request shape"
        )
        assert b"native HTTP chat passed" in stdout
        sent = json.loads(captures["requests"][0].content)
        assert sent["model"] == "gpt-5.4" and sent["reasoning_effort"] == "high"
        assert sent["max_completion_tokens"] == 100 and len(captures["requests"]) == 1
    finally:
        server.should_exit = True
        await task


async def test_baseline_bearer_cannot_use_trial_gateway(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    info = await grant(service, create)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway), base_url="http://test"
    ) as client:
        response = await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": "Bearer " + info["token"],
                "x-loom-provider-connection-id": create["provider_connection_id"],
            },
            json={"model": "gpt-4o", "messages": [{"role": "user", "content": "hello"}]},
        )
    assert response.status_code in (401, 403) and not captures["requests"]


async def test_submit_and_read_scope_boundaries(baseline_setup):
    gateway, service, create, captures, ctx = baseline_setup
    ctx.scopes[:] = ["submit"]
    info = await grant(service, create)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        path = "/api/v1/harbor-baselines/" + info["id"]
        assert (await client.get(path)).status_code == 403
        assert (await client.get(path + "/calls")).status_code == 403
        assert (await client.delete(path)).status_code == 204
        ctx.scopes[:] = ["read:own"]
        assert (await client.post("/api/v1/harbor-baselines", json=create)).status_code == 403
        assert (await client.delete(path)).status_code == 403
        assert (await client.get(path)).json()["revoked"] is True
    assert (await call(gateway, info["token"])).status_code == 401
    assert not captures["requests"]


async def test_unknown_context_is_explicit_operator_configuration(baseline_setup):
    gateway, service, create, captures, _ = baseline_setup
    async with gateway.state.session_factory() as session:
        await session.execute(
            update(ProviderModelCache)
            .where(ProviderModelCache.provider_connection_id == create["provider_connection_id"])
            .values(context_length=None)
        )
        await session.commit()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service), base_url="http://test"
    ) as client:
        result = await client.post("/api/v1/harbor-baselines", json=create)
        assert result.status_code == 409
    async with gateway.state.session_factory() as session:
        row = await session.scalar(
            select(ProviderModelCache).where(
                ProviderModelCache.provider_connection_id == create["provider_connection_id"]
            )
        )
        assert row.context_length is None and row.upstream_present
    assert not captures["requests"]
