"""Real TCP/DB proof for ordinary Trial deadline dispatch audit (#1858)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import create_engine, select, update
from starlette.responses import JSONResponse, StreamingResponse

from loom.auth import mint_step_jwt
from loom.db.schema import GatewayDispatchReceipt, LlmCall, ProviderConnection
from loom_llm_gateway.app import create_app
from loom_llm_gateway.routes import gemini, messages, responses
from tests.integration.test_issue_1748_deadline_canary import (
    _TEST_MASTER_KEY,
    _cleanup_gateway,
    _seed_gateway,
    _serve,
)

_CHAT_RESPONSE = {
    "id": "chatcmpl-local-audit",
    "object": "chat.completion",
    "model": "m",
    "choices": [
        {"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
    ],
    "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
}


def _receipts(postgres_url: str, trial_id: UUID) -> list[dict[str, object]]:
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    select(GatewayDispatchReceipt)
                    .where(
                        GatewayDispatchReceipt.trial_id == trial_id,
                    )
                    .order_by(GatewayDispatchReceipt.admitted_at)
                ).mappings()
            ]
    finally:
        engine.dispose()


async def _terminal_receipts(postgres_url: str, trial_id: UUID) -> list[dict[str, object]]:
    # HTTP body delivery and the middleware's bounded audit commit are
    # independent observations. The synchronous reader must run off the
    # servers' event loop so it cannot starve the audit commit's own budget.
    for _ in range(100):
        reader = asyncio.create_task(asyncio.to_thread(_receipts, postgres_url, trial_id))
        try:
            rows = await asyncio.shield(reader)
        except asyncio.CancelledError:
            # Cancelling a thread await does not stop its database query.
            # Drain it before fixture teardown, preserving cancellation even
            # if the reader fails or the caller cancels again while draining.
            drained = asyncio.gather(reader, return_exceptions=True)
            while not drained.done():
                try:
                    await asyncio.shield(drained)
                except asyncio.CancelledError:
                    pass
            raise
        if rows and all(row["gateway_outcome"] != "pending" for row in rows):
            return rows
        await asyncio.sleep(0.02)
    raise AssertionError("Gateway audit did not terminalize after 100 receipt checks")


@pytest.mark.parametrize(
    ("path", "provider_type", "bind_connection", "payload"),
    [
        (
            "/openai/v1/chat/completions",
            "openai-compatible",
            True,
            {"model": "m", "messages": [{"role": "user", "content": "private-content"}]},
        ),
        (
            "/v1/chat/completions",
            "openai-compatible",
            True,
            {"model": "openai/m", "messages": [{"role": "user", "content": "private-content"}]},
        ),
        (
            "/anthropic/v1/messages",
            "anthropic",
            True,
            {"model": "m", "messages": [], "max_tokens": 1},
        ),
        ("/google/v1beta/models/m:generateContent", "google", True, {"contents": []}),
        (
            "/v1/messages",
            "openai-compatible",
            False,
            {"model": "m", "messages": [], "max_tokens": 1},
        ),
        ("/v1beta/models/m:generateContent", "openai-compatible", False, {"contents": []}),
        ("/v1/responses", "openai-compatible", True, {"model": "m", "input": "private-content"}),
        ("/v1/responses", "openai-compatible", False, {"model": "m", "input": "private-content"}),
    ],
    ids=[
        "openai-facade",
        "chat-byo",
        "anthropic-facade",
        "google-facade",
        "messages",
        "gemini",
        "responses-byo",
        "responses-platform",
    ],
)
async def test_held_request_has_signed_dispatch_audit_without_billable_call(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    path: str,
    provider_type: str,
    bind_connection: bool,
    payload: dict[str, object],
) -> None:
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_MAX_ATTEMPTS", "1")
    for provider in ("ANTHROPIC", "GOOGLE", "OPENAI"):
        monkeypatch.setenv(f"LOOM_GW_{provider}_API_KEY", "local-placeholder-provider-key")
    received: list[dict[str, object]] = []
    provider = FastAPI()
    release_provider = asyncio.Event()

    @provider.post("/{rest:path}")
    async def hold(rest: str, request: Request) -> dict[str, object]:
        received.append({"headers": dict(request.headers), "body": await request.json()})
        # Keep the request in flight until the caller observes its deadline.
        # A fixed sleep can finish first when CI dispatch/setup is slow.
        await asyncio.wait_for(release_provider.wait(), timeout=15)
        return {"unused": True}

    async with _serve(provider) as provider_url:
        monkeypatch.setattr(messages, "ANTHROPIC_BASE_URL", provider_url)
        monkeypatch.setattr(gemini, "GEMINI_BASE_URL", provider_url)
        monkeypatch.setattr(responses, "OPENAI_BASE_URL", provider_url)
        settings, team_id, trial_id, connection_id, task_id, ref = await _seed_gateway(
            postgres_url=postgres_url,
            base_url=provider_url,
        )
        engine = create_engine(postgres_url)
        with engine.begin() as connection:
            connection.execute(
                update(ProviderConnection)
                .where(
                    ProviderConnection.id == connection_id,
                )
                .values(provider_type=provider_type)
            )
        engine.dispose()
        try:
            async with _serve(create_app(settings)) as gateway_url:
                agent_attempt_id, grant_id = uuid4(), uuid4()
                async with httpx.AsyncClient(base_url=gateway_url, timeout=10) as client:
                    # Client/TLS setup is not the behavior under test. Mint only
                    # after setup, with room for real HTTP/DB dispatch under load.
                    token = mint_step_jwt(
                        team_id=team_id,
                        trial_id=trial_id,
                        step_id="main",
                        ttl_sec=360,
                        signing_key=settings.step_jwt_signing_key.get_secret_value(),
                        provider_connection_id=connection_id if bind_connection else None,
                        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=5),
                        agent_attempt_id=agent_attempt_id,
                        step_jwt_id=grant_id,
                    )
                    try:
                        result = await client.post(
                            path,
                            json=payload,
                            headers={"Authorization": f"Bearer {token}"},
                        )
                    finally:
                        release_provider.set()
                    assert result.status_code == 504, result.text
                    assert result.json()["detail"] == {
                        "code": "agent_timeout",
                        "reason": "attempt_deadline_reached",
                    }
                    replay = await client.post(
                        path,
                        json=payload,
                        headers={
                            "Authorization": f"Bearer {token}",
                        },
                    )
                    assert replay.status_code == 504
                assert len(received) == 1
                rows = await _terminal_receipts(postgres_url, trial_id)
                assert len(rows) == 1
                row = rows[0]
                assert row["agent_attempt_id"] == agent_attempt_id
                assert row["step_jwt_id"] == grant_id
                assert row["team_id"] == team_id and row["step_id"] == "main"
                assert row["provider_connection_id"] == (connection_id if bind_connection else None)
                assert row["provider_outcome"] == "deadline"
                assert row["gateway_outcome"] == "deadline"
                assert row["gateway_http_status"] == 504
                assert row["dispatch_ordinal"] == 1 and row["attempt"] == 1
                assert row["purpose"] == "model_call"
                if path == "/openai/v1/chat/completions":
                    assert received[0]["headers"].get("x-request-id") == str(row["id"])
                persisted = repr(row)
                assert token not in persisted
                assert "private-content" not in persisted
                assert "local-placeholder-provider-key" not in persisted
                assert provider_url not in persisted
                # Existing provider authentication remains, but no step JWT
                # or private execution identity is forwarded to third parties.
                # The OpenAI facade carries only an opaque per-dispatch trace ID.
                upstream = repr(received[0])
                assert token not in upstream
                assert str(agent_attempt_id) not in upstream and str(grant_id) not in upstream
                engine = create_engine(postgres_url)
                with engine.connect() as connection:
                    calls = connection.execute(
                        select(LlmCall.input_tokens, LlmCall.output_tokens,
                               LlmCall.cost_usd, LlmCall.provider_extras)
                        .where(LlmCall.trial_id == trial_id)
                    ).mappings().all()
                    # The native deadline repair retains an attempted OpenAI
                    # facade call with missing usage. Dispatch remains audited
                    # independently and no usage or charge is fabricated.
                    assert len(calls) == int(path == "/openai/v1/chat/completions")
                    if calls:
                        assert calls[0]["input_tokens"] == calls[0]["output_tokens"] == 0
                        assert calls[0]["cost_usd"] == 0
                        extras = calls[0]["provider_extras"]
                        assert extras["_loom_call_status"] == "failed"
                        assert extras["_loom_usage_status"] == "missing"
                        assert extras["_loom_failure_category"] == "attempt_deadline_reached"
                engine.dispose()
        finally:
            _cleanup_gateway(
                postgres_url=postgres_url,
                team_id=team_id,
                trial_id=trial_id,
                task_id=task_id,
                secret_ref=ref,
            )


@pytest.mark.parametrize("mode", ["success", "retry", "probe"])
async def test_dispatch_receipts_preserve_success_accounting_and_probe_purpose(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    mode: str,
) -> None:
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_BASE_BACKOFF_SEC", "0")
    monkeypatch.setenv("LOOM_GW_LLM_RETRY_JITTER_SEC", "0")
    provider = FastAPI()
    counts = {"model": 0, "probe": 0}

    @provider.post("/{rest:path}")
    async def answer(rest: str, request: Request) -> JSONResponse:
        body = await request.json()
        if body.get("model") == "loom-probe-nonexistent-model":
            counts["probe"] += 1
            return JSONResponse({"error": "model not found"}, status_code=400)
        counts["model"] += 1
        if mode == "retry" and counts["model"] == 1:
            return JSONResponse({"error": "try again"}, status_code=503)
        if rest.endswith("responses"):
            return JSONResponse(
                {
                    "id": "resp_local_audit",
                    "object": "response",
                    "model": "m",
                    "output": [],
                    "status": "completed",
                    "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
                }
            )
        return JSONResponse(_CHAT_RESPONSE)

    async with _serve(provider) as provider_url:
        settings, team_id, trial_id, connection_id, task_id, ref = await _seed_gateway(
            postgres_url=postgres_url,
            base_url=provider_url,
        )
        if mode == "probe":
            engine = create_engine(postgres_url)
            with engine.begin() as connection:
                connection.execute(
                    update(ProviderConnection)
                    .where(
                        ProviderConnection.id == connection_id,
                    )
                    .values(responses_api_supported=None, responses_api_probed_at=None)
                )
            engine.dispose()
        try:
            async with _serve(create_app(settings)) as gateway_url:
                token = mint_step_jwt(
                    team_id=team_id,
                    trial_id=trial_id,
                    step_id="main",
                    ttl_sec=360,
                    signing_key=settings.step_jwt_signing_key.get_secret_value(),
                    provider_connection_id=connection_id,
                    attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=5),
                    agent_attempt_id=uuid4(),
                    step_jwt_id=uuid4(),
                )
                async with httpx.AsyncClient(base_url=gateway_url, timeout=7) as client:
                    result = await client.post(
                        "/v1/responses" if mode == "probe" else "/openai/v1/chat/completions",
                        json={"model": "m", "input": "x"}
                        if mode == "probe"
                        else {
                            "model": "m",
                            "messages": [{"role": "user", "content": "x"}],
                        },
                        headers={"Authorization": f"Bearer {token}"},
                    )
                    assert result.status_code == 200, result.text
                rows = await _terminal_receipts(postgres_url, trial_id)
                assert len(rows) == (1 if mode == "success" else 2)
                assert len({row["request_id"] for row in rows}) == 1
                assert [row["dispatch_ordinal"] for row in rows] == list(range(1, len(rows) + 1))
                assert all(row["provider_outcome"] == "response_received" for row in rows)
                assert all(row["gateway_outcome"] == "completed" for row in rows)
                if mode == "probe":
                    assert counts == {"model": 1, "probe": 1}
                    assert [row["purpose"] for row in rows] == ["capability_probe", "model_call"]
                if mode == "retry":
                    assert counts == {"model": 2, "probe": 0}
                    assert [row["attempt"] for row in rows] == [1, 2]
                    assert [row["provider_http_status"] for row in rows] == [503, 200]
                engine = create_engine(postgres_url)
                with engine.connect() as connection:
                    calls = connection.execute(
                        select(
                            LlmCall.input_tokens,
                            LlmCall.output_tokens,
                            LlmCall.attempt,
                        ).where(LlmCall.trial_id == trial_id)
                    ).all()
                engine.dispose()
                assert calls == [(3, 2, 2 if mode == "retry" else 1)]
        finally:
            _cleanup_gateway(
                postgres_url=postgres_url,
                team_id=team_id,
                trial_id=trial_id,
                task_id=task_id,
                secret_ref=ref,
            )


@pytest.mark.parametrize("facade", [False, True])
@pytest.mark.parametrize("upstream_status", [200, 400])
async def test_true_stream_deadline_keeps_transport_audit(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    facade: bool,
    upstream_status: int,
) -> None:
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "local-placeholder-provider-key")
    provider = FastAPI()
    received = []
    release_provider = asyncio.Event()

    @provider.post("/{rest:path}")
    async def stream(rest: str) -> StreamingResponse:
        received.append(rest)

        async def chunks():
            yield b"event: ping\ndata: {}\n\n"
            await asyncio.wait_for(release_provider.wait(), timeout=15)
            yield b"event: message_stop\ndata: {}\n\n"

        return StreamingResponse(
            chunks(),
            media_type="text/event-stream",
            status_code=upstream_status,
        )

    async with _serve(provider) as provider_url:
        monkeypatch.setattr(messages, "ANTHROPIC_BASE_URL", provider_url)
        settings, team_id, trial_id, connection_id, task_id, ref = await _seed_gateway(
            postgres_url=postgres_url,
            base_url=provider_url,
        )
        engine = create_engine(postgres_url)
        with engine.begin() as connection:
            connection.execute(
                update(ProviderConnection)
                .where(
                    ProviderConnection.id == connection_id,
                )
                .values(provider_type="anthropic")
            )
        engine.dispose()
        try:
            async with _serve(create_app(settings)) as gateway_url:
                async with httpx.AsyncClient(base_url=gateway_url, timeout=10) as client:
                    token = mint_step_jwt(
                        team_id=team_id,
                        trial_id=trial_id,
                        step_id="main",
                        ttl_sec=360,
                        signing_key=settings.step_jwt_signing_key.get_secret_value(),
                        provider_connection_id=connection_id if facade else None,
                        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=5),
                        agent_attempt_id=uuid4(),
                        step_jwt_id=uuid4(),
                    )
                    try:
                        result = await client.post(
                            "/anthropic/v1/messages" if facade else "/v1/messages",
                            json={"model": "m", "messages": [], "max_tokens": 1, "stream": True},
                            headers={"Authorization": f"Bearer {token}"},
                        )
                    except httpx.RemoteProtocolError:
                        assert upstream_status == 200
                        pass  # An interrupted already-started SSE response.
                    else:
                        if upstream_status == 200:
                            assert result.status_code == 200, result.text
                            assert "ping" in result.text and "message_stop" not in result.text
                        else:
                            assert result.status_code == 504, result.text
                            assert result.json()["detail"]["code"] == "agent_timeout"
                    finally:
                        release_provider.set()
                rows = await _terminal_receipts(postgres_url, trial_id)
                assert len(received) == 1 and len(rows) == 1
                assert rows[0]["provider_outcome"] == "deadline"
                assert rows[0]["gateway_outcome"] == "deadline"
                assert rows[0]["provider_http_status"] == upstream_status
                # Headers had already gone to the caller before stream expiry.
                assert rows[0]["gateway_http_status"] == (200 if upstream_status == 200 else 504)
        finally:
            _cleanup_gateway(
                postgres_url=postgres_url,
                team_id=team_id,
                trial_id=trial_id,
                task_id=task_id,
                secret_ref=ref,
            )
