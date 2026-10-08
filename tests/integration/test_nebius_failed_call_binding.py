"""Failed provider requests must remain in the authenticated native call ledger."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, update

from loom.db.schema import LlmCall, ProviderConnection
from loom.llm_call_ledger import read_service_execution_llm_calls
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.routes import (
    chat,
    facade_anthropic,
    facade_google,
    facade_openai,
    gemini,
    messages,
    responses,
)
from tests.integration.gateway_db import admit_trial_model
from tests.integration.test_gateway_facade_openai import facade_setup  # noqa: F401


@pytest.mark.parametrize("failure", ["timeout", "transport", "http"])
@pytest.mark.parametrize("dialect", [
    "openai", "anthropic", "google", "chat", "responses", "messages", "gemini",
])
async def test_failed_facade_call_keeps_authenticated_native_binding(
    facade_setup, monkeypatch: pytest.MonkeyPatch, failure: str, dialect: str,  # noqa: F811
) -> None:
    app, token, team_id, trial_id, connection_id, _captures = facade_setup
    lease_id = uuid4()
    module, path, provider_type = {
        "openai": (facade_openai, "/openai/v1/chat/completions", "openai-compatible"),
        "anthropic": (facade_anthropic, "/anthropic/v1/messages", "anthropic"),
        "google": (facade_google, "/google/v1beta/models/glm-5.2:generateContent", "google"),
        "chat": (chat, "/v1/chat/completions", "openai-compatible"),
        "responses": (responses, "/openai/v1/responses", "openai-compatible"),
        "messages": (messages, "/v1/messages", "anthropic"),
        "gemini": (gemini, "/v1beta/models/glm-5.2:generateContent", "google"),
    }[dialect]
    auth_name = "require_llm_call_bearer" if dialect in {"chat", "gemini"} else "verify_facade_auth"
    # The native Trial's admitted model, on the provider this route records.
    await admit_trial_model(app, trial_id, name="glm-5.2", provider={
        "anthropic": "anthropic", "messages": "anthropic", "google": "google", "gemini": "google",
    }.get(dialect, "openai"))
    original_auth = getattr(module, auth_name)

    async def authenticated_lease(*args, **kwargs):
        # Exercise normal bearer verification, then the already-authenticated
        # native context boundary; real lease/JWT fences have their own tests.
        ctx = await original_auth(*args, **kwargs)
        return replace(ctx, step_id="agent", service_execution_lease_id=lease_id,
                       service_execution_generation=4)

    monkeypatch.setattr(module, auth_name, authenticated_lease)
    if dialect in {"chat", "gemini"}:
        from unittest.mock import AsyncMock
        monkeypatch.setattr(module, "authorize_trial_execution_dispatch", AsyncMock())
    from pydantic import SecretStr
    app.state.settings.anthropic_api_key = SecretStr("test-provider-key")
    app.state.settings.google_api_key = SecretStr("test-provider-key")
    async with app.state.session_factory() as session:
        await session.execute(update(ProviderConnection).where(
            ProviderConnection.id == connection_id,
        ).values(provider_type=provider_type))
        await session.commit()
    app.state.settings.llm_retry_max_attempts = 1

    def upstream(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("no upstream response", request=request)
        if failure == "transport":
            raise httpx.ConnectError("connection failed", request=request)
        return httpx.Response(503, json={"error": "upstream unavailable"})

    await app.state.egress_client_pool.aclose()
    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client, proxy_url="",
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(path, headers={
            "Authorization": f"Bearer {token}",
            "x-loom-provider-connection-id": str(connection_id),
            "X-Loom-Execution-Lease-Id": "forged-header",
        }, json={
            "model": "openai/glm-5.2" if dialect == "chat" else "glm-5.2", "max_tokens": 16,
            "input": "hello",
            "messages": [{"role": "user", "content": "hello"}],
            "contents": [{"parts": [{"text": "hello"}]}],
            "service_execution": {"lease_id": "forged-body", "generation": 99},
        })
    assert response.status_code == {"timeout": 504, "transport": 502, "http": 503}[failure]
    async with app.state.session_factory() as session:
        row = (await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id))).one()
        assert row.provider_extras["_loom_usage_status"] == "missing"
        assert row.provider_extras["_loom_call_status"] == "failed"
        assert row.input_tokens == row.output_tokens == row.cost_usd == 0
        assert row.provider_extras.get("_loom_raw_provider_log", {}).get("service_execution") == {
            "lease_id": str(lease_id), "generation": 4,
        }
        lease = SimpleNamespace(id=lease_id, trial_id=trial_id, team_id=team_id, generation=4)
        calls = await read_service_execution_llm_calls(session, lease)
        assert [call["id"] for call in calls] == [str(row.id)]
        assert calls[0]["call_status"] == "failed"
        assert calls[0]["provider_extras"]["_loom_usage_status"] == "missing"
        assert await read_service_execution_llm_calls(session, lease, generation=5) == []
        lease.team_id = uuid4()
        assert await read_service_execution_llm_calls(session, lease) == []


def test_accounting_projection_preserves_unknown_usage_without_private_provider_data() -> None:
    from datetime import UTC, datetime

    from loom.llm_call_ledger import serialize_execution_accounting_call

    row = LlmCall(
        id=uuid4(), trial_id=uuid4(), step_id="agent", dialect="openai_facade",
        model="glm-5.2", input_tokens=0, output_tokens=0, cost_usd=0,
        rate_card_hash="failed-upstream", captured_at=datetime.now(UTC), attempt=1,
        provider_extras={
            "_loom_call_status": "failed", "_loom_usage_status": "missing",
            "_loom_failure_category": "upstream_timeout",
            "_loom_failure_error_type": "ReadTimeout",
            "private": "do not export", "_loom_raw_provider_log": {"private": "secret"},
        },
    )
    projected = serialize_execution_accounting_call(row)
    assert projected["provider_extras"] == {
        "_loom_call_status": "failed", "_loom_usage_status": "missing",
        "_loom_failure_category": "upstream_timeout",
    }
    row.provider_extras = {
        "_loom_call_status": "failed", "_loom_usage_status": "private-string",
        "_loom_failure_category": "private-string",
    }
    assert serialize_execution_accounting_call(row)["provider_extras"] == {
        "_loom_call_status": "failed",
    }


async def test_native_failure_route_rejects_unbound_lease_before_dispatch(facade_setup) -> None:  # noqa: F811
    from datetime import UTC, datetime, timedelta

    from loom.auth import mint_step_jwt

    app, _token, team_id, trial_id, connection_id, captures = facade_setup
    token = mint_step_jwt(
        team_id=team_id, trial_id=trial_id, step_id="agent", ttl_sec=60,
        signing_key=app.state.settings.step_jwt_signing_key.get_secret_value(),
        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=60),
        provider_connection_id=connection_id, provider_connection_id_bound=True,
        step_jwt_id=uuid4(), service_execution_lease_id=uuid4(),
        service_execution_generation=4, service_execution_role="attempt",
        service_execution_runtime_contract_sha256="sha256:" + "a" * 64,
        service_execution_candidate_sha="b" * 40,
        service_execution_task_revision_sha256="sha256:" + "c" * 64,
        service_execution_command_identity_sha256="sha256:" + "d" * 64,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/openai/v1/chat/completions", headers={
            "Authorization": f"Bearer {token}",
        }, json={"model": "glm-5.2", "messages": [{"role": "user", "content": "hello"}]})
    assert response.status_code == 403
    assert captures["requests"] == []
    async with app.state.session_factory() as session:
        assert list(await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id))) == []


@pytest.mark.parametrize("boundary", ["missing_generation", "different_trial", "different_team"])
async def test_failed_writer_rejects_misbound_native_context(boundary: str) -> None:
    from unittest.mock import AsyncMock

    from loom.auth import AuthContext
    from loom_llm_gateway.llm_calls import record_failed_call

    trial_id, team_id = uuid4(), uuid4()
    ctx = AuthContext(
        token_hash=b"", type="step_session", scopes=["llm:call"],
        team_id=uuid4() if boundary == "different_team" else team_id,
        trial_id=uuid4() if boundary == "different_trial" else trial_id,
        expires_at=None, step_id="agent", service_execution_lease_id=uuid4(),
        service_execution_generation=None if boundary == "missing_generation" else 4,
    )
    session = AsyncMock()
    with pytest.raises(ValueError, match="service execution call"):
        await record_failed_call(
            session, team_id=team_id, trial_id=trial_id, step_id="agent",
            dialect="openai_facade", model="glm-5.2", failure_category="upstream_timeout",
            auth_context=ctx,
        )
    session.execute.assert_not_called()
    session.commit.assert_not_called()


@pytest.mark.parametrize("dispatched", [False, True])
async def test_signed_deadline_audits_only_dispatched_calls(
    facade_setup, monkeypatch: pytest.MonkeyPatch, dispatched: bool,  # noqa: F811
) -> None:
    import asyncio
    import time

    from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline

    app, token, _team_id, trial_id, connection_id, _captures = facade_setup
    await admit_trial_model(app, trial_id, name="glm-5.2")
    lease_id = uuid4()
    original_auth = facade_openai.verify_facade_auth
    attempts = []

    async def authenticated_lease(*args, **kwargs):
        ctx = await original_auth(*args, **kwargs)
        kwargs["request"].state.loom_gateway_attempt_deadline = GatewayAttemptDeadline(
            time.monotonic() + (0.1 if dispatched else -1),
        )
        return replace(ctx, step_id="agent", service_execution_lease_id=lease_id,
                       service_execution_generation=4)

    monkeypatch.setattr(facade_openai, "verify_facade_auth", authenticated_lease)

    async def upstream(request: httpx.Request) -> httpx.Response:
        attempts.append(request.url.path)
        await asyncio.sleep(1)
        return httpx.Response(200, json={})

    await app.state.egress_client_pool.aclose()
    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client, proxy_url="",
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/openai/v1/chat/completions", headers={
            "Authorization": f"Bearer {token}",
            "x-loom-provider-connection-id": str(connection_id),
        }, json={"model": "glm-5.2", "messages": [{"role": "user", "content": "hello"}]})
    assert response.status_code == 504
    assert bool(attempts) == dispatched
    async with app.state.session_factory() as session:
        rows = list(await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id)))
        assert len(rows) == int(dispatched)
        if dispatched:
            assert rows[0].provider_extras["_loom_failure_category"] == "attempt_deadline_reached"
            assert rows[0].provider_extras["_loom_usage_status"] == "missing"
            assert rows[0].provider_extras["_loom_raw_provider_log"]["service_execution"] == {
                "lease_id": str(lease_id), "generation": 4,
            }
