"""Real PostgreSQL and HTTP boundaries for the isolated Terminus call ledger."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import LlmCall, ServiceExecutionTarget
from loom_control_plane.service_execution import enqueue_execution_transition
from loom_control_plane.service_execution_output import VerifiedExecutionPod
from loom_llm_gateway.routes import (
    chat,
    facade_openai,
    gemini,
    messages,
    responses,
    service_execution,
)
from tests.integration.gateway_db import admit_trial_model
from tests.integration.test_gateway_facade_openai import facade_setup  # noqa: F401
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def test_call_ledger_selects_current_lease_and_generation_and_fences_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    app = FastAPI()
    app.state.session_factory = sessions
    app.include_router(service_execution.router)
    ids = []
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease.pod_ip = "10.42.0.12"
            lease.pod_uid = "bound-pod"
            lease.observed_state = "running"
            target_row = await session.get(ServiceExecutionTarget, target.target_id)
            target_row.spec_json = {
                **target_row.spec_json, "cluster_scope_id": "primary",
                "pod_identity_audience": "loom-execution",
            }
            app.state.execution_pod_reviewer = SimpleNamespace(review=AsyncMock(
                return_value=VerifiedExecutionPod(
                    cluster_scope_id="primary", namespace=lease.namespace_name,
                    service_account="loom-execution-attempt", audience="loom-execution",
                    pod_uid="bound-pod",
                ),
            ))
            for lease_key, generation, step in (
                (str(lease.id), 1, "agent"), (str(uuid4()), 1, "agent"),
                (str(lease.id), 2, "agent"), (str(lease.id), 1, "verifier"),
            ):
                row_id = uuid4()
                ids.append(row_id)
                session.add(LlmCall(
                    id=row_id, team_id=lease.team_id, trial_id=trial_id, step_id=step,
                    model="glm-5.2", dialect="openai_facade", input_tokens=10, output_tokens=5,
                    cost_usd=Decimal("0.01"), rate_card_hash="test-rate", attempt=1,
                    # All timestamps overlap: timestamp guessing cannot pass.
                    captured_at=now, provider_extras={
                        "reasoning_tokens": 2,
                        "_loom_raw_provider_log": {"service_execution": {
                            "lease_id": lease_key, "generation": generation,
                        }, "request": {"body": {"messages": ["private prompt"]}}},
                    },
                ))
            await session.commit()
            lease_id, team_id = lease.id, lease.team_id
        headers = {
            "X-Loom-Execution-Lease-Id": str(lease_id),
            "X-Loom-Execution-Generation": "1", "X-Loom-Execution-Role": "attempt",
            "Authorization": "Bearer bound-pod-identity",
        }
        transport = httpx.ASGITransport(app=app, client=("192.0.2.10", 1234))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/internal/service-execution/llm-calls",
                headers={key: value for key, value in headers.items() if key != "Authorization"},
            )
            assert response.status_code == 403
            response = await client.get("/internal/service-execution/llm-calls", headers=headers)
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["trial_id"] == str(trial_id) and body["team_id"] == str(team_id)
            assert [row["id"] for row in body["items"]] == [str(ids[0])]
            assert body["items"][0]["provider_extras"] == {"reasoning_tokens": 2}
            response = await client.get(
                "/internal/service-execution/llm-calls?trial_id=foreign", headers=headers,
            )
            assert response.status_code == 403
            response = await client.get(
                "/internal/service-execution/llm-calls",
                headers={**headers, "X-Loom-Execution-Generation": "2"},
            )
            assert response.status_code == 409
            async with sessions() as session:
                await enqueue_execution_transition(
                    session, lease_id=lease_id, expected_generation=1,
                    desired_state="cancel", now=datetime.now(UTC),
                )
                await session.commit()
            response = await client.get("/internal/service-execution/llm-calls", headers=headers)
            assert response.status_code == 409
            assert response.json()["detail"] == "execution_generation_fenced"
    finally:
        async with sessions() as session:
            await session.execute(delete(LlmCall).where(LlmCall.id.in_(ids)))
            await session.commit()
        await engine.dispose()


@pytest.mark.parametrize("stream", [False, True])
async def test_facade_records_lease_from_auth_for_json_and_sse(
    facade_setup, monkeypatch: pytest.MonkeyPatch, stream: bool,  # noqa: F811
) -> None:
    app, token, _team_id, trial_id, connection_id, _captures = facade_setup
    await admit_trial_model(app, trial_id, name="glm-5.2")
    lease_id = uuid4()
    original_auth = facade_openai.verify_facade_auth

    async def authenticated_lease(*args, **kwargs):
        context = await original_auth(*args, **kwargs)
        return replace(context, service_execution_lease_id=lease_id, service_execution_generation=4)

    monkeypatch.setattr(facade_openai, "verify_facade_auth", authenticated_lease)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/openai/v1/chat/completions", headers={
            "Authorization": f"Bearer {token}", "x-loom-provider-connection-id": str(connection_id),
            "X-Loom-Execution-Lease-Id": "forged-header",
        }, json={
            "model": "glm-5.2", "stream": stream,
            "messages": [{"role": "user", "content": "hello"}],
            "service_execution": {"lease_id": "forged-body", "generation": 99},
        })
    assert response.status_code == 200, response.text
    async with app.state.session_factory() as session:
        row = (await session.execute(select(LlmCall).where(LlmCall.trial_id == trial_id))).scalar_one()
        assert row.provider_extras["_loom_raw_provider_log"]["service_execution"] == {
            "lease_id": str(lease_id), "generation": 4,
        }
    if stream:
        assert "data: [DONE]" in response.text


def _native_lease_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every model route see a native service-execution bearer.

    The real bearer is authenticated first; only the lease fields a native
    Pod token carries are added, as in the test above.
    """
    lease = {"service_execution_lease_id": uuid4(), "service_execution_generation": 1}
    for module in (facade_openai, responses, messages):
        original = module.verify_facade_auth

        async def native(*args, _original=original, **kwargs):
            return replace(await _original(*args, **kwargs), **lease)

        monkeypatch.setattr(module, "verify_facade_auth", native)
    for module in (chat, gemini):
        bearer = module.require_llm_call_bearer

        async def native_bearer(*args, _bearer=bearer, **kwargs):
            return replace(await _bearer(*args, **kwargs), **lease)

        monkeypatch.setattr(module, "require_llm_call_bearer", native_bearer)
        # The live-lease re-check has its own tests; this one is about the model.
        monkeypatch.setattr(module, "authorize_trial_execution_dispatch", AsyncMock())


@pytest.mark.parametrize(("path", "body"), [
    ("/openai/v1/chat/completions", {"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
    ("/v1/chat/completions", {"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
    ("/v1/chat/completions", {"model": "anthropic/glm-5.2", "messages": [{"role": "user", "content": "hi"}]}),
    ("/v1/responses", {"model": "gpt-4o", "input": "hi"}),
    ("/v1/messages", {"model": "glm-5.2", "max_tokens": 8, "messages": [{"role": "user", "content": "hi"}]}),
    ("/v1beta/models/glm-5.2:generateContent", {"contents": [{"parts": [{"text": "hi"}]}]}),
])
async def test_native_call_for_another_model_is_refused_before_any_provider(
    facade_setup, monkeypatch: pytest.MonkeyPatch, path: str, body: dict,  # noqa: F811
) -> None:
    """Native Trials may call only their admitted model. An off-model call,
    including the admitted name on another provider's route, is refused
    before any upstream request, so it records nothing and costs nothing."""
    app, token, _team_id, trial_id, connection_id, captures = facade_setup
    await admit_trial_model(app, trial_id, name="glm-5.2")
    _native_lease_auth(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(path, json=body, headers={
            "Authorization": f"Bearer {token}", "x-loom-provider-connection-id": str(connection_id),
        })
    assert response.status_code == 403, response.text
    assert response.json()["detail"] == "service execution model forbidden"
    assert captures["requests"] == []
    async with app.state.session_factory() as session:
        assert (await session.execute(select(LlmCall).where(LlmCall.trial_id == trial_id))).first() is None


@pytest.mark.parametrize(("path", "body"), [
    ("/openai/v1/chat/completions", {"model": "glm-5.2", "messages": [{"role": "user", "content": "hi"}]}),
    ("/v1/chat/completions", {"model": "glm-5.2", "messages": [{"role": "user", "content": "hi"}]}),
])
async def test_native_call_for_the_admitted_model_is_served(
    facade_setup, monkeypatch: pytest.MonkeyPatch, path: str, body: dict,  # noqa: F811
) -> None:
    app, token, _team_id, trial_id, connection_id, captures = facade_setup
    await admit_trial_model(app, trial_id, name="glm-5.2")
    _native_lease_auth(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(path, json=body, headers={
            "Authorization": f"Bearer {token}", "x-loom-provider-connection-id": str(connection_id),
        })
    assert response.status_code == 200, response.text
    assert len(captures["requests"]) == 1


async def test_non_native_calls_are_not_model_bound(facade_setup) -> None:  # noqa: F811
    # Ordinary step tokens (no service-execution lease) keep their behaviour.
    app, token, _team_id, trial_id, connection_id, captures = facade_setup
    await admit_trial_model(app, trial_id, name="glm-5.2")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/openai/v1/chat/completions", json={
            "model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}],
        }, headers={"Authorization": f"Bearer {token}", "x-loom-provider-connection-id": str(connection_id)})
    assert response.status_code == 200, response.text
    assert len(captures["requests"]) == 1
