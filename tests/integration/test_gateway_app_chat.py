"""POST /v1/chat/completions with LiteLLM stubbed."""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import (
    LlmCall,
    ProviderConnection,
    ProviderConnectionShare,
    RateCard,
    Secret,
    Team,
    Token,
)
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from tests.integration.gateway_db import (
    delete_gateway_trial,
    delete_team_and_quota,
    delete_teams_and_quotas,
    insert_gateway_trial,
)


@pytest.fixture
def seed_data(postgres_url: str) -> tuple[UUID, str]:
    # postgres_url uses postgresql+psycopg://; create_engine handles sync over psycopg 3.
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_token.encode()).digest(),
                type="team",
                scopes=["submit", "llm:call"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.execute(
            insert(RateCard).values(
                id="card-1",
                captured_at=datetime.now(UTC),
                table={
                    "id": "card-1",
                    "entries": [
                        {
                            "provider": "anthropic",
                            "model": "claude-opus-4-7",
                            "input_per_mtok": 3.0,
                            "output_per_mtok": 15.0,
                            "cache_read_per_mtok": 0.3,
                            "cache_write_per_mtok": 3.75,
                        }
                    ],
                },
            )
        )
        s.commit()
    yield team_id, raw_token
    # Cleanup so tests are isolated. Order matters: child tables before
    # parents (ProviderConnection/Secret FK → Team).
    with session_factory() as s:
        from sqlalchemy import delete

        s.execute(delete(LlmCall))
        s.execute(delete(ProviderConnection))
        s.execute(delete(Secret))
        s.execute(delete(Token))
        delete_teams_and_quotas(s, (team_id,))
        s.execute(delete(RateCard))
        s.commit()
    engine.dispose()


@pytest.fixture
def gateway_trial_factory(
    postgres_url: str,
    seed_data: tuple[UUID, str],
):
    """Create and track real Trial owners required by gateway writes."""
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    seed_team_id, _raw_token = seed_data
    seeded_trials: list[tuple[UUID, str, UUID]] = []

    def _insert(team_id: UUID) -> UUID:
        trial_id = uuid4()
        with session_factory() as s:
            task_id = insert_gateway_trial(s, team_id=team_id, trial_id=trial_id)
            s.commit()
        seeded_trials.append((trial_id, task_id, team_id))
        return trial_id

    try:
        yield _insert
    finally:
        with session_factory() as s:
            for trial_id, task_id, _team_id in seeded_trials:
                s.execute(delete(LlmCall).where(LlmCall.trial_id == trial_id))
                delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            delete_teams_and_quotas(
                s,
                {
                    team_id
                    for _trial_id, _task_id, team_id in seeded_trials
                    if team_id != seed_team_id
                },
            )
            s.commit()
        engine.dispose()


@pytest.fixture
def seeded_trial_id(gateway_trial_factory, seed_data):  # type: ignore[no-untyped-def]
    team_id, _raw_token = seed_data
    return gateway_trial_factory(team_id)


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed_data: tuple[UUID, str],
):
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
    settings = GatewaySettings(_env_file=None)
    a = create_app(settings)

    async def stub(**kwargs: Any) -> dict[str, Any]:
        return {
            "id": "stub",
            "model": kwargs.get("model"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "stubbed"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
        }

    monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", stub)
    return a


def test_chat_returns_loom_event_payload(  # type: ignore[no-untyped-def]
    app,
    seed_data,
    seeded_trial_id,
):
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["choices"][0]["message"]["content"] == "stubbed"
        assert "loom" in body
        assert body["loom"]["input_tokens"] == 10
        assert body["loom"]["output_tokens"] == 5
        assert body["loom"]["cost_usd"] > 0
        assert "rate_card_hash" in body["loom"]
        assert "gateway_request_id" in body["loom"]
        assert body["loom"]["attempt"] == 1


def test_chat_rejects_missing_loom_block(app, seed_data):  # type: ignore[no-untyped-def]
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert r.status_code == 400
        assert r.json()["detail"] == ("loom attribution is required without a step-scoped token")


def test_chat_step_token_supplies_loom_attribution(
    app,
    seed_data,
    seeded_trial_id,
):  # type: ignore[no-untyped-def]
    team_id, _ = seed_data
    with TestClient(app) as client:
        settings: GatewaySettings = app.state.settings
        step_jwt = mint_step_jwt(
            team_id=team_id,
            trial_id=seeded_trial_id,
            step_id="main",
            ttl_sec=60,
            signing_key=settings.step_jwt_signing_key.get_secret_value(),
        )
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    assert response.status_code == 200, response.text
    assert response.json()["choices"][0]["message"]["content"] == "stubbed"


def test_chat_rejects_bad_token(app, seed_data):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer bogus"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(uuid4()),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 401
        assert r.headers["www-authenticate"] == 'Bearer error="invalid_token"'
        assert r.json()["detail"] == {"code": "invalid_or_expired_bearer"}


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Basic opaque"},
        {"Authorization": "Bearer"},
    ],
)
def test_chat_rejects_missing_or_malformed_bearer_with_same_contract(
    app,
    seed_data,
    headers,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        response = client.post(
            "/v1/chat/completions",
            headers=headers,
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(uuid4()),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == 'Bearer error="invalid_token"'
    assert response.json()["detail"] == {"code": "invalid_or_expired_bearer"}


def test_chat_rejects_valid_token_without_llm_call_scope(
    app,
    seed_data,
    postgres_url,
):  # type: ignore[no-untyped-def]
    team_id, _ = seed_data
    raw_token = f"loom_team_{uuid4().hex}"
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_token.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
    assert r.status_code == 403
    assert r.json()["detail"] == {
        "code": "missing_scope",
        "required_scope": "llm:call",
    }


def test_chat_rejects_worker_token_for_model_call(
    app,
    seed_data,
    postgres_url,
):  # type: ignore[no-untyped-def]
    team_id, _ = seed_data
    worker_token = f"loom_worker_{uuid4().hex}"
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(worker_token.encode()).digest(),
                type="worker",
                scopes=["worker:register", "worker:claim", "worker:report"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
    assert r.status_code == 403
    assert r.json()["detail"] == {
        "code": "missing_scope",
        "required_scope": "llm:call",
    }


def test_chat_rejects_team_id_mismatch(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 1: a client-supplied loom.team_id that doesn't
    match the bearer token's team_id must be rejected. Otherwise team A
    can attribute spend to team B by lying in the body."""
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(uuid4()),  # wrong team
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 403
        assert "team_id" in r.json()["detail"]


def test_chat_rejects_missing_model_with_400(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 3: missing required `model` field → 400, not 500."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400


def test_chat_strips_reserved_body_kwargs(  # type: ignore[no-untyped-def]
    app,
    seed_data,
    seeded_trial_id,
):
    """Regression for Bug 4: a body containing reserved kwargs (api_key,
    timeout) must not duplicate-shadow the route's explicit named args."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                },
                # Reserved keys that used to cause TypeError → 500.
                "api_key": "client-attempt-to-override",
                "timeout": 9999,
            },
        )
        assert r.status_code == 200, r.text


def test_chat_rejects_unsupported_provider(app, seed_data):  # type: ignore[no-untyped-def]
    """Regression for Bug 6: an unknown provider in model="X/Y" → 400 with
    a clear allowed-providers list, instead of silently passing api_key=None."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "custom-co/foo-bar",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400
        assert "unsupported provider" in r.json()["detail"]


@pytest.mark.parametrize(
    "model", ["local/yibu/glm-5.2", "huggingface/meta-llama/Llama-3-8B", "local-vllm/llama3"],
)
def test_chat_rejects_retired_hosted_model_sources(app, seed_data, model):  # type: ignore[no-untyped-def]
    """#2054: the hosted `local-server` and `hf` routes are retired."""
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400
        assert "unsupported provider" in r.json()["detail"]


def test_chat_rejects_unknown_model_with_400(app, seed_data):  # type: ignore[no-untyped-def]
    """Spec: unknown rate-card lookup → 400 with structured detail."""
    _, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/gpt-99",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(seed_data[0]),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )
        assert r.status_code == 400
        assert "no entry" in r.json()["detail"]


# ─────────────────────────────────────────────────────────────────────
# BYO provider connection routing (#178 + #179)
# ─────────────────────────────────────────────────────────────────────

import base64  # noqa: E402
import json  # noqa: E402

import httpx  # noqa: E402

from loom.security.secret_store import LocalEncryptedSecretStore  # noqa: E402

# Deterministic key — matches facade tests so SecretStore decrypts.
_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
def app_with_byo(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed_data: tuple[UUID, str],
):
    """Same as `app` but with the SecretStore master key wired."""
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub-platform-key")
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _TEST_MASTER_KEY)
    settings = GatewaySettings(_env_file=None)
    a = create_app(settings)

    captured: dict[str, Any] = {}

    async def unexpected_litellm_stub(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        raise AssertionError(
            "BYO openai-compatible chat must use the egress client pool, "
            "not LiteLLM's internal HTTP client",
        )

    monkeypatch.setattr(
        "loom_llm_gateway.litellm_wrapper.acompletion",
        unexpected_litellm_stub,
    )
    return a, captured


def _seed_byo_connection(
    postgres_url: str,
    team_id: UUID,
    *,
    api_key: str = "sk-real-byo-key",
    base_url: str = "https://byo.example.com/v1",
    provider_type: str = "openai-compatible",
    pricing_source: str = "tokens-only",
) -> UUID:
    """Insert a provider connection with a SecretStore-encrypted api_key."""
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    async_url = postgres_url  # already postgresql+psycopg per test conftest
    engine = create_async_engine(async_url)
    sf = async_sessionmaker(engine, expire_on_commit=False)

    async def _insert() -> UUID:
        async with sf() as s:
            store = LocalEncryptedSecretStore(s)
            ref = await store.put(
                namespace=str(team_id),
                value=api_key,
            )
            await s.commit()
        conn_id = uuid4()
        async with sf() as s:
            await s.execute(
                insert(ProviderConnection).values(
                    id=conn_id,
                    team_id=team_id,
                    provider_type=provider_type,
                    display_name=f"byo-{conn_id}",
                    base_url=base_url,
                    upstream_host="byo.example.com",
                    resolved_egress_ips=["192.0.2.1"],
                    encrypted_api_key_ref=ref,
                    pricing_source=pricing_source,
                    status="valid",
                    created_by="team:test",
                )
            )
            await s.commit()
        return conn_id

    cid = asyncio.run(_insert())
    asyncio.run(engine.dispose())
    return cid


class _CapturingEgressPool:
    def __init__(
        self,
        response: httpx.Response | None = None,
        *,
        reject_null_message_fields: bool = False,
    ) -> None:
        self.connection_ids: list[Any] = []
        self.requests: list[httpx.Request] = []
        self.reject_null_message_fields = reject_null_message_fields
        self.response = response or httpx.Response(
            200,
            json={
                "id": "stub",
                "model": "some-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "stubbed"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                },
            },
        )
        self.client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._handle),
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.reject_null_message_fields:
            body = json.loads(request.content)
            for message in body.get("messages", []):
                if any(
                    message.get(field) is None and field in message
                    for field in ("name", "tool_calls", "tool_call_id")
                ):
                    return httpx.Response(
                        400,
                        json={"error": "explicit null chat message field"},
                    )
        return self.response

    async def get(self, connection_id: Any) -> httpx.AsyncClient:
        self.connection_ids.append(connection_id)
        return self.client

    async def aclose(self) -> None:
        await self.client.aclose()


def test_chat_byo_uses_egress_pool_for_openai_compatible(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    """#216: BYO openai-compatible chat must use EgressClientPool so
    Envoy sees the per-connection CONNECT header configured by the pool."""
    app, captured = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        api_key="sk-real-byo-key",
        base_url="https://byo.example.com/v1",
    )
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 200, r.text
    assert captured == {}
    assert pool.connection_ids == [conn_id]
    assert len(pool.requests) == 1
    upstream_request = pool.requests[0]
    assert upstream_request.method == "POST"
    assert str(upstream_request.url) == ("https://byo.example.com/v1/chat/completions")
    assert upstream_request.headers["Authorization"] == ("Bearer sk-real-byo-key")
    assert json.loads(upstream_request.content) == {
        "model": "some-model",
        "messages": [{"role": "user", "content": "hi"}],
    }


def test_chat_byo_accepts_header_only_connection_id(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    app, _ = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(postgres_url, team_id)
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {raw_token}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                },
            },
        )
    assert r.status_code == 200, r.text
    assert pool.connection_ids == [conn_id]


def test_chat_byo_accepts_matching_header_and_body(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    app, _ = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(postgres_url, team_id)
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {raw_token}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 200, r.text
    assert pool.connection_ids == [conn_id]


def test_chat_byo_rejects_mismatched_header_and_body(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    postgres_url,
):
    app, _ = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(postgres_url, team_id)
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {raw_token}",
                "x-loom-provider-connection-id": str(uuid4()),
            },
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 400
    assert "body says" in r.json()["detail"]
    assert "header says" in r.json()["detail"]
    assert pool.connection_ids == []


def test_chat_byo_omits_null_optional_message_fields_before_forwarding(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    """Strict OpenAI-compatible providers reject explicit null fields.

    The gateway should forward the compact chat wire shape upstream even if
    a platform client sends Pydantic defaults such as ``tool_calls=None``.
    """
    app, captured = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        api_key="sk-real-byo-key",
        base_url="https://byo.example.com/v1",
    )
    pool = _CapturingEgressPool(reject_null_message_fields=True)
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/some-model",
                "messages": [
                    {
                        "role": "user",
                        "content": "hi",
                        "name": None,
                        "tool_calls": None,
                        "tool_call_id": None,
                    }
                ],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 200, r.text
    assert captured == {}
    assert len(pool.requests) == 1
    assert json.loads(pool.requests[0].content)["messages"] == [
        {"role": "user", "content": "hi"},
    ]


def test_chat_byo_tokens_only_skips_missing_rate_card(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    """#179: BYO connection with pricing_source=tokens-only on a model
    NOT in the rate-card table should succeed with cost=0, NOT 400."""
    app, _captured = app_with_byo
    team_id, raw_token = seed_data
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        pricing_source="tokens-only",
    )
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                # gpt-99 is intentionally NOT in any rate card
                "model": "openai/gpt-99",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["loom"]["cost_usd"] == 0.0
    assert body["loom"]["rate_card_hash"] == "facade:tokens-only"


def test_chat_byo_records_llm_call_for_usage_attribution(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    """#222: successful BYO chat completions must persist llm_calls.

    The `/api/v1/usage` rollup reads from llm_calls; returning a Loom
    usage block without inserting this row makes BYO chat usage invisible.
    """
    app, _captured = app_with_byo
    team_id, raw_token = seed_data
    trial_id = seeded_trial_id
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        pricing_source="tokens-only",
    )
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/gpt-99",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )

    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(
            conn.execute(
                text("SELECT * FROM llm_calls WHERE trial_id = :trial_id"),
                {"trial_id": trial_id},
            )
        )
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "openai_chat"
    assert row["model"] == "gpt-99"
    assert row["input_tokens"] == 10
    assert row["output_tokens"] == 5
    assert row["provider_extras"]["cache_creation_input_tokens"] == 0
    assert row["provider_extras"]["cache_read_input_tokens"] == 0
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "facade:tokens-only"


def test_chat_byo_upstream_error_redacts_authorization_header(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
    caplog,
):
    """Upstream error bodies can echo provider Authorization headers.

    The gateway preserves the upstream status code, but must not let provider
    Authorization values reach logs or user-visible error text when a BYO
    provider call fails.
    """
    app, _captured = app_with_byo
    team_id, raw_token = seed_data
    api_key = "sk-live-upstream-secret"
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        api_key=api_key,
        base_url="https://httpbin.org/anything",
    )
    pool = _CapturingEgressPool(
        response=httpx.Response(
            401,
            text=(
                "upstream failure: "
                f"{{Authorization: Bearer {api_key}, "
                "Content-Type: application/json}}"
            ),
        ),
    )
    caplog.set_level(logging.WARNING, logger="loom_llm_gateway.routes.chat")

    with TestClient(app, raise_server_exceptions=False) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(seeded_trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )

    assert r.status_code == 401
    combined = "\n".join(
        [r.text, *(record.getMessage() for record in caplog.records)],
    )
    assert api_key not in combined
    assert f"Bearer {api_key}" not in combined
    assert "[REDACTED]" in combined


def test_chat_byo_upstream_500_records_failed_audit_row(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    seeded_trial_id,
    postgres_url,
):
    """#93: an attempted upstream request must not disappear on failure."""
    app, _captured = app_with_byo
    team_id, raw_token = seed_data
    trial_id = seeded_trial_id
    conn_id = _seed_byo_connection(
        postgres_url,
        team_id,
        api_key="sk-failed-upstream-secret",
        base_url="https://httpbin.org/anything",
    )
    pool = _CapturingEgressPool(
        response=httpx.Response(
            500,
            text="upstream exploded Authorization: Bearer sk-failed-upstream-secret",
        ),
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        app.state.egress_client_pool = pool
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "do not persist"}],
                "temperature": 0,
                "top_p": 0.5,
                "seed": 1234,
                "extra_body": {
                    "top_k": 40,
                    "api_key": "sk-request-param-secret",
                },
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "provider_connection_id": str(conn_id),
                },
            },
        )

    assert r.status_code == 500

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(
            conn.execute(
                text("SELECT * FROM llm_calls WHERE trial_id = :trial_id"),
                {"trial_id": trial_id},
            )
        )
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "openai_chat"
    assert row["model"] == "some-model"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert row["cost_usd"] == 0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_5xx",
        "_loom_failure_status_code": 500,
        "_loom_usage_status": "missing",
    }
    assert row["request_params"] == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "top_p": 0.5,
            "seed": 1234,
            "top_k": 40,
        },
    }
    rendered = json.dumps(
        {
            "provider_extras": row["provider_extras"],
            "request_params": row["request_params"],
        }
    )
    assert "do not persist" not in rendered
    assert "messages" not in rendered
    assert "sk-failed-upstream-secret" not in rendered
    assert "sk-request-param-secret" not in rendered


def test_chat_byo_invalid_uuid_returns_400(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
):
    """Malformed provider_connection_id is rejected upfront."""
    app, _ = app_with_byo
    team_id, raw_token = seed_data
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "openai/gpt-3.5-turbo",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                    "provider_connection_id": "not-a-uuid",
                },
            },
        )
    assert r.status_code == 400
    assert "valid UUID" in r.json()["detail"]


def test_chat_byo_shared_connection_uses_jwt_authority_and_target_attribution(
    app_with_byo,
    seed_data,
    gateway_trial_factory,
    postgres_url,
):  # type: ignore[no-untyped-def]
    app, _ = app_with_byo
    owner_team, _ = seed_data
    target_team = uuid4()
    conn_id = _seed_byo_connection(postgres_url, owner_team)

    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(insert(Team).values(id=target_team, name=f"target-{target_team}"))
        conn.execute(
            insert(ProviderConnectionShare).values(
                provider_connection_id=conn_id,
                target_team_id=target_team,
                created_by_actor="test:chat-share",
            )
        )
    engine.dispose()
    target_trial = gateway_trial_factory(target_team)

    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        settings: GatewaySettings = app.state.settings
        step_jwt = mint_step_jwt(
            team_id=target_team,
            trial_id=target_trial,
            step_id="family-evolver",
            ttl_sec=60,
            signing_key=settings.step_jwt_signing_key.get_secret_value(),
            provider_connection_id=conn_id,
        )
        r = client.post(
            "/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(target_team),
                    "trial_id": str(uuid4()),
                    "step_id": "caller-value-is-not-authority",
                    "provider_connection_id": str(conn_id),
                },
            },
        )
    assert r.status_code == 200, r.text
    assert pool.connection_ids == [conn_id]

    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT * FROM llm_calls WHERE trial_id = :trial_id"),
                {"trial_id": target_trial},
            )
            .mappings()
            .one()
        )
    engine.dispose()
    assert row["team_id"] == target_team
    assert row["trial_id"] == target_trial
    assert row["step_id"] == "family-evolver"


def test_chat_rejects_execution_attempt_token_before_dispatch(
    app_with_byo,
    seed_data,
):  # type: ignore[no-untyped-def]
    app, _ = app_with_byo
    team_id, _ = seed_data
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        settings: GatewaySettings = app.state.settings
        step_jwt = mint_step_jwt(
            team_id=team_id,
            execution_attempt_id=uuid4(),
            step_id="generate_card_00",
            ttl_sec=60,
            signing_key=settings.step_jwt_signing_key.get_secret_value(),
        )
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                },
            },
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "execution-attempt tokens are not supported by this route"}
    assert pool.connection_ids == []


def test_chat_byo_jwt_rejects_conflicting_body_before_lookup(
    app_with_byo,
    seed_data,
    postgres_url,
):  # type: ignore[no-untyped-def]
    app, _ = app_with_byo
    team_id, _ = seed_data
    conn_id = _seed_byo_connection(postgres_url, team_id)
    pool = _CapturingEgressPool()
    with TestClient(app) as client:
        app.state.egress_client_pool = pool
        settings: GatewaySettings = app.state.settings
        step_jwt = mint_step_jwt(
            team_id=team_id,
            trial_id=uuid4(),
            step_id="family-evolver",
            ttl_sec=60,
            signing_key=settings.step_jwt_signing_key.get_secret_value(),
            provider_connection_id=conn_id,
        )
        r = client.post(
            "/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "openai/some-model",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(uuid4()),
                    "step_id": "main",
                    "provider_connection_id": str(uuid4()),
                },
            },
        )
    assert r.status_code == 400
    assert "JWT scope is authoritative" in r.json()["detail"]
    assert "body says" in r.json()["detail"]
    assert pool.connection_ids == []


def test_chat_byo_cross_team_returns_404(  # type: ignore[no-untyped-def]
    app_with_byo,
    seed_data,
    postgres_url,
):
    """A team's token cannot read another team's connection — the
    backend returns 404 (not 403) to avoid leaking existence."""
    app, _ = app_with_byo
    team_id, raw_token = seed_data
    # Seed a connection owned by a DIFFERENT team.
    other_team = uuid4()
    from sqlalchemy import create_engine as _create_engine
    from sqlalchemy.orm import sessionmaker as _sessionmaker

    eng = _create_engine(postgres_url)
    sf = _sessionmaker(eng)
    try:
        with sf() as s:
            s.execute(insert(Team).values(id=other_team, name="other"))
            s.commit()
        eng.dispose()
        other_conn = _seed_byo_connection(postgres_url, other_team)
        with TestClient(app) as client:
            r = client.post(
                "/v1/chat/completions",
                headers={"Authorization": f"Bearer {raw_token}"},
                json={
                    "model": "openai/gpt-3.5-turbo",
                    "messages": [{"role": "user", "content": "hi"}],
                    "loom": {
                        "team_id": str(team_id),
                        "trial_id": str(uuid4()),
                        "step_id": "main",
                        "provider_connection_id": str(other_conn),
                    },
                },
            )
        assert r.status_code == 404
    finally:
        eng = _create_engine(postgres_url)
        sf = _sessionmaker(eng)
        with sf() as s:
            from sqlalchemy import delete

            s.execute(
                delete(ProviderConnection).where(
                    ProviderConnection.team_id == other_team,
                )
            )
            delete_team_and_quota(s, other_team)
            s.commit()
        eng.dispose()


def test_chat_records_family_evolver_dialect_from_loom_block(
    app,
    seed_data,
    seeded_trial_id,
    postgres_url,
):  # type: ignore[no-untyped-def]
    """#672 PR-3: when the client sets ``loom.dialect`` on a chat call,
    the resulting ``llm_calls`` row must carry that value instead of
    the default ``openai_chat`` so cost dashboards can partition
    family-run adapter spend from agent spend on the same trial.
    """
    team_id, raw_token = seed_data
    trial_id = seeded_trial_id
    with TestClient(app) as client:
        r = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "model": "anthropic/claude-opus-4-7",
                "messages": [{"role": "user", "content": "hi"}],
                "loom": {
                    "team_id": str(team_id),
                    "trial_id": str(trial_id),
                    "step_id": "main",
                    "dialect": "family_evolver",
                },
            },
        )
        assert r.status_code == 200, r.text

    from sqlalchemy import create_engine as _ce
    from sqlalchemy.orm import sessionmaker as _sm

    eng = _ce(postgres_url)
    sf = _sm(eng)
    try:
        with sf() as s:
            rows = s.execute(
                text("SELECT dialect FROM llm_calls WHERE trial_id = :tid"),
                {"tid": trial_id},
            ).all()
        assert len(rows) == 1
        assert rows[0].dialect == "family_evolver"
    finally:
        eng.dispose()
