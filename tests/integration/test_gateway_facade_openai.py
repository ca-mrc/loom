"""POST /openai/v1/chat/completions — provider-connection facade.

httpx.MockTransport intercepts the outbound POST to the connection's
base_url; the route looks up + decrypts the api_key via SecretStore
and forwards. On success we assert the upstream got the right
Authorization header, the response body passes through verbatim,
and an `llm_calls` row lands with the JWT's trial_id/step_id +
operator-supplied cost.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import (
    LlmCall,
    ProviderConnection,
    ProviderConnectionShare,
    RateCard,
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway import dispatch_audit
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.gateway_db import (
    delete_gateway_trial,
    delete_team_and_quota,
    delete_teams_and_quotas,
    insert_gateway_trial,
)

# Same deterministic test key the provider_connections route tests
# use so the SecretStore decrypts the seeded ciphertext.
_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
async def facade_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, list[httpx.Request]]]]:
    """Yields (app, step_jwt, team_id, trial_id, connection_id, captures).

    The fixture seeds:
    - team + token + team_quota
    - a provider connection with type='openai-compatible',
      operator-supplied pricing ($5/1M in, $15/1M out), and a real
      SecretStore-encrypted api_key
    - the gateway app with a MockTransport upstream that records every
      outbound request and returns a canned OpenAI shape

    Tests can override `captures["response"]` to inject different
    canned bodies / statuses.
    """
    # Keep real audit persistence without turning route/ledger assertions into
    # a one-second hosted-runner latency test. Admission timeout and fail-closed
    # behavior are tested explicitly in test_gateway_dispatch_audit.
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 10.0)
    for k, v in {
        "LOOM_GW_DB_URL": postgres_url,
        "LOOM_SECRET_STORE_MASTER_KEY": _TEST_MASTER_KEY,
    }.items():
        monkeypatch.setenv(k, v)
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)

    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    # Seed team + token + provider connection.
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    team_id = uuid4()
    raw_team_token = f"loom_team_{uuid4().hex}"
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_team_token.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()

    # SecretStore put runs against the async engine so the same row
    # is visible to the route.
    async_session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        ref = await store.put(
            namespace=f"team:{team_id}",
            value="sk-upstream-XYZ",
        )
        await ses.commit()

    connection_id = uuid4()
    with session_local() as s:
        s.execute(
            insert(ProviderConnection).values(
                id=connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="openai-prod",
                base_url="https://api.openai.com/v1",
                upstream_host="api.openai.com",
                resolved_egress_ips=["104.18.0.1"],
                encrypted_api_key_ref=ref,
                pricing_source="operator-supplied",
                pricing_data={
                    "input_usd_per_1m": 5.0,
                    "output_usd_per_1m": 15.0,
                },
                created_by="admin:fixture",
            )
        )
        s.commit()

    # Upstream MockTransport with a settable canned response. Tests
    # mutate `captures["response"]` to change the canned body/status.
    canned_default = httpx.Response(
        200,
        json={
            "id": "chatcmpl_test",
            "object": "chat.completion",
            "model": "gpt-4o",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": "hello"},
                }
            ],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
            },
        },
    )
    captures: dict[str, object] = {
        "requests": [],
        "response": canned_default,
        "gateway_team_ids": [team_id],
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)  # type: ignore[union-attr]
        return captures["response"]  # type: ignore[return-value]

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url="",
        upstream_timeout_sec=settings.upstream_timeout_sec,
    )

    trial_id = uuid4()
    with session_local() as s:
        task_id = insert_gateway_trial(s, team_id=team_id, trial_id=trial_id)
        s.commit()
    captures["gateway_trials"] = [(trial_id, task_id)]
    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id, connection_id, captures  # type: ignore[misc]
    finally:
        await app.state.egress_client_pool.aclose()
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            for cleanup_trial_id, cleanup_task_id in captures["gateway_trials"]:  # type: ignore[union-attr]
                delete_gateway_trial(
                    s,
                    trial_id=cleanup_trial_id,
                    task_id=cleanup_task_id,
                )
            s.execute(delete(RateCard))
            s.execute(delete(ProviderConnectionShare))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            delete_teams_and_quotas(s, captures["gateway_team_ids"])  # type: ignore[arg-type]
            s.commit()
        sync_engine.dispose()


async def _post(
    app: object,
    jwt: str,
    *,
    body: dict[str, object] | None = None,
    **headers: str,
) -> httpx.Response:
    """Single-shot POST against the facade. Defaults match a sensible
    happy path so tests only need to override what they care about."""
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    request_body = body or {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "hi"}],
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        return await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                **headers,
            },
            json=request_body,
        )


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


async def test_facade_forwards_with_decrypted_key_and_records_llm_call(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, team_id, trial_id, conn_id, captures = facade_setup
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Pass-through preserved.
    assert body["id"] == "chatcmpl_test"
    assert body["choices"][0]["message"]["content"] == "hello"

    # Upstream got the decrypted api_key in Bearer auth, NOT the
    # opaque ref or the user's loom_step_<jwt>.
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    up = requests[0]
    assert up.method == "POST"
    assert str(up.url) == "https://api.openai.com/v1/chat/completions"
    assert up.headers["Authorization"] == "Bearer sk-upstream-XYZ"
    # The user's body is forwarded verbatim — same model + messages.
    import json

    sent = json.loads(up.content)
    assert sent["model"] == "gpt-4o"
    assert sent["messages"] == [{"role": "user", "content": "hi"}]

    # llm_calls row written from the JWT's scope.
    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "step-1"
    assert row["dialect"] == "openai_facade"
    assert row["model"] == "gpt-4o"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50
    assert row["request_params"] == {
        "status": "available",
        "parameters": {},
    }
    # operator-supplied cost: 100/1M * $5 + 50/1M * $15
    #                       = 0.0005 + 0.00075 = 0.00125
    assert float(row["cost_usd"]) == pytest.approx(0.00125, abs=1e-7)
    assert "operator-supplied" in row["rate_card_hash"]
    assert row["provider_extras"]["_loom_cost_source"] == "operator-supplied"
    assert row["provider_extras"]["_loom_cost_confidence"] == "configured"
    assert row["provider_extras"]["_loom_cost_currency"] == "USD"
    assert row["provider_extras"]["_loom_pricing_source"] == "operator-supplied"


async def test_facade_records_redacted_request_params(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "do not persist this"}],
        "temperature": 0,
        "top_p": 0.5,
        "seed": 4321,
        "max_tokens": 256,
        "extra_body": {
            "top_k": 40,
            "repetition_penalty": 1.05,
            "api_key": "sk-should-not-persist",
        },
    }

    r = await _post(
        app,
        jwt,
        body=body,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    import json

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    sent = json.loads(requests[0].content)
    assert sent["messages"] == body["messages"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["request_params"] == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "top_p": 0.5,
            "seed": 4321,
            "max_tokens": 256,
            "top_k": 40,
            "repetition_penalty": 1.05,
        },
    }
    rendered = json.dumps(row["request_params"])
    assert "messages" not in rendered
    assert "do not persist this" not in rendered
    assert "api_key" not in rendered
    assert "sk-should-not-persist" not in rendered


async def test_facade_records_redacted_raw_provider_log(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, trial_id, conn_id, captures = facade_setup
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "keep this raw prompt"}],
        "extra_body": {
            "api_key": "sk-body-secret",
            "metadata": {"safe": "value"},
        },
    }
    captures["response"] = httpx.Response(
        200,
        headers={"x-request-id": "req_123", "authorization": "Bearer echoed-secret"},
        json={
            "id": "chatcmpl_raw",
            "object": "chat.completion",
            "model": "gpt-4o",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {
                        "role": "assistant",
                        "content": "raw assistant answer",
                    },
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
        },
    )

    r = await _post(
        app,
        jwt,
        body=body,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    raw_log = row["provider_extras"]["_loom_raw_provider_log"]
    assert raw_log["schema_version"] == "1"
    assert raw_log["trial_id"] == str(trial_id)
    assert raw_log["ref"] == (f"llm_calls/{row['id']}/provider_extras/_loom_raw_provider_log")
    assert raw_log["request"]["method"] == "POST"
    assert raw_log["request"]["url"] == "https://api.openai.com/v1/chat/completions"
    assert raw_log["request"]["body"]["messages"] == [
        {"role": "user", "content": "keep this raw prompt"}
    ]
    assert raw_log["request"]["body"]["extra_body"]["api_key"] == "[REDACTED]"
    assert raw_log["request"]["headers"]["authorization"] == "Bearer [REDACTED]"
    assert raw_log["response"]["status_code"] == 200
    assert raw_log["response"]["body"]["choices"][0]["message"]["content"] == (
        "raw assistant answer"
    )
    assert raw_log["response"]["headers"]["authorization"] == "Bearer [REDACTED]"
    rendered = str(raw_log)
    assert "sk-upstream-XYZ" not in rendered
    assert "sk-body-secret" not in rendered
    assert "echoed-secret" not in rendered


async def test_facade_rate_card_pricing_uses_connection_provider(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections "
                "SET pricing_source='rate-card', "
                "rate_card_provider='together', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(
            insert(RateCard).values(
                id="card-together",
                captured_at=datetime.now(UTC),
                table={
                    "entries": [
                        {
                            "provider": "together",
                            "model": "gpt-4o",
                            "input_per_mtok": 2.0,
                            "output_per_mtok": 8.0,
                            "cache_read_per_mtok": 0.0,
                            "cache_write_per_mtok": 0.0,
                        }
                    ],
                },
            )
        )
    sync_engine.dispose()

    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    # rate-card cost: 100/1M * $2 + 50/1M * $8
    #                 = 0.0002 + 0.0004 = 0.0006
    assert float(row["cost_usd"]) == pytest.approx(0.0006, abs=1e-7)
    assert row["rate_card_hash"] != "facade:rate-card"
    assert len(row["rate_card_hash"]) == 64
    assert row["provider_extras"]["_loom_cost_source"] == "rate-card"
    assert row["provider_extras"]["_loom_cost_confidence"] == "configured"
    assert row["provider_extras"]["_loom_rate_card_provider"] == "together"
    assert row["provider_extras"]["_loom_pricing_source"] == "rate-card"


async def test_facade_rate_card_missing_entry_records_missing_marker(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections "
                "SET pricing_source='rate-card', "
                "rate_card_provider='together', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(
            insert(RateCard).values(
                id="card-no-match",
                captured_at=datetime.now(UTC),
                table={
                    "entries": [
                        {
                            "provider": "together",
                            "model": "not-gpt-4o",
                            "input_per_mtok": 2.0,
                            "output_per_mtok": 8.0,
                            "cache_read_per_mtok": 0.0,
                            "cache_write_per_mtok": 0.0,
                        }
                    ],
                },
            )
        )
    sync_engine.dispose()

    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "facade:rate-card:missing"
    assert row["provider_extras"]["_loom_cost_source"] == "unpriced"
    assert row["provider_extras"]["_loom_cost_confidence"] == "unavailable"
    assert row["provider_extras"]["_loom_unpriced_reason"] == "missing_rate_card_entry"
    assert row["provider_extras"]["_loom_rate_card_provider"] == "together"


# ──────────────────────────────────────────────────────────────────────
# Auth + header validation
# ──────────────────────────────────────────────────────────────────────


async def test_facade_rejects_non_step_token(facade_setup) -> None:
    """A team-token bearer (no trial_id in scope) must be rejected —
    cost attribution depends on the JWT's trial/step claims."""
    app, _jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    bogus = f"loom_team_{uuid4().hex}"
    r = await _post(
        app,
        bogus,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == 'Bearer error="invalid_token"'
    assert r.json()["detail"] == {"code": "invalid_or_expired_bearer"}


async def test_facade_rejects_missing_connection_id_header(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, _conn_id, _captures = facade_setup
    r = await _post(app, jwt)  # no x-loom-provider-connection-id
    assert r.status_code == 400
    assert "x-loom-provider-connection-id" in r.json()["detail"]


async def test_facade_rejects_malformed_connection_id_header(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, _conn_id, _captures = facade_setup
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": "not-a-uuid"},
    )
    assert r.status_code == 400
    assert "not a valid UUID" in r.json()["detail"]


async def test_facade_stream_true_returns_synthetic_sse_and_records_usage(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, team_id, trial_id, conn_id, captures = facade_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/event-stream")
    lines = [line for line in r.text.splitlines() if line.startswith("data: ")]
    assert lines[-1] == "data: [DONE]"
    chunks = [line.removeprefix("data: ") for line in lines if line != "data: [DONE]"]
    assert chunks

    import json

    first_chunk = json.loads(chunks[0])
    assert first_chunk["object"] == "chat.completion.chunk"
    assert first_chunk["choices"][0]["delta"] == {
        "role": "assistant",
        "content": "hello",
    }
    final_choice_chunk = json.loads(chunks[1])
    assert final_choice_chunk["choices"][0]["finish_reason"] == "stop"
    usage_chunk = json.loads(chunks[-1])
    assert usage_chunk["choices"] == []
    assert usage_chunk["usage"] == {
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150,
    }

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    sent = json.loads(requests[0].content)
    assert sent["stream"] is False

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "step-1"
    assert row["dialect"] == "openai_facade"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50


async def test_facade_rejects_missing_model_field(facade_setup) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={"messages": [{"role": "user", "content": "hi"}]},
        )
    assert r.status_code == 400
    assert "model" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Connection resolution
# ──────────────────────────────────────────────────────────────────────


async def test_facade_returns_404_for_cross_team_connection(
    facade_setup,
    postgres_url: str,
) -> None:
    """A JWT scoped to team_a can't use team_b's connection — 404
    (not 403) so existence isn't leaked across teams."""
    app, _jwt_a, _team_a, _trial_a, conn_id, _captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    other_team = uuid4()
    other_jwt = mint_step_jwt(
        team_id=other_team,
        trial_id=uuid4(),
        step_id="s",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )
    # Need a Team row for the other team to satisfy any FK paths
    # — but the JWT-scope auth doesn't actually require it.
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    try:
        with session_local() as s:
            s.execute(insert(Team).values(id=other_team, name=f"t-{other_team}"))
            s.commit()

        r = await _post(
            app,
            other_jwt,
            **{"x-loom-provider-connection-id": str(conn_id)},
        )
    finally:
        try:
            with session_local() as s:
                delete_team_and_quota(s, other_team)
                s.commit()
        finally:
            sync_engine.dispose()
    assert r.status_code == 404


async def test_facade_routes_shared_provider_for_target_team_and_records_usage_to_target(
    facade_setup,
    postgres_url: str,
) -> None:
    app, _jwt_owner, _owner_team_id, _owner_trial_id, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    target_team = uuid4()
    target_trial = uuid4()
    target_jwt = mint_step_jwt(
        team_id=target_team,
        trial_id=target_trial,
        step_id="shared-provider-step",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=conn_id,
    )

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(insert(Team).values(id=target_team, name=f"target-{target_team}"))
        s.execute(insert(TeamQuota).values(team_id=target_team))
        target_task_id = insert_gateway_trial(
            s,
            team_id=target_team,
            trial_id=target_trial,
        )
        s.execute(
            insert(ProviderConnectionShare).values(
                provider_connection_id=conn_id,
                target_team_id=target_team,
                created_by_actor="test:provider-share",
            )
        )
        s.commit()
    captures["gateway_trials"].append((target_trial, target_task_id))  # type: ignore[union-attr]
    captures["gateway_team_ids"].append(target_team)  # type: ignore[union-attr]
    sync_engine.dispose()

    r = await _post(app, target_jwt)
    assert r.status_code == 200, r.text
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == "Bearer sk-upstream-XYZ"

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == target_team
    assert row["trial_id"] == target_trial
    assert row["step_id"] == "shared-provider-step"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50


async def test_facade_returns_404_for_soft_deleted_connection(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections SET deleted_at = now() WHERE id = :id",
            ),
            {"id": conn_id},
        )
    sync_engine.dispose()

    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 404


async def test_facade_rejects_anthropic_type_connection(
    facade_setup,
    postgres_url: str,
) -> None:
    """Anthropic-typed connections don't speak the chat-completion
    shape — route 400s so operators get a clear hint."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections SET provider_type='anthropic' WHERE id = :id",
            ),
            {"id": conn_id},
        )
    sync_engine.dispose()

    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 400
    assert "anthropic" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Upstream error mapping
# ──────────────────────────────────────────────────────────────────────


async def test_facade_surfaces_upstream_401_records_failed_audit_row(
    facade_setup,
    postgres_url: str,
) -> None:
    """Upstream 401 returns the upstream excerpt and leaves durable
    failed-call audit evidence."""
    app, jwt, _team_id, trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        401,
        json={"error": {"message": "Invalid API key"}},
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    assert "Invalid API key" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["dialect"] == "openai_facade"
    assert row["model"] == "gpt-4o"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_4xx",
        "_loom_failure_status_code": 401,
        "_loom_usage_status": "missing",
    }
    assert "messages" not in row["request_params"]


@pytest.mark.parametrize(
    ("status", "message", "category"),
    [
        (403, "Permission denied for this model", "upstream_http_4xx"),
        (404, "The model `gpt-4o` does not exist", "upstream_http_4xx"),
        (429, "Rate limit reached for requests", "upstream_http_4xx"),
        (503, "Upstream overloaded", "upstream_http_5xx"),
    ],
)
async def test_facade_surfaces_upstream_errors_with_missing_usage(
    facade_setup,
    postgres_url: str,
    status: int,
    message: str,
    category: str,
) -> None:
    """#2054: permission, model-not-found, rate-limit and upstream failures
    keep their status, surface the upstream reason, and are audited as
    failed calls with missing usage rather than a verified zero cost."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        status,
        json={"error": {"message": message}},
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == status
    assert message in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT provider_extras FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    extras = rows[0][0]
    assert extras["_loom_call_status"] == "failed"
    assert extras["_loom_failure_category"] == category
    assert extras["_loom_failure_status_code"] == status
    assert extras["_loom_usage_status"] == "missing"


async def test_facade_redacts_api_key_from_upstream_error_body(
    facade_setup,
) -> None:
    """If the upstream echoes the api_key in a 4xx debug page, the
    facade MUST scrub it before surfacing — the sandbox caller is
    less trusted than the operator who supplied the key."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    # Fixture seeds api_key="sk-upstream-XYZ" — same string the
    # SecretStore round-trips. Have the upstream echo it.
    captures["response"] = httpx.Response(  # type: ignore[index]
        401,
        text=("Auth failed. Token sent: Bearer sk-upstream-XYZ. Check Authorization header."),
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    detail = r.json()["detail"]
    assert "sk-upstream-XYZ" not in detail
    assert "[REDACTED]" in detail


async def test_facade_handles_missing_upstream_usage_block(
    facade_setup,
    postgres_url: str,
) -> None:
    """An operator endpoint that returns 200 without `usage` should
    still produce an llm_calls row with 0/0 + cost 0 — best-effort
    attribution beats silent loss."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(
        200,
        json={  # type: ignore[index]
            "id": "no-usage",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"}}],
        },
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["provider_extras"]["_loom_usage_status"] == "missing"


async def test_facade_marks_partial_upstream_usage_block(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(
        200,
        json={  # type: ignore[index]
            "id": "partial-usage",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"}}],
            "usage": {"prompt_tokens": 123},
        },
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["input_tokens"] == 123
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == pytest.approx(0.000615)
    assert row["provider_extras"]["_loom_usage_status"] == "partial"
    assert row["provider_extras"]["_loom_missing_usage_keys"] == ["completion_tokens"]
    assert row["provider_extras"]["_loom_provider_usage"] == {"prompt_tokens": 123}


async def test_facade_returns_502_on_upstream_request_error(
    facade_setup,
) -> None:
    """Connect / DNS errors → 502 with the underlying error type
    in the detail."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup

    def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    await app.state.egress_client_pool.aclose()  # type: ignore[attr-defined]
    await app.state.upstream_client.aclose()  # type: ignore[attr-defined]
    app.state.upstream_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(_raise),
    )
    app.state.egress_client_pool = EgressClientPool(  # type: ignore[attr-defined]
        upstream_client=app.state.upstream_client,  # type: ignore[attr-defined]
        proxy_url="",
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,  # type: ignore[attr-defined]
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 502
    assert "ConnectError" in r.json()["detail"]


async def test_facade_returns_504_on_upstream_timeout(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup

    def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("read timeout")

    await app.state.egress_client_pool.aclose()  # type: ignore[attr-defined]
    await app.state.upstream_client.aclose()  # type: ignore[attr-defined]
    app.state.upstream_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(_raise),
    )
    app.state.egress_client_pool = EgressClientPool(  # type: ignore[attr-defined]
        upstream_client=app.state.upstream_client,  # type: ignore[attr-defined]
        proxy_url="",
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,  # type: ignore[attr-defined]
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 504
    assert "timeout" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Issue #72 — JWT scope as the source of connection_id
# ──────────────────────────────────────────────────────────────────────


async def test_facade_routes_using_jwt_scope_without_header(
    facade_setup,
) -> None:
    """Issue #72: a JWT minted with provider_connection_id in scope
    routes correctly even when the x-loom-provider-connection-id
    header is absent. Forward-compatible with the Phase 3 sandbox
    SDK path."""
    app, _jwt, team_id, trial_id, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    # Mint a JWT whose scope includes the connection_id.
    scoped_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=conn_id,
    )
    # Call WITHOUT the x-loom-provider-connection-id header.
    r = await _post(app, scoped_jwt)
    assert r.status_code == 200, r.text
    # Upstream got the decrypted key — proving the JWT-scoped
    # connection_id reached the resolution path.
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == "Bearer sk-upstream-XYZ"


async def test_facade_400s_when_jwt_and_header_mismatch(
    facade_setup,
) -> None:
    """Issue #72: JWT scope is authoritative. If a caller supplies
    both a scoped JWT and a header pointing at a different id, the
    facade 400s and points the operator at the canonical source."""
    app, _jwt, team_id, trial_id, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    scoped_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=conn_id,
    )
    bogus_header = str(uuid4())
    r = await _post(
        app,
        scoped_jwt,
        **{"x-loom-provider-connection-id": bogus_header},
    )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "JWT scope is authoritative" in detail
    assert str(conn_id) in detail
    assert bogus_header in detail
    # Upstream MUST NOT have been called.
    assert len(captures["requests"]) == 0  # type: ignore[arg-type]


async def test_facade_accepts_matching_jwt_scope_and_header(
    facade_setup,
) -> None:
    """During the transition, the sandbox SDK can safely send both —
    the route accepts when they agree."""
    app, _jwt, team_id, trial_id, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    scoped_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=conn_id,
    )
    r = await _post(
        app,
        scoped_jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200
    assert len(captures["requests"]) == 1  # type: ignore[arg-type]


async def test_model_custom_pricing_is_recorded_and_not_repriced(facade_setup) -> None:
    from sqlalchemy import select

    from loom_service.usage_accounting import price_snapshots_for_trials
    app, jwt, _, trial_id, conn_id, captures = facade_setup
    async with app.state.session_factory() as session:
        row = await session.get(ProviderConnection, conn_id)
        row.pricing_config = {"pricing_mode": "custom", "custom_pricing": {
            "gpt-4o": {"input_usd_per_1m": 5, "output_usd_per_1m": 15, "cache_read_usd_per_1m": 1},
        }}
        await session.commit()
    response = captures["response"].json()
    response["usage"]["prompt_tokens_details"] = {"cached_tokens": 30}
    captures["response"] = httpx.Response(200, json=response)
    result = await _post(app, jwt, **{"x-loom-provider-connection-id": str(conn_id)})
    assert result.status_code == 200, result.text
    async with app.state.session_factory() as session:
        call = await session.scalar(select(LlmCall).where(LlmCall.trial_id == trial_id))
        assert float(call.cost_usd) == pytest.approx(0.00113)
        assert call.provider_extras["_loom_price_basis"]["prices"]["input_usd_per_1m"] == 5
        old_hash = call.rate_card_hash
        row = await session.get(ProviderConnection, conn_id)
        row.pricing_config = {"pricing_mode": "usage_only"}
        await session.commit()
        snapshots = await price_snapshots_for_trials(session, [trial_id])
        assert snapshots[0]["rate_card_hash"] == old_hash
        assert snapshots[0]["resolved"] is True
        assert snapshots[0]["prices"]["input_usd_per_1m"] == 5
