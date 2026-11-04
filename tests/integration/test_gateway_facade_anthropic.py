"""POST /anthropic/v1/messages — provider-connection facade.

Mirrors the structure of test_gateway_facade_openai.py: httpx
MockTransport intercepts the outbound POST to the connection's
base_url; the route looks up + decrypts the api_key via SecretStore
and forwards. Assertions cover the Anthropic-specific contract:
x-api-key header, anthropic-version header, cache_* token extras
preserved into llm_calls.provider_extras.
"""

from __future__ import annotations

import base64
import hashlib
import json
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
    RateCard,
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom.security.secret_store import LocalEncryptedSecretStore
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
from tests.unit.test_anthropic_chat_stream import _wire
from tests.unit.test_claude_gateway_lifecycle import _TerminalThenTimeout, _native_wire

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
async def facade_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, object]]]:
    """Yields (app, step_jwt, team_id, trial_id, connection_id, captures).

    Seeds an anthropic-typed connection with operator-supplied pricing
    ($3/1M in, $15/1M out) so cost assertions are deterministic.
    """
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

    async_session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        ref = await store.put(
            namespace=f"team:{team_id}",
            value="sk-ant-XYZ",
        )
        await ses.commit()

    connection_id = uuid4()
    with session_local() as s:
        s.execute(
            insert(ProviderConnection).values(
                id=connection_id,
                team_id=team_id,
                provider_type="anthropic",
                display_name="anthropic-prod",
                base_url="https://api.anthropic.com",
                upstream_host="api.anthropic.com",
                resolved_egress_ips=["104.18.0.2"],
                encrypted_api_key_ref=ref,
                pricing_source="operator-supplied",
                pricing_data={
                    "input_usd_per_1m": 3.0,
                    "output_usd_per_1m": 15.0,
                },
                created_by="admin:fixture",
            )
        )
        s.commit()

    # Canned upstream Anthropic-shape body — includes cache token
    # extras so we can assert they round-trip into llm_calls.provider_extras.
    canned_default = httpx.Response(
        200,
        json={
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-4-7",
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 20,
                "cache_read_input_tokens": 30,
            },
        },
    )
    captures: dict[str, object] = {
        "requests": [],
        "response": canned_default,
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
            delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            s.execute(delete(RateCard))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            delete_teams_and_quotas(s, (team_id,))
            s.commit()
        sync_engine.dispose()


def _messages_body() -> dict[str, object]:
    return {
        "model": "claude-opus-4-7",
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": "hi"}],
    }


async def _post_with_headers(
    app: object,
    headers: dict[str, str],
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        return await client.post(
            "/anthropic/v1/messages",
            headers=headers,
            json=_messages_body(),
        )


async def _post(app: object, jwt: str, **headers: str) -> httpx.Response:
    return await _post_with_headers(
        app,
        {
            "Authorization": f"Bearer {jwt}",
            **headers,
        },
    )


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


async def test_facade_forwards_with_xapikey_and_records_llm_call(
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
    # Pass-through preserved — cache fields visible.
    assert body["id"] == "msg_test"
    assert body["usage"]["cache_creation_input_tokens"] == 20

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    up = requests[0]
    assert up.method == "POST"
    assert str(up.url) == "https://api.anthropic.com/v1/messages"
    # Anthropic auth shape: x-api-key + anthropic-version. NOT Bearer.
    assert up.headers["x-api-key"] == "sk-ant-XYZ"
    assert up.headers["anthropic-version"] == "2023-06-01"
    assert "Authorization" not in up.headers or (
        not up.headers["Authorization"].startswith("Bearer sk-")
    )

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "step-1"
    assert row["dialect"] == "anthropic_facade"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50
    # cache extras round-trip into provider_extras
    extras = row["provider_extras"]
    assert extras["cache_creation_input_tokens"] == 20
    assert extras["cache_read_input_tokens"] == 30
    # operator-supplied cost: 100/1M * $3 + 50/1M * $15
    #                       = 0.0003 + 0.00075 = 0.00105
    assert float(row["cost_usd"]) == pytest.approx(0.00105, abs=1e-7)
    assert "operator-supplied" in row["rate_card_hash"]


async def test_facade_accepts_step_jwt_from_anthropic_x_api_key_header(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, team_id, trial_id, conn_id, captures = facade_setup
    r = await _post_with_headers(
        app,
        {
            "x-api-key": jwt,
            "x-loom-provider-connection-id": str(conn_id),
        },
    )
    assert r.status_code == 200, r.text

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    up = requests[0]
    assert up.headers["x-api-key"] == "sk-ant-XYZ"
    assert up.headers["x-api-key"] != jwt

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["dialect"] == "anthropic_facade"


async def test_facade_rate_card_pricing_includes_cache_tokens(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections "
                "SET pricing_source='rate-card', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(
            insert(RateCard).values(
                id="card-anthropic",
                captured_at=datetime.now(UTC),
                table={
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
    # 100 input, 50 output, 30 cache-read, 20 cache-write tokens.
    assert float(row["cost_usd"]) == pytest.approx(0.001134, abs=1e-8)
    assert len(row["rate_card_hash"]) == 64


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
                "SET pricing_source='rate-card', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(
            insert(RateCard).values(
                id="card-anthropic-missing",
                captured_at=datetime.now(UTC),
                table={
                    "entries": [
                        {
                            "provider": "anthropic",
                            "model": "not-claude-opus-4-7",
                            "input_per_mtok": 3.0,
                            "output_per_mtok": 15.0,
                            "cache_read_per_mtok": 0.3,
                            "cache_write_per_mtok": 3.75,
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


# ──────────────────────────────────────────────────────────────────────
# Auth + header validation (same shape as openai facade tests)
# ──────────────────────────────────────────────────────────────────────


async def test_facade_rejects_non_step_token(facade_setup) -> None:
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
    r = await _post(app, jwt)
    assert r.status_code == 400
    assert "x-loom-provider-connection-id" in r.json()["detail"]


async def test_facade_streams_sse_and_records_final_usage(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        200,
        headers={"content-type": "text/event-stream"},
        content=(
            "event: message_start\n"
            'data: {"type":"message_start","message":{"id":"msg_stream",'
            '"type":"message","role":"assistant",'
            '"model":"claude-opus-4-7","content":[],'
            '"usage":{"input_tokens":100,"output_tokens":1,'
            '"cache_creation_input_tokens":20,'
            '"cache_read_input_tokens":30}}}\n\n'
            "event: content_block_start\n"
            'data: {"type":"content_block_start","index":0,'
            '"content_block":{"type":"text","text":""}}\n\n'
            "event: content_block_delta\n"
            'data: {"type":"content_block_delta","index":0,'
            '"delta":{"type":"text_delta","text":"hello"}}\n\n'
            "event: message_delta\n"
            'data: {"type":"message_delta",'
            '"delta":{"stop_reason":"end_turn","stop_sequence":null},'
            '"usage":{"output_tokens":50}}\n\n'
            "event: message_stop\n"
            'data: {"type":"message_stop"}\n\n'
        ),
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/anthropic/v1/messages",
            headers={
                "Authorization": f"Bearer {jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "claude-opus-4-7",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )
    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["content-type"]
    assert "event: message_start" in r.text
    assert "event: message_stop" in r.text

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    upstream_payload = json.loads(requests[0].content)
    assert upstream_payload["stream"] is True
    assert requests[0].headers["x-api-key"] == "sk-ant-XYZ"

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "anthropic_facade"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50
    extras = row["provider_extras"]
    assert extras["cache_creation_input_tokens"] == 20
    assert extras["cache_read_input_tokens"] == 30
    assert float(row["cost_usd"]) == pytest.approx(0.00105, abs=1e-7)


# ──────────────────────────────────────────────────────────────────────
# Connection resolution
# ──────────────────────────────────────────────────────────────────────


async def test_facade_returns_404_for_cross_team_connection(
    facade_setup,
    postgres_url: str,
) -> None:
    """The probe must be skipped on cross-team — assert via captures."""
    app, _jwt, _team_a, _trial_a, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    other_team = uuid4()
    other_jwt = mint_step_jwt(
        team_id=other_team,
        trial_id=uuid4(),
        step_id="s",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )
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
    # Upstream never called — cross-team blocked before decrypt.
    assert len(captures["requests"]) == 0  # type: ignore[arg-type]


async def test_facade_rejects_google_type_connection(
    facade_setup,
    postgres_url: str,
) -> None:
    """Connection of type google shouldn't route through
    the anthropic facade — 400 with a hint."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections SET provider_type='google' WHERE id = :id",
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
    assert "google" in r.json()["detail"]


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


# ──────────────────────────────────────────────────────────────────────
# Upstream error mapping + redaction
# ──────────────────────────────────────────────────────────────────────


async def test_facade_surfaces_upstream_401_records_failed_audit_row(
    facade_setup,
    postgres_url: str,
) -> None:
    app, jwt, _team_id, trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        401,
        json={"error": {"type": "authentication_error", "message": "Invalid API key"}},
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
    assert row["dialect"] == "anthropic_facade"
    assert row["model"] == "claude-opus-4-7"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"].items() >= {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_4xx",
        "_loom_failure_status_code": 401,
        "_loom_usage_status": "missing",
    }.items()
    assert "messages" not in row["request_params"]


async def test_facade_redacts_api_key_from_upstream_error_body(
    facade_setup,
) -> None:
    """Anthropic's 4xx debug bodies occasionally echo the request
    headers. Redact `sk-ant-XYZ` before surfacing."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        401,
        text=("Auth failed. Token sent: x-api-key=sk-ant-XYZ. Check anthropic-version header."),
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    detail = r.json()["detail"]
    assert "sk-ant-XYZ" not in detail
    assert "[REDACTED]" in detail


async def test_facade_returns_502_on_missing_usage_block(
    facade_setup,
    postgres_url: str,
) -> None:
    """Anthropic 200s always carry a usage block; absence is a contract
    violation. Matches the existing /v1/messages route."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(
        200,
        json={  # type: ignore[index]
            "id": "msg_no_usage",
            "content": [{"type": "text", "text": "x"}],
            # no `usage` key
        },
    )
    r = await _post(
        app,
        jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 502
    assert "usage block" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    assert rows[0]._mapping["provider_extras"]["_loom_usage_status"] == "missing"
    assert rows[0]._mapping["provider_extras"]["_loom_call_status"] == "failed"


async def test_facade_returns_504_on_upstream_timeout(facade_setup) -> None:
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


@pytest.mark.parametrize("suffix", ["/v1", "/proxy", "/proxy/v1/"])
async def test_native_facade_normalizes_version_once(facade_setup, postgres_url, suffix):
    app, jwt, _, _, conn_id, captures = facade_setup
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(text("UPDATE provider_connections SET base_url=:base WHERE id=:id"),
                     {"base": "https://api.anthropic.com" + suffix, "id": conn_id})
    engine.dispose()
    response = await _post(app, jwt, **{"x-loom-provider-connection-id": str(conn_id)})
    assert response.status_code == 200
    expected = "/proxy/v1/messages" if "proxy" in suffix else "/v1/messages"
    assert captures["requests"][0].url.path == expected


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("provider_type", ["openai-compatible", "custom"])
async def test_chat_connection_serves_anthropic_tools_with_authoritative_accounting(
    facade_setup, postgres_url, stream, provider_type,
):
    app, jwt, _, trial_id, conn_id, captures = facade_setup
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(text("UPDATE provider_connections SET provider_type=:kind, base_url=:base WHERE id=:id"),
                     {"kind": provider_type, "base": "https://api.example/v1", "id": conn_id})
    tool = {"id": "write_1", "type": "function", "function": {"name": "Write", "arguments": '{"path":"proof.txt","text":"ok"}'}}
    usage = {"prompt_tokens": 100, "completion_tokens": 50, "prompt_tokens_details": {"cached_tokens": 30},
             "completion_tokens_details": {"reasoning_tokens": 7}}
    if stream:
        events = [
            {"id": "chat_a", "model": "upstream-alias", "choices": [{"index": 0, "delta": {
                "role": "assistant", "tool_calls": [{"index": 0, **tool}]}, "finish_reason": None}]},
            {"id": "chat_a", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
            {"id": "chat_a", "choices": [], "usage": usage},
        ]
        wire = "".join("data: " + json.dumps(item) + "\n\n" for item in events) + "data: [DONE]\n\n"
        captures["response"] = httpx.Response(200, content=wire, headers={"content-type": "text/event-stream"})
    else:
        captures["response"] = httpx.Response(200, json={"id": "chat_a", "model": "upstream-alias", "choices": [
            {"index": 0, "message": {"role": "assistant", "content": None, "tool_calls": [tool]}, "finish_reason": "tool_calls"}],
            "usage": usage})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        response = await client.post("/anthropic/v1/messages", headers={
            "x-api-key": jwt, "x-loom-provider-connection-id": str(conn_id), "anthropic-beta": "native-feature",
        }, json={**_messages_body(), "stream": stream, "thinking": {"type": "disabled"}, "output_config": {"effort": "high"}})
    assert response.status_code == 200, response.text
    assert "write_1" in response.text
    if stream:
        assert "event: message_stop" in response.text
    else:
        assert response.json()["content"][0]["input"] == {"path": "proof.txt", "text": "ok"}
        assert response.json()["usage"] == {"input_tokens": 70, "output_tokens": 50, "cache_read_input_tokens": 30}
    up = captures["requests"][0]
    assert up.url.path == "/v1/chat/completions"
    assert up.headers["authorization"] == "Bearer sk-ant-XYZ"
    assert "x-api-key" not in up.headers and "anthropic-beta" not in up.headers
    assert "thinking" not in json.loads(up.content)
    with engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls WHERE trial_id=:id"), {"id": trial_id}))
    engine.dispose()
    assert len(rows) == 1
    ledger = dict(rows[0]._mapping)
    assert ledger["input_tokens"] == 100 and ledger["output_tokens"] == 50
    assert ledger["response_model"] == "upstream-alias"
    assert float(ledger["cost_usd"]) == pytest.approx(0.00105)
    assert ledger["provider_extras"]["reasoning_tokens"] == 7
    assert ledger["provider_extras"]["_loom_gateway_route"] == "anthropic_to_chat"
    assert ledger["provider_extras"]["_loom_model_identity"] == {"provider": "openai", "name": "claude-opus-4-7"}
    assert "sk-ant-XYZ" not in json.dumps(ledger, default=str)


async def test_incomplete_native_stream_retains_observed_usage_as_failed(facade_setup, postgres_url):
    app, jwt, _, trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(200, headers={"content-type": "text/event-stream"}, content=(
        'event: message_start\ndata: {"type":"message_start","message":{"model":"alias","usage":{"input_tokens":12,"output_tokens":1}}}\n\n'
    ))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        response = await client.post("/anthropic/v1/messages", headers={
            "x-api-key": jwt, "x-loom-provider-connection-id": str(conn_id),
        }, json={**_messages_body(), "stream": True})
    assert "event: error" in response.text and "event: message_stop" not in response.text
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls WHERE trial_id=:id"), {"id": trial_id}))
    engine.dispose()
    assert len(rows) == 1
    ledger = dict(rows[0]._mapping)
    assert ledger["input_tokens"] == 12
    assert ledger["provider_extras"]["_loom_call_status"] == "failed"
    assert ledger["provider_extras"]["_loom_usage_status"] == "partial"


@pytest.mark.parametrize("native,input_tokens,output_tokens,cost", [(True, 12, 4, 0.000096), (False, 100, 20, 0.0006)])
async def test_terminal_event_finishes_facade_without_waiting_for_http_eof(
    facade_setup, postgres_url, native, input_tokens, output_tokens, cost,
):
    app, jwt, _, trial_id, conn_id, captures = facade_setup
    engine = create_engine(postgres_url)
    try:
        if not native:
            with engine.begin() as conn:
                conn.execute(text("UPDATE provider_connections SET provider_type='openai-compatible' WHERE id=:id"), {"id": conn_id})
        stream = _TerminalThenTimeout(_native_wire() if native else _wire())
        captures["response"] = httpx.Response(200, stream=stream, headers={"content-type": "text/event-stream"})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
            response = await client.post("/anthropic/v1/messages", headers={
                "x-api-key": jwt, "x-loom-provider-connection-id": str(conn_id),
            }, json={**_messages_body(), "stream": True})
        assert response.status_code == 200
        assert response.text.count("event: message_stop") == 1
        assert "event: error" not in response.text
        with engine.connect() as conn:
            rows = list(conn.execute(text("SELECT * FROM llm_calls WHERE trial_id=:id"), {"id": trial_id}))
        assert stream.closed and len(rows) == 1
        ledger = dict(rows[0]._mapping)
        assert ledger["input_tokens"] == input_tokens and ledger["output_tokens"] == output_tokens
        assert float(ledger["cost_usd"]) == pytest.approx(cost)
        assert "_loom_call_status" not in ledger["provider_extras"]
        assert "_loom_usage_status" not in ledger["provider_extras"]
    finally:
        engine.dispose()


async def test_hosted_model_authorization_works_with_one_database_connection(
    facade_setup, postgres_url, monkeypatch,
):
    """Authorization must finish before dispatch without a nested pool checkout."""
    from dataclasses import replace

    from sqlalchemy import update

    from loom.db.schema import Trial
    from loom_llm_gateway.routes import facade_anthropic

    app, jwt, _, trial_id, conn_id, _ = facade_setup
    original_factory = app.state.session_factory
    async with original_factory() as session:
        await session.execute(update(Trial).where(Trial.id == trial_id).values(config={
            "agent_name": "claude-code", "claude_code_mode": "native",
            "agent_model": {"provider": "vendor", "name": "claude-opus-4-7"},
        }))
        await session.commit()

    engine = create_async_engine(postgres_url, pool_size=1, max_overflow=0, pool_timeout=.2)
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    original_auth = facade_anthropic.verify_facade_auth

    async def hosted_auth(*args, **kwargs):
        ctx = await original_auth(*args, **kwargs)
        # Exercise hosted model authorization; lease fencing has its own tests.
        return replace(ctx, service_execution_lease_id=uuid4())

    async def dispatch(owner):
        assert engine.pool.checkedout() == 0
        assert owner.identity.provider == "vendor"
        return {"model": owner.identity.name}

    monkeypatch.setattr(facade_anthropic, "verify_facade_auth", hosted_auth)
    monkeypatch.setattr(facade_anthropic.MessagesDispatch, "complete", dispatch)
    try:
        response = await _post(app, jwt, **{"x-loom-provider-connection-id": str(conn_id)})
        assert response.status_code == 200, response.text
        assert response.json()["model"] == "claude-opus-4-7"
    finally:
        app.state.session_factory = original_factory
        await engine.dispose()
