"""POST /openai/v1/responses — probe-driven dispatch through the existing
`responses_chat_compat` translator when the connection's upstream is known
(or discovered) not to implement /v1/responses.

Covers three cases:

- `responses_api_supported = FALSE` on the connection → skip the native
  outbound call entirely, translate to `/v1/chat/completions`, return
  a Responses-shaped body.
- `responses_api_supported = NULL` (never probed) → probe with a sentinel
  model request, cache the result, dispatch based on the outcome.
- `responses_api_supported = TRUE` (cached fresh) → existing native
  pass-through path unchanged.

Reference: docs/architecture/responses-api.md
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import (
    LlmCall,
    ProviderConnection,
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
from loom_llm_gateway.responses_probe import ProbeBackoff
from tests.integration.gateway_db import delete_gateway_trial, insert_gateway_trial

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
async def route_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, dict[str, object]]]:
    """Yields (app, step_jwt, connection_id, captures).

    Captures record every outbound request the gateway makes to the mock
    upstream, split by path so tests can assert which endpoints were
    reached. `captures["route_response"]` is a dict keyed by path
    substring returning the httpx.Response for that path — tests mutate
    it to drive different scenarios."""
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
                display_name="yibuapi-like",
                base_url="https://upstream.example/v1",
                upstream_host="upstream.example",
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

    canned_chat_ok = httpx.Response(
        200,
        json={
            "id": "chatcmpl_test",
            "object": "chat.completion",
            "model": "glm-5.1-thinking",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {
                        "role": "assistant",
                        "content": "hello from chat completions",
                    },
                }
            ],
            "usage": {
                "prompt_tokens": 42,
                "completion_tokens": 13,
                "total_tokens": 55,
            },
        },
    )
    canned_responses_504 = httpx.Response(
        504,
        json={"detail": "upstream timeout on /v1/responses"},
    )
    captures: dict[str, object] = {
        "requests": [],
        "route_response": {
            "/responses": canned_responses_504,
            "/chat/completions": canned_chat_ok,
        },
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)  # type: ignore[union-attr]
        path = str(request.url.path)
        route_response = captures["route_response"]  # type: ignore[assignment]
        for suffix, resp in route_response.items():  # type: ignore[union-attr]
            if path.endswith(suffix):
                return resp
        return httpx.Response(500, json={"detail": f"no canned for {path}"})

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
        provider_connection_id=connection_id,
    )

    try:
        yield app, step_jwt, connection_id, captures  # type: ignore[misc]
    finally:
        await app.state.egress_client_pool.aclose()
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _post_responses(
    app: object,
    jwt: str,
    body: dict[str, object] | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    default_body: dict[str, object] = {
        "model": "glm-5.1-thinking",
        "input": [{"role": "user", "content": "hi"}],
        "instructions": "be helpful",
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        return await client.post(
            "/openai/v1/responses",
            headers={"Authorization": f"Bearer {jwt}"},
            json=body or default_body,
        )


async def _set_probe_state(
    postgres_url: str,
    connection_id: UUID,
    *,
    supported: bool | None,
    probed_at: datetime | None = None,
) -> None:
    engine = create_async_engine(postgres_url)
    async with async_sessionmaker(engine, expire_on_commit=False)() as ses:
        await ses.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == connection_id)
            .values(
                responses_api_supported=supported,
                responses_api_probed_at=probed_at,
            )
        )
        await ses.commit()
    await engine.dispose()


async def _read_probe_state(
    postgres_url: str,
    connection_id: UUID,
) -> tuple[bool | None, datetime | None]:
    engine = create_async_engine(postgres_url)
    async with async_sessionmaker(engine, expire_on_commit=False)() as ses:
        row = (
            await ses.execute(
                select(
                    ProviderConnection.responses_api_supported,
                    ProviderConnection.responses_api_probed_at,
                ).where(ProviderConnection.id == connection_id)
            )
        ).one()
    await engine.dispose()
    return row  # type: ignore[return-value]


async def _recorded_routes(postgres_url: str) -> list[str]:
    """Each recorded call's Gateway route (#2054): both share the Responses dialect."""
    engine = create_async_engine(postgres_url)
    async with async_sessionmaker(engine, expire_on_commit=False)() as ses:
        extras = (await ses.execute(select(LlmCall.provider_extras))).scalars().all()
    await engine.dispose()
    return [item["_loom_gateway_route"] for item in extras]


# ──────────────────────────────────────────────────────────────────────
# Cached FALSE → skip native, dispatch to translator
# ──────────────────────────────────────────────────────────────────────


async def test_cached_false_skips_native_and_translates(
    route_setup,
    postgres_url: str,
) -> None:
    app, jwt, conn_id, captures = route_setup
    await _set_probe_state(
        postgres_url,
        conn_id,
        supported=False,
        probed_at=datetime.now(UTC),
    )

    r = await _post_responses(app, jwt)

    assert r.status_code == 200, r.text
    body = r.json()
    # The response is Responses-shaped (has `output`), not Chat-shaped.
    assert "output" in body
    # The mock upstream ONLY got a /chat/completions request, never /responses.
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    paths = [str(req.url.path) for req in requests]
    assert not any(p.endswith("/responses") for p in paths), (
        f"native /responses call should have been skipped; got {paths}"
    )
    assert any(p.endswith("/chat/completions") for p in paths), (
        f"expected a /chat/completions call; got {paths}"
    )
    assert await _recorded_routes(postgres_url) == ["chat_translation"]


# ──────────────────────────────────────────────────────────────────────
# NULL → probe first, cache the outcome, dispatch based on it
# ──────────────────────────────────────────────────────────────────────


async def test_null_probes_then_dispatches_and_caches(
    route_setup,
    postgres_url: str,
) -> None:
    app, jwt, conn_id, captures = route_setup
    # Leave responses_api_supported as NULL (fixture default).
    captures["route_response"]["/responses"] = httpx.Response(404)  # type: ignore[index]

    r = await _post_responses(app, jwt)

    assert r.status_code == 200, r.text
    body = r.json()
    assert "output" in body

    # After the request settles, the DB should carry the probe finding.
    supported, probed_at = await _read_probe_state(postgres_url, conn_id)
    assert supported is False  # 404 on /responses → classified unsupported
    assert probed_at is not None


# ──────────────────────────────────────────────────────────────────────
# Cached TRUE → existing native pass-through, no translator invocation
# ──────────────────────────────────────────────────────────────────────


async def test_cached_true_uses_native_path_unchanged(
    route_setup,
    postgres_url: str,
) -> None:
    app, jwt, conn_id, captures = route_setup
    # Replace the canned /responses response with a real Responses body.
    canned_responses_ok = httpx.Response(
        200,
        json={
            "id": "resp_native",
            "object": "response",
            "model": "glm-5.1-thinking",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "hi from native"}],
                    "status": "completed",
                }
            ],
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "status": "completed",
        },
    )
    captures["route_response"]["/responses"] = canned_responses_ok  # type: ignore[index]

    await _set_probe_state(
        postgres_url,
        conn_id,
        supported=True,
        probed_at=datetime.now(UTC),
    )

    r = await _post_responses(app, jwt)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == "resp_native"
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    paths = [str(req.url.path) for req in requests]
    assert any(p.endswith("/responses") for p in paths)
    assert not any(p.endswith("/chat/completions") for p in paths), (
        f"native path should not have translated; got {paths}"
    )
    assert await _recorded_routes(postgres_url) == ["native_responses"]


# ──────────────────────────────────────────────────────────────────────
# Staleness — if probed_at is older than the TTL, re-probe
# ──────────────────────────────────────────────────────────────────────


async def test_stale_true_is_reprobed(
    route_setup,
    postgres_url: str,
) -> None:
    """Even a cached TRUE gets re-checked once it ages past the TTL —
    upstream config drift should not silently reintroduce the 40-min
    hang the whole probe exists to prevent."""
    app, jwt, conn_id, captures = route_setup
    captures["route_response"]["/responses"] = httpx.Response(404)  # type: ignore[index]
    # Set cached TRUE with a 48h-old probed_at (assumed TTL is 24h).
    await _set_probe_state(
        postgres_url,
        conn_id,
        supported=True,
        probed_at=datetime.now(UTC) - timedelta(hours=48),
    )

    await _post_responses(app, jwt)

    # After the request, the cache should have been refreshed to FALSE
    # (/responses now returns 404) — proving we
    # re-probed rather than trusted the stale bool.
    supported, probed_at = await _read_probe_state(postgres_url, conn_id)
    assert supported is False
    assert probed_at is not None
    assert probed_at > datetime.now(UTC) - timedelta(minutes=1)


# ──────────────────────────────────────────────────────────────────────
# Transient upstream failure — translate now, cache nothing, retry soon
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "previous",
    [None, True],
    ids=["unprobed", "expired-positive"],
)
async def test_transient_probe_failure_is_not_cached_and_recovers(
    route_setup,
    postgres_url: str,
    previous: bool | None,
) -> None:
    app, jwt, conn_id, captures = route_setup
    probed_at = datetime.now(UTC) - timedelta(hours=48) if previous else None
    await _set_probe_state(postgres_url, conn_id, supported=previous, probed_at=probed_at)
    now = [0.0]
    app.state.responses_probe_backoff = ProbeBackoff(clock=lambda: now[0])
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]

    def native_paths() -> list[str]:
        return [req.url.path for req in requests if req.url.path.endswith("/responses")]

    # The fixture's /responses returns 504: this request still gets an answer.
    assert (await _post_responses(app, jwt)).status_code == 200
    assert len(native_paths()) == 1
    assert await _read_probe_state(postgres_url, conn_id) == (previous, probed_at)

    # Inside the backoff nothing probes, and the request still translates.
    assert (await _post_responses(app, jwt)).status_code == 200
    assert len(native_paths()) == 1

    # Upstream recovers: the next probe after the backoff restores native.
    captures["route_response"]["/responses"] = httpx.Response(  # type: ignore[index]
        200,
        json={
            "id": "resp_native",
            "object": "response",
            "model": "glm-5.1-thinking",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "status": "completed",
        },
    )
    now[0] += 61
    assert (await _post_responses(app, jwt)).json()["id"] == "resp_native"
    assert len(native_paths()) == 3  # the probe, then the native call
    supported, recovered_at = await _read_probe_state(postgres_url, conn_id)
    assert supported is True
    assert recovered_at is not None
    assert recovered_at > datetime.now(UTC) - timedelta(minutes=1)
    assert await _recorded_routes(postgres_url) == [
        "chat_translation",
        "chat_translation",
        "native_responses",
    ]
