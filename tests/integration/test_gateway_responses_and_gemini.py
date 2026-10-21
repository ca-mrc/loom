"""POST /v1/responses + POST /v1beta/models/...:generateContent
native passthrough (Plan 9 Tasks 8 + 9).

Shares the gateway_setup fixture pattern with test_gateway_messages.py;
the upstream MockTransport intercepts either OpenAI or Gemini depending
on the request URL host/path.
"""

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import LlmCall, ProviderConnection, RateCard, Secret, Team
from loom.pipeline.keys import canonical_digest, canonical_document
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

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()

_RATE_CARD_TABLE = {
    "id": "card-test",
    "entries": [
        {
            "provider": "openai",
            "model": "gpt-5",
            "input_per_mtok": 5.0,
            "output_per_mtok": 10.0,
            "cache_read_per_mtok": 0.5,
            "cache_write_per_mtok": 6.25,
        },
        {
            "provider": "google",
            "model": "gemini-2.0-flash",
            "input_per_mtok": 0.075,
            "output_per_mtok": 0.30,
            "cache_read_per_mtok": 0.019,
            "cache_write_per_mtok": 0.09,
        },
    ],
}


@pytest.fixture
async def gateway(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID]]:
    for k, v in {
        "LOOM_GW_DB_URL": postgres_url,
        "LOOM_GW_OPENAI_API_KEY": "test-openai-key",
        "LOOM_GW_GOOGLE_API_KEY": "test-google-key",
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

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.openai.com":
            assert request.headers["authorization"] == "Bearer test-openai-key"
            return httpx.Response(
                200,
                json={
                    "id": "resp_test",
                    "model": "gpt-5",
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "ok"},
                            ],
                        }
                    ],
                    "usage": {
                        "input_tokens": 200,
                        "output_tokens": 80,
                        "output_tokens_details": {"reasoning_tokens": 20},
                    },
                },
            )
        if request.url.host == "generativelanguage.googleapis.com":
            assert request.url.params["key"] == "test-google-key"
            return httpx.Response(
                200,
                json={
                    "candidates": [
                        {
                            "content": {
                                "parts": [{"text": "ok"}],
                                "role": "model",
                            }
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 150,
                        "candidatesTokenCount": 60,
                        "cachedContentTokenCount": 40,
                        "thoughtsTokenCount": 12,
                    },
                },
            )
        return httpx.Response(404)

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(
            insert(RateCard).values(
                id="card-test",
                captured_at=datetime.now(UTC),
                table=_RATE_CARD_TABLE,
            )
        )
        s.commit()

    team_id = uuid4()
    trial_id = uuid4()
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        task_id = insert_gateway_trial(s, team_id=team_id, trial_id=trial_id)
        s.commit()
    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="main",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id
    finally:
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            delete_team_and_quota(s, team_id)
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()


@pytest.fixture
async def gateway_with_provider_connection(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, list[httpx.Request]]]]:
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

    captures: dict[str, list[httpx.Request]] = {"requests": []}

    def _handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)
        assert request.url.host == "provider.example"
        assert request.headers["authorization"] == "Bearer sk-provider-XYZ"
        if captures.get("timeout"):
            raise httpx.ReadTimeout("secret-transport-canary", request=request)
        if isinstance(captures.get("response"), httpx.Response):
            return captures["response"]
        body = json.loads(request.content)
        if body.get("stream") is True:
            payload = {
                "type": "response.completed",
                "response": {
                    "id": "resp_stream",
                    "model": body["model"],
                    "usage": {
                        "input_tokens": 12,
                        "output_tokens": 4,
                        "output_tokens_details": {"reasoning_tokens": 2},
                    },
                },
            }
            return httpx.Response(
                200,
                content=f"data: {json.dumps(payload)}\n\n".encode(),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "id": "resp_provider",
                "model": body["model"],
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "ok"},
                        ],
                    }
                ],
                "usage": {
                    "input_tokens": 21,
                    "output_tokens": 7,
                    "output_tokens_details": {"reasoning_tokens": 3},
                },
            },
        )

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url=settings.egress_proxy_url,
        upstream_timeout_sec=settings.upstream_timeout_sec,
    )

    team_id = uuid4()
    trial_id = uuid4()
    connection_id = uuid4()

    async_session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        ref = await store.put(
            namespace=f"team:{team_id}",
            value="sk-provider-XYZ",
        )
        await ses.commit()

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        task_id = insert_gateway_trial(s, team_id=team_id, trial_id=trial_id)
        s.execute(
            insert(ProviderConnection).values(
                id=connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="provider",
                base_url="https://provider.example/v1",
                upstream_host="provider.example",
                resolved_egress_ips=["203.0.113.10"],
                encrypted_api_key_ref=ref,
                pricing_source="tokens-only",
                created_by="test",
                # #277 / responses-api-support-probe: these tests
                # exercise the NATIVE /v1/responses path and its 400-
                # signature fallback. Pre-seeding the probe-cached
                # bool to TRUE skips the inline probe (which would
                # otherwise send {} to the mock and get interpreted as
                # a rejected-body signal, dispatching straight to the
                # translator and short-circuiting the native path this
                # suite is here to test).
                responses_api_supported=True,
                responses_api_probed_at=datetime.now(UTC),
            )
        )
        s.commit()

    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="main",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=connection_id,
    )

    try:
        yield app, step_jwt, team_id, trial_id, connection_id, captures
    finally:
        await app.state.egress_client_pool.aclose()
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            delete_gateway_trial(s, trial_id=trial_id, task_id=task_id)
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            delete_teams_and_quotas(s, (team_id,))
            s.commit()
        sync_engine.dispose()


async def test_responses_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"model": "gpt-5", "input": "hi"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # Reasoning tokens detail preserved.
        assert body["usage"]["output_tokens_details"]["reasoning_tokens"] == 20

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["input_tokens"] == 200
    assert row["output_tokens"] == 80
    assert row["provider_extras"] == {
        "reasoning_tokens": 20,
        "_loom_cache_usage_known": False,
        "_loom_input_includes_cache": True,
        "_loom_unsupported_billing": False,
    }
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_responses_upstream_500_records_failed_audit_row(  # type: ignore[no-untyped-def]
    gateway,
    postgres_url,
):
    app, step_jwt, team_id, trial_id = gateway

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.openai.com"
        return httpx.Response(500, text="internal provider error")

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"model": "gpt-5", "input": "hi", "temperature": 0.2},
        )
    assert r.status_code == 500
    assert "internal provider error" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "gpt-5"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_5xx",
        "_loom_failure_status_code": 500,
        "_loom_usage_status": "missing",
    }
    assert row["request_params"] == {
        "status": "available",
        "parameters": {"temperature": 0.2},
    }


@pytest.mark.parametrize("path", ["/v1/responses", "/openai/v1/responses"])
async def test_responses_routes_provider_connection_from_step_jwt(
    gateway_with_provider_connection,
    postgres_url: str,
    path: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "instructions": "You are Codex.",
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": "hi"}],
            }
        ],
        "parallel_tool_calls": True,
        "reasoning": {"effort": "low"},
        "store": False,
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            path,
            headers={"Authorization": f"Bearer {step_jwt}"},
            json=payload,
        )
    assert r.status_code == 200, r.text
    assert r.json()["id"] == "resp_provider"

    requests = captures["requests"]
    assert len(requests) == 1
    assert str(requests[0].url) == "https://provider.example/v1/responses"
    assert json.loads(requests[0].content) == payload

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen2.5-coder-7b-instruct"
    assert row["input_tokens"] == 21
    assert row["output_tokens"] == 7
    assert row["provider_extras"]["reasoning_tokens"] == 3
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_execution_attempt_responses_uses_exactly_once_provider_ledger(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, _trial_jwt, team_id, _trial_id, connection_id, captures = gateway_with_provider_connection
    run_id, stage_id, attempt_id, worker_id = uuid4(), uuid4(), uuid4(), uuid4()
    token_id, binding_id, source_id = uuid4(), uuid4(), uuid4()
    binding_digest = "sha256:" + "b" * 64
    authorization_digest = "sha256:" + "c" * 64
    execution_spec = {
        "container_node": {"network_profile": "gateway"},
        "control_binding_snapshots": [{"snapshot_sha256": binding_digest}],
    }
    execution_spec_digest = canonical_digest(execution_spec)
    authorization = {"schema_version": "loom.terminalgen-authoring-grant.v1"}
    binding_snapshot = {
        "schema_version": "loom.recipe-provider-binding.v2",
        "provider_connection_id": str(connection_id),
        "provider": "openai",
        "model": "gpt-5",
        "wire_api": "responses",
    }
    now = datetime.now(UTC)
    rate_card_id = f"card-terminalgen-{run_id}"
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    UPDATE provider_connections
                    SET status='valid',pricing_source='rate-card',rate_card_provider='openai'
                    WHERE id=:id
                """),
                {"id": connection_id},
            )
            connection.execute(
                insert(RateCard).values(
                    id=rate_card_id,
                    captured_at=now,
                    table={
                        "entries": [
                            {
                                "provider": "openai",
                                "model": "gpt-5",
                                "input_per_mtok": 5.0,
                                "output_per_mtok": 10.0,
                                "cache_read_per_mtok": 0.5,
                                "cache_write_per_mtok": 6.25,
                            }
                        ]
                    },
                )
            )
            connection.execute(
                text("""
                    INSERT INTO workers (
                        id,hostname,version,capabilities,registered_at,last_seen_at,status
                    ) VALUES (:id,:hostname,'test','[]'::jsonb,:now,:now,'active')
                """),
                {"id": worker_id, "hostname": f"provider-ledger-{worker_id}", "now": now},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                        graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                        resolved_inputs_json,budget_json,request_digest,idempotency_key,state
                    ) VALUES (
                        :id,:team,'ordinary','terminalgen-authoring',1,:digest,
                        '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                        :digest,:key,'running'
                    )
                """),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": "sha256:" + "a" * 64,
                    "key": f"provider-ledger-{run_id}",
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id,pipeline_run_id,node_key,shard_key,node_kind,state,
                        resolved_execution_spec_json,resolved_execution_spec_bytes,
                        execution_spec_digest,resource_profile_json,resource_profile_digest,
                        provider_connection_ref,resolved_input_bindings_json,
                        resolved_input_bindings_digest,failure_policy,attempt_count,
                        claimed_at,started_at
                    ) VALUES (
                        :id,:run,'generate_card_00','singleton','container','running',
                        CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'{}'::jsonb,:digest,
                        :connection,'[]'::jsonb,:inputs_digest,'fail_run',1,:now,:now
                    )
                """),
                {
                    "id": stage_id,
                    "run": run_id,
                    "spec": json.dumps(execution_spec),
                    "spec_bytes": canonical_document(execution_spec),
                    "spec_digest": execution_spec_digest,
                    "digest": "sha256:" + "d" * 64,
                    "connection": connection_id,
                    "inputs_digest": canonical_digest([]),
                    "now": now,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id,stage_run_id,attempt_number,state,worker_id,claim_id,lease_epoch,
                        lease_token_digest,lease_expires_at,step_jwt_id,
                        execution_authorization_json,execution_authorization_bytes,
                        execution_authorization_digest,queued_at,claimed_at,started_at
                    ) VALUES (
                        :id,:stage,1,'running',:worker,:claim,1,:lease_digest,:expires,:jti,
                        CAST(:authorization AS jsonb),:authorization_bytes,
                        :authorization_digest,:now,:now,:now
                    )
                """),
                {
                    "id": attempt_id,
                    "stage": stage_id,
                    "worker": worker_id,
                    "claim": uuid4(),
                    "lease_digest": "sha256:" + "e" * 64,
                    "expires": now + timedelta(minutes=5),
                    "jti": token_id,
                    "authorization": json.dumps(authorization),
                    "authorization_bytes": canonical_document(authorization),
                    "authorization_digest": authorization_digest,
                    "now": now,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_run_control_bindings (
                        id,pipeline_run_id,logical_name,kind,node_key,source_object_id,
                        source_version,snapshot_json,snapshot_bytes,snapshot_sha256,
                        provider_connection_id,provider_request_limit,
                        provider_cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (
                        :id,:run,'terminalgen_generate_card_00','provider',
                        'generate_card_00',:source,1,CAST(:snapshot AS jsonb),
                        :snapshot_bytes,:digest,:connection,2,10000,30
                    )
                """),
                {
                    "id": binding_id,
                    "run": run_id,
                    "source": source_id,
                    "snapshot": json.dumps(binding_snapshot),
                    "snapshot_bytes": canonical_document(binding_snapshot),
                    "digest": binding_digest,
                    "connection": connection_id,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id,provider_limit_microusd,gpu_limit_seconds,
                        artifact_limit_bytes,stage_run_limit,attempt_limit,wall_deadline_at
                    ) VALUES (:run,10000,0,0,1,1,:deadline)
                """),
                {"run": run_id, "deadline": now + timedelta(hours=1)},
            )
            connection.execute(
                text("""
                    INSERT INTO execution_attempt_provider_budgets (
                        attempt_id,binding_snapshot_sha256,request_limit,
                        cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (:attempt,:binding,2,10000,30)
                """),
                {"attempt": attempt_id, "binding": binding_digest},
            )

        step_jwt = mint_step_jwt(
            team_id=team_id,
            execution_attempt_id=attempt_id,
            step_id="generate_card_00",
            ttl_sec=300,
            signing_key=app.state.settings.step_jwt_signing_key.get_secret_value(),
            provider_connection_id=connection_id,
            provider_connection_id_bound=True,
            step_jwt_id=token_id,
            execution_attempt_lease_epoch=1,
            execution_spec_digest=execution_spec_digest,
            control_binding_snapshot_digest=binding_digest,
            execution_authorization_digest=authorization_digest,
        )
        request_id = uuid4()
        payload = {
            "model": "gpt-5",
            "input": "secret-prompt-canary",
            "temperature": 0.2,
        }
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
            missing_request_id = await client.post(
                "/v1/responses",
                headers={"Authorization": f"Bearer {step_jwt}"},
                json=payload,
            )
            streaming = await client.post(
                "/v1/responses",
                headers={
                    "Authorization": f"Bearer {step_jwt}",
                    "x-loom-provider-request-id": str(uuid4()),
                },
                json={**payload, "stream": True},
            )
            response = await client.post(
                "/v1/responses",
                headers={
                    "Authorization": f"Bearer {step_jwt}",
                    "x-loom-provider-request-id": str(request_id),
                },
                json=payload,
            )
            replay = await client.post(
                "/v1/responses",
                headers={
                    "Authorization": f"Bearer {step_jwt}",
                    "x-loom-provider-request-id": str(request_id),
                },
                json=payload,
            )
            conflict = await client.post(
                "/v1/responses",
                headers={
                    "Authorization": f"Bearer {step_jwt}",
                    "x-loom-provider-request-id": str(request_id),
                },
                json={**payload, "temperature": 0.7},
            )
        assert missing_request_id.status_code == 400
        assert streaming.status_code == 400
        assert response.status_code == 200, response.text
        assert replay.status_code == 409, replay.text
        assert replay.json()["detail"] == "provider_dispatch_replay_settled_succeeded"
        assert conflict.status_code == 409, conflict.text
        assert conflict.json()["detail"] == "provider_dispatch_request_id_conflict"
        assert len(captures["requests"]) == 1

        with engine.connect() as connection:
            row = connection.execute(
                text("""
                    SELECT d.state,d.outcome,d.upstream_attempt_count,
                           d.request_digest,d.response_digest,
                           c.trial_id,c.execution_attempt_id,c.attempt,
                           c.provider_extras::text,c.request_params::text
                    FROM pipeline_provider_dispatches d
                    JOIN llm_calls c ON c.id=d.llm_call_id
                    WHERE d.execution_attempt_id=:attempt
                """),
                {"attempt": attempt_id},
            ).one()
        assert row[:3] == ("settled", "succeeded", 1)
        assert row.request_digest == canonical_digest(payload, persisted=False)
        assert row.response_digest is not None
        assert row.trial_id is None
        assert row.execution_attempt_id == attempt_id
        assert row.attempt == 1
        assert "secret-prompt-canary" not in row.provider_extras
        assert "secret-prompt-canary" not in row.request_params

        captures["timeout"] = captures["requests"]
        timeout_request_id = uuid4()
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as client:
            timed_out = await client.post(
                "/v1/responses",
                headers={
                    "Authorization": f"Bearer {step_jwt}",
                    "x-loom-provider-request-id": str(timeout_request_id),
                },
                json={"model": "gpt-5", "input": "secret-timeout-canary"},
            )
        assert timed_out.status_code == 504
        assert "secret-transport-canary" not in timed_out.text
        assert len(captures["requests"]) == 2
        with engine.connect() as connection:
            timeout_row = connection.execute(
                text("""
                    SELECT d.state,d.outcome,d.upstream_attempt_count,
                           d.actual_cost_microusd,d.reserved_cost_microusd,
                           c.provider_extras::text,c.request_params::text
                    FROM pipeline_provider_dispatches d
                    JOIN llm_calls c ON c.id=d.llm_call_id
                    WHERE d.execution_attempt_id=:attempt
                      AND d.provider_request_id=:request_id
                """),
                {"attempt": attempt_id, "request_id": timeout_request_id},
            ).one()
        assert timeout_row[:3] == ("settled", "uncertain", 1)
        assert timeout_row.actual_cost_microusd == timeout_row.reserved_cost_microusd
        assert "secret-timeout-canary" not in timeout_row.provider_extras
        assert "secret-timeout-canary" not in timeout_row.request_params
        assert "secret-transport-canary" not in timeout_row.provider_extras
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM pipeline_provider_dispatches WHERE execution_attempt_id=:id"),
                {"id": attempt_id},
            )
            connection.execute(
                text("DELETE FROM llm_calls WHERE execution_attempt_id=:id"),
                {"id": attempt_id},
            )
            connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
            connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
            connection.execute(text("DELETE FROM rate_cards WHERE id=:id"), {"id": rate_card_id})
        engine.dispose()


async def test_responses_merges_query_request_params_into_provider_payload_and_audit(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "input": "hi",
        "store": False,
    }
    query_params = {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "messages": [{"role": "user", "content": "do not forward"}],
        "api_key": "sk-do-not-forward",
        "extra_body": {
            "top_k": 40,
            "prompt": "do not forward",
        },
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            params={"loom_request_params": json.dumps(query_params)},
            headers={"Authorization": f"Bearer {step_jwt}"},
            json=payload,
        )
    assert r.status_code == 200, r.text

    requests = captures["requests"]
    assert len(requests) == 1
    forwarded = json.loads(requests[0].content)
    assert forwarded == {
        **payload,
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {"top_k": 40},
    }
    rendered_forwarded = json.dumps(forwarded)
    assert "sk-do-not-forward" not in rendered_forwarded
    assert "do not forward" not in rendered_forwarded

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
            "seed": 1234,
            "top_k": 40,
        },
    }
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_responses_provider_connection_stream_passthrough(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "input": "hi",
        "stream": True,
        "store": False,
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "Accept": "text/event-stream",
            },
            json=payload,
        )
    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["content-type"]
    assert "response.completed" in r.text

    requests = captures["requests"]
    assert len(requests) == 1
    assert str(requests[0].url) == "https://provider.example/v1/responses"
    assert requests[0].headers["accept"] == "text/event-stream"
    assert json.loads(requests[0].content) == payload

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen2.5-coder-7b-instruct"
    assert row["input_tokens"] == 12
    assert row["output_tokens"] == 4
    assert row["provider_extras"]["reasoning_tokens"] == 2
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


@pytest.mark.parametrize("reported_usage", [False, True], ids=["missing-usage", "reported-zero"])
async def test_responses_stream_distinguishes_missing_usage_from_reported_zero(
    gateway_with_provider_connection, reported_usage: bool,
) -> None:
    from loom_service.routes.trials import _usage_by_trial_ids

    app, jwt, _, trial_id, conn_id, captures = gateway_with_provider_connection
    model = "qwen2.5-coder-7b-instruct"
    async with app.state.session_factory() as session:
        connection = await session.get(ProviderConnection, conn_id)
        connection.pricing_config = {"pricing_mode": "custom", "custom_pricing": {
            model: {"input_usd_per_1m": 5, "output_usd_per_1m": 15},
        }}
        await session.commit()
    response = {"id": "resp_zero", "model": model}
    if reported_usage:
        response["usage"] = {
            "input_tokens": 0, "output_tokens": 0,
            "input_tokens_details": {"cached_tokens": 0},
        }
    stream = f'data: {json.dumps({"type": "response.completed", "response": response})}\n\n'
    captures["response"] = httpx.Response(
        200, text=stream, headers={"content-type": "text/event-stream"},
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        result = await client.post("/v1/responses", headers={"Authorization": f"Bearer {jwt}"},
                                   json={"model": model, "input": "hi", "stream": True})
    assert result.status_code == 200, result.text
    assert result.text == stream
    async with app.state.session_factory() as session:
        call = (await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id))).one()
        summary = (await _usage_by_trial_ids(session, [trial_id]))[trial_id]
    assert summary["llm_calls_count"] == 1
    assert summary["usage_reporting_status"] == ("complete" if reported_usage else "missing")
    assert summary["usage_estimate_confidence"] == ("high" if reported_usage else "missing")
    assert summary["estimated_cost_usd"] == (0.0 if reported_usage else None)
    assert summary["cost_status"] == ("estimated" if reported_usage else "price_unknown")
    assert summary["cost_estimate_confidence"] == ("configured" if reported_usage else "unavailable")
    if not reported_usage:
        assert call.provider_extras["_loom_usage_status"] == "missing"
        assert call.provider_extras["_loom_unpriced_reason"] == "incomplete_usage"


async def _assert_failed_responses_accounting(app, trial_id: UUID) -> None:
    from loom_service.routes.trials import _usage_by_trial_ids

    async with app.state.session_factory() as session:
        call = (await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id))).one()
        summary = (await _usage_by_trial_ids(session, [trial_id]))[trial_id]
        receipt = (await session.execute(text(
            "SELECT provider_http_status, gateway_http_status FROM gateway_dispatch_receipts "
            "WHERE trial_id = :trial_id"
        ), {"trial_id": trial_id})).one()
    assert call.dialect == "openai_responses"
    assert call.attempt == 1
    assert call.rate_card_hash == "failed-upstream"
    assert call.provider_extras["_loom_call_status"] == "failed"
    assert call.provider_extras["_loom_usage_status"] == "missing"
    assert call.provider_extras["_loom_failure_category"] == "upstream_invalid_response"
    assert call.provider_extras["_loom_failure_status_code"] == 200
    assert "input" not in call.request_params
    assert summary["failed_upstream_llm_calls_count"] == 1
    assert summary["priced_llm_calls_count"] == 0
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_status"] == "failed_upstream"
    assert summary["usage_reporting_status"] == "missing"
    assert tuple(receipt) == (200, 502)


@pytest.mark.parametrize(("native", "body"), [
    (True, b"not JSON"), (True, b"[]"),
    (False, b"not JSON"), (False, b"[]"), (False, b"{}"),
], ids=["native-non-json", "native-non-object", "fallback-non-json", "fallback-non-object", "fallback-no-choices"])
async def test_responses_malformed_success_keeps_failed_accounting(
    gateway_with_provider_connection, native: bool, body: bytes,
) -> None:
    app, jwt, _, trial_id, conn_id, captures = gateway_with_provider_connection
    async with app.state.session_factory() as session:
        connection = await session.get(ProviderConnection, conn_id)
        connection.responses_api_supported = native
        await session.commit()
    captures["response"] = httpx.Response(200, content=body)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        result = await client.post("/v1/responses", headers={"Authorization": f"Bearer {jwt}"},
                                   json={"model": "qwen2.5-coder-7b-instruct", "input": "hi"})
    assert result.status_code == 502, result.text
    await _assert_failed_responses_accounting(app, trial_id)


@pytest.mark.parametrize("body", [b"not JSON", b"[]", b"{}"],
                         ids=["non-json", "non-object", "missing-usage"])
async def test_platform_responses_malformed_success_keeps_failed_accounting(gateway, body: bytes) -> None:
    app, jwt, _, trial_id = gateway
    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body)),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        result = await client.post("/v1/responses", headers={"Authorization": f"Bearer {jwt}"},
                                   json={"model": "gpt-5", "input": "hi"})
    assert result.status_code == 502, result.text
    await _assert_failed_responses_accounting(app, trial_id)


async def test_responses_facade_falls_back_to_chat_completions_for_chat_only_provider(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection

    def _chat_only_handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)
        assert request.url.host == "provider.example"
        assert request.headers["authorization"] == "Bearer sk-provider-XYZ"
        if request.url.path == "/v1/responses":
            return httpx.Response(
                400,
                json={
                    "error": {
                        "message": "you must provide a messages parameter",
                        "type": "invalid_request_error",
                        "param": "messages",
                        "code": "missing_required_parameter",
                    },
                },
            )
        assert request.url.path == "/v1/chat/completions"
        body = json.loads(request.content)
        assert body["model"] == "qwen3.6-35b-a3b"
        assert body["stream"] is False
        assert body["tool_choice"] == "auto"
        assert body["parallel_tool_calls"] is False
        assert body["messages"] == [
            {"role": "system", "content": "You are Codex."},
            {"role": "user", "content": "run echo"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_old",
                        "type": "function",
                        "function": {
                            "name": "exec_command",
                            "arguments": '{"cmd":"pwd"}',
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_old",
                "content": "ok",
            },
        ]
        assert body["tools"] == [
            {
                "type": "function",
                "function": {
                    "name": "exec_command",
                    "description": "run commands",
                    "parameters": {
                        "type": "object",
                        "properties": {"cmd": {"type": "string"}},
                        "required": ["cmd"],
                    },
                },
            }
        ]
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl_compat",
                "model": body["model"],
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call_new",
                                    "type": "function",
                                    "function": {
                                        "name": "exec_command",
                                        "arguments": '{"cmd":"echo hi"}',
                                    },
                                }
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                },
            },
        )

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_chat_only_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url=app.state.settings.egress_proxy_url,
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,
    )

    payload = {
        "model": "qwen3.6-35b-a3b",
        "instructions": "You are Codex.",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "run echo"}],
            },
            {
                "type": "function_call",
                "call_id": "call_old",
                "name": "exec_command",
                "arguments": '{"cmd":"pwd"}',
            },
            {
                "type": "function_call_output",
                "call_id": "call_old",
                "output": "ok",
            },
        ],
        "stream": True,
        "store": False,
        "tool_choice": "auto",
        "parallel_tool_calls": False,
        "tools": [
            {
                "type": "function",
                "name": "exec_command",
                "description": "run commands",
                "parameters": {
                    "type": "object",
                    "properties": {"cmd": {"type": "string"}},
                    "required": ["cmd"],
                },
            },
            {"type": "namespace", "name": "multi_agent_v1"},
        ],
    }
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/responses",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "Accept": "text/event-stream",
            },
            json=payload,
        )

    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["content-type"]
    assert "response.function_call_arguments.done" in r.text
    assert "call_new" in r.text
    assert "response.completed" in r.text

    requests = captures["requests"]
    assert [request.url.path for request in requests] == [
        "/v1/responses",
        "/v1/chat/completions",
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen3.6-35b-a3b"
    assert row["input_tokens"] == 11
    assert row["output_tokens"] == 7
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_gemini_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # usageMetadata preserved including thoughtsTokenCount.
        assert body["usageMetadata"]["thoughtsTokenCount"] == 12

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "gemini"
    assert row["model"] == "gemini-2.0-flash"
    assert row["input_tokens"] == 150
    assert row["output_tokens"] == 60
    assert row["provider_extras"]["cachedContentTokenCount"] == 40
    assert row["provider_extras"]["thoughtsTokenCount"] == 12
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


@pytest.mark.parametrize("key", [None, ""])
async def test_gemini_missing_key_returns_unavailable_without_upstream(gateway, key):
    app, step_jwt, _team_id, _trial_id = gateway
    app.state.settings.google_api_key = None if key is None else SecretStr(key)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        response = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )
    assert response.status_code == 503
    assert response.json()["detail"] == "google_api_key not configured on Gateway"


async def test_gemini_rejects_invalid_bearer_with_shared_auth_contract(
    gateway,
):  # type: ignore[no-untyped-def]
    app, _step_jwt, _team_id, _trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        response = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": "Bearer invalid"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == 'Bearer error="invalid_token"'
    assert response.json()["detail"] == {"code": "invalid_or_expired_bearer"}


async def test_gemini_upstream_503_records_failed_audit_row(  # type: ignore[no-untyped-def]
    gateway,
    postgres_url,
):
    app, step_jwt, team_id, trial_id = gateway

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "generativelanguage.googleapis.com"
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )
    assert r.status_code == 503
    assert "temporarily unavailable" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "gemini"
    assert row["model"] == "gemini-2.0-flash"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_5xx",
        "_loom_failure_status_code": 503,
        "_loom_usage_status": "missing",
    }
    assert "contents" not in row["request_params"]


async def test_gemini_rejects_path_without_colon(gateway):  # type: ignore[no-untyped-def]
    app, step_jwt, _t, _tr = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={},
        )
        assert r.status_code == 400
