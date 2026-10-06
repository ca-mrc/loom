"""GET /agents and GET /models — the catalogs the SPA's
AgentModelPicker reads (Plan 25)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    ProviderConnection,
    ProviderModelCache,
    RateCard,
    Team,
    TeamQuota,
    Token,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    conn_id = uuid4()
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        # Two rate cards with overlapping provider+model entries —
        # /models must de-duplicate.
        s.execute(
            insert(RateCard).values(
                id="card-a",
                captured_at=datetime.now(UTC),
                table={
                    "id": "card-a",
                    "entries": [
                        {
                            "provider": "anthropic",
                            "model": "claude-opus-4-7",
                            "input_per_mtok": 15,
                            "output_per_mtok": 75,
                            "cache_read_per_mtok": 0,
                            "cache_write_per_mtok": 0,
                        },
                        {
                            "provider": "openai",
                            "model": "gpt-4o",
                            "input_per_mtok": 5,
                            "output_per_mtok": 20,
                            "cache_read_per_mtok": 0,
                            "cache_write_per_mtok": 0,
                        },
                    ],
                },
            )
        )
        s.execute(
            insert(RateCard).values(
                id="card-b",
                captured_at=datetime.now(UTC),
                table={
                    "id": "card-b",
                    "entries": [
                        {
                            "provider": "anthropic",
                            "model": "claude-opus-4-7",
                            "input_per_mtok": 10,
                            "output_per_mtok": 50,
                            "cache_read_per_mtok": 0,
                            "cache_write_per_mtok": 0,
                        },
                        {
                            "provider": "google",
                            "model": "gemini-2.5-pro",
                            "input_per_mtok": 7,
                            "output_per_mtok": 21,
                            "cache_read_per_mtok": 0,
                            "cache_write_per_mtok": 0,
                        },
                    ],
                },
            )
        )
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="Lab vLLM",
                base_url="https://api.openai.com/v1",
                upstream_host="api.openai.com",
                resolved_egress_ips=["104.18.0.1"],
                encrypted_api_key_ref="test://lab-vllm",
                allowed_models=None,
                status="valid",
                pricing_source="tokens-only",
                pricing_data=None,
                rate_card_provider="openai",
                created_by="test:service-models",
            )
        )
        now = datetime.now(UTC)
        for model_id in [
            "deepseek-chat",
            "glm-5.1-thinking",
            "amap-coordinate-convert",
            "apisports-afl-games",
            "tushare-stock-basic",
        ]:
            s.execute(
                insert(ProviderModelCache).values(
                    provider_connection_id=conn_id,
                    model_id=model_id,
                    capabilities={},
                    visible=True,
                    last_seen_at=now,
                    upstream_present=True,
                )
            )
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(ProviderConnection))
            s.execute(delete(RateCard))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_agents_includes_builtins_and_adapters(
    monkeypatch: pytest.MonkeyPatch,
    setup: tuple[FastAPI, str],
) -> None:
    # Runtime-contract metadata; hosted Nebius availability is covered separately.
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    app, raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/agents",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    names = {a["name"] for a in r.json()["items"]}
    # Builtins.
    assert {"oracle", "direct-completion"}.issubset(names)
    assert "litellm" not in names
    # A representative adapter from loom-launcher.
    assert "claude-code" in names
    # Internal launcher canaries are not part of the user-facing API catalog.
    assert "hello" not in names
    # The legacy "claude-code-inbox" alias was retired (was redundant
    # with the on-demand-install claude-code adapter since #317).
    assert "claude-code-inbox" not in names
    # Oracle should be flagged needs_model=False; everything else True.
    by_name = {a["name"]: a for a in r.json()["items"]}
    assert all(a["catalog_visibility"] == "displayed" for a in by_name.values())
    assert by_name["oracle"]["needs_model"] is False
    assert by_name["direct-completion"]["needs_model"] is True
    assert by_name["direct-completion"]["aliases"] == ["litellm"]
    assert by_name["claude-code"]["needs_model"] is True
    assert by_name["oracle"]["kind"] == "builtin"
    assert by_name["claude-code"]["kind"] == "adapter"

    # #289 runtime readiness metadata: displayed agents must say whether
    # service-mode can run them before users submit a doomed batch.
    # #317 Phase 3c: install_script-equipped adapters now surface
    # an on-demand-install message rather than implying pre-baked binaries.
    opencode = by_name["opencode"]
    assert opencode["service_mode_ready"] is True
    assert opencode["readiness_status"] == "ready"
    assert "installs into the trial sandbox on demand" in (
        opencode["readiness_message"] or ""
    )
    assert opencode["runtime_contract"]["required_executables"] == ["opencode"]
    assert opencode["runtime_contract"]["required_packages"] == ["opencode-ai"]
    assert opencode["runtime_contract"]["capture"] == "stdout_jsonl"
    assert opencode["runtime_contract"]["endpoint_dialect"] == "openai_chat"

    # PR-A metadata: providers + sources are surfaced so the SPA can
    # filter the model picker by agent.
    assert by_name["oracle"]["supported_providers"] == []
    assert by_name["oracle"]["supported_model_sources"] == []
    assert by_name["direct-completion"]["supported_providers"] == ["*"]
    assert by_name["direct-completion"]["supported_model_sources"] == ["api"]
    # CLI adapters lock down to their provider.
    assert by_name["claude-code"]["supported_providers"] == ["anthropic"]
    assert by_name["claude-code"]["supported_model_sources"] == ["api"]

    # #2054: five supported product entries; the rest stay listed as deferred.
    assert {
        name for name, a in by_name.items() if a["product_support"] == "supported"
    } == {"oracle", "direct-completion", "terminus-2", "openhands-sdk", "codex"}
    assert by_name["claude-code"]["product_support"] == "deferred"
    assert by_name["claude-code"]["deferred_reason"]
    assert by_name["openhands-sdk"]["aliases"] == ["openhands"]
    assert by_name["openhands-sdk"]["display_name"] == "openhands"
    assert "openhands" not in by_name


async def test_models_deduplicates_across_rate_cards_and_adds_byo_metadata(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/models",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    rate_card_pairs = [(m["provider"], m["name"]) for m in items if m["source"] == "rate-card"]
    # claude-opus-4-7 appears in both cards but only once here.
    assert rate_card_pairs == [
        ("anthropic", "claude-opus-4-7"),
        ("google", "gemini-2.5-pro"),
        ("openai", "gpt-4o"),
    ]
    byo_items = [m for m in items if m.get("provider_connection_name") == "Lab vLLM"]
    assert [m["name"] for m in byo_items] == ["deepseek-chat", "glm-5.1-thinking"]
    item = next(m for m in byo_items if m["name"] == "glm-5.1-thinking")
    assert item["provider"] == "openai"
    assert item["provider_connection_id"]
    assert item["provider_connection_type"] == "openai-compatible"
    # Never probed: Gateway checks native Responses support on first call.
    assert item["responses_route"] == "unprobed"
    assert item["source"] == "discovered"
    assert item["agent_capable"] is True
    assert item["recommended"] is True
    assert item["visibility"] == "default"
    assert item["hidden_reason"] is None
    assert item["last_seen_at"] is not None


async def test_models_raw_view_explains_filtered_tool_api_entries(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/models?view=raw",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    by_name = {m["name"]: m for m in r.json()["items"]}
    for model_id in [
        "amap-coordinate-convert",
        "apisports-afl-games",
        "tushare-stock-basic",
    ]:
        item = by_name[model_id]
        assert item["provider_connection_name"] == "Lab vLLM"
        assert item["source"] == "discovered"
        assert item["agent_capable"] is False
        assert item["recommended"] is False
        assert item["visibility"] == "advanced"
        assert item["hidden_reason"] == "classifier-non-llm"


async def test_agents_unauthenticated_401(
    setup: tuple[FastAPI, str],
) -> None:
    app, _raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/agents")
    assert r.status_code == 401


async def test_models_unauthenticated_401(
    setup: tuple[FastAPI, str],
) -> None:
    app, _raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/models")
    assert r.status_code == 401


async def test_agents_report_hosted_native_availability(
    monkeypatch: pytest.MonkeyPatch,
    setup: tuple[FastAPI, str],
) -> None:
    """#2054: on hosted (Nebius) deployments the catalog reports what native
    execution can actually run instead of claiming every entry is ready."""
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    app, raw = setup
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/agents", headers={"Authorization": f"Bearer {raw}"})
    assert r.status_code == 200
    by_name = {a["name"]: a for a in r.json()["items"]}

    for name in ("direct-completion", "terminus-2", "oracle", "codex"):
        assert by_name[name]["service_mode_ready"] is True, name
        assert by_name[name]["readiness_status"] == "ready", name
    for name in ("openhands-sdk",):
        entry = by_name[name]
        assert entry["service_mode_ready"] is False, name
        assert entry["readiness_status"] == "unavailable", name
        assert "not yet runnable on hosted (Nebius) execution" in entry["readiness_message"]
        # Still a supported product entry; only its native path is missing.
        assert entry["product_support"] == "supported", name

    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as ac:
        local = await ac.get("/api/v1/agents", headers={"Authorization": f"Bearer {raw}"})
    local_by_name = {a["name"]: a for a in local.json()["items"]}
    assert local_by_name["codex"]["service_mode_ready"] is True
