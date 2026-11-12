"""GET /api/v1/trials list with filters + cursor pagination (Plan 18 Task 2).

Reward projections come from `Trial.result`; agent name/model from
`Trial.config`. `batch_id` is projected when set on the trial row.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Artifact,
    Batch,
    Benchmark,
    LlmCall,
    ProviderConnection,
    RateCard,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialEvent,
    User,
    Worker,
)
from loom_llm_gateway.rate_card import RateCardTable, hash_table
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def trials_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, list[UUID]]]:
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
    username = f"TrialOwner-{team_id.hex[:8]}"
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    task_id = f"local/task-{uuid4().hex[:8]}"
    trial_ids = [uuid4() for _ in range(3)]
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=now,
                expires_at=None,
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config={"task": {"id": task_id, "name": "t"}},
                source="local",
                license="MIT",
            )
        )
        for i, tid in enumerate(trial_ids):
            s.execute(
                insert(Trial).values(
                    id=tid,
                    task_id=task_id,
                    team_id=team_id,
                    state="succeeded" if i % 2 == 0 else "running",
                    config={"agent": {"name": "oracle", "model": None}},
                    requires_caps={},
                    submitted_by_user_id=user_id,
                    submitted_at=now - timedelta(minutes=i),
                    result=(
                        {
                            "aggregate_reward": 1.0,
                            "cost_usd": 0.05,
                            "reward": {"score": 1.0},
                            "steps": [
                                {
                                    "step_name": "main",
                                    "verifier_result": {
                                        "rewards": {"score": 1.0},
                                        "checks": [
                                            {
                                                "name": "structured_detail",
                                                "passed": True,
                                                "score": 1.0,
                                                "detail": {
                                                    "exit_code": 0,
                                                    "source": "compat-gate",
                                                },
                                            },
                                            {
                                                "name": "string_detail",
                                                "passed": True,
                                                "score": 1.0,
                                                "detail": "exit_code=0",
                                            },
                                        ],
                                    },
                                },
                            ],
                        }
                        if i % 2 == 0
                        else None
                    ),
                )
            )
        s.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": trial_ids[0],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "provider_extras": {},
                    "cost_usd": Decimal("9.990000"),
                    "rate_card_hash": "old-rate-card",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": trial_ids[0],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 2,
                    "output_tokens": 1,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000000"),
                    "rate_card_hash": "facade:rate-card:missing",
                },
            ],
        )
        s.commit()
    try:
        yield app, raw, team_id, trial_ids
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(TrialEvent))
            s.execute(delete(LlmCall))
            s.execute(delete(Artifact))
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Batch))
            s.execute(delete(RateCard))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.username_normalized == username.casefold()))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_trial_search_finds_later_pages_and_preserves_scope(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    """F1: a known trial outside page one must be found before pagination."""
    app, raw, team_id, trial_ids = trials_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        headers = {"Authorization": f"Bearer {raw}"}
        first = await client.get("/api/v1/trials", params={"limit": 1}, headers=headers)
        assert first.status_code == 200
        assert first.json()["items"][0]["id"] == str(trial_ids[0])
        assert first.json()["next_cursor"]
        cursor = first.json()["next_cursor"]
        for index in (1, 2):
            page = await client.get("/api/v1/trials", params={"q": f"TrialOwner-{team_id.hex[:8]}", "limit": 1, "cursor": cursor}, headers=headers)
            assert page.status_code == 200
            assert [item["id"] for item in page.json()["items"]] == [str(trial_ids[index])]
            cursor = page.json()["next_cursor"]
        assert cursor is None
        match = await client.get("/api/v1/trials", params={"q": str(trial_ids[2]), "limit": 1}, headers=headers)
        assert match.status_code == 200
        assert [item["id"] for item in match.json()["items"]] == [str(trial_ids[2])]
        assert match.json()["next_cursor"] is None
        assert match.json()["items"][0]["team_id"] == str(team_id)
        summary = await client.get("/api/v1/monitor/summary", params={"view": "trials", "q": str(trial_ids[2])}, headers=headers)
        assert summary.status_code == 200, summary.text
        assert summary.json()["state_counts"]["trials"]["succeeded"] == 1
        assert summary.json()["state_counts"]["trials"]["running"] == 0
        empty = await client.get("/api/v1/trials", params={"q": "no-such-task-2099"}, headers=headers)
        assert empty.json() == {"items": [], "next_cursor": None}
        literal = await client.get("/api/v1/trials", params={"q": "%_"}, headers=headers)
        assert literal.json()["items"] == []
        forbidden = await client.get("/api/v1/trials", params={"q": str(trial_ids[2]), "team_id": str(uuid4())}, headers=headers)
        assert forbidden.status_code == 403


async def test_list_my_trials(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert len(items) == 3
    # Newest first (submitted_at desc). trial_ids[0] is newest in the fixture.
    assert items[0]["id"] == str(trial_ids[0])
    assert items[0]["team_id"] == str(team_id)
    assert items[0]["team_name"] == f"t-{team_id}"
    assert items[0]["owner_team"] == {
        "id": str(team_id),
        "name": f"t-{team_id}",
    }
    submitted = items[0]["submitted_by_user"]
    UUID(submitted["id"])
    assert submitted["username"] == f"TrialOwner-{team_id.hex[:8]}"
    assert submitted["team_id"] == str(team_id)
    assert submitted["team_name"] == f"t-{team_id}"
    # Reward stays projected from result, but LLM usage is derived from
    # llm_calls so stale/frozen cost values are not exposed.
    assert items[0]["aggregate_reward"] == 1.0
    assert "cost_usd" not in items[0]
    assert items[0]["total_prompt_tokens"] == 12
    assert items[0]["total_completion_tokens"] == 6
    assert items[0]["estimated_cost_usd"] == pytest.approx(9.99)
    assert items[0]["cost_currency"] == "USD"
    assert items[0]["cost_status"] == "mixed"
    assert items[0]["pricing_modes"] == ["priced", "price-unknown"]
    assert items[0]["llm_calls_count"] == 2
    # Running trial: no reward.
    assert items[1]["aggregate_reward"] is None
    assert items[1]["total_prompt_tokens"] == 0
    assert items[1]["total_completion_tokens"] == 0
    assert items[1]["estimated_cost_usd"] is None
    assert items[1]["cost_status"] == "no_usage"
    assert items[1]["llm_calls_count"] == 0
    # Agent name pulled from config.
    assert items[0]["agent_name"] == "oracle"
    # failure_message field present in list response (issue #164).
    assert "failure_message" in items[0]


async def test_list_trials_filters_failure_reason_prefix(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(state="failed", failure_reason="timed_out")
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[1])
            .values(state="failed", failure_reason="task_error")
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[2])
            .values(state="failed", failure_reason=None)
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        timed = await ac.get(
            "/api/v1/trials",
            params={"state": "failure:timed_out"},
            headers={"Authorization": f"Bearer {raw}"},
        )
        unknown = await ac.get(
            "/api/v1/trials",
            params={"state": "failure:unknown_failure"},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert timed.status_code == 200, timed.text
    assert {item["id"] for item in timed.json()["items"]} == {str(trial_ids[0])}
    assert unknown.status_code == 200, unknown.text
    assert {item["id"] for item in unknown.json()["items"]} == {str(trial_ids[2])}


async def test_trial_list_and_detail_project_current_agent_config_shape(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    model = {"provider": "openai-compatible", "name": "gpt-5-mini"}

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(config={"agent_name": "oracle", "agent_model": None}),
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[1])
            .values(config={"agent_name": "litellm", "agent_model": model}),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        list_resp = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
        )
        detail_resp = await ac.get(
            f"/api/v1/trials/{trial_ids[1]}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert list_resp.status_code == 200, list_resp.text
    items_by_id = {item["id"]: item for item in list_resp.json()["items"]}
    assert items_by_id[str(trial_ids[0])]["agent_name"] == "oracle"
    assert items_by_id[str(trial_ids[0])]["model"] is None
    assert items_by_id[str(trial_ids[1])]["agent_name"] == "litellm"
    assert items_by_id[str(trial_ids[1])]["model"] == {
        "provider": "openai-compatible",
        "name": "gpt-5-mini",
    }

    assert detail_resp.status_code == 200, detail_resp.text
    detail = detail_resp.json()
    assert detail["agent_name"] == "litellm"
    assert detail["model"] == {
        "provider": "openai-compatible",
        "name": "gpt-5-mini",
    }
    submitted = detail["submitted_by_user"]
    UUID(submitted["id"])
    assert submitted["username"] == f"TrialOwner-{_team_id.hex[:8]}"
    assert submitted["team_id"] == str(_team_id)
    assert submitted["team_name"] == f"t-{_team_id}"


async def test_filter_by_state_succeeded_only(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?state=succeeded",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    assert all(i["state"] == "succeeded" for i in items)


async def test_filter_by_multi_state(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?state=succeeded,running",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 3


async def test_pagination_cursor(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/trials?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] is not None

        r2 = await ac.get(
            f"/api/v1/trials?limit=2&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    j2 = r2.json()
    assert len(j2["items"]) == 1
    assert j2["next_cursor"] is None


async def test_invalid_cursor_returns_400(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?cursor=!!!not-a-cursor",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_cross_team_forbidden_for_team(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    other_team = uuid4()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials?team_id={other_team}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_no_read_own_scope_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, _raw, team_id, _t = trials_setup
    no_scope_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(no_scope_raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {no_scope_raw}"},
        )
    assert r.status_code == 403


async def test_trial_detail_returns_service_download_urls(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == str(trial_ids[0])
    assert body["atif_url"] == (f"http://svc/api/v1/trials/{trial_ids[0]}/atif")
    assert body["trajectory_url"] == (
        f"http://svc/api/v1/trials/{trial_ids[0]}/trajectory/download"
    )
    assert "localhost:9000" not in body["atif_url"]
    assert "localhost:9000" not in body["trajectory_url"]
    assert "X-Amz-Signature" not in body["atif_url"]
    assert "X-Amz-Signature" not in body["trajectory_url"]
    assert body["artifacts"] == []
    assert "cost_usd" not in body
    assert body["total_prompt_tokens"] == 12
    assert body["total_completion_tokens"] == 6
    assert body["estimated_cost_usd"] == pytest.approx(9.99)
    assert body["cost_currency"] == "USD"
    assert body["cost_status"] == "mixed"
    assert body["pricing_modes"] == ["priced", "price-unknown"]
    assert body["llm_calls_count"] == 2
    details = [
        check["detail"]
        for check in body["result"]["steps"][0]["verifier_result"]["checks"]
    ]
    assert details == [{"exit_code": 0, "source": "compat-gate"}, "exit_code=0"]
    # failure_message field present in response (issue #164).
    assert "failure_message" in body


async def test_trial_detail_exposes_rate_card_snapshot_metadata(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    captured_at = datetime(2026, 6, 27, tzinfo=UTC)
    table = RateCardTable(
        id="yibuapi-pricing-v1",
        captured_at=captured_at,
        provider="yibuapi",
        source_url="https://yibuapi.com/api/pricing",
        pricing_version="pricing-v1",
        last_checked_at=captured_at,
        currency="USD",
        group="default",
        group_ratio=1.0,
        entry_count=1,
        skipped_model_count=0,
        entries=[
            {
                "provider": "yibuapi",
                "model": "qwen3.6-35b-a3b",
                "input_per_mtok": 0.25,
                "output_per_mtok": 0.75,
                "cache_read_per_mtok": 0.0,
                "cache_write_per_mtok": 0.0,
                "currency": "USD",
                "source_url": "https://yibuapi.com/api/pricing",
                "pricing_version": "pricing-v1",
                "source_model": "Qwen3.6 35B A3B",
                "pricing_unit": "mtok",
            }
        ],
    )
    table_hash = hash_table(table)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(RateCard).values(
                id=table.id,
                captured_at=table.captured_at,
                table=table.model_dump(mode="json", exclude={"captured_at"}),
            )
        )
        s.execute(
            update(LlmCall)
            .where(
                LlmCall.trial_id == trial_ids[0],
                LlmCall.rate_card_hash == "old-rate-card",
            )
            .values(rate_card_hash=table_hash),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    assert r.json()["price_snapshots"] == [
        {
            "rate_card_hash": table_hash,
            "rate_card_id": "yibuapi-pricing-v1",
            "resolved": True,
            "provider": "yibuapi",
            "source_url": "https://yibuapi.com/api/pricing",
            "pricing_version": "pricing-v1",
            "last_checked_at": "2026-06-27T00:00:00+00:00",
            "currency": "USD",
            "group": "default",
            "group_ratio": 1.0,
        }
    ]


async def test_trial_detail_exposes_incomplete_usage_confidence(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(LlmCall)
            .where(
                LlmCall.trial_id == trial_ids[0],
                LlmCall.rate_card_hash == "facade:rate-card:missing",
            )
            .values(
                input_tokens=0,
                output_tokens=0,
                provider_extras={"_loom_usage_status": "missing"},
            ),
        )
        s.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_ids[0],
                step_id="main",
                model="openai/gpt-test",
                dialect="openai_facade",
                input_tokens=3,
                output_tokens=0,
                provider_extras={
                    "_loom_usage_status": "partial",
                    "_loom_missing_usage_keys": ["completion_tokens"],
                    "_loom_provider_usage": {"prompt_tokens": 3},
                },
                cost_usd=Decimal("0.000000"),
                rate_card_hash="facade:tokens-only",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["partial_usage_llm_calls_count"] == 1
    assert body["missing_usage_llm_calls_count"] == 1
    assert body["usage_reporting_status"] == "partial"
    assert body["usage_estimate_confidence"] == "partial"


async def test_trial_detail_marks_real_provider_zero_call_evidence_invalid(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, _ = trials_setup
    trial_id = uuid4()
    task_id = f"local/task-{uuid4().hex[:8]}"
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="z" * 64,
                config={"task": {"id": task_id, "name": "zero-call"}},
                source="local",
                license="MIT",
            )
        )
        conn.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="succeeded",
                config={
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                requires_caps={},
                submitted_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                result={"aggregate_reward": 0.0},
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["aggregate_reward"] == 0.0
    assert body["llm_calls_count"] == 0
    assert body["llm_evidence_status"] == "no_calls_invalid"
    assert body["no_call"] is True
    assert body["no_call_reason"] == "terminal_model_backed_no_call"
    assert "clean parity" in body["no_call_message"]
    assert body["no_call_retryable"] is False


async def test_trial_debug_evidence_is_structured_and_redacted(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    trial_id = trial_ids[1]
    artifact_key = f"{team_id}/{trial_id}/main/diagnostics.txt"
    worker_id = uuid4()
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_id = s.execute(
            select(Trial.task_id).where(Trial.id == trial_id),
        ).scalar_one()
        s.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                config={
                    "task": {"id": task_id, "name": "t"},
                    "agent": {"name": "opencode", "timeout_sec": 2400.0},
                },
            )
        )
        s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="trt-gb10-debug",
                version="test",
                capabilities=[],
                max_concurrent=1,
                pool_name="gb10",
                registered_at=now - timedelta(minutes=5),
                last_seen_at=now - timedelta(seconds=5),
                status="active",
            )
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                state="failed",
                started_at=datetime.now(UTC) - timedelta(minutes=2),
                finished_at=datetime.now(UTC),
                attempt_count=2,
                failure_reason="verifier_error",
                failure_message=(
                    "Verifier failed while calling "
                    "http://loom-control-plane:8080/tasks with "
                    "Authorization: Bearer loom_api_supersecret"
                ),
                config={
                    "agent_name": "opencode",
                    "agent_timeout_multiplier": 1.0,
                    "agent_model": {"provider": "openai", "name": "gpt-debug"},
                },
                worker_id=worker_id,
                result={
                    "aggregate_reward": 0.0,
                    "reward": {"passed": 0.0},
                    "steps": [
                        {
                            "step_name": "main",
                            "verifier_result": {
                                "rewards": {"passed": 0.0},
                                "error": {
                                    "kind": "missing_tests",
                                    "message": ("pytest could not read sk-provider-secret"),
                                },
                            },
                            "error": {
                                "phase": "verifier",
                                "reason": "exception",
                                "message": (
                                    "raw signed url https://minio.internal/a?X-Amz-Signature=secret"
                                ),
                            },
                        }
                    ],
                },
                trajectory_index={
                    "trajectory_uri": (f"s3://trajectories/{team_id}/{trial_id}/events.jsonl"),
                    "atif_uri": (f"s3://trajectories/{team_id}/{trial_id}/atif.json"),
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": "artifacts",
                            "key": artifact_key,
                            "size": 99,
                            "share_status": "blocked",
                            "blocked_reason": ("secret-like content sk-artifact-secret"),
                        }
                    ],
                },
            ),
        )
        s.execute(
            insert(TrialEvent).values(
                id=uuid4(),
                trial_id=trial_id,
                seq=7,
                kind="stdout",
                source="worker",
                schema_version=1,
                payload={
                    "kind": "stdout",
                    "seq": 7,
                    "step_id": "main",
                    "emitted_at": (now - timedelta(seconds=30)).isoformat(),
                },
                created_at=now - timedelta(seconds=30),
            )
        )
        s.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="openai/gpt-debug",
                dialect="openai",
                input_tokens=11,
                output_tokens=7,
                provider_extras={"finish_reason": "error"},
                request_params={
                    "status": "available",
                    "parameters": {
                        "temperature": 0,
                        "top_p": 1,
                        "seed": 1234,
                        "reasoning": {"effort": "low"},
                    },
                },
                cost_usd=Decimal("0.000001"),
                rate_card_hash="debug-rate-card",
                attempt=3,
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        debug = await ac.get(
            f"/api/v1/trials/{trial_id}/debug",
            headers={"Authorization": f"Bearer {raw}"},
        )
        diagnosis = await ac.get(
            f"/api/v1/trials/{trial_id}/diagnosis",
            headers={"Authorization": f"Bearer {raw}"},
        )
        detail = await ac.get(
            f"/api/v1/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert debug.status_code == 200, debug.text
    body = debug.json()
    assert body["schema_version"] == "1"
    assert body["entity"]["type"] == "trial"
    assert body["entity"]["id"] == str(trial_id)
    assert body["entity"]["team_id"] == str(team_id)
    assert body["failure"]["reason_code"] == "trial.verifier_error"
    assert body["failure"]["category"] == "verifier"
    assert body["failure"]["attribution"] == "benchmark"
    assert body["task"]["task_id"].startswith("local/task-")
    assert body["task"]["task_checksum"] == "x" * 64
    assert body["task"]["materialization_state"] == "ready"
    assert body["provider"]["llm_calls_count"] == 1
    assert body["provider"]["total_prompt_tokens"] == 11
    assert body["provider"]["total_completion_tokens"] == 7
    assert body["provider"]["max_attempt"] == 3
    assert body["provider"]["models"] == ["openai/gpt-debug"]
    assert body["provider"]["request_params_status_counts"] == {"available": 1}
    assert body["lifecycle"]["runtime_sec"] is not None
    assert body["agent"]["timeout"]["agent_timeout_sec"] == 2400.0
    assert body["worker"]["worker_id"] == str(worker_id)
    assert body["worker"]["pool_name"] == "gb10"
    assert body["worker"]["heartbeat_fresh"] is True
    assert body["activity"]["last_trial_event"]["kind"] == "stdout"
    assert body["activity"]["last_trial_event"]["seq"] == 7
    assert body["activity"]["last_llm_call_at"] is not None
    assert body["stale_running"]["decision"] == "keep"
    assert body["stale_running"]["reason"] == "not_running"
    assert body["provider"]["request_params"] == [
        {
            "llm_call_id": body["provider"]["request_params"][0]["llm_call_id"],
            "step_id": "main",
            "dialect": "openai",
            "model": "openai/gpt-debug",
            "attempt": 3,
            "status": "available",
            "parameters": {
                "temperature": 0,
                "top_p": 1,
                "seed": 1234,
                "reasoning": {"effort": "low"},
            },
        },
    ]
    assert "messages" not in str(body["provider"]["request_params"])
    assert "Authorization" not in str(body["provider"]["request_params"])
    assert body["reward"]["aggregate_reward"] == 0.0
    assert body["reward"]["components"] == {"passed": 0.0}
    assert body["evidence_refs"]["atif"]["ready"] is True
    assert body["evidence_refs"]["trajectory"]["ready"] is True
    artifact_url = body["evidence_refs"]["artifacts"][0]["download_url"]
    assert artifact_url.startswith(
        f"http://svc/api/v1/trials/{trial_id}/artifacts/download?key=",
    )
    assert artifact_key.replace("/", "%2F") in artifact_url
    rendered = json.dumps(body)
    assert "loom_api_supersecret" not in rendered
    assert "sk-provider-secret" not in rendered
    assert "sk-artifact-secret" not in rendered
    assert "loom-control-plane" not in rendered
    assert "X-Amz-Signature=secret" not in rendered
    assert "provider preflight" not in " ".join(body["next_actions"]).lower()

    assert diagnosis.status_code == 200, diagnosis.text
    diagnosis_body = diagnosis.json()
    assert diagnosis_body["schema_version"] == "1"
    assert diagnosis_body["entity"] == {
        "type": "trial",
        "id": str(trial_id),
    }
    assert diagnosis_body["primary_cause"]["reason_code"] == ("trial.verifier_error")
    assert diagnosis_body["primary_cause"]["category"] == "verifier"
    assert diagnosis_body["primary_cause"]["attribution"] == "benchmark"
    assert "not reliable" in diagnosis_body["impact"]
    assert diagnosis_body["reason_clusters"][0]["representative_trial_id"] == (str(trial_id))
    rendered_diagnosis = json.dumps(diagnosis_body)
    assert "loom_api_supersecret" not in rendered_diagnosis
    assert "sk-provider-secret" not in rendered_diagnosis
    assert "sk-artifact-secret" not in rendered_diagnosis
    assert "loom-control-plane" not in rendered_diagnosis
    assert "X-Amz-Signature=secret" not in rendered_diagnosis

    assert detail.status_code == 200, detail.text
    assert detail.json()["debug_evidence"]["failure"] == body["failure"]
    assert detail.json()["diagnosis"]["primary_cause"] == (diagnosis_body["primary_cause"])


async def test_trial_debug_evidence_distinguishes_failed_upstream_attempt(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    trial_id = trial_ids[1]
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 500.",
                config={
                    "agent": {
                        "name": "litellm",
                        "model": {"provider": "openai", "name": "gpt-debug"},
                    },
                },
            ),
        )
        s.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="openai/gpt-debug",
                dialect="openai_chat",
                input_tokens=0,
                output_tokens=0,
                provider_extras={
                    "_loom_call_status": "failed",
                    "_loom_failure_category": "upstream_http_5xx",
                    "_loom_failure_status_code": 500,
                    "_loom_usage_status": "missing",
                },
                request_params={
                    "status": "available",
                    "parameters": {"temperature": 0, "top_p": 0.5},
                },
                cost_usd=Decimal("0.000000"),
                rate_card_hash="failed-upstream",
                attempt=2,
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        debug = await ac.get(
            f"/api/v1/trials/{trial_id}/debug",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert debug.status_code == 200, debug.text
    body = debug.json()
    assert body["provider"]["llm_calls_count"] == 1
    assert body["provider"]["call_status_counts"] == {"failed": 1}
    assert body["provider"]["failed_llm_calls_count"] == 1
    assert body["provider"]["failure_category_counts"] == {
        "upstream_http_5xx": 1,
    }
    assert body["provider"]["llm_evidence_status"] == "calls_observed"
    assert body["provider"]["request_params_status_counts"] == {"available": 1}
    assert body["provider"]["request_params"][0]["parameters"] == {
        "temperature": 0,
        "top_p": 0.5,
    }
    assert body["failure"]["reason_code"] == "trial.gateway_error"


async def test_trial_debug_cross_team_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, _raw_a, _team_a, trial_ids_a = trials_setup
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=other_team, name=f"o-{other_team}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=other_team,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
        ) as ac:
            r = await ac.get(
                f"/api/v1/trials/{trial_ids_a[0]}/debug",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
            diagnosis = await ac.get(
                f"/api/v1/trials/{trial_ids_a[0]}/diagnosis",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
        assert r.status_code == 403
        assert diagnosis.status_code == 403
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            from loom.db.schema import Token as TokenModel

            s.execute(
                delete(TokenModel).where(
                    TokenModel.team_id == other_team,
                )
            )
            s.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id == other_team,
                )
            )
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()


async def test_trial_detail_exposes_projected_artifacts(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    app.state.settings.public_base_url = "https://yylx.world/dev"
    artifact_key = f"{team_id}/{trial_ids[0]}/main/result.txt"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(
                trajectory_index={
                    "trajectory_uri": (f"s3://trajectories/{team_id}/{trial_ids[0]}/events.jsonl"),
                    "atif_uri": f"s3://trajectories/{team_id}/{trial_ids[0]}/atif.json",
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": "artifacts",
                            "key": artifact_key,
                            "size": 5,
                            "share_status": "blocked",
                            "blocked_reason": "secret-like content detected",
                        }
                    ],
                }
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["artifacts"] == [
        {
            "step_name": "main",
            "key": artifact_key,
            "size": 5,
            "share_status": "blocked",
            "blocked_reason": "secret-like content detected",
            "download_url": body["artifacts"][0]["download_url"],
        }
    ]
    assert body["artifacts"][0]["download_url"] == (
        f"https://yylx.world/dev/api/v1/trials/{trial_ids[0]}/artifacts/download"
        f"?key={team_id}%2F{trial_ids[0]}%2Fmain%2Fresult.txt"
    )
    assert "localhost:9000" not in body["artifacts"][0]["download_url"]
    assert "X-Amz-Signature" not in body["artifacts"][0]["download_url"]


async def test_trial_detail_download_urls_do_not_call_presign_client(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    artifact_key = f"{team_id}/{trial_ids[0]}/main/result.txt"

    public_presign_client = MagicMock()
    app.state.minio_presign_client = public_presign_client

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(
                trajectory_index={
                    "trajectory_uri": (f"s3://trajectories/{team_id}/{trial_ids[0]}/events.jsonl"),
                    "atif_uri": f"s3://trajectories/{team_id}/{trial_ids[0]}/atif.json",
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": "artifacts",
                            "key": artifact_key,
                            "size": 5,
                        }
                    ],
                }
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    urls = [
        body["atif_url"],
        body["trajectory_url"],
        body["artifacts"][0]["download_url"],
    ]
    assert all(url.startswith("http://svc/api/v1/trials/") for url in urls)
    assert all("localhost:9000" not in url for url in urls)
    public_presign_client.generate_presigned_url.assert_not_called()


async def test_trajectory_download_proxies_via_service_without_presigned_redirect(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    app.state.minio_client = MagicMock()
    app.state.minio_client.get_object.return_value = {
        "Body": BytesIO(b'{"kind": "trial_start"}\n'),
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.content == b'{"kind": "trial_start"}\n'


async def test_atif_download_proxies_via_service_without_presigned_redirect(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    app.state.minio_client = MagicMock()
    app.state.minio_client.get_object.return_value = {
        "Body": BytesIO(b'{"version": "1.7"}'),
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.json() == {"version": "1.7"}


async def test_artifact_download_proxies_via_service_without_presigned_redirect(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    artifact_key = f"{team_id}/{trial_ids[0]}/main/result.txt"
    app.state.minio_client = MagicMock()
    app.state.minio_client.get_object.return_value = {
        "Body": BytesIO(b"hello artifact"),
    }

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": "artifacts",
                            "key": artifact_key,
                            "size": 14,
                        }
                    ],
                }
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}/artifacts/download",
            params={"key": artifact_key},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.content == b"hello artifact"


async def test_service_execution_artifacts_are_listed_and_downloadable(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    trial_id = trial_ids[0]
    artifact_id = uuid4()
    lease_id = uuid4()
    relative_path = "model-output.txt"
    artifact_key = (
        f"trials/{team_id}/{trial_id}/attempts/1/"
        f"bundles/{artifact_id}/files/{relative_path}"
    )
    canonical_bucket = "canonical-artifacts"
    app.state.minio_client = MagicMock()
    app.state.minio_client.get_object.return_value = {
        "Body": BytesIO(b"NEBIUS_SMOKE_OK\n"),
    }

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Artifact).values(
                id=artifact_id,
                artifact_type="loom.execution-runtime-evidence.v1",
                name="runtime_evidence",
                team_id=team_id,
                trial_id=trial_id,
                producer_kind=None,
                control_producer_kind="service_execution",
                control_producer_id=lease_id,
                content_hash="sha256:" + "1" * 64,
                storage={
                    "files": [
                        {
                            "relative_path": relative_path,
                            "size_bytes": 16,
                            "sha256": "sha256:" + "2" * 64,
                            "media_type": "text/plain",
                            "bucket": canonical_bucket,
                            "key": artifact_key,
                        }
                    ]
                },
                visibility="team",
                share_status="pending_scan",
                safety_state="verified_internal",
                access_class="team_runtime",
                provenance={"lease_id": str(lease_id), "generation": 1},
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        detail = await ac.get(
            f"/api/v1/trials/{trial_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        downloaded = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": artifact_key},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert detail.status_code == 200, detail.text
    assert detail.json()["artifacts"] == [
        {
            "key": artifact_key,
            "size": 16,
            "sha256": "sha256:" + "2" * 64,
            "media_type": "text/plain",
            "share_status": "pending_scan",
            "blocked_reason": None,
            "step_name": "runtime_evidence",
            "download_url": detail.json()["artifacts"][0]["download_url"],
        }
    ]
    assert downloaded.status_code == 200
    assert downloaded.content == b"NEBIUS_SMOKE_OK\n"
    app.state.minio_client.get_object.assert_called_once_with(
        Bucket=canonical_bucket,
        Key=artifact_key,
    )


async def test_artifact_download_cross_team_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    """A team-B caller cannot use a team-A artifact proxy URL."""

    app, _raw_a, team_a, trial_ids_a = trials_setup
    artifact_key = f"{team_a}/{trial_ids_a[0]}/main/result.txt"
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids_a[0])
            .values(
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": "artifacts",
                            "key": artifact_key,
                            "size": 14,
                            "share_status": "blocked",
                            "blocked_reason": "secret-like content detected",
                        }
                    ],
                },
            ),
        )
        s.execute(insert(Team).values(id=other_team, name=f"o-{other_team}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=other_team,
                issued_at=datetime.now(UTC),
            ),
        )
        s.commit()
    sync_engine.dispose()

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
            follow_redirects=False,
        ) as ac:
            r = await ac.get(
                f"/api/v1/trials/{trial_ids_a[0]}/artifacts/download",
                params={"key": artifact_key},
                headers={"Authorization": f"Bearer {other_raw}"},
            )
        assert r.status_code == 403
        assert "secret-like content detected" not in r.text
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            from loom.db.schema import Token as TokenModel

            s.execute(
                delete(TokenModel).where(
                    TokenModel.team_id == other_team,
                )
            )
            s.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id == other_team,
                )
            )
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()


async def test_trial_detail_not_found(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trial_detail_cross_team_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    """A team-A caller can't read team-B's trial detail."""
    app, _raw_a, _team_a, trial_ids_a = trials_setup
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=other_team, name=f"o-{other_team}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(other_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=other_team,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://svc",
        ) as ac:
            r = await ac.get(
                f"/api/v1/trials/{trial_ids_a[0]}",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
        assert r.status_code == 403
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            from loom.db.schema import Token as TokenModel

            s.execute(
                delete(TokenModel).where(
                    TokenModel.team_id == other_team,
                )
            )
            s.execute(
                delete(TeamQuota).where(
                    TeamQuota.team_id == other_team,
                )
            )
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()


async def test_trial_detail_carries_ready_flags(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    """Audit M1: ready flags so the SPA can skip rendering a download
    link that would 404. trajectory_ready iff started_at is not null;
    ATIF's legacy fallback requires a started and finished terminal trial."""
    app, raw, _team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    body = r.json()
    # Seeded trial: succeeded state, but no started_at/finished_at
    # were set on insert — so both flags should be False.
    assert body["atif_ready"] is False
    assert body["trajectory_ready"] is False


@pytest.mark.parametrize(
    ("state", "started", "indexed", "ready"),
    [
        ("failed", False, False, False),
        ("cancelled", False, False, False),
        ("failed", True, False, True),
        ("failed", False, True, True),
    ],
)
async def test_terminal_trial_without_execution_does_not_advertise_atif(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
    state: str,
    started: bool,
    indexed: bool,
    ready: bool,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    trial_id = trial_ids[0]
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as session:
            session.execute(update(Trial).where(Trial.id == trial_id).values(
                state=state,
                started_at=now - timedelta(seconds=1) if started else None,
                finished_at=now,
                attempt_count=int(started),
                failure_reason="task_image_build_failed" if state == "failed" else None,
                result=None,
                trajectory_index={
                    "atif_uri": f"s3://trajectories/{team_id}/{trial_id}/atif.json",
                } if indexed else {},
            ))
            session.commit()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc",
        ) as client:
            response = await client.get(
                f"/api/v1/trials/{trial_id}",
                headers={"Authorization": f"Bearer {raw}"},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["atif_ready"] is ready
        assert body["debug_evidence"]["evidence_refs"]["atif"]["ready"] is ready
        assert body["trajectory_ready"] is started
    finally:
        sync_engine.dispose()


async def test_filter_by_task_id(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
        )
        # Pull the task_id from one trial, then filter by it.
        task_id = r.json()["items"][0]["task_id"]
        r2 = await ac.get(
            f"/api/v1/trials?task_id={task_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r2.status_code == 200
    items = r2.json()["items"]
    assert all(it["task_id"] == task_id for it in items)


async def test_filter_by_batch_id(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    """Inject a batch + back-link the first two trials; the third
    keeps `batch_id = NULL`. Filtering by the batch id must return
    exactly the two linked trials."""
    app, raw, team_id, trial_ids = trials_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="qa-batch-161",
                task_filter={},
                trial_config={},
                created_by_token_prefix="abcdef12",
            )
        )
        s.execute(
            update(Trial).where(Trial.id.in_(trial_ids[:2])).values(batch_id=batch_id),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials?batch_id={batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    returned_ids = {it["id"] for it in items}
    assert returned_ids == {str(trial_ids[0]), str(trial_ids[1])}


async def test_filter_by_benchmark_agent_and_model(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    provider_connection_id = uuid4()
    wanted_model = {"provider": "openai-compatible", "name": "qwen2.5-coder"}
    other_model = {"provider": "openai-compatible", "name": "gpt-4o-mini"}

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(ProviderConnection).values(
                id=provider_connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="trial-filter-provider",
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                encrypted_api_key_ref="env:TRIAL_FILTER_PROVIDER_KEY",
                created_by="test",
                status="valid",
            )
        )
        s.execute(
            insert(Benchmark),
            [
                {
                    "id": "mbpp",
                    "display_name": "MBPP",
                    "upstream_kind": "fixture",
                    "upstream_locator": "fixture://mbpp",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.invalid/mit",
                    "splits": ["test"],
                },
                {
                    "id": "humaneval",
                    "display_name": "HumanEval",
                    "upstream_kind": "fixture",
                    "upstream_locator": "fixture://humaneval",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.invalid/mit",
                    "splits": ["test"],
                },
            ],
        )
        s.execute(
            insert(Task).values(
                id="mbpp/1",
                checksum="m" * 64,
                config={"task": {"id": "mbpp/1", "name": "mbpp"}},
                source="local",
                license="MIT",
                benchmark_id="mbpp",
            )
        )
        s.execute(
            insert(Task).values(
                id="humaneval/1",
                checksum="h" * 64,
                config={"task": {"id": "humaneval/1", "name": "humaneval"}},
                source="local",
                license="MIT",
                benchmark_id="humaneval",
            )
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(
                task_id="mbpp/1",
                config={"agent_name": "litellm", "agent_model": wanted_model},
                provider_connection_id=provider_connection_id,
                provider_model_id="qwen2.5-coder",
            )
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[1])
            .values(
                task_id="mbpp/1",
                config={"agent_name": "swe-agent", "agent_model": wanted_model},
                provider_connection_id=provider_connection_id,
                provider_model_id="qwen2.5-coder",
            )
        )
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[2])
            .values(
                task_id="humaneval/1",
                config={"agent_name": "litellm", "agent_model": other_model},
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?benchmark_id=mbpp&agent=litellm&model=qwen2.5-coder",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert [item["id"] for item in items] == [str(trial_ids[0])]

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            (
                "/api/v1/trials?benchmark_id=mbpp&agent_name=litellm"
                "&model_provider=openai-compatible&model_name=qwen2.5-coder"
                f"&provider_connection_id={provider_connection_id}"
                "&provider_model_id=qwen2.5-coder"
            ),
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert [item["id"] for item in items] == [str(trial_ids[0])]


async def test_running_trial_diagnosis_stays_pending_through_public_routes(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _, trial_ids = trials_setup
    trial_id = trial_ids[1]  # The persisted fixture's active trial.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
        headers={"Authorization": f"Bearer {raw}"},
    ) as client:
        debug = await client.get(f"/api/v1/trials/{trial_id}/debug")
        diagnosis = await client.get(f"/api/v1/trials/{trial_id}/diagnosis")
        detail = await client.get(f"/api/v1/trials/{trial_id}")
    assert debug.status_code == diagnosis.status_code == detail.status_code == 200
    assert debug.json()["failure"]["failure_class"] == "active"
    assert debug.json()["failure"]["rerunnable"] is False
    report = diagnosis.json()
    assert "still active (running)" in report["summary"]
    assert report["primary_cause"]["attribution"] == "pending"
    assert report["reason_clusters"] == []
    assert detail.json()["diagnosis"]["summary"] == report["summary"]
