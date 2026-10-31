"""GET /api/v1/benchmarks/{id}/tags — distinct-tag discovery for the
SPA's tag-filter UI (PR-2 of the series/tags catalog redesign).

Walks `jsonb_each_text(tags)` for every task in the benchmark and
returns key → sorted distinct values."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, Task, Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def tags_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
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
        engine, expire_on_commit=False,
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        # Seed a small AIME-style benchmark: 4 tasks across years +
        # exams so the discovery endpoint has multiple keys.
        s.execute(insert(Benchmark).values(
            execution_state="runnable",
            id="aime-22",
            display_name="AIME (AIMO validation)",
            upstream_kind="huggingface",
            upstream_locator="AI-MO/aimo-validation-aime",
            upstream_revision="main",
            license_spdx="proprietary-MAA", license_url="",
            splits=["train"], series="aime",
        ))
        for tags, task_id in (
            ({"year": "2024", "exam": "I", "problem": "1"},
                "aime-22/2024-I/1"),
            ({"year": "2024", "exam": "II", "problem": "1"},
                "aime-22/2024-II/1"),
            ({"year": "2023", "exam": "I", "problem": "1"},
                "aime-22/2023-I/1"),
            ({"year": "2023", "exam": "II", "problem": "5"},
                "aime-22/2023-II/5"),
        ):
            s.execute(insert(Task).values(
                id=task_id,
                checksum="0" * 64,
                config={},
                source=f"hf://PRHW/loom-benchmark-aime/{task_id}/",
                license="proprietary-MAA",
                benchmark_id="aime-22",
                tags=tags,
            ))
        # An untagged benchmark so the empty-tags path is exercised.
        s.execute(insert(Benchmark).values(
            execution_state="runnable",
            id="humaneval", display_name="HumanEval",
            upstream_kind="huggingface",
            upstream_locator="openai/openai_humaneval",
            upstream_revision="main",
            license_spdx="MIT", license_url="",
            splits=["test"], series=None,
        ))
        s.execute(insert(Task).values(
            id="humaneval/HumanEval/0",
            checksum="0" * 64,
            config={},
            source="hf://PRHW/loom-benchmark-humaneval/HumanEval_0/",
            license="MIT",
            benchmark_id="humaneval",
            tags={},
        ))
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_tags_endpoint_returns_keys_and_distinct_values(
    tags_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tags_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/aime-22/tags",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    by_key = {it["key"]: it["values"] for it in items}
    assert by_key["year"] == ["2023", "2024"]
    assert by_key["exam"] == ["I", "II"]
    assert by_key["problem"] == ["1", "5"]


async def test_tags_endpoint_returns_empty_list_when_no_tags(
    tags_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tags_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/humaneval/tags",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"items": []}


async def test_tags_endpoint_404_on_unknown_benchmark(
    tags_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tags_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/no-such/tags",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_benchmarks_listing_surfaces_series(
    tags_setup: tuple[FastAPI, str],
) -> None:
    """SPA reads `series` to group rows in the dropdown — confirm it
    propagates from DB → manifest → SPA-facing response. This fixture seeds
    placeholder task configs, so use include_empty to inspect metadata rows."""
    app, raw = tags_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    by_id = {b["id"]: b for b in items}
    assert by_id["aime-22"]["series"] == "aime"
    assert by_id["humaneval"]["series"] is None


async def test_unauthenticated_401(
    tags_setup: tuple[FastAPI, str],
) -> None:
    app, _ = tags_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/benchmarks/aime-22/tags")
    assert r.status_code == 401


@pytest.mark.parametrize("ids,status", [
    (["aime-22", "humaneval", "aime-22"], 200),
    (["humaneval"], 200),
    (["aime-22", "missing"], 404),
])
async def test_aggregate_tag_discovery(tags_setup, ids, status):
    app, raw = tags_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post(
            "/api/v1/benchmarks/discover", json={"benchmark_ids": ids},
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert response.status_code == status
    if status == 200:
        assert response.json() == {"items": [
            {"key": "exam", "values": ["I", "II"]},
            {"key": "problem", "values": ["1", "5"]},
            {"key": "year", "values": ["2023", "2024"]},
        ] if "aime-22" in ids else []}


async def test_aggregate_tag_discovery_requires_auth(tags_setup):
    app, _ = tags_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as client:
        response = await client.post("/api/v1/benchmarks/discover", json={"benchmark_ids": ["aime-22"]})
    assert response.status_code == 401
