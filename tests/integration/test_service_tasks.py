"""Tasks browse (Plan 18 Task 6)."""

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
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, Task, TaskSet, Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def tasks_setup(
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
        s.execute(
            insert(Benchmark).values(
                execution_state="runnable",
                id="humaneval",
                display_name="HumanEval",
                upstream_kind="huggingface",
                upstream_locator="openai_humaneval",
                upstream_revision="",
                license_spdx="MIT",
                license_url="https://example/license",
                splits=["test"],
            )
        )

        # Realistic config so the list route can surface name +
        # description + agent + verifier + step_count from the JSON.
        def _config(name: str, desc: str) -> dict:
            return {
                "schema_version": "1",
                "task": {"id": name, "name": name, "description": desc},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}, {"name": "checkpoint"}],
            }

        for i, (tid, lic, bench, name, desc) in enumerate(
            (
                (
                    "humaneval/HumanEval/0",
                    "MIT",
                    "humaneval",
                    "Two-sum",
                    "Return indices of two numbers that sum to a target.",
                ),
                (
                    "humaneval/HumanEval/1",
                    "MIT",
                    "humaneval",
                    "Balanced parens",
                    "Validate a balanced-parens expression.",
                ),
                (
                    "local/hand-written",
                    "Apache-2.0",
                    None,
                    "Hello world",
                    "Smallest possible Loom task.",
                ),
            )
        ):
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="x" * 64,
                    config=_config(name, desc),
                    source="local",
                    license=lic,
                    benchmark_id=bench,
                )
            )
            del i
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TaskSet))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_tasks(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 3
    # Sorted by id ascending — HumanEval/0 < HumanEval/1 < local/.
    assert items[0]["id"] == "humaneval/HumanEval/0"


async def test_private_taskset_source_is_visible_only_to_its_owning_team(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Catalog metadata stays browseable while foreign storage locations do not."""
    app, owning_raw = tasks_setup
    foreign_team_id = uuid4()
    foreign_raw = f"loom_team_{uuid4().hex}"
    task_set_id = f"ts/{foreign_team_id}/private-source"
    task_id = f"{task_set_id}/tasks/row-1"
    private_source = "s3://loom-artifacts/tasksets/user/foreign/private-source/task.toml"
    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.begin() as session:
            session.execute(
                insert(Team).values(id=foreign_team_id, name=f"t-{foreign_team_id}"),
            )
            session.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(foreign_raw.encode()).digest(),
                    type="team",
                    scopes=["read:own"],
                    team_id=foreign_team_id,
                    issued_at=datetime.now(UTC),
                ),
            )
            session.execute(
                insert(TaskSet).values(
                    id=task_set_id,
                    owning_team_id=foreign_team_id,
                    slug="private-source",
                    display_name="Private source TaskSet",
                    status="ready",
                    intents=["trajectory_generation"],
                    evaluation_ready=False,
                    task_count=1,
                    manifest_blob_uri="s3://loom-artifacts/private-source/manifest.yaml",
                ),
            )
            session.execute(
                insert(Task).values(
                    id=task_id,
                    task_set_id=task_set_id,
                    checksum="p" * 64,
                    config=_valid_task_config(task_id),
                    source=private_source,
                    license="MIT",
                ),
            )
    finally:
        sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        foreign_catalog = await client.get(
            "/api/v1/tasks",
            headers={"Authorization": f"Bearer {owning_raw}"},
        )
        foreign_detail = await client.get(
            f"/api/v1/tasks/{task_id}",
            headers={"Authorization": f"Bearer {owning_raw}"},
        )
        own_catalog = await client.get(
            "/api/v1/tasks",
            headers={"Authorization": f"Bearer {foreign_raw}"},
        )

    assert foreign_catalog.status_code == 200, foreign_catalog.text
    foreign_item = next(item for item in foreign_catalog.json()["items"] if item["id"] == task_id)
    assert foreign_item["source"] is None
    assert foreign_detail.status_code == 200, foreign_detail.text
    assert foreign_detail.json()["source"] is None
    assert own_catalog.status_code == 200, own_catalog.text
    own_item = next(item for item in own_catalog.json()["items"] if item["id"] == task_id)
    assert own_item["source"] == private_source
    assert (
        next(item for item in own_catalog.json()["items"] if item["id"] == "local/hand-written")[
            "source"
        ]
        == "local"
    )


async def test_list_surfaces_name_description_agent_verifier_step_count(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: each list row carries the fields the SPA browse view
    needs to decide whether to submit a trial — name, description,
    agent, verifier, step_count — extracted from config JSON."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?benchmark_id=humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    first = next(i for i in items if i["id"] == "humaneval/HumanEval/0")
    assert first["name"] == "Two-sum"
    assert first["description"].startswith("Return indices")
    assert first["agent_name"] == "oracle"
    assert first["verifier_name"] == "pytest"
    assert first["step_count"] == 2


async def test_q_substring_matches_task_id(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: substring search on task id replaces the dropped
    license filter — users locate tasks by typing a fragment."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?q=hand",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == "local/hand-written"


async def test_q_is_case_insensitive(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?q=HUMANEVAL",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 2


async def test_license_query_param_no_longer_filters(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: the legacy `license=` filter is gone. FastAPI silently
    ignores unknown query params, so the request still 200s — assert
    every row comes back rather than just MIT rows."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?license=MIT",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    # All three tasks come back — the license param is now a no-op.
    assert len(r.json()["items"]) == 3


async def test_filter_by_benchmark_id(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?benchmark_id=humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 2
    assert all(it["benchmark_id"] == "humaneval" for it in items)


async def test_pagination(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/tasks?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] is not None

        r2 = await ac.get(
            f"/api/v1/tasks?limit=2&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    j2 = r2.json()
    assert len(j2["items"]) == 1
    assert j2["next_cursor"] is None


async def test_offset_is_rejected_not_silently_ignored(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    # #917: paging by offset must fail fast with guidance, not silently return
    # the first page (which loops a bulk consumer forever).
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            "/api/v1/tasks?offset=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400
    assert "cursor" in r.json()["detail"]


async def test_get_task_detail(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks/humaneval/HumanEval/0",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "humaneval/HumanEval/0"
    assert body["license"] == "MIT"
    assert body["benchmark_id"] == "humaneval"
    assert "config" in body


async def test_get_task_not_found(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks/no/such/task",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_unauthenticated_401(tasks_setup: tuple[FastAPI, str]) -> None:
    app, _raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/tasks")
    assert r.status_code == 401


# ──────────────────────────────────────────────────────────────────────
# POST /tasks/count — closes #28 (zero-task batch with tag_filters)
# ──────────────────────────────────────────────────────────────────────


async def test_count_empty_filter_returns_all(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """No filter ⇒ count = every task in the catalog."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {}},
        )
    assert r.status_code == 200
    assert r.json() == {"count": 3}


async def test_count_benchmark_id_filter(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "humaneval"}},
        )
    assert r.status_code == 200
    assert r.json() == {"count": 2}


async def test_count_owned_task_set_id_filter(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
            ),
        ).scalar_one()
        task_set_id = f"ts/{team_id}/count-taskset"
        task_id = f"{task_set_id}/tasks/row-1"
        s.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_id,
                slug="count-taskset",
                display_name="Count TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{team_id}/count-taskset/manifest.yaml"
                ),
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_id}/count-taskset/tasks/row-1/",
                license="MIT",
                task_set_id=task_set_id,
                benchmark_id=None,
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"task_set_id": task_set_id}},
        )
    assert r.status_code == 200
    assert r.json() == {"count": 1}


async def test_count_rejects_cross_team_explicit_task_set_task_id(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = tasks_setup
    team_b = uuid4()
    task_set_id = f"ts/{team_b}/private-count-taskset"
    task_id = f"{task_set_id}/tasks/row-1"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_b, name=f"t-{team_b}"))
        s.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=team_b,
                slug="private-count-taskset",
                display_name="Private Count TaskSet",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{team_b}/private-count-taskset/manifest.yaml"
                ),
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://bucket/tasksets/user/{team_b}/private-count-taskset/tasks/row-1/",
                license="MIT",
                task_set_id=task_set_id,
                benchmark_id=None,
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_filter": {
                    "task_ids": [task_id],
                    "subset_kind": "explicit",
                }
            },
        )
    assert r.status_code == 404
    assert r.json()["detail"] == "task not found"


async def test_count_excludes_unsupported_ui_benchmarks(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark).values(
                execution_state="runnable",
                id="osworld",
                display_name="OSWorld",
                upstream_kind="git",
                upstream_locator="https://github.com/xlang-ai/OSWorld.git",
                upstream_revision="main",
                license_spdx="Apache-2.0",
                license_url="https://example/osworld",
                splits=["test"],
            )
        )
        s.execute(
            insert(Task).values(
                id="osworld/task-001",
                checksum="o" * 64,
                config=_valid_task_config("osworld/task-001"),
                source="s3://bucket/osworld/task-001/",
                license="Apache-2.0",
                benchmark_id="osworld",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        osworld_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "osworld"}},
        )
        all_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {}},
        )

    assert osworld_count.status_code == 409
    assert osworld_count.json()["detail"]["reason"] == "unsupported_runtime"
    assert all_count.status_code == 200
    assert all_count.json() == {"count": 3}


async def test_count_excludes_non_v1_builtin_benchmarks(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark).values(
                execution_state="runnable",
                id="browsecomp",
                display_name="BrowseComp",
                upstream_kind="huggingface",
                upstream_locator="upstream/browsecomp",
                upstream_revision="",
                license_spdx="CC-BY-4.0",
                license_url="https://example/browsecomp",
                splits=["test"],
            )
        )
        s.execute(
            insert(Task).values(
                id="browsecomp/task-001",
                checksum="b" * 64,
                config=_valid_task_config("browsecomp/task-001"),
                source="s3://bucket/browsecomp/task-001/",
                license="CC-BY-4.0",
                benchmark_id="browsecomp",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        browsecomp_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "browsecomp"}},
        )
        all_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {}},
        )

    assert browsecomp_count.status_code == 409
    assert browsecomp_count.json()["detail"]["reason"] == "not_v1_supported"
    assert all_count.status_code == 200
    assert all_count.json() == {"count": 3}


async def test_count_ignores_team_license_allowlist(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Legacy team license allowlists must not reduce New Batch counts."""
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
            ),
        ).scalar_one()
        s.execute(
            insert(TeamQuota).values(
                team_id=team_id,
                license_allowlist=["MIT"],
            )
        )
        s.execute(
            insert(Task).values(
                id="humaneval/noncommercial",
                checksum="y" * 64,
                config={
                    "schema_version": "1",
                    "task": {
                        "id": "humaneval/noncommercial",
                        "name": "NC task",
                    },
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
                source="local",
                license="CC-BY-NC-4.0",
                benchmark_id="humaneval",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "humaneval"}},
        )

    assert r.status_code == 200
    assert r.json() == {"count": 3}


async def test_count_ignores_invalid_stored_task_configs(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """New Batch uses this count to decide whether a benchmark is
    launchable, so placeholder rows with empty config must not count."""
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="humaneval/unpublished-placeholder",
                checksum="y" * 64,
                config={},
                source=None,
                license="MIT",
                benchmark_id="humaneval",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        benchmark_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "humaneval"}},
        )
        all_count = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {}},
        )

    assert benchmark_count.status_code == 200
    assert benchmark_count.json() == {"count": 2}
    assert all_count.status_code == 200
    assert all_count.json() == {"count": 3}


async def test_count_no_match_returns_zero_not_error(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Issue #28 acceptance: count returns 0 (not 400) on a filter
    that materializes to zero. The SPA needs to distinguish "no
    rows matched" from "invalid filter"."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"benchmark_id": "no-such-benchmark"}},
        )
    assert r.status_code == 200
    assert r.json() == {"count": 0}


async def test_count_tag_filter_narrows_to_zero(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """The user-facing #28 scenario: benchmark_id has matches, but
    tag_filters narrow to zero. count returns 0 — the SPA gates submit
    on this, preventing the empty-batch confusion."""
    app, raw = tasks_setup
    # Annotate one humaneval task with a tag; query for a value
    # that doesn't match.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        from sqlalchemy import update as sa_update

        s.execute(
            sa_update(Task)
            .where(Task.id == "humaneval/HumanEval/0")
            .values(tags={"verified": "true"}),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_filter": {
                    "benchmark_id": "humaneval",
                    "tag_filters": {"verified": ["unverified-value"]},
                }
            },
        )
    assert r.status_code == 200
    assert r.json() == {"count": 0}


async def test_count_tag_filter_narrows_to_some(
    tasks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Same setup as above but the tag value matches — proves the
    filter pipeline materializes the right count, not just zero."""
    app, raw = tasks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        from sqlalchemy import update as sa_update

        s.execute(
            sa_update(Task)
            .where(Task.id == "humaneval/HumanEval/0")
            .values(tags={"verified": "true"}),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_filter": {
                    "benchmark_id": "humaneval",
                    "tag_filters": {"verified": ["true"]},
                }
            },
        )
    assert r.status_code == 200
    assert r.json() == {"count": 1}


async def test_count_rejects_unknown_filter_key(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Same validation contract as `POST /batches`: typos in the
    task_filter shape 400 with a clear message."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"liscense": "MIT"}},
        )
    assert r.status_code == 400
    assert "unknown task_filter keys" in r.json()["detail"]


async def test_count_subset_kind_first_n_truncates(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """The route uses the same materialization as `POST /batches`,
    so subset_kind='first_n' trims to the requested size."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tasks/count",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_filter": {"subset_kind": "first_n", "n": 1}},
        )
    assert r.status_code == 200
    assert r.json() == {"count": 1}


async def test_count_unauthenticated(tasks_setup: tuple[FastAPI, str]) -> None:
    app, _raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post("/api/v1/tasks/count", json={"task_filter": {}})
    assert r.status_code == 401
