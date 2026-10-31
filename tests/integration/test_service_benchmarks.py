"""Benchmarks browse (Plan 18 Task 7)."""

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

from loom.db.schema import Benchmark, Task, Team, TeamQuota, Token
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
async def benchmarks_setup(
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
        # This fixture seeds fixed benchmark ids; clear any left over from a
        # prior test sharing the per-shard database so the insert is resilient
        # (teardown wipes them symmetrically).
        s.execute(delete(Task))
        s.execute(delete(Benchmark))
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        for bid, dn, lic in (
            ("aime", "AIME", "proprietary-MAA"),
            ("gaia", "GAIA", "CC-BY-SA-4.0"),
            ("humaneval", "HumanEval", "MIT"),
            ("mbpp", "MBPP", "CC-BY-4.0"),
            ("osworld", "OSWorld", "Apache-2.0"),
        ):
            s.execute(
                insert(Benchmark).values(
                    execution_state="runnable",
                    id=bid,
                    display_name=dn,
                    upstream_kind="huggingface",
                    upstream_locator=f"upstream/{bid}",
                    upstream_revision="",
                    license_spdx=lic,
                    license_url=f"https://example/{bid}",
                    splits=["test"],
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
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_benchmarks(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    """The default listing hides empty benchmarks (Plan 28 fix). The
    fixture seeds 3 benchmarks with no tasks; default response is empty.
    `?include_empty=true` returns the full set."""
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r_default.status_code == 200
    assert r_default.json()["items"] == []
    assert r_all.status_code == 200
    items = r_all.json()["items"]
    assert len(items) == 5
    # Sorted by display_name.
    assert [it["display_name"] for it in items] == [
        "AIME",
        "GAIA",
        "HumanEval",
        "MBPP",
        "OSWorld",
    ]
    # Every row reports task_count=0 (no tasks linked to these
    # benchmark ids in the fixture).
    assert all(it["task_count"] == 0 for it in items)


async def test_list_benchmarks_shows_imported(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Once a task is registered for a benchmark, the default listing
    surfaces that benchmark with task_count > 0."""
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="humaneval/HumanEval/0",
                benchmark_id="humaneval",
                config=_valid_task_config("humaneval/HumanEval/0"),
                checksum="0" * 64,
                source="s3://bucket/prefix/",
                license="MIT",
                registered_at=datetime.now(UTC),
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
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == "humaneval"
    assert items[0]["task_count"] == 1
    # Cleanup the inserted task so the next test's empty-default
    # invariant still holds.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(delete(Task))
        s.commit()
    sync_engine.dispose()


async def test_list_benchmarks_keeps_unsupported_ui_benchmark_visible_but_disabled(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """OSWorld/WebArena are visible catalog rows, but not currently
    launchable until Loom has a real UI benchmark runtime contract."""
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="osworld/task-001",
                benchmark_id="osworld",
                config=_valid_task_config("osworld/task-001"),
                checksum="9" * 64,
                source="s3://bucket/osworld/task-001/",
                license="Apache-2.0",
                registered_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    assert "osworld" not in {item["id"] for item in r_default.json()["items"]}

    assert r_all.status_code == 200
    osworld = {
        item["id"]: item for item in r_all.json()["items"]
    }["osworld"]
    assert (
        osworld
        | {
            "task_count": 0,
            "raw_task_count": 1,
            "valid_task_config_count": 1,
            "license_allowed_task_count": 0,
            "readiness_state": "blocked",
            "readiness_label": "Not supported yet",
            "selectable": False,
            "blocker_reason": "unsupported_runtime",
        }
        == osworld
    )
    assert "UI benchmark runtime" in osworld["readiness_message"]


async def test_list_benchmarks_keeps_deferred_gaia_visible_but_disabled(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    """GAIA remains visible for roadmap transparency, but is outside the
    current supported catalog until the gated dataset is published."""
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    assert "gaia" not in {item["id"] for item in r_default.json()["items"]}

    assert r_all.status_code == 200
    gaia = {item["id"]: item for item in r_all.json()["items"]}["gaia"]
    assert (
        gaia
        | {
            "task_count": 0,
            "raw_task_count": 0,
            "valid_task_config_count": 0,
            "license_allowed_task_count": 0,
            "readiness_state": "blocked",
            "readiness_label": "Deferred",
            "selectable": False,
            "blocker_reason": "deferred_support",
        }
        == gaia
    )
    assert "GAIA-authorized" in gaia["readiness_message"]


async def test_list_benchmarks_keeps_non_v1_builtin_visible_but_disabled(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Built-in benchmarks outside the v1.0 allowlist stay visible for
    transparency but are not selectable, even when they have valid tasks."""
    app, raw = benchmarks_setup
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
                benchmark_id="browsecomp",
                config=_valid_task_config("browsecomp/task-001"),
                checksum="b" * 64,
                source="s3://bucket/browsecomp/task-001/",
                license="CC-BY-4.0",
                registered_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    assert "browsecomp" not in {item["id"] for item in r_default.json()["items"]}

    assert r_all.status_code == 200
    browsecomp = {
        item["id"]: item for item in r_all.json()["items"]
    }["browsecomp"]
    assert (
        browsecomp
        | {
            "task_count": 0,
            "raw_task_count": 1,
            "valid_task_config_count": 1,
            "license_allowed_task_count": 0,
            "readiness_state": "blocked",
            "readiness_label": "Not in v1.0",
            "selectable": False,
            "blocker_reason": "not_v1_supported",
        }
        == browsecomp
    )
    assert "v1.0 benchmark support set" in browsecomp["readiness_message"]


async def test_list_benchmarks_hides_sample_tasks_local_benchmark(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """The temporary sample-tasks bridge must not appear in the v1.0
    production benchmark catalog, even if stale local rows still exist."""
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark).values(
                    execution_state="runnable",
                id="sample-tasks",
                display_name="Sample Tasks",
                upstream_kind="local-folder",
                upstream_locator="fixture://sample-tasks",
                upstream_revision="",
                license_spdx="proprietary",
                license_url="",
                splits=["test"],
            )
        )
        s.execute(
            insert(Task).values(
                id="sample-tasks/task-001",
                benchmark_id="sample-tasks",
                config=_valid_task_config("sample-tasks/task-001"),
                checksum="s" * 64,
                source="fixture://sample-tasks/task-001/",
                license="proprietary",
                registered_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_detail = await ac.get(
            "/api/v1/benchmarks/sample-tasks",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    assert "sample-tasks" not in {item["id"] for item in r_default.json()["items"]}
    assert r_all.status_code == 200
    assert "sample-tasks" not in {item["id"] for item in r_all.json()["items"]}
    assert r_detail.status_code == 404


async def test_list_benchmarks_counts_only_runnable_task_configs(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Placeholder task rows with empty config are not runnable and
    must not make New Batch advertise a benchmark as launchable."""
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="humaneval/HumanEval/0",
                benchmark_id="humaneval",
                config=_valid_task_config("humaneval/HumanEval/0"),
                checksum="0" * 64,
                source="s3://bucket/prefix/",
                license="MIT",
                registered_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Task).values(
                id="mbpp/unpublished-placeholder",
                benchmark_id="mbpp",
                config={},
                checksum="1" * 64,
                source=None,
                license="CC-BY-4.0",
                registered_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    assert [item["id"] for item in r_default.json()["items"]] == [
        "humaneval",
    ]
    all_items = {item["id"]: item for item in r_all.json()["items"]}
    assert all_items["humaneval"]["task_count"] == 1
    assert all_items["mbpp"]["task_count"] == 0


async def test_list_benchmarks_treats_task_licenses_as_informational(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """Benchmark readiness must not exclude tasks by source license."""
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="aime/noncommercial-0",
                benchmark_id="aime",
                config=_valid_task_config("aime/noncommercial-0"),
                checksum="2" * 64,
                source="s3://bucket/aime/",
                license="CC-BY-NC-4.0",
                registered_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r_default = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
        r_all = await ac.get(
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r_default.status_code == 200
    default_items = {item["id"]: item for item in r_default.json()["items"]}
    assert default_items["aime"]["task_count"] == 1

    assert r_all.status_code == 200
    items = {item["id"]: item for item in r_all.json()["items"]}
    aime = items["aime"]
    assert (
        aime
        | {
            "task_count": 1,
            "raw_task_count": 1,
            "valid_task_config_count": 1,
            "license_allowed_task_count": 1,
            "license_blocked_task_count": 0,
            "blocked_licenses": [],
            "readiness_state": "runnable",
            "readiness_label": "Ready",
            "readiness_message": "1 runnable task is registered.",
            "selectable": True,
            "blocker_reason": None,
        }
        == aime
    )


async def test_list_benchmarks_surfaces_readiness_diagnostics(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """The SPA picker needs API-driven readiness details, not a
    benchmark-name allowlist. Empty metadata rows, legacy placeholder
    rows, and runnable rows must be distinguishable from one response.
    """
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="humaneval/HumanEval/0",
                benchmark_id="humaneval",
                config=_valid_task_config("humaneval/HumanEval/0"),
                checksum="0" * 64,
                source="s3://bucket/prefix/",
                license="MIT",
                registered_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Task).values(
                id="mbpp/legacy-placeholder",
                benchmark_id="mbpp",
                config={},
                checksum="1" * 64,
                source="s3://bucket/legacy/",
                license="CC-BY-4.0",
                registered_at=datetime.now(UTC),
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
            "/api/v1/benchmarks?include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    items = {item["id"]: item for item in r.json()["items"]}

    assert (
        items["humaneval"]
        | {
            "task_count": 1,
            "raw_task_count": 1,
            "valid_task_config_count": 1,
            "invalid_task_config_count": 0,
            "readiness_state": "runnable",
            "readiness_label": "Ready",
            "selectable": True,
            "blocker_reason": None,
        }
        == items["humaneval"]
    )

    assert (
        items["aime"]
        | {
            "task_count": 0,
            "raw_task_count": 0,
            "valid_task_config_count": 0,
            "invalid_task_config_count": 0,
            "readiness_state": "blocked",
            "readiness_label": "Needs publish",
            "selectable": False,
            "blocker_reason": "manifest_missing",
        }
        == items["aime"]
    )
    assert "Publish" in items["aime"]["readiness_message"]

    assert (
        items["mbpp"]
        | {
            "task_count": 0,
            "raw_task_count": 1,
            "valid_task_config_count": 0,
            "invalid_task_config_count": 1,
            "readiness_state": "blocked",
            "readiness_label": "Needs republish",
            "selectable": False,
            "blocker_reason": "manifest_legacy_missing_task_config",
        }
        == items["mbpp"]
    )
    assert "valid TaskConfig" in items["mbpp"]["readiness_message"]


async def test_pagination(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/benchmarks?limit=2&include_empty=true",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] == "GAIA"
        r2 = await ac.get(
            f"/api/v1/benchmarks?limit=2&include_empty=true&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j2 = r2.json()
        r3 = await ac.get(
            f"/api/v1/benchmarks?limit=2&include_empty=true&cursor={j2['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert len(j2["items"]) == 2
    assert [item["id"] for item in j2["items"]] == ["humaneval", "mbpp"]
    assert j2["next_cursor"] == "MBPP"
    j3 = r3.json()
    assert [item["id"] for item in j3["items"]] == ["osworld"]
    assert j3["next_cursor"] is None


async def test_get_benchmark_detail(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "humaneval"
    assert body["license_spdx"] == "MIT"
    assert body["upstream_kind"] == "huggingface"
    assert body["task_count"] == 0
    assert "imported_at" in body


async def test_get_benchmark_not_found(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/no-such-bench",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_unauthenticated_401(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, _raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/benchmarks")
    assert r.status_code == 401


async def test_explicit_deferred_selection_matches_catalog_reason(
    benchmarks_setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = benchmarks_setup
    sync_engine = create_engine(postgres_url)
    with sessionmaker(sync_engine)() as session:
        session.execute(insert(Task).values(
            id="gaia/task-001", benchmark_id="gaia",
            config=_valid_task_config("gaia/task-001"),
            checksum="d" * 64, source=None,
        ))
        session.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        headers = {"Authorization": f"Bearer {raw}"}
        catalog = await client.get("/api/v1/benchmarks/gaia", headers=headers)
        assert catalog.status_code == 200
        assert catalog.json()["blocker_reason"] == "deferred_support"
        for task_filter in (
            {"benchmark_id": "gaia"},
            {"task_ids": ["gaia/task-001"], "subset_kind": "explicit"},
        ):
            response = await client.post(
                "/api/v1/tasks/count", json={"task_filter": task_filter}, headers=headers,
            )
            assert response.status_code == 409
            assert response.json()["detail"]["reason"] == "deferred_support"
