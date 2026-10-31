"""Public benchmark aliases resolve to runnable physical profiles (#749)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.benchmark_profiles import resolve_benchmark_selectors
from loom.db.schema import Benchmark, BenchmarkAlias, Task, Trial
from loom_service.routes.benchmarks import get_benchmark, list_benchmarks
from loom_service.task_filter import resolve_task_filter_with_diagnostics

PUBLIC_ALIAS = "terminal-bench-2"
ACTIVE_PROFILE = "terminal-bench-2@tb2.1-r6"
HISTORICAL_PROFILE = "terminal-bench-2@tb2.0-91e10457"
ACTIVE_TASK_ID = f"{ACTIVE_PROFILE}/chess-best-move"
HISTORICAL_TASK_ID = f"{HISTORICAL_PROFILE}/legacy-chess-best-move"


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
async def session(postgres_url: str) -> AsyncIterator[AsyncSession]:
    sync_engine = create_engine(postgres_url)
    sync_session = sessionmaker(sync_engine)
    with sync_session() as sync:
        # This suite asserts the *exact* benchmark catalog, so isolate it from
        # any benchmark/task rows leaked by other tests sharing this DB (test
        # order is not fixed across shards). FK-safe wipe order: dependents of
        # benchmarks (aliases, then benchmark-referencing tasks and their
        # trials) before the benchmarks themselves.
        sync.execute(delete(Trial))
        sync.execute(delete(BenchmarkAlias))
        sync.execute(delete(Task))
        sync.execute(delete(Benchmark))
        sync.commit()
        for benchmark_id, execution_state in (
            (ACTIVE_PROFILE, "runnable"),
            (HISTORICAL_PROFILE, "historical"),
        ):
            sync.execute(
                insert(Benchmark).values(
                    id=benchmark_id,
                    display_name=benchmark_id,
                    upstream_kind="fixture",
                    upstream_locator="fixture://tb2",
                    upstream_revision="fixture",
                    license_spdx="MIT",
                    license_url="https://example.test/license",
                    splits=["test"],
                    execution_state=execution_state,
                ),
            )
        sync.execute(
            insert(Task).values(
                id=ACTIVE_TASK_ID,
                checksum="x" * 64,
                config=_valid_task_config(ACTIVE_TASK_ID),
                source="fixture://tb2/chess-best-move",
                license="MIT",
                benchmark_id=ACTIVE_PROFILE,
            ),
        )
        sync.execute(
            insert(Task).values(
                id=HISTORICAL_TASK_ID,
                checksum="x" * 64,
                config=_valid_task_config(HISTORICAL_TASK_ID),
                source="fixture://tb2/legacy-chess-best-move",
                license="MIT",
                benchmark_id=HISTORICAL_PROFILE,
            ),
        )
        sync.commit()
    sync_engine.dispose()

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as async_session:
            yield async_session
    finally:
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        with sessionmaker(sync_engine)() as sync:
            sync.execute(
                delete(Task).where(Task.id.in_([ACTIVE_TASK_ID, HISTORICAL_TASK_ID])),
            )
            sync.execute(
                delete(BenchmarkAlias).where(BenchmarkAlias.alias == PUBLIC_ALIAS),
            )
            sync.execute(
                delete(Benchmark).where(
                    Benchmark.id.in_([ACTIVE_PROFILE, HISTORICAL_PROFILE]),
                ),
            )
            sync.commit()
        sync_engine.dispose()


async def seed_alias(
    session: AsyncSession,
    alias: str,
    benchmark_id: str,
) -> None:
    session.add(BenchmarkAlias(alias=alias, benchmark_id=benchmark_id))
    await session.commit()


async def test_alias_selects_active_physical_profile(
    session: AsyncSession,
) -> None:
    await seed_alias(session, PUBLIC_ALIAS, ACTIVE_PROFILE)

    selectors = await resolve_benchmark_selectors(
        session,
        [PUBLIC_ALIAS],
        require_runnable=True,
    )
    result = await resolve_task_filter_with_diagnostics(
        session,
        {"benchmark_id": PUBLIC_ALIAS},
    )

    assert selectors.physical_ids == (ACTIVE_PROFILE,)
    assert result.task_ids == [ACTIVE_TASK_ID]
    assert result.benchmark_selection_provenance[0]["resolved_profile"] == ACTIVE_PROFILE


async def test_archived_profile_is_not_a_new_submission(
    session: AsyncSession,
) -> None:
    with pytest.raises(HTTPException, match="benchmark_retired") as exc_info:
        await resolve_task_filter_with_diagnostics(
            session,
            {"benchmark_id": HISTORICAL_PROFILE},
        )

    assert exc_info.value.status_code == 409


async def test_explicit_historical_task_id_is_not_a_new_submission(
    session: AsyncSession,
) -> None:
    with pytest.raises(HTTPException) as exc_info:
        await resolve_task_filter_with_diagnostics(
            session,
            {"subset_kind": "explicit", "task_ids": [HISTORICAL_TASK_ID]},
        )

    assert exc_info.value.status_code == 409


async def test_unscoped_filter_rejects_historical_candidate_for_new_submission(
    session: AsyncSession,
) -> None:
    with pytest.raises(HTTPException) as exc_info:
        await resolve_task_filter_with_diagnostics(session, {})

    assert exc_info.value.status_code == 409


async def test_catalog_hides_historical_profiles_but_direct_read_remains_available(
    session: AsyncSession,
) -> None:
    await seed_alias(session, PUBLIC_ALIAS, ACTIVE_PROFILE)

    catalog = await list_benchmarks((session, None), include_empty=True)
    historical = await get_benchmark(HISTORICAL_PROFILE, (session, None))

    assert [item["id"] for item in catalog["items"]] == [PUBLIC_ALIAS]
    assert catalog["items"][0]["physical_profile"] == ACTIVE_PROFILE
    assert historical["physical_profile"] == HISTORICAL_PROFILE
    assert historical["execution_state"] == "historical"


@pytest.mark.parametrize("task_filter", [
    {"benchmark_id": PUBLIC_ALIAS},
    {"benchmark_id": ACTIVE_PROFILE},
    {"subset_kind": "explicit", "task_ids": [ACTIVE_TASK_ID]},
])
async def test_pending_profile_cannot_be_selected_through_any_selector(
    session: AsyncSession, task_filter: dict[str, object],
) -> None:
    await seed_alias(session, PUBLIC_ALIAS, ACTIVE_PROFILE)
    profile = await session.get(Benchmark, ACTIVE_PROFILE)
    profile.execution_state = "pending"
    await session.commit()

    with pytest.raises(HTTPException) as failure:
        await resolve_task_filter_with_diagnostics(session, task_filter)

    assert failure.value.status_code == 409
    assert failure.value.detail["reason"] == "benchmark_not_runnable"
    assert failure.value.detail["benchmark_profile"] == ACTIVE_PROFILE
    frozen = await resolve_benchmark_selectors(session, [PUBLIC_ALIAS], require_runnable=False)
    assert frozen.physical_ids == (ACTIVE_PROFILE,)
