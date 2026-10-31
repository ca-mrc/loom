"""task_filter.benchmark_ids + task_filter.tag_filters resolution.

Exercises the new multi-benchmark + tag filter knobs PR-2 added to
the shared resolver. Doesn't go through the full POST
/batches path (which also runs subset_kind, dedup, validation, etc.)
— here we call `resolve_task_filter` directly with seeded tasks to
keep the contract surface narrow."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, Task
from loom_service.task_filter import resolve_task_filter


@pytest.fixture
async def session(
    postgres_url: str,
) -> AsyncIterator[AsyncSession]:
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Benchmark).values(
            execution_state="runnable",
            id="aime-24", display_name="AIME 2024",
            upstream_kind="huggingface",
            upstream_locator="x", upstream_revision="main",
            license_spdx="proprietary-MAA", license_url="",
            splits=["train"], series="aime",
        ))
        s.execute(insert(Benchmark).values(
            execution_state="runnable",
            id="aime-25", display_name="AIME 2025",
            upstream_kind="huggingface",
            upstream_locator="y", upstream_revision="main",
            license_spdx="proprietary-MAA", license_url="",
            splits=["train"], series="aime",
        ))
        # Mixed AIME tasks across the two v1.0-supported AIME benchmarks.
        rows = [
            ("aime-24/2024-I/1", "aime-24",
                {"year": "2024", "exam": "I"}),
            ("aime-24/2024-II/3", "aime-24",
                {"year": "2024", "exam": "II"}),
            ("aime-24/2023-I/7", "aime-24",
                {"year": "2023", "exam": "I"}),
            ("aime-25/2025-I/2", "aime-25",
                {"year": "2025", "exam": "I"}),
            ("aime-25/2025-II/9", "aime-25",
                {"year": "2025", "exam": "II"}),
        ]
        for tid, bid, tags in rows:
            s.execute(insert(Task).values(
                id=tid, checksum="0" * 64, config={},
                source=f"hf://x/{tid}/",
                license="proprietary-MAA",
                benchmark_id=bid, tags=tags,
            ))
        s.commit()
    sync_engine.dispose()

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as s2:
            yield s2
    finally:
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        with sessionmaker(sync_engine)() as s:
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.commit()
        sync_engine.dispose()


async def test_benchmark_ids_unions_two_benchmarks(
    session: AsyncSession,
) -> None:
    """Group-select of an `aime` series → both benchmarks resolved
    in one call. Total 5 tasks across both."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": ["aime-24", "aime-25"],
    })
    assert len(out) == 5
    assert all(
        t.startswith(("aime-24/", "aime-25/"))
        for t in out
    )


async def test_benchmark_ids_takes_precedence_over_benchmark_id(
    session: AsyncSession,
) -> None:
    """Backwards compat: clients that send both keep working but
    the plural list wins so the SPA's group-select path is the
    authoritative one."""
    out = await resolve_task_filter(session, {
        "benchmark_id": "humaneval",  # nonexistent here
        "benchmark_ids": ["aime-24"],
    })
    assert all(t.startswith("aime-24/") for t in out)
    assert len(out) == 3


async def test_empty_benchmark_ids_matches_no_tasks(
    session: AsyncSession,
) -> None:
    """An empty benchmark multi-select is an intentional empty slate,
    not a request for every task in the catalog."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": [],
    })
    assert out == []


async def test_tag_filters_AND_across_keys_OR_within_value_list(  # noqa: N802
    session: AsyncSession,
) -> None:
    """`{year: ["2024"], exam: ["I"]}` → only the 2024 AIME I row."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": ["aime-24", "aime-25"],
        "tag_filters": {
            "year": ["2024"], "exam": ["I"],
        },
    })
    assert out == ["aime-24/2024-I/1"]


async def test_tag_filters_value_list_is_OR(  # noqa: N802
    session: AsyncSession,
) -> None:
    """Two values for the same key → union (OR semantics)."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": ["aime-24", "aime-25"],
        "tag_filters": {"year": ["2023", "2025"]},
    })
    assert sorted(out) == sorted([
        "aime-24/2023-I/7",
        "aime-25/2025-I/2",
        "aime-25/2025-II/9",
    ])


async def test_tag_filters_empty_value_list_is_no_op(
    session: AsyncSession,
) -> None:
    """SPA may send `{year: []}` while the user is mid-edit; that
    shouldn't drop the result set to empty."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": ["aime-24"],
        "tag_filters": {"year": []},
    })
    assert len(out) == 3  # all three aime-24 rows


async def test_invalid_benchmark_ids_type_400s(
    session: AsyncSession,
) -> None:
    with pytest.raises(HTTPException) as exc:
        await resolve_task_filter(session, {
            "benchmark_ids": "aime-24",  # str not list
        })
    assert exc.value.status_code == 400
    assert "benchmark_ids" in exc.value.detail


async def test_invalid_tag_filter_value_type_400s(
    session: AsyncSession,
) -> None:
    with pytest.raises(HTTPException) as exc:
        await resolve_task_filter(session, {
            "benchmark_ids": ["aime-24"],
            "tag_filters": {"year": "2024"},  # str not list
        })
    assert exc.value.status_code == 400
    assert "tag_filters" in exc.value.detail


async def test_benchmark_ids_combined_with_subset_random_n(
    session: AsyncSession,
) -> None:
    """Union of benchmarks, then random_n across the union — verifies
    the subset_kind path still works on the multi-benchmark slate."""
    out = await resolve_task_filter(session, {
        "benchmark_ids": ["aime-24", "aime-25"],
        "subset_kind": "random_n", "n": 2, "seed": 42,
    })
    assert len(out) == 2
