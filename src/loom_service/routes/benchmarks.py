"""Benchmarks browse (spec §5.4).

Schema-real shape: `Benchmark.id` is a text PK (e.g. `humaneval`),
fields are `display_name`, `upstream_kind`, `upstream_locator`,
`upstream_revision`, `license_spdx`, `license_url`, `splits`,
`imported_at`, `imported_by`.

Cursor pagination uses `display_name` since rows are sorted by it.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Select, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_profiles import resolve_benchmark_selectors
from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    BenchmarkReadinessItem,
    TaskAuditSource,
    build_readiness_item,
    readiness_display_fields,
)
from loom.db.schema import Benchmark, BenchmarkAlias, Task
from loom_service import wire_responses as wire
from loom_service.dependencies import SessionAndCtx

router = APIRouter()

_HIDDEN_FROM_V1_CATALOG_BENCHMARK_IDS = frozenset({"sample-tasks"})


def _load_registry_names() -> set[str]:
    try:
        from loom_benchmarks.registry import REGISTRY
    except Exception:
        return set()
    return set(REGISTRY.keys())


def _readiness_for_benchmark(
    b: Benchmark,
    *,
    tasks: list[TaskAuditSource],
    registry_names: set[str],
) -> BenchmarkReadinessItem:
    return build_readiness_item(
        BenchmarkAuditSource(
            id=b.id,
            display_name=b.display_name,
            series=b.series,
            upstream_kind=b.upstream_kind,
            upstream_locator=b.upstream_locator,
            upstream_revision=b.upstream_revision,
            execution_state=b.execution_state,
        ),
        tasks=tasks,
        registry_names=registry_names,
    )


def _bench_row(
    b: Benchmark,
    readiness: BenchmarkReadinessItem,
    *,
    aliases: list[str],
    public_selector: str,
) -> dict[str, Any]:
    readiness_fields = readiness_display_fields(readiness)
    return {
        "id": public_selector,
        "aliases": aliases,
        "physical_profile": b.id,
        "profile_provenance": dict(b.profile_provenance or {}),
        "execution_state": b.execution_state,
        "display_name": b.display_name,
        "upstream_kind": b.upstream_kind,
        "upstream_locator": b.upstream_locator,
        "upstream_revision": b.upstream_revision,
        "license_spdx": b.license_spdx,
        "license_url": b.license_url,
        "splits": list(b.splits),
        "imported_at": b.imported_at.isoformat(),
        "imported_by": b.imported_by,
        # Plan 28/#276: task_count is the number of fully runnable
        # TaskConfig-valid rows. Raw/invalid rows are exposed in the
        # readiness diagnostics below so the SPA can explain why a
        # benchmark is blocked instead of treating every zero as empty.
        "task_count": readiness.license_allowed_task_count,
        # PR-2 series/tags: surface series so the SPA can group
        # related benchmarks in the dropdown. NULL = standalone.
        "series": b.series,
        **readiness_fields,
    }


def _visible_benchmarks_statement(*, include_historical: bool) -> Select[tuple[Benchmark]]:
    stmt = select(Benchmark).where(
        ~Benchmark.id.in_(_HIDDEN_FROM_V1_CATALOG_BENCHMARK_IDS),
    )
    if not include_historical:
        stmt = stmt.where(Benchmark.execution_state == "runnable")
    return stmt


async def _aliases_by_profile(
    session: AsyncSession,
    profile_ids: list[str],
) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = defaultdict(list)
    if not profile_ids:
        return aliases
    for alias, benchmark_id in (
        await session.execute(
            select(BenchmarkAlias.alias, BenchmarkAlias.benchmark_id).where(
                BenchmarkAlias.benchmark_id.in_(profile_ids),
            ),
        )
    ).all():
        aliases[str(benchmark_id)].append(str(alias))
    for items in aliases.values():
        items.sort()
    return aliases


async def _benchmark_rows_with_readiness(
    session: AsyncSession,
    benchmarks: list[Benchmark],
) -> list[tuple[Benchmark, BenchmarkReadinessItem]]:
    if not benchmarks:
        return []
    benchmark_ids = [row.id for row in benchmarks]
    task_rows = (
        await session.execute(
            select(
                Task.benchmark_id,
                Task.id,
                Task.config,
                Task.source,
                Task.license,
                Task.tags,
            ).where(Task.benchmark_id.in_(benchmark_ids)),
        )
    ).all()
    tasks_by_benchmark: dict[str, list[TaskAuditSource]] = defaultdict(list)
    for benchmark_id, task_id, config, source, license_, tags in task_rows:
        if benchmark_id is None:
            continue
        tasks_by_benchmark[str(benchmark_id)].append(
            TaskAuditSource(
                id=str(task_id),
                config=dict(config),
                source=source,
                license=license_,
                tags=dict(tags or {}),
            )
        )

    registry_names = _load_registry_names()
    return [
        (
            row,
            _readiness_for_benchmark(
                row,
                tasks=tasks_by_benchmark.get(row.id, []),
                registry_names=registry_names,
            ),
        )
        for row in benchmarks
    ]


@router.get("/benchmarks", response_model=wire.BenchmarkList, response_model_exclude_unset=True)
async def list_benchmarks(
    sc: SessionAndCtx,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    # By default the SPA-facing listing hides benchmarks with zero
    # imported tasks — "registered" on its own (metadata-only seeding)
    # would otherwise show benchmarks in the dropdown that produce a
    # confusing "0 tasks match" when picked. Operators can pass
    # `?include_empty=true` to see every registered benchmark (e.g.
    # admin tools that drive imports).
    include_empty: Annotated[bool, Query()] = False,
    include_historical: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    s, _ctx = sc
    stmt = _visible_benchmarks_statement(
        include_historical=include_historical,
    ).order_by(Benchmark.display_name)
    if cursor:
        stmt = stmt.where(Benchmark.display_name > cursor)
    benchmarks = list((await s.scalars(stmt)).all())
    rows = await _benchmark_rows_with_readiness(
        s,
        benchmarks,
    )
    aliases_by_profile = await _aliases_by_profile(
        s,
        [benchmark.id for benchmark, _readiness in rows],
    )
    if not include_empty:
        rows = [row for row in rows if row[1].readiness_state == "runnable"]
    rows = rows[: limit + 1]
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor: str | None = rows[-1][0].display_name
    else:
        next_cursor = None
    return {
        "items": [
            _bench_row(
                benchmark,
                readiness,
                aliases=aliases_by_profile.get(benchmark.id, []),
                public_selector=(aliases_by_profile.get(benchmark.id, [benchmark.id])[0]),
            )
            for benchmark, readiness in rows
        ],
        "next_cursor": next_cursor,
    }


class BenchmarkDiscoveryRequest(BaseModel):
    benchmark_ids: list[str] = Field(min_length=1, max_length=200)


class BenchmarkTag(BaseModel):
    key: str
    values: list[str]


class BenchmarkDiscoveryResponse(BaseModel):
    items: list[BenchmarkTag]


@router.post("/benchmarks/discover", response_model=BenchmarkDiscoveryResponse)
async def discover_benchmarks(
    body: BenchmarkDiscoveryRequest,
    sc: SessionAndCtx,
) -> BenchmarkDiscoveryResponse:
    """Union selected tags in bulk. Catalog readiness is already a bulk read.

    Keep readiness on GET /benchmarks; loading task configurations again for
    every selection would duplicate its authoritative catalog projection.
    """
    session, _ctx = sc
    resolved = await resolve_benchmark_selectors(
        session, sorted(set(body.benchmark_ids)), require_runnable=False,
    )
    physical_ids = sorted(set(resolved.physical_ids))
    benchmarks = list((await session.scalars(
        select(Benchmark).where(Benchmark.id.in_(physical_ids)).order_by(Benchmark.id),
    )).all())
    if len(benchmarks) != len(physical_ids):
        raise HTTPException(status_code=404, detail="benchmark not found")
    rows = (await session.execute(text(
        "SELECT kv.key, ARRAY_AGG(DISTINCT kv.value ORDER BY kv.value) "
        "FROM tasks t, jsonb_each_text(t.tags) AS kv(key, value) "
        "WHERE t.benchmark_id = ANY(:ids) GROUP BY kv.key ORDER BY kv.key",
    ), {"ids": physical_ids})).all()
    return BenchmarkDiscoveryResponse(
        items=[BenchmarkTag(key=key, values=list(values)) for key, values in rows],
    )


@router.get("/benchmarks/{benchmark_id}")
async def get_benchmark(
    benchmark_id: str,
    sc: SessionAndCtx,
) -> dict[str, Any]:
    s, _ctx = sc
    if benchmark_id in _HIDDEN_FROM_V1_CATALOG_BENCHMARK_IDS:
        raise HTTPException(
            status_code=404,
            detail="benchmark not found",
        )
    resolved = await resolve_benchmark_selectors(
        s,
        [benchmark_id],
        require_runnable=False,
    )
    b = (
        await s.execute(
            select(Benchmark).where(Benchmark.id == resolved.physical_ids[0]),
        )
    ).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404,
            detail="benchmark not found",
        )
    rows = await _benchmark_rows_with_readiness(
        s,
        [b],
    )
    aliases_by_profile = await _aliases_by_profile(s, [b.id])
    return _bench_row(
        rows[0][0],
        rows[0][1],
        aliases=aliases_by_profile.get(b.id, []),
        public_selector=(
            benchmark_id if benchmark_id in aliases_by_profile.get(b.id, []) else b.id
        ),
    )


@router.get("/benchmarks/{benchmark_id}/tags", response_model=wire.BenchmarkTagsResponse, response_model_exclude_unset=True)
async def list_benchmark_tags(
    benchmark_id: str,
    sc: SessionAndCtx,
) -> dict[str, Any]:
    """Distinct tag keys → distinct values for a benchmark's tasks.

    Drives the SPA's tag-filter UI (PR-3): one dropdown per key, each
    populated with the values actually present in that benchmark. The
    SPA can then build a `task_filter.tag_filters` payload from user
    selections without guessing what's in the data.

    Implementation: `jsonb_each_text(tags)` cross-joined to each task
    row in the benchmark, grouped by key, distinct values aggregated.
    """
    s, _ctx = sc
    # 404 if the benchmark itself doesn't exist so the SPA can
    # distinguish "no tags here" (empty list, 200) from "wrong id"
    # (404).
    resolved = await resolve_benchmark_selectors(
        s,
        [benchmark_id],
        require_runnable=False,
    )
    physical_profile = resolved.physical_ids[0]
    exists = (
        await s.execute(
            select(Benchmark.id).where(Benchmark.id == physical_profile),
        )
    ).scalar_one_or_none()
    if exists is None:
        raise HTTPException(
            status_code=404,
            detail="benchmark not found",
        )
    rows = (
        await s.execute(
            text(
                "SELECT kv.key, ARRAY_AGG(DISTINCT kv.value ORDER BY kv.value) "
                "FROM tasks t, jsonb_each_text(t.tags) AS kv(key, value) "
                "WHERE t.benchmark_id = :bid "
                "GROUP BY kv.key "
                "ORDER BY kv.key",
            ),
            {"bid": physical_profile},
        )
    ).all()
    return {
        "items": [{"key": key, "values": list(values)} for key, values in rows],
    }
