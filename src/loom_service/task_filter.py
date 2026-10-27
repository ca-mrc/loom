"""Shared task_filter resolution.

Hoisted from `routes/batches.py` so `POST /tasks/count` (the SPA's
real-count check on NewBatch — issue #28) can reuse the exact same
materialization the batch creator runs. No behavior change vs. the
previous in-batches.py implementation; just relocated.

Filter shape (`task_filter`):
- `license: str` — exact match on Task.license
- `task_ids: list[str]` — restrict to the given ids (also serves
  as the existence-check in subset_kind='explicit')
- `benchmark_id: str` — singular benchmark filter
- `benchmark_ids: list[str]` — multi-select; takes precedence over
  the singular form. Empty list ⇒ zero tasks (the SPA's group-select
  uses this convention)
- `task_set_id: str` — singular team-owned TaskSet filter
- `task_set_ids: list[str]` — multi-select TaskSet filter; takes
  precedence over the singular form. Empty list ⇒ zero tasks
- `tag_filters: dict[str, list[str]]` — JSONB containment match per
  key; AND across keys, OR within each key's values
- `subset_kind: "all" | "first_n" | "last_n" | "random_n" | "explicit"`
- `n: int` (required when subset_kind ∈ {first_n, last_n, random_n})
- `seed: int` (required when subset_kind = "random_n" so the same
  call reproduces the same sample)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import cast, false, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_profiles import (
    reject_non_runnable_benchmark_profiles,
    resolve_benchmark_selectors,
)
from loom.catalog_policy import CURRENTLY_UNSUPPORTED_BENCHMARK_IDS
from loom.db.schema import Task, TaskSet
from loom.db.task_set_visibility import visible_task_sets, visible_tasks
from loom.models.task import TaskConfig
from loom.task_runtime_compatibility import task_hardware_rejections

# Recognized task_filter keys. Anything else is rejected so a typo
# (`liscense` instead of `license`) doesn't silently match nothing.
FILTER_KEYS: frozenset[str] = frozenset(
    {
        "license",
        "task_ids",
        "benchmark_id",
        "subset_kind",
        "n",
        "seed",
        "benchmark_ids",
        "task_set_id",
        "task_set_ids",
        "tag_filters",
    }
)

SUBSET_KINDS: frozenset[str] = frozenset(
    {"all", "first_n", "last_n", "random_n", "explicit"},
)


@dataclass(frozen=True)
class TaskFilterResult:
    task_ids: list[str]
    benchmark_selection_provenance: list[dict[str, str]] = field(default_factory=list)


async def resolve_task_filter(
    session: AsyncSession,
    task_filter: Mapping[str, Any],
    *,
    team_id: UUID | None = None,
    require_runnable: bool = True,
) -> list[str]:
    return (
        await resolve_task_filter_with_diagnostics(
            session,
            task_filter,
            team_id=team_id,
            require_runnable=require_runnable,
        )
    ).task_ids


async def reject_unsupported_task_hardware(session: AsyncSession, task_ids: list[str]) -> None:
    if not task_ids:
        return
    rows = (await session.execute(
        select(Task.id, Task.config).where(Task.id.in_(task_ids)).order_by(Task.id),
    )).all()
    reasons: dict[str, list[str]] = {}
    for task_id, raw_config in rows:
        try:
            config = TaskConfig.model_validate(raw_config)
        except ValueError:
            # Preserve the caller's existing invalid-config diagnostic.
            continue
        if rejected := task_hardware_rejections(config):
            reasons[str(task_id)] = list(rejected)
    if reasons:
        unsupported_architecture = any(
            "unsupported_cpu_architecture" in reason
            for task_reasons in reasons.values() for reason in task_reasons
        )
        raise HTTPException(status_code=409, detail={
            "reason": "unsupported_cpu_architecture" if unsupported_architecture else "task_requirements_unsupported",
            "task_ids": list(reasons),
            "rejection_reasons": reasons,
        })


def _task_set_ids_from_filter(task_filter: Mapping[str, Any]) -> list[str] | None:
    task_set_ids_raw = task_filter.get("task_set_ids")
    if task_set_ids_raw is not None:
        if not isinstance(task_set_ids_raw, (list, tuple)) or not all(
            isinstance(x, str) for x in task_set_ids_raw
        ):
            raise HTTPException(
                status_code=400,
                detail="task_filter.task_set_ids must be a list of strings",
            )
        return list(task_set_ids_raw)

    task_set_id_raw = task_filter.get("task_set_id")
    if task_set_id_raw is None:
        return None
    if not isinstance(task_set_id_raw, str):
        raise HTTPException(
            status_code=400,
            detail="task_filter.task_set_id must be a string",
        )
    return [task_set_id_raw]


async def _reject_invisible_or_unrunnable_task_sets(
    session: AsyncSession,
    *,
    team_id: UUID | None,
    task_set_ids: list[str] | None,
) -> None:
    if task_set_ids is None or not task_set_ids:
        return

    rows = (
        (
            await session.execute(
                visible_task_sets(team_id=team_id).where(
                    TaskSet.id.in_(task_set_ids),
                ),
            )
        )
        .scalars()
        .all()
    )
    visible_by_id = {row.id: row for row in rows}
    missing = sorted(set(task_set_ids) - set(visible_by_id))
    if missing:
        raise HTTPException(status_code=404, detail="task set not found")

    unrunnable = sorted(
        row.id for row in visible_by_id.values() if row.status not in {"ready", "partial"}
    )
    if unrunnable:
        raise HTTPException(
            status_code=400,
            detail=(f"task_filter references TaskSets that are not ready: {unrunnable}"),
        )


async def resolve_task_filter_with_diagnostics(
    session: AsyncSession,
    task_filter: Mapping[str, Any],
    *,
    team_id: UUID | None = None,
    require_runnable: bool = True,
) -> TaskFilterResult:
    """Materialize a task_filter into a list of Task.id strings.

    Honors `subset_kind`:
    - `"all"` (default): every matching task.
    - `"first_n"`: first N task ids sorted ascending.
    - `"last_n"`: last N task ids sorted ascending.
    - `"random_n"`: N randomly-chosen task ids. Requires `seed` so
      the same call reproduces the same selection (Python's seeded
      `random.sample` on the materialized candidate list).
    - `"explicit"`: returns the requested task ids only when every id
      resolves to a visible candidate.
    """
    unknown = set(task_filter.keys()) - FILTER_KEYS
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"unknown task_filter keys: {sorted(unknown)}",
        )
    subset_kind = task_filter.get("subset_kind", "all")
    if subset_kind not in SUBSET_KINDS:
        raise HTTPException(
            status_code=400,
            detail=(f"unknown subset_kind {subset_kind!r}. valid: {sorted(SUBSET_KINDS)}"),
        )
    task_set_ids = _task_set_ids_from_filter(task_filter)
    await _reject_invisible_or_unrunnable_task_sets(
        session,
        team_id=team_id,
        task_set_ids=task_set_ids,
    )

    # Build the candidate query using the predicate keys. subset_kind
    # / n / seed are applied to the candidate set in Python.
    #
    # `benchmark_ids` (list) takes precedence over the singular
    # `benchmark_id` for multi-select / group-select UX. `tag_filters`
    # is a dict like `{"verified": ["true"]}` applied as a JSONB
    # containment predicate per key — `AND` across keys, `OR` within
    # each key's value list.
    stmt = (
        visible_tasks(team_id=team_id)
        .with_only_columns(
            Task.id,
            Task.benchmark_id,
        )
        .order_by(Task.id.asc())
    )
    if require_runnable and "task_ids" in task_filter:
        # Resolve visible explicit task ownership before the broad supported-
        # catalog predicate hides an excluded profile. This preserves the
        # existing policy while giving the same reason as alias/profile and
        # catalog surfaces; invisible tasks still disclose no profile facts.
        explicit_profiles = (await session.execute(
            visible_tasks(team_id=team_id)
            .with_only_columns(Task.benchmark_id)
            .where(Task.id.in_([str(value) for value in task_filter["task_ids"]]))
        )).scalars().all()
        await reject_non_runnable_benchmark_profiles(
            session, [str(profile_id) for profile_id in explicit_profiles if profile_id is not None],
        )
    if CURRENTLY_UNSUPPORTED_BENCHMARK_IDS:
        stmt = stmt.where(
            or_(
                Task.benchmark_id.is_(None),
                ~Task.benchmark_id.in_(sorted(CURRENTLY_UNSUPPORTED_BENCHMARK_IDS)),
            ),
        )
    if "license" in task_filter:
        stmt = stmt.where(Task.license == task_filter["license"])
    if "task_ids" in task_filter:
        ids = [str(x) for x in task_filter["task_ids"]]
        stmt = stmt.where(Task.id.in_(ids))
    source_selector_seen = False
    source_clauses: list[Any] = []
    benchmark_selection_provenance: list[dict[str, str]] = []
    benchmark_ids_raw = task_filter.get("benchmark_ids")
    if benchmark_ids_raw is not None:
        source_selector_seen = True
        if not isinstance(benchmark_ids_raw, (list, tuple)) or not all(
            isinstance(x, str) for x in benchmark_ids_raw
        ):
            raise HTTPException(
                status_code=400,
                detail="task_filter.benchmark_ids must be a list of strings",
            )
        if benchmark_ids_raw:
            resolved = await resolve_benchmark_selectors(
                session,
                list(benchmark_ids_raw),
                require_runnable=require_runnable,
            )
            source_clauses.append(Task.benchmark_id.in_(resolved.physical_ids))
            benchmark_selection_provenance = list(resolved.provenance)
    elif "benchmark_id" in task_filter:
        source_selector_seen = True
        benchmark_id_raw = task_filter["benchmark_id"]
        if not isinstance(benchmark_id_raw, str):
            raise HTTPException(
                status_code=400,
                detail="task_filter.benchmark_id must be a string",
            )
        resolved = await resolve_benchmark_selectors(
            session,
            [benchmark_id_raw],
            require_runnable=require_runnable,
        )
        source_clauses.append(Task.benchmark_id.in_(resolved.physical_ids))
        benchmark_selection_provenance = list(resolved.provenance)
    if task_set_ids is not None:
        source_selector_seen = True
        if task_set_ids:
            source_clauses.append(Task.task_set_id.in_(task_set_ids))
    if source_selector_seen:
        stmt = stmt.where(or_(*source_clauses) if source_clauses else false())
    tag_filters_raw = task_filter.get("tag_filters")
    if tag_filters_raw is not None:
        if not isinstance(tag_filters_raw, dict):
            raise HTTPException(
                status_code=400,
                detail="task_filter.tag_filters must be a JSON object",
            )
        for tag_key, tag_values in tag_filters_raw.items():
            if not isinstance(tag_key, str) or not tag_key:
                raise HTTPException(
                    status_code=400,
                    detail="tag_filters keys must be non-empty strings",
                )
            if not isinstance(tag_values, list) or not all(isinstance(v, str) for v in tag_values):
                raise HTTPException(
                    status_code=400,
                    detail=(f"tag_filters[{tag_key!r}] must be a list of strings"),
                )
            if not tag_values:
                continue
            # OR within a key: any of the listed values matches.
            value_clauses = [
                Task.tags.op("@>")(
                    cast({tag_key: v}, JSONB),
                )
                for v in tag_values
            ]
            stmt = stmt.where(or_(*value_clauses))
    candidate_rows = (await session.execute(stmt)).all()
    if require_runnable:
        await reject_non_runnable_benchmark_profiles(
            session,
            [
                str(benchmark_id)
                for _task_id, benchmark_id in candidate_rows
                if benchmark_id is not None
            ],
        )
    candidates = [str(task_id) for task_id, _benchmark_id in candidate_rows]

    async def selected_result(selected_ids: list[str]) -> TaskFilterResult:
        if require_runnable:
            await reject_unsupported_task_hardware(session, selected_ids)
        return TaskFilterResult(
            task_ids=selected_ids,
            benchmark_selection_provenance=benchmark_selection_provenance,
        )

    if subset_kind == "all":
        return await selected_result(candidates)
    if subset_kind == "explicit":
        requested_ids = {str(task_id) for task_id in task_filter.get("task_ids", [])}
        if requested_ids != set(candidates):
            raise HTTPException(status_code=404, detail="task not found")
        return await selected_result(candidates)

    n_raw = task_filter.get("n")
    try:
        n = int(n_raw) if n_raw is not None else None
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="task_filter.n must be an integer",
        ) from exc
    if n is None or n < 1:
        raise HTTPException(
            status_code=400,
            detail=(f"task_filter.n is required and must be ≥ 1 when subset_kind={subset_kind!r}"),
        )

    if subset_kind == "first_n":
        selected = candidates[:n]
        return await selected_result(selected)
    if subset_kind == "last_n":
        selected = candidates[-n:] if n <= len(candidates) else candidates
        return await selected_result(selected)
    # random_n
    seed_raw = task_filter.get("seed")
    if seed_raw is None:
        raise HTTPException(
            status_code=400,
            detail="task_filter.seed is required when subset_kind='random_n'",
        )
    try:
        seed = int(seed_raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="task_filter.seed must be an integer",
        ) from exc
    import random as _random

    rng = _random.Random(seed)
    if n >= len(candidates):
        selected = candidates
    else:
        selected = sorted(rng.sample(candidates, n))
    return await selected_result(selected)
