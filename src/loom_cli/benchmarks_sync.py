"""Sync engine for `config/benchmarks.toml` (issue #234).

Walks the parsed `BenchmarksConfig` and UPSERTs rows into the
`benchmarks` + `tasks` tables. Idempotent: re-running against the same
TOML + same on-disk task bundles yields no row churn.

Topology note: this engine assumes the caller has access to BOTH the
DB session AND the on-disk `fixtures_root` directory tree. In dev
compose CP runs on the host so it satisfies both. In k8s, this is an
operator-run one-shot Job that mounts the same data PV as the worker.
The control-plane lifespan does NOT auto-sync — that would require
mounting the data PV into CP which violates the workload separation.
"""
from __future__ import annotations

import logging
import os
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_publication import prepare_profile_publication
from loom.config.benchmarks import (
    BenchmarksConfig,
    LocalBenchmarkEntry,
    RemapBenchmarkEntry,
)
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.models.task_checksum import task_checksum
from loom.task_image_materialization import ensure_task_image_materializations

logger = logging.getLogger(__name__)

LOCAL_FOLDER_KIND = "local-folder"
SYNC_IMPORTED_BY = "benchmarks-toml-sync"

PlanAction = Literal["INSERT", "UPDATE", "SKIP", "ERROR"]


@dataclass(frozen=True)
class PlanRow:
    kind: Literal["local", "remap"]
    id: str
    action: PlanAction
    reason: str


@dataclass(frozen=True)
class TaskCounts:
    """Per-benchmark task-row diff outcome from one sync pass."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.unchanged


@dataclass
class SyncPlan:
    """Per-entry decisions + per-benchmark task diff counts."""

    rows: list[PlanRow] = field(default_factory=list)
    tasks: dict[str, TaskCounts] = field(default_factory=dict)

    def add(
        self,
        *,
        kind: Literal["local", "remap"],
        id: str,
        action: PlanAction,
        reason: str = "",
    ) -> None:
        self.rows.append(
            PlanRow(kind=kind, id=id, action=action, reason=reason),
        )


class SyncError(Exception):
    """Raised on a hard sync failure (TOML invalid, collision, bad task)."""


def preflight(
    cfg: BenchmarksConfig,
    *,
    registry_names: set[str],
) -> None:
    """Validate cross-cutting constraints before any DB work.

    Raises `SyncError` for any condition that should abort sync.
    """
    for local_entry in cfg.local:
        if local_entry.id in registry_names:
            raise SyncError(
                f"local benchmark id {local_entry.id!r} collides with a "
                "registered entry-point adapter; pick a different id "
                "to avoid shadowing the built-in",
            )
    for remap_entry in cfg.remap:
        if remap_entry.id in registry_names:
            raise SyncError(
                f"remap benchmark id {remap_entry.id!r} collides with a "
                "registered entry-point adapter; pick a different id",
            )
        if remap_entry.inherit not in registry_names:
            raise SyncError(
                f"remap {remap_entry.id!r} inherits from "
                f"{remap_entry.inherit!r} which is not in REGISTRY "
                f"(available: {sorted(registry_names)})",
            )


async def sync(
    cfg: BenchmarksConfig,
    *,
    fixtures_root: Path,
    session: AsyncSession,
    registry_names: set[str],
    base_adapter_lookup: Callable[[str], _RemapBase | None] | None = None,
    dry_run: bool = False,
) -> SyncPlan:
    """Apply the TOML to the DB. Returns the plan.

    `base_adapter_lookup` resolves a `[[remap]] inherit` name to its
    base adapter so we can inherit defaults for `series` / `splits`.
    Defaults to looking up `loom_benchmarks.REGISTRY` lazily. Tests
    inject a stub to avoid importing the real registry.
    """
    preflight(cfg, registry_names=registry_names)
    plan = SyncPlan()

    for local_entry in cfg.local:
        await _sync_local(
            local_entry,
            fixtures_root=fixtures_root,
            session=session,
            plan=plan,
            dry_run=dry_run,
        )

    for remap_entry in cfg.remap:
        await _sync_remap(
            remap_entry,
            session=session,
            plan=plan,
            dry_run=dry_run,
            base_adapter_lookup=base_adapter_lookup or _default_base_lookup,
        )

    return plan


async def _sync_local(
    entry: LocalBenchmarkEntry,
    *,
    fixtures_root: Path,
    session: AsyncSession,
    plan: SyncPlan,
    dry_run: bool,
) -> None:
    entry_root = fixtures_root / entry.id
    source_dir = entry_root / entry.source_subdir if entry.source_subdir else entry_root
    if not source_dir.is_dir():
        logger.warning(
            "benchmarks_sync_skip kind=local id=%s reason=missing_source_dir path=%s",
            entry.id,
            source_dir,
        )
        plan.add(
            kind="local",
            id=entry.id,
            action="SKIP",
            reason=f"source dir missing: {source_dir}",
        )
        return

    task_tomls = walk_task_tomls(source_dir)
    if not task_tomls:
        logger.warning(
            "benchmarks_sync_skip kind=local id=%s reason=empty_source_dir path=%s",
            entry.id, source_dir,
        )
        plan.add(
            kind="local", id=entry.id, action="SKIP",
            reason=f"source dir empty: {source_dir}",
        )
        return

    prepared_tasks = _prepare_local_tasks(entry, source_dir=source_dir, task_tomls=task_tomls)
    desired = dict(
        id=entry.id,
        display_name=entry.display_name,
        series=entry.series,
        license_spdx=entry.license_spdx,
        license_url="",
        upstream_kind=LOCAL_FOLDER_KIND,
        upstream_locator=str(source_dir),
        upstream_revision="",
        splits=[],
        imported_by=SYNC_IMPORTED_BY,
    )
    existing = await _get_benchmark(session, entry.id)
    if existing is None:
        action: PlanAction = "INSERT"
        reason = "new"
    elif _benchmark_differs(existing, desired):
        action = "UPDATE"
        reason = "metadata changed"
    else:
        action = "SKIP"
        reason = "unchanged"

    plan.add(kind="local", id=entry.id, action=action, reason=reason)

    async with session.begin_nested():
        if not dry_run:
            identity = {**desired, "profile_provenance": existing.profile_provenance or {} if existing else {}}
            desired["execution_state"] = await prepare_profile_publication(
                session, profile_id=entry.id, profile_identity=identity, tasks=prepared_tasks,
            )
        if not dry_run and action != "SKIP":
            await session.execute(
                pg_insert(Benchmark).values(**desired).on_conflict_do_update(
                    index_elements=["id"],
                    set_={k: v for k, v in desired.items() if k != "id"},
                ),
            )

        plan.tasks[entry.id] = await _sync_local_tasks(
            prepared_tasks=prepared_tasks, session=session, dry_run=dry_run,
        )
    if not dry_run:
        await session.commit()


def _prepare_local_tasks(
    entry: LocalBenchmarkEntry, *, source_dir: Path, task_tomls: list[Path],
) -> list[dict[str, Any]]:
    """Read/validate/hash all local bytes before the catalog-lock transaction."""
    rows = []
    for task_toml in task_tomls:
        bundle_dir = task_toml.parent
        rel = bundle_dir.relative_to(source_dir)
        task_id = entry.id if rel == Path(".") else f"{entry.id}/{rel.as_posix()}"
        try:
            with task_toml.open("rb") as file:
                config: dict[str, Any] = tomllib.load(file)
            TaskConfig.model_validate(config)
            config["task"] = {**config["task"], "id": task_id}
        except Exception as exc:
            raise SyncError(
                f"invalid task.toml at {task_toml}: {exc}. Fix the file "
                "or remove the bundle to unblock sync.",
            ) from exc
        rows.append({
            "id": task_id, "checksum": task_checksum(bundle_dir), "config": config,
            "source": _fixture_source(entry, rel), "source_provenance": {},
            "license": entry.license_spdx, "benchmark_id": entry.id,
        })
    return rows


async def _sync_local_tasks(
    *,
    prepared_tasks: list[dict[str, Any]],
    session: AsyncSession,
    dry_run: bool,
) -> TaskCounts:
    """Apply prepared task facts inside the caller's final catalog transaction.

    Unchanged rows retain identity while image prerequisites may recover their
    retired references. All file parsing and hashing happened before row locks.
    """
    inserted = updated = unchanged = 0
    for desired in sorted(prepared_tasks, key=lambda row: row["id"]):
        task_id = desired["id"]
        existing = await _get_task(session, task_id, lock=not dry_run)
        if existing is None:
            inserted += 1
        elif any(getattr(existing, key) != value for key, value in desired.items()):
            updated += 1
        else:
            unchanged += 1
            if not dry_run and existing is not None:
                await ensure_task_image_materializations(session, task_row=existing)
            continue

        if not dry_run:
            task_row = (
                await session.execute(
                    pg_insert(TaskRow)
                    .values(**desired)
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            key: value for key, value in desired.items() if key != "id"
                        },
                    )
                    .returning(TaskRow)
                    .execution_options(populate_existing=True),
                )
            ).scalar_one()
            await ensure_task_image_materializations(session, task_row=task_row)
    return TaskCounts(
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
    )


def _fixture_source(entry: LocalBenchmarkEntry, rel: Path) -> str:
    parts = [entry.id]
    if entry.source_subdir:
        parts.extend(entry.source_subdir.split("/"))
    if rel != Path("."):
        parts.extend(rel.as_posix().split("/"))
    return "fixture://" + "/".join(parts)


def walk_task_tomls(source_dir: Path) -> list[Path]:
    """Find every `task.toml` under `source_dir`, no symlink-following.

    Returned paths are sorted for stable plan output.
    """
    found: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(
        source_dir, followlinks=False,
    ):
        if "task.toml" in filenames:
            found.append(Path(dirpath) / "task.toml")
    found.sort()
    return found


class _RemapBase(Protocol):
    """Subset of `BenchmarkAdapter` we read for series / splits inheritance."""

    splits: tuple[str, ...]


if TYPE_CHECKING:  # pragma: no cover
    pass


def _default_base_lookup(inherit: str) -> _RemapBase | None:
    """Lazy import so unit tests that mock the lookup don't pay the
    REGISTRY entry-point scan cost. Cast: REGISTRY is typed as
    `BenchmarkAdapter` but only `series` + `splits` are read.
    """
    from loom_benchmarks.registry import REGISTRY
    base: Any = REGISTRY.get(inherit)
    return base  # type: ignore[no-any-return]


async def _sync_remap(
    entry: RemapBenchmarkEntry,
    *,
    session: AsyncSession,
    plan: SyncPlan,
    dry_run: bool,
    base_adapter_lookup: Callable[[str], _RemapBase | None],
) -> None:
    """Write a `benchmarks` row for the remap. Tasks are imported
    separately via `loom benchmark-tool import <remap.id>` (issue #234,
    PR-2 plumbing in `loom_benchmark_tool/import_cmd.py`).
    """
    base = base_adapter_lookup(entry.inherit)
    # Pre-flight guarantees `inherit in registry_names`, so base is
    # not-None at runtime — but the lookup signature still admits it.
    if base is None:  # pragma: no cover - defensive
        raise SyncError(
            f"remap {entry.id!r} inherit={entry.inherit!r} not in REGISTRY",
        )
    desired = dict(
        id=entry.id,
        display_name=entry.display_name,
        series=entry.series or getattr(base, "series", None),
        license_spdx=entry.license_spdx,
        license_url=entry.license_url,
        upstream_kind=entry.upstream_kind,
        upstream_locator=entry.upstream_locator,
        upstream_revision="",  # filled in by benchmark-tool import
        splits=entry.splits if entry.splits is not None else list(base.splits),
        imported_by=SYNC_IMPORTED_BY,
    )
    if not dry_run:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
            {"profile_id": entry.id},
        )
    existing = await _get_benchmark(session, entry.id)
    if existing is not None:
        desired["upstream_revision"] = existing.upstream_revision
    if existing is None:
        action: PlanAction = "INSERT"
        reason = "new"
    elif _benchmark_differs(existing, desired):
        action = "UPDATE"
        reason = "metadata changed"
    else:
        action = "SKIP"
        reason = "unchanged"

    plan.add(kind="remap", id=entry.id, action=action, reason=reason)

    if not dry_run:
        current_tasks = list(await session.scalars(
            select(TaskRow).where(TaskRow.benchmark_id == entry.id).order_by(TaskRow.id),
        ))
        desired["execution_state"] = await prepare_profile_publication(
            session, profile_id=entry.id,
            profile_identity={**desired, "profile_provenance": existing.profile_provenance or {} if existing else {}},
            tasks=current_tasks,
        )
    if not dry_run and action != "SKIP":
        await session.execute(
            pg_insert(Benchmark).values(**desired).on_conflict_do_update(
                index_elements=["id"],
                set_={k: v for k, v in desired.items() if k != "id"},
            ),
        )
    if not dry_run:
        await session.commit()


async def _get_benchmark(
    session: AsyncSession, benchmark_id: str,
) -> Benchmark | None:
    result = await session.execute(
        select(Benchmark).where(Benchmark.id == benchmark_id),
    )
    return result.scalar_one_or_none()


async def _get_task(session: AsyncSession, task_id: str, *, lock: bool = False) -> TaskRow | None:
    query = select(TaskRow).where(TaskRow.id == task_id)
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    result = await session.execute(query)
    return result.scalar_one_or_none()


def _benchmark_differs(row: Benchmark, desired: dict[str, Any]) -> bool:
    """Compare the columns we manage. Ignore imported_at / generated."""
    for key in (
        "display_name",
        "series",
        "license_spdx",
        "license_url",
        "upstream_kind",
        "upstream_locator",
        "upstream_revision",
        "splits",
    ):
        if getattr(row, key) != desired[key]:
            return True
    return False


def render_plan_table(plan: SyncPlan) -> str:
    """Plain-text dry-run output (used by the CLI subcommand)."""
    if not plan.rows:
        return "(no entries)"
    header = ("KIND", "ID", "ACTION", "REASON")
    rows = [header] + [
        (r.kind, r.id, r.action, r.reason) for r in plan.rows
    ]
    widths = [max(len(str(r[i])) for r in rows) for i in range(4)]
    lines = []
    for r in rows:
        lines.append(
            "  ".join(str(r[i]).ljust(widths[i]) for i in range(4)).rstrip(),
        )
    return "\n".join(lines)
