"""Operator admission of published native profiles, without runtime qualification."""

from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_readiness import BenchmarkAuditSource, TaskAuditSource, build_readiness_item
from loom.db.schema import Benchmark, Task
from loom.models.harbor import HarborBenchmarkSpec, UpstreamOrigin
from loom.models.task import TaskConfig
from loom.task_bundle_source_journal import admit_task_bundle_source


async def activate_native_benchmark(
    session: AsyncSession, *, spec: HarborBenchmarkSpec,
) -> int:
    """Admit one exact native catalog in a caller-owned transaction.

    Lock order matches publication: benchmark, sorted tasks, sorted sources.
    Existing source registration owns immutable object verification; admission
    reuses its available incarnation and retained catalog reference. Neither
    this operation nor its runnable state claims successful task execution.
    """
    if spec.id == "terminal-bench-2@tb2.1-r6":
        raise ValueError("TB2.1 requires datasets activate and its existing audit")
    benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == spec.id)
        .execution_options(populate_existing=True).with_for_update(),
    )
    if benchmark is None:
        raise ValueError("native benchmark is not published")
    if benchmark.execution_state not in {"pending", "runnable"}:
        raise ValueError(f"native benchmark state cannot be activated: {benchmark.execution_state}")
    provenance = benchmark.profile_provenance or {}
    if (
        UpstreamOrigin.model_validate(provenance.get("upstream_origin")) != spec.origin
        or provenance.get("source_task_count") != spec.expected_task_count
        or benchmark.upstream_kind != spec.origin.kind
        or benchmark.upstream_locator != spec.origin.locator
        or benchmark.upstream_revision != spec.origin.revision
    ):
        raise ValueError("native benchmark does not match the pinned source descriptor")
    tasks = list(await session.scalars(
        select(Task).where(Task.benchmark_id == spec.id).order_by(Task.id)
        .execution_options(populate_existing=True).with_for_update(),
    ))
    expected_count = len(spec.origin.subset) or spec.expected_task_count
    if len(tasks) != expected_count:
        raise ValueError(f"native task census: expected {expected_count}, found {len(tasks)}")
    upstream_ids: set[str] = set()
    for task in tasks:
        config = TaskConfig.model_validate(task.config)
        upstream_id = config.upstream_task_id
        task_provenance = task.source_provenance or {}
        if (
            not upstream_id or upstream_id in upstream_ids
            or task.id != f"{spec.id}/{upstream_id}"
            or config.task.id != task.id
            or config.upstream_origin != spec.origin
            or task_provenance.get("upstream_task_id") != upstream_id
            or UpstreamOrigin.model_validate(task_provenance.get("upstream_origin")) != spec.origin
        ):
            raise ValueError(f"native task identity does not match source descriptor: {task.id}")
        upstream_ids.add(upstream_id)
    if spec.origin.subset and upstream_ids != set(spec.origin.subset):
        raise ValueError("native task census does not match the exact source subset")
    readiness = build_readiness_item(
        BenchmarkAuditSource(
            id=benchmark.id, display_name=benchmark.display_name, series=benchmark.series,
            upstream_kind=benchmark.upstream_kind, upstream_locator=benchmark.upstream_locator,
            upstream_revision=benchmark.upstream_revision,
        ),
        tasks=[TaskAuditSource(id=task.id, config=task.config, source=task.source) for task in tasks],
        registry_names={spec.id},
    )
    if readiness.readiness_state != "runnable":
        raise ValueError(f"native task admission blocked: {readiness.blocker_reason}; "
                         + "; ".join(readiness.runtime_blockers))
    for task in sorted(tasks, key=lambda row: hashlib.sha256((row.source or "").encode()).hexdigest()):
        await admit_task_bundle_source(
            session, task_id=task.id, task_checksum=task.checksum, task_config=task.config,
            task_source=task.source, task_source_provenance=task.source_provenance,
            reference_kind="catalog", owner_id=task.id,
        )
    benchmark.execution_state = "runnable"
    await session.flush()
    return len(tasks)
