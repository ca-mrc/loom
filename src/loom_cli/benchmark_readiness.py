"""Benchmark readiness audit command helpers."""

from __future__ import annotations

import asyncio
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from loom_bundle_checksum import sha256_of_dir
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    BenchmarkReadinessItem,
    TaskAuditSource,
    build_readiness_item,
    render_readiness_json,
    render_readiness_table,
)
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.task_image_materialization import canonical_task_checksum
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url

BUNDLE_VERIFICATION_SCHEMA_VERSION = 2
BUNDLE_VERIFICATION_KIND = "complete_bundle_sha256_of_dir_v1"
_BUNDLE_AUDIT_CONCURRENCY = 8


def _load_registry_names() -> set[str]:
    try:
        from loom_benchmarks.registry import REGISTRY
    except Exception:
        return set()
    return set(REGISTRY.keys())


@dataclass(frozen=True)
class BundleVerificationFailure:
    task_id: str
    source: str
    reason: str
    expected_checksum: str | None = None
    actual_checksum: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "task_id": self.task_id,
            "source": self.source,
            "reason": self.reason,
            "expected_checksum": self.expected_checksum,
            "actual_checksum": self.actual_checksum,
        }


@dataclass(frozen=True)
class BundlePresenceReport:
    s3_tasks: int
    verified: int
    failures: tuple[BundleVerificationFailure, ...]

    @property
    def failed(self) -> int:
        return len(self.failures)

    @property
    def checksum_mismatches(self) -> int:
        return sum(failure.reason == "checksum_mismatch" for failure in self.failures)

    @property
    def verification_errors(self) -> int:
        return self.failed - self.checksum_mismatches

    @property
    def missing(self) -> int:
        """Backward-compatible count for callers that only understood presence."""
        return self.verification_errors

    @property
    def missing_sources(self) -> list[str]:
        """Backward-compatible sources for non-checksum verification failures."""
        return [
            failure.source
            for failure in self.failures
            if failure.reason != "checksum_mismatch"
        ]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": BUNDLE_VERIFICATION_SCHEMA_VERSION,
            "verification_kind": BUNDLE_VERIFICATION_KIND,
            "s3_tasks": self.s3_tasks,
            "verified": self.verified,
            "failed": self.failed,
            "checksum_mismatches": self.checksum_mismatches,
            "verification_errors": self.verification_errors,
            "failures": [failure.to_dict() for failure in self.failures],
            # Retain the original presence-only fields for JSON consumers
            # while they move to the stronger verification contract.
            "missing": self.missing,
            "missing_sources": self.missing_sources,
        }


async def run_readiness_audit(
    *,
    db_url: str,
    benchmark: str | None = None,
    registry_names: set[str] | None = None,
) -> list[BenchmarkReadinessItem]:
    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            benchmark_stmt = select(Benchmark).order_by(Benchmark.id)
            if benchmark is not None:
                benchmark_stmt = benchmark_stmt.where(Benchmark.id == benchmark)
            benchmarks = list((await session.scalars(benchmark_stmt)).all())
            if not benchmarks:
                return []

            benchmark_ids = [row.id for row in benchmarks]
            task_rows = (
                await session.execute(
                    select(
                        TaskRow.benchmark_id,
                        TaskRow.id,
                        TaskRow.config,
                        TaskRow.source,
                    ).where(TaskRow.benchmark_id.in_(benchmark_ids)),
                )
            ).all()
    finally:
        await engine.dispose()

    tasks_by_benchmark: dict[str, list[TaskAuditSource]] = defaultdict(list)
    for benchmark_id, task_id, config, source in task_rows:
        if benchmark_id is None:
            continue
        tasks_by_benchmark[str(benchmark_id)].append(
            TaskAuditSource(
                id=str(task_id),
                config=dict(config),
                source=source,
            )
        )

    names = registry_names if registry_names is not None else _load_registry_names()
    return [
        build_readiness_item(
            BenchmarkAuditSource(
                id=row.id,
                display_name=row.display_name,
                series=row.series,
                upstream_kind=row.upstream_kind,
                upstream_locator=row.upstream_locator,
                upstream_revision=row.upstream_revision,
                execution_state=row.execution_state,
            ),
            tasks=tasks_by_benchmark.get(row.id, []),
            registry_names=names,
        )
        for row in benchmarks
    ]


def _parse_s3_source(source: str | None) -> tuple[str, str] | None:
    if source is None or not source.startswith("s3://"):
        return None
    rest = source[len("s3://"):]
    if "/" not in rest:
        return None
    bucket, prefix = rest.split("/", 1)
    if not bucket or not prefix:
        return None
    return bucket, prefix if prefix.endswith("/") else f"{prefix}/"


async def _verify_bundle_task(
    *,
    task_id: object,
    checksum: object,
    source: object,
    object_store: ObjectStore,
    semaphore: asyncio.Semaphore,
) -> BundleVerificationFailure | None:
    task_id_text = str(task_id)
    source_text = str(source)
    try:
        expected_checksum = canonical_task_checksum(str(checksum))
    except ValueError:
        return BundleVerificationFailure(
            task_id=task_id_text,
            source=source_text,
            reason="invalid_checksum",
        )
    parsed = _parse_s3_source(source if isinstance(source, str) else None)
    if parsed is None:
        return BundleVerificationFailure(
            task_id=task_id_text,
            source=source_text,
            reason="invalid_source",
            expected_checksum=expected_checksum,
        )
    bucket, prefix = parsed
    async with semaphore:
        try:
            with tempfile.TemporaryDirectory(prefix="loom-bundle-audit-") as temp:
                bundle_dir = Path(temp)
                downloaded = await object_store.download_prefix(
                    bucket=bucket,
                    prefix=prefix,
                    out_dir=bundle_dir,
                )
                if downloaded == 0:
                    return BundleVerificationFailure(
                        task_id=task_id_text,
                        source=source_text,
                        reason="empty_bundle",
                        expected_checksum=expected_checksum,
                    )
                if not (bundle_dir / "task.toml").is_file():
                    return BundleVerificationFailure(
                        task_id=task_id_text,
                        source=source_text,
                        reason="missing_task_toml",
                        expected_checksum=expected_checksum,
                    )
                try:
                    actual_checksum = sha256_of_dir(bundle_dir)
                except Exception:
                    return BundleVerificationFailure(
                        task_id=task_id_text,
                        source=source_text,
                        reason="hash_error",
                        expected_checksum=expected_checksum,
                    )
        except Exception:
            return BundleVerificationFailure(
                task_id=task_id_text,
                source=source_text,
                reason="download_error",
                expected_checksum=expected_checksum,
            )
    if actual_checksum != expected_checksum:
        return BundleVerificationFailure(
            task_id=task_id_text,
            source=source_text,
            reason="checksum_mismatch",
            expected_checksum=expected_checksum,
            actual_checksum=actual_checksum,
        )
    return None


async def run_bundle_presence_audit(
    *,
    db_url: str,
    object_store: ObjectStore,
    benchmark: str | None = None,
) -> BundlePresenceReport:
    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            task_stmt = select(
                TaskRow.id,
                TaskRow.checksum,
                TaskRow.source,
                TaskRow.benchmark_id,
                TaskRow.task_set_id,
            ).where(
                or_(
                    TaskRow.benchmark_id.is_not(None),
                    TaskRow.task_set_id.is_not(None),
                ),
                TaskRow.source.like("s3://%"),
            )
            if benchmark is not None:
                task_stmt = task_stmt.where(TaskRow.benchmark_id == benchmark)
            task_rows = list((await session.execute(task_stmt)).all())
    finally:
        await engine.dispose()

    semaphore = asyncio.Semaphore(_BUNDLE_AUDIT_CONCURRENCY)
    results = await asyncio.gather(
        *(
            _verify_bundle_task(
                task_id=task_id,
                checksum=checksum,
                source=source,
                object_store=object_store,
                semaphore=semaphore,
            )
            for task_id, checksum, source, _benchmark_id, _task_set_id in task_rows
        )
    )
    failures = tuple(
        sorted(
            (failure for failure in results if failure is not None),
            key=lambda failure: (failure.task_id, failure.source, failure.reason),
        )
    )

    return BundlePresenceReport(
        s3_tasks=len(task_rows),
        verified=len(task_rows) - len(failures),
        failures=failures,
    )


__all__ = [
    "BUNDLE_VERIFICATION_KIND",
    "BUNDLE_VERIFICATION_SCHEMA_VERSION",
    "BenchmarkAuditSource",
    "BenchmarkReadinessItem",
    "BundlePresenceReport",
    "BundleVerificationFailure",
    "TaskAuditSource",
    "build_readiness_item",
    "render_readiness_json",
    "render_readiness_table",
    "run_bundle_presence_audit",
    "run_readiness_audit",
]
