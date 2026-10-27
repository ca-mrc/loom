"""Reviewed TB2.1 validation adapter for the shared catalog lifecycle.

The legacy package contract predates source-journal publication. Its authoritative
byte/workspace/verifier audit remains the adoption boundary; uploaded audit JSON
only identifies the expected observation and never supplies validation authority.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from loom.benchmark_admission import AdapterValidation
    from loom.db.schema import Benchmark
    from loom.models.benchmark_profile import BenchmarkProfileSpec
    from loom.trajectory.storage import ObjectStore
    from loom_benchmark_tool.audit_cmd import AuditResult


CONTRACT = "tb21.v1"
PROFILE = "terminal-bench-2@tb2.1-r6"
PUBLIC_ALIAS = "terminal-bench-2"


def matches_profile(benchmark: Benchmark) -> bool:
    """Choose the reviewed adapter from stored facts, never an uploaded policy."""
    provenance = benchmark.profile_provenance or {}
    return (
        benchmark.id == PROFILE
        or provenance.get("physical_profile") == PROFILE
        or provenance.get("verifier_identity") == "tb21-native-reward-file-v1"
    )


def profile_spec() -> BenchmarkProfileSpec:
    """Normalize the reviewed package lock into the common descriptor."""
    from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock

    from loom.models.benchmark_profile import BenchmarkProfileSpec, ProfileOrigin

    lock = load_tb21_lock()
    return BenchmarkProfileSpec(
        id=PROFILE,
        origin=ProfileOrigin(
            kind="harbor-package", locator=lock.dataset, revision=lock.revision,
            release="2.1", importer="loom.tb21.v1",
        ),
        expected_task_count=len(lock.tasks),
        selected_task_ids=tuple(sorted(task.name.removeprefix("terminal-bench/") for task in lock.tasks)),
        selection="full", validation_contract=CONTRACT,
    )


async def prepare_validation(
    session: AsyncSession, *, object_store: ObjectStore, audit_evidence: AuditResult,
) -> tuple[BenchmarkProfileSpec, AdapterValidation, AuditResult]:
    """Audit without catalog locks and bind the exact rows observed to a receipt."""
    from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock

    from loom.benchmark_admission import AdapterValidation, snapshot_profile, snapshot_task
    from loom.db.schema import Benchmark, Task
    from loom_benchmark_tool.audit_cmd import ProfileActivationError, audit_tb21_profile

    benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == PROFILE)
        .execution_options(populate_existing=True),
    )
    if benchmark is None:
        raise ProfileActivationError("physical TB2.1 profile is not registered")
    rows = list(await session.scalars(
        select(Task).where(Task.benchmark_id == PROFILE).order_by(Task.id)
        .execution_options(populate_existing=True),
    ))
    profile_facts = snapshot_profile(benchmark)
    task_facts = {row.id: snapshot_task(row) for row in rows}
    current = await audit_tb21_profile(session, object_store=object_store)
    audit_evidence.require_current_snapshot(current)
    lock = load_tb21_lock()
    current.require_exact_profile(PROFILE, task_count=len(lock.tasks))
    spec = profile_spec()
    validation = AdapterValidation(
        contract=CONTRACT, profile_facts=profile_facts, task_facts=task_facts,
        legacy_source_authority=True, audit_record=validation_record(current),
        legacy_source_task_ids=tuple(sorted(task_facts)),
    )
    return spec, validation, current


def validation_record(current: AuditResult) -> dict[str, object]:
    """Existing operator audit facts, independently of runtime qualification."""
    from datetime import UTC, datetime

    return {
        "schema_version": 1, "snapshot_id": current.snapshot_id,
        "verified_bundles": current.verified_bundles,
        "verified_at": datetime.now(UTC).isoformat(),
    }
