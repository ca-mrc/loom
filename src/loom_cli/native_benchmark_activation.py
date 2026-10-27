"""Compatibility wrapper for the shared, format-neutral catalog admission."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.benchmark_admission import admit_benchmark_profile
from loom.db.schema import Benchmark, Task
from loom.models.benchmark_profile import BenchmarkProfileSpec
from loom.models.harbor import HarborBenchmarkSpec


async def activate_native_benchmark(
    session: AsyncSession, *, spec: HarborBenchmarkSpec,
) -> int:
    """Normalize the legacy native descriptor; all mutation is shared admission."""
    benchmark = await session.get(Benchmark, spec.id)
    provenance = benchmark.profile_provenance or {} if benchmark is not None else {}
    selected = tuple(provenance.get("selected_task_ids") or ())
    if not selected:
        # Compatibility with native profiles published before exact selected
        # identities were persisted. Registered source authority still binds
        # every observed task/config; the supplied full/subset census is checked.
        selected = tuple(
            identity.removeprefix(spec.id + "/")
            for identity in await session.scalars(
                select(Task.id).where(Task.benchmark_id == spec.id).order_by(Task.id),
            )
        )
    neutral = BenchmarkProfileSpec.from_harbor(spec, selected_task_ids=selected)
    return await admit_benchmark_profile(session, spec=neutral)
