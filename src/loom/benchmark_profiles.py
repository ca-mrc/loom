"""Resolve benchmark selectors and enforce physical-profile lifecycle state."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.catalog_policy import CURRENTLY_UNSUPPORTED_BENCHMARK_IDS, catalog_support_policy
from loom.db.schema import Benchmark, BenchmarkAlias


@dataclass(frozen=True)
class ResolvedBenchmarkSelectors:
    """Physical benchmark profiles and their requested-selector audit trail."""

    physical_ids: tuple[str, ...]
    provenance: tuple[dict[str, str], ...]


def benchmark_not_runnable_error(profile_id: str, execution_state: str) -> HTTPException:
    """Build the shared machine-readable rejection for a non-runnable profile."""

    reason = "benchmark_retired" if execution_state == "historical" else "benchmark_not_runnable"
    lifecycle = "historical" if execution_state == "historical" else execution_state
    return HTTPException(
        status_code=409,
        detail={
            "reason": reason,
            "benchmark_profile": profile_id,
            "message": (
                f"{lifecycle} benchmark profile {profile_id!r} "
                "cannot be selected for a new submission"
            ),
        },
    )


async def reject_non_runnable_benchmark_profiles(
    session: AsyncSession,
    profile_ids: Sequence[str],
) -> None:
    """Reject every non-runnable profile at a new-execution boundary."""

    requested_profiles = tuple(dict.fromkeys(profile_ids))
    if not requested_profiles:
        return
    unavailable = (
        await session.execute(
            select(Benchmark.id, Benchmark.execution_state)
            .where(Benchmark.id.in_(requested_profiles))
            .where(Benchmark.execution_state != "runnable")
            .order_by(Benchmark.id.asc()),
        )
    ).first()
    if unavailable is not None:
        profile_id, execution_state = unavailable
        raise benchmark_not_runnable_error(str(profile_id), str(execution_state))
    unsupported_profiles = sorted(set(requested_profiles) & CURRENTLY_UNSUPPORTED_BENCHMARK_IDS)
    if unsupported_profiles:
        profile_id = await session.scalar(
            select(Benchmark.id).where(Benchmark.id.in_(unsupported_profiles)).order_by(Benchmark.id),
        )
        if profile_id is not None:
            policy = catalog_support_policy(str(profile_id))
            raise HTTPException(status_code=409, detail={
                "reason": policy.state,
                "benchmark_profile": str(profile_id),
                "message": policy.message,
            })


async def resolve_benchmark_selectors(
    session: AsyncSession,
    selectors: Sequence[str],
    *,
    require_runnable: bool,
) -> ResolvedBenchmarkSelectors:
    """Resolve aliases and optionally reject non-runnable physical profiles.

    Unknown selectors deliberately remain unchanged so the existing task-filter
    contract still produces an empty selection rather than a new 404.
    """

    requested = tuple(selectors)
    if not requested:
        return ResolvedBenchmarkSelectors(physical_ids=(), provenance=())

    aliases = {
        str(alias): str(benchmark_id)
        for alias, benchmark_id in (
            await session.execute(
                select(BenchmarkAlias.alias, BenchmarkAlias.benchmark_id).where(
                    BenchmarkAlias.alias.in_(requested),
                ),
            )
        ).all()
    }
    physical_ids = tuple(aliases.get(selector, selector) for selector in requested)
    if require_runnable:
        await reject_non_runnable_benchmark_profiles(session, physical_ids)

    return ResolvedBenchmarkSelectors(
        physical_ids=physical_ids,
        provenance=tuple(
            {
                "kind": "benchmark_profile_selection",
                "requested_selector": selector,
                "resolved_profile": physical_id,
            }
            for selector, physical_id in zip(requested, physical_ids, strict=True)
        ),
    )


__all__ = [
    "ResolvedBenchmarkSelectors",
    "benchmark_not_runnable_error",
    "reject_non_runnable_benchmark_profiles",
    "resolve_benchmark_selectors",
]
