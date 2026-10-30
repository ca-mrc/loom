"""Service-side helper: seed family-run state on batch accept (#672 PR-3).

Called from ``loom_service.routes.batches._create_batch_record`` when
either the trial_config or the benchmark catalog opts a batch into
family-run mode. Responsibilities:

1. Resolve the effective ``ResolvedFamilyRunSpec`` from the catalog
   default + trial_config override.
2. Materialise per-family state (empty tarball) via the configured
   ``state_backend`` plugin.
3. Insert ``batch_family_state`` rows with the ordered task sequences.
4. Return a ``task_id -> family_key`` mapping so the fanout path can
   stamp ``trials.family_key`` when it submits each trial to the CP.

Kept as a standalone module so the batches route stays testable at
the shape level: a fake ``state_backend`` covers the wire path
without standing up MinIO, and the resolver / seeder in
``loom.family_run`` cover the plugin dispatch on their own.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import BatchFamilyState
from loom.family_run.protocols import TaskLike
from loom.family_run.resolve import FamilyRunNotEnabledError, resolve_family_run_spec
from loom.family_run.spec import FamilyRunSpec, ResolvedFamilyRunSpec
from loom.family_run.submit import seed_family_state
from loom.family_run.validation import normalize_evolver_provider_connection
from loom_service.provider_connection_lookup import validate_provider_connection


@dataclass(frozen=True)
class SeededFamilyRun:
    """Return value of :func:`prepare_family_run_state`."""

    resolved_spec: ResolvedFamilyRunSpec
    task_id_to_family_key: dict[str, str]


class _TaskShim:
    """Minimal TaskLike shim over a SQLAlchemy Task row.

    The family-run key extractors read ``id`` and ``tags`` — anything
    else on the ORM row would come along for the ride but is unused.
    Building a dataclass keeps the extractor test doubles clean.
    """

    __slots__ = ("id", "tags")

    def __init__(self, task_id: str, tags: dict[str, str] | list[str] | None) -> None:
        self.id = task_id
        self.tags = tags


async def prepare_family_run_state(
    *,
    session: AsyncSession,
    batch_id: UUID,
    submission_team_id: UUID,
    tasks: list[Any],
    catalog_default: FamilyRunSpec | None,
    override: FamilyRunSpec | None,
    state_backend: Any,
) -> SeededFamilyRun | None:
    """Resolve the spec, seed per-family state, insert batch_family_state.

    Returns ``None`` when family-run mode is not enabled for this batch
    (neither the catalog nor the override opted in).

    Raises :class:`ValueError` when the resolver rejects a partial spec —
    the caller translates that into a 400 with a structured reason.
    """
    try:
        resolved = resolve_family_run_spec(
            catalog=catalog_default,
            override=override,
        )
    except FamilyRunNotEnabledError:
        return None

    resolved, evolver_provider_connection_id = normalize_evolver_provider_connection(
        resolved,
    )
    if evolver_provider_connection_id is not None:
        await validate_provider_connection(
            session,
            evolver_provider_connection_id,
            team_id=submission_team_id,
            # Evolver adapter calls are outside hosted Terminus-2 Chat pools.
            submission_configs=[{"agent_name": "family-evolver"}],
        )

    task_shims: list[TaskLike] = [
        _TaskShim(task_id=str(t.id), tags=getattr(t, "tags", None))
        for t in tasks
    ]

    seeds = await seed_family_state(
        batch_id=batch_id,
        tasks=task_shims,
        resolved=resolved,
        state_backend=state_backend,
    )

    task_id_to_family_key: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for seed in seeds:
        for task_id in seed.task_sequence:
            task_id_to_family_key[task_id] = seed.family_key
        rows.append({
            "batch_id": batch_id,
            "family_key": seed.family_key,
            "task_sequence": list(seed.task_sequence),
            "current_index": 0,
            "state": "pending",
            "state_uri": seed.state_uri,
            "attempt_count": 0,
        })
    if rows:
        await session.execute(insert(BatchFamilyState), rows)

    return SeededFamilyRun(
        resolved_spec=resolved,
        task_id_to_family_key=task_id_to_family_key,
    )
