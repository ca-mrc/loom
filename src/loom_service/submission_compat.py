"""Shared tenant-scoped agent/task admission for service submissions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from loom.db.schema import Task
from loom.db.task_set_visibility import visible_tasks
from loom.models.batch import Combination
from loom_service.agent_catalog import AgentEntry, get_agent
from loom_service.task_compat import agent_task_compatibility


class AgentTaskIncompatibilityError(HTTPException):
    """A capability mismatch that batch creation records as a rejection metric."""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=400, detail=detail)


def _submission_agent_names(
    *,
    combinations: Sequence[Combination | Mapping[str, Any]],
    trial_config: Mapping[str, Any],
) -> list[str]:
    """Extract requested names without normalizing persisted configuration."""
    if combinations:
        names: list[str] = []
        for index, combination in enumerate(combinations):
            raw_name = (
                combination.agent_name
                if isinstance(combination, Combination)
                else combination.get("agent_name")
            )
            if not isinstance(raw_name, str) or not raw_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"combinations[{index}].agent_name must be a non-empty string",
                )
            names.append(raw_name)
        return names

    raw_name = trial_config.get("agent_name")
    return [raw_name] if isinstance(raw_name, str) and raw_name else []


def _resolved_submission_agents(
    names: Sequence[str],
) -> list[tuple[str, AgentEntry]]:
    """Resolve aliases to catalog entries, retaining request names for diagnostics."""
    resolved: list[tuple[str, AgentEntry]] = []
    canonical_names: set[str] = set()
    for name in names:
        entry = get_agent(name)
        if entry is None:
            raise HTTPException(
                status_code=400,
                detail=(f"unknown agent_name {name!r}. GET /api/v1/agents for the catalog."),
            )
        if entry.name not in canonical_names:
            canonical_names.add(entry.name)
            resolved.append((name, entry))
    return resolved


def _resolved_agent_task_pairs(
    agent_task_pairs: Sequence[tuple[str, str]],
) -> list[tuple[str, str, AgentEntry]]:
    """Resolve ``(task_id, agent_name)`` pairs without expanding coordinates."""
    resolved: list[tuple[str, str, AgentEntry]] = []
    canonical_pairs: set[tuple[str, str]] = set()
    for task_id, requested_name in agent_task_pairs:
        entry = get_agent(requested_name)
        if entry is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"unknown agent_name {requested_name!r}. "
                    "GET /api/v1/agents for the catalog."
                ),
            )
        canonical_pair = (task_id, entry.name)
        if canonical_pair not in canonical_pairs:
            canonical_pairs.add(canonical_pair)
            resolved.append((task_id, requested_name, entry))
    return resolved


async def _visible_task_contracts(
    session: Any,
    *,
    team_id: UUID | None,
    task_ids: Sequence[str],
) -> dict[str, tuple[Mapping[str, Any], Any, str | None]]:
    requested_task_ids = list(dict.fromkeys(task_ids))
    if not requested_task_ids:
        return {}
    rows = (
        await session.execute(
            visible_tasks(team_id=team_id, lock_task_sets=True)
            .with_only_columns(
                Task.id,
                Task.config,
                Task.tags,
                Task.benchmark_id,
            )
            .where(Task.id.in_(requested_task_ids)),
        )
    ).all()
    tasks = {
        str(task_id): (
            config if isinstance(config, Mapping) else {},
            tags,
            str(benchmark_id) if benchmark_id is not None else None,
        )
        for task_id, config, tags, benchmark_id in rows
    }
    if set(tasks) != set(requested_task_ids):
        raise HTTPException(status_code=404, detail="task not found")
    return tasks


async def validate_submission_agent_task_pairs(
    session: Any,
    *,
    team_id: UUID | None,
    agent_task_pairs: Sequence[tuple[str, str]],
) -> None:
    """Reject missing/invisible tasks and incompatible exact coordinates.

    Each pair is ``(task_id, agent_name)``. Unlike ordinary submission
    admission, this function never creates an agent×task cross-product.
    """
    resolved_pairs = _resolved_agent_task_pairs(agent_task_pairs)
    if not resolved_pairs:
        return
    requested_task_ids = list(dict.fromkeys(task_id for task_id, _, _ in resolved_pairs))
    tasks = await _visible_task_contracts(
        session,
        team_id=team_id,
        task_ids=requested_task_ids,
    )

    offenders: dict[str, list[tuple[str, frozenset[str], frozenset[str]]]] = {}
    for task_id, requested_name, entry in resolved_pairs:
        config, raw_tags, benchmark_id = tasks[task_id]
        compatibility = agent_task_compatibility(
            config,
            agent_requires=entry.requires_capabilities,
            agent_provides=entry.provides_capabilities,
            tags=dict(raw_tags) if isinstance(raw_tags, Mapping) else None,
            benchmark_id=benchmark_id,
        )
        if not compatibility.compatible:
            offenders.setdefault(requested_name, []).append(
                (
                    task_id,
                    compatibility.missing_from_task,
                    compatibility.missing_from_agent,
                ),
            )

    if not offenders:
        return

    pairs: list[str] = []
    for name, failures in offenders.items():
        task_id, missing_from_task, missing_from_agent = failures[0]
        reasons: list[str] = []
        if missing_from_task:
            reasons.append(f"task missing {sorted(missing_from_task)}")
        if missing_from_agent:
            reasons.append(f"agent missing {sorted(missing_from_agent)}")
        pairs.append(f"{name}: {len(failures)} task(s) (e.g. {task_id}; {', '.join(reasons)})")
    raise AgentTaskIncompatibilityError(
        f"agent×task capability mismatch — {'; '.join(pairs)}. The listed agents "
        "cannot run these tasks at the platform level (e.g. oracle requires a "
        "benchmark adapter that ships `solution/solve.sh`). Choose a compatible "
        "agent or task.",
    )


async def validate_submission_agent_task_compatibility(
    session: Any,
    *,
    team_id: UUID | None,
    task_ids: Sequence[str],
    combinations: Sequence[Combination | Mapping[str, Any]] = (),
    trial_config: Mapping[str, Any],
) -> None:
    """Reject missing/invisible tasks and incompatible agent/task pairs.

    Every lookup is constrained by ``visible_tasks`` so an inaccessible
    TaskSet task is indistinguishable from a missing task. The original
    requested agent spelling is retained in errors; aliases are resolved only
    for their immutable catalog capabilities.
    """
    requested_task_ids = list(dict.fromkeys(task_ids))
    requested_agents = _submission_agent_names(
        combinations=combinations,
        trial_config=trial_config,
    )
    if not requested_task_ids:
        _resolved_submission_agents(requested_agents)
        return
    if not requested_agents:
        await _visible_task_contracts(
            session,
            team_id=team_id,
            task_ids=requested_task_ids,
        )
        return
    await validate_submission_agent_task_pairs(
        session,
        team_id=team_id,
        agent_task_pairs=[
            (task_id, agent_name)
            for agent_name in requested_agents
            for task_id in requested_task_ids
        ],
    )
