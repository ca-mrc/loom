"""Agent ↔ task capability matching (#320).

Filters incompatible (agent, task) pairs at `POST /api/v1/batches`
preflight so the matrix runner / SPA / CLI never launch a trial that
will deterministically fail at the worker with
`AgentError: OracleAgent requires .../solution/solve.sh; not found`.

Capability vocabulary is intentionally small in V1 — only
`solution_solve_sh` (the oracle case from #320 evidence). Adding a
capability is two steps: declare it in `AgentEntry.requires_capabilities`
on the consuming agent, and teach `task_provides_capability` how to
recognize it on a `TaskConfig`-shaped dict (or `tags` for adapters
that publish a per-instance marker — e.g. SkillLearnBench's
`oracle_eligible` tag covers a benchmark whose 100 tasks include 27
without an upstream `solve.sh`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# Convention: current pytest adapters co-emit `solution/solve.sh` next to a
# `solution/_reference.py` (the post-#388/#414 stub layout). Mixed adapters
# must publish an explicit per-task eligibility tag.
_PYTEST_VERIFIER = "pytest"
_ORACLE_ELIGIBLE_TAG = "oracle_eligible"
_IMMUTABLE_WORKSPACE_EXEC_BENCHMARK_IDS = frozenset(
    {"terminal-bench-2@tb2.1-r6"},
)


@dataclass(frozen=True)
class AgentTaskCompatibility:
    """Missing capabilities in each direction for one agent/task pair."""

    missing_from_task: frozenset[str]
    missing_from_agent: frozenset[str]

    @property
    def compatible(self) -> bool:
        return not self.missing_from_task and not self.missing_from_agent


def task_required_agent_capabilities(
    task_config: Mapping[str, Any],
    *,
    benchmark_id: str | None = None,
) -> frozenset[str]:
    raw = task_config.get("required_agent_capabilities")
    declared = (
        frozenset(item for item in raw if isinstance(item, str))
        if isinstance(raw, (list, tuple, set, frozenset))
        else frozenset()
    )
    if benchmark_id in _IMMUTABLE_WORKSPACE_EXEC_BENCHMARK_IDS:
        return declared | {"workspace_exec"}
    # A task that builds its own image never runs in the response-only runner
    # image, so only a workspace harness can execute it (#2054).
    environment = task_config.get("environment")
    if isinstance(environment, Mapping) and environment.get("dockerfile"):
        return declared | {"workspace_exec"}
    return declared


def agent_task_compatibility(
    task_config: Mapping[str, Any],
    *,
    agent_requires: Iterable[str],
    agent_provides: Iterable[str],
    tags: Mapping[str, str] | None = None,
    benchmark_id: str | None = None,
) -> AgentTaskCompatibility:
    required_from_task = frozenset(agent_requires)
    provided_by_agent = frozenset(agent_provides)
    missing_from_task = frozenset(
        capability
        for capability in required_from_task
        if not task_provides_capability(task_config, capability, tags=tags)
    )
    missing_from_agent = (
        task_required_agent_capabilities(
            task_config,
            benchmark_id=benchmark_id,
        )
        - provided_by_agent
    )
    return AgentTaskCompatibility(
        missing_from_task=missing_from_task,
        missing_from_agent=missing_from_agent,
    )


def task_provides_capability(
    task_config: Mapping[str, Any],
    capability: str,
    *,
    tags: Mapping[str, str] | None = None,
) -> bool:
    """Best-effort derivation: does the materialized bundle expose
    the named agent capability?

    `solution_solve_sh` is granted when (a) the per-task
    `oracle_eligible` tag is `"true"`, or (b) the task's verifier is
    `pytest`. An explicit `oracle_eligible="false"` tag wins over the
    pytest convention so a heterogeneous oracle slate (for example,
    SkillLearnBench or Terminal-Bench-2 tasks that lack an upstream
    solution) is honored.
    A future adapter that breaks the convention should add an explicit
    capability marker instead of broadening every script verifier task.

    Unknown capabilities return False — fail closed so a typo in
    `AgentEntry.requires_capabilities` doesn't silently permit
    everything.
    """
    if capability == "solution_solve_sh":
        if tags is not None:
            tag_value = tags.get(_ORACLE_ELIGIBLE_TAG)
            if tag_value == "true":
                return True
            if tag_value == "false":
                return False
        verifier = task_config.get("verifier") or {}
        if isinstance(verifier, Mapping):
            if verifier.get("name") == _PYTEST_VERIFIER:
                return True
        return False
    return False


def task_supports_agent(
    task_config: Mapping[str, Any],
    required: Iterable[str],
    *,
    tags: Mapping[str, str] | None = None,
) -> bool:
    """True iff the task provides EVERY capability the agent requires.
    Agents with empty `requires_capabilities` always pass."""
    return all(
        task_provides_capability(task_config, cap, tags=tags)
        for cap in required
    )


def filter_tasks_by_agent_capability(
    *,
    task_configs: Mapping[str, Mapping[str, Any]],
    required: Iterable[str],
    task_tags: Mapping[str, Mapping[str, str]] | None = None,
) -> tuple[list[str], list[str]]:
    """Split `task_configs` (id → config dict) into `(compatible_ids,
    incompatible_ids)` in input-order. Used by the batch-create
    preflight to surface counts + concrete examples without leaking
    raw configs back to the caller.
    """
    required_set = frozenset(required)
    if not required_set:
        return list(task_configs.keys()), []
    compatible: list[str] = []
    incompatible: list[str] = []
    for task_id, config in task_configs.items():
        tags = task_tags.get(task_id) if task_tags else None
        if task_supports_agent(config, required_set, tags=tags):
            compatible.append(task_id)
        else:
            incompatible.append(task_id)
    return compatible, incompatible
