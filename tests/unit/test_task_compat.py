"""Unit tests for `loom_service.task_compat` (#320).

Pin the (agent, task) capability-matching heuristics that the batch
preflight uses to skip combos that would deterministically AgentError
mid-trial (the original case: oracle vs. non-pytest benchmarks)."""

from __future__ import annotations

import pytest

from loom_service import task_compat
from loom_service.agent_catalog import get_agent
from loom_service.task_compat import (
    filter_tasks_by_agent_capability,
    task_provides_capability,
    task_supports_agent,
)


def _config(
    verifier_name: str,
    *,
    task_id: str = "local/task-0",
) -> dict[str, object]:
    """Minimal TaskConfig-shaped dict — only the field the heuristic
    looks at (`verifier.name`) needs to be present for most cases."""
    return {"task": {"id": task_id}, "verifier": {"name": verifier_name}}


def test_oracle_agent_declares_solution_solve_sh_requirement() -> None:
    """Wiring guard: oracle MUST advertise the solve.sh requirement
    so the preflight knows to filter incompatible tasks. Regression
    against silent removal of `requires_capabilities`."""
    oracle = get_agent("oracle")
    assert oracle is not None
    assert "solution_solve_sh" in oracle.requires_capabilities
    assert oracle.provides_capabilities == frozenset({"workspace_exec"})


def test_litellm_agent_has_no_hard_capability_requirements() -> None:
    """The direct completion runtime has no task prerequisites, but it
    also does not claim a workspace execution surface."""
    litellm = get_agent("litellm")
    assert litellm is not None
    assert litellm.requires_capabilities == frozenset()
    assert litellm.provides_capabilities == frozenset()


def test_terminus_2_provides_workspace_execution() -> None:
    terminus = get_agent("terminus-2")
    assert terminus is not None
    assert terminus.provides_capabilities == frozenset({"workspace_exec"})


def test_agent_task_compatibility_reports_missing_agent_capability() -> None:
    cfg = {
        **_config("script"),
        "required_agent_capabilities": ["workspace_exec"],
    }

    compatibility = getattr(task_compat, "agent_task_compatibility", None)
    assert compatibility is not None
    result = compatibility(
        cfg,
        agent_requires=frozenset(),
        agent_provides=frozenset(),
    )

    assert result.compatible is False
    assert result.missing_from_agent == frozenset({"workspace_exec"})
    assert result.missing_from_task == frozenset()


def test_immutable_tb21_profile_requires_workspace_exec_without_config_mutation() -> None:
    result = task_compat.agent_task_compatibility(
        _config(
            "script",
            task_id="terminal-bench-2@tb2.1-r6/chess-best-move",
        ),
        agent_requires=frozenset(),
        agent_provides=frozenset(),
        benchmark_id="terminal-bench-2@tb2.1-r6",
    )

    assert result.compatible is False
    assert result.missing_from_agent == frozenset({"workspace_exec"})
    assert result.missing_from_task == frozenset()


@pytest.mark.parametrize(("dockerfile", "requires_workspace"), [
    ("environment/Dockerfile.loom-nebius", True), (None, False),
])
def test_task_that_builds_its_own_image_requires_workspace_exec(
    dockerfile: str | None, requires_workspace: bool,
) -> None:
    # Response-only direct-completion never runs in a task-built image (#2054).
    cfg = {**_config("script"), "environment": {"dockerfile": dockerfile}}
    for provides, compatible in ((frozenset(), not requires_workspace), (frozenset({"workspace_exec"}), True)):
        result = task_compat.agent_task_compatibility(
            cfg, agent_requires=frozenset(), agent_provides=provides,
        )
        assert result.compatible is compatible


def test_agent_task_compatibility_requires_both_directions() -> None:
    cfg = {
        **_config("script"),
        "required_agent_capabilities": ["workspace_exec"],
    }

    compatibility = getattr(task_compat, "agent_task_compatibility", None)
    assert compatibility is not None
    result = compatibility(
        cfg,
        agent_requires=frozenset({"solution_solve_sh"}),
        agent_provides=frozenset({"workspace_exec"}),
    )

    assert result.compatible is False
    assert result.missing_from_agent == frozenset()
    assert result.missing_from_task == frozenset({"solution_solve_sh"})


def test_pytest_verifier_task_provides_solve_sh_capability() -> None:
    """mbpp/humaneval/livecodebench all use the pytest verifier and
    every one of those adapters co-emits `solution/solve.sh` next to
    `_reference.py`. So pytest⇒solve.sh is the post-#388/#414
    invariant the heuristic relies on."""
    assert task_provides_capability(
        _config("pytest"), "solution_solve_sh",
    ) is True


def test_script_verifier_task_does_not_provide_solve_sh() -> None:
    """aime/gpqa/mmlu-pro tasks use the `script` verifier and ship no
    solve.sh. Oracle can't run them — this is the failure pattern #320
    set out to block at submit time."""
    assert task_provides_capability(
        _config("script"), "solution_solve_sh",
    ) is False


def test_tb21_oracle_capability_uses_explicit_tag() -> None:
    """The active TB2.1 profile publishes Oracle eligibility per task."""
    assert task_provides_capability(
        _config(
            "script",
            task_id="terminal-bench-2@tb2.1-r6/chess-best-move",
        ),
        "solution_solve_sh",
        tags={"oracle_eligible": "true"},
    ) is True


def test_tagless_legacy_terminal_bench_id_has_no_oracle_capability() -> None:
    """Retired TB2 rows cannot regain Oracle eligibility from their id."""
    assert task_provides_capability(
        _config(
            "script",
            task_id="terminal-bench-2/simple-web-scraper",
        ),
        "solution_solve_sh",
    ) is False


def test_explicit_false_tag_overrides_pytest_convention() -> None:
    """An explicit eligibility decision wins over the pytest convention."""
    assert task_provides_capability(
        _config("pytest", task_id="custom/pytest-without-solution"),
        "solution_solve_sh",
        tags={"oracle_eligible": "false"},
    ) is False


def test_oracle_eligible_tag_grants_solve_sh_capability_for_script_verifier() -> None:
    """SkillLearnBench uses the script verifier across all 100 tasks,
    but 73 of them DO ship `solution/solve.sh` upstream. The adapter
    emits `oracle_eligible=true` per-instance; the capability check
    honors that marker so those tasks can run under the oracle agent."""
    assert task_provides_capability(
        _config("script", task_id="skilllearnbench/court-form-filling/court-form-filling-1"),
        "solution_solve_sh",
        tags={"oracle_eligible": "true", "method": "human_authored"},
    ) is True


def test_oracle_eligible_false_tag_keeps_task_incompatible() -> None:
    """The 27 SLB tasks without upstream `solve.sh` tag
    `oracle_eligible=false`; the capability check must keep treating
    them as incompatible so the oracle agent doesn't try to run them."""
    assert task_provides_capability(
        _config("script", task_id="skilllearnbench/schedule-planning/schedule-planning-1"),
        "solution_solve_sh",
        tags={"oracle_eligible": "false", "method": "human_authored"},
    ) is False


def test_oracle_eligible_tag_without_tags_arg_is_ignored() -> None:
    """Back-compat: legacy callers that don't pass `tags=` see the
    pre-tag behavior — script-verifier tasks remain incompatible."""
    assert task_provides_capability(
        _config("script", task_id="skilllearnbench/foo/bar"),
        "solution_solve_sh",
    ) is False


def test_filter_tasks_consumes_per_task_tags() -> None:
    """`task_tags` is a parallel id→tags mapping; the filter passes
    each task's tags to the capability check so SLB's heterogeneous
    oracle slate splits correctly."""
    configs = {
        "skilllearnbench/court-form-filling/court-form-filling-1": _config(
            "script",
            task_id="skilllearnbench/court-form-filling/court-form-filling-1",
        ),
        "skilllearnbench/schedule-planning/schedule-planning-1": _config(
            "script",
            task_id="skilllearnbench/schedule-planning/schedule-planning-1",
        ),
    }
    tags = {
        "skilllearnbench/court-form-filling/court-form-filling-1": {
            "oracle_eligible": "true",
        },
        "skilllearnbench/schedule-planning/schedule-planning-1": {
            "oracle_eligible": "false",
        },
    }
    compat, incompat = filter_tasks_by_agent_capability(
        task_configs=configs,
        required=frozenset({"solution_solve_sh"}),
        task_tags=tags,
    )
    assert compat == [
        "skilllearnbench/court-form-filling/court-form-filling-1",
    ]
    assert incompat == [
        "skilllearnbench/schedule-planning/schedule-planning-1",
    ]


def test_unknown_capability_fails_closed() -> None:
    """Typo'd capability names must not silently grant access — they
    return False so a misconfigured agent gets filtered out rather
    than passing every preflight."""
    assert task_provides_capability(
        _config("pytest"), "nonexistent_capability",
    ) is False


def test_task_supports_agent_passes_when_requirements_empty() -> None:
    """Agents with empty `requires_capabilities` (litellm, all
    subprocess agents) always pass — they have no platform-level
    hard requirements on task shape."""
    assert task_supports_agent(_config("script"), frozenset()) is True


def test_task_supports_agent_requires_all_capabilities() -> None:
    """Multi-capability requirements are conjunctive — missing any
    one disqualifies the task."""
    cfg = _config("pytest")  # provides solution_solve_sh
    assert task_supports_agent(
        cfg, frozenset({"solution_solve_sh"}),
    ) is True
    assert task_supports_agent(
        cfg, frozenset({"solution_solve_sh", "nonexistent_capability"}),
    ) is False


def test_filter_tasks_by_agent_capability_splits_compat_and_incompat() -> None:
    """The (compatible, incompatible) split mirrors the matrix
    runner's #316 evidence: tasks with oracle-compatible solution
    wrappers land in `compatible`, script-only tasks land in
    `incompatible`."""
    configs = {
        "mbpp/100": _config("pytest"),
        "aime-25/2025-I/2": _config("script"),
        "humaneval/0": _config("pytest"),
        "terminal-bench-2/simple-web-scraper": _config(
            "script",
            task_id="terminal-bench-2/simple-web-scraper",
        ),
        "gpqa/q1": _config("script"),
    }
    compat, incompat = filter_tasks_by_agent_capability(
        task_configs=configs, required=frozenset({"solution_solve_sh"}),
    )
    assert compat == [
        "mbpp/100",
        "humaneval/0",
    ]
    assert incompat == [
        "aime-25/2025-I/2",
        "terminal-bench-2/simple-web-scraper",
        "gpqa/q1",
    ]


def test_filter_with_no_requirements_returns_everything_as_compatible() -> None:
    """Empty `required` short-circuits — no DB cost, no filtering."""
    configs = {
        "a": _config("pytest"),
        "b": _config("script"),
    }
    compat, incompat = filter_tasks_by_agent_capability(
        task_configs=configs, required=frozenset(),
    )
    assert compat == ["a", "b"]
    assert incompat == []
