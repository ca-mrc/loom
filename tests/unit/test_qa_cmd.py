"""Unit tests for `loom qa matrix` helpers."""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from loom_cli import qa_cmd


class _CatalogClient:
    def get(self, path: str) -> httpx.Response:
        request = httpx.Request("GET", f"http://loom.test{path}")
        if path == "/api/v1/agents":
            return httpx.Response(
                200,
                request=request,
                json={
                    "items": [
                        {
                            "name": "direct-completion",
                            "aliases": ["litellm"],
                            "service_mode_ready": True,
                        },
                        {
                            "name": "oracle",
                            "aliases": [],
                            "service_mode_ready": True,
                        },
                    ],
                },
            )
        if path == "/api/v1/benchmarks":
            return httpx.Response(200, request=request, json={"items": []})
        raise AssertionError(f"unexpected path {path}")

# ─── _provider_compatible ──────────────────────────────────────────


def test_provider_compatible_wildcard() -> None:
    """Generic agents (litellm, aider) accept any provider."""
    a = {"supported_providers": ["*"]}
    assert qa_cmd._provider_compatible(a, "openai") is True
    assert qa_cmd._provider_compatible(a, "anthropic") is True


def test_provider_compatible_locked() -> None:
    """Provider-locked agents (claude-code, gemini-cli) reject non-matching."""
    a = {"supported_providers": ["anthropic"]}
    assert qa_cmd._provider_compatible(a, "anthropic") is True
    assert qa_cmd._provider_compatible(a, "openai") is False


def test_provider_compatible_empty_treated_as_no_match() -> None:
    """No declared providers = no compat (oracle has none but
    needs_model=False so this branch isn't hit in practice)."""
    a = {"supported_providers": []}
    assert qa_cmd._provider_compatible(a, "openai") is False


def test_fetch_catalogs_resolves_legacy_agent_alias_to_canonical_entry() -> None:
    agents, _benchmarks = qa_cmd._fetch_catalogs(
        _CatalogClient(),  # type: ignore[arg-type]
        agent_filter={"litellm", "direct-completion"},
        benchmark_filter=None,
    )

    assert [agent["name"] for agent in agents] == ["direct-completion"]


# ─── _build_cells_and_combinations ─────────────────────────────────


def _agent(name: str, *, needs_model: bool = True,
           supported: list[str] | None = None) -> dict:
    return {
        "name": name,
        "needs_model": needs_model,
        "supported_providers": supported or ["*"],
    }


def _bench(bid: str) -> dict:
    return {"id": bid}


def test_build_cells_marks_no_task_skipped() -> None:
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("aider")],
        benchmarks=[_bench("humaneval"), _bench("empty-bench")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    states = {(c.benchmark, c.state) for c in cells}
    assert ("humaneval", "PENDING") in states
    assert ("empty-bench", "SKIPPED") in states
    empty = next(c for c in cells if c.benchmark == "empty-bench")
    assert "no runnable tasks" in (empty.reason or "")
    # aider stays in combinations because at least one cell is PENDING
    assert any(c["agent_name"] == "aider" for c in combos)


def test_build_cells_marks_provider_mismatch_skipped() -> None:
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("claude-code", supported=["anthropic"])],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert len(cells) == 1
    assert cells[0].state == "SKIPPED"
    assert "anthropic" in (cells[0].reason or "")
    # No combinations submitted for an agent with zero PENDING cells.
    assert combos == []


def test_build_cells_no_model_agent_includes_oracle() -> None:
    """needs_model=False agents (oracle) always compat and get a
    null agent_model in the combination."""
    cells, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("oracle", needs_model=False, supported=[])],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert len(cells) == 1
    assert cells[0].state == "PENDING"
    assert combos == [{"agent_name": "oracle", "agent_model": None}]


def test_build_cells_model_combination_carries_provider_and_model() -> None:
    _, combos = qa_cmd._build_cells_and_combinations(
        agents=[_agent("aider")],
        benchmarks=[_bench("humaneval")],
        task_ids_by_benchmark={"humaneval": "humaneval/0"},
        provider_family="openai",
        model="gpt-4o-mini",
    )
    assert combos == [{
        "agent_name": "aider",
        "agent_model": {
            "provider": "openai",
            "name": "gpt-4o-mini",
            "source": "api",
        },
    }]


# ─── _classify_trial ───────────────────────────────────────────────


def test_classify_trial_succeeded_with_reward() -> None:
    trial = {
        "state": "succeeded",
        "result": {"aggregate_reward": 0.75},
    }
    state, reason, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reason is None
    assert reward == 0.75


def test_classify_trial_succeeded_with_zero_reward_still_passes() -> None:
    """Per the #316 spec: reward=0 with verifier output IS a platform pass."""
    trial = {"state": "succeeded", "result": {"aggregate_reward": 0}}
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reward == 0.0


def test_classify_trial_failed_records_reason() -> None:
    trial = {
        "state": "failed",
        "failure_reason": "agent_error",
        "failure_message": "exited rc=127; stderr: claude: not found",
    }
    state, reason, reward = qa_cmd._classify_trial(trial)
    assert state == "FAIL_PLATFORM"
    assert "agent_error" in (reason or "")
    assert "rc=127" in (reason or "")
    assert reward is None


def test_classify_trial_non_terminal_is_stuck() -> None:
    state, reason, _ = qa_cmd._classify_trial({"state": "running"})
    assert state == "STUCK"
    assert "running" in (reason or "")


def test_classify_trial_zero_calls_pass_with_needs_model_is_suspect() -> None:
    """#388/#392: a model-using agent that "succeeded" but made no LLM call is
    almost certainly passing on a pre-shipped reference solution
    (the bundle ships the answer; the verifier passes regardless
    of agent contribution). Re-classify as SUSPECT_PASS."""
    trial = {
        "state": "succeeded",
        "aggregate_reward": 1.0,
        "llm_calls_count": 0,
    }
    state, reason, reward = qa_cmd._classify_trial(
        trial, agent_needs_model=True,
    )
    assert state == "SUSPECT_PASS"
    assert "pre-shipped reference solution" in (reason or "")
    assert reward == 1.0


def test_classify_trial_zero_calls_pass_without_needs_model_is_real_pass() -> None:
    """Oracle (needs_model=False) legitimately passes without calling
    the LLM at all. Stay PASS_PLATFORM."""
    trial = {
        "state": "succeeded",
        "aggregate_reward": 1.0,
        "llm_calls_count": 0,
    }
    state, reason, reward = qa_cmd._classify_trial(
        trial, agent_needs_model=False,
    )
    assert state == "PASS_PLATFORM"
    assert reason is None
    assert reward == 1.0


def test_classify_trial_nonzero_calls_pass_is_real_pass() -> None:
    """Model-using agent with LLM calls → genuine PASS, not SUSPECT."""
    trial = {
        "state": "succeeded",
        "aggregate_reward": 1.0,
        "llm_calls_count": 2,
    }
    state, reason, _ = qa_cmd._classify_trial(
        trial, agent_needs_model=True,
    )
    assert state == "PASS_PLATFORM"
    assert reason is None


def test_classify_cells_propagates_needs_model_and_captures_llm_calls() -> None:
    cells = [
        qa_cmd.MatrixCell(agent="aider", benchmark="mbpp", state="PENDING"),
        qa_cmd.MatrixCell(agent="oracle", benchmark="mbpp", state="PENDING"),
    ]
    trials = [
        {
            "id": "t-aider",
            "agent_name": "aider",
            "task_id": "mbpp/100",
            "state": "succeeded",
            "aggregate_reward": 1.0,
            "llm_calls_count": 0,
        },
        {
            "id": "t-oracle",
            "agent_name": "oracle",
            "task_id": "mbpp/100",
            "state": "succeeded",
            "aggregate_reward": 1.0,
            "llm_calls_count": 0,
        },
    ]
    qa_cmd._classify_cells(
        cells, trials, agents_needing_model={"aider"},
    )
    # aider needs model + no LLM calls = SUSPECT
    assert cells[0].state == "SUSPECT_PASS"
    assert cells[0].llm_calls_count == 0
    # oracle doesn't need a model = legitimate PASS
    assert cells[1].state == "PASS_PLATFORM"
    assert cells[1].llm_calls_count == 0


def test_classify_trial_oracle_solve_sh_missing_is_skipped() -> None:
    """OracleAgent fails when the task bundle has no `solution/solve.sh`.
    That's a declared (agent, benchmark) capability mismatch, not a
    platform failure — re-classify to SKIPPED so PASS/FAIL counts
    reflect real platform health."""
    trial = {
        "state": "failed",
        "failure_reason": "agent_error",
        "failure_message": (
            "OracleAgent requires /tmp/loom-trial-abc/solution/solve.sh; "
            "not found"
        ),
    }
    state, reason, _ = qa_cmd._classify_trial(trial)
    assert state == "SKIPPED"
    assert "capability mismatch" in (reason or "")


def test_is_capability_mismatch_recognizes_oracle_solve_sh() -> None:
    assert qa_cmd._is_capability_mismatch(
        "OracleAgent requires /tmp/x/solution/solve.sh; not found",
    )


def test_is_capability_mismatch_does_not_match_generic_failures() -> None:
    """Real platform failures (rc=127, ModuleNotFoundError, gateway
    timeout) must NOT be downgraded to SKIPPED."""
    for msg in [
        "aider exited rc=127 on step main",
        "ModuleNotFoundError: openhands_sdk",
        "Loom gateway timeout",
        "verifier_error: test runner crashed",
    ]:
        assert not qa_cmd._is_capability_mismatch(msg), msg


def test_classify_trial_reads_top_level_aggregate_reward() -> None:
    """The real /api/v1/trials list response exposes
    `aggregate_reward` at the TOP level, not nested under `result`.
    The unit test that uses `result.aggregate_reward` covers the
    detail-view fallback; this one covers the live list shape."""
    trial = {
        "state": "succeeded",
        "aggregate_reward": 1.0,
    }
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "PASS_PLATFORM"
    assert reward == 1.0


def test_classify_cells_reads_top_level_agent_name_and_derives_benchmark() -> None:
    """Real /api/v1/trials list shape: `agent_name` at top level,
    no `benchmark_id` key — benchmark is derived from `task_id`'s
    first path segment. Bug caught when running the matrix live
    against a real cluster."""
    cells = [
        qa_cmd.MatrixCell(agent="oracle", benchmark="mbpp", state="PENDING"),
    ]
    trials = [
        {
            "id": "t-1",
            "agent_name": "oracle",
            "task_id": "mbpp/100",
            "state": "succeeded",
            "aggregate_reward": 1.0,
        },
    ]
    qa_cmd._classify_cells(cells, trials)
    assert cells[0].state == "PASS_PLATFORM"
    assert cells[0].trial_id == "t-1"
    assert cells[0].reward == 1.0


def test_classify_trial_succeeded_without_reward_is_failure() -> None:
    """If a trial says succeeded but emitted no aggregate_reward, the
    matrix should treat that as a platform failure (verifier didn't
    actually produce a numeric reward)."""
    trial = {"state": "succeeded", "result": {}}
    state, _, reward = qa_cmd._classify_trial(trial)
    assert state == "STUCK"
    assert reward is None


# ─── _classify_cells ───────────────────────────────────────────────


def test_classify_cells_matches_trial_to_cell() -> None:
    cells = [
        qa_cmd.MatrixCell(agent="aider", benchmark="humaneval", state="PENDING"),
        qa_cmd.MatrixCell(agent="aider", benchmark="mbpp", state="PENDING"),
    ]
    trials = [
        {
            "id": "trial-1",
            "config": {"agent_name": "aider"},
            "benchmark_id": "humaneval",
            "state": "succeeded",
            "result": {"aggregate_reward": 1.0},
        },
        {
            "id": "trial-2",
            "config": {"agent_name": "aider"},
            "benchmark_id": "mbpp",
            "state": "failed",
            "failure_reason": "verifier_error",
        },
    ]
    qa_cmd._classify_cells(cells, trials)
    assert cells[0].state == "PASS_PLATFORM"
    assert cells[0].trial_id == "trial-1"
    assert cells[0].reward == 1.0
    assert cells[1].state == "FAIL_PLATFORM"
    assert cells[1].trial_id == "trial-2"
    assert cells[1].failure_reason == "verifier_error"


def test_classify_cells_marks_unmatched_stuck() -> None:
    cells = [
        qa_cmd.MatrixCell(agent="aider", benchmark="humaneval", state="PENDING"),
    ]
    qa_cmd._classify_cells(cells, [])  # no trials at all
    assert cells[0].state == "STUCK"
    assert "no trial" in (cells[0].reason or "")


def test_classify_cells_leaves_already_skipped_alone() -> None:
    """SKIPPED cells from the build step (no task, provider mismatch)
    should not be re-classified by trial matching."""
    cells = [
        qa_cmd.MatrixCell(
            agent="claude-code", benchmark="humaneval",
            state="SKIPPED", reason="provider mismatch",
        ),
    ]
    qa_cmd._classify_cells(cells, [])
    assert cells[0].state == "SKIPPED"
    assert cells[0].reason == "provider mismatch"


# ─── _render_markdown ──────────────────────────────────────────────


def test_render_markdown_includes_summary_and_cells() -> None:
    result = qa_cmd.MatrixResult(
        started_at="2026-06-23T00:00:00+00:00",
        finished_at="2026-06-23T00:05:00+00:00",
        cluster_url="http://cluster",
        provider_connection="qa-relay",
        model="gpt-4o-mini",
        batch_ids=["b-1"],
        cells=[
            qa_cmd.MatrixCell(
                agent="aider", benchmark="humaneval",
                state="PASS_PLATFORM", reward=1.0, trial_id="t-1",
            ),
            qa_cmd.MatrixCell(
                agent="aider", benchmark="mbpp",
                state="FAIL_PLATFORM",
                reason="agent_error: rc=127", failure_reason="agent_error",
                trial_id="t-2",
            ),
            qa_cmd.MatrixCell(
                agent="claude-code", benchmark="humaneval",
                state="SKIPPED", reason="provider mismatch",
            ),
        ],
    )
    md = qa_cmd._render_markdown(result)
    assert "qa-relay" in md
    assert "gpt-4o-mini" in md
    assert "b-1" in md
    # Summary counts
    assert "PASS_PLATFORM: 1" in md
    assert "FAIL_PLATFORM: 1" in md
    assert "SKIPPED: 1" in md
    # Cell rows
    assert "aider" in md
    assert "humaneval" in md
    assert "agent_error: rc=127" in md
    # Failures sort first
    fail_idx = md.index("FAIL_PLATFORM")
    pass_idx = md.index("PASS_PLATFORM", fail_idx)  # after fail block
    assert fail_idx < pass_idx


# ─── provider compatibility plan ───────────────────────────────────


def test_provider_compatibility_matrix_records_status_dimensions() -> None:
    matrix = qa_cmd._build_provider_compatibility_matrix()
    payload = qa_cmd._provider_compatibility_matrix_to_json_payload(matrix)

    assert payload["schema_version"] == "provider-harness-compatibility-v1"
    assert payload["issue"] == "https://github.com/ca-mrc/loom/issues/114"
    assert payload["live_provider_calls"] == "not_run"

    cells = {
        (cell["agent"], cell["provider_endpoint_type"]): cell
        for cell in payload["cells"]
    }
    codex_yibu = cells[("codex", "yibuapi-openai-compatible")]
    assert codex_yibu["status"] == "supported"
    assert codex_yibu["protocol_surface"] == "responses"
    assert codex_yibu["streaming"] == "supported"
    assert codex_yibu["tool_use"] == "supported"
    assert codex_yibu["request_params"] == "supported"
    assert codex_yibu["max_tokens"] == "supported"
    assert codex_yibu["usage"] == "pending_live_smoke"
    assert codex_yibu["diagnostics"] == "pending_live_smoke"
    assert codex_yibu["redaction"] == "supported"
    assert codex_yibu["live_smoke"] is None

    oracle_openai = cells[("oracle", "user-hosted-openai-compatible")]
    assert oracle_openai["status"] == "skipped"
    assert "no-model" in oracle_openai["skip_reason"]

    # #2054: this phase supports OpenAI-compatible endpoints only.
    assert {e["id"] for e in payload["provider_endpoint_types"]} == {
        "yibuapi-openai-compatible",
        "user-hosted-openai-compatible",
    }
    assert payload["summary"]["supported"] > 0
    assert payload["summary"].get("blocked", 0) == 0
    assert payload["summary"]["skipped"] == len(payload["provider_endpoint_types"])
    assert sum(payload["summary"].values()) == len(payload["cells"])


def test_provider_compatibility_matrix_covers_supported_product_entries() -> None:
    """#2054: the matrix covers exactly the supported product entries."""
    from loom_service import agent_catalog

    matrix = qa_cmd._build_provider_compatibility_matrix()
    payload = qa_cmd._provider_compatibility_matrix_to_json_payload(matrix)

    expected_agents = {
        agent.name
        for agent in agent_catalog.list_agents()
        if agent.service_mode_ready and agent.product_support == "supported"
    }
    assert expected_agents == set(agent_catalog._SUPPORTED_PRODUCT_ENTRIES)
    endpoint_types = {
        endpoint["id"] for endpoint in payload["provider_endpoint_types"]
    }
    emitted_by_agent: dict[str, set[str]] = {}
    for cell in payload["cells"]:
        emitted_by_agent.setdefault(cell["agent"], set()).add(
            cell["provider_endpoint_type"],
        )

    assert set(emitted_by_agent) == expected_agents
    for agent_name in expected_agents:
        assert emitted_by_agent[agent_name] == endpoint_types


def test_provider_compatibility_matrix_includes_generic_agents_per_agent() -> None:
    matrix = qa_cmd._build_provider_compatibility_matrix()
    payload = qa_cmd._provider_compatibility_matrix_to_json_payload(matrix)
    cells = {
        (cell["agent"], cell["provider_endpoint_type"]): cell
        for cell in payload["cells"]
    }

    for agent_name in ("direct-completion", "openhands-sdk", "terminus-2"):
        openai = cells[(agent_name, "user-hosted-openai-compatible")]
        assert openai["status"] == "supported"
        assert openai["usage"] == "pending_live_smoke"
        assert "supported_providers=['*']" in openai["support_reason"]
        assert openai["agent_group"] == "generic-provider"


def test_provider_compatibility_matrix_excludes_deferred_agents() -> None:
    """#2054: deferred agents (e.g. provider-locked CLIs) are not part of
    this phase's matrix, so they emit no cells rather than blocked ones."""
    matrix = qa_cmd._build_provider_compatibility_matrix()
    payload = qa_cmd._provider_compatibility_matrix_to_json_payload(matrix)
    agents = {cell["agent"] for cell in payload["cells"]}

    for deferred in ("kimi-cli", "claude-code", "gemini-cli", "opencode", "swe-agent"):
        assert deferred not in agents

def test_provider_compatibility_evidence_override_is_serialized() -> None:
    matrix = qa_cmd._build_provider_compatibility_matrix(
        evidence_overrides=[
            {
                "agent": "codex",
                "provider_endpoint_type": "yibuapi-openai-compatible",
                "live_smoke": {
                    "status": "passed",
                    "checked_at": "2026-07-06T12:00:00+00:00",
                    "llm_calls_count": 1,
                    "usage": "priced",
                    "diagnostics": "sanitized",
                    "redaction": "passed",
                    "notes": "smoke used env:YIBUAPI_API_KEY and sanitized debug evidence",
                    "evidence_url": "https://github.com/ca-mrc/loom/issues/114#issuecomment-1",
                },
            },
        ],
    )
    payload = qa_cmd._provider_compatibility_matrix_to_json_payload(matrix)

    cell = next(
        c for c in payload["cells"]
        if c["agent"] == "codex"
        and c["provider_endpoint_type"] == "yibuapi-openai-compatible"
    )
    assert cell["usage"] == "priced"
    assert cell["diagnostics"] == "sanitized"
    assert cell["redaction"] == "passed"
    assert cell["live_smoke"] == {
        "status": "passed",
        "checked_at": "2026-07-06T12:00:00+00:00",
        "llm_calls_count": 1,
        "usage": "priced",
        "diagnostics": "sanitized",
        "redaction": "passed",
        "notes": "smoke used env:YIBUAPI_API_KEY and sanitized debug evidence",
        "evidence_url": "https://github.com/ca-mrc/loom/issues/114#issuecomment-1",
    }


def test_provider_compatibility_evidence_rejects_secret_like_values() -> None:
    raw_token = "loom_api_abcdefghijklmnopqrstuvwxyz012345"
    signed_url = "https://storage.example.test/evidence?X-Amz-Signature=abc123"
    with pytest.raises(ValueError) as exc_info:
        qa_cmd._build_provider_compatibility_matrix(
            evidence_overrides=[
                {
                    "agent": "codex",
                    "provider_endpoint_type": "yibuapi-openai-compatible",
                    "live_smoke": {
                        "status": "failed",
                        "notes": f"failed with Authorization: Bearer {raw_token}",
                        "evidence_url": signed_url,
                    },
                },
            ],
        )

    message = str(exc_info.value)
    assert "secret-like content" in message
    assert raw_token not in message
    assert "X-Amz-Signature=abc123" not in message


def test_provider_compatibility_plan_cli_does_not_require_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_login() -> None:
        raise AssertionError("plan-only compatibility matrix must not log in")

    monkeypatch.setattr(qa_cmd, "require_logged_in", fail_login)
    json_output = tmp_path / "compat.json"
    md_output = tmp_path / "compat.md"

    rc = qa_cmd.dispatch([
        "matrix",
        "--compatibility-plan",
        "--json-output",
        str(json_output),
        "--output",
        str(md_output),
    ])

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "# Agent harness x provider compatibility matrix" in stdout
    assert "yibuapi-openai-compatible" in stdout
    payload = json.loads(json_output.read_text())
    assert payload["schema_version"] == "provider-harness-compatibility-v1"
    assert payload["summary"]["supported"] > 0
    assert md_output.read_text() == stdout


def test_provider_compatibility_plan_cli_loads_safe_evidence_file(
    tmp_path: Path,
) -> None:
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text(json.dumps({
        "cells": [
            {
                "agent": "codex",
                "provider_endpoint_type": "user-hosted-openai-compatible",
                "live_smoke": {
                    "status": "passed",
                    "checked_at": "2026-07-06T12:30:00+00:00",
                    "llm_calls_count": 1,
                    "usage": "tokens-only",
                    "diagnostics": "sanitized",
                    "redaction": "passed",
                    "notes": "vLLM chat fallback evidence, provider key kept as env:VLLM_API_KEY",
                },
            },
        ],
    }))
    json_output = tmp_path / "compat.json"

    rc = qa_cmd.dispatch([
        "matrix",
        "--compatibility-plan",
        "--compatibility-evidence",
        str(evidence_file),
        "--json-output",
        str(json_output),
    ])

    assert rc == 0
    payload = json.loads(json_output.read_text())
    cell = next(
        c for c in payload["cells"]
        if c["agent"] == "codex"
        and c["provider_endpoint_type"] == "user-hosted-openai-compatible"
    )
    assert cell["usage"] == "tokens-only"
    assert cell["live_smoke"]["llm_calls_count"] == 1
