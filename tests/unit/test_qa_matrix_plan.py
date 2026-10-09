"""Unit tests for the offline #35 QA matrix pre-submit planner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from loom_cli import qa_cmd
from loom_cli.qa_matrix_plan import (
    build_preflight_plan,
    matrix_preflight_plan_to_json_payload,
    render_matrix_preflight_markdown,
)


def _catalog_snapshot() -> dict[str, Any]:
    return {
        "agents": {
            "items": [
                {
                    "name": "codex",
                    "needs_model": True,
                    "supported_providers": ["openai"],
                    "service_mode_ready": True,
                    "readiness_status": "ready",
                    "requires_capabilities": [],
                },
                {
                    "name": "gemini-cli",
                    "needs_model": True,
                    "supported_providers": ["google"],
                    "service_mode_ready": True,
                    "readiness_status": "ready",
                    "requires_capabilities": [],
                },
                {
                    "name": "oracle",
                    "needs_model": False,
                    "supported_providers": [],
                    "service_mode_ready": True,
                    "readiness_status": "ready",
                    "requires_capabilities": ["solution_solve_sh"],
                },
            ],
        },
        "benchmarks": {
            "items": [
                {
                    "id": "good-bench",
                    "readiness_state": "runnable",
                    "selectable": True,
                    "task_count": 1,
                    "representative_task_id": "good-bench/0",
                    "license_spdx": "MIT",
                    "capability_evidence": {"solution_solve_sh": True},
                    "architecture_evidence": {"cpu_arch": ["x86_64", "arm64"]},
                },
                {
                    "id": "empty-bench",
                    "readiness_state": "runnable",
                    "selectable": True,
                    "task_count": 0,
                    "license_spdx": "MIT",
                    "capability_evidence": {"solution_solve_sh": True},
                    "architecture_evidence": {"cpu_arch": ["x86_64", "arm64"]},
                },
                {
                    "id": "license-missing",
                    "readiness_state": "runnable",
                    "selectable": True,
                    "task_count": 1,
                    "representative_task_id": "license-missing/0",
                    "capability_evidence": {"solution_solve_sh": True},
                    "architecture_evidence": {"cpu_arch": ["x86_64", "arm64"]},
                },
            ],
        },
    }


def _compatibility_plan() -> dict[str, Any]:
    return {
        "schema_version": "provider-harness-compatibility-v1",
        "provider_endpoint_types": [
            {
                "id": "yibuapi-openai-compatible",
                "provider_family": "openai",
                "protocol_surface": "chat",
                "description": "YibuAPI OpenAI-compatible endpoint",
            },
            {
                "id": "yibuapi-gemini-native",
                "provider_family": "google",
                "protocol_surface": "gemini",
                "description": "YibuAPI Gemini endpoint",
            },
        ],
        "cells": [
            {
                "agent": "codex",
                "provider_endpoint_type": "yibuapi-openai-compatible",
                "status": "supported",
                "usage": "priced",
                "diagnostics": "sanitized",
                "redaction": "passed",
                "support_reason": "OpenAI Responses smoke passed",
                "live_smoke": {
                    "status": "passed",
                    "llm_calls_count": 1,
                    "evidence_url": "https://github.com/ca-mrc/loom/issues/114#issuecomment-1",
                },
            },
            {
                "agent": "codex",
                "provider_endpoint_type": "yibuapi-gemini-native",
                "status": "blocked",
                "blocked_reason": "supported_providers=['openai'] excludes google",
            },
            {
                "agent": "gemini-cli",
                "provider_endpoint_type": "yibuapi-openai-compatible",
                "status": "blocked",
                "blocked_reason": "supported_providers=['google'] excludes openai",
            },
            {
                "agent": "gemini-cli",
                "provider_endpoint_type": "yibuapi-gemini-native",
                "status": "supported",
                "usage": "pending_live_smoke",
                "diagnostics": "pending_live_smoke",
                "redaction": "supported",
                "support_reason": "metadata supports google; awaiting live smoke",
            },
            {
                "agent": "oracle",
                "provider_endpoint_type": "yibuapi-openai-compatible",
                "status": "skipped",
                "skip_reason": "oracle is a no-model harness",
            },
            {
                "agent": "oracle",
                "provider_endpoint_type": "yibuapi-gemini-native",
                "status": "skipped",
                "skip_reason": "oracle is a no-model harness",
            },
        ],
    }


def test_preflight_plan_counts_and_reasons_are_deterministic() -> None:
    plan = build_preflight_plan(
        catalog_snapshot=_catalog_snapshot(),
        compatibility_plan=_compatibility_plan(),
    )
    payload = matrix_preflight_plan_to_json_payload(plan)

    assert payload["schema_version"] == "agent-benchmark-preflight-plan-v1"
    assert payload["issue"] == "https://github.com/ca-mrc/loom/issues/35"
    assert payload["live_provider_calls"] == "not_run"
    assert payload["summary"] == {
        "blocked": 8,
        "planned_submit": 2,
        "skipped": 5,
    }
    assert len(payload["cells"]) == 15

    cells = {
        (
            cell["agent"],
            cell["benchmark"],
            cell["provider_endpoint_type"],
        ): cell
        for cell in payload["cells"]
    }

    codex_openai = cells[("codex", "good-bench", "yibuapi-openai-compatible")]
    assert codex_openai["status"] == "planned_submit"
    assert codex_openai["reason_category"] == "compatibility_live_evidence_ready"
    assert codex_openai["provider_family"] == "openai"
    assert codex_openai["agent_model"] == {
        "provider": "openai",
        "source": "api",
        "name": None,
    }

    codex_google = cells[("codex", "good-bench", "yibuapi-gemini-native")]
    assert codex_google["status"] == "blocked"
    assert codex_google["reason_category"] == "provider_mismatch"
    assert "excludes google" in codex_google["reason"]

    gemini_google = cells[("gemini-cli", "good-bench", "yibuapi-gemini-native")]
    assert gemini_google["status"] == "blocked"
    assert gemini_google["reason_category"] == "pending_live_evidence"
    assert "#114" in gemini_google["reason"]

    oracle_cells = [
        cell
        for cell in payload["cells"]
        if cell["agent"] == "oracle" and cell["benchmark"] == "good-bench"
    ]
    assert len(oracle_cells) == 1
    assert oracle_cells[0]["provider_endpoint_type"] == "no-model"
    assert oracle_cells[0]["provider_family"] is None
    assert oracle_cells[0]["agent_model"] is None
    assert oracle_cells[0]["status"] == "planned_submit"
    assert oracle_cells[0]["reason_category"] == "no_model_agent"

    empty_statuses = {
        cell["reason_category"] for cell in payload["cells"] if cell["benchmark"] == "empty-bench"
    }
    assert empty_statuses == {"no_runnable_task"}

    missing_license_statuses = {
        cell["reason_category"]
        for cell in payload["cells"]
        if cell["benchmark"] == "license-missing"
    }
    assert missing_license_statuses == {"license_evidence_missing"}


def test_preflight_plan_rejects_secret_like_evidence_without_leaking_value() -> None:
    catalog = _catalog_snapshot()
    raw_token = "loom_api_abcdefghijklmnopqrstuvwxyz012345"
    signed_url = "https://storage.example.test/evidence?X-Amz-Signature=abc123"
    catalog["benchmarks"]["items"][0]["operator_notes"] = (
        f"provider failed with Authorization: Bearer {raw_token}"
    )
    catalog["benchmarks"]["items"][0]["evidence_url"] = signed_url

    with pytest.raises(ValueError) as exc_info:
        build_preflight_plan(
            catalog_snapshot=catalog,
            compatibility_plan=_compatibility_plan(),
        )

    message = str(exc_info.value)
    assert "secret-like content" in message
    assert raw_token not in message
    assert "X-Amz-Signature=abc123" not in message


def test_preflight_plan_blocks_missing_readiness_capability_and_architecture() -> None:
    catalog = _catalog_snapshot()
    catalog["benchmarks"]["items"] = [
        {
            "id": "readiness-missing",
            "task_count": 1,
            "representative_task_id": "readiness-missing/0",
            "license_spdx": "MIT",
            "capability_evidence": {"solution_solve_sh": True},
            "architecture_evidence": {"cpu_arch": ["x86_64", "arm64"]},
        },
        {
            "id": "capability-missing",
            "readiness_state": "runnable",
            "selectable": True,
            "task_count": 1,
            "representative_task_id": "capability-missing/0",
            "license_spdx": "MIT",
            "architecture_evidence": {"cpu_arch": ["x86_64", "arm64"]},
        },
        {
            "id": "architecture-missing",
            "readiness_state": "runnable",
            "selectable": True,
            "task_count": 1,
            "representative_task_id": "architecture-missing/0",
            "license_spdx": "MIT",
            "capability_evidence": {"solution_solve_sh": True},
        },
    ]

    plan = build_preflight_plan(
        catalog_snapshot=catalog,
        compatibility_plan=_compatibility_plan(),
    )
    payload = matrix_preflight_plan_to_json_payload(plan)

    by_benchmark = {
        benchmark: {
            cell["reason_category"] for cell in payload["cells"] if cell["benchmark"] == benchmark
        }
        for benchmark in (
            "readiness-missing",
            "capability-missing",
            "architecture-missing",
        )
    }
    assert by_benchmark["readiness-missing"] == {"readiness_evidence_missing"}
    assert by_benchmark["capability-missing"] == {
        "capability_evidence_missing",
        "compatibility_live_evidence_ready",
        "pending_live_evidence",
        "provider_mismatch",
    }
    assert by_benchmark["architecture-missing"] == {"architecture_evidence_missing"}

    oracle_capability = next(
        cell
        for cell in payload["cells"]
        if cell["agent"] == "oracle" and cell["benchmark"] == "capability-missing"
    )
    assert oracle_capability["status"] == "blocked"
    assert "solution_solve_sh" in oracle_capability["reason"]


def test_preflight_plan_markdown_includes_live_acceptance_caveat() -> None:
    plan = build_preflight_plan(
        catalog_snapshot=_catalog_snapshot(),
        compatibility_plan=_compatibility_plan(),
    )

    md = render_matrix_preflight_markdown(plan)

    assert "# Agent x benchmark pre-submit plan" in md
    assert "does not satisfy live #35 acceptance" in md
    assert "| codex | good-bench | yibuapi-openai-compatible | planned_submit |" in md
    assert "pending_live_evidence" in md


def test_preflight_plan_cli_does_not_require_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_login() -> None:
        raise AssertionError("preflight plan must not log in")

    monkeypatch.setattr(qa_cmd, "require_logged_in", fail_login)

    catalog_path = tmp_path / "catalog.json"
    compatibility_path = tmp_path / "compatibility.json"
    json_output = tmp_path / "preflight.json"
    md_output = tmp_path / "preflight.md"
    catalog_path.write_text(json.dumps(_catalog_snapshot()))
    compatibility_path.write_text(json.dumps(_compatibility_plan()))

    rc = qa_cmd.dispatch(
        [
            "matrix",
            "--preflight-plan",
            "--catalog-snapshot",
            str(catalog_path),
            "--provider-compatibility-plan",
            str(compatibility_path),
            "--json-output",
            str(json_output),
            "--output",
            str(md_output),
        ]
    )

    assert rc == 0
    stdout = capsys.readouterr().out
    assert "# Agent x benchmark pre-submit plan" in stdout
    assert md_output.read_text() == stdout

    payload = json.loads(json_output.read_text())
    assert payload["summary"]["planned_submit"] == 2
    assert payload["summary"]["blocked"] == 8
    assert payload["summary"]["skipped"] == 5


def test_preflight_plan_skips_deferred_agents() -> None:
    """#2054: agents the catalog marks deferred are not planned at all."""
    snapshot = _catalog_snapshot()
    for agent in snapshot["agents"]["items"]:
        if agent["name"] == "gemini-cli":
            agent["product_support"] = "deferred"
        else:
            agent["product_support"] = "supported"

    plan = build_preflight_plan(
        catalog_snapshot=snapshot,
        compatibility_plan=_compatibility_plan(),
    )
    payload = matrix_preflight_plan_to_json_payload(plan)

    agents = {cell["agent"] for cell in payload["cells"]}
    assert "gemini-cli" not in agents
    assert {"codex", "oracle"} <= agents
