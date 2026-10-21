"""--tb2-report publishes completed results through the public CLI entry point."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.__main__ import _build_parser, main


def test_parse_args_accepts_tb2_report_flag() -> None:
    parser = _build_parser()
    parsed = parser.parse_args([
        "run", "--dataset", "terminal-bench-2", "--agent", "claude-code",
        "--tb2-report", "/tmp/out.json",
    ])
    assert parsed.tb2_report == Path("/tmp/out.json")


def test_tb2_report_omitted_defaults_to_none() -> None:
    parser = _build_parser()
    parsed = parser.parse_args([
        "run", "--dataset", "humaneval", "--agent", "claude-code",
    ])
    assert parsed.tb2_report is None


@pytest.mark.parametrize("models,parallel", [
    ([], False),
    (["openai/model-a", "openai/model-b"], False),
    (["openai/model-a", "openai/model-b"], True),
])
def test_tb2_report_written_after_trials_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], models: list[str], parallel: bool,
) -> None:
    """End-to-end: stub the registry + LocalRunner so `loom run` returns
    a TrialResult per stub task, then verify the JSON file is written
    with to_tb2_report output."""
    from loom_benchmarks import registry

    from tests.loom_cli.test_task_loader import _StubAdapter

    monkeypatch.setitem(registry.REGISTRY, "tb2-stub", _StubAdapter(name="tb2-stub"))

    out_path = tmp_path / "tb2.json"
    rc = main([
        "run",
        "--dataset", "tb2-stub",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "runs"),
        "--tb2-report", str(out_path),
        "--json",
        *[arg for model in models for arg in ("--model", model)],
        *(["--parallel-models"] if parallel else []),
    ])
    assert rc in {0, 1}
    assert out_path.exists()
    parsed = json.loads(out_path.read_text())
    # Two stub instances + to_tb2_report contract.
    assert "accuracy" in parsed
    assert "results" in parsed
    assert "pass_at_k" in parsed
    assert isinstance(parsed["results"], list)
    completed = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(completed) == 2 * max(1, len(models))
    assert {item["uuid"] for item in parsed["results"]} == {
        item["trial_id"] for item in completed
    }
