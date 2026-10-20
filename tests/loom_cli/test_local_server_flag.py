"""`loom run --local-server URL --model M` — inline dispatch against an
already-running OpenAI-compatible server.

Coverage: argument validation (mutual exclusion + required pairing),
env-var fallback for the API key, and the smoke path that the inline
provider is registered + the model spec is rewritten to point at it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter  # reuse stub


def test_local_server_rejects_hf_model(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-loc-1", _StubAdapter(name="stub-loc-1"),
    )
    with pytest.raises(SystemExit, match="mutually exclusive"):
        main([
            "run",
            "--dataset", "stub-loc-1",
            "--agent", "oracle",
            "--backend", "fake",
            "--local-server", "http://localhost:8000/v1",
            "--model", "hf:meta-llama/Llama-3.1-8B",
            "--output-dir", str(tmp_path / "out"),
        ])


def test_local_server_rejects_path_model(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-loc-2", _StubAdapter(name="stub-loc-2"),
    )
    weights_dir = tmp_path / "weights"
    weights_dir.mkdir()
    with pytest.raises(SystemExit, match="mutually exclusive"):
        main([
            "run",
            "--dataset", "stub-loc-2",
            "--agent", "oracle",
            "--backend", "fake",
            "--local-server", "http://localhost:8000/v1",
            "--model", str(weights_dir),
            "--output-dir", str(tmp_path / "out"),
        ])


def test_local_server_requires_model(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-loc-3", _StubAdapter(name="stub-loc-3"),
    )
    with pytest.raises(SystemExit, match="requires --model"):
        main([
            "run",
            "--dataset", "stub-loc-3",
            "--agent", "oracle",
            "--backend", "fake",
            "--local-server", "http://localhost:8000/v1",
            "--output-dir", str(tmp_path / "out"),
        ])


@pytest.mark.parametrize("models,parallel", [
    (["meta-llama/Llama-3.1-8B-Instruct"], False),
    (["llama3"], False),
    (["llama3", "qwen3"], False),
    (["llama3", "qwen3"], True),
])
def test_local_server_smoke_runs_through_with_inline_dispatch(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    models: list[str], parallel: bool,
) -> None:
    """Smoke: with --local-server set, the trial runs through the fake
    backend (which no-ops the LLM call) and the output dir is
    populated. Validates that the inline provider registration +
    model-spec rewrite don't blow up the dispatch path."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-loc-4", _StubAdapter(name="stub-loc-4"),
    )
    monkeypatch.setenv("LOOM_LOCAL_API_KEY", "env-key-fallback")
    from loom_cli import run_cmd

    seen = []
    original = run_cmd._patch_agent

    def capture_model(config, agent, model):
        patched = original(config, agent, model)
        seen.append(patched.agent.model)
        return patched

    monkeypatch.setattr(run_cmd, "_patch_agent", capture_model)

    main([
        "run",
        "--dataset", "stub-loc-4",
        "--agent", "oracle",
        "--backend", "fake",
        "--local-server", "http://localhost:8000/v1",
        *[arg for model in models for arg in ("--model", model)],
        *(["--parallel-models"] if parallel else []),
        "--output-dir", str(tmp_path / "out"),
    ])

    # Output dir populated → trial ran → registration + rewrite worked.
    assert (tmp_path / "out").is_dir()
    assert any((tmp_path / "out").iterdir()), \
        "expected at least one trial sub-dir in output"
    assert {model.name for model in seen} == {f"_inline/{model}" for model in models}
    assert all(model.provider == "local" for model in seen)
