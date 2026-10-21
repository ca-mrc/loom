"""`loom run --model A --model B --dataset X` — compare N models on
the same dataset.

Sequential by default: launch A → run all tasks → unload A → launch
B → run all tasks → unload B. Peak GPU = max, not sum.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter  # reuse stub


def test_sequential_two_classic_models_buckets_output_by_slug(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two cloud-provider model specs (no vLLM launch). Outputs
    should bucket under <out>/<slug>/<trial-id>/."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-seq-1", _StubAdapter(name="stub-seq-1"),
    )
    out = tmp_path / "out"
    rc = main([
        "run",
        "--dataset", "stub-seq-1",
        "--agent", "oracle",
        "--backend", "fake",
        "--model", "anthropic/claude-opus-4-7",
        "--model", "openai/gpt-4o",
        "--output-dir", str(out),
    ])
    assert rc in {0, 1}

    # Two slug buckets exist, each containing at least one trial dir
    assert (out / "claude-opus-4-7").is_dir()
    assert (out / "gpt-4o").is_dir()
    assert any((out / "claude-opus-4-7").iterdir())
    assert any((out / "gpt-4o").iterdir())


def test_sequential_one_model_fails_other_still_runs(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If _run_one_model raises for model A, model B should still
    run. Final exit code = 2 (non-zero) so failure surfaces."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-seq-2", _StubAdapter(name="stub-seq-2"),
    )

    import loom_cli.run_cmd as rc
    real = rc._run_one_model

    async def _fail_for_anthropic(args, *, model, output_dir, completed=None):
        if model is not None and model.provider == "anthropic":
            raise RuntimeError("simulated failure for model A")
        return await real(args, model=model, output_dir=output_dir, completed=completed)

    monkeypatch.setattr(rc, "_run_one_model", _fail_for_anthropic)

    out = tmp_path / "out"
    code = main([
        "run",
        "--dataset", "stub-seq-2",
        "--agent", "oracle",
        "--backend", "fake",
        "--model", "anthropic/claude-opus-4-7",   # will "fail"
        "--model", "openai/gpt-4o",
        "--output-dir", str(out),
    ])
    assert code == 2

    # Model B still produced outputs
    assert (out / "gpt-4o").is_dir()
    assert any((out / "gpt-4o").iterdir())


def test_parallel_two_classic_models_buckets_output_by_slug(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--parallel-models: two cloud-provider specs run concurrently.
    Output still bucketed by slug; both buckets populated."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-par-1", _StubAdapter(name="stub-par-1"),
    )
    out = tmp_path / "out"
    rc = main([
        "run",
        "--dataset", "stub-par-1",
        "--agent", "oracle",
        "--backend", "fake",
        "--model", "anthropic/claude-opus-4-7",
        "--model", "openai/gpt-4o",
        "--parallel-models",
        "--output-dir", str(out),
    ])
    assert rc in {0, 1}
    assert (out / "claude-opus-4-7").is_dir()
    assert (out / "gpt-4o").is_dir()
    assert any((out / "claude-opus-4-7").iterdir())
    assert any((out / "gpt-4o").iterdir())


def test_parallel_models_with_single_model_warns_and_continues(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--parallel-models` with one `--model` is a no-op; warn on
    stderr so users notice the typo."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-par-2", _StubAdapter(name="stub-par-2"),
    )
    main([
        "run",
        "--dataset", "stub-par-2",
        "--agent", "oracle",
        "--backend", "fake",
        "--model", "anthropic/claude-opus-4-7",
        "--parallel-models",
        "--output-dir", str(tmp_path / "out"),
    ])
    err = capsys.readouterr().err
    assert "parallel-models" in err.lower()
    assert "no effect" in err.lower() or "single" in err.lower()


def test_local_server_with_multiple_models_each_uses_its_own_spec(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--local-server URL --model A --model B` should run BOTH
    models against the URL, not silently run A twice."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-loc-multi", _StubAdapter(name="stub-loc-multi"),
    )

    seen_models: list[str] = []
    import loom_cli.run_cmd as rc
    real = rc._run_one_model

    async def _capture(args, *, model, output_dir, completed=None):
        # Capture the pre-rewrite model string so we can assert
        # that each iteration received a distinct spec.
        if model is not None:
            seen_models.append(f"{model.provider}/{model.name}")
        else:
            seen_models.append("<none>")
        return await real(args, model=model, output_dir=output_dir, completed=completed)

    monkeypatch.setattr(rc, "_run_one_model", _capture)

    out = tmp_path / "out"
    main([
        "run",
        "--dataset", "stub-loc-multi",
        "--agent", "oracle",
        "--backend", "fake",
        "--local-server", "http://localhost:8000/v1",
        "--model", "meta-llama/Model-A",
        "--model", "meta-llama/Model-B",
        "--output-dir", str(out),
    ])

    # Output buckets — both slugs present
    assert (out / "model-a").is_dir()
    assert (out / "model-b").is_dir()
    # Two distinct models hit _run_one_model (not model-A twice)
    assert seen_models == ["local/_inline/meta-llama/Model-A", "local/_inline/meta-llama/Model-B"]


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize("keep_alive", [False, True])
def test_parallel_managed_vllms_dont_kill_each_other(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parallel: bool, keep_alive: bool,
) -> None:
    """With `--parallel-models --model hf:A --model hf:B`, the
    teardown of model A must not also kill model B's vLLM."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-par-managed", _StubAdapter(name="stub-par-managed"),
    )

    from unittest.mock import MagicMock

    import loom_cli.run_cmd as rc
    import loom_cli.vllm_runner as vr

    # Mock launch_vllm to return distinct VLLMServerInfos
    launches: list[str] = []
    retained_flags: list[bool] = []
    owned = []

    def _fake_launch(spec):
        if not parallel:
            assert all(proc not in vr._LIVE_PROCESSES for proc in owned)
        launches.append(spec.model)
        retained_flags.append(spec.keep_alive)
        proc = MagicMock()
        proc.pid = 10000 + len(launches)
        proc.poll.return_value = None
        vr._LIVE_PROCESSES.append(proc)
        owned.append(proc)
        return vr.VLLMServerInfo(
            base_url=f"http://localhost:{8234 + len(launches)}/v1",
            served_model_name=spec.model,
            pid=proc.pid,
        )

    monkeypatch.setattr(rc, "launch_vllm", _fake_launch)

    # Mock stop_one to verify it's called per-process, not stop_all
    stop_one_calls: list[int] = []

    def _capture_stop_one(proc):
        stop_one_calls.append(proc.pid)
        vr._LIVE_PROCESSES.remove(proc)

    monkeypatch.setattr(rc, "stop_one", _capture_stop_one)

    # Also stub stop_all at the source module so a regression that
    # re-introduces stop_all() becomes visible. run_cmd no longer
    # imports stop_all directly (it was removed), so we patch vllm_runner.
    stop_all_called: list[bool] = []
    monkeypatch.setattr(vr, "stop_all", lambda: stop_all_called.append(True))

    main([
        "run",
        "--dataset", "stub-par-managed",
        "--agent", "oracle",
        "--backend", "fake",
        "--model", "hf:meta-llama/A",
        "--model", "hf:meta-llama/B",
        *(["--parallel-models"] if parallel else []),
        *(["--keep-alive"] if keep_alive else []),
        "--output-dir", str(tmp_path / "out"),
    ])

    # Both vLLMs launched
    assert len(launches) == 2
    assert retained_flags == [False, False]
    # Each was torn down by stop_one (per-process), not stop_all
    assert len(stop_one_calls) == 2
    assert stop_all_called == [], \
        "stop_all should not be called in --parallel-models — it would kill siblings"
