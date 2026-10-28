"""End-to-end through `main(["run", ...])` with a stub adapter +
FakeDriver. Verifies fan-out + result printing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter  # reuse stub


def test_run_text_output(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run", _StubAdapter(name="stub-run"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run",
        "--agent", "oracle",
        "--backend", "fake",
        "--concurrency", "2",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 2
    assert all("stub-run/t" in line for line in lines)
    assert (tmp_path / "out").is_dir()


def test_run_json_output(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-j", _StubAdapter(name="stub-run-j"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run-j",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
        "--json",
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    parsed = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert len(parsed) == 2
    assert all("trial_id" in obj and "state" in obj for obj in parsed)


def test_run_task_filter(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-f", _StubAdapter(name="stub-run-f"),
    )
    rc = main([
        "run",
        "--task", "stub-run-f/t2",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc in {0, 1}
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
    assert "stub-run-f/t2" in lines[0]


def test_run_unknown_agent_errors_cleanly(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(
        registry.REGISTRY, "stub-run-u", _StubAdapter(name="stub-run-u"),
    )
    rc = main([
        "run",
        "--dataset", "stub-run-u",
        "--agent", "no-such-agent-zzz",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc == 1


def test_single_model_run_layout_unchanged(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: with a single --model, output layout is
    <output-dir>/<trial-id>/ (no extra slug bucket)."""
    from loom_benchmarks import registry
    monkeypatch.setitem(
        registry.REGISTRY, "stub-single", _StubAdapter(name="stub-single"),
    )
    rc = main([
        "run",
        "--dataset", "stub-single",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "out"),
    ])
    assert rc in {0, 1}
    # No slug bucket — trial dirs land directly under out/
    entries = list((tmp_path / "out").iterdir())
    assert entries, "expected at least one trial dir"
    # The only sub-dir should look like a trial UUID, not a slug.
    for e in entries:
        if e.is_dir():
            # Trial dirs contain events.jsonl + atif.json
            assert (e / "events.jsonl").exists() or e.name.startswith("_")


@pytest.mark.parametrize(
    "flag,enabled", [("--mainland-proxy", True), ("--no-mainland-proxy", False)]
)
def test_local_run_mainland_proxy_parser(flag, enabled):
    from loom_cli.__main__ import _build_parser

    args = _build_parser().parse_args(["run", "--dataset", "stub", "--agent", "oracle", flag])
    assert args.mainland_proxy is enabled


def test_local_run_mainland_proxy_rejects_unsupported_backend(capsys):
    assert (
        main(
            [
                "run",
                "--dataset",
                "unused",
                "--agent",
                "oracle",
                "--backend",
                "fake",
                "--mainland-proxy",
            ]
        )
        == 2
    )
    assert "requires --backend docker" in capsys.readouterr().err


def test_local_run_mainland_proxy_reaches_trial_config(tmp_xdg_home, tmp_path, monkeypatch):
    from loom_benchmarks import registry

    from loom_cli import run_cmd

    monkeypatch.setitem(registry.REGISTRY, "stub-proxy", _StubAdapter(name="stub-proxy"))
    captured = []

    async def driver_factory(*_args, **_kwargs):
        return lambda: None

    class CaptureRunner:
        def __init__(self, **kwargs):
            captured.append(kwargs["trial_config"])

        async def run(self):
            raise RuntimeError("intentional stop before sandbox")

    monkeypatch.setattr(run_cmd, "_driver_factory", driver_factory)
    monkeypatch.setattr(run_cmd, "LocalRunner", CaptureRunner)
    assert (
        main(
            [
                "run",
                "--dataset",
                "stub-proxy",
                "--agent",
                "oracle",
                "--mainland-proxy",
                "--output-dir",
                str(tmp_path / "out"),
            ]
        )
        == 1
    )
    assert len(captured) == 2
    assert all(config.mainland_proxy and config.agent_name == "oracle" for config in captured)
