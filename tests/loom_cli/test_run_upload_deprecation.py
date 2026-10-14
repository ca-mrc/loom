"""Local runs retain results without contacting the retired upload endpoint."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter


@pytest.mark.parametrize("source", ["flag", "environment", "both", "unset"])
def test_run_upload_compatibility_keeps_local_results(
    source: str,
    tmp_xdg_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(registry.REGISTRY, "upload-test", _StubAdapter(name="upload-test"))
    monkeypatch.delenv("LOOM_SERVER_URL", raising=False)
    if source in {"environment", "both"}:
        monkeypatch.setenv("LOOM_SERVER_URL", "https://environment.invalid/private-value")
    calls: list[str] = []

    async def capture_post(self: httpx.AsyncClient, url: str, **kwargs: object) -> httpx.Response:
        calls.append(url)
        return httpx.Response(404)

    monkeypatch.setattr(httpx.AsyncClient, "post", capture_post)
    output = tmp_path / "results"
    argv = [
        "run", "--dataset", "upload-test", "--agent", "oracle", "--backend", "fake",
        "--output-dir", str(output), "--json", "--model", "openai/gpt-4o-mini",
        "--model", "anthropic/claude-opus-4-7",
    ]
    if source in {"flag", "both"}:
        argv += ["--server-url", "https://explicit.invalid/private-value"]

    rc = main(argv)
    captured = capsys.readouterr()
    results = [json.loads(line) for line in captured.out.splitlines() if line.strip()]
    assert rc in {0, 1}
    assert len(results) == 4
    assert len(list(output.glob("*/*/events.jsonl"))) == 4
    assert len(list(output.glob("*/*/atif.json"))) == 4
    assert calls == [], "local results must never POST to a nonexistent intake"
    if source == "unset":
        assert "deprecated" not in captured.err
    else:
        assert captured.err.count("deprecated") == 1
        assert "not uploaded" in captured.err
        assert "--output-dir" in captured.err
        assert "private-value" not in captured.err
