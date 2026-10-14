from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from tests.integration.test_execution_actuator_k3s import _build_image


def _fake_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, stderr: str, code: int) -> None:
    command = tmp_path / "docker"
    command.write_text(
        f"#!{sys.executable}\nimport sys\nsys.stderr.write({stderr!r})\nraise SystemExit({code})\n"
    )
    command.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))


@pytest.mark.parametrize("prefix", ["", "earlier build output\n" * 2000], ids=["short", "long"])
def test_failed_fixture_build_preserves_bounded_underlying_error(tmp_path, monkeypatch, prefix):
    detail = "guest archive digest: HTTP 503, attempt 3/3\n"
    _fake_docker(tmp_path, monkeypatch, stderr=prefix + detail, code=23)

    with pytest.raises(subprocess.CalledProcessError) as caught:
        _build_image(tag="loom-fixture:test", dockerfile="deploy/Dockerfile.execution-runtime", platform="linux/amd64")

    assert caught.value.returncode == 23
    assert caught.value.stderr == prefix + detail
    diagnostic = "\n".join(getattr(caught.value, "__notes__", []))
    assert detail in diagnostic
    assert len(diagnostic) <= 17_000


def test_successful_fixture_build_does_not_emit_captured_output(tmp_path, monkeypatch, capsys):
    _fake_docker(tmp_path, monkeypatch, stderr="ordinary build progress\n", code=0)
    _build_image(tag="loom-fixture:test", dockerfile="deploy/Dockerfile.execution-runtime", platform="linux/amd64")
    assert capsys.readouterr().err == ""


def test_failed_fixture_build_without_stderr_keeps_original_failure(tmp_path, monkeypatch):
    _fake_docker(tmp_path, monkeypatch, stderr="", code=23)
    with pytest.raises(subprocess.CalledProcessError) as caught:
        _build_image(tag="loom-fixture:test", dockerfile="deploy/Dockerfile.execution-runtime", platform="linux/amd64")
    assert caught.value.returncode == 23
    assert not getattr(caught.value, "__notes__", [])
