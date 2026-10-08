from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest
import yaml
from scripts.ops.harbor_upstream import (
    MANIFEST,
    PIN_LOCATIONS,
    ROOT,
    GitHubClient,
    UpstreamError,
    discover,
    frozen_worker_status,
    load_pin,
    main,
    pin_plan,
    update,
)


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    paths = {MANIFEST, *(relative for relative, _, _ in PIN_LOCATIONS)}
    paths.update({"deploy/worker-image.lock", "deploy/worker-image.wheels.json"})
    for relative in paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((ROOT / relative).read_bytes())
    return tmp_path


def test_local_pins_match_canonical_manifest() -> None:
    pin = load_pin(ROOT)
    pin_plan(ROOT, pin, pin)


def test_discovery_reads_version_at_resolved_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    pin = load_pin(ROOT)
    client = GitHubClient()
    calls: list[str] = []

    def get(path: str) -> Any:
        calls.append(path)
        if "/commits/" in path:
            return {"sha": "a" * 40}
        return {
            "encoding": "base64",
            "content": base64.b64encode(b'[project]\nversion = "0.2.0"\n').decode(),
        }

    monkeypatch.setattr(client, "get", get)
    latest = discover(pin, client)
    assert latest.source_revision == "a" * 40
    assert latest.version == "0.2.0"
    assert calls == [
        "/repos/harbor-framework/harbor/commits/main",
        f"/repos/harbor-framework/harbor/contents/pyproject.toml?ref={'a' * 40}",
    ]


@pytest.mark.parametrize(
    "payloads, message",
    [
        ([{"sha": "main"}], "full source SHA"),
        ([{}, {}], "full source SHA"),
        ([{"sha": "a" * 40}, {"encoding": "base64", "content": "aW52YWxpZA=="}], "project.version"),
        ([{"sha": "a" * 40}, {"encoding": "none", "content": ""}], "project.version"),
    ],
)
def test_invalid_upstream_response_fails_clearly(
    monkeypatch: pytest.MonkeyPatch, payloads: list[Any], message: str
) -> None:
    client = GitHubClient()
    responses = iter(payloads)
    monkeypatch.setattr(client, "get", lambda _path: next(responses))
    with pytest.raises(UpstreamError, match=message):
        discover(load_pin(ROOT), client)


def test_http_failure_reports_status_without_response_body_or_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenOpener:
        def open(self, request: Any, **kwargs: Any) -> None:
            raise HTTPError(request.full_url, 403, "secret response", {}, None)

    monkeypatch.setattr("scripts.ops.harbor_upstream.build_opener", lambda *_: BrokenOpener())
    with pytest.raises(UpstreamError, match="HTTP 403") as error:
        GitHubClient("secret token").get("/repos/harbor-framework/harbor/commits/main")
    assert "secret" not in str(error.value)


def test_explicit_update_is_deterministic_and_preserves_frozen_evidence(checkout: Path) -> None:
    current = load_pin(checkout)
    target = replace(current, source_revision="a" * 40, version="0.2.0")
    frozen_before = {
        relative: (checkout / relative).read_bytes()
        for relative in ("deploy/worker-image.lock", "deploy/worker-image.wheels.json")
    }
    before = {
        relative: (checkout / relative).read_bytes()
        for relative in pin_plan(checkout, current, current)
    }
    dry_run = update(checkout, target, dry_run=True)
    assert len(dry_run["changed_files"]) == 4
    assert dry_run["frozen_worker"]["regeneration_required"] is True
    assert all((checkout / relative).read_bytes() == text for relative, text in before.items())
    result = update(checkout, target, dry_run=False)
    assert result["changed_files"] == dry_run["changed_files"]
    pin_plan(checkout, target, target)
    assert load_pin(checkout) == target
    assert update(checkout, target, dry_run=False)["changed_files"] == []
    assert all(
        (checkout / relative).read_bytes() == text for relative, text in frozen_before.items()
    )
    assert frozen_worker_status(checkout, target)["regeneration_required"] is True


def test_pin_drift_prevents_all_writes(checkout: Path) -> None:
    path = checkout / "src/loom/agent/terminus2/provenance.py"
    path.write_text(path.read_text().replace('"LOOM_HARBOR_SOURCE_REVISION"', '"RENAMED"'))
    before = {path: path.read_bytes() for path in checkout.rglob("*") if path.is_file()}
    target = replace(load_pin(checkout), source_revision="a" * 40, version="0.2.0")
    with pytest.raises(UpstreamError, match="expected exactly one"):
        update(checkout, target, dry_run=False)
    assert all(path.read_bytes() == data for path, data in before.items())


@pytest.mark.parametrize("revision, version", [("main", "0.2.0"), ("a" * 40, "0.2.0\nevil=true")])
def test_invalid_explicit_pin_is_rejected(checkout: Path, revision: str, version: str) -> None:
    assert (
        main(["--root", str(checkout), "update", "--revision", revision, "--version", version]) == 2
    )


def test_check_pins_fails_on_consumer_divergence(
    checkout: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = checkout / "deploy/Dockerfile.worker"
    path.write_text(path.read_text().replace(load_pin(checkout).source_revision, "b" * 40))
    assert main(["--root", str(checkout), "pins"]) == 2
    assert "differs from config/harbor-runtime.json" in capsys.readouterr().err


def test_cli_dry_run_and_local_check_work_without_network(checkout: Path) -> None:
    command = [
        sys.executable,
        str(ROOT / "scripts/ops/harbor_upstream.py"),
        "--root",
        str(checkout),
    ]
    run = subprocess.run(
        [*command, "update", "--revision", "a" * 40, "--version", "0.2.0", "--dry-run"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(run.stdout)["dry_run"] is True
    assert load_pin(checkout).source_revision != "a" * 40
    run = subprocess.run([*command, "pins"], capture_output=True, text=True, check=True)
    assert json.loads(run.stdout)["active_pins_match"] is True


def test_check_reports_drift_and_optional_failure_status(
    checkout: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "scripts.ops.harbor_upstream.discover",
        lambda pin, _client: replace(pin, source_revision="a" * 40, version="0.2.0"),
    )
    output = checkout / "outputs"
    arguments = ["--root", str(checkout), "check", "--check-pins", "--github-output", str(output)]
    assert main(arguments) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["aligned"] is False
    assert f"revision={'a' * 40}\n" in output.read_text()
    assert "aligned=false\n" in output.read_text()
    assert main([*arguments, "--require-aligned"]) == 1


def test_manifest_rejects_unknown_fields(checkout: Path) -> None:
    data = json.loads((checkout / MANIFEST).read_text())
    data["token"] = "not allowed"
    (checkout / MANIFEST).write_text(json.dumps(data))
    with pytest.raises(UpstreamError, match="exactly the four"):
        load_pin(checkout)


@pytest.mark.parametrize("enabled, expected_status", [("", 1), ("false", 1), ("true", 0)])
def test_workflow_publication_activation_boundary(
    tmp_path: Path, enabled: str, expected_status: int
) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/harbor-upstream.yml").read_text())
    steps = workflow["jobs"]["candidate"]["steps"]
    activation_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Check candidate publication activation"
    )
    publication_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Prepare one Draft candidate"
    )
    assert activation_index < publication_index
    summary = tmp_path / "summary.md"
    result = subprocess.run(
        ["/bin/bash", "-c", steps[activation_index]["run"]],
        cwd=tmp_path,
        env={**os.environ, "PATH": "", "PR_ENABLED": enabled, "GITHUB_STEP_SUMMARY": str(summary)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == expected_status
    if expected_status:
        assert "No candidate branch was pushed" in summary.read_text()
        assert "LOOM_HARBOR_UPSTREAM_PR_ENABLED=true" in summary.read_text()
        assert "only creates Drafts" in summary.read_text()
    else:
        assert not summary.exists()


@pytest.mark.parametrize("failure", ["", "build", "probe"])
def test_workflow_qualifies_only_prepared_candidate_and_reports_failure(
    tmp_path: Path, failure: str
) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/harbor-upstream.yml").read_text())
    steps = workflow["jobs"]["candidate"]["steps"]
    prepare = next(step for step in steps if step.get("id") == "candidate")
    qualify = next(
        step
        for step in steps
        if step.get("name") == "Qualify prepared candidate with real Harbor offline"
    )
    # A Ready early-return emits no prepared output, so it cannot qualify dev.
    assert qualify["if"] == "steps.candidate.outputs.prepared == 'true'"
    assert prepare["run"].rfind("prepared=true") > prepare["run"].rfind("gh pr edit")
    assert steps.index(qualify) > steps.index(prepare)

    binaries = tmp_path / "bin"
    binaries.mkdir()
    calls = tmp_path / "calls.jsonl"
    summary = tmp_path / "summary.md"
    shim = f"""#!{Path(sys.executable).resolve()}
import json, os, pathlib, sys
command = pathlib.Path(sys.argv[0]).name
arguments = sys.argv[1:]
with open(os.environ['COMMAND_LOG'], 'a') as output:
    output.write(json.dumps([command, *arguments]) + '\\n')
if command == 'git':
    print('a' * 40)
if command == 'docker' and (
    (os.environ['FAILURE'] == 'build' and arguments[0] == 'build') or
    (os.environ['FAILURE'] == 'probe' and arguments[0] == 'run')
):
    sys.exit(17)
"""
    for name in ("docker", "git", "python3"):
        executable = binaries / name
        executable.write_text(shim)
        executable.chmod(0o755)
    result = subprocess.run(
        ["/bin/bash", "-c", qualify["run"]],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": str(binaries),
            "COMMAND_LOG": str(calls),
            "FAILURE": failure,
            "GITHUB_RUN_ID": "123",
            "GITHUB_STEP_SUMMARY": str(summary),
        },
        capture_output=True,
        text=True,
    )
    assert calls.exists(), result.stderr
    recorded = [json.loads(line) for line in calls.read_text().splitlines()]
    docker_calls = [arguments for command, *arguments in recorded if command == "docker"]
    build = docker_calls[0]
    assert build[:3] == ["build", "--file", "deploy/Dockerfile.harbor-runtime"]
    assert build[build.index("--build-arg") + 1] == f"LOOM_BUILD_SHA={'a' * 40}"
    assert build[-1] == "."
    if failure:
        assert result.returncode == 17
        assert "Candidate offline qualification failed" in summary.read_text()
        assert "remains Draft and has not been accepted" in summary.read_text()
        assert "qualification passed" not in summary.read_text()
        assert len(docker_calls) == (1 if failure == "build" else 2)
    else:
        assert result.returncode == 0, result.stderr
        assert "Candidate offline qualification passed" in summary.read_text()
        assert "Frozen worker dependencies must still be rebuilt" in summary.read_text()
        assert "Real model execution" in summary.read_text()
        runs = docker_calls[1:]
        assert len(runs) == 2
        for arguments, probe in zip(
            runs, ("terminus_conformance_probe.py", "terminus_continuation_probe.py"), strict=True
        ):
            assert arguments[0] == "run"
            assert arguments[arguments.index("--network") + 1] == "none"
            assert arguments[arguments.index("--cap-drop") + 1] == "ALL"
            assert arguments[arguments.index("--security-opt") + 1] == "no-new-privileges"
            assert arguments.count("-v") == 1
            assert (
                arguments[arguments.index("-v") + 1] == f"{ROOT}/tests/support/{probe}:/probe.py:ro"
            )
            assert arguments[-5:] == ["python", "loom-harbor-upstream:123", "-I", "-B", "/probe.py"]
            assert arguments[arguments.index("-e") + 1] == "LITELLM_LOCAL_MODEL_COST_MAP=True"


def test_workflow_ready_candidate_does_not_emit_prepared_output(tmp_path: Path) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/harbor-upstream.yml").read_text())
    prepare = next(
        step for step in workflow["jobs"]["candidate"]["steps"] if step.get("id") == "candidate"
    )
    binaries = tmp_path / "bin"
    binaries.mkdir()
    shim = f"""#!{Path(sys.executable).resolve()}
import json, pathlib, sys
command = pathlib.Path(sys.argv[0]).name
arguments = sys.argv[1:]
if command == 'git' and arguments[0] == 'config':
    pass
elif command == 'gh' and arguments[:2] == ['pr', 'list']:
    print(json.dumps([{{'number': 9, 'isDraft': False}}]))
elif command == 'jq':
    print('9' if 'number' in arguments[1] else 'false')
else:
    sys.exit('Ready candidate unexpectedly reached a write operation')
"""
    for name in ("git", "gh", "jq"):
        executable = binaries / name
        executable.write_text(shim)
        executable.chmod(0o755)
    summary = tmp_path / "summary.md"
    output = tmp_path / "output"
    result = subprocess.run(
        ["/bin/bash", "-c", prepare["run"]],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": str(binaries),
            "GITHUB_REPOSITORY": "qianyi-sun/loom",
            "CANDIDATE_BRANCH": "bot/harbor-upstream",
            "RUNNER_TEMP": str(tmp_path),
            "GITHUB_STEP_SUMMARY": str(summary),
            "GITHUB_OUTPUT": str(output),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Candidate is Ready" in summary.read_text()
    assert not output.exists()
