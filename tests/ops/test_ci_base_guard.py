"""Stop obsolete strict-base PR fan-out without interfering with native queues."""

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from scripts import ci_fast_feedback as feedback

OLD = "a" * 40
NEW = "b" * 40
STRICT = {"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": True}}
ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("current,accepted", [(OLD, True), (NEW, False)])
def test_strict_nonqueue_base_checks_exact_current_sha(current, accepted):
    calls = []

    def read(endpoint):
        calls.append(endpoint)
        return [STRICT] if "/rules/" in endpoint else {"object": {"sha": current}}

    if accepted:
        feedback.check_current_base("ca-mrc/loom", "feature/base", OLD, read=read)
    else:
        with pytest.raises(ValueError, match=r"base.*advanced"):
            feedback.check_current_base("ca-mrc/loom", "feature/base", OLD, read=read)
    assert calls == ["repos/ca-mrc/loom/rules/branches/feature%2Fbase", "repos/ca-mrc/loom/git/ref/heads/feature%2Fbase"]


@pytest.mark.parametrize("rules", [
    [STRICT, {"type": "merge_queue"}],
    [{"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": False}}],
    [],
])
def test_native_queue_and_nonstrict_rules_do_not_require_rebase(rules):
    calls = []

    def read(endpoint):
        calls.append(endpoint)
        return rules

    feedback.check_current_base("ca-mrc/loom", "dev", OLD, read=read)
    assert calls == ["repos/ca-mrc/loom/rules/branches/dev"]


@pytest.mark.parametrize("rules", [None, {}, [None], [{}], [{"type": 1}],
    [{"type": "required_status_checks", "parameters": {}}],
    [{"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": "true"}}],
])
def test_invalid_rule_shapes_fail_closed(rules):
    with pytest.raises(ValueError, match="rules"):
        feedback.check_current_base("ca-mrc/loom", "dev", OLD, read=lambda _: rules)


@pytest.mark.parametrize("payload", [None, {}, {"object": {}}, {"object": {"sha": "dev"}}, {"object": {"sha": None}}])
def test_invalid_current_ref_fails_closed(payload):
    with pytest.raises(ValueError, match=r"base.*SHA"):
        feedback.check_current_base("ca-mrc/loom", "dev", OLD, read=lambda path: [STRICT] if "/rules/" in path else payload)


@pytest.mark.parametrize("repository,branch,sha", [("ca-mrc/loom?token=secret", "dev", OLD), ("ca-mrc/loom", "", OLD), ("ca-mrc/loom", "dev", "dev")])
def test_invalid_input_fails_before_any_request(repository, branch, sha):
    def read(_):
        raise AssertionError("invalid input reached the API")

    with pytest.raises(ValueError):
        feedback.check_current_base(repository, branch, sha, read=read)


@pytest.mark.parametrize("failure", ["http", "timeout", "json", "missing"])
def test_cli_transport_failure_does_not_echo_token_or_response(monkeypatch, failure, capsys):
    secret = "private-token-do-not-print"

    def run(argv, **kwargs):
        assert argv == ["gh", "api", "repos/ca-mrc/loom/rules/branches/dev"]
        assert kwargs == {"capture_output": True, "text": True, "timeout": 20, "check": False}
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 20, output=secret, stderr=secret)
        if failure == "missing":
            raise OSError(secret)
        return subprocess.CompletedProcess(argv, 1 if failure == "http" else 0, secret, secret)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ValueError) as error:
        feedback.check_current_base("ca-mrc/loom", "dev", OLD)
    assert secret not in str(error.value)
    assert secret not in "".join(capsys.readouterr())


@pytest.mark.parametrize("current,mode,expected,requests", [(OLD, "success", 0, 2), (NEW, "success", 1, 2), (OLD, "http", 1, 1), (OLD, "json", 1, 1)])
def test_actual_check_base_cli_uses_bounded_native_transport(tmp_path, current, mode, expected, requests):
    calls = tmp_path / "calls"
    stub = tmp_path / "gh.py"
    stub.write_text(
        "import json, os, sys\n"
        "assert sys.argv[1] == 'api'\n"
        "with open(os.environ['CALLS'], 'a') as output: output.write(sys.argv[2] + '\\n')\n"
        "if os.environ['MODE'] == 'http':\n"
        " print(os.environ['GH_TOKEN'], file=sys.stderr); sys.exit(1)\n"
        "if os.environ['MODE'] == 'json':\n"
        " print(os.environ['GH_TOKEN']); sys.exit(0)\n"
        "print(json.dumps([{'type':'required_status_checks','parameters':{'strict_required_status_checks_policy':True}}]"
        " if '/rules/' in sys.argv[2] else {'object':{'sha':os.environ['CURRENT']}}))\n"
    )
    gh = tmp_path / "gh"
    gh.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(stub))} \"$@\"\n")
    gh.chmod(0o755)
    secret = "private-token-do-not-print"
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/ci_fast_feedback.py"), "check-base"],
        capture_output=True, text=True, env={**os.environ,
            "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
            "GITHUB_REPOSITORY": "ca-mrc/loom", "PR_BASE_REF": "dev", "PR_BASE_SHA": OLD,
            "GH_TOKEN": secret, "CALLS": str(calls), "CURRENT": current, "MODE": mode},
    )
    assert result.returncode == expected, result.stderr
    assert calls.read_text().splitlines() == ["repos/ca-mrc/loom/rules/branches/dev", "repos/ca-mrc/loom/git/ref/heads/dev"][:requests]
    assert secret not in result.stdout + result.stderr
