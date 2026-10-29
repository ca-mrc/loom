"""Stop obsolete strict-base PR fan-out without interfering with native queues."""

import os
import subprocess
import sys
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

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


def test_http_transport_failure_does_not_echo_token_or_response(monkeypatch, capsys):
    secret = "private-token-do-not-print"
    monkeypatch.setenv("GH_TOKEN", secret)

    def open_request(request, *, timeout):
        assert request.full_url == "https://api.github.com/repos/ca-mrc/loom/rules/branches/dev"
        assert request.get_header("Authorization") == f"Bearer {secret}"
        assert timeout == 20
        raise TimeoutError(secret)

    monkeypatch.setattr(urllib.request, "urlopen", open_request)
    with pytest.raises(ValueError) as error:
        feedback.check_current_base("ca-mrc/loom", "dev", OLD)
    assert secret not in str(error.value)
    assert secret not in "".join(capsys.readouterr())


@pytest.mark.parametrize("current,mode,expected", [(OLD, "success", 0), (NEW, "success", 1), (OLD, "http", 1), (OLD, "json", 1)])
def test_actual_check_base_cli_uses_bounded_native_http(tmp_path, current, mode, expected):
    # Exercise the real CLI without gh or external network access.
    import json

    secret = "private-token-do-not-print"
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            assert self.headers["Authorization"] == f"Bearer {secret}"
            self.send_response(503 if mode == "http" else 200)
            self.end_headers()
            value = [STRICT] if "/rules/" in self.path else {"object": {"sha": current}}
            self.wfile.write(secret.encode() if mode != "success" else json.dumps(value).encode())

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/ci_fast_feedback.py"), "check-base"],
            capture_output=True, text=True, timeout=10, env={**os.environ,
                "GITHUB_API_URL": f"http://127.0.0.1:{server.server_port}",
                "GITHUB_REPOSITORY": "ca-mrc/loom", "PR_BASE_REF": "dev", "PR_BASE_SHA": OLD,
                "GH_TOKEN": secret},
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert result.returncode == expected, result.stderr
    requests = 2 if mode == "success" else 1
    assert calls == ["/repos/ca-mrc/loom/rules/branches/dev", "/repos/ca-mrc/loom/git/ref/heads/dev"][:requests]
    assert secret not in result.stdout + result.stderr
