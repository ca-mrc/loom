"""Execute the fixed probe against real HTTP; no second Loom process is started."""
from __future__ import annotations

import http.client
import importlib
import json
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest


def module():
    return importlib.import_module("scripts.ops.nebius_development_probe")


@pytest.fixture
def service(monkeypatch):
    state = SimpleNamespace(status=200, auth=True, mode="api_only", revision="a" * 40, paths=[], oversized=False)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            auth = self.headers.get("Authorization")
            state.paths.append((self.path, auth))
            code = 200
            body = {"buildRevision": state.revision}
            if self.path == "/api/v1/health/ready":
                code = state.status if not state.auth or auth == "Bearer private-admin-token" else 401
                body = {"status": "ready", "mode": state.mode, "postgres": "ready", "object_store": "ready", "blockers": []}
            self.send_response(code)
            self.end_headers()
            self.wfile.write(b"x" * 9000 if state.oversized else json.dumps(body).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original = http.client.HTTPConnection

    def connection(host, port, *, timeout):
        assert (host, port, timeout) == ("127.0.0.1", 8090, 5)
        return original("127.0.0.1", server.server_port, timeout=timeout)

    monkeypatch.setattr(http.client, "HTTPConnection", connection)
    original_read = Path.read_text

    def read(path, *args, **kwargs):
        if str(path) == "/var/run/loom/admin/secrets.toml":
            return '[admin]\ntoken = "private-admin-token"\n'
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(sys, "argv", ["-c", "a" * 40])
    yield state
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def test_fixed_probe_qualifies_auth_dependencies_and_exact_running_build(service, capsys):
    exec(compile(module().SERVICE_PROBE, "<fixed-dev-probe>", "exec"), {})
    assert json.loads(capsys.readouterr().out) == {"status": "development_dependencies_verified", "candidate": "a" * 40}
    assert service.paths == [("/api/v1/health/ready", None),
        ("/api/v1/health/ready", "Bearer loom-dev-invalid-probe-token"),
        ("/api/v1/health/ready", "Bearer private-admin-token"), ("/api/v1/version", None)]


@pytest.mark.parametrize("failure", ["auth", "status", "mode", "revision", "oversized"])
def test_probe_failure_emits_no_response_body_or_private_material(service, capsys, failure):
    setattr(service, failure, {"auth": False, "status": 503, "mode": "application", "revision": "b" * 40,
                              "oversized": True}[failure])
    with pytest.raises(SystemExit) as error:
        exec(compile(module().SERVICE_PROBE, "<fixed-dev-probe>", "exec"), {})
    assert error.value.code == 1
    assert capsys.readouterr().out == ""


def test_probe_transport_uses_private_temporary_kubeconfig_and_fixed_bounded_command(tmp_path, monkeypatch):
    mod = module()
    executable = tmp_path / "kubectl"
    executable.write_text("test-only executable")
    executable.chmod(0o700)
    calls = []

    def run(args, *, timeout):
        assert timeout == 45
        config_path = Path(args[2])
        config = json.loads(config_path.read_text())
        assert config["clusters"][0]["cluster"]["server"] == "https://cluster.example"
        assert config["clusters"][0]["cluster"]["certificate-authority-data"]
        assert config["users"] == [{"name": "operator", "user": {"token": "private-k8s-token"}}]
        assert config_path.stat().st_mode & 0o077 == 0
        assert args[3:] == ["--context=dev-proof", "--request-timeout=10s", "--namespace=loom-dev",
            "exec", "--pod-running-timeout=10s", "loom-service-test", "--container=loom-service", "--",
            "python", "-c", mod.SERVICE_PROBE, "a" * 40]
        assert "private-k8s-token" not in str(args)
        calls.append(config_path)
        return json.dumps({"status": "development_dependencies_verified", "candidate": "a" * 40}).encode()

    monkeypatch.setattr(mod, "run_private", run)
    mod.probe_private_service(kubectl=executable, api_server="https://cluster.example",
        ssl_context=ssl.create_default_context(), token="private-k8s-token", pod_name="loom-service-test", candidate="a" * 40)
    assert len(calls) == 1 and not calls[0].exists()


@pytest.mark.parametrize("report", [b'{"status":"ready"}', b"private-sensitive-error", b"x" * 70000])
def test_probe_transport_rejects_unqualified_report_without_echoing_it(tmp_path, monkeypatch, report):
    mod = module()
    executable = tmp_path / "kubectl"
    executable.write_text("test-only executable")
    executable.chmod(0o700)
    monkeypatch.setattr(mod, "run_private", lambda *args, **kwargs: report)
    with pytest.raises(mod.DevelopmentInstallError) as error:
        mod.probe_private_service(kubectl=executable, api_server="https://cluster.example",
            ssl_context=ssl.create_default_context(), token="private-k8s-token", pod_name="loom-service-test", candidate="a" * 40)
    assert "sensitive" not in str(error.value)
