"""The new regional route cannot widen retained primary or regional grants."""
from __future__ import annotations

import copy
import json
import os
import shlex
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ops import nebius_regional_rollout_gateway as gateway
from scripts.ops.install_nebius_regional_rollout_gateway import install, restore
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs


@pytest.fixture
def scope(tmp_path):
    return {
        "schema_version": gateway.SCHEMA,
        "kubeconfig": str(tmp_path / "retained-kubeconfig"),
        "kubectl": str(tmp_path / "kubectl"),
        "regions": [{
            "context": "existing-west", "cluster_name": "cluster-west",
            "api_server": "https://west.example.test", "namespace": "loom-west-execution",
            "namespace_uid": "11111111-1111-1111-1111-111111111111",
            "resources": [
                {"kind": "Namespace", "name": "loom-west-execution"},
                {"kind": "ServiceAccount", "name": "loom-execution-attempt"},
                {"kind": "ServiceAccount", "name": "loom-execution-policy-probe"},
                {"kind": "Role", "name": "loom-execution-attempt"},
                {"kind": "ValidatingAdmissionPolicy", "name": "loom-west-execution-private-root-v1"},
            ],
        }],
    }


class FakeKubectl:
    def __init__(self, scope):
        self.calls = []
        self.namespace_uid = scope["regions"][0]["namespace_uid"]
        self.view = {
            "clusters": [
                {"name": "cluster-west", "cluster": {"server": "https://west.example.test", "certificate-authority-data": "private-ca-data"}},
                {"name": "other-cluster", "cluster": {"server": "https://other.example.test"}},
            ],
            "contexts": [
                {"name": "existing-west", "context": {"cluster": "cluster-west", "user": "secret-user"}},
                {"name": "other-region", "context": {"cluster": "other-cluster"}},
            ],
            "users": [{"name": "secret-user", "user": {"token": "private-token", "client-key-data": "private-key", "exec": {"args": ["private-plugin-arg"]}}}],
        }

    def __call__(self, scope, args, *, payload=None, timeout=30):
        self.calls.append((args, payload))
        if args == ["config", "view", "-o", "json"]:
            data = self.view
        elif "--dry-run=client" in args:
            data = json.loads(payload)
        elif "get" in args and "namespace" in args:
            data = {"metadata": {"uid": self.namespace_uid}}
        else:
            data = {"accepted": True}
        return subprocess.CompletedProcess(args, 0, json.dumps(data).encode(), b"")


@pytest.fixture
def native(scope, monkeypatch):
    fake = FakeKubectl(scope)
    monkeypatch.setattr(gateway, "_run", fake)
    return fake


def command(scope, *args):
    return shlex.join(["kubectl", "--kubeconfig", scope["kubeconfig"], *args])


def regional(scope, *args):
    return command(scope, "--context", "existing-west", "--request-timeout=30s", *args)


def test_old_primary_and_exact_reader_grants_fall_through(scope, native):
    for cmd in [command(scope, "--request-timeout=30s", "get", "pods", "-n", "primary"), command(scope, "config", "view", "--minify", "-o", "json"), "loom-nebius-legacy-pool-completion-v1 retained-operation"]:
        assert gateway.handle(scope, cmd) is None
    assert native.calls == []


def test_discovery_projection_has_only_configured_context_identity(scope, native):
    code, output, error = gateway.handle(scope, command(scope, "config", "view", "-o", "json"))
    data = json.loads(output)
    assert code == 0 and error == b""
    assert data["clusters"] == [{"name": "cluster-west", "cluster": {"server": "https://west.example.test"}}]
    assert data["contexts"] == [{"name": "existing-west", "context": {"cluster": "cluster-west"}}]
    assert not any(s in output.decode() for s in ["private-", "users", "other-cluster", "other-region", "exec", "token"])
    _, output, _ = gateway.handle(scope, regional(scope, "config", "view", "--minify", "-o", "json"))
    assert json.loads(output)["clusters"][0]["cluster"]["certificate-authority"] == "retained-inline-ca"
    assert "private-" not in output.decode()


@pytest.mark.parametrize("args", [
    ["get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json"],
    ["get", "validatingadmissionpolicy", "loom-west-execution-private-root-v1", "-n", "loom-west-execution", "--ignore-not-found", "-o", "json"],
    ["auth", "can-i", "patch", "namespaces"],
    ["auth", "can-i", "create", "roles.rbac.authorization.k8s.io", "-n", "loom-west-execution"],
])
def test_exact_required_regional_read_and_permission_probes(scope, native, args):
    code, output, _ = gateway.handle(scope, regional(scope, *args))
    assert code == 0 and json.loads(output) == {"accepted": True}
    assert native.calls[-1][0] == ["--context", "existing-west", "--request-timeout=30s", *args]


@pytest.mark.parametrize("args", [
    ["get", "secrets", "-n", "loom-west-execution", "-o", "json"],
    ["get", "pods", "--all-namespaces", "-o", "json"],
    ["exec", "-n", "loom-west-execution", "pod", "--", "sh"],
    ["auth", "can-i", "create", "roles.rbac.authorization.k8s.io", "-n", "foreign"],
    ["config", "view", "--raw", "-o", "json"],
    ["get", "nodes", "--server=https://other.example.test", "-o", "json"],
    ["get", "nodes", "--as=other", "-o", "json"],
    ["apply", "-f", "https://remote.example.test/manifest"],
])
def test_connection_namespace_and_arbitrary_operation_escape_rejected(scope, native, args):
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, *args))
    assert not any("apply" in args for args, _ in native.calls)


def test_unconfigured_context_and_context_alias_rejected(scope, native):
    for cmd in [command(scope, "--context", "other-region", "get", "nodes"), command(scope, "--context=existing-west", "get", "nodes")]:
        with pytest.raises(gateway.ScopeError):
            gateway.handle(scope, cmd)
    assert native.calls == []


def test_apply_accepts_only_existing_resource_identities_and_namespace_uid(scope, native):
    body = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "loom-execution-attempt", "namespace": "loom-west-execution"}}
    payload = json.dumps(body).encode()
    assert gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=payload)[0] == 0
    assert native.calls[-1][1] == payload
    assert "--dry-run=client" in native.calls[-3][0]
    native.namespace_uid = "replaced-namespace"
    with pytest.raises(gateway.ScopeError, match="namespace identity"):
        gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=payload)


@pytest.mark.parametrize("body", [
    {"kind": "Secret", "metadata": {"name": "unexpected", "namespace": "loom-west-execution"}},
    {"kind": "Role", "metadata": {"name": "other-role", "namespace": "loom-west-execution"}},
    {"kind": "ServiceAccount", "metadata": {"name": "loom-execution-attempt", "namespace": "foreign"}},
    {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "loom-execution-policy-probe", "namespace": "loom-west-execution"}},
    {"kind": "List", "items": [{"kind": "Namespace", "metadata": {"name": "loom-west-execution"}}, {"kind": "Namespace", "metadata": {"name": "foreign"}}]},
])
def test_manifest_scope_rejects_before_regional_apply(scope, native, body):
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(body).encode())
    assert not any("apply" in args for args, _ in native.calls)


def test_policy_probe_is_server_dry_run_only_and_context_drift_fails(scope, native):
    probe = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "loom-execution-policy-probe", "namespace": "loom-west-execution"}}
    assert gateway.handle(scope, regional(scope, "apply", "-f", "-", "--dry-run=server"), stdin=json.dumps(probe).encode())[0] == 0
    native.view["clusters"][0]["cluster"]["server"] = "https://replaced.example.test"
    with pytest.raises(gateway.ScopeError, match="differs"):
        gateway.handle(scope, regional(scope, "get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json"))


def test_plan_atomic_install_legacy_grants_idempotency_and_restore(scope, tmp_path):
    tmp_path.chmod(0o700)
    wrapper = tmp_path / "github-kubectl.py"
    original = (f'# retained completion, retirement, shared-input and primary grants\ntry:\n    expected = {scope["kubeconfig"]!r}\nexcept Exception:\n    raise\nif False:\n    import os\n    os.execv({scope["kubectl"]!r}, [])\nprint("old grant")\n').encode()
    wrapper.write_bytes(original)
    wrapper.chmod(0o600)
    source = b'def entry(path):\n    print("narrow extension")\n    return 0\n'
    revision = "a" * 40
    assert install(tmp_path, source, scope, revision)["status"] == "planned"
    assert not (tmp_path / "regional-compatibility").exists()
    assert install(tmp_path, source, scope, revision, apply=True)["status"] == "installed"
    installed = wrapper.read_bytes()
    assert installed.endswith(original) and stat.S_IMODE(wrapper.stat().st_mode) == 0o600
    assert install(tmp_path, source, scope, revision, apply=True)["status"] == "unchanged"
    for cmd, output in [(command(scope, "get", "pods"), "old grant"), (command(scope, "config", "view", "-o", "json"), "narrow extension"), ("loom-nebius-legacy-pool-completion-v1 operation", "old grant")]:
        result = subprocess.run([sys.executable, str(wrapper)], env={**os.environ, "SSH_ORIGINAL_COMMAND": cmd}, capture_output=True, text=True)
        assert result.returncode == 0 and result.stdout.strip() == output
    wrapper.write_bytes(installed + b"\n# later retained operator grant\n")
    with pytest.raises(ValueError, match="later gateway grants"):
        restore(tmp_path, revision)
    wrapper.write_bytes(installed)
    # A missing extension component cannot break an unrelated old grant.
    (tmp_path / "regional-compatibility" / revision / "scope.json").unlink()
    result = subprocess.run([sys.executable, str(wrapper)], env={**os.environ, "SSH_ORIGINAL_COMMAND": command(scope, "get", "pods")}, capture_output=True, text=True)
    assert result.returncode == 0 and result.stdout.strip() == "old grant"
    assert restore(tmp_path, revision)["status"] == "restored"
    assert wrapper.read_bytes() == original and stat.S_IMODE(wrapper.stat().st_mode) == 0o600


def test_installer_rejects_symlink_and_scope_wildcards(scope, tmp_path):
    tmp_path.chmod(0o700)
    target = tmp_path / "other"
    target.write_text('print("unchanged")')
    target.chmod(0o600)
    (tmp_path / "github-kubectl.py").symlink_to(target)
    with pytest.raises(ValueError, match="private"):
        install(tmp_path, b'print(1)', scope, "a" * 40, apply=True)
    wildcard = copy.deepcopy(scope)
    wildcard["regions"][0]["context"] = "*"
    with pytest.raises(gateway.ScopeError):
        gateway.validate_scope(wildcard)


@pytest.mark.parametrize("revision", ["../outside", "a" * 39, "a" * 41, "A" * 40])
def test_restore_rejects_revision_path_escape(tmp_path, revision):
    with pytest.raises(ValueError, match="revision"):
        restore(tmp_path, revision)


def test_installer_cannot_change_retained_connection(scope, tmp_path):
    tmp_path.chmod(0o700)
    wrapper = tmp_path / "github-kubectl.py"
    original = b'expected = "/retained/config"\nimport os\nos.execv("/usr/local/bin/kubectl", [])\n'
    wrapper.write_bytes(original)
    wrapper.chmod(0o600)
    with pytest.raises(ValueError, match="preserve the retained"):
        install(tmp_path, b'print(1)', scope, "a" * 40, apply=True)
    assert wrapper.read_bytes() == original
    assert not (tmp_path / "regional-compatibility").exists()


def test_real_driver_serialization_reaches_gateway(scope, native, tmp_path, monkeypatch):
    from scripts.ops import deploy_nebius_platform as deploy

    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "operator@gateway.example.test")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", str(tmp_path / "known_hosts"))
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", str(tmp_path / "key"))
    captured = []

    def ssh(argv, *, input, **kwargs):
        assert argv[0] == "ssh" and argv[-2] == "operator@gateway.example.test"
        captured.append((shlex.split(argv[-1]), input))
        result = gateway.handle(scope, argv[-1], stdin=input.encode() if input else None)
        assert result is not None
        code, output, error = result
        return subprocess.CompletedProcess(argv, code, output.decode(), error.decode())

    monkeypatch.setattr(deploy.subprocess, "run", ssh)
    primary = deploy.Kubectl(Path(scope["kubeconfig"]))
    primary.run("config", "view", "-o", "json")
    remote = deploy.Kubectl(Path(scope["kubeconfig"]), context="existing-west")
    remote.run("config", "view", "--minify", "-o", "json")
    remote.run("auth", "can-i", "patch", "namespaces")
    remote.get("validatingadmissionpolicy", "loom-west-execution-private-root-v1", "loom-west-execution")
    body = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "loom-execution-policy-probe", "namespace": "loom-west-execution"}}
    probe = tmp_path / "policy probe.json"
    probe.write_text(json.dumps(body))
    remote.run("apply", "-f", str(probe), "--dry-run=server")
    assert captured[0][0][3:] == ["--request-timeout=30s", "config", "view", "-o", "json"]
    assert captured[-1][0][3:] == ["--context", "existing-west", "--request-timeout=30s", "apply", "-f", "-", "--dry-run=server"]
    assert json.loads(captured[-1][1]) == body


def test_native_kubectl_parses_full_regional_multidocument_render(regional_inputs, scope, native, tmp_path, monkeypatch):
    """Exercise the real native YAML parser against disposable API discovery."""
    import http.server
    import shutil
    import threading

    import yaml

    from loom.nebius_platform_render import build_regional_execution

    executable = shutil.which("kubectl")
    if executable is None:
        pytest.skip("native kubectl unavailable; run this operator parser check where installed")
    config, candidate, _ = regional_inputs
    target = config["regional_execution_targets"][0]
    target["task_identity_policy"] = {"mode": "private-root-v1", "target_id": target["target_id"], "execution_namespace": target["execution_namespace"]}
    documents = build_regional_execution(config, candidate, repo_root=Path(__file__).resolve().parents[2])[target["target_id"]]
    payload = yaml.safe_dump_all(documents).encode()
    region = scope["regions"][0]
    region["namespace"] = target["execution_namespace"]
    region["resources"] = [{"kind": d["kind"], "name": d["metadata"]["name"]} for d in documents]
    resources = {}
    for document in documents:
        resource = next(k for k, v in gateway.RESOURCE_KINDS.items() if v == document["kind"])
        resources.setdefault(document["apiVersion"], []).append({
            "name": resource.split(".", 1)[0], "singularName": "", "kind": document["kind"],
            "namespaced": document["kind"] not in gateway.CLUSTER_KINDS, "verbs": ["create"],
        })
    groups = [{"name": v.split("/")[0], "versions": [{"groupVersion": v, "version": v.split("/")[1]}], "preferredVersion": {"groupVersion": v, "version": v.split("/")[1]}} for v in resources if "/" in v]

    class Discovery(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            version = path.removeprefix("/apis/") if path.startswith("/apis/") else path.removeprefix("/api/")
            if path == "/api":
                data = {"kind": "APIVersions", "versions": ["v1"], "serverAddressByClientCIDRs": []}
            elif path == "/apis":
                data = {"kind": "APIGroupList", "groups": groups}
            elif version in resources:
                data = {"kind": "APIResourceList", "groupVersion": version, "resources": resources[version]}
            else:
                self.send_error(404)
                return
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Discovery)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = tmp_path / "synthetic-config"
    connection.write_text(json.dumps({"apiVersion": "v1", "kind": "Config", "clusters": [{"name": "synthetic", "cluster": {"server": f"http://127.0.0.1:{server.server_port}"}}], "contexts": [{"name": "existing-west", "context": {"cluster": "synthetic"}}], "current-context": "existing-west"}))
    parsed = []
    try:
        def parse_or_fake(scope, args, *, payload=None, timeout=30):
            if "--dry-run=client" not in args:
                return native(scope, args, payload=payload, timeout=timeout)
            result = subprocess.run([executable, "--kubeconfig", str(connection), "--cache-dir", str(tmp_path / "discovery-cache"), *args], input=payload, capture_output=True, timeout=timeout)
            assert result.returncode == 0, result.stderr.decode()
            parsed.append(result.stdout)
            return result

        monkeypatch.setattr(gateway, "_run", parse_or_fake)
        assert gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=payload)[0] == 0
        assert native.calls[-1][1] == payload
        assert len(documents) > 5
        # Native client versions may print a List or concatenated JSON objects.
        remaining = parsed[0].decode().strip()
        parsed_documents = []
        while remaining:
            item, offset = json.JSONDecoder().raw_decode(remaining)
            parsed_documents.extend(item["items"] if item.get("kind") == "List" else [item])
            remaining = remaining[offset:].lstrip()
        assert len(parsed_documents) == len(documents)
        foreign = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": documents[1]["metadata"]["name"], "namespace": "foreign"}}
        native.calls.clear()
        with pytest.raises(gateway.ScopeError, match="outside installed"):
            gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=payload + b"---\n" + yaml.safe_dump(foreign).encode())
        assert not any("apply" in args for args, _ in native.calls)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()



def test_same_kind_name_in_foreign_api_group_rejected(scope, native):
    body = {"apiVersion": "foreign.example.test/v1", "kind": "Role", "metadata": {"name": "loom-execution-attempt", "namespace": "loom-west-execution"}}
    with pytest.raises(gateway.ScopeError, match="API group"):
        gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(body).encode())
    assert not any("apply" in args for args, _ in native.calls)
