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
            "namespace_uids": {"loom-west-execution": "11111111-1111-1111-1111-111111111111"}, "secret_key_reads": [],
            "resources": [
                {"kind": "Namespace", "name": "loom-west-execution", "namespace": None},
                {"kind": "ServiceAccount", "name": "loom-execution-attempt", "namespace": "loom-west-execution"},
                {"kind": "ServiceAccount", "name": "loom-execution-policy-probe", "namespace": "loom-west-execution"},
                {"kind": "Role", "name": "loom-execution-attempt", "namespace": "loom-west-execution"},
                {"kind": "ValidatingAdmissionPolicy", "name": "loom-west-execution-private-root-v1", "namespace": None},
            ],
        }],
    }


class FakeKubectl:
    def __init__(self, scope):
        self.calls = []
        self.namespace_uid = scope["regions"][0]["namespace_uids"][scope["regions"][0]["namespace"]]
        self.namespace_uids = copy.deepcopy(scope["regions"][0]["namespace_uids"])
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
            name = args[args.index("namespace") + 1]
            data = {"metadata": {"uid": self.namespace_uid if name == scope["regions"][0]["namespace"] else self.namespace_uids.get(name)}}
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
    region["namespace_uids"] = {region["namespace"]: native.namespace_uid}
    region["resources"] = [{"kind": d["kind"], "name": d["metadata"]["name"], "namespace": d["metadata"].get("namespace")} for d in documents]
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


def add_build_scope(scope):
    region = scope["regions"][0]
    build = region["namespace"] + "-build"
    region["namespace_uids"][build] = "22222222-2222-2222-2222-222222222222"
    region["resources"].extend([
        {"kind": "Namespace", "name": build, "namespace": None},
        {"kind": "ServiceAccount", "name": "loom-execution-attempt", "namespace": build},
    ])
    region["secret_key_reads"] = [{"name": "loom-task-build-source", "namespace": build, "required_keys": ["access-key", "secret-key"]}]
    gateway.validate_scope(scope)
    return build


def test_combined_namespace_identity_and_permission_scope(scope, native):
    build = add_build_scope(scope)
    native.namespace_uids[build] = scope["regions"][0]["namespace_uids"][build]
    # The same service account name is independently authorized in each scope.
    for namespace in ("loom-west-execution", build):
        body = {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "loom-execution-attempt", "namespace": namespace}}
        assert gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(body).encode())[0] == 0
        assert gateway.handle(scope, regional(scope, "auth", "can-i", "patch", "serviceaccounts", "-n", namespace))[0] == 0
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, "auth", "can-i", "patch", "roles.rbac.authorization.k8s.io", "-n", build))
    native.namespace_uids[build] = "replaced-build-namespace"
    with pytest.raises(gateway.ScopeError, match="namespace identity"):
        gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(body).encode())


def test_secret_read_only_fixed_key_names_never_values(scope, native, monkeypatch):
    build = add_build_scope(scope)
    native.namespace_uids[build] = scope["regions"][0]["namespace_uids"][build]

    def output(scope, args, **kwargs):
        if args[-1] == gateway.SECRET_KEY_TEMPLATE:
            return subprocess.CompletedProcess(args, 0, b"access-key\nsecret-key\nundeclared-key\nsynthetic-private-value\n", b"synthetic-private-error")
        return native(scope, args, **kwargs)

    monkeypatch.setattr(gateway, "_run", output)
    args = ["get", "secret", "loom-task-build-source", "-n", build, "-o", gateway.SECRET_KEY_TEMPLATE]
    code, stdout, stderr = gateway.handle(scope, regional(scope, *args))
    assert code == 0 and stdout == b"access-key\nsecret-key\n" and stderr == b""
    for replacement in ["json", 'go-template={{.data}}', 'go-template={{index .data "secret-key"}}']:
        with pytest.raises(gateway.ScopeError):
            gateway.handle(scope, regional(scope, *args[:-1], replacement))
    for name, namespace in [("unknown-secret", build), ("loom-task-build-source", "loom-west-execution")]:
        with pytest.raises(gateway.ScopeError):
            gateway.handle(scope, regional(scope, "get", "secret", name, "-n", namespace, "-o", gateway.SECRET_KEY_TEMPLATE))
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, "auth", "can-i", "create", "secrets", "-n", build))
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "loom-task-build-source", "namespace": build}}
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(secret).encode())


def test_scope_rejects_additional_namespace_or_missing_namespace_identity(scope):
    for mutation in ("foreign", "missing_build_resource"):
        value = copy.deepcopy(scope)
        region = value["regions"][0]
        region["namespace_uids"]["foreign" if mutation == "foreign" else region["namespace"] + "-build"] = "22222222-2222-2222-2222-222222222222"
        with pytest.raises(gateway.ScopeError):
            gateway.validate_scope(value)


def test_combined_preflight_through_installed_wrapper_and_native_kubectl(regional_inputs, scope, tmp_path, monkeypatch):
    """Real deployer/SSH serialization, TLS discovery, native key-name template."""
    import base64
    import datetime
    import http.server
    import ipaddress
    import shutil
    import ssl
    import threading

    import yaml
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    from scripts.ops import deploy_nebius_platform as deploy

    from loom.nebius_platform_render import build_regional_execution

    executable = shutil.which("kubectl")
    if executable is None:
        pytest.skip("native kubectl unavailable; run operator protocol check where installed")
    config, candidate, _ = regional_inputs
    target = config["regional_execution_targets"][0]
    target["cluster_id"] = "mk8s-test"
    target["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
        "cache_bucket": "loom-shared-build-cache", "max_concurrent": 2,
        "cpu_millis": 8000, "memory_mib": 8192, "ephemeral_storage_mib": 65536,
        "archive_secret_name": "loom-task-image-archive-writer",
    }
    required = deploy._build_secret_requirements(target)
    secret_value = base64.b64encode(b"synthetic-secret-value-never-over-ssh").decode()
    requests = []
    state = {"missing_build_namespace": False, "documents": []}
    resources = {
        "v1": [{"name": "nodes", "kind": "Node", "namespaced": False},
               {"name": "secrets", "kind": "Secret", "namespaced": True}],
        "authorization.k8s.io/v1": [{"name": "selfsubjectaccessreviews", "kind": "SelfSubjectAccessReview", "namespaced": False}],
        "foreign.example.test/v1": [{"name": "roles", "kind": "Role", "namespaced": True}],
    }

    class API(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def reply(self, data, status=200):
            body = json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            version = path.removeprefix("/apis/") if path.startswith("/apis/") else path.removeprefix("/api/")
            if path == "/api":
                self.reply({"kind": "APIVersions", "versions": ["v1"], "serverAddressByClientCIDRs": []})
            elif path == "/apis":
                groups = [{"name": v.split("/")[0], "versions": [{"groupVersion": v, "version": v.split("/")[1]}], "preferredVersion": {"groupVersion": v, "version": v.split("/")[1]}} for v in resources if "/" in v]
                self.reply({"kind": "APIGroupList", "groups": groups})
            elif version in resources:
                self.reply({"kind": "APIResourceList", "groupVersion": version, "resources": [{**r, "singularName": "", "verbs": ["get", "create", "patch"]} for r in resources[version]]})
            elif path == "/api/v1/nodes":
                self.reply({"apiVersion": "v1", "kind": "NodeList", "items": [{"apiVersion": "v1", "kind": "Node", "metadata": {"name": "computeinstance-test", "labels": {"loom.nebius/node-role": "system"}}, "spec": {"providerID": "nebius://computeinstance-test"}}]})
            elif "/secrets/" in path:
                parts = path.split("/")
                namespace, name = parts[4], parts[6]
                keys = required.get((namespace, name))
                if keys is None:
                    self.send_error(404)
                else:
                    self.reply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace}, "data": {key: secret_value for key in keys}})
            else:
                parts = path.split("/")
                name, plural = parts[-1], parts[-2]
                namespace = parts[parts.index("namespaces") + 1] if "namespaces" in parts and parts.index("namespaces") + 2 < len(parts) else None
                kind = next((v for k, v in gateway.RESOURCE_KINDS.items() if k.split(".", 1)[0] == plural), None)
                document = next((d for d in state["documents"] if d["kind"] == kind and d["metadata"]["name"] == name and d["metadata"].get("namespace") == namespace), None)
                if document is None or (kind == "Namespace" and name.endswith("-build") and state["missing_build_namespace"]):
                    self.send_error(404)
                else:
                    observed = copy.deepcopy(document)
                    if kind == "Namespace":
                        observed["metadata"]["uid"] = "11111111-1111-1111-1111-111111111111"
                    self.reply(observed)

        def do_POST(self):
            # Native clients may use protobuf for this authorization request;
            # returning JSON is supported independently of its wire encoding.
            body = self.rfile.read(int(self.headers["Content-Length"]))
            path = self.path.split("?", 1)[0]
            requests.append((path, body))
            if path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews":
                self.reply({"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview", "spec": {}, "status": {"allowed": True}}, 201)
            else:
                self.send_error(403)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), API)
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-loopback")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False).sign(key, hashes.SHA256()))
    ca, private = tmp_path / "ca.pem", tmp_path / "tls-key.pem"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    private.chmod(0o600)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(ca, private)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f"https://127.0.0.1:{server.server_port}"
        target["kubernetes_api_server"] = endpoint
        target["resource_observer_api_cidrs"] = ["192.0.2.20/32"]
        documents = build_regional_execution(config, candidate, repo_root=Path(__file__).resolve().parents[2])[target["target_id"]]
        state["documents"] = documents
        region = scope["regions"][0]
        region.update({"cluster_name": "cluster-west-test", "api_server": endpoint, "namespace": target["execution_namespace"],
                       "namespace_uids": {d["metadata"]["name"]: "11111111-1111-1111-1111-111111111111" for d in documents if d["kind"] == "Namespace"},
                       "resources": [{"kind": d["kind"], "name": d["metadata"]["name"], "namespace": d["metadata"].get("namespace")} for d in documents],
                       "secret_key_reads": [{"namespace": ns, "name": name, "required_keys": sorted(keys)} for (ns, name), keys in required.items()]})
        scope["kubectl"] = executable
        gateway.validate_scope(scope)
        for document in documents:
            version = document["apiVersion"]
            plural = next(k.split(".", 1)[0] for k, v in gateway.RESOURCE_KINDS.items() if v == document["kind"])
            row = {"name": plural, "kind": document["kind"], "namespaced": document["kind"] not in gateway.CLUSTER_KINDS}
            if row not in resources.setdefault(version, []):
                resources[version].append(row)
        Path(scope["kubeconfig"]).write_text(json.dumps({"apiVersion": "v1", "kind": "Config", "clusters": [{"name": region["cluster_name"], "cluster": {"server": endpoint, "certificate-authority": str(ca)}}], "contexts": [{"name": "existing-west", "context": {"cluster": region["cluster_name"], "user": "synthetic"}}], "users": [{"name": "synthetic", "user": {"token": "synthetic-token-never-over-ssh"}}], "current-context": "existing-west"}))
        tmp_path.chmod(0o700)
        wrapper = tmp_path / "github-kubectl.py"
        wrapper.write_text(f'import os\ntry:\n    expected = {scope["kubeconfig"]!r}\nexcept Exception:\n    raise\nos.execv({executable!r}, ["kubectl"])\n')
        wrapper.chmod(0o600)
        install(tmp_path, Path(gateway.__file__).read_bytes(), scope, "a" * 40, apply=True)
        render = tmp_path / "render"
        render.mkdir()
        (render / (target["target_id"] + ".yaml")).write_text(yaml.safe_dump_all(documents))
        actual_run = subprocess.run
        ssh_outputs = []
        ssh_commands = []

        def ssh(argv, *, input, **kwargs):
            assert argv[0] == "ssh"
            result = actual_run([sys.executable, str(wrapper)], input=input, capture_output=True, text=True, timeout=30,
                                env={**os.environ, "HOME": str(tmp_path), "SSH_ORIGINAL_COMMAND": argv[-1]})
            ssh_outputs.append(result.stdout + result.stderr)
            ssh_commands.append(shlex.split(argv[-1]))
            if shlex.split(argv[-1])[-6:] == ["get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json"]:
                assert result.returncode == 0, result.stderr
            return result

        monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "operator@gateway.example.test")
        monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", str(tmp_path / "known_hosts"))
        monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", str(tmp_path / "ssh-key"))
        monkeypatch.setattr(deploy.subprocess, "run", ssh)
        selected = deploy.preflight_regional_rollout(deploy.Kubectl(Path(scope["kubeconfig"])), config, render)
        assert list(selected) == [target["target_id"]]
        assert {args[args.index("-n") + 1] for args in ssh_commands if "auth" in args and "serviceaccounts" in args} == {target["execution_namespace"], target["execution_namespace"] + "-build", target["execution_namespace"] + "-observations"}
        assert any(output == "access-key\nsecret-key\n" for output in ssh_outputs)
        remote = selected[target["target_id"]][0]
        for args in [("get", "secret", "loom-task-build-source", "-n", target["execution_namespace"] + "-build", "-o", "json"),
                     ("auth", "can-i", "patch", "serviceaccounts", "-n", "foreign")]:
            with pytest.raises(deploy.DeploymentError):
                remote.run(*args)
        for namespace, name, version in [(target["execution_namespace"] + "-build", "new-resource", "v1"),
                                         ("foreign", "loom-execution-attempt", "v1"),
                                         (target["execution_namespace"] + "-build", "loom-execution-attempt", "foreign.example.test/v1")]:
            if version.startswith("foreign"):
                name = next(d["metadata"]["name"] for d in documents if d["kind"] == "Role" and d["metadata"].get("namespace") == namespace)
            body = {"apiVersion": version, "kind": "Role" if version.startswith("foreign") else "ServiceAccount", "metadata": {"name": name, "namespace": namespace}}
            file = tmp_path / "rejected.json"
            file.write_text(json.dumps(body))
            with pytest.raises(deploy.DeploymentError):
                remote.run("apply", "-f", str(file))
        private_scope = tmp_path / "scope.json"
        private_scope.write_text(json.dumps(scope))
        private_scope.chmod(0o600)
        plan_command = [sys.executable, "-m", "scripts.ops.install_nebius_regional_rollout_gateway", "--root", str(tmp_path), "--source-sha", "a" * 40, "--scope", str(private_scope)]
        operator_env = {**os.environ, "HOME": str(tmp_path), "PYTHONPATH": "src:."}
        before_plan = wrapper.read_bytes()
        planned = actual_run(plan_command, capture_output=True, text=True, env=operator_env, timeout=30)
        assert planned.returncode == 0 and json.loads(planned.stdout)["status"] == "planned", planned.stdout
        state["missing_build_namespace"] = True
        blocked = actual_run(plan_command, capture_output=True, text=True, env=operator_env, timeout=30)
        assert blocked.returncode == 1 and json.loads(blocked.stdout)["status"] == "blocked"
        assert wrapper.read_bytes() == before_plan
        assert all(path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" for path, _ in requests)
        assert not any(secret_value in output or "synthetic-secret-value-never-over-ssh" in output or "synthetic-token-never-over-ssh" in output for output in ssh_outputs)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _observer_scope(scope, native):
    namespace = scope["regions"][0]["namespace"] + "-observations"
    uid = "22222222-2222-2222-2222-222222222222"
    region = scope["regions"][0]
    region["namespace_uids"][namespace] = uid
    region["resources"].extend([
        {"kind": "Namespace", "name": namespace, "namespace": None},
        {"kind": "DaemonSet", "name": gateway.OBSERVER_NAME, "namespace": namespace},
        {"kind": "CiliumNetworkPolicy", "name": gateway.OBSERVER_NAME, "namespace": namespace},
    ])
    native.namespace_uids[namespace] = uid
    return namespace


def test_observer_scope_accepts_only_exact_daemonset_and_policy(scope, native):
    namespace = _observer_scope(scope, native)
    gateway.validate_scope(scope)
    for kind, version in [("DaemonSet", "apps/v1"), ("CiliumNetworkPolicy", "cilium.io/v2")]:
        payload = json.dumps({"apiVersion": version, "kind": kind, "metadata": {"name": gateway.OBSERVER_NAME, "namespace": namespace}}).encode()
        assert gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=payload)[0] == 0
        foreign = json.loads(payload)
        foreign["metadata"]["name"] = "foreign-observer"
        with pytest.raises(gateway.ScopeError, match="resource identities"):
            gateway.handle(scope, regional(scope, "apply", "-f", "-"), stdin=json.dumps(foreign).encode())
    escaped = copy.deepcopy(scope)
    escaped["regions"][0]["resources"][-2]["namespace"] = "loom-west-execution"
    with pytest.raises(gateway.ScopeError, match="exact dedicated observer"):
        gateway.validate_scope(escaped)


def test_observer_readiness_wait_and_read_have_exact_scoped_commands(scope, native):
    namespace = _observer_scope(scope, native)
    for args in [("rollout", "status", "daemonset/" + gateway.OBSERVER_NAME, "-n", namespace, "--timeout=300s"),
                 ("get", "daemonset", gateway.OBSERVER_NAME, "-n", namespace, "--ignore-not-found", "-o", "json")]:
        assert gateway.handle(scope, regional(scope, *args))[0] == 0
    with pytest.raises(gateway.ScopeError):
        gateway.handle(scope, regional(scope, "rollout", "status", "daemonset/foreign", "-n", namespace, "--timeout=300s"))
