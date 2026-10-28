"""Actual shared HTTPS, legacy SNI passthrough and namespace CNI isolation."""

from __future__ import annotations

import http.client
import inspect
import json
import os
import socket
import ssl
import subprocess
import sys
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_environment_render import render_environment
from loom.nebius_platform_render import _namespace, build_platform
from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import BOB, registration_for
from tests.unit.test_nebius_shared_ingress import ingress_input as ingress_input

# Qualified upstream versions; production inputs require their native registry mirrors.
TRAEFIK = "docker.io/library/traefik@sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18"
PYTHON = "docker.io/library/python@sha256:9b8dad7f66b5c7751df6cb7a64a07812e86bed85d0116efe82b3a11209f1440d"
ROOT = Path(__file__).resolve().parents[2]
SERVER = '''import base64, hashlib, http.server, os, ssl, time
upload_bytes = 0
class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_POST(self):
        global upload_bytes
        upload_bytes += len(self.rfile.read(1))
        upload_bytes += len(self.rfile.read(1))
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")
    def do_GET(self):
        if self.headers.get("Upgrade", "").lower() == "websocket":
            accept = base64.b64encode(hashlib.sha1((self.headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            self.wfile.write(b"\\x81\\x05hello")
            self.wfile.flush()
            self.close_connection = True
            return
        body = (os.environ["IDENTITY"] + ":" + os.environ["PORT"]).encode()
        if self.path == "/api/upload-observed": body = str(upload_bytes).encode()
        if self.path == "/api/stream": body = b"first-second"
        self.send_response(200)
        if self.path == "/api/stream":
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
        else: self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.path == "/api/stream":
            self.wfile.write(b"6\\r\\n" + body[:6] + b"\\r\\n"); self.wfile.flush(); time.sleep(2)
            self.wfile.write(b"6\\r\\n" + body[6:] + b"\\r\\n0\\r\\n\\r\\n"); self.wfile.flush()
        else: self.wfile.write(body)
    def log_message(self, *args): pass
server = http.server.ThreadingHTTPServer(("0.0.0.0", int(os.environ["PORT"])), Handler)
if os.environ["PORT"] == "8443":
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
    context.set_alpn_protocols(["http/1.1", "acme-tls/1"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
'''


def _certificate(hosts):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, hosts[0])])
    now = datetime.now(UTC)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
                   .add_extension(x509.SubjectAlternativeName([x509.DNSName(h) for h in hosts]), False)
                   .sign(key, hashes.SHA256()))
    return {"tls.crt": certificate.public_bytes(serialization.Encoding.PEM).decode(),
            "tls.key": key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()).decode()}


def _run(container, *args, payload=None, timeout=120):
    result = subprocess.run(["docker", "exec", "-i", container.get_wrapped_container().id, *args],
                            input=payload, text=True, capture_output=True, timeout=timeout)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _kernel_wait_snapshot(ident, *, proc_root="/proc", module_root="/sys/module"):
    """Read only kernel fields of processes verified to belong to this node."""
    # Self-contained: the failure observer runs this function in a bounded child,
    # without importing the test module or relying on a responsive node exec.
    import json
    import os
    import re
    import subprocess
    from pathlib import Path

    if not isinstance(ident, str) or re.fullmatch(r"[0-9a-f]{64}", ident) is None:
        return {"error": "invalid_container_id"}
    proc = Path(proc_root)
    if not (proc / "self/stat").exists():
        return {"error": "host_proc_unavailable"}
    result = {"kernel_release": "unavailable", "modules": {}, "processes": [], "top": []}
    allowed = {"iptables", "ip6tables", "iptables-save", "ip6tables-save",
               "iptables-restor", "ip6tables-resto", "flannel", "bridge"}

    def read(fd, name, limit=4096):
        child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
        try:
            return os.read(child, limit).decode(errors="replace").strip()
        finally:
            os.close(child)

    def identity(fd):
        stat = read(fd, "stat")
        fields = stat[stat.rindex(")") + 2:].split()
        cgroup = read(fd, "cgroup")
        if re.search(r"(?:^|/)(?:docker-)?" + ident + r"(?:\.scope)?(?:/|$)",
                     cgroup, re.MULTILINE) is None:
            raise ValueError("unowned_process")
        return read(fd, "comm", 64), fields[19], fields[0], fields[1]

    def open_pid(pid):
        return os.open(proc / str(pid), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

    def token(path):
        with path.open() as handle:
            value = handle.read(128).strip()
        return value if re.fullmatch(r"[A-Za-z0-9_.+\-]+", value) else "unavailable"

    try:
        result["kernel_release"] = token(proc / "sys/kernel/osrelease")
    except OSError as error:
        result["kernel_release"] = type(error).__name__
    for module in ("nf_tables", "nft_compat", "ip_tables", "ip6_tables", "br_netfilter"):
        result["modules"][module] = {}
        for name in ("initstate", "refcnt"):
            try:
                result["modules"][module][name] = token(Path(module_root) / module / name)
            except OSError as error:
                result["modules"][module][name] = type(error).__name__
    init_fd = None
    try:
        # This observer never follows a persisted or environment-selected remote
        # Docker context. Ownership must resolve against this host's own /proc.
        docker = ["docker", "--host", "unix:///var/run/docker.sock"]
        inspect_result = subprocess.run([*docker, "inspect", "--format={{.State.Pid}}", ident],
                                        text=True, capture_output=True, timeout=1)
        pid_text = inspect_result.stdout.strip()
        if inspect_result.returncode or not pid_text.isdecimal() or not 0 < int(pid_text) < 2**31:
            return {"error": "node_pid_unavailable"}
        init_fd = open_pid(int(pid_text))
        initial = identity(init_fd)
        top = subprocess.run([*docker, "top", ident, "-eo", "pid,ppid,stat,wchan:32,comm"],
                             text=True, capture_output=True, timeout=1)
        if top.returncode:
            return {"error": "node_top_unavailable"}
        rows = []
        for line in top.stdout.splitlines()[:257]:
            fields = line.split()
            if len(fields) != 5 or not all(x.isdecimal() for x in fields[:2]):
                continue
            pid, ppid = map(int, fields[:2])
            state, wchan, comm = fields[2:]
            if (not 0 < pid < 2**31 or re.fullmatch(r"[DIRSTtWXZx][<NLsl+]{0,8}", state) is None
                    or not all(re.fullmatch(r"[A-Za-z0-9_.+\-]{1,64}", x) for x in (wchan, comm))):
                continue
            if re.fullmatch(r"(?:0x)?[0-9a-f]{8,}", wchan):
                wchan = "address_redacted"
            rows.append({"pid": pid, "ppid": ppid, "state": state, "wchan": wchan, "comm": comm})
        result["top"] = rows[:32]
        # Sleeping CNI helpers can precede every blocked netfilter child. Spend
        # the existing eight-stack cap on D-state targets first, stably ordered.
        candidates = sorted((row for row in rows if row["comm"] in allowed),
                            key=lambda row: not row["state"].startswith("D"))
        for row in candidates[:8]:
            pid, comm = row["pid"], row["comm"]
            item = {"pid": pid}
            fd = None
            try:
                fd = open_pid(pid)
                before = identity(fd)
                if before[0] != comm:
                    raise ValueError("changed_process")
                wchan = read(fd, "wchan", 64)
                if re.fullmatch(r"[A-Za-z0-9_.+\-]{1,64}", wchan) is None:
                    wchan = "unavailable"
                elif re.fullmatch(r"(?:0x)?[0-9a-f]{8,}", wchan):
                    wchan = "address_redacted"
                item.update(comm=comm, state=before[2], ppid=int(before[3]), wchan=wchan)
                try:
                    # Retain function/module names, never kernel addresses.
                    frames = re.findall(r"([A-Za-z_][A-Za-z0-9_.$]*)\+0x[0-9a-f]+/0x[0-9a-f]+"
                                        r"(?:\s+\[([A-Za-z0-9_.\-]+)\])?", read(fd, "stack", 1024))
                    item["stack"] = [{"function": function, "module": module or "kernel"}
                                     for function, module in frames[:16]]
                except OSError as error:
                    item["stack_error"] = type(error).__name__
                if identity(fd)[:2] != before[:2]:
                    raise ValueError("changed_process")
            except (OSError, ValueError, IndexError) as error:
                item = {"pid": pid, "error": type(error).__name__}
            finally:
                if fd is not None:
                    os.close(fd)
            result["processes"].append(item)
        if identity(init_fd)[:2] != initial[:2]:
            return {"error": "changed_node_process"}
    except (OSError, ValueError, IndexError, subprocess.SubprocessError) as error:
        return {"error": type(error).__name__}
    finally:
        if init_fd is not None:
            os.close(init_fd)
    while len(json.dumps(result)) > 15000:
        if result["top"]:
            result["top"].pop()
        elif result["processes"]:
            result["processes"].pop()
        else:
            return {"error": "snapshot_limit"}
    return result


def _add_failure_diagnostics(container, namespace, error, *, core=None):
    """Best-effort reads must not hide the original failing assertion/timeout."""
    if core is not None:
        # The node's exec path may itself be blocked. Read through the existing
        # authenticated API client first; never include Pod specs or Secrets.
        for label, read in (
            ("API Pod status", lambda: [{"name": pod.metadata.name, "node": pod.spec.node_name,
                "status": pod.status.to_dict()} for pod in core.list_namespaced_pod(
                    namespace, limit=32, _request_timeout=(2, 3)).items]),
            ("API warning events", lambda: [{"object": event.involved_object.name,
                "reason": event.reason, "message": event.message, "count": event.count}
                for event in core.list_namespaced_event(namespace, limit=32,
                    field_selector="type=Warning", _request_timeout=(2, 3)).items]),
        ):
            try:
                detail = json.dumps(read(), default=str)
            except Exception as diagnostic_error:
                detail = type(diagnostic_error).__name__
            error.add_note(label + "\n" + detail[-16384:])
    ident = container.get_wrapped_container().id
    try:
        from scripts.configure_ci_registry_mirror import disposable_runner

        # Existing CI guests already permit sudo for daemon/tool setup. Never elevate
        # on a developer/shared host or a machine without Linux host proc access.
        privilege = ["sudo", "-n"] if (
            sys.platform == "linux" and Path("/proc/self/stat").exists()
            and os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_OS") == "Linux"
            and not os.environ.get("DOCKER_HOST") and not os.environ.get("DOCKER_CONTEXT")
            and disposable_runner()
        ) else []
        kernel_command = [*privilege, sys.executable, "-c", inspect.getsource(_kernel_wait_snapshot)
                          + "\nimport json, sys\nprint(json.dumps(_kernel_wait_snapshot(sys.argv[1])))", ident]
    except Exception as diagnostic_error:
        # Source/permission-scope discovery must remain best effort as well.
        error.add_note("owned-node kernel observer unavailable: " + type(diagnostic_error).__name__)
        kernel_command = ["docker", "top", ident, "-eo", "pid,ppid,stat,wchan:32,comm"]
    commands = [
        ["docker", "logs", "--tail=100", ident],
        ["docker", "inspect", "--format={{json .State}}", ident],
        kernel_command,
        *[["docker", "exec", ident, *command] for command in (
            ["kubectl", "--request-timeout=3s", "get", "pods", "-A", "-o", "wide"],
            ["kubectl", "--request-timeout=3s", "get", "events", "-A", "--sort-by=.metadata.creationTimestamp"],
            ["tail", "-n", "200", "/var/lib/rancher/k3s/agent/containerd/containerd.log"],
            ["crictl", "pods"],
            ["crictl", "ps", "-a"],
            ["iptables-save", "-c", "-t", "filter"],
            ["kubectl", "--request-timeout=3s", "logs", "-n", namespace,
             "deployment/loom-shared-ingress", "--tail=60"],
            ["kubectl", "--request-timeout=3s", "get", "services,endpointslices", "-n", namespace, "-o", "wide"],
            ["kubectl", "--request-timeout=3s", "get", "networkpolicies", "-n", "loom-dev-alice", "-o", "yaml"],
        )],
    ]
    for command in commands:
        try:
            result = subprocess.run(command, text=True, capture_output=True, timeout=5)
            output = ("collector unavailable" if command is kernel_command and result.returncode
                      else result.stdout + result.stderr)
            detail = f"exit={result.returncode}\n{output[-16300:]}"
        except subprocess.TimeoutExpired as diagnostic_error:
            # communicate() carries bytes even with text=True on a timeout.
            output = diagnostic_error.output or b""
            stderr = diagnostic_error.stderr or b""
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            detail = f"TimeoutExpired\n{(output + stderr)[-16300:]}"
        except Exception as diagnostic_error:
            detail = (type(diagnostic_error).__name__ if command is kernel_command
                      else f"{type(diagnostic_error).__name__}: {diagnostic_error}")
        label = "owned-node kernel wait snapshot" if command is kernel_command else " ".join(command)
        error.add_note(label + "\n" + detail[-16384:])


@pytest.mark.parametrize("original", [AssertionError("readiness failed"),
                                     subprocess.TimeoutExpired(["kubectl", "rollout"], 75),
                                     pytest.fail.Exception("route did not become ready")])
def test_failure_diagnostics_preserve_original_when_commands_timeout(monkeypatch, original):
    def fail_command(*args, **kwargs):
        assert 0 < kwargs["timeout"] <= 5
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"], output=b"partial output")

    monkeypatch.setattr(subprocess, "run", fail_command)
    from types import SimpleNamespace
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="disposable-node"))
    with pytest.raises(type(original)) as caught:
        try:
            raise original
        except (Exception, pytest.fail.Exception) as exc:
            _add_failure_diagnostics(container, "test-platform", exc)
            raise
    assert caught.value is original
    assert len(original.__notes__) >= 4
    assert all("TimeoutExpired" in note and "partial output" in note for note in original.__notes__)
    assert any("docker logs" in note for note in original.__notes__)
    assert any("events" in note for note in original.__notes__)


def test_failure_diagnostics_continue_after_error_and_bound_output(monkeypatch):
    from types import SimpleNamespace
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="disposable-node"))

    def command(args, **kwargs):
        assert 0 < kwargs["timeout"] <= 5
        if "iptables-save" in args:
            return subprocess.CompletedProcess(args, 1, "", "rules unavailable")
        if "tail" in args:
            return subprocess.CompletedProcess(args, 0, "RunPodSandbox returned sandbox-id", "")
        if "crictl" in args:
            return subprocess.CompletedProcess(args, 0, "runtime " + args[-1], "")
        if "events" in args and "type=Warning" not in args:
            return subprocess.CompletedProcess(args, 0, "Normal Pulling addon image", "")
        return subprocess.CompletedProcess(args, 0, "x" * 100000 + "useful tail", "")

    monkeypatch.setattr(subprocess, "run", command)
    error = AssertionError("original")
    _add_failure_diagnostics(container, "test-platform", error)
    assert any("rules unavailable" in note for note in error.__notes__)
    assert any("useful tail" in note for note in error.__notes__)
    assert all(len(note) < 17000 for note in error.__notes__)
    assert all("\nexit=" in note for note in error.__notes__)
    notes = "\n".join(error.__notes__)
    assert "RunPodSandbox returned sandbox-id" in notes
    assert "runtime pods" in notes and "runtime -a" in notes
    assert "Normal Pulling addon image" in notes


@pytest.mark.parametrize("pods_unavailable", [False, True])
def test_failure_diagnostics_read_api_without_exec_or_pod_specs(monkeypatch, pods_unavailable):
    from types import SimpleNamespace

    from kubernetes import client

    pod = client.V1Pod(metadata=client.V1ObjectMeta(name="control", annotations={"private": "private-annotation"}),
        spec=client.V1PodSpec(node_name="disposable-node", containers=[client.V1Container(
            name="fixture", env=[client.V1EnvVar(name="TOKEN", value="private-env")])]),
        status=client.V1PodStatus(phase="Pending", conditions=[client.V1PodCondition(
            type="PodReadyToStartContainers", status="False")]))
    event = client.CoreV1Event(metadata=client.V1ObjectMeta(),
        involved_object=client.V1ObjectReference(name="control", kind="Pod"),
        reason="FailedCreatePodSandBox", message="sandbox network stalled", count=3)

    def pods(namespace, **kwargs):
        assert namespace == "test-platform" and kwargs == {"limit": 32, "_request_timeout": (2, 3)}
        if pods_unavailable:
            raise RuntimeError("private-api-response")
        return client.V1PodList(items=[pod])

    def events(namespace, **kwargs):
        assert namespace == "test-platform"
        assert kwargs == {"limit": 32, "field_selector": "type=Warning", "_request_timeout": (2, 3)}
        return client.CoreV1EventList(items=[event])

    def command(args, **kwargs):
        if "exec" in args:
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return subprocess.CompletedProcess(args, 0, "process-wait-state", "")

    monkeypatch.setattr(subprocess, "run", command)
    core = SimpleNamespace(list_namespaced_pod=pods, list_namespaced_event=events)
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="disposable-node"))
    original = AssertionError("original")
    with pytest.raises(AssertionError) as caught:
        try:
            raise original
        except AssertionError as error:
            _add_failure_diagnostics(container, "test-platform", error, core=core)
            raise
    assert caught.value is original
    notes = "\n".join(original.__notes__)
    assert "FailedCreatePodSandBox" in notes and "sandbox network stalled" in notes
    assert "private-" not in notes
    assert any("owned-node kernel wait snapshot" in note and "process-wait-state" in note
               for note in original.__notes__)
    if pods_unavailable:
        assert "RuntimeError" in original.__notes__[0]
    else:
        assert "PodReadyToStartContainers" in notes and "Pending" in notes
    assert all(len(note) < 17000 for note in original.__notes__)


@pytest.fixture
def kernel_proc(tmp_path, monkeypatch):
    ident = "a" * 64
    proc = tmp_path / "proc"
    modules = tmp_path / "module"
    (proc / "self").mkdir(parents=True)
    (proc / "self/stat").write_text("supported")
    (proc / "sys/kernel").mkdir(parents=True)
    (proc / "sys/kernel/osrelease").write_text("6.14.0-test\n")

    def process(pid, comm, start):
        directory = proc / str(pid)
        directory.mkdir()
        (directory / "stat").write_text(f"{pid} ({comm}) D 100 " + "0 " * 17 + f"{start} 0\n")
        (directory / "comm").write_text(comm + "\n")
        (directory / "cgroup").write_text(f"0::/system.slice/docker-{ident}.scope\n")
        (directory / "wchan").write_text("call_usermodehelper_exec\n")
        (directory / "stack").write_text("[<ffffffff81234567>] call_usermodehelper_exec+0x1/0x20\n"
                                        "[<ffffffff81234568>] nft_chain_validate+0x2/0x30 [nf_tables]\n")
        (directory / "environ").write_text("private-env")
        (directory / "cmdline").write_text("private-argv")
        return directory

    process(100, "k3s", 10)
    target = process(201, "iptables-restor", 20)
    (modules / "nf_tables").mkdir(parents=True)
    (modules / "nf_tables/initstate").write_text("live\n")
    (modules / "nf_tables/refcnt").write_text("3\n")
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        assert args[:3] == ["docker", "--host", "unix:///var/run/docker.sock"]
        assert args[-1] == ident or args[-1] == "pid,ppid,stat,wchan:32,comm"
        assert kwargs == {"text": True, "capture_output": True, "timeout": 1}
        output = "100\n" if "inspect" in args else (
            "PID PPID STAT WCHAN COMMAND\n100 1 S futex_do_wait k3s\n"
            "201 100 D call_usermodehelper_exec iptables-restor\n")
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(subprocess, "run", command)
    return ident, proc, modules, target, calls


def test_kernel_wait_snapshot_retains_owned_chain_without_addresses_or_commands(kernel_proc):
    ident, proc, modules, _, calls = kernel_proc
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert snapshot["kernel_release"] == "6.14.0-test"
    assert snapshot["modules"]["nf_tables"] == {"initstate": "live", "refcnt": "3"}
    assert snapshot["processes"] == [{"pid": 201, "comm": "iptables-restor", "state": "D",
        "ppid": 100, "wchan": "call_usermodehelper_exec", "stack": [
            {"function": "call_usermodehelper_exec", "module": "kernel"},
            {"function": "nft_chain_validate", "module": "nf_tables"}]}]
    detail = json.dumps(snapshot)
    assert "ffffffff" not in detail and "private-" not in detail
    assert len(detail) < 16384 and len(calls) == 2


def test_kernel_wait_snapshot_prioritizes_blocked_targets_before_sleeping_helpers(kernel_proc, monkeypatch):
    from shutil import copytree

    ident, proc, modules, target, _ = kernel_proc
    rows = []
    for pid in range(301, 309):
        directory = proc / str(pid)
        copytree(target, directory)
        (directory / "comm").write_text("bridge\n")
        (directory / "stat").write_text((directory / "stat").read_text().replace(
            "201 (iptables-restor) D", f"{pid} (bridge) S"))
        rows.append(f"{pid} 100 Sl do_wait bridge\n")
    for pid in range(201, 208):
        if pid != 201:
            copytree(target, proc / str(pid))
        rows.append(f"{pid} 100 D call_usermodehelper_exec iptables-restor\n")

    def command(args, **kwargs):
        output = "100\n" if "inspect" in args else "PID PPID STAT WCHAN COMMAND\n" + "".join(rows)
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(subprocess, "run", command)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert [item["pid"] for item in snapshot["processes"]] == [*range(201, 208), 301]
    assert all(item["stack"] for item in snapshot["processes"][:7])
    assert snapshot["top"][0]["pid"] == 301  # Keep the original top output order.
    assert len(snapshot["top"]) <= 32 and len(snapshot["processes"]) == 8


@pytest.mark.parametrize("state,accepted", [
    ("D<", True), ("Dl+", True), ("Ss", True), ("SNLs+", True),
    ("D>", False), ("D;bad", False), ("Q", False),
])
def test_kernel_wait_snapshot_accepts_only_standard_ps_state_flags(kernel_proc, monkeypatch, state, accepted):
    ident, proc, modules, _, _ = kernel_proc

    def command(args, **kwargs):
        output = "100\n" if "inspect" in args else f"201 100 {state} do_wait iptables-restor\n"
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(subprocess, "run", command)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert bool(snapshot["processes"]) is accepted
    assert bool(snapshot["top"]) is accepted


@pytest.mark.parametrize("ident", ["", "a" * 63, "A" * 64, "a" * 64 + ";", "../201", 201])
def test_kernel_wait_snapshot_rejects_arbitrary_container_inputs(monkeypatch, ident):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("must not invoke Docker"))
    assert _kernel_wait_snapshot(ident) == {"error": "invalid_container_id"}


@pytest.mark.parametrize("cgroup", ["0::/docker/" + "b" * 64, "0::/docker/" + "a" * 64 + "-other"])
def test_kernel_wait_snapshot_never_reads_unowned_stack(kernel_proc, monkeypatch, cgroup):
    ident, proc, modules, target, _ = kernel_proc
    (target / "cgroup").write_text(cgroup + "\n")
    original_open = os.open

    def guarded_open(path, flags, *args, **kwargs):
        assert path != "stack", "unowned stack must not be read"
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", guarded_open)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert snapshot["processes"] == [{"pid": 201, "error": "ValueError"}]


@pytest.mark.parametrize("changed", ["target_starttime", "target_comm", "node_starttime"])
def test_kernel_wait_snapshot_discards_reused_process_identity(kernel_proc, monkeypatch, changed):
    ident, proc, modules, target, _ = kernel_proc
    original_open = os.open

    def changed_open(path, flags, *args, **kwargs):
        if path == "stack":
            if changed == "target_comm":
                (target / "comm").write_text("other\n")
            else:
                stat = (proc / "100" if changed == "node_starttime" else target) / "stat"
                stat.write_text(stat.read_text().replace("10 0\n", "11 0\n").replace("20 0\n", "21 0\n"))
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", changed_open)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    if changed == "node_starttime":
        assert snapshot == {"error": "changed_node_process"}
    else:
        assert snapshot["processes"] == [{"pid": 201, "error": "ValueError"}]


def test_kernel_wait_snapshot_permission_failure_is_safe(kernel_proc, monkeypatch):
    ident, proc, modules, _, _ = kernel_proc
    original_open = os.open

    def denied_open(path, flags, *args, **kwargs):
        if path == "stack":
            raise PermissionError("private host detail")
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", denied_open)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert snapshot["processes"][0]["stack_error"] == "PermissionError"
    assert "private" not in json.dumps(snapshot)


def test_kernel_wait_snapshot_bounds_large_tables_and_redacts_wait_addresses(kernel_proc, monkeypatch):
    ident, proc, modules, target, _ = kernel_proc
    (target / "wchan").write_text("ffffffff81234567")

    def command(args, **kwargs):
        output = "100\n" if "inspect" in args else "201 100 D ffffffff81234567 iptables-restor\n" * 10000
        return subprocess.CompletedProcess(args, 0, output, "")

    monkeypatch.setattr(subprocess, "run", command)
    snapshot = _kernel_wait_snapshot(ident, proc_root=proc, module_root=modules)
    assert len(snapshot["top"]) <= 32 and len(snapshot["processes"]) <= 8
    assert len(json.dumps(snapshot)) < 16384
    assert "ffffffff" not in json.dumps(snapshot)
    assert snapshot["processes"][0]["wchan"] == "address_redacted"


def test_kernel_wait_snapshot_unsupported_proc_does_not_invoke_docker(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("must not invoke Docker"))
    assert _kernel_wait_snapshot("a" * 64, proc_root=tmp_path) == {"error": "host_proc_unavailable"}


@pytest.mark.parametrize("platform,guest,remote,elevated", [
    ("linux", True, False, True), ("linux", False, False, False),
    ("darwin", True, False, False), ("linux", True, True, False),
])
def test_kernel_observer_elevates_only_existing_local_linux_ci_guests(monkeypatch, platform, guest, remote, elevated):
    from types import SimpleNamespace

    from scripts import configure_ci_registry_mirror

    monkeypatch.setattr(sys, "platform", platform)
    original_exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda path: True if str(path) == "/proc/self/stat" else original_exists(path))
    monkeypatch.setattr(configure_ci_registry_mirror, "disposable_runner", lambda: guest)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("RUNNER_OS", "Linux")
    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    if remote:
        monkeypatch.setenv("DOCKER_HOST", "ssh://private-host")
    else:
        monkeypatch.delenv("DOCKER_HOST", raising=False)
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        assert kwargs["timeout"] == 5
        return subprocess.CompletedProcess(args, 0, "{}", "")

    monkeypatch.setattr(subprocess, "run", command)
    error = AssertionError("original")
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="a" * 64))
    _add_failure_diagnostics(container, "test-platform", error)
    observer = next(args for args in calls if "-c" in args)
    assert (observer[:2] == ["sudo", "-n"]) is elevated
    assert len(calls) == 12  # The original top slot is replaced, never added.
    assert "def _kernel_wait_snapshot" not in "\n".join(error.__notes__)
    assert "private-host" not in "\n".join(error.__notes__)


@pytest.mark.parametrize("failure", ["prepare", "execute"])
def test_kernel_observer_failure_never_masks_original_or_exposes_program(monkeypatch, failure):
    from types import SimpleNamespace

    def command(args, **kwargs):
        if sys.executable in args:
            raise RuntimeError("private-command-or-environment")
        return subprocess.CompletedProcess(args, 0, "fixed safe fields", "")

    if failure == "prepare":
        def unavailable_source(*args):
            raise OSError("private-source-path")
        monkeypatch.setattr(inspect, "getsource", unavailable_source)
    monkeypatch.setattr(subprocess, "run", command)
    container = SimpleNamespace(get_wrapped_container=lambda: SimpleNamespace(id="a" * 64))
    original = AssertionError("readiness original")
    with pytest.raises(AssertionError) as caught:
        try:
            raise original
        except AssertionError as error:
            _add_failure_diagnostics(container, "test-platform", error)
            raise
    assert caught.value is original
    detail = "\n".join(original.__notes__)
    assert "private-" not in detail and "def _kernel_wait_snapshot" not in detail
    assert "owned-node kernel" in detail


def _backend(ns, name, port, *, tls=False):
    spec = {"automountServiceAccountToken": False,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                                "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
            "volumes": [{"name": "code", "configMap": {"name": "backend-code"}}],
            "containers": [{"name": "server", "image": PYTHON, "command": ["python", "/code/server.py"],
                            "env": [{"name": "IDENTITY", "value": ns}, {"name": "PORT", "value": str(port)}],
                            "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                                          "limits": {"cpu": "500m", "memory": "128Mi"}},
                            "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                            "volumeMounts": [{"name": "code", "mountPath": "/code", "readOnly": True}],
                            "readinessProbe": {"tcpSocket": {"port": port}, "periodSeconds": 1}}]}
    if tls:
        spec["volumes"].append({"name": "tls", "secret": {"secretName": "legacy-tls"}})
        spec["containers"][0]["volumeMounts"].append({"name": "tls", "mountPath": "/tls", "readOnly": True})
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": ns,
                                                             "labels": {"app": name}}, "spec": spec}


@pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                    reason="requires an explicitly disposable Kubernetes API")
@pytest.mark.timeout(240)
def test_shared_tls_routes_streams_and_preserves_legacy(ingress_input, platform_inputs, tmp_path):
    config, candidate, profile = platform_inputs
    ns, old_host = config["namespace"], config["public_host"]
    foundation = FoundationBinding.model_validate(ingress_input["foundation"])
    shared_tls, old_tls = _certificate(["*.dev.example.com", "manage.example.com"]), _certificate([old_host])
    docs = [_namespace(ns)]
    for name, data in ((ingress_input["tls_secret_name"], shared_tls), ("legacy-tls", old_tls)):
        docs.append({"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls",
                     "metadata": {"name": name, "namespace": ns}, "stringData": data})
    docs += render_shared_ingress(SharedIngressInstallation.model_validate(ingress_input))
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    deployment["spec"]["template"]["spec"]["containers"][0]["image"] = TRAEFIK
    standalone = build_platform({**config, "shared_ingress_enabled": True}, candidate, profile, {}, repo_root=ROOT)
    # Local NodePort only, never a cloud LoadBalancer or real cluster context.
    public = deepcopy(standalone["70-public.yaml"][0])
    public["metadata"].pop("annotations")
    public["spec"].update(type="NodePort")
    public["spec"]["ports"][0]["nodePort"] = 30443
    docs.append(public)
    docs += [d for d in standalone["10-config-network.yaml"] if d["kind"] == "NetworkPolicy"
             and d["metadata"]["name"] in {"default-deny-ingress", "public-web"}]
    docs.append(_backend(ns, "loom-web", 8443, tls=True))
    for slug, identity in (("alice", None), ("bob", BOB)):
        row = registration_for(foundation, slug, **({} if identity is None else {"identity": identity}))
        result = render_environment(row, candidate, foundation, profile=profile, keyring={}, repo_root=ROOT)
        docs += [d for batch in result.files.values() for d in batch
                 if d["metadata"].get("namespace", d["metadata"]["name"]) == row.application_namespace
                 and (d["kind"] in {"Namespace", "Ingress", "NetworkPolicy"}
                      or (d["kind"] == "Service" and d["metadata"]["name"] in {"loom-web", "loom-service"}))]
        docs += [_backend(row.application_namespace, "loom-web", 8080),
                 _backend(row.application_namespace, "loom-service", 8090)]
    for namespace in (ns, "loom-dev-alice", "loom-dev-bob"):
        docs.append({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "backend-code", "namespace": namespace},
                     "data": {"server.py": SERVER}})
    # Import immutable images to disposable containerd; no registry credentials.
    archive = tmp_path / "images.tar"
    subprocess.run(["docker", "pull", TRAEFIK], check=True, capture_output=True, timeout=180)
    subprocess.run(["docker", "pull", PYTHON], check=True, capture_output=True, timeout=180)
    subprocess.run(["docker", "save", "-o", str(archive), TRAEFIK, PYTHON], check=True, capture_output=True, timeout=120)
    # The shared developer filesystem is large and >90% used despite tens of
    # GiB free. A disposable node needs an absolute test-local eviction floor.
    container = _start_k3s(node_name="loom-ingress-test", ephemeral_storage_floor="2Gi")
    try:
        _load_client(container)
        ident = container.get_wrapped_container().id
        subprocess.run(["docker", "cp", str(archive), ident + ":/tmp/images.tar"], check=True, capture_output=True)
        _run(container, "ctr", "images", "import", "/tmp/images.tar")
        _run(container, "kubectl", "wait", "--for=create", "node/loom-ingress-test", "--timeout=60s")
        _run(container, "kubectl", "wait", "--for=condition=Ready", "node/loom-ingress-test", "--timeout=60s")
        _run(container, "kubectl", "label", "nodes", "--all", "loom.nebius/node-role=system", "loom.nebius/platform=integration")
        prerequisites = [d for d in docs if d["kind"] == "Namespace"]
        prerequisites += [{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
            "name": "default", "namespace": namespace,
        }} for namespace in (ns, "loom-dev-alice", "loom-dev-bob")]
        _run(container, "kubectl", "apply", "-f", "-", payload=yaml.safe_dump_all(prerequisites))
        _run(container, "kubectl", "apply", "--validate=strict", "-f", "-", payload=yaml.safe_dump_all(docs))
        _run(container, "kubectl", "rollout", "status", "deployment/loom-shared-ingress", "-n", ns, "--timeout=150s", timeout=165)
        _run(container, "kubectl", "rollout", "status", "deployment/coredns", "-n", "kube-system", "--timeout=60s", timeout=75)
        for namespace in (ns, "loom-dev-alice", "loom-dev-bob"):
            _run(container, "kubectl", "wait", "pods", "--all", "-n", namespace, "--for=condition=Ready", "--timeout=60s", timeout=75)
        networks = json.loads(subprocess.check_output(["docker", "inspect", ident]))[0]["NetworkSettings"]["Networks"]
        addresses = [network["IPAddress"] for network in networks.values() if network.get("IPAddress")]
        assert len(addresses) == 1, "disposable test node must have one local Docker network"
        ip = addresses[0]
        context = ssl.create_default_context(cadata=shared_tls["tls.crt"] + old_tls["tls.crt"])

        def connect(host, *, alpn=None):
            context.set_alpn_protocols(alpn or ["http/1.1"])
            return context.wrap_socket(socket.create_connection((ip, 30443), timeout=5), server_hostname=host)

        def get(host, path="/"):
            with connect(host) as stream:
                stream.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
                response = http.client.HTTPResponse(stream)
                response.begin()
                return response.status, response.read()

        def wait_for_route(host, path, expected):
            # Pod readiness does not establish provider-watch or origin DNS/TLS
            # readiness. The legacy file/TCP and Kubernetes routes are independent.
            deadline = time.monotonic() + 30
            last = None
            while time.monotonic() < deadline:
                try:
                    last = get(host, path)
                except (ConnectionRefusedError, ssl.SSLError, TimeoutError) as exc:
                    last = type(exc).__name__ + ": " + str(exc)
                if last == expected:
                    return
                time.sleep(0.5)
            pytest.fail(f"route {host}{path} did not become ready: {last!r}")

        wait_for_route(old_host, "/", (200, (ns + ":8443").encode()))
        wait_for_route("alice.dev.example.com", "/api/v1/health", (200, b"loom-dev-alice:8090"))
        # Each host/backend pair converges independently in the provider watch.
        # Alice's API readiness does not establish Bob's routes or either web backend.
        wait_for_route("bob.dev.example.com", "/api", (200, b"loom-dev-bob:8090"))
        wait_for_route("alice.dev.example.com", "/apiary", (200, b"loom-dev-alice:8080"))
        wait_for_route("bob.dev.example.com", "/", (200, b"loom-dev-bob:8080"))
        assert get(old_host) == (200, (ns + ":8443").encode())
        with connect(old_host, alpn=["acme-tls/1"]) as stream:
            assert stream.selected_alpn_protocol() == "acme-tls/1"
        assert get("unknown.dev.example.com")[0] == 404
        with pytest.raises(ssl.SSLError):
            with connect("unrelated.example.net"):
                pass
        with connect("alice.dev.example.com") as stream:
            started = time.monotonic()
            stream.sendall(b"GET /api/stream HTTP/1.1\r\nHost: alice.dev.example.com\r\n\r\n")
            response = http.client.HTTPResponse(stream)
            response.begin()
            assert response.read(6) == b"first-"
            assert time.monotonic() - started < 1.5
            assert response.read() == b"second"
        with connect("alice.dev.example.com") as stream:
            # Backend must receive the first byte before the client finishes.
            stream.sendall(b"POST /api/upload HTTP/1.1\r\nHost: alice.dev.example.com\r\nContent-Length: 2\r\n\r\nx")
            deadline = time.monotonic() + 3
            observed = None
            while time.monotonic() < deadline:
                observed = get("alice.dev.example.com", "/api/upload-observed")
                if observed == (200, b"1"):
                    break
                time.sleep(0.1)
            assert observed == (200, b"1")
            stream.sendall(b"y")
            response = http.client.HTTPResponse(stream)
            response.begin()
            assert response.status == 200
            assert response.read() == b"ok"
        with connect("alice.dev.example.com") as stream:
            stream.sendall(b"GET /api/socket HTTP/1.1\r\nHost: alice.dev.example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
            data = b""
            while b"hello" not in data:
                chunk = stream.recv(4096)
                assert chunk, "WebSocket closed before sending its frame"
                data += chunk
            assert b"101 Switching Protocols" in data
            assert b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in data
        probe = '''import sys, urllib.error, urllib.request
try:
    print(urllib.request.urlopen(sys.argv[1], timeout=2).read().decode())
except urllib.error.URLError as exc:
    if isinstance(exc.reason, (TimeoutError, ConnectionRefusedError)):
        print("NETWORK_DENIED")
        sys.exit(42)
    raise
'''
        alice_ip = _run(container, "kubectl", "get", "pod", "loom-service", "-n", "loom-dev-alice",
                        "-o", "jsonpath={.status.podIP}").strip()
        for target in ("loom-service.loom-dev-alice", alice_ip):
            url = f"http://{target}:8090"
            # Prove both Service and direct Pod paths work from an allowed peer.
            assert _run(container, "kubectl", "exec", "-n", "loom-dev-alice", "loom-web", "--",
                        "python", "-c", probe, url).strip() == "loom-dev-alice:8090"
            deadline = time.monotonic() + 45
            while True:
                result = container.exec(["kubectl", "exec", "-n", "loom-dev-bob", "loom-service", "--",
                                         "python", "-c", probe, url])
                # Kube-router REJECTs with ICMP port-unreachable; other CNIs may
                # silently drop. DNS failures and generic command errors do not count.
                if result.exit_code == 42 and b"NETWORK_DENIED" in result.output:
                    break
                assert time.monotonic() < deadline, result.output.decode()
                time.sleep(0.5)
            # Denial is not evidence if the service itself stopped responding.
            assert _run(container, "kubectl", "exec", "-n", "loom-dev-alice", "loom-web", "--",
                        "python", "-c", probe, url).strip() == "loom-dev-alice:8090"
        assert get("alice.dev.example.com", "/api") == (200, b"loom-dev-alice:8090")
    except (Exception, pytest.fail.Exception) as exc:
        _add_failure_diagnostics(container, ns, exc)
        raise
    finally:
        container.stop()
