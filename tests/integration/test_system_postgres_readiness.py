"""Cold-init readiness must describe the TCP endpoint used by system migrations."""

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml
from alembic.config import Config
from alembic.script import ScriptDirectory

from tests.system import docker_compose

pytestmark = [pytest.mark.integration, pytest.mark.docker]
POSTGRES_IMAGE = "postgres:16"


def _require_local_docker_endpoint():
    context = os.environ.get("DOCKER_CONTEXT")
    endpoint = (os.environ.get("DOCKER_HOST") or None) if not context else None
    if endpoint is None:
        # Context inspection reads local configuration; it never contacts the
        # daemon. An explicit context overrides DOCKER_HOST, as Docker does.
        command = ["docker", "context", "inspect"]
        if context:
            command.append(context)
        command.extend(["--format", '{{(index .Endpoints "docker").Host}}'])
        try:
            result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.SubprocessError):
            raise AssertionError("local Docker context inspection unavailable") from None
        if result.returncode != 0:
            raise AssertionError("local Docker context inspection unavailable")
        endpoint = result.stdout.strip()
    if not endpoint.startswith("unix:///"):
        raise AssertionError("test requires a local Unix Docker endpoint")


@pytest.mark.parametrize("context,host,configured,allowed", [
    ("explicit", "unix:///local.sock", "ssh://private-endpoint", False),
    ("explicit", "ssh://private-endpoint", "unix:///local.sock", True),
    (None, "ssh://private-endpoint", None, False),
    (None, "unix:///local.sock", None, True),
    (None, None, "ssh://private-endpoint", False),
    (None, None, "unix:///local.sock", True),
])
def test_local_endpoint_guard_respects_docker_precedence(monkeypatch, context, host, configured, allowed):
    for name, value in (("DOCKER_CONTEXT", context), ("DOCKER_HOST", host)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    calls = []

    def inspect(command, **kwargs):
        calls.append(command)
        assert command[:3] == ["docker", "context", "inspect"]
        assert command[3:-2] == ([context] if context else [])
        return subprocess.CompletedProcess(command, 0, stdout=configured + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", inspect)
    if allowed:
        _require_local_docker_endpoint()
    else:
        with pytest.raises(AssertionError, match="local Unix Docker endpoint") as error:
            _require_local_docker_endpoint()
        assert "private-endpoint" not in str(error.value)
    assert len(calls) == (1 if context or not host else 0)


@pytest.fixture
def init_barrier():
    # Give the synthetic script an executable shebang: Docker Desktop's bind
    # access check can report -x even for 0644, which then fails execve.
    with tempfile.NamedTemporaryFile(mode="w", prefix="loom-pg-readiness-", suffix=".sh",
                                     dir="/tmp", delete=False) as script:
        script.write("#!/bin/sh\nmkfifo /tmp/loom-readiness-release\n"
                     "touch /tmp/loom-readiness-entered\n"
                     "read -r release </tmp/loom-readiness-release\n")
        Path(script.name).chmod(0o755)
    # Close the writable descriptor before Linux execve, avoiding ETXTBSY.
    try:
        yield Path(script.name)
    finally:
        Path(script.name).unlink(missing_ok=True)


def test_system_postgres_health_waits_for_final_tcp_server(tmp_path, monkeypatch, init_barrier):
    _require_local_docker_endpoint()
    project = "loom-pg-readiness-" + uuid4().hex[:12]
    service = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text())["services"]["postgres"]
    assert service["image"] == POSTGRES_IMAGE
    # Keep the real service image, port mapping, health command and intervals.
    # Hold the official entrypoint while its socket-only init server is live.
    service.update(cpus=1, mem_limit="512m",
                   volumes=[f"{init_barrier}:/docker-entrypoint-initdb.d/readiness.sh:ro"])
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(yaml.safe_dump({"services": {"postgres": service}}))
    monkeypatch.setattr(docker_compose, "COMPOSE_FILE", compose_file)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    monkeypatch.setenv("LOOM_SYSTEM_POSTGRES_PORT", "0")

    def docker(*args, check=True):
        return subprocess.run(["docker", *args], check=check, capture_output=True,
                              text=True, timeout=5)

    state_template = (
        '{"running":{{json .State.Running}},"health":{{json .State.Health.Status}},"checks":['
        '{{range $i,$item := .State.Health.Log}}{{if $i}},{{end}}'
        '{"exit_code":{{json $item.ExitCode}},"end":{{json $item.End}}}{{end}}]}'
    )
    owned_volumes = []
    try:
        docker_compose._compose("up", "-d", "postgres", timeout_sec=30)
        container = docker_compose._compose("ps", "-q", "postgres", timeout_sec=5).stdout.strip()
        owned_volumes = docker("inspect", "--format", '{{range .Mounts}}{{if eq .Type "volume"}}'
                               '{{println .Name}}{{end}}{{end}}', container).stdout.split()
        deadline = time.monotonic() + 20
        while True:
            entered = docker("exec", container, "test", "-e", "/tmp/loom-readiness-entered",
                             check=False).returncode == 0
            state = json.loads(docker("inspect", "--format", state_template, container).stdout)
            assert state["running"], "initialization container exited before the barrier"
            if entered:
                break
            assert time.monotonic() < deadline, "initialization barrier not reached"
            time.sleep(.05)
        # The first new completion can be an in-flight pre-barrier probe. Docker
        # runs probes serially, so the second new completion must be post-barrier.
        previous_cycle = state["checks"][-1]["end"] if state["checks"] else None
        completed_cycles = []
        while len(completed_cycles) < 2:
            state = json.loads(docker("inspect", "--format", state_template, container).stdout)
            assert state["running"], "initialization container exited at the barrier"
            if state["checks"] and state["checks"][-1]["end"] != previous_cycle:
                previous_cycle = state["checks"][-1]["end"]
                completed_cycles.append(previous_cycle)
            assert time.monotonic() < deadline, "two health cycles did not complete at the init barrier"
            time.sleep(.05)
        old_probe = docker("exec", container, "pg_isready", "-U", "loom", check=False)
        tcp_probe = docker("exec", container, "pg_isready", "-h", "127.0.0.1",
                           "-U", "loom", "-d", "loom", check=False)
        db_url = docker_compose.database_url()
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(db_url.replace("postgresql+psycopg://", "postgresql://"),
                            connect_timeout=2)
        entrypoint = docker("exec", container, "cat", "/usr/local/bin/docker-entrypoint.sh").stdout
        assert "-c listen_addresses=''" in entrypoint
        record = {
            "old_unix_probe_exit": old_probe.returncode,
            "tcp_probe_exit": tcp_probe.returncode,
            "host_tcp_connection_failed": True,
            "docker_health_during_init": state,
            "health_cycles_after_barrier": completed_cycles,
            "postgres_version": docker("exec", container, "postgres", "--version").stdout.strip(),
            "image": POSTGRES_IMAGE,
            "architecture": docker("image", "inspect", POSTGRES_IMAGE,
                                   "--format", "{{.Architecture}}").stdout.strip(),
            "socket_only_temp_server_in_actual_entrypoint": True,
        }
        print("postgres-readiness " + json.dumps(record, sort_keys=True))
        assert old_probe.returncode == 0 and tcp_probe.returncode == 2
        assert state["health"] != "healthy", "socket-only initialization was reported ready for TCP"

        docker("exec", container, "sh", "-c", "printf 'release\\n' >/tmp/loom-readiness-release")
        # Exercise the existing readiness gate and exactly one real migration.
        docker_compose._wait_services_healthy(["postgres"], timeout_sec=300)
        subprocess.run([sys.executable, "-m", "alembic", "-c", str(docker_compose.ALEMBIC_CONFIG),
                        "upgrade", "head"], check=True, cwd=docker_compose.REPO_ROOT,
                       env={**os.environ, "LOOM_DB_URL": db_url}, capture_output=True, text=True)
        with psycopg.connect(db_url.replace("postgresql+psycopg://", "postgresql://")) as connection:
            actual_heads = {row[0] for row in connection.execute("SELECT version_num FROM alembic_version")}
        expected_heads = set(ScriptDirectory.from_config(Config(str(docker_compose.ALEMBIC_CONFIG))).get_heads())
        assert actual_heads == expected_heads
        print("postgres-migration " + json.dumps({"attempts": 1, "heads": sorted(actual_heads)}))
    finally:
        docker_compose._compose("down", "--volumes", "--remove-orphans", timeout_sec=60)
        assert not docker("ps", "-aq", "--filter", "label=com.docker.compose.project=" + project).stdout.strip()
        assert not docker("network", "ls", "-q", "--filter", "label=com.docker.compose.project=" + project).stdout.strip()
        assert not docker("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=" + project).stdout.strip()
        volumes_removed = all(docker("volume", "inspect", name, check=False).returncode != 0
                              for name in owned_volumes)
        assert volumes_removed
        print("postgres-cleanup " + json.dumps({"owned_volume_ids": owned_volumes,
              "owned_volumes_removed": volumes_removed, "containers_removed": True,
              "networks_removed": True}, sort_keys=True))
