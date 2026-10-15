"""Keep every protected CI Docker consumer on the credential-free pull path."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/configure_ci_registry_mirror.py"
MIRROR = "https://mirror.gcr.io"


@pytest.fixture
def registry_setup() -> ModuleType:
    assert SCRIPT.is_file(), "CI must provide the Docker daemon mirror setup"
    spec = importlib.util.spec_from_file_location("ci_registry_mirror", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daemon_setup_preserves_settings_and_validates_before_install(
    registry_setup: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "daemon.json"
    original = {
        "log-driver": "local",
        "features": {"containerd-snapshotter": True},
        "registry-mirrors": ["https://existing.example"],
    }
    config.write_text(json.dumps(original))
    commands: list[list[str]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        assert kwargs["check"] is True
        if command[:3] == ["sudo", "dockerd", "--validate"]:
            assert json.loads(config.read_text()) == original
            candidate = json.loads(Path(command[-1]).read_text())
            assert candidate == {
                **original,
                "registry-mirrors": ["https://existing.example", MIRROR],
            }
        elif command[:2] == ["sudo", "install"]:
            shutil.copyfile(command[-2], command[-1])
        elif command[:3] == ["sudo", "systemctl", "restart"]:
            assert MIRROR in json.loads(config.read_text())["registry-mirrors"]
        elif command[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(command, 0, json.dumps([MIRROR + "/"]))
        else:
            pytest.fail(f"unexpected host command: {command}")
        return subprocess.CompletedProcess(command, 0, "")

    monkeypatch.setattr(registry_setup.subprocess, "run", run)
    registry_setup.configure_daemon(config)
    assert [command[:2] for command in commands] == [
        ["sudo", "dockerd"],
        ["sudo", "install"],
        ["sudo", "systemctl"],
        ["docker", "info"],
    ]


@pytest.mark.parametrize(
    "existing", [{}, {"registry-mirrors": [MIRROR]}, {"registry-mirrors": [MIRROR + "/"]}]
)
def test_daemon_mirror_is_added_once(registry_setup: ModuleType, existing: dict) -> None:
    rendered = registry_setup.with_mirror(existing)
    assert [mirror.rstrip("/") for mirror in rendered["registry-mirrors"]] == [MIRROR]
    assert registry_setup.with_mirror(rendered) == rendered


@pytest.mark.parametrize(
    "existing", [[], None, {"registry-mirrors": "https://wrong"}, {"registry-mirrors": [None]}]
)
def test_malformed_daemon_settings_fail_closed(
    registry_setup: ModuleType, existing: object
) -> None:
    with pytest.raises(ValueError):
        registry_setup.with_mirror(existing)


def test_validation_failure_does_not_replace_daemon_config(
    registry_setup: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = tmp_path / "daemon.json"
    original = '{"unknown-dockerd-setting": true}'
    config.write_text(original)

    def reject(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command[:3] == ["sudo", "dockerd", "--validate"]
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(registry_setup.subprocess, "run", reject)
    with pytest.raises(subprocess.CalledProcessError):
        registry_setup.configure_daemon(config)
    assert config.read_text() == original


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "self-hosted", "RUNNER_OS": "Linux"},
        {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "RUNNER_OS": "macOS"},
        {
            "GITHUB_ACTIONS": "true",
            "RUNNER_ENVIRONMENT": "github-hosted",
            "RUNNER_OS": "Linux",
            "DOCKER_HOST": "tcp://remote.example:2375",
        },
    ],
)
def test_setup_refuses_non_hosted_or_remote_daemons(
    environment: dict[str, str], tmp_path: Path
) -> None:
    assert SCRIPT.is_file(), "CI must provide the Docker daemon mirror setup"
    # Model an unmarked shared host even when this test itself runs inside a
    # marked disposable worker. Never inspect or modify the execution host.
    entry = (
        "import runpy, sys\n"
        "from pathlib import Path\n"
        "main = runpy.run_path(sys.argv[1])['main']\n"
        "main.__globals__['EPHEMERAL_MARKER'] = Path(sys.argv[2])\n"
        "raise SystemExit(main())\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", entry, str(SCRIPT), str(tmp_path / "absent-marker")],
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], **environment},
    )
    assert result.returncode != 0
    assert "GitHub-hosted Linux runner with its local Docker daemon" in result.stderr


@pytest.mark.parametrize(
    ("workflow", "job"),
    [
        ("ci", "tests-root"),
        ("ci", "runtime-payload"),
        ("ci", "integration"),
        ("ci", "integration-docker"),
        ("cluster-smoke", "cluster-contract"),
        ("staging-smoke", "system-smoke"),
        ("images", "build"),
        ("images", "nebius-harness-build"),
    ],
)
def test_each_docker_consumer_configures_mirror_immediately_after_checkout(
    workflow: str,
    job: str,
) -> None:
    jobs = yaml.safe_load((ROOT / f".github/workflows/{workflow}.yml").read_text())["jobs"]
    steps = jobs[job]["steps"]
    assert steps[0]["uses"].startswith("actions/checkout@")
    assert steps[1].get("run") == "python3 scripts/configure_ci_registry_mirror.py"
    assert not steps[1].get("continue-on-error")
    assert not steps[1].get("if")


@pytest.mark.parametrize("host_config_exists", [False, True])
def test_buildkit_uses_tracked_mirror_unless_host_config_is_present(
    tmp_path: Path,
    host_config_exists: bool,
) -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())["jobs"]
    step = next(
        step
        for step in jobs["build"]["steps"]
        if step.get("name") == "Create native buildx builder"
    )
    host_config = tmp_path / "host.toml"
    if host_config_exists:
        host_config.write_text('[registry."docker.io"]\nmirrors = ["host.example"]\n')
    log = tmp_path / "commands"
    docker = tmp_path / "docker"
    docker.write_text(
        "#!/usr/bin/env python3\nimport json, os, sys\n"
        'with open(os.environ["COMMAND_LOG"], "a") as output:\n'
        '    output.write(json.dumps(sys.argv[1:]) + "\\n")\n'
    )
    docker.chmod(0o755)
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"].replace("/etc/buildkit/loom-ci.toml", str(host_config))],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "COMMAND_LOG": str(log)},
    )
    assert result.returncode == 0, result.stderr
    commands = [json.loads(line) for line in log.read_text().splitlines()]
    create = commands[0]
    assert "--buildkitd-config" in create, "container builders do not inherit daemon mirrors"
    config_path = Path(create[create.index("--buildkitd-config") + 1])
    parsed = tomllib.loads((ROOT / config_path).read_text())
    assert parsed["registry"]["docker.io"]["mirrors"] == (
        ["host.example"] if host_config_exists else ["mirror.gcr.io"]
    )
    assert commands[-1] == ["buildx", "inspect"]


@pytest.mark.parametrize("failure", ["restart", "readback"])
def test_daemon_activation_failure_is_not_ignored(
    registry_setup: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    config = tmp_path / "daemon.json"

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert kwargs["check"] is True
        if command[:3] == ["sudo", "systemctl", "restart"] and failure == "restart":
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, "[]")

    monkeypatch.setattr(registry_setup.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError if failure == "restart" else ValueError):
        registry_setup.configure_daemon(config)


def test_persisted_remote_docker_context_is_rejected_before_sudo(tmp_path: Path) -> None:
    (tmp_path / "config.json").write_text('{"currentContext": "remote"}')
    docker = tmp_path / "docker"
    docker.write_text(
        "#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
        'assert sys.argv[1:] == ["context", "inspect", "--format", '
        '"{{json .Endpoints.docker.Host}}"]\n'
        'config = json.loads((pathlib.Path(os.environ["DOCKER_CONFIG"]) / "config.json").read_text())\n'
        'assert config["currentContext"] == "remote"\n'
        'print(json.dumps("tcp://remote.example:2376"))\n'
    )
    docker.chmod(0o755)
    marker = tmp_path / "sudo-called"
    sudo = tmp_path / "sudo"
    sudo.write_text(f'#!/bin/sh\ntouch "{marker}"\nexit 99\n')
    sudo.chmod(0o755)
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        capture_output=True,
        text=True,
        env={
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "DOCKER_CONFIG": str(tmp_path),
            "GITHUB_ACTIONS": "true",
            "RUNNER_ENVIRONMENT": "github-hosted",
            "RUNNER_OS": "Linux",
        },
    )
    assert result.returncode != 0
    assert not marker.exists(), "remote context must be rejected before host mutation"
    assert "local Docker daemon" in result.stderr


def test_default_local_docker_endpoint_is_accepted(
    registry_setup: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command == [
            "docker",
            "context",
            "inspect",
            "--format",
            "{{json .Endpoints.docker.Host}}",
        ]
        assert kwargs["check"] is True
        return subprocess.CompletedProcess(command, 0, '"unix:///var/run/docker.sock"\n')

    monkeypatch.setattr(registry_setup.subprocess, "run", run)
    registry_setup.require_local_context()


def test_marked_ephemeral_guest_uses_local_daemon(registry_setup, tmp_path, monkeypatch):
    import types

    marker = tmp_path / "marker"
    marker.write_text('{"schema_version":1,"provider":"oldlab"}')

    # Root ownership is a host property, independent of the test process uid.
    class Marker:
        def lstat(self):
            return types.SimpleNamespace(st_uid=0, st_mode=0o100444, st_size=marker.stat().st_size)

        def is_symlink(self):
            return False

        def read_text(self):
            return marker.read_text()

    monkeypatch.setattr(registry_setup, "EPHEMERAL_MARKER", Marker())
    monkeypatch.setenv("RUNNER_ENVIRONMENT", "self-hosted")
    assert registry_setup.disposable_runner()
    marker.write_text('{"schema_version":1,"provider":"shared-host"}')
    assert not registry_setup.disposable_runner()


def test_user_writable_marker_cannot_authorize_daemon_change(registry_setup, monkeypatch):
    import types

    class Marker:
        def lstat(self):
            return types.SimpleNamespace(st_uid=0, st_mode=0o100666, st_size=48)

        def is_symlink(self):
            return False

        def read_text(self):
            pytest.fail("unsafe marker must not be read")

    monkeypatch.setattr(registry_setup, "EPHEMERAL_MARKER", Marker())
    monkeypatch.setenv("RUNNER_ENVIRONMENT", "self-hosted")
    assert not registry_setup.disposable_runner()
