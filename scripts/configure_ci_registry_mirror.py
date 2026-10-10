#!/usr/bin/env python3
"""Add the public Docker Hub mirror on disposable GitHub-hosted Linux runners."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

MIRROR = "https://mirror.gcr.io"


def with_mirror(config: object) -> dict[str, Any]:
    """Preserve host settings and mirror priority; append our credential-free default."""
    if not isinstance(config, dict):
        raise ValueError("Docker daemon configuration must be a JSON object")
    mirrors = config.get("registry-mirrors", [])
    if not isinstance(mirrors, list) or not all(isinstance(item, str) for item in mirrors):
        raise ValueError("Docker registry-mirrors must be a list of URLs")
    if MIRROR not in [item.rstrip("/") for item in mirrors]:
        mirrors = [*mirrors, MIRROR]
    return {**config, "registry-mirrors": mirrors}


def configure_daemon(config_path: Path) -> None:
    """Validate before replacing the runner config, then verify Docker accepted it."""
    current = json.loads(config_path.read_text()) if config_path.exists() else {}
    config = with_mirror(current)
    with tempfile.TemporaryDirectory(prefix="loom-ci-docker-") as directory:
        candidate = Path(directory) / "daemon.json"
        candidate.write_text(json.dumps(config, indent=2) + "\n")
        subprocess.run(
            ["sudo", "dockerd", "--validate", "--config-file", str(candidate)], check=True
        )
        subprocess.run(
            ["sudo", "install", "-D", "-m", "0644", str(candidate), str(config_path)], check=True
        )
    subprocess.run(["sudo", "systemctl", "restart", "docker"], check=True)
    result = subprocess.run(
        ["docker", "info", "--format", "{{json .RegistryConfig.Mirrors}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    mirrors = json.loads(result.stdout)
    if not isinstance(mirrors, list) or MIRROR not in [str(item).rstrip("/") for item in mirrors]:
        raise ValueError("Docker did not activate the CI registry mirror")
    print(f"Docker Hub pulls use {MIRROR}; image references and digests are unchanged")


def require_local_context() -> None:
    """A persisted context can select a remote daemon without environment overrides."""
    result = subprocess.run(
        ["docker", "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    if json.loads(result.stdout) != "unix:///var/run/docker.sock":
        raise ValueError("Requires the runner's local Docker daemon at /var/run/docker.sock")


def main() -> int:
    if (
        os.environ.get("GITHUB_ACTIONS") != "true"
        or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
        or os.environ.get("RUNNER_OS") != "Linux"
        or os.environ.get("DOCKER_HOST")
        or os.environ.get("DOCKER_CONTEXT")
    ):
        print("Requires a GitHub-hosted Linux runner with its local Docker daemon", file=sys.stderr)
        return 1
    try:
        require_local_context()
        configure_daemon(Path("/etc/docker/daemon.json"))
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"CI registry mirror setup failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
