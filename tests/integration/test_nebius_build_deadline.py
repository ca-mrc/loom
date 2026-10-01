"""Actual non-root PID1 qualification for the native build deadline guard."""
from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import docker
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.fixture(scope="module")
def deadline_image():
    client = docker.from_env(timeout=180)
    try:
        # This is the production binary stage copied into the service image.
        image, _ = client.images.build(path=str(Path(__file__).resolve().parents[2]),
            dockerfile="deploy/Dockerfile.service", target="build-deadline", rm=True)
        yield client, image.id
    finally:
        client.close()


def run_guard(deadline_image, *, cutoff: datetime, script: str, install: bool = False):
    client, image = deadline_image
    command = ["--deadline-at", cutoff.isoformat()]
    if install:
        command.append("--install-runtime")
    command.extend(["--", "/bin/sh", "-c", script])
    container = client.containers.create(image, command, entrypoint="/out/loom-build-deadline",
        user="1000:1000", cap_drop=["ALL"], network_mode="none", read_only=True,
        security_opt=["no-new-privileges:true"], pids_limit=64, mem_limit="128m",
        tmpfs={"/loom/deadline-runtime": "rw,nosuid,nodev,size=8m,uid=1000,gid=1000,mode=0700"})
    try:
        container.start()
        # /wait can lose its reply after the process has already exited. The
        # retained exit state and timestamp are the evidence, not that response.
        observation_deadline = time.monotonic() + 75
        while True:
            container.reload()
            if not container.attrs["State"]["Running"]:
                break
            assert time.monotonic() < observation_deadline
            time.sleep(0.5)
        logs = container.logs().decode()
        assert container.attrs["State"]["Running"] is False
        assert not container.attrs["State"]["OOMKilled"]
        finished_at = datetime.fromisoformat(container.attrs["State"]["FinishedAt"].replace("Z", "+00:00"))
        return container.attrs["State"]["ExitCode"], logs, finished_at
    except Exception as error:
        container.reload()
        raise AssertionError({"cutoff": cutoff.isoformat(), "state": container.attrs["State"],
                              "logs": container.logs().decode()}) from error
    finally:
        container.remove(force=True)


def test_expired_container_cannot_start_build(deadline_image):
    code, logs, _ = run_guard(deadline_image, cutoff=datetime.now(UTC) - timedelta(seconds=1),
        script="echo unauthorized-build")
    assert code == 124
    assert "unauthorized-build" not in logs


def test_pid1_survives_hostile_stop_and_bounds_escaped_descendants(deadline_image):
    # Give Docker startup room on shared CI hosts, without changing the cutoff
    # after creation. The exit assertion still enforces only the fixed 10s grace.
    cutoff = datetime.now(UTC) + timedelta(seconds=30)
    code, logs, finished_at = run_guard(deadline_image, cutoff=cutoff, script="""
        set -eu
        test ! -r /proc/1/mem
        kill -STOP 1
        setsid sh -c 'trap "" TERM; echo escaped; while :; do sleep 1; done' &
        trap '' TERM
        echo ready
        while :; do sleep 1; done
    """)
    assert code == 124, logs
    assert "ready" in logs and "escaped" in logs
    # Inspect the actual container exit, not time spent fetching logs/removing it.
    assert finished_at < cutoff + timedelta(seconds=15), (cutoff, finished_at, logs)


def test_prepare_installs_identical_guard_outside_build_output(deadline_image):
    code, logs, _ = run_guard(deadline_image, cutoff=datetime.now(UTC) + timedelta(minutes=2),
        install=True, script="""
        set -eu
        cmp /out/loom-build-deadline /loom/deadline-runtime/loom-build-deadline
        test -x /loom/deadline-runtime/loom-build-deadline
        echo installed
        exit 17
    """)
    assert code == 17 and "installed" in logs, (code, logs)
