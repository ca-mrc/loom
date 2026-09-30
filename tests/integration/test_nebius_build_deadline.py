"""Actual non-root PID1 qualification for the native build deadline guard."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import docker
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.docker]


@pytest.fixture(scope="module")
def deadline_image():
    with docker.from_env(timeout=180) as client:
        # This is the production binary stage copied into the service image.
        image, _ = client.images.build(path=str(Path(__file__).resolve().parents[2]),
            dockerfile="deploy/Dockerfile.service", target="build-deadline", rm=True)
        yield client, image.id


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
        result = container.wait(timeout=30)
        logs = container.logs().decode()
        container.reload()
        assert container.attrs["State"]["Running"] is False
        assert not container.attrs["State"]["OOMKilled"]
        return result["StatusCode"], logs
    finally:
        container.remove(force=True)


def test_expired_container_cannot_start_build(deadline_image):
    code, logs = run_guard(deadline_image, cutoff=datetime.now(UTC) - timedelta(seconds=1),
        script="echo unauthorized-build")
    assert code == 124
    assert "unauthorized-build" not in logs


def test_pid1_survives_hostile_stop_and_bounds_escaped_descendants(deadline_image):
    cutoff = datetime.now(UTC) + timedelta(seconds=3)
    code, logs = run_guard(deadline_image, cutoff=cutoff, script="""
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
    assert datetime.now(UTC) < cutoff + timedelta(seconds=15)


def test_prepare_installs_identical_guard_outside_build_output(deadline_image):
    code, logs = run_guard(deadline_image, cutoff=datetime.now(UTC) + timedelta(seconds=10),
        install=True, script="""
        set -eu
        cmp /out/loom-build-deadline /loom/deadline-runtime/loom-build-deadline
        test -x /loom/deadline-runtime/loom-build-deadline
        echo installed
        exit 17
    """)
    assert code == 17 and "installed" in logs
