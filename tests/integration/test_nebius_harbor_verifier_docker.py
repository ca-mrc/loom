"""Run the generated Harbor wrapper with real shell exit/reward semantics."""

from __future__ import annotations

import io
import json
import subprocess
import tarfile
from pathlib import Path
from uuid import uuid4

import pytest

from loom.models.verifier import VerifierResult
from loom.nebius_terminus_image import prepare_nebius_terminus_image
from loom.nebius_terminus_ingest import offline_verifier_run_sh_bytes

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]


@pytest.fixture
def run_wrapper(tmp_path: Path):
    import docker

    client = docker.from_env()
    tmp_path.chmod(0o755)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/proof").write_text("private test input\n")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/run.sh").write_bytes(offline_verifier_run_sh_bytes())

    def run(script: str) -> tuple[int, dict | None]:
        (tmp_path / "verifier/harbor-offline.sh").write_text(script)
        container = client.containers.run(
            "python:3.11-slim", ["/source/verifier/run.sh"], entrypoint="/bin/sh",
            detach=True, network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            environment={"LOOM_TASK_DIR": "/source", "LOOM_VERIFIER_OUTPUT": "/result.json"},
            volumes={str(tmp_path): {"bind": "/source", "mode": "ro"}},
        )
        try:
            status = container.wait(timeout=30)["StatusCode"]
            print(container.logs().decode(errors="replace"))
            try:
                chunks, _ = container.get_archive("/result.json")
            except docker.errors.NotFound:
                return status, None
            with tarfile.open(fileobj=io.BytesIO(b"".join(chunks))) as archive:
                member = archive.extractfile("result.json")
                assert member is not None
                result = json.load(member)
            VerifierResult.model_validate(result)
            return status, result
        finally:
            container.remove(force=True)

    try:
        yield run
    finally:
        client.close()


@pytest.mark.parametrize("exit_code", [1, 2, 137])
def test_failed_harbor_verifier_retains_reward_and_original_failure(run_wrapper, exit_code):
    # Harbor's EXIT trap records zero even when set -e stops at pytest or setup.
    status, result = run_wrapper(
        "set -eu\n"
        "trap 'echo 0 > /logs/verifier/reward.txt' EXIT\n"
        "test -f /tests/proof\n"
        f"exit {exit_code}\n"
    )
    assert status == exit_code
    assert result is not None
    assert result["rewards"] == {"resolved": 0, "passed": 0}
    assert result["structured"] == {"exit_code": exit_code, "reward": 0}
    assert result["checks"][0]["passed"] is False


@pytest.mark.parametrize("reward", [0, 1])
def test_successful_harbor_verifier_preserves_zero_and_positive_rewards(run_wrapper, reward):
    status, result = run_wrapper(f"echo {reward} > /logs/verifier/reward.txt\n")
    assert status == 0
    assert result is not None
    assert result["rewards"] == {"resolved": reward, "passed": reward}


@pytest.mark.parametrize("script", ["exit 0\n", "echo invalid > /logs/verifier/reward.txt\n"])
def test_missing_or_invalid_harbor_reward_still_fails_without_result(run_wrapper, script):
    status, result = run_wrapper(script)
    assert status != 0
    assert result is None


@pytest.mark.timeout(600)
def test_prepared_image_preserves_task_tools_and_records_resolved_dependencies(tmp_path: Path):
    import docker

    shell_setup = (
        "printf '#!/bin/sh\\necho called >> /authored-shell.log\\nexec /bin/sh \"$@\"\\n' "
        "> /task-shell && chmod +x /task-shell && "
        "ln -s /usr/local/bin/python /usr/bin/python"
    )
    original = (
        "FROM python:3.11-slim\n"
        "RUN " + json.dumps(["/bin/sh", "-c", shell_setup]) + "\n"
        'SHELL ["/task-shell", "-c"]\n'
        "RUN touch /authored-run\nWORKDIR /app\n"
    )
    (tmp_path / "Dockerfile").write_text(original)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(
        "pip install pytest==8.4.1 packaging\npytest /tests/test_example.py\n"
    )
    environment = {"dockerfile": "Dockerfile", "docker_build_context": ".", "workdir": "/app"}
    prepare_nebius_terminus_image(tmp_path, environment)
    tag = "loom-preparation-shell-test:" + uuid4().hex
    client = docker.from_env()
    container = None
    try:
        result = subprocess.run(
            ["docker", "build", "--tag", tag, "--file", str(tmp_path / environment["dockerfile"]), str(tmp_path)],
            capture_output=True, text=True, timeout=480,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        built = client.images.get(tag)
        assert built.attrs["Config"]["Shell"] == ["/task-shell", "-c"]
        container = client.containers.run(
            tag, entrypoint="/bin/sh", command=["-exc", (
                'test "$(id -u)" = 65532; test -f /authored-run; '
                'test "$(cat /authored-shell.log)" = called; '
                'test "$(readlink /usr/bin/python)" = /usr/local/bin/python; '
                'test "$(python -c \'import sys; print(sys.version_info[:2])\')" = "(3, 11)"; '
                '/opt/verifier/bin/python -m pytest --version; '
                '/opt/verifier/bin/python -c \'import importlib.metadata as m; '
                'from pathlib import Path; '
                'resolved = Path("/opt/verifier/resolved-requirements.txt").read_text().splitlines(); '
                'assert all(name + "==" + m.version(name) in resolved '
                'for name in ("pytest", "packaging"))\''
            )],
            network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"], detach=True,
        )
        status = container.wait(timeout=30)["StatusCode"]
        assert status == 0, container.logs().decode(errors="replace")
        assert b"pytest 8.4.1" in container.logs()
        assert (tmp_path / "Dockerfile").read_text() == original
    finally:
        if container is not None:
            container.remove(force=True)
        try:
            client.images.remove(tag, force=True)
        except docker.errors.ImageNotFound:
            pass
        client.close()


@pytest.mark.timeout(600)
def test_unpinned_uvx_resolves_verifier_python_without_replacing_old_task_tools(tmp_path: Path):
    import docker

    original = (
        "FROM php:7.1-cli\n"
        "RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g; "
        "s|security.debian.org/debian-security|archive.debian.org/debian-security|g; "
        "/buster-updates/d' /etc/apt/sources.list\nWORKDIR /app\n"
    )
    (tmp_path / "Dockerfile").write_text(original)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(
        "uvx --with pytest==8.4.1 --with requests pytest /tests/test_example.py\n"
    )
    environment = {"dockerfile": "Dockerfile", "docker_build_context": ".", "workdir": "/app"}
    prepare_nebius_terminus_image(tmp_path, environment)
    tag = "loom-preparation-php-test:" + uuid4().hex
    client = docker.from_env()
    container = None
    try:
        result = subprocess.run(
            ["docker", "build", "--tag", tag, "--file", str(tmp_path / environment["dockerfile"]), str(tmp_path)],
            capture_output=True, text=True, timeout=480,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        container = client.containers.run(
            tag, entrypoint="/bin/sh", command=["-exc", (
                'php -r "exit(PHP_MAJOR_VERSION === 7 && PHP_MINOR_VERSION === 1 ? 0 : 1);"; '
                'python3 -c "import sys; assert sys.version_info[:2] == (3, 7)"; '
                '/opt/verifier/bin/python -c "import sys, pytest, requests; '
                'assert sys.version_info >= (3, 9); assert pytest.__version__ == \'8.4.1\'"; '
                '/opt/verifier/bin/pytest --version'
            )],
            network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"], detach=True,
        )
        assert container.wait(timeout=30)["StatusCode"] == 0, container.logs().decode(errors="replace")
        assert (tmp_path / "Dockerfile").read_text() == original
    finally:
        if container is not None:
            container.remove(force=True)
        try:
            client.images.remove(tag, force=True)
        except docker.errors.ImageNotFound:
            pass
        client.close()


# archlinux:base-20261101.0.620205 (Docker Hub), and the archive day it was built from.
_ARCH_BASE_20261101_DIGEST = "sha256:b1c2fe6e717bfd5673af8fc217e1e1c3ce0dcbb1d53c3414c8425e7ce79fc6d9"
_ARCH_ARCHIVE_20261101 = "https://archive.archlinux.org/repos/2026/11/01"


@pytest.mark.timeout(600)
def test_arch_preparation_retains_authored_packages_and_offline_cache(tmp_path: Path):
    import docker

    # Arch is rolling: `archlinux:latest` plus live repositories breaks whenever
    # the repositories move past the image (November 2026: `python` started
    # depending on `openssl-3`, whose files the image's `openssl` owns). Pin the
    # image and the matching Arch Linux Archive snapshot, the "reviewed
    # compatible image/repository snapshot" preparation itself asks for.
    original = (
        f"FROM archlinux:base@{_ARCH_BASE_20261101_DIGEST}\n"
        f"RUN echo 'Server = {_ARCH_ARCHIVE_20261101}/$repo/os/$arch' > /etc/pacman.d/mirrorlist\n"
        "RUN pacman -Q > /authored-packages && mkdir -p /var/cache/pacman/pkg && "
        "printf task-input > /var/cache/pacman/pkg/loom-authored-cache\nWORKDIR /app\n"
    )
    (tmp_path / "Dockerfile").write_text(original)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(
        "uvx --with pytest==8.4.1 --with pytest-json-ctrf==0.3.5 pytest /tests/test_example.py\n"
    )
    (tmp_path / "tests/test_example.py").write_text("# Private verifier input must not enter the task image.\n")
    environment = {"dockerfile": "Dockerfile", "docker_build_context": ".", "workdir": "/app"}
    prepare_nebius_terminus_image(tmp_path, environment)
    tag = "loom-preparation-arch-test:" + uuid4().hex
    client = docker.from_env()
    container = None
    try:
        result = subprocess.run(
            ["docker", "build", "--tag", tag, "--file", str(tmp_path / environment["dockerfile"]), str(tmp_path)],
            capture_output=True, text=True, timeout=480,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        container = client.containers.run(
            tag, entrypoint="/bin/sh", command=["-ec", (
                'test "$(id -u)" = 65532; test "$HOME" = /home/agent; '
                'test "$(cat /var/cache/pacman/pkg/loom-authored-cache)" = task-input; '
                'while read -r package version; do test "$(pacman -Q "$package")" = "$package $version"; done < /authored-packages; '
                'test ! -e /tests/test_example.py; '
                'tmux -V; asciinema --version; tar --version; '
                '/opt/verifier/bin/python -m pytest --version; '
                'printf "%s\\n" "def test_interpreter():" "    import sys; assert sys.version_info.major == 3" '
                '> /tests/test_example.py; '
                '/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_example.py; '
                '/opt/verifier/bin/python -c \'import json; '
                'assert json.load(open("/logs/verifier/ctrf.json"))["results"]["summary"]["passed"] == 1\'; '
                'test -s /opt/verifier/resolved-requirements.txt'
            )], network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"], detach=True,
        )
        assert container.wait(timeout=30)["StatusCode"] == 0, container.logs().decode(errors="replace")
        assert (tmp_path / "Dockerfile").read_text() == original
    finally:
        if container is not None:
            container.remove(force=True)
        try:
            client.images.remove(tag, force=True)
        except docker.errors.ImageNotFound:
            pass
        client.close()


@pytest.mark.timeout(600)
def test_alpine_preparation_retains_authored_packages_and_wget_task(tmp_path: Path):
    import docker

    original = (
        "FROM alpine:3.20\n"
        "RUN cp /lib/apk/db/installed /authored-packages && mkdir -p /var/cache/apk && "
        "printf task-input > /var/cache/apk/loom-authored-cache\nWORKDIR /app\n"
    )
    (tmp_path / "Dockerfile").write_text(original)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(
        "apk add --no-cache curl\nuvx --python 3.13 --with pytest==8.4.1 --with pytest-json-ctrf==0.3.5 pytest /tests/test_example.py\n"
    )
    (tmp_path / "tests/test_example.py").write_text("# Private verifier input must not enter the task image.\n")
    environment = {"dockerfile": "Dockerfile", "docker_build_context": ".", "workdir": "/app"}
    prepare_nebius_terminus_image(tmp_path, environment)
    tag = "loom-preparation-alpine-test:" + uuid4().hex
    client = docker.from_env()
    container = None
    try:
        result = subprocess.run(
            ["docker", "build", "--tag", tag, "--file", str(tmp_path / environment["dockerfile"]), str(tmp_path)],
            capture_output=True, text=True, timeout=480,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        container = client.containers.run(
            tag, entrypoint="/bin/sh", command=["-ec", (
                'test "$(id -u)" = 65532; test "$HOME" = /home/agent; '
                'test "$(cat /var/cache/apk/loom-authored-cache)" = task-input; '
                'test "$(cat /etc/alpine-release | cut -d. -f1,2)" = 3.20; '
                'test ! -e /tests/test_example.py; test ! -e /app/get-docker.sh; apk info -e musl; ! apk info -e wget; '
                'tmux -V; asciinema --version; tar --version; '
                '/opt/verifier/bin/python -m pytest --version; '
                'printf "%s\\n" "def test_interpreter():" "    import sys; assert sys.version_info[:2] == (3, 13)" '
                '> /tests/test_example.py; '
                '/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_example.py; '
                '/opt/verifier/bin/python -c \'import json; '
                'assert json.load(open("/logs/verifier/ctrf.json"))["results"]["summary"]["passed"] == 1\'; '
                '/opt/verifier/bin/python -c \'from pathlib import Path; '
                'parse=lambda path: {dict(line.split(":", 1) for line in record.splitlines() if ":" in line)["P"]: '
                'dict(line.split(":", 1) for line in record.splitlines() if ":" in line)["V"] '
                'for record in Path(path).read_text().strip().split("\\n\\n")}; '
                'before=parse("/authored-packages"); after=parse("/lib/apk/db/installed"); '
                'assert all(after.get(name)==version for name, version in before.items())\'; '
                'test -s /opt/verifier/resolved-requirements.txt'
            )], network_mode="none", cap_drop=["ALL"],
            security_opt=["no-new-privileges"], detach=True,
        )
        assert container.wait(timeout=30)["StatusCode"] == 0, container.logs().decode(errors="replace")
        assert (tmp_path / "Dockerfile").read_text() == original
    finally:
        if container is not None:
            container.remove(force=True)
        try:
            client.images.remove(tag, force=True)
        except docker.errors.ImageNotFound:
            pass
        client.close()
