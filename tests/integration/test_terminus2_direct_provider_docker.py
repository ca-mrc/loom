"""Run the authentic Harbor direct-provider contracts in an offline 3.12 image."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tomllib
from pathlib import Path
from uuid import uuid4

import pytest

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA

pytestmark = [pytest.mark.docker, pytest.mark.timeout(900)]


def _run(args: list[str], *, cwd: Path, timeout: int) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_pinned_harbor_direct_provider_contracts(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    suffix = uuid4().hex
    base = os.environ.get("LOOM_TEST_HARBOR_IMAGE")
    owns_base = base is None
    if base is None:
        base = f"loom-harbor-direct-base-test:{suffix}"
    image = f"loom-harbor-direct-test:{suffix}"
    container = f"loom-harbor-direct-test-{suffix}"

    # Test the current Loom lock, including native provider SDKs, rather than
    # whichever dependency versions happen to be in a reusable Harbor image.
    exported = _run([
        "uv", "export", "--frozen", "--no-dev", "--no-emit-project",
        "--no-emit-workspace", "--no-hashes", "--format", "requirements-txt",
    ], cwd=root, timeout=30).stdout
    # Harbor's base Supabase dependency requires websockets<16. Loom does not
    # directly constrain websockets; allow the resolver to choose a compatible
    # version for this optional Harbor environment while preserving other pins.
    constraints = "\n".join(
        line for line in exported.splitlines() if not line.startswith("websockets==")
    )
    lock = tomllib.loads((root / "uv.lock").read_text())
    requirements = "-c /tmp/qualification-constraints.txt"
    for name in (
        "pytest", "pytest-asyncio", "aiofiles", "boto3", "tomli-w",
        "litellm", "openai", "anthropic", "google-genai",
    ):
        versions = {package["version"] for package in lock["package"] if package["name"] == name}
        assert len(versions) == 1, f"expected one locked version of {name}"
        requirements += f"\n{name}=={versions.pop()}"
    (tmp_path / "constraints.txt").write_text(constraints + "\n")
    (tmp_path / "requirements.txt").write_text(requirements + "\n")
    (tmp_path / "Dockerfile").write_text(
        "ARG HARBOR_IMAGE\nFROM ${HARBOR_IMAGE}\nUSER 0\n"
        "COPY constraints.txt /tmp/qualification-constraints.txt\n"
        "COPY requirements.txt /tmp/qualification-requirements.txt\n"
        "RUN python -m pip install --no-cache-dir -r /tmp/qualification-requirements.txt "
        "&& python -m pip check "
        "&& rm /tmp/qualification-requirements.txt /tmp/qualification-constraints.txt\n"
        "USER 65532:65532\n"
    )

    try:
        if owns_base:
            _run([
                "docker", "build", "--file", "deploy/Dockerfile.harbor-runtime",
                "--tag", base, ".",
            ], cwd=root, timeout=540)
        manifest = json.loads(_run(
            ["docker", "image", "inspect", base], cwd=root, timeout=30,
        ).stdout)[0]
        assert manifest["Config"]["Labels"]["io.loom.harbor_source_revision"] == HARBOR_COMPAT_SHA
        _run([
            "docker", "build", "--build-arg", f"HARBOR_IMAGE={base}",
            "--tag", image, str(tmp_path),
        ], cwd=root, timeout=240)
        mounts = [
            (root / "src/loom", "loom"),
            (root / "src/loom_cli", "loom_cli"),
            (root / "packages/loom-bundle-checksum/loom_bundle_checksum", "loom_bundle_checksum"),
        ]
        args = [
            "docker", "run", "--rm", "--name", container, "--network", "none",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--memory", "1g", "--cpus", "1",
            "-e", "LITELLM_LOCAL_MODEL_COST_MAP=True",
        ]
        for source, package in mounts:
            args.extend(["-v", f"{source}:/usr/local/lib/python3.12/site-packages/{package}:ro"])
        # Retain the repository layout for the probe's canonical version pin.
        args.extend([
            "-v", f"{root / 'tests/unit/terminus2/test_direct_runtime_real_harbor.py'}:/qualification/tests/unit/terminus2/test_direct_runtime_real_harbor.py:ro",
            "-v", f"{root / 'config/harbor-runtime.json'}:/qualification/config/harbor-runtime.json:ro",
            "--entrypoint", "python", image, "-I", "-B", "-m", "pytest", "-q",
            "/qualification/tests/unit/terminus2/test_direct_runtime_real_harbor.py", "--basetemp=/tmp/qualification-tests",
            "-o", "cache_dir=/tmp/pytest-cache", "-o", "asyncio_mode=auto",
        ])
        result = _run(args, cwd=root, timeout=90)
        assert re.search(r"\b[1-9]\d* passed\b", result.stdout), result.stdout
        assert "skipped" not in result.stdout, result.stdout
    finally:
        # A timeout can leave docker run's container behind after its client is
        # killed. Remove only this test's UUID-named container and derived image.
        subprocess.run(["docker", "rm", "--force", container], capture_output=True, timeout=30)
        subprocess.run(["docker", "image", "rm", image], capture_output=True, timeout=30)
        if owns_base:
            subprocess.run(["docker", "image", "rm", base], capture_output=True, timeout=30)
