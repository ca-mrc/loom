"""Worker image provenance tests (#744 Gate 1)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA
from loom.agent.terminus2.worker_provenance import (
    WORKER_PYTHON_VERSION,
    WORKER_WHEELS_REL,
    load_worker_wheel_provenance,
    worker_image_lock_pins,
    worker_image_provenance_summary,
)


def test_dockerfile_harbor_sha_matches_provenance() -> None:
    dockerfile = Path(__file__).resolve().parents[3] / "deploy" / "Dockerfile.worker"
    text = dockerfile.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("ARG HARBOR_COMPAT_SHA="):
            assert line.split("=", 1)[1] == HARBOR_COMPAT_SHA
            return
    raise AssertionError("HARBOR_COMPAT_SHA ARG not found in Dockerfile.worker")


def test_worker_image_lock_pins_harbor_openai_litellm() -> None:
    pins = worker_image_lock_pins()
    assert pins["harbor"].endswith(HARBOR_COMPAT_SHA)
    assert pins["openai"].startswith("2.")
    assert pins["litellm"].startswith("1.")


def test_worker_wheel_provenance_matches_lock() -> None:
    wheels = load_worker_wheel_provenance()
    pins = worker_image_lock_pins()
    assert wheels["harbor_compat_sha"] == HARBOR_COMPAT_SHA
    assert wheels["python_version"] == WORKER_PYTHON_VERSION
    assert wheels["packages"]["openai"]["version"] == pins["openai"]
    assert wheels["packages"]["litellm"]["version"] == pins["litellm"]
    for pkg in ("openai", "litellm"):
        assert len(wheels["packages"][pkg]["sha256"]) == 64


def test_worker_image_provenance_summary() -> None:
    summary = worker_image_provenance_summary()
    assert summary["python_version"] == WORKER_PYTHON_VERSION
    assert summary["harbor_compat_sha"] == HARBOR_COMPAT_SHA
    assert summary["openai_version"] is not None
    assert summary["wheel_hashes"]["openai"]


def test_worker_wheels_json_is_valid() -> None:
    path = Path(__file__).resolve().parents[3] / WORKER_WHEELS_REL
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == "1"


def _regenerate_with_wheel_hashes(
    tmp_path: Path, litellm_records: str
) -> tuple[subprocess.CompletedProcess[str], Path]:
    root = tmp_path / "worker"
    script = root / "scripts" / "ops" / "update_worker_image_lock.sh"
    script.parent.mkdir(parents=True)
    source_root = Path(__file__).resolve().parents[3]
    shutil.copyfile(source_root / "scripts" / "ops" / script.name, script)
    deploy = root / "deploy"
    deploy.mkdir()
    (deploy / "Dockerfile.worker").write_text(
        f"ARG HARBOR_COMPAT_SHA={HARBOR_COMPAT_SHA}\n", encoding="utf-8"
    )
    hash_output = tmp_path / "wheel-hashes.txt"
    hash_output.write_text(
        "openai-2.54.0-py3-none-any.whl:\n"
        f"--hash=sha256:{'a' * 64}\n{litellm_records}",
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker = fake_bin / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  build*) ;;
  *"pip freeze") printf 'openai==2.54.0\\nlitellm==1.104.2\\n' ;;
  *"pip hash"*) cat "${MOCK_WHEEL_HASH_OUTPUT}" ;;
  *'m.version("openai")'*) printf '2.54.0\\n' ;;
  *'m.version("litellm")'*) printf '1.104.2\\n' ;;
  *'m.version("harbor")'*) printf '0.24.0\\n' ;;
  *"pip check") printf 'No broken requirements found.\\n' ;;
  *) printf 'Unexpected mock Docker invocation: %s\\n' "$*" >&2; exit 97 ;;
esac
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "MOCK_WHEEL_HASH_OUTPUT": str(hash_output),
        "TMPDIR": str(tmp_path),
    }
    result = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, env=env, check=False
    )
    return result, deploy / "worker-image.wheels.json"


def test_worker_regeneration_records_downloaded_platform_wheel(tmp_path: Path) -> None:
    wheel = "litellm-1.104.2-cp310-abi3-manylinux_2_28_x86_64.whl"
    result, output = _regenerate_with_wheel_hashes(
        tmp_path, f"{wheel}:\n--hash=sha256:{'b' * 64}\n"
    )
    assert result.returncode == 0, result.stderr
    packages = json.loads(output.read_text(encoding="utf-8"))["packages"]
    assert packages["litellm"]["wheel"] == wheel
    assert packages["litellm"]["sha256"] == "b" * 64
    assert packages["openai"]["wheel"] == "openai-2.54.0-py3-none-any.whl"


@pytest.mark.parametrize(
    "litellm_records",
    [
        "",
        "litellm-1.104.2-cp310-abi3-manylinux_2_28_x86_64.whl:\n"
        f"--hash=sha256:{'b' * 64}\n"
        "litellm-1.104.2-py3-none-any.whl:\n"
        f"--hash=sha256:{'c' * 64}\n",
    ],
    ids=["missing", "ambiguous"],
)
def test_worker_regeneration_rejects_missing_or_ambiguous_wheel(
    tmp_path: Path, litellm_records: str
) -> None:
    result, output = _regenerate_with_wheel_hashes(tmp_path, litellm_records)
    assert result.returncode != 0
    assert not output.exists()
