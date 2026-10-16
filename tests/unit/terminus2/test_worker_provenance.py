"""Worker image provenance tests (#744 Gate 1)."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

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


def test_worker_freeze_satisfies_installed_project_requirements() -> None:
    root = Path(__file__).resolve().parents[3]
    dockerfile = (root / "deploy/Dockerfile.worker").read_text()
    freeze = (root / "deploy/worker-image.lock").read_text()
    # Derive the first-party installation set from the actual worker build.
    # Optional development/GPU extras are not installed by this Dockerfile.
    editable_paths = re.findall(r"pip install --no-cache-dir -e (\S+)", dockerfile)
    assert editable_paths, "worker editable installs were not recognized"
    assert editable_paths == re.findall(
        r"(?:^|\s)(?:-e|--editable)(?:=|\s+)(\S+)", dockerfile
    ), "update worker provenance coverage for the changed editable-install syntax"
    pins = {canonicalize_name(name): pin for name, pin in worker_image_lock_pins().items()}
    for name, version in re.findall(
        r"^# Editable install with no version control \(([^=]+)==([^\)]+)\)$",
        freeze,
        re.MULTILINE,
    ):
        pins[canonicalize_name(name)] = version
    environment = default_environment()
    environment.update(
        python_version=WORKER_PYTHON_VERSION,
        python_full_version=f"{WORKER_PYTHON_VERSION}.0",
        sys_platform="linux",
        platform_system="Linux",
        os_name="posix",
        extra="",
    )
    mismatches: list[str] = []
    for editable in editable_paths:
        path = Path(editable)
        assert f"-e /app{('/' + path.as_posix()) if path != Path('.') else ''}" in freeze.splitlines()
        project = tomllib.loads((root / path / "pyproject.toml").read_text())["project"]
        for value in project["dependencies"]:
            requirement = Requirement(value)
            if requirement.marker and not requirement.marker.evaluate(environment):
                continue
            version = pins.get(canonicalize_name(requirement.name))
            if version is None or version not in requirement.specifier:
                mismatches.append(f"{project['name']}: {value}; worker freeze has {version}")
    assert not mismatches, "Regenerate worker dependency evidence:\n" + "\n".join(mismatches)


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
