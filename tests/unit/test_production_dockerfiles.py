import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from loom.dockerfile_instructions import dockerfile_instructions

ROOT = Path(__file__).resolve().parents[2]

_EDITABLE_ROOT_IMAGES = (
    "Dockerfile.control-plane",
    "Dockerfile.family-orchestrator",
    "Dockerfile.gateway",
    "Dockerfile.pipeline-orchestrator",
    "Dockerfile.service",
    "Dockerfile.worker",
)


def test_editable_root_images_install_neutral_bundle_checksum_first() -> None:
    """A clean image build must satisfy Loom's local checksum dependency."""
    for name in _EDITABLE_ROOT_IMAGES:
        dockerfile = ROOT / "deploy" / name
        text = dockerfile.read_text()
        assert (
            "COPY packages ./packages" in text
            or "COPY packages/loom-bundle-checksum ./packages/loom-bundle-checksum" in text
        ), dockerfile
        checksum_install = "pip install --no-cache-dir -e ./packages/loom-bundle-checksum"
        assert checksum_install in text, dockerfile
        install_commands = [
            shlex.split(line.strip().removeprefix("RUN ").removesuffix("\\"))
            for line in text.splitlines()
            if "pip install" in line
        ]
        checksum_index = next(
            index
            for index, command in enumerate(install_commands)
            if command[:5] == shlex.split(checksum_install)
        )
        root_index = next(
            index
            for index, command in enumerate(install_commands)
            if command[:4] == ["pip", "install", "--no-cache-dir", "-e"]
            and len(command) >= 5
            and re.fullmatch(r"\.(?:\[[A-Za-z0-9_,-]+\])?", command[4])
        )
        assert checksum_index < root_index, dockerfile


def test_db_facing_images_include_migrations_for_schema_startup() -> None:
    dockerfiles = [
        ROOT / "deploy" / "Dockerfile.control-plane",
        ROOT / "deploy" / "Dockerfile.gateway",
        ROOT / "deploy" / "Dockerfile.service",
    ]

    for dockerfile in dockerfiles:
        text = dockerfile.read_text()
        assert "COPY database/migrations ./database/migrations" in text, dockerfile


def test_service_image_exposes_immutable_build_revision_to_runtime() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "ARG LOOM_BUILD_SHA=unknown" in text
    assert 'LABEL org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in text
    assert 'ENV LOOM_BUILD_SHA="${LOOM_BUILD_SHA}"' not in text
    assert "printf '%s\\n' \"${LOOM_BUILD_SHA}\" > /opt/loom/build-sha" in text
    assert "chmod 0444 /opt/loom/build-sha" in text
    assert text.index("pip install --no-cache-dir -e .") < text.index("ARG LOOM_BUILD_SHA=unknown")


def test_control_plane_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.control-plane").read_text()

    assert "chmod -R a+rX ./src ./database/migrations ./database/capacity_guard_migrations" in text


def test_control_plane_image_contains_capacity_guard_migrations() -> None:
    text = (ROOT / "deploy" / "Dockerfile.control-plane").read_text()

    assert "COPY database/capacity_guard_migrations ./database/capacity_guard_migrations" in text


def test_gateway_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.gateway").read_text()

    assert "chmod -R a+rX ./src ./database/migrations" in text


def test_service_source_is_readable_by_declared_nonroot_workloads() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "chmod -R a+rX ./src ./packages ./database/migrations ./database/capacity_guard_migrations" in text


def test_service_image_contains_capacity_guard_migrations() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "COPY database/capacity_guard_migrations ./database/capacity_guard_migrations" in text


def test_service_image_contains_digest_pinned_kubectl_for_personal_lifecycle() -> None:
    text = (ROOT / "deploy" / "Dockerfile.service").read_text()

    assert "registry.k8s.io/kubectl:v1.36.2@sha256:" in text
    assert "COPY --from=kubectl /bin/kubectl /usr/local/bin/kubectl" in text


def test_actuator_image_copied_source_imports_both_processes_without_checkout_fallback(tmp_path: Path) -> None:
    """A source-tree import must not hide a missing slim-image dependency."""
    for instruction in dockerfile_instructions((ROOT / "deploy/Dockerfile.execution-actuator").read_text()):
        if instruction.keyword != "COPY":
            continue
        *sources, destination = shlex.split(instruction.arguments)
        if not destination.startswith("./src/"):
            continue
        target = tmp_path / destination
        target.mkdir(parents=True, exist_ok=True)
        for source in sources:
            path = ROOT / source
            if path.is_dir():
                shutil.copytree(path, target, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                shutil.copy2(path, target / path.name)
    script = """
import importlib.abc
import importlib.machinery
import pathlib
import sys
source = pathlib.Path(sys.argv[1]).resolve()
sys.path.insert(0, str(source))
class PackagedSourceOnly(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'loom', 'loom_control_plane', 'loom_service',
            'loom_execution_actuator', 'loom_execution_capacity_collector',
        }:
            spec = importlib.machinery.PathFinder.find_spec(fullname, path)
            if spec is None or spec.origin is None or not pathlib.Path(spec.origin).is_relative_to(source):
                raise ModuleNotFoundError('missing image dependency: ' + fullname)
            return spec
sys.meta_path.insert(0, PackagedSourceOnly())
import loom_execution_actuator.__main__
import loom_execution_capacity_collector.__main__
"""
    result = subprocess.run([sys.executable, "-I", "-c", script, str(tmp_path / "src")],
                            cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
