from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
UV_TOOLCHAIN = tomllib.loads((ROOT / "config/uv-toolchain.toml").read_text(encoding="utf-8"))
UV_VERSION = UV_TOOLCHAIN["version"]
SETUP_UV_PREFIX = "astral-sh/setup-uv@"
UV_CHECKSUMS = {target: metadata["sha256"] for target, metadata in UV_TOOLCHAIN["archives"].items()}
MATRIX_CHECKSUM_EXPRESSION = "${{ matrix.uv_checksum }}"
SAFE_SAVE_EXPRESSION = (
    "${{ github.event_name != 'pull_request' && github.event_name != 'merge_group' }}"
)
TARGET_ENVIRONMENTS = {
    "sys_platform == 'darwin' and platform_machine == 'arm64'",
    "sys_platform == 'linux' and platform_machine == 'x86_64'",
    "sys_platform == 'linux' and platform_machine == 'aarch64'",
}
WORKSPACE_MEMBERS = {
    "packages/loom-launcher",
    "packages/loom-bundle-checksum",
    "packages/loom-benchmarks",
    "packages/loom-benchmark-terminal-bench-2",
    "packages/loom-terminalgen",
}


def _workflows() -> dict[Path, dict[str, Any]]:
    workflow_paths = {
        *(ROOT / ".github/workflows").glob("*.yml"),
        *(ROOT / ".github/workflows").glob("*.yaml"),
    }
    return {
        path: yaml.safe_load(path.read_text(encoding="utf-8")) for path in sorted(workflow_paths)
    }


def _workflow_steps(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    return [step for job in workflow["jobs"].values() for step in job.get("steps", [])]


def test_uv_metadata_defines_one_non_narrowing_workspace_lock() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    uv = pyproject["tool"]["uv"]

    assert uv["required-version"] == f"=={UV_VERSION}"
    assert set(uv["environments"]) == TARGET_ENVIRONMENTS
    assert set(uv["required-environments"]) == TARGET_ENVIRONMENTS
    assert set(uv["workspace"]["members"]) == WORKSPACE_MEMBERS
    assert pyproject["project"]["requires-python"] == ">=3.11"

    sources = uv["sources"]
    assert sources["loom-bundle-checksum"] == {"workspace": True}
    assert sources["loom-benchmarks"] == {"workspace": True}
    assert sources["loom-benchmark-terminal-bench-2"] == {"workspace": True}
    assert sources["loom-terminalgen"] == {"workspace": True}

    terminal_bench = tomllib.loads(
        (ROOT / "packages/loom-benchmark-terminal-bench-2/pyproject.toml").read_text(
            encoding="utf-8"
        )
    )
    assert terminal_bench["tool"]["uv"]["sources"] == {
        "loom": {"workspace": True},
        "loom-benchmarks": {"workspace": True},
    }

    ignored = {
        line.strip()
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert "uv.lock" not in ignored
    assert (ROOT / "uv.lock").is_file()


def test_uv_toolchain_authority_pins_official_archives() -> None:
    assert UV_TOOLCHAIN["schema_version"] == 1
    assert UV_TOOLCHAIN["release_url"] == (
        f"https://github.com/astral-sh/uv/releases/tag/{UV_VERSION}"
    )
    assert UV_TOOLCHAIN["archives"] == {
        "macos-arm64": {
            "target": "aarch64-apple-darwin",
            "asset": "uv-aarch64-apple-darwin.tar.gz",
            "sha256": UV_CHECKSUMS["macos-arm64"],
        },
        "linux-x86_64": {
            "target": "x86_64-unknown-linux-gnu",
            "asset": "uv-x86_64-unknown-linux-gnu.tar.gz",
            "sha256": UV_CHECKSUMS["linux-x86_64"],
        },
        "linux-arm64": {
            "target": "aarch64-unknown-linux-gnu",
            "asset": "uv-aarch64-unknown-linux-gnu.tar.gz",
            "sha256": UV_CHECKSUMS["linux-arm64"],
        },
    }
    assert all(
        len(checksum) == 64 and set(checksum) <= set("0123456789abcdef")
        for checksum in UV_CHECKSUMS.values()
    )


def test_uv_binary_and_lock_are_exact_and_current() -> None:
    version = subprocess.run(
        ["uv", "--version"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.split()[1]
    assert version == UV_VERSION

    result = subprocess.run(
        ["uv", "lock", "--check"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_workflows_use_checksum_verified_uv_locked_sync_and_safe_caches() -> None:
    setup_steps: list[tuple[Path, dict[str, Any]]] = []

    workflows = _workflows()
    for path, workflow in workflows.items():
        for step in _workflow_steps(workflow):
            uses = str(step.get("uses", ""))
            assert not uses.startswith("actions/cache@"), path
            if uses.startswith(SETUP_UV_PREFIX):
                setup_steps.append((path, step))
            if uses.startswith("actions/setup-go@"):
                assert step.get("with", {}).get("cache") is False, path

            run = str(step.get("run", ""))
            assert "astral.sh/uv/install.sh" not in run, path
            # Join shell line-continuations so a multi-line command reads as a
            # single logical line for the per-command lock assertions below.
            for line in run.replace("\\\n", " ").splitlines():
                if "uv pip install" in line:
                    # Installing a locally-built wheel with --no-deps performs no
                    # index resolution and stays lockfile-reproducible; an
                    # index-resolving `uv pip install` remains forbidden.
                    assert "--no-deps" in line, (path, line)
                if "uv sync" in line:
                    assert "--locked" in line, (path, line)
                if "uv run" in line:
                    assert "uv run --no-sync" in line, (path, line)

    assert setup_steps
    for path, step in setup_steps:
        inputs = step.get("with", {})
        assert inputs.get("version") == UV_VERSION, path
        checksum = inputs.get("checksum")
        assert checksum in {UV_CHECKSUMS["linux-x86_64"], MATRIX_CHECKSUM_EXPRESSION}, path
        assert inputs.get("enable-cache") is True, path
        triggers = workflows[path].get("on", workflows[path].get(True, {}))
        untrusted_events = {"pull_request", "pull_request_target", "merge_group", "workflow_call"}
        if untrusted_events.intersection(triggers):
            assert inputs.get("save-cache") == SAFE_SAVE_EXPRESSION, path
        else:
            assert inputs.get("save-cache") is True, path
        assert inputs.get("cache-dependency-glob") == "uv.lock", path


def test_ci_requires_real_locked_install_on_nebius_server_architecture() -> None:
    # Per-PR server validation follows the amd64-only Nebius runtime. The macOS
    # target stays in the uv.lock authority (TARGET_ENVIRONMENTS) and is
    # exercised by the nightly `macos-locked-environment.yml` schedule rather
    # than billed at ~10x on every pull request.
    workflow = _workflows()[ROOT / ".github/workflows/ci.yml"]
    jobs = workflow["jobs"]
    matrix_job = jobs["locked-environments"]

    assert matrix_job["if"] == "needs.workflow-plan.outputs.locked_environments == 'true'"
    assert matrix_job["strategy"]["fail-fast"] is False
    assert matrix_job["strategy"]["matrix"]["include"] == [
        {
            "target": "linux-x86_64",
            "runner": "ubuntu-24.04",
            "expected_system": "Linux",
            "expected_machine": "x86_64",
            "uv_checksum": UV_CHECKSUMS["linux-x86_64"],
        },
    ]
    script = "\n".join(str(step.get("run", "")) for step in matrix_job["steps"] if "run" in step)
    assert "uv sync --locked --all-packages --extra dev --python 3.11" in script
    assert "uv pip check --python .venv/bin/python" in script
    for package in (
        "loom",
        "loom_bundle_checksum",
        "loom_launcher",
        "loom_benchmarks",
        "loom_benchmark_terminal_bench_2",
    ):
        assert f"import {package}" in script

    assert "locked-environments" in jobs["repository-checks"]["needs"]
    validation = next(
        step
        for step in jobs["repository-checks"]["steps"]
        if step.get("name") == "Enforce selected validation results"
    )["run"]
    assert '"$LOCKED_RESULT"' in validation

    setup_uv = next(
        step
        for step in matrix_job["steps"]
        if str(step.get("uses", "")).startswith(SETUP_UV_PREFIX)
    )
    assert setup_uv["with"]["checksum"] == MATRIX_CHECKSUM_EXPRESSION





def test_runbook_uv_commands_never_resolve_implicitly() -> None:
    runbooks = sorted((ROOT / "docs/runbooks").glob("*.md"))
    assert runbooks
    for path in runbooks:
        for line in path.read_text(encoding="utf-8").splitlines():
            if re.search(r"^\s*uv sync ", line):
                assert "--locked" in line, (path, line)
            if re.search(r"^\s*uv run ", line):
                assert "uv run --no-sync " in line, (path, line)

    operator_runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(encoding="utf-8")
    assert (
        "uv sync --locked --all-packages --extra cluster --extra rollout --extra dev --python 3.11"
    ) in operator_runbook
    assert "uv pip check --python .venv/bin/python" in operator_runbook
