"""Unit coverage for Nebius Terminus publish-local ingest profile (#1996)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.nebius_terminus_ingest import (
    DEFAULT_CPUS,
    DEFAULT_MEMORY_MB,
    DEFAULT_STORAGE_MB,
    NEBIUS_TERMINUS_PROFILE,
    VERIFIER_SCRIPT_PATH,
    adapt_bundle_for_nebius_terminus,
    offline_verifier_run_sh_bytes,
    preflight_nebius_terminus_admission,
    resolve_execution_profile,
)
from loom.terminal_bench_normalize import (
    DEFAULT_VERIFIER_SCRIPT_PATH,
    normalize_terminal_bench_task_toml,
)

_SEI = {
    "service_execution_input": {
        "schema_version": "loom.service-execution-input.v1",
        "manifest_uri": "s3://artifacts/task-inputs/task-1.json",
        "manifest_sha256": "sha256:" + "d" * 64,
        "file_count": 3,
        "total_bytes": 4096,
    }
}


def _harbor_shaped_config(**env_updates: object) -> dict:
    environment: dict = {
        "os": "linux",
        "dockerfile": "environment/Dockerfile",
        "docker_build_context": "environment",
        "user": "root",
        "workdir": "/home/root",
    }
    environment.update(env_updates)
    return {
        "schema_version": "1",
        "task": {"id": "sample", "name": "Sample"},
        "environment": environment,
        "agent": {"name": "oracle"},
        "verifier": {
            "name": "script",
            "user": "root",
            "env_mode": "separate",
            "args": {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH},
        },
        "steps": [{"name": "main", "instruction_file": "instruction.md"}],
    }


def _write_runtime_inputs(staged: Path) -> None:
    (staged / "environment").mkdir(exist_ok=True)
    (staged / "environment/Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /app\n")
    (staged / "tests").mkdir(exist_ok=True)
    (staged / "tests/test.sh").write_text(
        "#!/bin/bash\napt-get update\napt-get install -y curl\n"
        "curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n"
        "source $HOME/.local/bin/env\n"
        "uvx -p 3.13 -w pytest==8.4.1 -w pytest-json-ctrf==0.3.5 "
        "pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA\n"
        "if [ $? -eq 0 ]; then echo 1 > /logs/verifier/reward.txt; "
        "else echo 0 > /logs/verifier/reward.txt; fi\n",
    )


def test_resolve_execution_profile_accepts_known_and_rejects_unknown() -> None:
    assert resolve_execution_profile(None) is None
    assert resolve_execution_profile(NEBIUS_TERMINUS_PROFILE) == NEBIUS_TERMINUS_PROFILE
    with pytest.raises(ValueError, match="unknown execution profile"):
        resolve_execution_profile("gb10-magic")


def test_adapt_fills_resources_forces_gateway_and_verifier(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    (staged / "tests").mkdir(exist_ok=True)
    (staged / "tests" / "test_outputs.py").write_text("def test_ok():\n    assert True\n")

    adapted, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())

    env = adapted["environment"]
    assert env["cpus"] == DEFAULT_CPUS
    assert env["memory_mb"] == DEFAULT_MEMORY_MB
    assert env["storage_mb"] == DEFAULT_STORAGE_MB
    assert env["cpu_arch"] == "x86_64"
    assert env["user"] == "root"
    assert env["workdir"] == "/home/root"
    assert env["network_policies_supported"] == ["gateway-only"]
    assert env["baseline_network_policy"] == {"kind": "gateway-only"}
    assert adapted["verifier"]["user"] == "root"
    assert adapted["verifier"]["env_mode"] == "separate"
    assert adapted["verifier"]["args"]["script_path"] == VERIFIER_SCRIPT_PATH
    assert adapted["verifier"]["args"] == {"script_path": VERIFIER_SCRIPT_PATH}
    assert stats.resources_filled
    assert stats.network_forced_gateway_only
    assert not stats.verifier_identity_stripped
    assert stats.verifier_path_forced
    assert stats.cpu_arch_forced
    assert not stats.workspace_identity_forced
    assert stats.verifier_wrapper_installed
    wrapper = staged / "verifier" / "run.sh"
    assert wrapper.is_file()
    assert wrapper.stat().st_mode & 0o111
    assert b"harbor-offline.sh" in wrapper.read_bytes()
    assert env["dockerfile"] != "environment/Dockerfile"
    assert (staged / env["dockerfile"]).is_file()


def test_adapt_preserves_complete_resource_triple(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    adapted, stats = adapt_bundle_for_nebius_terminus(
        staged,
        _harbor_shaped_config(cpus=2, memory_mb=4096, storage_mb=8192),
    )
    env = adapted["environment"]
    assert env["cpus"] == 2
    assert env["memory_mb"] == 4096
    assert env["storage_mb"] == 8192
    assert not stats.resources_filled


def test_adapt_replaces_harbor_online_bridge_wrapper(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    verifier = staged / "verifier"
    verifier.mkdir()
    online = verifier / "run.sh"
    online.write_text(
        "#!/bin/sh\n"
        "echo 'harbor loom bridge: tests/test.sh not found' >&2\n"
        "python3 -m pip install -q pytest\n",
        encoding="utf-8",
    )
    online.chmod(0o755)

    _, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())
    assert stats.verifier_wrapper_installed
    body = online.read_bytes()
    assert body == offline_verifier_run_sh_bytes()
    assert b"pip install" not in body


def test_adapt_keeps_existing_offline_wrapper(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    verifier = staged / "verifier"
    verifier.mkdir()
    existing = offline_verifier_run_sh_bytes()
    target = verifier / "run.sh"
    target.write_bytes(existing)
    target.chmod(0o755)

    _, stats = adapt_bundle_for_nebius_terminus(staged, _harbor_shaped_config())
    assert not stats.verifier_wrapper_installed
    assert target.read_bytes() == existing


def test_adapt_upgrades_old_pytest_only_wrapper(tmp_path: Path) -> None:
    _write_runtime_inputs(tmp_path)
    target = tmp_path / "verifier/run.sh"
    target.parent.mkdir()
    target.write_text("#!/bin/sh\n/opt/verifier/bin/pytest /tests/test_outputs.py\n")
    _, stats = adapt_bundle_for_nebius_terminus(tmp_path, _harbor_shaped_config())
    assert stats.verifier_wrapper_installed
    assert b"harbor-offline.sh" in target.read_bytes()


def test_adapt_upgrades_known_wrapper_that_discarded_failed_reward(tmp_path: Path) -> None:
    _write_runtime_inputs(tmp_path)
    target = tmp_path / "verifier/run.sh"
    target.parent.mkdir()
    legacy = Path(__file__).parents[1] / "fixtures/harbor/legacy-offline-verifier.sh"
    target.write_bytes(legacy.read_bytes())
    _, stats = adapt_bundle_for_nebius_terminus(tmp_path, _harbor_shaped_config())
    assert stats.verifier_wrapper_installed
    assert target.read_bytes() == offline_verifier_run_sh_bytes()


def test_adapt_rejects_unknown_custom_verifier(tmp_path: Path) -> None:
    _write_runtime_inputs(tmp_path)
    target = tmp_path / "verifier/run.sh"
    target.parent.mkdir()
    original = b"#!/bin/sh\nexec ./custom-evaluator\n"
    target.write_bytes(original)
    with pytest.raises(ValueError, match="custom verifier"):
        adapt_bundle_for_nebius_terminus(tmp_path, _harbor_shaped_config())
    assert target.read_bytes() == original


def test_adapt_after_normalize_fixes_absolute_verifier_path(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    raw = {
        "schema_version": "1.1",
        "task": {
            "name": "terminal-bench/sample",
            "description": "Native TB2.1 task.",
        },
        "environment": {
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "cpus": 1,
            "memory_mb": 2048,
            "storage_mb": 4096,
            "user": "root",
            "architecture": "any",
            "allow_internet": True,
        },
        "verifier": {"timeout_sec": 100.0, "user": "root"},
        "agent": {"timeout_sec": 100.0},
    }
    normalized = normalize_terminal_bench_task_toml(raw)
    assert normalized["verifier"]["args"]["script_path"] == DEFAULT_VERIFIER_SCRIPT_PATH
    assert normalized["environment"]["cpu_arch"] == "any"

    adapted, stats = adapt_bundle_for_nebius_terminus(staged, normalized)
    assert adapted["verifier"]["args"]["script_path"] == VERIFIER_SCRIPT_PATH
    assert adapted["environment"]["cpu_arch"] == "x86_64"
    assert adapted["environment"]["baseline_network_policy"] == {"kind": "public-web"}
    assert adapted["environment"]["network_policies_supported"] == ["public-web"]
    assert not stats.network_forced_gateway_only
    assert adapted["environment"]["user"] == "root"
    assert adapted["verifier"]["user"] == "root"
    assert stats.cpu_arch_forced
    assert stats.verifier_wrapper_installed
    assert stats.artifact_globs_stripped
    assert adapted.get("steps") == [{"name": "main"}]
    reasons = preflight_nebius_terminus_admission(adapted, _SEI)
    assert reasons == ()


def test_adapt_keeps_no_internet_off_the_dialer(tmp_path: Path) -> None:
    staged = tmp_path / "bundle"
    staged.mkdir()
    _write_runtime_inputs(staged)
    raw = {
        "schema_version": "1.1",
        "task": {"name": "terminal-bench/offline"},
        "environment": {
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "cpus": 1,
            "memory_mb": 2048,
            "storage_mb": 4096,
            "user": "root",
            "allow_internet": False,
        },
        "verifier": {"timeout_sec": 100.0, "user": "root"},
        "agent": {"timeout_sec": 100.0},
    }
    adapted, stats = adapt_bundle_for_nebius_terminus(
        staged, normalize_terminal_bench_task_toml(raw),
    )
    assert adapted["environment"]["baseline_network_policy"] == {"kind": "gateway-only"}
    assert stats.network_forced_gateway_only
    assert "public-web" not in str(adapted["environment"])
    assert preflight_nebius_terminus_admission(adapted, _SEI) == ()


def test_preflight_rejects_unadapted_harbor_config(tmp_path: Path) -> None:
    reasons = preflight_nebius_terminus_admission(_harbor_shaped_config(), _SEI)
    assert "gateway_only_network_required" in reasons
    assert "resource_limits_required" in reasons
    assert "standard_workspace_identity_required" not in reasons
    assert "custom_verifier_identity_unsupported" not in reasons
    assert "script_verifier_required" not in reasons
    assert "private_verifier_directory_required" in reasons


@pytest.mark.parametrize("workdir", ["/media/project", "/opt/project", "/root", "/data/project"])
def test_adaptation_preserves_declared_task_workdir(tmp_path: Path, workdir: str) -> None:
    _write_runtime_inputs(tmp_path)
    adapted, stats = adapt_bundle_for_nebius_terminus(
        tmp_path, _harbor_shaped_config(workdir=workdir),
    )
    assert adapted["environment"]["workdir"] == workdir
    assert not stats.workspace_identity_forced
    assert preflight_nebius_terminus_admission(adapted, _SEI) == ()


@pytest.mark.parametrize("workdir", [
    "/", "/loom/task", "/opt", "/opt/verifier", "/opt/verifier-python/project",
    "/proc/self", "/var", "/var/run/task", "/tests/project", "/tmp",
    "relative", "/app/../tests", "/app;touch /tmp/unsafe", "/app/$(id)",
    "", "/app/", "//app", "/app/./state", "/app//state", "/app\n", "/app/\x00state",
])
def test_adaptation_rejects_unsafe_workdir_before_writing_outputs(
    tmp_path: Path, workdir: str,
) -> None:
    _write_runtime_inputs(tmp_path)
    before = {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="workdir"):
        adapt_bundle_for_nebius_terminus(tmp_path, _harbor_shaped_config(workdir=workdir))
    assert {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("explicit_null", [False, True])
def test_adaptation_defaults_only_an_absent_workdir(tmp_path: Path, explicit_null: bool) -> None:
    _write_runtime_inputs(tmp_path)
    raw = _harbor_shaped_config()
    raw["environment"].pop("workdir")
    if explicit_null:
        raw["environment"]["workdir"] = None
    adapted, stats = adapt_bundle_for_nebius_terminus(tmp_path, raw)
    assert adapted["environment"]["workdir"] == "/app"
    assert stats.workspace_identity_forced


def test_offline_template_preserves_full_harbor_runner() -> None:
    wrapper = offline_verifier_run_sh_bytes()
    assert b'cp -R "$task_dir/tests/." /tests/' in wrapper
    assert b'bash "$task_dir/verifier/harbor-offline.sh"' in wrapper
    assert b"pip install" not in wrapper


def test_ingest_preserves_explicit_web_allowlist_and_identity(tmp_path: Path) -> None:
    _write_runtime_inputs(tmp_path)
    policy = {"kind": "web-allowlist", "destinations": [{"host": "registry.npmjs.org", "protocol": "https"}]}
    config = _harbor_shaped_config(
        user="1001:1002", environment={"HOME": "/home/miles"},
        baseline_network_policy=policy, network_policies_supported=["web-allowlist"],
    )
    adapted, stats = adapt_bundle_for_nebius_terminus(tmp_path, config)
    assert adapted["environment"]["baseline_network_policy"] == policy
    assert adapted["environment"]["network_policies_supported"] == ["web-allowlist"]
    assert adapted["environment"]["user"] == "1001:1002"
    assert adapted["environment"]["environment"] == {"HOME": "/home/miles"}
    assert not stats.network_forced_gateway_only
