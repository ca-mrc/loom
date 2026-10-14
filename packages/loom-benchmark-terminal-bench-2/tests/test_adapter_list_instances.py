"""Native Harbor rev-6 instance discovery contracts."""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import replace
from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2 import upstream
from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter

from loom.models.task import TaskConfig


def _render_manifest(lock: upstream.TB21Lock) -> str:
    lines = ["[dataset]", f'name = "{lock.dataset}"', ""]
    for task in lock.tasks:
        lines.extend(
            (
                "[[tasks]]",
                f'name = "{task.name}"',
                f'digest = "{task.digest}"',
                "",
            )
        )
    return "\n".join(lines)


def _write_native_task(task_dir: Path, *, task_name: str) -> None:
    task_dir.mkdir(parents=True)
    (task_dir / "task.toml").write_text(
        'schema_version = "1.1"\n'
        'artifacts = ["logs/verifier/ctrf.json"]\n\n'
        "[task]\n"
        f'name = "{task_name}"\n'
        'description = "Native Harbor task."\n'
        'keywords = ["terminal"]\n\n'
        "[verifier]\n"
        "timeout_sec = 900.0\n\n"
        "[agent]\n"
        "timeout_sec = 900.0\n\n"
        "[environment]\n"
        "build_timeout_sec = 600.0\n"
        'docker_image = "example/tb21:rev6"\n'
        "cpus = 1\n"
        "memory_mb = 2048\n"
        "storage_mb = 10240\n"
        "gpus = 0\n"
        "allow_internet = true\n"
        'architecture = "x86_64"\n\n'
        "[environment.env]\n"
        'TB21_NATIVE = "true"\n',
    )
    instruction = task_dir / "instruction.md"
    instruction.write_bytes(b"Complete the native task.\n")
    environment = task_dir / "environment"
    environment.mkdir()
    dockerfile = environment / "Dockerfile"
    dockerfile.write_bytes(b"FROM example/tb21:rev6\n")
    dockerfile.chmod(0o640)
    tests = task_dir / "tests"
    tests.mkdir()
    test_sh = tests / "test.sh"
    test_sh.write_bytes(b"#!/bin/sh\nprintf '0\\n' > /logs/verifier/reward.txt\n")
    test_sh.chmod(0o750)
    solution = task_dir / "solution"
    solution.mkdir()
    solve_sh = solution / "solve.sh"
    solve_sh.write_bytes(b"#!/bin/sh\nexit 0\n")
    solve_sh.chmod(0o750)


@pytest.fixture
def source_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A verified native materialization containing every locked task.

    Task 3 owns the canonical checksum lock.  This fixture deliberately uses
    every canonical task identity and digest, while a synthetic audit manifest
    gives the test a self-contained byte fixture for the verification call.
    """
    canonical_lock = upstream.load_tb21_lock()
    root = tmp_path / "harbor-materialization"
    manifest = _render_manifest(canonical_lock)
    test_lock = replace(
        canonical_lock,
        manifest_sha256=hashlib.sha256(manifest.encode()).hexdigest(),
        source_manifest_divergences=[],
    )
    monkeypatch.setattr(upstream, "load_tb21_lock", lambda: test_lock)
    root.mkdir()
    (root / "harbor-materialization.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": test_lock.dataset,
                "revision": test_lock.revision,
                "metadata_version": test_lock.hub_metadata_version,
                "package_digests": test_lock.package_digests,
            }
        )
    )
    manifest_path = root / "audit" / test_lock.manifest_source
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(manifest)
    for task in test_lock.tasks:
        _write_native_task(
            root / "tasks" / task.name.removeprefix("terminal-bench/"),
            task_name=task.name,
        )
    return root


def test_adapter_declares_tb21_profile_metadata() -> None:
    assert TerminalBench2Adapter.name == "terminal-bench-2@tb2.1-r6"
    assert TerminalBench2Adapter.display_name == "Terminal-Bench 2.1 (Harbor rev 6)"
    assert TerminalBench2Adapter.task_count == 89


def test_adapter_lists_all_locked_native_tasks_and_converts_one(
    source_root: Path,
    tmp_path: Path,
) -> None:
    adapter = TerminalBench2Adapter()
    instances = list(adapter.list_instances(source_dir=source_root, split="test"))

    assert len(instances) == 89
    assert [instance.instance_id for instance in instances] == sorted(
        instance.instance_id for instance in instances
    )
    assert instances[0].raw == {
        "source_path": str(source_root / "tasks" / instances[0].instance_id),
    }
    assert instances[0].tags["oracle_eligible"] == "true"

    bundle = tmp_path / "bundle"
    converted = adapter.convert_instance(instances[0], out_dir=bundle)
    assert converted.task_id.startswith("terminal-bench-2@tb2.1-r6/")
    assert (bundle / "tests" / "test.sh").is_file()
    assert (bundle / "solution" / "solve.sh").is_file()
    assert (bundle / "upstream-task.toml").read_bytes() == (
        Path(instances[0].raw["source_path"]) / "task.toml"
    ).read_bytes()

    cfg = TaskConfig.model_validate(tomllib.loads((bundle / "task.toml").read_text()))
    assert cfg.task.id == converted.task_id
    assert cfg.environment.docker_image == "example/tb21:rev6"
    assert cfg.steps[0].artifacts == tomllib.loads(
        (bundle / "upstream-task.toml").read_text()
    )["artifacts"]


def test_oracle_eligibility_requires_a_real_non_symlink_solution(
    source_root: Path,
) -> None:
    task_dir = source_root / "tasks" / "adaptive-rejection-sampler"
    solve_sh = task_dir / "solution" / "solve.sh"
    solve_sh.unlink()
    solve_sh.symlink_to(task_dir / "tests" / "test.sh")

    instances = list(
        TerminalBench2Adapter().list_instances(
            source_dir=source_root,
            split="test",
        )
    )
    first = next(instance for instance in instances if instance.instance_id == task_dir.name)
    assert first.tags["oracle_eligible"] == "false"


def test_list_instances_fails_closed_without_a_verified_materialization(
    tmp_path: Path,
) -> None:
    with pytest.raises(upstream.TB21LockError):
        list(
            TerminalBench2Adapter().list_instances(
                source_dir=tmp_path / "native-without-lock",
                split="test",
            )
        )


def test_list_instances_fails_closed_on_untranslated_compose_semantics(
    source_root: Path,
) -> None:
    task_dir = source_root / "tasks" / "adaptive-rejection-sampler"
    (task_dir / "environment" / "docker-compose.yaml").write_text(
        "services:\n  helper:\n    image: redis:7\n",
    )

    with pytest.raises(upstream.TB21LockError, match="unsupported compose semantics"):
        list(
            TerminalBench2Adapter().list_instances(
                source_dir=source_root,
                split="test",
            )
        )
