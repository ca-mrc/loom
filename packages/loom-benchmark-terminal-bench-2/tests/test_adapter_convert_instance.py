"""Native Harbor task conversion preserves source files and modes."""

from __future__ import annotations

import tomllib
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2 import upstream
from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig


def _write_native_task(source: Path) -> None:
    source.mkdir(parents=True)
    (source / "task.toml").write_text(
        'schema_version = "1.1"\n'
        'artifacts = ["logs/verifier/ctrf.json", "answer.json"]\n\n'
        "[task]\n"
        'name = "terminal-bench/native-copy"\n'
        'description = "Keep native content."\n'
        'keywords = ["native", "terminal"]\n\n'
        "[verifier]\n"
        "timeout_sec = 111.0\n\n"
        "[agent]\n"
        "timeout_sec = 222.0\n\n"
        "[environment]\n"
        "build_timeout_sec = 333.0\n"
        'docker_image = "example/native:rev6"\n'
        "cpus = 2\n"
        "memory_mb = 4096\n"
        "storage_mb = 20480\n"
        "gpus = 0\n"
        "allow_internet = true\n"
        'architecture = "x86_64"\n\n'
        "[environment.env]\n"
        'NATIVE = "true"\n',
    )
    (source / "instruction.md").write_bytes(b"Preserve instruction bytes.\n")
    environment = source / "environment"
    environment.mkdir()
    dockerfile = environment / "Dockerfile"
    dockerfile.write_bytes(b"FROM example/native:rev6\n")
    dockerfile.chmod(0o640)
    tests = source / "tests"
    tests.mkdir()
    test_sh = tests / "test.sh"
    test_sh.write_bytes(b"#!/bin/sh\nprintf '0\\n' > /logs/verifier/reward.txt\n")
    test_sh.chmod(0o750)
    solution = source / "solution"
    solution.mkdir()
    solve_sh = solution / "solve.sh"
    solve_sh.write_bytes(b"#!/bin/sh\necho oracle\n")
    solve_sh.chmod(0o750)


def test_convert_preserves_native_bytes_modes_and_supported_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source" / "tasks" / "native-copy"
    _write_native_task(source)
    source_files = {
        relative: (source / relative).read_bytes()
        for relative in (
            "task.toml",
            "instruction.md",
            "environment/Dockerfile",
            "tests/test.sh",
            "solution/solve.sh",
        )
    }
    source_modes = {
        relative: (source / relative).stat().st_mode & 0o777
        for relative in (
            "environment/Dockerfile",
            "tests/test.sh",
            "solution/solve.sh",
        )
    }
    instance = BenchmarkInstance(
        instance_id="native-copy",
        split="test",
        raw={"source_path": str(source)},
    )
    out_dir = tmp_path / "bundle"

    verified_roots: list[Path] = []
    test_lock = replace(
        upstream.load_tb21_lock(),
        tasks=(
            upstream.TB21TaskLock(
                "terminal-bench/native-copy",
                "sha256:" + "a" * 64,
            ),
        ),
    )
    monkeypatch.setattr(upstream, "load_tb21_lock", lambda: test_lock)
    monkeypatch.setattr(
        upstream,
        "verify_tb21_materialization",
        lambda root: verified_roots.append(root),
    )

    converted = TerminalBench2Adapter().convert_instance(instance, out_dir=out_dir)

    assert converted.task_id == "terminal-bench-2@tb2.1-r6/native-copy"
    assert verified_roots == [source.parent.parent]
    assert (out_dir / "upstream-task.toml").read_bytes() == source_files["task.toml"]
    assert (out_dir / "instruction.md").read_bytes() == source_files["instruction.md"]
    assert (out_dir / "environment" / "Dockerfile").read_bytes() == source_files[
        "environment/Dockerfile"
    ]
    assert (out_dir / "tests" / "test.sh").read_bytes() == source_files["tests/test.sh"]
    assert (out_dir / "solution" / "solve.sh").read_bytes() == source_files["solution/solve.sh"]
    for relative, mode in source_modes.items():
        assert (out_dir / relative).stat().st_mode & 0o777 == mode

    cfg = TaskConfig.model_validate(tomllib.loads((out_dir / "task.toml").read_text()))
    assert cfg.task.id == converted.task_id
    assert cfg.task.name == "terminal-bench/native-copy"
    assert cfg.task.description == "Keep native content."
    assert cfg.task.labels == ["native", "terminal"]
    assert cfg.environment.docker_image == "example/native:rev6"
    assert cfg.environment.build_timeout_sec == 333.0
    assert cfg.environment.cpus == 2
    assert cfg.environment.memory_mb == 4096
    assert cfg.environment.storage_mb == 20480
    assert cfg.environment.gpus == 0
    assert cfg.environment.workdir.as_posix() == "/app"
    assert cfg.environment.environment == {"NATIVE": "true"}
    assert cfg.agent.timeout_sec == 222.0
    assert cfg.verifier.timeout_sec == 111.0
    assert cfg.verifier.args["script_path"] == "/app/verifier/run.sh"
    assert cfg.steps[0].artifacts == tomllib.loads(source_files["task.toml"].decode())["artifacts"]

    provenance = TerminalBench2Adapter().task_source_provenance(
        instance=instance,
        bundle_dir=out_dir,
        task_config=cfg.model_dump(mode="json"),
        checksum=converted.checksum,
    )
    assert provenance["verifier_asset"] == {
        "script_path": "/app/verifier/run.sh",
        "sha256": f"sha256:{sha256((out_dir / 'verifier' / 'run.sh').read_bytes()).hexdigest()}",
        "mode": "0755",
    }
    assert provenance["resource_limits"] == {
        "cpus": 2.0,
        "memory_mb": 4096,
        "storage_mb": 20480,
        "gpus": 0,
    }


def test_convert_fails_closed_without_explicit_runnable_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source" / "tasks" / "native-copy"
    _write_native_task(source)
    task_toml = source / "task.toml"
    task_toml.write_text(
        task_toml.read_text().replace('docker_image = "example/native:rev6"\n', ""),
    )
    instance = BenchmarkInstance(
        instance_id="native-copy",
        split="test",
        raw={"source_path": str(source)},
    )
    test_lock = replace(
        upstream.load_tb21_lock(),
        tasks=(
            upstream.TB21TaskLock(
                "terminal-bench/native-copy",
                "sha256:" + "a" * 64,
            ),
        ),
    )
    monkeypatch.setattr(upstream, "load_tb21_lock", lambda: test_lock)
    monkeypatch.setattr(upstream, "verify_tb21_materialization", lambda _root: None)

    with pytest.raises(upstream.TB21LockError, match="no explicit runnable image"):
        TerminalBench2Adapter().convert_instance(instance, out_dir=tmp_path / "bundle")
