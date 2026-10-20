"""SWE-Bench Verified adapter contract (Plan 15 Phase 1)."""

from __future__ import annotations

import json
import subprocess
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkInstance
from loom_benchmarks.fetch import fetch_upstream

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult

FIXTURE = (
    Path(__file__).parent
    / "fixtures" / "swe_bench_verified" / "sample.json"
)


def test_instance_selection_preserves_fetched_revision(tmp_path, monkeypatch) -> None:
    adapter = SWEBenchVerifiedAdapter()
    calls = []

    def dataset(locator, subset=None, *, revision=None, cache_dir):
        calls.append(revision)
        row = json.loads(FIXTURE.read_text())[0]
        row["instance_id"] = "pinned-row" if revision == adapter.upstream_source.revision else "head-row"
        return {"test": [row]}

    monkeypatch.setattr("datasets.load_dataset", dataset)
    fetched = fetch_upstream(adapter.upstream_source, cache_root=tmp_path)
    instances = list(adapter.list_instances(source_dir=fetched, split="test"))
    assert [instance.instance_id for instance in instances] == ["pinned-row"]
    assert calls == [adapter.upstream_source.revision, adapter.upstream_source.revision]


def test_convert_emits_valid_task(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter = SWEBenchVerifiedAdapter()
    converted = adapter.convert_instance(inst, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "swe-bench-verified/django__django-12345"
    assert cfg.verifier.name == "pytest"
    assert cfg.required_agent_capabilities == frozenset({"workspace_exec"})
    assert "swebench/sweb.eval.x86_64" in (cfg.environment.docker_image or "")
    assert (tmp_path / "instruction.md").read_text().startswith(
        "Querysets with `.filter",
    )

    solve_sh = (tmp_path / "solution" / "solve.sh").read_text()
    assert "git apply" in solve_sh
    assert "django/db/models/query.py" in solve_sh

    test_py = (tmp_path / "tests" / "test_swebench.py").read_text()
    assert "test_empty_in_short_circuits" in test_py
    assert "test_basic_filter" in test_py
    assert converted.license_spdx == "MIT"


def test_solve_sh_is_executable(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchVerifiedAdapter().convert_instance(inst, out_dir=tmp_path)
    assert (tmp_path / "solution" / "solve.sh").stat().st_mode & 0o111


def test_empty_test_node_lists_emit_script_verifier_reward_zero(
    tmp_path: Path,
) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    rec["FAIL_TO_PASS"] = "[]"
    rec["PASS_TO_PASS"] = "[]"
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchVerifiedAdapter().convert_instance(inst, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args == {"script_path": "/workspace/verifier/run.sh"}
    assert "\n[verifier]\nname = \"script\"\n" in (
        tmp_path / "task.toml"
    ).read_text()

    output = tmp_path / "verifier-output.json"
    subprocess.run(
        [str(tmp_path / "verifier" / "run.sh")],
        env={"LOOM_VERIFIER_OUTPUT": str(output)},
        check=True,
    )
    result = VerifierResult.model_validate_json(output.read_text())
    assert result.rewards["passed"] == 0.0
    assert result.checks[0].message == (
        "no SWE-Bench test node ids were provided by upstream"
    )


def test_image_slug_replaces_double_underscore() -> None:
    from loom_benchmarks.adapters.swe_bench_verified import _image_for
    assert "_1776_" in _image_for("django__django-12345")
    assert "django__" not in _image_for("django__django-12345")
