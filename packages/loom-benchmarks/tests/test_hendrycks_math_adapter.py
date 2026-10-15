from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.hendrycks_math import (
    HendrycksMATHAdapter,
    MATH500Adapter,
    _extract_boxed_answer,
)

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


class _FakeSplit:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


def test_math500_lists_public_500_problem_test_split(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_load_dataset(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {
            "test": _FakeSplit(
                [
                    {
                        "problem": "Convert to polar coordinates.",
                        "level": 2,
                        "subject": "Precalculus",
                        "solution": "Therefore \\boxed{\\left(3,\\frac{\\pi}{2}\\right)}.",
                        "answer": "\\left(3,\\frac{\\pi}{2}\\right)",
                        "unique_id": "test/precalculus/807.json",
                    },
                    {
                        "problem": "Find x.",
                        "level": 4,
                        "subject": "Algebra",
                        "solution": "Thus \\boxed{7}.",
                        "answer": "7",
                        "unique_id": "test/algebra/123.json",
                    },
                ],
            ),
        }

    monkeypatch.setattr(
        "loom_benchmarks.adapters.hendrycks_math.datasets.load_dataset",
        fake_load_dataset,
    )

    instances = list(
        MATH500Adapter().list_instances(source_dir=Path("/cache"), split="test"),
    )

    assert calls == [
        {
            "args": ("HuggingFaceH4/MATH-500",),
            "kwargs": {
                "revision": "6e4ed1a2a79af7d8630a6b768ec859cb5af4d3be",
                "cache_dir": "/cache",
            },
        },
    ]
    assert [i.instance_id for i in instances] == ["test/00000", "test/00001"]
    assert instances[0].tags == {
        "math_split": "test",
        "level": "2",
        "type": "Precalculus",
        "unique_id": "test/precalculus/807.json",
    }


def test_hendrycks_math_extracts_nested_boxed_answer() -> None:
    assert _extract_boxed_answer(r"So $a+b=\boxed{\frac{26}{3}}$.") == r"\frac{26}{3}"
    assert _extract_boxed_answer(r"First \boxed{1}, finally \boxed{2}") == "2"


def test_hendrycks_math_convert_writes_valid_task(tmp_path: Path) -> None:
    instance = next(
        HendrycksMATHAdapter()._instances_from_rows(
            [
                {
                    "problem": "How many vertical asymptotes?",
                    "level": "Level 3",
                    "type": "Algebra",
                    "solution": "The answer is \\boxed{2}.",
                },
            ],
            split="test",
        ),
    )

    converted = HendrycksMATHAdapter().convert_instance(instance, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(tomllib.loads((tmp_path / "task.toml").read_text()))
    assert converted.task_id == "hendrycks-math/test/00000"
    assert cfg.task.id == "hendrycks-math/test/00000"
    assert cfg.environment.docker_image == "python:3.11-slim"
    assert cfg.verifier.args["script_path"] == "/workspace/verifier/run.sh"
    assert cfg.steps[0].artifacts == ["final_answer.txt"]
    assert "How many vertical asymptotes?" in (tmp_path / "instruction.md").read_text()
    assert json.loads((tmp_path / "answer_key.json").read_text()) == {
        "answer": "2",
        "instance_id": "test/00000",
    }


def test_math500_convert_writes_math500_task_id(tmp_path: Path) -> None:
    instance = next(
        MATH500Adapter()._instances_from_rows(
            [
                {
                    "problem": "Convert to polar coordinates.",
                    "level": 2,
                    "subject": "Precalculus",
                    "solution": "Therefore \\boxed{\\left(3,\\frac{\\pi}{2}\\right)}.",
                    "answer": "\\left(3,\\frac{\\pi}{2}\\right)",
                    "unique_id": "test/precalculus/807.json",
                },
            ],
            split="test",
        ),
    )

    converted = MATH500Adapter().convert_instance(instance, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(tomllib.loads((tmp_path / "task.toml").read_text()))
    assert converted.task_id == "math-500/test/00000"
    assert cfg.task.id == "math-500/test/00000"
    assert "# MATH-500" in (tmp_path / "instruction.md").read_text()
    assert json.loads((tmp_path / "answer_key.json").read_text()) == {
        "answer": "\\left(3,\\frac{\\pi}{2}\\right)",
        "instance_id": "test/00000",
    }


def test_hendrycks_math_verifier_accepts_equivalent_boxed_output(
    tmp_path: Path,
) -> None:
    instance = next(
        HendrycksMATHAdapter()._instances_from_rows(
            [
                {
                    "problem": "Compute $1/3$.",
                    "level": "Level 1",
                    "type": "Prealgebra",
                    "solution": "Therefore \\boxed{\\frac{1}{3}}.",
                },
            ],
            split="test",
        ),
    )
    HendrycksMATHAdapter().convert_instance(instance, out_dir=tmp_path)
    (tmp_path / "final_answer.txt").write_text("Reasoning...\nFinal answer: $\\frac{1}{3}$\n")
    verifier_out = tmp_path / "verifier-output.json"

    env = dict(os.environ)
    env["PYTHON"] = sys.executable
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)
    result = subprocess.run(
        ["sh", str(tmp_path / "verifier" / "run.sh")],
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].passed is True
