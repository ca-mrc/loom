"""MATH adapters for the MATH-500 subset and full test split.

The full adapter uses the public HuggingFaceTB mirror, pinned to a content SHA
and the `all` config, and publishes the official 5,000-row test split. MATH-500
uses the public HuggingFaceH4 500-problem subset with the same answer verifier.
"""

from __future__ import annotations

import json
import textwrap
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import datasets  # type: ignore[import-untyped]

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.scripted_huggingface import load_scripted_source
from loom_benchmarks.util import (
    sha256_of_dir,
    structured_verifier_script,
    toml_string,
)

_CHECK_PY = (
    textwrap.dedent(r'''
    import json
    import os
    import pathlib
    import re


    def extract_boxed_answer(text):
        starts = []
        for command in (r"\boxed", r"\fbox"):
            start = 0
            while True:
                idx = text.find(command, start)
                if idx == -1:
                    break
                starts.append((idx, command))
                start = idx + len(command)
        if not starts:
            return None

        idx, command = max(starts, key=lambda item: item[0])
        pos = idx + len(command)
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text):
            return None
        if text[pos] != "{":
            end = pos
            while end < len(text) and not text[end].isspace():
                end += 1
            return text[pos:end].strip()

        depth = 1
        pos += 1
        start = pos
        while pos < len(text):
            char = text[pos]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start:pos].strip()
            pos += 1
        return None


    def normalize_answer(text):
        text = str(text).strip()
        boxed = extract_boxed_answer(text)
        if boxed is not None:
            text = boxed
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if lines:
            text = lines[-1]
        text = re.sub(r"(?i)^final\s+answer\s*[:：]?\s*", "", text).strip()
        text = text.strip(" $.。")
        replacements = {
            "−": "-",
            r"\left": "",
            r"\right": "",
            r"\!": "",
            r"\,": "",
            r"\;": "",
            r"\:": "",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        text = re.sub(r"\s+", "", text)
        return text


    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "final_answer.txt"))
    )
    answer_key = json.loads((task_dir / "answer_key.json").read_text())
    text = output_path.read_text() if output_path.is_file() else ""
    expected = normalize_answer(answer_key["answer"])
    got = normalize_answer(text)
    passed = got == expected
    score = 1.0 if passed else 0.0
    result = {
        "rewards": {"score": score},
        "checks": [
            {
                "name": "math_answer",
                "passed": passed,
                "score": score,
                "message": f"expected {expected}, got {got or '<none>'}",
            }
        ],
        "structured": {"got": got, "expected": expected},
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(json.dumps(result))
''').strip()
    + "\n"
)

_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


class HendrycksMATHAdapter(CatalogBackedAdapter):
    name = "hendrycks-math"
    answer_field: ClassVar[str | None] = None

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        args: list[str] = [self.upstream_source.locator]
        if self.upstream_source.subset:
            args.append(self.upstream_source.subset)
        if self.upstream_source.trust_remote_code:
            ds = load_scripted_source(self.upstream_source, cache_dir=source_dir)
        else:
            ds = datasets.load_dataset(
                *args,
                revision=self.upstream_source.revision,
                cache_dir=str(source_dir),
            )
        rows = cast(Iterable[dict[str, Any]], ds[split])
        yield from self._instances_from_rows(rows, split=split)

    def _instances_from_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        for idx, record in enumerate(rows):
            rec = dict(record)
            level = str(rec.get("level", "")).strip()
            problem_type = str(rec.get("type") or rec.get("subject") or "").strip()
            unique_id = str(rec.get("unique_id", "")).strip()
            yield BenchmarkInstance(
                instance_id=f"{split}/{idx:05d}",
                split=split,
                raw=rec,
                tags={
                    "math_split": split,
                    **({"level": level} if level else {}),
                    **({"type": problem_type} if problem_type else {}),
                    **({"unique_id": unique_id} if unique_id else {}),
                },
            )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        answer = self._answer_for_instance(instance)
        if answer is None:
            raise ValueError(f"MATH solution has no boxed answer: {instance.instance_id}")

        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            _render_instruction(
                title=self.display_name,
                problem=str(r["problem"]).strip(),
                level=str(r.get("level", "")).strip(),
                problem_type=str(r.get("type") or r.get("subject") or "").strip(),
            ),
        )
        (out_dir / "answer_key.json").write_text(
            json.dumps(
                {
                    "answer": answer,
                    "instance_id": instance.instance_id,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_CHECK_PY)
        structured_verifier_script(_RUN_SH, out_dir=out_dir)

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(
            textwrap.dedent(f"""
            schema_version = "1"

            [task]
            id = {toml_id}
            name = {toml_name}

            [environment]
            os = "linux"
            docker_image = "python:3.11-slim"

            [agent]
            name = "oracle"

            [verifier]
            name = "script"

            [verifier.args]
            script_path = "/workspace/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = ["final_answer.txt"]
        """).strip()
            + "\n",
        )

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )

    def _answer_for_instance(self, instance: BenchmarkInstance) -> str | None:
        if self.answer_field:
            answer = instance.raw.get(self.answer_field)
            if answer is not None:
                return str(answer).strip()
        return _extract_boxed_answer(str(instance.raw["solution"]))


class MATH500Adapter(HendrycksMATHAdapter):
    name = "math-500"
    answer_field = "answer"


def _extract_boxed_answer(text: str) -> str | None:
    starts: list[tuple[int, str]] = []
    for command in (r"\boxed", r"\fbox"):
        start = 0
        while True:
            idx = text.find(command, start)
            if idx == -1:
                break
            starts.append((idx, command))
            start = idx + len(command)
    if not starts:
        return None

    idx, command = max(starts, key=lambda item: item[0])
    pos = idx + len(command)
    while pos < len(text) and text[pos].isspace():
        pos += 1
    if pos >= len(text):
        return None
    if text[pos] != "{":
        end = pos
        while end < len(text) and not text[end].isspace():
            end += 1
        return text[pos:end].strip()

    depth = 1
    pos += 1
    start = pos
    while pos < len(text):
        char = text[pos]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:pos].strip()
        pos += 1
    return None


def _render_instruction(*, title: str, problem: str, level: str, problem_type: str) -> str:
    metadata = []
    if problem_type:
        metadata.append(f"Subject: {problem_type}")
    if level:
        metadata.append(f"Difficulty: {level}")
    metadata_block = "\n".join(metadata)
    if metadata_block:
        metadata_block += "\n\n"
    return (
        f"# {title}\n\n"
        f"{metadata_block}"
        f"{problem}\n\n"
        "Write your reasoning, then write the final answer in final_answer.txt. "
        "Use either `Final answer: ...` or a LaTeX `\\boxed{...}` answer.\n"
    )
