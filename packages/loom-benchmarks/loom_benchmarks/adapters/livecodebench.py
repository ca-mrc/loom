"""LiveCodeBench — competitive-programming bench. Spec §5.2 row 10.

Each test case is an (input, output) pair against a stdin-driven
script. We emit one pytest file per case that runs `solution.py` in a
subprocess and compares stdout to the expected output.
"""

from __future__ import annotations

import base64
import json
import pickletools
import textwrap
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.scripted_huggingface import load_scripted_source
from loom_benchmarks.util import (
    oracle_copy_reference_solve_script,
    sha256_of_dir,
    toml_string,
)

_LIVECODEBENCH_STUB_SCRIPT = (
    "import sys\n"
    "sys.stderr.write(\n"
    "    'solution.py not implemented; the agent must overwrite this "
    "file with a real solution.\\n'\n"
    ")\n"
    "sys.exit(1)\n"
)


def _write_noop_solve_script(*, solution_dir: Path) -> Path:
    solution_dir.mkdir(parents=True, exist_ok=True)
    solve_sh = solution_dir / "solve.sh"
    solve_sh.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "# No bundled reference solution exists for this task; leave\n"
        "# solution.py unchanged so the pytest verifier scores the\n"
        "# starter/agent output instead of failing oracle setup.\n",
    )
    solve_sh.chmod(0o755)
    return solve_sh


def _stdin_pytest_case(idx: int, inp: str, expected: str) -> str:
    return textwrap.dedent(f"""
        import subprocess
        import sys
        from pathlib import Path

        SOLUTION = Path(__file__).parent.parent / "solution" / "solution.py"


        def test_lcb_{idx}() -> None:
            result = subprocess.run(
                [sys.executable, str(SOLUTION)],
                input={inp!r}, capture_output=True, text=True, timeout=10,
            )
            assert result.returncode == 0, result.stderr
            assert result.stdout == {expected!r}
    """).strip() + "\n"


def _functional_pytest_case(
    idx: int,
    inp: str,
    expected: str,
    *,
    func_name: str,
) -> str:
    return textwrap.dedent(f"""
        import importlib.util
        import inspect
        import json
        from pathlib import Path

        SOLUTION = Path(__file__).parent.parent / "solution" / "solution.py"


        def _load_solution_class():
            spec = importlib.util.spec_from_file_location("lcb_solution", SOLUTION)
            assert spec is not None
            assert spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.Solution


        def _json_value(raw: str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                lines = [line for line in raw.splitlines() if line.strip()]
                if len(lines) > 1:
                    return [json.loads(line) for line in lines]
                return raw


        def test_lcb_{idx}() -> None:
            solution = _load_solution_class()()
            method = getattr(solution, {func_name!r})
            case_input = _json_value({inp!r})
            expected = _json_value({expected!r})
            param_count = len(inspect.signature(method).parameters)
            if param_count == 1:
                got = method(case_input)
            else:
                assert isinstance(case_input, list)
                got = method(*case_input)
            assert got == expected
    """).strip() + "\n"


def _json_string_from_safe_pickle(data: bytes) -> str | None:
    """Extract a string from the restricted LCB private-case pickle shape.

    Upstream stores private test cases as base64(zlib(pickle(json_string))).
    We do not call `pickle.loads`; only the observed protocol opcodes that
    carry one unicode string are accepted.
    """
    value: str | None = None
    allowed = {"PROTO", "FRAME", "BINUNICODE", "SHORT_BINUNICODE", "MEMOIZE", "STOP"}
    try:
        ops = list(pickletools.genops(data))
    except Exception:
        return None
    for op, arg, _pos in ops:
        if op.name not in allowed:
            return None
        if op.name in {"BINUNICODE", "SHORT_BINUNICODE"}:
            if value is not None or not isinstance(arg, str):
                return None
            value = arg
    if not ops or ops[-1][0].name != "STOP":
        return None
    return value


def _decode_cases(raw: object) -> list[dict[str, object]]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [c for c in raw if isinstance(c, dict)]
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            compressed = base64.b64decode(raw + "=" * (-len(raw) % 4))
            payload = zlib.decompress(compressed)
        except Exception:
            return []
        json_payload = _json_string_from_safe_pickle(payload)
        if json_payload is None:
            return []
        try:
            parsed = json.loads(json_payload)
        except json.JSONDecodeError:
            return []
    return [c for c in parsed if isinstance(c, dict)] if isinstance(parsed, list) else []


class LiveCodeBenchAdapter(CatalogBackedAdapter):
    # CC-BY-NC-4.0 is preserved as upstream metadata; Loom does not use
    # benchmark source licenses to gate research evaluation.
    # The catalog retains the legacy script-source identity; loading is data-only.
    name = "livecodebench"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = load_scripted_source(self.upstream_source, cache_dir=source_dir)[split]
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            yield BenchmarkInstance(
                instance_id=str(rec["question_id"]), split=split, raw=rec,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            f"# LiveCodeBench {instance.instance_id} "
            f"({r.get('difficulty', '?')})\n\n"
            f"{r['question_content']}\n\n"
            f"## Starter\n\n```python\n{r.get('starter_code', '')}```\n",
        )

        sol_dir = out_dir / "solution"
        sol_dir.mkdir(parents=True, exist_ok=True)
        # Two upstream shapes:
        # 1) `code` present → reference solution. Ship it as
        #    `_reference.py` (frozen); `solution.py` is a stub script
        #    that exits non-zero so the pytest subprocess fails unless
        #    the agent overwrites it. Oracle's solve.sh copies the
        #    reference over the stub at trial start.
        # 2) only `starter_code` → no canonical answer to ship as a
        #    silent free pass; the starter goes into `solution.py`
        #    directly so the agent has a function signature to extend.
        reference = r.get("code")
        if reference:
            (sol_dir / "_reference.py").write_text(str(reference))
            (sol_dir / "solution.py").write_text(_LIVECODEBENCH_STUB_SCRIPT)
            oracle_copy_reference_solve_script(solution_dir=sol_dir)
        else:
            (sol_dir / "solution.py").write_text(
                str(r.get("starter_code", "")),
            )
            _write_noop_solve_script(solution_dir=sol_dir)

        tests_dir = out_dir / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)
        metadata = r.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        func_name = str(metadata.get("func_name", ""))
        cases = (
            _decode_cases(r.get("public_test_cases"))
            + _decode_cases(r.get("private_test_cases"))
        )
        for idx, c in enumerate(cases):
            testtype = c.get("testtype")
            if testtype == "functional":
                if not func_name:
                    raise ValueError(
                        "LiveCodeBench functional case missing metadata.func_name "
                        f"for {instance.instance_id}",
                    )
                body = _functional_pytest_case(
                    idx,
                    str(c["input"]),
                    str(c["output"]),
                    func_name=func_name,
                )
            else:
                body = _stdin_pytest_case(idx, str(c["input"]), str(c["output"]))
            (tests_dir / f"test_lcb_{idx}.py").write_text(
                body,
            )

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(textwrap.dedent(f"""
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
            name = "pytest"
            timeout_sec = 420

            [verifier.args]
            install_timeout_sec = 120
            pytest_timeout_sec = 240

            [[steps]]
            name = "main"
            artifacts = ["solution/solution.py"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
