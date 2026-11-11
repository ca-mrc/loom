"""PytestVerifier — runs pytest in the sandbox, parses junit XML output.

Spec §2.4 (Verifier framework), §6.1 (testing tiers).

Output pattern: pytest writes junit XML to /loom/verifier/junit.xml; the
verifier downloads the file and parses it locally. We do NOT parse stdout
— the 10 MB cap could truncate large reports mid-document.

#865 / #867: On every pytest (and failed install) exec, also persist a
capped stdout/stderr audit log under ``{workdir}/.loom/verifier/`` and
attach a bounded ``loom_verifier_audit`` summary to structured.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from xml.etree import ElementTree

from loom.driver.base import Driver
from loom.models.exec import ExecResult
from loom.models.verifier import CheckResult, VerifierError, VerifierResult
from loom.verifier.audit import (
    MAX_VERIFIER_JUNIT_BYTES,
    add_canonical_artifact,
    merge_loom_verifier_audit,
    persist_verifier_audit_log,
    persist_verifier_file,
    workspace_from_task,
)
from loom.verifier.output_dir import prepare_verifier_output_dir

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader

_JUNIT_PATH = PurePosixPath("/loom/verifier/junit.xml")
_PYTEST_LOG_NAME = "pytest.log"
_INSTALL_LOG_NAME = "pytest-install.log"
_JUNIT_NAME = "junit.xml"


def build_pytest_install_command() -> str:
    """Return a shell snippet that ensures pytest is importable.

    Some SWE-Bench eval images ship an older pip that does not know
    `--root-user-action`. Try the quieter modern form first, then fall back
    to a plain install so those images still produce junit output.
    """
    return (
        "python -c 'import pytest' 2>/dev/null || "
        "{ "
        "python -m pip install --quiet --root-user-action=ignore pytest "
        "1>/dev/null 2>/dev/null || "
        "python -m pip install --quiet pytest 1>/dev/null; "
        "}"
    )


@dataclass
class PytestVerifier:
    name: str = "pytest"
    tests_dir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace/tests"),
    )
    pytest_args: tuple[str, ...] = field(
        default_factory=lambda: ("--maxfail=0", "-q"),
    )
    user: str | int | None = None
    install_timeout_sec: float | None = 120.0
    pytest_timeout_sec: float | None = None
    diagnostic_bytes: int = 4096

    def __post_init__(self) -> None:
        # #186: task.toml ships verifier args as JSON-friendly types
        # (str / list). Coerce so the worker can pass `task_config.
        # verifier.args` straight through without an adapter layer.
        if isinstance(self.tests_dir, str):
            self.tests_dir = PurePosixPath(self.tests_dir)
        if isinstance(self.pytest_args, list):
            self.pytest_args = tuple(self.pytest_args)

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        # A driver/workspace may be reused. Remove the prior JUnit file before
        # this attempt so pytest must produce a current report.
        prepare_result = await prepare_verifier_output_dir(
            env, stale_file=_JUNIT_PATH, user=self.user,
        )
        if prepare_result.return_code != 0:
            return VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="exec_failure",
                    message="failed to prepare a clean pytest JUnit path",
                    detail={
                        "junit_xml_path": _JUNIT_PATH.as_posix(),
                        "return_code": prepare_result.return_code,
                    },
                ),
            )
        workspace = workspace_from_task(task, artifacts_dir=artifacts_dir)
        # #186: bare `python:3.11-slim` (HumanEval's default image)
        # doesn't ship pytest. Install on demand if missing — pip is
        # fast-path (~1s) when already installed. Task authors who care
        # about cold-start latency can bake pytest into the image.
        #
        # Keep dependency setup and pytest execution as separate exec phases.
        # If setup fails or times out, that is verifier infrastructure failure.
        # If pytest itself times out, coding-benchmark tasks treat it as a
        # scored model outcome (`passed=0`) with structured diagnostics: bad
        # or hanging generated code should not erase reward coverage.
        install_cmd = build_pytest_install_command()
        try:
            install_result = await env.exec(
                install_cmd,
                user=self.user,
                timeout_sec=self.install_timeout_sec,
            )
        except TimeoutError:
            detail = _timeout_detail(
                phase="install",
                timeout_sec=self.install_timeout_sec,
            )
            return VerifierResult(
                rewards={},
                structured={"install_timeout": detail},
                error=VerifierError(
                    kind="timeout",
                    message=(
                        f"pytest dependency setup exceeded {self.install_timeout_sec:g}s"
                        if self.install_timeout_sec is not None
                        else "pytest dependency setup timed out"
                    ),
                    detail=detail,
                ),
            )
        if install_result.return_code != 0:
            audit = await persist_verifier_audit_log(
                env,
                workspace=workspace,
                exec_result=install_result,
                log_name=_INSTALL_LOG_NAME,
                script_path="pytest-install",
            )
            detail = _exec_detail(
                phase="install",
                command=install_cmd,
                result=install_result,
                diagnostic_bytes=self.diagnostic_bytes,
            )
            return VerifierResult(
                rewards={},
                structured=merge_loom_verifier_audit(
                    {"install_exec": detail},
                    audit,
                ),
                error=VerifierError(
                    kind="exec_failure",
                    message=(
                        "pytest dependency setup failed with return code "
                        f"{install_result.return_code}"
                    ),
                    detail=detail,
                ),
            )

        cmd = (
            f"cd {self.tests_dir.as_posix()} && "
            f"pytest --junitxml={_JUNIT_PATH.as_posix()} " + " ".join(self.pytest_args)
        )
        try:
            pytest_result = await env.exec(
                cmd,
                user=self.user,
                timeout_sec=self.pytest_timeout_sec,
            )
        except TimeoutError:
            detail = _timeout_detail(
                phase="pytest",
                timeout_sec=self.pytest_timeout_sec,
            )
            return VerifierResult(
                rewards={"passed": 0.0, "pytest_pass_rate": 0.0},
                structured={
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "pytest_timeout": detail,
                },
                error=VerifierError(
                    kind="timeout",
                    message=(
                        f"pytest exceeded {self.pytest_timeout_sec:g}s"
                        if self.pytest_timeout_sec is not None
                        else "pytest timed out"
                    ),
                    detail=detail,
                ),
            )

        audit = await persist_verifier_audit_log(
            env,
            workspace=workspace,
            exec_result=pytest_result,
            log_name=_PYTEST_LOG_NAME,
            script_path=cmd,
        )
        pytest_detail = _exec_detail(
            phase="pytest",
            command=cmd,
            result=pytest_result,
            diagnostic_bytes=self.diagnostic_bytes,
        )

        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "junit.xml"
            try:
                await env.download(_JUNIT_PATH, local)
            except FileNotFoundError:
                return VerifierResult(
                    rewards={},
                    structured=merge_loom_verifier_audit(
                        {"pytest_exec": pytest_detail},
                        audit,
                    ),
                    error=VerifierError(
                        kind="missing_tests",
                        message=f"pytest did not produce {_JUNIT_PATH}",
                        detail=pytest_detail,
                    ),
                )
            if await persist_verifier_file(
                env,
                workspace=workspace,
                local_file=local,
                name=_JUNIT_NAME,
                max_bytes=MAX_VERIFIER_JUNIT_BYTES,
            ):
                audit = add_canonical_artifact(
                    audit,
                    relpath=f".loom/verifier/{_JUNIT_NAME}",
                    kind="junit_xml",
                )
            try:
                parsed = parse_junit_xml(local.read_text(encoding="utf-8"))
            except ElementTree.ParseError as exc:
                detail = {
                    **pytest_detail,
                    "parse_error": str(exc),
                }
                return VerifierResult(
                    rewards={},
                    structured=merge_loom_verifier_audit(
                        {"pytest_exec": pytest_detail},
                        audit,
                    ),
                    error=VerifierError(
                        kind="parse_failure",
                        message=f"junit XML parse failed: {exc}",
                        detail=detail,
                    ),
                )

        return VerifierResult(
            rewards={
                "passed": float(parsed["passed"] > 0 and parsed["passed"] == parsed["total"]),
                "pytest_pass_rate": parsed["pass_rate"],
            },
            checks=parsed["checks"],
            structured=merge_loom_verifier_audit(
                {
                    "total": parsed["total"],
                    "passed": parsed["passed"],
                    "failed": parsed["total"] - parsed["passed"],
                },
                audit,
            ),
        )


def parse_junit_xml(xml_text: str) -> dict[str, Any]:
    """Parse junit XML into checks + counts. Pure function for testability."""
    root = ElementTree.fromstring(xml_text)
    checks: list[CheckResult] = []
    total = 0
    passed = 0
    for suite in root.findall(".//testsuite"):
        for case in suite.findall("testcase"):
            total += 1
            failure = case.find("failure")
            error = case.find("error")
            ok = failure is None and error is None
            if ok:
                passed += 1
            message: str | None = None
            if failure is not None:
                message = failure.get("message")
            elif error is not None:
                message = error.get("message")
            checks.append(
                CheckResult(
                    name=case.get("name", "<unnamed>"),
                    passed=ok,
                    score=1.0 if ok else 0.0,
                    message=message,
                    duration_sec=float(case.get("time", 0.0)),
                )
            )
    pass_rate = (passed / total) if total else 0.0
    return {
        "total": total,
        "passed": passed,
        "pass_rate": pass_rate,
        "checks": checks,
    }


def _timeout_detail(
    *,
    phase: str,
    timeout_sec: float | None,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "timeout_sec": timeout_sec,
        "junit_xml_path": _JUNIT_PATH.as_posix(),
    }


def _exec_detail(
    *,
    phase: str,
    command: str,
    result: ExecResult,
    diagnostic_bytes: int,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "command": command,
        "return_code": result.return_code,
        "stdout_tail": _decode_tail(result.stdout, diagnostic_bytes),
        "stderr_tail": _decode_tail(result.stderr, diagnostic_bytes),
        "stdout_bytes": len(result.stdout),
        "stderr_bytes": len(result.stderr),
        "driver_truncated": result.truncated,
        "duration_sec": result.duration_sec,
        "junit_xml_path": _JUNIT_PATH.as_posix(),
    }


def _decode_tail(data: bytes, limit: int) -> str:
    if limit <= 0:
        return ""
    return data[-limit:].decode("utf-8", errors="replace")
