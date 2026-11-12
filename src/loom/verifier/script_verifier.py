"""ScriptVerifier — runs an arbitrary script, reads $LOOM_VERIFIER_OUTPUT JSON.

Contract: the script writes a JSON object {rewards, checks, structured?,
confidence?} to the path in `LOOM_VERIFIER_OUTPUT`. We then download +
parse it. Missing file or invalid JSON surfaces as VerifierResult.error.

#865 / #867: On every verify attempt, persist a capped copy of the shim
stdout/stderr under ``{workdir}/.loom/verifier/`` via the shared audit
channel, and attach a bounded ``loom_verifier_audit`` summary to
``VerifierResult.structured``.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal

from loom.driver.base import Driver
from loom.models.exec import ExecResult
from loom.models.verifier import CheckResult, VerifierError, VerifierResult
from loom.verifier.audit import (
    MAX_VERIFIER_LOG_BYTES,
    MAX_VERIFIER_OUTPUT_BYTES,
    VERIFIER_LOG_TRUNCATION_MARKER,
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

_OUTPUT_PATH = PurePosixPath("/loom/verifier/output.json")
_DIAGNOSTIC_TAIL_BYTES = 4096
# Back-compat aliases for tests / callers that imported PR1 constants.
_VERIFIER_LOG_NAME = "script.log"
_VERIFIER_META_NAME = "script.log.meta.json"
_VERIFIER_OUTPUT_NAME = "output.json"
_VERIFIER_LOG_TRUNCATION_MARKER = VERIFIER_LOG_TRUNCATION_MARKER


@dataclass
class ScriptVerifier:
    script_path: PurePosixPath
    name: str = "script"
    user: str | int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.script_path, str):
            self.script_path = PurePosixPath(self.script_path)

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        # A driver/workspace may be reused. Remove the prior scoring file
        # before this attempt so a script that writes nothing cannot inherit
        # a previous step's result.
        prepare_result = await prepare_verifier_output_dir(
            env, stale_file=_OUTPUT_PATH, user=self.user,
        )
        if prepare_result.return_code != 0:
            return VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="exec_failure",
                    message="failed to prepare a clean verifier output path",
                    detail={
                        "output_path": _OUTPUT_PATH.as_posix(),
                        "return_code": prepare_result.return_code,
                    },
                ),
            )
        cmd = f"sh {self.script_path.as_posix()}"
        # Standard task-context env vars for verifier scripts:
        # - LOOM_TASK_DIR: where the bundle was materialized (DockerDriver
        #   defaults to /workspace; other drivers may differ). In normal
        #   trial execution this comes from the task's configured workdir;
        #   `artifacts_dir.parent` remains only a compatibility fallback
        #   for direct verifier calls that do not provide a task.
        # - LOOM_AGENT_OUTPUT: if the task's first step declares exactly
        #   one artifact that is a bare file path (no glob metachars),
        #   resolve it against the workspace. Scripts that grade a single
        #   agent-produced answer file (AIME, GAIA, etc.) can rely on
        #   this without inventing their own convention. Multi-artifact
        #   or glob-based tasks (HumanEval, SWE-Bench) get their pytest
        #   verifier and don't need this variable set. See #688.
        # - LOOM_VERIFIER_OUTPUT: the path the script must write its
        #   result JSON to (unchanged; existing contract).
        workspace = workspace_from_task(task, artifacts_dir=artifacts_dir)
        script_env = {
            "LOOM_TASK_DIR": workspace,
            "LOOM_VERIFIER_OUTPUT": _OUTPUT_PATH.as_posix(),
        }
        agent_output = _agent_output_path(task, workspace=workspace)
        if agent_output is not None:
            script_env["LOOM_AGENT_OUTPUT"] = agent_output
        exec_result = await env.exec(
            cmd,
            user=self.user,
            env=script_env,
        )
        diagnostic = _exec_diagnostic(
            exec_result=exec_result,
            output_path=_OUTPUT_PATH,
            script_path=self.script_path,
        )
        audit = await persist_verifier_audit_log(
            env,
            workspace=workspace,
            exec_result=exec_result,
            log_name=_VERIFIER_LOG_NAME,
            script_path=self.script_path.as_posix(),
        )

        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "output.json"
            try:
                await env.download(_OUTPUT_PATH, local)
            except FileNotFoundError:
                # #380: enrich the diagnostic with the post-mortem state
                # of the output directory so an operator can tell script
                # bug vs. permission bug vs. env-var bug apart without
                # rerunning. Runs a single non-mutating exec.
                diagnostic = {
                    **diagnostic,
                    **await _output_dir_post_mortem(env, _OUTPUT_PATH),
                }
                kind: Literal["exec_failure", "missing_output"] = (
                    "exec_failure" if exec_result.return_code != 0 else "missing_output"
                )
                return VerifierResult(
                    rewards={},
                    structured=merge_loom_verifier_audit(None, audit),
                    error=VerifierError(
                        kind=kind,
                        message=f"script did not write {_OUTPUT_PATH}",
                        detail=diagnostic,
                    ),
                )
            if await persist_verifier_file(
                env,
                workspace=workspace,
                local_file=local,
                name=_VERIFIER_OUTPUT_NAME,
                max_bytes=MAX_VERIFIER_OUTPUT_BYTES,
            ):
                audit = add_canonical_artifact(
                    audit,
                    relpath=f".loom/verifier/{_VERIFIER_OUTPUT_NAME}",
                    kind="scoring_json",
                )
            try:
                data = json.loads(local.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                return VerifierResult(
                    rewards={},
                    structured=merge_loom_verifier_audit(None, audit),
                    error=VerifierError(
                        kind="parse_failure",
                        message=f"$LOOM_VERIFIER_OUTPUT json parse failed: {exc}",
                        detail=diagnostic,
                    ),
                )

        shim_structured = data.get("structured")
        if shim_structured is not None and not isinstance(shim_structured, dict):
            return VerifierResult(
                rewards={},
                structured=merge_loom_verifier_audit(None, audit),
                error=VerifierError(
                    kind="parse_failure",
                    message="$LOOM_VERIFIER_OUTPUT structured must be an object",
                    detail=diagnostic,
                ),
            )
        return VerifierResult(
            rewards=dict(data.get("rewards", {})),
            checks=[CheckResult(**c) for c in data.get("checks", [])],
            structured=merge_loom_verifier_audit(
                shim_structured if isinstance(shim_structured, dict) else None,
                audit,
            ),
            confidence=data.get("confidence"),
        )


def _agent_output_path(task: TaskConfig | None, *, workspace: str) -> str | None:
    """Resolve LOOM_AGENT_OUTPUT for the given task, or return None.

    Set only when the task's first step declares exactly one artifact
    that is a plain relative file path (no `*`, `?`, `[` globbing).
    Multi-artifact or glob-based tasks don't get a single output
    variable — those verifiers should walk `LOOM_TASK_DIR` themselves."""
    if task is None or not task.steps:
        return None
    artifacts = task.steps[0].artifacts
    if len(artifacts) != 1:
        return None
    artifact = artifacts[0]
    if any(c in artifact for c in "*?["):
        return None
    return f"{workspace.rstrip('/')}/{artifact.lstrip('/')}"


async def _output_dir_post_mortem(
    env: Driver,
    output_path: PurePosixPath,
) -> dict[str, object]:
    """Return a small dict describing the state of ``output_path.parent``.

    Used only in the missing-output failure path (#380) to help operators
    distinguish a script-side bug (wrote nothing) from a permission bug
    (dir wasn't writable) or an env-var bug (script wrote to a different
    path) without a repro run. Single, non-mutating exec.
    """
    output_dir = output_path.parent.as_posix()
    probe = (
        # `stat` prints one line per requested field; `ls -la` gives us
        # the sibling listing so the operator sees what DID land in the
        # dir (e.g. an output.json.tmp the script forgot to rename).
        f"echo -- MODE ; stat -c %a {output_dir!s} 2>/dev/null || echo MISSING ; "
        f"echo -- OWNER ; stat -c %U:%G {output_dir!s} 2>/dev/null || echo MISSING ; "
        f"echo -- LISTING ; ls -la {output_dir!s} 2>&1 || true"
    )
    try:
        probe_result = await env.exec(probe, user="root")
    except Exception as exc:  # pragma: no cover - defensive
        return {"output_dir_post_mortem_error": repr(exc)}
    text = probe_result.stdout.decode("utf-8", errors="replace")
    return {
        "output_dir": output_dir,
        "output_dir_probe": text,
        "output_dir_probe_return_code": probe_result.return_code,
    }


def _exec_diagnostic(
    *,
    exec_result: ExecResult,
    output_path: PurePosixPath,
    script_path: PurePosixPath,
) -> dict[str, object]:
    return {
        "return_code": exec_result.return_code,
        "stdout_tail": _decode_tail(exec_result.stdout),
        "stderr_tail": _decode_tail(exec_result.stderr),
        "truncated": exec_result.truncated,
        "duration_sec": exec_result.duration_sec,
        "output_path": output_path.as_posix(),
        "script_path": script_path.as_posix(),
    }


def _decode_tail(data: bytes) -> str:
    tail = data[-_DIAGNOSTIC_TAIL_BYTES:]
    return tail.decode("utf-8", errors="replace")


# Re-exports used by unit tests that previously imported PR1 helpers.
__all__ = [
    "MAX_VERIFIER_LOG_BYTES",
    "_VERIFIER_LOG_NAME",
    "_VERIFIER_LOG_TRUNCATION_MARKER",
    "_VERIFIER_META_NAME",
    "ScriptVerifier",
]
