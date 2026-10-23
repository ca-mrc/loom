"""OracleAgent — deterministic baseline that runs `solution/solve.sh`.

Used in CI to verify the trial harness works end-to-end without LLM
uncertainty, and as a v1 reference for the AgentRuntime contract.

Spec §2.1 (concrete utility), §4.1 (task layout).
"""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.exec import ExecResult
from loom.models.mcp import MCPConnection
from loom.models.trajectory import EnvExecEvent
from loom.models.types import OS, ModelSpec

if TYPE_CHECKING:
    from loom.trajectory.writer import TrajectoryWriter

_SOLVE_SCRIPT_NAME = "solve.sh"


@dataclass
class OracleAgent:
    """Runs `solution/solve.sh` from a task directory inside the sandbox.

    The trial loop materializes the full task bundle into `workdir` before
    invoking the agent; OracleAgent executes that materialized script from
    `workdir/solution` so scripts can resolve sibling files normally.

    Native Harbor callers can stage the trusted solution outside the business
    tree and explicitly run from its image workdir via `solution_dir` and
    `execution_cwd`. Other callers keep the sibling-file execution contract.

    `trial_id` is required so emitted events carry the correct attribution.
    Pass the trial's UUID from the worker's TrialContext; do NOT try to
    derive it from any other source.
    """

    task_dir: Path
    trial_id: UUID
    mode: Literal["out-of-box", "in-box"] = "out-of-box"
    name: str = "oracle"
    version: str = "1.0"
    supports_os: frozenset[OS] = field(default_factory=lambda: frozenset({"linux"}))
    model: ModelSpec | None = None
    workdir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

    solution_dir: PurePosixPath | None = None
    execution_cwd: PurePosixPath | None = None

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None:
        local_solve = self.task_dir / "solution" / _SOLVE_SCRIPT_NAME
        if not local_solve.is_file():
            # Bug 5 fix: raise AgentError so step_runner catches it as a
            # phase=agent StepError (and classify_failure maps to AGENT_ERROR
            # if it ever propagates) — FileNotFoundError used to skip both
            # filters and become INTERNAL_ERROR.
            raise AgentError(
                f"OracleAgent requires {local_solve}; not found",
            )

        solution_dir = self.solution_dir or self.workdir / "solution"
        execution_cwd = self.execution_cwd or solution_dir
        remote_solve = shlex.quote((solution_dir / _SOLVE_SCRIPT_NAME).as_posix())
        cmd = f"chmod +x {remote_solve} && {remote_solve}"
        result: ExecResult = await env.exec(cmd, cwd=execution_cwd)

        await trajectory.append(EnvExecEvent(
            emitted_at=datetime.now(UTC),
            trial_id=self.trial_id,
            step_id=step_id,
            seq=0,
            cmd=cmd,
            user=None,
            cwd=execution_cwd.as_posix(),
            return_code=result.return_code,
            stdout_bytes=len(result.stdout),
            stderr_bytes=len(result.stderr),
            truncated=result.truncated,
            duration_sec=result.duration_sec,
        ))

        if result.return_code != 0:
            raise AgentError(
                f"OracleAgent: solve.sh exited rc={result.return_code}; "
                f"stderr={result.stderr[:512]!r}",
            )
