"""Oracle on native execution: the task's reference solution, no model (#2054).

The trusted controller stages `solution/` into Oracle's private task sandbox
only, runs the existing `OracleAgent`, and removes the solution again before
the workspace snapshot and verifier. Model agents never receive it.
"""

from __future__ import annotations

import shlex
from pathlib import Path, PurePosixPath
from typing import Any, cast
from uuid import UUID

from pydantic import TypeAdapter

from loom.agent.oracle import OracleAgent
from loom.attempt_deadline import AttemptDeadline
from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.task import TaskConfig
from loom.models.trajectory import EnvExecEvent, TrajectoryEvent
from loom.models.trial import TrialConfig
from loom.service_execution_terminus2 import _LocalTrajectory

ORACLE_SOLUTION_PATHS = ("solution/**",)
ORACLE_USAGE_SCHEMA = "loom.service-execution-oracle-usage.v1"
_EVENT: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


def oracle_usage() -> dict[str, Any]:
    """Oracle's accounting is known, not unknown: it makes no model calls."""
    return {"schema_version": ORACLE_USAGE_SCHEMA, "model": None, "call_count": 0}


def parse_oracle_events(body: bytes | None, *, trial_id: UUID | None = None) -> list[TrajectoryEvent]:
    """Accept only the solver's own command records, in order, for one Trial."""
    events: list[TrajectoryEvent] = []
    for line in (body or b"").splitlines():
        event = _EVENT.validate_json(line)
        if not isinstance(event, EnvExecEvent):
            raise ValueError("Oracle trace may only record solver execution")
        if event.seq != len(events) or event.step_id != "agent":
            raise ValueError("Oracle trace order or step identity is invalid")
        if trial_id is None:
            trial_id = event.trial_id
        if event.trial_id != trial_id:
            raise ValueError("Oracle trace has another Trial identity")
        events.append(event)
    return events


async def run_oracle(
    *,
    driver: Driver,
    task_dir: Path,
    workspace: Path,
    task_config: TaskConfig,
    trial_config: TrialConfig,
    trial_id: UUID,
    deadline: AttemptDeadline | None = None,
    max_trajectory_bytes: int = 1024 * 1024,
) -> None:
    """Run `solution/solve.sh` in the already-staged sandbox and record it.

    `task_dir` is the controller's immutable task input; the caller has
    already materialized `solution/` into the sandbox workdir.
    """
    if trial_config.agent_name != "oracle" or trial_config.agent_model is not None:
        raise AgentError("Oracle execution requires the oracle agent and no model")
    timeout = (
        trial_config.override_agent_timeout_sec or task_config.agent.timeout_sec
    ) * trial_config.agent_timeout_multiplier
    deadline = deadline or AttemptDeadline.after(timeout)
    workspace.mkdir(parents=True, exist_ok=True)
    events_path = workspace / "trajectory.jsonl"
    with events_path.open("xb"):
        pass
    events_path.chmod(0o600)
    trajectory = _LocalTrajectory(events_path, deadline, max_trajectory_bytes)
    agent = OracleAgent(task_dir=task_dir, trial_id=trial_id, workdir=task_config.environment.workdir)
    await agent.run(
        instruction="", env=driver, trajectory=cast(Any, trajectory), mcp=(), skills_dir=None,
        step_id="agent",
    )


async def remove_oracle_solution(driver: Driver, workdir: PurePosixPath) -> None:
    """The snapshot and an in-place verifier must not see the reference solution."""
    result = await driver.exec("rm -rf -- " + shlex.quote((workdir / "solution").as_posix()), cwd=workdir)
    if result.return_code:
        raise AgentError("Oracle solution could not be removed from the task sandbox")


__all__ = [
    "ORACLE_SOLUTION_PATHS",
    "ORACLE_USAGE_SCHEMA",
    "oracle_usage",
    "parse_oracle_events",
    "remove_oracle_solution",
    "run_oracle",
]
