"""Safe current task-image prerequisite diagnostics for authorized trial readers.

Build output is arbitrary user code and can echo unrecognizable credentials.
Project structured observations instead of publishing logs, frozen claims, source
locations or registry references. This is current shared preparation state, not
an immutable history of the requesting trial's execution attempt.
"""

from datetime import datetime
from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_failure import image_preparation_failure

_REASONS = {
    "build_prepare_failed": "Task image source preparation failed. Check that the task source and build context are available.",
    "build_build_failed": "Task image build failed. Check the task Dockerfile and its build inputs.",
    "build_publish_failed": "Task image publication failed. Contact the platform operator to check registry availability.",
    "build_cancelled": "Task image preparation stopped because no active trial requires this build.",
    "build_deadline_exceeded": "Task image preparation exceeded its deadline.",
    "build_oom_killed": "The image build exceeded its memory limit.",
    "build_storage_exceeded": "The image build exceeded its temporary storage limit.",
    "build_job_missing": "The task image build job disappeared before completion.",
    "build_pod_identity_changed": "The task image build stopped after an unexpected container replacement.",
}


def _timestamp(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat() if parsed.tzinfo is not None else None


def _phases(native: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    rows = native.get("phases")
    if not isinstance(rows, list):
        return result
    for name in ("prepare", "build", "publish"):
        row = next(
            (item for item in rows if isinstance(item, dict) and item.get("name") == name), None
        )
        states = row.get("state") if row else None
        if not isinstance(states, dict):
            continue
        for state in ("waiting", "running", "terminated"):
            detail = states.get(state)
            if not isinstance(detail, dict):
                continue
            phase: dict[str, Any] = {"name": name, "state": state}
            code = detail.get("exitCode")
            if state == "terminated" and type(code) is int and 0 <= code <= 255:
                phase["exit_code"] = code
            for source, target in (("startedAt", "started_at"), ("finishedAt", "finished_at")):
                value = _timestamp(detail.get(source))
                if value is not None:
                    phase[target] = value
            result.append(phase)
            break
    return result


def preparation_response(
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt | None,
) -> dict[str, Any]:
    native = {}
    if (
        attempt is not None
        and attempt.materialization_id == row.id
        and attempt.lease_epoch == row.lease_epoch
        and isinstance(attempt.native_build, dict)
    ):
        native = attempt.native_build
    phases = _phases(native)
    reason = row.failure_reason
    typed_failure = image_preparation_failure(reason)
    message = _REASONS.get(reason) if reason is not None else None
    if typed_failure is not None:
        message = typed_failure["message"]
    if reason and message is None:
        reason, message = (
            "build_failed",
            "Task image preparation did not complete. Contact the platform operator for build diagnostics.",
        )
    if reason == "build_build_failed":
        code = next((phase.get("exit_code") for phase in phases if phase["name"] == "build"), None)
        if code is not None:
            message = f"Task image build failed (exit code {code}). Check the task Dockerfile and its build inputs."
    if reason == "build_deadline_exceeded":
        # Job expiry takes precedence: terminating a running wrapper can also
        # yield 124. Never infer a per-command budget from elapsed Pod time.
        if any(c.get("status") == "True" and c.get("reason") == "DeadlineExceeded"
               for c in native.get("job_conditions", [])):
            message = "Image preparation exceeded the platform Job lifecycle deadline (including setup and publication)."
        elif any(p["name"] == "build" and p.get("exit_code") == 124 for p in phases):
            message = "An image build command exceeded the task's build-time budget."
    return {
        "observation_scope": "current_materialization",
        "cpu_arch": row.cpu_arch,
        "state": row.state,
        "attempt_count": row.attempt_count,
        "failure_reason": reason,
        **({"retryable": typed_failure["retryable"], "attribution": typed_failure["attribution"]}
           if typed_failure is not None else {}),
        "message": message,
        "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
        "phases": phases,
        "resources_released": bool(_timestamp(native.get("capacity_released_at")))
        if native
        else None,
    }


async def task_image_preparation_for_trial(
    session: AsyncSession, trial: Trial
) -> list[dict[str, Any]]:
    """Caller must authorize access to the trial before looking up prerequisites."""
    rows = await session.execute(
        select(TaskImageMaterialization, TaskImageMaterializationAttempt)
        .join(
            TrialTaskImageMaterialization,
            TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        )
        .outerjoin(
            TaskImageMaterializationAttempt,
            and_(
                TaskImageMaterializationAttempt.materialization_id == TaskImageMaterialization.id,
                TaskImageMaterializationAttempt.lease_epoch == TaskImageMaterialization.lease_epoch,
            ),
        )
        .where(
            TrialTaskImageMaterialization.trial_id == trial.id,
            TaskImageMaterialization.task_id == trial.task_id,
        )
        .order_by(TaskImageMaterialization.cpu_arch)
    )
    return [preparation_response(row, attempt) for row, attempt in rows]
