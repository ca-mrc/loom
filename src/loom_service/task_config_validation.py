"""TaskConfig validation helpers shared by batch creation and fan-out."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Task
from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig


@dataclass(frozen=True)
class InvalidTaskConfig:
    task_id: str
    detail: str


async def split_valid_task_configs(
    session: AsyncSession,
    task_ids: Sequence[str],
) -> tuple[list[str], list[InvalidTaskConfig]]:
    """Return task ids whose stored config validates as TaskConfig.

    The caller owns the policy decision: API creation rejects invalid
    configs, while the runner filters legacy bad rows so existing
    batches can reach a diagnosable terminal/progress state.
    """
    if not task_ids:
        return [], []

    rows = (
        await session.execute(
            select(Task.id, Task.config).where(Task.id.in_(task_ids)),
        )
    ).all()
    configs = {str(task_id): config for task_id, config in rows}

    valid: list[str] = []
    invalid: list[InvalidTaskConfig] = []
    for task_id in task_ids:
        try:
            config = TaskConfig.model_validate(configs[task_id])
            execution_cpu_arch(config.environment.cpu_arch)
        except ValueError as exc:
            invalid.append(
                InvalidTaskConfig(
                    task_id=task_id,
                    detail=str(exc),
                )
            )
        else:
            valid.append(task_id)
    return valid, invalid


def invalid_task_config_detail(invalid: Sequence[InvalidTaskConfig]) -> str:
    task_list = ", ".join(item.task_id for item in invalid)
    first = invalid[0].detail if invalid else "unknown validation error"
    return f"invalid task config for {task_list}: {first}"


def expected_trial_count(
    *,
    task_count: int,
    n_per_task: int,
    combinations: Sequence[Mapping[str, Any]] | None,
) -> int:
    if combinations:
        return sum(task_count * int(combo.get("n_per_task", 1)) for combo in combinations)
    return task_count * n_per_task
