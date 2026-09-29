"""Team-scoped TaskSet visibility helper (#242 sub-plan 1)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import Select, and_, false, or_, select

from loom.db.schema import Task, TaskSet


def visible_task_sets(*, team_id: UUID | None) -> Select[tuple[TaskSet]]:
    """Return TaskSets visible to the given team.

    v1 is team-private: only non-soft-deleted rows owned by ``team_id``
    with ``visibility='private'`` are visible. ``team_id=None`` yields an
    empty result set.
    """
    if team_id is None:
        return select(TaskSet).where(false())
    return select(TaskSet).where(
        and_(
            TaskSet.owning_team_id == team_id,
            TaskSet.visibility == "private",
            TaskSet.soft_deleted_at.is_(None),
        )
    )


def visible_tasks(*, team_id: UUID | None, lock_task_sets: bool = False) -> Select[tuple[Task]]:
    """Return tasks visible to the given team.

    Global tasks have no TaskSet. TaskSet-backed tasks are visible only
    through the submitting team's visible TaskSets.
    """
    visible_task_set_ids = visible_task_sets(team_id=team_id).with_only_columns(
        TaskSet.id,
    )
    if lock_task_sets:
        # Admission holds share locks until its Batch/Trial commits. Expiry
        # takes an update lock and rechecks work after admission has committed.
        visible_task_set_ids = visible_task_set_ids.with_for_update(read=True)
    return select(Task).where(
        or_(
            Task.task_set_id.is_(None),
            Task.task_set_id.in_(visible_task_set_ids),
        ),
    )
