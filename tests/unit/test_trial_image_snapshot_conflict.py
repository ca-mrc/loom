"""Deterministic image conflicts must be distinguishable from transient failures."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.db.schema import Task
from loom.task_image_materialization import TaskImageSnapshotConflictError
from loom_control_plane.routes import trials


@pytest.mark.parametrize(
    "message",
    [
        "task image materialization identity conflict",
        "task image materialization snapshot conflicts with task checksum",
        "frozen content-manifest snapshot conflicts with existing materialization",
    ],
)
async def test_snapshot_conflict_has_non_retryable_http_detail(monkeypatch, message):
    session = AsyncMock()
    session.scalar.return_value = None
    conflict = TaskImageSnapshotConflictError(message)
    ensure = AsyncMock(side_effect=conflict)
    monkeypatch.setattr(trials, "ensure_task_image_materializations", ensure)
    task = Task(id="snapshot-task", checksum="a" * 64, config={})

    with pytest.raises(HTTPException) as caught:
        await trials._ensure_trial_task_image_links(session, trial_id=uuid4(), task_row=task)

    assert isinstance(conflict, RuntimeError)
    assert caught.value.status_code == 409
    assert caught.value.detail == {
        "reason": "task_image_snapshot_conflict",
        "task_id": task.id,
        "message": message,
    }
    assert caught.value.__cause__ is conflict
    assert session.execute.await_count == 1  # Only the Trial lock; no links written.


@pytest.mark.parametrize("error", [RuntimeError("database unavailable"), ValueError("bad digest")])
async def test_unrelated_materialization_errors_are_not_reclassified(monkeypatch, error):
    session = AsyncMock()
    session.scalar.return_value = None
    monkeypatch.setattr(trials, "ensure_task_image_materializations", AsyncMock(side_effect=error))
    task = Task(id="snapshot-task", checksum="a" * 64, config={})

    with pytest.raises(type(error)) as caught:
        await trials._ensure_trial_task_image_links(session, trial_id=uuid4(), task_row=task)

    assert caught.value is error
