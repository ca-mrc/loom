"""Submission rejects unsupported image preparation before creating work."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from loom_service.execution_admission import admit_execution_backend
from tests.unit.test_guest_execution_materialization import _guest_inputs
from tests.unit.test_service_execution_materialization import _provenance


async def test_unsupported_final_base_rejected_by_shared_admission(monkeypatch):
    task, trial, profile = _guest_inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "docker_image": None, "dockerfile": "environment/Dockerfile",
    })})
    rows = Mock()
    rows.all.return_value = [("task", task.model_dump(mode="json"), _provenance(), "a" * 64, None,
                             "s3://artifacts/tasks/task/")]
    session = SimpleNamespace(execute=AsyncMock(return_value=rows))
    monkeypatch.setattr("loom_service.execution_admission.get_service_execution_backend_pools",
                        AsyncMock(return_value=[SimpleNamespace(pool_name="nebius-cpu")]))
    reader = AsyncMock(return_value="image_preparation_base_unsupported")
    with pytest.raises(HTTPException) as error:
        await admit_execution_backend(session, backend="nebius", task_ids=["task"],
            trial_config=trial.model_dump(mode="json"), combinations=[],
            runtime_profile_json=profile.model_dump_json(), resolve_versions=False,
            image_preparation_reader=reader)
    assert error.value.status_code == 400
    assert error.value.detail["rejection_reasons"]["task"] == ["image_preparation_base_unsupported"]
    reader.assert_awaited_once()
