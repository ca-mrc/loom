"""Preview can inherit only the same renewable compiled execution wait."""
import pytest

from loom.db.schema import Trial
from loom_control_plane.execution_capacity_wait import read_execution_capacity_waits
from loom_execution_capacity_collector.contracts import ResourceTotals
from tests.integration.test_execution_capacity_head_wakeup import _successor_wait
from tests.integration.test_nebius_task_image_capacity import native_setup as native_setup
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows as _cleanup_service_execution_test_rows,
)


@pytest.mark.parametrize("matching_runtime", [False, True])
async def test_preview_preserves_queue_age_only_for_same_frozen_runtime(native_setup, matching_runtime):
    sessions, owned = native_setup
    now, _, successor = await _successor_wait(sessions, owned)
    async with sessions() as session:
        trial = await session.get(Trial, successor)
        payload = trial.scheduling_observation["capacity_wait"]
        _, order = await read_execution_capacity_waits(session, now=now,
            preview_trial_id=successor, preview_target_id=payload["target_id"],
            preview_resources=ResourceTotals.model_validate(payload["resources"]),
            preview_runtime_sha256=payload["runtime_contract_sha256"] if matching_runtime else "sha256:" + "9" * 64)
        assert (order is not None) is matching_runtime
