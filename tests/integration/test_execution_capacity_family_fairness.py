"""Guest aliases preserve the ordinary native builder's prior waiting authority."""

from datetime import timedelta

import pytest

from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_execution_shared_capacity import _guest_reserve, _register_guest
from tests.integration.test_nebius_task_image_claims import claim_setup as _claim_setup
from tests.integration.test_nebius_task_image_controller import rows
from tests.integration.test_nebius_task_image_fairness import (
    native_build_setup as _native_build_setup,
)
from tests.integration.test_nebius_task_image_fairness import (
    waiting_build as _waiting_build,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)

claim_setup = _claim_setup
native_build_setup = _native_build_setup
waiting_build = _waiting_build


async def test_guest_cannot_overtake_a_waiting_native_builder(waiting_build):
    controller, sessions, _, image_id, _, trial_id, target, now = waiting_build
    async with sessions() as session, session.begin():
        guest = await _register_guest(session, (trial_id, target), now)
        group = await resolve_capacity_targets(session, target.target_id)
        free = placement_fixture(target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1)
        free["target_scope"] = group.scope.model_dump(mode="json")
        await _record(session, target.target_id, now + timedelta(seconds=1), free)
    with pytest.raises(ExecutionProvisioningBlockedError, match="quota_nodes_exceeded"):
        async with sessions() as session, session.begin():
            await _guest_reserve(session, guest, now + timedelta(seconds=2))
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state in {"claimed", "running"}
    assert attempts[0].native_build["capacity_reserved_at"]
