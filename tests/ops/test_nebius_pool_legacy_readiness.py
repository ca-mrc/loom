"""Runtime readiness needs the database-bound retained restart fixture."""
import pytest
from tests.ops.test_nebius_pool_legacy_restart import legacy_runtime_readiness_case
from tests.ops.test_nebius_pool_startup_live import closed_startup as closed_startup
from tests.ops.test_nebius_pool_startup_live import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup_live import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup_live import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup_live import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup_live import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup_live import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup_live import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_startup_live import (
    unbound_cutover_inputs as unbound_cutover_inputs,
)


@pytest.mark.timeout(600)
def test_legacy_runtime_readiness_binds_completed_restart_and_rechecks_closed_authority(closed_startup):
    # Reuse the renderer/journal scenario, but include actual retained database
    # identities; the closed-restart-only tests deliberately omit those bindings.
    legacy_runtime_readiness_case(closed_startup)
