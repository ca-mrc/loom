"""Small multi-owner pool inputs for connected transport contracts.

Roster/rendering tests retain production, staging and development. The HTTP
adapters use the same code for every owner; two independent owners still cover
cross-namespace identity, partial recovery and an already reopened owner. Every
test builds fresh typed inputs, credentials and journals through real fixtures.
"""

import pytest
from tests.ops.test_nebius_pool_runtime import runtime_inputs_for_environments


@pytest.fixture
def runtime_inputs(platform_inputs, management_inputs):
    return runtime_inputs_for_environments(
        platform_inputs, management_inputs, environments=("staging", "development"),
    )
