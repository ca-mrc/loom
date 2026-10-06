"""Collector image cancellation: identical shared cases on a separate CI shard."""
import pytest
from tests.ops.test_nebius_pool_image_cancellation import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    checks as checks,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    historical_cutover as historical_cutover,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    image_repair_case as image_repair_case,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    installation as installation,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    material as material,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    prepared_repair as prepared_repair,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    runtime_inputs as runtime_inputs,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    test_cancellation_fences_image_cas_before_successor_shutdown as test_cancellation_fences_image_cas_before_successor_shutdown,
)
from tests.ops.test_nebius_pool_image_cancellation import (
    test_https_cancellation_fences_image_intent_not_completed_source_repair as test_https_cancellation_fences_image_intent_not_completed_source_repair,
)


@pytest.fixture(params=["collector"])
def target(request):
    return request.param
