"""Retained shared release survives guard-release and completion faults."""
from __future__ import annotations

import pytest
from tests.ops.test_nebius_pool_participant_adapter import connection as connection
from tests.ops.test_nebius_pool_participant_delivery import (
    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle as _exercise_release,
)
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)


@pytest.mark.timeout(120)
@pytest.mark.parametrize('recover', ['guard-release', 'release-before-commit', 'completion-prepare', 'completion-ack'])
def test_retained_shared_release_survives_completion_faults(
        operation_inputs, connection, tmp_path, monkeypatch, recover):
    _exercise_release(operation_inputs, connection, tmp_path, monkeypatch, recover=recover, file_writes=True)
