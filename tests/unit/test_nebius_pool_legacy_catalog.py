"""A retained staging catalog is not development scheduling authority."""
from __future__ import annotations

import pytest

from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.nebius_pool_settings import PoolRuntimeSettings
from tests.unit.test_nebius_pool_contract import participant
from tests.unit.test_nebius_pool_priority import origin


@pytest.mark.parametrize("environment", ["development", "staging", "production"])
def test_default_catalog_binding_preserves_historical_registration_bytes(environment):
    payload = {"schema_version": "loom.pool-participant.v1", **participant(environment_class=environment)}
    value = PoolParticipantV1.model_validate(payload)
    assert value.model_dump(mode="json") == payload
    assert value.catalog_environment == environment


@pytest.mark.parametrize("workload_kind", ["trial", "verifier", "task_image_build"])
def test_legacy_staging_catalog_keeps_staging_priority(workload_kind):
    value = PoolParticipantV1.model_validate(participant(
        environment_class="staging", legacy_catalog_environment="development"))
    assert value.catalog_environment == "development"
    assert value.model_dump(mode="json")["legacy_catalog_environment"] == "development"
    assert pool_request_priority(value, PoolWorkOriginV1.model_validate(origin()), workload_kind=workload_kind) == 1
    with pytest.raises(ValueError, match="pool_origin_scope"):
        pool_request_priority(value, PoolWorkOriginV1.model_validate(origin("application")), workload_kind=workload_kind)


@pytest.mark.parametrize("environment,legacy", [
    ("development", "development"), ("development", "staging"), ("development", "production"),
    ("staging", "staging"), ("staging", "production"),
    ("production", "development"), ("production", "staging"), ("production", "production"),
])
def test_catalog_override_cannot_promote_arbitrary_environment_classes(environment, legacy):
    with pytest.raises(ValueError):
        PoolParticipantV1.model_validate(participant(environment_class=environment, legacy_catalog_environment=legacy))


def test_runtime_requires_exact_registered_legacy_catalog_environment():
    value = PoolParticipantV1.model_validate(participant(
        environment_class="staging", legacy_catalog_environment="development"))
    options = dict(participant=value, environment="development", logical_pool_id="nebius-default",
        management_origin="https://manage.example.com", bearer_token_file="/var/run/loom/pool-token")
    settings = PoolRuntimeSettings.model_validate(options)
    assert settings.participant.environment_class == "staging"
    assert settings.environment == "development"
    with pytest.raises(ValueError):
        PoolRuntimeSettings.model_validate({**options, "environment": "staging"})
    with pytest.raises(ValueError):
        PoolRuntimeSettings.model_validate({**options, "participant": participant(environment_class="staging")})
