"""Transport policy is frozen without changing historical content identities."""
import pytest
from pydantic import ValidationError

from loom.application_image_build import ApplicationImageBuildClaimV1
from loom_execution_actuator.application_image_runtime import _cache_binding
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


def test_explicit_app_policy_reaches_shared_cache_helpers_without_recipe_invalidation(build_inputs):
    old = build_inputs[0]
    frozen = ApplicationImageBuildClaimV1.model_validate({**old.model_dump(mode="json"),
        "cache_bucket": "cache-fixture", "cache_total_mib": 65536, "cache_gc_policy": "budget-pressure"})
    assert frozen.recipe.digest == old.recipe.digest
    assert frozen.cache_key == old.cache_key
    assert _cache_binding(frozen)["cache_total_mib"] == 65536
    assert _cache_binding(frozen)["cache_gc_policy"] == "budget-pressure"
    assert {"cache_total_mib", "cache_gc_policy"}.isdisjoint(old.model_dump())
    assert {"cache_total_mib", "cache_gc_policy"}.isdisjoint(_cache_binding(old))


@pytest.mark.parametrize("field,value", [("cache_total_mib", True), ("cache_total_mib", 0),
    ("cache_total_mib", None), ("cache_gc_policy", "pressure"), ("cache_gc_policy", None)])
def test_app_policy_is_strict_not_silent_fallback(build_inputs, field, value):
    with pytest.raises(ValidationError):
        ApplicationImageBuildClaimV1.model_validate({**build_inputs[0].model_dump(), field: value})


@pytest.mark.parametrize('field,value', [('cache_total_mib', 65536), ('cache_gc_policy', 'budget-pressure')])
def test_shared_catalog_rejects_app_task_policy_split_but_preserves_distinct_scopes(build_inputs, field, value):
    from loom_service.pool_management.profiles import PoolProfileCatalog
    from tests.integration.test_nebius_pool_installation import (
        add_application_builder,
        installation,
    )

    raw, _ = installation(('development',))
    raw, _, _ = add_application_builder(raw, build_inputs[0].recipe)
    for row in [*raw['profiles']['task_images'], *raw['profiles']['application_images']]:
        row['settings'].update(cache_bucket='shared-cache', cache_secret_name='cache')
    app = raw['profiles']['application_images'][0]['settings']
    app[field] = value
    parsed = PoolProfileCatalog.model_validate(raw['profiles'])
    with pytest.raises(ValueError, match='consistent budget'):
        parsed.validate_cache_policy_consistency()
    scope = (app['storage_endpoint'], app['storage_region'], app['cache_bucket'])
    parsed.validate_cache_policy_consistency({scope: (65536, 'budget-pressure')})
    app['cache_bucket'] = 'independent-cache'
    parsed = PoolProfileCatalog.model_validate(raw['profiles'])
    parsed.validate_cache_policy_consistency()
    assert getattr(parsed.application_images[0].settings, field) == value
