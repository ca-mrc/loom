"""A closed development successor keeps application builds and personal releases coherent."""
from __future__ import annotations

import copy
import importlib
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_activation import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_activation import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_activation import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_activation import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_activation import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_activation import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_activation import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_activation import (
    connected as connected,
)
from tests.ops.test_nebius_development_activation import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_activation import (
    entry as entry,
)
from tests.ops.test_nebius_development_activation import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_activation import (
    installation as installation,
)
from tests.ops.test_nebius_development_activation import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_activation import (
    live as live,
)
from tests.ops.test_nebius_development_activation import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_activation import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_activation import (
    material as material,
)
from tests.ops.test_nebius_development_activation import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_activation import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_activation import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_activation import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_activation import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_activation import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_activation import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_activation import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_activation import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_activation import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_activation import (
    publication as publication,
)
from tests.ops.test_nebius_development_activation import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_activation import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_activation import (
    retained as retained,
)
from tests.ops.test_nebius_development_activation import (
    route as route,
)
from tests.ops.test_nebius_development_activation import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_activation import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_development_pool_retained import (
    RuntimeParentAPI,
    foundation_publication,
    runtime_install_request,
)

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True),
    pytest.mark.parametrize('published_source', ['0175'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


def next_publication(previous, *, revision='0178', seed='b'):
    """Only fixture publication metadata; remote approval remains the parent's job."""
    from loom_service.environment_management.manager import CandidateBundle
    candidate, profile = copy.deepcopy(previous.bundle.candidate), copy.deepcopy(previous.bundle.profile)
    candidate.update(candidate_sha=seed * 40, source_archive_sha256='sha256:' + seed * 64,
        service_schema_head=revision)
    profile['candidate_sha'] = candidate['candidate_sha']
    selected = previous.publication.model_copy(update={'candidate_id': uuid4(), 'source_sha': candidate['candidate_sha']})
    source = previous.source.model_copy(update={'source_sha': candidate['candidate_sha'],
        'source_archive_sha256': candidate['source_archive_sha256']})
    return replace(previous, source=source, publication=selected,
        bundle=CandidateBundle(selected.candidate_id, candidate, profile))


@pytest.fixture
def successor_inputs(completed_pool, publisher_cloud, published_source):
    from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
    runtime, request = runtime_install_request(completed_pool, publisher_cloud,
        data_publication=foundation_publication(completed_pool))
    plan = runtime.prepare_runtime_install(request)
    api = RuntimeParentAPI(request, plan)
    api.database_complete = api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    completed = load_completed_runtime(request)
    return dict(runtime=completed, operation_id=uuid4(),
        data_publication=next_publication(request.database.data_runtime),
        manager_publication=next_publication(request.database.manager.publication, seed='c'))


def project(**inputs):
    name = 'scripts.ops.nebius_development_schema_projection'
    assert importlib.util.find_spec(name), 'closed development schema projection is missing'
    return importlib.import_module(name).project_development_schema_successor(**inputs)


def test_actual_split_schema_builder_becomes_one_coherent_successor(successor_inputs):
    original = copy.deepcopy(successor_inputs)
    result = project(**successor_inputs)
    assert successor_inputs == original
    assert result.before_manager.installation.applications.shared.schema_revision == '0175'
    assert result.manager_publication != result.data_publication
    assert result.after_pool.policy_revision == result.before_pool.policy_revision + 1
    assert result.participant.binding_revision == result.before_pool.participants[0].binding_revision + 1
    application = result.after_manager.installation.applications
    assert application.shared.schema_revision == '0178'
    binding = application.runtime.build.binding
    profile, = result.after_pool.profiles.application_images
    assert binding.recipe == profile.recipe and binding.profile_id == profile.profile_id
    assert binding.participant_revision == result.participant.binding_revision
    assert binding.recipe.schema_revision == '0178'
    assert result.after_manager.pool_catalog_operation_id == result.after_pool.operation_id
    release, = application.releases
    assert release.schema_revision == '0178'
    assert release.release_id not in {row.release_id for row in result.before_manager.installation.applications.releases}
    assert result.after_pool.machines == result.before_pool.machines
    assert all(path.read_bytes() == raw for path, raw in successor_inputs['runtime'].files.items())
    assert project(**successor_inputs) == result


@pytest.mark.parametrize('damage', ['same-schema', 'publication-source', 'runtime-capability', 'builder-binding'])
def test_projection_rejects_incoherent_source_and_old_builder(successor_inputs, damage):
    if damage == 'same-schema':
        successor_inputs['data_publication'].bundle.candidate['service_schema_head'] = '0175'
    elif damage == 'publication-source':
        successor_inputs['data_publication'].bundle.candidate['candidate_sha'] = 'd' * 40
    elif damage == 'runtime-capability':
        profile = successor_inputs['data_publication'].bundle.profile
        profile['supports_task_identity'] = not profile.get('supports_task_identity', False)
    else:
        manager = successor_inputs['runtime'].request.database.manager
        application = manager.deployment.installation.applications
        damaged = application.runtime.build.binding.model_copy(update={'participant_revision': 99})
        application.runtime.build.__dict__['binding'] = damaged
    with pytest.raises(ValueError):
        project(**successor_inputs)
