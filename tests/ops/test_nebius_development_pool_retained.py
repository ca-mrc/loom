"""Runtime successors consume closed pool evidence without replaying installation."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from pathlib import Path
from uuid import UUID

import pytest
from tests.ops.test_nebius_development_pool_install import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_pool_install import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_pool_install import (
    connected as connected,
)
from tests.ops.test_nebius_development_pool_install import (
    installation as installation,
)
from tests.ops.test_nebius_development_pool_install import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_pool_install import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    manager_entry as original_manager_entry,  # noqa: F401
)
from tests.ops.test_nebius_development_pool_install import (
    material as material,
)
from tests.ops.test_nebius_development_pool_install import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_pool_install import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_pool_install import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_pool_install import (
    retained as retained,
)
from tests.ops.test_nebius_development_pool_install import (
    route as route,
)
from tests.ops.test_nebius_development_pool_install import (
    tls_material as tls_material,
)


def module():
    name = 'scripts.ops.nebius_development_pool_retained'
    if importlib.util.find_spec(name) is None:
        pytest.fail('read-only completed development pool loader is missing')
    return importlib.import_module(name)


@pytest.fixture
def manager_entry(request):
    original = request.getfixturevalue('original_manager_entry')
    if getattr(request, 'param', False):
        from tests.ops.test_nebius_development_source_intake import source_inputs

        return source_inputs(original)
    return original


@pytest.fixture
def completed_pool(pool_entry, capsys):
    entry, operation, inputs, path, connected = pool_entry
    connected.server.complete = True
    assert entry.main(str(path), 'install') == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'development_pool_installed_closed'
    return operation, inputs, path, connected


def reference(loader, completed_pool):
    path = completed_pool[2]
    return loader.RetainedDevelopmentPoolReference(operation_path=path,
        operation_sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def publication_inputs(completed_pool):
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource

    from loom_service.environment_management.candidates import ProtectedPublication
    from loom_service.environment_management.manager import CandidateBundle

    inputs = completed_pool[3].server.request.retained.inputs
    candidate, profile = copy.deepcopy(inputs.candidate), copy.deepcopy(inputs.profile)
    candidate.update(candidate_sha='e' * 40, source_archive_sha256='sha256:' + '5' * 64, run_id=12345)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '6' * 64
    profile.update(candidate_sha=candidate['candidate_sha'], task_image_ref=candidate['images']['service']['image_ref'])
    source = PreparedDevelopmentSource(source_sha=candidate['candidate_sha'], source_archive_sha256=candidate['source_archive_sha256'])
    selected = ProtectedPublication(candidate_id=UUID('47f27bd6-bfd9-4ae8-a585-82916a848c85'),
        source_sha=source.source_sha, run_id=12345, run_attempt=1, artifact_id=23456,
        artifact_sha256='sha256:' + '7' * 64, pull_request=2399)
    return source, selected, CandidateBundle(selected.candidate_id, candidate, profile)


def runtime_publication(completed_pool):
    from scripts.ops import nebius_development_runtime_render as runtime

    if not hasattr(runtime, 'DevelopmentRuntimePublication'):
        pytest.fail('source-bound development runtime publication is missing')
    return runtime.DevelopmentRuntimePublication(*publication_inputs(completed_pool))


def test_completed_pool_load_is_readonly_and_keeps_actual_identities(completed_pool, monkeypatch):
    from scripts.ops import nebius_development_pool_install, nebius_development_pool_intent

    loader = module()
    operation, _, _, connected = completed_pool
    before = len(connected.server.calls)
    monkeypatch.setattr(nebius_development_pool_install, 'install_development_pool',
        lambda **_: pytest.fail('installer replayed'))
    for name in ('namespace_documents', 'delivery_documents'):
        monkeypatch.setattr(nebius_development_pool_intent, name,
            lambda *_: pytest.fail('historical runtime rerendered'))
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.operation == operation
    participant, = value.request.registration.spec.participants
    for namespace in (participant.execution_namespace, participant.build_namespace):
        assert str(namespace.uid) == connected.server.namespaces[namespace.name]['metadata']['uid']
    deployment, = [row for row in value.resources.values() if row['kind'] == 'Deployment']
    assert deployment['spec']['replicas'] == 0
    assert len(connected.server.calls) == before
    assert all(path.read_bytes() == raw for path, raw in value.files.items())
    assert not any(token in repr(value) for token in connected.intent.tokens.values())


def test_retired_operator_files_do_not_invalidate_completed_pool(completed_pool):
    loader = module()
    _, inputs, _, connected = completed_pool
    for name in ('ca_file', 'credentials_file'):
        Path(inputs['operator_connection'][name]).unlink()
    value = loader.load_retained_pool(reference(loader, completed_pool))
    assert value.request.registration.spec.pool_id == connected.server.request.registration.spec.pool_id


@pytest.mark.parametrize('manager_entry', [False, True], indirect=True, ids=['new-source', 'retained-source'])
def test_manager_runtime_consumes_closed_catalog_and_retains_source_identity(completed_pool):
    name = 'scripts.ops.nebius_development_runtime_render'
    if importlib.util.find_spec(name) is None:
        pytest.fail('retained development manager runtime preparation is missing')
    publication = runtime_publication(completed_pool)
    prepared = importlib.import_module(name).prepare_manager_runtime(reference(module(), completed_pool), publication=publication)
    retained = prepared.retained
    spec = retained.request.registration.spec
    before = retained.request.retained.inputs.deployment
    assert prepared.deployment.pool_catalog_operation_id == spec.operation_id
    builder, = [row for row in spec.machines if row.workload_scope == 'application_builder']
    assert prepared.deployment.application_builder_machine_id == builder.machine_id
    assert prepared.delivery.deployment['spec']['replicas'] == 0
    assert prepared.delivery.deployment['spec']['template']['spec']['containers'][0]['image'] == publication.bundle.candidate['images']['service']['image_ref']
    assert prepared.original['spec']['template']['spec']['containers'][0]['image'] != publication.bundle.candidate['images']['service']['image_ref']
    assert prepared.original['metadata']['uid'] == completed_pool[3].server.store.resources['Deployment:loom-service']['metadata']['uid']
    old_volumes = {row['name']: row for row in prepared.original['spec']['template']['spec']['volumes']}
    volumes = {row['name']: row for row in prepared.delivery.deployment['spec']['template']['spec']['volumes']}
    for name in ('management-cloud', 'application-shared', 'db-ca'):
        assert volumes[name] == old_volumes[name]
    assert 'pool-profiles' in volumes
    configuration, = [row for row in prepared.delivery.configuration if row['kind'] == 'ConfigMap']
    config = json.loads(configuration['data']['installation.json'])
    assert config['applications']['runtime']['build']['binding']['pool_id'] == str(spec.pool_id)
    source = volumes['application-source-credentials']['secret']['secretName']
    assert source == prepared.delivery.source_secret_name
    if before.installation.applications.runtime.source_upload is not None:
        assert source == old_volumes['application-source-credentials']['secret']['secretName']
        assert prepared.requires_source_material is False
    else:
        assert prepared.requires_source_material is True
    assert prepared.deployment.installation.applications.shared == before.installation.applications.shared


@pytest.mark.parametrize('damage', ['source', 'archive', 'profile', 'selection', 'run'])
def test_runtime_rejects_mixed_source_publication_before_delivery(completed_pool, damage):
    from dataclasses import replace

    from scripts.ops import nebius_development_runtime_render as runtime

    target = runtime_publication(completed_pool)
    if damage == 'source':
        target = replace(target, source=target.source.model_copy(update={'source_sha': 'f' * 40}))
    elif damage == 'archive':
        target.bundle.candidate['source_archive_sha256'] = 'sha256:' + 'f' * 64
    elif damage == 'profile':
        target.bundle.profile['candidate_sha'] = 'f' * 40
    elif damage == 'selection':
        target = replace(target, publication=target.publication.model_copy(update={'candidate_id': UUID('47f27bd6-bfd9-4ae8-a585-82916a848c86')}))
    else:
        target.bundle.candidate['run_id'] += 1
    calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development manager runtime unqualified'):
        runtime.prepare_manager_runtime(reference(module(), completed_pool), publication=target)
    assert len(completed_pool[3].server.calls) == calls


@pytest.mark.parametrize('damage', ['anchor', 'parent', 'phase', 'incomplete', 'inputs', 'receipt'])
def test_missing_or_changed_completion_cannot_become_successor_authority(completed_pool, damage):
    loader = module()
    _, _, _, connected = completed_pool
    selector = reference(loader, completed_pool)
    original = Path(connected.server.request.retained.operation['state_dir']).parent
    state = original / 'pool-installation'
    parent_path = state / 'installation.json'
    parent = json.loads(parent_path.read_bytes())
    if damage == 'anchor':
        (Path(connected.server.request.retained.operation['anchor_dir']) / 'pool-installation.json').unlink()
    elif damage == 'parent':
        parent_path.unlink()
    elif damage == 'phase':
        (state / 'material/stage.json').unlink()
    elif damage == 'incomplete':
        parent['phases']['workload'] = {'status': 'started', 'sha256': None}
        parent_path.write_text(json.dumps(parent))
    elif damage == 'inputs':
        inputs_path = Path(completed_pool[0]['inputs_path'])
        inputs_path.write_bytes(inputs_path.read_bytes() + b' ')
    else:
        registration = original / 'pool-registration/registration.json'
        value = json.loads(registration.read_bytes())
        value['proof']['registration']['mode'] = 'global'
        registration.write_text(json.dumps(value))
        parent['phases']['registration']['sha256'] = hashlib.sha256(registration.read_bytes()).hexdigest()
        parent_path.write_text(json.dumps(parent))
    before = len(connected.server.calls)
    with pytest.raises(ValueError, match='retained development pool') as caught:
        loader.load_retained_pool(selector)
    assert len(connected.server.calls) == before
    assert not any(token in str(caught.value) for token in connected.intent.tokens.values())
