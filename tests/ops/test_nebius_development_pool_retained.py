"""Runtime successors consume closed pool evidence without replaying installation."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import json
from pathlib import Path
from uuid import UUID

import pytest
from tests.ops.test_nebius_development_management_foundation import (
    development_inputs as original_development_inputs,  # noqa: F401
)
from tests.ops.test_nebius_development_management_foundation import entry as entry
from tests.ops.test_nebius_development_management_foundation import handoff as handoff
from tests.ops.test_nebius_development_management_foundation import live as live
from tests.ops.test_nebius_development_management_foundation import preflight as preflight
from tests.ops.test_nebius_development_management_foundation import publication as publication
from tests.ops.test_nebius_development_management_foundation import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_management_foundation import (
    source_checkout as source_checkout,
)
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
def development_inputs(request):
    config, candidate, profile = request.getfixturevalue('original_development_inputs')
    # The public-manager fixture selects this host. Its completed foundation
    # must have reserved that same endpoint, not a different earlier hostname.
    config['public_host'] = 'shared.dev.example.com'
    return config, candidate, profile


@pytest.fixture
def manager_entry(request):
    mode = getattr(request, 'param', None)
    foundation = request.getfixturevalue('handoff') if isinstance(mode, str) and mode.startswith('foundation') else None
    original = request.getfixturevalue('original_manager_entry')
    if foundation is not None:
        from dataclasses import asdict

        from tests.ops.test_nebius_development_management_install import DevelopmentAPI
        from tests.ops.test_nebius_development_source_intake import save_inputs

        reference, manager, _, _, _ = foundation
        operation, payload, path, _ = original
        payload.update(binding=asdict(manager.binding), shared_namespace_uid=manager.shared_namespace_uid,
            deployment=manager.deployment.model_dump(mode='json'))
        applications = payload['deployment']['installation']['applications']
        schema = '0159' if mode == 'foundation-old-schema' else '0174'
        applications['shared']['schema_revision'] = schema
        for release in applications['releases']:
            release['schema_revision'] = schema
        if mode == 'foundation-wrong-namespace':
            payload['shared_namespace_uid'] = 'af74765d-efdb-4700-9aef-46d2b56d0e37'
        payload['prerequisites']['foundation'] = reference
        for name in ('ca_pem', 'secret_store_master_keys'):
            if mode != 'foundation-wrong-' + name:
                Path(payload['application_files'][name]).write_text(getattr(manager.application_material, name))
        save_inputs(operation, payload, path)
        api = DevelopmentAPI(manager.binding)
        api.shared_uid = payload['shared_namespace_uid']
        return operation, payload, path, api
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


def database_runtime(completed_pool, **changes):
    name = 'scripts.ops.nebius_development_runtime_setup'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database delivery is missing')
    arguments = dict(publication=runtime_publication(completed_pool),
        operation_id=UUID('aecc7407-b7b8-4c38-8d1f-bca5dca9840f'),
        actuator_password='runtime-actuator-' + 'p' * 40, batch_runner_token='loom_br_' + 'r' * 64)
    arguments.update(changes)
    return importlib.import_module(name).prepare_database_runtime(reference(module(), completed_pool), **arguments)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_delivery_keeps_old_data_identity_and_separates_credentials(completed_pool):
    from sqlalchemy.engine import make_url

    calls = len(completed_pool[3].server.calls)
    prepared = database_runtime(completed_pool)
    manager, foundation = prepared.manager, prepared.foundation
    shared, worker = prepared.material
    config, job = prepared.database
    participant, = manager.retained.request.registration.spec.participants
    assert shared['metadata']['namespace'] == job['metadata']['namespace'] == config['metadata']['namespace'] == 'loom-dev'
    assert worker['metadata']['namespace'] == participant.execution_namespace.name
    assert shared['immutable'] is worker['immutable'] is config['immutable'] is True
    assert set(shared['data']) == {'actuator-password', 'batch-runner-token'}
    assert set(worker['data']) == {'actuator-url', 'ca.crt'}
    material = {key: base64.b64decode(value).decode() for key, value in worker['data'].items()}
    database = make_url(material['actuator-url'])
    assert (database.username, database.password, database.host, database.database) == (
        'loom_actuator', 'runtime-actuator-' + 'p' * 40, 'loom-postgres.loom-dev.svc', 'loom')
    assert dict(database.query) == {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}
    original = foundation.bootstrap['material']['loom-platform-db']
    assert material['ca.crt'] == original['ca.crt']
    assert original['postgres-password'] not in repr(material)
    assert json.loads(config['data']['setup.json']) == {'namespace': 'loom-dev',
        'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'schema_revision': '0174'}
    pod = job['spec']['template']['spec']
    container, = pod['containers']
    assert container['command'] == ['python', '-m', 'loom.nebius_development_runtime_database']
    assert container['image'] == manager.publication.bundle.candidate['images']['service']['image_ref']
    assert pod['automountServiceAccountToken'] is False
    assert pod['restartPolicy'] == 'Never' and job['spec']['backoffLimit'] == 0
    assert not pod.get('initContainers')
    assert container['securityContext']['allowPrivilegeEscalation'] is False
    assert container['securityContext']['capabilities']['drop'] == ['ALL']
    env = {row['name']: row for row in container['env']}
    assert set(env) == {'LOOM_DEVELOPMENT_RUNTIME_CONFIG', 'LOOM_DB_URL', 'LOOM_DB_ACTUATOR_PASSWORD', 'LOOM_BATCH_RUNNER_TOKEN'}
    assert env['LOOM_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'admin-url'}
    for setting, key in [('LOOM_DB_ACTUATOR_PASSWORD', 'actuator-password'), ('LOOM_BATCH_RUNNER_TOKEN', 'batch-runner-token')]:
        assert env[setting]['valueFrom']['secretKeyRef'] == {'name': shared['metadata']['name'], 'key': key}
    assert {row['name'] for row in container['volumeMounts']} == {'db-ca', 'runtime-setup'}
    assert {row['name'] for row in pod['volumes']} == {'db-ca', 'runtime-setup'}
    ca, = [row for row in pod['volumes'] if row['name'] == 'db-ca']
    assert ca['secret'] == {'secretName': 'loom-platform-db', 'defaultMode': 0o440,
        'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}
    assert len(completed_pool[3].server.calls) == calls
    assert all(path.read_bytes() == raw for path, raw in foundation.files.items())


@pytest.mark.parametrize('changes', [{'operation_id': UUID(int=0)},
    {'actuator_password': 'too-short'}, {'batch_runner_token': 'wrong-kind-' + 'r' * 64}])
def test_runtime_database_delivery_refuses_invalid_material(completed_pool, changes):
    # This protects the pre-write boundary, not merely the eventual SQL failure.
    name = 'scripts.ops.nebius_development_runtime_setup'
    if importlib.util.find_spec(name) is None:
        pytest.fail('fixed development runtime database delivery is missing')
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool, **changes)


@pytest.mark.parametrize('manager_entry', ['foundation-old-schema', 'foundation-wrong-namespace',
    'foundation-wrong-ca_pem', 'foundation-wrong-secret_store_master_keys'], indirect=True)
def test_runtime_database_delivery_rejects_manager_foundation_mismatch(completed_pool):
    calls = len(completed_pool[3].server.calls)
    with pytest.raises(ValueError, match='development runtime database delivery unqualified'):
        database_runtime(completed_pool)
    assert len(completed_pool[3].server.calls) == calls


def runtime_database_stage(request, api, state):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'stage_database_runtime'):
        pytest.fail('fixed development runtime database stage is missing')
    return setup.stage_database_runtime(request=request, api=api, state_dir=state)


def runtime_database_api(request):
    from tests.ops.test_nebius_management_stage import PhaseAPI

    class NamespacedAPI(PhaseAPI):
        @staticmethod
        def key(doc):
            return ':'.join((doc['kind'], doc['metadata']['namespace'], doc['metadata']['name']))

    return NamespacedAPI(request.manager.retained.request.retained.binding)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_retains_four_exact_creates_and_replays(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    first = runtime_database_stage(request, api, state)
    before = copy.deepcopy(api.resources)
    assert runtime_database_stage(request, api, state) == first
    assert api.resources == before
    name = 'loom-dev-runtime-aecc7407b7b84c388d1fbca5dca9840f'
    assert api.creates == ['Secret:loom-dev:' + name, 'Secret:loom-nebius-dev-execution:' + name,
        'ConfigMap:loom-dev:' + name, 'Job:loom-dev:' + name]
    assert first['status'] == 'management_phase_staged'
    assert first['phase'] == 'development-runtime-database'
    assert len(first['resource_uids']) == 4
    assert 'runtime-actuator-' not in json.dumps(first)


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_runtime_database_stage_never_retries_uncertain_create(completed_pool, tmp_path, failure):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    api.failure = failure
    if failure == 'after':
        receipt = runtime_database_stage(request, api, state)
        assert runtime_database_stage(request, api, state) == receipt
        assert len(api.creates) == 4
    else:
        for _ in range(2):
            with pytest.raises(ValueError, match='development runtime database stage unqualified'):
                runtime_database_stage(request, api, state)
        assert len(api.creates) == 1


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_stage_refuses_caller_modified_job_before_writes(completed_pool, tmp_path):
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    request.database[1]['spec']['template']['spec']['containers'][0]['command'] = ['sh', '-c', 'echo unapproved']
    with pytest.raises(ValueError, match='development runtime database stage unqualified'):
        runtime_database_stage(request, api, state)
    assert not api.creates and not state.exists()


@pytest.mark.parametrize('manager_entry', ['foundation'], indirect=True)
def test_runtime_database_proof_binds_job_operation_role_and_token(completed_pool, tmp_path):
    from scripts.ops import nebius_development_runtime_setup as setup

    if not hasattr(setup, 'validate_database_runtime_proof'):
        pytest.fail('fixed development runtime database receipt validation is missing')
    request = database_runtime(completed_pool)
    api, state = runtime_database_api(request), tmp_path / 'runtime-database'
    runtime_database_stage(request, api, state)
    job, = [row for row in api.resources.values() if row['kind'] == 'Job']
    proof = {'job_uid': job['metadata']['uid'], 'pod_uid': '27d6a159-4df3-4bbf-8722-897b2b3c619b',
        'database': {'status': 'development_runtime_database_installed',
            'operation_id': 'aecc7407-b7b8-4c38-8d1f-bca5dca9840f', 'role': 'loom_actuator',
            'role_oid': 17000, 'token_sha256': hashlib.sha256(('loom_br_' + 'r' * 64).encode()).hexdigest()}}
    assert setup.validate_database_runtime_proof(request, state, proof) is None
    for field, value in [('status', 'pending'), ('operation_id', '27d6a159-4df3-4bbf-8722-897b2b3c619b'),
            ('role', 'postgres'), ('role_oid', True), ('role_oid', 0), ('role_oid', 2**32),
            ('token_sha256', '0' * 64), ('extra', 'not-allowed')]:
        changed = copy.deepcopy(proof)
        changed['database'][field] = value
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, changed)
    for field, value in [('job_uid', proof['pod_uid']), ('pod_uid', str(UUID(int=0))), ('extra', True)]:
        with pytest.raises(ValueError, match='development runtime database receipt unqualified'):
            setup.validate_database_runtime_proof(request, state, {**proof, field: value})


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
