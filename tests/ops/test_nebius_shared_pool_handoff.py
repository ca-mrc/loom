"""A shared handoff consumes both real completion chains, not asserted readiness."""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
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
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_activation import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_activation import (
    pool_entry as pool_entry,
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
    database_runtime,
    runtime_install_request,
)
from tests.ops.test_nebius_development_pool_retained import (
    pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_pool_predecessor import finish_cutover

ROOT = Path(__file__).resolve().parents[2]
pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


@pytest.fixture
def original_platform_inputs():
    from tests.unit.test_nebius_platform_render import platform_inputs

    config, candidate, profile = platform_inputs.__wrapped__()
    config['cluster_id'] = 'mk8scluster-e00fixture'
    candidate.update(repository='ca-mrc/loom', source_ref='refs/heads/dev')
    return config, candidate, profile


@pytest.fixture
def staging_history(original_platform_inputs, retained, tmp_path):
    """Run the existing legacy installer/recovery in an independent fixture root.

    Only external I/O is doubled by those fixtures. Calling their builders
    explicitly avoids colliding legacy/dev fixture names or patching history
    readers in the behavior under test.
    """
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from tests.ops import test_nebius_management_install as install
    from tests.ops import test_nebius_pool_cutover_entry as entry
    from tests.ops.test_nebius_pool_runtime import runtime_inputs_for_environments

    base = tmp_path / 'legacy'
    base.mkdir()
    platform = copy.deepcopy(original_platform_inputs)
    management = entry.management_inputs.__wrapped__(platform)
    installed = install.installation.__wrapped__(management, entry.material.__wrapped__())
    bound = replace(installed[0].binding, kube_system_uid=retained[3].binding.kube_system_uid)
    installed = replace(installed[0], binding=bound), install.InstallationAPI(bound)
    with contextmanager(entry.checks.__wrapped__)(installed, entry.cloud.__wrapped__(), base) as checks:
        inputs = entry.entry_inputs.__wrapped__(base, checks, installed)
        application = entry.application_management_inputs.__wrapped__(management, platform)
        upgrade = entry.private_upgrade.__wrapped__(inputs, application, entry.application_material.__wrapped__(), base)
        completed = entry.completed_upgrade.__wrapped__(upgrade)
        runtime = runtime_inputs_for_environments(platform, management, ('development',))
        collector = entry.collector_inputs.__wrapped__(platform, runtime)
        retirement = entry.retirement_inputs.__wrapped__(runtime, platform)
        fencing = entry.fencing_inputs.__wrapped__(retirement)
        cutover = entry.cutover_inputs.__wrapped__(collector, retirement, fencing, runtime)
        with pytest.MonkeyPatch.context() as patch:
            database = entry.database_guard.__wrapped__(runtime, platform, base, patch)
            operation, _, root = entry.private_cutover.__wrapped__(completed, cutover, database)
            _, completion = finish_cutover(operation, legacy=True)
    return load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=completion['completion_sha256']), original=root)


@pytest.fixture
def staging_observation(request):
    return {'independent': True} if getattr(request, 'param', None) == 'independent' else {}


@pytest.fixture
def published_source(publication, source_checkout, tmp_path):
    from tests.ops import test_nebius_development_preflight as preflight

    original = preflight.inputs

    def canonical(path):
        record, private, keyring = original(path)
        record['repository'] = 'ca-mrc/loom'
        return record, private, keyring

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(preflight, 'inputs', canonical)
        return preflight.published_source.__wrapped__(publication, source_checkout, tmp_path)


@pytest.fixture
def pool_inputs(request, retained, staging_history, staging_observation, original_platform_inputs, tmp_path):
    from scripts.ops.nebius_staging_pool_attachment import prepare_staging_pool_attachment

    from loom.nebius_platform_render import build_platform
    from loom.nebius_pool_contract import PoolParticipantV1

    reference, catalog, tokens = dev_pool_inputs.__wrapped__(request, retained)
    legacy = staging_history
    old, = legacy.context.inputs.installation.participants
    guard, = legacy.context.inputs.guards
    candidate = copy.deepcopy(retained[3].candidate)
    profile = copy.deepcopy(retained[3].profile)
    independent = staging_observation.pop('independent', False)
    if independent:
        from scripts.ops.nebius_candidate import create_candidate

        record = copy.deepcopy(candidate)
        record.update(candidate_sha='f' * 40, runtime_binary_sha256=profile['runtime_binary_sha256'],
            policy_sha256='sha256:' + 'd' * 64)
        for row in record['images'].values():
            row.update(image_ref=row['image_ref'].split('@')[0] + '@sha256:' + 'e' * 64,
                sbom_sha256='sha256:' + 'e' * 64, vulnerability_report_sha256='sha256:' + 'f' * 64,
                highest_vulnerability_severity='high')
        candidate, profile = create_candidate(record, signing_key=tmp_path / 'signer.pem', signing_key_id='publisher',
            keyring_json=json.dumps(catalog['profiles']['image_admission_keyring']), supports_task_identity=True,
            guest_runtime='qemu-tcg-v1', guest_runtime_volume_mib=1024, guest_max_artifact_bytes=6 * 1024**3,
            supports_emulated_pkcs11=True)
    config = copy.deepcopy(original_platform_inputs[0])
    config.update(namespace=guard.namespace, execution_namespace=old.execution_namespace.name,
        target_id=old.targets[0].target_id, cluster_id=catalog['cluster_id'],
        execution_node_group_id=catalog['node_group_id'],
        task_image_builder={'registry_repository': 'cr.eu-north1.nebius.cloud/staging/task-images', 'max_concurrent': 2})
    if independent:
        config.update(guest_execution_target={'target_id': 'staging-guest'},
            emulated_auth_execution_target={'target_id': 'staging-auth'},
            task_identity_policy={'mode': 'private-root-v1', 'target_id': config['target_id'],
                'execution_namespace': config['execution_namespace']})
    rendered = build_platform(config, candidate, profile, catalog['profiles']['image_admission_keyring'], repo_root=ROOT)
    kinds = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
        'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
    rows = [row for group in rendered.values() for row in group if row['kind'] in kinds]
    pins = {**legacy.completion.workloads, _key(guard.database.statefulset): guard.database.statefulset,
        _key(guard.database.service): guard.database.service}
    namespaces = {guard.namespace: str(guard.namespace_uid), old.execution_namespace.name: str(old.execution_namespace.uid),
        old.build_namespace.name: str(old.build_namespace.uid)}
    for row in rows:
        prior = pins.get(_key(row), {}).get('metadata', {})
        row['metadata'].update(uid=prior.get('uid', str(uuid4())), resourceVersion='201')
        row['metadata'].setdefault('annotations', {}).update(prior.get('annotations', {}))
        if row['kind'] == 'Namespace':
            row['metadata']['uid'] = namespaces[row['metadata']['name']]
    staging_observation.update(schema_version='loom.nebius-pool-resource-observation.v1',
        cluster_id=config['cluster_id'], namespace=config['namespace'], candidate_sha=candidate['candidate_sha'], resources=rows,
        database_credential={'uid': str(guard.database.credential_uid), 'resource_version': '202'},
        actuator_credential={'uid': str(guard.database.actuator_credential_uid), 'resource_version': '203'})
    staged = old.model_dump(mode='json')
    staged.update(participant_id=str(uuid4()), pool_id=catalog['pool_id'], installation_id=catalog['installation_id'],
        environment_class='staging', legacy_catalog_environment='development', admission_epoch=catalog['admission_epoch'])
    staged['targets'][0]['workload_kinds'] = ['trial', 'verifier', 'task_image_build']
    if independent:
        staged['targets'].extend({'target_id': name, 'profile_id': str(uuid4()), 'workload_kinds': ['trial', 'verifier']}
            for name in ('staging-guest', 'staging-auth'))
    attachment = prepare_staging_pool_attachment(participant=PoolParticipantV1.model_validate(staged),
        observation=staging_observation, candidate=candidate, cluster_id=catalog['cluster_id'],
        node_group_id=catalog['node_group_id'], repo_root=ROOT)
    for kind in ('execution', 'task_images'):
        catalog['profiles'][kind].extend(attachment.profiles.model_dump(mode='json')[kind])
    catalog['participants'].append(staged)
    machine = copy.deepcopy(next(row for row in catalog['machines'] if row['role'] == 'participant'))
    machine.update(machine_id=str(uuid4()), participant_id=staged['participant_id'])
    token = 'staging-test-token'
    machine['token_sha256'] = hashlib.sha256(token.encode()).hexdigest()
    catalog['machines'].append(machine)
    tokens[UUID(machine['machine_id'])] = token
    if independent:
        raw = dict(staging_observation)
        staging_observation.clear()
        staging_observation.update(schema_version='loom.nebius-staging-pool-source.v1',
            candidate=candidate, observation=raw,
            publication={'candidate_id': str(uuid4()), 'source_sha': candidate['candidate_sha'],
                'run_id': candidate['run_id'], 'run_attempt': 1, 'artifact_id': 9876,
                'artifact_sha256': 'sha256:' + '9' * 64, 'pull_request': 2443})
    return reference, catalog, tokens


@pytest.fixture
def shared_runtime(completed_pool, publisher_cloud):
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
    from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication

    from loom_service.environment_management.candidates import ProtectedPublication
    from loom_service.environment_management.manager import CandidateBundle

    runtime, request = runtime_install_request(completed_pool, publisher_cloud)
    retained = request.database.manager.retained.request.retained.inputs
    candidate, profile = copy.deepcopy(retained.candidate), copy.deepcopy(retained.profile)
    source = PreparedDevelopmentSource(source_sha=candidate['candidate_sha'], source_archive_sha256=candidate['source_archive_sha256'])
    selected = ProtectedPublication(candidate_id=uuid4(), source_sha=source.source_sha, run_id=candidate['run_id'],
        run_attempt=1, artifact_id=23456, artifact_sha256='sha256:' + '7' * 64, pull_request=2399)
    baseline = DevelopmentRuntimePublication(source, selected, CandidateBundle(selected.candidate_id, candidate, profile))
    request = replace(request, database=database_runtime(completed_pool, publication=baseline))
    plan = runtime.prepare_runtime_install(request)
    api = RuntimeParentAPI(request, plan)
    api.database_complete = api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    return request, api


@pytest.fixture
def shared_operation(shared_runtime, staging_history, staging_observation, completed_pool, handoff):
    """A real source/history plan and GET-qualified database-bound migration."""
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_guard_bindings
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory

    request, parent = shared_runtime
    _, documents, handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    for row in documents.values():
        if row['kind'] == 'Secret':
            row['metadata'].setdefault('resourceVersion', '501')
    args = dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT)
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server, transport=httpx.MockTransport(handler))
        bound = observe_shared_pool_guard_bindings(**args, development=observer)
    return args, bound


def _shared_entry_inputs(args, bindings):
    """Retain genuine runtime input/source and a separately captured shared operation."""
    import base64
    import json
    from dataclasses import asdict

    from scripts.ops import nebius_certificates as private_state

    runtime = args['runtime']
    manager = runtime.database.manager.retained.request.retained
    owner = Path(manager.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-development-runtime' / manager.binding.installation_id / str(runtime.database.operation_id)
    root.mkdir(parents=True, mode=0o700)
    material = {}
    values = runtime.database.material[0]['data']
    for field, raw in (('collector_credential_file', runtime.collector_credential),
            ('registry_credential_file', runtime.registry_credential),
            ('actuator_password_file', base64.b64decode(values['actuator-password'])),
            ('batch_runner_token_file', base64.b64decode(values['batch-runner-token']))):
        path = root / (field + '.private')
        private_state._write_private(path, raw)
        material[field] = str(path)
    cache = None
    if runtime.cache_material is not None:
        cache = {}
        for index, (key, value) in enumerate(runtime.cache_material.items()):
            path = root / f'cache-{index}.private'
            private_state._write_private(path, value.encode())
            cache[key] = str(path)
    publication = runtime.database.manager.publication
    source = publication.source.model_dump(mode='json')
    private_state._atomic_json(root / 'development-runtime-source.json', source)
    retired = {'endpoint': runtime.database.foundation.inputs.config['kubernetes_api_server'],
        'ca_file': str(root / 'retired-operator-ca.pem'), 'credentials_file': str(root / 'retired-operator-key.json')}
    value = {'schema_version': 'loom.nebius-development-runtime-inputs.v1',
        'retained': runtime.database.reference.model_dump(mode='json'),
        'publication': publication.publication.model_dump(mode='json'),
        'candidate': publication.bundle.candidate, 'profile': publication.bundle.profile,
        'collector_scope': runtime.collector_scope.model_dump(mode='json'),
        'registry_scope': runtime.registry_scope.model_dump(mode='json'),
        'cache_files': cache, 'operator_connection': retired, **material}
    private_state._atomic_json(root / 'inputs.json', value)
    runtime_operation = {'schema': 'loom.nebius-development-runtime-operation.v1',
        'source_sha': source['source_sha'], 'installation_id': manager.binding.installation_id,
        'namespace': manager.binding.namespace, 'operation_id': str(runtime.database.operation_id),
        'inputs_path': str(root / 'inputs.json'), 'inputs_sha256': hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()}
    private_state._atomic_json(root / 'operation.json', runtime_operation)
    shared_root = owner / '.loom/nebius-shared-pool' / manager.binding.installation_id / str(bindings.migration.registration.spec.operation_id)
    shared_root.mkdir(parents=True, mode=0o700)
    private_state._atomic_json(shared_root / 'shared-pool-source.json', source)
    private_state._write_private(shared_root / 'operator-ca.pem', runtime.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'].encode())
    private_state._write_private(shared_root / 'operator-key.json', b'private-new-shared-operator')
    private_state._atomic_json(shared_root / 'staging.json', args['observation'])
    dev, = (row for row in bindings.migration.guards if row.namespace == 'loom-dev')
    inputs = {'schema_version': 'loom.nebius-shared-pool-inputs.v1',
        'runtime': {'operation_path': str(root / 'operation.json'),
            'operation_sha256': hashlib.sha256((root / 'operation.json').read_bytes()).hexdigest()},
        'legacy': args['legacy'].selector.model_dump(mode='json'),
        'staging_observation_file': str(shared_root / 'staging.json'),
        'staging_observation_sha256': hashlib.sha256((shared_root / 'staging.json').read_bytes()).hexdigest(),
        'development_guard': json.loads(json.dumps(asdict(dev), default=str)),
        'operator_connection': {'endpoint': retired['endpoint'], 'ca_file': str(shared_root / 'operator-ca.pem'),
            'credentials_file': str(shared_root / 'operator-key.json')}}
    private_state._atomic_json(shared_root / 'inputs.json', inputs)
    operation = {'schema': 'loom.nebius-shared-pool-operation.v1', 'source_sha': source['source_sha'],
        'installation_id': manager.binding.installation_id, 'namespace': manager.binding.namespace,
        'operation_id': str(bindings.migration.registration.spec.operation_id),
        'inputs_path': str(shared_root / 'inputs.json'),
        'inputs_sha256': hashlib.sha256((shared_root / 'inputs.json').read_bytes()).hexdigest()}
    private_state._atomic_json(shared_root / 'operation.json', operation)
    return shared_root, operation, inputs, root, retired


def test_shared_tooling_can_advance_without_reinstalling_runtime(shared_operation, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_entry as entry

    args, bindings = shared_operation
    root, operation, _, _, _ = _shared_entry_inputs(args, bindings)
    operation['source_sha'] = 'e' * 40
    private_state._atomic_json(root / 'shared-pool-source.json',
        {'source_sha': operation['source_sha'], 'source_archive_sha256': 'sha256:' + 'd' * 64})
    monkeypatch.setattr(entry, 'SOURCE_RECORD', root / 'shared-pool-source.json')
    context = entry.load_inputs(operation)
    assert context.runtime == args['runtime']
    assert context.bindings == bindings
    assert context.runtime.database.manager.publication.source.source_sha != operation['source_sha']
    with pytest.raises(ValueError, match='shared pool private inputs unqualified'):
        entry.load_inputs({**operation, 'source_sha': 'd' * 40})


@pytest.mark.parametrize('staging_observation', ['independent'], indirect=True)
def test_shared_handoff_preserves_independent_staging_images(shared_runtime, staging_history, staging_observation):
    from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff

    runtime, _ = shared_runtime
    plan = prepare_shared_pool_handoff(runtime=runtime, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    selected = staging_observation['candidate']
    assert plan.migration.registration.candidate == runtime.database.manager.publication.bundle.candidate
    assert selected['candidate_sha'] != plan.migration.registration.candidate['candidate_sha']
    for component, name in [('control_plane', 'loom-control-plane'), ('service', 'loom-service'),
            ('execution_actuator', 'loom-execution-actuator'), ('execution_actuator', 'staging-guest-actuator'),
            ('execution_actuator', 'staging-auth-actuator')]:
        row, = (value for value in plan.staging_targets.values() if value['metadata']['name'] == name)
        before = plan.staging.originals[_key(row)]
        pod = row['spec']['template']['spec']
        assert pod['containers'][0]['image'] == before['spec']['template']['spec']['containers'][0]['image']
        assert pod['containers'][0]['image'] == selected['images'][component]['image_ref']
        previous_initializers = {row['name']: row['image'] for row in before['spec']['template']['spec'].get('initContainers', [])}
        for initializer in pod.get('initContainers', []):
            assert initializer['image'] == previous_initializers.get(initializer['name'], selected['images']['service']['image_ref'])
    assert plan.staging.attachment.runtime_profile.candidate_sha == selected['candidate_sha']
    for damage in ('missing-publication', 'wrong-sha', 'wrong-run', 'overlay', 'raw-observation'):
        changed = copy.deepcopy(staging_observation)
        if damage == 'missing-publication':
            del changed['publication']
        elif damage == 'wrong-sha':
            changed['publication']['source_sha'] = 'b' * 40
        elif damage == 'wrong-run':
            changed['publication']['run_id'] += 1
        elif damage == 'overlay':
            changed['profile'] = {}
        else:
            changed = changed['observation']
        with pytest.raises(ValueError, match='shared pool handoff intent unqualified'):
            prepare_shared_pool_handoff(runtime=runtime, observation=changed, legacy=staging_history, repo_root=ROOT)


@pytest.mark.parametrize('staging_observation', ['independent'], indirect=True)
async def test_independent_staging_publication_requires_exact_approved_artifact_and_protocol(
        shared_runtime, staging_history, staging_observation, published_source):
    import io
    import zipfile

    import httpx
    from httpx._client import AsyncClient
    from scripts.ops import nebius_development_runtime_external as external
    from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff

    runtime, _ = shared_runtime
    plan = prepare_shared_pool_handoff(runtime=runtime, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    reference = staging_observation['publication']
    candidate = staging_observation['candidate']
    profile = plan.staging.attachment.runtime_profile.model_dump(mode='json')
    # Real verifier, signed bytes and HTTP protocol; no readiness callback.
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, 'w') as archive:
        archive.writestr('candidate.json', json.dumps(candidate))
        archive.writestr('runtime-profile.json', json.dumps(profile))
    reference['artifact_sha256'] = 'sha256:' + hashlib.sha256(payload.getvalue()).hexdigest()
    responses = copy.deepcopy(published_source.responses)
    responses['actions/runs/123/attempts/1']['head_sha'] = candidate['candidate_sha']
    pr = responses.pop('pulls/40')
    pr.update(number=reference['pull_request'], merge_commit_sha=candidate['candidate_sha'])
    responses['pulls/' + str(reference['pull_request'])] = pr
    artifact = responses.pop('actions/artifacts/123')
    artifact.update(id=reference['artifact_id'], digest=reference['artifact_sha256'], size_in_bytes=len(payload.getvalue()),
        name=f"nebius-candidate-{candidate['candidate_sha']}-123-1")
    artifact['workflow_run']['head_sha'] = candidate['candidate_sha']
    responses['actions/artifacts/' + str(reference['artifact_id'])] = artifact
    floor = '5781389317608cedf95be68590505c96257aa6c7'
    comparison = {'base_commit': {'sha': floor}, 'merge_base_commit': {'sha': floor},
        'status': 'ahead', 'ahead_by': 3, 'behind_by': 0}
    mode, reads = {'unavailable': False}, []

    def respond(request):
        assert request.method == 'GET'
        assert request.headers['Authorization'] == 'Bearer scoped-publication-test-token'
        assert request.url.host == 'api.github.com'
        path = request.url.path.removeprefix('/repositories/1281629473/')
        reads.append(path)
        if mode['unavailable']:
            return httpx.Response(503)
        if path == f"actions/artifacts/{reference['artifact_id']}/zip":
            return httpx.Response(200, content=payload.getvalue())
        if path.startswith('compare/'):
            assert path == f"compare/{floor}...{candidate['candidate_sha']}"
            return httpx.Response(200, json=comparison)
        return httpx.Response(200, json=responses[path])

    plan = prepare_shared_pool_handoff(runtime=runtime, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    async with AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await external.qualify_staging_publication(request=runtime, staging=plan.staging, http=http)
        assert any(path.startswith('compare/') for path in reads)
        for damage in ('profile', 'candidate', 'unapproved', 'old-protocol', 'unavailable'):
            changed = copy.deepcopy(plan.staging)
            if damage == 'profile':
                changed = replace(changed, attachment=replace(changed.attachment,
                    runtime_profile=changed.attachment.runtime_profile.model_copy(update={'max_artifact_bytes': 1})))
            elif damage == 'candidate':
                changed.source.candidate['images']['service']['image_ref'] += '-changed'
            elif damage == 'unapproved':
                pr['merged'] = False
            elif damage == 'old-protocol':
                comparison.update(status='behind', ahead_by=0, behind_by=2,
                    merge_base_commit={'sha': candidate['candidate_sha']})
            else:
                mode['unavailable'] = True
            with pytest.raises(ValueError, match='staging runtime publication unqualified') as error:
                await external.qualify_staging_publication(request=runtime, staging=changed, http=http)
            assert 'scoped-publication-test-token' not in str(error.value)
            pr['merged'] = True
            comparison.update(status='ahead', ahead_by=3, behind_by=0, merge_base_commit={'sha': floor})
            mode['unavailable'] = False


def test_shared_private_entry_binds_retained_runtime_without_retired_operator_access(shared_operation, monkeypatch):
    import importlib.util

    from scripts.ops import nebius_certificates as private_state

    name = 'scripts.ops.nebius_shared_pool_entry'
    assert importlib.util.find_spec(name) is not None, 'protected shared-pool private entry is missing'
    module = __import__(name, fromlist=['load_inputs'])
    args, bindings = shared_operation
    root, operation, inputs, runtime_root, retired = _shared_entry_inputs(args, bindings)
    monkeypatch.setattr(module, 'SOURCE_RECORD', root / 'shared-pool-source.json')
    monkeypatch.setattr(module, '_transport', lambda *_: pytest.fail('intake must finish before credential exchange'))
    context = module.load_inputs(operation)
    assert context.runtime == args['runtime'] and context.legacy == args['legacy']
    assert context.bindings == bindings and context.observation == args['observation']
    assert all(path.read_bytes() == raw for path, raw in context.files.items())
    assert {root / 'inputs.json', root / 'staging.json', runtime_root / 'operation.json',
        runtime_root / 'inputs.json', runtime_root / 'development-runtime-source.json'} <= context.files.keys()
    assert all(not Path(retired[key]).exists() and Path(retired[key]) not in context.files
        for key in ('ca_file', 'credentials_file'))
    assert 'private-new-shared-operator' not in repr(context)
    original = {path: path.read_bytes() for path in context.files}
    for damage in ('source', 'runtime-source', 'snapshot', 'runtime-reference', 'dev-database', 'dev-controller',
            'staging-namespace', 'pool-operation', 'legacy', 'missing-runtime-child', 'non-private-key'):
        changed = copy.deepcopy(operation)
        altered = copy.deepcopy(inputs)
        try:
            if damage == 'source':
                changed['source_sha'] = 'f' * 40
            elif damage == 'runtime-source':
                private_state._atomic_json(runtime_root / 'development-runtime-source.json',
                    {'source_sha': 'f' * 40, 'source_archive_sha256': 'sha256:' + 'e' * 64})
            elif damage == 'snapshot':
                (root / 'staging.json').write_bytes((root / 'staging.json').read_bytes() + b'\n')
            elif damage == 'runtime-reference':
                altered['runtime']['operation_sha256'] = 'f' * 64
            elif damage == 'dev-database':
                altered['development_guard']['database']['credential_uid'] = str(uuid4())
            elif damage == 'dev-controller':
                altered['development_guard']['controller']['spec']['replicas'] = 0
            elif damage == 'staging-namespace':
                changed['namespace'] = 'loom-staging'
            elif damage == 'pool-operation':
                changed['operation_id'] = str(uuid4())
            elif damage == 'legacy':
                altered['legacy']['completion_sha256'] = 'f' * 64
            elif damage == 'missing-runtime-child':
                (bindings.plan.development.runtime.state_dir / 'start/transition.json').unlink()
            else:
                (root / 'operator-key.json').chmod(0o644)
            private_state._atomic_json(root / 'inputs.json', altered)
            changed['inputs_sha256'] = hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()
            with pytest.raises(ValueError, match='shared pool private inputs unqualified'):
                module.load_inputs(changed)
        finally:
            for path, raw in original.items():
                path.write_bytes(raw)
                path.chmod(0o600)
    # Old operation paths may reside in their authenticated immutable release;
    # an arbitrary same-content operation copied elsewhere is not a predecessor.
    copied = root / 'foreign/operation.json'
    copied.parent.mkdir(mode=0o700)
    copied.write_bytes((runtime_root / 'operation.json').read_bytes())
    copied.chmod(0o600)
    inputs['runtime']['operation_path'] = str(copied)
    private_state._atomic_json(root / 'inputs.json', inputs)
    operation['inputs_sha256'] = hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='shared pool private inputs unqualified'):
        module.load_inputs(operation)


def test_shared_history_loads_original_source_without_retired_operator_credentials(shared_operation, monkeypatch, tmp_path):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_entry as entry

    assert hasattr(entry, 'load_historical_inputs'), 'normal release cannot load historical shared inputs independently'
    args, bindings = shared_operation
    root, operation, inputs, _, _ = _shared_entry_inputs(args, bindings)
    path = root / 'operation.json'
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    retired = {Path(inputs['operator_connection'][field]) for field in ('ca_file', 'credentials_file')}
    for credential in retired:
        credential.unlink()
    newer_source = tmp_path / 'new-installer-source.json'
    private_state._atomic_json(newer_source, {'source_sha': 'e' * 40, 'source_archive_sha256': 'sha256:' + '9' * 64})
    monkeypatch.setattr(entry, 'SOURCE_RECORD', newer_source)
    monkeypatch.setattr(entry, '_transport', lambda *_: pytest.fail('history must not exchange credentials'))
    context = entry.load_historical_inputs(operation_path=path, operation_sha256=checksum)
    assert context.operation == operation and context.runtime == args['runtime']
    assert context.legacy == args['legacy'] and context.bindings == bindings
    assert context.observation == args['observation']
    assert context.files[path] == path.read_bytes()
    assert root / 'shared-pool-source.json' in context.files
    assert not ({newer_source, *retired} & context.files.keys())
    assert not hasattr(context, 'inputs'), 'historical scope must not masquerade as a current operator connection'
    before = {item: item.read_bytes() for item in context.files}

    # The immutable installed tooling location is a valid historical reference.
    release = root / 'releases' / ('a' * 64)
    private_state._private_directory(release.parent)
    private_state._private_directory(release)
    for name in ('operation.json', 'shared-pool-source.json'):
        private_state._write_private(release / name, (root / name).read_bytes())
    loaded = entry.load_historical_inputs(operation_path=release / 'operation.json', operation_sha256=checksum)
    assert loaded.bindings == context.bindings
    for damage in ('operation-digest', 'foreign-path', 'changed-source', 'changed-inputs', 'missing-child'):
        selected, digest = path, checksum
        if damage == 'operation-digest':
            digest = 'f' * 64
        elif damage == 'foreign-path':
            selected = tmp_path / 'operation.json'
            private_state._write_private(selected, path.read_bytes())
            private_state._write_private(tmp_path / 'shared-pool-source.json', (root / 'shared-pool-source.json').read_bytes())
        elif damage == 'changed-source':
            (root / 'shared-pool-source.json').write_bytes(newer_source.read_bytes())
        elif damage == 'changed-inputs':
            (root / 'inputs.json').write_bytes(b'{}')
        else:
            (bindings.plan.development.runtime.state_dir / 'start/transition.json').unlink()
        with pytest.raises(ValueError, match='historical'):
            entry.load_historical_inputs(operation_path=selected, operation_sha256=digest)
        for item, raw in before.items():
            item.write_bytes(raw)
            item.chmod(0o600)
    assert all(item.read_bytes() == raw for item, raw in before.items())
    assert all(not item.exists() for item in retired)


# Real retained-history setup plus semantic refusal checks reached 59.32s in
# CI (#2470). Keep the full lifecycle bounded while allowing runner variance.
@pytest.mark.timeout(120)
def test_shared_history_refuses_synthetic_completion_without_opening_a_connection(shared_operation, monkeypatch):
    """A completed parent/checksums cannot replace its actual child contracts."""
    from scripts.ops import nebius_pool_participant_entry as participant_entry
    from scripts.ops import nebius_shared_pool_entry as entry
    from scripts.ops import nebius_shared_pool_operation as parent

    from loom.nebius_kubernetes import NebiusKubernetesConnection

    assert callable(getattr(entry, 'load_completed_history', None)), 'protected entry lacks semantic shared history loading'
    args, bindings = shared_operation
    root, _, inputs, _, _ = _shared_entry_inputs(args, bindings)
    for field in ('ca_file', 'credentials_file'):
        Path(inputs['operator_connection'][field]).unlink()
    monkeypatch.setattr(entry, '_transport', lambda *_: pytest.fail('historical completion must not authenticate'))
    monkeypatch.setattr(entry, 'HTTPSSharedPoolOperationAPI', lambda **_: pytest.fail('historical completion must not connect'))
    options = dict(operation_path=root / 'operation.json',
        operation_sha256=hashlib.sha256((root / 'operation.json').read_bytes()).hexdigest())
    with pytest.raises(ValueError, match='completed shared pool history'):
        entry.load_completed_history(**options)
    api = SharedPhaseAPI(parent)
    assert parent.run_shared_pool_operation(**args, bindings=bindings, api=api,
        action='install')['status'] == 'shared_pool_handoff_complete'
    # The deliberately synthetic fixture is useful for parent sequencing only.
    # Its valid parent and file hashes must not pass production child validation.
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    before = {path: path.read_bytes() for path in state.rglob('*') if path.is_file()}
    with pytest.raises(ValueError, match='completed shared pool history'):
        entry.load_completed_history(**options)
    assert {path: path.read_bytes() for path in state.rglob('*') if path.is_file()} == before
    # The actual normal-release host entry must use this semantic loader before
    # credentials or reference data can reach a release connection. In particular,
    # an initial handoff's parent status plus checksums is insufficient.
    monkeypatch.setattr(participant_entry, 'authenticated_participant_release',
        lambda **_: pytest.fail('synthetic completion must not reach authentication'))
    with pytest.raises(ValueError, match='participant host inputs'):
        participant_entry.execute_retained_participant_release(reference=None,
            shared_operation=options['operation_path'], shared_operation_sha256=options['operation_sha256'],
            state_root=root / 'participant-releases', anchor_dir=root / 'participant-anchors',
            operator_connection=NebiusKubernetesConnection.model_validate(inputs['operator_connection']),
            executable=root / 'kubectl')
    assert not (root / 'participant-releases').exists() and not (root / 'participant-anchors').exists()
    assert {path: path.read_bytes() for path in state.rglob('*') if path.is_file()} == before


@pytest.mark.timeout(420)
def test_shared_private_entry_dispatches_real_parent_and_preserves_uncertain_intent(
        shared_operation, shared_guard_transport, monkeypatch, capsys):
    """Entry, concrete parent and guard child are real; external observations/transport are controlled."""
    import json
    import ssl
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as concrete
    from scripts.ops import nebius_shared_pool_entry as entry

    assert callable(getattr(entry, 'main', None)), 'protected shared entry dispatch is missing'
    assert callable(getattr(entry, 'connected_api', None)), 'protected shared connection is missing'
    args, bindings = shared_operation
    guard, model = shared_guard_transport
    root, operation, _, _, _ = _shared_entry_inputs(args, bindings)
    monkeypatch.setattr(entry, 'SOURCE_RECORD', root / 'shared-pool-source.json')
    operator = root / 'operator-key.json'
    credentials, configs, failures = [], [], set()
    context = ssl.create_default_context(cadata=(root / 'operator-ca.pem').read_text())

    async def transport(connection):
        assert connection.endpoint == args['runtime'].database.foundation.inputs.config['kubernetes_api_server']
        assert connection.ca_file == root / 'operator-ca.pem' and connection.credentials_file == operator
        credentials.append(operator.read_bytes())
        if 'authentication' in failures:
            operator.write_bytes(b'changed-during-token-exchange')
        return context, 'test-operator-token'

    monkeypatch.setattr(entry, '_transport', transport)

    def checked(**options):
        if 'bindings' in options:
            assert options['bindings'] == bindings
        return SimpleNamespace(plan=bindings.plan, bindings=bindings, pending_workloads=(),
            pool_state='closed', guard_states={target.namespace: 'held' if target.namespace in model.held else 'open'
                for target in bindings.migration.guards}, input_digest=bindings.input_digest, authority={})

    async def publication(**options):
        assert options['request'] == args['runtime']

    async def provider(**options):
        assert options['operator_credentials'] == operator and options['runtime'] == args['runtime']
        return checked(**options)

    monkeypatch.setattr(concrete, 'qualify_runtime_publication', publication)
    monkeypatch.setattr(concrete, 'observe_shared_pool_provider', provider)
    for name in ('observe_shared_pool_writers', 'observe_shared_pool_forward_writers',
            'observe_shared_pool_processes', 'observe_shared_pool_forward_processes'):
        monkeypatch.setattr(concrete, name, checked)
    real_api = concrete.HTTPSSharedPoolOperationAPI

    def api(**options):
        connection = real_api(**options)
        connection.client.close()
        connection.client = type(connection.client)(base_url=connection.api_server,
            transport=httpx.MockTransport(lambda request: httpx.Response(404)))
        guard.prefix = list(connection.guards.prefix)
        configs.append(Path(connection.prefix[connection.prefix.index('--kubeconfig') + 1]))
        return connection

    monkeypatch.setattr(entry, 'HTTPSSharedPoolOperationAPI', api)
    manager = args['runtime'].database.manager.retained.request.retained
    anchor = Path(manager.operation['anchor_dir'])
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    def run(action, code, status):
        assert entry.main(str(root / 'operation.json'), action) == code
        raw = capsys.readouterr().out
        assert 'private-new-shared-operator' not in raw and 'test-operator-token' not in raw
        value = json.loads(raw)
        assert value['status'] == status
        assert all(not path.exists() for path in configs), 'temporary connection leaked after entry exit'
        return value

    assert run('qualify', 0, 'tooling_qualified') == {'status': 'tooling_qualified'}
    assert not credentials
    assert run('forward-guards', 1, 'blocked') == {'status': 'blocked', 'stage': 'operation'}
    assert not credentials and not model.acquires and not state.exists()
    report = run('preflight', 0, 'shared_pool_preflight_qualified')
    assert {key: report[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')} == {
        key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
    assert report['admission_open'] is False and report['writer_migration_complete'] is False
    assert not state.exists() and not (anchor / 'shared-pool-handoff.json').exists() and not model.acquires
    # Selecting recovery without an existing handoff must never become install.
    assert run('recover', 1, 'blocked')['stage'] == 'handoff'
    assert not state.exists() and not model.acquires

    failures.add('authentication')
    assert run('install', 1, 'blocked')['stage'] == 'connection'
    assert not model.acquires and not state.exists()
    failures.clear()
    operator.write_bytes(b'private-new-shared-operator')
    # A changed pinned file is rejected even before another credential exchange.
    loaded = entry.load_inputs(operation)
    before = len(credentials)
    operator.write_bytes(b'changed-before-connect')
    with pytest.raises(ValueError):
        with entry.connected_api(loaded):
            pytest.fail('changed private input reached the handoff')
    assert len(credentials) == before
    operator.write_bytes(b'private-new-shared-operator')

    model.busy = bindings.migration.guards[0].namespace
    assert run('install', 0, 'pending')['phase'] == 'forward-guards'
    assert model.acquires == [model.busy] and not model.held
    model.busy, model.uncertain = None, bindings.migration.guards[0].namespace
    assert run('install', 1, 'blocked')['stage'] == 'handoff'
    attempted = list(model.acquires)
    assert run('install', 1, 'blocked')['stage'] == 'handoff'
    assert model.acquires == attempted
    assert json.loads((state / 'operation.json').read_bytes())['forward']['forward-guards']['status'] == 'started'


class SharedPhaseAPI:
    """Only the phase-adapter boundary is doubled in parent-journal tests.

    This models a child engine that writes intent before remote dispatch and
    reads an uncertain result on resumption. It does not prove live adapters.
    """
    def __init__(self, module):
        self.supported_phases = frozenset((*module.FORWARD_PHASES, *module.RECOVERY_PHASES))
        self.events = []
        self.qualifications = []
        self.preflights = 0
        self.block = None
        self.uncertain = None
        self.prepare_failure = None

    def preflight(self, bindings):
        self.preflights += 1

    def qualify(self, bindings, record, *, recovering):
        self.qualifications.append((record['outcome'], recovering))

    def prepare(self, phase, bindings, state):
        from scripts.ops import nebius_certificates as private_state

        if self.prepare_failure == (phase, 'before'):
            raise TimeoutError('simulated interruption before local child preparation')
        private_state._private_directory(state)
        if self.prepare_failure == (phase, 'directory'):
            raise TimeoutError('simulated interruption after private directory creation')
        path = state / 'receipt.json'
        if not path.exists():
            private_state._atomic_json(path, {'phase': phase, 'status': 'pending'})
        self.validate_prepared_phase(phase, state)
        if self.prepare_failure == (phase, 'after'):
            raise TimeoutError('simulated interruption after local child preparation')

    def validate_prepared_phase(self, phase, state):
        import json

        assert json.loads((state / 'receipt.json').read_bytes()) == {'phase': phase, 'status': 'pending'}

    def validate_phase(self, phase, state, *, complete):
        import json

        record = json.loads((state / 'receipt.json').read_bytes())
        assert record['phase'] == phase
        assert record['status'] in {'pending', 'applied'}
        if complete:
            assert record['status'] == 'applied'

    def advance(self, phase, bindings, state):
        import json

        from scripts.ops import nebius_certificates as private_state

        path = state / 'receipt.json'
        if path.exists() and json.loads(path.read_bytes())['status'] == 'applied':
            return True  # Readback settles the prior dispatch; no second write.
        private_state._private_directory(state)
        private_state._atomic_json(path, {'phase': phase, 'status': 'pending'})
        if phase == self.block:
            return False
        self.events.append(phase)
        private_state._atomic_json(path, {'phase': phase, 'status': 'applied'})
        if phase == self.uncertain:
            raise TimeoutError('simulated response loss after child commit')
        return True


@pytest.fixture
def shared_guard_transport(shared_operation, tmp_path, monkeypatch):
    """Actual bound guard adapter; only kubectl's external transport is doubled."""
    import base64
    import hmac
    import json
    import subprocess
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlsplit

    from pydantic import PostgresDsn
    from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
    from scripts.ops.nebius_pool_migration_guard import (
        _BOUND_GUARD_COMMAND,
        KubectlPoolGuardAPI,
        pool_runtime_role_sql,
    )
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        populate_runtime_pods,
    )

    _, bindings = shared_operation
    request = bindings.migration
    documents = {}

    def add(document):
        kind = document['kind']
        plural = {'Namespace': 'namespaces', 'Deployment': 'deployments', 'StatefulSet': 'statefulsets',
            'Service': 'services', 'Secret': 'secrets', 'ReplicaSet': 'replicasets'}[kind]
        prefix = '/api/v1' if document['apiVersion'] == 'v1' else '/apis/' + document['apiVersion']
        namespace = document['metadata'].get('namespace')
        path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + document['metadata']['name']
        documents[path] = copy.deepcopy(document)

    add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
        'uid': request.registration.binding.kube_system_uid}})
    urls = {}
    for target in request.guards:
        add({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': target.namespace, 'uid': str(target.namespace_uid)}})
        for row in (target.controller, target.database.statefulset, target.database.service):
            add(row)
        env, = (row for row in target.controller['spec']['template']['spec']['containers'][0]['env']
            if row['name'] == 'LOOM_CP_DB_URL')
        reference = env['valueFrom']['secretKeyRef']
        urls[target.namespace] = f'postgresql+psycopg://loom_cp:private-test@loom-postgres.{target.namespace}.svc:5432/loom'
        add({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': reference['name'], 'namespace': target.namespace,
            'uid': str(target.database.credential_uid), 'resourceVersion': target.database.credential_resource_version},
            'data': {reference['key']: base64.b64encode(urls[target.namespace].encode()).decode()}})
    populate_runtime_pods(documents)
    populate_database_backends(documents)
    for row in list(documents.values()):
        if row['kind'] == 'ReplicaSetList':
            for replica in row['items']:
                add(replica)
    kubeconfig = tmp_path / 'shared-guard-kubeconfig'
    kubeconfig.write_text('private-test-config')
    kubeconfig.chmod(0o600)
    api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
    model = SimpleNamespace(held=set(), busy=None, uncertain=None, acquires=[], documents=documents,
        runtime_roles=set(), runtime_stages=[], runtime_reads=[], runtime_uncertain=False, runtime_lost_read=False,
        readiness_pages={}, readiness_calls=[], readiness_failure=None)

    def run(argv, **kwargs):
        assert argv[:len(api.prefix)] == api.prefix
        args = argv[len(api.prefix):]
        if args[0] == 'get':
            if args[1] == '--raw':
                url = urlsplit(args[2])
                value = copy.deepcopy(documents[url.path])
                selector = parse_qs(url.query).get('labelSelector')
                if selector:
                    label, wanted = selector[0].split('=', 1)
                    value['items'] = [row for row in value['items'] if row['metadata'].get('labels', {}).get(label) == wanted]
            else:
                kind, name = args[1:3]
                namespace = args[args.index('-n') + 1] if '-n' in args else None
                plural = {'namespace': 'namespaces', 'deployment': 'deployments', 'statefulset': 'statefulsets',
                    'service': 'services', 'secret': 'secrets', 'replicaset': 'replicasets'}[kind]
                prefix = '/apis/apps/v1' if kind in {'deployment', 'statefulset', 'replicaset'} else '/api/v1'
                value = documents[prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name]
        else:
            assert args[:2] == ['exec', '-n'] and args[6] == '--'
            namespace = args[2]
            if args[7] == 'psql':
                assert args[3:6] == ['pod/loom-postgres-0', '-c', 'loom-postgres']
                if args[-1] == rollout_guard_observation_sql(owner=str(request.registration.spec.operation_id),
                        candidate=request.registration.candidate['candidate_sha']):
                    value = {'status': 'held' if namespace in model.held else 'open'}
                elif (namespace, args[-1]) in model.readiness_pages:
                    assert namespace in model.held
                    model.readiness_calls.append(namespace)
                    if model.readiness_failure == namespace:
                        raise RuntimeError('simulated real SQL rejection of charged work')
                    value = copy.deepcopy(model.readiness_pages[namespace, args[-1]])
                else:
                    action, = (action for action in ('stage', 'observe') if args[-1] == pool_runtime_role_sql(
                        owner=str(request.registration.spec.operation_id), candidate=request.registration.candidate['candidate_sha'], action=action))
                    assert namespace in model.held  # Real SQL independently enforces the held idle guard.
                    if action == 'stage':
                        model.runtime_stages.append(namespace)
                        model.runtime_roles.add(namespace)
                        if model.runtime_uncertain:
                            model.runtime_uncertain, model.runtime_lost_read = False, True
                            raise TimeoutError('simulated lost committed runtime-role transaction')
                    else:
                        model.runtime_reads.append(namespace)
                        if model.runtime_lost_read:
                            model.runtime_lost_read = False
                            raise TimeoutError('simulated lost first runtime-role readback')
                        assert namespace in model.runtime_roles
                    value = {'status': 'staged' if action == 'stage' else 'qualified'}
            else:
                assert args[3:6] == ['pod/loom-control-plane-a12b34-test', '-c', 'loom-control-plane']
                assert args[7:11] == ['python', '-c', _BOUND_GUARD_COMMAND, 'acquire']
                assert args[11:13] == [str(request.registration.spec.operation_id), request.registration.candidate['candidate_sha']]
                assert hmac.new(bytes.fromhex(args[13]), str(PostgresDsn(urls[namespace])).encode(), 'sha256').hexdigest() == args[14]
                model.acquires.append(namespace)
                if namespace == model.uncertain:
                    raise TimeoutError('simulated unknown guard acquisition')
                if namespace == model.busy:
                    value = {'status': 'skipped_busy'}
                else:
                    model.held.add(namespace)
                    value = {'status': 'acquired'}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', run)
    return api, model


@pytest.mark.timeout(420)
def test_shared_handoff_binds_real_completed_dev_and_current_staging(shared_runtime, staging_history, staging_observation):
    import base64

    from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
    from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff

    request, api = shared_runtime
    dev = load_completed_runtime(request)
    before = {**{path: path.read_bytes() for path in staging_history.history}, **dev.files}
    writes = copy.deepcopy(api.store.resources)
    result = prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
        legacy=staging_history, repo_root=ROOT)
    # Preserve the historical v1 digest shape, including absence (not null) of
    # an independent-source selection. Existing journals must remain loadable.
    from loom.nebius_platform_render import digest

    staging = result.staging
    registration = result.migration.registration
    database = staging.attachment.guard.database
    assert staging.source is None
    assert staging.input_digest == digest({'registration': registration.spec.model_dump(mode='json'),
        'candidate': registration.candidate, 'legacy': staging_history.selector.model_dump(mode='json'),
        'history': {str(path): value for path, value in staging_history.history.items()},
        'guard': {'namespace_uid': str(staging.attachment.guard.namespace_uid),
            'database_credential_uid': str(database.credential_uid),
            'database_credential_resource_version': database.credential_resource_version,
            'actuator_credential_uid': str(database.actuator_credential_uid),
            'actuator_credential_resource_version': database.actuator_credential_resource_version},
        'originals': staging.originals, 'stopped': staging.stopped})
    assert result.development.runtime == dev
    assert len(result.migration.guards) == 2
    assert {row.namespace for row in result.migration.guards} == {'loom-dev', staging_observation['namespace']}
    assert all(row['spec'].get('replicas', 0) == 0 for row in result.staging_targets.values())
    collector, = (row for row in result.staging_targets.values() if row['kind'] == 'CronJob')
    assert collector['spec']['suspend'] is True
    assert all(row['metadata']['namespace'] != 'loom-dev' for row in result.staging_targets.values())
    controller = result.staging_targets['Deployment:' + staging_observation['namespace'] + ':loom-control-plane']
    settings = {row['name']: row.get('value') for row in controller['spec']['template']['spec']['containers'][0]['env']}
    from loom.nebius_pool_settings import PoolRuntimeSettings

    bound = PoolRuntimeSettings.model_validate_json(settings['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON'])
    assert bound.participant.environment_class == 'staging' and bound.environment == 'development'
    assert bound.management_origin == 'https://' + request.database.manager.deployment.public_host
    participant = result.staging.attachment.participant
    execution, build = participant.execution_namespace.name, participant.build_namespace.name
    assert hasattr(result, 'staging_material'), 'handoff must derive staged credentials and role targets'
    assert len(result.staging_material) == 2
    assert {row['metadata']['namespace'] for row in result.staging_material.values()} == {staging_observation['namespace'], execution}
    assert all(row['kind'] == 'Secret' and row['immutable'] is True
        and base64.b64decode(row['data']['token']) == b'staging-test-token' for row in result.staging_material.values())
    roles = {f'Role:{execution}:loom-execution-actuator', f'Role:{build}:loom-task-image-builder'}
    assert set(result.staging_role_originals) == set(result.staging_role_targets) == roles
    for key in roles:
        original = result.staging_role_originals[key]
        assert original == next(row for row in staging_observation['resources'] if _key(row) == key)
        target = result.staging_role_targets[key]
        assert target['rules'] == [
            {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
            {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
            {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']},
        ]
        assert target['metadata']['annotations']['loom.nebius/shared-pool-handoff-operation'] == str(result.migration.registration.spec.operation_id)
        assert target['metadata'].get('labels') == original['metadata'].get('labels')
    assert {row['kind'] for row in result.staging_reader_authority.values()} == {'ClusterRole', 'ClusterRoleBinding'}
    reader, = (row for row in result.staging_reader_authority.values() if row['kind'] == 'ClusterRole')
    assert reader['rules'] == [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
        'resourceNames': [execution, build]}]
    binding, = (row for row in result.staging_reader_authority.values() if row['kind'] == 'ClusterRoleBinding')
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-execution-actuator', 'namespace': execution}]
    assert 'staging-test-token' not in repr(result)
    assert api.store.resources == writes
    assert all(path.read_bytes() == raw for path, raw in before.items())
    tokens = request.database.manager.retained.intent.tokens
    machine, = (row for row in result.migration.registration.spec.machines if row.participant_id == participant.participant_id)
    token = tokens[machine.machine_id]
    tokens[machine.machine_id] = 'wrong-machine-token'
    try:
        with pytest.raises(ValueError, match='shared pool handoff intent unqualified') as error:
            prepare_shared_pool_handoff(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT)
        assert 'wrong-machine-token' not in str(error.value)
    finally:
        tokens[machine.machine_id] = token
    changed = copy.deepcopy(staging_observation)
    role = next(row for row in changed['resources'] if _key(row) in roles)
    role['metadata'].setdefault('annotations', {})['loom.nebius/shared-pool-handoff-operation'] = str(result.migration.registration.spec.operation_id)
    with pytest.raises(ValueError, match='shared pool handoff intent unqualified'):
        prepare_shared_pool_handoff(runtime=request, observation=changed, legacy=staging_history, repo_root=ROOT)
    assert api.store.resources == writes
    assert all(path.read_bytes() == raw for path, raw in before.items())


@pytest.mark.timeout(420)
def test_shared_guard_binding_uses_live_dev_database_and_real_bound_guard_reader(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, monkeypatch, tmp_path):
    """An unbound dev guard must not fall back to the old controller-only path."""
    import base64
    import json
    import ssl
    import subprocess
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlsplit

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    request, parent = shared_runtime
    calls, documents, original_handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    populate_database_backends(documents)
    credential_path = '/api/v1/namespaces/loom-dev/secrets/loom-platform-db'
    actuator_secret = request.database.material[1]
    actuator_path = '/api/v1/namespaces/' + actuator_secret['metadata']['namespace'] + '/secrets/' + actuator_secret['metadata']['name']
    for path in (credential_path, actuator_path):
        documents[path]['metadata']['resourceVersion'] = '301'
    mode = {'late_version': False, 'reads': 0}

    def respond(message):
        if mode['late_version'] and message.url.path == credential_path:
            mode['reads'] += 1
            if mode['reads'] == 2:
                documents[credential_path]['metadata']['resourceVersion'] = '302'
        return original_handler(message)

    # Keep real history, HTTPS observer and guard implementation. Only their
    # external Kubernetes/psql transport is simulated.
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(respond))
        from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_guard_bindings

        def observe():
            return observe_shared_pool_guard_bindings(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=observer)

        bound = observe()
        guard, = (row for row in bound.migration.guards if row.namespace == 'loom-dev')
        database = guard.database
        assert database is not None
        assert str(database.credential_uid) == documents[credential_path]['metadata']['uid']
        assert database.credential_resource_version == '301'
        assert str(database.actuator_credential_uid) == documents[actuator_path]['metadata']['uid']
        assert database.actuator_credential_resource_version == '301'
        for kind, row in (('StatefulSet', database.statefulset), ('Service', database.service)):
            assert row['kind'] == kind and row['metadata']['namespace'] == 'loom-dev'
            assert row['metadata']['name'] == 'loom-postgres'
        staged, = (row for row in bound.migration.guards if row.namespace != 'loom-dev')
        assert staged == bound.plan.staging.attachment.guard
        kubeconfig = tmp_path / 'guard-kubeconfig'
        kubeconfig.write_text('test-only-private-kubeconfig')
        kubeconfig.chmod(0o600)
        commands = []

        def kubectl(command, **kwargs):
            commands.append(command)
            start = next(index for index, item in enumerate(command) if item in {'get', 'exec'})
            args = command[start:]
            if args[0] == 'exec':
                assert args[1:7] == ['-n', 'loom-dev', 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
                assert args[7] == 'psql'
                assert str(bound.migration.registration.spec.operation_id) in args[-1]
                assert bound.migration.registration.candidate['candidate_sha'] in args[-1]
                value = {'status': 'open'}
            elif args[1] == '--raw':
                url = urlsplit(args[2])
                value = copy.deepcopy(documents[url.path])
                query = parse_qs(url.query)
                if 'labelSelector' in query:
                    label, wanted = query['labelSelector'][0].split('=', 1)
                    value['items'] = [row for row in value['items'] if row['metadata']['labels'].get(label) == wanted]
            else:
                kind, name = args[1:3]
                namespace = args[args.index('-n') + 1] if '-n' in args else None
                if kind == 'namespace' and name == 'kube-system':
                    value = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                        'uid': bound.migration.registration.binding.kube_system_uid}}
                else:
                    plural = {'namespace': 'namespaces', 'secret': 'secrets', 'statefulset': 'statefulsets', 'service': 'services'}[kind]
                    prefix = '/apis/apps/v1' if kind == 'statefulset' else '/api/v1'
                    value = documents[prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + name]
            return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode())

        with monkeypatch.context() as transport:
            transport.setattr(subprocess, 'run', kubectl)
            api = KubectlPoolGuardAPI(request=bound.migration, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
            assert api.guard(guard, 'observe') == {'status': 'open'}
        assert sum('exec' in row for row in commands) == 1
        assert calls and all(row.method == 'GET' for row in calls)
        assert base64.b64decode(documents[credential_path]['data']['control-plane-url']).decode() not in repr(bound)

        original = copy.deepcopy(documents[credential_path])
        for damage in ('uid', 'data'):
            if damage == 'uid':
                documents[credential_path]['metadata']['uid'] = str(uuid4())
            else:
                documents[credential_path]['data']['control-plane-url'] = base64.b64encode(b'foreign-database').decode()
            with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
                observe()
            documents[credential_path] = copy.deepcopy(original)
        mode.update(late_version=True, reads=0)
        with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
            observe()
        assert all(row.method == 'GET' for row in calls)
        mode['late_version'] = False
        documents[credential_path] = copy.deepcopy(original)
        child = bound.plan.development.runtime.state_dir / 'start' / 'transition.json'
        raw = child.read_bytes()
        child.unlink()
        before_reads = len(calls)
        try:
            with pytest.raises(ValueError, match='shared pool guard bindings unqualified'):
                observe()
            assert len(calls) == before_reads
        finally:
            child.write_bytes(raw)
            child.chmod(0o600)


def _exercise_recovery_databases(*, bindings, documents, mode, checked, commands, probes, args, process_observation=False):
    """Real parent/child records and fixed SQL transports; earlier effects are doubled."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_reopening as reopening
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_pool_activation_database import pool_activation_state_sql

    name = 'scripts.ops.nebius_shared_pool_recovery_databases'
    assert importlib.util.find_spec(name) is not None, 'phase-bound recovery database observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_recovery_databases'])
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
    staging = bindings.plan.staging.attachment.guard.namespace
    children = {
        'forward-guards': (guards.prepare_shared_pool_guards, guards.validate_shared_pool_guards, 'guards.json'),
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (reopening.prepare_shared_recovery_reopening, reopening.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (reopening.prepare_shared_recovery_verification, reopening.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(SharedPhaseAPI):
        def prepare(self, phase, bound, directory):
            if phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                children[phase][0](bindings=bound, state_dir=directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in {'forward-guards', 'recovery-revoke'}:
                self.validate_phase(phase, directory, complete=False)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)

        def validate_phase(self, phase, directory, *, complete):
            if phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                mode['machine_state'] = 'revoked'
                path = directory / 'gateway.json'
                record = _read(path)
                for row in record['roles'].values():
                    row['status'] = 'absent'
                private_state._atomic_json(path, record)
                return True
            path = directory / children[phase][2]
            record = _read(path)
            if phase == 'recovery-fence':
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                mode.update(pool_state='fenced', guard_states={row.namespace: 'fenced' for row in bound.migration.guards})
            elif phase in {'recovery-drain', 'recovery-verify'}:
                record['status'] = 'observed'
            elif phase == 'recovery-stop':
                for key, choices in stop._sources(bound, state).items():
                    before = _snapshot(choices[-1])
                    before['metadata']['uid'] = _uid(choices[-1])
                    record['resources'][key] = {'status': 'stopped', 'before': before,
                        'before_resource_version': '1', 'expected': stop._stopped(bound, before)}
            elif phase in {'recovery-restore-workloads', 'recovery-restart'}:
                component = templates if phase == 'recovery-restore-workloads' else restart
                before, targets = component._inputs(bound, state)
                for key, row in record['resources'].items():
                    row.update(status='restored' if component is templates else 'started',
                        before_resource_version='2' if _stable(before[key]) != _stable(targets[key]) else None)
            elif phase == 'recovery-restore-roles':
                for row in record['roles'].values():
                    row.update(status='restored', before_resource_version='201')
            elif phase == 'recovery-reopen':
                record['status'] = 'released'
                mode['guard_states'][staging] = 'open'
            else:
                pytest.fail('unexpected phase: ' + phase)
            private_state._atomic_json(path, record)
            return True

    api = API(parent)
    api.block = 'forward-guards'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'

    if process_observation:
        _exercise_recovery_processes(kwargs=kwargs, state=state, api=api, documents=documents,
            probes=probes, mode=mode, checked=checked)
        return

    def observe():
        return module.observe_shared_pool_recovery_databases(bindings=bindings, state_dir=state,
            kubeconfig=Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1]), executable=Path(probes.prefix[0]))

    error = 'shared pool recovery database observation unqualified'
    with pytest.raises(ValueError, match=error):
        observe()  # No recovery marker; neither a caller flag nor forward history grants it.
    kwargs['action'], api.block = 'recover', 'recovery-fence'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    # A crashed successor must not make database recovery depend on app health.
    for document in documents.values():
        if document['kind'] == 'PodList':
            document['items'] = [row for row in document['items'] if row['metadata']['name'] == 'loom-postgres-0']
    checked.clear()
    first_command = len(commands)
    result = observe()
    assert result.pool_state == 'closed' and result.machine_authority == 'active' and result.retired_authority == 'revoked'
    assert result.guard_states == {'loom-dev': 'open', staging: 'open'}
    assert result.pool_drained is None and result.participant_drained == {'loom-dev': None, staging: None}
    assert checked == {'old-revoked', 'recovery-machines', 'pool-state',
        ('active-guard', 'loom-dev'), ('active-guard', staging), ('runtime-role', 'loom-dev')}

    fence_path = state / 'recovery-fence/fence.json'
    record = _read(fence_path)
    mode['pool_state'] = 'fenced'
    with pytest.raises(ValueError, match=error):
        observe()  # A remotely fenced state is not adopted before local intent.
    record['pool'] = 'intent'
    private_state._atomic_json(fence_path, record)
    assert observe().pool_state == 'fenced'
    mode['pool_state'] = 'closed'
    assert observe().pool_state == 'closed'
    record['pool'], mode['pool_state'] = 'fenced', 'fenced'
    private_state._atomic_json(fence_path, record)
    first_guard = bindings.migration.guards[0]
    mode['guard_states'] = {first_guard.namespace: 'fenced'}
    with pytest.raises(ValueError, match=error):
        observe()
    record['guards'][str(first_guard.participant_id)] = 'intent'
    private_state._atomic_json(fence_path, record)
    assert observe().guard_states[first_guard.namespace] == 'fenced'
    mode['guard_states'][first_guard.namespace] = 'open'
    assert observe().guard_states[first_guard.namespace] == 'open'
    record['guards'][str(first_guard.participant_id)] = 'fenced'
    private_state._atomic_json(fence_path, record)
    with pytest.raises(ValueError, match=error):
        observe()
    mode['guard_states'][first_guard.namespace] = 'fenced'
    api.block = 'recovery-drain'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-drain'
    mode.update(pool_busy=True, busy={'loom-dev', staging})
    result = observe()
    assert result.pool_drained is False and result.participant_drained == {'loom-dev': False, staging: False}
    mode.update(pool_busy=False, busy=set())

    # Bracket the final SQL read, not just each database's own first query.
    baseline = copy.deepcopy(documents)
    for damage in ('old-active', 'guard-foreign', 'late-manager-database-at-final-state'):
        mode.update(damage=damage, final_state_reads=1)
        with pytest.raises(ValueError, match=error):
            observe()
        documents.clear()
        documents.update(copy.deepcopy(baseline))
    mode['damage'] = None
    namespaces = (bindings.migration.registration.binding.namespace,
        bindings.plan.staging.legacy.context.original.upgrade.setup.binding.namespace, 'loom-dev', staging)
    for namespace in namespaces:
        observed = {'queries': 0, 'injected': False}
        def restart_database(argv, observed=observed, namespace=namespace):
            if argv[-1] == pool_activation_state_sql(bindings.migration.registration.spec):
                observed['queries'] += 1
                if observed['queries'] == 2:
                    pod, = (row for row in documents['/api/v1/namespaces/' + namespace + '/pods']['items']
                        if row['metadata']['name'] == 'loom-postgres-0')
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
                    observed['injected'] = True
        mode['sql_hook'] = restart_database
        with pytest.raises(ValueError, match=error):
            observe()
        assert observed['injected']
        documents.clear()
        documents.update(copy.deepcopy(baseline))
    mode['sql_hook'] = None
    receipt_bytes = fence_path.read_bytes()
    mode['sql_hook'] = lambda argv: fence_path.write_bytes(receipt_bytes + b'\n')
    with pytest.raises(ValueError, match=error):
        observe()
    mode['sql_hook'] = None
    fence_path.write_bytes(receipt_bytes)

    api.block = 'recovery-revoke'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
    assert observe().machine_authority == 'active'
    mode['pool_busy'] = True
    with pytest.raises(ValueError, match=error):
        observe()  # A previous drain receipt is not current drain proof.
    mode['pool_busy'] = False
    mode['machine_state'] = 'revoked'
    with pytest.raises(ValueError, match=error):
        observe()
    machine_path = state / 'recovery-revoke/machines.json'
    private_state._atomic_json(machine_path, {**_read(machine_path), 'status': 'intent'})
    assert observe().machine_authority == 'revoked'
    mode['machine_state'] = 'active'
    assert observe().machine_authority == 'active'
    api.block = 'recovery-reopen'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-reopen'
    assert observe().machine_authority == 'revoked'
    mode['machine_state'] = 'active'
    with pytest.raises(ValueError, match=error):
        observe()
    mode['machine_state'] = 'revoked'
    mode['guard_states'][staging] = 'open'
    with pytest.raises(ValueError, match=error):
        observe()
    reopen_path = state / 'recovery-reopen/reopen.json'
    private_state._atomic_json(reopen_path, {**_read(reopen_path), 'status': 'intent'})
    mode['busy'] = {staging}
    result = observe()
    assert result.participant_drained == {'loom-dev': True, staging: None}
    mode['guard_states'][staging], mode['busy'] = 'fenced', set()
    assert observe().participant_drained == {'loom-dev': True, staging: True}
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    mode['busy'] = {staging}
    assert observe().participant_drained == {'loom-dev': True, staging: None}
    mode['busy'] = {'loom-dev', staging}
    with pytest.raises(ValueError, match=error):
        observe()
    mode['busy'] = {staging}
    # A late change to the caller's nested binding cannot reuse a copied proof.
    captured = bindings.plan.staging.originals
    saved = copy.deepcopy(captured)
    def mutate_binding(argv):
        next(iter(captured.values()))['metadata'].setdefault('annotations', {})['late-test'] = 'changed'
    mode['sql_hook'] = mutate_binding
    with pytest.raises(ValueError, match=error):
        observe()
    captured.clear()
    captured.update(saved)
    mode['sql_hook'] = None
    assert 'private-test-only' not in repr(result)
    assert all(row[0] == 'get' or (row[0] == 'exec' and row[7] == 'psql') for row in commands[first_command:])


def _exercise_recovery_processes(*, kwargs, state, api, documents, probes, mode, checked):
    """Real recovery history/readiness; external I/O and collector publication are explicit doubles."""
    import importlib.util

    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    name = 'scripts.ops.nebius_shared_pool_recovery_processes'
    assert importlib.util.find_spec(name) is not None, 'phase-bound recovery process observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_recovery_processes'])
    bindings = kwargs['bindings']
    staging = bindings.plan.staging.attachment.guard
    completed = bindings.plan.development.runtime
    roots = {**bindings.plan.staging.originals,
        **{key: completed.resources[key] for key in (*completed.plan.fixed['workloads'], *completed.plan.originals)},
        _key(bindings.plan.development.gateway_original): bindings.plan.development.gateway_original}
    # The fixture's original collector is active. Its lineage/publication reader
    # has a separate real callback regression; this test controls just that read.
    collector_calls = []
    def collection_health(**options):
        assert options['bindings'] == bindings and options['guards'].request == bindings.migration
        assert _key(options['expected']) in bindings.plan.staging.originals
        collector_calls.append(_key(options['expected']))
        if mode.get('process_hook') is not None:
            mode['process_hook']()
        return mode.get('collector_ready', True)

    def current(key):
        return documents[runtime_resource_path(roots[key]) + '/' + roots[key]['metadata']['name']]

    def replace(key, wanted):
        actual = current(key)
        identity = {field: actual['metadata'][field] for field in ('uid', 'resourceVersion')}
        generation = actual['metadata'].get('generation', 1) + 1
        actual.clear()
        actual.update(copy.deepcopy(wanted))
        actual['metadata'].update(**identity, generation=generation)
        actual['status'] = {'observedGeneration': generation}

    def descendants():
        # Model the controllers completing their actual desired state. Keep the
        # retained database Pods; the real runtime challenges must still find them.
        for row in documents.values():
            if row['kind'] == 'PodList':
                row['items'] = [pod for pod in row['items'] if pod['metadata']['name'] == 'loom-postgres-0']
            elif row['kind'] == 'ReplicaSetList':
                row['items'] = []
        populate_runtime_pods(documents)
        for row in tuple(documents.values()):
            if row['kind'] == 'PodList':
                for pod in row['items']:
                    pod['spec']['nodeName'] = 'platform-test'
            if row['kind'] == 'ReplicaSetList':
                for child in row['items']:
                    documents['/apis/apps/v1/namespaces/' + child['metadata']['namespace']
                        + '/replicasets/' + child['metadata']['name']] = copy.deepcopy(child)

    for row in roots.values():
        if row['kind'] == 'CronJob':
            namespace = row['metadata']['namespace']
            documents.setdefault('/apis/batch/v1/namespaces/' + namespace + '/jobs',
                {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '501'}, 'items': []})

    def observe():
        try:
            return module.observe_shared_pool_recovery_processes(bindings=bindings, state_dir=state,
                kubeconfig=Path(probes.prefix[probes.prefix.index('--kubeconfig') + 1]), executable=Path(probes.prefix[0]))
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('shared pool recovery process observation unqualified: ' + ', '.join(frames)) from None

    error = 'shared pool recovery process observation unqualified'
    with pytest.raises(ValueError, match=error):
        observe()  # No recovery marker exists.
    kwargs['action'], api.block = 'recover', 'recovery-fence'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    for row in documents.values():
        if row['kind'] == 'PodList':
            row['items'] = [pod for pod in row['items'] if pod['metadata']['name'] == 'loom-postgres-0']
    checked.clear()
    assert observe().pending_workloads == ()  # Crashed apps cannot prevent fencing.
    assert not checked  # No application or SQL health challenge before restoration.
    api.block = 'recovery-stop'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    key = next(key for key in record['resources'] if roots[key]['kind'] == 'Deployment')
    # A later root cannot borrow pending permission from another root's intent.
    assert observe().pending_workloads == ()
    for selected in stop._sources(bindings, state):
        before = _snapshot(current(selected))
        before['metadata']['uid'] = _uid(current(selected))
        expected = stop._stopped(bindings, before)
        record['resources'][selected].update(status='intent', before=before,
            before_resource_version=current(selected)['metadata']['resourceVersion'], expected=expected)
        private_state._atomic_json(journal, record)
        assert observe().pending_workloads == (selected,)
        replace(selected, expected)
        descendants()
        assert observe().pending_workloads == ()
        record['resources'][selected]['status'] = 'stopped'
        private_state._atomic_json(journal, record)
    # A stale stopped status cannot mask a currently lingering Pod.
    namespace = roots[key]['metadata']['namespace']
    pod_path = '/api/v1/namespaces/' + namespace + '/pods'
    lingering = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'lingering', 'namespace': namespace,
        'uid': str(uuid4()), 'labels': copy.deepcopy(roots[key]['spec']['selector']['matchLabels'])},
        'spec': copy.deepcopy(roots[key]['spec']['template']['spec']), 'status': {'phase': 'Running'}}
    documents[pod_path]['items'].append(lingering)
    with pytest.raises(ValueError, match=error):
        observe()
    documents[pod_path]['items'].remove(lingering)
    api.block = 'recovery-restart'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-restart'
    originals, targets = restart._inputs(bindings, state)
    for selected, original in originals.items():
        replace(selected, original)
    descendants()
    assert observe().pending_workloads == ()
    journal = state / 'recovery-restart/restart.json'
    record = _read(journal)
    from scripts.ops import nebius_shared_pool_recovery_collector as collector

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(collector, 'observe_restored_staging_collector', collection_health)
        for selected, wanted in targets.items():
            changed = _stable(originals[selected]) != _stable(wanted)
            if changed:
                record['resources'][selected].update(status='intent',
                    before_resource_version=current(selected)['metadata']['resourceVersion'])
                private_state._atomic_json(journal, record)
                assert observe().pending_workloads == (selected,)
                replace(selected, wanted)
                if wanted['kind'] == 'Deployment':
                    assert observe().pending_workloads == (selected,)  # Spec alone is not Pod readiness.
                descendants()
                if wanted['kind'] == 'CronJob':
                    mode['collector_ready'] = False
                    assert observe().pending_workloads == (selected,)
                    mode['collector_ready'] = True
                assert observe().pending_workloads == ()
            record['resources'][selected].update(status='started', before_resource_version='701' if changed else None)
            private_state._atomic_json(journal, record)
        assert observe().pending_workloads == ()
        assert collector_calls and ('settings', staging.namespace, 'loom-control-plane') in checked
        assert ('database', staging.namespace, 'loom-service') in checked
        mode['collector_ready'] = False
        with pytest.raises(ValueError, match=error):
            observe()  # Completed restoration cannot borrow another phase's wait.
        mode['collector_ready'] = True
        before = copy.deepcopy(documents)
        def late_backend():
            pod, = (row for row in documents['/api/v1/namespaces/' + staging.namespace + '/pods']['items']
                if row['metadata']['name'] == 'loom-postgres-0')
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['process_hook'] = late_backend
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        documents.clear()
        documents.update(before)
        # A service restart during the earlier collector challenge is not
        # covered by that callback's controller/database checks. The aggregate
        # selected-process readback must reject the changed service too.
        before = copy.deepcopy(documents)
        def late_service():
            pod, = (row for row in documents['/api/v1/namespaces/' + staging.namespace + '/pods']['items']
                if row['metadata']['name'] == 'loom-service-a12b34-test')
            pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['process_hook'] = late_service
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        documents.clear()
        documents.update(before)
        api.block = None
        assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
        assert observe().pending_workloads == ()  # Terminal replay constructs no active child.
        raw = journal.read_bytes()
        mode['process_hook'] = lambda: journal.write_bytes(raw + b'\n')
        with pytest.raises(ValueError, match=error):
            observe()
        mode['process_hook'] = None
        journal.write_bytes(raw)
        assert observe().pending_workloads == ()
        captured = bindings.plan.staging.originals
        original = copy.deepcopy(captured)
        mode['process_hook'] = captured.clear
        try:
            with pytest.raises(ValueError, match=error):
                observe()  # Caller-owned nested bindings cannot change mid-read.
        finally:
            captured.update(original)
            mode['process_hook'] = None
        _exercise_connected_parent_terminal_recovery(kwargs=kwargs, state=state, documents=documents, mode=mode)


def _exercise_connected_parent_terminal_recovery(*, kwargs, state, documents, mode):
    """Real concrete replay, SQL/process readers and permission reviews; cloud/census I/O are controlled boundaries."""
    import json
    import ssl
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_shared_pool_api as module
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_shared_pool_handoff_live import (
        SharedPoolWriterObservation,
        _authority_roots,
    )

    bindings = kwargs['bindings']
    request = kwargs['runtime']
    authority = _authority_roots(bindings.plan)
    for row in authority.values():
        row['metadata'].setdefault('resourceVersion', '501')
    reviews = []

    def handle(message):
        if message.method == 'GET':
            plural = message.url.path.rsplit('/', 1)[-1]
            kind = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole',
                'clusterrolebindings': 'ClusterRoleBinding'}[plural]
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                'metadata': {'resourceVersion': '501'}, 'items': [row for row in authority.values() if row['kind'] == kind]})
        assert message.method == 'POST' and message.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
        principal = message.headers['Impersonate-User']
        account_namespace, account = principal.split(':')[2:]
        namespace = json.loads(message.content)['spec']['namespace']
        reviews.append((account_namespace, account, namespace))
        rules = []
        # Simulate Kubernetes' effective rights from this independent fixture
        # inventory; the production validator still sees actual review documents.
        for binding in authority.values():
            if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                continue
            if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
                continue
            if not any(subject['kind'] == 'ServiceAccount' and subject['name'] == account
                    and subject['namespace'] == account_namespace for subject in binding.get('subjects', [])):
                continue
            reference = binding['roleRef']
            selected = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
            rules.extend(copy.deepcopy(authority[selected]['rules']))
        if mode.get('parent_permission_failure') == account:
            rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['deletecollection']})
        return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {},
            'status': {'incomplete': False, 'evaluationError': '', 'resourceRules': rules, 'nonResourceRules': []}})

    async def publication(**options):
        assert options['request'] == request

    async def provider(**options):
        assert options['runtime'] == request and options['development'].shared_bindings == bindings
        return SimpleNamespace(plan=bindings.plan)

    def census(**options):
        assert options['bindings'] == bindings and options['state_dir'] == state
        return SharedPoolWriterObservation(bindings.plan, '501', {}, copy.deepcopy(authority), bindings.input_digest)

    def active_child(**options):
        pytest.fail('terminal replay must not construct active phase adapters')

    operator = state.parent / 'terminal-operator-connection'
    operator.write_bytes(b'private-terminal-operator')
    operator.chmod(0o600)
    context = ssl.create_default_context(cadata=request.database.foundation.bootstrap['material']['loom-platform-db']['ca.crt'])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(module, 'qualify_runtime_publication', publication)
        patch.setattr(module, 'observe_shared_pool_provider', provider)
        patch.setattr(module, 'observe_shared_pool_recovery_writers', census)
        for component, name in ((module.recovery_gateway, 'HTTPSSharedRecoveryGateway'),
                (module.recovery_roles, 'HTTPSSharedRecoveryRoles'), (module.restart, 'HTTPSSharedRecoveryRestart')):
            patch.setattr(component, name, active_child)
        with module.HTTPSSharedPoolOperationAPI(bindings=bindings, observation=kwargs['observation'], repo_root=ROOT,
                api_server=request.database.foundation.inputs.config['kubernetes_api_server'], ssl_context=context,
                token='operator-test', operator_credentials=operator, private_files={operator: operator.read_bytes()}) as api:
            api.client.close()
            api.client = type(api.client)(base_url=api.api_server, transport=httpx.MockTransport(handle))
            frame = _read(state / 'operation.json')
            def qualify():
                try:
                    api.qualify(bindings, frame, recovering=True)
                except ValueError as exc:
                    import traceback

                    frames, cause = [], exc
                    while cause is not None:
                        frames.extend(f'{Path(item.filename).name}:{item.lineno}:{item.name}'
                            for item in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('terminal parent failure at ' + ', '.join(frames)) from None
            qualify()
            assert reviews and any(account == 'loom-pool-gateway' for _, account, _ in reviews)
            assert any(account == 'loom-platform' for _, account, _ in reviews)
            for account in ('loom-pool-gateway', 'loom-platform'):
                mode['parent_permission_failure'] = account
                with pytest.raises(ValueError):
                    qualify()
            mode['parent_permission_failure'] = None
            for phase in api.supported_phases:
                with pytest.raises(ValueError):
                    api.advance(phase, bindings, state / phase)
            before = copy.deepcopy(documents)
            def late_dev_backend():
                pod, = (row for row in documents['/api/v1/namespaces/loom-dev/pods']['items']
                    if row['metadata']['name'] == 'loom-postgres-0')
                pod['status']['containerStatuses'][0]['restartCount'] += 1
            mode['process_hook'] = late_dev_backend
            try:
                with pytest.raises(ValueError):
                    qualify()  # All retained DBs must remain pinned across the process/collector reads.
            finally:
                mode['process_hook'] = None
                documents.clear()
                documents.update(before)


def _exercise_scoped_process_observation(*, development, documents, state, calls, catalog_path):
    """Bound API traffic without reusing live readiness or changed history."""
    runtime_state = development.shared_bindings.plan.development.runtime.state_dir
    key = 'Deployment:loom-dev:loom-control-plane'
    workload_path = '/apis/apps/v1/namespaces/loom-dev/deployments/loom-control-plane'

    def running():
        return development._running(key, runtime_state)

    before = len(calls)
    with development.process_observation(state_dir=runtime_state):
        first = running()
        assert running() == first
        development.qualify_runtime_settings(key=key, state_dir=runtime_state)
        development.qualify_runtime_database(key=key, state_dir=runtime_state)
    assert sum(call.url.path == catalog_path for call in calls[before:]) == 2
    assert sum(call.url.path == workload_path for call in calls[before:]) >= 12
    for path in (state / 'forward-guards/guards.json', runtime_state / 'installation.json'):
        raw = path.read_bytes()
        try:
            with pytest.raises(ValueError):
                with development.process_observation(state_dir=runtime_state):
                    running()
                    path.write_bytes(raw + b'\n')
                    running()  # Semantically equal receipt bytes are still changed.
        finally:
            path.write_bytes(raw)
            path.chmod(0o600)
        assert running()[0] == 'controller'  # Failed scopes never leak their view.
    for path in (catalog_path, workload_path):
        saved = copy.deepcopy(documents[path])
        try:
            with pytest.raises(ValueError):
                with development.process_observation(state_dir=runtime_state):
                    running()
                    documents[path]['metadata']['uid'] = str(uuid4())
                    if path == workload_path:
                        running()  # Target identity is checked on every nested read.
                    # Unrelated inventory drift must be rejected at scope exit.
        finally:
            documents[path] = saved
    original = copy.deepcopy(development.shared_bindings.plan.staging.originals)
    try:
        with pytest.raises(ValueError):
            with development.process_observation(state_dir=runtime_state):
                development.shared_bindings.plan.staging.originals.clear()
                running()
    finally:
        development.shared_bindings.plan.staging.originals.update(original)
    with development.process_observation(state_dir=runtime_state):
        with pytest.raises(ValueError):
            with development.process_observation(state_dir=runtime_state):
                pytest.fail('nested observation must not replace the active scope')
        assert running()[0] == 'controller'


def _exercise_forward_processes(*, bindings, development, documents, mode, checked, commands, add, calls, args,
        activation_transport=False, observation_only=False):
    """Real parent, process transports and readiness validators; no live effects."""
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_build_activation as build_child
    from scripts.ops import nebius_shared_pool_database as database
    from scripts.ops import nebius_shared_pool_gateway as gateway_child
    from scripts.ops import nebius_shared_pool_guards as guards
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_workloads as workloads
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_stage import _comparison_snapshot, _stage_fixed_documents
    from tests.ops.test_nebius_development_pool_retained import populate_runtime_pods

    from loom.nebius_platform_render import digest

    name = 'scripts.ops.nebius_shared_pool_forward_processes'
    assert importlib.util.find_spec(name) is not None, 'phase-bound shared process observation is missing'
    module = __import__(name, fromlist=['observe_shared_pool_forward_processes'])
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    class ProcessPhaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, child):
            if phase == 'forward-activate':
                if self.prepare_failure == (phase, 'before'):
                    raise ValueError('simulated preparation interruption')
                activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=child)
            elif phase == 'forward-guards':
                guards.prepare_shared_pool_guards(bindings=bound, state_dir=child)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=child)
            elif phase == 'forward-database':
                database.prepare_shared_staging_database(bindings=bound, state_dir=child)
            elif phase == 'forward-build-policy':
                build_child.prepare_shared_build_activation(bindings=bound, state_dir=child)
            elif phase == 'forward-gateway':
                gateway_child.prepare_shared_gateway(bindings=bound, state_dir=child)
            else:
                super().prepare(phase, bound, child)

        def validate_prepared_phase(self, phase, child):
            if phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-guards':
                value = guards.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=False)
                assert set(value['guards'].values()) == {'prepared'}
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase,
                    state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-database':
                database.validate_shared_staging_database(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-build-policy':
                build_child.validate_shared_build_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, child)

        def validate_phase(self, phase, child, *, complete):
            if phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-guards':
                guards.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=complete)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=child, complete=complete)
            elif phase == 'forward-database':
                database.validate_shared_staging_database(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-build-policy':
                build_child.validate_shared_build_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=complete)
            else:
                super().validate_phase(phase, child, complete=complete)

        def advance(self, phase, bound, child):
            if phase == 'forward-activate':
                record = json.loads((child / 'activation.json').read_bytes())
                return record['opening'] == 'opened' and all(row['release'] == 'released' for row in record['guards'].values())
            if phase == 'forward-guards':
                return set(json.loads((child / 'guards.json').read_bytes())['guards'].values()) == {'held'}
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                return all(row['status'] == 'applied' for row in json.loads((child / 'transition.json').read_bytes())['resources'].values())
            if phase == 'forward-database':
                return json.loads((child / 'database.json').read_bytes())['status'] == 'staged'
            if phase == 'forward-build-policy':
                return json.loads((child / 'build-policy.json').read_bytes())['status'] == 'activated'
            if phase == 'forward-gateway':
                return all(row['status'] == 'applied' for row in json.loads((child / 'start/transition.json').read_bytes())['resources'].values())
            return super().advance(phase, bound, child)

    api = ProcessPhaseAPI(parent)
    api.block = 'forward-roles'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-guards'
    originals = bindings.plan.staging.originals
    for row in originals.values():
        if row['kind'] == 'CronJob':
            add(row)
        namespace = row['metadata']['namespace']
        documents.setdefault('/apis/batch/v1/namespaces/' + namespace + '/jobs',
            {'apiVersion': 'batch/v1', 'kind': 'JobList', 'metadata': {'resourceVersion': '1'}, 'items': []})

    def observe():
        try:
            return module.observe_shared_pool_forward_processes(bindings=bindings, state_dir=state, development=development,
                kubeconfig=Path(development.prefix[development.prefix.index('--kubeconfig') + 1]),
                executable=Path(development.prefix[0]))
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('forward process failure at ' + ', '.join(frames)) from None

    checked.clear()
    reads_before = len(calls)
    assert observe().pending_workloads == ()
    # Each whole inventory reads this unrelated retained ConfigMap. Repeating
    # the full inventory for every nested process challenge exhausted the
    # existing timeout and amplifies real API traffic as components increase.
    catalog_path = next(path for path, row in documents.items() if row['kind'] == 'ConfigMap')
    assert sum(call.url.path == catalog_path for call in calls[reads_before:]) <= 4
    assert {'new-closed', 'old-revoked', ('runtime-role', 'loom-dev')} <= checked
    if observation_only:
        saved = copy.deepcopy(documents)
        inventory_reads = 0
        def late_restart(message):
            nonlocal inventory_reads
            if message.url.path == catalog_path:
                inventory_reads += 1
                if inventory_reads == 4:
                    pod, = (row for row in documents['/api/v1/namespaces/' + manager.binding.namespace + '/pods']['items']
                        if row['metadata']['name'] == 'loom-postgres-0')
                    pod['status']['containerStatuses'][0]['restartCount'] += 1
        mode['http_hook'] = late_restart
        try:
            with pytest.raises(ValueError):
                observe()  # The last inventory scan must precede the final DB identity check.
            assert inventory_reads == 4
        finally:
            mode['http_hook'] = None
            documents.clear()
            documents.update(saved)
        _exercise_scoped_process_observation(development=development, documents=documents,
            state=state, calls=calls, catalog_path=catalog_path)
        return
    guard_path = state / 'forward-guards/guards.json'
    guard_record = json.loads(guard_path.read_bytes())
    first = bindings.migration.guards[0]
    mode['held'] = {first.namespace}
    with pytest.raises(ValueError):
        observe()  # An unrecorded held guard is not owned by this operation.
    guard_record['guards'][str(first.participant_id)] = 'acquire_intent'
    private_state._atomic_json(guard_path, guard_record)
    assert observe().guard_states[first.namespace] == 'held'
    for target in bindings.migration.guards:
        mode['held'].add(target.namespace)
        guard_record['guards'][str(target.participant_id)] = 'held'
    private_state._atomic_json(guard_path, guard_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'
    assert observe().pending_workloads == ()

    def path_for(row):
        prefix = '/apis/apps/v1' if row['kind'] == 'Deployment' else '/apis/batch/v1'
        plural = 'deployments' if row['kind'] == 'Deployment' else 'cronjobs'
        return prefix + '/namespaces/' + row['metadata']['namespace'] + '/' + plural + '/' + row['metadata']['name']

    def replace_live(row, wanted):
        identity = {key: row['metadata'][key] for key in ('uid', 'resourceVersion')}
        generation = row['metadata'].get('generation', 1) + 1
        row.clear()
        row.update(copy.deepcopy(wanted))
        row['metadata'].update(**identity, generation=generation)
        row['status'] = {'observedGeneration': generation}

    stop_path = state / 'forward-stop/transition.json'
    stop_record = json.loads(stop_path.read_bytes())
    lingering = None
    for key, wanted in bindings.plan.staging.stopped.items():
        wanted = _comparison_snapshot(wanted)
        stop_record['resources'][key].update(status='intent', expected=wanted)
        private_state._atomic_json(stop_path, stop_record)
        current = documents[path_for(wanted)]
        replace_live(current, wanted)
        namespace = current['metadata']['namespace']
        if current['kind'] == 'Deployment':
            pod_path = '/api/v1/namespaces/' + namespace + '/pods'
            labels = current['spec']['selector']['matchLabels']
            selected = [pod for pod in documents[pod_path]['items']
                if all(pod['metadata'].get('labels', {}).get(k) == v for k, v in labels.items())]
            assert selected
            selected[0]['metadata']['deletionTimestamp'] = '2026-10-10T00:00:00Z'
            assert key in observe().pending_workloads  # Zero replicas alone is not drain.
            lingering = (key, pod_path, copy.deepcopy(selected[0]))
            documents[pod_path]['items'] = [pod for pod in documents[pod_path]['items'] if pod not in selected]
            for replica in documents['/apis/apps/v1/namespaces/' + namespace + '/replicasets']['items']:
                if any(owner['uid'] == current['metadata']['uid'] for owner in replica['metadata'].get('ownerReferences', [])):
                    replica['spec']['replicas'] = 0
                    replica['status'] = {'observedGeneration': replica['metadata']['generation']}
        stop_record['resources'][key]['status'] = 'applied'
        private_state._atomic_json(stop_path, stop_record)
    assert observe().pending_workloads == ()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
    before = len(commands)
    assert observe().pending_workloads == ()
    namespaces = {row['metadata']['namespace'] for row in originals.values()}
    assert not any(row[0] == 'exec' and row[2] in namespaces and row[7] == 'python' for row in commands[before:])
    key, pod_path, pod = lingering
    documents[pod_path]['items'].append(pod)
    with pytest.raises(ValueError):
        observe()  # Once stop is complete, new lingering processes are not a resumable wait.
    documents[pod_path]['items'].remove(pod)
    mode.update(damage='late-stopped-pod', manager_reads=0, late_pod=(pod_path, pod))
    with pytest.raises(ValueError):
        observe()  # A Pod appearing after its drain check invalidates the whole observation.
    documents[pod_path]['items'] = [row for row in documents[pod_path]['items'] if row['metadata']['uid'] != pod['metadata']['uid']]
    mode['damage'] = None
    api.block = 'forward-material'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
    db_path = state / 'forward-database/database.json'
    db_record = json.loads(db_path.read_bytes())
    db_record['status'] = 'staged'
    private_state._atomic_json(db_path, db_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    checked.clear()
    observe()
    assert ('runtime-role', bindings.plan.staging.attachment.guard.namespace) in checked
    api.block = 'forward-build-policy'
    for phase in ('forward-wire', 'forward-start'):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        if phase == 'forward-start':
            _, pod_path, pod = lingering
            documents[pod_path]['items'].append(pod)
            with pytest.raises(ValueError):
                observe()  # A still-stopped resource cannot become pending again during another resource's startup.
            documents[pod_path]['items'].remove(pod)
        journal = state / phase / 'transition.json'
        record = json.loads(journal.read_bytes())
        for key, item in record['resources'].items():
            current = documents[path_for(originals[key])]
            wanted = _comparison_snapshot(bindings.plan.staging_targets[key] if phase == 'forward-wire' else current)
            if phase == 'forward-start':
                wanted['spec']['replicas'] = originals[key]['spec']['replicas']
            item.update(status='intent', expected=wanted)
            private_state._atomic_json(journal, record)
            replace_live(current, wanted)
            if phase == 'forward-start':
                mode['shared_settings'] = True
                assert key in observe().pending_workloads
                runtime = {path_for(current): current}
                populate_runtime_pods(runtime)
                for path, value in runtime.items():
                    if value['kind'] in {'PodList', 'ReplicaSetList'}:
                        # This fixture reuses a deterministic hash: collect an
                        # old zero replica before creating its same-name successor.
                        names = {row['metadata']['name'] for row in value['items']}
                        documents[path]['items'] = [row for row in documents[path]['items'] if row['metadata']['name'] not in names]
                        documents[path]['items'].extend(value['items'])
                        for row in value['items']:
                            if row['kind'] == 'ReplicaSet':
                                add(row)
                            else:
                                row['spec']['nodeName'] = 'platform-test'
            assert key not in observe().pending_workloads
            item['status'] = 'applied'
            private_state._atomic_json(journal, record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    checked.clear()
    assert observe().pending_workloads == ()
    for row in originals.values():
        if row['kind'] == 'Deployment':
            assert ('settings', row['metadata']['namespace'], row['metadata']['name']) in checked
            assert ('database', row['metadata']['namespace'], row['metadata']['name']) in checked
    # The activation variant reuses the real transition setup, not the already
    # covered process fault matrices; its own faults exercise opening/release.
    for damage in (() if activation_transport else (
            'old-active', 'new-not-closed', 'settings:' + bindings.plan.staging.attachment.guard.namespace,
            'database:loom-dev', 'late-staging-actuator', 'late-staging-restart', 'late-dev-restart', 'late-staging-template')):
        baseline = copy.deepcopy(documents)
        mode['damage'] = damage
        mode['manager_reads'] = 0
        with pytest.raises(ValueError):
            observe()
        documents.clear()
        documents.update(baseline)
    mode['damage'] = None
    build_path = state / 'forward-build-policy/build-policy.json'
    build_record = json.loads(build_path.read_bytes())
    build_record['status'] = 'activated'
    private_state._atomic_json(build_path, build_record)
    plan = bindings.plan.development
    build_namespace = '/api/v1/namespaces/' + plan.build_namespace_original['metadata']['name']
    namespace = copy.deepcopy(plan.build_namespace_target)
    namespace['metadata']['uid'] = plan.build_namespace_original['metadata']['uid']
    documents[build_namespace] = namespace
    api.block = 'forward-gateway'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    checked.clear()
    assert observe().pending_workloads == ()
    gateway_key = _key(plan.gateway_original)
    gateway_path = path_for(plan.gateway_original)
    gateway = documents[gateway_path]
    original_gateway = copy.deepcopy(gateway)
    started = _comparison_snapshot(plan.gateway_target)
    replace_live(gateway, started)
    with pytest.raises(ValueError):
        observe()  # A planned but unrecorded start is not process authority.
    gateway.clear()
    gateway.update(original_gateway)
    lingering_gateway = {gateway_path: copy.deepcopy(gateway)}
    lingering_gateway[gateway_path]['spec']['replicas'] = 1
    populate_runtime_pods(lingering_gateway)
    pod_path = '/api/v1/namespaces/' + manager.binding.namespace + '/pods'
    lingering_pod, = lingering_gateway[pod_path]['items']
    documents[pod_path]['items'].append(lingering_pod)
    with pytest.raises(ValueError):
        observe()  # Zero replicas cannot excuse a live gateway descendant.
    documents[pod_path]['items'].remove(lingering_pod)
    fixed, stored = plan.authority, {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def grants(message):
        if message.method == 'GET':
            key = paths[message.url.path]
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        row = json.loads(message.content)
        key = _key(row)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        row['metadata'].update(uid=str(uuid4()), resourceVersion='501')
        if message.url.params.get('dryRun') != 'All':
            assert not message.url.query and key not in stored
            stored[key] = row
        return httpx.Response(201, json=row)

    with gateway_child.HTTPSSharedGateway(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None,
            observe_ready=lambda *_: False) as delivery:
        delivery.client.close()
        delivery.client = type(development.client)(base_url=delivery.api_server, transport=httpx.MockTransport(grants))
        _stage_fixed_documents(documents=fixed, revision=digest(fixed), phase='forward-gateway-authority',
            binding=delivery.binding, api=delivery, state_dir=state / 'forward-gateway/authority')
    start_path = state / 'forward-gateway/start/transition.json'
    start_record = json.loads(start_path.read_bytes())
    start_record['resources'][gateway_key].update(status='intent', expected=started)
    private_state._atomic_json(start_path, start_record)
    assert observe().pending_workloads == ()  # The PATCH may not have committed.
    replace_live(gateway, started)
    checked.clear()
    assert observe().pending_workloads == (gateway_key,)
    assert not any(isinstance(row, tuple) and row[-1] == 'loom-pool-gateway' for row in checked)
    runtime = {gateway_path: gateway}
    populate_runtime_pods(runtime)
    for path, value in runtime.items():
        if value['kind'] in {'PodList', 'ReplicaSetList'}:
            documents[path]['items'].extend(value['items'])
            for row in value['items']:
                if row['kind'] == 'ReplicaSet':
                    add(row)
                else:
                    row['spec']['nodeName'] = 'platform-test'
    checked.clear()
    assert observe().pending_workloads == ()
    assert {(check, manager.binding.namespace, 'loom-pool-gateway')
        for check in ('settings', 'database', 'kubernetes', 'capacity')} <= checked
    mode['gateway_receipt'] = start_path
    for damage in (() if activation_transport else ('gateway-settings', 'gateway-database', 'gateway-kubernetes', 'gateway-capacity',
            'late-gateway-secret', 'late-gateway-restart', 'late-gateway-database-restart', 'late-gateway-receipt',
            'late-manager-secret-after-gateway', 'late-manager-database-after-gateway', 'late-manager-database-at-final-state')):
        baseline, receipt = copy.deepcopy(documents), start_path.read_bytes()
        mode.update(damage=damage, capacity_seen=False, late_injected=False, final_state_reads=0)
        try:
            observe()
        except ValueError:
            pass
        else:
            pytest.fail(f'forward process observation accepted {damage}')
        if damage.startswith('late-manager-'):
            assert mode['late_injected'], damage
        documents.clear()
        documents.update(baseline)
        start_path.write_bytes(receipt)
        start_path.chmod(0o600)
    mode['damage'] = None
    start_record['resources'][gateway_key]['status'] = 'applied'
    private_state._atomic_json(start_path, start_record)
    api.prepare_failure = ('forward-activate', 'before')
    with pytest.raises(ValueError):
        parent.run_shared_pool_operation(**kwargs)
    assert observe().pending_workloads == ()
    documents[gateway_path]['status']['readyReplicas'] = 0
    with pytest.raises(ValueError):
        observe()  # A completed startup cannot claim an in-flight readiness wait.
    documents[gateway_path]['status']['readyReplicas'] = 1
    api.prepare_failure = None
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    if activation_transport:
        _exercise_shared_activation_transport(bindings=bindings, development=development, state=state,
            mode=mode, checked=checked, parent=parent, kwargs=kwargs, api=api)
        return
    assert observe().pool_state == 'closed'
    mode['pool_state'] = 'global'
    with pytest.raises(ValueError):
        observe()  # A live open pool cannot qualify before its opening intent.
    mode['pool_state'] = 'closed'
    activation_path = state / 'forward-activate/activation.json'
    activation = json.loads(activation_path.read_bytes())
    activation['opening'] = 'intent'
    private_state._atomic_json(activation_path, activation)
    assert observe().pool_state == 'closed'
    mode['pool_state'] = 'global'
    checked.clear()
    assert observe().pool_state == 'global'
    assert 'new-active' in checked and 'new-closed' not in checked
    assert ('active-capacity', manager.binding.namespace, 'loom-pool-gateway') in checked
    activation['opening'] = 'opened'
    for target in bindings.migration.guards:
        key = str(target.participant_id)
        activation['guards'][key]['release'] = 'intent'
        private_state._atomic_json(activation_path, activation)
        assert observe().guard_states[target.namespace] == 'held'
        mode['held'].remove(target.namespace)
        assert observe().guard_states[target.namespace] == 'open'
        activation['guards'][key]['release'] = 'released'
        private_state._atomic_json(activation_path, activation)
    api.block = 'forward-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    assert observe().pool_state == 'global'
    for damage in ('new-not-active', 'guard-foreign', 'gateway-active-capacity', 'old-active'):
        mode['damage'] = damage
        with pytest.raises(ValueError):
            observe()
    mode['damage'] = None
    guard_path.unlink()
    before = len(commands)
    with pytest.raises(ValueError):
        observe()
    assert len(commands) == before


def _exercise_shared_activation_transport(*, bindings, development, state, mode, checked, parent, kwargs, api):
    """Actual process/SQL/guard transports under the shared opening controller.

    The preceding helper provides real child receipts and remote-I/O doubles.
    The parent's separate writer/provider qualification is a boundary double
    here; it must be connected in the full protected parent, not claimed by this
    test. An unhealthy actual runtime must fail even when that boundary passes.
    """
    import importlib.util

    from scripts.ops.nebius_shared_pool_activation import advance_shared_pool_activation

    name = 'scripts.ops.nebius_shared_pool_activation_live'
    assert importlib.util.find_spec(name) is not None, 'concrete shared opening transport is missing'
    module = __import__(name, fromlist=['SharedPoolActivationTransport'])
    qualification = []
    mode['effects'] = []

    def qualify():
        qualification.append('writers-provider')
        if mode.get('qualification_failure'):
            raise ValueError('simulated lost parent writer/provider qualification')

    transport = module.SharedPoolActivationTransport(bindings=bindings, development=development, qualify=qualify)
    child = state / 'forward-activate'
    roster = tuple(str(row.participant_id) for row in bindings.migration.guards)
    for call in (transport.open_pool, lambda: transport.release_guard(roster[0]),
            transport.fence_pool, lambda: transport.fence_guard(roster[0])):
        with pytest.raises(ValueError):
            call()
    assert mode['effects'] == []
    mode['qualification_failure'] = True
    with pytest.raises(ValueError):
        advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert qualification and mode['effects'] == []
    mode['qualification_failure'] = False
    mode['damage'] = 'gateway-capacity'
    with pytest.raises(ValueError):
        advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == []
    mode['damage'] = None
    mode['release_failure'] = roster[0]
    assert not advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['pool_state'] == 'global'
    assert mode['effects'] == [('open', None), ('release', roster[0])]
    # Unknown release remains observation-only even after the transport returns.
    assert not advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == [('open', None), ('release', roster[0])]
    mode['held'].remove(bindings.migration.guards[0].namespace)  # Delayed commit becomes observable.
    mode['release_failure'] = None
    checked.clear()
    assert advance_shared_pool_activation(bindings=bindings, api=transport, state_dir=child)
    assert mode['effects'] == [('open', None), *(('release', key) for key in roster)]
    assert not mode['held'] and 'new-active' in checked and 'new-closed' not in checked
    assert any(isinstance(row, tuple) and row[0] == 'active-capacity' for row in checked)
    api.block = 'forward-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-verify'
    with pytest.raises(ValueError):
        transport.open_pool()  # Completed child is no longer dispatch authority.
    assert mode['effects'] == [('open', None), *(('release', key) for key in roster)]


def _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode, *,
        recovery=False, participant=False, publisher_cloud=None):
    """Observe real parent/child receipts; remote transitions are fixture state.

    Accepting an unrecorded successor, an old template after completion, or a
    changed receipt during the census must fail independently of the mutator.
    """
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_activation as activation_child
    from scripts.ops import nebius_shared_pool_gateway as gateway_child
    from scripts.ops import nebius_shared_pool_guards as guard_child
    from scripts.ops import nebius_shared_pool_material as material
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_roles as roles
    from scripts.ops import nebius_shared_pool_workloads as workloads
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_management_stage import _comparison_snapshot, _stage_fixed_documents

    from loom.nebius_platform_render import digest

    # Later lifecycle cases need genuine forward receipts, not another copy of
    # the forward fault matrix. Keep every assertion in the forward case; only
    # its extra census probes are omitted while arranging recovery/release.
    check_forward = not recovery and not participant
    name = 'scripts.ops.nebius_shared_pool_forward_census'
    assert importlib.util.find_spec(name) is not None, 'phase-bound shared writer census is missing'
    module = __import__(name, fromlist=['observe_shared_pool_forward_writers'])
    if recovery:
        assert importlib.util.find_spec('scripts.ops.nebius_shared_pool_recovery_census') is not None, 'recovery-bound writer census is missing'
    args, bindings = shared_operation
    manager = args['runtime'].database.manager.retained.request.retained
    state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    class CensusPhaseAPI(SharedPhaseAPI):
        def prepare(self, phase, bound, child):
            if phase == 'forward-guards':
                guard_child.prepare_shared_pool_guards(bindings=bound, state_dir=child)
            elif phase == 'forward-activate':
                activation_child.prepare_shared_pool_activation(bindings=bound, state_dir=child)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.prepare_shared_staging_workloads(bindings=bound, phase=phase, state_dir=child)
            elif phase == 'forward-roles':
                roles.prepare_shared_staging_roles(bindings=bound, state_dir=child)
            elif phase == 'forward-material':
                material.prepare_shared_staging_material(bindings=bound, state_dir=child)
            elif phase == 'forward-gateway':
                gateway_child.prepare_shared_gateway(bindings=bound, state_dir=child)
            else:
                super().prepare(phase, bound, child)

        def validate_prepared_phase(self, phase, child):
            if phase == 'forward-guards':
                guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=False)
            elif phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase,
                    state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-roles':
                roles.validate_shared_staging_roles(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-material':
                material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False, prepared=True)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, child)

        def validate_phase(self, phase, child, *, complete):
            if phase == 'forward-guards':
                guard_child.validate_shared_pool_guards(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-activate':
                activation_child.validate_shared_pool_activation(bindings=bindings, state_dir=child, complete=complete)
            elif phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                workloads.validate_shared_staging_workloads(bindings=bindings, phase=phase, state_dir=child, complete=complete)
            elif phase == 'forward-roles':
                roles.validate_shared_staging_roles(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-material':
                material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=complete)
            elif phase == 'forward-gateway':
                gateway_child.validate_shared_gateway(bindings=bindings, state_dir=child, complete=complete)
            else:
                super().validate_phase(phase, child, complete=complete)

        def advance(self, phase, bound, child):
            if phase == 'forward-guards':
                path = child / 'guards.json'
                record = json.loads(path.read_bytes())
                record['guards'].update(dict.fromkeys(record['guards'], 'held'))
                private_state._atomic_json(path, record)
                return True
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                return all(row['status'] == 'applied' for row in json.loads((child / 'transition.json').read_bytes())['resources'].values())
            if phase == 'forward-roles':
                return all(status == 'restricted' for status in json.loads((child / 'roles.json').read_bytes())['roles'].values())
            if phase == 'forward-material':
                child_state = material.validate_shared_staging_material(bindings=bindings, state_dir=child, complete=False)
                return child_state['stage'] is not None and all(row['status'] == 'created'
                    for row in child_state['stage']['resources'].values())
            if phase == 'forward-gateway':
                return all(row['status'] == 'applied' for row in json.loads((child / 'start/transition.json').read_bytes())['resources'].values())
            if phase == 'forward-activate':
                return getattr(self, 'activation_complete', False)
            return super().advance(phase, bound, child)

    api = CensusPhaseAPI(parent)
    api.block = 'forward-database'
    kwargs = {**args, 'bindings': bindings, 'api': api, 'action': 'install'}
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-stop'

    def observe():
        try:
            return module.observe_shared_pool_forward_writers(bindings=bindings, state_dir=state, transport=transport)
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            raise ValueError('census failure at ' + ', '.join(frames)) from None

    initial = observe()
    assert initial.resource_version == '500'
    journal = state / 'forward-stop/transition.json'
    stopped = json.loads(journal.read_bytes())
    key = next(key for key, row in bindings.plan.staging.originals.items() if row['kind'] == 'Deployment')
    actual = next(row for row in inventory['deployments'] if _key(row) == key)
    original = copy.deepcopy(actual)
    expected = _comparison_snapshot(bindings.plan.staging.stopped[key])

    def replace_live(actual, wanted):
        identity = {field: actual['metadata'][field] for field in ('uid', 'resourceVersion')}
        actual.clear()
        actual.update(copy.deepcopy(wanted))
        actual['metadata'].update(identity)

    if check_forward:
        replace_live(actual, expected)
        with pytest.raises(ValueError):
            observe()  # No intent exists yet: matching a planned target is insufficient.
        replace_live(actual, original)
        stopped['resources'][key].update(status='intent', expected=expected)
        private_state._atomic_json(journal, stopped)
        assert observe().workloads[key] == original
        replace_live(actual, expected)
        assert observe().workloads[key] == actual  # A lost PATCH reply permits either recorded side.
        actual['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
        with pytest.raises(ValueError):
            observe()
        replace_live(actual, expected)
        raw = journal.read_bytes()
        journal.unlink()
        before = len(calls)
        with pytest.raises(ValueError):
            observe()
        assert len(calls) == before
        journal.write_bytes(raw)
        journal.chmod(0o600)
        mode['during_read'] = journal.unlink
        with pytest.raises(ValueError):
            observe()  # Started children are not covered by completed-child byte pins.
        journal.write_bytes(raw)
        journal.chmod(0o600)

    for key, target in bindings.plan.staging.stopped.items():
        wanted = _comparison_snapshot(target)
        stopped['resources'][key].update(status='applied', expected=wanted)
        row = next(row for row in inventory[kinds[target['kind']]] if _key(row) == key)
        replace_live(row, wanted)
    private_state._atomic_json(journal, stopped)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-roles'
    observe()
    key = min(bindings.plan.staging_role_originals)
    role = next(row for row in inventory['roles'] if _key(row) == key)
    original_role = copy.deepcopy(role)
    target_role = bindings.plan.staging_role_targets[key]
    role_path = state / 'forward-roles/roles.json'
    role_record = json.loads(role_path.read_bytes())
    if check_forward:
        replace_live(role, target_role)
        with pytest.raises(ValueError):
            observe()
        role_record['roles'][key] = 'intent'
        private_state._atomic_json(role_path, role_record)
        assert observe().authority[key] == role
        replace_live(role, original_role)
        assert observe().authority[key] == original_role
    for key, target in bindings.plan.staging_role_targets.items():
        role_record['roles'][key] = 'restricted'
        row = next(row for row in inventory['roles'] if _key(row) == key)
        replace_live(row, target)
    private_state._atomic_json(role_path, role_record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-database'
    observe()
    if check_forward:
        successor = copy.deepcopy(role)
        replace_live(role, original_role)
        with pytest.raises(ValueError):
            observe()  # Completion must not retain the old write grant as an alternative.
        replace_live(role, successor)
        reader = copy.deepcopy(next(iter(bindings.plan.staging_reader_authority.values())))
        reader['metadata'].update(uid=str(uuid4()), resourceVersion='401')
        inventory[kinds[reader['kind']]].append(reader)
        with pytest.raises(ValueError):
            observe()  # Even planned new authority is foreign until its retained create intent.
        inventory[kinds[reader['kind']]].remove(reader)

    api.block = 'forward-build-policy'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-material'
    observe()
    fixed = {**bindings.plan.staging_material, **bindings.plan.staging_reader_authority}
    stored = {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}

    def material_request(message):
        if message.method == 'GET':
            key = paths[message.url.path]
            return httpx.Response(200, json=stored[key]) if key in stored else httpx.Response(404)
        assert message.method == 'POST'
        row = json.loads(message.content)
        key = _key(row)
        assert key in fixed and message.url.path == runtime_resource_path(fixed[key])
        row['metadata'].update(uid=str(uuid4()), resourceVersion='401')
        if message.url.params.get('dryRun') != 'All':
            assert not message.url.query and key not in stored
            stored[key] = row
            if key in bindings.plan.staging_reader_authority or key in bindings.plan.development.authority:
                inventory[kinds[row['kind']]].append(row)
                if check_forward:
                    observe()  # The child still has create_intent, including an unobserved UID.
        return httpx.Response(201, json=row)

    with material.HTTPSSharedStagingMaterial(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(),
            qualify_runtime=lambda *_: observe() if check_forward else None) as delivery:
        delivery.client.close()
        delivery.client = type(transport.client)(base_url=delivery.api_server, transport=httpx.MockTransport(material_request))
        try:
            assert material.advance_shared_staging_material(bindings=bindings, api=delivery, state_dir=state / 'forward-material')
        except ValueError as exc:
            import traceback

            frames, cause = [], exc
            while cause is not None:
                frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                    for frame in traceback.extract_tb(cause.__traceback__))
                cause = cause.__context__
            pytest.fail('census material integration failed at ' + ', '.join(frames), pytrace=False)
    result = observe()
    assert set(bindings.plan.staging_reader_authority) <= result.authority.keys()
    if check_forward:
        for key in bindings.plan.staging_reader_authority:
            row = stored[key]
            original_uid = row['metadata']['uid']
            row['metadata']['uid'] = str(uuid4())
            with pytest.raises(ValueError):
                observe()  # A completed create no longer permits a same-name replacement.
            row['metadata']['uid'] = original_uid

    for phase in ('forward-wire', 'forward-start'):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        observe()
        path = state / phase / 'transition.json'
        record = json.loads(path.read_bytes())
        for key, item in record['resources'].items():
            row = next(row for row in inventory['deployments'] if _key(row) == key)
            wanted = _comparison_snapshot(bindings.plan.staging_targets[key] if phase == 'forward-wire' else row)
            if phase == 'forward-start':
                wanted['spec']['replicas'] = bindings.plan.staging.originals[key]['spec']['replicas']
            item.update(status='intent', expected=wanted)
            private_state._atomic_json(path, record)
            replace_live(row, wanted)
            observe()
            item['status'] = 'applied'
            private_state._atomic_json(path, record)
        observe()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-build-policy'
    observe()
    api.block = 'forward-gateway'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-gateway'
    observe()  # Started gateway child still permits only its stopped predecessor.
    gateway = bindings.plan.development.gateway_original
    gateway_key = _key(gateway)
    live_gateway = next(row for row in inventory['deployments'] if _key(row) == gateway_key)
    stopped_gateway = copy.deepcopy(live_gateway)
    started_gateway = _comparison_snapshot(bindings.plan.development.gateway_target)
    fixed = bindings.plan.development.authority
    if check_forward:
        replace_live(live_gateway, started_gateway)
        before = len(calls)
        with pytest.raises(ValueError):
            observe()  # Planned startup alone is not retained dispatch intent.
        assert len(calls) > before
        replace_live(live_gateway, stopped_gateway)
        untracked = copy.deepcopy(next(iter(fixed.values())))
        untracked['metadata'].update(uid=str(uuid4()), resourceVersion='401')
        inventory[kinds[untracked['kind']]].append(untracked)
        with pytest.raises(ValueError):
            observe()  # Even a source-qualified grant is forbidden before its intent.
        inventory[kinds[untracked['kind']]].remove(untracked)
    stored = {}
    paths = {runtime_resource_path(row) + '/' + row['metadata']['name']: key for key, row in fixed.items()}
    with gateway_child.HTTPSSharedGateway(bindings=bindings,
            api_server=args['runtime'].database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context(), qualify_runtime=lambda *_: None,
            observe_ready=lambda *_: False) as delivery:
        delivery.client.close()
        delivery.client = type(transport.client)(base_url=delivery.api_server, transport=httpx.MockTransport(material_request))
        _stage_fixed_documents(documents=fixed, revision=digest(fixed), phase='forward-gateway-authority',
            binding=delivery.binding, api=delivery, state_dir=state / 'forward-gateway/authority')
    assert set(fixed) <= observe().authority.keys()
    if check_forward:
        for row in stored.values():
            original_uid = row['metadata']['uid']
            row['metadata']['uid'] = str(uuid4())
            with pytest.raises(ValueError):
                observe()  # Created grants must keep their recorded UID.
            row['metadata']['uid'] = original_uid
    path = state / 'forward-gateway/start/transition.json'
    record = json.loads(path.read_bytes())
    record['resources'][gateway_key].update(status='intent', expected=started_gateway)
    private_state._atomic_json(path, record)
    assert observe().workloads[gateway_key] == stopped_gateway
    replace_live(live_gateway, started_gateway)
    assert observe().workloads[gateway_key] == live_gateway
    if check_forward:
        live_gateway['spec']['template']['spec']['containers'][0]['image'] = 'foreign.invalid/image:latest'
        with pytest.raises(ValueError):
            observe()
        replace_live(live_gateway, started_gateway)
        for receipt in ('gateway.json', 'authority/stage.json', 'authority/dispatch.json', 'start/transition.json'):
            selected = state / 'forward-gateway' / receipt
            raw = selected.read_bytes()
            selected.unlink()
            before = len(calls)
            with pytest.raises(ValueError):
                observe()
            assert len(calls) == before
            selected.write_bytes(raw)
            selected.chmod(0o600)
        raw = path.read_bytes()
        mode['during_read'] = path.unlink
        with pytest.raises(ValueError):
            observe()
        path.write_bytes(raw)
        path.chmod(0o600)
    record['resources'][gateway_key]['status'] = 'applied'
    private_state._atomic_json(path, record)
    observe()
    if check_forward:
        replace_live(live_gateway, stopped_gateway)
        with pytest.raises(ValueError):
            observe()  # Completed startup no longer admits the stopped predecessor.
        replace_live(live_gateway, started_gateway)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'forward-activate'
    assert observe().workloads[gateway_key] == live_gateway
    activation_path = state / 'forward-activate/activation.json'
    activation_bytes = activation_path.read_bytes()
    if check_forward:
        for mutation in ('bytes', 'missing'):
            def change(mutation=mutation):
                if mutation == 'bytes':
                    activation_path.write_bytes(activation_bytes + b'\n')
                else:
                    activation_path.unlink()
            mode['during_read'] = change
            with pytest.raises(ValueError):
                observe()
            activation_path.write_bytes(activation_bytes)
            activation_path.chmod(0o600)
    if participant:
        record = json.loads(activation_bytes)
        record['opening'] = 'opened'
        for row in record['guards'].values():
            row['release'] = 'released'
        private_state._atomic_json(activation_path, record)
        api.activation_complete = True
        assert parent.run_shared_pool_operation(**kwargs)['status'] == 'shared_pool_handoff_complete'
        _exercise_participant_writer_census(args, bindings, state, api, transport, inventory, kinds, mode, publisher_cloud,
            preparation=participant == 'preparation')
        return
    if recovery:
        _exercise_recovery_writer_census(kwargs, state, transport, inventory, kinds, calls, mode, replace_live, CensusPhaseAPI)
        return
    api.block = 'recovery-fence'
    assert parent.run_shared_pool_operation(**{**kwargs, 'action': 'recover'})['phase'] == 'recovery-fence'
    before = len(calls)
    with pytest.raises(ValueError):
        observe()
    assert len(calls) == before


def _exercise_participant_writer_census(args, bindings, state, parent_api, transport, inventory, kinds, mode, publisher_cloud,
        *, preparation=False):
    """Normal release must retain the entire shared pool, not just its resources."""
    import importlib.util
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
    from scripts.ops.nebius_pool_participant_observation import HTTPSParticipantResourceObserver
    from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
    from scripts.ops.nebius_pool_participant_resource import (
        HTTPSParticipantResourceAPI,
        RetainedCandidateFileWriter,
        _resource_path,
    )
    from scripts.ops.nebius_shared_pool_operation import load_completed_shared_pool_operation
    from tests.ops.nebius_participant_external_fixture import (
        external_connection,
        publication_fixture,
    )

    from loom.nebius_platform_render import build_platform

    assert importlib.util.find_spec('scripts.ops.nebius_pool_participant_entry') is not None, 'protected release qualification is not composed'
    from scripts.ops import nebius_pool_participant_entry as participant_entry

    assert hasattr(participant_entry, 'connected_participant_release'), 'protected candidate and pool delivery are not composed'
    assert hasattr(participant_entry.ParticipantReleaseQualifier, 'candidate_follows'), 'protected source ancestry is not connected'

    predecessor = load_completed_shared_pool_operation(**args, bindings=bindings, api=parent_api)
    attached = bindings.plan.staging.attachment
    spec = predecessor.migration.registration.spec
    old_files = build_platform(attached.config, predecessor.migration.registration.candidate,
        attached.runtime_profile.model_dump(mode='json'), spec.profiles.image_admission_keyring, repo_root=ROOT)
    current = {_key(row): copy.deepcopy(row) for rows in old_files.values() for row in rows}
    for row in current.values():
        row['metadata'].update(uid=str(uuid4()), resourceVersion='401')
    current.update({_key(row): copy.deepcopy(row) for row in attached.resources})
    for rows in inventory.values():
        for row in rows:
            if _key(row) in current:
                current[_key(row)] = copy.deepcopy(row)
    manager = next(row for row in inventory['deployments']
        if row['metadata']['namespace'] == predecessor.migration.registration.binding.namespace
        and row['metadata']['name'] == 'loom-service')
    catalog = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {
        'name': 'loom-pool-profiles-' + spec.operation_id.hex,
        'namespace': manager['metadata']['namespace'], 'uid': str(uuid4()), 'resourceVersion': '401'},
        'immutable': True, 'data': {'profiles.json': spec.profiles.model_dump_json()}}
    candidate = copy.deepcopy(predecessor.migration.registration.candidate)
    candidate.update(schema_version='loom.nebius-candidate.v1', candidate_sha='d' * 40,
        run_id=812, source_archive_sha256='sha256:' + 'a' * 64, service_schema_head='0175')
    profile = attached.runtime_profile.model_copy(update={'candidate_sha': candidate['candidate_sha']})
    publication, published_payload = publication_fixture(candidate, profile)
    release_inputs = dict(predecessor=predecessor, current=tuple(current.values()),
        config=attached.config, candidate=candidate, runtime_profile=profile, operation_id=uuid4(),
        management_origin='https://' + args['runtime'].database.manager.deployment.public_host, repo_root=ROOT,
        rendered_files=build_platform(attached.config, candidate, profile.model_dump(mode='json'),
            spec.profiles.image_admission_keyring, repo_root=ROOT),
        source=PreparedDevelopmentSource(source_sha=candidate['candidate_sha'],
            source_archive_sha256=candidate['source_archive_sha256']), publication=publication,
        manager=manager, catalog=catalog, state_root=state.parent / 'participant-releases',
        anchor_dir=state.parent / 'participant-anchors')
    if preparation:
        from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
        from scripts.ops.nebius_pool_participant_prepare import capture_participant_release

        # Actual receipt-selected manager/catalog, not the historical helper's
        # independently generated catalog identity. Initial parent sequencing is
        # still fixture-qualified; this tests its real forward receipt consumer.
        catalog = copy.deepcopy(bindings.plan.development.runtime.resources[_key(catalog)])
        captured_resources = {**current, _key(manager): manager, _key(catalog): catalog}
        paths = {_resource_path(row): row for row in captured_resources.values()}
        capture_calls = []

        def read(message):
            assert message.method == 'GET' and not message.url.query
            capture_calls.append(message.url.path)
            if message.url.path == '/api/v1/namespaces/kube-system':
                return transport.client.send(message)
            row = paths.get(message.url.path)
            return httpx.Response(404) if row is None else httpx.Response(200, json=row)

        with ManagementKubernetesTransport(api_server=transport.api_server, ssl_context=ssl.create_default_context()) as capture_api:
            capture_api.client.close()
            capture_api.client = type(transport.client)(base_url=transport.api_server, transport=httpx.MockTransport(read))
            snapshot = capture_participant_release(predecessor=predecessor, bindings=bindings,
                shared_state=state, repo_root=ROOT, transport=capture_api)
        assert {_key(row): row for row in snapshot.current} == {
            key: row for key, row in current.items() if row['kind'] != 'Job'}
        assert snapshot.manager == manager and snapshot.catalog == catalog and capture_calls
        retained = retain_participant_rollout(**{**release_inputs, 'current': snapshot.current,
            'manager': snapshot.manager, 'catalog': snapshot.catalog})
        assert retained.inputs.current == snapshot.current
        return
    retained = retain_participant_rollout(**release_inputs)
    resources = {**current, _key(manager): manager, _key(catalog): catalog}
    writes = []
    reviews = []

    def respond(message):
        if message.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            namespace, account = message.headers['Impersonate-User'].removeprefix('system:serviceaccount:').split(':')
            assert set(message.headers.get_list('Impersonate-Group')) == {
                'system:serviceaccounts', 'system:serviceaccounts:' + namespace, 'system:authenticated'}
            selected = json.loads(message.content)['spec']['namespace']
            reviews.append((namespace, account, selected))
            rules = []
            catalog = {_key(row): row for plural in ('roles', 'clusterroles') for row in inventory[plural]}
            for binding in (*inventory['rolebindings'], *inventory['clusterrolebindings']):
                if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != selected:
                    continue
                if not any(row['kind'] == 'ServiceAccount' and (row['namespace'], row['name']) == (namespace, account)
                        for row in binding.get('subjects', [])):
                    continue
                reference = binding['roleRef']
                key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(catalog[key]['rules']))
            if mode.get('effective_write'):
                rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['foreign-job']})
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'resourceRules': rules, 'nonResourceRules': [], 'incomplete': False, 'evaluationError': ''}})
        plural = message.url.path.rsplit('/', 1)[-1]
        if plural in {'rolebindings', 'clusterrolebindings'} and 'resourceVersion' not in message.url.params:
            assert message.method == 'GET'
            return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1',
                'kind': 'RoleBindingList' if plural == 'rolebindings' else 'ClusterRoleBindingList',
                'metadata': {'resourceVersion': '500', 'continue': ''}, 'items': inventory[plural]})
        if plural in kinds.values() or message.url.path == '/api/v1/namespaces/kube-system':
            return transport.client.send(message)
        row = next((row for row in resources.values() if _resource_path(row) == message.url.path), None)
        if message.method == 'GET':
            return httpx.Response(200, json=row) if row is not None else httpx.Response(404)
        assert message.method == 'PATCH' and row is not None
        target = copy.deepcopy(row)
        for patch in json.loads(message.content):
            parts = [part.replace('~1', '/').replace('~0', '~') for part in patch['path'].strip('/').split('/')]
            owner = target
            for part in parts[:-1]:
                owner = owner[part]
            if patch['op'] == 'test':
                assert owner[parts[-1]] == patch['value']
            elif patch['op'] == 'remove':
                del owner[parts[-1]]
            else:
                assert patch['op'] in {'add', 'replace'}
                owner[parts[-1]] = patch['value']
        if not message.url.params:
            writes.append(_key(row))
            target['metadata']['resourceVersion'] = str(int(row['metadata']['resourceVersion']) + 1)
            resources[_key(row)] = target
            if row['kind'] in kinds:
                inventory[kinds[row['kind']]] = [target if _key(item) == _key(row) else item
                    for item in inventory[kinds[row['kind']]]]
        else:
            assert dict(message.url.params) == {'dryRun': 'All'}
        return httpx.Response(200, json=target)

    def connect(api):
        api.client.close()
        api.client = type(transport.client)(base_url=transport.api_server, transport=httpx.MockTransport(respond))
        return api

    with connect(HTTPSParticipantResourceObserver(retained=retained, api_server=transport.api_server,
            ssl_context=ssl.create_default_context())) as observer:
        assert hasattr(observer, 'observe_writers'), 'normal rollout does not qualify the complete shared writer set'

        def observe(preparing=None):
            try:
                return observer.observe_writers(bindings=bindings, shared_state=state, preparing=preparing)
            except ValueError as error:
                import traceback

                frames, cause = [], error
                while cause is not None:
                    frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                        for frame in traceback.extract_tb(cause.__traceback__))
                    cause = cause.__context__
                raise ValueError('participant census unqualified; preserve evidence; ' + ', '.join(frames)) from None

        result = observe()
        assert not writes
        assert result.workloads[_key(manager)] == manager
        assert any(row['metadata']['namespace'] == 'loom-dev' for row in result.workloads.values())
        mode['effective_write'] = True
        with pytest.raises(ValueError, match='preserve'):
            observe()
        mode.pop('effective_write')
        assert reviews, 'RBAC objects alone do not prove effective permissions'

        # A newly captured source snapshot cannot replace an already retained
        # actuator UID, even if all individual resource reads agree with it.
        changed = copy.deepcopy(release_inputs)
        changed['operation_id'] = uuid4()
        actuator = next(row for row in changed['current'] if row['kind'] == 'Deployment'
            and row['metadata']['namespace'] == attached.config['execution_namespace']
            and row['metadata']['name'] == 'loom-execution-actuator')
        actuator['metadata']['uid'] = str(uuid4())
        substituted = retain_participant_rollout(**changed)
        actuator_key = _key(actuator)
        saved = resources[actuator_key]
        resources[actuator_key] = actuator
        saved_inventory = copy.deepcopy(inventory['deployments'])
        inventory['deployments'] = [actuator if _key(row) == actuator_key else row for row in inventory['deployments']]
        try:
            with connect(HTTPSParticipantResourceObserver(retained=substituted, api_server=transport.api_server,
                    ssl_context=ssl.create_default_context())) as replaced:
                with pytest.raises(ValueError, match='preserve'):
                    replaced.observe_writers(bindings=bindings, shared_state=state)
        finally:
            resources[actuator_key] = saved
            inventory['deployments'] = saved_inventory

        def resource(key):
            return connect(HTTPSParticipantResourceAPI(retained=retained, key=key,
                api_server=transport.api_server, ssl_context=ssl.create_default_context(),
                qualify_resource=lambda _retained, selected: observe(selected)))

        writer = RetainedCandidateFileWriter(retained=retained, open_resource=resource,
            open_creation=lambda _: pytest.fail('unexpected creation'))
        writer.apply_file('20-database.yaml')
        key = 'StatefulSet:' + attached.config['namespace'] + ':loom-postgres'
        assert observe().workloads[key] == resources[key]
        assert len(writes) == 2
        snapshot = copy.deepcopy(inventory)
        for damage in ('cloned-controller', 'revived-collector', 'other-owner', 'extra-grant', 'old-template'):
            inventory.clear()
            inventory.update(copy.deepcopy(snapshot))
            if damage == 'cloned-controller':
                clone = copy.deepcopy(next(row for row in inventory['deployments']
                    if row['metadata']['namespace'] == attached.config['namespace'] and row['metadata']['name'] == 'loom-control-plane'))
                clone['metadata'].update(name='foreign-controller', uid=str(uuid4()))
                clone['spec']['replicas'] = 0
                inventory['deployments'].append(clone)
            elif damage == 'revived-collector':
                next(row for row in inventory['cronjobs'] if row['metadata']['namespace'] == attached.config['execution_namespace'])['spec']['suspend'] = False
            elif damage == 'other-owner':
                next(row for row in inventory['deployments'] if row['metadata']['namespace'] == 'loom-dev')['spec']['replicas'] = 0
            elif damage == 'extra-grant':
                next(row for row in inventory['roles'] if row['metadata']['namespace'] == attached.config['execution_namespace'])['rules'].append(
                    {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
            else:
                inventory['statefulsets'] = [current[key] if _key(row) == key else row for row in inventory['statefulsets']]
            with pytest.raises(ValueError, match='preserve'):
                observe()
            assert len(writes) == 2
        inventory.clear()
        inventory.update(snapshot)
        record = retained.state_dir / 'candidate-file-writes/dispatch.json'
        raw = record.read_bytes()
        mode['during_read'] = lambda: record.write_bytes(raw + b'\n')
        with pytest.raises(ValueError, match='preserve'):
            observe()
        record.write_bytes(raw)
        assert observe().workloads[key] == resources[key]

        # Exercise the real fence/catalog/manager/open/capacity children with
        # this same whole-pool qualifier, not the former resource-only callback.
        from tests.ops.test_nebius_pool_participant_adapter import connection as pool_connection

        with pytest.MonkeyPatch.context() as patch:
            import base64
            import hmac
            import subprocess
            from types import SimpleNamespace
            from urllib.parse import parse_qs, urlsplit

            from pydantic import PostgresDsn
            from scripts.ops.nebius_management_material import _documents as manager_material
            from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND
            from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
            from scripts.ops.nebius_pool_runtime_settings import (
                BOUND_POOL_SETTINGS_COMMAND,
                expected_pool_runtime_settings,
            )
            from scripts.ops.nebius_pool_startup_database import pool_participant_authority_sql
            from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
            from tests.ops.test_nebius_development_pool_retained import (
                populate_database_backends,
                populate_runtime_pods,
            )

            from loom_service.pool_management.capacity import digest

            patch.setattr(httpx, 'Client', type(transport.client))
            pool_state, kube, connect_pool = pool_connection.__wrapped__(release_inputs, patch)
            injected = set()
            manager_history = bindings.plan.development.runtime.request.database.manager.retained.request.retained
            material = json.loads(manager_history.files[Path(manager_history.operation['state_dir']) / 'bootstrap/material/material.json'])
            secret = manager_material(material['material'], manager_history.binding, material['operation_id'])['loom-platform-db']
            secret['metadata'].update(uid=material['resources']['loom-platform-db']['uid'], resourceVersion='701')
            resources[_key(secret)] = secret
            target = _gateway_history_target(bindings, secret)
            database_rows = {_resource_path(row): copy.deepcopy(row) for row in (target.database.statefulset, target.database.service)}
            populate_database_backends(database_rows)
            gateway = copy.deepcopy(next(row for row in inventory['deployments']
                if row['metadata']['namespace'] == target.namespace and row['metadata']['name'] == 'loom-pool-gateway'))
            gateway_rows = {_resource_path(gateway): gateway}
            populate_runtime_pods(gateway_rows)
            for plural in ('pods', 'replicasets'):
                inventory[plural][:] = [row for row in inventory[plural] if not (
                    row['metadata']['namespace'] == target.namespace
                    and row['metadata'].get('labels', {}).get('app.kubernetes.io/name') == 'loom-pool-gateway')]
                children = next(row['items'] for row in gateway_rows.values()
                    if row['kind'] == ('PodList' if plural == 'pods' else 'ReplicaSetList'))
                for row in children:
                    row['metadata']['resourceVersion'] = '501'
                inventory[plural].extend(children)
            gateway_pod, = next(row['items'] for row in gateway_rows.values() if row['kind'] == 'PodList')
            gateway_replica, = next(row['items'] for row in gateway_rows.values() if row['kind'] == 'ReplicaSetList')
            kubeconfig = state.parent / 'participant-registry-kubeconfig'
            kubeconfig.write_text('private-test-only')
            kubeconfig.chmod(0o600)
            registry_mode, registry_reads, manager_checks = {}, [], []

            def execute(command, **options):
                assert command[:3] == ['/usr/bin/kubectl', '--kubeconfig', str(kubeconfig)]
                assert options['timeout'] == 40 and options['check'] is False
                offset = next(index for index, value in enumerate(command) if value in {'get', 'exec'})
                selected = command[offset:]
                if selected[0] == 'exec' and selected[2:4] != [target.namespace, 'pod/loom-postgres-0']:
                    is_gateway = selected[3] == 'pod/' + gateway_pod['metadata']['name']
                    pod = gateway_pod if is_gateway else pool_state.children[1]['items'][0]
                    assert selected[:9] == ['exec', '-n', target.namespace, 'pod/' + pod['metadata']['name'],
                        '-c', 'gateway' if is_gateway else 'loom-service', '--', 'python', '-c']
                    script, component, nonce, challenge = selected[9:]
                    if script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        assert is_gateway
                        namespaces = {ns.name: str(ns.uid) for participant in retained.release.refresh.before.participants
                            for ns in (participant.execution_namespace, participant.build_namespace)}
                        assert json.loads(component) == sorted(namespaces)
                        machine, = (row for row in retained.release.refresh.before.machines if row.role == 'gateway')
                        wanted = expected_pool_runtime_settings('gateway', gateway, token_sha256=machine.token_sha256)
                        projection = {key: wanted[key] for key in ('pool_id', 'installation_id', 'machine_id', 'admission_epoch', 'kubernetes')}
                        payload = json.dumps({**projection, 'namespaces': namespaces}, sort_keys=True, separators=(',', ':')).encode()
                    elif script == _BOUND_DATABASE_COMMAND:
                        assert component == ('gateway' if is_gateway else 'service')
                        container, = pool_state.manager['spec']['template']['spec']['containers']
                        reference, = (row['valueFrom']['secretKeyRef'] for row in container['env'] if row['name'] == 'LOOM_SVC_DB_URL')
                        url = base64.b64decode(secret['data'][reference['key']]).decode()
                        payload = (url if is_gateway else str(PostgresDsn(url))).encode()
                    else:
                        assert script == BOUND_POOL_SETTINGS_COMMAND and component == ('gateway' if is_gateway else 'manager')
                        if is_gateway:
                            machine, = (row for row in retained.release.refresh.before.machines if row.role == 'gateway')
                            wanted = expected_pool_runtime_settings('gateway', gateway, token_sha256=machine.token_sha256)
                        else:
                            profiles = (retained.release.refresh.after if 'manager' in pool_state.effects else retained.release.refresh.before).profiles
                            wanted = expected_pool_runtime_settings('manager', pool_state.manager,
                                catalog_sha256=hashlib.sha256(profiles.model_dump_json().encode()).hexdigest())
                        payload = json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode()
                    manager_checks.append(component)
                    assert challenge == hmac.new(bytes.fromhex(nonce), payload, hashlib.sha256).hexdigest()
                    result = {'status': 'unqualified' if registry_mode.get('manager-settings') else 'qualified'}
                    if is_gateway and registry_mode.get('gateway-kubernetes') and script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        result['status'] = 'unqualified'
                    if is_gateway and registry_mode.get('gateway-restart'):
                        pod['status']['containerStatuses'][0]['restartCount'] += 1
                    if is_gateway and registry_mode.get('gateway-credential') and script == BOUND_GATEWAY_KUBERNETES_COMMAND:
                        secret['metadata']['resourceVersion'] = '702'
                    if registry_mode.get('manager-restart'):
                        status = pod['status']['containerStatuses'][0]
                        status['restartCount'] = status.get('restartCount', 0) + 1
                    if registry_mode.get('manager-history'):
                        path = retained.state_dir / 'pool-refresh/dispatch.json'
                        path.write_bytes(path.read_bytes() + b'\n')
                    if registry_mode.get('manager-connection'):
                        kubeconfig.write_text('changed-test-connection')
                    if registry_mode.get('manager-credential') and component == 'manager':
                        secret['metadata']['resourceVersion'] = '702'
                elif selected[0] == 'exec':
                    assert selected[:-1] == ['exec', '-n', target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                        'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c']
                    assert selected[-1] == pool_participant_authority_sql(retained.release.refresh)
                    registry_reads.append(selected)
                    current_state = ('complete' if 'open' in pool_state.effects else 'staged' if 'stage' in pool_state.effects else
                        'fenced' if 'fence' in pool_state.effects else 'ready')
                    result = {'schema': 'loom.pool-participant-authority.v1',
                        'operation_id': str(retained.reference.operation_id), 'pool_id': str(spec.pool_id),
                        'before_sha256': digest(retained.release.refresh.before.model_dump(mode='json')),
                        'after_sha256': digest(retained.release.refresh.after.model_dump(mode='json')),
                        'read_only': True, 'state': registry_mode.get('state', current_state)}
                    if registry_mode.get('bad-report'):
                        result['operation_id'] = str(uuid4())
                    if registry_mode.get('history'):
                        path = retained.state_dir / 'pool-refresh/dispatch.json'
                        path.write_bytes(path.read_bytes() + b'\n')
                    if registry_mode.get('connection'):
                        kubeconfig.write_text('changed-test-connection')
                    if registry_mode.get('credential'):
                        secret['metadata']['resourceVersion'] = '702'
                    if registry_mode.get('restart'):
                        listing = next(row for row in database_rows.values() if row['kind'] == 'PodList')
                        listing['items'][0]['status']['containerStatuses'][0]['restartCount'] = len(registry_reads)
                elif selected[1] == 'namespace':
                    name = selected[2]
                    if name == 'kube-system':
                        uid = retained.inputs.previous.registration.binding.kube_system_uid
                    else:
                        assert name == target.namespace
                        uid = str(target.namespace_uid)
                    result = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
                elif selected[1] == '--raw':
                    parsed = urlsplit(selected[2])
                    if parsed.path.endswith('/replicasets'):
                        result = copy.deepcopy(pool_state.children[0])
                        result['items'].append(copy.deepcopy(gateway_replica))
                    else:
                        result = copy.deepcopy(database_rows[parsed.path])
                    if result['kind'] == 'PodList':
                        result['items'].extend(copy.deepcopy(pool_state.children[1]['items']))
                        result['items'].append(copy.deepcopy(gateway_pod))
                    if 'labelSelector' in parse_qs(parsed.query):
                        label, value = parse_qs(parsed.query)['labelSelector'][0].split('=', 1)
                        result['items'] = [row for row in result['items'] if row['metadata']['labels'].get(label) == value]
                    if registry_mode.get('backend') and result['kind'] == 'EndpointSliceList':
                        result['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
                elif selected[1] in {'deployment', 'replicaset'}:
                    if selected[2] == gateway['metadata']['name']:
                        result = gateway
                    elif selected[2] == gateway_replica['metadata']['name']:
                        result = gateway_replica
                    else:
                        result = pool_state.manager if selected[1] == 'deployment' else pool_state.children[0]['items'][0]
                    assert selected[2:] == [result['metadata']['name'], '-n', target.namespace, '-o', 'json']
                    result = copy.deepcopy(result)
                    if registry_mode.get('manager-pending') and selected[1] == 'deployment':
                        result['status']['readyReplicas'] = 0
                else:
                    kind = selected[1]
                    assert kind in {'statefulset', 'service', 'secret'}
                    assert selected[2:] == ['loom-platform-db' if kind == 'secret' else 'loom-postgres', '-n', target.namespace, '-o', 'json']
                    result = (secret if kind == 'secret' else database_rows[_resource_path(
                        target.database.statefulset if kind == 'statefulset' else target.database.service)])
                return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

            patch.setattr(subprocess, 'run', execute)

            def registry():
                try:
                    return observer.observe_registry(bindings=bindings, kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
                except ValueError as error:
                    import traceback

                    frames, cause = [], error
                    while cause is not None:
                        frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                            for frame in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('participant registry unqualified; preserve evidence; ' + ', '.join(frames)) from None

            assert registry() == 'ready'
            def manager_process(phase='guarded'):
                return observer.observe_manager_process(bindings=bindings, kubeconfig=kubeconfig,
                    executable=Path('/usr/bin/kubectl'), phase=phase)

            def gateway_process():
                try:
                    return observer.observe_gateway_process(bindings=bindings, shared_state=state,
                        kubeconfig=kubeconfig, executable=Path('/usr/bin/kubectl'))
                except ValueError as error:
                    import traceback

                    frames, cause = [], error
                    while cause is not None:
                        frames.extend(f'{Path(frame.filename).name}:{frame.lineno}:{frame.name}'
                            for frame in traceback.extract_tb(cause.__traceback__))
                        cause = cause.__context__
                    raise ValueError('gateway process unqualified; preserve evidence; ' + ', '.join(frames)) from None

            from tests.ops.test_nebius_pool_participant_processes import peer_process_connection

            selected_guard = {'status': 'held'}
            peer_process, peer_fault = peer_process_connection(observer=observer, bindings=bindings,
                shared_state=state, inventory=inventory, resources=resources, kubeconfig=kubeconfig,
                patch=patch, fallback=execute, selected_guard=selected_guard)
            peer_result = peer_process()
            assert peer_result['status'] == 'available'
            external, external_fault, external_mode, external_calls, provider_reads, collector_key, reader_key, operator = external_connection(
                observer=observer, bindings=bindings, shared_state=state, resources=resources,
                publisher_cloud=publisher_cloud, patch=patch, payload=published_payload)
            capacity = external()
            assert capacity.node_count == capacity.ready_node_count == capacity.target_node_count == 0
            assert manager_process() == ()
            assert gateway_process() is None
            registry_mode['manager-pending'] = True
            with pytest.raises(ValueError, match='preserve'):
                manager_process()
            registry_mode.clear()
            for foreign in ('fenced', 'staged', 'complete'):
                registry_mode['state'] = foreign
                with pytest.raises(ValueError, match='preserve'):
                    registry()
            registry_mode.clear()

            def qualify_pool(_phase):
                nonlocal injected
                resources.update(pool_state.api.resources)
                for row in pool_state.api.resources.values():
                    if row['kind'] in kinds:
                        collection = inventory[kinds[row['kind']]]
                        collection[:] = [other for other in collection if _key(other) != _key(row)] + [row]
                descendants = (*pool_state.children[0]['items'], *pool_state.children[1]['items'], *pool_state.pods.values())
                for plural in ('replicasets', 'pods'):
                    inventory[plural][:] = [row for row in inventory[plural] if row['metadata']['uid'] not in injected]
                injected = {row['metadata']['uid'] for row in descendants}
                for row in descendants:
                    inventory[kinds[row['kind']]].append(row)
                observe()
                registry()
                if _phase == 'ready':
                    assert manager_process('ready') == ()
                    assert gateway_process() is None
                    assert peer_process()['status'] == 'available'
                    external()

            pool_state.qualify = qualify_pool
            pool = connect_pool()
            assert pool.activate(kube, retained.release) == 'ready'
            assert registry() == 'staged'
            assert gateway_process() is None  # A fenced participant is not a gateway failure.
            registry_mode['manager-settings'] = True
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            registry_mode.clear()
            peer_fault['mode'] = 'settings'
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            peer_fault.clear()
            external_fault('checks')
            with pytest.raises(ValueError, match='preserve'):
                pool.finish(kube, retained.release)
            assert 'open' not in pool_state.effects
            external_fault(None)
            pool.finish(kube, retained.release)
            pool.read_capacity(kube, retained.release, observation_id=uuid4())
            assert pool_state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity']
            assert observe().workloads[_key(manager)] == pool_state.manager
            assert registry() == 'complete'
            assert manager_process('ready') == ()
            record = retained.state_dir / 'pool-refresh/dispatch.json'
            before = record.read_bytes()
            for damage in ('manager-settings', 'manager-restart', 'manager-history', 'manager-connection', 'manager-credential', 'backend'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    manager_process('ready')
                record.write_bytes(before)
                kubeconfig.write_text('private-test-only')
                secret['metadata']['resourceVersion'] = '701'
                registry_mode.clear()
            registry_mode['manager-pending'] = True
            assert manager_process() == (_key(manager),)
            with pytest.raises(ValueError, match='preserve'):
                manager_process('ready')
            registry_mode.clear()
            assert manager_process('released') == ()
            assert {'service', 'manager'} <= set(manager_checks)
            for damage in ('gateway-kubernetes', 'gateway-restart', 'gateway-credential'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    gateway_process()
                secret['metadata']['resourceVersion'] = '701'
                registry_mode.clear()
            assert gateway_process() is None
            for damage in ('database', 'pending', 'restart', 'credential', 'telemetry-binding', 'telemetry-restart'):
                peer_fault['mode'] = damage
                with pytest.raises(ValueError, match='preserve'):
                    peer_process()
                peer_fault.clear()
            peer_fault['mode'] = 'telemetry-unavailable'
            unavailable = peer_process()
            assert unavailable['status'] == 'unavailable'
            assert unavailable['checks'] >= 2 and unavailable['unavailable'] == unavailable['checks']
            assert unavailable['reasons'] == ['kubelet_network']
            peer_fault.clear()
            for damage in ('artifact', 'deleting', 'cluster'):
                external_fault(damage)
                with pytest.raises(ValueError, match='preserve'):
                    external()
            for condition in ('exhausted', 'scaling'):
                external_fault(condition)
                observed = external()
                if condition == 'exhausted':
                    assert observed.used_nodes == observed.quota_nodes
                else:
                    assert observed.autoscaler_state == 'scaling'
            external_fault(None)
            registry_keys = {key for key, row in resources.items() if row['kind'] == 'Secret'
                and row['metadata'].get('labels', {}).get('loom.nebius/development-material-purpose') == 'registry'}
            assert registry_keys
            collector_config, = (key for key, row in resources.items() if row['kind'] == 'ConfigMap'
                and row.get('data', {}).get('LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE') == 'pool')
            for key in (collector_key, reader_key, collector_config, *sorted(registry_keys)):
                original = copy.deepcopy(resources[key])
                reads_before = len(provider_reads)
                resources[key]['metadata']['uid'] = str(uuid4())
                with pytest.raises(ValueError, match='preserve'):
                    external()
                assert len(provider_reads) == reads_before
                resources[key] = original
            original_reader = copy.deepcopy(resources[reader_key])
            external_mode['hook'] = lambda: resources[reader_key]['metadata'].update(uid=str(uuid4()))
            with pytest.raises(ValueError, match='preserve'):
                external()
            resources[reader_key] = copy.deepcopy(original_reader)
            original_connection = observer.api_server
            external_mode['hook'] = lambda: setattr(observer, 'api_server', 'https://foreign.invalid')
            reads_before = len(provider_reads)
            with pytest.raises(ValueError, match='preserve'):
                external()
            assert len(provider_reads) == reads_before
            observer.api_server = original_connection
            original = operator.read_bytes()
            external_mode['hook'] = lambda: operator.write_bytes(original + b' ')
            with pytest.raises(ValueError, match='preserve'):
                external()
            operator.write_bytes(original)
            assert external_calls and provider_reads
            original_database_rows = copy.deepcopy(database_rows)
            for damage in ('backend', 'bad-report', 'history', 'connection', 'credential', 'restart'):
                registry_mode[damage] = True
                with pytest.raises(ValueError, match='preserve'):
                    registry()
                record.write_bytes(before)
                kubeconfig.write_text('private-test-only')
                secret['metadata']['resourceVersion'] = '701'
                database_rows.clear()
                database_rows.update(copy.deepcopy(original_database_rows))
                registry_mode.clear()
            assert registry_reads and registry() == 'complete'

            # The production composed qualifier, consumed by the actual pool
            # adapter, must run all real observer families on this same world.
            # Only Kubernetes/HTTP/SDK transport is doubled above.
            from scripts.ops.nebius_pool_participant_completion import initialize_participant_head

            initialize_participant_head(initial=predecessor, state_root=retained.state_dir.parent,
                anchor_dir=retained.anchor_dir)
            kube = SimpleNamespace(kubeconfig=kubeconfig, context=None)
            effects = list(pool_state.effects)
            database_key = 'StatefulSet:' + attached.config['namespace'] + ':loom-postgres'
            # Socket transport alone is replaced here: the protected connection
            # must construct the real qualifier, candidate APIs and pool adapter.
            # A lost guard must stop both existing-resource and Job access.
            patch.setattr(httpx, 'HTTPTransport', lambda **_: httpx.MockTransport(respond))
            with participant_entry.connected_participant_release(
                    retained=retained, initial=predecessor, expected=predecessor,
                    bindings=bindings, shared_state=state, kube=kube, kubeconfig=kubeconfig,
                    executable=Path('/usr/bin/kubectl'), operator_credentials=operator,
                    private_files={kubeconfig: kubeconfig.read_bytes(), operator: operator.read_bytes()},
                    ssl_context=ssl.create_default_context(), token='test-operator-token') as delivery:
                previous_source = json.loads(current['ConfigMap:' + attached.config['namespace']
                    + ':loom-platform-config']['data']['profile.json'])['candidate_sha']
                assert delivery.qualifier.candidate_follows(previous_source, candidate['candidate_sha']) is True
                external_fault('ancestry-diverged')
                assert delivery.qualifier.candidate_follows(previous_source, candidate['candidate_sha']) is False
                external_fault(None)
                external_mode['hook'] = lambda: resources[reader_key]['metadata'].update(uid=str(uuid4()))
                with pytest.raises(ValueError, match='preserve'):
                    delivery.qualifier.candidate_follows(previous_source, candidate['candidate_sha'])
                resources[reader_key] = copy.deepcopy(original_reader)
                before_writes = list(writes)
                delivery.pool.under_guard(kube, retained.release)
                with delivery.writer.open_resource(database_key) as api:
                    assert api.read_workload(database_key) == resources[database_key]
                with delivery.writer.open_creation('pre-upgrade-backup') as api:
                    assert api.client.headers['Authorization'] == 'Bearer test-operator-token'
                    assert api.get_resource(api.document) is None
                for damage in ('guard', 'publication', 'selected-process', 'connection'):
                    if damage == 'guard':
                        selected_guard['status'] = 'foreign'
                    elif damage == 'publication':
                        external_fault('checks')
                    elif damage == 'selected-process':
                        peer_fault['mode'] = 'settings'
                    else:
                        kubeconfig.write_text('foreign-connection')
                    with pytest.raises(ValueError, match='preserve'):
                        if damage == 'guard':
                            with delivery.writer.open_resource(database_key) as api:
                                api.read_workload(database_key)
                        elif damage == 'publication':
                            with delivery.writer.open_creation('pre-upgrade-backup') as api:
                                api.get_resource(api.document)
                        else:
                            delivery.pool.under_guard(kube, retained.release)
                    selected_guard['status'] = 'held'
                    external_fault(None)
                    peer_fault.clear()
                    kubeconfig.write_text('private-test-only')
                peer_fault['mode'] = 'telemetry-unavailable'
                delivery.pool.under_guard(kube, retained.release)
                peer_fault.clear()
                assert writes == before_writes and pool_state.effects == effects
                observer_client = delivery.qualifier.observer.client
            assert observer_client.is_closed


def _exercise_recovery_writer_census(kwargs, state, transport, inventory, kinds, calls, mode, replace_live, forward_api):
    """Real phase journals and cluster census; remote recovery effects are fixture state."""
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_shared_pool_operation as parent
    from scripts.ops import nebius_shared_pool_recovery_census as census
    from scripts.ops import nebius_shared_pool_recovery_drain as drain
    from scripts.ops import nebius_shared_pool_recovery_fence as fence
    from scripts.ops import nebius_shared_pool_recovery_gateway as gateway
    from scripts.ops import nebius_shared_pool_recovery_machines as machines
    from scripts.ops import nebius_shared_pool_recovery_reopening as reopening
    from scripts.ops import nebius_shared_pool_recovery_restart as restart
    from scripts.ops import nebius_shared_pool_recovery_roles as roles
    from scripts.ops import nebius_shared_pool_recovery_templates as templates
    from scripts.ops import nebius_shared_pool_recovery_workloads as stop
    from scripts.ops.nebius_development_runtime_install import _read
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_switch import _stable

    bindings = kwargs['bindings']
    children = {
        'recovery-fence': (fence.prepare_shared_recovery_fence, fence.validate_shared_recovery_fence, 'fence.json'),
        'recovery-drain': (drain.prepare_shared_recovery_drain, drain.validate_shared_recovery_drain, 'drain.json'),
        'recovery-stop': (stop.prepare_shared_recovery_workloads, stop.validate_shared_recovery_workloads, 'stop.json'),
        'recovery-restore-workloads': (templates.prepare_shared_recovery_templates, templates.validate_shared_recovery_templates, 'templates.json'),
        'recovery-restore-roles': (roles.prepare_shared_recovery_roles, roles.validate_shared_recovery_roles, 'roles.json'),
        'recovery-restart': (restart.prepare_shared_recovery_restart, restart.validate_shared_recovery_restart, 'restart.json'),
        'recovery-reopen': (reopening.prepare_shared_recovery_reopening, reopening.validate_shared_recovery_reopening, 'reopen.json'),
        'recovery-verify': (reopening.prepare_shared_recovery_verification, reopening.validate_shared_recovery_verification, 'verify.json'),
    }

    class API(forward_api):
        def prepare(self, phase, bound, directory):
            if phase in children:
                children[phase][0](bindings=bound, state_dir=directory)
            elif phase == 'recovery-revoke':
                machines.prepare_shared_recovery_machines(bindings=bound, state_dir=directory)
                gateway.prepare_shared_recovery_gateway(bindings=bound, state_dir=directory)
            else:
                super().prepare(phase, bound, directory)

        def validate_prepared_phase(self, phase, directory):
            if phase in children:
                children[phase][1](bindings=bindings, state_dir=directory, complete=False, prepared=True)
            elif phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=False, prepared=True)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=False, prepared=True)
            else:
                super().validate_prepared_phase(phase, directory)

        def validate_phase(self, phase, directory, *, complete):
            if phase in children:
                children[phase][1](bindings=bindings, state_dir=directory, complete=complete)
            elif phase == 'recovery-revoke':
                machines.validate_shared_recovery_machines(bindings=bindings, state_dir=directory, complete=complete)
                gateway.validate_shared_recovery_gateway(bindings=bindings, state_dir=directory, complete=complete)
            else:
                super().validate_phase(phase, directory, complete=complete)

        def advance(self, phase, bound, directory):
            if phase == self.block:
                return False
            if phase == 'recovery-fence':
                path = directory / 'fence.json'
                record = _read(path)
                record['pool'] = 'fenced'
                record['guards'].update(dict.fromkeys(record['guards'], 'fenced'))
                private_state._atomic_json(path, record)
                return True
            if phase == 'recovery-drain':
                path = directory / 'drain.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'observed'})
                return True
            if phase == 'recovery-revoke':
                path = directory / 'machines.json'
                private_state._atomic_json(path, {**_read(path), 'status': 'revoked'})
                return all(row['status'] == 'restricted' for row in _read(directory / 'gateway.json')['roles'].values())
            if phase in {'recovery-reopen', 'recovery-verify'}:
                record = _read(directory / children[phase][2])
                return record['status'] == ('released' if phase == 'recovery-reopen' else 'observed')
            if phase in children:
                record = _read(directory / children[phase][2])
                return all(row['status'] in {'stopped', 'restored', 'started'}
                    for row in record.get('resources', record.get('roles', {})).values())
            return super().advance(phase, bound, directory)

    def observe():
        return census.observe_shared_pool_recovery_writers(bindings=bindings, state_dir=state, transport=transport)

    def live(key):
        return next(row for rows in inventory.values() for row in rows if _key(row) == key)

    api = API(parent)
    api.block = 'recovery-fence'
    kwargs.update(api=api, action='recover')
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-fence'
    frozen = {path: path.read_bytes() for path in state.glob('forward-*/**/*.json')}
    initial = observe()
    assert initial.resource_version == '500'
    gateway_key = _key(bindings.plan.development.gateway_original)
    assert initial.workloads[gateway_key]['spec']['replicas'] == 1
    api.block = 'recovery-reopen'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-stop'
    journal = state / 'recovery-stop/stop.json'
    record = _read(journal)
    for index, key in enumerate(stop._sources(bindings, state)):
        actual = live(key)
        before = _snapshot(actual)
        before['metadata']['uid'] = _uid(actual)
        expected = stop._stopped(bindings, before)
        if index == 0:
            saved = copy.deepcopy(actual)
            replace_live(actual, expected)
            with pytest.raises(ValueError):
                observe()  # A planned recovery stop is forbidden before this child's intent.
            replace_live(actual, saved)
        record['resources'][key].update(status='intent', before=before,
            before_resource_version=actual['metadata']['resourceVersion'], expected=expected)
        private_state._atomic_json(journal, record)
        assert observe().workloads[key] == actual
        replace_live(actual, expected)
        assert observe().workloads[key] == actual
        record['resources'][key]['status'] = 'stopped'
        private_state._atomic_json(journal, record)
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-revoke'
    assert observe().workloads[gateway_key]['spec']['replicas'] == 0
    journal = state / 'recovery-revoke/gateway.json'
    record = _read(journal)
    for index, key in enumerate(key for key in gateway._sources(bindings, state) if key in record['roles']):
        actual = live(key)
        expected = gateway._readonly(bindings, actual)
        saved = copy.deepcopy(actual)
        if index == 0:
            replace_live(actual, expected)
            with pytest.raises(ValueError):
                observe()
            replace_live(actual, saved)
        record['roles'][key].update(status='patch_intent', uid=_uid(actual),
            before_resource_version=actual['metadata']['resourceVersion'])
        private_state._atomic_json(journal, record)
        assert observe().authority[key] == actual
        replace_live(actual, expected)
        assert observe().authority[key] == actual
        record['roles'][key]['status'] = 'restricted'
        private_state._atomic_json(journal, record)
        if index == 0:
            replace_live(actual, saved)
            with pytest.raises(ValueError):
                observe()  # Completed retirement cannot admit the old gateway writer.
            replace_live(actual, expected)
    for phase, component, terminal in (
            ('recovery-restore-workloads', templates, 'restored'),
            ('recovery-restore-roles', roles, 'restored'),
            ('recovery-restart', restart, 'started')):
        assert parent.run_shared_pool_operation(**kwargs)['phase'] == phase
        observe()
        journal = state / phase / children[phase][2]
        record = _read(journal)
        targets = component._inputs(bindings, state)[1]
        for key in sorted(targets) if component is roles else targets:
            actual = live(key)
            expected = targets[key]
            row = record['roles' if component is roles else 'resources'][key]
            changed = _stable(actual) != _stable(expected)
            if changed:
                saved = copy.deepcopy(actual)
                replace_live(actual, expected)
                with pytest.raises(ValueError):
                    observe()
                replace_live(actual, saved)
                row.update(status='intent', before_resource_version=actual['metadata']['resourceVersion'])
                private_state._atomic_json(journal, record)
                observe()  # Uncertain intent permits only this before/after pair.
                replace_live(actual, expected)
                observed = observe()
                assert (observed.workloads if component is not roles else observed.authority)[key] == actual
            row.update(status=terminal, before_resource_version=actual['metadata']['resourceVersion'] if changed or component is roles else None)
            private_state._atomic_json(journal, record)
        observe()
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-reopen'
    final = observe()
    for key, captured in bindings.plan.staging.originals.items():
        assert final.workloads[key]['spec'] == captured['spec']
    assert final.workloads[gateway_key]['spec']['replicas'] == 0
    assert all(path.read_bytes() == raw for path, raw in frozen.items())
    receipt = state / 'recovery-reopen/reopen.json'
    raw = receipt.read_bytes()
    mode['during_read'] = lambda: receipt.write_bytes(raw + b'\n')
    with pytest.raises(ValueError):
        observe()
    receipt.write_bytes(raw)
    # The whole-cluster algorithm must still run after recovery; fixed-root
    # equality alone would miss this external named Job writer.
    foreign = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
        'metadata': {'name': 'unexpected-recovery-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
        'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create'], 'resourceNames': ['one-job']}]}
    foreign_binding = {'apiVersion': foreign['apiVersion'], 'kind': 'ClusterRoleBinding',
        'metadata': {'name': 'unexpected-recovery-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
        'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': foreign['metadata']['name']},
        'subjects': [{'kind': 'ServiceAccount', 'name': 'stranger', 'namespace': 'outside-pool'}]}
    inventory['clusterroles'].append(foreign)
    inventory['clusterrolebindings'].append(foreign_binding)
    with pytest.raises(ValueError):
        observe()
    inventory['clusterroles'].remove(foreign)
    inventory['clusterrolebindings'].remove(foreign_binding)
    observe()
    captured = next(iter(bindings.plan.staging.originals.values()))
    original_spec = copy.deepcopy(captured['spec'])
    mode['during_read'] = lambda: captured['spec'].update({'unapproved': True})
    try:
        with pytest.raises(ValueError):
            observe()  # Nested caller-owned plan objects cannot change mid-read.
    finally:
        captured['spec'] = original_spec
    # Terminal recovery retains the same read-only census authority, not the
    # forward view or permission to replay completed recovery mutations.
    private_state._atomic_json(receipt, {**_read(receipt), 'status': 'released'})
    api.block = 'recovery-verify'
    assert parent.run_shared_pool_operation(**kwargs)['phase'] == 'recovery-verify'
    observe()
    receipt = state / 'recovery-verify/verify.json'
    private_state._atomic_json(receipt, {**_read(receipt), 'status': 'observed'})
    api.block = None
    assert parent.run_shared_pool_operation(**kwargs)['status'] == 'staging_restored_dev_closed'
    assert observe().workloads[gateway_key]['spec']['replicas'] == 0
    from scripts.ops.nebius_shared_pool_forward_census import observe_shared_pool_forward_writers

    with pytest.raises(ValueError):
        observe_shared_pool_forward_writers(bindings=bindings, state_dir=state, transport=transport)
    assert calls and all(call.method == 'GET' for call in calls)


def _shared_recovery_database_io(*, remote, bindings, guard_api, database_model,
        shared_runtime, completed_pool, handoff, monkeypatch):
    """Replace only external kubectl I/O, retaining real DB identity and SQL checks."""
    import json
    import subprocess
    from types import SimpleNamespace

    from scripts.ops.nebius_pool_activation_database import (
        fence_pool_activation_sql,
        pool_activation_state_sql,
    )
    from scripts.ops.nebius_pool_guard_activation import pool_guard_activation_sql
    from scripts.ops.nebius_pool_machine_database import pool_machine_retirement_sql
    from scripts.ops.nebius_pool_recovery_database import (
        participant_recovery_drain_sql,
        pool_recovery_drain_sql,
    )
    from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql
    from tests.ops.test_nebius_development_pool_retained import (
        populate_database_backends,
        runtime_http_inventory,
    )

    from loom_service.pool_management.capacity import digest

    runtime, parent = shared_runtime
    _, inventory, _ = runtime_http_inventory(runtime, parent, completed_pool[3].server, handoff[2])
    namespace = bindings.migration.registration.binding.namespace
    documents = {path: row for path, row in inventory.items() if '/namespaces/' + namespace + '/' in path}
    documents['/api/v1/namespaces/' + namespace] = {'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {'name': namespace, 'uid': bindings.migration.registration.binding.namespace_uid}}
    for row in documents.values():
        if row['kind'] == 'Deployment':
            row['spec']['replicas'] = 0
            row['status'] = {'readyReplicas': 0}  # No healthy manager/gateway process is available.
    populate_database_backends(documents)
    database_model.documents.update(documents)
    spec = bindings.migration.registration.spec
    machine_queries = {pool_machine_retirement_sql(spec, action=action): action for action in ('observe', 'revoke')}
    fallback = subprocess.run

    def run(argv, **kwargs):
        assert argv[:len(guard_api.prefix)] == guard_api.prefix
        args = argv[len(guard_api.prefix):]
        if args[0] == 'get':
            return fallback(argv, **kwargs)
        assert args[:2] == ['exec', '-n'] and args[3:8] == ['pod/loom-postgres-0', '-c', 'loom-postgres', '--', 'psql']
        assert args[8:-2] == ['-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom']
        assert args[-2] == '-c'
        current_namespace, query = args[2], args[-1]
        if current_namespace == namespace:
            if query in machine_queries:
                assert remote.mode == 'fenced' and not any(remote.pool_counts.values())
                if machine_queries[query] == 'revoke':
                    remote.retire_machines()
                value = {'schema': 'loom.pool-machine-retirement.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'state': remote.machines}
                if machine_queries[query] == 'observe':
                    remote.after_machine_observation()
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == pool_recovery_drain_sql(spec):
                remote.reads.append('pool')
                assert remote.mode == 'fenced'
                value = {'schema': 'loom.pool-recovery-drain.v1', 'operation_id': str(spec.operation_id),
                    'installation_sha256': digest(spec.model_dump(mode='json')), 'read_only': True, 'counts': remote.pool_counts}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == fence_pool_activation_sql(spec):
                remote.fence_pool()
            else:
                assert query == pool_activation_state_sql(spec)
            value = {'schema': 'loom.pool-activation-state.v1', 'operation_id': str(spec.operation_id),
                'installation_sha256': digest(spec.model_dump(mode='json')), 'state': remote.pool_state()}
        else:
            target, = (row for row in bindings.migration.guards if row.namespace == current_namespace)
            participant = str(target.participant_id)
            if query == pool_guard_recovery_release_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha']):
                remote.release_staging(participant)
                value = {'schema': 'loom.pool-recovery-release.v1', 'operation_id': str(spec.operation_id),
                    'participant_id': participant, 'candidate_sha': bindings.migration.registration.candidate['candidate_sha'],
                    'status': 'open'}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == participant_recovery_drain_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha']):
                remote.reads.append(participant)
                assert remote.local[participant] == 'fenced'
                value = {'schema': 'loom.pool-participant-drain.v1', 'operation_id': str(spec.operation_id),
                    'participant_id': participant, 'candidate_sha': bindings.migration.registration.candidate['candidate_sha'],
                    'read_only': True, 'counts': remote.local_counts[participant]}
                return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')
            if query == pool_guard_activation_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha'], action='fence'):
                remote.fence_guard(participant)
            else:
                assert query == pool_guard_activation_sql(spec.operation_id, target.participant_id,
                    bindings.migration.registration.candidate['candidate_sha'], action='observe')
            value = {'schema': 'loom.pool-local-guard.v1', 'operation_id': str(spec.operation_id),
                'participant_id': participant, 'status': remote.guard_state(participant)}
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

    monkeypatch.setattr(subprocess, 'run', run)


def _exercise_completed_recovery_readers(bindings):
    """A fresh process may read completed authority without recreating old mutators."""
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_live import runtime_resource_path
    from scripts.ops.nebius_shared_pool_recovery_gateway import HTTPSSharedRecoveryGateway
    from scripts.ops.nebius_shared_pool_recovery_roles import HTTPSSharedRecoveryRoles
    from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER

    catalog = {_key(row): copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}}
    for key in bindings.plan.staging_role_originals:
        catalog[key]['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        catalog[key]['metadata']['resourceVersion'] = '202'
    identity = bindings.migration.registration.binding
    namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
        **{guard.namespace: str(guard.namespace_uid) for guard in bindings.migration.guards},
        **{ns.name: str(ns.uid) for participant in bindings.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace)}}
    calls = []

    def handle(message):
        path = message.url.path
        calls.append((message.method, path))
        if path.startswith('/api/v1/namespaces/'):
            assert message.method == 'GET'
            namespace = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': namespace, 'uid': namespaces[namespace], 'resourceVersion': '1'}})
        kinds = {'roles': 'Role', 'rolebindings': 'RoleBinding', 'clusterroles': 'ClusterRole', 'clusterrolebindings': 'ClusterRoleBinding'}
        for resource, kind in kinds.items():
            if path == '/apis/rbac.authorization.k8s.io/v1/' + resource:
                assert message.method == 'GET'
                return httpx.Response(200, json={'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': kind + 'List',
                    'metadata': {'resourceVersion': '71'}, 'items': [row for row in catalog.values() if row['kind'] == kind]})
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            _, _, account_namespace, account = message.headers['Impersonate-User'].split(':')
            namespace = json.loads(message.content)['spec']['namespace']
            rules = []
            for row in catalog.values():
                if not row['kind'].endswith('Binding') or (row['kind'] == 'RoleBinding' and row['metadata']['namespace'] != namespace):
                    continue
                if not any(subject['kind'] == 'ServiceAccount' and (subject['namespace'], subject['name']) == (account_namespace, account)
                        for subject in row.get('subjects', [])):
                    continue
                reference = row['roleRef']
                key = reference['kind'] + ':' + (row['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
                rules.extend(copy.deepcopy(catalog[key]['rules']))
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': []}})
        assert message.method == 'GET', 'completed readers must not replay Kubernetes writes'
        value = next((row for row in catalog.values() if path == runtime_resource_path(row) + '/' + row['metadata']['name']), None)
        return httpx.Response(404) if value is None else httpx.Response(200, json=value)

    config = bindings.plan.development.runtime.request.database.foundation.inputs.config
    with HTTPSSharedRecoveryGateway(bindings=bindings, machines=None,
            api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context()) as gateway:
        gateway.client.close()
        gateway.client = type(gateway.client)(base_url=gateway.api_server, transport=httpx.MockTransport(handle))
        gateway.verify_permissions()
        with HTTPSSharedRecoveryRoles(bindings=bindings, templates=None, gateway=gateway,
                qualify_runtime=lambda *_: pytest.fail('completed reader must not call the former write-phase qualifier'),
                api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context()) as roles:
            roles.client.close()
            roles.client = type(roles.client)(base_url=roles.api_server, transport=httpx.MockTransport(handle))
            roles.verify_permissions()
            key = next(iter(bindings.plan.staging_role_originals))
            saved = catalog[key]['rules']
            catalog[key]['rules'] = []
            with pytest.raises(ValueError):
                roles.verify_permissions()  # Completed receipts cannot excuse current missing rights.
            catalog[key]['rules'] = saved
            with pytest.raises(ValueError):
                roles.restore_role(key, record_intent=lambda *_: pytest.fail('completed Role writer must not dispatch'))
        with pytest.raises(ValueError):
            gateway.qualify()  # Write-phase qualification still requires the active revoke phase.
    assert any(method == 'POST' for method, _ in calls)


@pytest.mark.timeout(420)
async def test_shared_provider_uses_installed_observer_and_retains_scale_zero(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff,
        publisher_cloud, monkeypatch, tmp_path):
    """The common-pool preflight must use actual observer IAM/pool readbacks."""
    import ssl

    import httpx
    from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
    from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_provider
    from tests.ops.test_nebius_development_pool_retained import (
        runtime_cloud_transport,
        runtime_http_inventory,
    )

    request, parent = shared_runtime
    calls, documents, handler = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    operator, sessions, reads, closed, mode = runtime_cloud_transport(request, publisher_cloud, monkeypatch, tmp_path)
    spec = request.database.manager.retained.request.registration.spec
    with HTTPSDevelopmentRuntimeObserver(request=request,
            api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as observer:
        for connection in (observer, observer.manager, observer.foundation):
            connection.client.close()
            connection.client = type(completed_pool[3].api.client)(base_url=connection.api_server,
                transport=httpx.MockTransport(handler))

        async def observe():
            return await observe_shared_pool_provider(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, development=observer, operator_credentials=operator)

        result = await observe()
        assert result.provider.node_group.id == spec.node_group_id
        assert result.provider.node_count == result.provider.ready_node_count == result.provider.target_node_count == 0
        assert result.provider.provider_capacity_state == 'available' and result.provider.autoscaler_state == 'ready'
        assert reads == [('quotas', request.collector_scope.account_id), ('nodegroup', request.collector_scope.account_id)]
        assert sessions == closed and not sessions[-1].path.exists()
        assert 'provider' not in repr(result) and all(row.method == 'GET' for row in calls)
        for state in ('exhausted', 'scaling'):
            mode['damage'] = state
            current = await observe()
            if state == 'exhausted':
                assert current.provider.used_nodes == current.provider.quota_nodes
            else:
                assert current.provider.autoscaler_state == 'scaling'

        for damage in ('deleting', 'cluster', 'credential'):
            mode['damage'] = damage
            if damage == 'credential':
                secret = next(row for row in documents.values() if row['kind'] == 'Secret'
                    and row.get('data', {}).get('credentials.json')
                    and row['metadata']['namespace'] == spec.participants[0].execution_namespace.name)
                previous_uid = secret['metadata']['uid']
                secret['metadata']['uid'] = str(uuid4())
                before = len(reads)
            with pytest.raises(ValueError, match='shared pool provider unqualified'):
                await observe()
            if damage == 'credential':
                assert len(reads) == before  # Fail before exchanging a mismatched installed identity.
                secret['metadata']['uid'] = previous_uid
            assert sessions == closed
        assert calls and all(row.method == 'GET' for row in calls)
