"""Ordinary deployment proof selects exact source without widening retained authority."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_platform_rollout_successor import (
    OrdinaryPlatformRollout,
    resolve_platform_rollout,
)
from tests.ops import test_nebius_pool_cutover_entry as cutover_fixture
from tests.ops.test_nebius_management_refresh_connected import (
    connected_refresh as connected_refresh,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import application_material as application_material
from tests.ops.test_nebius_pool_cutover_entry import checks as checks
from tests.ops.test_nebius_pool_cutover_entry import cloud as cloud
from tests.ops.test_nebius_pool_cutover_entry import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover_entry import completed_upgrade as completed_upgrade
from tests.ops.test_nebius_pool_cutover_entry import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover_entry import database_guard as database_guard
from tests.ops.test_nebius_pool_cutover_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_pool_cutover_entry import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover_entry import installation as installation
from tests.ops.test_nebius_pool_cutover_entry import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover_entry import material as material
from tests.ops.test_nebius_pool_cutover_entry import private_upgrade as private_upgrade
from tests.ops.test_nebius_pool_cutover_entry import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover_entry import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_database_guard import workload_database as workload_database_fixture
from tests.ops.test_nebius_pool_predecessor import finish_cutover
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.support import nebius_platform


@pytest.fixture
def platform_inputs():
    config, candidate, profile = copy.deepcopy(nebius_platform.platform_inputs.__wrapped__())
    # Match the original development participant before generating any history.
    config.update(namespace='loom-nebius-platform-2', execution_namespace='loom-nebius-exec-2',
        target_id='pool-target-2', environment='development',
        task_image_builder={'registry_repository': 'cr.eu-north1.nebius.cloud/test/task-images', 'max_concurrent': 16})
    return config, candidate, profile


@pytest.fixture
def private_cutover(completed_upgrade, cutover_inputs, database_guard, request):
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
    from tests.ops.test_nebius_pool_dormant import dormant_consumer
    metadata, inputs, root = cutover_fixture.private_cutover.__wrapped__(completed_upgrade, cutover_inputs, database_guard)
    # Retain the real historical CP shape before any pool history is created.
    # The current ordinary source adds this recovery-only public config mount.
    namespace = root.deployment.installation.foundation.platform_config['namespace']
    controller, = (row['controller'] for row in inputs['guards'] if row['namespace'] == namespace)
    pod = controller['spec']['template']['spec']
    pod['volumes'] = [row for row in pod['volumes'] if row['name'] != 'recovery-platform-config']
    container, = pod['containers']
    container['volumeMounts'] = [row for row in container['volumeMounts']
        if row['name'] != 'recovery-platform-config']
    annotations = getattr(request, 'param', None)
    if annotations is not None:
        # Freeze historical operator metadata before any pool phase or receipt
        # exists; never rewrite sealed history to make the successor pass.
        assert not Path(metadata['state_dir']).exists()
        assert not Path(metadata['anchor_dir']).exists()
        service, = (row for row in inputs['services'] if row['metadata']['namespace'] == namespace)
        service['spec']['template']['metadata'].setdefault('annotations', {}).update(annotations)
    cutover_fixture.save_private(metadata, inputs)
    retirement = load_pool_cutover_inputs(metadata).request.fencing.retirement
    registration = retirement.migration.registration
    participants = registration.spec.participants
    ordered = sorted(participants, key=lambda row: row.environment_class != 'development')
    for_fixture = replace(retirement, migration=replace(retirement.migration, registration=replace(registration,
        spec=registration.spec.model_copy(update={'participants': tuple(ordered)}))))
    consumer = dormant_consumer(for_fixture, target='nebius-retained-west')
    inputs['dormant_consumers'] = [asdict(consumer)]
    cutover_fixture.save_private(metadata, inputs)
    return metadata, inputs, root


@pytest.fixture
def ordinary_rollout(private_cutover, monkeypatch, request):
    import os
    import subprocess

    from scripts.ops import nebius_candidate_render

    from loom_service.environment_management.manager import CandidateBundle

    if not hasattr(os, 'waitid'):
        # The production bounded watchdog is Linux-only. Keep the real isolated
        # selected renderer in local macOS tests; Linux exercises its watchdog.
        def run_private(args, *, timeout):
            return subprocess.run(args, timeout=timeout, check=True, capture_output=True,
                env={'PATH': os.defpath, 'LANG': 'C.UTF-8'}).stdout
        monkeypatch.setattr(nebius_candidate_render, 'run_private', run_private)
    _, inputs, root = private_cutover
    reference = OrdinaryPlatformRollout(deployment_id=700, publication=inputs['publication'])
    config = copy.deepcopy(root.deployment.installation.foundation.platform_config)
    changes = getattr(request, 'param', None)
    if isinstance(changes, dict):
        for key, value in changes.items():
            if key == 'task_image_builder':
                config[key].update(value)
            else:
                config[key] = value
    elif changes is not None:
        config['execution_disk_type'] = changes
    source = reference.publication.source_sha
    scope = {key: config[key] for key in ('cluster_id', 'namespace', 'execution_namespace')}
    receipt = dict(schema_version='loom.nebius-deployment.v1', mode='apply', status='complete',
        candidate_sha=source, previous_candidate_sha='a' * 40, **scope,
        phases=[{'name': name} for name in ('pre-upgrade-backup', 'public-https-smoke', 'candidate-readback')],
        backup_job='loom-predeploy-' + source[:12] + '-12345678')
    repo = {'full_name': 'ca-mrc/loom', 'id': 1281629473}
    payload = receipt | {'run_id': 701, 'run_attempt': 1}
    responses = {
        'deployments/700': {'id': 700, 'environment': 'nebius-integration', 'sha': source, 'payload': payload},
        'deployments/700/statuses?per_page=1': [{'state': 'success'}],
        'actions/runs/701/attempts/1': {'id': 701, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success',
            'head_sha': 'd' * 40, 'head_branch': 'dev', 'path': '.github/workflows/nebius-rollout.yml',
            'event': 'workflow_dispatch', 'repository': repo, 'head_repository': repo}}
    state = SimpleNamespace(receipt=receipt, reads=[], candidate=copy.deepcopy(inputs['candidate']),
        profile=copy.deepcopy(inputs['profile']), follows=True, reference=reference, responses=responses)
    async def read(url, *, api, limit):
        from loom.repository_identity import GITHUB_API_ROOT
        assert api and url.startswith(GITHUB_API_ROOT)
        path = url.removeprefix(GITHUB_API_ROOT)
        state.reads.append(path)
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, 'w') as bundle:
            bundle.writestr('deployment-' + 'a' * 32 + '.json', json.dumps(state.receipt))
        raw = archive.getvalue()
        if path == 'actions/runs/701/artifacts?per_page=100':
            value = {'total_count': 1, 'artifacts': [{'id': 702, 'name': 'nebius-rollout-701-1',
                'expired': False, 'size_in_bytes': len(raw), 'digest': 'sha256:' + hashlib.sha256(raw).hexdigest(),
                'workflow_run': {'id': 701, 'head_sha': 'd' * 40, 'repository_id': repo['id'], 'head_repository_id': repo['id']}}]}
        elif path == 'actions/artifacts/702/zip':
            return raw, None
        else:
            value = responses[path]
        return json.dumps(value).encode(), None
    async def resolve(identity):
        assert identity == reference.publication.candidate_id
        return CandidateBundle(identity, state.candidate, state.profile)
    async def follows(**kwargs):
        assert kwargs == {'candidate_id': reference.publication.candidate_id, 'current_sha': 'a' * 40}
        return state.follows
    async def source_files(**kwargs):
        assert kwargs == {'candidate_id': reference.publication.candidate_id}
        directory = Path(__file__).resolve().parents[2]
        paths = [*(directory / 'deploy/k8s').rglob('*'), *(directory / 'src/loom').rglob('*.py'),
            directory / 'scripts/ops/render_nebius_platform.py']
        files = {path.relative_to(directory).as_posix(): path.read_bytes() for path in paths
            if path.is_file() and '__pycache__' not in path.parts}
        # Distinguish selected source from the local renderer used by historical fixtures.
        renderer = 'scripts/ops/render_nebius_platform.py'
        files[renderer] = files[renderer].replace(b'        remote = build_regional_execution',
            b'        for row in files["40-services.yaml"]:\n'
            b'            if row["kind"] == "Deployment":\n'
            b'                row["spec"]["template"]["metadata"].setdefault("annotations", {})["test-selected-source"] = "yes"\n'
            b'        remote = build_regional_execution')
        return files
    catalog = SimpleNamespace(_read=read, resolve=resolve, candidate_follows=follows, render_source=source_files)
    async def selected():
        return await resolve_platform_rollout(reference=reference, catalog=catalog, config=config,
            keyring=root.deployment.installation.keyring, previous_sha='a' * 40)
    return selected, state


@pytest.fixture
async def refresh_database(private_cutover, ordinary_rollout, runtime_inputs, guest_runtime_inputs,
        platform_inputs, tmp_path, monkeypatch):
    from scripts.ops.nebius_pool_migration import migration_contract
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh

    from loom.nebius_platform_render import digest

    operation, inputs, root = private_cutover
    namespace = root.deployment.installation.foundation.platform_config['namespace']
    migration = runtime_inputs[0]
    primary, = (row for row in migration.guards if row.namespace == namespace)
    ordered = (primary, *(row for row in migration.guards if row != primary))
    directory = tmp_path / 'primary-database'
    directory.mkdir()
    base, state = database_guard.__wrapped__((replace(migration, guards=ordered), *runtime_inputs[1:]),
        platform_inputs, directory, monkeypatch)
    bookkeeping = 'kubectl.kubernetes.io/last-applied-configuration'
    state.database['metadata']['annotations'] = {bookkeeping: '{"retained":"apply-bookkeeping"}'}
    retained = replace(state.target.database, statefulset=copy.deepcopy(state.database))
    state.target = replace(state.target, database=retained)
    guest_migration = replace(guest_runtime_inputs[0], guards=(state.target, *ordered[1:]))
    api, state = workload_database_fixture.__wrapped__(SimpleNamespace(param='controller'), (base, state),
        (guest_migration, *guest_runtime_inputs[1:]), monkeypatch)
    # Bind this real transport fixture before creating immutable pool history.
    primary_input, = (row for row in inputs['guards'] if row['namespace'] == namespace)
    for field in ('statefulset', 'service', 'credential_uid', 'credential_resource_version'):
        primary_input['database'][field] = asdict(retained)[field]
    cutover_fixture.save_private(operation, inputs)
    _, result = finish_cutover(operation, legacy=True)
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    successor = await ordinary_rollout[0]()
    metadata, payload, _ = private_refresh(root, pool)
    payload['platform_rollout'] = successor.reference.model_dump(mode='json')
    payload['deployment']['installation']['foundation']['platform_config_json'] = json.dumps(successor.config)
    cutover_fixture.save_private(metadata, payload)
    selected = context(metadata)
    bound = PoolManagerRefresh(root, pool, selected.request, Path(metadata['state_dir']), successor=successor)
    api.request = pool.context.request.fencing.retirement.migration
    api.contract_sha256 = digest(migration_contract(api.request))
    state.target, = (row for row in api.request.guards if row.namespace == namespace)
    state.original.clear()
    state.original.update(copy.deepcopy(state.target.controller))
    state.credential = (state.target.database.credential_uid, state.target.database.credential_resource_version)
    source = successor.resources['StatefulSet:' + namespace + ':loom-postgres']
    revision = 'loom.nebius/configuration-revision'
    assert state.database['spec']['template']['metadata']['annotations'][revision] != source['spec']['template']['metadata']['annotations'][revision]
    state.database['spec']['template']['metadata']['annotations'][revision] = source['spec']['template']['metadata']['annotations'][revision]
    state.database['metadata']['annotations'][bookkeeping] = json.dumps(source)
    transport, calls = api._run, []
    def read(args):
        calls.append(args)
        if args[:2] == ['get', 'namespace']:
            name = args[2]
            uid = api.request.registration.binding.kube_system_uid if name == 'kube-system' else str(state.target.namespace_uid)
            assert name in {'kube-system', namespace}
            return {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': uid}}
        return transport(args)
    monkeypatch.setattr(api, '_run', read)
    # The old implementation ignores this attribute, so RED reaches the actual
    # snapshot mismatch rather than failing on a not-yet-added constructor kwarg.
    api.refresh = bound
    return api, state, bound, pool, calls


@pytest.mark.asyncio
async def test_completed_ordinary_successor_restores_historical_control_plane_recovery_config(private_cutover, ordinary_rollout):
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh

    _, inputs, root = private_cutover
    namespace = root.deployment.installation.foundation.platform_config['namespace']
    original, = (row['controller'] for row in inputs['guards'] if row['namespace'] == namespace)
    retained = copy.deepcopy(original)
    proven = await ordinary_rollout[0]()
    bound = PoolManagerRefresh(None, SimpleNamespace(pool_baseline=SimpleNamespace(context=SimpleNamespace(
        request=SimpleNamespace(fencing=SimpleNamespace(retirement=SimpleNamespace(dormant_consumers=())))))),
        None, Path('/unused'), successor=proven)
    assert len(original['spec']['template']['spec']['volumes']) == 3
    expected = bound.successor_document(original)
    assert expected == {**proven.resources[_key(original)],
        'metadata': {**proven.resources[_key(original)]['metadata'], 'uid': original['metadata']['uid']}}
    assert bound.matches_workload(expected, expected, original)
    assert original == retained


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['secret', 'projected_secret', 'pvc', 'image_pull_secret',
    'unknown_config_volume', 'mount_name', 'extra_recovery_mount', 'initializer_mount',
    'old_dangling_mount', 'writable_mount', 'reference', 'items', 'mount_path',
    'service_scope', 'execution_scope'])
async def test_recovery_config_addition_cannot_widen_retained_authority(private_cutover, ordinary_rollout, damage):
    from scripts.ops.nebius_platform_rollout_successor import PlatformRolloutSuccessor
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh

    _, inputs, root = private_cutover
    namespace = root.deployment.installation.foundation.platform_config['namespace']
    original = copy.deepcopy(next(row['controller'] for row in inputs['guards'] if row['namespace'] == namespace))
    proven = await ordinary_rollout[0]()
    desired = copy.deepcopy(proven.resources[_key(original)])
    pod = desired['spec']['template']['spec']
    container, = pod['containers']
    volume, = (row for row in pod['volumes'] if row['name'] == 'recovery-platform-config')
    mount, = (row for row in container['volumeMounts'] if row['name'] == 'recovery-platform-config')
    if damage == 'secret':
        next(row['secret'] for row in pod['volumes'] if 'secret' in row)['secretName'] = 'foreign'
    elif damage == 'projected_secret':
        pod['volumes'].append({'name': 'foreign', 'projected': {'sources': [{'secret': {'name': 'foreign'}}]}})
    elif damage == 'pvc':
        pod['volumes'].append({'name': 'foreign', 'persistentVolumeClaim': {'claimName': 'foreign'}})
    elif damage == 'image_pull_secret':
        pod['imagePullSecrets'] = [*pod.get('imagePullSecrets', []), {'name': 'foreign'}]
    elif damage == 'unknown_config_volume':
        pod['volumes'].append({'name': 'foreign', 'configMap': {'name': 'loom-platform-config'}})
    elif damage == 'mount_name':
        mount['name'] = 'foreign'
    elif damage == 'extra_recovery_mount':
        container['volumeMounts'].append({**mount, 'mountPath': '/var/run/foreign'})
    elif damage == 'initializer_mount':
        pod['initContainers'][0]['volumeMounts'].append(copy.deepcopy(mount))
    elif damage == 'old_dangling_mount':
        original['spec']['template']['spec']['containers'][0]['volumeMounts'].append(copy.deepcopy(mount))
    elif damage == 'writable_mount':
        mount['readOnly'] = False
    elif damage == 'reference':
        volume['configMap']['name'] = 'foreign'
    elif damage == 'items':
        volume['configMap']['items'][0]['key'] = 'foreign.json'
    elif damage == 'mount_path':
        mount['mountPath'] = '/var/run/foreign'
    elif damage == 'service_scope':
        for row in (original, desired):
            row['metadata']['name'] = 'loom-service'
            row['spec']['selector'] = {'matchLabels': {'app': 'loom-service'}}
            row['spec']['template']['spec']['containers'][0]['name'] = 'loom-service'
    else:
        for row in (original, desired):
            row['metadata']['namespace'] = proven.config['execution_namespace']
    successor = PlatformRolloutSuccessor(proven.reference, proven.config, {_key(original): desired})
    bound = PoolManagerRefresh(None, SimpleNamespace(pool_baseline=SimpleNamespace(context=SimpleNamespace(
        request=SimpleNamespace(fencing=SimpleNamespace(retirement=SimpleNamespace(dormant_consumers=())))))),
        None, Path('/unused'), successor=successor)
    retained, selected = copy.deepcopy(original), copy.deepcopy(desired)
    with pytest.raises(ValueError):
        bound.successor_document(original)
    assert original == retained and desired == selected


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', [None, 'incomplete', 'scope', 'backup', 'publication', 'ancestry', 'foreign_run'])
async def test_ordinary_proof_requires_completed_exact_scoped_publication(ordinary_rollout, damage):
    selected, state = ordinary_rollout
    if damage == 'incomplete':
        state.receipt['status'] = 'running'
    elif damage == 'scope':
        state.receipt['namespace'] = 'foreign'
    elif damage == 'backup':
        state.receipt['backup_job'] = 'foreign'
    elif damage == 'publication':
        state.candidate['candidate_sha'] = 'f' * 40
    elif damage == 'ancestry':
        state.follows = False
    elif damage == 'foreign_run':
        state.responses['actions/runs/701/attempts/1']['head_repository'] = {'full_name': 'foreign/loom', 'id': 1}
    if damage:
        with pytest.raises(ValueError, match='ordinary_platform_rollout_unqualified'):
            await selected()
    else:
        successor = await selected()
        service = successor.resources['Deployment:' + successor.config['namespace'] + ':loom-service']
        assert service['spec']['template']['metadata']['annotations']['test-selected-source'] == 'yes'
        assert state.reads and all(not name.startswith(('POST', 'PATCH')) for name in state.reads)


@pytest.mark.asyncio
async def test_successor_preserves_periodic_writer_credentials_and_exact_template(ordinary_rollout):
    from scripts.ops.nebius_platform_rollout_successor import PlatformRolloutSuccessor
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh

    selected, _ = ordinary_rollout
    proven = await selected()
    desired = copy.deepcopy(next(row for row in proven.resources.values() if row['kind'] == 'CronJob'
        and row['metadata']['namespace'] == proven.config['namespace']))
    original = copy.deepcopy(desired)
    original['metadata']['uid'] = '02108af1-0897-4cf9-87eb-8769c1229073'
    templates = [row['spec']['jobTemplate']['spec']['template'] for row in (original, desired)]
    for template in templates:
        template['spec']['containers'][0].setdefault('env', []).append({'name': 'TEST_DB_URL',
            'valueFrom': {'secretKeyRef': {'name': 'retained-db', 'key': 'url'}}})
    templates[0]['spec']['containers'][0]['env'][-1]['valueFrom']['secretKeyRef']['optional'] = False
    for template in templates:
        template['spec'].setdefault('volumes', []).append({'name': 'retained-secret',
            'secret': {'secretName': 'retained-secret'}})
    templates[0]['spec']['volumes'][-1]['secret'].update(defaultMode=420, optional=False)
    desired['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] += '-selected-successor'
    successor = PlatformRolloutSuccessor(proven.reference, proven.config, {_key(original): desired})
    bound = PoolManagerRefresh(None, SimpleNamespace(pool_baseline=SimpleNamespace(context=SimpleNamespace(
        request=SimpleNamespace(fencing=SimpleNamespace(retirement=SimpleNamespace(dormant_consumers=())))))),
        None, Path('/unused'), successor=successor)
    expected = bound.successor_document(original)
    assert bound.matches_workload(expected, expected, original)
    actual = copy.deepcopy(expected)
    actual['spec']['jobTemplate']['spec']['template']['metadata'].setdefault('annotations', {})['unrelated'] = 'yes'
    assert not bound.matches_workload(actual, expected, original)
    entry = desired['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['env'][-1]
    entry['valueFrom']['secretKeyRef']['name'] = 'foreign-db'
    with pytest.raises(ValueError):
        bound.successor_document(original)


@pytest.mark.asyncio
async def test_publication_renderer_uses_validated_current_observed_target(private_cutover, monkeypatch):
    from scripts.ops import nebius_management_refresh_entry as entry
    from scripts.ops.nebius_management_refresh import _configuration

    from loom_service.environment_management.deployment import ManagementDeployment

    _, inputs, root = private_cutover
    before = root.deployment
    data = before.model_dump(mode='json')
    config = json.loads(data['installation']['foundation']['platform_config_json'])
    config['guest_execution_target'] = {'target_id': 'nebius-guest-current'}
    config['emulated_auth_execution_target'] = {'target_id': 'nebius-auth-current'}
    data['installation']['foundation']['platform_config_json'] = json.dumps(config)
    after = ManagementDeployment.model_validate(data)
    _configuration(before, after)
    reference = OrdinaryPlatformRollout(deployment_id=700, publication=inputs['publication'])
    calls = []
    async def resolve(**kwargs):
        assert kwargs['config'] == config
        assert kwargs['reference'] == reference
        calls.append(kwargs)
        return 'selected-current-config'
    monkeypatch.setattr(entry, 'resolve_platform_rollout', resolve)
    context = SimpleNamespace(original=root, request=SimpleNamespace(platform_rollout=reference,
        resources=SimpleNamespace(switch=SimpleNamespace(render=SimpleNamespace(after=after)))))
    baseline = SimpleNamespace(context=SimpleNamespace(inputs=SimpleNamespace(candidate=inputs['candidate'])))
    assert await entry._platform_successor(context, baseline) == 'selected-current-config'
    assert len(calls) == 1
    assert 'emulated_auth_execution_target' not in before.installation.foundation.platform_config
