"""Ordinary deployment proof selects exact source without widening retained authority."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import ssl
import zipfile
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import certifi
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
from tests.ops.test_nebius_pool_predecessor import finish_cutover, pool_refresh_http
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
def private_cutover(completed_upgrade, cutover_inputs, database_guard):
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


@pytest.mark.timeout(420)
@pytest.mark.parametrize('ordinary_rollout', [None, 'NETWORK_SSD_NON_REPLICATED',
    {'task_image_builder': {'max_concurrent': None}},
    {'execution_disk_type': 'NETWORK_SSD_NON_REPLICATED', 'task_image_builder': {'max_concurrent': None}}],
    ids=['ssd', 'nrd', 'resource-only-builder', 'nrd-resource-only-builder'], indirect=True)
def test_completed_ordinary_successor_preflight_is_read_only_and_rejects_drift(private_cutover, ordinary_rollout, connected_refresh, monkeypatch):
    import asyncio
    from contextlib import contextmanager

    from scripts.ops import nebius_management_refresh_entry as refresh_entry
    from scripts.ops import nebius_pool_cutover_entry as cutover_entry
    from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
    from tests.ops.test_nebius_pool_cutover_entry import save_private

    operation, _, root = private_cutover
    _, result = finish_cutover(operation, legacy=True)
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    successor = asyncio.run(ordinary_rollout[0]())
    metadata, payload, _ = private_refresh(root, pool)
    payload.update(manager_revision='0174', target_manager_revision='0177')
    payload['platform_rollout'] = successor.reference.model_dump(mode='json')
    payload['deployment']['installation']['foundation']['platform_config_json'] = json.dumps(successor.config)
    save_private(metadata, payload)
    selected_context = context(metadata)
    request, state = selected_context.request, Path(metadata['state_dir'])
    if (successor.config.get('execution_disk_type') == 'NETWORK_SSD_NON_REPLICATED'
            or successor.config['task_image_builder']['max_concurrent'] is None):
        # Valid disk/builder references grant no refresh authority without the
        # existing completed ordinary-rollout binding, including builder-only changes.
        unbound = copy.deepcopy(payload)
        unbound.pop('platform_rollout')
        save_private(metadata, unbound)
        with pytest.raises(refresh_entry.EntryError):
            context(metadata)
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            PoolManagerRefresh(root, pool, replace(request, platform_rollout=None), state).qualify()
        foreign = copy.deepcopy(payload)
        foundation = foreign['deployment']['installation']['foundation']
        config = json.loads(foundation['platform_config_json'])
        config['execution_disk_type'] = 'NETWORK_HDD'
        foundation['platform_config_json'] = json.dumps(config)
        save_private(metadata, foreign)
        with pytest.raises(refresh_entry.EntryError):
            context(metadata)
        save_private(metadata, payload)
    bound = PoolManagerRefresh(root, pool, request, state, successor=successor)
    original_history = {path: path.read_bytes() for path in pool.history}
    with pool_refresh_http(bound)() as (verifier, external):
        before = copy.deepcopy(external.objects)
        verifier.qualify()
        external.failure = 'manager_schema'
        with pytest.raises(ValueError):
            verifier.qualify()
        external.failure = None
        dormant = pool.context.request.fencing.retirement.dormant_consumers[0]
        for original in (dormant.actuator, dormant.collector):
            assert _key(original) not in successor.resources
            assert bound.successor_document(pool.completion.workloads[_key(original)]) == pool.completion.workloads[_key(original)]
            assert external.objects[_key(original)]['spec'].get('replicas', 0) == 0
            if original['kind'] == 'CronJob':
                assert external.objects[_key(original)]['spec']['suspend'] is True
        # Run the actual entry runtime qualifier with fixed reader credentials;
        # only remote Pod/SQL/telemetry I/O is doubled.
        guards = object.__new__(KubectlPoolGuardAPI)
        guards.request = pool.context.request.fencing.retirement.migration
        guards._get = lambda _kind, name, namespace: external.objects['Deployment:' + namespace + ':' + name]
        runtime_checks, telemetry_checks = [], []
        def database_probe(target, *, original, expected, credential_uid, credential_resource_version):
            assert expected == external.objects[_key(original)]
            binding = target.database
            actuator = original['metadata']['namespace'] != target.namespace
            assert (credential_uid, credential_resource_version) == ((binding.actuator_credential_uid,
                binding.actuator_credential_resource_version) if actuator else
                (binding.credential_uid, binding.credential_resource_version))
            runtime_checks.append(_key(original))
        def telemetry_probe(target, *, original, expected):
            assert expected == external.objects[_key(original)]
            telemetry_checks.append(_key(original))
        guards.qualify_runtime_database, guards.qualify_runtime_telemetry = database_probe, telemetry_probe
        cutover_entry.qualify_pool_runtime_databases(pool.context, guards, refresh=bound)
        assert len(runtime_checks) == len(guards.request.guards) * 3
        assert len(telemetry_checks) == len(guards.request.guards)
        old_api, live = connected_refresh
        live.request, live.directory, live.anchor, live.operation = request, state, Path(metadata['anchor_dir']), metadata
        live.manager.clear()
        live.manager.update(copy.deepcopy(pool.active))
        live.manager['metadata'].update(resourceVersion='30', generation=5)
        live.values.update(external.objects)
        live.values[_key(pool.active)] = live.manager
        external.objects = live.values
        connections = []
        @contextmanager
        def connect(inputs, ingress, *, foundation_candidate):
            assert inputs == root.original_inputs and ingress == root.ingress
            assert foundation_candidate == selected_context.inputs.foundation_candidate
            yield old_api.checks, old_api.ssl_context, old_api.token
        @contextmanager
        def reader(actual, *, refresh):
            assert actual == pool.context and refresh == bound
            connections.append(refresh)
            yield verifier.parent
        async def publication(actual, baseline):
            assert actual == selected_context and baseline == pool
            return successor
        trust = ssl.create_default_context(cafile=certifi.where())
        certificate = ssl.DER_cert_to_PEM_cert(trust.get_ca_certs(binary_form=True)[0]).encode()
        def ca(path, limit):
            # The old install fixture stores the literal 'fixture-ca'. Supply
            # valid read-only TLS trust at this external certificate boundary.
            assert path == root.original_inputs.operator_connection.ca_file
            return certificate
        monkeypatch.setattr(refresh_entry, '_private', ca)
        monkeypatch.setattr(refresh_entry, 'connected_checks', connect)
        monkeypatch.setattr(refresh_entry, 'ApplicationUpgradePrerequisites', lambda **_: old_api.checks)
        monkeypatch.setattr(refresh_entry, '_platform_successor', publication)
        monkeypatch.setattr(cutover_entry, 'connected_pool_api', reader)
        before = copy.deepcopy(external.objects)
        try:
            report = refresh_entry.execute_refresh(selected_context, metadata, 'preflight')
        except Exception as error:
            pytest.fail('owning preflight stage: ' + str(getattr(error, 'stage', type(error).__name__)))
        assert report['status'] == 'preflight_qualified'
        assert len(connections) == 1
        assert all(call.method == 'GET' for call in live.calls)
        assert external.objects == before
        assert {path: path.read_bytes() for path in original_history} == original_history
        assert not state.exists()
        assert all(call.method == 'GET' or call.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
            for call in external.calls)
        key = 'Deployment:' + successor.config['namespace'] + ':loom-service'
        actual = external.objects[key]
        for damage in ('image', 'env', 'uid', 'destination'):
            saved = copy.deepcopy(actual)
            if damage == 'image':
                actual['spec']['template']['spec']['containers'][0]['image'] += '-unrelated'
            elif damage == 'env':
                actual['spec']['template']['spec']['containers'][0]['env'].append({'name': 'UNRELATED', 'value': 'yes'})
            elif damage == 'uid':
                actual['metadata']['uid'] = 'foreign'
            else:
                actual['metadata']['namespace'] = 'foreign'
            with pytest.raises(ValueError):
                verifier.qualify()
            actual.clear()
            actual.update(saved)
        verifier.qualify()
        assert external.objects == before


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
