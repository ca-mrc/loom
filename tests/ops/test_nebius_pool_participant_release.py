"""A normal candidate must stay a shared participant, not restore local writers."""
from __future__ import annotations

import copy
import importlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import yaml
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_migration import migration_request
from tests.ops.test_nebius_pool_runtime import env
from tests.ops.test_nebius_staging_pool_attachment import (
    ROOT,
    prepare,
)
from tests.ops.test_nebius_staging_pool_attachment import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render as render_manager
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

from loom.nebius_platform_render import build_platform
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation import PoolInstallation


def module():
    name = 'scripts.ops.nebius_pool_participant_release'
    assert importlib.util.find_spec(name) is not None, 'shared participant normal-release projection is missing'
    return importlib.import_module(name)


@pytest.fixture
def release_inputs(staging_attachment_inputs):
    from scripts.ops.nebius_pool_runtime import participant_readonly_roles, wire_participant

    options, config = copy.deepcopy(staging_attachment_inputs)
    previous = migration_request(('staging', 'development'))
    spec = previous.registration.spec.model_dump(mode='json')
    payload = options['participant'].model_dump(mode='json')
    payload.update(pool_id=spec['pool_id'], installation_id=spec['installation_id'], admission_epoch=spec['admission_epoch'],
        participant_id=spec['participants'][0]['participant_id'])
    options['participant'] = PoolParticipantV1.model_validate(payload)
    attached = prepare(options)
    # Normal rollout reads the persisted, renderer-normalized environment, not
    # the pre-render author input (which can omit default resource requests).
    config = copy.deepcopy(attached.config)
    spec['participants'][0] = attached.participant.model_dump(mode='json')
    spec.update(cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'])
    spec['node_selector'] = copy.deepcopy(attached.profiles.execution[0].runtime.node_selector)
    for kind, runtime in (('execution', 'runtime'), ('task_images', 'target')):
        other = spec['profiles'][kind][1]
        other[runtime]['node_selector'] = spec['node_selector']
        spec['profiles'][kind] = attached.profiles.model_dump(mode='json')[kind] + [other]
    spec = PoolInstallation.model_validate(spec)
    previous = replace(previous, registration=replace(previous.registration, spec=spec, candidate=options['candidate']),
        guards=(attached.guard, previous.guards[1]))
    files = build_platform(config, options['candidate'], attached.runtime_profile.model_dump(mode='json'),
        spec.profiles.image_admission_keyring, repo_root=ROOT)
    observed = {_key(row): row for row in options['observation']['resources']}
    current = {_key(row): copy.deepcopy(observed.get(_key(row), row)) for group in files.values() for row in group}
    for row in current.values():
        row['metadata'].setdefault('uid', str(uuid4()))
        row['metadata'].setdefault('resourceVersion', '17')
    ordinary, *guests = attached.actuators
    wired = wire_participant(request=previous, participant_id=attached.participant.participant_id,
        management_origin='https://manage.example.test', actuator=ordinary, guest_actuators=tuple(guests),
        service=current[f'Deployment:{config["namespace"]}:loom-service'], runtime_profile=attached.runtime_profile)
    marker = 'loom.nebius/shared-pool-handoff-operation'
    for row in wired.values():
        row['spec']['replicas'] = 1
        row['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
        current[_key(row)] = row
    for role in participant_readonly_roles(request=previous):
        if role['metadata'].get('namespace') not in {attached.participant.execution_namespace.name,
                attached.participant.build_namespace.name}:
            continue
        retained = current[_key(role)]
        for key in ('rules', 'subjects', 'roleRef'):
            if key in role:
                retained[key] = copy.deepcopy(role[key])
        retained['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
    collector = current[f'CronJob:{config["execution_namespace"]}:loom-execution-capacity-collector']
    collector['spec']['suspend'] = True
    collector['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
    candidate = copy.deepcopy(options['candidate'])
    candidate['candidate_sha'] = 'd' * 40
    for value in candidate['images'].values():
        value['image_ref'] = value['image_ref'].split('@')[0] + '@sha256:' + 'e' * 64
    profile = attached.runtime_profile.model_dump(mode='json')
    refs = (candidate['images']['service']['image_ref'], candidate['images']['execution_runtime']['image_ref'])
    profile.update(candidate_sha=candidate['candidate_sha'], task_image_ref=refs[0], runtime_image_ref=refs[1],
        runtime_binary_sha256='sha256:' + 'f' * 64,
        image_admission=signed_image_admission_bundle(refs).model_dump(mode='json'))
    return dict(previous=previous, current=tuple(current.values()), config=config, candidate=candidate,
        runtime_profile=ServiceExecutionRuntimeProfileV1.model_validate(profile), operation_id=uuid4(),
        management_origin='https://manage.example.test', repo_root=ROOT)


def test_normal_release_keeps_shared_wiring_and_preserves_retired_and_other_owner_authority(release_inputs):
    from loom_service.pool_management.participant_refresh import PoolParticipantRefresh

    before = copy.deepcopy(release_inputs)
    plan = module().prepare_staging_pool_release(**release_inputs)
    assert release_inputs == before
    assert module().prepare_staging_pool_release(**release_inputs) == plan
    refresh = PoolParticipantRefresh.model_validate(plan.refresh.model_dump())
    previous = release_inputs['previous']
    assert refresh.before == previous.registration.spec
    assert refresh.after.policy_revision == refresh.before.policy_revision + 1
    assert refresh.after.participants[1] == refresh.before.participants[1]
    assert refresh.after.machines == refresh.before.machines
    selected = refresh.after.participants[0]
    assert selected.participant_id == refresh.participant_id
    original = refresh.before.participants[0]
    assert {row.profile_id for row in selected.targets}.isdisjoint(row.profile_id for row in original.targets)
    assert len(plan.files['30-migrate.yaml']) == len(plan.files['50-configure.yaml']) == 1
    rows = {_key(row): row for group in plan.files.values() for row in group}
    namespace = release_inputs['config']['namespace']
    cp = rows[f'Deployment:{namespace}:loom-control-plane']
    api = rows[f'Deployment:{namespace}:loom-service']
    control = PoolRuntimeSettings.model_validate_json(env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
    assert control.participant == selected and control.management_origin == 'https://manage.example.test'
    assert json.loads(env(api)['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])['data_environment_id'] == str(selected.environment_id)
    workloads = [cp, api]
    for index, target in enumerate(selected.targets):
        name = 'loom-execution-actuator' if index == 0 else target.target_id + '-actuator'
        actuator = rows[f'Deployment:{selected.execution_namespace.name}:{name}']
        runtime = PoolRuntimeSettings.model_validate_json(env(actuator)['LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL']['value'])
        assert runtime.participant == selected
        assert ('LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER' in env(actuator)) == (index == 0)
        workloads.append(actuator)
    current = {_key(row): row for row in release_inputs['current']}
    for row in workloads:
        assert row['spec']['replicas'] == 1 and row['spec']['strategy'] == {'type': 'Recreate'}
        assert row['metadata']['annotations']['loom.nebius/shared-pool-handoff-operation'] == str(refresh.before.operation_id)
        assert row['spec']['template']['spec']['containers'][0]['image'].endswith('@sha256:' + 'e' * 64)
    for key, row in plan.preserved.items():
        assert row == current[key]
        assert key not in rows
    collector = f'CronJob:{selected.execution_namespace.name}:loom-execution-capacity-collector'
    assert plan.preserved[collector]['spec']['suspend'] is True
    assert not any(row['kind'] in {'ClusterRole', 'ClusterRoleBinding'} for row in rows.values())
    for ns, name in ((selected.execution_namespace.name, 'loom-execution-actuator'),
            (selected.build_namespace.name, 'loom-task-image-builder')):
        role = rows[f'Role:{ns}:{name}']
        assert {verb for rule in role['rules'] for verb in rule['verbs']} <= {'get', 'list'}
        assert role['metadata']['annotations'] == current[_key(role)]['metadata']['annotations']
    # Source config/profile follows the selected candidate, not an old render.
    configmap = rows[f'ConfigMap:{namespace}:loom-platform-config']
    assert json.loads(configmap['data']['profile.json'])['candidate_sha'] == 'd' * 40
    assert plan.migration.registration.spec == refresh.after


def test_successive_normal_releases_keep_original_handoff_lineage_and_current_registration(release_inputs):
    options = copy.deepcopy(release_inputs)
    first = module().prepare_staging_pool_release(**options)
    current = {_key(row): copy.deepcopy(row) for row in options['current']}
    for rows in first.files.values():
        for desired in rows:
            key = _key(desired)
            metadata = current.get(key, {}).get('metadata', {})
            row = copy.deepcopy(desired)
            row['metadata'].update(uid=metadata.get('uid', str(uuid4())), resourceVersion='22')
            current[key] = row
    options.update(previous=first.migration, current=tuple(current.values()), operation_id=uuid4())
    options['candidate']['candidate_sha'] = 'e' * 40
    for value in options['candidate']['images'].values():
        value['image_ref'] = value['image_ref'].split('@')[0] + '@sha256:' + 'f' * 64
    profile = options['runtime_profile'].model_dump(mode='json')
    refs = (options['candidate']['images']['service']['image_ref'], options['candidate']['images']['execution_runtime']['image_ref'])
    profile.update(candidate_sha='e' * 40, task_image_ref=refs[0], runtime_image_ref=refs[1],
        image_admission=signed_image_admission_bundle(refs).model_dump(mode='json'))
    options['runtime_profile'] = ServiceExecutionRuntimeProfileV1.model_validate(profile)
    second = module().prepare_staging_pool_release(**options)
    assert second.refresh.before == first.refresh.after
    assert second.refresh.after.policy_revision == first.refresh.before.policy_revision + 2
    assert second.refresh.after.participants[0].binding_revision == first.refresh.before.participants[0].binding_revision + 2
    assert second.refresh.after.participants[1] == first.refresh.before.participants[1]
    for rows in second.files.values():
        for row in rows:
            annotations = row['metadata'].get('annotations', {})
            if 'loom.nebius/participant-refresh-operation' in annotations:
                assert annotations['loom.nebius/participant-refresh-operation'] == str(options['operation_id'])
                assert annotations['loom.nebius/shared-pool-handoff-operation'] == str(first.refresh.before.operation_id)
    assert second.preserved == first.preserved


@pytest.mark.parametrize('damage', ['source', 'operation', 'scope', 'namespace-uid', 'global-participant',
    'global-origin', 'build-policy', 'runtime-profile', 'collector-active', 'readonly-role', 'role-binding', 'cluster-role'])
def test_normal_release_rejects_scope_or_live_binding_drift(release_inputs, damage):
    options = copy.deepcopy(release_inputs)
    current = {_key(row): row for row in options['current']}
    namespace, execution = options['config']['namespace'], options['config']['execution_namespace']
    cp = current[f'Deployment:{namespace}:loom-control-plane']
    if damage == 'source':
        options['candidate']['source_ref'] = 'refs/heads/feature/unqualified'
    elif damage == 'operation':
        options['operation_id'] = options['previous'].registration.spec.operation_id
    elif damage == 'scope':
        options['config']['execution_node_group_id'] = 'foreign'
    elif damage == 'namespace-uid':
        current[f'Namespace:-:{execution}']['metadata']['uid'] = str(uuid4())
    elif damage in {'global-participant', 'global-origin'}:
        settings = json.loads(env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
        if damage == 'global-participant':
            settings['participant']['incarnation'] = str(uuid4())
        else:
            settings['management_origin'] = 'https://foreign.example.test'
        env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'] = json.dumps(settings)
    elif damage == 'build-policy':
        options['config']['task_image_builder']['max_concurrent'] += 1
        current[f'ConfigMap:{namespace}:loom-platform-config']['data']['environment.json'] = json.dumps(options['config'])
    elif damage == 'runtime-profile':
        options['runtime_profile'] = options['runtime_profile'].model_copy(update={'candidate_sha': 'f' * 40})
    elif damage == 'collector-active':
        current[f'CronJob:{execution}:loom-execution-capacity-collector']['spec']['suspend'] = False
    elif damage == 'readonly-role':
        current[f'Role:{execution}:loom-execution-actuator']['rules'][0]['verbs'].append('create')
    elif damage == 'role-binding':
        current[f'RoleBinding:{execution}:loom-execution-actuator']['subjects'][0]['name'] = 'foreign'
    else:
        current[f'ClusterRole:{"-"}:{execution}-actuator-usage']['rules'][0]['resources'] = ['nodes/proxy']
    with pytest.raises(ValueError):
        module().prepare_staging_pool_release(**options)


@pytest.fixture
def catalog_inputs(release_inputs, management_inputs):
    from scripts.ops.nebius_pool_runtime import wire_manager

    from loom_service.pool_management.installation_render import render_gateway

    plan = module().prepare_staging_pool_release(**release_inputs)
    previous = release_inputs['previous']
    inputs = copy.deepcopy(management_inputs)
    inputs[0].update(namespace=previous.registration.binding.namespace,
        installation_id=previous.registration.binding.installation_id)
    manager, = [row for row in render_manager(inputs).files['40-services.yaml'] if row['kind'] == 'Deployment']
    manager['metadata'].update(uid=str(uuid4()), resourceVersion='12')
    manager['metadata']['labels']['loom.nebius/management-installation'] = previous.registration.binding.installation_id
    manager = wire_manager(request=previous, original=manager)
    manager['spec']['replicas'] = 1
    catalog, = [row for row in render_gateway(plan.refresh.before, namespace=previous.registration.binding.namespace,
        service_image=manager['spec']['template']['spec']['containers'][0]['image'],
        kubernetes_endpoint=release_inputs['config']['kubernetes_api_server'])['configuration'] if row['kind'] == 'ConfigMap']
    catalog['metadata'].update(uid=str(uuid4()), resourceVersion='13')
    return dict(release=plan, manager=manager, catalog=catalog)


def test_manager_refresh_changes_only_catalog_reference_and_keeps_executable_credentials(catalog_inputs):
    from scripts.ops.nebius_ingress_stage import _snapshot

    from loom_service.pool_management.profiles import PoolProfileCatalog

    before = copy.deepcopy(catalog_inputs)
    result = module().prepare_manager_catalog_refresh(**catalog_inputs)
    assert catalog_inputs == before
    after = catalog_inputs['release'].refresh.after
    expected = _snapshot(catalog_inputs['manager'])
    volume, = [row for row in expected['spec']['template']['spec']['volumes'] if row['name'] == 'pool-profiles']
    volume['configMap']['name'] = 'loom-pool-profiles-' + after.operation_id.hex
    assert result['manager'] == expected
    assert result['catalog']['metadata']['name'] == volume['configMap']['name']
    assert result['catalog']['immutable'] is True
    assert 'uid' not in result['catalog']['metadata']
    assert PoolProfileCatalog.model_validate_json(result['catalog']['data']['profiles.json']) == after.profiles
    for action in ('observe', 'capacity', 'fence', 'stage', 'open'):
        observation_id = uuid4() if action in {'observe', 'capacity'} else None
        config, job = module().refresh_documents(catalog_inputs['release'], manager=catalog_inputs['manager'],
            action=action, observation_id=observation_id)
        pod = job['spec']['template']['spec']
        container, = pod['containers']
        assert container['command'] == ['python', '-m', 'loom_service.pool_management.participant_refresh', action]
        assert container['image'] == expected['spec']['template']['spec']['containers'][0]['image']
        assert job['spec']['backoffLimit'] == 0 and pod['restartPolicy'] == 'Never'
        assert pod['automountServiceAccountToken'] is False
        assert config['immutable'] is True
        assert json.loads(config['data']['refresh.json']) == catalog_inputs['release'].refresh.model_dump(mode='json')
        assert job['metadata']['name'] == ('loom-pool-' + action + '-' + observation_id.hex if observation_id else
            'loom-pool-refresh-' + after.operation_id.hex + '-' + action)


@pytest.mark.parametrize('action', ['observe', 'capacity'])
def test_refresh_readbacks_have_distinct_jobs_without_changing_the_request(catalog_inputs, action):
    """A completed pre-fence Job must not stand in for a post-open database read."""
    observations = []
    for _ in range(2):
        observation_id = uuid4()
        config, job = module().refresh_documents(catalog_inputs['release'], manager=catalog_inputs['manager'],
            action=action, observation_id=observation_id)
        assert job['metadata']['labels']['loom.nebius/pool-observation'] == str(observation_id)
        assert job['spec']['template']['metadata']['labels'] == job['metadata']['labels']
        observations.append((config, job))
    assert observations[0][0] == observations[1][0]
    assert observations[0][1]['metadata']['name'] != observations[1][1]['metadata']['name']
    assert observations[0][1]['spec']['template']['spec'] == observations[1][1]['spec']['template']['spec']


@pytest.mark.parametrize('action,identity', [('observe', None), ('observe', UUID(int=0)),
    ('capacity', None), ('capacity', UUID(int=0)), ('fence', uuid4()), ('stage', uuid4()), ('open', uuid4())])
def test_refresh_mutation_identity_cannot_be_rotated_to_retry(catalog_inputs, action, identity):
    with pytest.raises(ValueError):
        module().refresh_documents(catalog_inputs['release'], manager=catalog_inputs['manager'],
            action=action, observation_id=identity)


@pytest.mark.parametrize('failure', [None, 'migrate', 'configure', 'actuator', 'https', 'readback'])
def test_shared_candidate_uses_full_normal_rollout_without_restoring_local_writers(release_inputs, tmp_path, monkeypatch, failure):
    """Missing schema/config phases or restoring the collector must break this path.

    This exercises the common workload executor, not the still-pending protected
    shared parent. Kubernetes and public HTTPS are the external test boundaries.
    """
    from scripts.ops import deploy_nebius_platform as deploy
    from tests.ops.test_deploy_nebius_platform import FakeKubectl

    release = module().prepare_staging_pool_release(**release_inputs)
    config, files = release_inputs['config'], release.files
    for name, rows in files.items():
        (tmp_path / name).write_text(yaml.safe_dump_all(rows))

    class CandidateKube(FakeKubectl):
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            if args[0] == 'apply':
                filename = Path(args[2]).name
                if (failure, filename) in {('migrate', '30-migrate.yaml'), ('configure', '50-configure.yaml')}:
                    raise deploy.DeploymentError('ambiguous test write')
            if failure == 'actuator' and args[:3] == ('rollout', 'status', 'deployment/loom-execution-actuator'):
                raise deploy.DeploymentError('actuator not ready')
            return result

    kube = CandidateKube(config, files, database=True)
    for row in release_inputs['current']:
        kube.objects[row['kind'].lower(), row['metadata']['name']] = copy.deepcopy(row)
    before_collector = copy.deepcopy(kube.objects['cronjob', 'loom-execution-capacity-collector'])
    events = []

    def smoke(origin, environment):
        # This attachment fixture retains the historical development catalog
        # label; normal release must not silently reclassify the environment.
        assert origin == 'https://' + config['public_host'] and environment == 'development'
        events.append('https')
        if failure == 'https':
            raise deploy.DeploymentError('HTTPS not ready')
        if failure == 'readback':
            kube.objects['deployment', 'loom-service']['spec']['template']['spec']['containers'][0]['image'] = 'wrong'

    monkeypatch.setattr(deploy, 'public_smoke', smoke)
    kwargs = dict(kube=kube, manifest={'public_origin': 'https://' + config['public_host']}, config=config,
        files=files, snapshot_root=tmp_path, retry_failed_jobs=False, phase=events.append,
        before_write=lambda: events.append('write-intent'))
    if failure:
        with pytest.raises(deploy.DeploymentError):
            deploy._rollout_candidate_files(**kwargs)
    else:
        deploy._rollout_candidate_files(**kwargs)
        assert [event for event in events if event not in {'write-intent', 'https'}] == [
            '00-namespaces', '10-config-network', '20-database', 'database-ready', '80-backup', '30-migrate',
            '40-services', '50-configure', '60-execution', '70-public', 'public-load-balancer-ready',
            'public-https-smoke', 'candidate-readback']
        for name in ('30-migrate.yaml', '50-configure.yaml'):
            assert deploy.job_complete(kube.objects['job', files[name][0]['metadata']['name']])
        for row in files['60-execution.yaml']:
            if row['kind'] == 'Deployment':
                assert ('rollout', 'status', 'deployment/' + row['metadata']['name'], '-n',
                    config['execution_namespace'], '--timeout=300s') in kube.commands
        settings = env(kube.objects['deployment', 'loom-execution-actuator'])
        assert PoolRuntimeSettings.model_validate_json(settings['LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL']['value']).participant == release.refresh.after.participants[0]
        assert events[-1] == 'candidate-readback'
    applied = [Path(command[2]).name for command in kube.commands if command[0] == 'apply']
    if failure in {'migrate', 'configure'}:
        assert '60-execution.yaml' not in applied
    if failure == 'actuator':
        assert '70-public.yaml' not in applied
    assert kube.objects['cronjob', 'loom-execution-capacity-collector'] == before_collector
    assert not any(command[0] in {'exec', 'patch', 'delete', 'create'} for command in kube.commands)
    for command in kube.commands:
        if command[0] == 'apply':
            assert not any(row['kind'] in {'ClusterRole', 'ClusterRoleBinding'}
                for row in yaml.safe_load_all(Path(command[2]).read_text()))
    for row in files['60-execution.yaml']:
        if row['kind'] == 'Role':
            assert all(set(rule['verbs']) <= {'get', 'list', 'watch'} for rule in row['rules'])


@pytest.mark.parametrize('outcome', ['complete', 'busy', 'preflight', 'guard-drift', 'activation-unknown', 'capacity-stale',
    'recover-activation-unknown', 'recover-capacity-stale', 'guard-lost', 'guard-lost-after-activation',
    'capacity-missing', 'capacity-expired', 'capacity-future', 'capacity-expires-before-release',
    'recover-capacity-expired', 'capacity-exceeds-policy', 'capacity-elapsed', 'capacity-clock-reversal', 'slow-reopen'])
def test_guarded_deployer_orders_shared_refresh_and_never_releases_uncertain_mutations(release_inputs, tmp_path, monkeypatch, outcome):
    """Use the real deploy parent; only the protected pool I/O adapter is doubled."""
    import argparse
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from scripts.ops import deploy_nebius_platform as deploy
    from tests.ops.test_deploy_nebius_platform import FakeKubectl

    from loom.nebius_platform_render import write_platform
    from loom_service.pool_management.participant_refresh import ParticipantCapacityReceipt

    release = module().prepare_staging_pool_release(**release_inputs)
    config = release_inputs['config']
    render_dir = tmp_path / 'render'
    write_platform(release.files, config, release_inputs['candidate'], render_dir)
    args = argparse.Namespace(render_dir=render_dir, kubeconfig=tmp_path / 'kubeconfig',
        evidence_dir=tmp_path / 'evidence', expected_cluster_id=config['cluster_id'],
        apply=True, retry_failed_jobs=False)
    events = []
    pool = {'phase': 'ready', 'capacity': False}
    fault = outcome.removeprefix('recover-')
    registry_mutations = []
    observations = []
    clock = {'now': datetime.now(UTC), 'offset': 0}
    monotonic = deploy.time.monotonic
    monkeypatch.setattr(deploy, 'time', SimpleNamespace(
        monotonic=lambda: monotonic() + clock['offset'], sleep=deploy.time.sleep))

    class Clock:
        @staticmethod
        def now(_zone):
            return clock['now']

    monkeypatch.setattr(deploy, 'datetime', Clock)
    binding = deploy._participant_release_binding

    def checked_binding(*values):
        result = binding(*values)
        if fault == 'capacity-expires-before-release' and pool['capacity']:
            clock['now'] += timedelta(seconds=61)
        if fault == 'capacity-elapsed' and pool['capacity']:
            clock['offset'] += 61
        if fault == 'capacity-clock-reversal' and pool['capacity']:
            clock['now'] -= timedelta(seconds=1)
        return result

    monkeypatch.setattr(deploy, '_participant_release_binding', checked_binding)

    class Kube(FakeKubectl):
        def get(self, kind, name, namespace):
            result = super().get(kind, name, namespace)
            if kind == 'validatingadmissionpolicy' and result:
                result['metadata']['generation'] = 1
                result['status'] = {'observedGeneration': 1}
            return result

        def run(self, *command, **kwargs):
            if command[0] == 'apply':
                assert pool['phase'] in {'staged', 'open'} and self.guard_identity is not None
                events.append('workload:' + Path(command[2]).name)
                if '--dry-run=server' in command:
                    self.commands.append(command)
                    pod = yaml.safe_load(Path(command[2]).read_text())
                    if pod['spec']['containers'][0]['securityContext']['capabilities'].get('add'):
                        raise deploy.TaskIdentityPolicyDeniedError(config['execution_namespace'] + '-private-root-v1')
                    return ''
            if command[0] == 'exec' and 'release' in command:
                assert pool['phase'] == 'ready' or (pool['phase'] == 'open' and pool['capacity'])
                events.append('release')
            result = super().run(*command, **kwargs)
            if command[:2] == ('create', 'job') and fault == 'guard-lost':
                self.guard_identity = None
                events.append('lost-guard')
            if command[:2] == ('config', 'view'):
                view = json.loads(result)
                view['clusters'][0]['name'] = config['cluster_id']
                return json.dumps(view)
            return result

    kube = Kube(config, release.files, database=True)
    for row in release_inputs['current']:
        kube.objects[row['kind'].lower(), row['metadata']['name']] = copy.deepcopy(row)
    monkeypatch.setattr(deploy, 'public_smoke', lambda *_: events.append('https'))

    class PoolIO:
        def preflight(self, observed_kube, plan):
            assert observed_kube is kube and plan == release
            assert kube.guard_identity is None or getattr(args, 'resume_guard_owner', None) == kube.guard_identity[0]
            events.append('preflight')
            if fault == 'preflight':
                raise deploy.DeploymentError('retained manager does not support this catalog')

        def under_guard(self, observed_kube, plan):
            assert observed_kube is kube and plan == release and kube.guard_identity is not None
            events.append('guarded')
            if fault == 'guard-drift':
                raise deploy.DeploymentError('current shared history changed')

        def activate(self, observed_kube, plan):
            assert observed_kube is kube and plan == release and kube.guard_identity is not None
            events.append('activate')
            if fault == 'busy':
                return 'busy'  # Fixed fence receipt positively proves no change.
            if pool['phase'] == 'ready':
                registry_mutations.append('stage')
                pool['phase'] = 'staged'
            if fault == 'activation-unknown':
                raise deploy.DeploymentError('lost write response')
            if fault == 'guard-lost-after-activation':
                kube.guard_identity = None
                events.append('lost-guard-after-activation')
            return 'ready'

        def finish(self, observed_kube, plan):
            assert observed_kube is kube and plan == release and kube.guard_identity is not None
            assert 'https' in events
            events.append('open')
            if pool['phase'] != 'open':
                registry_mutations.append('open')
            pool['phase'] = 'open'
            if fault == 'slow-reopen':
                clock['offset'] += 61

        def read_capacity(self, observed_kube, plan, *, observation_id):
            assert observed_kube is kube and plan == release and pool['phase'] == 'open'
            assert kube.guard_identity is not None
            records = [json.loads(path.read_text()) for path in args.evidence_dir.glob('deployment-*.json')]
            pending, = (row for row in records if row['status'] == 'running')
            assert pending['pool_refresh']['capacity_observation_id'] == str(observation_id)
            observations.append(observation_id)
            if fault == 'capacity-stale':
                raise deploy.DeploymentError('post-open capacity unavailable')
            pool['capacity'] = True
            events.append('fresh-capacity')
            if fault == 'capacity-missing':
                return None
            checked = clock['now']
            if fault == 'capacity-expired':
                checked -= timedelta(seconds=180)
            elif fault == 'capacity-future':
                checked += timedelta(seconds=180)
            elif fault == 'capacity-elapsed':
                checked += timedelta(seconds=30)  # Database clock ahead, still within permitted skew.
            validity = 120 if fault == 'capacity-exceeds-policy' else 60
            return ParticipantCapacityReceipt(checked_at=checked, valid_until=checked + timedelta(seconds=validity),
                selected_observation_id=uuid4(), selected_observed_at=checked,
                connected_pools=1, observations_sha256='a' * 64)

    if fault in {'complete', 'busy', 'slow-reopen'}:
        report = deploy.deploy(args, kube=kube, participant_release=release, pool=PoolIO())
        assert report['status'] == ('skipped_busy' if outcome == 'busy' else 'complete')
    else:
        with pytest.raises(deploy.DeploymentError):
            deploy.deploy(args, kube=kube, participant_release=release, pool=PoolIO())
    if outcome == 'complete':
        assert events.index('guarded') < events.index('activate') < events.index('workload:30-migrate.yaml')
        assert events.index('workload:50-configure.yaml') < events.index('open') < events.index('fresh-capacity') < events.index('release')
    if fault in {'busy', 'preflight', 'guard-drift', 'activation-unknown', 'guard-lost', 'guard-lost-after-activation'}:
        assert not any(event.startswith('workload:') for event in events)
    assert {'preflight': 'preflight', 'guard-drift': 'guarded', 'activation-unknown': 'activate',
        'capacity-stale': 'open', 'complete': 'fresh-capacity', 'busy': 'activate', 'guard-lost': 'lost-guard',
        'guard-lost-after-activation': 'lost-guard-after-activation', 'capacity-missing': 'fresh-capacity',
        'capacity-expired': 'fresh-capacity', 'capacity-future': 'fresh-capacity',
        'capacity-expires-before-release': 'fresh-capacity', 'capacity-exceeds-policy': 'fresh-capacity',
        'capacity-elapsed': 'fresh-capacity', 'capacity-clock-reversal': 'fresh-capacity',
        'slow-reopen': 'fresh-capacity'}[fault] in events
    assert (kube.guard_identity is not None) == (fault in {'activation-unknown', 'capacity-stale',
        'capacity-missing', 'capacity-expired', 'capacity-future', 'capacity-expires-before-release',
        'capacity-exceeds-policy', 'capacity-elapsed', 'capacity-clock-reversal'})
    if outcome.startswith('recover-'):
        args.resume_guard_owner = kube.guard_identity[0]
        args.retry_failed_jobs = True
        fault = None
        assert deploy.deploy(args, kube=kube, participant_release=release, pool=PoolIO())['status'] == 'complete'
        assert kube.guard_identity is None and pool['capacity'] is True
        assert registry_mutations == ['stage', 'open']
    assert all(isinstance(identity, UUID) and identity.int for identity in observations)
    assert len(observations) == len(set(observations))


@pytest.mark.parametrize('damage', ['missing-adapter', 'missing-release', 'standalone-files', 'wrong-guard', 'retire-target'])
def test_shared_deploy_has_no_unbound_mode_or_standalone_render_escape(release_inputs, tmp_path, damage):
    import argparse

    from scripts.ops import deploy_nebius_platform as deploy
    from tests.ops.test_deploy_nebius_platform import FakeKubectl

    from loom.nebius_platform_render import write_platform

    release = module().prepare_staging_pool_release(**release_inputs)
    config = release_inputs['config']
    files = release.files
    if damage == 'standalone-files':
        files = build_platform(config, release_inputs['candidate'], release_inputs['runtime_profile'].model_dump(mode='json'),
            release.refresh.after.profiles.image_admission_keyring, repo_root=ROOT)
    write_platform(files, config, release_inputs['candidate'], tmp_path / 'render')
    args = argparse.Namespace(render_dir=tmp_path / 'render', kubeconfig=tmp_path / 'kubeconfig',
        evidence_dir=tmp_path / 'evidence', expected_cluster_id=config['cluster_id'],
        apply=True, retry_failed_jobs=False)
    if damage == 'wrong-guard':
        args.resume_guard_owner = 'rollout-' + uuid4().hex
    if damage == 'retire-target':
        args.retire_target = 'different-target'
    kube = FakeKubectl(config, files, database=True)

    class ForbiddenAdapter:
        def preflight(self, *_):
            pytest.fail('invalid shared binding reached pool I/O')

    with pytest.raises(deploy.DeploymentError):
        deploy.deploy(args, kube=kube,
            participant_release=None if damage == 'missing-release' else release,
            pool=None if damage == 'missing-adapter' else ForbiddenAdapter())
    assert not any(command[0] in {'apply', 'create', 'delete', 'exec', 'patch'} for command in kube.commands)


@pytest.mark.parametrize('damage', ['namespace', 'catalog', 'volume', 'mount', 'setting', 'mode', 'replicas', 'mutable-image'])
def test_manager_refresh_refuses_changed_catalog_or_process_binding(catalog_inputs, damage):
    options = copy.deepcopy(catalog_inputs)
    manager, catalog = options['manager'], options['catalog']
    pod = manager['spec']['template']['spec']
    container, = pod['containers']
    if damage == 'namespace':
        manager['metadata']['namespace'] = 'foreign'
    elif damage == 'catalog':
        catalog['data']['profiles.json'] = '{}'
    elif damage == 'volume':
        next(row for row in pod['volumes'] if row['name'] == 'pool-profiles')['configMap']['name'] = 'foreign'
    elif damage == 'mount':
        next(row for row in container['volumeMounts'] if row['name'] == 'pool-profiles')['mountPath'] = '/foreign'
    elif damage == 'setting':
        env(manager)['LOOM_SVC_POOL_PROFILES_FILE']['value'] = '/foreign/profiles.json'
    elif damage == 'mode':
        env(manager)['LOOM_SVC_SERVICE_MODE']['value'] = 'full'
    elif damage == 'replicas':
        manager['spec']['replicas'] = 0
    else:
        container['image'] = 'registry.example/service:latest'
    with pytest.raises(ValueError):
        module().prepare_manager_catalog_refresh(**options)
