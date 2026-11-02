from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from scripts.ops import nebius_execution_tuning as tuning
from scripts.ops.deploy_nebius_platform import DeploymentError

from loom.nebius_platform_render import build_platform
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

ROOT = Path(__file__).resolve().parents[2]


class FakeKube:
    def __init__(self, inputs: tuple):
        config, candidate, profile = inputs
        self.config = config
        self.objects = {}
        for documents in build_platform(config, candidate, profile, {}, repo_root=ROOT).values():
            for doc in documents:
                key = (doc['kind'].lower(), doc['metadata'].get('namespace'), doc['metadata']['name'])
                self.objects[key] = deepcopy(doc)
                if doc['kind'] == 'Deployment':
                    self.objects[key]['status'] = {'readyReplicas': doc['spec'].get('replicas', 1)}
                if doc['kind'] == 'ResourceQuota':
                    self.objects[key]['metadata']['resourceVersion'] = '1'
        self.cm = self.objects['configmap', config['namespace'], 'loom-platform-config']
        self.cm['metadata']['resourceVersion'] = '1'
        self.policies = {target: {**deepcopy(row['capacity_policy']), 'version': 1}
                         for target, row in tuning._owners(config).items()}
        self.calls: list[tuple] = []
        self.fail_set = False
        self.guard_held = False
        self.tuning_owner = None
        self.builder_request_support = True
        self.builder_cache_budget_support = True
        self.builder_configuration_source = 'actuator'

    def get(self, kind, name, namespace):
        return deepcopy(self.objects.get((kind, namespace, name), {}))

    def run(self, *args, **kwargs):
        self.calls.append(args)
        namespace = args[args.index('-n') + 1]
        if args[0] == 'get':
            if args[1] == 'jobs':
                return json.dumps({'items': [row for key, row in self.objects.items()
                                            if key[0] == 'job' and key[1] == namespace]})
            return json.dumps({'items': [row for key, row in self.objects.items()
                                        if key[0] == 'deployment' and key[1] == namespace]})
        if args[0] == 'exec':
            if 'psql' in args:
                return '1' if self.guard_held else '0'
            program = args[args.index('-c') + 1]
            if program == tuning._GUARD:
                query, owner, candidate = args[-3:]
                if query == tuning._TUNING_ACQUIRE_SQL:
                    ok = not (self.guard_held or self.tuning_owner)
                    if ok:
                        self.tuning_owner = (owner, candidate)
                else:
                    assert query == tuning._TUNING_RELEASE_SQL
                    ok = self.tuning_owner == (owner, candidate)
                    if ok:
                        self.tuning_owner = None
                return json.dumps({'ok': ok})
            if program == tuning._API:
                method, path, body = args[-3:]
                if method == 'GET':
                    return json.dumps({'targets': [{'target_id': key, 'policy': value}
                                                  for key, value in self.policies.items()]})
                assert method == 'PUT' and path.startswith('/admin/execution-capacity-policies/')
                target = path.rsplit('/', 1)[-1]
                self.policies[target] = {**json.loads(body), 'version': self.policies[target]['version'] + 1}
                return json.dumps(self.policies[target])
            name = next(value.removeprefix('deployment/') for value in args if value.startswith('deployment/'))
            env = tuning._env(self.objects['deployment', namespace, name])
            if name == 'loom-control-plane':
                return json.dumps({'placement_policy': env['LOOM_CP_SERVICE_EXECUTION_PLACEMENT_POLICY']})
            builder = json.loads(env['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER']) if env.get('LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER') else None
            return json.dumps({'command_concurrency': int(env['LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY']),
                'builder_enabled': builder is not None, 'builder_request_support': self.builder_request_support,
                'builder_cache_budget_support': self.builder_cache_budget_support,
                'builder_configuration_source': self.builder_configuration_source,
                'builder_resources': None if builder is None else {
                    'effective_cpu_request_millis': builder.get('cpu_request_millis') or builder['cpu_millis'],
                    'cache_total_mib': builder.get('cache_total_mib', 4096) if self.builder_cache_budget_support else None,
                    **{k: builder[k] for k in ('cpu_millis', 'memory_mib', 'ephemeral_storage_mib', 'namespace')},
                }})
        if args[0] == 'patch':
            if args[1] == 'resourcequota':
                quota = self.objects['resourcequota', namespace, args[2]]
                operations = json.loads(args[-1])
                assert operations[0] == {'op': 'test', 'path': '/metadata/resourceVersion',
                                         'value': quota['metadata']['resourceVersion']}
                assert operations[1]['op'] == 'replace' and operations[1]['path'] == '/spec/hard'
                quota['spec']['hard'] = operations[1]['value']
                quota['metadata']['resourceVersion'] = str(int(quota['metadata']['resourceVersion']) + 1)
                return ''
            assert args[1:3] == ('configmap', 'loom-platform-config')
            operations = json.loads(args[-1])
            assert operations[0] == {'op': 'test', 'path': '/metadata/resourceVersion',
                                     'value': self.cm['metadata']['resourceVersion']}
            for op in operations[1:]:
                self.cm['data'][op['path'].removeprefix('/data/')] = op['value']
            self.cm['metadata']['resourceVersion'] = str(int(self.cm['metadata']['resourceVersion']) + 1)
            return ''
        if args[:2] == ('set', 'env'):
            if self.fail_set:
                self.fail_set = False
                raise DeploymentError('test controller update interrupted')
            name = args[2].removeprefix('deployment/')
            env = self.objects['deployment', namespace, name]['spec']['template']['spec']['containers'][0]['env']
            for change in args[args.index('-n') + 2:]:
                key, value = change.split('=', 1)
                existing = next((row for row in env if row['name'] == key), None)
                if existing is None:
                    env.append({'name': key, 'value': value})
                else:
                    existing['value'] = value
            return ''
        assert args[:2] == ('rollout', 'status')
        return 'ready'


@pytest.mark.parametrize('knob', ['placement', 'concurrency', 'rate'])
def test_each_tuning_knob_persists_independently_and_survives_next_render(regional_inputs, knob):
    config, candidate, profile = regional_inputs
    kube = FakeKube(regional_inputs)
    images = {key: [row['image'] for row in doc['spec']['template']['spec']['containers']]
              for key, doc in kube.objects.items() if key[0] == 'deployment'}
    target = config['regional_execution_targets'][0]['target_id']
    kwargs = {'placement_policy': 'environment-local-capacity-balanced'} if knob == 'placement' else (
        {'command_concurrency': 2} if knob == 'concurrency' else {'create_rates': {target: 27}})
    result = tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], **kwargs)
    saved = json.loads(kube.cm['data']['environment.json'])
    assert saved.get('execution_placement_policy', 'environment-local-health-first') == (
        'environment-local-capacity-balanced' if knob == 'placement' else 'environment-local-health-first')
    assert saved.get('execution_command_concurrency', 1) == (2 if knob == 'concurrency' else 1)
    assert result['task_jobs_replaced'] is False
    assert all(images[key] == [row['image'] for row in doc['spec']['template']['spec']['containers']]
               for key, doc in kube.objects.items() if key[0] == 'deployment')
    next_candidate, next_profile = deepcopy(candidate), deepcopy(profile)
    next_candidate['candidate_sha'] = next_profile['candidate_sha'] = 'e' * 40
    updated = build_platform(saved, next_candidate, next_profile, {}, repo_root=ROOT)
    cm = next(row for row in updated['10-config-network.yaml'] if row['kind'] == 'ConfigMap'
              and row['metadata']['name'] == 'loom-platform-config')
    assert json.loads(cm['data']['environment.json']) == saved
    assert json.loads(cm['data']['profile.json'])['candidate_sha'] == 'e' * 40
    for group in updated.values():
        for row in group:
            if row['kind'] == 'Deployment' and 'LOOM_EXECUTION_ACTUATOR_TARGET_ID' in tuning._env(row):
                assert tuning._env(row)['LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY'] == str(
                    saved.get('execution_command_concurrency', 1))
    if knob == 'rate':
        assert saved['regional_execution_targets'][0]['capacity_policy']['max_create_per_minute'] == 27
        assert kube.policies[target]['max_create_per_minute'] == 27
        assert result['restarted_controllers'] == []
    else:
        assert all(not (args[0] == 'exec' and tuning._API in args and args[-3] == 'PUT') for args in kube.calls)
    assert all(args[0] in {'get', 'exec', 'patch', 'set', 'rollout'} for args in kube.calls)
    assert not any('cancel' in str(args) or 'delete' in str(args) for args in kube.calls)


def test_interrupted_controller_update_can_converge_from_saved_values(regional_inputs):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    kube.fail_set = True
    with pytest.raises(DeploymentError, match='interrupted'):
        tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], command_concurrency=2)
    assert json.loads(kube.cm['data']['environment.json'])['execution_command_concurrency'] == 2
    assert kube.tuning_owner is None
    version = kube.cm['metadata']['resourceVersion']
    result = tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], command_concurrency=2)
    assert result['persisted']['command_concurrency'] == 2
    assert kube.cm['metadata']['resourceVersion'] == version
    assert all(row.get('command_concurrency', 2) == 2 for row in result['controllers'])


@pytest.mark.parametrize('kwargs', [
    {'expected_candidate': 'd' * 40, 'command_concurrency': 2},
    {'command_concurrency': 9}, {'command_concurrency': True},
    {'create_rates': {'unknown-owner': 20}}, {'placement_policy': 'unknown-policy'},
])
def test_invalid_tuning_has_no_mutations(regional_inputs, kwargs):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    kwargs = {'expected_candidate': candidate['candidate_sha'], **kwargs}
    with pytest.raises((DeploymentError, ValueError)):
        tuning.apply(kube, config['namespace'], **kwargs)
    assert not any(args[0] in {'patch', 'set', 'rollout'} for args in kube.calls)
    assert kube.cm['metadata']['resourceVersion'] == '1'


def test_rate_change_preserves_live_non_rate_limits_or_rejects_drift(regional_inputs):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    target = config['target_id']
    kube.policies[target]['max_nodes'] += 1
    with pytest.raises(DeploymentError, match='non-rate capacity limits differ'):
        tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                     create_rates={target: 21})
    assert not any(args[0] == 'patch' for args in kube.calls)


def test_tuning_waits_for_existing_version_rollout_without_changing_its_guard(regional_inputs):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    kube.guard_held = True
    with pytest.raises(DeploymentError, match='version rollout owns'):
        tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                     command_concurrency=2)
    assert kube.guard_held is True
    assert not any(args[0] in {'patch', 'set', 'rollout'} for args in kube.calls)


def test_operator_cli_applies_and_reads_back_each_independent_knob(
    regional_inputs, monkeypatch, capsys,
):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    monkeypatch.setattr(tuning, 'Kubectl', lambda path: kube)
    common = ['execution-tuning', 'apply', '--kubeconfig', '/operator/config',
              '--namespace', config['namespace'], '--expected-candidate', candidate['candidate_sha']]
    target = config['target_id']
    for flags in (
        ['--placement-policy', 'environment-local-capacity-balanced'],
        ['--command-concurrency', '2'], ['--create-rate', target + '=27'],
    ):
        monkeypatch.setattr('sys.argv', common + flags)
        tuning.main()
        result = json.loads(capsys.readouterr().out)
        assert result['candidate_sha'] == candidate['candidate_sha']
        assert result['task_jobs_replaced'] is False
    monkeypatch.setattr('sys.argv', [*common[:1], 'inspect', *common[2:6]])
    tuning.main()
    result = json.loads(capsys.readouterr().out)
    assert result['persisted']['placement_policy'] == 'environment-local-capacity-balanced'
    assert result['persisted']['command_concurrency'] == 2
    assert result['persisted']['create_rates'][target] == result['live_create_rates'][target] == 27


def test_postgres_staging_does_not_restart_database_and_survives_later_deploy(regional_inputs):
    config, candidate, profile = regional_inputs
    kube = FakeKube(regional_inputs)
    original_database = deepcopy(kube.objects['statefulset', config['namespace'], 'loom-postgres'])
    settings = {'cpu_request_millis': 1000, 'cpu_limit_millis': 3000,
                'memory_request_mib': 1024, 'memory_limit_mib': 2048, 'shared_buffers_mib': 512}
    result = tuning.stage_postgres(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  settings=settings)
    assert result['database_restarted'] is False and result['effective_on_next_deployment'] is True
    assert kube.objects['statefulset', config['namespace'], 'loom-postgres'] == original_database
    assert not any(args[0] in {'set', 'rollout'} for args in kube.calls)
    tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], command_concurrency=2)
    saved = json.loads(kube.cm['data']['environment.json'])
    assert saved['postgres_settings'] == settings
    next_candidate, next_profile = deepcopy(candidate), deepcopy(profile)
    next_candidate['candidate_sha'] = next_profile['candidate_sha'] = 'e' * 40
    files = build_platform(saved, next_candidate, next_profile, {}, repo_root=ROOT)
    pg = next(row for row in files['20-database.yaml'] if row['kind'] == 'StatefulSet')
    container = pg['spec']['template']['spec']['containers'][0]
    assert container['resources']['requests'] == {'cpu': '1', 'memory': '1Gi'}
    assert container['resources']['limits'] == {'cpu': '3', 'memory': '2Gi'}
    assert 'shared_buffers=512MB' in container['args']


def test_operator_cli_stages_postgres_without_live_restart(regional_inputs, monkeypatch, capsys):
    config, candidate, _ = regional_inputs
    kube = FakeKube(regional_inputs)
    monkeypatch.setattr(tuning, 'Kubectl', lambda path: kube)
    common = ['execution-tuning', 'stage-postgres', '--kubeconfig', '/operator/config',
              '--namespace', config['namespace'], '--expected-candidate', candidate['candidate_sha'],
              '--postgres-cpu-request-millis', '1000', '--postgres-cpu-limit-millis', '3000',
              '--postgres-memory-request-mib', '1024', '--postgres-memory-limit-mib', '2048',
              '--postgres-shared-buffers-mib', '512']
    monkeypatch.setattr('sys.argv', common)
    tuning.main()
    assert json.loads(capsys.readouterr().out)['postgres_settings']['shared_buffers_mib'] == 512
    assert not any(args[0] in {'set', 'rollout'} for args in kube.calls)
    version = kube.cm['metadata']['resourceVersion']
    common[1] = 'apply'
    monkeypatch.setattr('sys.argv', common)
    with pytest.raises(SystemExit) as error:
        tuning.main()
    assert error.value.code == 1
    assert kube.cm['metadata']['resourceVersion'] == version


def test_dormant_actuators_keep_configuration_without_requiring_a_pod(regional_inputs):
    config, candidate, _ = regional_inputs

    class DormantKube(FakeKube):
        def run(self, *args, **kwargs):
            if args[0] == 'exec' and 'deployment/dormant-actuator' in args:
                raise RuntimeError('cannot exec a deployment with zero replicas')
            return super().run(*args, **kwargs)

    kube = DormantKube(regional_inputs)
    active = next(row for key, row in kube.objects.items()
                  if key[0] == 'deployment' and 'LOOM_EXECUTION_ACTUATOR_TARGET_ID' in tuning._env(row))
    dormant = deepcopy(active)
    dormant['metadata']['name'] = 'dormant-actuator'
    dormant['spec']['replicas'] = 0
    dormant['status'] = {'readyReplicas': 0}
    key = ('deployment', config['execution_namespace'], 'dormant-actuator')
    kube.objects[key] = dormant
    before = tuning.inspect(kube, config['namespace'])
    assert next(row for row in before['controllers'] if row['name'] == 'dormant-actuator')[
        'command_concurrency'] is None
    result = tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                          command_concurrency=2)
    observed = next(row for row in result['controllers'] if row['name'] == 'dormant-actuator')
    assert observed['replicas'] == observed['ready_replicas'] == 0
    assert observed['command_concurrency'] is None
    assert observed['configured_command_concurrency'] == 2
    assert tuning._env(kube.objects[key])['LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY'] == '2'
    assert 'dormant-actuator' not in result['restarted_controllers']
    assert not any(args[:3] == ('rollout', 'status', 'deployment/dormant-actuator') for args in kube.calls)


@pytest.mark.parametrize('operation', ['apply', 'stage-postgres'])
def test_version_deployment_cannot_acquire_guard_during_tuning(regional_inputs, operation):
    config, candidate, _ = regional_inputs

    class RacingKube(FakeKube):
        deployment_granted = None

        def run(self, *args, **kwargs):
            if args[0] == 'patch':
                self.deployment_granted = not (self.guard_held or getattr(self, 'tuning_owner', None))
                if self.deployment_granted:
                    self.guard_held = True
            return super().run(*args, **kwargs)

    kube = RacingKube(regional_inputs)
    if operation == 'apply':
        tuning.apply(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                     command_concurrency=2)
    else:
        tuning.stage_postgres(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              settings={'shared_buffers_mib': 256})
    assert kube.deployment_granted is False
    assert getattr(kube, 'tuning_owner', None) is None


def test_stage_build_requests_and_region_preserves_limits_and_requires_normal_rollout(regional_inputs):
    from loom.nebius_platform_render import build_regional_execution
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, profile = regional_inputs
    config['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    west = config['regional_execution_targets'][0]['target_id']
    old_deployments = {key: deepcopy(row) for key, row in kube.objects.items() if key[0] == 'deployment'}
    result = tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
        resources={config['target_id']: {'cpu_request_millis': 2000}, west: {'cpu_request_millis': 1000}},
        enable_regional=(west,))
    saved = json.loads(kube.cm['data']['environment.json'])
    assert saved['capacity_policy'] == config['capacity_policy']
    assert saved['regional_execution_targets'][0]['capacity_policy'] == config['regional_execution_targets'][0]['capacity_policy']
    assert result['regional_preflight_required'] == [west]
    assert result['effective_on_next_deployment'] and not result['controllers_restarted'] and not result['task_jobs_replaced']
    assert old_deployments == {key: row for key, row in kube.objects.items() if key[0] == 'deployment'}
    assert not any(args[0] in {'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)
    files = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    for documents in files.values():
        for doc in documents:
            if doc['kind'] == 'Deployment' and 'LOOM_EXECUTION_ACTUATOR_TARGET_ID' in tuning._env(doc):
                env = tuning._env(doc)
                settings = json.loads(env['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER'])
                assert settings['cpu_request_millis'] == (1000 if env['LOOM_EXECUTION_ACTUATOR_TARGET_ID'] == west else 2000)
                assert (settings['cpu_millis'], settings['memory_mib'], settings['ephemeral_storage_mib']) == (8000, 8192, 65536)
                assert settings['storage_region'] == config['region']
                assert settings['archive_secret_name'] == builder()['archive_secret_name']
    remote = build_regional_execution(saved, candidate, repo_root=ROOT)[west]
    quota = next(doc for doc in remote if doc['kind'] == 'ResourceQuota'
                 and doc['metadata']['namespace'].endswith('-build'))['spec']['hard']
    assert quota['requests.cpu'] == '2000m' and quota['limits.cpu'] == '16000m'
    assert quota['requests.memory'] == quota['limits.memory'] == '16384Mi'
    assert quota['requests.ephemeral-storage'] == quota['limits.ephemeral-storage'] == '131072Mi'
    version = kube.cm['metadata']['resourceVersion']
    tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
        resources={config['target_id']: {'cpu_request_millis': 2000}, west: {'cpu_request_millis': 1000}},
        enable_regional=(west,))
    assert kube.cm['metadata']['resourceVersion'] == version


@pytest.mark.parametrize('damage', ['unknown', 'primary-optin', 'duplicate-optin', 'missing-optin',
                                  'request-over-limit', 'boolean', 'storage-floor', 'grow-limit'])
def test_invalid_build_staging_has_no_mutation(regional_inputs, damage):
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, _ = regional_inputs
    config['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    west = config['regional_execution_targets'][0]['target_id']
    resources, enable = {config['target_id']: {'cpu_request_millis': 2000}}, ()
    if damage == 'unknown':
        resources = {'unknown': {'cpu_request_millis': 1000}}
    elif damage == 'primary-optin':
        enable = (config['target_id'],)
    elif damage == 'duplicate-optin':
        enable = (west, west)
    elif damage == 'missing-optin':
        resources = {west: {'cpu_request_millis': 1000}}
    elif damage == 'request-over-limit':
        resources = {config['target_id']: {'cpu_request_millis': 8001}}
    elif damage == 'boolean':
        resources = {config['target_id']: {'cpu_request_millis': True}}
    elif damage == 'storage-floor':
        resources = {config['target_id']: {'ephemeral_storage_mib': 8192}}
    elif damage == 'grow-limit':
        resources = {config['target_id']: {'memory_mib': 16384}}
    with pytest.raises((DeploymentError, ValueError)):
        tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              resources=resources, enable_regional=enable)
    assert not any(args[0] in {'patch', 'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)
    assert kube.cm['metadata']['resourceVersion'] == '1'
    assert kube.tuning_owner is None


def test_operator_cli_stages_builder_requests_and_rejects_accidental_live_application(regional_inputs, monkeypatch, capsys):
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, _ = regional_inputs
    config['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    west = config['regional_execution_targets'][0]['target_id']
    monkeypatch.setattr(tuning, 'Kubectl', lambda path: kube)
    command = ['execution-tuning', 'stage-builders', '--kubeconfig', '/operator/config',
               '--namespace', config['namespace'], '--expected-candidate', candidate['candidate_sha'],
               '--enable-regional-builder', west, '--build-cpu-request-millis', west + '=1000']
    monkeypatch.setattr('sys.argv', command)
    tuning.main()
    assert json.loads(capsys.readouterr().out)['builders'][west]['cpu_request_millis'] == 1000
    version = kube.cm['metadata']['resourceVersion']
    command[1] = 'apply'
    with pytest.raises(SystemExit) as caught:
        tuning.main()
    assert caught.value.code == 1
    assert kube.cm['metadata']['resourceVersion'] == version


def _active_builder(regional_inputs):
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, profile = regional_inputs
    config['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    for ns in (config['execution_namespace'], config['execution_namespace'] + '-build'):
        kube.objects['job', ns, 'already-running'] = {
            'metadata': {'name': 'already-running', 'namespace': ns, 'uid': ns + '-original-uid'},
            'spec': {'activeDeadlineSeconds': 2700, 'template': {'spec': {'containers': [
                {'name': 'build', 'resources': {'requests': {'cpu': '8', 'memory': '8Gi',
                 'ephemeral-storage': '64Gi'}, 'limits': {'cpu': '8', 'memory': '8Gi',
                 'ephemeral-storage': '64Gi'}}}], 'initContainers': []}}},
        }
    return config, candidate, profile, kube


def test_live_builder_tuning_retains_jobs_and_can_restore_persisted_original_ceilings(regional_inputs):
    config, candidate, profile, kube = _active_builder(regional_inputs)
    target = config['target_id']
    jobs = {key: deepcopy(value) for key, value in kube.objects.items() if key[0] == 'job'}
    desired = {'cpu_request_millis': 2000, 'ephemeral_storage_mib': 49152}
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources={target: desired})
    assert result['task_jobs_replaced'] is False
    assert result['restarted_controllers'] == ['loom-execution-actuator']
    assert result['job_uid_snapshots'][target]['before'] == result['job_uid_snapshots'][target]['after']
    assert jobs == {key: value for key, value in kube.objects.items() if key[0] == 'job'}
    assert not any(args[0] in {'delete', 'create', 'apply'} for args in kube.calls)
    # A frozen old attempt can still create its original 8CPU/64Gi Job. Lower
    # future requests must not shrink the existing admission envelope mid-flight.
    assert not any(args[:2] == ('patch', 'resourcequota') for args in kube.calls)
    assert all('loom-control-plane' not in args and not any('west' in str(a) for a in args)
               for args in kube.calls if args[0] in {'set', 'rollout'})
    saved = json.loads(kube.cm['data']['environment.json'])
    ceiling = {'cpu_millis': 8000, 'memory_mib': 8192, 'ephemeral_storage_mib': 65536}
    assert saved['task_image_builder']['resource_ceiling'] == ceiling
    rendered = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    cm = next(d for d in rendered['10-config-network.yaml'] if d['kind'] == 'ConfigMap'
              and d['metadata']['name'] == 'loom-platform-config')
    assert json.loads(cm['data']['environment.json'])['task_image_builder']['resource_ceiling'] == ceiling
    version = kube.cm['metadata']['resourceVersion']
    again = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                 resources={target: desired})
    assert again['restarted_controllers'] == [] and kube.cm['metadata']['resourceVersion'] == version
    restored = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                    resources={target: {'ephemeral_storage_mib': 65536}})
    assert restored['builders'][target]['resource_ceiling'] == ceiling
    assert restored['builders'][target]['ephemeral_storage_mib'] == 65536
    assert jobs == {key: value for key, value in kube.objects.items() if key[0] == 'job'}


@pytest.mark.parametrize('damage', ['unsupported', 'disabled', 'identity', 'grow', 'version-lock'])
def test_live_builder_tuning_rejects_non_resource_or_uninstalled_changes_without_writes(regional_inputs, damage):
    config, candidate, _, kube = _active_builder(regional_inputs)
    target = config['target_id']
    resources = {target: {'cpu_request_millis': 2000}}
    if damage == 'unsupported':
        kube.builder_request_support = False
    elif damage == 'disabled':
        resources = {config['regional_execution_targets'][0]['target_id']: {'cpu_request_millis': 2000}}
    elif damage == 'identity':
        saved = json.loads(kube.cm['data']['environment.json'])
        saved['task_image_builder']['registry_secret_name'] = 'other-authority'
        kube.cm['data']['environment.json'] = json.dumps(saved)
    elif damage == 'grow':
        resources = {target: {'ephemeral_storage_mib': 65537}}
    elif damage == 'version-lock':
        kube.guard_held = True
    with pytest.raises((DeploymentError, ValueError)):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert not any(args[0] in {'patch', 'set', 'rollout', 'delete', 'create', 'apply'} for args in kube.calls)


def test_interrupted_builder_update_retries_saved_intent_without_replacing_jobs(regional_inputs):
    config, candidate, _, kube = _active_builder(regional_inputs)
    desired = {config['target_id']: {'cpu_request_millis': 2000}}
    kube.fail_set = True
    with pytest.raises(DeploymentError, match='interrupted'):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=desired)
    assert json.loads(kube.cm['data']['environment.json'])['task_image_builder']['cpu_request_millis'] == 2000
    assert kube.tuning_owner is None
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=desired)
    assert result['job_uid_snapshots'][config['target_id']]['before'] == result['job_uid_snapshots'][config['target_id']]['after']
    assert result['task_jobs_replaced'] is False


def test_live_builder_quota_readback_accepts_apiserver_quantity_canonicalization(regional_inputs):
    config, candidate, _, kube = _active_builder(regional_inputs)
    assert tuning._same_quota({'requests.cpu': '2', 'limits.memory': '8Gi'},
                             {'requests.cpu': '2000m', 'limits.memory': '8192Mi'})
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources={config['target_id']: {'cpu_request_millis': 2000}})
    assert not result['task_jobs_replaced']


def test_live_builder_restores_quota_when_original_ceiling_allows_larger_futures(regional_inputs):
    config, candidate, _, kube = _active_builder(regional_inputs)
    target = config['target_id']
    tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                          resources={target: {'ephemeral_storage_mib': 49152}})
    quota = kube.objects['resourcequota', config['execution_namespace'] + '-build', 'loom-task-image-build-capacity']
    # Simulate a later idle deployment tightening the quota while preserving the
    # original durable ceiling. Restoring the old budget must expand only this.
    quota['spec']['hard']['requests.ephemeral-storage'] = '96Gi'
    quota['spec']['hard']['limits.ephemeral-storage'] = '96Gi'
    tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                          resources={target: {'ephemeral_storage_mib': 65536}})
    assert quota['spec']['hard']['requests.ephemeral-storage'] == '131072Mi'
    assert any(args[:2] == ('patch', 'resourcequota') for args in kube.calls)


def test_live_regional_builder_uses_bound_remote_surface_and_rolls_only_its_actuator(regional_inputs, monkeypatch):
    from loom.nebius_platform_render import build_regional_execution
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, _ = regional_inputs
    config['task_image_builder'] = builder()
    west = config['regional_execution_targets'][0]
    west['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    for doc in build_regional_execution(config, candidate, repo_root=ROOT)[west['target_id']]:
        if doc['kind'] == 'ResourceQuota':
            doc['metadata']['resourceVersion'] = '1'
            kube.objects['resourcequota', doc['metadata']['namespace'], doc['metadata']['name']] = doc
    routed = []
    installed = json.loads(kube.cm['data']['environment.json'])
    def remote(current, saved, target):
        assert current is kube and saved == installed and target == west['target_id']
        routed.append(target)
        return kube
    monkeypatch.setattr(tuning, '_builder_kube', remote)
    primary = deepcopy(kube.objects['deployment', config['execution_namespace'], 'loom-execution-actuator'])
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources={west['target_id']: {'cpu_request_millis': 2000}})
    assert routed == [west['target_id']]
    assert result['restarted_controllers'] == [west['target_id'] + '-actuator']
    assert primary == kube.objects['deployment', config['execution_namespace'], 'loom-execution-actuator']


def test_builder_context_discovery_requires_unique_retained_context_and_verifies_identity(regional_inputs, monkeypatch):
    from types import SimpleNamespace

    config, _, _ = regional_inputs
    owner = config['regional_execution_targets'][0]
    view = {'clusters': [{'name': 'west', 'cluster': {'server': owner['kubernetes_api_server']}}],
            'contexts': [{'name': 'retained-west', 'context': {'cluster': 'west'}}]}
    local = SimpleNamespace(kubeconfig=Path('/private/retained'), local_executable=None,
                            run=lambda *args: json.dumps(view))
    remote = object()
    def construct(path, **kwargs):
        assert path == local.kubeconfig and kwargs == {'context': 'retained-west', 'local_executable': None}
        return remote
    checked = []
    monkeypatch.setattr(tuning, 'Kubectl', construct)
    monkeypatch.setattr(tuning, 'verify_cluster_identity', lambda *args: checked.append(args))
    assert tuning._builder_kube(local, config, owner['target_id']) is remote
    assert checked == [(remote, owner, owner['cluster_id'])]
    view['contexts'].append({'name': 'ambiguous', 'context': {'cluster': 'west'}})
    with pytest.raises(DeploymentError, match='unique retained'):
        tuning._builder_kube(local, config, owner['target_id'])


def test_live_builder_detects_existing_uid_or_frozen_envelope_replacement(regional_inputs):
    config, candidate, _, kube = _active_builder(regional_inputs)
    normal = kube.run
    def run(*args, **kwargs):
        result = normal(*args, **kwargs)
        if args[:2] == ('rollout', 'status'):
            kube.objects['job', config['execution_namespace'] + '-build', 'already-running']['metadata']['uid'] = 'replacement'
        return result
    kube.run = run
    with pytest.raises(DeploymentError, match='UID or frozen resource envelope changed'):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              resources={config['target_id']: {'cpu_request_millis': 2000}})


def test_cache_total_budget_64g_stages_both_regions_without_changing_resources(regional_inputs):
    from loom.nebius_platform_render import build_regional_execution
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, profile = regional_inputs
    config['task_image_builder'] = builder()
    west = config['regional_execution_targets'][0]
    west['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    original = deepcopy(config)
    deployments = {key: deepcopy(row) for key, row in kube.objects.items() if key[0] == 'deployment'}
    resources = {target: {'cache_total_mib': 65536} for target in (config['target_id'], west['target_id'])}
    result = tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources=resources)
    saved = json.loads(kube.cm['data']['environment.json'])
    for target in resources:
        expected = deepcopy(tuning._owners(original)[target]['task_image_builder'])
        expected['cache_total_mib'] = 65536
        assert tuning._owners(saved)[target]['task_image_builder'] == expected
        assert result['builders'][target]['cache_total_mib'] == 65536
    assert deployments == {key: row for key, row in kube.objects.items() if key[0] == 'deployment'}
    assert not any(args[0] in {'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)
    rendered = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    actuators = [doc for docs in rendered.values() for doc in docs if doc['kind'] == 'Deployment'
                 and 'LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER' in tuning._env(doc)]
    assert len(actuators) == 2
    for doc in actuators:
        settings = json.loads(tuning._env(doc)['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER'])
        assert settings['cache_total_mib'] == 65536
        assert (settings['cpu_millis'], settings['memory_mib'], settings['ephemeral_storage_mib']) == (8000, 8192, 65536)
    remote = build_regional_execution(saved, candidate, repo_root=ROOT)[west['target_id']]
    quota = next(doc for doc in remote if doc['kind'] == 'ResourceQuota'
                 and doc['metadata']['name'] == 'loom-task-image-build-capacity')
    assert quota['spec']['hard']['limits.ephemeral-storage'] == '131072Mi'
    version = kube.cm['metadata']['resourceVersion']
    tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert kube.cm['metadata']['resourceVersion'] == version


def _active_shared_cache_builders(regional_inputs, monkeypatch):
    from loom.nebius_platform_render import build_regional_execution
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, profile = regional_inputs
    config['task_image_builder'] = builder()
    west = config['regional_execution_targets'][0]
    west['task_image_builder'] = builder()
    kube = FakeKube(regional_inputs)
    for doc in build_regional_execution(config, candidate, repo_root=ROOT)[west['target_id']]:
        if doc['kind'] == 'ResourceQuota':
            doc['metadata']['resourceVersion'] = '1'
            kube.objects['resourcequota', doc['metadata']['namespace'], doc['metadata']['name']] = deepcopy(doc)
    for owner in tuning._owners(config).values():
        for ns in (owner['execution_namespace'], owner['execution_namespace'] + '-build'):
            kube.objects['job', ns, 'inflight-before-cache-tuning'] = {
                'metadata': {'name': 'inflight-before-cache-tuning', 'namespace': ns, 'uid': ns + '-original'},
                'spec': {'activeDeadlineSeconds': 2700, 'template': {'spec': {'containers': [
                    {'name': 'task', 'resources': {'requests': {'cpu': '8', 'memory': '8Gi',
                     'ephemeral-storage': '64Gi'}, 'limits': {'cpu': '8', 'memory': '8Gi',
                     'ephemeral-storage': '64Gi'}}}], 'initContainers': []}}},
            }
    monkeypatch.setattr(tuning, '_builder_kube', lambda current, saved, target: kube)
    targets = (config['target_id'], west['target_id'])
    return config, candidate, profile, kube, targets


def test_cache_total_budget_live_roundtrip_preserves_jobs_quota_and_resource_ceilings(regional_inputs, monkeypatch):
    config, candidate, profile, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    jobs = {key: deepcopy(value) for key, value in kube.objects.items() if key[0] == 'job'}
    quotas = {key: deepcopy(value) for key, value in kube.objects.items() if key[0] == 'resourcequota'}
    original = deepcopy(config)
    resources = {target: {'cache_total_mib': 65536} for target in targets}
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources=resources)
    assert set(result['restarted_controllers']) == {'loom-execution-actuator', targets[1] + '-actuator'}
    assert result['task_jobs_replaced'] is False
    saved = json.loads(kube.cm['data']['environment.json'])
    for target in targets:
        expected = deepcopy(tuning._owners(original)[target]['task_image_builder'])
        expected['cache_total_mib'] = 65536
        assert tuning._owners(saved)[target]['task_image_builder'] == expected
        assert result['builders'][target]['cache_total_mib'] == 65536
        assert result['job_uid_snapshots'][target]['before'] == result['job_uid_snapshots'][target]['after']
    assert jobs == {key: value for key, value in kube.objects.items() if key[0] == 'job'}
    assert quotas == {key: value for key, value in kube.objects.items() if key[0] == 'resourcequota'}
    assert not any(args[0] in {'delete', 'create', 'apply'} for args in kube.calls)
    assert not any(args[:2] == ('patch', 'resourcequota') for args in kube.calls)
    assert all('loom-control-plane' not in args for args in kube.calls if args[0] in {'set', 'rollout'})
    version = kube.cm['metadata']['resourceVersion']
    again = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert again['restarted_controllers'] == [] and kube.cm['metadata']['resourceVersion'] == version
    # A later ordinary deployment retains the persisted 64Gi intent.
    rendered = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    for docs in rendered.values():
        for doc in docs:
            if doc['kind'] == 'Deployment' and 'LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER' in tuning._env(doc):
                assert json.loads(tuning._env(doc)['LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER'])['cache_total_mib'] == 65536
    restored = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                    resources={target: {'cache_total_mib': 4096} for target in targets})
    assert all(restored['builders'][target]['cache_total_mib'] == 4096 for target in targets)
    assert jobs == {key: value for key, value in kube.objects.items() if key[0] == 'job'}


@pytest.mark.parametrize('operation', ['stage', 'apply'])
@pytest.mark.parametrize('invalid', [0, -1, 1023, 1048577, True, '65536'])
def test_cache_total_budget_invalid_values_never_write(regional_inputs, monkeypatch, operation, invalid):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    before = deepcopy(kube.objects)
    function = tuning.stage_builders if operation == 'stage' else tuning.apply_builders
    with pytest.raises((DeploymentError, ValueError)):
        function(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                 resources={target: {'cache_total_mib': invalid} for target in targets})
    assert kube.objects == before
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)


@pytest.mark.parametrize('operation', ['stage', 'apply'])
@pytest.mark.parametrize('resources_kind', ['partial-shared', 'different-shared', 'unknown-target'])
def test_cache_total_budget_requires_one_consistent_shared_bucket_intent(regional_inputs, monkeypatch, operation, resources_kind):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    resources = {targets[0]: {'cache_total_mib': 65536}}
    if resources_kind == 'different-shared':
        resources[targets[1]] = {'cache_total_mib': 32768}
    elif resources_kind == 'unknown-target':
        resources = {'unknown-builder': {'cache_total_mib': 65536}}
    function = tuning.stage_builders if operation == 'stage' else tuning.apply_builders
    with pytest.raises((DeploymentError, ValueError)):
        function(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert kube.cm['metadata']['resourceVersion'] == '1'
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)


@pytest.mark.parametrize('support', ['false', 'missing'])
def test_cache_total_budget_old_image_fails_before_persisting_intent(regional_inputs, monkeypatch, support):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    normal = kube.run

    def run(*args, **kwargs):
        response = normal(*args, **kwargs)
        if args[0] == 'exec' and args[args.index('-c') + 1] not in {tuning._API, tuning._GUARD}:
            value = json.loads(response)
            if value.get('builder_enabled'):
                if support == 'missing':
                    value.pop('builder_cache_budget_support', None)
                    value['builder_resources'].pop('cache_total_mib', None)
                else:
                    value['builder_cache_budget_support'] = False
                    value['builder_resources']['cache_total_mib'] = None
                return json.dumps(value)
        return response

    kube.run = run
    with pytest.raises(DeploymentError):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              resources={target: {'cache_total_mib': 65536} for target in targets})
    assert kube.cm['metadata']['resourceVersion'] == '1'
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout'} for args in kube.calls)


def test_cache_total_budget_absence_does_not_break_existing_resource_only_tuning(regional_inputs):
    config, candidate, _, kube = _active_builder(regional_inputs)
    kube.builder_cache_budget_support = False
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources={config['target_id']: {'cpu_request_millis': 2000}})
    assert result['builders'][config['target_id']]['cpu_request_millis'] == 2000
    assert result['task_jobs_replaced'] is False
    assert 'cache_total_mib' not in json.loads(kube.cm['data']['environment.json'])['task_image_builder']


def test_cache_total_budget_partial_rollout_fails_and_retries_saved_intent(regional_inputs, monkeypatch, capsys):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    jobs = {key: deepcopy(value) for key, value in kube.objects.items() if key[0] == 'job'}
    normal = kube.run
    fail = True

    def run(*args, **kwargs):
        nonlocal fail
        if fail and args[:3] == ('set', 'env', 'deployment/' + targets[1] + '-actuator'):
            fail = False
            raise DeploymentError('regional cache update interrupted after primary rollout')
        return normal(*args, **kwargs)

    kube.run = run
    monkeypatch.setattr(tuning, 'Kubectl', lambda path: kube)
    command = ['execution-tuning', 'apply-builders', '--kubeconfig', '/operator/config',
               '--namespace', config['namespace'], '--expected-candidate', candidate['candidate_sha']]
    for target in targets:
        command += ['--build-cache-total-mib', target + '=65536']
    monkeypatch.setattr('sys.argv', command)
    with pytest.raises(SystemExit) as failure:
        tuning.main()
    assert failure.value.code == 1
    output = capsys.readouterr()
    assert output.out == '' and 'did not converge' in output.err
    saved = json.loads(kube.cm['data']['environment.json'])
    assert all(tuning._owners(saved)[target]['task_image_builder']['cache_total_mib'] == 65536 for target in targets)
    assert kube.tuning_owner is None
    version = kube.cm['metadata']['resourceVersion']
    monkeypatch.setattr('sys.argv', command)
    tuning.main()
    result = json.loads(capsys.readouterr().out)
    assert result['restarted_controllers'] == [targets[1] + '-actuator']
    assert kube.cm['metadata']['resourceVersion'] == version
    assert jobs == {key: value for key, value in kube.objects.items() if key[0] == 'job'}


def test_cache_total_budget_stale_runtime_readback_cannot_report_success(regional_inputs, monkeypatch):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    normal = kube.run

    def run(*args, **kwargs):
        response = normal(*args, **kwargs)
        if args[0] == 'exec' and args[args.index('-c') + 1] not in {tuning._API, tuning._GUARD}:
            value = json.loads(response)
            if value.get('builder_enabled'):
                value['builder_resources']['cache_total_mib'] = 4096
                return json.dumps(value)
        return response

    kube.run = run
    with pytest.raises(DeploymentError, match='readback differs'):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              resources={target: {'cache_total_mib': 65536} for target in targets})
    assert kube.cm['metadata']['resourceVersion'] != '1'
    assert kube.tuning_owner is None


@pytest.mark.parametrize('operation', ['stage-builders', 'apply-builders'])
def test_cache_total_budget_cli_accepts_two_targets_in_one_saved_change(regional_inputs, monkeypatch, capsys, operation):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    monkeypatch.setattr(tuning, 'Kubectl', lambda path: kube)
    command = ['execution-tuning', operation, '--kubeconfig', '/operator/config', '--namespace', config['namespace'],
               '--expected-candidate', candidate['candidate_sha']]
    for target in targets:
        command += ['--build-cache-total-mib', target + '=65536']
    monkeypatch.setattr('sys.argv', command)
    tuning.main()
    result = json.loads(capsys.readouterr().out)
    assert all(result['builders'][target]['cache_total_mib'] == 65536 for target in targets)
    assert kube.cm['metadata']['resourceVersion'] == '2'
    assert result['task_jobs_replaced'] is False


@pytest.mark.parametrize('support', [False, True])
@pytest.mark.parametrize('global_mode', [False, True])
def test_cache_total_budget_runtime_probe_detects_actual_installed_settings_shape(monkeypatch, support, global_mode):
    from contextlib import redirect_stdout
    from io import StringIO
    from types import SimpleNamespace

    import loom_execution_actuator.config as actuator_config

    values = {'cpu_request_millis': 4000, 'cpu_millis': 8000, 'memory_mib': 8192,
              'ephemeral_storage_mib': 51200, 'namespace': 'native-build'}
    if support:
        values['cache_total_mib'] = 65536
    settings = SimpleNamespace(command_concurrency=1, task_image_builder=SimpleNamespace(**values),
                               global_pool=SimpleNamespace() if global_mode else None)
    monkeypatch.setattr(actuator_config, 'ExecutionActuatorSettings', lambda: settings)

    def run(*args, **kwargs):
        assert args[:2] == ('exec', '-n') and args[-2] == '-c'
        output = StringIO()
        # Execute the real fixed runtime probe against installed-model shapes;
        # no external command or configuration/credential read is performed.
        with redirect_stdout(output):
            exec(args[-1], {})
        return output.getvalue()

    controller = {'metadata': {'name': 'builder-actuator', 'namespace': 'execution'},
                  'spec': {'replicas': 1}}
    observed = tuning._runtime(SimpleNamespace(run=run), controller)
    assert observed['builder_cache_budget_support'] is support
    assert observed['builder_configuration_source'] == ('management-profile' if global_mode else 'actuator')
    assert observed['builder_resources']['cache_total_mib'] == (65536 if support else None)
    assert observed['builder_resources']['effective_cpu_request_millis'] == 4000


@pytest.mark.parametrize('other_bucket', [None, 'independent-native-cache'])
def test_cache_total_budget_independent_or_disabled_buckets_are_not_coupled(regional_inputs, other_bucket):
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, _ = regional_inputs
    config['task_image_builder'] = builder()
    west = config['regional_execution_targets'][0]
    west['task_image_builder'] = {**builder(), 'cache_bucket': other_bucket}
    kube = FakeKube(regional_inputs)
    result = tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                                  resources={config['target_id']: {'cache_total_mib': 65536}})
    assert result['builders'][config['target_id']]['cache_total_mib'] == 65536
    assert result['builders'][west['target_id']]['cache_total_mib'] == 4096
    assert not any(args[0] in {'set', 'rollout'} for args in kube.calls)


@pytest.mark.parametrize('staged_budget', [4096, 65536])
def test_cache_total_budget_staged_explicit_value_blocks_resource_only_on_old_image(
    regional_inputs, monkeypatch, staged_budget,
):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    kube.builder_cache_budget_support = False
    tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                          resources={target: {'cache_total_mib': staged_budget} for target in targets})
    before = deepcopy(kube.objects)
    calls_before = len(kube.calls)
    with pytest.raises(DeploymentError):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                              resources={targets[0]: {'cpu_request_millis': 2000}})
    assert kube.objects == before
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout'} for args in kube.calls[calls_before:])


@pytest.mark.parametrize('staged_field,staged_value', [
    ('cpu_request_millis', 2000), ('ephemeral_storage_mib', 49152),
])
def test_cache_total_budget_only_cannot_apply_unrequested_staged_resources(
    regional_inputs, monkeypatch, staged_field, staged_value,
):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
                          resources={targets[0]: {staged_field: staged_value}})
    before = deepcopy(kube.objects)
    calls_before = len(kube.calls)
    resources = {target: {'cache_total_mib': 65536} for target in targets}
    with pytest.raises(DeploymentError):
        tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert kube.objects == before
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout'} for args in kube.calls[calls_before:])
    # Explicitly requesting the staged resource too is an intentional combined
    # update, and keeps in-flight Jobs and the saved original hard ceiling.
    resources[targets[0]][staged_field] = staged_value
    result = tuning.apply_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert result['builders'][targets[0]][staged_field] == staged_value
    assert result['builders'][targets[0]]['cache_total_mib'] == 65536
    assert result['task_jobs_replaced'] is False
    assert {key: value for key, value in kube.objects.items() if key[0] == 'job'} == {
        key: value for key, value in before.items() if key[0] == 'job'}


@pytest.mark.parametrize('operation', ['stage', 'apply'])
@pytest.mark.parametrize('knob', ['cache', 'resource'])
def test_management_profile_builders_reject_standalone_tuning_before_any_write(
    regional_inputs, monkeypatch, operation, knob,
):
    config, candidate, _, kube, targets = _active_shared_cache_builders(regional_inputs, monkeypatch)
    kube.builder_configuration_source = 'management-profile'
    resources = ({target: {'cache_total_mib': 65536} for target in targets} if knob == 'cache'
                 else {targets[0]: {'cpu_request_millis': 2000}})
    before = deepcopy(kube.objects)
    function = tuning.stage_builders if operation == 'stage' else tuning.apply_builders
    with pytest.raises(DeploymentError, match='management'):
        function(kube, config['namespace'], expected_candidate=candidate['candidate_sha'], resources=resources)
    assert kube.objects == before
    assert kube.cm['metadata']['resourceVersion'] == '1'
    assert kube.tuning_owner is None
    assert not any(args[0] in {'patch', 'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)


@pytest.mark.parametrize('regional_controller', ['absent', 'dormant'])
def test_cache_total_budget_stage_optin_keeps_future_regional_install_semantics(
    regional_inputs, regional_controller,
):
    from tests.unit.test_nebius_platform_render_regional_builder import builder

    config, candidate, _ = regional_inputs
    config['task_image_builder'] = builder()
    west = config['regional_execution_targets'][0]['target_id']
    kube = FakeKube(regional_inputs)
    key = next(key for key, doc in kube.objects.items() if key[0] == 'deployment'
               and tuning._env(doc).get('LOOM_EXECUTION_ACTUATOR_TARGET_ID') == west)
    if regional_controller == 'absent':
        del kube.objects[key]
    else:
        kube.objects[key]['spec']['replicas'] = 0
        kube.objects[key]['status'] = {'readyReplicas': 0}
    deployments = {key: deepcopy(row) for key, row in kube.objects.items() if key[0] == 'deployment'}
    result = tuning.stage_builders(kube, config['namespace'], expected_candidate=candidate['candidate_sha'],
        resources={target: {'cache_total_mib': 65536} for target in (config['target_id'], west)},
        enable_regional=(west,))
    saved = json.loads(kube.cm['data']['environment.json'])
    for target in (config['target_id'], west):
        assert tuning._owners(saved)[target]['task_image_builder']['cache_total_mib'] == 65536
        assert result['builders'][target]['cache_total_mib'] == 65536
    assert result['effective_on_next_deployment'] is True
    assert result['regional_preflight_required'] == [west]
    assert result['controllers_restarted'] is False and result['task_jobs_replaced'] is False
    assert deployments == {key: row for key, row in kube.objects.items() if key[0] == 'deployment'}
    assert not any(args[0] in {'set', 'rollout', 'apply', 'delete', 'create'} for args in kube.calls)
