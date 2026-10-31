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
        self.cm = self.objects['configmap', config['namespace'], 'loom-platform-config']
        self.cm['metadata']['resourceVersion'] = '1'
        self.policies = {target: {**deepcopy(row['capacity_policy']), 'version': 1}
                         for target, row in tuning._owners(config).items()}
        self.calls: list[tuple] = []
        self.fail_set = False
        self.guard_held = False
        self.tuning_owner = None

    def get(self, kind, name, namespace):
        return deepcopy(self.objects.get((kind, namespace, name), {}))

    def run(self, *args, **kwargs):
        self.calls.append(args)
        namespace = args[args.index('-n') + 1]
        if args[0] == 'get':
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
            return json.dumps({'command_concurrency': int(env['LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY'])})
        if args[0] == 'patch':
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
