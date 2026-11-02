"""Configuration-only disk maintenance must retain the deployment source."""
from __future__ import annotations

import json
from copy import deepcopy

import pytest
from scripts.ops import nebius_execution_tuning as tuning
from scripts.ops.deploy_nebius_platform import DeploymentError

from loom.nebius_platform_render import build_platform
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs
from tests.unit.test_nebius_execution_tuning import ROOT, FakeKube


class DiskKube(FakeKube):
    def __init__(self, inputs):
        super().__init__(inputs)
        self.disk_support = True
        self.global_mode = False
        self.active = False
        self.fail_collector = False
        for (kind, _, _), obj in self.objects.items():
            if kind == 'configmap':
                obj['metadata'].update(resourceVersion='1', uid=obj['metadata']['name'])

    def run(self, *args, **kwargs):
        if args[0] == 'exec' and getattr(tuning, '_IDLE_STATE', None) in args:
            self.calls.append(args)
            return json.dumps({'idle': not self.active})
        if args[:2] == ('patch', 'configmap') and args[2] != 'loom-platform-config':
            self.calls.append(args)
            if self.fail_collector:
                self.fail_collector = False
                raise DeploymentError('interrupted collector update')
            obj = self.objects['configmap', args[args.index('-n') + 1], args[2]]
            for op in json.loads(args[-1]):
                if op['op'] == 'test':
                    assert obj['metadata'][op['path'].rsplit('/', 1)[-1]] == op['value']
                else:
                    obj['data'][op['path'].removeprefix('/data/')] = op['value']
            obj['metadata']['resourceVersion'] = str(int(obj['metadata']['resourceVersion']) + 1)
            return ''
        result = super().run(*args, **kwargs)
        if args[0] == 'exec' and 'import loom.nebius_platform_render' in args[args.index('-c') + 1]:
            return json.dumps({**json.loads(result), 'regional_disk_configuration_support': self.disk_support,
                               'warm_fit_requires_manager_policy': self.global_mode})
        return result


def test_disk_configuration_persists_without_restarting_services(regional_inputs, monkeypatch):
    config, candidate, profile = regional_inputs
    kube = DiskKube(regional_inputs)
    monkeypatch.setattr(tuning, '_builder_kube', lambda *_: kube)
    before = deepcopy({k: v for k, v in kube.objects.items() if k[0] == 'deployment'})
    target = config['regional_execution_targets'][0]['target_id']
    result = tuning.apply_regional_disks(kube, config['namespace'],
        expected_candidate=candidate['candidate_sha'], disk_types={target: 'NETWORK_SSD_NON_REPLICATED'})
    saved = json.loads(kube.cm['data']['environment.json'])
    assert saved['regional_execution_targets'][0]['execution_disk_type'] == 'NETWORK_SSD_NON_REPLICATED'
    assert result['cloud_disks_changed'] is False
    assert result['database_restarted'] is False
    assert before == {k: v for k, v in kube.objects.items() if k[0] == 'deployment'}
    rendered = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    collectors = [d for docs in rendered.values() for d in docs if d['kind'] == 'ConfigMap'
                  and d['metadata']['name'] == target + '-collector']
    key = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_NAME'
    assert collectors[0]['data'][key] == 'compute.disk.size.network-ssd-non-replicated'
    assert kube.objects['configmap', config['execution_namespace'], target + '-collector']['data'][key] == collectors[0]['data'][key]
    assert kube.tuning_owner is None


@pytest.mark.parametrize('boundary', ['active', 'old_image', 'global_owner', 'wrong_group', 'primary_target', 'invalid_type', 'candidate'])
def test_disk_preflight_rejects_before_any_write(regional_inputs, monkeypatch, boundary):
    config, candidate, _ = regional_inputs
    kube = DiskKube(regional_inputs)
    monkeypatch.setattr(tuning, '_builder_kube', lambda *_: kube)
    target = config['regional_execution_targets'][0]['target_id']
    kind = 'NETWORK_SSD_NON_REPLICATED'
    sha = candidate['candidate_sha']
    if boundary == 'active':
        kube.active = True
    elif boundary == 'old_image':
        kube.disk_support = False
    elif boundary == 'global_owner':
        kube.global_mode = True
    elif boundary == 'wrong_group':
        kube.objects['configmap', config['execution_namespace'], target + '-collector']['data']['LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID'] = 'other'
    elif boundary == 'primary_target':
        target = config['target_id']
    elif boundary == 'invalid_type':
        kind = 'LOCAL_SSD'
    else:
        sha = 'd' * 40
    with pytest.raises(DeploymentError):
        tuning.apply_regional_disks(kube, config['namespace'], expected_candidate=sha, disk_types={target: kind})
    assert not any(call[0] in {'patch', 'set', 'apply', 'delete'} for call in kube.calls)


def test_interrupted_collector_update_is_failed_and_same_intent_converges(regional_inputs, monkeypatch):
    config, candidate, _ = regional_inputs
    kube = DiskKube(regional_inputs)
    monkeypatch.setattr(tuning, '_builder_kube', lambda *_: kube)
    target = config['regional_execution_targets'][0]['target_id']
    kube.fail_collector = True
    kwargs = dict(expected_candidate=candidate['candidate_sha'], disk_types={target: 'NETWORK_SSD_NON_REPLICATED'})
    with pytest.raises(DeploymentError, match='interrupted'):
        tuning.apply_regional_disks(kube, config['namespace'], **kwargs)
    assert json.loads(kube.cm['data']['environment.json'])['regional_execution_targets'][0]['execution_disk_type'] == 'NETWORK_SSD_NON_REPLICATED'
    tuning.apply_regional_disks(kube, config['namespace'], **kwargs)


def test_disk_operator_cli_rejects_mixed_changes(regional_inputs, monkeypatch, capsys):
    config, candidate, _ = regional_inputs
    kube = DiskKube(regional_inputs)
    monkeypatch.setattr(tuning, 'Kubectl', lambda *_: kube)
    monkeypatch.setattr(tuning, '_builder_kube', lambda *_: kube)
    target = config['regional_execution_targets'][0]['target_id']
    command = ['tuning', 'apply-regional-disks', '--kubeconfig', '/unused', '--namespace', config['namespace'],
               '--expected-candidate', candidate['candidate_sha'], '--regional-disk-type', target + '=NETWORK_SSD_NON_REPLICATED']
    monkeypatch.setattr('sys.argv', [*command, '--command-concurrency', '2'])
    with pytest.raises(SystemExit):
        tuning.main()
    assert not any(call[0] == 'patch' for call in kube.calls)
    monkeypatch.setattr('sys.argv', command)
    tuning.main()
    assert json.loads(capsys.readouterr().out)['cloud_disks_changed'] is False
