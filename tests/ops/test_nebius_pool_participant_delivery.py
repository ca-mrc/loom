"""Normal idle rollout consumes the retained shared operation, including recovery."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from scripts.ops import deploy_nebius_platform as deploy
from scripts.ops import nebius_idle_rollout as idle
from scripts.ops import nebius_pool_participant_operation as operation
from tests.ops.test_deploy_nebius_platform import FakeKubectl
from tests.ops.test_nebius_idle_rollout import recovery_api
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.pool_management.participant_refresh import ParticipantCapacityReceipt


def reference():
    return operation.ParticipantRolloutReference(operation_id=UUID('bc7b4d07-26ab-4ac5-9b5b-4583f678f91b'),
        inputs_sha256='d' * 64, publication=ProtectedPublication(candidate_id=UUID('76bcff62-f3a6-4b06-a115-036ce7f9b6d1'),
            source_sha='a' * 40, run_id=42, run_attempt=2, artifact_id=72, artifact_sha256='sha256:' + 'e' * 64, pull_request=2425))


def recovery_intent():
    return {'schema_version': 'loom.nebius-deployment.v1', 'status': 'failed', 'mode': 'apply',
        'candidate_sha': 'a' * 40, 'guard_owner': 'rollout-' + reference().operation_id.hex,
        'cluster_id': 'test-cluster', 'namespace': 'loom-nebius-platform',
        'execution_namespace': 'loom-nebius-platform-execution',
        'shared_pool_release': reference().model_dump(mode='json')}


def test_shared_recovery_reference_survives_evidence_projection():
    record = recovery_intent()
    assert idle.recovery_record({**record, 'private_diagnostic': 'do-not-export'}) == record


@pytest.mark.parametrize('damage', ['missing-digest', 'unknown', 'null', 'operation', 'candidate', 'noncanonical-operation'])
def test_shared_recovery_reference_cannot_be_reinterpreted_as_standalone(damage):
    record = recovery_intent()
    value = record['shared_pool_release']
    if damage == 'missing-digest':
        value.pop('inputs_sha256')
    elif damage == 'unknown':
        value['unreviewed'] = True
    elif damage == 'null':
        record['shared_pool_release'] = None
    elif damage == 'operation':
        value['operation_id'] = str(uuid4())
    elif damage == 'candidate':
        value['publication']['source_sha'] = 'b' * 40
    else:
        value['operation_id'] = value['operation_id'].upper()
    with pytest.raises(deploy.DeploymentError, match='shared'):
        idle.recovery_record(record)


@pytest.mark.parametrize('damage', [None, 'attempt', 'artifact-id', 'artifact-digest', 'duplicate-artifact'])
def test_shared_recovery_selects_original_publication_not_another_success_for_same_sha(monkeypatch, tmp_path, damage):
    calls = []

    def github(path, payload=None):
        calls.append(path)
        if path.startswith('actions/workflows/nebius-candidate.yml/runs?'):
            return {'workflow_runs': [{'id': 999, 'head_sha': 'a' * 40}]}
        if path.startswith('actions/runs/999'):
            pytest.fail('must not substitute another publication for the retained release')
        if path == 'actions/runs/42':
            pytest.fail('recovery must query the retained attempt, not the latest run attempt')
        result = recovery_api(path, payload)
        if path == 'actions/runs/42/attempts/2' and damage == 'attempt':
            result = {**result, 'run_attempt': 3}
        if path == 'actions/runs/42/artifacts?per_page=100':
            artifact, = result['artifacts']
            artifact = {**artifact, 'id': 73 if damage == 'artifact-id' else 72,
                'digest': 'sha256:' + ('f' if damage == 'artifact-digest' else 'e') * 64}
            result = {'artifacts': [artifact] * (2 if damage == 'duplicate-artifact' else 1)}
        return result

    def command(argv, **kwargs):
        if argv[:3] == ['gh', 'run', 'download']:
            (tmp_path / 'recovery' / 'deployment-shared.json').write_text(json.dumps(recovery_intent()))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(idle, 'github', github)
    monkeypatch.setattr(idle.subprocess, 'run', command)
    if damage:
        with pytest.raises(deploy.DeploymentError, match='publication'):
            idle.select_recovery('91', tmp_path / 'recovery')
    else:
        result = idle.select_recovery('91', tmp_path / 'recovery')
        assert result['run_id'] == '42'
        assert result['artifact'] == 'nebius-candidate-' + 'a' * 40 + '-42-2'
    assert not any(path.startswith('actions/workflows/nebius-candidate.yml/runs?') for path in calls)
    assert 'actions/runs/42/attempts/2' in calls


@pytest.mark.parametrize('recover', [False, True])
def test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(operation_inputs, tmp_path, monkeypatch, recover):
    """Only Kubernetes/GitHub/HTTPS and the pending concrete pool I/O are doubled."""
    inputs = operation_inputs
    retained = operation.retain_participant_rollout(**inputs)
    config, plan = inputs['config'], retained.release
    publication = tmp_path / 'publication'
    publication.mkdir()
    (publication / 'candidate.json').write_text(json.dumps(inputs['candidate']))
    (publication / 'runtime-profile.json').write_text(inputs['runtime_profile'].model_dump_json())
    args = SimpleNamespace(kubeconfig=tmp_path / 'unused', namespace=config['namespace'],
        expected_cluster_id=config['cluster_id'], publication_dir=publication,
        candidate=inputs['candidate']['candidate_sha'], evidence_dir=tmp_path / 'evidence', github=True)
    intents, phases, mutations = [], [], []
    state = {'pool': 'ready', 'fail_https': recover}

    class Kube(FakeKubectl):
        def get(self, kind, name, namespace):
            result = super().get(kind, name, namespace)
            if kind == 'validatingadmissionpolicy' and result:
                result['metadata']['generation'] = 1
                result['status'] = {'observedGeneration': 1}
            return result

        def run(self, *command, **kwargs):
            if command[0] == 'exec' and 'acquire' in command:
                assert intents[-1]['shared_pool_release'] == retained.reference.model_dump(mode='json')
                assert intents[-1]['guard_owner'] == retained.guard_owner
            if command[0] == 'apply':
                assert self.guard_identity is not None and state['pool'] in {'staged', 'open'}
                phases.append(Path(command[2]).name)
                if '--dry-run=server' in command:
                    import yaml

                    pod = yaml.safe_load(Path(command[2]).read_text())
                    if pod['spec']['containers'][0]['securityContext']['capabilities'].get('add'):
                        raise deploy.TaskIdentityPolicyDeniedError(config['execution_namespace'] + '-private-root-v1')
                    return ''
            result = super().run(*command, **kwargs)
            if command[:2] == ('config', 'view'):
                value = json.loads(result)
                value['clusters'][0]['name'] = config['cluster_id']
                return json.dumps(value)
            return result

    kube = Kube(config, plan.files, database=True)
    for row in inputs['current']:
        kube.objects[row['kind'].lower(), row['metadata']['name']] = copy.deepcopy(row)

    def github(path, payload):
        if path == 'deployments':
            intents.append(copy.deepcopy(payload['payload']))
            return {'id': len(intents)}
        return {}

    def smoke(*_):
        if state['fail_https']:
            state['fail_https'] = False
            raise deploy.DeploymentError('simulated post-workload HTTPS failure')

    class PoolIO:
        def preflight(self, observed_kube, release):
            assert observed_kube is kube and release == plan

        def under_guard(self, observed_kube, release):
            assert observed_kube is kube and release == plan and kube.guard_identity is not None

        def activate(self, observed_kube, release):
            self.under_guard(observed_kube, release)
            if state['pool'] == 'ready':
                mutations.append('stage')
                state['pool'] = 'staged'
            return 'ready'

        def finish(self, observed_kube, release):
            self.under_guard(observed_kube, release)
            if state['pool'] != 'open':
                mutations.append('open')
                state['pool'] = 'open'

        def read_capacity(self, observed_kube, release, *, observation_id):
            self.under_guard(observed_kube, release)
            assert isinstance(observation_id, UUID) and state['pool'] == 'open'
            now = datetime.now(UTC)
            return ParticipantCapacityReceipt(checked_at=now, valid_until=now + timedelta(seconds=60),
                selected_observation_id=uuid4(), selected_observed_at=now, connected_pools=1, observations_sha256='f' * 64)

    monkeypatch.setattr(idle, 'Kubectl', lambda _: kube)
    monkeypatch.setattr(idle, 'github', github)
    monkeypatch.setattr(idle, 'candidate_follows', lambda *_: True)
    monkeypatch.setattr(idle, 'candidate_schema_head', lambda _: '0175')
    monkeypatch.setattr(idle, 'render_candidate', lambda *_: pytest.fail('retained shared files must not be regenerated'))
    monkeypatch.setattr(deploy, 'public_smoke', smoke)
    monkeypatch.setenv('GITHUB_RUN_ID', '991')
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '1')
    pool = PoolIO()
    if recover:
        with pytest.raises(deploy.DeploymentError, match='HTTPS'):
            idle.rollout(args, participant_rollout=retained, pool=pool)
        failed, = args.evidence_dir.glob('deployment-*.json')
        assert json.loads(failed.read_text())['shared_pool_release'] == retained.reference.model_dump(mode='json')
        assert kube.guard_identity == (retained.guard_owner, inputs['candidate']['candidate_sha'])
        args.recovery_evidence = failed
        # Recovery uses the original snapshot even though the platform's live
        # configuration now identifies the partially deployed new candidate.
        assert json.loads(kube.objects['configmap', 'loom-platform-config']['data']['profile.json'])['candidate_sha'] == 'd' * 40
        retained = operation.load_participant_rollout(reference=retained.reference,
            state_root=inputs['state_root'], anchor_dir=inputs['anchor_dir'])
    result = idle.rollout(args, participant_rollout=retained, pool=pool)
    assert result['status'] == 'complete' and kube.guard_identity is None
    assert result['shared_pool_release'] == retained.reference.model_dump(mode='json')
    assert mutations == ['stage', 'open']
    assert {'30-migrate.yaml', '50-configure.yaml', '40-services.yaml', '60-execution.yaml', '70-public.yaml'} <= set(phases)
    assert all(row['shared_pool_release'] == retained.reference.model_dump(mode='json') for row in intents)
    assert len(intents) == (2 if recover else 1)
