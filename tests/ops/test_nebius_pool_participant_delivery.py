"""Normal idle rollout consumes the retained shared operation, including recovery."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from scripts.ops import deploy_nebius_platform as deploy
from scripts.ops import nebius_idle_rollout as idle
from scripts.ops import nebius_pool_participant_operation as operation
from tests.ops.test_deploy_nebius_platform import FakeKubectl
from tests.ops.test_nebius_idle_rollout import recovery_api
from tests.ops.test_nebius_pool_participant_adapter import connection as connection
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom_service.environment_management.candidates import ProtectedPublication


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


@pytest.mark.parametrize('damage', [None, 'attempt', 'artifact-id', 'artifact-digest', 'artifact-run'])
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
        if path == 'actions/runs/42/artifacts?per_page=100':
            pytest.fail('a pinned artifact must not depend on its position in an artifact-list page')
        if path == 'actions/artifacts/72':
            return {'id': 73 if damage == 'artifact-id' else 72,
                'name': 'nebius-candidate-' + 'a' * 40 + '-42-2', 'expired': False,
                'digest': 'sha256:' + ('f' if damage == 'artifact-digest' else 'e') * 64,
                'workflow_run': {'id': 43 if damage == 'artifact-run' else 42, 'head_sha': 'a' * 40}}
        result = recovery_api(path, payload)
        if path == 'actions/runs/42/attempts/2' and damage == 'attempt':
            result = {**result, 'run_attempt': 3}
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


@pytest.mark.parametrize('recover,file_writes', [(False, False), (False, True), (True, False), (True, True),
    ('pre-upgrade-backup', True), ('30-migrate.yaml', True), ('50-configure.yaml', True), ('guard-release', True),
    ('release-before-commit', True), ('completion-prepare', True), ('completion-ack', True)])
def test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(operation_inputs, connection, tmp_path, monkeypatch, recover, file_writes):
    """Real pool effects and deployment lifecycle; external I/O/qualification doubled."""
    inputs = operation_inputs
    pool_state, _, connect = connection
    retained = pool_state.retained
    config, plan = inputs['config'], retained.release
    publication = tmp_path / 'publication'
    publication.mkdir()
    (publication / 'candidate.json').write_text(json.dumps(inputs['candidate']))
    (publication / 'runtime-profile.json').write_text(inputs['runtime_profile'].model_dump_json())
    args = SimpleNamespace(kubeconfig=tmp_path / 'unused', namespace=config['namespace'],
        expected_cluster_id=config['cluster_id'], publication_dir=publication,
        candidate=inputs['candidate']['candidate_sha'], evidence_dir=tmp_path / 'evidence', github=True)
    intents, phases = [], []
    completion_faults = {'guard-release', 'release-before-commit', 'completion-prepare', 'completion-ack'}
    failed_job = ('loom-predeploy-' + retained.reference.operation_id.hex if recover == 'pre-upgrade-backup' else
        plan.files[recover][0]['metadata']['name'] if isinstance(recover, str) and recover not in completion_faults else None)
    state = {'fail_https': recover is True, 'fail_job': failed_job, 'lose_release': recover == 'guard-release'}
    if recover in {'completion-prepare', 'completion-ack'}:
        from scripts.ops import nebius_certificates as private_state

        original_write, original_atomic = private_state._write_private, private_state._atomic_json
        fault = [True]

        def write(path, payload, *args, **kwargs):
            if path.name.endswith('-completion.json') and recover == 'completion-prepare' and fault[0]:
                fault[0] = False
                raise OSError('simulated interrupted effect-free preparation')
            return original_write(path, payload, *args, **kwargs)

        def atomic(path, payload, *args, **kwargs):
            result = original_atomic(path, payload, *args, **kwargs)
            if path.name == 'release.json' and payload.get('status') == 'released' and recover == 'completion-ack' and fault[0]:
                fault[0] = False
                raise OSError('simulated committed completion acknowledgement loss')
            return result

        monkeypatch.setattr(private_state, '_write_private', write)
        monkeypatch.setattr(private_state, '_atomic_json', atomic)
    release_attempts = []

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
                assert self.guard_identity is not None and 'manager' in pool_state.effects
                phases.append(Path(command[2]).name)
                if '--dry-run=server' in command:
                    import yaml

                    pod = yaml.safe_load(Path(command[2]).read_text())
                    if pod['spec']['containers'][0]['securityContext']['capabilities'].get('add'):
                        raise deploy.TaskIdentityPolicyDeniedError(config['execution_namespace'] + '-private-root-v1')
                    return ''
            if 'loom.nebius_rollout_guard' in command and 'release' in command:
                release_attempts.append(command)
                if recover == 'release-before-commit' and len(release_attempts) == 1:
                    raise deploy.DeploymentError('guard release failed before commit')
            result = super().run(*command, **kwargs)
            if 'loom.nebius_rollout_guard' in command and 'release' in command and state['lose_release']:
                state['lose_release'] = False
                raise deploy.DeploymentError('guard release response lost after commit')
            if command[:2] == ('config', 'view'):
                value = json.loads(result)
                value['clusters'][0]['name'] = config['cluster_id']
                return json.dumps(value)
            return result

    kube = Kube(config, plan.files, database=True)
    pool_state.kube = kube
    for row in inputs['current']:
        kube.objects[row['kind'].lower(), row['metadata']['name']] = copy.deepcopy(row)
    previous_candidate = json.loads(kube.objects['configmap', 'loom-platform-config']['data']['profile.json'])['candidate_sha']

    def github(path, payload):
        if path == 'deployments':
            intents.append(copy.deepcopy(payload['payload']))
            return {'id': len(intents)}
        return {}

    def smoke(*_):
        if state['fail_https']:
            state['fail_https'] = False
            raise deploy.DeploymentError('simulated post-workload HTTPS failure')

    def qualify_pool(phase):
        if phase == 'released':
            assert kube.guard_identity is None
        elif phase != 'preflight':
            assert kube.guard_identity == (retained.guard_owner, inputs['candidate']['candidate_sha'])

    pool_state.qualify = qualify_pool

    monkeypatch.setattr(idle, 'Kubectl', lambda _: kube)
    monkeypatch.setattr(idle, 'github', github)
    monkeypatch.setattr(idle, 'candidate_follows', lambda *_: True)
    monkeypatch.setattr(idle, 'candidate_schema_head', lambda _: '0175')
    monkeypatch.setattr(idle, 'render_candidate', lambda *_: pytest.fail('retained shared files must not be regenerated'))
    monkeypatch.setattr(deploy, 'public_smoke', smoke)
    monkeypatch.setenv('GITHUB_RUN_ID', '991')
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '1')
    pool = connect()
    writer_args = {}
    written = []
    job_creates = []
    job_deletes = []
    if file_writes:
        import ssl
        from contextlib import nullcontext

        import httpx
        from scripts.ops.nebius_management_switch import _matches
        from scripts.ops.nebius_pool_participant_creation import HTTPSParticipantCreationAPI
        from scripts.ops.nebius_pool_participant_resource import RetainedCandidateFileWriter
        from tests.ops.test_nebius_management_stage import PhaseAPI
        from tests.ops.test_nebius_pool_participant_creation import terminal_pod

        # The older kubectl fake indexes only kind/name. The real API separates
        # identically named execution/build ServiceAccounts by namespace.
        inventory = {row['kind'] + ':' + row['metadata'].get('namespace', '-') + ':' + row['metadata']['name']:
            copy.deepcopy(row) for row in inputs['current']}
        jobs = PhaseAPI(plan.migration.registration.binding)
        jobs.key = lambda row: row['kind'] + ':' + row['metadata'].get('namespace', '-') + ':' + row['metadata']['name']
        jobs.resources = inventory
        pods = []

        def open_job(key):
            api = HTTPSParticipantCreationAPI(retained=retained, key=key,
                api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
                qualify_creation=lambda *_: pool.under_guard(kube, plan))

            def respond(request):
                if request.method == 'GET':
                    if request.url.path == '/api/v1/namespaces/' + config['namespace'] + '/pods':
                        assert dict(request.url.params) == {'limit': '1000'}
                        return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'PodList',
                            'metadata': {'resourceVersion': '70'}, 'items': pods})
                    assert request.url.path.startswith('/apis/batch/v1/namespaces/' + config['namespace'] + '/jobs/')
                    row = inventory.get('Job:' + config['namespace'] + ':' + request.url.path.rsplit('/', 1)[-1])
                    return httpx.Response(404) if row is None else httpx.Response(200, json=row)
                if request.method == 'DELETE':
                    name = request.url.path.rsplit('/', 1)[-1]
                    assert name == failed_job
                    assert request.url.path == '/apis/batch/v1/namespaces/' + config['namespace'] + '/jobs/' + name
                    row = inventory['Job:' + config['namespace'] + ':' + name]
                    assert json.loads(request.content) == {'apiVersion': 'v1', 'kind': 'DeleteOptions',
                        'propagationPolicy': 'Foreground', 'preconditions': {
                            'uid': row['metadata']['uid'], 'resourceVersion': row['metadata']['resourceVersion']}}
                    del inventory[jobs.key(row)]
                    del kube.objects['job', name]
                    pods[:] = [pod for pod in pods
                        if pod['metadata']['ownerReferences'][0]['uid'] != row['metadata']['uid']]
                    job_deletes.append(name)
                    return httpx.Response(202, json={'apiVersion': 'v1', 'kind': 'Status', 'status': 'Success'})
                assert request.method == 'POST'
                assert request.url.path == '/apis/batch/v1/namespaces/' + config['namespace'] + '/jobs'
                document = json.loads(request.content)
                if request.url.params:
                    assert dict(request.url.params) == {'dryRun': 'All'}
                    return httpx.Response(201, json=jobs.default_resource(document))
                jobs.create_resource(document)
                row = jobs.get_resource(document)
                success = row['metadata']['name'] != state['fail_job']
                if not success:
                    state['fail_job'] = None
                row['status'] = {'conditions': [{'type': 'Complete' if success else 'Failed', 'status': 'True'}],
                    'succeeded': int(success), 'failed': int(not success), 'active': 0}
                inventory[jobs.key(row)] = row
                kube.objects['job', row['metadata']['name']] = row
                pods.append(terminal_pod(row, succeeded=success))
                job_creates.append(row['metadata']['name'])
                return httpx.Response(201, json=row)

            api.client.close()
            api.client = httpx.Client(base_url=config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
            return api

        class ResourceIO:
            def qualify(self):
                assert kube.guard_identity is not None and 'manager' in pool_state.effects

            def read_workload(self, key):
                return copy.deepcopy(inventory[key])

            def preview_workload(self, key, before, desired):
                return copy.deepcopy(desired)

            def patch_workload(self, key, before, desired):
                actual = self.read_workload(key)
                assert actual['metadata']['uid'] == before['metadata']['uid']
                assert actual['metadata']['resourceVersion'] == before['metadata']['resourceVersion']
                value = {**copy.deepcopy(desired), 'metadata': {**copy.deepcopy(desired['metadata']),
                    'uid': before['metadata']['uid'], 'resourceVersion': '100', 'generation': 1}}
                if value['kind'] in {'Deployment', 'StatefulSet'}:
                    replicas = value['spec'].get('replicas', 1)
                    value['status'] = {'observedGeneration': 1, 'replicas': replicas,
                        'updatedReplicas': replicas, 'availableReplicas': replicas, 'readyReplicas': replicas}
                kube.objects[value['kind'].lower(), value['metadata']['name']] = value
                inventory[key] = copy.deepcopy(value)
                written.append(key)
                return True

            def workload_ready(self, key, expected):
                observed = self.read_workload(key)
                return _matches(observed, expected, observed['metadata']['uid'])

        writer_args['candidate_writer'] = RetainedCandidateFileWriter(retained=retained,
            open_resource=lambda _: nullcontext(ResourceIO()), open_creation=open_job)
    if recover:
        error = ('HTTPS' if recover is True else {'guard-release': 'guard release response lost',
            'release-before-commit': 'guard release failed', 'completion-prepare': 'release intent',
            'completion-ack': 'release completion'}.get(recover, 'execution unconfirmed'))
        with pytest.raises(ValueError, match=error):
            idle.rollout(args, participant_rollout=retained, pool=pool, **writer_args)
        failed, = args.evidence_dir.glob('deployment-*.json')
        assert json.loads(failed.read_text())['shared_pool_release'] == retained.reference.model_dump(mode='json')
        assert kube.guard_identity == (None if recover in {'guard-release', 'completion-ack'} else
            (retained.guard_owner, inputs['candidate']['candidate_sha']))
        args.recovery_evidence = failed
        # Recovery uses the original snapshot even though the platform's live
        # configuration now identifies the partially deployed new candidate.
        expected_candidate = previous_candidate if recover == 'pre-upgrade-backup' else 'd' * 40
        assert json.loads(kube.objects['configmap', 'loom-platform-config']['data']['profile.json'])['candidate_sha'] == expected_candidate
        retained = operation.load_participant_rollout(reference=retained.reference,
            state_root=inputs['state_root'], anchor_dir=inputs['anchor_dir'])
        if recover == 'guard-release':
            from scripts.ops import nebius_certificates as private_state
            from scripts.ops.nebius_pool_participant_completion import RetainedParticipantCompletion

            completion = RetainedParticipantCompletion(writer_args['candidate_writer'], pool)
            original_completion = completion.path.read_bytes()
            original_anchor = completion.anchor.read_bytes()
            for path, original in ((completion.path, original_completion), (completion.anchor, original_anchor)):
                path.unlink()
                with pytest.raises(ValueError, match='preserve'):
                    completion.reconcile(kube)
                private_state._write_private(path, original)
            damaged = json.loads(original_completion)
            next(iter(damaged['jobs'].values()))['pod_uids'] = [str(uuid4())]
            private_state._atomic_json(completion.path, damaged)
            with pytest.raises(ValueError, match='preserve'):
                completion.reconcile(kube)
            private_state._atomic_json(completion.path, json.loads(original_completion))
            pool_state.ready = False
            with pytest.raises(ValueError, match='preserve'):
                completion.reconcile(kube)
            pool_state.ready = True
    result = idle.rollout(args, participant_rollout=retained, pool=pool, **writer_args)
    assert result['status'] == 'complete' and kube.guard_identity is None
    assert result['shared_pool_release'] == retained.reference.model_dump(mode='json')
    assert pool_state.effects == ['config', 'fence', 'stage', 'config', 'manager', 'open', 'capacity'] + (
        ['capacity'] if recover in {'release-before-commit', 'completion-prepare'} else [])
    if file_writes:
        assert not set(phases) & {'00-namespaces.yaml', '10-config-network.yaml', '20-database.yaml',
            '40-services.yaml', '60-execution.yaml', '70-public.yaml', '80-backup.yaml',
            'identity-policy-bootstrap.yaml', '00-task-identity-policy.yaml', '30-migrate.yaml', '50-configure.yaml'}
        assert not any(command[0] in {'create', 'delete'} for command in kube.commands)
        expected_jobs = ['loom-predeploy-' + retained.reference.operation_id.hex,
            plan.files['30-migrate.yaml'][0]['metadata']['name'], plan.files['50-configure.yaml'][0]['metadata']['name']]
        if failed_job is not None:
            expected_jobs.insert(expected_jobs.index(failed_job), failed_job)
        assert job_creates == expected_jobs
        assert job_deletes == ([] if failed_job is None else [failed_job])
        assert len(written) == len(set(written)), 'recovery must read back prior writes'
        assert not any(key.startswith(('PersistentVolumeClaim:', 'ResourceQuota:')) for key in written)
        assert {'Deployment:' + config['namespace'] + ':loom-service',
            'Deployment:' + config['execution_namespace'] + ':loom-execution-actuator',
            'Service:' + config['namespace'] + ':loom-web'} <= set(written)
    else:
        assert {'40-services.yaml', '60-execution.yaml', '70-public.yaml'} <= set(phases)
        assert {'30-migrate.yaml', '50-configure.yaml'} <= set(phases)
    assert all(row['shared_pool_release'] == retained.reference.model_dump(mode='json') for row in intents)
    assert len(intents) == (2 if recover else 1)
    if recover in completion_faults:
        # Completion reconciliation must not reacquire intake or repeat any
        # build/deploy/pool effect after the original release committed.
        actions = [command for command in kube.commands if 'loom.nebius_rollout_guard' in command
            and ('acquire' in command or 'release' in command)]
        assert len(actions) == 2


def test_completion_preserves_the_existing_database_clock_skew_allowance(
        operation_inputs, connection, tmp_path, monkeypatch):
    from datetime import datetime, timedelta

    from tests.ops import test_nebius_pool_participant_adapter as transport

    class DatabaseClock:
        @staticmethod
        def now(tz):
            return datetime.now(tz) + timedelta(seconds=5)

    monkeypatch.setattr(transport, 'datetime', DatabaseClock)
    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)


def test_completed_release_is_the_next_receipt_selected_predecessor(
        operation_inputs, connection, tmp_path, monkeypatch):
    from scripts.ops import nebius_pool_participant_completion as completion
    from scripts.ops.nebius_ingress_stage import _key, _uid
    from scripts.ops.nebius_management_switch import _stable

    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
    loader = getattr(completion, 'load_completed_participant_rollout', None)
    assert callable(loader), 'a released operation must have a semantic predecessor loader'
    retained = connection[0].retained
    before = {path: path.read_bytes() for root in (retained.state_dir, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()}
    successor = loader(reference=retained.reference, state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir)
    assert {path: path.read_bytes() for path in before} == before
    selected = retained.release.refresh.participant_id
    guard, = (row for row in successor.migration.guards if row.participant_id == selected)
    old_guard, = (row for row in retained.inputs.previous.guards if row.participant_id == selected)
    kube = connection[0].kube
    assert _stable(guard.controller) == _stable(kube.objects['deployment', 'loom-control-plane'])
    assert _uid(guard.controller) == _uid(old_guard.controller)
    assert guard.controller['metadata']['annotations']['loom.nebius/participant-candidate-operation'] == str(retained.reference.operation_id)
    assert _stable(guard.database.statefulset) == _stable(kube.objects['statefulset', 'loom-postgres'])
    assert _stable(guard.database.service) == _stable(kube.objects['service', 'loom-postgres'])
    assert guard.database.credential_uid == old_guard.database.credential_uid
    assert guard.database.credential_resource_version == old_guard.database.credential_resource_version
    assert tuple(row for row in successor.migration.guards if row.participant_id != selected) == tuple(
        row for row in retained.inputs.previous.guards if row.participant_id != selected)
    assert successor.migration.registration.spec == retained.release.refresh.after
    assert successor.manager['spec']['template']['spec']['volumes'] != retained.inputs.manager['spec']['template']['spec']['volumes']
    assert successor.catalog['metadata']['name'] == 'loom-pool-profiles-' + retained.reference.operation_id.hex
    assert retained.state_dir / 'completion/release.json' in successor.history
    assert retained.state_dir / 'pool-refresh/dispatch.json' in successor.history
    assert retained.state_dir.parent / 'latest.json' not in successor.history

    # Prepare another real release using the receipt-derived starting point,
    # including the updated manager/catalog and the unchanged peer.
    inputs = copy.deepcopy(operation_inputs)
    inputs.update(predecessor=successor, operation_id=uuid4(), manager=successor.manager,
        catalog=successor.catalog, current=tuple(successor.resources[_key(row)] for row in retained.inputs.current
            if row['kind'] != 'Job'))
    following = operation.retain_participant_rollout(**inputs)
    assert following.inputs.previous == successor.migration
    assert following.release.refresh.before == retained.release.refresh.after
    assert following.release.refresh.after.policy_revision == retained.release.refresh.after.policy_revision + 1
    assert loader(reference=retained.reference, state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir) == successor


def test_completed_predecessor_rejects_pending_or_changed_history(
        operation_inputs, connection, tmp_path, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_participant_completion as completion

    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
    loader = getattr(completion, 'load_completed_participant_rollout', None)
    assert callable(loader), 'a released flag alone is not completed predecessor evidence'
    retained = connection[0].retained

    def load():
        return loader(reference=retained.reference, state_root=retained.state_dir.parent,
            anchor_dir=retained.anchor_dir)

    baseline = load()
    record_path = retained.state_dir / 'completion/release.json'
    source = record_path.read_bytes()
    for status in ('prepared', 'release_intent'):
        record = json.loads(source)
        record['status'] = status
        private_state._atomic_json(record_path, record)
        with pytest.raises(ValueError, match='completed participant'):
            load()
    private_state._atomic_json(record_path, json.loads(source))
    for path in (record_path, retained.anchor_dir / (str(retained.reference.operation_id) + '-completion.json'),
            retained.state_dir / 'pool-refresh/dispatch.json', retained.state_dir / 'inputs.json',
            next(iter(retained.inputs.history))):
        original = path.read_bytes()
        path.unlink()
        with pytest.raises(ValueError, match='completed participant'):
            load()
        assert not path.exists(), 'history loading must not recreate missing evidence'
        private_state._write_private(path, original)
        private_state._atomic_json(path, {'altered': True})
        with pytest.raises(ValueError, match='completed participant'):
            load()
        private_state._atomic_json(path, json.loads(original))
    assert load() == baseline


def test_latest_participant_head_advances_only_from_exact_completed_predecessor(
        operation_inputs, connection, tmp_path, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_participant_completion as completion

    initialize = getattr(completion, 'initialize_participant_head', None)
    assert callable(initialize), 'normal releases need a retained compare-and-swap head'
    initial = operation_inputs['predecessor']
    retained = connection[0].retained
    options = dict(initial=initial, state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir)
    assert initialize(**options) == initial
    assert initialize(**options) == initial
    pointer = retained.state_dir.parent / 'latest.json'
    initial_bytes = pointer.read_bytes()
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    assert pointer.read_bytes() == initial_bytes
    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)

    wrong = replace(initial, state_dir=tmp_path)
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=wrong, reference=retained.reference)
    assert pointer.read_bytes() == initial_bytes
    # A committed pointer update whose acknowledgement is lost must be safe to
    # retry; the completed release's frozen history must not contain that pointer.
    atomic = private_state._atomic_json

    def lose_ack(path, payload):
        atomic(path, payload)
        if path == pointer:
            raise OSError('simulated head-promotion acknowledgement loss')

    with monkeypatch.context() as patch:
        patch.setattr(private_state, '_atomic_json', lose_ack)
        with pytest.raises(ValueError, match='participant head'):
            completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    successor = completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    promoted_bytes = pointer.read_bytes()
    assert promoted_bytes != initial_bytes
    assert completion.load_participant_head(**options) == successor
    assert pointer not in successor.history
    assert completion.promote_participant_head(**options, expected=initial, reference=retained.reference) == successor
    assert pointer.read_bytes() == promoted_bytes
    assert initialize(**options) == successor, 'initialization must never rewind a completed head'
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=successor, reference=retained.reference)
    assert pointer.read_bytes() == promoted_bytes
    pointer.unlink()
    with pytest.raises(ValueError, match='participant head'):
        initialize(**options)
    assert not pointer.exists(), 'lost latest pointer must not reset to the initial handoff'
    private_state._write_private(pointer, promoted_bytes)
    value = json.loads(promoted_bytes)
    value['selected']['operation_id'] = str(uuid4())
    private_state._atomic_json(pointer, value)
    with pytest.raises(ValueError, match='participant head'):
        completion.load_participant_head(**options)
    private_state._atomic_json(pointer, json.loads(promoted_bytes))
    # Re-reading old immutable history remains possible after pointer movement.
    assert completion.load_completed_participant_rollout(reference=retained.reference,
        state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir) == successor


def test_expired_monotonic_capacity_cannot_create_a_release_intent(operation_inputs, connection, tmp_path, monkeypatch):
    import time

    from scripts.ops.nebius_pool_participant_adapter import RetainedParticipantPoolAdapter

    elapsed = [0]
    original = RetainedParticipantPoolAdapter.read_capacity

    def read_capacity(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        elapsed[0] += 61  # Transport fixture's capacity receipt has a 60-second lease.
        return result

    monkeypatch.setattr(RetainedParticipantPoolAdapter, 'read_capacity', read_capacity)
    monkeypatch.setattr(deploy, 'time', SimpleNamespace(monotonic=lambda: time.monotonic() + elapsed[0], sleep=time.sleep))
    with pytest.raises(deploy.DeploymentError, match='capacity proof unavailable or expired'):
        test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle(
            operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
    retained = connection[0].retained
    assert not (retained.state_dir / 'completion/release.json').exists()
    assert not (retained.anchor_dir / (str(retained.reference.operation_id) + '-completion.json')).exists()


@pytest.mark.parametrize('damage', ['missing-adapter', 'missing-operation', 'mutated-operation',
    'candidate', 'profile', 'lost-reference', 'standalone-recovery', 'late-history-change'])
def test_shared_delivery_rejects_binding_changes_before_deployer_dispatch(operation_inputs, tmp_path, monkeypatch, damage):
    from scripts.ops import nebius_certificates as private_state

    inputs = operation_inputs
    retained = operation.retain_participant_rollout(**inputs)
    config = inputs['config']
    candidate = copy.deepcopy(inputs['candidate'])
    profile = inputs['runtime_profile'].model_dump(mode='json')
    if damage == 'candidate':
        candidate['images']['web']['image_ref'] = 'registry.example/foreign@sha256:' + 'f' * 64
    if damage == 'profile':
        profile['max_log_bytes_per_stream'] = 17
    (tmp_path / 'candidate.json').write_text(json.dumps(candidate))
    (tmp_path / 'runtime-profile.json').write_text(json.dumps(profile))
    args = SimpleNamespace(kubeconfig=tmp_path / 'unused', namespace=config['namespace'],
        publication_dir=tmp_path, candidate=candidate['candidate_sha'], evidence_dir=tmp_path / 'evidence', github=True)
    data = next(row['data'] for row in inputs['current'] if row['kind'] == 'ConfigMap'
        and row['metadata']['name'] == 'loom-platform-config')
    monkeypatch.setattr(idle, 'Kubectl', lambda _: SimpleNamespace(get=lambda *_: {'data': copy.deepcopy(data)}))
    monkeypatch.setattr(idle, 'candidate_follows', lambda *_: True)
    monkeypatch.setattr(idle, 'candidate_schema_head', lambda *_: '0175')
    monkeypatch.setattr(idle, 'deploy', lambda *args, **kwargs: pytest.fail('unqualified shared input reached deployment'))
    monkeypatch.setattr(idle, 'render_candidate', lambda *_: pytest.fail('shared recovery fell back to standalone rendering'))
    seen = []

    def github(path, payload):
        seen.append((path, payload))
        if path == 'deployments':
            return {'id': 1}
        if damage == 'late-history-change' and payload['state'] == 'in_progress':
            history, = inputs['predecessor'].history
            private_state._atomic_json(history, {'changed': True})
        return {}

    monkeypatch.setattr(idle, 'github', github)
    selected, pool = retained, object()
    if damage == 'missing-adapter':
        pool = None
    elif damage == 'missing-operation':
        selected = None
    elif damage == 'mutated-operation':
        selected = replace(retained, inputs=retained.inputs.model_copy(deep=True))
        selected.inputs.release.files['70-public.yaml'] = []
    elif damage in {'lost-reference', 'standalone-recovery'}:
        record = {'schema_version': 'loom.nebius-deployment.v1', 'status': 'failed', 'mode': 'apply',
            'candidate_sha': candidate['candidate_sha'], 'previous_candidate_sha': 'c' * 40,
            'guard_owner': retained.guard_owner,
            **{key: config[key] for key in ('cluster_id', 'namespace', 'execution_namespace')}}
        if damage == 'standalone-recovery':
            record['shared_pool_release'] = retained.reference.model_dump(mode='json')
            selected, pool = None, None
        args.recovery_evidence = tmp_path / 'failed.json'
        args.recovery_evidence.write_text(json.dumps(record))
    with pytest.raises(ValueError):
        idle.rollout(args, participant_rollout=selected, pool=pool)
    if damage == 'late-history-change':
        assert seen[-1][1]['state'] == 'failure'
    else:
        assert not seen
