"""Normal idle rollout consumes the retained shared operation, including recovery."""
from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as idle
from tests.ops.test_nebius_pool_participant_adapter import connection as connection
from tests.ops.test_nebius_pool_participant_delivery import (
    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle as _exercise_release,
)
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)


@pytest.mark.timeout(120)
def test_operator_host_executes_retained_source_without_runner_paths_or_github(
        operation_inputs, connection, tmp_path, monkeypatch, recovery=False, *, via_dispatch=False, via_runner=False):
    from scripts.ops import nebius_pool_participant_entry as entry
    from scripts.ops import nebius_shared_pool_entry as shared_entry
    from scripts.ops.nebius_pool_participant_completion import initialize_participant_head

    from loom.nebius_kubernetes import NebiusKubernetesConnection

    assert callable(getattr(entry, 'execute_retained_participant_release', None)), 'host release intake is not connected'
    retained, initial = connection[0].retained, operation_inputs['predecessor']
    initialize_participant_head(initial=initial, state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir)
    ca, credential = (tmp_path / name for name in ('current-ca.pem', 'current-operator.json'))
    for path in (ca, credential):
        path.write_bytes(b'current-private-test-material')
        path.chmod(0o600)
    operator = NebiusKubernetesConnection(endpoint=retained.inputs.config['kubernetes_api_server'],
        ca_file=ca, credentials_file=credential)
    historical = SimpleNamespace(bindings=object(), files={path: path.read_bytes() for path in initial.history})
    shared_operation = tmp_path / 'original-shared-operation.json'

    def shared_history(**kwargs):
        assert kwargs == {'operation_path': shared_operation, 'operation_sha256': 'c' * 64}
        # This lifecycle fixture supplies the parent's qualified initial history;
        # real semantic handoff loading has separate coverage, not a fake receipt.
        return historical, initial

    monkeypatch.setattr(shared_entry, 'load_completed_history', shared_history)
    original = idle.rollout
    paths, completed, arguments, private_results, private_errors = [], [], [], [], []

    def connected(args, **kwargs):
        # The runner's publication/kubeconfig/evidence paths are unavailable on
        # the host. Only the reference and bounded recovery projection cross it.
        incoming = idle.recovery_record(json.loads(args.recovery_evidence.read_bytes())) if getattr(args, 'recovery_evidence', None) else None
        kube = connection[0].kube
        kube.kubeconfig = tmp_path / 'private-host-kubeconfig'
        qualifier = SimpleNamespace(retained=retained, initial=initial, kube=kube,
            kubeconfig=kube.kubeconfig, candidate_follows=lambda *_: True)
        delivery = entry.ParticipantReleaseConnection(qualifier, initial, kwargs['pool'], kwargs['candidate_writer'])

        @contextmanager
        def authenticated(**options):
            assert options['retained'] == retained and options['initial'] == initial
            assert options['expected'] == initial and options['bindings'] is historical.bindings
            assert options['shared_state'] == initial.state_dir
            assert options['operator_connection'] == operator
            assert options['operator_credentials'] == credential
            assert options['private_files'] == {**historical.files, ca: ca.read_bytes(), credential: credential.read_bytes()}
            yield delivery

        def deployed(host_args, **host_kwargs):
            paths.append(host_args.publication_dir)
            assert host_args.kubeconfig == kube.kubeconfig and host_args.github is False
            assert host_args.evidence_dir == retained.state_dir / 'deployment-evidence'
            assert json.loads((host_args.publication_dir / 'candidate.json').read_bytes()) == retained.inputs.candidate
            assert host_args.publication_dir != args.publication_dir
            # Feed this private path back only to the local lifecycle fixture's
            # recovery reader, not to a runner or a public operation report.
            args.evidence_dir = host_args.evidence_dir
            arguments.append(host_args)
            try:
                outcome = original(host_args, **host_kwargs)
                private_results.append(outcome)
                return outcome
            except Exception as error:
                private_errors.append(error)
                raise

        with monkeypatch.context() as patch:
            patch.setattr(idle, 'rollout', deployed)
            patch.setattr(idle, 'candidate_follows', lambda *_: pytest.fail('host has no Git checkout'))
            patch.setattr(idle, 'candidate_schema_head', lambda *_: pytest.fail('host has no migrations checkout'))
            patch.setattr(entry, 'authenticated_participant_release', authenticated)
            try:
                selection = dict(shared_operation=shared_operation, shared_operation_sha256='c' * 64,
                    state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir,
                    operator_connection=operator, executable=tmp_path / 'kubectl')
                if via_dispatch or via_runner:
                    from scripts.ops.nebius_pool_participant_dispatch import (
                        ParticipantReleaseHost,
                        dispatch_participant_release,
                    )

                    action = 'recover' if incoming is not None else 'execute'
                    request = {'schema_version': 'loom.nebius-participant-command.v1', 'action': action,
                        'reference': retained.reference.model_dump(mode='json'),
                        **({'recovery': incoming} if incoming is not None else {'automatic': False})}
                    host = ParticipantReleaseHost(**selection, repo_root=operation_inputs['repo_root'])
                    if via_runner:
                        from scripts.ops import nebius_pool_participant_rollout as runner

                        private_github = idle.github
                        public = tmp_path / 'runner-evidence'
                        attempt = 2 if incoming is not None else 1
                        runner_recovery = idle.recovery_record(json.loads((public / 'deployment-991-1.json').read_text())) if incoming else None
                        intents = []
                        reports = []

                        def github(path, payload=None):
                            if path.startswith('deployments?'):
                                return []
                            if path == 'deployments':
                                intents.append(copy.deepcopy(payload['payload']))
                            return {'id': 123}

                        def prepare(**options):
                            # Reuse this fixture's qualified preparation. Actual
                            # source/receipt preparation is tested separately.
                            assert options['operation_id'] == retained.reference.operation_id
                            assert options['publication'] == retained.reference.publication
                            return retained

                        def transport(command, *, action):
                            if action != 'prepare':
                                assert intents and intents[-1]['shared_pool_release'] == command['reference']
                            with monkeypatch.context() as host_patch:
                                host_patch.setattr(idle, 'github', private_github)
                                outcome = dispatch_participant_release(json.dumps(command).encode(), action=action, host=host)
                            reports.append(outcome)
                            return outcome

                        patch.setattr(idle, 'github', github)
                        patch.setattr(entry, 'prepare_retained_participant_release', prepare)
                        patch.setattr(runner, '_identity', lambda *parts: str(retained.reference.operation_id))
                        try:
                            result = runner.run_release(publication=None if incoming else retained.reference.publication,
                                run_id=991, run_attempt=attempt, evidence_dir=public, transport=transport,
                                recovery=runner_recovery)
                            assert result['status'] == 'complete'
                        except runner.RolloutError:
                            assert private_errors, 'runner failed without an injected deployment fault'
                            record = json.loads((public / f'deployment-991-{attempt}.json').read_text())
                            assert record['status'] == 'failed' and str(retained.state_dir) not in json.dumps(record)
                            raise private_errors.pop() from None
                        report = reports[-1]
                    else:
                        report = dispatch_participant_release(json.dumps(request).encode(), action=action, host=host)
                    assert report['release']['reference'] == retained.reference.model_dump(mode='json')
                    assert str(retained.state_dir) not in json.dumps(report)
                    if report['status'] == 'blocked':
                        assert private_errors, 'dispatch failed without an injected deployment fault'
                        assert set(report) == {'schema_version', 'status', 'stage', 'release'}
                        assert report['stage'] == 'execution'
                        # Only the local fixture needs the original exception
                        # for its fault assertions; the public report has none.
                        raise private_errors.pop()
                    assert set(report) == {'schema_version', 'status', 'release'} and report['status'] == 'complete'
                    assert report['release']['guard_owner'] == retained.guard_owner
                    result = private_results[-1]
                else:
                    result = entry.execute_retained_participant_release(reference=retained.reference,
                        recovery=incoming, **selection)
                completed.append(result)
            finally:
                assert not paths[-1].exists(), 'private temporary publication survived the host call'
        return result

    monkeypatch.setattr(idle, 'rollout', connected)
    _exercise_release(
        operation_inputs, connection, tmp_path, monkeypatch, recover=recovery, file_writes=True, host_delivery=True)
    assert completed[-1]['status'] == 'complete'
    assert all(not path.exists() for path in paths)


@pytest.mark.timeout(120)
def test_data_only_dispatch_runs_actual_release_and_original_recovery(
        operation_inputs, connection, tmp_path, monkeypatch):
    test_operator_host_executes_retained_source_without_runner_paths_or_github(
        operation_inputs, connection, tmp_path, monkeypatch, False, via_dispatch=True)


@pytest.mark.parametrize('recovery', [False, True, 'completion-ack'])
@pytest.mark.timeout(120)
def test_protected_runner_connects_dispatch_to_actual_rollout_and_original_recovery(
        operation_inputs, connection, tmp_path, monkeypatch, recovery):
    test_operator_host_executes_retained_source_without_runner_paths_or_github(
        operation_inputs, connection, tmp_path, monkeypatch, recovery, via_runner=True)
