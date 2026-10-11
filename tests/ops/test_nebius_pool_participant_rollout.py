"""Runner persists release identity before effects and never retries uncertain SSH."""
from __future__ import annotations

import copy
import importlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as idle
from tests.ops.test_nebius_pool_participant_gateway import report

from loom.repository_identity import REPOSITORY_ID


def module():
    name = 'scripts.ops.nebius_pool_participant_rollout'
    assert importlib.util.find_spec(name) is not None, 'protected runner delivery is missing'
    return importlib.import_module(name)


@pytest.fixture
def publication_api(monkeypatch):
    repo = {'id': REPOSITORY_ID, 'full_name': 'ca-mrc/loom'}
    artifact = {'id': 72, 'name': 'nebius-candidate-' + 'a' * 40 + '-42-2', 'expired': False,
        'digest': 'sha256:' + 'e' * 64, 'workflow_run': {'id': 42, 'head_sha': 'a' * 40,
            'head_branch': 'dev', 'repository_id': REPOSITORY_ID, 'head_repository_id': REPOSITORY_ID}}
    rows = {'actions/runs/42': {'id': 42, 'run_attempt': 2, 'head_sha': 'a' * 40,
        'head_branch': 'dev', 'repository': repo, 'head_repository': repo,
        'path': '.github/workflows/nebius-candidate.yml', 'event': 'push',
        'status': 'completed', 'conclusion': 'success'},
        'actions/runs/42/artifacts?per_page=100&page=1': {'artifacts': [artifact]},
        'commits/' + 'a' * 40 + '/pulls?per_page=100&page=1': [{'number': 2425}],
        'pulls/2425': {'number': 2425, 'merged': True, 'state': 'closed',
            'merge_commit_sha': 'a' * 40, 'base': {'ref': 'dev', 'repo': repo}}}
    monkeypatch.setattr(idle, 'github', lambda path, payload=None: copy.deepcopy(rows[path]))
    return rows


@pytest.mark.parametrize('damage', [None, 'attempt', 'sha', 'run-repository', 'artifact-repository',
    'artifact-sha', 'artifact-expired', 'duplicate-artifact', 'pr-base', 'pr-merge', 'pr-open', 'duplicate-pr'])
def test_select_publication_binds_run_artifact_and_merged_dev_pr(publication_api, damage):
    runner = module()
    rows = publication_api
    run = rows['actions/runs/42']
    artifacts = rows['actions/runs/42/artifacts?per_page=100&page=1']['artifacts']
    pr = rows['pulls/2425']
    if damage == 'attempt':
        run['run_attempt'] = 3
    elif damage == 'sha':
        run['head_sha'] = 'b' * 40
    elif damage == 'run-repository':
        run['repository'] = {'id': 7, 'full_name': 'foreign/loom'}
    elif damage == 'artifact-repository':
        artifacts[0]['workflow_run']['repository_id'] = 7
    elif damage == 'artifact-sha':
        artifacts[0]['workflow_run']['head_sha'] = 'b' * 40
    elif damage == 'artifact-expired':
        artifacts[0]['expired'] = True
    elif damage == 'duplicate-artifact':
        artifacts.append(copy.deepcopy(artifacts[0]))
    elif damage == 'pr-base':
        pr['base']['ref'] = 'main'
    elif damage == 'pr-merge':
        pr['merge_commit_sha'] = 'b' * 40
    elif damage == 'pr-open':
        pr['merged'] = False
    elif damage == 'duplicate-pr':
        rows['commits/' + 'a' * 40 + '/pulls?per_page=100&page=1'].append({'number': 2426})
        rows['pulls/2426'] = {**copy.deepcopy(pr), 'number': 2426}
    kwargs = dict(run_id=42, candidate_sha='a' * 40, artifact_name='nebius-candidate-' + 'a' * 40 + '-42-2')
    if damage:
        with pytest.raises(runner.RolloutError):
            runner.select_publication(**kwargs)
    else:
        publication = runner.select_publication(**kwargs)
        value = publication.model_dump(mode='json')
        assert {key: value[key] for key in value if key != 'candidate_id'} == {
            'source_sha': 'a' * 40, 'run_id': 42, 'run_attempt': 2, 'artifact_id': 72,
            'artifact_sha256': 'sha256:' + 'e' * 64, 'pull_request': 2425}
        assert runner.select_publication(**kwargs) == publication


@pytest.mark.parametrize('failure', [None, 'exit', 'timeout', 'foreign-reference', 'private-report'])
def test_transfer_uses_one_fixed_command_and_closed_response(tmp_path, monkeypatch, failure):
    runner = module()
    binding = report()['release']
    request = {'schema_version': 'loom.nebius-participant-command.v1', 'action': 'execute',
        'reference': binding['reference'], 'automatic': False}
    calls = []

    def process(argv, **kwargs):
        calls.append((argv, kwargs))
        if failure == 'timeout':
            raise subprocess.TimeoutExpired(argv, 2400, stderr='private-secret')
        result = report()
        if failure == 'foreign-reference':
            result['release']['reference']['inputs_sha256'] = 'f' * 64
        elif failure == 'private-report':
            result['exception'] = 'private-secret'
        return SimpleNamespace(returncode=1 if failure == 'exit' else 0,
            stdout=json.dumps(result).encode(), stderr=b'private-secret')

    monkeypatch.setattr(runner.subprocess, 'run', process)
    kwargs = dict(action='execute', target='operator@gateway.example.test', key=tmp_path / 'key', known_hosts=tmp_path / 'hosts')
    if failure:
        with pytest.raises(runner.RolloutError) as error:
            runner.transfer(request, **kwargs)
        assert 'private-secret' not in str(error.value)
    else:
        assert runner.transfer(request, **kwargs) == report()
    assert len(calls) == 1
    argv, options = calls[0]
    assert argv[:3] == ['ssh', '-F', '/dev/null']
    assert 'StrictHostKeyChecking=yes' in argv and 'IdentitiesOnly=yes' in argv
    assert argv[-2:] == ['operator@gateway.example.test', 'loom-nebius-participant-execute-v1']
    assert json.loads(options['input']) == request and options.get('shell', False) is False


@pytest.mark.parametrize('outcome', ['complete', 'skipped_busy', 'blocked', 'transport-lost',
    'binding-changed', 'intent-lost', 'status-lost', 'success-status-lost', 'history-blocked', 'already-deployed'])
def test_runner_persists_original_intent_before_execute(tmp_path, monkeypatch, outcome):
    runner = module()
    from loom_service.environment_management.candidates import ProtectedPublication

    publication = ProtectedPublication.model_validate(report()['release']['reference']['publication'])
    events, payloads = [], []
    prepared = report('prepared')
    evidence_dir = tmp_path / 'evidence'
    saved_intent = []

    def github(path, payload=None):
        events.append(path)
        if path.startswith('deployments?'):
            if outcome in {'history-blocked', 'already-deployed'}:
                return [{'id': 1, 'environment': 'nebius-integration', 'sha': 'a' * 40,
                    'payload': {'schema_version': 'loom.nebius-deployment.v1', 'mode': 'apply',
                        'candidate_sha': 'a' * 40, **{key: prepared['release'][key]
                            for key in ('cluster_id', 'namespace', 'execution_namespace')}}}]
            return []
        if path == 'deployments/1/statuses?per_page=1':
            return [{'state': 'failure' if outcome == 'history-blocked' else 'success'}]
        if path == 'deployments':
            saved_intent.append(copy.deepcopy(payload['payload']))
            records = list(evidence_dir.glob('deployment-*.json'))
            assert len(records) == 1 and json.loads(records[0].read_text()) == payload['payload']
            if outcome == 'intent-lost':
                raise RuntimeError('private GitHub diagnostic')
            return {'id': 99}
        assert path == 'deployments/99/statuses'
        payloads.append(payload)
        if (outcome == 'status-lost' and payload['state'] == 'in_progress'
                or outcome == 'success-status-lost' and payload['state'] == 'success'):
            raise RuntimeError('private GitHub diagnostic')
        return {'id': 100}

    def transport(request, *, action):
        events.append(action)
        if action == 'prepare':
            prepared['release']['reference']['operation_id'] = request['operation_id']
            prepared['release']['guard_owner'] = 'rollout-' + request['operation_id'].replace('-', '')
            if outcome == 'already-deployed':
                prepared['release']['previous_candidate_sha'] = 'a' * 40
            return copy.deepcopy(prepared)
        assert action == 'execute'
        assert saved_intent and payloads[-1]['state'] == 'in_progress'
        assert request['reference'] == saved_intent[0]['shared_pool_release']
        assert request['automatic'] is True
        if outcome == 'transport-lost':
            raise runner.RolloutError('private transport diagnostic')
        result = copy.deepcopy(prepared)
        result['status'] = outcome if outcome in {'complete', 'skipped_busy', 'blocked'} else 'complete'
        if outcome == 'blocked':
            result['stage'] = 'execution'
        elif outcome == 'binding-changed':
            result['release']['namespace'] = 'foreign-platform'
        return result

    monkeypatch.setattr(idle, 'github', github)
    args = dict(publication=publication, run_id=91, run_attempt=2, evidence_dir=evidence_dir,
        transport=transport, automatic=True)
    failures = {'blocked', 'transport-lost', 'binding-changed', 'intent-lost', 'status-lost', 'success-status-lost'}
    if outcome in failures:
        with pytest.raises(runner.RolloutError) as error:
            runner.run_release(**args)
        assert 'private' not in str(error.value)
    else:
        result = runner.run_release(**args)
        expected = {'history-blocked': 'blocked_recovery', 'already-deployed': 'skipped_already_deployed'}.get(outcome, outcome)
        assert result['status'] == expected
    assert events.count('execute') == (0 if outcome in {'intent-lost', 'status-lost', 'history-blocked', 'already-deployed'} else 1)
    if outcome in {'history-blocked', 'already-deployed'}:
        assert not saved_intent
        return
    intent, = saved_intent
    assert intent['run_id'] == 91 and intent['run_attempt'] == 2
    assert intent['previous_candidate_sha'] == 'b' * 40
    assert intent['shared_pool_release'] == prepared['release']['reference']
    record_path, = evidence_dir.glob('deployment-*.json')
    record = json.loads(record_path.read_text())
    assert record['status'] == ('failed' if outcome in failures else outcome)
    assert record['shared_pool_release'] == intent['shared_pool_release']
    assert 'private' not in record_path.read_text()
    if outcome in failures:
        assert idle.recovery_record(record)['shared_pool_release'] == intent['shared_pool_release']
        with pytest.raises(runner.RolloutError):
            runner.run_release(**args)
        assert events.count('execute') <= 1
    else:
        assert payloads[-1]['state'] == ('success' if outcome == 'complete' else 'inactive')


@pytest.mark.parametrize('damage', [None, 'reference', 'platform', 'standalone', 'automatic'])
def test_recovery_uses_original_reference_without_preparation(tmp_path, monkeypatch, damage):
    runner = module()
    binding = report()['release']
    record = {'schema_version': 'loom.nebius-deployment.v1', 'mode': 'apply', 'status': 'failed',
        'shared_pool_release': binding['reference'], 'run_id': 91, 'run_attempt': 2,
        **{key: binding[key] for key in ('candidate_sha', 'previous_candidate_sha', 'guard_owner',
            'cluster_id', 'namespace', 'execution_namespace')}}
    if damage == 'standalone':
        del record['shared_pool_release']
    events, intents = [], []

    def github(path, payload=None):
        if path == 'deployments':
            intents.append(payload['payload'])
        return {'id': 99}

    def transport(request, *, action):
        events.append(action)
        assert action == 'recover' and intents
        assert request['reference'] == binding['reference'] and request['recovery'] == record
        result = report()
        if damage == 'reference':
            result['release']['reference']['inputs_sha256'] = 'f' * 64
        elif damage == 'platform':
            result['release']['previous_candidate_sha'] = 'c' * 40
        return result

    monkeypatch.setattr(idle, 'github', github)
    args = dict(publication=None, run_id=92, run_attempt=1, evidence_dir=tmp_path / 'evidence',
        transport=transport, recovery=record, automatic=damage == 'automatic')
    if damage:
        with pytest.raises(runner.RolloutError):
            runner.run_release(**args)
    else:
        assert runner.run_release(**args)['status'] == 'complete'
        assert intents[0]['run_id'] == 92 and intents[0]['run_attempt'] == 1
        assert intents[0]['shared_pool_release'] == record['shared_pool_release']
    assert 'prepare' not in events and len(events) <= 1
