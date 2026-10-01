"""Periodic rollout checks reuse publications and never repeat failed mutations."""
import json
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as module

SHA = 'a' * 40
CONFIG = {'cluster_id': 'cluster', 'namespace': 'platform', 'execution_namespace': 'execution'}


def test_latest_full_publication_skips_harness_and_paginates(monkeypatch):
    visited = []
    def api(path):
        if 'page=2' in path:
            return {'workflow_runs': [{'id': 1}]}
        return {'workflow_runs': [{'id': n} for n in range(101, 1, -1)]}
    def select(run):
        visited.append(run)
        return {'status': 'ready' if run == '1' else 'skipped_no_platform_candidate', 'run_id': run}
    monkeypatch.setattr(module, 'github', api)
    monkeypatch.setattr(module, 'select_publication', select)
    assert module.latest_publication() == {'status': 'ready', 'run_id': '1'}
    assert visited[0] == '101' and visited[-1] == '1'


def intent(state, *, sha=SHA, config=CONFIG):
    return {'id': 9, 'sha': sha, 'environment': 'nebius-integration',
            'payload': {'schema_version': 'loom.nebius-deployment.v1', 'mode': 'apply',
                        'candidate_sha': sha, **config}, '_state': state}


@pytest.mark.parametrize('state,expected', [('failure', 'blocked_recovery'), ('error', 'blocked_recovery'),
                                           ('success', 'skipped_already_deployed')])
def test_latest_own_deployment_decides_retry(monkeypatch, state, expected):
    row = intent(state)
    monkeypatch.setattr(module, 'github', lambda path: [row] if path.startswith('deployments?') else [{'state': state}])
    assert module.deployment_decision(CONFIG, SHA, SHA)['status'] == expected


def test_partial_config_is_not_success_and_failed_attempts_block_newer_candidate(monkeypatch):
    row = intent('failure')
    monkeypatch.setattr(module, 'github', lambda path: [row] if path.startswith('deployments?') else [{'state': 'failure'}])
    assert module.deployment_decision(CONFIG, SHA, 'b' * 40)['status'] == 'blocked_recovery'


def test_busy_skip_does_not_hide_previous_failure(monkeypatch):
    rows = [{**intent('inactive'), 'id': 10}, intent('failure')]
    monkeypatch.setattr(module, 'github', lambda path: rows if path.startswith('deployments?') else [
        {'state': 'inactive' if '/10/' in path else 'failure'}])
    assert module.deployment_decision(CONFIG, SHA, SHA)['status'] == 'blocked_recovery'


def test_foreign_environment_record_cannot_prove_success(monkeypatch):
    row = intent('success', config={**CONFIG, 'cluster_id': 'foreign'})
    monkeypatch.setattr(module, 'github', lambda path: [row] if path.startswith('deployments?') else pytest.fail('foreign status'))
    assert module.deployment_decision(CONFIG, SHA, SHA)['status'] == 'ready'


def test_success_requires_current_and_selected_candidate(monkeypatch):
    row = intent('success')
    monkeypatch.setattr(module, 'github', lambda path: [row] if path.startswith('deployments?') else [{'state': 'success'}])
    assert module.deployment_decision(CONFIG, 'b' * 40, SHA)['status'] == 'ready'


def test_empty_status_after_lost_runner_is_blocked(monkeypatch):
    row = intent('pending')
    monkeypatch.setattr(module, 'github', lambda path: [row] if path.startswith('deployments?') else [])
    assert module.deployment_decision(CONFIG, SHA, SHA)['status'] == 'blocked_recovery'


def check_inputs(monkeypatch, *, guard=False, active=0):
    data = {'environment.json': json.dumps(CONFIG), 'profile.json': json.dumps({'candidate_sha': SHA})}
    snapshot = {'locked': guard, 'active': {'trials': active, 'executions': 0, 'builds': 0, 'build_cleanup': 0}}
    kube = SimpleNamespace(get=lambda *a: {'data': data})
    monkeypatch.setattr(module, 'Kubectl', lambda path: kube)
    monkeypatch.setattr(module, 'idle_snapshot', lambda *a: snapshot)
    monkeypatch.setattr(module, 'deployment_decision', lambda *a: {'status': 'ready'})
    return SimpleNamespace(kubeconfig='unused', namespace='platform', expected_cluster_id='cluster', candidate=SHA,
                           automatic=True, github=True)


@pytest.mark.parametrize('guard,active,expected', [(False, 1, 'skipped_busy'), (False, 0, 'ready'),
                                                 (True, 0, 'blocked_recovery')])
def test_check_is_read_only_and_classifies_live_state(monkeypatch, guard, active, expected):
    args = check_inputs(monkeypatch, guard=guard, active=active)
    monkeypatch.setattr(module, 'deploy', lambda *a, **kw: pytest.fail('check must not deploy'))
    assert module.check_rollout(args)['status'] == expected


def test_wrong_cluster_rejected_before_reading_guard(monkeypatch):
    args = check_inputs(monkeypatch)
    args.expected_cluster_id = 'foreign'
    monkeypatch.setattr(module, 'idle_snapshot', lambda *a: pytest.fail('wrong cluster'))
    with pytest.raises(module.DeploymentError, match='binding'):
        module.check_rollout(args)


def test_auto_failure_policy_precedes_busy_retry(monkeypatch):
    args = check_inputs(monkeypatch, active=1)
    monkeypatch.setattr(module, 'deployment_decision', lambda *a: {'status': 'blocked_recovery'})
    assert module.check_rollout(args)['status'] == 'blocked_recovery'


def test_read_only_snapshot_uses_database_without_control_plane():
    calls = []
    def run(*args):
        calls.append(args)
        return json.dumps({'locked': False, 'active': {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0}})
    assert module.idle_snapshot(SimpleNamespace(run=run), 'platform')['locked'] is False
    argv = calls[0]
    assert 'statefulset/loom-postgres' in argv and 'deployment/loom-control-plane' not in argv
    assert 'BEGIN READ ONLY' in argv[-1] and 'ROLLBACK' in argv[-1]
    assert 'INSERT' not in argv[-1] and 'DELETE' not in argv[-1]


def test_automatic_run_rechecks_decision_before_any_mutation(monkeypatch):
    decision = {'status': 'skipped_already_deployed', 'candidate_sha': SHA}
    monkeypatch.setattr(module, 'check_rollout', lambda args: decision)
    monkeypatch.setattr(module, 'Kubectl', lambda *a: pytest.fail('no render, backup or deployment on repeat'))
    assert module.rollout(SimpleNamespace(automatic=True)) == decision


def test_failed_check_cli_reports_failed_without_leaking_exception(monkeypatch, tmp_path, capsys):
    import sys
    monkeypatch.setenv('GITHUB_STEP_SUMMARY', str(tmp_path / 'summary'))
    monkeypatch.setattr(sys, 'argv', ['rollout', 'check', '--candidate', SHA, '--kubeconfig', 'unused',
                                     '--expected-cluster-id', 'cluster', '--github', '--automatic'])
    def fail(args):
        raise module.DeploymentError('private database connection detail')
    monkeypatch.setattr(module, 'check_rollout', fail)
    assert module.main() == 1
    output = capsys.readouterr()
    assert '"outcome": "failed"' in output.out
    assert 'private database' not in output.out + output.err + (tmp_path / 'summary').read_text()


def test_candidate_manifest_archive_is_pinned_and_cleaned(monkeypatch):
    import io
    import tarfile
    blob = io.BytesIO()
    with tarfile.open(fileobj=blob, mode='w') as archive:
        row = tarfile.TarInfo('deploy/k8s/candidate.yaml')
        payload = b'candidate manifest'
        row.size = len(payload)
        archive.addfile(row, io.BytesIO(payload))
    def command(argv, **kwargs):
        assert argv == ['git', 'archive', SHA, 'deploy/k8s', 'src/loom', 'scripts/ops/render_nebius_platform.py']
        return SimpleNamespace(returncode=0, stdout=blob.getvalue())
    monkeypatch.setattr(module.subprocess, 'run', command)
    with module.candidate_manifests(SHA) as source:
        assert (source / 'deploy/k8s/candidate.yaml').read_bytes() == b'candidate manifest'
    assert not source.exists()


@pytest.mark.parametrize('status', ['ready', 'skipped_no_platform_candidate'])
def test_publication_trigger_coalesces_only_full_platform_candidates(monkeypatch, status):
    import sys
    selected = []
    monkeypatch.setattr(sys, 'argv', ['rollout', 'select', '--trigger-run-id', '42'])
    monkeypatch.setattr(module, 'select_publication', lambda run_id: {'status': status})
    monkeypatch.setattr(module, 'latest_publication', lambda: selected.append(True) or {'status': 'ready'})
    assert module.main() == 0
    assert selected == ([True] if status == 'ready' else [])
