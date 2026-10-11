"""Data-only host dispatch cannot expose private evidence or choose authority."""
from __future__ import annotations

import copy
import importlib
import json
from uuid import uuid4

import pytest
from scripts.ops import nebius_pool_participant_entry as entry
from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom.nebius_kubernetes import NebiusKubernetesConnection


def module():
    name = 'scripts.ops.nebius_pool_participant_dispatch'
    assert importlib.util.find_spec(name) is not None, 'fixed host release dispatch is not connected'
    return importlib.import_module(name)


@pytest.fixture
def selected(operation_inputs, tmp_path):
    retained = retain_participant_rollout(**operation_inputs)
    config = retained.inputs.config
    host = module().ParticipantReleaseHost(
        shared_operation=tmp_path / 'original-shared-operation.json', shared_operation_sha256='c' * 64,
        state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir,
        repo_root=operation_inputs['repo_root'], executable=tmp_path / 'kubectl',
        operator_connection=NebiusKubernetesConnection(endpoint=config['kubernetes_api_server'],
            ca_file=tmp_path / 'current-ca.pem', credentials_file=tmp_path / 'current-operator.json'))
    binding = {'reference': retained.reference.model_dump(mode='json'),
        'candidate_sha': retained.inputs.candidate['candidate_sha'],
        'previous_candidate_sha': operation_inputs['predecessor'].migration.registration.candidate['candidate_sha'],
        'guard_owner': 'rollout-' + operation_inputs['operation_id'].hex,
        **{key: config[key] for key in ('cluster_id', 'namespace', 'execution_namespace')},
        'environment_url': 'https://' + config['public_host']}
    return retained, host, binding


def request(retained, action):
    value = {'schema_version': 'loom.nebius-participant-command.v1', 'action': action}
    if action == 'prepare':
        value.update(operation_id=str(retained.reference.operation_id),
            publication=retained.reference.publication.model_dump(mode='json'))
    else:
        value['reference'] = retained.reference.model_dump(mode='json')
        if action == 'execute':
            value['automatic'] = False
    return value


def recovery(binding):
    return {'schema_version': 'loom.nebius-deployment.v1', 'mode': 'apply', 'status': 'failed',
        'shared_pool_release': binding['reference'], **{key: binding[key] for key in (
            'candidate_sha', 'previous_candidate_sha', 'guard_owner', 'cluster_id', 'namespace', 'execution_namespace')}}


@pytest.mark.parametrize('action', ['prepare', 'execute', 'recover'])
def test_dispatch_uses_installed_selection_and_exports_only_exact_binding(selected, monkeypatch, action):
    retained, host, binding = selected
    incoming = request(retained, action)
    if action == 'recover':
        incoming['recovery'] = recovery(binding)
    calls = []

    def prepare(**options):
        assert action == 'prepare' and options['publication'] == retained.reference.publication
        assert options['operation_id'] == retained.reference.operation_id and options['repo_root'] == host.repo_root
        calls.append(options)
        return retained

    def execute(**options):
        assert action != 'prepare' and options['reference'] == retained.reference
        assert options['automatic'] is False
        assert options['recovery'] == (incoming['recovery'] if action == 'recover' else None)
        calls.append(options)
        return {**recovery(binding), 'status': 'complete', 'private_diagnostic': 'private-evidence-not-for-runner',
            'rendered': {'Secret': 'private-value'}, 'host_path': str(host.state_root)}

    monkeypatch.setattr(entry, 'prepare_retained_participant_release', prepare)
    monkeypatch.setattr(entry, 'execute_retained_participant_release', execute)
    result = module().dispatch_participant_release(json.dumps(incoming).encode(), action=action, host=host)
    assert result == {'schema_version': 'loom.nebius-participant-report.v1',
        'status': 'prepared' if action == 'prepare' else 'complete', 'release': binding}
    assert len(calls) == 1
    for name in ('shared_operation', 'shared_operation_sha256', 'state_root', 'anchor_dir', 'operator_connection', 'executable'):
        assert calls[0][name] == getattr(host, name)
    assert 'private-' not in json.dumps(result) and str(host.state_root) not in json.dumps(result)


@pytest.mark.parametrize('damage', ['path', 'source', 'credentials', 'extra', 'wrong-action', 'duplicate',
    'nil-operation', 'noncanonical-operation', 'bool-run', 'oversize', 'unknown-action', 'automatic-type',
    'automatic-recovery', 'foreign-recovery', 'extra-recovery'])
def test_invalid_public_command_never_reaches_host_intake(selected, monkeypatch, damage):
    retained, host, binding = selected
    action = 'recover' if damage.endswith('recovery') else 'execute' if damage == 'automatic-type' else 'prepare'
    incoming = request(retained, action)
    if action == 'recover':
        incoming['recovery'] = recovery(binding)
    if damage in {'path', 'source', 'credentials', 'extra'}:
        incoming[damage] = '/private/runner-chosen-authority'
    elif damage == 'wrong-action':
        incoming['action'] = 'recover'
    elif damage == 'nil-operation':
        incoming['operation_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'noncanonical-operation':
        incoming['operation_id'] = str(uuid4()).upper()
    elif damage == 'bool-run':
        incoming['publication']['run_id'] = True
    elif damage == 'unknown-action':
        incoming['action'] = action = 'install'
    elif damage == 'automatic-type':
        incoming['automatic'] = 1
    elif damage == 'automatic-recovery':
        incoming['automatic'] = True
    elif damage == 'foreign-recovery':
        incoming['recovery']['shared_pool_release'] = copy.deepcopy(binding['reference'])
        incoming['recovery']['shared_pool_release']['operation_id'] = str(uuid4())
    elif damage == 'extra-recovery':
        incoming['recovery']['host_path'] = '/private/runner-chosen-authority'
    raw = json.dumps(incoming).encode()
    if damage == 'duplicate':
        raw = raw[:-1] + b',"action":"prepare"}'
    elif damage == 'oversize':
        raw += b' ' * 32768

    def unexpected(**options):
        pytest.fail('unqualified data reached private host intake')

    monkeypatch.setattr(entry, 'prepare_retained_participant_release', unexpected)
    monkeypatch.setattr(entry, 'execute_retained_participant_release', unexpected)
    result = module().dispatch_participant_release(raw, action=action, host=host)
    assert result == {'schema_version': 'loom.nebius-participant-report.v1', 'status': 'blocked', 'stage': 'request'}


@pytest.mark.parametrize('failure', ['exception', 'unknown-status', 'foreign-binding', 'bare-success', 'skip'])
def test_dispatch_preserves_uncertain_outcome_without_exporting_or_retrying(selected, monkeypatch, failure):
    retained, host, binding = selected
    calls = []

    def execute(**options):
        calls.append(options)
        if failure == 'exception':
            raise ValueError('private-credential-in-error')
        if failure == 'bare-success':
            return {'status': 'complete'}
        if failure == 'skip':
            return {'status': 'skipped_busy', 'candidate_sha': binding['candidate_sha'], 'private_detail': 'private-value'}
        result = {**recovery(binding), 'status': 'complete'}
        if failure == 'unknown-status':
            result['status'] = 'some_new_success'
        else:
            result['namespace'] = 'foreign'
        return result

    monkeypatch.setattr(entry, 'execute_retained_participant_release', execute)
    result = module().dispatch_participant_release(json.dumps(request(retained, 'execute')).encode(), action='execute', host=host)
    if failure == 'skip':
        assert result == {'schema_version': 'loom.nebius-participant-report.v1', 'status': 'skipped_busy', 'release': binding}
    else:
        assert result == {'schema_version': 'loom.nebius-participant-report.v1', 'status': 'blocked', 'stage': 'execution', 'release': binding}
    assert len(calls) == 1 and 'private-' not in json.dumps(result)
