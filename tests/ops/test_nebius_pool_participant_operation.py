"""Durable release input is frozen before dispatch, never recaptured on recovery.

These tests exercise local retention, not completed-handoff or live readiness
qualification. The protected parent must supply an actually qualified predecessor.
"""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_pool_participant_release import prepare_staging_pool_release
from scripts.ops.nebius_shared_pool_operation import CompletedSharedPoolOperation
from tests.ops.test_nebius_pool_participant_release import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_release import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_release import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_release import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_release import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom.nebius_platform_render import build_platform
from loom_service.environment_management.candidates import ProtectedPublication


def module():
    name = 'scripts.ops.nebius_pool_participant_operation'
    assert importlib.util.find_spec(name) is not None, 'durable normal-release operation is missing'
    return importlib.import_module(name)


@pytest.fixture
def operation_inputs(release_inputs, catalog_inputs, tmp_path):
    inputs = copy.deepcopy(release_inputs)
    previous = inputs.pop('previous')
    candidate = inputs['candidate']
    candidate.update(schema_version='loom.nebius-candidate.v1', run_id=812,
        source_archive_sha256='sha256:' + 'a' * 64, registry_prefix='cr.eu-north1.nebius.cloud/test')
    predecessor_dir = tmp_path / 'predecessor'
    private_state._private_directory(predecessor_dir)
    history = predecessor_dir / 'completed.json'
    private_state._atomic_json(history, {'test_boundary': 'qualified predecessor supplied by parent'})
    inputs.update(predecessor=CompletedSharedPoolOperation(previous, predecessor_dir,
        {history: hashlib.sha256(history.read_bytes()).hexdigest()}),
        source=PreparedDevelopmentSource(source_sha=candidate['candidate_sha'],
            source_archive_sha256=candidate['source_archive_sha256']),
        publication=ProtectedPublication(candidate_id=uuid4(), source_sha=candidate['candidate_sha'],
            run_id=812, run_attempt=2, artifact_id=923, artifact_sha256='sha256:' + 'b' * 64, pull_request=2425),
        manager=copy.deepcopy(catalog_inputs['manager']), catalog=copy.deepcopy(catalog_inputs['catalog']),
        state_root=tmp_path / 'releases', anchor_dir=tmp_path / 'anchors')
    inputs['rendered_files'] = build_platform(inputs['config'], candidate,
        inputs['runtime_profile'].model_dump(mode='json'), previous.registration.spec.profiles.image_admission_keyring,
        repo_root=inputs['repo_root'])
    return inputs


def load(options, reference):
    return module().load_participant_rollout(reference=reference,
        state_root=options['state_root'], anchor_dir=options['anchor_dir'])


def test_recovery_loads_original_release_without_rendering_or_recapturing_live_inputs(operation_inputs, monkeypatch):
    inputs = operation_inputs
    original = copy.deepcopy(inputs)
    prepared = module().retain_participant_rollout(**inputs)
    assert inputs == original
    assert prepared.reference.operation_id == inputs['operation_id']
    assert prepared.reference.publication == inputs['publication']
    assert prepared.guard_owner == 'rollout-' + inputs['operation_id'].hex
    assert prepared.release.refresh.before == inputs['predecessor'].migration.registration.spec
    assert prepared.release.migration.registration.candidate == inputs['candidate']
    assert prepared.state_dir == inputs['state_root'] / str(inputs['operation_id'])
    assert prepared.inputs.manager == inputs['manager'] and prepared.inputs.catalog == inputs['catalog']

    def no_reconstruction(*args, **kwargs):
        pytest.fail('recovery must load the original, not run preparation again')

    monkeypatch.setattr(module(), 'prepare_staging_pool_release', no_reconstruction)
    inputs['candidate']['candidate_sha'] = 'e' * 40
    inputs['current'][0]['metadata']['uid'] = str(uuid4())
    inputs['manager']['spec']['replicas'] = 0
    loaded = load(inputs, prepared.reference)
    assert loaded == prepared
    assert loaded.inputs.candidate['candidate_sha'] == 'd' * 40
    assert loaded.inputs.manager['spec']['replicas'] == 1
    for path in prepared.state_dir.rglob('*'):
        assert path.stat().st_mode & 0o077 == 0


def test_identical_preparation_is_idempotent_but_changed_candidate_render_is_not(operation_inputs):
    inputs = operation_inputs
    first = module().retain_participant_rollout(**inputs)
    files = {path: path.read_bytes() for path in inputs['state_root'].rglob('*') if path.is_file()}
    assert module().retain_participant_rollout(**inputs) == first
    web = next(row for rows in inputs['rendered_files'].values() for row in rows
        if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-web')
    web['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = '334m'
    with pytest.raises(ValueError, match='participant rollout'):
        module().retain_participant_rollout(**inputs)
    assert {path: path.read_bytes() for path in files} == files
    assert load(inputs, first.reference) == first


@pytest.mark.parametrize('damage', ['inputs-missing', 'inputs-altered', 'anchor-missing', 'anchor-altered',
    'history-missing', 'history-altered', 'operation', 'digest', 'publication', 'public-inputs', 'symlink-inputs'])
def test_recovery_refuses_lost_or_changed_evidence_without_repairing_it(operation_inputs, damage, tmp_path):
    inputs = operation_inputs
    retained = module().retain_participant_rollout(**inputs)
    reference = retained.reference
    path = retained.state_dir / 'inputs.json'
    anchor, = inputs['anchor_dir'].glob('*.json')
    history, = inputs['predecessor'].history
    if damage.endswith('missing'):
        {'inputs-missing': path, 'anchor-missing': anchor, 'history-missing': history}[damage].unlink()
    elif damage.endswith('altered'):
        target = {'inputs-altered': path, 'anchor-altered': anchor, 'history-altered': history}[damage]
        private_state._atomic_json(target, {'foreign': True})
    elif damage == 'operation':
        reference = reference.model_copy(update={'operation_id': uuid4()})
    elif damage == 'digest':
        reference = reference.model_copy(update={'inputs_sha256': 'e' * 64})
    elif damage == 'publication':
        reference = reference.model_copy(update={'publication': reference.publication.model_copy(update={'run_attempt': 3})})
    elif damage == 'public-inputs':
        path.chmod(0o644)
    else:
        other = tmp_path / 'other-inputs.json'
        path.rename(other)
        path.symlink_to(other)
    before = {item: item.lstat() for root in (inputs['state_root'], inputs['anchor_dir']) for item in root.rglob('*')}
    with pytest.raises(ValueError, match='participant rollout'):
        load(inputs, reference)
    assert {item: item.lstat() for item in before} == before
    if damage in {'inputs-missing', 'inputs-altered', 'anchor-altered'}:
        with pytest.raises(ValueError, match='participant rollout'):
            module().retain_participant_rollout(**inputs)


@pytest.mark.parametrize('damage', ['source-sha', 'source-archive', 'publication-run', 'publication-sha',
    'history', 'catalog', 'predecessor', 'manager'])
def test_unqualified_inputs_fail_before_retention(operation_inputs, damage):
    inputs = operation_inputs
    if damage == 'source-sha':
        inputs['source'] = inputs['source'].model_copy(update={'source_sha': 'e' * 40})
    elif damage == 'source-archive':
        inputs['source'] = inputs['source'].model_copy(update={'source_archive_sha256': 'sha256:' + 'e' * 64})
    elif damage == 'publication-run':
        inputs['publication'] = inputs['publication'].model_copy(update={'run_id': 813})
    elif damage == 'publication-sha':
        inputs['publication'] = inputs['publication'].model_copy(update={'source_sha': 'e' * 40})
    elif damage == 'history':
        inputs['predecessor'] = replace(inputs['predecessor'], history={})
    elif damage == 'catalog':
        inputs['catalog']['data']['profiles.json'] = '{}'
    elif damage == 'manager':
        inputs['manager']['spec']['replicas'] = 0
    else:
        inputs['predecessor'] = replace(inputs['predecessor'], migration=replace(inputs['predecessor'].migration,
            guards=()))
    with pytest.raises(ValueError, match='participant rollout'):
        module().retain_participant_rollout(**inputs)
    assert not inputs['state_root'].exists() and not inputs['anchor_dir'].exists()


def test_original_input_digest_binds_the_projected_release_and_publication(operation_inputs):
    inputs = operation_inputs
    retained = module().retain_participant_rollout(**inputs)
    path = retained.state_dir / 'inputs.json'
    before = json.loads(path.read_bytes())
    assert before['publication']['run_id'] == 812 and before['publication']['run_attempt'] == 2
    assert before['source']['source_archive_sha256'] == 'sha256:' + 'a' * 64
    assert before['predecessor_operation_id'] == str(inputs['predecessor'].migration.registration.spec.operation_id)
    assert retained.release == prepare_staging_pool_release(**{key: value for key, value in inputs.items()
        if key in {'current', 'config', 'candidate', 'runtime_profile', 'operation_id', 'management_origin', 'repo_root', 'rendered_files'}},
        previous=inputs['predecessor'].migration)
    # Changing both the caller's digest and stored data still cannot change the
    # independent original anchor or turn this into another release operation.
    before['release']['files']['70-public.yaml'] = []
    private_state._atomic_json(path, before)
    reference = retained.reference.model_copy(update={'inputs_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    with pytest.raises(ValueError, match='participant rollout'):
        load(inputs, reference)
