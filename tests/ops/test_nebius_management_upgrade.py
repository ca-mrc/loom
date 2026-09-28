"""Fixed upgrade composes real stages and cutover without replaying bootstrap."""
from __future__ import annotations

import copy
import json
import shutil
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_install import run as install
from tests.ops.test_nebius_management_supplied import material as material
from tests.ops.test_nebius_management_switch import SwitchAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class UpgradeAPI:
    """Only external API effects/probes are doubled; stages and journals are real."""

    def __init__(self, installed, setup):
        from scripts.ops.nebius_management_switch import ManagementSwitchRequest

        from loom_service.environment_management.deployment import render_management

        self.store = installed.store
        original = self.store.resources['Deployment:loom-service']
        original['metadata']['generation'] = 1
        target = next(doc for doc in render_management(setup.deployment, candidate=setup.candidate,
            profile=setup.profile, repo_root=setup.repo_root).files['40-services.yaml'] if doc['kind'] == 'Deployment')
        self.switch = SwitchAPI(ManagementSwitchRequest(setup, original), target)
        self.authority = False
        self.public_ready = False
        self.preflight_failure = False
        self.events = []

    def preflight(self, request):
        self.events.append('preflight')
        if self.preflight_failure:
            raise RuntimeError('private-prerequisite-failure')

    @contextmanager
    def resources(self, request, phase):
        self.events.append(phase)
        yield self.store

    @contextmanager
    def switch_api(self, request):
        assert request.original['metadata']['uid'] == self.switch.document['metadata']['uid']
        yield self.switch

    def qualify_authority(self, request, state_dir):
        self.events.append('authority')
        return self.authority

    def verify_public(self, request, state_dir):
        self.events.append('public')
        if not self.public_ready:
            raise RuntimeError('private-public-failure')

    def complete(self, prefix):
        for row in self.store.resources.values():
            if row['metadata']['name'].startswith(prefix):
                if row['kind'] == 'ValidatingAdmissionPolicy':
                    row['metadata']['generation'] = 1
                    row['status'] = {'observedGeneration': 1, 'typeChecking': {'expressionWarnings': []}}
                elif row['kind'] == 'Job':
                    row['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}


@pytest.fixture
def upgrade(installation, application_management_inputs, application_material, tmp_path):
    from scripts.ops.nebius_application_setup import ApplicationSetupRequest
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeRequest
    from tests.unit.test_nebius_management_render import ROOT

    from loom_service.environment_management.deployment import ManagementDeployment

    original, installed = installation
    original_config = original.deployment.model_dump(mode='json')
    original_config['installation']['publications'] = [{'candidate_id': str(uuid4()),
        'source_sha': 'e' * 40, 'run_id': 10, 'run_attempt': 1, 'artifact_id': 20,
        'artifact_sha256': 'sha256:' + 'c' * 64, 'pull_request': 30}]
    original = replace(original, deployment=ManagementDeployment.model_validate(original_config))
    installation = original, installed
    install(installation, tmp_path)
    for kind in ('StatefulSet', 'Job', 'Job', 'Deployment'):
        installed.complete(kind)
        result = install(installation, tmp_path)
    assert result['status'] == 'management_installed'
    raw = original.deployment.model_dump(mode='json')
    raw['installation']['provider_runtime'] = None
    raw['installation']['applications'] = copy.deepcopy(application_management_inputs[0]['installation']['applications'])
    setup = ApplicationSetupRequest(ManagementDeployment.model_validate(raw), original.candidate, original.profile,
        installed.store.binding, str(uuid4()), ROOT, application_material)
    request = ManagementUpgradeRequest(original=original, setup=setup,
        original_state=tmp_path / 'installation', original_anchor=tmp_path / 'independent')
    return request, UpgradeAPI(installed, setup)


def run(upgrade, tmp_path):
    from scripts.ops.nebius_management_upgrade import upgrade_management

    return upgrade_management(request=upgrade[0], api=upgrade[1], state_dir=tmp_path / 'upgrade',
        anchor_dir=tmp_path / 'upgrade-anchor')


def to_retirement(upgrade, tmp_path):
    assert run(upgrade, tmp_path)['phase'] == 'admission'
    api = upgrade[1]
    assert api.events.count('preflight') == 1
    api.complete('loom-applications-')
    # Authority policy names are derived from the installation, not Job prefix.
    api.complete(upgrade[0].setup.deployment.installation.applications.authority.name)
    assert run(upgrade, tmp_path)['phase'] == 'authority'
    api.authority = True
    assert run(upgrade, tmp_path)['phase'] == 'database'
    api.complete('loom-applications-setup-')
    assert run(upgrade, tmp_path)['phase'] == 'retirement'
    api.complete(upgrade[0].setup.deployment.installation.applications.authority.name)
    assert run(upgrade, tmp_path)['phase'] == 'retire'


def test_upgrade_waits_for_every_barrier_and_never_retires_new_runtime_on_replay(upgrade, tmp_path):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    request, api = upgrade
    original_files = {path: path.read_bytes() for root in (request.original_state, request.original_anchor)
        for path in root.rglob('*.json')}
    original_resources = copy.deepcopy(api.store.resources)
    to_retirement(upgrade, tmp_path)
    assert api.switch.calls == ['retire']
    assert not any(row['metadata']['name'].startswith('loom-management-migrate-') and key not in original_resources
        for key, row in api.store.resources.items())
    assert run(upgrade, tmp_path)['phase'] == 'retire'
    api.switch.processes = False
    assert run(upgrade, tmp_path)['phase'] == 'migration'
    assert api.switch.calls == ['retire']
    api.complete('loom-management-migrate-')
    with pytest.raises(ManagementUpgradeError):
        run(upgrade, tmp_path)
    assert api.switch.calls == ['retire', 'activate']
    api.public_ready = True
    result = run(upgrade, tmp_path)
    assert result['status'] == 'management_upgraded'
    before = len(api.store.creates)
    assert run(upgrade, tmp_path) == result
    assert len(api.store.creates) == before and api.switch.calls == ['retire', 'activate']
    assert api.switch.document['metadata']['uid'] == original_resources['Deployment:loom-service']['metadata']['uid']
    for key, value in original_resources.items():
        assert api.store.resources[key] == value
    assert all(path.read_bytes() == value for path, value in original_files.items())
    assert 'private-' not in json.dumps(result)


@pytest.mark.parametrize('damage', ['unfinished', 'input', 'anchor', 'service', 'namespace', 'scope'])
def test_upgrade_rejects_unqualified_bootstrap_before_any_writes(upgrade, tmp_path, damage):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    request, api = upgrade
    if damage == 'unfinished':
        path = request.original_state / 'installation.json'
        value = json.loads(path.read_text())
        value['phases'].pop('public')
        path.write_text(json.dumps(value))
    elif damage == 'input':
        request.original.material['loom-management-publications']['token'] = 'different-private-input'
    elif damage == 'anchor':
        shutil.rmtree(request.original_anchor)
    elif damage == 'service':
        (request.original_state / 'service' / 'stage.json').unlink()
    elif damage == 'namespace':
        request = replace(request, setup=replace(request.setup,
            binding=replace(request.setup.binding, namespace_uid=str(uuid4()))))
    else:
        request = replace(request, setup=replace(request.setup,
            deployment=request.setup.deployment.model_copy(update={'postgres_storage_gi': 20})))
    before = len(api.store.creates)
    with pytest.raises(ManagementUpgradeError):
        run((request, api), tmp_path)
    assert len(api.store.creates) == before and not api.switch.calls
    assert not (tmp_path / 'upgrade').exists()


@pytest.mark.parametrize('lost', ['upgrade', 'upgrade-anchor', 'upgrade/config', 'upgrade/admission'])
def test_lost_upgrade_state_does_not_reopen_creates(upgrade, tmp_path, lost):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    run(upgrade, tmp_path)
    api = upgrade[1]
    before = len(api.store.creates)
    shutil.rmtree(tmp_path / lost)
    with pytest.raises(ManagementUpgradeError):
        run(upgrade, tmp_path)
    assert len(api.store.creates) == before and not api.switch.calls


def test_failed_preflight_keeps_running_legacy_and_creates_nothing(upgrade, tmp_path):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    api = upgrade[1]
    api.preflight_failure = True
    before = len(api.store.creates)
    with pytest.raises(ManagementUpgradeError) as error:
        run(upgrade, tmp_path)
    assert 'private-' not in str(error.value)
    assert len(api.store.creates) == before and not api.switch.calls
    assert not (tmp_path / 'upgrade').exists()


@pytest.mark.parametrize('change', ['append', 'remove', 'rewrite'])
def test_upgrade_allows_new_protected_publication_without_rewriting_retained_catalog(upgrade, tmp_path, change):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    from loom_service.environment_management.deployment import ManagementDeployment

    request, api = upgrade
    raw = request.setup.deployment.model_dump(mode='json')
    publications = raw['installation']['publications']
    assert publications
    if change == 'append':
        publications.append({**publications[0], 'candidate_id': str(uuid4()), 'source_sha': 'f' * 40})
    elif change == 'remove':
        publications.clear()
    else:
        publications[0]['source_sha'] = 'f' * 40
    changed = replace(request, setup=replace(request.setup, deployment=ManagementDeployment.model_validate(raw)))
    before = len(api.store.creates)
    if change == 'append':
        assert run((changed, api), tmp_path)['phase'] == 'admission'
        assert len(api.store.creates) > before
    else:
        with pytest.raises(ManagementUpgradeError):
            run((changed, api), tmp_path)
        assert len(api.store.creates) == before


def test_failed_management_migration_keeps_old_worker_stopped_and_never_activates(upgrade, tmp_path):
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeError

    to_retirement(upgrade, tmp_path)
    api = upgrade[1]
    api.switch.processes = False
    assert run(upgrade, tmp_path)['phase'] == 'migration'
    latest = [doc for doc in api.store.resources.values() if doc['kind'] == 'Job'
        and doc['metadata']['name'].startswith('loom-management-migrate-')][-1]
    latest['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}
    before = len(api.store.creates)
    for _ in range(2):
        with pytest.raises(ManagementUpgradeError):
            run(upgrade, tmp_path)
    assert len(api.store.creates) == before and api.switch.calls == ['retire']
    assert api.switch.document['spec']['replicas'] == 0
