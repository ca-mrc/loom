"""Refresh parent ordering and recovery never reopen uncertain child effects."""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_refresh_resources import (
    resources_request as resources_request,
)
from tests.ops.test_nebius_management_refresh_switch import API as SWITCH_API
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def install(resources_request, tmp_path):
    return install_case(resources_request, tmp_path)


def install_case(resources_request, tmp_path, *, history=None, installation_anchor=None, pool_baseline=None):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallRequest

    if history is None:
        predecessor = tmp_path / 'predecessor.json'
        predecessor.write_text('{"fixture":"completed predecessor qualified by entry"}')
        predecessor.chmod(0o600)
        history = {predecessor: hashlib.sha256(predecessor.read_bytes()).hexdigest()}
    original_anchor = installation_anchor or tmp_path / 'original-anchor'
    if installation_anchor is None:
        original_anchor.mkdir(mode=0o700)
    options = {} if pool_baseline is None else {'pool_baseline': pool_baseline}
    request = ManagementRefreshInstallRequest(resources_request,
        history, original_anchor, **options)

    class API:
        def __init__(self):
            self.stages = {}
            self.switch = SWITCH_API(resources_request.switch)
            self.switch.drained = self.switch.qualified = True
            self.pending = self.failed = None
            self.public_ready = True
            self.backup_available = True
            self.events = []

        def preflight(self, received):
            assert received == request
            self.events.append('preflight')

        @contextmanager
        def resources(self, received, phase):
            assert received == resources_request
            outer = self
            class Stage(PhaseAPI):
                def get_resource(self, document):
                    value = super().get_resource(document)
                    if value is not None and value['kind'] == 'Job':
                        value['status'] = ({'conditions': [{'type': 'Failed', 'status': 'True'}]}
                            if outer.failed == phase else {} if outer.pending == phase else
                            {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1})
                    return value
            self.events.append(phase)
            yield self.stages.setdefault(phase, Stage(resources_request.binding))

        @contextmanager
        def switch_api(self, received):
            assert received == resources_request.switch
            yield self.switch

        def verify_probe(self, received, phase, state_dir):
            assert received == request
            assert (state_dir / 'stage.json').is_file()
            self.events.append('proof:' + phase)
            record = json.loads((state_dir / 'stage.json').read_text())
            job = next(item for item in record['resources'].values() if item['desired']['kind'] == 'Job')
            mode = 'shared' if phase == 'shared-probe' else 'manager'
            return {'job_uid': job['uid'], 'pod_uid': job['uid'], 'probe': {
                'schema': 'loom.nebius-management-refresh-probe.v1', 'status': 'qualified', 'mode': mode,
                'revision': (resources_request.manager_revision if phase == 'manager-probe' else
                    resources_request.switch.render.after.installation.applications.shared.schema_revision
                    if phase == 'shared-probe' else resources_request.target_manager_revision), 'operations_checked': 0}}

        def verify_backup(self, received, state_dir):
            assert received == request
            self.events.append('proof:backup')
            if not self.backup_available:
                raise ValueError('private-object-error')
            record = json.loads((state_dir / 'stage.json').read_text())
            job, = record['resources'].values()
            return {'job_uid': job['uid'], 'key': 'fixture/backup.dump', 'sha256': 'a' * 64, 'bytes': 5}

        def verify_public(self, received, state_dir):
            assert received == request and (state_dir / 'switch/cutover.json').is_file()
            self.events.append('public')
            return self.public_ready

    return request, API(), tmp_path / 'state', tmp_path / 'anchor'


def run(install):
    from scripts.ops.nebius_management_refresh_install import refresh_management

    request, api, state, anchor = install
    return refresh_management(request=request, api=api, state_dir=state, anchor_dir=anchor)


def test_complete_refresh_binds_receipt_and_replay_only_reads(install):
    request, api, state, _ = install
    original = {path: path.read_bytes() for path in request.history}
    first = run(install)
    assert first['status'] == 'management_refreshed'
    assert first['operation_id'] == str(request.resources.switch.operation_id)
    assert api.events.index('proof:backup') < api.events.index('migration') < api.events.index('proof:post-migration-probe') < api.events.index('public')
    writes = {phase: list(stage.creates) for phase, stage in api.stages.items()}
    saved = {path: path.read_bytes() for path in state.rglob('*.json')}
    assert run(install) == first
    assert api.switch.calls == ['retire', 'activate']
    assert {phase: stage.creates for phase, stage in api.stages.items()} == writes
    assert {path: path.read_bytes() for path in saved} == saved
    assert {path: path.read_bytes() for path in original} == original


@pytest.mark.parametrize('pending', ['manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe'])
def test_pending_barrier_never_reaches_later_writes(install, pending):
    _, api, _, _ = install
    api.pending = pending
    report = run(install)
    assert report['status'] == 'pending' and report['phase'] == pending
    assert api.switch.calls == ['retire']
    assert api.switch.document['spec']['replicas'] == 0
    assert 'public' not in api.events
    assert list(api.stages)[-1] == pending
    api.pending = None
    assert run(install)['status'] == 'management_refreshed'
    assert api.switch.calls == ['retire', 'activate']


def test_manager_drain_precedes_database_jobs(install):
    _, api, _, _ = install
    api.switch.drained = False
    assert run(install)['phase'] == 'retire'
    assert list(api.stages) == ['config']
    api.switch.drained = True
    assert run(install)['status'] == 'management_refreshed'


def test_backup_object_failure_blocks_management_migration(install):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, api, _, _ = install
    api.backup_available = False
    with pytest.raises(ManagementRefreshInstallError) as error:
        run(install)
    assert error.value.stage == 'backup' and 'private' not in str(error.value)
    assert 'migration' not in api.stages and api.switch.calls == ['retire']


def test_migration_failure_leaves_manager_stopped_and_has_no_rollback(install):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, api, _, _ = install
    api.failed = 'migration'
    with pytest.raises(ManagementRefreshInstallError):
        run(install)
    assert api.switch.calls == ['retire'] and api.switch.document['spec']['replicas'] == 0
    assert 'post-migration-probe' not in api.stages


@pytest.mark.parametrize('child', ['config/stage.json', 'manager-probe/stage.json', 'migration/stage.json', 'switch/cutover.json'])
def test_lost_child_journal_cannot_reopen_a_completed_write(install, child):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, api, state, _ = install
    assert run(install)['status'] == 'management_refreshed'
    before = copy.deepcopy(api.events)
    (state / child).unlink()
    with pytest.raises(ManagementRefreshInstallError):
        run(install)
    assert api.events == before
    assert api.switch.calls == ['retire', 'activate']


def test_changed_predecessor_and_private_input_cannot_resume(install):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    request, api, _, _ = install
    api.pending = 'manager-probe'
    run(install)
    before = list(api.events)
    changed = replace(request, resources=replace(request.resources, manager_revision='0000'))
    with pytest.raises(ManagementRefreshInstallError):
        run((changed, *install[1:]))
    path, = request.history
    path.write_text('{"changed":true}')
    with pytest.raises(ManagementRefreshInstallError):
        run(install)
    assert api.events == before


def test_public_pending_resumes_without_retiring_new_manager(install):
    _, api, _, _ = install
    api.public_ready = False
    assert run(install)['phase'] == 'public'
    assert api.switch.calls == ['retire', 'activate']
    api.public_ready = True
    assert run(install)['status'] == 'management_refreshed'
    assert api.switch.calls == ['retire', 'activate']


@pytest.mark.parametrize('damage', ['probe_job', 'probe_revision', 'probe_shape', 'backup_job', 'backup_shape'])
def test_parent_cannot_accept_an_unbound_connected_receipt(install, damage):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, api, _, _ = install
    if damage.startswith('probe'):
        original = api.verify_probe
        def altered(*args):
            result = original(*args)
            if damage == 'probe_job':
                result['job_uid'] = str(uuid4())
            elif damage == 'probe_revision':
                result['probe']['revision'] = '0000'
            else:
                result['probe']['private'] = 'must-not-be-saved'
            return result
        api.verify_probe = altered
    else:
        original = api.verify_backup
        def altered(*args):
            result = original(*args)
            if damage == 'backup_job':
                result['job_uid'] = str(uuid4())
            else:
                result['bytes'] = True
            return result
        api.verify_backup = altered
    with pytest.raises(ManagementRefreshInstallError):
        run(install)
    assert 'migration' not in api.stages and api.switch.calls == ['retire']


def test_invalid_operation_is_rejected_before_private_markers_or_preflight(install):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    request, api, state, anchor = install
    request = replace(request, resources=replace(request.resources,
        switch=replace(request.resources.switch, operation_id=UUID(int=0))))
    api.preflight = lambda _request: api.events.append('preflight')
    with pytest.raises(ManagementRefreshInstallError):
        run((request, api, state, anchor))
    assert not api.events and not state.exists()


def activation(install):
    from scripts.ops.nebius_management_refresh_install import qualify_refresh_activation

    request, api, state, _ = install
    return qualify_refresh_activation(request=request, api=api, state_dir=state)


def test_activation_rechecks_all_phase_evidence_without_replaying_writes(install):
    _, api, state, _ = install
    api.public_ready = False
    assert run(install)['phase'] == 'public'
    before = {path: path.read_bytes() for path in state.rglob('*.json')}
    writes = {phase: list(stage.creates) for phase, stage in api.stages.items()}
    api.events.clear()
    assert activation(install) is True
    assert {phase: stage.creates for phase, stage in api.stages.items()} == writes
    assert api.switch.calls == ['retire', 'activate']
    assert {path: path.read_bytes() for path in before} == before
    assert api.events == ['config', 'manager-probe', 'proof:manager-probe', 'shared-probe', 'proof:shared-probe',
        'backup', 'proof:backup', 'migration', 'post-migration-probe', 'proof:post-migration-probe']


@pytest.mark.parametrize('damage', ['no_parent', 'no_activation', 'wrong_input', 'missing_phase', 'child_hash',
    'lost_child', 'unbound_report', 'changed_report', 'unavailable_object', 'migration_failed', 'migration_pending'])
def test_activation_cannot_bypass_incomplete_or_changed_barriers(install, damage):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError

    _, api, state, _ = install
    run(install)
    parent = state / 'refresh.json'
    record = json.loads(parent.read_text())
    if damage == 'no_parent':
        parent.unlink()
    elif damage == 'lost_child':
        (state / 'migration/stage.json').unlink()
    elif damage == 'unavailable_object':
        api.backup_available = False
    elif damage in {'migration_failed', 'migration_pending'}:
        setattr(api, 'failed' if damage == 'migration_failed' else 'pending', 'migration')
    elif damage == 'changed_report':
        original = api.verify_probe
        def changed(*args):
            value = original(*args)
            value['probe']['operations_checked'] = 1
            return value
        api.verify_probe = changed
    else:
        if damage == 'no_activation':
            record['activation_started'] = False
        elif damage == 'wrong_input':
            record['input_digest'] = 'sha256:' + '0' * 64
        elif damage == 'missing_phase':
            record['phases'].pop('post-migration-probe')
        elif damage == 'child_hash':
            record['phases']['migration']['sha256'] = '0' * 64
        else:
            record['phases']['manager-probe']['proof']['job_uid'] = str(uuid4())
        parent.write_text(json.dumps(record))
    writes = {phase: list(stage.creates) for phase, stage in api.stages.items()}
    if damage == 'migration_pending':
        assert activation(install) is False
    else:
        with pytest.raises(ManagementRefreshInstallError):
            activation(install)
    assert {phase: stage.creates for phase, stage in api.stages.items()} == writes
    assert api.switch.calls == ['retire', 'activate']
