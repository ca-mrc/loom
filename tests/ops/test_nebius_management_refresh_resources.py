"""Refresh creates only fixed operation-bound Jobs/config, with retained credentials."""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def resources_request(refresh_request):
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh_resources import ManagementRefreshResourcesRequest
    from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest

    binding = ManagementBinding(str(refresh_request.after.installation_id), refresh_request.after.namespace,
        str(uuid4()), str(uuid4()))
    return ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(refresh_request, uuid4()),
        binding, str(uuid4()), '0166', '0168')


def documents(request, phase):
    from scripts.ops.nebius_management_refresh_resources import refresh_documents

    return list(refresh_documents(request, phase).values())


@pytest.mark.parametrize('phase,mode,revision', [
    ('manager-probe', 'manager', '0166'), ('shared-probe', 'shared', '0168'),
    ('post-migration-probe', 'manager', '0168'),
])
def test_fixed_probe_jobs_have_no_write_or_manager_runtime_identity(resources_request, phase, mode, revision):
    from loom.nebius_management_refresh_probe import RefreshProbeSettings

    request = resources_request
    raw = request.switch.render.after.model_dump(mode='json')
    raw['installation']['applications']['shared']['schema_revision'] = '0168'
    for release in raw['installation']['applications']['releases']:
        release['schema_revision'] = '0168'
    # A new release identity, not a changed historical release.
    raw['installation']['applications']['releases'][0]['release_id'] = str(uuid4())
    request = replace(request, switch=replace(request.switch, render=replace(request.switch.render,
        after=type(request.switch.render.after).model_validate(raw))))
    before = copy.deepcopy(request)
    cm, job = documents(request, phase)
    settings = RefreshProbeSettings.model_validate_json(cm['data']['probe.json'])
    namespace = request.binding.namespace if mode == 'manager' else settings.shared.platform_namespace
    assert settings.mode == mode and settings.expected_revision == revision
    assert cm['immutable'] is True and cm['metadata']['namespace'] == namespace
    assert job['metadata']['namespace'] == namespace and job['kind'] == 'Job'
    assert request.switch.operation_id.hex in job['metadata']['name']
    assert 'ttlSecondsAfterFinished' not in job['spec']
    assert job['spec']['backoffLimit'] == 0 and job['spec']['activeDeadlineSeconds'] == 180
    pod = job['spec']['template']['spec']
    assert pod['automountServiceAccountToken'] is False and pod['serviceAccountName'] == 'loom-platform'
    assert pod['restartPolicy'] == 'Never' and not pod.get('initContainers')
    assert job['spec']['template']['metadata']['labels']['app'] != 'loom-service'
    container, = pod['containers']
    assert container['command'] == ['python', '-m', 'loom.nebius_management_refresh_probe']
    assert container['image'] == request.switch.render.candidate['images']['service']['image_ref']
    assert container['env'] == [{'name': 'LOOM_REFRESH_DB_URL', 'valueFrom': {
        'secretKeyRef': {'name': 'loom-platform-db', 'key': 'service-url'}}}]
    assert {volume['name'] for volume in pod['volumes']} == {'refresh-probe', 'db-ca'}
    assert not any('projected' in volume for volume in pod['volumes'])
    assert request == before


def test_migration_and_backup_are_manager_only_and_operation_specific(resources_request):
    request = resources_request
    migration, = documents(request, 'migration')
    backup, = documents(request, 'backup')
    for job in (migration, backup):
        assert job['metadata']['namespace'] == request.binding.namespace
        assert request.switch.operation_id.hex in job['metadata']['name']
        assert job['spec']['backoffLimit'] == 0 and 'ttlSecondsAfterFinished' not in job['spec']
        assert job['spec']['template']['spec']['automountServiceAccountToken'] is False
    command = migration['spec']['template']['spec']['containers'][0]['command']
    assert command == ['python', '-m', 'loom.nebius_platform_bootstrap', 'management-database']
    assert 'backup' in backup['spec']['template']['spec']['containers'][0]['command']
    second = replace(request, switch=replace(request.switch, operation_id=uuid4()))
    assert documents(second, 'migration')[0]['metadata']['name'] != migration['metadata']['name']
    assert documents(second, 'backup')[0]['metadata']['name'] != backup['metadata']['name']


@pytest.mark.parametrize('phase', ['config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe'])
def test_stage_replay_preserves_resource_uids(resources_request, tmp_path, phase):
    from scripts.ops.nebius_management_refresh_resources import stage_refresh_resources

    request = resources_request
    api = PhaseAPI(request.binding)
    args = dict(request=request, phase=phase, api=api, state_dir=tmp_path)
    first = stage_refresh_resources(**args)
    retained = copy.deepcopy(api.resources)
    assert stage_refresh_resources(**args) == first
    assert api.resources == retained and len(api.creates) == len(retained)
    assert all(value['kind'] in {'ConfigMap', 'Job'} for value in retained.values())


def test_unknown_create_and_lost_stage_cannot_be_reissued(resources_request, tmp_path):
    from scripts.ops.nebius_management_refresh_resources import stage_refresh_resources
    from scripts.ops.nebius_management_stage import ManagementStageError

    api = PhaseAPI(resources_request.binding)
    api.failure = 'before'
    for _ in range(2):
        with pytest.raises(ManagementStageError, match='unresolved'):
            stage_refresh_resources(request=resources_request, phase='migration', api=api, state_dir=tmp_path)
    assert len(api.creates) == 1


@pytest.mark.parametrize('damage', ['operation', 'namespace', 'shared_uid', 'revision', 'phase'])
def test_invalid_scope_fails_before_any_resource_write(resources_request, tmp_path, damage):
    from scripts.ops.nebius_management_refresh_resources import stage_refresh_resources
    from scripts.ops.nebius_management_stage import ManagementStageError

    request = resources_request
    if damage == 'operation':
        request = replace(request, switch=replace(request.switch, operation_id=UUID(int=0)))
    elif damage == 'namespace':
        request = replace(request, binding=replace(request.binding, namespace='another'))
    elif damage == 'shared_uid':
        request = replace(request, shared_namespace_uid=str(UUID(int=0)))
    elif damage == 'revision':
        request = replace(request, manager_revision='HEAD; arbitrary')
    api = PhaseAPI(request.binding)
    with pytest.raises(ManagementStageError):
        stage_refresh_resources(request=request, phase='permissions' if damage == 'phase' else 'migration', api=api,
            state_dir=tmp_path)
    assert not api.creates


def test_readiness_checks_recorded_job_but_does_not_claim_probe_or_backup_proof(resources_request, tmp_path):
    from scripts.ops.nebius_management_refresh_resources import refresh_resources_ready, stage_refresh_resources
    from scripts.ops.nebius_management_stage import ManagementStageError

    api = PhaseAPI(resources_request.binding)
    args = dict(request=resources_request, phase='migration', api=api, state_dir=tmp_path)
    stage_refresh_resources(**args)
    assert refresh_resources_ready(**args) is False
    job, = api.resources.values()
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    assert refresh_resources_ready(**args) is True
    job['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}
    with pytest.raises(ManagementStageError):
        refresh_resources_ready(**args)
    assert len(api.creates) == 1


def test_https_adapter_rejects_wider_document_even_with_valid_resource_name(resources_request):
    from scripts.ops.nebius_management_refresh_resources import HTTPSManagementRefreshResourcesAPI
    from scripts.ops.nebius_management_stage import ManagementStageError

    with HTTPSManagementRefreshResourcesAPI(request=resources_request, phase='migration', api_server='https://kubernetes.example',
            ssl_context=ssl.create_default_context()) as api:
        migration, = documents(resources_request, 'migration')
        assert api._approved(migration).endswith('/namespaces/loom-nebius-management/jobs')
        migration['spec']['template']['spec']['containers'][0]['command'].append('unapproved')
        with pytest.raises(ManagementStageError):
            api._approved(migration)
