"""Software refresh is an image/config delta, never another installation."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.unit.test_nebius_management_render import (
    ROOT,
    application_management_inputs as application_management_inputs,
    management_inputs as management_inputs,
    render,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def refresh_request(application_management_inputs):
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest

    from loom_service.environment_management.deployment import ManagementDeployment

    before, candidate, profile = copy.deepcopy(application_management_inputs)
    active = next(doc for doc in render(application_management_inputs).files['40-services.yaml']
        if doc['kind'] == 'Deployment')
    active['metadata'].update(uid=str(uuid4()), resourceVersion='19', generation=3)
    active['metadata'].setdefault('annotations', {})['loom.nebius/management-upgrade-id'] = str(uuid4())
    active['status'] = {'observedGeneration': 3, 'replicas': 1, 'availableReplicas': 1}
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '9' * 64
    return ManagementRefreshRenderRequest(ManagementDeployment.model_validate(before),
        ManagementDeployment.model_validate(before), active, candidate, profile, ROOT)


def build(request):
    from scripts.ops.nebius_management_refresh import render_refresh

    return render_refresh(request)


def test_refresh_changes_only_image_config_and_revision(refresh_request):
    from scripts.ops.nebius_ingress_stage import _snapshot

    request = refresh_request
    before = copy.deepcopy(request)
    result = build(request)
    original = request.active['spec']['template']
    changed = copy.deepcopy(result.deployment['spec']['template'])
    assert result.config['kind'] == 'ConfigMap' and result.config['immutable'] is True
    assert json.loads(result.config['data']['installation.json']) == request.after.installation.model_dump(mode='json')
    assert result.config['metadata']['namespace'] == request.before.namespace
    assert result.deployment['metadata'] == _snapshot(request.active)['metadata']
    assert changed['spec']['containers'][0]['image'] == request.candidate['images']['service']['image_ref']
    changed['spec']['containers'][0]['image'] = original['spec']['containers'][0]['image']
    assert changed['metadata']['annotations']['loom.nebius/configuration-revision'] == result.revision
    changed['metadata']['annotations']['loom.nebius/configuration-revision'] = original['metadata']['annotations']['loom.nebius/configuration-revision']
    for volume, old in zip(changed['spec']['volumes'], original['spec']['volumes'], strict=True):
        if volume['name'] == 'management-config':
            assert volume['configMap']['name'] == result.config['metadata']['name']
            volume['configMap']['name'] = old['configMap']['name']
        else:
            assert volume == old
    assert changed == original
    assert request == before


@pytest.mark.parametrize('path,value', [
    (('backup_bucket',), 'another-manager-backup'),
    (('postgres_storage_gi',), 20),
    (('public_host',), 'another-manager.example.com'),
    (('installation', 'platform_budget', 'cpu_millis'), 3000),
    (('installation', 'applications', 'runtime', 'poll_seconds'), 6),
    (('installation', 'applications', 'storage', 'data_group_id'), 'group-other'),
    (('installation', 'keyring'), {'schema_version': 1, 'keys': []}),
])
def test_refresh_rejects_non_release_configuration_changes(refresh_request, path, value):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = refresh_request.after.model_dump(mode='json')
    # An equal keyring is valid; give the before side an independently valid
    # different value using an existing scalar boundary instead of fake key bytes.
    if path == ('installation', 'keyring'):
        path, value = ('installation', 'registry_prefix'), 'cr.eu-north1.nebius.cloud/other'
    node = raw
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, after=ManagementDeployment.model_validate(raw)))


@pytest.mark.parametrize('damage', ['nil_uid', 'legacy', 'namespace', 'installation', 'replicas',
                                  'owner', 'deleting', 'container', 'privilege', 'secret', 'missing_config'])
def test_refresh_rejects_invalid_or_incompatible_retained_runtime(refresh_request, damage):
    active = copy.deepcopy(refresh_request.active)
    pod = active['spec']['template']['spec']
    if damage == 'nil_uid':
        active['metadata']['uid'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'legacy':
        pod['serviceAccountName'] = 'loom-management-provisioner'
    elif damage == 'namespace':
        active['metadata']['namespace'] = 'loom-nebius-management-other'
    elif damage == 'installation':
        active['metadata']['labels']['loom.nebius/management-installation'] = str(uuid4())
    elif damage == 'replicas':
        active['spec']['replicas'] = 2
    elif damage == 'owner':
        active['metadata']['ownerReferences'] = [{'uid': str(uuid4())}]
    elif damage == 'deleting':
        active['metadata']['deletionTimestamp'] = '2026-09-29T00:00:00Z'
    elif damage == 'container':
        pod['containers'].append(copy.deepcopy(pod['containers'][0]))
    elif damage == 'privilege':
        pod['containers'][0]['securityContext']['privileged'] = True
    elif damage == 'secret':
        next(row for row in pod['volumes'] if row['name'] == 'management-cloud')['secret']['secretName'] = 'unrelated-secret'
    else:
        pod['volumes'] = [row for row in pod['volumes'] if row['name'] != 'management-config']
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, active=active))


def test_refresh_accepts_api_defaults_and_retains_their_exact_values(refresh_request):
    active = copy.deepcopy(refresh_request.active)
    active['spec']['revisionHistoryLimit'] = 10
    active['spec']['template']['spec']['dnsPolicy'] = 'ClusterFirst'
    active['spec']['template']['spec']['containers'][0]['terminationMessagePath'] = '/dev/termination-log'
    result = build(replace(refresh_request, active=active))
    assert result.deployment['spec']['revisionHistoryLimit'] == 10
    assert result.deployment['spec']['template']['spec']['dnsPolicy'] == 'ClusterFirst'
    assert result.deployment['spec']['template']['spec']['containers'][0]['terminationMessagePath'] == '/dev/termination-log'


def test_schema_advance_requires_new_release_identity_and_consistent_selection(refresh_request):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = refresh_request.after.model_dump(mode='json')
    application = raw['installation']['applications']
    application['shared']['schema_revision'] = '0168'
    application['releases'][0]['schema_revision'] = '0168'
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, after=ManagementDeployment.model_validate(raw)))
    application['releases'][0]['release_id'] = str(uuid4())
    result = build(replace(refresh_request, after=ManagementDeployment.model_validate(raw)))
    assert json.loads(result.config['data']['installation.json'])['applications']['shared']['schema_revision'] == '0168'
    application['releases'][0]['schema_revision'] = '0000'
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, after=ManagementDeployment.model_validate(raw)))


def test_refresh_cannot_remove_or_rebind_prior_publications(refresh_request):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = refresh_request.before.model_dump(mode='json')
    publication = {'candidate_id': str(uuid4()), 'source_sha': 'e' * 40, 'run_id': 10,
        'run_attempt': 1, 'artifact_id': 20, 'artifact_sha256': 'sha256:' + 'c' * 64, 'pull_request': 30}
    raw['installation']['publications'] = [publication]
    request = replace(refresh_request, before=ManagementDeployment.model_validate(raw))
    with pytest.raises(ValueError, match='refresh'):
        build(request)
    raw['installation']['publications'][0]['artifact_id'] = 21
    with pytest.raises(ValueError, match='refresh'):
        build(replace(request, after=ManagementDeployment.model_validate(raw)))
