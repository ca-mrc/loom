"""Software refresh is an image/config delta, never another installation."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import (
    ROOT,
    render,
)
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    builder_management_inputs as builder_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    management_inputs as management_inputs,
)
from tests.unit.test_nebius_management_render import (
    source_management_inputs as source_management_inputs,
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
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    return ManagementRefreshRenderRequest(ManagementDeployment.model_validate(before),
        ManagementDeployment.model_validate(before), active, candidate, profile, ROOT)


def build(request):
    from scripts.ops.nebius_management_refresh import render_refresh

    return render_refresh(request)


@pytest.fixture
def builder_refresh_request(builder_management_inputs):
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest

    from loom_service.environment_management.deployment import ManagementDeployment

    raw, candidate, profile = copy.deepcopy(builder_management_inputs)
    active = next(doc for doc in render(builder_management_inputs).files['40-services.yaml']
        if doc['kind'] == 'Deployment')
    active['metadata'].update(uid=str(uuid4()), resourceVersion='31', generation=4)
    # First cutover keeps the original cloud/shared material and adds source-only
    # material at its own revision. A later image refresh must retain all three.
    for volume in active['spec']['template']['spec']['volumes']:
        if volume['name'] in {'management-cloud', 'application-shared'}:
            volume['secret']['secretName'] = volume['secret']['secretName'][:-12] + '0123456789ab'
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '9' * 64
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    before = ManagementDeployment.model_validate(raw)
    return ManagementRefreshRenderRequest(before, before, active, candidate, profile, ROOT)


def test_builder_refresh_preserves_independently_revisioned_source_credentials(builder_refresh_request):
    request = builder_refresh_request
    original = copy.deepcopy(request)
    first = build(request)
    active = copy.deepcopy(first.deployment)
    active['metadata'].update(uid=request.active['metadata']['uid'], resourceVersion='33', generation=5)
    candidate, profile = copy.deepcopy(request.candidate), copy.deepcopy(request.profile)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '8' * 64
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    second = build(replace(request, active=active, candidate=candidate, profile=profile))
    for result in (first, second):
        pod = result.deployment['spec']['template']['spec']
        secrets = [volume for volume in pod['volumes'] if 'secret' in volume]
        assert secrets == [volume for volume in request.active['spec']['template']['spec']['volumes'] if 'secret' in volume]
        assert json.loads(result.config['data']['installation.json'])['applications']['runtime'] == (
            request.before.installation.applications.runtime.model_dump(mode='json'))
        template = copy.deepcopy(result.deployment['spec']['template'])
        old_template = request.active['spec']['template']
        template['metadata']['annotations']['loom.nebius/configuration-revision'] = (
            old_template['metadata']['annotations']['loom.nebius/configuration-revision'])
        old_image = old_template['spec']['containers'][0]['image']
        for container in template['spec']['containers'] + template['spec']['initContainers']:
            assert container['image'] == (request.candidate if result is first else candidate)['images']['service']['image_ref']
            container['image'] = old_image
        for volume, previous in zip(template['spec']['volumes'], old_template['spec']['volumes'], strict=True):
            if volume['name'] == 'management-config':
                assert volume['configMap']['name'] == result.config['metadata']['name']
                volume['configMap']['name'] = previous['configMap']['name']
        assert template == old_template
    assert first.revision != second.revision
    assert request == original


@pytest.mark.parametrize('name', ['foreign-secret', 'loom-applications-source-0123456789a',
                                'loom-applications-source-0123456789az'])
def test_builder_refresh_rejects_unqualified_source_material_names(builder_refresh_request, name):
    active = copy.deepcopy(builder_refresh_request.active)
    source, = (volume for volume in active['spec']['template']['spec']['volumes']
        if volume['name'] == 'application-source-credentials')
    source['secret']['secretName'] = name
    with pytest.raises(ValueError, match='refresh'):
        build(replace(builder_refresh_request, active=active))


def test_builder_refresh_cannot_change_source_runtime(builder_refresh_request):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = builder_refresh_request.after.model_dump(mode='json')
    raw['installation']['applications']['runtime']['source_upload']['max_inflight'] = 3
    changed = ManagementDeployment.model_validate(raw)
    with pytest.raises(ValueError, match='refresh'):
        build(replace(builder_refresh_request, after=changed))


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
    for container, old in zip(changed['spec']['initContainers'], original['spec']['initContainers'], strict=True):
        assert container['image'] == request.candidate['images']['service']['image_ref']
        container['image'] = old['image']
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
    (('installation', 'applications', 'storage', 'source_group_id'), 'group-other'),
    (('installation', 'registry_prefix'), 'cr.eu-north1.nebius.cloud/other'),
])
def test_refresh_rejects_non_release_configuration_changes(refresh_request, path, value):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = refresh_request.after.model_dump(mode='json')
    node = raw
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, after=ManagementDeployment.model_validate(raw)))


@pytest.mark.parametrize('field', ['storage_origin', 'artifacts', 'trajectories', 'source', 'project'])
def test_refresh_preserves_original_object_probe_scope(refresh_request, field):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = refresh_request.after.model_dump(mode='json')
    foundation = raw['installation']['foundation']
    config = json.loads(foundation['platform_config_json'])
    if field in {'artifacts', 'trajectories', 'source'}:
        config['buckets'][field] = 'foreign-probe-bucket'
    elif field == 'project':
        foundation['provisioning_project_id'] = 'project-other'
        raw['installation']['applications']['storage']['project_id'] = 'project-other'
    else:
        config.update(region='eu-west1', storage_endpoint='https://storage.eu-west1.nebius.cloud')
        config['execution_price']['region'] = 'eu-west1'
    foundation['platform_config_json'] = json.dumps(config)
    # Independently valid installation inputs must still be rejected as a
    # refresh: legacy frozen plans rely on the original bucket/endpoint scope.
    changed = ManagementDeployment.model_validate(raw)
    with pytest.raises(ValueError, match='refresh'):
        build(replace(refresh_request, after=changed))


@pytest.mark.parametrize('damage', ['nil_uid', 'legacy', 'namespace', 'installation', 'replicas',
                                  'owner', 'deleting', 'container', 'privilege', 'secret', 'missing_config', 'init_image'])
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
    elif damage == 'init_image':
        pod['initContainers'][0]['image'] = pod['initContainers'][0]['image'].split('@')[0] + '@sha256:' + 'f' * 64
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


def test_successive_refreshes_keep_the_original_material_names(refresh_request):
    first = build(refresh_request)
    active = copy.deepcopy(first.deployment)
    active['metadata'].update(uid=refresh_request.active['metadata']['uid'], resourceVersion='27', generation=5)
    candidate, profile = copy.deepcopy(refresh_request.candidate), copy.deepcopy(refresh_request.profile)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '8' * 64
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    second = build(replace(refresh_request, before=refresh_request.after, active=active, candidate=candidate, profile=profile))
    assert first.config['metadata']['name'] != second.config['metadata']['name']
    def secrets(doc):
        return [row for row in doc['spec']['template']['spec']['volumes'] if 'secret' in row]
    assert secrets(second.deployment) == secrets(first.deployment) == secrets(refresh_request.active)


def test_candidate_runtime_contract_change_requires_a_wider_operation(refresh_request, monkeypatch):
    from scripts.ops import nebius_management_refresh as module

    renderer = module.render_management

    def altered(*args, **kwargs):
        rendered = renderer(*args, **kwargs)
        deployment = next(doc for doc in rendered.files['40-services.yaml'] if doc['kind'] == 'Deployment')
        deployment['spec']['template']['spec']['containers'][0]['env'].append({'name': 'NEW_RUNTIME_SETTING', 'value': 'needed'})
        return rendered

    monkeypatch.setattr(module, 'render_management', altered)
    with pytest.raises(ValueError, match='refresh'):
        build(refresh_request)


def test_refresh_config_survives_canonical_private_input_roundtrip(refresh_request):
    from loom_service.environment_management.deployment import ManagementDeployment

    after = ManagementDeployment.model_validate_json(json.dumps(
        refresh_request.after.model_dump(mode='json'), sort_keys=True))
    assert build(replace(refresh_request, after=after)) == build(refresh_request)


@pytest.fixture
def pool_refresh_request(refresh_request, runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_manager

    from loom_service.environment_management.deployment import ManagementDeployment
    from loom_service.pool_management.installation import PoolInstallation

    migration, _, _, _ = runtime_inputs
    registration = migration.registration
    identity = refresh_request.before.installation_id
    spec = registration.spec.model_dump(mode='json')
    spec['installation_id'] = str(identity)
    for participant in spec['participants']:
        participant['installation_id'] = str(identity)
    migration = replace(migration, registration=replace(registration,
        binding=replace(registration.binding, installation_id=str(identity)),
        spec=PoolInstallation.model_validate(spec)))
    active = wire_manager(request=migration, original=refresh_request.active)
    # Simulate only the later activation. The actual wiring supplies the Pod;
    # no test-only normalization may hide a stale image or incompatible mount.
    active['spec']['replicas'] = 1
    raw = refresh_request.before.model_dump(mode='json')
    raw['pool_catalog_operation_id'] = str(migration.registration.spec.operation_id)
    deployment = ManagementDeployment.model_validate(raw)
    return replace(refresh_request, before=deployment, after=deployment, active=active)


def test_refresh_preserves_installed_pool_catalog_across_successors(pool_refresh_request):
    request = pool_refresh_request
    first = build(request)
    active = copy.deepcopy(first.deployment)
    active['metadata'].update(uid=request.active['metadata']['uid'], resourceVersion='28', generation=4)
    candidate = copy.deepcopy(request.candidate)
    candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '8' * 64
    profile = copy.deepcopy(request.profile)
    profile['task_image_ref'] = candidate['images']['service']['image_ref']
    second = build(replace(request, active=active, candidate=candidate, profile=profile))
    for result in (first, second):
        pod = result.deployment['spec']['template']['spec']
        setting, = [row for row in pod['containers'][0]['env'] if row['name'] == 'LOOM_SVC_POOL_PROFILES_FILE']
        assert setting == {'name': 'LOOM_SVC_POOL_PROFILES_FILE', 'value': '/var/run/loom-pool-profiles/profiles.json'}
        volume, = [row for row in pod['volumes'] if row['name'] == 'pool-profiles']
        assert volume['configMap'] == {'name': 'loom-pool-profiles-' + request.before.pool_catalog_operation_id.hex,
            'items': [{'key': 'profiles.json', 'path': 'profiles.json'}]}
        mount, = [row for row in pod['containers'][0]['volumeMounts'] if row['name'] == 'pool-profiles']
        assert mount == {'name': 'pool-profiles', 'mountPath': '/var/run/loom-pool-profiles', 'readOnly': True}
        assert all(row['image'] == pod['containers'][0]['image'] for row in pod['initContainers'])
        assert result.deployment['spec']['strategy'] == {'type': 'Recreate'}


@pytest.mark.parametrize('change', ['remove', 'replace', 'add'])
def test_refresh_cannot_change_pool_catalog_binding(pool_refresh_request, change):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw = pool_refresh_request.after.model_dump(mode='json')
    raw['pool_catalog_operation_id'] = str(uuid4()) if change == 'replace' else None
    changed = ManagementDeployment.model_validate(raw)
    request = (replace(pool_refresh_request, before=changed) if change == 'add'
        else replace(pool_refresh_request, after=changed))
    with pytest.raises(ValueError, match='refresh'):
        build(request)


@pytest.mark.parametrize('damage', ['catalog', 'setting', 'writable', 'extra_mount'])
def test_refresh_rejects_changed_pool_catalog_runtime(pool_refresh_request, damage):
    active = copy.deepcopy(pool_refresh_request.active)
    pod = active['spec']['template']['spec']
    container = pod['containers'][0]
    if damage == 'catalog':
        next(row for row in pod['volumes'] if row['name'] == 'pool-profiles')['configMap']['name'] = 'loom-pool-profiles-' + uuid4().hex
    elif damage == 'setting':
        next(row for row in container['env'] if row['name'] == 'LOOM_SVC_POOL_PROFILES_FILE')['value'] = '/tmp/foreign.json'
    elif damage == 'writable':
        next(row for row in container['volumeMounts'] if row['name'] == 'pool-profiles')['readOnly'] = False
    else:
        container['volumeMounts'].append({'name': 'pool-profiles', 'mountPath': '/tmp/foreign', 'readOnly': True})
    with pytest.raises(ValueError, match='refresh'):
        build(replace(pool_refresh_request, active=active))
