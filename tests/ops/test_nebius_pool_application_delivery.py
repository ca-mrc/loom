"""Derive first-cutover builder delivery from the exact protected pool catalog."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from uuid import uuid4

import pytest
from tests.integration.test_nebius_pool_installation import add_application_builder, installation
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import (
    ROOT,
    render,
)
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def delivery_inputs(application_management_inputs, build_inputs):
    from loom_service.environment_management.deployment import ManagementDeployment
    from loom_service.pool_management.installation import PoolInstallation

    before = ManagementDeployment.model_validate(application_management_inputs[0])
    application, config = before.installation.applications, before.installation.foundation.platform_config
    spec, _ = installation(('development',))
    spec.update(installation_id=str(before.installation_id), cluster_id=application.shared.cluster_id,
        node_group_id=config['execution_node_group_id'])
    participant, = spec['participants']
    participant.update(installation_id=str(before.installation_id), environment_id=str(application.shared.data_environment_id))
    participant['execution_namespace']['name'] = config['execution_namespace']
    participant['build_namespace']['name'] = config['execution_namespace'] + '-build'
    spec['node_selector']['nebius.com/node-group-id'] = spec['node_group_id']
    for kind, key in [('execution', 'runtime'), ('task_images', 'target')]:
        for row in spec['profiles'][kind]:
            row[key]['namespace'] = participant['execution_namespace' if kind == 'execution' else 'build_namespace']['name']
            row[key]['node_selector']['nebius.com/node-group-id'] = spec['node_group_id']
            if kind == 'task_images':
                row['settings'].update(namespace=row[key]['namespace'], storage_endpoint=config['storage_endpoint'],
                    storage_region=config['region'], source_bucket=config['buckets']['source'],
                    registry_repository=build_inputs[0].registry_repository)
    recipe = build_inputs[0].recipe.model_copy(update={'schema_revision': application.shared.schema_revision})
    spec, machine_id, _ = add_application_builder(spec, recipe)
    spec['node_selector'] = dict(spec['profiles']['application_images'][0]['target']['node_selector'])
    return before, PoolInstallation.model_validate(spec), machine_id


def test_builder_delivery_is_derived_without_replacing_retained_manager_identity(delivery_inputs):
    from scripts.ops.nebius_pool_application_delivery import derive_application_build_deployment

    before, spec, machine_id = delivery_inputs
    snapshot = before.model_dump(mode='json')
    result = derive_application_build_deployment(before, spec)
    assert before.model_dump(mode='json') == snapshot
    assert result.pool_catalog_operation_id == spec.operation_id
    assert result.application_builder_machine_id == machine_id
    runtime = result.installation.applications.runtime
    binding = runtime.build.binding
    participant, = spec.participants
    profile, = spec.profiles.application_images
    assert binding.source.source_bucket == before.installation.foundation.platform_config['buckets']['source']
    assert binding.recipe == profile.recipe
    assert (binding.pool_id, binding.participant_id, binding.profile_id, binding.target_id) == (
        spec.pool_id, participant.participant_id, profile.profile_id, profile.target.target_id)
    assert (binding.admission_epoch, binding.participant_revision) == (spec.admission_epoch, participant.binding_revision)
    assert runtime.build.management_origin == 'https://' + before.public_host
    assert runtime.source_upload.max_inflight == 2
    assert runtime.build.binding.source.upload_ttl_seconds == runtime.source_upload.upload_ttl_seconds
    # Every change is the explicit first-pool delivery, not a foundation rewrite.
    actual = result.model_dump(mode='json')
    actual.pop('pool_catalog_operation_id')
    actual.pop('application_builder_machine_id')
    for field in ('source_upload', 'build'):
        actual['installation']['applications']['runtime'].pop(field)
    assert actual == snapshot
    with pytest.raises(ValueError, match='pool_application_delivery_unqualified'):
        derive_application_build_deployment(result, spec)


@pytest.mark.parametrize('damage', ['installation', 'cluster', 'data', 'namespace', 'schema', 'source', 'node-group', 'missing-builder'])
def test_builder_delivery_rejects_pool_scope_drift_before_runtime_changes(delivery_inputs, damage):
    from scripts.ops.nebius_pool_application_delivery import derive_application_build_deployment

    from loom_service.pool_management.installation import PoolInstallation

    before, spec, _ = delivery_inputs
    value = copy.deepcopy(spec.model_dump(mode='json'))
    participant, = value['participants']
    if damage == 'installation':
        value['installation_id'] = participant['installation_id'] = str(uuid4())
    elif damage == 'cluster':
        value['cluster_id'] = 'foreign-cluster'
    elif damage == 'data':
        participant['environment_id'] = str(uuid4())
    elif damage == 'namespace':
        participant['build_namespace']['name'] = 'foreign-build'
        for row in value['profiles']['task_images'] + value['profiles']['application_images']:
            row['target']['namespace'] = row['settings']['namespace'] = 'foreign-build'
    elif damage == 'schema':
        value['profiles']['application_images'][0]['recipe']['schema_revision'] = '9999'
    elif damage == 'source':
        value['profiles']['application_images'][0]['settings']['source_bucket'] = 'foreign-source'
    elif damage == 'node-group':
        value['node_group_id'] = 'foreign-group'
        value['node_selector']['nebius.com/node-group-id'] = 'foreign-group'
        for kind, key in [('execution', 'runtime'), ('task_images', 'target'), ('application_images', 'target')]:
            for row in value['profiles'][kind]:
                row[key]['node_selector']['nebius.com/node-group-id'] = 'foreign-group'
    else:
        value['profiles'].pop('application_images')
        participant['targets'] = [row for row in participant['targets'] if row['workload_kinds'] != ['application_image_build']]
        value['machines'] = [row for row in value['machines'] if row.get('workload_scope') != 'application_builder']
    changed = PoolInstallation.model_validate(value)
    with pytest.raises(ValueError, match='pool_application_delivery_unqualified'):
        derive_application_build_deployment(before, changed)


def test_first_cutover_derives_stopped_builder_and_only_required_configuration(delivery_inputs, application_management_inputs):
    from scripts.ops.nebius_pool_application_delivery import render_application_build_delivery

    before, pool, machine_id = delivery_inputs
    _, candidate, profile = application_management_inputs
    active = next(doc for doc in render(application_management_inputs).files['40-services.yaml'] if doc['kind'] == 'Deployment')
    active['metadata'].update(uid=str(uuid4()), resourceVersion='41')
    snapshot = copy.deepcopy(active)
    result = render_application_build_delivery(before=before, pool=pool, active=active,
        candidate=candidate, profile=profile, repo_root=ROOT)
    assert active == snapshot
    assert result.deployment['metadata'] == {key: value for key, value in active['metadata'].items()
        if key not in {'uid', 'resourceVersion'}}
    assert result.deployment['spec']['replicas'] == 0
    assert result.deployment['spec']['strategy'] == {'type': 'Recreate'}
    volumes = {row['name']: row for row in result.deployment['spec']['template']['spec']['volumes']}
    for volume in active['spec']['template']['spec']['volumes']:
        if volume['name'] in {'management-cloud', 'application-shared'}:
            assert volumes[volume['name']] == volume
    assert volumes['pool-token-source']['secret']['secretName'] == 'loom-pool-machine-' + machine_id.hex
    assert volumes['application-source-credentials']['secret']['secretName'] == result.source_secret_name
    assert {row['kind'] for row in result.configuration} == {'ConfigMap', 'Role', 'RoleBinding'}
    config, = (row for row in result.configuration if row['kind'] == 'ConfigMap')
    assert config['metadata']['name'] == volumes['management-config']['configMap']['name']
    settings = json.loads(config['data']['installation.json'])['applications']['runtime']
    assert settings['build']['binding']['pool_id'] == str(pool.pool_id)
    assert settings['source_upload']['credentials_file'] == '/var/run/loom-application-source-credentials/credentials.json'
    role, = (row for row in result.configuration if row['kind'] == 'Role')
    assert role['metadata']['namespace'] == pool.participants[0].build_namespace.name
    assert {verb for rule in role['rules'] for verb in rule['verbs']} == {'get', 'list'}
    binding, = (row for row in result.configuration if row['kind'] == 'RoleBinding')
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-application-provisioner', 'namespace': before.namespace}]


def test_first_cutover_rejects_unqualified_original_manager(delivery_inputs, application_management_inputs):
    from scripts.ops.nebius_pool_application_delivery import render_application_build_delivery

    before, pool, _ = delivery_inputs
    _, candidate, profile = application_management_inputs
    active = next(doc for doc in render(application_management_inputs).files['40-services.yaml'] if doc['kind'] == 'Deployment')
    active['metadata'].update(uid=str(uuid4()), resourceVersion='41')
    active['spec']['template']['spec']['containers'][0]['env'].append({'name': 'UNQUALIFIED_RUNTIME', 'value': '1'})
    with pytest.raises(ValueError, match='pool_application_delivery_unqualified'):
        render_application_build_delivery(before=before, pool=pool, active=active,
            candidate=candidate, profile=profile, repo_root=ROOT)


@pytest.fixture
def source_material_inputs(delivery_inputs, platform_inputs):
    from loom.nebius_platform_render import build_platform

    before, _, _ = delivery_inputs
    config, candidate, profile = copy.deepcopy(platform_inputs)
    documents = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    controller = next(row for group in documents.values() for row in group if row['kind'] == 'Deployment'
        and row['metadata']['name'] == 'loom-control-plane')
    controller['metadata'].update(uid=str(uuid4()), resourceVersion='21')
    material = {'access-key': 'source-only-access', 'secret-key': 'source-only-secret'}
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'namespace': before.installation.applications.shared.platform_namespace,
            'name': 'loom-platform-storage', 'uid': str(uuid4()), 'resourceVersion': '25'},
        'data': {**{'source-' + key: base64.b64encode(value.encode()).decode() for key, value in material.items()},
            'access-key': base64.b64encode(b'data-access-must-not-copy').decode(),
            'secret-key': base64.b64encode(b'data-secret-must-not-copy').decode()}}
    pin = {'uid': secret['metadata']['uid'], 'resource_version': '25',
        'sha256': hashlib.sha256(json.dumps(material, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    return before, controller, secret, pin, material


def test_source_material_copies_only_exact_retained_source_identity(source_material_inputs):
    from scripts.ops.nebius_pool_application_delivery import (
        ApplicationSourceCredentialPin,
        qualify_application_source_material,
    )

    before, controller, secret, pin, expected = source_material_inputs
    snapshot = copy.deepcopy((controller, secret))
    result = qualify_application_source_material(before=before, controller=controller,
        secret=secret, pin=ApplicationSourceCredentialPin.model_validate(pin))
    assert result == expected
    assert (controller, secret) == snapshot
    result['access-key'] = 'detached-result'
    assert qualify_application_source_material(before=before, controller=controller,
        secret=secret, pin=ApplicationSourceCredentialPin.model_validate(pin)) == expected


@pytest.mark.parametrize('damage', ['uid', 'version', 'hash', 'namespace', 'name', 'deleting',
    'controller_namespace', 'source_reference', 'endpoint', 'bucket', 'duplicate_env', 'payload', 'whitespace'])
def test_source_material_rejects_drift_and_never_returns_other_shared_credentials(source_material_inputs, damage):
    from scripts.ops.nebius_pool_application_delivery import (
        ApplicationSourceCredentialPin,
        qualify_application_source_material,
    )

    before, controller, secret, pin, _ = source_material_inputs
    env = controller['spec']['template']['spec']['containers'][0]['env']
    if damage in {'uid', 'namespace', 'name'}:
        secret['metadata'][damage] = str(uuid4()) if damage == 'uid' else 'foreign'
    elif damage == 'version':
        secret['metadata']['resourceVersion'] = '26'
    elif damage == 'hash':
        pin['sha256'] = '0' * 64
    elif damage == 'deleting':
        secret['metadata']['deletionTimestamp'] = '2026-10-02T00:00:00Z'
    elif damage == 'controller_namespace':
        controller['metadata']['namespace'] = 'foreign'
    elif damage == 'source_reference':
        next(row for row in env if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_SOURCE_ACCESS_KEY')['valueFrom']['secretKeyRef']['key'] = 'access-key'
    elif damage in {'endpoint', 'bucket'}:
        next(row for row in env if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_SOURCE_' + damage.upper())['value'] = 'foreign'
    elif damage == 'duplicate_env':
        env.append(copy.deepcopy(env[0]))
    else:
        secret['data']['source-secret-key'] = 'not-base64' if damage == 'payload' else base64.b64encode(b'secret with space').decode()
    with pytest.raises(ValueError, match='pool_application_source_unqualified'):
        qualify_application_source_material(before=before, controller=controller,
            secret=secret, pin=ApplicationSourceCredentialPin.model_validate(pin))
