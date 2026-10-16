"""Initial shared-catalog staging rows come from its current published runtime."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_contract import participant
from tests.unit.test_nebius_pool_profiles import document

from loom.nebius_pool_contract import PoolParticipantV1

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.parametrize('staging_attachment_inputs', [0, 1, 2], indirect=True)


@pytest.fixture
def staging_attachment_inputs(platform_inputs, request):
    """Use the richest roster unless a topology-owning test requests another.

    Recovery, corruption and transport matrices do not depend on the number of
    sibling targets. Their negative paths still exercise both guest and auth
    targets; attachment and connected delivery tests explicitly cover 0/1/2.
    """
    from loom.nebius_platform_render import build_platform
    from loom.service_execution_materialization import build_nebius_runtime_profile

    config, candidate, _ = copy.deepcopy(platform_inputs)
    candidate.update(source_ref='refs/heads/dev', repository='ca-mrc/loom')
    config['task_image_builder'] = {'registry_repository': 'cr.eu-north1.nebius.cloud/test/task-images', 'max_concurrent': 2}
    siblings = []
    for field, name in [('guest_execution_target', 'staging-guest'), ('emulated_auth_execution_target', 'staging-auth')][:getattr(request, 'param', 2)]:
        config[field] = {'target_id': name}
        siblings.append(name)
    images = candidate['images']
    refs = [images['service']['image_ref'], images['execution_runtime']['image_ref']]
    profile = build_nebius_runtime_profile(candidate_sha=candidate['candidate_sha'],
        task_image_ref=refs[0], runtime_image_ref=refs[1], runtime_binary_sha256='sha256:' + 'a' * 64,
        image_admission=signed_image_admission_bundle(refs)).model_dump(mode='json')
    if siblings:
        config['task_identity_policy'] = {'mode': 'private-root-v1', 'target_id': config['target_id'],
            'execution_namespace': config['execution_namespace']}
        profile.update(guest_runtime='qemu-tcg-v1', guest_runtime_volume_mib=1024,
            guest_max_artifact_bytes=6 * 1024**3, supports_task_identity=True)
    keyring = document()[2]['image_admission_keyring']
    rendered = build_platform(config, candidate, profile, keyring, repo_root=ROOT)
    observed_kinds = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
        'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
    resources = [copy.deepcopy(row) for group in rendered.values() for row in group if row['kind'] in observed_kinds]
    for row in resources:
        row['metadata'].update(uid=str(uuid4()), resourceVersion='12')
    roots = {row['metadata']['name']: row for row in resources if row['kind'] == 'Namespace'}
    execution, build = config['execution_namespace'], config['execution_namespace'] + '-build'
    binding = PoolParticipantV1.model_validate(participant(environment_class='staging',
        legacy_catalog_environment='development',
        execution_namespace={'name': execution, 'uid': roots[execution]['metadata']['uid']},
        build_namespace={'name': build, 'uid': roots[build]['metadata']['uid']},
        targets=[{'target_id': name, 'profile_id': str(uuid4()),
            'workload_kinds': ['trial', 'verifier', 'task_image_build'] if index == 0 else ['trial', 'verifier']}
            for index, name in enumerate([config['target_id'], *siblings])]))
    observation = {'schema_version': 'loom.nebius-pool-resource-observation.v1',
        'cluster_id': config['cluster_id'], 'namespace': config['namespace'],
        'candidate_sha': candidate['candidate_sha'], 'resources': resources,
        'database_credential': {'uid': str(uuid4()), 'resource_version': '15'},
        'actuator_credential': {'uid': str(uuid4()), 'resource_version': '16'}}
    return dict(participant=binding, observation=observation, candidate=candidate,
        cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'], repo_root=ROOT), config


def prepare(options):
    from scripts.ops.nebius_staging_pool_attachment import prepare_staging_pool_attachment

    return prepare_staging_pool_attachment(**options)


def test_current_staging_attachment_derives_real_catalog_without_republishing_or_mutation(staging_attachment_inputs):
    options, config = staging_attachment_inputs
    before = copy.deepcopy(options)
    result = prepare(options)
    assert options == before
    assert result.participant == options['participant']
    assert result.guard.namespace == config['namespace']
    assert str(result.guard.database.credential_uid) == options['observation']['database_credential']['uid']
    assert result.runtime_profile.candidate_sha == options['candidate']['candidate_sha']
    assert {row.runtime.target_id for row in result.profiles.execution} == {target.target_id for target in result.participant.targets}
    assert len(result.actuators) == len(result.participant.targets)
    assert len(result.profiles.task_images) == 1
    build = result.profiles.task_images[0]
    assert build.settings.source_bucket == config['buckets']['artifacts']
    assert build.settings.namespace == config['execution_namespace'] + '-build'
    assert build.target.node_selector['nebius.com/node-group-id'] == config['execution_node_group_id']
    assert not result.profiles.application_images
    result.profiles.profiles()


def test_staging_build_profile_drives_real_global_native_builder(staging_attachment_inputs):
    from tests.unit.test_nebius_pool_task_image_render import build_inputs, render

    options, config = staging_attachment_inputs
    result = prepare(options)
    binding = result.participant
    _, body, _ = build_inputs()
    body.update(pool_id=binding.pool_id, admission_epoch=binding.admission_epoch,
        participant_revision=binding.binding_revision, target_id=config['target_id'])
    body['key']['participant_id'] = binding.participant_id
    body['origin']['data_environment_id'] = binding.environment_id
    body['build']['source']['uri'] = 's3://' + config['buckets']['artifacts'] + '/tasks/revision/'
    profile, = result.profiles.profiles().task_images.values()
    built = render(binding, body, profile)
    pod = built.job['spec']['template']['spec']
    assert pod['nodeSelector'] == {'nebius.com/node-group-id': config['execution_node_group_id'],
        'loom.nebius/node-role': 'integration-execution', 'loom.nebius/platform': 'integration',
        'loom.nebius/node-os': 'linux', 'loom.nebius/node-arch': 'amd64'}
    assert built.namespace_uid == binding.build_namespace.uid
    assert json.loads(built.configmap['data']['claim.json'])['source_bucket'] == config['buckets']['artifacts']


def test_staging_attachment_ignores_ambient_actuator_settings(staging_attachment_inputs, monkeypatch):
    options, _ = staging_attachment_inputs
    monkeypatch.setenv('LOOM_EXECUTION_ACTUATOR_RUNTIME_CLASS_NAME', 'foreign')
    monkeypatch.setenv('LOOM_EXECUTION_ACTUATOR_SERVICE_ACCOUNT_NAME', 'foreign')
    result = prepare(options)
    for row in result.profiles.execution:
        assert row.runtime.runtime_class_name is None
        assert row.runtime.service_account_name == 'loom-execution-attempt'


def test_current_source_attachment_preserves_completed_legacy_markers_and_uids(staging_attachment_inputs):
    options, _ = staging_attachment_inputs
    operation = str(uuid4())
    marker = 'loom.nebius/pool-retirement-operation'
    for row in options['observation']['resources']:
        if row['kind'] in {'Deployment', 'CronJob'}:
            row['metadata'].setdefault('annotations', {})[marker] = operation
    result = prepare(options)
    originals = {row['metadata']['uid']: row for row in options['observation']['resources']}
    for row in (*result.actuators, result.guard.controller):
        assert row == originals[row['metadata']['uid']]
        assert row['metadata']['annotations'][marker] == operation
    # Preparation retains this history; only the later protected forward
    # composition can qualify it as a predecessor, never a rollback replay.


def test_real_staging_catalog_fits_complete_initial_pool_installation(staging_attachment_inputs):
    from tests.integration.test_nebius_pool_installation import installation

    from loom_service.pool_management.installation import PoolInstallation

    options, config = staging_attachment_inputs
    value, _ = installation(('development',))
    payload = options['participant'].model_dump(mode='json')
    payload.update(pool_id=value['pool_id'], installation_id=value['installation_id'], admission_epoch=value['admission_epoch'])
    options['participant'] = PoolParticipantV1.model_validate(payload)
    result = prepare(options)
    value.update(cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'])
    selector = {'nebius.com/node-group-id': value['node_group_id'],
        'loom.nebius/node-os': 'linux', 'loom.nebius/node-arch': 'amd64'}
    value['node_selector'] = selector
    value['profiles']['execution'][0]['runtime']['node_selector'] = selector
    value['profiles']['task_images'][0]['target']['node_selector'] = selector
    value['participants'].append(result.participant.model_dump(mode='json'))
    profile_data = result.profiles.model_dump(mode='json')
    for kind in ('execution', 'task_images'):
        value['profiles'][kind].extend(profile_data[kind])
    machine = copy.deepcopy(next(row for row in value['machines'] if row['role'] == 'participant'))
    machine.update(machine_id=str(uuid4()), participant_id=str(result.participant.participant_id), token_sha256='f' * 64)
    value['machines'].append(machine)
    spec = PoolInstallation.model_validate(value)
    assert {row.environment_class for row in spec.participants} == {'development', 'staging'}
    assert len(spec.profiles.task_images) == 2


def test_attached_staging_wires_real_controller_service_and_guest_actuators(staging_attachment_inputs):
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_pool_migration import PoolMigrationRequest
    from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
    from scripts.ops.nebius_pool_runtime import wire_participant
    from tests.integration.test_nebius_pool_installation import installation

    from loom_service.pool_management.installation import PoolInstallation

    options, config = staging_attachment_inputs
    value, _ = installation(('staging',))
    payload = options['participant'].model_dump(mode='json')
    payload.update(pool_id=value['pool_id'], installation_id=value['installation_id'],
        admission_epoch=value['admission_epoch'], participant_id=value['participants'][0]['participant_id'])
    options['participant'] = PoolParticipantV1.model_validate(payload)
    result = prepare(options)
    value.update(participants=[result.participant.model_dump(mode='json')], profiles=result.profiles.model_dump(mode='json'),
        cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'],
        node_selector={'nebius.com/node-group-id': config['execution_node_group_id']})
    spec = PoolInstallation.model_validate(value)
    binding = ManagementBinding(str(spec.installation_id), 'loom-nebius-management-dev', str(uuid4()), str(uuid4()))
    registration = PoolRegistrationRequest(spec=spec, binding=binding, candidate=options['candidate'])
    migration = PoolMigrationRequest(registration, (result.guard,))
    service, = [row for row in result.resources if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-service']
    ordinary, *guests = result.actuators
    wired = wire_participant(request=migration, participant_id=result.participant.participant_id,
        management_origin='https://management.example.test', actuator=ordinary, guest_actuators=tuple(guests),
        service=service, runtime_profile=result.runtime_profile)
    assert len(wired) == 3 + len(guests)
    assert all(row['spec']['replicas'] == 0 for row in wired.values())
    cp_env = {row['name']: row.get('value') for row in wired['control_plane']['spec']['template']['spec']['containers'][0]['env']}
    settings = json.loads(cp_env['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON'])
    assert settings['environment'] == 'development'
    assert settings['participant']['environment_class'] == 'staging'
    assert settings['participant']['legacy_catalog_environment'] == 'development'


@pytest.mark.parametrize('damage', ['candidate', 'image', 'namespace_uid', 'cluster', 'group',
    'undeclared_legacy', 'development', 'extra_target', 'missing_actuator', 'duplicate_uid', 'container_hook',
    'pod_hook', 'pod_selector', 'collector_config', 'mount_path', 'optional_secret', 'extra_resources',
    'external_ips', 'owner_reference', 'collector_prefix'])
def test_staging_attachment_rejects_stale_or_unbound_current_runtime(staging_attachment_inputs, damage):
    options, config = copy.deepcopy(staging_attachment_inputs)
    rows = options['observation']['resources']
    if damage == 'candidate':
        options['candidate']['candidate_sha'] = 'd' * 40
    elif damage == 'image':
        options['candidate']['images']['control_plane']['image_ref'] = 'registry.example/foreign@sha256:' + 'f' * 64
    elif damage == 'namespace_uid':
        root, = [row for row in rows if row['kind'] == 'Namespace' and row['metadata']['name'] == config['execution_namespace']]
        root['metadata']['uid'] = str(uuid4())
    elif damage in {'cluster', 'group'}:
        options['cluster_id' if damage == 'cluster' else 'node_group_id'] = 'foreign'
    elif damage in {'undeclared_legacy', 'development', 'extra_target'}:
        payload = options['participant'].model_dump(mode='json')
        if damage == 'undeclared_legacy':
            del payload['legacy_catalog_environment']
        elif damage == 'development':
            del payload['legacy_catalog_environment']
            payload['environment_class'] = 'development'
        else:
            payload['targets'].append({'target_id': 'foreign-target', 'profile_id': str(uuid4()), 'workload_kinds': ['trial']})
        options['participant'] = PoolParticipantV1.model_validate(payload)
    elif damage == 'duplicate_uid':
        rows[1]['metadata']['uid'] = rows[0]['metadata']['uid']
    elif damage == 'collector_prefix':
        collector, = [row for row in rows if row['kind'] == 'CronJob' and row['metadata']['name'] == 'loom-execution-capacity-collector']
        collector['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['envFrom'][0]['prefix'] = 'FOREIGN_'
    elif damage == 'collector_config':
        collector, = [row for row in rows if row['kind'] == 'ConfigMap' and
            'LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR' in row.get('data', {})]
        collector['data']['LOOM_EXECUTION_CAPACITY_COLLECTOR_EXTRA'] = 'unpublished'
    elif damage in {'external_ips', 'owner_reference'}:
        public, = [row for row in rows if row['kind'] == 'Service' and row['spec'].get('type') == 'LoadBalancer']
        if damage == 'external_ips':
            public['spec']['externalIPs'] = ['192.0.2.123']
        else:
            public['metadata']['ownerReferences'] = [{'apiVersion': 'v1', 'kind': 'Secret',
                'name': 'foreign', 'uid': str(uuid4())}]
    else:
        actuator, = [row for row in rows if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-execution-actuator']
        if damage == 'missing_actuator':
            rows.remove(actuator)
        elif damage == 'container_hook':
            actuator['spec']['template']['spec']['containers'][0]['lifecycle'] = {'postStart': {'exec': {'command': ['foreign']}}}
        elif damage == 'pod_hook':
            actuator['spec']['template']['spec']['readinessGates'] = [{'conditionType': 'foreign'}]
        elif damage == 'mount_path':
            actuator['spec']['template']['spec']['containers'][0]['volumeMounts'][0]['subPath'] = 'foreign'
        elif damage == 'optional_secret':
            volume = next(row for row in actuator['spec']['template']['spec']['volumes'] if 'secret' in row)
            volume['secret']['optional'] = True
        elif damage == 'extra_resources':
            actuator['spec']['template']['spec']['containers'][0]['resources']['limits']['nvidia.com/gpu'] = '1'
        else:
            actuator['spec']['template']['spec']['nodeSelector']['foreign'] = 'true'
    with pytest.raises(ValueError, match='staging pool attachment unqualified'):
        prepare(options)


def test_staging_attachment_accepts_known_api_defaults_and_equivalent_quantities(staging_attachment_inputs):
    options, _ = staging_attachment_inputs
    for row in options['observation']['resources']:
        if row['kind'] in {'Deployment', 'StatefulSet', 'CronJob'}:
            spec = row['spec'] if row['kind'] != 'CronJob' else row['spec']['jobTemplate']['spec']
            pod = spec['template']['spec']
            for key, value in {'dnsPolicy': 'ClusterFirst', 'schedulerName': 'default-scheduler',
                'restartPolicy': 'Always', 'terminationGracePeriodSeconds': 30,
                'serviceAccount': pod.get('serviceAccountName', 'default')}.items():
                pod.setdefault(key, value)
            for container in pod.get('containers', []) + pod.get('initContainers', []):
                container.setdefault('imagePullPolicy', 'IfNotPresent')
                container.setdefault('terminationMessagePath', '/dev/termination-log')
                container.setdefault('terminationMessagePolicy', 'File')
                for mount in container.get('volumeMounts', []):
                    mount.setdefault('readOnly', False)
                for port in container.get('ports', []):
                    port.setdefault('protocol', 'TCP')
                limits = container.get('resources', {}).get('limits', {})
                if limits.get('cpu') == '1000m':
                    limits['cpu'] = '1'
            for volume in pod.get('volumes', []):
                for field in ('secret', 'configMap'):
                    if field in volume:
                        volume[field].setdefault('defaultMode', 420)
                        volume[field].setdefault('optional', False)
        if row['kind'] == 'Service' and row['spec'].get('type') == 'LoadBalancer':
            row['spec'].update(clusterIP='10.96.0.10', clusterIPs=['10.96.0.10'], ipFamilies=['IPv4'],
                ipFamilyPolicy='SingleStack', sessionAffinity='None', externalTrafficPolicy='Cluster',
                internalTrafficPolicy='Cluster', allocateLoadBalancerNodePorts=True)
            row['spec']['ports'][0].update(nodePort=31080, protocol='TCP')
    assert prepare(options).participant == options['participant']
