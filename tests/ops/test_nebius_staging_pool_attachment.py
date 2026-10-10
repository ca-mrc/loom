"""Initial shared-catalog staging rows come from its current published runtime."""
from __future__ import annotations

import copy
from pathlib import Path
from uuid import uuid4

import pytest

from loom.nebius_pool_contract import PoolParticipantV1
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs
from tests.unit.test_nebius_pool_contract import participant
from tests.unit.test_nebius_pool_profiles import document

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(params=[0, 1, 2])
def staging_attachment_inputs(platform_inputs, request):
    from loom.nebius_platform_render import build_platform
    from loom.service_execution_materialization import build_nebius_runtime_profile

    config, candidate, _ = copy.deepcopy(platform_inputs)
    candidate.update(source_ref='refs/heads/dev', repository='ca-mrc/loom')
    config['task_image_builder'] = {'registry_repository': 'cr.eu-north1.nebius.cloud/test/task-images', 'max_concurrent': 2}
    siblings = []
    for field, name in [('guest_execution_target', 'staging-guest'), ('emulated_auth_execution_target', 'staging-auth')][:request.param]:
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


@pytest.mark.parametrize('damage', ['candidate', 'image', 'namespace_uid', 'cluster', 'group',
    'undeclared_legacy', 'development', 'extra_target', 'missing_actuator', 'duplicate_uid', 'container_hook'])
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
    else:
        actuator, = [row for row in rows if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-execution-actuator']
        if damage == 'missing_actuator':
            rows.remove(actuator)
        else:
            actuator['spec']['template']['spec']['containers'][0]['lifecycle'] = {'postStart': {'exec': {'command': ['foreign']}}}
    with pytest.raises(ValueError, match='staging pool attachment unqualified'):
        prepare(options)
