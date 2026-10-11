"""A normal candidate must stay a shared participant, not restore local writers."""
from __future__ import annotations

import copy
import importlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_migration import migration_request
from tests.ops.test_nebius_pool_runtime import env
from tests.ops.test_nebius_staging_pool_attachment import (
    ROOT,
    prepare,
)
from tests.ops.test_nebius_staging_pool_attachment import (
    staging_attachment_inputs as staging_attachment_inputs,
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

from loom.nebius_platform_render import build_platform
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation import PoolInstallation


def module():
    name = 'scripts.ops.nebius_pool_participant_release'
    assert importlib.util.find_spec(name) is not None, 'shared participant normal-release projection is missing'
    return importlib.import_module(name)


@pytest.fixture
def release_inputs(staging_attachment_inputs):
    from scripts.ops.nebius_pool_runtime import participant_readonly_roles, wire_participant

    options, config = copy.deepcopy(staging_attachment_inputs)
    previous = migration_request(('staging', 'development'))
    spec = previous.registration.spec.model_dump(mode='json')
    payload = options['participant'].model_dump(mode='json')
    payload.update(pool_id=spec['pool_id'], installation_id=spec['installation_id'], admission_epoch=spec['admission_epoch'],
        participant_id=spec['participants'][0]['participant_id'])
    options['participant'] = PoolParticipantV1.model_validate(payload)
    attached = prepare(options)
    spec['participants'][0] = attached.participant.model_dump(mode='json')
    spec.update(cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'])
    spec['node_selector'] = copy.deepcopy(attached.profiles.execution[0].runtime.node_selector)
    for kind, runtime in (('execution', 'runtime'), ('task_images', 'target')):
        other = spec['profiles'][kind][1]
        other[runtime]['node_selector'] = spec['node_selector']
        spec['profiles'][kind] = attached.profiles.model_dump(mode='json')[kind] + [other]
    spec = PoolInstallation.model_validate(spec)
    previous = replace(previous, registration=replace(previous.registration, spec=spec, candidate=options['candidate']),
        guards=(attached.guard, previous.guards[1]))
    files = build_platform(config, options['candidate'], attached.runtime_profile.model_dump(mode='json'),
        spec.profiles.image_admission_keyring, repo_root=ROOT)
    observed = {_key(row): row for row in options['observation']['resources']}
    current = {_key(row): copy.deepcopy(observed.get(_key(row), row)) for group in files.values() for row in group}
    for row in current.values():
        row['metadata'].setdefault('uid', str(uuid4()))
        row['metadata'].setdefault('resourceVersion', '17')
    ordinary, *guests = attached.actuators
    wired = wire_participant(request=previous, participant_id=attached.participant.participant_id,
        management_origin='https://manage.example.test', actuator=ordinary, guest_actuators=tuple(guests),
        service=current[f'Deployment:{config["namespace"]}:loom-service'], runtime_profile=attached.runtime_profile)
    marker = 'loom.nebius/shared-pool-handoff-operation'
    for row in wired.values():
        row['spec']['replicas'] = 1
        row['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
        current[_key(row)] = row
    for role in participant_readonly_roles(request=previous):
        if role['metadata'].get('namespace') not in {attached.participant.execution_namespace.name,
                attached.participant.build_namespace.name}:
            continue
        retained = current[_key(role)]
        for key in ('rules', 'subjects', 'roleRef'):
            if key in role:
                retained[key] = copy.deepcopy(role[key])
        retained['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
    collector = current[f'CronJob:{config["execution_namespace"]}:loom-execution-capacity-collector']
    collector['spec']['suspend'] = True
    collector['metadata'].setdefault('annotations', {})[marker] = str(spec.operation_id)
    candidate = copy.deepcopy(options['candidate'])
    candidate['candidate_sha'] = 'd' * 40
    for value in candidate['images'].values():
        value['image_ref'] = value['image_ref'].split('@')[0] + '@sha256:' + 'e' * 64
    profile = attached.runtime_profile.model_dump(mode='json')
    refs = (candidate['images']['service']['image_ref'], candidate['images']['execution_runtime']['image_ref'])
    profile.update(candidate_sha=candidate['candidate_sha'], task_image_ref=refs[0], runtime_image_ref=refs[1],
        runtime_binary_sha256='sha256:' + 'f' * 64,
        image_admission=signed_image_admission_bundle(refs).model_dump(mode='json'))
    return dict(previous=previous, current=tuple(current.values()), config=config, candidate=candidate,
        runtime_profile=ServiceExecutionRuntimeProfileV1.model_validate(profile), operation_id=uuid4(),
        management_origin='https://manage.example.test', repo_root=ROOT)


def test_normal_release_keeps_shared_wiring_and_preserves_retired_and_other_owner_authority(release_inputs):
    from loom_service.pool_management.participant_refresh import PoolParticipantRefresh

    before = copy.deepcopy(release_inputs)
    plan = module().prepare_staging_pool_release(**release_inputs)
    assert release_inputs == before
    assert module().prepare_staging_pool_release(**release_inputs) == plan
    refresh = PoolParticipantRefresh.model_validate(plan.refresh.model_dump())
    previous = release_inputs['previous']
    assert refresh.before == previous.registration.spec
    assert refresh.after.policy_revision == refresh.before.policy_revision + 1
    assert refresh.after.participants[1] == refresh.before.participants[1]
    assert refresh.after.machines == refresh.before.machines
    selected = refresh.after.participants[0]
    assert selected.participant_id == refresh.participant_id
    original = refresh.before.participants[0]
    assert {row.profile_id for row in selected.targets}.isdisjoint(row.profile_id for row in original.targets)
    assert len(plan.files['30-migrate.yaml']) == len(plan.files['50-configure.yaml']) == 1
    rows = {_key(row): row for group in plan.files.values() for row in group}
    namespace = release_inputs['config']['namespace']
    cp = rows[f'Deployment:{namespace}:loom-control-plane']
    api = rows[f'Deployment:{namespace}:loom-service']
    control = PoolRuntimeSettings.model_validate_json(env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
    assert control.participant == selected and control.management_origin == 'https://manage.example.test'
    assert json.loads(env(api)['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])['data_environment_id'] == str(selected.environment_id)
    workloads = [cp, api]
    for index, target in enumerate(selected.targets):
        name = 'loom-execution-actuator' if index == 0 else target.target_id + '-actuator'
        actuator = rows[f'Deployment:{selected.execution_namespace.name}:{name}']
        runtime = PoolRuntimeSettings.model_validate_json(env(actuator)['LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL']['value'])
        assert runtime.participant == selected
        assert ('LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER' in env(actuator)) == (index == 0)
        workloads.append(actuator)
    current = {_key(row): row for row in release_inputs['current']}
    for row in workloads:
        assert row['spec']['replicas'] == 1 and row['spec']['strategy'] == {'type': 'Recreate'}
        assert row['metadata']['annotations']['loom.nebius/shared-pool-handoff-operation'] == str(refresh.before.operation_id)
        assert row['spec']['template']['spec']['containers'][0]['image'].endswith('@sha256:' + 'e' * 64)
    for key, row in plan.preserved.items():
        assert row == current[key]
        assert key not in rows
    collector = f'CronJob:{selected.execution_namespace.name}:loom-execution-capacity-collector'
    assert plan.preserved[collector]['spec']['suspend'] is True
    assert not any(row['kind'] in {'ClusterRole', 'ClusterRoleBinding'} for row in rows.values())
    for ns, name in ((selected.execution_namespace.name, 'loom-execution-actuator'),
            (selected.build_namespace.name, 'loom-task-image-builder')):
        role = rows[f'Role:{ns}:{name}']
        assert {verb for rule in role['rules'] for verb in rule['verbs']} <= {'get', 'list'}
        assert role['metadata']['annotations'] == current[_key(role)]['metadata']['annotations']
    # Source config/profile follows the selected candidate, not an old render.
    configmap = rows[f'ConfigMap:{namespace}:loom-platform-config']
    assert json.loads(configmap['data']['profile.json'])['candidate_sha'] == 'd' * 40
    assert plan.migration.registration.spec == refresh.after


@pytest.mark.parametrize('damage', ['source', 'operation', 'scope', 'namespace-uid', 'global-participant',
    'global-origin', 'build-policy', 'runtime-profile', 'collector-active', 'readonly-role', 'role-binding', 'cluster-role'])
def test_normal_release_rejects_scope_or_live_binding_drift(release_inputs, damage):
    options = copy.deepcopy(release_inputs)
    current = {_key(row): row for row in options['current']}
    namespace, execution = options['config']['namespace'], options['config']['execution_namespace']
    cp = current[f'Deployment:{namespace}:loom-control-plane']
    if damage == 'source':
        options['candidate']['source_ref'] = 'refs/heads/feature/unqualified'
    elif damage == 'operation':
        options['operation_id'] = options['previous'].registration.spec.operation_id
    elif damage == 'scope':
        options['config']['execution_node_group_id'] = 'foreign'
    elif damage == 'namespace-uid':
        current[f'Namespace:-:{execution}']['metadata']['uid'] = str(uuid4())
    elif damage in {'global-participant', 'global-origin'}:
        settings = json.loads(env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'])
        if damage == 'global-participant':
            settings['participant']['incarnation'] = str(uuid4())
        else:
            settings['management_origin'] = 'https://foreign.example.test'
        env(cp)['LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON']['value'] = json.dumps(settings)
    elif damage == 'build-policy':
        options['config']['task_image_builder']['max_concurrent'] += 1
        current[f'ConfigMap:{namespace}:loom-platform-config']['data']['environment.json'] = json.dumps(options['config'])
    elif damage == 'runtime-profile':
        options['runtime_profile'] = options['runtime_profile'].model_copy(update={'candidate_sha': 'f' * 40})
    elif damage == 'collector-active':
        current[f'CronJob:{execution}:loom-execution-capacity-collector']['spec']['suspend'] = False
    elif damage == 'readonly-role':
        current[f'Role:{execution}:loom-execution-actuator']['rules'][0]['verbs'].append('create')
    elif damage == 'role-binding':
        current[f'RoleBinding:{execution}:loom-execution-actuator']['subjects'][0]['name'] = 'foreign'
    else:
        current[f'ClusterRole:{"-"}:{execution}-actuator-usage']['rules'][0]['resources'] = ['nodes/proxy']
    with pytest.raises(ValueError):
        module().prepare_staging_pool_release(**options)
