"""Independent runtime delivery uses actual foundation and process consumers."""
from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path

import pytest

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.profiles import PoolProfileCatalog
from tests.integration.test_nebius_pool_profile_registry import catalog
from tests.ops.test_nebius_pool_runtime import desired_profile, env
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_foundation import declaration
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def runtime_request(runtime_inputs, platform_inputs):
    migration, _, services, _ = runtime_inputs
    old = migration.registration.spec.participants[-1]
    wanted = declaration(platform_inputs)
    identity = wanted.registration.model_dump(mode='json')
    for name in ('installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation'):
        identity[name] = str(getattr(old, name))
    config = wanted.platform_config
    config['registration'] = identity
    wanted = EnvironmentFoundationDeclarationV2.model_validate(wanted.model_dump() | {
        'registration': identity, 'platform_config_json': json.dumps(config)})
    primary_id = old.targets[0].target_id
    participant = old.model_dump(mode='json')
    participant['execution_namespace']['name'] = 'work-red'
    participant['build_namespace']['name'] = 'build-red'
    for target in participant['targets']:
        if target['target_id'] == primary_id:
            target['target_id'] = 'target-red'
    participant = PoolParticipantV1.model_validate(participant)
    profiles = catalog(migration.registration.spec, old).model_dump(mode='json')
    for profile in profiles['execution']:
        profile['runtime'].update(target_id='target-red', namespace='work-red',
            credential_broker_url='http://loom-llm-gateway.app-red.svc.cluster.local:9100/internal/service-execution')
    for profile in profiles['task_images']:
        profile['target'].update(target_id='target-red', namespace='build-red')
        profile['settings'].update(namespace='build-red', source_bucket=config['buckets']['artifacts'],
            storage_endpoint=config['storage_endpoint'], storage_region=config['region'])
    machine, = [row for row in migration.registration.spec.machines if row.participant_id == old.participant_id]
    return dict(declaration=wanted, participant=participant, profiles=PoolProfileCatalog.model_validate(profiles),
        machine=machine, candidate=migration.registration.candidate, runtime_profile=desired_profile(migration, services[old.participant_id]),
        management_origin='https://platform.example.com', management_namespace='platform-services', repo_root=ROOT)


def subject():
    name = 'scripts.ops.nebius_environment_runtime'
    assert importlib.util.find_spec(name), 'independent foundation-to-runtime delivery is missing'
    return importlib.import_module(name)


def test_runtime_is_scoped_closed_and_consumable_without_staging_history(runtime_request, monkeypatch):
    from loom_control_plane.config import ControlPlaneSettings
    from loom_execution_actuator.config import ExecutionActuatorSettings

    render = subject().render_environment_runtime
    original = copy.deepcopy(runtime_request)
    result = render(**runtime_request)
    assert runtime_request == original
    workloads = result.workloads
    assert {row['metadata']['name'] for row in workloads.values()} == {
        'loom-service', 'loom-control-plane', 'loom-execution-actuator'}
    assert all(row['spec']['replicas'] == 0 and 'uid' not in row['metadata'] for row in workloads.values())
    documents = [*workloads.values(), *result.readers, *result.gateway_authority, *result.isolation]
    assert not any(row['kind'] in {'CronJob', 'Secret', 'Namespace', 'ResourceQuota'} for row in documents)
    assert all(row['metadata'].get('namespace') in {None, 'app-red', 'work-red', 'build-red'} for row in documents)
    for role in result.readers:
        assert all(set(rule['verbs']) <= {'get', 'list', 'watch'} for rule in role.get('rules', []))
        assert not any('nodes/proxy' in rule['resources'] for rule in role.get('rules', []))
    bindings = [row for row in result.gateway_authority if row['kind'].endswith('Binding')]
    assert bindings and all(row['subjects'] == [dict(kind='ServiceAccount', name='loom-pool-gateway',
        namespace='platform-services')] for row in bindings)
    cluster_role, = [row for row in result.gateway_authority if row['kind'] == 'ClusterRole']
    assert cluster_role['rules'] == [dict(apiGroups=[''], resources=['namespaces'], verbs=['get'],
        resourceNames=['work-red', 'build-red'])]
    assert any(row['kind'] == 'ValidatingAdmissionPolicyBinding' for row in result.isolation)
    for component, settings_class, values in (
        ('control_plane', ControlPlaneSettings, {'LOOM_CP_DB_URL': 'postgresql+psycopg://u:p@localhost/test',
            'LOOM_CP_MINIO_ACCESS_KEY': 'fixture', 'LOOM_CP_MINIO_SECRET_KEY': 'fixture'}),
        ('actuator', ExecutionActuatorSettings, {'LOOM_EXECUTION_ACTUATOR_DB_URL': 'postgresql+psycopg://u:p@localhost/test',
            'LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID': 'fixture'}),
    ):
        with monkeypatch.context() as environment:
            for name, row in env(workloads[component]).items():
                if 'value' in row:
                    environment.setenv(name, row['value'])
            for name, value in values.items():
                environment.setenv(name, value)
            settings = settings_class(_env_file=None)
            assert settings.global_pool.participant == runtime_request['participant']
            if component == 'control_plane':
                assert settings.service_execution_scheduler_enabled is True
            else:
                assert settings.task_image_builder.namespace == 'build-red'
                assert settings.node_selector == runtime_request['profiles'].execution[0].runtime.node_selector
    source = json.loads(env(workloads['service'])['LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON']['value'])
    assert source['data_environment_id'] == str(runtime_request['participant'].environment_id)


@pytest.mark.parametrize('damage', ['source_bucket', 'profile_namespace', 'target', 'environment', 'management_namespace'])
def test_runtime_cannot_substitute_data_or_authority(runtime_request, damage):
    render = subject().render_environment_runtime
    if damage == 'management_namespace':
        runtime_request['management_namespace'] = 'work-red'
    elif damage in {'target', 'environment'}:
        runtime_request['participant'] = runtime_request['participant'].model_copy(update={
            'environment_class': 'staging'} if damage == 'environment' else {
                'targets': tuple(target.model_copy(update={'target_id': 'foreign'}) for target in runtime_request['participant'].targets)})
    else:
        body = runtime_request['profiles'].model_dump(mode='json')
        if damage == 'source_bucket':
            body['task_images'][0]['settings']['source_bucket'] = 'foreign-private-data'
        else:
            body['execution'][0]['runtime']['namespace'] = 'foreign-work'
        runtime_request['profiles'] = PoolProfileCatalog.model_validate(body)
    with pytest.raises(ValueError, match='environment_runtime_unqualified'):
        render(**runtime_request)
