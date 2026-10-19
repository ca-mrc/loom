"""Independent runtime delivery uses actual foundation and process consumers."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import json
from pathlib import Path

import pytest
from tests.integration.test_nebius_pool_profile_registry import catalog
from tests.ops.test_nebius_development_build_cloud import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_development_collector_cloud import collector_cloud as collector_cloud
from tests.ops.test_nebius_development_collector_cloud import original_cloud as original_cloud
from tests.ops.test_nebius_pool_runtime import desired_profile, env
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_foundation import declaration
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.pool_management.profiles import PoolProfileCatalog

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
    identity['physical_pool_id'] = config['execution_node_group_id'] = migration.registration.spec.node_group_id
    identity['cluster_id'] = config['cluster_id'] = migration.registration.spec.cluster_id
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


def test_closed_foundation_network_allows_reader_control_traffic_not_task_database_access(runtime_request):
    result = subject().render_environment_runtime(**runtime_request)
    policies = [row for row in result.isolation if row['kind'] == 'NetworkPolicy']
    reader = [row for row in policies if row['spec']['podSelector'] == {
        'matchLabels': {'app.kubernetes.io/name': 'loom-execution-actuator'}}]
    assert reader, 'zero-execution foundation default-deny still blocks the actuator'
    rules = [rule for policy in reader for rule in policy['spec'].get('egress', [])]
    assert any(rule.get('ports') == [{'protocol': 'TCP', 'port': 5432}] and rule.get('to') == [{
        'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'app-red'}},
        'podSelector': {'matchLabels': {'app': 'loom-postgres'}}}] for rule in rules)
    assert any({'protocol': 'TCP', 'port': 443} in rule.get('ports', []) for rule in rules)
    task_rules = [rule for policy in policies if policy['spec']['podSelector'] == {
        'matchLabels': {'app.kubernetes.io/component': 'execution-unit'}} for rule in policy['spec'].get('egress', [])]
    assert all(rule.get('ports') and all(port['port'] in {53, 9100} for port in rule['ports']) for rule in task_rules)


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


@pytest.fixture
def material_request(runtime_request, publisher_cloud):
    from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope

    from loom_service.environment_management.credentials import generate_material

    cloud = publisher_cloud
    declaration = runtime_request['declaration']
    config = declaration.platform_config
    config.update(project_id=cloud.scope['project_id'], quota_parent_id=cloud.scope['tenant_id'], region=cloud.scope['region'])
    runtime_request['declaration'] = EnvironmentFoundationDeclarationV2.model_validate(declaration.model_dump() | {
        'platform_config_json': json.dumps(config)})
    profiles = runtime_request['profiles'].model_dump(mode='json')
    profiles['task_images'][0]['settings']['registry_repository'] = cloud.repositories[0]
    runtime_request['profiles'] = PoolProfileCatalog.model_validate(profiles)
    token = 'test-pool-machine-token-' + 'a' * 40
    runtime_request['machine'] = runtime_request['machine'].model_copy(update={'token_sha256': hashlib.sha256(token.encode()).hexdigest()})
    material = generate_material(namespace='app-red', tls_secret_name='test-db-tls')
    material['loom-platform-storage'] = {'access-key': 'canonical-access', 'secret-key': 'canonical-secret',
        'source-access-key': 'source-access', 'source-secret-key': 'source-secret'}
    return dict(runtime=runtime_request, foundation_material=material, token=token,
        registry_scope=DevelopmentRegistryCloudScope.model_validate(cloud.scope), registry_credential=cloud.credential)


def test_runtime_material_delivers_only_scoped_machine_and_actuator_credentials(material_request):
    from sqlalchemy.engine import make_url

    material = getattr(subject(), 'render_runtime_material', None)
    assert callable(material), 'runtime deployment still has unresolved credential references'
    documents = material(**material_request)
    secrets = {(row['metadata']['namespace'], row['metadata']['name']): {
        key: base64.b64decode(value).decode() for key, value in row['data'].items()} for row in documents}
    assert all(row['kind'] == 'Secret' and row['immutable'] is True for row in documents)
    machine = material_request['runtime']['machine']
    name = 'loom-pool-machine-' + machine.machine_id.hex
    assert secrets['app-red', name] == secrets['work-red', name] == {'token': material_request['token']}
    database = secrets['work-red', 'loom-execution-actuator-db']
    assert set(database) == {'db-url', 'ca.crt'}
    url = make_url(database['db-url'])
    assert (url.username, url.host, url.database) == ('loom_actuator', 'loom-postgres.app-red.svc', 'loom')
    assert url.password == material_request['foundation_material']['loom-platform-db']['actuator-password']
    assert dict(url.query) == {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}
    assert secrets['build-red', 'loom-task-build-source'] == {'access-key': 'canonical-access', 'secret-key': 'canonical-secret'}
    assert secrets['build-red', 'loom-task-build-registry'] == {'credentials.json': material_request['registry_credential'].decode()}
    assert len(secrets) == 5
    material_request['token'] = 'foreign-machine-token'
    with pytest.raises(ValueError, match='environment_runtime_material_unqualified'):
        material(**material_request)


@pytest.mark.parametrize('damage', ['database', 'secret_collision', 'registry', 'cache'])
def test_runtime_material_cannot_deliver_foreign_or_ambiguous_credentials(material_request, damage):
    material = getattr(subject(), 'render_runtime_material', None)
    assert callable(material), 'runtime deployment still has unresolved credential references'
    if damage == 'database':
        db = material_request['foundation_material']['loom-platform-db']
        db['actuator-url'] = db['admin-url']
    elif damage == 'registry':
        material_request['registry_scope'] = material_request['registry_scope'].model_copy(update={'project_id': 'project-foreign'})
    elif damage == 'cache':
        material_request['cache_material'] = {'access-key': 'unbound', 'secret-key': 'unbound'}
    else:
        body = material_request['runtime']['profiles'].model_dump(mode='json')
        body['task_images'][0]['settings']['registry_secret_name'] = body['task_images'][0]['settings']['source_secret_name']
        material_request['runtime']['profiles'] = PoolProfileCatalog.model_validate(body)
    with pytest.raises(ValueError, match='environment_runtime_material_unqualified'):
        material(**material_request)
