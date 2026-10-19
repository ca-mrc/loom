"""Independent shared foundations render closed without a staging template identity."""
from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from tests.support.nebius_platform import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def module():
    name = 'loom.nebius_environment_foundation'
    assert importlib.util.find_spec(name), 'generic protected environment foundation is missing'
    return importlib.import_module(name)


def declaration(platform_inputs, *, kind='development', suffix='red', backups=False):
    subject = module()
    config = copy.deepcopy(platform_inputs[0])
    registration = dict(schema_version='loom.environment-foundation-identity.v2', environment_id=str(uuid4()),
        incarnation=str(uuid4()), participant_id=str(uuid4()), installation_id=str(uuid4()), pool_id=str(uuid4()),
        kind=kind, slug=suffix, cluster_id=config['cluster_id'], physical_pool_id=config['execution_node_group_id'],
        application_namespace='app-' + suffix, execution_namespace='work-' + suffix, build_namespace='build-' + suffix,
        public_host=suffix + '.env.example.com', target_id='target-' + suffix)
    config.update(schema_version='loom.nebius-environment-platform.v2', registration=registration,
        namespace=registration['application_namespace'], execution_namespace=registration['execution_namespace'],
        environment=kind, target_id=registration['target_id'], public_host=registration['public_host'], public_tls_bootstrap=False)
    config['capacity_policy']['enabled'] = False
    for key in ('task_image_builder', 'task_identity_policy', 'shared_ingress_enabled'):
        config.pop(key, None)
    config['buckets'] = dict(artifacts=suffix + '-canonical', trajectories=suffix + '-canonical', source=suffix + '-source', backup=suffix + '-backup')
    return subject.EnvironmentFoundationDeclarationV2(schema_version='loom.environment-foundation.v2', operation_id=uuid4(),
        registration=registration, platform_config_json=json.dumps(config), public_dns_zone='env.example.com',
        ingress_class_name='shared-https', ingress_namespace='platform-ingress', ingress_controller_label='edge',
        node_selector={'platform.example/role': 'applications'}, tolerations=(), backups_enabled=backups)


@pytest.mark.parametrize('kind', ['development', 'staging', 'production'])
def test_generic_foundations_use_own_explicit_names_data_and_closed_execution(platform_inputs, kind):
    subject = module()
    wanted = declaration(platform_inputs, kind=kind, backups=kind == 'production')
    original = wanted.model_dump(mode='json')
    result = subject.render_foundation(wanted, platform_inputs[1], profile=platform_inputs[2], keyring={}, repo_root=ROOT)
    documents = [doc for group in result.files.values() for doc in group]
    assert wanted.model_dump(mode='json') == original
    assert result.execution_enabled is False
    assert {doc['metadata']['name'] for doc in documents if doc['kind'] == 'Namespace'} == {'app-red', 'work-red', 'build-red'}
    assert all(doc['metadata']['namespace'] in {'app-red', 'work-red', 'build-red'} for doc in documents if doc['kind'] != 'Namespace')
    assert not any(doc['kind'] in {'ClusterRole', 'ClusterRoleBinding', 'Secret'} for doc in documents)
    assert {doc['metadata']['namespace'] for doc in documents if doc['kind'] == 'ResourceQuota' and doc['spec']['hard'].get('pods') == '0'} == {'work-red', 'build-red'}
    database, = (doc for doc in documents if doc['kind'] == 'StatefulSet')
    assert database['metadata']['namespace'] == 'app-red'
    ingress, = (doc for doc in documents if doc['kind'] == 'Ingress')
    assert ingress['spec']['tls'] == [{'hosts': ['red.env.example.com']}]
    assert any(doc['kind'] == 'CronJob' for doc in documents) == (kind == 'production')
    for doc in documents:
        if doc['kind'] not in {'Deployment', 'StatefulSet', 'Job', 'CronJob'}:
            continue
        spec = doc['spec']['jobTemplate']['spec'] if doc['kind'] == 'CronJob' else doc['spec']
        pod = spec['template']['spec']
        assert pod['nodeSelector'] == {'platform.example/role': 'applications'}
        for container in pod['containers']:
            environment = {row['name']: row.get('value') for row in container.get('env', [])}
            if 'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED' in environment:
                assert environment['LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED'] == 'false'
    assert result.config['buckets']['artifacts'] == result.config['buckets']['trajectories'] == 'red-canonical'
    assert not any(doc['kind'] in {'Deployment', 'Job', 'CronJob'} and doc['metadata']['namespace'] in {'work-red', 'build-red'}
        for doc in documents), 'foundation creation cannot install standalone execution writers'


def test_two_same_class_foundations_do_not_select_each_other(platform_inputs):
    subject = module()
    a, b = [subject.render_foundation(declaration(platform_inputs, kind='staging', suffix=name),
        platform_inputs[1], profile=platform_inputs[2], keyring={}, repo_root=ROOT) for name in ('red', 'blue')]
    assert a.registration.kind == b.registration.kind == 'staging'
    assert not set(a.registration.namespaces) & set(b.registration.namespaces)
    assert not set(a.config['buckets'].values()) & set(b.config['buckets'].values())
    assert a.registration.physical_pool_id == b.registration.physical_pool_id


@pytest.mark.parametrize('damage', ['namespace', 'pool', 'class', 'standalone', 'dns', 'production_recovery'])
def test_foundation_cannot_smuggle_other_identity_or_allocator(platform_inputs, damage):
    subject = module()
    wanted = declaration(platform_inputs)
    body = wanted.model_dump(mode='json')
    config = json.loads(body['platform_config_json'])
    if damage == 'namespace':
        config['namespace'] = 'foreign'
    elif damage == 'pool':
        config['execution_node_group_id'] = 'foreign'
    elif damage == 'class':
        config['environment'] = 'production'
    elif damage == 'standalone':
        config['capacity_policy']['enabled'] = True
    elif damage == 'dns':
        body['public_dns_zone'] = 'foreign.example'
    else:
        body['registration']['kind'] = 'production'
        config['registration']['kind'] = config['environment'] = 'production'
    body['platform_config_json'] = json.dumps(config)
    with pytest.raises(ValueError):
        subject.EnvironmentFoundationDeclarationV2.model_validate(body)
