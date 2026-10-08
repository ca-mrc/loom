"""Connected dev pool preparation cannot become staging or runtime authority."""
from __future__ import annotations

import copy
import hashlib
import importlib
from uuid import UUID, uuid4

import pytest
from tests.integration.test_nebius_pool_installation import add_application_builder
from tests.ops.test_nebius_development_pool_registration import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_pool_registration import application_material as application_material
from tests.ops.test_nebius_development_pool_registration import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_pool_registration import cloud as cloud
from tests.ops.test_nebius_development_pool_registration import configured
from tests.ops.test_nebius_development_pool_registration import installation as installation
from tests.ops.test_nebius_development_pool_registration import inventory as inventory
from tests.ops.test_nebius_development_pool_registration import management_inputs as management_inputs
from tests.ops.test_nebius_development_pool_registration import manager_entry as manager_entry
from tests.ops.test_nebius_development_pool_registration import material as material
from tests.ops.test_nebius_development_pool_registration import platform_inputs as original_platform_inputs
from tests.ops.test_nebius_development_pool_registration import provider_checks as provider_checks
from tests.ops.test_nebius_development_pool_registration import retained as retained
from tests.ops.test_nebius_development_pool_registration import route as route
from tests.ops.test_nebius_development_pool_registration import tls_material as tls_material
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


@pytest.fixture
def platform_inputs(original_platform_inputs):
    config, candidate, profile = original_platform_inputs
    profile['runtime_binary_sha256'] = 'sha256:' + 'c' * 64
    return config, candidate, profile


def module():
    name = 'scripts.ops.nebius_development_pool_intent'
    if importlib.util.find_spec(name) is None:
        pytest.fail('connected namespace-free development pool intent is missing')
    return importlib.import_module(name)


@pytest.fixture
def pool_inputs(retained, build_inputs):
    reference, spec = configured(retained)
    value = spec.model_dump(mode='json')
    config = retained[3].deployment.installation.foundation.platform_config
    candidate, profile = retained[3].candidate, retained[3].profile
    for execution in value['profiles']['execution']:
        execution.update(candidate_sha=candidate['candidate_sha'],
            runtime_image_ref=candidate['images']['execution_runtime']['image_ref'],
            runtime_binary_sha256=profile['runtime_binary_sha256'])
    for build in value['profiles']['task_images']:
        build['settings'].update(service_image=candidate['images']['service']['image_ref'],
            storage_endpoint=config['storage_endpoint'], storage_region=config['region'],
            source_bucket=config['buckets']['source'], registry_repository=build_inputs[0].registry_repository)
    recipe = build_inputs[0].recipe.model_copy(update={
        'schema_revision': retained[3].deployment.installation.applications.shared.schema_revision})
    value, _, _ = add_application_builder(value, recipe)
    value['node_selector'] = copy.deepcopy(value['profiles']['application_images'][0]['target']['node_selector'])
    for participant in value['participants']:
        for field in ('execution_namespace', 'build_namespace'):
            participant[field].pop('uid')
    tokens = {}
    for machine in value['machines']:
        token = 'private-pool-test-' + machine['machine_id']
        tokens[UUID(machine['machine_id'])] = token
        machine['token_sha256'] = hashlib.sha256(token.encode()).hexdigest()
    return reference, value, tokens


def prepare(pool_inputs):
    reference, catalog, tokens = pool_inputs
    return module().prepare_intent(reference=reference, catalog=catalog, tokens=tokens)


def test_complete_intent_binds_only_observed_namespace_uids(pool_inputs):
    intent = prepare(pool_inputs)
    row, = pool_inputs[1]['participants']
    names = [row[field]['name'] for field in ('execution_namespace', 'build_namespace')]
    uids = dict(zip(names, (str(uuid4()), str(uuid4())), strict=True))
    request = module().bind_namespaces(intent, uids)
    participant, = request.registration.spec.participants
    assert str(participant.execution_namespace.uid) == uids[names[0]]
    assert str(participant.build_namespace.uid) == uids[names[1]]
    assert request.registration.binding.namespace == 'loom-nebius-management-dev'
    assert len(request.registration.spec.machines) == 4
    assert all('uid' not in row[field] for field in ('execution_namespace', 'build_namespace'))
    assert not any(token in repr(intent) for token in pool_inputs[2].values())


@pytest.mark.parametrize('damage', ['missing-builder', 'publication', 'runtime-binary', 'service-image',
    'schema', 'source', 'physical-group', 'supplied-uid', 'token', 'extra-token', 'missing-task-profile'])
def test_incomplete_or_foreign_catalog_is_rejected_before_namespace_creation(pool_inputs, damage):
    reference, value, tokens = pool_inputs
    participant, = value['participants']
    if damage == 'missing-builder':
        value['profiles'].pop('application_images')
        participant['targets'] = [row for row in participant['targets'] if row['workload_kinds'] != ['application_image_build']]
        removed, = [row for row in value['machines'] if row.get('workload_scope') == 'application_builder']
        value['machines'].remove(removed)
        del tokens[UUID(removed['machine_id'])]
    elif damage == 'publication':
        value['profiles']['execution'][0]['candidate_sha'] = 'd' * 40
    elif damage == 'runtime-binary':
        value['profiles']['execution'][0]['runtime_binary_sha256'] = 'sha256:' + 'd' * 64
    elif damage == 'service-image':
        value['profiles']['task_images'][0]['settings']['service_image'] = 'registry.example/foreign@sha256:' + 'd' * 64
    elif damage == 'schema':
        value['profiles']['application_images'][0]['recipe']['schema_revision'] = '9999'
    elif damage == 'source':
        value['profiles']['task_images'][0]['settings']['source_bucket'] = 'foreign-source'
    elif damage == 'physical-group':
        value['node_group_id'] = 'foreign-group'
    elif damage == 'supplied-uid':
        participant['execution_namespace']['uid'] = str(uuid4())
    elif damage == 'token':
        tokens[next(iter(tokens))] = 'changed-private-token'
    elif damage == 'extra-token':
        tokens[uuid4()] = 'extra-private-token'
    else:
        value['profiles']['task_images'] = []
    with pytest.raises(ValueError, match='development pool intent') as caught:
        module().prepare_intent(reference=reference, catalog=value, tokens=tokens)
    assert not any(token in str(caught.value) for token in tokens.values())


@pytest.mark.parametrize('damage', ['missing', 'extra', 'duplicate', 'nil'])
def test_namespace_receipt_requires_exact_distinct_non_nil_identities(pool_inputs, damage):
    intent = prepare(pool_inputs)
    participant, = pool_inputs[1]['participants']
    names = [participant[field]['name'] for field in ('execution_namespace', 'build_namespace')]
    uids = {name: str(uuid4()) for name in names}
    if damage == 'missing':
        del uids[names[0]]
    elif damage == 'extra':
        uids['loom-nebius-platform'] = str(uuid4())
    elif damage == 'duplicate':
        uids[names[1]] = uids[names[0]]
    else:
        uids[names[0]] = str(UUID(int=0))
    with pytest.raises(ValueError, match='development pool namespace'):
        module().bind_namespaces(intent, uids)


def test_fixed_delivery_keeps_credentials_scoped_and_runtime_disabled(pool_inputs):
    intent = prepare(pool_inputs)
    namespaces = module().namespace_documents(intent)
    assert len(namespaces) == 2
    assert {row['kind'] for row in namespaces.values()} == {'Namespace'}
    assert all(row['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'
        for row in namespaces.values())
    uids = {row['metadata']['name']: str(uuid4()) for row in namespaces.values()}
    request = module().bind_namespaces(intent, uids)
    phases = module().delivery_documents(intent, request)
    assert set(phases) == {'material', 'configuration', 'workload'}
    assert {row['kind'] for row in phases['configuration'].values()} == {'ConfigMap', 'ServiceAccount'}
    gateway, = phases['workload'].values()
    assert gateway['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert gateway['spec']['replicas'] == 0
    assert gateway['spec']['template']['spec']['automountServiceAccountToken'] is False
    machines = {str(row.machine_id): row for row in request.registration.spec.machines}
    destinations = {}
    for secret in phases['material'].values():
        assert secret['immutable'] is True and set(secret['data']) == {'token'}
        identity = secret['metadata']['labels']['loom.nebius/pool-machine']
        destinations.setdefault(identity, set()).add(secret['metadata']['namespace'])
    for identity, machine in machines.items():
        assert destinations[identity] == ({'loom-nebius-management-dev'} if machine.role == 'gateway'
            or machine.workload_scope == 'application_builder' else
            ({'loom-dev', request.registration.spec.participants[0].execution_namespace.name}
             if machine.role == 'participant' else {request.registration.spec.participants[0].execution_namespace.name}))
