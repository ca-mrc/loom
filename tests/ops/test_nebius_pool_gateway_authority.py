"""Effective gateway rights include named and foreign-namespace bindings."""
from __future__ import annotations

import copy

import pytest
from tests.ops.test_nebius_pool_startup import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_startup import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_startup import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_startup import management_inputs as management_inputs
from tests.ops.test_nebius_pool_startup import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_startup import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_startup import runtime_inputs as runtime_inputs


@pytest.mark.parametrize('destination', ['execution', 'platform', 'foreign'])
@pytest.mark.parametrize('damage', [None, 'missing', 'named_write', 'secrets', 'namespace_wildcard',
    'wildcard', 'incomplete', 'evaluation', 'non_resource', 'huge_rule'])
def test_gateway_review_requires_exact_fixed_rights_and_complete_resolution(cutover_inputs, destination, damage):
    from scripts.ops.nebius_pool_cutover import cutover_documents
    from scripts.ops.nebius_pool_gateway_authority import qualify_gateway_rules

    request, _ = cutover_inputs
    migration = request.fencing.retirement.migration
    spec = migration.registration.spec
    namespace = {'execution': spec.participants[0].execution_namespace.name,
        'platform': migration.guards[0].namespace, 'foreign': 'foreign-team'}[destination]
    pool_names = sorted({ns.name for row in spec.participants for ns in (row.execution_namespace, row.build_namespace)})
    rules = [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'], 'resourceNames': pool_names}]
    if destination == 'execution':
        rules.extend([
            {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'create', 'delete']},
            {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get', 'create', 'delete']},
            {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list', 'delete']},
        ])
    review = {'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {},
        'status': {'incomplete': False, 'evaluationError': '', 'resourceRules': rules,
            'nonResourceRules': [{'verbs': ['get'], 'nonResourceURLs': ['/api', '/apis/*', '/version']}]}}
    rules.append({'apiGroups': ['authorization.k8s.io'], 'resources': ['selfsubjectaccessreviews', 'selfsubjectrulesreviews'], 'verbs': ['create']})
    if damage == 'missing':
        rules.pop(0)
    elif damage == 'named_write':
        rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['patch'], 'resourceNames': ['hidden-job']})
    elif damage == 'secrets':
        rules.append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get'], 'resourceNames': ['hidden-secret']})
    elif damage == 'namespace_wildcard':
        rules[0].pop('resourceNames')
    elif damage == 'wildcard':
        rules.append({'apiGroups': ['*'], 'resources': ['*'], 'verbs': ['*']})
    elif damage == 'incomplete':
        review['status']['incomplete'] = True
    elif damage == 'evaluation':
        review['status']['evaluationError'] = 'private-resolver-marker'
    elif damage == 'non_resource':
        review['status']['nonResourceRules'].append({'verbs': ['get'], 'nonResourceURLs': ['/*']})
    elif damage == 'huge_rule':
        rules.append({'apiGroups': ['batch'] * 1001, 'resources': ['jobs'], 'verbs': ['get']})
    before = copy.deepcopy(review)
    if damage:
        with pytest.raises(ValueError) as error:
            qualify_gateway_rules(review, namespace=namespace, authority=cutover_documents(request)['authority'])
        assert 'private-' not in str(error.value)
    else:
        qualify_gateway_rules(review, namespace=namespace, authority=cutover_documents(request)['authority'])
    assert review == before


@pytest.mark.parametrize('subject', [
    {'kind': 'ServiceAccount', 'name': 'loom-pool-gateway'},
    {'kind': 'User', 'name': 'system:serviceaccount:MANAGER:loom-pool-gateway', 'apiGroup': 'rbac.authorization.k8s.io'},
    {'kind': 'Group', 'name': 'system:serviceaccounts:MANAGER', 'apiGroup': 'rbac.authorization.k8s.io'},
    {'kind': 'Group', 'name': 'system:serviceaccounts', 'apiGroup': 'rbac.authorization.k8s.io'},
    {'kind': 'Group', 'name': 'system:authenticated', 'apiGroup': 'rbac.authorization.k8s.io'},
    {'kind': 'ServiceAccount', 'name': 'foreign-reader'},
])
def test_effective_review_includes_every_namespace_with_a_gateway_subject_or_group(cutover_inputs, subject):
    from scripts.ops.nebius_pool_gateway_authority import gateway_review_namespaces

    request, _ = cutover_inputs
    migration = request.fencing.retirement.migration
    manager = migration.registration.binding.namespace
    subject = copy.deepcopy(subject)
    subject['name'] = subject['name'].replace('MANAGER', manager)
    if subject['kind'] == 'ServiceAccount':
        subject['namespace'] = manager
    bindings = [{'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'name': 'extra-grant', 'namespace': 'foreign-team'}, 'subjects': [subject]}]
    expected = {manager, *(guard.namespace for guard in migration.guards),
        *(ns.name for participant in migration.registration.spec.participants for ns in (participant.execution_namespace, participant.build_namespace))}
    if subject['name'] != 'foreign-reader':
        expected.add('foreign-team')
    assert gateway_review_namespaces(migration, bindings) == tuple(sorted(expected))
