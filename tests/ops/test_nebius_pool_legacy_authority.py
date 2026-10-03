"""Restored permissions belong to one retained identity and destination only."""
from __future__ import annotations

import copy
import json

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_role_fencing import role_fence_documents
from tests.ops.test_nebius_pool_role_fencing import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_role_fencing import management_inputs as management_inputs
from tests.ops.test_nebius_pool_role_fencing import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_role_fencing import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_fencing import runtime_inputs as runtime_inputs


def review(rules):
    return {'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {},
        'status': {'incomplete': False, 'evaluationError': '', 'resourceRules': rules,
            'nonResourceRules': [{'verbs': ['get'], 'nonResourceURLs': ['/api', '/apis/*']}]}}


@pytest.mark.parametrize('destination', ['execution', 'build', 'other_owner', 'platform', 'foreign'])
@pytest.mark.parametrize('restored', [False, True])
def test_effective_rights_follow_only_this_subjects_binding_in_this_namespace(fencing_inputs, destination, restored):
    from scripts.ops.nebius_pool_legacy_authority import qualify_legacy_rules

    request = fencing_inputs
    participants = request.retirement.migration.registration.spec.participants
    own, other = participants[:2]
    namespace = {'execution': own.execution_namespace.name, 'build': own.build_namespace.name,
        'other_owner': other.execution_namespace.name, 'platform': request.retirement.migration.guards[0].namespace,
        'foreign': 'foreign-team'}[destination]
    roles = role_fence_documents(request)
    # Only the first owner's execution Role is restored. Its build role and the
    # other owners still have their reduced rights.
    if restored:
        original = next(row for row in request.originals if row['metadata']['namespace'] == own.execution_namespace.name)
        roles[_key(original)] = copy.deepcopy(original)
    rules = [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
        'resourceNames': [own.execution_namespace.name, own.build_namespace.name]}]
    if destination == 'execution' and restored:
        rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'create', 'delete']})
    elif destination in {'execution', 'build'}:
        rules.extend([{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
            {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
            {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}])
    rules.extend([{'apiGroups': [''], 'resources': ['nodes/stats'], 'verbs': ['get']},
        {'apiGroups': ['authorization.k8s.io'], 'resources': ['selfsubjectrulesreviews'], 'verbs': ['create']}])
    actual = review(rules)
    saved = copy.deepcopy(actual)
    subject = (own.execution_namespace.name, 'loom-execution-actuator')
    qualify_legacy_rules(actual, request=request, roles=roles, subject=subject, namespace=namespace)
    assert actual == saved
    # Restoring a Role elsewhere must never justify writes here. In the exact
    # restored namespace, patch remains an excess grant too.
    rules.append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['patch' if destination == 'execution' and restored else 'create'],
        'resourceNames': ['hidden-job']})
    with pytest.raises(ValueError):
        qualify_legacy_rules(actual, request=request, roles=roles, subject=subject, namespace=namespace)


@pytest.mark.parametrize('damage', ['missing', 'collector', 'other_owner', 'secrets', 'exec', 'token',
    'incomplete', 'evaluation', 'non_resource', 'wildcard', 'huge', 'unknown_subject'])
def test_legacy_reviews_reject_missing_or_cross_subject_rights_and_unsafe_extras(fencing_inputs, damage):
    from scripts.ops.nebius_pool_legacy_authority import qualify_legacy_rules

    request = fencing_inputs
    own, other = request.retirement.migration.registration.spec.participants[:2]
    roles = {_key(row): copy.deepcopy(row) for row in request.originals}
    subject = (own.execution_namespace.name, 'loom-execution-actuator')
    namespace = own.execution_namespace.name
    rules = [{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
        'resourceNames': [own.execution_namespace.name, own.build_namespace.name]},
        {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'create', 'delete']}]
    actual = review(rules)
    if damage == 'missing':
        rules[-1]['verbs'].remove('delete')
    elif damage == 'collector':
        subject = (own.execution_namespace.name, 'loom-execution-capacity-collector')
    elif damage == 'other_owner':
        namespace = other.execution_namespace.name
    elif damage in {'secrets', 'exec', 'token'}:
        rules.append({'apiGroups': [''], 'resources': [{'secrets': 'secrets', 'exec': 'pods/exec', 'token': 'serviceaccounts/token'}[damage]],
            'verbs': ['create' if damage == 'token' else 'get'], 'resourceNames': ['hidden']})
    elif damage == 'incomplete':
        actual['status']['incomplete'] = True
    elif damage == 'evaluation':
        actual['status']['evaluationError'] = 'private-marker'
    elif damage == 'non_resource':
        actual['status']['nonResourceRules'][0]['nonResourceURLs'] = ['/*']
    elif damage == 'wildcard':
        rules[-1]['verbs'] = ['*']
    elif damage == 'huge':
        rules[-1]['verbs'] = ['get'] * 1001
    else:
        subject = (namespace, 'unknown-account')
    with pytest.raises(ValueError) as error:
        qualify_legacy_rules(actual, request=request, roles=roles, subject=subject, namespace=namespace)
    assert 'private-' not in str(error.value)


@pytest.mark.parametrize('kind,name,relevant', [
    ('ServiceAccount', 'loom-execution-actuator', True),
    ('ServiceAccount', 'another-account', False),
    ('User', 'system:serviceaccount:NAMESPACE:loom-execution-actuator', True),
    ('Group', 'system:serviceaccounts:NAMESPACE', True),
    ('Group', 'system:serviceaccounts', True), ('Group', 'system:authenticated', True),
    ('Group', 'system:serviceaccounts:foreign', False),
])
def test_retained_subject_reviews_discover_relevant_foreign_namespaces(fencing_inputs, kind, name, relevant):
    from scripts.ops.nebius_pool_gateway_authority import subject_review_namespaces

    request = fencing_inputs.retirement.migration
    namespace = request.registration.spec.participants[0].execution_namespace.name
    item = {'kind': kind, 'name': name.replace('NAMESPACE', namespace)}
    item.update({'namespace': namespace} if kind == 'ServiceAccount' else {'apiGroup': 'rbac.authorization.k8s.io'})
    binding = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
        'metadata': {'namespace': 'outside-pool'}, 'subjects': [item]}
    observed = subject_review_namespaces(request, [binding], subject=(namespace, 'loom-execution-actuator'))
    assert ('outside-pool' in observed) is relevant
    assert {request.registration.binding.namespace, *(row.namespace for row in request.guards),
        *(ns.name for row in request.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))} <= set(observed)


@pytest.mark.parametrize('failure', [None, 'transport', 'oversize', 'encoding', 'incomplete', 'wrong_subject'])
def test_restored_authority_reviews_use_fixed_groups_bounded_response_and_no_retry(fencing_inputs, failure):
    from scripts.ops.nebius_pool_legacy_authority import review_legacy_rules

    request = fencing_inputs
    own = request.retirement.migration.registration.spec.participants[0]
    subject = (own.execution_namespace.name, 'loom-execution-actuator')
    calls = []

    def respond(message):
        calls.append(message)
        assert message.method == 'POST' and message.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
        assert message.headers['Impersonate-User'] == f'system:serviceaccount:{subject[0]}:{subject[1]}'
        assert message.headers.get_list('Impersonate-Group') == [
            'system:serviceaccounts', 'system:serviceaccounts:' + subject[0], 'system:authenticated']
        assert json.loads(message.content) == {'apiVersion': 'authorization.k8s.io/v1',
            'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': 'foreign-team'}}
        if failure == 'transport':
            raise httpx.ReadTimeout('private-marker')
        if failure == 'oversize':
            return httpx.Response(201, content=b' ' * (4 * 1024**2 + 1))
        if failure == 'encoding':
            return httpx.Response(201, headers={'content-encoding': 'unsupported'})
        result = review([{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
            'resourceNames': [own.execution_namespace.name, own.build_namespace.name]}])
        result['status']['incomplete'] = failure == 'incomplete'
        return httpx.Response(201, json=result)

    with httpx.Client(base_url='https://cluster.example', transport=httpx.MockTransport(respond)) as client:
        if failure:
            with pytest.raises(ValueError) as error:
                review_legacy_rules(client, request=request, roles=role_fence_documents(request),
                    subject=(subject[0], 'unretained') if failure == 'wrong_subject' else subject, namespace='foreign-team')
            assert 'private-' not in str(error.value)
        else:
            review_legacy_rules(client, request=request, roles=role_fence_documents(request), subject=subject, namespace='foreign-team')
        assert len(calls) == (0 if failure == 'wrong_subject' else 1)
        assert 'Impersonate-User' not in client.headers and 'Impersonate-Group' not in client.headers
