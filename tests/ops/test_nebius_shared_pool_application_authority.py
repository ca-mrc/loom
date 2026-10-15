"""Personal lifecycle permissions are not generic readers or pool writers."""
from __future__ import annotations

import copy
from uuid import UUID, uuid4

import pytest

from loom.nebius_application_authority import (
    ApplicationNamespaceAuthorityV1,
    application_namespace_binding,
    render_application_authority,
)


def key(row):
    return f"{row['kind']}:{row['metadata'].get('namespace', '-')}:{row['metadata']['name']}"


@pytest.fixture
def authority_case():
    domain = ApplicationNamespaceAuthorityV1(installation_id=UUID('74adc025-372e-4229-8e8b-61074600073c'),
        namespace='loom-nebius-management-dev', cluster_id='mk8scluster-test',
        data_environment_id=UUID('fcce8b80-227c-48b8-8855-763ad083942c'), shared_namespace='loom-dev')
    fixed = render_application_authority(domain)
    for row in fixed:
        row['metadata'].update(uid=str(uuid4()), resourceVersion='10')
    namespace = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
        'name': 'loom-dev-test-owner', 'uid': str(uuid4()), 'resourceVersion': '10',
        'labels': {'loom.nebius/application-installation': str(domain.installation_id),
            'loom.nebius/data-environment-id': str(domain.data_environment_id),
            'loom.nebius/application-id': '4d2e033c-2384-4d21-845b-467df44ac7d9',
            'loom.nebius/incarnation': '75008a68-0487-470a-bb4c-c704f0099ae5',
            'pod-security.kubernetes.io/enforce': 'restricted'}}}
    binding = application_namespace_binding(domain, namespace['metadata']['name'])
    binding['metadata'].update(uid=str(uuid4()), resourceVersion='10',
        labels={name: namespace['metadata']['labels'][name]
                for name in ('loom.nebius/application-id', 'loom.nebius/incarnation')},
        annotations={'loom.nebius/operation-id': '8eb4ed5a-3629-42e7-81e1-15161d96a7fc',
            'loom.nebius/deployment-generation': '1', 'loom.nebius/effect-key': 'namespace-binding'})
    current = [*copy.deepcopy(fixed), copy.deepcopy(namespace), binding]
    return dict(authorities=(domain,), retained={key(row): row for row in fixed},
        current={key(row): row for row in current}, forbidden_namespaces=frozenset({
            'loom-dev', 'loom-nebius-management-dev', 'loom-nebius-execution', 'loom-nebius-build'})), key(binding)


def test_exact_personal_binding_has_separate_scope_evidence(authority_case):
    from scripts.ops.nebius_shared_pool_application_authority import qualify_application_bindings

    arguments, selected = authority_case
    bindings, scope = qualify_application_bindings(**arguments)
    assert set(bindings) == {selected}
    assert bindings[selected]['roleRef']['kind'] == 'ClusterRole'
    assert 'Namespace:-:loom-dev-test-owner' in scope
    assert any(row['kind'] == 'ValidatingAdmissionPolicy' for row in scope.values())
    # The returned observations cannot mutate retained/current authority.
    bindings[selected]['subjects'].clear()
    assert arguments['current'][selected]['subjects']


@pytest.mark.parametrize('damage', ['foreign_installation', 'foreign_data', 'nil_application', 'nil_incarnation',
    'privileged_namespace', 'legacy_namespace', 'terminating_namespace', 'missing_namespace', 'protected_namespace',
    'binding_subject', 'binding_group', 'binding_application', 'binding_incarnation', 'binding_operation',
    'binding_generation', 'binding_extra_field', 'cluster_binding', 'expanded_role', 'replaced_role',
    'missing_policy', 'replaced_policy', 'policy_ignore', 'policy_selector', 'policy_condition',
    'policy_extra_validation', 'policy_binding_audit', 'unretained_policy', 'retained_expansion',
    'cluster_binding_foreign', 'namespace_owner', 'namespace_installation', 'namespace_reserved',
    'binding_nil_uid', 'binding_generation_overflow', 'role_aggregation'])
def test_personal_scope_cannot_hide_other_authority(authority_case, damage):
    from scripts.ops.nebius_shared_pool_application_authority import qualify_application_bindings

    arguments, selected = authority_case
    current = arguments['current']
    namespace = current['Namespace:-:loom-dev-test-owner']
    labels = namespace['metadata']['labels']
    binding = current[selected]
    domain = arguments['authorities'][0]
    role = current['ClusterRole:-:' + domain.name + '-resources']
    policy_key = 'ValidatingAdmissionPolicy:-:' + domain.name + '-bindings'
    policy = current[policy_key]
    if damage == 'foreign_installation':
        labels['loom.nebius/application-installation'] = str(uuid4())
    elif damage == 'foreign_data':
        labels['loom.nebius/data-environment-id'] = str(uuid4())
    elif damage in {'nil_application', 'nil_incarnation'}:
        labels['loom.nebius/' + ('application-id' if damage == 'nil_application' else 'incarnation')] = str(UUID(int=0))
    elif damage == 'privileged_namespace':
        labels['pod-security.kubernetes.io/enforce'] = 'privileged'
    elif damage == 'legacy_namespace':
        labels['loom.nebius/environment-id'] = str(uuid4())
    elif damage == 'terminating_namespace':
        namespace['metadata']['deletionTimestamp'] = '2026-10-15T00:00:00Z'
    elif damage == 'missing_namespace':
        del current[key(namespace)]
    elif damage == 'protected_namespace':
        arguments['forbidden_namespaces'] |= {'loom-dev-test-owner'}
    elif damage in {'binding_subject', 'binding_group'}:
        binding['subjects'] = [{'kind': 'Group', 'name': 'system:authenticated', 'apiGroup': 'rbac.authorization.k8s.io'}]
        if damage == 'binding_subject':
            binding['subjects'] = [{'kind': 'ServiceAccount', 'name': 'foreign', 'namespace': domain.namespace}]
    elif damage in {'binding_application', 'binding_incarnation'}:
        binding['metadata']['labels']['loom.nebius/' + (
            'application-id' if damage == 'binding_application' else 'incarnation')] = str(uuid4())
    elif damage in {'binding_operation', 'binding_generation'}:
        binding['metadata']['annotations']['loom.nebius/' + (
            'operation-id' if damage == 'binding_operation' else 'deployment-generation')] = '0'
    elif damage == 'binding_extra_field':
        binding['extraAuthority'] = True
    elif damage in {'cluster_binding', 'cluster_binding_foreign'}:
        del current[selected]
        binding['kind'] = 'ClusterRoleBinding'
        binding['metadata'].pop('namespace')
        current[key(binding)] = binding
        if damage == 'cluster_binding_foreign':
            binding['subjects'][0]['name'] = 'foreign'
    elif damage == 'expanded_role':
        role['rules'].append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
    elif damage == 'replaced_role':
        role['metadata']['uid'] = str(uuid4())
    elif damage == 'missing_policy':
        del current[policy_key]
    elif damage == 'replaced_policy':
        policy['metadata']['uid'] = str(uuid4())
    elif damage == 'policy_ignore':
        policy['spec']['failurePolicy'] = 'Ignore'
    elif damage == 'policy_selector':
        policy['spec']['matchConstraints']['namespaceSelector'] = {'matchLabels': {'bypass': 'yes'}}
    elif damage == 'policy_condition':
        policy['spec']['matchConditions'].append({'name': 'bypass', 'expression': 'false'})
    elif damage == 'policy_extra_validation':
        policy['spec']['validations'] = [{'expression': 'true'}]
    elif damage == 'policy_binding_audit':
        current['ValidatingAdmissionPolicyBinding:-:' + domain.name + '-bindings']['spec']['validationActions'] = ['Audit']
    elif damage == 'retained_expansion':
        role['rules'].append({'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']})
        arguments['retained'][key(role)] = copy.deepcopy(role)
    elif damage == 'namespace_owner':
        namespace['metadata']['ownerReferences'] = [{'kind': 'Namespace', 'uid': str(uuid4())}]
    elif damage == 'namespace_installation':
        labels['loom.nebius/namespace-installation'] = str(uuid4())
    elif damage == 'namespace_reserved':
        del current[key(namespace)]
        namespace['metadata']['name'] = 'loom-dev-shared'
        current[key(namespace)] = namespace
        del current[selected]
        binding['metadata']['namespace'] = 'loom-dev-shared'
        current[key(binding)] = binding
    elif damage == 'binding_nil_uid':
        binding['metadata']['uid'] = str(UUID(int=0))
    elif damage == 'binding_generation_overflow':
        binding['metadata']['annotations']['loom.nebius/deployment-generation'] = str(2**63)
    elif damage == 'role_aggregation':
        role['aggregationRule'] = {'clusterRoleSelectors': [{}]}
    else:
        del arguments['retained'][policy_key]
    with pytest.raises(ValueError, match='application lifecycle authority unqualified'):
        qualify_application_bindings(**arguments)


def test_harmless_kubernetes_defaults_do_not_require_new_policy(authority_case):
    from scripts.ops.nebius_shared_pool_application_authority import qualify_application_bindings

    arguments, selected = authority_case
    for values in (arguments['current'], arguments['retained']):
        for row in values.values():
            if row['kind'] == 'ValidatingAdmissionPolicy':
                constraints = row['spec']['matchConstraints']
                constraints.update(matchPolicy='Equivalent', namespaceSelector={}, objectSelector={})
                for rule in constraints['resourceRules']:
                    rule['scope'] = '*'
            if row['kind'] in {'RoleBinding', 'ClusterRoleBinding'}:
                for subject in row['subjects']:
                    subject['apiGroup'] = ''
    assert set(qualify_application_bindings(**arguments)[0]) == {selected}


def test_retained_installer_marker_is_identity_not_extra_permission(authority_case):
    from scripts.ops.nebius_shared_pool_application_authority import qualify_application_bindings

    arguments, selected = authority_case
    for key in arguments['retained']:
        for values in (arguments['retained'], arguments['current']):
            values[key]['metadata']['annotations'] = {
                'loom.nebius/management-stage-operation': 'fe8862e4-274b-420c-bdbd-f310124c2c8c'}
    assert set(qualify_application_bindings(**arguments)[0]) == {selected}


@pytest.mark.parametrize('api,resource,kind', [('v1', 'namespaces', 'Namespace'),
    ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicies', 'ValidatingAdmissionPolicy'),
    ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicybindings', 'ValidatingAdmissionPolicyBinding')])
def test_scope_inventory_is_complete_and_version_pinned(api, resource, kind):
    from scripts.ops.nebius_management_prerequisites import (
        ManagementPrerequisiteError,
        inventory_resources,
    )

    calls = []
    versions = ['10', '10']

    def read(method, path):
        calls.append((method, path))
        page = len(calls) - 1
        return {'apiVersion': api, 'kind': kind + 'List',
            'metadata': {'resourceVersion': versions[page], 'continue': 'next' if page == 0 else ''},
            'items': [{'metadata': {'name': 'scope-' + str(page)}}]}

    rows = inventory_resources(read, api, resource, kind)
    assert [row['metadata']['name'] for row in rows] == ['scope-0', 'scope-1']
    assert all(method == 'GET' and resource + '?' in path for method, path in calls)
    assert 'continue=next' in calls[-1][1]
    calls.clear()
    versions[1] = '11'
    with pytest.raises(ManagementPrerequisiteError):
        inventory_resources(read, api, resource, kind)


@pytest.mark.parametrize('recovering', [False, True])
@pytest.mark.parametrize('damage', ['namespace_uid', 'namespace_labels', 'policy_uid', 'missing_scope'])
def test_shared_parent_rejects_application_scope_drift_before_process_barrier(
        authority_case, monkeypatch, tmp_path, recovering, damage):
    from types import SimpleNamespace

    from scripts.ops import nebius_shared_pool_api as module

    arguments, _ = authority_case
    scope = {name: copy.deepcopy(row) for name, row in arguments['current'].items()
        if row['kind'] in {'Namespace', 'ValidatingAdmissionPolicy'}}
    changed = copy.deepcopy(scope)
    if damage == 'missing_scope':
        changed.clear()
    else:
        kind = 'ValidatingAdmissionPolicy' if damage == 'policy_uid' else 'Namespace'
        target = next(row for row in changed.values() if row['kind'] == kind)
        if damage.endswith('_uid'):
            target['metadata']['uid'] = str(uuid4())
        else:
            target['metadata']['labels']['loom.nebius/incarnation'] = str(uuid4())
    plan = object()
    observations = iter([module.SharedPoolWriterObservation(plan, '10', {}, {}, 'first', scope),
        module.SharedPoolWriterObservation(plan, '11', {}, {}, 'second', changed)])
    monkeypatch.setattr(module, 'observe_shared_pool_recovery_writers' if recovering else
        'observe_shared_pool_forward_writers', lambda **_: next(observations))
    monkeypatch.setattr(module, '_files', lambda _: {})

    async def external(_):
        pass

    def barrier(**_):
        pytest.fail('changed personal scope reached the process barrier')

    monkeypatch.setattr(module, 'observe_shared_pool_forward_processes', barrier)
    monkeypatch.setattr(module, 'observe_shared_pool_recovery_databases', barrier)
    api = SimpleNamespace(_state=lambda: ({}, recovering), shared_state=tmp_path,
        _captured=SimpleNamespace(plan=plan), _external=external, _permissions=lambda *_, **__: None,
        guards=SimpleNamespace(kubeconfig=tmp_path / 'not-read'), prefix=['/usr/bin/kubectl'])
    with pytest.raises(ValueError, match='shared writer authority changed'):
        module.HTTPSSharedPoolOperationAPI._observe(api)


@pytest.mark.parametrize('recovering', [False, True])
@pytest.mark.parametrize('damage', [None, 'namespace_uid', 'namespace_labels', 'policy_uid', 'binding_uid', 'missing'])
def test_shared_parent_rechecks_personal_scope_during_process_barrier(
        authority_case, monkeypatch, tmp_path, recovering, damage):
    from types import SimpleNamespace

    from scripts.ops import nebius_shared_pool_api as module

    arguments, _ = authority_case
    scope = {name: copy.deepcopy(row) for name, row in arguments['current'].items()
        if row['kind'] in {'Namespace', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}
    paths = {'Namespace': '/api/v1/namespaces/',
        'ValidatingAdmissionPolicy': '/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/',
        'ValidatingAdmissionPolicyBinding': '/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/'}
    live = {paths[row['kind']] + row['metadata']['name']: copy.deepcopy(row) for row in scope.values()}
    captured = SimpleNamespace(plan=object())
    writers = module.SharedPoolWriterObservation(captured.plan, '10', {}, {}, 'census', scope)
    monkeypatch.setattr(module, 'observe_shared_pool_recovery_writers' if recovering else
        'observe_shared_pool_forward_writers', lambda **_: writers)
    monkeypatch.setattr(module, '_files', lambda _: {})
    databases = SimpleNamespace(bindings=captured)
    monkeypatch.setattr(module, 'observe_shared_pool_recovery_databases', lambda **_: databases)
    result = SimpleNamespace(bindings=captured, pending_workloads=())
    reads = []

    def process(**options):
        if damage:
            kind = ('ValidatingAdmissionPolicy' if damage == 'policy_uid' else
                'ValidatingAdmissionPolicyBinding' if damage == 'binding_uid' else 'Namespace')
            path, row = next((path, row) for path, row in live.items() if row['kind'] == kind)
            if damage == 'missing':
                del live[path]
            elif damage.endswith('_uid'):
                row['metadata']['uid'] = str(uuid4())
            else:
                row['metadata']['labels']['loom.nebius/incarnation'] = str(uuid4())
        if options.get('recheck_application_scopes') is not None:
            options['recheck_application_scopes']()
        return result

    monkeypatch.setattr(module, 'observe_shared_pool_recovery_processes' if recovering else
        'observe_shared_pool_forward_processes', process)

    async def external(_):
        pass

    def request(method, path):
        assert method == 'GET' and '?' not in path
        reads.append(path)
        return copy.deepcopy(live.get(path))

    api = SimpleNamespace(_state=lambda: ({}, recovering), shared_state=tmp_path, _captured=captured,
        _external=external, _permissions=lambda *_, **__: None, _request=request, _scope=lambda: None,
        guards=SimpleNamespace(kubeconfig=tmp_path / 'not-read'), prefix=['/usr/bin/kubectl'])
    if damage:
        with pytest.raises(ValueError):
            module.HTTPSSharedPoolOperationAPI._observe(api)
    else:
        assert module.HTTPSSharedPoolOperationAPI._observe(api) is result
        assert set(reads) == set(live)
