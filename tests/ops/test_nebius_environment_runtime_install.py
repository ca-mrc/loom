"""Runtime installation must not accept defaulting that expands authority."""
from __future__ import annotations

import copy

import pytest
from scripts.ops import nebius_environment_runtime_install as installer
from scripts.ops.nebius_environment_runtime import render_environment_runtime
from scripts.ops.nebius_environment_runtime_install import _defaulted
from scripts.ops.nebius_management_stage import ManagementStageError
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

from loom.nebius_environment_foundation import render_foundation


@pytest.mark.parametrize('damage', ['aggregation', 'condition', 'selector', 'account'])
def test_runtime_preview_refuses_authority_or_enforcement_changes(runtime_request, damage):
    rendered = render_environment_runtime(**runtime_request)
    kind = {'aggregation': 'ClusterRole', 'condition': 'ValidatingAdmissionPolicy',
        'selector': 'ValidatingAdmissionPolicyBinding', 'account': 'ServiceAccount'}[damage]
    desired = next(doc for doc in (*rendered.readers, *rendered.isolation) if doc['kind'] == kind)
    observed = copy.deepcopy(desired)
    if damage == 'aggregation':
        observed['aggregationRule'] = {'clusterRoleSelectors': [{'matchLabels': {'foreign': 'true'}}]}
    elif damage == 'condition':
        observed['spec']['matchConditions'] = [{'name': 'bypass', 'expression': 'false'}]
    elif damage == 'selector':
        observed['spec']['matchResources']['objectSelector'] = {'matchLabels': {'foreign': 'true'}}
    elif damage == 'account':
        observed['imagePullSecrets'] = [{'name': 'foreign'}]

    class API:
        def default_resource(self, document):
            return observed

    with pytest.raises((ValueError, ManagementStageError)):
        _defaulted(API(), desired)


def test_runtime_preview_accepts_only_inert_policy_and_subject_defaults(runtime_request):
    rendered = render_environment_runtime(**runtime_request)
    for desired in (*rendered.readers, *rendered.isolation):
        observed = copy.deepcopy(desired)
        if desired['kind'] in {'RoleBinding', 'ClusterRoleBinding'}:
            for subject in observed['subjects']:
                subject['apiGroup'] = ''
        if desired['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
            field = 'matchConstraints' if desired['kind'] == 'ValidatingAdmissionPolicy' else 'matchResources'
            for key, value in [('matchPolicy', 'Equivalent'), ('objectSelector', {}), ('namespaceSelector', {})]:
                observed['spec'][field].setdefault(key, value)
        if desired['kind'] == 'NetworkPolicy':
            for key in ('ingress', 'egress'):
                if observed['spec'].get(key) == []:
                    observed['spec'].pop(key)

        class API:
            def __init__(self, result):
                self.result = result

            def default_resource(self, document):
                return self.result

        assert _defaulted(API(observed), desired) == observed


def test_only_the_exact_foundation_actuator_account_is_reused(runtime_request):
    rendered = render_environment_runtime(**runtime_request)
    foundation = render_foundation(runtime_request['declaration'], runtime_request['candidate'],
        profile=runtime_request['runtime_profile'].model_dump(mode='json'),
        keyring=runtime_request['profiles'].image_admission_keyring, repo_root=runtime_request['repo_root'])
    accounts = installer._uncreated_readers(rendered.readers, foundation.files,
        runtime_request['participant'].execution_namespace.name)
    assert not any(doc['kind'] == 'ServiceAccount' and doc['metadata']['name'] == 'loom-execution-actuator' for doc in accounts)
    foreign = copy.deepcopy(foundation.files)
    account = next(doc for docs in foreign.values() for doc in docs if doc['kind'] == 'ServiceAccount'
        and doc['metadata']['namespace'] == 'work-red' and doc['metadata']['name'] == 'loom-execution-actuator')
    account['automountServiceAccountToken'] = True
    with pytest.raises(ValueError):
        installer._uncreated_readers(rendered.readers, foreign, 'work-red')
    foreign = copy.deepcopy(foundation.files)
    foreign['extra'] = [copy.deepcopy(next(doc for doc in rendered.readers if doc['kind'] == 'ClusterRole'))]
    with pytest.raises(ValueError):
        installer._uncreated_readers(rendered.readers, foreign, 'work-red')
