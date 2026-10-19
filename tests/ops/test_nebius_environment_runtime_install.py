"""Runtime installation must not accept defaulting that expands authority."""
from __future__ import annotations

import copy

import pytest
from scripts.ops.nebius_environment_runtime import render_environment_runtime
from scripts.ops.nebius_environment_runtime_install import _defaulted
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


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

    with pytest.raises(ValueError):
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

        class API:
            def __init__(self, result):
                self.result = result

            def default_resource(self, document):
                return self.result

        assert _defaulted(API(observed), desired) == observed
