"""Effective restoration rights are per subject, not the union of writers."""
from __future__ import annotations

import copy

import pytest
from scripts.ops.nebius_shared_pool_recovery_roles import _qualify_rules


def test_restored_rules_require_only_this_subjects_present_fixed_bindings():
    jobs = {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create', 'get', 'delete']}
    configmaps = {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['create', 'get', 'delete']}
    nodes = {'apiGroups': [''], 'resources': ['nodes'], 'verbs': ['get']}
    catalog = {
        'Role:execution:worker': {'kind': 'Role', 'metadata': {'namespace': 'execution', 'name': 'worker'}, 'rules': [jobs]},
        'Role:execution:builder': {'kind': 'Role', 'metadata': {'namespace': 'execution', 'name': 'builder'}, 'rules': [configmaps]},
        'ClusterRole:-:reader': {'kind': 'ClusterRole', 'metadata': {'name': 'reader'}, 'rules': [nodes]},
    }
    for account in ('worker', 'builder'):
        catalog['RoleBinding:execution:' + account] = {'kind': 'RoleBinding',
            'metadata': {'namespace': 'execution', 'name': account},
            'subjects': [{'kind': 'ServiceAccount', 'namespace': 'platform', 'name': account}],
            'roleRef': {'kind': 'Role', 'name': account}}

    def review(rules):
        return {'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {},
            'status': {'incomplete': False, 'resourceRules': copy.deepcopy(rules), 'nonResourceRules': []}}

    _qualify_rules(review([jobs]), catalog=catalog, subject=('platform', 'worker'), namespace='execution')
    _qualify_rules(review([configmaps]), catalog=catalog, subject=('platform', 'builder'), namespace='execution')
    _qualify_rules(review([]), catalog=catalog, subject=('platform', 'worker'), namespace='unbound')
    _qualify_rules(review([]), catalog=catalog, subject=('platform', 'unbound'), namespace='execution')
    for subject in ('worker', 'builder', 'unbound'):
        with pytest.raises(ValueError):
            _qualify_rules(review([jobs, configmaps]), catalog=catalog, subject=('platform', subject), namespace='execution')
    with pytest.raises(ValueError):
        _qualify_rules(review([]), catalog=catalog, subject=('platform', 'worker'), namespace='execution')
    with pytest.raises(ValueError):
        _qualify_rules(review([{**jobs, 'resourceNames': ['one-job']}]), catalog=catalog,
            subject=('platform', 'worker'), namespace='execution')

    # The reader Role can exist before its binding; only the actual binding
    # makes it required. This does not assume all forward material was installed.
    catalog['ClusterRoleBinding:-:reader'] = {'kind': 'ClusterRoleBinding', 'metadata': {'name': 'reader'},
        'subjects': [{'kind': 'ServiceAccount', 'namespace': 'platform', 'name': 'worker'}],
        'roleRef': {'kind': 'ClusterRole', 'name': 'reader'}}
    with pytest.raises(ValueError):
        _qualify_rules(review([jobs]), catalog=catalog, subject=('platform', 'worker'), namespace='execution')
    _qualify_rules(review([jobs, nodes]), catalog=catalog, subject=('platform', 'worker'), namespace='execution')
    _qualify_rules(review([configmaps]), catalog=catalog, subject=('platform', 'builder'), namespace='execution')
