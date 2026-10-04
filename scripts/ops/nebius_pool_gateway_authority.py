"""Read-only effective gateway permission qualification; never grant authority."""
from __future__ import annotations

import copy
import json
import re
from collections.abc import Sequence
from itertools import product
from math import prod
from typing import Any

import httpx
from scripts.ops.nebius_pool_migration import PoolMigrationRequest
from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules

_Grant = tuple[str, str, str, str | None]


def subject_review_namespaces(request: PoolMigrationRequest, bindings: Sequence[dict[str, Any]], *,
                              subject: tuple[str, str]) -> tuple[str, ...]:
    """Include foreign RoleBindings to this account or any of its actual groups."""
    try:
        manager = request.registration.binding.namespace
        account_namespace, account = subject
        namespaces = {manager, *(target.namespace for target in request.guards),
            *(ns.name for row in request.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}
        identities = {'User': {f'system:serviceaccount:{account_namespace}:{account}'},
            'Group': {'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'}}
        for binding in bindings:
            if binding['apiVersion'] != 'rbac.authorization.k8s.io/v1' or binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                raise ValueError
            subjects = binding.get('subjects', [])
            if not isinstance(subjects, list) or len(subjects) > 1000:
                raise ValueError
            for entry in subjects:
                kind, name = entry['kind'], entry['name']
                if kind == 'ServiceAccount':
                    if entry.keys() - {'kind', 'name', 'namespace', 'apiGroup'} or entry.get('apiGroup', '') != '':
                        raise ValueError
                    relevant = (entry['namespace'], name) == (account_namespace, account)
                elif kind in identities:
                    if set(entry) != {'kind', 'name', 'apiGroup'} or entry['apiGroup'] != 'rbac.authorization.k8s.io':
                        raise ValueError
                    relevant = name in identities[kind]
                else:
                    raise ValueError
                if relevant and binding['kind'] == 'RoleBinding':
                    namespace = binding['metadata']['namespace']
                    if not isinstance(namespace, str) or re.fullmatch(r'[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?', namespace) is None:
                        raise ValueError
                    namespaces.add(namespace)
        return tuple(sorted(namespaces))
    except Exception:
        raise ValueError('pool_subject_review_scope_unqualified') from None


def gateway_review_namespaces(request: PoolMigrationRequest, bindings: Sequence[dict[str, Any]]) -> tuple[str, ...]:
    """Preserve the gateway's fixed identity and namespace discovery contract."""
    try:
        return subject_review_namespaces(request, bindings,
            subject=(request.registration.binding.namespace, 'loom-pool-gateway'))
    except Exception:
        raise ValueError('pool_gateway_review_scope_unqualified') from None


def _grants(rules: Any) -> set[_Grant]:
    if not isinstance(rules, (list, tuple)) or len(rules) > 1000:
        raise ValueError
    grants: set[_Grant] = set()
    for rule in rules:
        if set(rule) - {'apiGroups', 'resources', 'verbs', 'resourceNames'}:
            raise ValueError
        fields = []
        for field in ('apiGroups', 'resources', 'verbs', 'resourceNames'):
            values = rule.get(field, []) if field == 'resourceNames' else rule[field]
            if (not isinstance(values, list) or len(values) > 1000
                    or (not values and field != 'resourceNames')
                    or any(not isinstance(value, str) or len(value) > 1024 or (not value and field != 'apiGroups') for value in values)):
                raise ValueError
            fields.append(set(values) if values else {None})
        if prod(map(len, fields)) > 20_000:
            raise ValueError
        for group, resource, verb, name in product(*fields):
            if group is None or resource is None or verb is None:
                raise ValueError
            grants.add((group, resource, verb, name))
        if len(grants) > 20_000:
            raise ValueError
    return grants


def qualify_gateway_rules(review: dict[str, Any], *, namespace: str, authority: Sequence[dict[str, Any]]) -> None:
    """Require all fixed grants and reject widening, including resourceNames."""
    try:
        # Reuse the existing complete-review and harmless discovery checks.
        common = copy.deepcopy(review)
        common['status']['resourceRules'] = []
        qualify_pool_reader_rules(common, namespace=namespace)
        required = _grants([rule for row in authority if row['kind'] == 'ClusterRole'
            or (row['kind'] == 'Role' and row['metadata']['namespace'] == namespace) for rule in row['rules']])
        observed = _grants(review['status']['resourceRules'])
        self_inspection = {(group, resource, 'create', None) for group, resource in (
            ('authorization.k8s.io', 'selfsubjectaccessreviews'), ('authorization.k8s.io', 'selfsubjectrulesreviews'),
            ('authentication.k8s.io', 'selfsubjectreviews'))}
        if not required <= observed or not observed <= required | self_inspection:
            raise ValueError
    except Exception:
        raise ValueError('pool_gateway_effective_authority_unqualified') from None


def review_gateway_rules(client: httpx.Client, *, manager_namespace: str, namespace: str,
                         authority: Sequence[dict[str, Any]]) -> None:
    """One nonpersisted review with fixed per-request impersonation, no token issue."""
    try:
        headers = [('Impersonate-User', f'system:serviceaccount:{manager_namespace}:loom-pool-gateway'),
            *(('Impersonate-Group', group) for group in ('system:serviceaccounts',
                'system:serviceaccounts:' + manager_namespace, 'system:authenticated'))]
        with client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews', headers=headers,
                json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': namespace}}) as response:
            if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                raise ValueError
            content = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(content) + len(chunk) > 4 * 1024**2:
                    raise ValueError
                content.extend(chunk)
            qualify_gateway_rules(json.loads(content), namespace=namespace, authority=authority)
    except Exception:
        raise ValueError('pool_gateway_effective_authority_unconfirmed') from None
