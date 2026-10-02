"""Effective retained-subject rights during anchored, stopped Role restoration."""
from __future__ import annotations

import copy
import json
from typing import Any

import httpx
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_gateway_authority import _grants
from scripts.ops.nebius_pool_role_fencing import (
    PoolRoleFenceRequest,
    qualify_pool_reader_rules,
    role_fence_review_scope,
)
from scripts.ops.nebius_pool_runtime import participant_readonly_roles


def qualify_legacy_rules(review: dict[str, Any], *, request: PoolRoleFenceRequest,
                         roles: dict[str, dict[str, Any]], subject: tuple[str, str], namespace: str) -> None:
    """Require this subject's exact grants, allowing only harmless reader extras.

    The caller binds every supplied Role to its anchored before/after projection.
    Bindings are fixed by the retained participant catalog, never inferred from
    the union of all writers' permissions or from the effective review itself.
    """
    try:
        if subject not in role_fence_review_scope(request)[0]:
            raise ValueError
        catalog = {_key(row): row for row in participant_readonly_roles(request=request.retirement.migration)}
        if set(roles) != {key for key, row in catalog.items() if row['kind'] == 'Role'}:
            raise ValueError
        required_rules = []
        for binding in catalog.values():
            if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                continue
            if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
                continue
            if not any((row['namespace'], row['name']) == subject for row in binding['subjects']):
                continue
            reference = binding['roleRef']
            key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
            role = roles[key] if reference['kind'] == 'Role' else catalog[key]
            required_rules.extend(role['rules'])
        required, observed = _grants(required_rules), _grants(review['status']['resourceRules'])
        if not required <= observed:
            raise ValueError
        common = copy.deepcopy(review)
        common['status']['resourceRules'] = [
            {'apiGroups': [group], 'resources': [resource], 'verbs': [verb],
                **({'resourceNames': [name]} if name is not None else {})}
            for group, resource, verb, name in observed - required]
        qualify_pool_reader_rules(common, namespace=namespace)
    except Exception:
        raise ValueError('pool_legacy_effective_authority_unqualified') from None


def review_legacy_rules(client: httpx.Client, *, request: PoolRoleFenceRequest,
                        roles: dict[str, dict[str, Any]], subject: tuple[str, str], namespace: str) -> None:
    """One bounded nonpersisted review for an exact retained ServiceAccount."""
    try:
        if subject not in role_fence_review_scope(request)[0]:
            raise ValueError
        account_namespace, account = subject
        headers = [('Impersonate-User', f'system:serviceaccount:{account_namespace}:{account}'),
            *(('Impersonate-Group', group) for group in ('system:serviceaccounts',
                'system:serviceaccounts:' + account_namespace, 'system:authenticated'))]
        with client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews', headers=headers,
                json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': namespace}}) as response:
            if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                raise ValueError
            content = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(content) + len(chunk) > 4 * 1024**2:
                    raise ValueError
                content.extend(chunk)
            qualify_legacy_rules(json.loads(content), request=request, roles=roles, subject=subject, namespace=namespace)
    except Exception:
        raise ValueError('pool_legacy_effective_authority_unconfirmed') from None
