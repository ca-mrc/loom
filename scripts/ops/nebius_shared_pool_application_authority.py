"""Exact personal lifecycle grants, separate from generic pool-reader authority."""
from __future__ import annotations

import copy
import re
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _MARKER
from scripts.ops.nebius_management_switch import _matches

from loom.nebius_application_authority import (
    APPLICATION_INSTALLATION_LABEL,
    ApplicationNamespaceAuthorityV1,
    application_namespace_binding,
    render_application_authority,
)

Document = dict[str, Any]
APPLICATION_SCOPE_COLLECTIONS = (
    ('v1', 'namespaces', 'Namespace'),
    ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicies', 'ValidatingAdmissionPolicy'),
    ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicybindings', 'ValidatingAdmissionPolicyBinding'),
)


def application_binding_candidates(authorities: tuple[ApplicationNamespaceAuthorityV1, ...],
        current: dict[str, Document]) -> dict[str, Document]:
    """Select for qualification, never exempt a grant by its name alone."""
    names = {authority.name for authority in authorities}
    roles = {name + '-resources' for name in names}
    return {key: row for key, row in current.items() if row['kind'] in {'RoleBinding', 'ClusterRoleBinding'}
        and (row['metadata']['name'] in names or row.get('roleRef', {}).get('name') in roles)}


def _uuid(value: Any) -> str:
    if not isinstance(value, str) or str(UUID(value)) != value or UUID(value).int == 0:
        raise ValueError
    return value


def _normalized(document: Document) -> Document:
    """Only API defaults which preserve the fixed renderer's enforcement scope."""
    row = _snapshot(document)
    if row['kind'] == 'ValidatingAdmissionPolicy':
        constraints = row['spec']['matchConstraints']
        for field, default in (('matchPolicy', 'Equivalent'), ('namespaceSelector', {}), ('objectSelector', {})):
            if constraints.pop(field, default) != default:
                raise ValueError
        for rule in constraints['resourceRules']:
            if rule.pop('scope', '*') != '*':
                raise ValueError
    elif row['kind'] in {'RoleBinding', 'ClusterRoleBinding'}:
        for subject in row['subjects']:
            if subject.pop('apiGroup', '') != '':
                raise ValueError
    return row


def _qualify_fixed(authority: ApplicationNamespaceAuthorityV1, retained: dict[str, Document],
        current: dict[str, Document]) -> dict[str, Document]:
    policies = {}
    for expected in render_application_authority(authority):
        key = _key(expected)
        original, actual = retained[key], current[key]
        if not _matches(actual, original, _uid(original)):
            raise ValueError
        observed = _normalized(actual)
        annotations = observed['metadata'].get('annotations', {})
        if _MARKER in annotations:
            _uuid(annotations.pop(_MARKER))
        if not annotations:
            observed['metadata'].pop('annotations', None)
        if observed != expected:
            raise ValueError
        if expected['kind'].startswith('ValidatingAdmissionPolicy'):
            policies[key] = copy.deepcopy(actual)
    return policies


def qualify_application_bindings(*, authorities: tuple[ApplicationNamespaceAuthorityV1, ...],
        retained: dict[str, Document], current: dict[str, Document], forbidden_namespaces: frozenset[str]
        ) -> tuple[dict[str, Document], dict[str, Document]]:
    """Qualify dynamic grants from completed installations and same-revision GETs.

    Labels identify a candidate scope; retained fixed grants and fail-closed
    admission establish the exception. Returned scope evidence must be compared
    across repeated observations, including namespace UID replacement.
    """
    try:
        domains = {authority.name: ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())
            for authority in authorities}
        if len(domains) != len(authorities):
            raise ValueError
        bindings, scopes = {}, {}
        checked = set()
        for key, row in application_binding_candidates(authorities, current).items():
            if key != _key(row) or row['kind'] != 'RoleBinding':
                raise ValueError
            _uid(row)
            domain = domains[row['metadata']['name']]
            namespace = row['metadata']['namespace']
            reserved = forbidden_namespaces | {item.namespace for item in authorities} | {
                item.shared_namespace for item in authorities} | {
                'loom-dev-dev', 'loom-dev-staging', 'loom-dev-prod', 'loom-dev-shared'}
            if (namespace in reserved
                    or re.fullmatch(r'loom-dev-[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?', namespace) is None):
                raise ValueError
            scope = current['Namespace:-:' + namespace]
            _uid(scope)
            _snapshot(scope)
            if scope['kind'] != 'Namespace' or scope['apiVersion'] != 'v1' or _key(scope) != 'Namespace:-:' + namespace:
                raise ValueError
            labels = scope['metadata']['labels']
            if (labels.get(APPLICATION_INSTALLATION_LABEL) != str(domain.installation_id)
                    or labels.get('loom.nebius/data-environment-id') != str(domain.data_environment_id)
                    or labels.get('pod-security.kubernetes.io/enforce') != 'restricted'
                    or {'loom.nebius/environment-id', 'loom.nebius/namespace-installation'} & labels.keys()):
                raise ValueError
            identity = {name: _uuid(labels[name])
                for name in ('loom.nebius/application-id', 'loom.nebius/incarnation')}
            actual = _normalized(row)
            if actual['metadata'].pop('labels') != identity:
                raise ValueError
            annotations = actual['metadata'].pop('annotations')
            _uuid(annotations['loom.nebius/operation-id'])
            generation = annotations['loom.nebius/deployment-generation']
            if (not isinstance(generation, str) or re.fullmatch(r'[1-9][0-9]{0,18}', generation) is None
                    or int(generation) > 2**63 - 1
                    or set(annotations) != {'loom.nebius/operation-id', 'loom.nebius/deployment-generation',
                        'loom.nebius/effect-key'}
                    or not isinstance(annotations['loom.nebius/effect-key'], str)
                    or not 0 < len(annotations['loom.nebius/effect-key']) <= 1024
                    or actual != application_namespace_binding(domain, namespace)):
                raise ValueError
            if domain.name not in checked:
                scopes.update(_qualify_fixed(domain, retained, current))
                checked.add(domain.name)
            bindings[key], scopes[_key(scope)] = copy.deepcopy(row), copy.deepcopy(scope)
        return bindings, scopes
    except Exception:
        raise ValueError('application lifecycle authority unqualified') from None
