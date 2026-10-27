"""Target-bound release evidence, reusing every existing release check.

The validation view substitutes legacy labels only after checking the actual
targets, their isolation and every affected evidence field. V1 remains unchanged.
This module is stdlib-only so the protected gate can run before package imports.
"""
from __future__ import annotations

import copy
import re
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

_IDENTITIES = {'installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation'}
_STORAGE = {'task_bucket', 'trajectories_bucket', 'artifacts_bucket'}
_FIELDS = {'scope', 'namespace', 'execution_namespace', 'database_identity', 'database_name',
    'provider_connection_namespace', 'public_url', 'object_storage', 'secret_ref_prefix'}


def validate_target(value: dict[str, Any], *, authority: str) -> None:
    if (not isinstance(value, dict) or set(value) != _FIELDS or authority not in {'production', 'staging'}
            or not isinstance(value['scope'], dict) or set(value['scope']) != _IDENTITIES | {'authority'}
            or value['scope']['authority'] != authority):
        raise ValueError('production_target_unqualified')
    for name in _IDENTITIES:
        identity = value['scope'][name]
        if not isinstance(identity, str) or str(UUID(identity)) != identity or not UUID(identity).int:
            raise ValueError('production_target_identity_unqualified')
    for name in ('namespace', 'execution_namespace', 'provider_connection_namespace'):
        if not isinstance(value[name], str) or re.fullmatch(r'[a-z][a-z0-9-]{0,62}', value[name]) is None:
            raise ValueError('production_target_namespace_unqualified')
    if value['namespace'] == value['execution_namespace']:
        raise ValueError('production_target_namespace_alias')
    for name in ('database_identity', 'database_name'):
        if not isinstance(value[name], str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:._/-]{0,511}', value[name]) is None:
            raise ValueError('production_target_database_unqualified')
    url = urlsplit(value['public_url'])
    if (url.scheme != 'https' or not url.hostname or url.path or url.query or url.fragment
            or url.username or url.password or url.port not in {None, 443}
            or value['public_url'] != 'https://' + url.netloc):
        raise ValueError('production_target_route_unqualified')
    if (not isinstance(value['object_storage'], dict) or set(value['object_storage']) != _STORAGE
            or any(not isinstance(bucket, str) or re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket) is None
                for bucket in value['object_storage'].values())
            or not isinstance(value['secret_ref_prefix'], str)
            or re.fullmatch('github-environment:' + authority + r'/[A-Z][A-Z0-9_]{0,127}', value['secret_ref_prefix']) is None):
        raise ValueError('production_target_storage_unqualified')


def validation_view(manifest: dict[str, Any], *, routes: dict[str, str], identities: dict[str, Any],
        secret_fields: tuple[str, ...]) -> dict[str, Any]:
    """Validate explicit targets before applying existing semantic check validators."""
    value = copy.deepcopy(manifest)
    targets = value['targets']
    if (type(value['schema_version']) is not int or value['schema_version'] != 2
            or not isinstance(targets, dict) or set(targets) != {'production', 'staging'}):
        raise ValueError('production_gate_targets_unqualified')
    for kind, target in targets.items():
        validate_target(target, authority=kind)
    production, staging = targets['production'], targets['staging']
    if (any(production['scope'][name] == staging['scope'][name] for name in ('participant_id', 'environment_id', 'incarnation'))
            or {production['namespace'], production['execution_namespace']} & {staging['namespace'], staging['execution_namespace']}
            or any(production[name] == staging[name] for name in ('database_identity', 'provider_connection_namespace', 'public_url'))
            or set(production['object_storage'].values()) & set(staging['object_storage'].values())
            or value['staging_url'] != staging['public_url']):
        raise ValueError('production_gate_targets_not_isolated')
    checks = value['checks']
    isolation = checks['prod_staging_isolation']
    if any(not isinstance(isolation[name], dict) or set(isolation[name]) != {'production', 'staging'}
            for name in ('state_profiles', 'frontend', 'workers')):
        raise ValueError('production_gate_target_evidence_unqualified')
    for kind, target in targets.items():
        profile, frontend, worker = (isolation[name][kind] for name in ('state_profiles', 'frontend', 'workers'))
        fields = ('namespace', 'database_name', 'database_identity', 'provider_connection_namespace', 'object_storage', 'scope')
        if (any(profile.get(name) != target[name] for name in fields)
                or profile.get('environment') != kind or profile.get('github_environment') != kind
                or frontend.get('route') != target['public_url'] or frontend.get('api_base') != target['public_url'] + '/api'
                or worker.get('api_url') != target['public_url'] + '/api' or worker.get('environment') != kind
                or worker.get('k8s_namespace') != target['execution_namespace']
                or worker.get('source_commit') != value['candidate_sha']
                or worker.get('image') != value['image_digests']['loom-execution-runtime']
                or worker.get('image_digest') != value['image_digests']['loom-execution-runtime']):
            raise ValueError('production_gate_target_evidence_mismatch')
        refs = profile['secret_refs']
        if any(not isinstance(refs.get(name), str) or not refs[name].startswith(target['secret_ref_prefix']) for name in secret_fields):
            raise ValueError('production_gate_secret_scope_mismatch')
        for suffix, expected in (('_route', target['public_url']), ('_api_base', target['public_url'] + '/api')):
            if checks['frontend_route_evidence'].get(kind + suffix) != expected:
                raise ValueError('production_gate_frontend_mismatch')
            checks['frontend_route_evidence'][kind + suffix] = routes[kind + suffix]
        # Only identity labels change; approval, smoke, recovery and every other
        # semantic condition below still passes through the established gate.
        for name in ('namespace', 'database_name', 'provider_connection_namespace', 'object_storage'):
            profile[name] = copy.deepcopy(identities[kind][name])
        for name in secret_fields:
            refs[name] = identities[kind]['secret_ref_prefix'] + name.upper()
        frontend.update(route=routes[kind + '_route'], api_base=routes[kind + '_api_base'])
        worker.update(api_url=routes[kind + '_api_base'], k8s_namespace=identities[kind]['namespace'])
    records = checks['execution_capacity_smoke']['execution_records']
    if not isinstance(records, list) or not records or any(row.get('namespace') != staging['execution_namespace'] for row in records):
        raise ValueError('production_gate_execution_target_mismatch')
    value['schema_version'] = 1
    return value
