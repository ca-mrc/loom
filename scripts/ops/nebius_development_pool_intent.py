"""Complete fresh-dev intent, before Kubernetes assigns execution namespace UIDs.

Preview UIDs are local schema-validation sentinels, never installation authority.
Only the anchored namespace stage may supply the actual registration identities.
This module performs no live reads or writes and grants no runtime permissions.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from scripts.ops.nebius_development_management_retained import RetainedManagementReference
from scripts.ops.nebius_development_pool_registration import (
    DevelopmentPoolRegistrationRequest,
    prepare_registration,
)
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_application_delivery import derive_application_build_deployment

from loom.nebius_platform_render import _namespace
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.installation_render import render_gateway

_FIELDS = ('execution_namespace', 'build_namespace')
_PREVIEW = (UUID('ffffffff-ffff-ffff-ffff-fffffffffffe'), UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'))


@dataclass(frozen=True, repr=False)
class DevelopmentPoolIntent:
    reference: RetainedManagementReference
    catalog: dict[str, Any]
    tokens: dict[UUID, str]


def _bound(catalog: dict[str, Any], uids: dict[str, str]) -> PoolInstallation:
    value = copy.deepcopy(catalog)
    participant, = value['participants']
    names = [participant[field]['name'] for field in _FIELDS]
    if set(uids) != set(names):
        raise ValueError()
    for field, name in zip(_FIELDS, names, strict=True):
        if participant[field] != {'name': name}:
            raise ValueError()
        participant[field]['uid'] = uids[name]
    return PoolInstallation.model_validate(value)


def _preview(intent: DevelopmentPoolIntent) -> DevelopmentPoolRegistrationRequest:
    """Validation only; callers must never register or render this preview."""
    participant, = intent.catalog['participants']
    uids = {participant[field]['name']: str(uid) for field, uid in zip(_FIELDS, _PREVIEW, strict=True)}
    return prepare_registration(reference=intent.reference, spec=_bound(intent.catalog, uids))


def prepare_intent(*, reference: RetainedManagementReference, catalog: dict[str, Any],
                   tokens: dict[UUID, str]) -> DevelopmentPoolIntent:
    """Reject an unusable one-shot catalog and wrong private material before writes."""
    try:
        intent = DevelopmentPoolIntent(reference, copy.deepcopy(catalog), dict(tokens))
        request = _preview(intent)
        spec, before = request.registration.spec, request.retained.inputs.deployment
        config, candidate = before.installation.foundation.platform_config, request.registration.candidate
        derive_application_build_deployment(before, spec)
        participant, = spec.participants
        capabilities = {kind for target in participant.targets for kind in target.workload_kinds}
        if not {'trial', 'task_image_build'} <= capabilities:
            raise ValueError()
        for execution in spec.profiles.execution:
            if (execution.candidate_sha != candidate['candidate_sha']
                    or execution.runtime_image_ref != candidate['images']['execution_runtime']['image_ref']
                    or execution.runtime_binary_sha256 != request.retained.inputs.profile['runtime_binary_sha256']):
                raise ValueError()
        for settings in ([row.settings for row in spec.profiles.task_images]
                         + [row.settings for row in spec.profiles.application_images]):
            if (settings.service_image != candidate['images']['service']['image_ref']
                    or (settings.storage_endpoint, settings.storage_region, settings.source_bucket) != (
                        config['storage_endpoint'], config['region'], config['buckets']['source'])):
                raise ValueError()
        if set(tokens) != {row.machine_id for row in spec.machines}:
            raise ValueError()
        for machine in spec.machines:
            token = tokens[machine.machine_id]
            if (not isinstance(token, str) or not 0 < len(token) <= 512
                    or re.fullmatch(r'[A-Za-z0-9._~+/-]+={0,2}', token) is None
                    or hashlib.sha256(token.encode()).hexdigest() != machine.token_sha256):
                raise ValueError()
        return intent
    except Exception:
        raise ValueError('development pool intent unqualified') from None


def bind_namespaces(intent: DevelopmentPoolIntent, uids: dict[str, str]) -> DevelopmentPoolRegistrationRequest:
    try:
        intent = prepare_intent(reference=intent.reference, catalog=intent.catalog, tokens=intent.tokens)
        if any(UUID(value) in _PREVIEW or str(UUID(value)) != value for value in uids.values()):
            raise ValueError()
        return prepare_registration(reference=intent.reference, spec=_bound(intent.catalog, uids))
    except Exception:
        raise ValueError('development pool namespace receipt unqualified') from None


def namespace_documents(intent: DevelopmentPoolIntent) -> dict[str, dict[str, Any]]:
    request = _preview(intent)
    spec = request.registration.spec
    participant, = spec.participants
    result = {}
    for binding in (participant.execution_namespace, participant.build_namespace):
        document = _namespace(binding.name)
        document['metadata']['labels'].update({'loom.nebius/management-installation': str(spec.installation_id),
            'loom.nebius/pool': str(spec.pool_id), 'loom.nebius/pool-operation': str(spec.operation_id)})
        result[_key(document)] = document
    return result


def delivery_documents(intent: DevelopmentPoolIntent, request: DevelopmentPoolRegistrationRequest,
                       ) -> dict[str, dict[str, dict[str, Any]]]:
    """Fixed credentials/catalog and stopped gateway, deliberately excluding RBAC."""
    spec, binding = request.registration.spec, request.registration.binding
    participant, = spec.participants
    if request != bind_namespaces(intent, {row.name: str(row.uid)
            for row in (participant.execution_namespace, participant.build_namespace)}):
        raise ValueError('development pool delivery differs from intent')
    gateway = render_gateway(spec, namespace=binding.namespace,
        service_image=request.registration.candidate['images']['service']['image_ref'],
        kubernetes_endpoint=request.retained.inputs.deployment.installation.foundation.platform_config['kubernetes_api_server'])
    material = {}
    for machine in spec.machines:
        destinations = [binding.namespace]
        if machine.role == 'observer':
            destinations = [participant.execution_namespace.name]
        elif machine.role == 'participant' and machine.workload_scope == 'environment':
            destinations = ['loom-dev', participant.execution_namespace.name]
        for namespace in destinations:
            document = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'immutable': True,
                'metadata': {'name': 'loom-pool-machine-' + machine.machine_id.hex, 'namespace': namespace,
                    'labels': {'loom.nebius/management-installation': binding.installation_id,
                        'loom.nebius/pool': str(spec.pool_id), 'loom.nebius/pool-operation': str(spec.operation_id),
                        'loom.nebius/pool-machine': str(machine.machine_id)}},
                'data': {'token': base64.b64encode(intent.tokens[machine.machine_id].encode()).decode()}}
            material[_key(document)] = document
    return {'material': material, **{phase: {_key(row): row for row in gateway[phase]}
        for phase in ('configuration', 'workload')}}
