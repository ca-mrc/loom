"""Fixed activation intent for a genuinely completed fresh development runtime.

Preparing this projection grants no authority and changes no namespace labels.
Its protected consumer must qualify sole-writer ownership and effective build
policy before staging grants, changing PSA, starting the gateway or opening SQL.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_retained import (
    CompletedDevelopmentRuntime,
    load_completed_runtime,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid

from loom.nebius_platform_render import digest
from loom_service.pool_management.installation_render import render_gateway

MARKER = 'loom.nebius/development-activation-operation'


@dataclass(frozen=True, repr=False)
class DevelopmentActivationPlan:
    runtime: CompletedDevelopmentRuntime
    authority: dict[str, dict[str, Any]]
    gateway_original: dict[str, Any]
    gateway_target: dict[str, Any]
    build_namespace_original: dict[str, Any]
    build_namespace_target: dict[str, Any]
    build_policy: dict[str, dict[str, Any]]
    input_digest: str


def prepare_development_activation(request: DevelopmentRuntimeInstallRequest) -> DevelopmentActivationPlan:
    """Derive only catalog-fixed gateway grants and exact dev resource successors."""
    try:
        runtime = load_completed_runtime(request)
        database = runtime.request.database
        registration = database.manager.retained.request.registration
        spec, binding = registration.spec, registration.binding
        participant, = spec.participants
        if binding.namespace != 'loom-nebius-management-dev' or database.foundation.inputs.config['namespace'] != 'loom-dev':
            raise ValueError
        rendered = render_gateway(spec, namespace=binding.namespace,
            service_image=database.manager.publication.bundle.candidate['images']['service']['image_ref'],
            kubernetes_endpoint=database.foundation.inputs.config['kubernetes_api_server'])
        authority = {_key(row): copy.deepcopy(row) for row in rendered['authority']}
        if authority.keys() & runtime.resources.keys():
            raise ValueError
        gateway, = rendered['workload']
        original = copy.deepcopy(runtime.resources[_key(gateway)])
        if original['spec']['replicas'] != 0 or MARKER in original['metadata'].get('annotations', {}):
            raise ValueError
        target = _snapshot(original)
        target['spec'] = copy.deepcopy(gateway['spec'])
        target['spec']['replicas'] = 1
        target['metadata'].setdefault('annotations', {})[MARKER] = str(database.operation_id)
        namespace = copy.deepcopy(runtime.resources['Namespace:-:' + participant.build_namespace.name])
        if (_uid(namespace) != str(participant.build_namespace.uid)
                or namespace['metadata']['labels']['pod-security.kubernetes.io/enforce'] != 'restricted'
                or MARKER in namespace['metadata'].get('annotations', {})):
            raise ValueError
        build_target = _snapshot(namespace)
        # Rootless BuildKit needs SETUID/SETGID and unconfined syscall profiles.
        # The already-installed, fail-closed catalog policy permits only that
        # exact builder exception; audit/warn labels and all other metadata stay.
        build_target['metadata']['labels']['pod-security.kubernetes.io/enforce'] = 'privileged'
        build_target['metadata'].setdefault('annotations', {})[MARKER] = str(database.operation_id)
        policy = {key: copy.deepcopy(runtime.resources[key])
            for key, row in runtime.plan.fixed['isolation'].items()
            if row['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}
        if len(policy) != 2:
            raise ValueError
        checksum = digest({'history': runtime.history_sha256, 'authority': authority,
            'gateway_original': original, 'gateway_target': target, 'build_namespace_original': namespace,
            'build_namespace_target': build_target, 'build_policy': policy})
        return DevelopmentActivationPlan(runtime, authority, original, target, namespace, build_target, policy, checksum)
    except Exception:
        raise ValueError('development activation intent unqualified') from None
