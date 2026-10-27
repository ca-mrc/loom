"""Fixed activation intent for a genuinely completed fresh development runtime.

Preparing this projection grants no authority and changes no namespace labels.
Its protected consumer must qualify sole-writer ownership and effective build
policy before staging grants, changing PSA, starting the gateway or opening SQL.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_retained import (
    CompletedDevelopmentRuntime,
    load_completed_runtime,
)
from scripts.ops.nebius_development_runtime_selection import (
    DevelopmentRuntimeSelection,
    select_development_runtime,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_gateway_authority import (
    review_gateway_rules,
    service_account_review_namespaces,
)

from loom.nebius_platform_render import digest
from loom_service.pool_management.installation_render import render_gateway

if TYPE_CHECKING:
    from scripts.ops.nebius_development_schema_upgrade import CompletedDevelopmentSchemaSuccessor

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
    schema_successor: CompletedDevelopmentSchemaSuccessor | None = None

    @property
    def selection(self) -> DevelopmentRuntimeSelection:
        return DevelopmentRuntimeSelection(self.runtime, self.schema_successor)


def prepare_development_activation(request: DevelopmentRuntimeInstallRequest, *,
        schema_successor: CompletedDevelopmentSchemaSuccessor | None = None) -> DevelopmentActivationPlan:
    """Derive only catalog-fixed gateway grants and exact dev resource successors."""
    try:
        runtime = load_completed_runtime(request)
        selected = select_development_runtime(runtime, schema_successor)
        database = runtime.request.database
        registration = selected.registration
        spec, binding = registration.spec, registration.binding
        participant = selected.participant
        if binding.namespace != 'loom-nebius-management-dev' or database.foundation.inputs.config['namespace'] != 'loom-dev':
            raise ValueError
        rendered = render_gateway(spec, namespace=binding.namespace,
            service_image=selected.manager_publication.bundle.candidate['images']['service']['image_ref'],
            kubernetes_endpoint=database.foundation.inputs.config['kubernetes_api_server'])
        authority = {_key(row): copy.deepcopy(row) for row in rendered['authority']}
        if authority.keys() & selected.resources.keys():
            raise ValueError
        gateway, = rendered['workload']
        original = copy.deepcopy(selected.resources[_key(gateway)])
        if original['spec']['replicas'] != 0 or MARKER in original['metadata'].get('annotations', {}):
            raise ValueError
        target = _snapshot(original)
        target['spec'] = copy.deepcopy(gateway['spec'])
        target['spec']['replicas'] = 1
        target['metadata'].setdefault('annotations', {})[MARKER] = str(database.operation_id)
        namespace = copy.deepcopy(selected.resources['Namespace:-:' + participant.build_namespace.name])
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
        policy = {key: copy.deepcopy(selected.resources[key])
            for key, row in runtime.plan.fixed['isolation'].items()
            if row['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}
        if len(policy) != 2:
            raise ValueError
        checksum = digest({'history': runtime.history_sha256 if schema_successor is None else schema_successor.history_sha256, 'authority': authority,
            'gateway_original': original, 'gateway_target': target, 'build_namespace_original': namespace,
            'build_namespace_target': build_target, 'build_policy': policy})
        return DevelopmentActivationPlan(runtime, authority, original, target, namespace, build_target, policy, checksum, schema_successor)
    except Exception:
        raise ValueError('development activation intent unqualified') from None


def review_development_gateway_permissions(request: DevelopmentRuntimeInstallRequest, *,
        transport: ManagementKubernetesTransport, granted: bool,
        schema_successor: CompletedDevelopmentSchemaSuccessor | None = None) -> tuple[str, ...]:
    """Inspect the fixed dev gateway, including grants outside dev namespaces.

    This makes only inventory GETs and nonpersisted permission reviews. It never
    grants authority or fences staging. The protected activation parent must bind
    this observation to live cluster/resource identity and its recorded grant
    phase, and separately qualify competing Kubernetes/provider writers. Passing
    this check alone is not sole-physical-pool ownership or permission to activate.
    """
    try:
        if type(granted) is not bool:
            raise ValueError
        plan = prepare_development_activation(request, schema_successor=schema_successor)
        database = plan.runtime.request.database
        registration = plan.selection.registration
        if transport.api_server != database.foundation.inputs.config['kubernetes_api_server']:
            raise ValueError

        def permissions() -> dict[str, dict[str, Any]]:
            rows: dict[str, dict[str, Any]] = {}
            uids: set[str] = set()
            for resource, kind in (('roles', 'Role'), ('clusterroles', 'ClusterRole'),
                    ('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding')):
                for row in inventory_resources(transport._request, 'rbac.authorization.k8s.io/v1', resource, kind):
                    key, uid = _key(row), _uid(row)
                    metadata = row['metadata']
                    if (key in rows or uid in uids
                            or not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
                            or (kind in {'Role', 'RoleBinding'}) != bool(metadata.get('namespace'))):
                        raise ValueError
                    _snapshot(row)
                    rows[key], uids = row, uids | {uid}
            return rows

        original = permissions()
        manager = registration.binding.namespace
        base = (manager, database.foundation.inputs.config['namespace'],
            *(ns.name for participant in registration.spec.participants
                for ns in (participant.execution_namespace, participant.build_namespace)))
        bindings = tuple(row for row in original.values() if row['kind'] in {'RoleBinding', 'ClusterRoleBinding'})
        namespaces = service_account_review_namespaces(base, bindings,
            subject=(manager, 'loom-pool-gateway'))
        authority = list(plan.authority.values()) if granted else []
        for namespace in namespaces:
            review_gateway_rules(transport.client, manager_namespace=manager, namespace=namespace, authority=authority)
        if permissions() != original or prepare_development_activation(request, schema_successor=schema_successor) != plan:
            raise ValueError
        return namespaces
    except Exception:
        raise ValueError('development gateway permissions unqualified') from None
