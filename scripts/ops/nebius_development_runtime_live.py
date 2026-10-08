"""Fixed HTTPS resource phases for the independent development runtime parent.

The connected parent supplies phase-aware qualification; it must not replay the
original workload predicates after their journaled replacements. This module
does not itself qualify startup, grant writer authority or open pool admission.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import (
    DevelopmentRuntimeInstallRequest,
    prepare_runtime_install,
)
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

_PATHS = {
    'Secret': ('v1', 'secrets', False), 'ConfigMap': ('v1', 'configmaps', False),
    'ServiceAccount': ('v1', 'serviceaccounts', False),
    'Role': ('rbac.authorization.k8s.io/v1', 'roles', False),
    'RoleBinding': ('rbac.authorization.k8s.io/v1', 'rolebindings', False),
    'ClusterRole': ('rbac.authorization.k8s.io/v1', 'clusterroles', True),
    'ClusterRoleBinding': ('rbac.authorization.k8s.io/v1', 'clusterrolebindings', True),
    'NetworkPolicy': ('networking.k8s.io/v1', 'networkpolicies', False),
    'ValidatingAdmissionPolicy': ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicies', True),
    'ValidatingAdmissionPolicyBinding': ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicybindings', True),
    'Deployment': ('apps/v1', 'deployments', False),
    'Job': ('batch/v1', 'jobs', False), 'CronJob': ('batch/v1', 'cronjobs', False),
}


def runtime_resource_path(document: dict[str, Any]) -> str:
    version, plural, cluster = _PATHS[document['kind']]
    namespace = document['metadata'].get('namespace')
    if (document['apiVersion'] != version or (cluster and namespace is not None)
            or (not cluster and (not isinstance(namespace, str) or not namespace))):
        raise ValueError('development runtime resource scope differs')
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + (
        '' if cluster else '/namespaces/' + str(namespace)) + '/' + plural


class HTTPSDevelopmentRuntimeResources(ManagementKubernetesTransport):
    """POST only regenerated phase documents, after durable parent qualification."""

    def __init__(self, *, request: DevelopmentRuntimeInstallRequest, phase: str, api_server: str,
                 ssl_context: ssl.SSLContext, qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
                 token: str | None = None, private_files: dict[Path, bytes] | None = None):
        plan = prepare_runtime_install(request)
        if (phase not in plan.fixed or not callable(qualify_runtime)
                or api_server.rstrip('/') != request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('development runtime resource connection differs')
        self.request, self.phase = copy.deepcopy(request), phase
        self.binding = self.request.database.manager.retained.request.retained.binding
        self.documents = plan.fixed[phase]
        self.private_files = dict(private_files or {})
        self.qualify_runtime = qualify_runtime
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files):
            raise ValueError('development runtime private inputs changed')

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        self._private_inputs()
        return super()._request(method, path, document=document)

    def _qualify(self, *, writing: bool) -> None:
        self._private_inputs()
        self.qualify_runtime(copy.deepcopy(self.request), self.phase, writing)
        self._private_inputs()

    def verify_identity(self, binding: ManagementBinding) -> None:
        if binding != self.binding:
            raise ValueError('development runtime resource binding differs')
        self._qualify(writing=False)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            value = copy.deepcopy(document)
            expected = self.documents[_key(value)]
            annotations = value['metadata'].get('annotations', {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and 'annotations' not in expected['metadata']:
                value['metadata'].pop('annotations', None)
            if value != expected:
                raise ValueError
            return runtime_resource_path(expected)
        except Exception:
            raise ValueError('resource outside fixed development runtime phase') from None

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        return self._request('GET', self._approved(document) + '/' + document['metadata']['name'])

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        path = self._approved(document, writing=True)
        self._qualify(writing=True)
        result = self._request('POST', path + '?dryRun=All', document=document)
        assert result is not None
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        path = self._approved(document, writing=True)
        self._qualify(writing=True)
        self._request('POST', path, document=document)

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('database storage outside fixed development runtime phase')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('database storage outside fixed development runtime phase')
