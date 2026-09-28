"""Fixed application prerequisites through the existing protected create journal.

The upgrade caller owns ordering: checked admission precedes permission grants;
actual-subject probes and completed SQL setup precede management activation.
This module is not a CLI and accepts no arbitrary manifests or update operations.
"""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_authority_stage import _defaulted as authority_defaulted
from scripts.ops.nebius_management_material import ManagementBinding, _uuid
from scripts.ops.nebius_management_stage import (
    _MARKER,
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest
from loom_service.application_management.deployment import render_application_setup
from loom_service.environment_management.deployment import ManagementDeployment

_PATHS = {
    'ValidatingAdmissionPolicy': ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicies'),
    'ValidatingAdmissionPolicyBinding': ('admissionregistration.k8s.io/v1', 'validatingadmissionpolicybindings'),
    'ClusterRole': ('rbac.authorization.k8s.io/v1', 'clusterroles'),
    'ClusterRoleBinding': ('rbac.authorization.k8s.io/v1', 'clusterrolebindings'),
    'Role': ('rbac.authorization.k8s.io/v1', 'roles'), 'RoleBinding': ('rbac.authorization.k8s.io/v1', 'rolebindings'),
    'NetworkPolicy': ('networking.k8s.io/v1', 'networkpolicies'),
    'ConfigMap': ('v1', 'configmaps'), 'Job': ('batch/v1', 'jobs'),
}


@dataclass(frozen=True, repr=False)
class ApplicationSetupRequest:
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    binding: ManagementBinding
    shared_namespace_uid: str
    repo_root: Path


def _documents(request: ApplicationSetupRequest, phase: str) -> dict[str, dict[str, Any]]:
    try:
        _uuid(request.shared_namespace_uid)
        deployment = request.deployment
        if (str(deployment.installation_id), deployment.namespace) != (request.binding.installation_id, request.binding.namespace):
            raise ValueError
        phases = render_application_setup(deployment, candidate=request.candidate, profile=request.profile,
                                          repo_root=request.repo_root)
        return {_key(doc): doc for doc in phases[phase]}
    except Exception:
        raise ManagementStageError('application setup binding or phase differs') from None


def _revision(request: ApplicationSetupRequest, documents: dict[str, dict[str, Any]]) -> str:
    return digest({'documents': documents, 'shared_namespace_uid': request.shared_namespace_uid})


class HTTPSApplicationSetupAPI(HTTPSManagementStageAPI):
    """Exact rendered scope and management/shared namespace identity checks."""

    def __init__(self, *, request: ApplicationSetupRequest, phase: str, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = _documents(request, phase)
        application = request.deployment.installation.applications
        assert application is not None
        if api_server.rstrip('/') != application.runtime.kubernetes.endpoint:
            raise ManagementStageError('application setup endpoint differs')
        self.binding = request.binding
        self.shared_namespace = application.shared.platform_namespace
        self.shared_namespace_uid = request.shared_namespace_uid
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: ManagementBinding) -> None:
        super().verify_identity(binding)
        actual = self._request('GET', '/api/v1/namespaces/' + self.shared_namespace)
        if (actual is None or actual.get('kind') != 'Namespace'
                or actual.get('metadata', {}).get('name') != self.shared_namespace
                or _uid(actual) != self.shared_namespace_uid
                or actual['metadata'].get('deletionTimestamp') or actual['metadata'].get('ownerReferences')):
            raise ManagementStageError('application setup shared namespace identity differs')

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired['metadata'].get('annotations', {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or UUID(operation).int == 0:
                    raise ValueError
            if not annotations and 'annotations' not in expected['metadata']:
                desired['metadata'].pop('annotations', None)
            if desired != expected:
                raise ValueError
            version, resource = _PATHS[desired['kind']]
            path = '/api/v1' if version == 'v1' else '/apis/' + version
            namespace = desired['metadata'].get('namespace')
            return path + (('/namespaces/' + namespace) if namespace else '') + '/' + resource
        except Exception:
            raise ManagementStageError('resource outside fixed application setup scope') from None


def _setup_defaulted(api: ManagementStageAPI, document: dict[str, Any]) -> dict[str, Any]:
    if document['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding',
                            'ClusterRole', 'ClusterRoleBinding', 'Role', 'RoleBinding'}:
        return authority_defaulted(api, document)
    return _defaulted(api, document)


def stage_application_setup(*, request: ApplicationSetupRequest, phase: str,
                            api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    documents = _documents(request, phase)
    return _stage_fixed_documents(documents=documents, revision=_revision(request, documents),
        phase='application-' + phase, binding=request.binding, api=api, state_dir=state_dir,
        default_document=_setup_defaulted)


def application_setup_ready(*, request: ApplicationSetupRequest, phase: str,
                            api: ManagementStageAPI, state_dir: Path) -> bool:
    """Only recorded, unchanged admission and SQL Job observations count."""
    try:
        if phase not in {'admission', 'database'}:
            raise ManagementStageError('application setup phase has no readiness barrier')
        documents = _documents(request, phase)
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(request.binding),
            'revision': _revision(request, documents), 'phase': 'application-' + phase}
        with private_state._locked_state(state_dir):
            record = json.loads(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024 * 1024))
            _validate_record(record, identity, documents)
            ready = True
            for item in record['resources'].values():
                api.verify_identity(request.binding)
                if item['status'] != 'created':
                    raise ManagementStageError('application setup was not fully staged')
                actual = api.get_resource(item['desired'])
                if actual is None or _uid(actual) != item['uid'] or _snapshot(actual) != item['observed']:
                    raise ManagementStageError('application setup identity or configuration changed')
                status = actual.get('status', {})
                if actual['kind'] == 'ValidatingAdmissionPolicy':
                    checking = status.get('typeChecking')
                    if isinstance(checking, dict) and checking.get('expressionWarnings'):
                        raise ManagementStageError('application admission has type-check warnings')
                    ready &= (isinstance(checking, dict)
                        and status.get('observedGeneration', 0) >= actual['metadata'].get('generation', 1))
                elif actual['kind'] == 'Job':
                    conditions = {row['type']: row['status'] for row in status.get('conditions', [])}
                    if conditions.get('Failed') == 'True':
                        raise ManagementStageError('application SQL setup failed; explicit recovery required')
                    ready &= conditions.get('Complete') == 'True' and status.get('succeeded', 0) >= actual['spec'].get('completions', 1)
            api.verify_identity(request.binding)
            return ready
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError('application setup readiness unavailable') from None
