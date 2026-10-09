"""Fixed HTTPS resource phases for the independent development runtime parent.

The connected parent supplies phase-aware qualification; it must not replay the
original workload predicates after their journaled replacements. This module
does not itself qualify startup, grant writer authority or open pool admission.
"""
from __future__ import annotations

import copy
import json
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import (
    DevelopmentRuntimeInstallRequest,
    prepare_runtime_install,
    runtime_transition_inputs,
)
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER, _qualified_defaulted
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain

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


class HTTPSDevelopmentRuntimeWorkloads(ManagementKubernetesTransport):
    """CAS only the parent's regenerated phase targets; ambiguous writes raise."""

    def __init__(self, *, request: DevelopmentRuntimeInstallRequest, phase: str, api_server: str,
                 ssl_context: ssl.SSLContext, qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
                 observe_ready: Callable[[DevelopmentRuntimeInstallRequest, str, dict[str, Any], dict[str, Any], dict[str, Any]], bool],
                 token: str | None = None, private_files: dict[Path, bytes] | None = None):
        plan = prepare_runtime_install(request)
        if (phase not in {'stop', 'replace', 'control', 'start'} or not callable(qualify_runtime)
                or not callable(observe_ready)
                or api_server.rstrip('/') != request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('development runtime workload connection differs')
        self.request, self.phase = copy.deepcopy(request), phase
        state = Path(request.database.manager.retained.request.retained.operation['state_dir']).parent / 'runtime-installation'
        self.originals, self.targets = runtime_transition_inputs(plan, state, phase)
        self.private_files = dict(private_files or {})
        self.qualify_runtime, self.observe_ready = qualify_runtime, observe_ready
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files):
            raise ValueError('development runtime private inputs changed')

    def _qualify(self, *, writing: bool) -> None:
        self._private_inputs()
        self.qualify_runtime(copy.deepcopy(self.request), self.phase, writing)
        self._private_inputs()

    def qualify(self) -> None:
        self._qualify(writing=False)

    def _path(self, key: str) -> str:
        original = self.originals[key]
        return runtime_resource_path(original) + '/' + str(original['metadata']['name'])

    def read_workload(self, key: str) -> dict[str, Any]:
        path = self._path(key)
        self.qualify()
        actual = self._request('GET', path)
        if actual is None or _key(actual) != key or _uid(actual) != _uid(self.originals[key]):
            raise ValueError('development runtime workload identity differs')
        _snapshot(actual)
        return actual

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        try:
            original = self.originals[key]
            if desired != self.targets[key] or not _matches(before, original, _uid(original)):
                raise ValueError
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(original)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': before['spec']}]
            for field in ('labels', 'annotations'):
                if field in desired['metadata']:
                    patches.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
                elif field in before['metadata']:
                    patches.append({'op': 'remove', 'path': '/metadata/' + field})
            patches.append({'op': 'replace', 'path': '/spec', 'value': desired['spec']})
            self._qualify(writing=True)
            with self.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                if (response.status_code not in {200, 409, 422}
                        or response.headers.get('content-encoding', 'identity').lower() != 'identity'):
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    content.extend(chunk)
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise ValueError
                if response.status_code in {409, 422}:
                    if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Status' or value.get('status') != 'Failure'
                            or value.get('code') != response.status_code
                            or value.get('reason') != {409: 'Conflict', 422: 'Invalid'}[response.status_code]):
                        raise ValueError
                    return None
                if _uid(value) != _uid(original):
                    raise ValueError
                _qualified_defaulted(desired, value)
                return value
        except Exception:
            raise ValueError('development runtime workload update unconfirmed') from None

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return self._patch(key, before, desired, preview=True)

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False) is not None

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        _qualified_defaulted(self.targets[key], expected)
        actual = self.read_workload(key)
        if not _matches(actual, expected, _uid(self.originals[key])):
            raise ValueError('development runtime workload readiness differs')
        namespace = actual['metadata']['namespace']
        if actual['kind'] == 'Deployment' or actual['spec'].get('suspend') is True:
            deployment = actual['kind'] == 'Deployment'
            path = ('/apis/apps/v1/namespaces/' + namespace + '/replicasets' if deployment
                else '/apis/batch/v1/namespaces/' + namespace + '/jobs')
            self._private_inputs()
            children = self._request('GET', path + '?limit=1000')
            self._private_inputs()
            pods = self._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
            if children is None or pods is None:
                raise ValueError('development runtime workload collections unavailable')
            def observed_ready(current: dict[str, Any]) -> bool:
                if deployment and current['spec']['replicas'] == 1:
                    return qualify_started_deployment(current=current, children=children, pods=pods,
                        region=self.request.database.foundation.inputs.config['region'])
                return qualify_closed_workload_drain(original=self.originals[key], desired=expected,
                    current=current, children=children, pods=pods)
            ready = observed_ready(actual)
            final = self.read_workload(key)
            if (final['metadata']['generation'] != actual['metadata']['generation']
                    or _snapshot(final) != _snapshot(actual)):
                raise ValueError('development runtime workload changed during observation')
            if not ready or not observed_ready(final):
                return False
        return self.observe_ready(copy.deepcopy(self.request), self.phase, copy.deepcopy(self.originals[key]),
            copy.deepcopy(expected), actual)
