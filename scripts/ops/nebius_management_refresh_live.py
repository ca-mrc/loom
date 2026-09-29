"""Bound HTTPS refresh cutover; no generic update, apply or credential fallback."""
from __future__ import annotations

import json
import ssl
from collections.abc import Callable
from typing import Any

from scripts.ops.nebius_application_setup import ApplicationSetupRequest, HTTPSApplicationSetupAPI
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_refresh_switch import (
    ManagementRefreshSwitchRequest,
    qualify_refresh_drain,
    refresh_target,
)
from scripts.ops.nebius_management_stage import ManagementStageError
from scripts.ops.nebius_management_switch import _matches


class HTTPSManagementRefreshSwitchAPI(HTTPSApplicationSetupAPI):
    """Only one retained Deployment, scoped by explicit cluster/namespace UIDs."""

    def __init__(self, *, request: ManagementRefreshSwitchRequest, binding: ManagementBinding,
                 shared_namespace_uid: str, api_server: str, ssl_context: ssl.SSLContext,
                 activation_check: Callable[[ManagementRefreshSwitchRequest], bool], token: str | None = None):
        self.refresh = request
        self.check_activation = activation_check
        refresh_target(request, 'activate')
        setup = ApplicationSetupRequest(request.render.after, request.render.candidate, request.render.profile,
            binding, shared_namespace_uid, request.render.repo_root)
        super().__init__(request=setup, phase='config', api_server=api_server, ssl_context=ssl_context, token=token)
        self.path = '/apis/apps/v1/namespaces/' + binding.namespace + '/deployments/loom-service'

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('resource outside fixed management refresh cutover')

    def read(self) -> dict[str, Any]:
        self.verify_identity(self.binding)
        value = self._request('GET', self.path)
        if (value is None or value.get('apiVersion') != 'apps/v1' or value.get('kind') != 'Deployment'
                or value['metadata'].get('namespace') != self.binding.namespace
                or value['metadata'].get('name') != 'loom-service'
                or _uid(value) != _uid(self.refresh.render.active)):
            raise ManagementStageError('management refresh Deployment identity differs')
        _snapshot(value)
        return value

    def _patch(self, before: dict[str, Any], action: str, operation_id: str, *, preview: bool) -> dict[str, Any] | None:
        try:
            if (operation_id != str(self.refresh.operation_id) or action not in {'retire', 'activate'}
                    or (preview and action != 'activate')):
                raise ValueError
            expected = self.refresh.render.active if action == 'retire' else refresh_target(self.refresh, 'retire')
            desired = refresh_target(self.refresh, 'activate' if action == 'activate' else 'retire')
            version = before['metadata']['resourceVersion']
            if (not _matches(before, expected, _uid(self.refresh.render.active))
                    or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            patches = [
                {'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']},
                {'op': 'replace', 'path': '/spec/replicas', 'value': desired['spec']['replicas']},
            ]
            if action == 'activate':
                patches.append({'op': 'replace', 'path': '/spec/template', 'value': desired['spec']['template']})
            self.verify_identity(self.binding)
            with self.client.stream('PATCH', self.path + ('?dryRun=All' if preview else ''), json=patches,
                                    headers={'Content-Type': 'application/json-patch+json'}) as response:
                if response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise ValueError
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(raw) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    raw.extend(chunk)
                observed = json.loads(raw)
                if not isinstance(observed, dict):
                    raise ValueError
                status = response.status_code
                if status in {409, 422}:
                    if (observed.get('apiVersion') == 'v1' and observed.get('kind') == 'Status'
                            and observed.get('status') == 'Failure' and observed.get('code') == status
                            and observed.get('reason') == ('Conflict' if status == 409 else 'Invalid')):
                        return None
                    raise ValueError
                if (status != 200 or observed.get('apiVersion') != 'apps/v1' or observed.get('kind') != 'Deployment'
                        or _uid(observed) != _uid(self.refresh.render.active)):
                    raise ValueError
                return observed
        except Exception:
            raise ManagementStageError('management refresh update outcome unavailable') from None

    def preview(self, before: dict[str, Any], operation_id: str) -> dict[str, Any]:
        observed = self._patch(before, 'activate', operation_id, preview=True)
        if observed is None:
            raise ManagementStageError('management refresh preview conflicted')
        return observed

    def patch(self, before: dict[str, Any], action: str, operation_id: str) -> bool:
        return self._patch(before, action, operation_id, preview=False) is not None

    def retired(self) -> bool:
        try:
            original = self.read()
            if original['spec']['replicas'] != 0:
                return False
            suffix = '/namespaces/' + self.binding.namespace
            query = '?limit=100&labelSelector=app%3Dloom-service'
            sets = self._request('GET', '/apis/apps/v1' + suffix + '/replicasets' + query)
            pods = self._request('GET', '/api/v1' + suffix + '/pods' + query)
            if sets is None or pods is None:
                raise ValueError
            if not qualify_refresh_drain(self.refresh, deployment=original, replicasets=sets, pods=pods):
                return False
            final = self.read()
            if final['metadata']['generation'] != original['metadata']['generation']:
                raise ValueError
            return qualify_refresh_drain(self.refresh, deployment=final, replicasets=sets, pods=pods)
        except Exception:
            raise ManagementStageError('management refresh drain observation unavailable') from None

    def activation_ready(self) -> bool:
        try:
            self.verify_identity(self.binding)
            return self.check_activation(self.refresh) is True
        except Exception:
            raise ManagementStageError('management refresh activation qualification unavailable') from None
