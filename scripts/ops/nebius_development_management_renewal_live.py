"""Narrow HTTPS dev-manager TLS successor; no controller or staging writes."""
from __future__ import annotations

import hashlib
import re
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops import nebius_development_management_route as routes
from scripts.ops.nebius_development_management_renewal import (
    RenewalError,
    RenewalRequest,
    _stable,
    _target,
)
from scripts.ops.nebius_development_management_tls import (
    HTTPSManagementTLSAPI,
    ManagementTLSMaterial,
)
from scripts.ops.nebius_ingress_probe import ProbeError
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import ManagementStageAPI

from loom_service.environment_management.candidates import _json


class HTTPSDevelopmentManagementRenewalAPI(HTTPSManagementTLSAPI):
    """Immutable bound Secret creation plus one UID/resourceVersion Ingress CAS.

    The caller owns the route transport's lifetime. Identity reads precede the
    composer's final Ingress read; PATCH checks captured local files without slow
    network requalification that could unnecessarily stale that resourceVersion.
    """

    error_type = RenewalError

    def __init__(self, *, request: RenewalRequest, route: routes.HTTPSDevelopmentManagementRoute,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None,
                 private_files: dict[Path, bytes] | None = None):
        self.request, self.route = request, route
        self.private_files = dict(private_files or {})
        for path, raw in request.retained.files.items():
            if path in self.private_files and self.private_files[path] != raw:
                raise RenewalError('development management renewal captured files differ')
            self.private_files[path] = raw
        binding = request.retained.binding
        foundation = request.retained.inputs.deployment.installation.foundation
        if (binding.namespace != 'loom-nebius-management-dev'
                or api_server.rstrip('/') != foundation.platform_config['kubernetes_api_server'].rstrip('/')
                or route.api_server.rstrip('/') != api_server.rstrip('/')
                or request.material.public_host != request.retained.inputs.deployment.public_host):
            raise RenewalError('development management renewal connection differs')
        self.path = '/apis/networking.k8s.io/v1/namespaces/loom-nebius-management-dev/ingresses/loom-management'
        self.generation = hashlib.sha256(request.material.chain.encode()).hexdigest()
        self._private_inputs()
        super().__init__(material=request.material, binding=binding,
            api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        try:
            if any(private_state._private_read(path, limit=4 * 1024**2) != raw
                   for path, raw in self.private_files.items()):
                raise ValueError()
        except Exception:
            raise RenewalError('development management renewal private input changed') from None

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        if method != 'GET':
            self._private_inputs()
        return super()._request(method, path, document=document)

    def verify_identity(self, binding: ManagementBinding) -> None:
        self._private_inputs()
        super().verify_identity(binding)

    def verify(self) -> None:
        self.verify_identity(self.binding)

    def read_ingress(self) -> dict[str, Any]:
        try:
            value = self._request('GET', self.path)
            if (value is None or value.get('apiVersion') != 'networking.k8s.io/v1' or value.get('kind') != 'Ingress'
                    or value['metadata'].get('namespace') != self.binding.namespace
                    or value['metadata'].get('name') != 'loom-management'
                    or _uid(value) != _uid(self.request.retained.ingress)):
                raise ValueError()
            _stable(value)
            return value
        except Exception:
            raise RenewalError('development management renewal Ingress identity differs') from None

    def _route(self, expected: dict[str, Any]) -> str:
        try:
            return self.route.qualify_retained(deployment=self.request.retained.inputs.deployment,
                kube_system_uid=self.binding.kube_system_uid, expected=expected)
        except Exception:
            raise RenewalError('development management renewal route unqualified') from None

    def qualify_route(self, expected: dict[str, Any]) -> None:
        self._route(expected)

    @contextmanager
    def tls(self, material: ManagementTLSMaterial, binding: ManagementBinding) -> Iterator[ManagementStageAPI]:
        if material != self.request.material or binding != self.binding:
            raise RenewalError('development management renewal TLS binding differs')
        self.verify()
        yield self

    def _patch(self, before: dict[str, Any], target: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        try:
            original = _stable(self.request.retained.ingress)
            name = before['spec']['tls'][0]['secretName']
            original['spec']['tls'][0]['secretName'] = name
            version = before['metadata']['resourceVersion']
            if (not isinstance(name, str) or not re.fullmatch(r'loom-management-tls-[0-9a-f]{40}', name)
                    or _stable(before) != original
                    or target != _target(before, self.binding.installation_id, self.generation)
                    or target == original or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError()
            patches = [
                {'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec/tls/0/secretName', 'value': target['spec']['tls'][0]['secretName']},
            ]
            self._private_inputs()
            with self.client.stream('PATCH', self.path + ('?dryRun=All' if preview else ''), json=patches,
                                    headers={'Content-Type': 'application/json-patch+json'}) as response:
                if response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise ValueError()
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(raw) + len(chunk) > 4 * 1024**2:
                        raise ValueError()
                    raw.extend(chunk)
                observed = _json(bytes(raw))
                if not isinstance(observed, dict):
                    raise ValueError()
                status = response.status_code
                if status in {403, 409, 422}:
                    if (observed.get('apiVersion') == 'v1' and observed.get('kind') == 'Status'
                            and observed.get('status') == 'Failure' and observed.get('code') == status
                            and observed.get('reason') == {403: 'Forbidden', 409: 'Conflict', 422: 'Invalid'}[status]):
                        return None
                    raise ValueError()
                if status != 200 or _stable(observed) != target:
                    raise ValueError()
                return observed
        except Exception:
            raise RenewalError('development management certificate update outcome unavailable') from None

    def preview(self, before: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
        result = self._patch(before, target, preview=True)
        if result is None:
            raise RenewalError('development management certificate preview rejected')
        return result

    def patch(self, before: dict[str, Any], target: dict[str, Any]) -> bool:
        return self._patch(before, target, preview=False) is not None

    def public_ready(self, target: dict[str, Any], fingerprint: str) -> bool:
        address = self._route(target)
        ready = True
        try:
            routes.qualify_tls_address(self.request.material.public_host, address, fingerprint)
        except ProbeError:
            ready = False
        if self._route(target) != address:
            raise RenewalError('development management renewal public address changed')
        return ready
