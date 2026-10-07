"""Connect only the fresh dev manager to fixed transports and actual-subject probes.

Reuse retained backup/public evidence, not legacy installation or ingress history.
The private entry must supply dev-specific foundation/provider/route qualification.
"""
from __future__ import annotations

import json
import re
import ssl
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import (
    ApplicationSetupRequest,
    HTTPSApplicationSetupAPI,
    _documents,
    _revision,
    application_setup_ready,
)
from scripts.ops.nebius_development_live import _private
from scripts.ops.nebius_development_management_install import (
    _APPLICATION_PHASES,
    DevelopmentManagementRequest,
    _setup,
    render_installation,
)
from scripts.ops.nebius_development_management_tls import HTTPSManagementTLSAPI
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
from scripts.ops.nebius_management_bootstrap import HTTPSBootstrapAPI
from scripts.ops.nebius_management_install import ManagementInstallError
from scripts.ops.nebius_management_live import (
    HTTPSManagementInstallationAPI,
    ManagementPrerequisites,
)
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI, _validate_record
from scripts.ops.nebius_management_supplied import HTTPSSuppliedMaterialAPI

from loom_service.environment_management.deployment import RenderedManagement


class DevelopmentManagementPrerequisites(ManagementPrerequisites, Protocol):
    def qualify_storage(self, request: DevelopmentManagementRequest, binding: ManagementBinding,
                        rendered: RenderedManagement, receipt: dict[str, Any]) -> None: ...


class HTTPSDevelopmentManagementAPI(HTTPSManagementInstallationAPI):
    _public_runtime: Literal['legacy', 'applications'] = 'applications'

    def __init__(self, *, request: DevelopmentManagementRequest, api_server: str, ssl_context: ssl.SSLContext,
                 runtime_ca_pem: str | None, checks: DevelopmentManagementPrerequisites, token: str | None = None,
                 private_files: dict[Path, bytes] | None = None):
        # Do not invoke the legacy constructor: its renderer requires the legacy
        # provisioner. Only common fixed phase/evidence methods are inherited.
        self.request, self.development_request = request, request
        self.rendered, self.checks = render_installation(request), checks
        self.development_checks = checks
        if api_server.rstrip('/') != request.deployment.installation.foundation.platform_config['kubernetes_api_server'].rstrip('/'):
            raise ManagementInstallError('development management cluster endpoint differs')
        self.diagnostic_stage = None
        self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
        self.runtime_trust = ssl.create_default_context(cadata=runtime_ca_pem)
        self.private_files = dict(private_files or {})

    def _private_inputs(self) -> None:
        try:
            if any(_private(path) != raw for path, raw in self.private_files.items()):
                raise ValueError()
        except Exception:
            raise ManagementInstallError('development management private input changed') from None

    def bootstrap_api(self) -> HTTPSBootstrapAPI:
        self._private_inputs()
        return super().bootstrap_api()

    def _binding(self, binding: ManagementBinding) -> None:
        self._private_inputs()
        super()._binding(binding)

    def resources(self, binding: ManagementBinding, phase: str) -> HTTPSManagementStageAPI:
        self._binding(binding)
        if phase == 'authority':
            raise ManagementInstallError('legacy authority is outside development management')
        if phase == 'supplied':
            return HTTPSSuppliedMaterialAPI(material=self.request.material, binding=binding, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token, application_only=True)
        if phase == 'tls':
            return HTTPSManagementTLSAPI(material=self.development_request.tls_material, binding=binding,
                api_server=self.api_server, ssl_context=self.ssl_context, token=self.token)
        return super().resources(binding, phase)

    def application_resources(self, request: ApplicationSetupRequest, phase: str) -> HTTPSApplicationSetupAPI:
        self._binding(request.binding)
        if request != _setup(self.development_request, request.binding) or phase not in _APPLICATION_PHASES:
            raise ManagementInstallError('resource outside development application setup')
        return HTTPSApplicationSetupAPI(request=request, phase=phase, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token)

    def qualify_storage(self, binding: ManagementBinding, rendered: RenderedManagement, receipt: dict[str, Any]) -> None:
        self._binding(binding)
        if rendered != self.rendered:
            raise ManagementInstallError('development management storage input differs')
        self.development_checks.qualify_storage(self.development_request, binding, rendered, receipt)

    def _recorded_setup(self, request: ApplicationSetupRequest, phase: str, state: Path) -> dict[str, dict[str, Any]]:
        documents = _documents(request, phase)
        record = json.loads(private_state._private_read(state / ('application-' + phase) / 'stage.json', limit=4 * 1024**2))
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(request.binding),
            'revision': _revision(request, documents), 'phase': 'application-' + phase}
        _validate_record(record, identity, documents)
        result = {}
        with self.application_resources(request, phase) as api:
            api.verify_identity(request.binding)
            for item in record['resources'].values():
                if item['status'] != 'created':
                    raise ManagementInstallError('development application setup incomplete')
                actual = api.get_resource(item['desired'])
                if actual is None or _uid(actual) != item['uid'] or _snapshot(actual) != item['observed']:
                    raise ManagementInstallError('development application setup changed')
                result[_key(actual)] = actual
            api.verify_identity(request.binding)
        return result

    def qualify_application(self, request: ApplicationSetupRequest, state_dir: Path) -> None:
        try:
            self._binding(request.binding)
            if (request != _setup(self.development_request, request.binding)
                    or not (state_dir / 'application-admission/stage.json').is_file()):
                raise ValueError()
            with self.application_resources(request, 'admission') as api:
                if not application_setup_ready(request=request, phase='admission', api=api,
                                               state_dir=state_dir / 'application-admission'):
                    raise ManagementInstallError('development application admission propagation pending')
            self._recorded_setup(request, 'permissions', state_dir)
            key = 'ServiceAccount:' + request.binding.namespace + ':loom-application-provisioner'
            account = self._recorded_setup(request, 'config', state_dir)[key]
            path = '/api/v1/namespaces/' + request.binding.namespace + '/serviceaccounts/loom-application-provisioner/token'
            with self.application_resources(request, 'config') as api:
                api.verify_identity(request.binding)
                result = api._request('POST', path, document={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest',
                    'spec': {'audiences': [], 'expirationSeconds': 600}})
            after = self._recorded_setup(request, 'config', state_dir)[key]
            if (result is None or result.get('kind') != 'TokenRequest'
                    or _uid(after) != _uid(account) or _snapshot(after) != _snapshot(account)):
                raise ValueError()
            token = result['status']['token']
            expires = datetime.fromisoformat(result['status']['expirationTimestamp'].replace('Z', '+00:00'))
            if (not isinstance(token, str) or not 0 < len(token) <= 16384
                    or re.fullmatch(r'[A-Za-z0-9._~+/-]+={0,2}', token) is None
                    or not 30 < (expires - datetime.now(UTC)).total_seconds() <= 660):
                raise ValueError()
            application = request.deployment.installation.applications
            assert application is not None
            with HTTPSManagementAuthorityProbe(authority=application.authority, service_account_uid=_uid(account),
                api_server=self.api_server, ssl_context=self.runtime_trust, token=token) as probe:
                if not probe.qualify():
                    raise ManagementInstallError('development application authority propagation pending')
            after = self._recorded_setup(request, 'config', state_dir)[key]
            if _uid(after) != _uid(account) or _snapshot(after) != _snapshot(account):
                raise ManagementInstallError('development application runtime account changed')
            self._recorded_setup(request, 'permissions', state_dir)
        except ManagementInstallError:
            raise
        except Exception:
            raise ManagementInstallError('development application runtime subject unavailable') from None
