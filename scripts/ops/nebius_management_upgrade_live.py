"""Connect the fixed upgrade to retained Kubernetes state and actual subjects."""
from __future__ import annotations

import json
import re
import ssl
import tomllib
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import (
    ApplicationSetupRequest,
    HTTPSApplicationSetupAPI,
    _documents,
    _revision,
    application_setup_ready,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
from scripts.ops.nebius_management_authority_stage import _PATHS as _AUTHORITY_PATHS
from scripts.ops.nebius_management_material import _documents as material_documents
from scripts.ops.nebius_management_proofs import ManagementPublicProbe
from scripts.ops.nebius_management_stage import _RESOURCES, ManagementStageError, _validate_record
from scripts.ops.nebius_management_storage import verify_management_storage
from scripts.ops.nebius_management_switch import (
    HTTPSManagementSwitchAPI,
    ManagementSwitchRequest,
    _matches,
)
from scripts.ops.nebius_management_upgrade import (
    ManagementUpgradeError,
    ManagementUpgradeRequest,
    _original,
)

from loom_service.environment_management.deployment import render_management


class ManagementUpgradePrerequisites(Protocol):
    def preflight(self, request: ManagementUpgradeRequest) -> None:
        """Exact publication, live shared material/schema/IAM, route and physical fit."""
        ...

    def public_route(self, request: ManagementUpgradeRequest) -> None: ...


class HTTPSManagementUpgradeAPI(HTTPSApplicationSetupAPI):
    def __init__(self, *, request: ManagementUpgradeRequest, api_server: str, ssl_context: ssl.SSLContext,
                 runtime_ca_pem: str | None, checks: ManagementUpgradePrerequisites, token: str | None = None):
        self.request, self.checks = request, checks
        self.diagnostic_stage: str | None = None
        self.ssl_context, self.token = ssl_context, token
        # Never let operator mTLS authenticate a purported service-account probe.
        self.runtime_trust = ssl.create_default_context(cadata=runtime_ca_pem)
        self.rendered = render_management(request.setup.deployment, candidate=request.setup.candidate,
            profile=request.setup.profile, repo_root=request.setup.repo_root)
        super().__init__(request=request.setup, phase='config', api_server=api_server, ssl_context=ssl_context, token=token)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('resource outside connected upgrade operation')

    def resources(self, request: ApplicationSetupRequest, phase: str) -> HTTPSApplicationSetupAPI:
        if request != self.request.setup:
            raise ManagementStageError('application upgrade stage binding differs')
        return HTTPSApplicationSetupAPI(request=request, phase=phase, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token)

    def switch_api(self, request: ManagementSwitchRequest) -> HTTPSManagementSwitchAPI:
        original, _ = _original(self.request)
        if request.setup != self.request.setup or not _matches(request.original, original, _uid(original)):
            raise ManagementStageError('application upgrade switch binding differs')
        return HTTPSManagementSwitchAPI(request=request, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token)

    def _read_retained(self, document: dict[str, Any], uid: str) -> dict[str, Any]:
        """Only read the fixed kinds retained in hash-qualified installation state."""
        self.verify_identity(self.binding)
        kind, metadata = document['kind'], document['metadata']
        name = metadata['name']
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', name):
            raise ManagementStageError('invalid retained management name')
        if kind in _AUTHORITY_PATHS:
            if metadata.get('namespace'):
                raise ManagementStageError('retained cluster authority scope differs')
            path = _AUTHORITY_PATHS[kind]
        else:
            version, resource = _RESOURCES[kind]
            if metadata.get('namespace') != self.binding.namespace or document['apiVersion'] != version:
                raise ManagementStageError('retained management namespace differs')
            path = ('/api/v1' if version == 'v1' else '/apis/' + version) + '/namespaces/' + self.binding.namespace + '/' + resource
        actual = self._request('GET', path + '/' + name)
        if actual is None or _uid(actual) != uid or _snapshot(actual) != document:
            raise ManagementStageError('retained management resource changed')
        return actual

    def _retained_phase(self, phase: str) -> dict[str, dict[str, Any]]:
        if phase not in {'config', 'authority', 'supplied', 'database', 'migration', 'backup', 'schedule', 'service', 'public'}:
            raise ManagementStageError('unqualified retained management phase')
        record = json.loads(private_state._private_read(self.request.original_state / phase / 'stage.json', limit=4 * 1024**2))
        if record['binding'] != asdict(self.binding) or record['schema'] != 'loom.nebius-management-stage.v1':
            raise ManagementStageError('retained management phase binding differs')
        result = {}
        for key, item in record['resources'].items():
            if item['status'] != 'created':
                raise ManagementStageError('retained management phase incomplete')
            if phase == 'service' and item['observed']['kind'] == 'Deployment':
                continue  # The fixed switch, not old-template equality, owns it.
            result[key] = self._read_retained(item['observed'], item['uid'])
        return result

    def _retained_material(self) -> dict[str, Any]:
        record = json.loads(private_state._private_read(
            self.request.original_state / 'bootstrap/material/material.json', limit=1024**2))
        if record['binding'] != asdict(self.binding) or record['status'] != 'delivered':
            raise ManagementStageError('retained management credentials unavailable')
        for name, document in material_documents(record['material'], self.binding, record['operation_id']).items():
            self._read_retained(document, record['resources'][name]['uid'])
        return dict(record['material'])

    def get_database_claim(self) -> dict[str, Any] | None:
        self.verify_identity(self.binding)
        return self._request('GET', '/api/v1/namespaces/' + self.binding.namespace + '/persistentvolumeclaims/data-loom-postgres-0')

    def get_database_volume(self) -> dict[str, Any] | None:
        claim = self.get_database_claim()
        name = (claim or {}).get('spec', {}).get('volumeName')
        if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', name):
            raise ManagementStageError('retained management volume name unavailable')
        return self._request('GET', '/api/v1/persistentvolumes/' + name)

    @staticmethod
    def _ready(value: dict[str, Any]) -> bool:
        status = value.get('status', {})
        if (value['spec'].get('replicas') != 1 or status.get('observedGeneration', 0) < value['metadata'].get('generation', 1)
                or any(status.get(key, 0) != 1 for key in ('replicas', 'updatedReplicas', 'readyReplicas'))):
            return False
        if value['kind'] == 'StatefulSet':
            return bool(status.get('currentRevision')) and status.get('currentRevision') == status.get('updateRevision')
        return bool(status.get('availableReplicas', 0) == 1 and status.get('unavailableReplicas', 0) == 0)

    def preflight(self, request: ManagementUpgradeRequest) -> None:
        try:
            self.diagnostic_stage = 'recovery'
            if request != self.request:
                raise ValueError
            _original(request)
            self.diagnostic_stage = 'cluster_identity'
            self.verify_identity(self.binding)
            self.diagnostic_stage = 'resource_inventory'
            retained = {}
            for phase in ('config', 'authority', 'supplied', 'database', 'migration', 'backup', 'schedule', 'service', 'public'):
                retained.update(self._retained_phase(phase))
            self._retained_material()
            database = retained['StatefulSet:' + self.binding.namespace + ':loom-postgres']
            if not self._ready(database):
                raise ValueError
            self.diagnostic_stage = 'persistent_storage'
            # Use the old recorded template/revision, not a new rendering of it.
            record = json.loads(private_state._private_read(request.original_state / 'database/stage.json', limit=4 * 1024**2))
            original = replace(self.rendered, revision=record['revision'], files={'20-database.yaml': [database]})
            verify_management_storage(rendered=original, binding=self.binding, api=self,
                state_dir=request.original_state / 'database', evidence_dir=request.original_state / 'storage')
            self.diagnostic_stage = 'prerequisites'
            self.checks.preflight(request)
            self.verify_identity(self.binding)
            self.diagnostic_stage = None
        except Exception:
            raise ManagementUpgradeError(stage=self.diagnostic_stage or 'prerequisites') from None

    def _recorded_setup(self, phase: str, state_dir: Path) -> dict[str, dict[str, Any]]:
        setup = self.request.setup
        documents = _documents(setup, phase)
        record = json.loads(private_state._private_read(state_dir / phase / 'stage.json', limit=4 * 1024**2))
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': _revision(setup, documents), 'phase': 'application-' + phase}
        _validate_record(record, identity, documents)
        result = {}
        with self.resources(setup, phase) as api:
            api.verify_identity(self.binding)
            for item in record['resources'].values():
                if item['status'] != 'created':
                    raise ManagementStageError('application upgrade prerequisite not staged')
                actual = api.get_resource(item['desired'])
                if actual is None or _uid(actual) != item['uid'] or _snapshot(actual) != item['observed']:
                    raise ManagementStageError('application upgrade prerequisite changed')
                result[_key(actual)] = actual
        return result

    def qualify_authority(self, request: ApplicationSetupRequest, state_dir: Path) -> bool:
        try:
            if request != self.request.setup or not (state_dir / 'admission/stage.json').is_file():
                raise ValueError
            with self.resources(request, 'admission') as api:
                if not application_setup_ready(request=request, phase='admission', api=api, state_dir=state_dir / 'admission'):
                    return False
            self._recorded_setup('permissions', state_dir)
            key = 'ServiceAccount:' + self.binding.namespace + ':loom-application-provisioner'
            account = self._recorded_setup('config', state_dir)[key]
            path = '/api/v1/namespaces/' + self.binding.namespace + '/serviceaccounts/loom-application-provisioner/token'
            result = self._request('POST', path, document={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest',
                'spec': {'audiences': [], 'expirationSeconds': 600}})
            after = self._recorded_setup('config', state_dir)[key]
            if (result is None or result.get('kind') != 'TokenRequest'
                    or _uid(after) != _uid(account) or _snapshot(after) != _snapshot(account)):
                raise ValueError
            token = result['status']['token']
            expires = datetime.fromisoformat(result['status']['expirationTimestamp'].replace('Z', '+00:00'))
            if (not isinstance(token, str) or not 0 < len(token) <= 16384
                    or re.fullmatch(r'[A-Za-z0-9._~+/-]+={0,2}', token) is None
                    or not 30 < (expires - datetime.now(UTC)).total_seconds() <= 660):
                raise ValueError
            application = request.deployment.installation.applications
            assert application is not None
            with HTTPSManagementAuthorityProbe(authority=application.authority, service_account_uid=_uid(account),
                api_server=self.api_server, ssl_context=self.runtime_trust, token=token) as probe:
                return probe.qualify()
        except ManagementStageError:
            raise
        except Exception:
            raise ManagementStageError('application runtime subject qualification unavailable') from None

    def verify_public(self, request: ManagementUpgradeRequest, state_dir: Path) -> bool:
        try:
            if request != self.request:
                raise ValueError
            original, _ = _original(request)
            record = json.loads(private_state._private_read(state_dir / 'switch/switch.json', limit=4 * 1024**2))
            if (record['phase'] != 'active' or record['binding'] != asdict(self.binding)
                    or record['revision'] != self.rendered.revision or record['original_uid'] != _uid(original)
                    or record['shared_namespace_uid'] != request.setup.shared_namespace_uid):
                raise ValueError
            with self.switch_api(ManagementSwitchRequest(request.setup, original)) as api:
                actual = api.read()
                if not _matches(actual, record['active'], _uid(original)):
                    raise ValueError
                if not self._ready(actual):
                    return False
            self._retained_phase('service')
            self._retained_phase('public')
            material = self._retained_material()
            self.checks.public_route(request)
            token = tomllib.loads(material['loom-admin-secret']['secrets.toml'])['admin']['token']
            with ManagementPublicProbe(host=request.setup.deployment.public_host, runtime='applications') as probe:
                probe.verify(admin_token=token)
            return True
        except Exception:
            raise ManagementUpgradeError(stage='public_authentication') from None
