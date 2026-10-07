"""Read-only handoff from a retained independent dev installation.

The protected manager input pins the original operation and installation digest.
Completed journals plus current Kubernetes readback qualify the data binding; a
new manager never re-renders old workloads or needs old operator credentials.
Publication and provider qualification remain the connected caller's obligations.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import (
    _namespace_document,
    _secret_documents,
    _uuid,
    _validate_record,
)
from scripts.ops.nebius_development_entry import DevelopmentPrivateInputs
from scripts.ops.nebius_development_install import (
    _PHASES,
    DevelopmentInstallRequest,
    _history,
    _phase_files,
    _storage_intent,
    _storage_observation,
)
from scripts.ops.nebius_development_management_install import DevelopmentManagementRequest
from scripts.ops.nebius_development_operation import validate_operation
from scripts.ops.nebius_development_stage import (
    _MARKER,
    _RESOURCES,
    DevelopmentResourceBinding,
    DevelopmentStageInput,
    _identity,
    _validate,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_install import ManagementInstallError
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from sqlalchemy.engine import make_url

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


class RetainedDevelopmentReference(BaseModel):
    """Operator-approved identity pins, never an owner-supplied ready assertion."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_path: Path
    operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    installation_input_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')
    qualification_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')

    @field_validator('operation_path')
    @classmethod
    def canonical_operation(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve() or value.name != 'operation.json':
            raise ValueError('retained development operation path differs')
        return value


@dataclass(frozen=True, repr=False)
class RetainedDevelopmentState:
    inputs: DevelopmentPrivateInputs
    binding: DevelopmentResourceBinding
    bootstrap: dict[str, Any]
    phases: dict[str, dict[str, Any]]
    files: dict[Path, bytes]


def _read(path: Path) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise ManagementInstallError('retained development path differs')
    return private_state._private_read(path, limit=4 * 1024**2)


def load_retained_foundation(reference: RetainedDevelopmentReference) -> RetainedDevelopmentState:
    """Validate all local history before opening any foundation connection."""
    try:
        files: dict[Path, bytes] = {}

        def read(path: Path) -> dict[str, Any]:
            raw = _read(path)
            files[path] = raw
            value = _json(raw)
            if not isinstance(value, dict):
                raise ValueError()
            return value

        operation = read(reference.operation_path)
        if hashlib.sha256(files[reference.operation_path]).hexdigest() != reference.operation_sha256:
            raise ValueError()
        validate_operation(operation)
        path, state, anchor = (Path(operation[key]) for key in ('inputs_path', 'state_dir', 'anchor_dir'))
        root = path.parent
        if (reference.operation_path != root / 'operation.json'
                and (reference.operation_path.parent.parent != root / 'releases'
                     or re.fullmatch(r'[0-9a-f]{64}', reference.operation_path.parent.name) is None)):
            raise ValueError()
        inputs = DevelopmentPrivateInputs.model_validate(read(path))
        source = inputs.settings.preflight.source
        if (hashlib.sha256(files[path]).hexdigest() != operation['inputs_sha256']
                or inputs.binding.installation_id != operation['installation_id']
                or inputs.binding.kube_system_uid != str(inputs.settings.preflight.kube_system_uid)
                or inputs.config.get('namespace') != 'loom-dev' or inputs.config.get('environment') != 'development'
                or inputs.candidate.get('candidate_sha') != operation['candidate']
                or inputs.profile.get('candidate_sha') != operation['candidate']
                or source.source_sha != operation['source_sha']
                or inputs.settings.preflight.publication.source_sha != source.source_sha
                or inputs.candidate.get('source_archive_sha256') != source.source_archive_sha256):
            raise ValueError()
        identity = {'schema': 'loom.nebius-development-install.v1',
            'input_digest': reference.installation_input_digest, 'state_dir': str(state), 'binding': asdict(inputs.binding)}
        started = read(anchor / (inputs.binding.installation_id + '.json'))
        if set(started) != {*identity, 'operation_id'} or any(started[key] != value for key, value in identity.items()):
            raise ValueError()
        _uuid(started['operation_id'])
        record = read(state / 'installation.json')
        if (set(record.get('phases', {})) != set(_PHASES)
                or any(item.get('status') != 'complete' for item in record['phases'].values())):
            raise ValueError()
        for phase in _PHASES:
            for name in _phase_files(phase, complete=True):
                read(state / phase / name)
        _history(record, started, state)
        local_identity = {'schema': 'loom.nebius-development-local-bootstrap.v1',
            'binding': asdict(inputs.binding), 'state_dir': str(state / 'bootstrap')}
        marker = read(anchor / 'local-bootstrap' / (inputs.binding.installation_id + '.json'))
        if (set(marker) != {*local_identity, 'operation_id', 'material_sha256'}
                or any(marker[key] != value for key, value in local_identity.items())):
            raise ValueError()
        bootstrap = _json(files[state / 'bootstrap/bootstrap.json'])
        _validate_record(bootstrap, marker, inputs.binding)
        if bootstrap['namespace']['status'] != 'created' or any(item['status'] != 'created' for item in bootstrap['secrets'].values()):
            raise ValueError()
        binding = DevelopmentResourceBinding(inputs.binding, bootstrap['namespace']['uid'], marker['operation_id'])
        receipt = {'status': 'development_local_bootstrap_complete', 'installation_id': inputs.binding.installation_id,
            'namespace': 'loom-dev', 'namespace_uid': binding.namespace_uid,
            'secret_uids': {name: item['uid'] for name, item in bootstrap['secrets'].items()}}
        if record['phases']['bootstrap']['receipt'] != receipt:
            raise ValueError()
        phases = {phase: _json(files[state / phase / 'stage.json']) for phase in _PHASES if phase != 'bootstrap'}
        revision = phases['config']['revision']
        if not isinstance(revision, str) or re.fullmatch(r'sha256:[0-9a-f]{64}', revision) is None:
            raise ValueError()
        for phase, journal in phases.items():
            if phase == 'storage':
                intent = _storage_intent(binding, revision)
                if (_json(files[state / 'database/storage-intent.json']) != intent
                        or _json(files[state / 'storage/intent.json']) != intent
                        or set(journal) != {*intent, 'pvc_uid', 'pv_uid', 'pv_name', 'pvc_spec', 'pv_spec'}
                        or any(journal[key] != value for key, value in intent.items())
                        or record['phases'][phase]['receipt'] != {'status': 'development_storage_verified',
                            'pvc_uid': journal['pvc_uid'], 'pv_uid': journal['pv_uid']}):
                    raise ValueError()
                continue
            documents = {}
            for key, item in journal['resources'].items():
                doc = copy.deepcopy(item['desired'])
                doc['metadata']['annotations'].pop(_MARKER)
                if not doc['metadata']['annotations']:
                    del doc['metadata']['annotations']
                if (doc['metadata'].get('namespace') != 'loom-dev' or doc['apiVersion'] != _RESOURCES[doc['kind']][0]
                        or doc['metadata'].get('labels', {}).get('loom.nebius/development-installation') != inputs.binding.installation_id
                        or item['status'] != 'created' or key != doc['kind'] + ':' + doc['metadata']['name']):
                    raise ValueError()
                documents[key] = doc
            _validate(journal, _identity(binding, phase, revision), documents)
            receipt = {'status': 'development_phase_staged', 'phase': phase, 'revision': revision,
                'installation_id': inputs.binding.installation_id, 'namespace_uid': binding.namespace_uid,
                'resource_uids': {key: item['uid'] for key, item in journal['resources'].items()}}
            if record['phases'][phase]['receipt'] != receipt:
                raise ValueError()
        # Reconstruct the original request's preimage, using its retained
        # credential/qualification fingerprints, not today's operator files.
        # Merely pinning a newly edited operation must not bless a different
        # source/configuration under an old installation anchor.
        storage = phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
        selection = DevelopmentStageInput(inputs.config, inputs.candidate, inputs.profile, inputs.keyring,
            {key: base64.b64decode(value, validate=True).decode() for key, value in storage.items()})
        original = DevelopmentInstallRequest(inputs.binding, selection, reference.qualification_digest)
        if digest(asdict(original)) != reference.installation_input_digest:
            raise ValueError()
        return RetainedDevelopmentState(inputs, binding, bootstrap, phases, files)
    except Exception:
        raise ManagementInstallError('retained development installation history unqualified') from None


class HTTPSRetainedDevelopmentFoundation(ManagementKubernetesTransport):
    """Only explicit GETs for the pinned foundation; no create/resume methods."""

    error_type = ManagementInstallError

    def verify_identity(self, binding: DevelopmentResourceBinding) -> None:
        for name, uid in (('kube-system', binding.bootstrap.kube_system_uid), ('loom-dev', binding.namespace_uid)):
            row = self._request('GET', '/api/v1/namespaces/' + name)
            if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                    or row['metadata'].get('name') != name or _uid(row) != uid
                    or row['metadata'].get('deletionTimestamp') or row['metadata'].get('ownerReferences')):
                raise ManagementInstallError('retained development namespace identity differs')
            if name == 'loom-dev':
                observed = _snapshot(row)
                label = observed['metadata'].get('labels', {}).pop('kubernetes.io/metadata.name', 'loom-dev')
                spec = observed.pop('spec', {})
                if (label != 'loom-dev' or spec not in ({}, {'finalizers': ['kubernetes']})
                        or observed != _namespace_document(binding.bootstrap, binding.operation_id)):
                    raise ManagementInstallError('retained development namespace policy differs')

    def get_database_claim(self) -> dict[str, Any] | None:
        return self._request('GET', '/api/v1/namespaces/loom-dev/persistentvolumeclaims/data-loom-postgres-0')

    def get_database_volume(self) -> dict[str, Any] | None:
        claim = self.get_database_claim()
        name = (claim or {}).get('spec', {}).get('volumeName')
        if not isinstance(name, str) or re.fullmatch(r'pvc-[0-9a-f-]{36}', name) is None:
            raise ManagementInstallError('retained development volume identity unavailable')
        return self._request('GET', '/api/v1/persistentvolumes/' + name)

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        version, resource = _RESOURCES[document['kind']]
        name = document['metadata']['name']
        if (document['metadata'].get('namespace') != 'loom-dev' or document['apiVersion'] != version
                or not isinstance(name, str) or re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', name) is None):
            raise ManagementInstallError('resource outside retained development scope')
        prefix = '/api/v1' if version == 'v1' else '/apis/' + version
        return self._request('GET', prefix + '/namespaces/loom-dev/' + resource + '/' + name)

    @staticmethod
    def _ready(row: dict[str, Any]) -> bool:
        kind, status = row['kind'], row.get('status', {})
        if kind == 'Job':
            conditions = {item['type']: item['status'] for item in status.get('conditions', [])}
            return (conditions.get('Failed') != 'True' and conditions.get('Complete') == 'True'
                and status.get('succeeded', 0) >= row['spec'].get('completions', 1))
        if kind not in {'StatefulSet', 'Deployment'}:
            return True
        replicas = row['spec'].get('replicas', 1)
        if (replicas <= 0 or status.get('observedGeneration', 0) < row['metadata'].get('generation', 1)
                or any(status.get(field, 0) != replicas for field in ('replicas', 'readyReplicas', 'updatedReplicas'))):
            return False
        if kind == 'StatefulSet':
            return bool(status.get('currentRevision')) and status.get('currentRevision') == status.get('updateRevision')
        return bool(status.get('availableReplicas', 0) == replicas and status.get('unavailableReplicas', 0) == 0)

    def verify(self, *, reference: RetainedDevelopmentReference, request: DevelopmentManagementRequest) -> dict[str, Any]:
        try:
            retained = load_retained_foundation(reference)
            inputs, binding = retained.inputs, retained.binding
            app = request.deployment.installation.applications
            if (app is None or app.shared.platform_namespace != 'loom-dev'
                    or str(app.shared.data_environment_id) != binding.bootstrap.installation_id
                    or app.shared.cluster_id != inputs.config['cluster_id']
                    or request.shared_namespace_uid != binding.namespace_uid
                    or request.binding.kube_system_uid != binding.bootstrap.kube_system_uid
                    or request.deployment.installation.foundation.platform_config != inputs.config
                    or self.api_server.rstrip('/') != inputs.config['kubernetes_api_server'].rstrip('/')):
                raise ValueError()
            material = retained.bootstrap['material']
            database = make_url(material['loom-platform-db']['admin-url'])
            if (material['loom-platform-db']['ca.crt'] != request.application_material.ca_pem
                    or material['loom-platform-auth']['secret-store-master-key'] != request.application_material.secret_store_master_keys
                    or database.host != 'loom-postgres.loom-dev.svc' or database.port != 5432
                    or database.database != request.application_material.database_name or database.username != 'postgres'
                    or database.query != {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}):
                raise ValueError()
            self.verify_identity(binding)
            for name, doc in _secret_documents(material, inputs.binding, binding.operation_id).items():
                row = self.get_resource(doc)
                if row is None or _uid(row) != retained.bootstrap['secrets'][name]['uid'] or _snapshot(row) != doc:
                    raise ValueError()
            for phase, journal in retained.phases.items():
                if phase == 'storage':
                    # Storage observation consumes only config, never rerenders
                    # workloads or requires old operator/storage credentials.
                    selection = DevelopmentStageInput(inputs.config, inputs.candidate, inputs.profile, inputs.keyring, {})
                    observed = _storage_observation(DevelopmentInstallRequest(inputs.binding, selection), binding, self)
                    if observed is None or any(journal[key] != value for key, value in observed.items()):
                        raise ValueError()
                    continue
                for item in journal['resources'].values():
                    row = self.get_resource(item['desired'])
                    if (row is None or _uid(row) != item['uid'] or _snapshot(row) != item['observed']
                            or not self._ready(row)):
                        raise ValueError()
            self.verify_identity(binding)
            if any(_read(path) != raw for path, raw in retained.files.items()):
                raise ValueError()
            return {'namespace': 'loom-dev', 'namespace_uid': binding.namespace_uid,
                'data_environment_id': binding.bootstrap.installation_id,
                'source_sha': inputs.settings.preflight.source.source_sha,
                'retained_digest': digest({str(path): hashlib.sha256(raw).hexdigest() for path, raw in retained.files.items()})}
        except Exception:
            raise ManagementInstallError('retained development foundation unqualified') from None
