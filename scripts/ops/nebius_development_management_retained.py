"""Read the original dev-manager identity for its narrowly scoped TLS successor.

No live calls, initial-installer replay or current-source rendering. Historical
certificate expiry and retired operator credential files do not invalidate the
retained installation. A caller must still qualify the live route and new leaf.
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
from scripts.ops.nebius_application_setup import ApplicationSetupMaterial
from scripts.ops.nebius_development_management_entry import DevelopmentManagementPrivateInputs
from scripts.ops.nebius_development_management_install import _PHASES, _history
from scripts.ops.nebius_development_management_operation import validate_operation
from scripts.ops.nebius_development_management_tls import (
    ManagementTLSMaterial,
    management_tls_secret_name,
)
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_install import _journal_names
from scripts.ops.nebius_management_material import ManagementBinding, _uuid
from scripts.ops.nebius_management_stage import _MARKER, _validate_record

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


class RetainedManagementReference(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_path: Path
    operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    installation_input_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')
    qualification_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')

    @field_validator('operation_path')
    @classmethod
    def canonical_operation(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve() or value.name != 'operation.json':
            raise ValueError('retained development management operation path differs')
        return value


@dataclass(frozen=True, repr=False)
class RetainedManagementState:
    operation: dict[str, Any]
    inputs: DevelopmentManagementPrivateInputs
    binding: ManagementBinding
    ingress: dict[str, Any]
    tls: dict[str, Any]
    files: dict[Path, bytes]


def load_retained_management(reference: RetainedManagementReference) -> RetainedManagementState:
    """Check original source/input preimage, anchored phases and resource snapshots."""
    try:
        files: dict[Path, bytes] = {}

        def read(path: Path) -> dict[str, Any]:
            if not path.is_absolute() or path != path.resolve():
                raise ValueError()
            raw = private_state._private_read(path, limit=4 * 1024**2)
            files[path] = raw
            value = _json(raw)
            if not isinstance(value, dict):
                raise ValueError()
            return value

        operation = read(reference.operation_path)
        validate_operation(operation)
        if hashlib.sha256(files[reference.operation_path]).hexdigest() != reference.operation_sha256:
            raise ValueError()
        path, state, anchor = (Path(operation[key]) for key in ('inputs_path', 'state_dir', 'anchor_dir'))
        root = path.parent
        if (reference.operation_path != root / 'operation.json'
                and (reference.operation_path.parent.parent != root / 'releases'
                     or re.fullmatch(r'[0-9a-f]{64}', reference.operation_path.parent.name) is None)):
            raise ValueError()
        inputs = DevelopmentManagementPrivateInputs.model_validate(read(path))
        source = PreparedDevelopmentSource.model_validate(read(reference.operation_path.parent / 'development-management-source.json'))
        app = inputs.deployment.installation.applications
        if (hashlib.sha256(files[path]).hexdigest() != operation['inputs_sha256']
                or (inputs.binding.installation_id, inputs.binding.namespace) != (operation['installation_id'], operation['namespace'])
                or (str(inputs.deployment.installation_id), inputs.deployment.namespace) != (operation['installation_id'], operation['namespace'])
                or source.source_sha != operation['source_sha']
                or inputs.candidate.get('candidate_sha') != source.source_sha
                or inputs.profile.get('candidate_sha') != source.source_sha
                or inputs.candidate.get('source_archive_sha256') != source.source_archive_sha256
                or inputs.deployment.installation.foundation.platform_config['namespace'] != 'loom-dev'
                or inputs.deployment.installation.foundation.platform_config['environment'] != 'development'
                or inputs.deployment.installation.provider_runtime is not None
                or app is None or app.shared.platform_namespace != 'loom-dev'
                or app.runtime.build is not None or app.runtime.source_upload is not None):
            raise ValueError()
        identity = {'schema': 'loom.nebius-development-management-install.v1',
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
            for name in _journal_names(phase):
                read(state / phase / name)
        _history(record, started, state)
        bootstrap = _json(files[state / 'bootstrap/bootstrap.json'])
        if (bootstrap['schema'] != 'loom.nebius-management-bootstrap.v1'
                or bootstrap['binding'] != asdict(inputs.binding) or bootstrap['stage'] != 'bootstrapped'):
            raise ValueError()
        _uuid(bootstrap['operation_id'])
        binding = ManagementBinding(inputs.binding.installation_id, inputs.binding.namespace,
            bootstrap['namespace_uid'], inputs.binding.kube_system_uid)
        receipt = record['phases']['bootstrap']['receipt']
        if (receipt['status'] != 'management_bootstrapped' or receipt['namespace_uid'] != binding.namespace_uid
                or receipt['installation_id'] != binding.installation_id):
            raise ValueError()

        def resources(phase: str, stage: str) -> dict[str, dict[str, Any]]:
            journal = _json(files[state / phase / 'stage.json'])
            if not re.fullmatch(r'sha256:[0-9a-f]{64}', journal['revision']):
                raise ValueError()
            documents = {}
            observed = {}
            for name, item in journal['resources'].items():
                document = copy.deepcopy(item['desired'])
                annotations = document['metadata']['annotations']
                if annotations.pop(_MARKER) != journal['operation_id'] or item['status'] != 'created':
                    raise ValueError()
                if not annotations:
                    del document['metadata']['annotations']
                if name != _key(document):
                    raise ValueError()
                documents[name] = document
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                _uid(actual)
                if _snapshot(actual) != item['observed']:
                    raise ValueError()
                observed[name] = actual
            _validate_record(journal, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
                'revision': journal['revision'], 'phase': stage}, documents)
            return observed

        supplied = resources('supplied', 'supplied-material')
        application = resources('application-material', 'application-material')
        public = resources('public', '70-public.yaml')
        tls_resources = resources('tls', 'development-management-tls')

        def data(document: dict[str, Any]) -> dict[str, str]:
            return {key: base64.b64decode(value, validate=True).decode() for key, value in document['data'].items()}

        material = {document['metadata']['name']: data(document) for document in supplied.values()}

        def application_data(prefix: str) -> dict[str, str]:
            document, = (doc for doc in application.values() if doc['metadata']['name'].startswith(prefix))
            return data(document)

        shared = _json(application_data('loom-applications-shared-')['shared.json'].encode())
        application_material = ApplicationSetupMaterial(**shared,
            manager_password=application_data('loom-applications-manager-')['password'],
            cloud_credentials_json=application_data('loom-applications-cloud-')['credentials.json'])
        tls, = tls_resources.values()
        old = data(tls)
        tls_material = ManagementTLSMaterial(inputs.deployment.public_host, old['tls.crt'], old['tls.key'])
        ingress, = public.values()
        name = management_tls_secret_name(binding.installation_id, tls_material)
        if (hashlib.sha256(tls_material.chain.encode()).hexdigest() != inputs.certificate.generation
                or inputs.deployment.public_tls_secret_name != name or tls['metadata']['name'] != name
                or tls['metadata']['namespace'] != binding.namespace or tls['type'] != 'kubernetes.io/tls'
                or tls.get('immutable') is not True or ingress['kind'] != 'Ingress'
                or ingress['apiVersion'] != 'networking.k8s.io/v1'
                or ingress['metadata']['name'] != 'loom-management'
                or ingress['metadata']['namespace'] != binding.namespace
                or ingress['metadata']['labels'].get('loom.nebius/management-installation') != binding.installation_id
                or ingress['spec']['tls'] != [{'hosts': [inputs.deployment.public_host], 'secretName': name}]):
            raise ValueError()
        original = {'binding': asdict(inputs.binding), 'deployment': inputs.deployment.model_dump(mode='json'),
            'candidate': inputs.candidate, 'profile': inputs.profile, 'material': material,
            'application_material': asdict(application_material), 'shared_namespace_uid': str(inputs.shared_namespace_uid),
            'tls_material': asdict(tls_material), 'qualification_digest': reference.qualification_digest}
        if digest(original) != reference.installation_input_digest or any(
                private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files.items()):
            raise ValueError()
        return RetainedManagementState(operation, inputs, binding, ingress, tls, files)
    except Exception:
        raise ValueError('retained development management installation history unqualified') from None
