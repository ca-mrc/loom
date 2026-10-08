"""Read completed fresh-dev pool history without replay, transport or new rendering.

Historical evidence is not current runtime authority. A successor must separately
qualify live identities and configuration before any grant or workload transition.
"""
from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_retained import load_retained_management
from scripts.ops.nebius_development_pool_entry import DevelopmentPoolPrivateInputs
from scripts.ops.nebius_development_pool_intent import (
    DevelopmentPoolIntent,
    bind_namespaces,
    prepare_intent,
)
from scripts.ops.nebius_development_pool_operation import operator_home, validate_operation
from scripts.ops.nebius_development_pool_registration import DevelopmentPoolRegistrationRequest
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _MARKER, _comparison_snapshot, _validate_record
from scripts.ops.nebius_pool_registration import registration_documents, validate_registration_proof

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json

_PHASES = ('namespaces', 'registration', 'material', 'configuration', 'workload')


class RetainedDevelopmentPoolReference(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_path: Path
    operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('operation_path')
    @classmethod
    def canonical_operation(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve() or value.name != 'operation.json':
            raise ValueError('retained development pool operation path differs')
        return value


@dataclass(frozen=True, repr=False)
class RetainedDevelopmentPoolState:
    operation: dict[str, Any]
    intent: DevelopmentPoolIntent
    request: DevelopmentPoolRegistrationRequest
    resources: dict[str, dict[str, Any]]
    files: dict[Path, bytes]


def load_retained_pool(reference: RetainedDevelopmentPoolReference) -> RetainedDevelopmentPoolState:
    """Read original inputs, complete anchored stages and the exact SQL receipt."""
    try:
        reference = RetainedDevelopmentPoolReference.model_validate(reference.model_dump())
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

        def checksum(path: Path) -> str:
            return hashlib.sha256(files[path]).hexdigest()

        operation = read(reference.operation_path)
        validate_operation(operation)
        if checksum(reference.operation_path) != reference.operation_sha256:
            raise ValueError()
        input_path = Path(operation['inputs_path'])
        root = input_path.parent
        if (reference.operation_path != root / 'operation.json'
                and (reference.operation_path.parent.parent != root / 'releases'
                     or re.fullmatch(r'[0-9a-f]{64}', reference.operation_path.parent.name) is None)):
            raise ValueError()
        inputs = DevelopmentPoolPrivateInputs.model_validate(read(input_path))
        source = PreparedDevelopmentSource.model_validate(read(reference.operation_path.parent / 'development-pool-source.json'))
        if checksum(input_path) != operation['inputs_sha256'] or source.source_sha != operation['source_sha']:
            raise ValueError()
        intent = prepare_intent(reference=inputs.retained, catalog=inputs.catalog,
            tokens={identity: value.get_secret_value() for identity, value in inputs.tokens.items()})
        # Namespace binding below loads and qualifies the original manager. No
        # retired operator CA/credential file is opened to read historical state.
        retained = load_retained_management(inputs.retained)
        if (retained.binding.installation_id != operation['installation_id']
                or retained.binding.namespace != operation['namespace']
                or Path(retained.operation['inputs_path']).parents[3] != operator_home(operation)
                or intent.catalog['operation_id'] != operation['operation_id']
                or inputs.operator_connection.endpoint != retained.inputs.deployment.installation.foundation.platform_config[
                    'kubernetes_api_server'].rstrip('/')):
            raise ValueError()
        files.update(retained.files)
        original = Path(retained.operation['state_dir']).parent
        state, anchor = original / 'pool-installation', Path(retained.operation['anchor_dir'])
        identity = {'schema': 'loom.nebius-development-pool-install.v1',
            'operation_id': intent.catalog['operation_id'], 'state_dir': str(state),
            'intent_sha256': digest({'reference': intent.reference.model_dump(mode='json'), 'catalog': intent.catalog})}
        if read(anchor / 'pool-installation.json') != identity:
            raise ValueError()
        parent = read(state / 'installation.json')
        if (set(parent) != {*identity, 'phases'} or any(parent[key] != value for key, value in identity.items())
                or set(parent['phases']) != set(_PHASES)):
            raise ValueError()
        records = {}
        for phase in _PHASES:
            path = (original / 'pool-registration/registration.json' if phase == 'registration'
                    else state / phase / 'stage.json')
            records[phase] = read(path)
            if parent['phases'][phase] != {'status': 'complete', 'sha256': checksum(path)}:
                raise ValueError()
        resources: dict[str, dict[str, Any]] = {}

        def stage(record: dict[str, Any], *, phase: str, revision: str | None = None) -> dict[str, dict[str, Any]]:
            documents, observed = {}, {}
            for key, item in record['resources'].items():
                document = copy.deepcopy(item['desired'])
                annotations = document['metadata']['annotations']
                if annotations.pop(_MARKER) != record['operation_id'] or item['status'] != 'created':
                    raise ValueError()
                if not annotations:
                    del document['metadata']['annotations']
                if key != _key(document):
                    raise ValueError()
                documents[key] = document
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                _uid(actual)
                if _snapshot(actual) != item['observed'] or _comparison_snapshot(actual) != item['expected']:
                    raise ValueError()
                observed[key] = actual
            _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(retained.binding),
                'revision': revision or digest(documents), 'phase': phase}, documents)
            if not observed or resources.keys() & observed.keys():
                raise ValueError()
            resources.update(observed)
            return observed

        namespaces = stage(records['namespaces'], phase='development-pool-namespaces')
        if any(row['kind'] != 'Namespace' for row in namespaces.values()):
            raise ValueError()
        request = bind_namespaces(intent, {row['metadata']['name']: _uid(row) for row in namespaces.values()})
        registration = request.registration
        reg_state = original / 'pool-registration'
        reg_documents = registration_documents(registration)
        reg_identity = {'schema': 'loom.nebius-development-pool-registration.v1',
            'operation_id': str(registration.spec.operation_id), 'state_dir': str(reg_state),
            'binding': asdict(retained.binding), 'contract_sha256': digest({
                'original': inputs.retained.model_dump(mode='json'), 'documents': reg_documents})}
        record = records['registration']
        if (read(anchor / 'pool-registration.json') != reg_identity
                or set(record) != {*reg_identity, 'phase', 'proof', 'stage_sha256'}
                or any(record[key] != value for key, value in reg_identity.items()) or record['phase'] != 'complete'):
            raise ValueError()
        reg_path = reg_state / 'resources/stage.json'
        reg_record = read(reg_path)
        if record['stage_sha256'] != checksum(reg_path):
            raise ValueError()
        _validate_record(reg_record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(retained.binding),
            'revision': digest({'documents': reg_documents, 'binding': asdict(retained.binding)}),
            'phase': 'pool-registration'}, reg_documents)
        validate_registration_proof(registration, reg_path.parent, record['proof'])
        stage(reg_record, phase='pool-registration', revision=reg_record['revision'])
        for phase in ('material', 'configuration', 'workload'):
            stage(records[phase], phase='development-pool-' + phase)
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files.items()):
            raise ValueError()
        return RetainedDevelopmentPoolState(operation, intent, request, resources, files)
    except Exception:
        raise ValueError('retained development pool unqualified') from None
