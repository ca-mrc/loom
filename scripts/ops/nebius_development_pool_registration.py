"""Closed registration rooted in the independent dev manager, not legacy cutover.

Internal protected composition, not an operator CLI. This writes only the fixed
registration ConfigMap/Job and its closed metadata transaction. Opening admission,
runtime delivery and qualification of sole physical-pool authority are separate.
"""
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_retained import (
    RetainedManagementReference,
    RetainedManagementState,
    load_retained_management,
)
from scripts.ops.nebius_management_stage import ManagementStageAPI
from scripts.ops.nebius_pool_registration import (
    PoolRegistrationRequest,
    registration_documents,
    stage_pool_registration,
    validate_registration_proof,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.installation import PoolInstallation


@dataclass(frozen=True, repr=False)
class DevelopmentPoolRegistrationRequest:
    reference: RetainedManagementReference
    retained: RetainedManagementState
    registration: PoolRegistrationRequest


def prepare_registration(*, reference: RetainedManagementReference,
                         spec: PoolInstallation) -> DevelopmentPoolRegistrationRequest:
    """Derive the database namespace and published image from completed history."""
    try:
        reference = RetainedManagementReference.model_validate(reference.model_dump())
        retained = load_retained_management(reference)
        spec = PoolInstallation.model_validate(spec.model_dump())
        config = retained.inputs.deployment.installation.foundation.platform_config
        applications = retained.inputs.deployment.installation.applications
        participant, = spec.participants
        if (retained.binding.namespace != 'loom-nebius-management-dev'
                or str(spec.installation_id) != retained.binding.installation_id
                or spec.cluster_id != config['cluster_id']
                or spec.node_group_id != config['execution_node_group_id']
                or applications is None
                or participant.environment_class != 'development'
                or participant.environment_id != applications.shared.data_environment_id
                or participant.execution_namespace.name != config['execution_namespace']
                or participant.build_namespace.name != config['execution_namespace'] + '-build'):
            raise ValueError()
        registration = PoolRegistrationRequest(spec=spec, binding=retained.binding, candidate=retained.inputs.candidate)
        registration_documents(registration)
        return DevelopmentPoolRegistrationRequest(reference, retained, registration)
    except Exception:
        raise ValueError('development pool registration inputs unqualified') from None


class DevelopmentPoolRegistrationAPI(ManagementStageAPI, Protocol):
    def qualify(self, request: DevelopmentPoolRegistrationRequest) -> None:
        """Verify unchanged original database/manager resources and namespaces."""
        ...

    def registration_report(self, state_dir: Path) -> dict[str, Any] | None: ...


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def register_development_pool(*, request: DevelopmentPoolRegistrationRequest,
                              api: DevelopmentPoolRegistrationAPI, execute: bool) -> dict[str, Any]:
    """One anchored operation per original manager; no uncertain-create retry."""
    try:
        if type(execute) is not bool or request != prepare_registration(
                reference=request.reference, spec=request.registration.spec):
            raise ValueError()
        retained, registration = request.retained, request.registration
        original = Path(retained.operation['state_dir'])
        state, anchor = original.parent / 'pool-registration', Path(retained.operation['anchor_dir'])
        if (state != state.resolve() or anchor != anchor.resolve() or not anchor.is_dir()
                or state == anchor or state in anchor.parents or anchor in state.parents):
            raise ValueError()
        identity = {'schema': 'loom.nebius-development-pool-registration.v1',
            'operation_id': str(registration.spec.operation_id), 'state_dir': str(state),
            'binding': asdict(registration.binding), 'contract_sha256': digest({
                'original': request.reference.model_dump(mode='json'),
                'documents': registration_documents(registration)})}
        marker, journal, resources = anchor / 'pool-registration.json', state / 'registration.json', state / 'resources'

        def verify() -> None:
            if any(private_state._private_read(path, limit=4 * 1024**2) != raw
                   for path, raw in retained.files.items()):
                raise ValueError()
            api.qualify(request)

        with private_state._locked_state(anchor):
            verify()
            if marker.exists() or marker.is_symlink():
                if _json(private_state._private_read(marker)) != identity:
                    raise ValueError()
                record = _json(private_state._private_read(journal, limit=4 * 1024**2))
                if (not isinstance(record, dict) or set(record) != {*identity, 'phase', 'proof', 'stage_sha256'}
                        or any(record[key] != value for key, value in identity.items())
                        or record['phase'] not in {'prepared', 'started', 'complete'}
                        or (record['phase'] == 'complete') != (record['proof'] is not None)
                        or (record['phase'] == 'complete') != (record['stage_sha256'] is not None)):
                    raise ValueError()
                if record['phase'] == 'prepared':
                    if resources.exists() or resources.is_symlink():
                        raise ValueError()
                elif not (resources / 'stage.json').is_file() or resources != resources.resolve():
                    raise ValueError()
                if record['phase'] == 'complete':
                    if _hash(resources / 'stage.json') != record['stage_sha256']:
                        raise ValueError()
                    validate_registration_proof(registration, resources, record['proof'])
            else:
                if state.exists() or state.is_symlink():
                    raise ValueError()
                record = {**identity, 'phase': 'prepared', 'proof': None, 'stage_sha256': None}
            base = {'operation_id': identity['operation_id'], 'pool_id': str(registration.spec.pool_id),
                'installation_id': retained.binding.installation_id, 'namespace': retained.binding.namespace,
                'admission_open': False, 'writer_migration_complete': False}
            if not execute:
                return {**base, 'status': 'development_pool_registration_preflight_qualified'}
            if not marker.exists():
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                private_state._atomic_json(journal, record)
            if record['phase'] == 'prepared':
                record['phase'] = 'started'
                private_state._atomic_json(journal, record)
            verify()
            stage_pool_registration(request=registration, api=api, state_dir=resources)
            proof = api.registration_report(resources)
            verify()
            if proof is None:
                if record['phase'] == 'complete':
                    raise ValueError()
                return {**base, 'status': 'pending_registration'}
            validate_registration_proof(registration, resources, proof)
            checksum = _hash(resources / 'stage.json')
            if record['phase'] == 'complete':
                if record['proof'] != proof or record['stage_sha256'] != checksum:
                    raise ValueError()
            else:
                record.update(phase='complete', proof=proof, stage_sha256=checksum)
                private_state._atomic_json(journal, record)
            return {**base, 'status': 'development_pool_registered_closed'}
    except Exception:
        raise ValueError('development pool registration unqualified; preserve retained evidence') from None
