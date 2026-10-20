"""Anchored closed-development schema successor, separate from installation.

The original runtime is immutable ancestry. A completed successor is consumed
only with its exact private reference and independently qualified live state.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_runtime_entry import DevelopmentDataRuntimeInputs
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_retained import (
    CompletedDevelopmentRuntime,
    RetainedRuntimeReference,
    load_completed_runtime,
)
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    _advance_fixed_resources,
    validate_runtime_transition,
)
from scripts.ops.nebius_development_schema_catalog import qualify_closed_development_catalog_report
from scripts.ops.nebius_development_schema_projection import (
    DevelopmentSchemaProjection,
    project_development_schema_successor,
)
from scripts.ops.nebius_development_schema_resources import (
    DevelopmentSchemaResources,
    schema_runtime_resources,
)
from scripts.ops.nebius_environment_runtime_install import qualify_runtime_defaulted
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _comparison_snapshot,
    _qualified_defaulted,
    _stage_fixed_documents,
    _validate_record,
)

from loom.nebius_development_schema_database import DevelopmentSchemaProbe, DevelopmentSchemaUpgrade
from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json

PHASES = ('stop', 'shared-probe', 'manager-probe', 'shared-backup', 'manager-backup',
    'shared-upgrade', 'manager-upgrade', 'configuration', 'replace', 'policy', 'catalog', 'start',
    'shared-inspect', 'manager-inspect', 'verify')
_LIMIT = 16 * 1024**2


class DevelopmentSchemaReference(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_id: UUID
    inputs_path: Path
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('inputs_path')
    @classmethod
    def canonical_path(cls, path: Path) -> Path:
        if not path.is_absolute() or path != path.resolve() or path == Path('/'):
            raise ValueError('development_schema_reference_path_invalid')
        return path

    @field_validator('operation_id')
    @classmethod
    def nonzero(cls, identity: UUID) -> UUID:
        if not identity.int:
            raise ValueError('development_schema_reference_identity_invalid')
        return identity


@dataclass(frozen=True, repr=False)
class CompletedDevelopmentSchemaSuccessor:
    reference: DevelopmentSchemaReference
    runtime: CompletedDevelopmentRuntime
    projection: DevelopmentSchemaProjection
    resources: dict[str, dict[str, Any]]
    files: dict[Path, bytes]
    history_sha256: str


class DevelopmentSchemaInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.development-schema-inputs.v1'] = 'loom.development-schema-inputs.v1'
    runtime: RetainedRuntimeReference
    data_runtime: DevelopmentDataRuntimeInputs
    manager_runtime: DevelopmentDataRuntimeInputs
    connection: NebiusKubernetesConnection


@dataclass(frozen=True, repr=False)
class RetainedDevelopmentSchema:
    reference: DevelopmentSchemaReference
    inputs: DevelopmentSchemaInputs
    runtime: CompletedDevelopmentRuntime
    projection: DevelopmentSchemaProjection
    plan: DevelopmentSchemaResources
    state_dir: Path
    anchor_dir: Path
    files: dict[Path, bytes]


def _read(path: Path) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise ValueError('development_schema_history_path_changed')
    return state._private_read(path, limit=_LIMIT)


def _unchanged(files: dict[Path, bytes]) -> None:
    if any(_read(path) != raw for path, raw in files.items()):
        raise ValueError('development_schema_history_changed')


def load_development_schema_upgrade(reference: DevelopmentSchemaReference, *,
        operator: bool = False) -> RetainedDevelopmentSchema:
    from scripts.ops.nebius_shared_pool_entry import _retained_runtime

    reference = DevelopmentSchemaReference.model_validate(reference.model_dump())
    raw = _read(reference.inputs_path)
    if hashlib.sha256(raw).hexdigest() != reference.inputs_sha256:
        raise ValueError('development_schema_inputs_changed')
    inputs = DevelopmentSchemaInputs.model_validate_json(raw)
    files = {reference.inputs_path: raw}
    request = _retained_runtime(inputs.runtime, files)
    runtime = load_completed_runtime(request)
    projection = project_development_schema_successor(runtime=runtime, operation_id=reference.operation_id,
        data_publication=inputs.data_runtime.selection(), manager_publication=inputs.manager_runtime.selection())
    plan = schema_runtime_resources(runtime=runtime, projection=projection)
    if inputs.connection.endpoint.rstrip('/') != request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/'):
        raise ValueError('development_schema_connection_changed')
    if operator:
        for path in (inputs.connection.ca_file, inputs.connection.credentials_file):
            if path in files:
                raise ValueError('development_schema_material_alias')
            files[path] = _read(path)
    manager = request.database.manager.retained.request.retained
    root = Path(manager.operation['state_dir']).parent / 'schema-upgrades' / str(reference.operation_id)
    anchor = Path(manager.operation['anchor_dir'])
    if root != root.resolve() or anchor != anchor.resolve() or not anchor.is_dir():
        raise ValueError('development_schema_state_path_changed')
    _unchanged(files)
    return RetainedDevelopmentSchema(reference, inputs, runtime, projection, plan, root, anchor, files)


def _identity(retained: RetainedDevelopmentSchema) -> dict[str, Any]:
    return dict(schema_version='loom.development-schema-operation.v1', reference=retained.reference.model_dump(mode='json'),
        history_sha256=retained.runtime.history_sha256, plan_sha256=retained.plan.input_digest,
        state_dir=str(retained.state_dir))


def _anchor(retained: RetainedDevelopmentSchema) -> Path:
    return retained.anchor_dir / ('schema-upgrade-' + str(retained.reference.operation_id) + '.json')


def _record(retained: RetainedDevelopmentSchema, *, _prechild_phase: str | None = None) -> dict[str, Any]:
    identity = _identity(retained)
    if _json(_read(_anchor(retained))) != identity:
        raise ValueError('development_schema_anchor_changed')
    record = _json(_read(retained.state_dir / 'operation.json'))
    if (set(record) != {*identity, 'phases'} or any(record[key] != value for key, value in identity.items())
            or set(record['phases']) != set(PHASES)):
        raise ValueError('development_schema_operation_changed')
    if _prechild_phase is not None:
        if _prechild_phase not in PHASES or record['phases'][_prechild_phase]['status'] != 'started':
            raise ValueError('development_schema_prechild_changed')
        pending = _child(retained, _prechild_phase)
        if (pending is None or pending.exists() or pending.is_symlink() or pending != pending.resolve()
                or (pending.parent.exists() and any(path.name != 'operation.lock' for path in pending.parent.iterdir()))):
            raise ValueError('development_schema_prechild_changed')
    unfinished = False
    for phase in PHASES:
        item = record['phases'][phase]
        if (set(item) != {'status', 'proof', 'child_sha256'} or item['status'] not in {'prepared', 'started', 'complete'}
                or (unfinished and item['status'] != 'prepared')
                or (item['status'] != 'complete' and (item['proof'] is not None or item['child_sha256'] is not None))):
            raise ValueError('development_schema_phase_order_changed')
        unfinished |= item['status'] != 'complete'
        path = _child(retained, phase)
        if path is not None:
            if item['status'] == 'prepared' and (path.parent.exists() or path.parent.is_symlink()):
                raise ValueError('development_schema_unstarted_child_changed')
            if item['status'] == 'started' and phase != _prechild_phase and not path.is_file():
                raise ValueError('development_schema_child_missing')
        if item['status'] == 'complete':
            _validate_child(retained, record, phase)
        elif item['status'] == 'started' and phase != _prechild_phase and path is not None:
            if phase in {'stop', 'replace', 'policy', 'start'}:
                _transition_resources(retained, phase, allow_pending=True)
            elif phase == 'catalog':
                child = _json(_read(path))
                expected = dict(request_sha256=digest(retained.projection.catalog_request.model_dump(mode='json')),
                    status='intent', proof=None)
                if child.get('status') == 'complete':
                    validate_phase_proof(retained, record, phase, child.get('proof'))
                    expected.update(status='complete', proof=child['proof'])
                if child != expected:
                    raise ValueError('development_schema_catalog_intent_changed')
            else:
                _staged_resources(retained, record, phase, allow_pending=True)
    return record


def _child(retained: RetainedDevelopmentSchema, phase: str) -> Path | None:
    if phase == 'verify':
        return None
    name = ('transition.json' if phase in {'stop', 'replace', 'policy', 'start'}
        else 'registry.json' if phase == 'catalog' else 'stage.json')
    return retained.state_dir / phase / name


def phase_documents(retained: RetainedDevelopmentSchema, record: dict[str, Any], phase: str) -> dict[str, dict[str, Any]]:
    from scripts.ops.nebius_development_schema_jobs import schema_job_documents

    if phase == 'configuration':
        return copy.deepcopy(retained.plan.fixed)
    mode, action = phase.split('-')
    return schema_job_documents(retained.runtime, retained.projection, phase,
        predecessor=record['phases'][mode + '-probe']['proof']['database'] if action in {'upgrade', 'inspect'} else None,
        checkpoint=record['phases'][mode + '-backup']['proof'] if action in {'upgrade', 'inspect'} else None)


def transition_documents(retained: RetainedDevelopmentSchema, phase: str
        ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    plan = retained.plan
    if phase == 'stop':
        return copy.deepcopy(plan.originals), {key: _snapshot(row) for key, row in plan.stopped.items()}
    if phase == 'policy':
        return {key: copy.deepcopy(plan.source[key]) for key in plan.updates}, {key: _snapshot(row) for key, row in plan.updates.items()}
    previous = 'stop' if phase == 'replace' else 'replace'
    originals = _transition_resources(retained, previous)
    targets = {key: _snapshot(row) for key, row in plan.targets.items()}
    if phase == 'replace':
        for row in targets.values():
            row['spec']['replicas' if row['kind'] == 'Deployment' else 'suspend'] = 0 if row['kind'] == 'Deployment' else True
    elif phase != 'start':
        raise ValueError('development_schema_transition_phase_invalid')
    return originals, targets


def _defaulted(desired: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    if desired['kind'] in {'CronJob', 'Job'}:
        return _qualified_defaulted(desired, actual)
    return qualify_runtime_defaulted(desired, actual)


def _transition_resources(retained: RetainedDevelopmentSchema, phase: str, *,
        allow_pending: bool = False) -> dict[str, dict[str, Any]]:
    originals, targets = transition_documents(retained, phase)
    path = _child(retained, phase)
    assert path is not None
    if not originals:
        if _json(_read(path)) != {'status': 'empty', 'phase': phase, 'input_digest': retained.plan.input_digest}:
            raise ValueError('development_schema_empty_transition_changed')
        return {}
    child = _json(_read(path))
    validate_runtime_transition(child, originals=originals, targets=targets, input_digest=retained.plan.input_digest,
        phase=phase, qualify_defaulted=_defaulted)
    result = {}
    for key, original in originals.items():
        item = child['resources'][key]
        if item['status'] != 'applied':
            if allow_pending:
                continue
            raise ValueError('development_schema_transition_incomplete')
        row = copy.deepcopy(item['expected'])
        row['metadata']['uid'] = _uid(original)
        result[key] = row
    return result


def _staged_resources(retained: RetainedDevelopmentSchema, record: dict[str, Any], phase: str, *,
        allow_pending: bool = False) -> dict[str, dict[str, Any]]:
    documents = phase_documents(retained, record, phase)
    path = _child(retained, phase)
    assert path is not None
    child = _json(_read(path))
    binding = retained.runtime.request.database.manager.retained.request.retained.binding
    _validate_record(child, dict(schema='loom.nebius-management-stage.v1', binding=asdict(binding),
        revision=digest(documents), phase='schema-' + phase), documents)
    result = {}
    for key, item in child['resources'].items():
        if item['status'] != 'created':
            if allow_pending:
                continue
            raise ValueError('development_schema_stage_incomplete')
        row = copy.deepcopy(item['observed'])
        row['metadata']['uid'] = item['uid']
        _uid(row)
        if (_key(row) != key or _comparison_snapshot(row) != item['expected']
                or _defaulted(item['desired'], row) != item['expected']):
            raise ValueError('development_schema_stage_snapshot_changed')
        result[key] = row
    return result


def validate_phase_proof(retained: RetainedDevelopmentSchema, record: dict[str, Any], phase: str, proof: Any) -> None:
    from scripts.ops.nebius_development_schema_jobs import schema_database_selection

    if phase in {'stop', 'replace', 'policy', 'start', 'configuration'}:
        if proof is not None:
            raise ValueError('development_schema_phase_proof_changed')
        return
    if phase == 'catalog':
        if qualify_closed_development_catalog_report(retained.projection.catalog_request, proof, read_only=True) != 'complete':
            raise ValueError('development_schema_catalog_incomplete')
        return
    if phase == 'verify':
        if proof != {'status': 'qualified', 'plan_sha256': retained.plan.input_digest, 'admission_open': False}:
            raise ValueError('development_schema_final_proof_changed')
        return
    mode, action = phase.split('-')
    modes: dict[str, Literal['shared', 'manager']] = {'shared': 'shared', 'manager': 'manager'}
    selection = schema_database_selection(retained.runtime, retained.projection, modes[mode])
    staged = _staged_resources(retained, record, phase)
    job, = (row for row in staged.values() if row['kind'] == 'Job')
    if not isinstance(proof, dict) or proof.get('job_uid') != _uid(job):
        raise ValueError('development_schema_job_proof_changed')
    if action == 'backup':
        if (set(proof) != {'job_uid', 'key', 'sha256', 'bytes'} or type(proof['bytes']) is not int or proof['bytes'] <= 0
                or not isinstance(proof['key'], str) or not proof['key'].startswith(selection.namespace + '/')
                or not isinstance(proof['sha256'], str) or len(proof['sha256']) != 64
                or any(char not in '0123456789abcdef' for char in proof['sha256'])):
            raise ValueError('development_schema_checkpoint_changed')
        return
    if set(proof) != {'job_uid', 'pod_uid', 'database'} or not UUID(proof['pod_uid']).int:
        raise ValueError('development_schema_job_execution_changed')
    if action == 'probe':
        observed = DevelopmentSchemaProbe.model_validate(proof['database'])
        selection_digest = hashlib.sha256(json.dumps(selection.model_dump(mode='json'), sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()
        if observed.selection_sha256 != selection_digest or observed.revision != selection.predecessor_revision:
            raise ValueError('development_schema_probe_changed')
    else:
        documents = phase_documents(retained, record, phase)
        config, = (row for row in documents.values() if row['kind'] == 'ConfigMap')
        request = DevelopmentSchemaUpgrade.model_validate_json(next(iter(config['data'].values())))
        result = proof['database']
        expected_digest = hashlib.sha256(json.dumps(request.model_dump(mode='json'), sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()
        successor = DevelopmentSchemaProbe.model_validate(result['successor'])
        expected = dict(schema_version='loom.development-schema-upgrade.v1', status='complete',
            operation_id=str(selection.operation_id), mode=selection.mode, request_sha256=expected_digest,
            revision=selection.candidate_revision, checkpoint_sha256=request.checkpoint_sha256,
            successor=successor.model_dump(mode='json'))
        if (result != expected or successor.selection_sha256 != request.predecessor.selection_sha256
                or successor.revision != selection.candidate_revision
                or any(getattr(successor, field) != getattr(request.predecessor, field)
                    for field in ('database_oid', 'system_identifier', 'authority_sha256'))):
            raise ValueError('development_schema_migration_proof_changed')
        if action == 'inspect' and result != record['phases'][mode + '-upgrade']['proof']['database']:
            raise ValueError('development_schema_inspection_proof_changed')


def _validate_child(retained: RetainedDevelopmentSchema, record: dict[str, Any], phase: str) -> None:
    item = record['phases'][phase]
    path = _child(retained, phase)
    if (None if path is None else hashlib.sha256(_read(path)).hexdigest()) != item['child_sha256']:
        raise ValueError('development_schema_child_changed')
    if phase in {'stop', 'replace', 'policy', 'start'}:
        _transition_resources(retained, phase)
    elif phase == 'catalog':
        assert path is not None
        if _json(_read(path)) != {'request_sha256': digest(retained.projection.catalog_request.model_dump(mode='json')),
                'status': 'complete', 'proof': item['proof']}:
            raise ValueError('development_schema_catalog_history_changed')
    elif phase != 'verify':
        _staged_resources(retained, record, phase)
    validate_phase_proof(retained, record, phase, item['proof'])


class DevelopmentSchemaAPI(Protocol):
    def qualify(self, retained: RetainedDevelopmentSchema, record: dict[str, Any] | None) -> None: ...
    def resources(self, phase: str, documents: dict[str, dict[str, Any]]) -> ManagementStageAPI: ...
    def workloads(self, phase: str, originals: dict[str, dict[str, Any]],
        targets: dict[str, dict[str, Any]]) -> DevelopmentRuntimeWorkloadAPI: ...
    def report(self, phase: str, record: dict[str, Any]) -> dict[str, Any] | None: ...
    def catalog_observe(self) -> dict[str, Any]: ...
    def catalog_commit(self) -> None: ...
    def verify(self, record: dict[str, Any]) -> bool: ...


def run_development_schema_upgrade(*, retained: RetainedDevelopmentSchema, api: DevelopmentSchemaAPI,
        action: Literal['prepare', 'apply', 'recover', 'status']) -> dict[str, Any]:
    """One stopped successor, with durable intent preceding every remote effect."""
    frozen = copy.deepcopy(retained)
    identity = _identity(frozen)
    result = dict(status='pending', operation_id=str(frozen.reference.operation_id), admission_open=False)
    with state._locked_state(frozen.anchor_dir / 'shared-pool-handoff-lock'):
        _unchanged(frozen.files)
        marker = _anchor(frozen)
        record = _record(frozen) if marker.exists() else None
        if record is None:
            if action == 'recover':
                raise ValueError('development_schema_recovery_missing')
            if frozen.state_dir.exists():
                raise ValueError('development_schema_anchor_missing')
        if record is None or any(row['status'] != 'complete' for row in record['phases'].values()):
            manager = frozen.runtime.request.database.manager.retained.request.retained
            forbidden = (Path(manager.operation['state_dir']).parent / 'shared-pool-handoff',
                frozen.anchor_dir / 'shared-pool-handoff.json', frozen.anchor_dir / 'shared-pool-recovery.json',
                *frozen.anchor_dir.glob('*-pool-preactivation-dispatch.json'))
            if any(path.exists() or path.is_symlink() for path in forbidden):
                raise ValueError('development_schema_requires_unactivated_runtime')
        api.qualify(frozen, record)
        _unchanged(frozen.files)
        if action in {'prepare', 'status'}:
            return dict(result, status='prepared' if action == 'prepare' else 'complete' if record is not None
                and all(row['status'] == 'complete' for row in record['phases'].values()) else 'pending')
        if record is None:
            state._private_directory(frozen.state_dir.parent)
            state._atomic_json(marker, identity)
            state._private_directory(frozen.state_dir)
            record = dict(identity, phases={phase: dict(status='prepared', proof=None, child_sha256=None) for phase in PHASES})
            state._atomic_json(frozen.state_dir / 'operation.json', record)
        for phase in PHASES:
            if record['phases'][phase]['status'] == 'complete':
                continue
            if retained != frozen:
                raise ValueError('development_schema_selection_changed')
            _unchanged(frozen.files)
            api.qualify(frozen, record)
            item = record['phases'][phase]
            if item['status'] == 'prepared':
                item['status'] = 'started'
                state._atomic_json(frozen.state_dir / 'operation.json', record)
            proof = None
            ready = False
            if phase in {'stop', 'replace', 'policy', 'start'}:
                originals, targets = transition_documents(frozen, phase)
                if not originals:
                    path = _child(frozen, phase)
                    assert path is not None
                    state._private_directory(path.parent)
                    state._atomic_json(path, dict(status='empty', phase=phase, input_digest=frozen.plan.input_digest))
                    ready = True
                else:
                    ready = _advance_fixed_resources(originals=originals, targets=targets, input_digest=frozen.plan.input_digest,
                        phase=phase, api=api.workloads(phase, originals, targets), state_dir=frozen.state_dir / phase,
                        phases=frozenset({'stop', 'replace', 'policy', 'start'}),
                        kinds=frozenset({'Deployment', 'CronJob', 'ConfigMap', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}),
                        qualify_defaulted=_defaulted)
            elif phase == 'catalog':
                path = _child(frozen, phase)
                assert path is not None
                wanted = dict(request_sha256=digest(frozen.projection.catalog_request.model_dump(mode='json')),
                    status='intent', proof=None)
                if not path.exists():
                    report = api.catalog_observe()
                    if qualify_closed_development_catalog_report(frozen.projection.catalog_request, report, read_only=True) != 'ready':
                        raise ValueError('development_schema_catalog_predecessor_changed')
                    state._private_directory(path.parent)
                    state._atomic_json(path, wanted)
                    try:
                        api.catalog_commit()
                    except Exception:
                        pass  # Every unknown dispatch is reconciled, never retried.
                elif _json(_read(path)) not in (wanted, dict(wanted, status='complete', proof=api.catalog_observe())):
                    raise ValueError('development_schema_catalog_intent_changed')
                proof = api.catalog_observe()
                validate_phase_proof(frozen, record, phase, proof)
                state._atomic_json(path, dict(wanted, status='complete', proof=proof))
                ready = True
            elif phase == 'verify':
                ready = api.verify(record)
                proof = dict(status='qualified', plan_sha256=frozen.plan.input_digest, admission_open=False) if ready else None
            else:
                documents = phase_documents(frozen, record, phase)
                binding = frozen.runtime.request.database.manager.retained.request.retained.binding
                _stage_fixed_documents(documents=documents, revision=digest(documents), phase='schema-' + phase,
                    binding=binding, api=api.resources(phase, documents), state_dir=frozen.state_dir / phase,
                    default_document=lambda transport, desired: _defaulted(desired, transport.default_resource(desired)))
                proof = api.report(phase, record) if phase != 'configuration' else None
                ready = phase == 'configuration' or proof is not None
            _unchanged(frozen.files)
            api.qualify(frozen, record)
            if not ready:
                return dict(result, phase=phase)
            validate_phase_proof(frozen, record, phase, proof)
            path = _child(frozen, phase)
            item.update(status='complete', proof=proof,
                child_sha256=None if path is None else hashlib.sha256(_read(path)).hexdigest())
            state._atomic_json(frozen.state_dir / 'operation.json', record)
            _record(frozen)
        return dict(result, status='complete')


def load_completed_development_schema_successor(*, runtime: DevelopmentRuntimeInstallRequest,
        reference: DevelopmentSchemaReference) -> CompletedDevelopmentSchemaSuccessor:
    """Require the original history and all anchored upgrade phase receipts."""
    retained = load_development_schema_upgrade(reference)
    if retained.runtime.request != runtime:
        raise ValueError('development_schema_original_runtime_changed')
    record = _record(retained)
    if any(row['status'] != 'complete' for row in record['phases'].values()):
        raise ValueError('development_schema_successor_incomplete')
    resources = copy.deepcopy(retained.plan.source)
    resources.update(_staged_resources(retained, record, 'configuration'))
    resources.update(_transition_resources(retained, 'policy'))
    resources.update(_transition_resources(retained, 'start'))
    if len({_uid(row) for row in resources.values()}) != len(resources):
        raise ValueError('development_schema_successor_identity_collision')
    files = dict(retained.files)
    for path in (_anchor(retained), retained.state_dir / 'operation.json',
            *(_child(retained, phase) for phase in PHASES)):
        if path is not None:
            if path in files:
                raise ValueError('development_schema_history_alias')
            files[path] = _read(path)
    _unchanged(files)
    return CompletedDevelopmentSchemaSuccessor(reference, retained.runtime, retained.projection, resources, files,
        digest({str(path): hashlib.sha256(raw).hexdigest() for path, raw in files.items()}))
