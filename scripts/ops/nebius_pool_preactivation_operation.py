"""Immutable preactivation ancestry, separate from completed installation history.

Preparation retains local inputs only. Neither a reference nor a completed
historical receipt proves current authority: the protected connected parent must
qualify publication, the exact CLOSED endpoint and stopped gateway under the
common handoff lock before every effect. No staging operation is exposed here.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_retained import (
    RetainedRuntimeReference,
    load_completed_runtime,
)
from scripts.ops.nebius_development_runtime_transition import DevelopmentRuntimeWorkloadAPI
from scripts.ops.nebius_management_stage import ManagementStageAPI
from scripts.ops.nebius_pool_participant_catalog import (
    AppliedCatalogResources,
    ParticipantCatalogRequest,
    read_applied_catalog,
    refresh_participant_manager,
    stage_participant_catalog,
)
from scripts.ops.nebius_pool_participant_release import (
    PoolCatalogRefresh,
    prepare_staging_preactivation_refresh,
)
from scripts.ops.nebius_pool_preactivation_database import qualify_preactivation_report
from scripts.ops.nebius_pool_preactivation_registry import (
    PreactivationRegistryAPI,
    advance_preactivation_registry,
    validate_preactivation_registry,
)
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
from scripts.ops.nebius_shared_pool_handoff import StagingPoolSource
from scripts.ops.nebius_shared_pool_operation import _directory

from loom_service.environment_management.candidates import _json
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh

_LIMIT = 16 * 1024**2
_CHILDREN = {'catalog': 'catalog/stage.json', 'manager': 'manager/transition.json', 'registry': 'registry/registry.json'}


class PreactivationReference(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_id: UUID
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('operation_id')
    @classmethod
    def nonzero(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError('preactivation operation must be nonzero')
        return value


class PreactivationInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.pool-preactivation-inputs.v1'] = 'loom.pool-preactivation-inputs.v1'
    runtime: RetainedRuntimeReference
    predecessor: PreactivationReference | None
    source: StagingPoolSource
    refresh: PoolParticipantRefresh
    manager: dict[str, Any]
    catalog: dict[str, Any]
    history: dict[Path, str]


@dataclass(frozen=True, repr=False)
class RetainedPreactivationCatalog:
    reference: PreactivationReference
    inputs: PreactivationInputs
    runtime: DevelopmentRuntimeInstallRequest
    state_dir: Path
    anchor_dir: Path
    files: dict[Path, bytes]

    @property
    def registration(self) -> PoolRegistrationRequest:
        original = self.runtime.database.manager.retained.request.registration
        return replace(original, spec=self.inputs.refresh.after)

    @property
    def catalog_request(self) -> ParticipantCatalogRequest:
        return ParticipantCatalogRequest(PoolCatalogRefresh(self.inputs.refresh, self.registration.binding),
            self.inputs.manager, self.inputs.catalog)


@dataclass(frozen=True, repr=False)
class CompletedPreactivationCatalog:
    retained: RetainedPreactivationCatalog
    resources: AppliedCatalogResources
    files: dict[Path, bytes]


def _read(path: Path) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise ValueError
    return private_state._private_read(path, limit=_LIMIT)


def _unchanged(files: dict[Path, bytes]) -> None:
    if any(_read(path) != raw for path, raw in files.items()):
        raise ValueError


def _paths(runtime: DevelopmentRuntimeInstallRequest, operation_id: UUID) -> tuple[Path, Path, Path]:
    manager = runtime.database.manager.retained.request.retained
    root = Path(manager.operation['state_dir']).parent
    anchor = Path(manager.operation['anchor_dir'])
    _directory(root)
    _directory(anchor)
    return root / 'pool-preactivation' / str(operation_id), anchor, root / 'shared-pool-handoff'


def _anchor(reference: PreactivationReference, state: Path) -> dict[str, Any]:
    return {'schema': 'loom.pool-preactivation-anchor.v1', 'reference': reference.model_dump(mode='json'),
        'state_dir': str(state)}


def _basis(*, runtime: RetainedRuntimeReference, predecessor: PreactivationReference | None,
        repo_root: Path, seen: frozenset[UUID]) -> tuple[DevelopmentRuntimeInstallRequest,
            PoolRegistrationRequest, dict[str, Any], dict[str, Any], dict[Path, bytes]]:
    from scripts.ops.nebius_shared_pool_entry import _retained_runtime

    files: dict[Path, bytes] = {}
    request = _retained_runtime(runtime, files)
    registration = request.database.manager.retained.request.registration
    if predecessor is not None:
        prior = _load(runtime=runtime, reference=predecessor, repo_root=repo_root, seen=seen)
        completed = _completed(prior)
        files = dict(completed.files)
        return request, prior.registration, completed.resources.manager, completed.resources.catalog, files
    original = load_completed_runtime(request)
    namespace = registration.binding.namespace
    manager = original.resources['Deployment:' + namespace + ':loom-service']
    catalog = original.resources['ConfigMap:' + namespace + ':loom-pool-profiles-' + registration.spec.operation_id.hex]
    return request, registration, copy.deepcopy(manager), copy.deepcopy(catalog), files


def _inputs(*, runtime: RetainedRuntimeReference, predecessor: PreactivationReference | None,
        source: StagingPoolSource, operation_id: UUID, repo_root: Path, seen: frozenset[UUID]
        ) -> tuple[PreactivationInputs, DevelopmentRuntimeInstallRequest, dict[Path, bytes]]:
    if operation_id in seen or len(seen) >= 32 or not operation_id.int:
        raise ValueError
    seen = seen | {operation_id}
    runtime = RetainedRuntimeReference.model_validate(runtime.model_dump())
    source = StagingPoolSource.model_validate(source.model_dump())
    if (source.publication.source_sha != source.candidate.get('candidate_sha')
            or source.publication.run_id != source.candidate.get('run_id')):
        raise ValueError
    request, registration, manager, catalog, files = _basis(runtime=runtime, predecessor=predecessor,
        repo_root=repo_root, seen=seen)
    if operation_id == request.database.manager.retained.request.registration.spec.operation_id:
        raise ValueError
    refresh = prepare_staging_preactivation_refresh(registration=registration,
        observation=source.observation, candidate=source.candidate, operation_id=operation_id, repo_root=repo_root)
    result = PreactivationInputs(runtime=runtime, predecessor=predecessor, source=source, refresh=refresh,
        manager=manager, catalog=catalog, history={path: hashlib.sha256(raw).hexdigest() for path, raw in files.items()})
    _unchanged(files)
    return result, request, files


def _load(*, runtime: RetainedRuntimeReference, reference: PreactivationReference,
        repo_root: Path, seen: frozenset[UUID]) -> RetainedPreactivationCatalog:
    from scripts.ops.nebius_shared_pool_entry import _retained_runtime

    reference = PreactivationReference.model_validate(reference.model_dump())
    if reference.operation_id in seen or len(seen) >= 32:
        raise ValueError
    original_files: dict[Path, bytes] = {}
    original = _retained_runtime(runtime, original_files)
    state, anchor, _ = _paths(original, reference.operation_id)
    marker = anchor / (str(reference.operation_id) + '-pool-preactivation.json')
    marker_raw, raw = _read(marker), _read(state / 'inputs.json')
    if _json(marker_raw) != _anchor(reference, state) or hashlib.sha256(raw).hexdigest() != reference.inputs_sha256:
        raise ValueError
    supplied = PreactivationInputs.model_validate(_json(raw))
    if supplied.runtime != runtime:
        raise ValueError
    expected, request, files = _inputs(runtime=runtime, predecessor=supplied.predecessor,
        source=supplied.source, operation_id=reference.operation_id, repo_root=repo_root, seen=seen)
    if supplied != expected or any(path in files for path in (marker, state / 'inputs.json')):
        raise ValueError
    files.update({marker: marker_raw, state / 'inputs.json': raw})
    _unchanged(files)
    return RetainedPreactivationCatalog(reference, supplied, request, state, anchor, files)


def load_preactivation_catalog(*, runtime: RetainedRuntimeReference, reference: PreactivationReference,
        repo_root: Path) -> RetainedPreactivationCatalog:
    try:
        return _load(runtime=runtime, reference=reference, repo_root=repo_root, seen=frozenset())
    except Exception:
        raise ValueError('preactivation ancestry unqualified; preserve evidence') from None


def _completed(retained: RetainedPreactivationCatalog) -> CompletedPreactivationCatalog:
    marker = retained.anchor_dir / (str(retained.reference.operation_id) + '-pool-preactivation-dispatch.json')
    marker_raw = _read(marker)
    identity = {'schema': 'loom.pool-preactivation-operation.v1', 'reference': retained.reference.model_dump(mode='json')}
    if _json(marker_raw) != identity:
        raise ValueError
    resources = read_applied_catalog(request=retained.catalog_request, state_dir=retained.state_dir)
    state = retained.state_dir
    validate_preactivation_registry(request=retained.inputs.refresh, state_dir=state / 'registry', complete=True)
    children = {**resources.files, state / 'registry/registry.json': _read(state / 'registry/registry.json')}
    raw = _read(state / 'operation.json')
    expected = {'schema': 'loom.pool-preactivation-operation.v1',
        'reference': retained.reference.model_dump(mode='json'), 'status': 'complete',
        'phases': {phase: 'complete' for phase in ('catalog', 'manager', 'registry')},
        'files': {str(path.relative_to(state)): hashlib.sha256(value).hexdigest() for path, value in children.items()}}
    if _json(raw) != expected or retained.files.keys() & children.keys():
        raise ValueError
    files = {**retained.files, **children, state / 'operation.json': raw, marker: marker_raw}
    _unchanged(files)
    return CompletedPreactivationCatalog(retained, resources, files)


def load_completed_preactivation(*, runtime: RetainedRuntimeReference, reference: PreactivationReference,
        repo_root: Path) -> CompletedPreactivationCatalog:
    try:
        return _completed(load_preactivation_catalog(runtime=runtime, reference=reference, repo_root=repo_root))
    except Exception:
        raise ValueError('completed preactivation unqualified; preserve evidence') from None


def retain_preactivation_catalog(*, runtime: RetainedRuntimeReference, source: dict[str, Any],
        operation_id: UUID, repo_root: Path, predecessor: PreactivationReference | None = None) -> RetainedPreactivationCatalog:
    """Local source-qualified preparation, never a cluster/SQL or guard operation."""
    try:
        inputs, request, files = _inputs(runtime=runtime, predecessor=predecessor,
            source=StagingPoolSource.model_validate(copy.deepcopy(source)), operation_id=operation_id,
            repo_root=repo_root, seen=frozenset())
        raw = json.dumps(inputs.model_dump(mode='json'), sort_keys=True).encode()
        if len(raw) > _LIMIT:
            raise ValueError
        reference = PreactivationReference(operation_id=operation_id, inputs_sha256=hashlib.sha256(raw).hexdigest())
        state, anchor, shared_state = _paths(request, operation_id)
        marker = anchor / (str(operation_id) + '-pool-preactivation.json')
        with private_state._locked_state(anchor / 'shared-pool-handoff-lock'):
            for path in (shared_state, anchor / 'shared-pool-handoff.json', anchor / 'shared-pool-recovery.json'):
                if path.exists() or path.is_symlink():
                    raise ValueError
            _unchanged(files)
            if marker.exists() or marker.is_symlink():
                return load_preactivation_catalog(runtime=runtime, reference=reference, repo_root=repo_root)
            private_state._private_directory(state.parent)
            private_state._private_directory(state)
            if any(item.name != 'inputs.json' for item in state.iterdir()):
                raise ValueError
            path = state / 'inputs.json'
            if path.exists() or path.is_symlink():
                if _read(path) != raw:
                    raise ValueError
            else:
                private_state._atomic_json(path, inputs.model_dump(mode='json'))
            _unchanged(files)
            private_state._atomic_json(marker, _anchor(reference, state))
            return load_preactivation_catalog(runtime=runtime, reference=reference, repo_root=repo_root)
    except Exception:
        raise ValueError('preactivation preparation unqualified; preserve evidence') from None


class PreactivationOperationAPI(Protocol):
    catalog: ManagementStageAPI
    manager: DevelopmentRuntimeWorkloadAPI
    registry: PreactivationRegistryAPI

    def qualify(self, retained: RetainedPreactivationCatalog, phase: str) -> None:
        """Fresh publication, CLOSED registry, stopped gateway and fixed live scope."""
        ...

    def qualify_manager(self, retained: RetainedPreactivationCatalog, resources: AppliedCatalogResources) -> None:
        """Require real manager process settings and the selected catalog hash."""
        ...


class _ScopedCatalog:
    def __init__(self, api: ManagementStageAPI, check: Callable[[], None]):
        self.api, self.check = api, check

    def verify_identity(self, binding: Any) -> None:
        self.check()
        self.api.verify_identity(binding)

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        self.check()
        return self.api.get_resource(document)

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        self.check()
        result = self.api.default_resource(document)
        self.check()
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        self.check()
        try:
            self.api.create_resource(document)
        finally:
            self.check()

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('database storage is outside catalog scope')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('database storage is outside catalog scope')


class _ScopedManager:
    def __init__(self, api: DevelopmentRuntimeWorkloadAPI, check: Callable[[], None]):
        self.api, self.check = api, check

    def qualify(self) -> None:
        self.check()
        self.api.qualify()
        self.check()

    def read_workload(self, key: str) -> dict[str, Any]:
        self.check()
        return self.api.read_workload(key)

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        self.check()
        result = self.api.preview_workload(key, before, desired)
        self.check()
        return result

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        self.check()
        try:
            return self.api.patch_workload(key, before, desired)
        finally:
            self.check()

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        self.check()
        result = self.api.workload_ready(key, expected)
        self.check()
        return result


def run_preactivation_catalog(*, retained: RetainedPreactivationCatalog, api: PreactivationOperationAPI,
        repo_root: Path) -> bool:
    """One closed-only operation; False means known manager readiness pending.

    This is an internal sequencer, not an operator entrypoint. The connected
    adapter must qualify live sources, exact journal-permitted resource states,
    never-used CLOSED authority and an inactive gateway. No staging mutation or
    execution guard exists in its interface. Missing dispatched evidence is not
    reconstructed, even if the endpoint happens to look like its predecessor.
    """
    try:
        frozen = load_preactivation_catalog(runtime=retained.inputs.runtime, reference=retained.reference, repo_root=repo_root)
        if frozen != retained:
            raise ValueError
        state, anchor, shared = _paths(frozen.runtime, frozen.reference.operation_id)
        path = state / 'operation.json'
        marker = anchor / (str(frozen.reference.operation_id) + '-pool-preactivation-dispatch.json')
        identity = {'schema': 'loom.pool-preactivation-operation.v1', 'reference': frozen.reference.model_dump(mode='json')}
        record: dict[str, Any] = {**identity, 'status': 'pending',
            'phases': {phase: 'prepared' for phase in _CHILDREN}, 'files': {}}
        qualification_failed = False

        def roots() -> None:
            if retained != frozen:
                raise ValueError
            _unchanged(frozen.files)
            for item in (shared, anchor / 'shared-pool-handoff.json', anchor / 'shared-pool-recovery.json'):
                if item.exists() or item.is_symlink():
                    raise ValueError

        def local() -> None:
            if qualification_failed:
                raise ValueError
            roots()
            if _json(_read(marker)) != identity or _json(_read(path)) != record:
                raise ValueError
            for suffix, checksum in record['files'].items():
                if hashlib.sha256(_read(state / suffix)).hexdigest() != checksum:
                    raise ValueError

        def scope(phase: str) -> None:
            nonlocal qualification_failed
            local()
            # A child legitimately advances its journal between callbacks, but
            # a remote qualification must not change the evidence authorizing
            # the next effect. Pin active (not just completed) journals across
            # that boundary. Latch failure so a child's uncertain-write catch
            # cannot read back and reconstruct the damaged journal afterward.
            active = {state / suffix: _read(state / suffix)
                for name, suffix in _CHILDREN.items()
                if record['phases'][name] == 'intent'
                and ((state / suffix).exists() or (state / suffix).is_symlink())}
            try:
                api.qualify(copy.deepcopy(frozen), phase)
                _unchanged(active)
                local()
            except Exception:
                qualification_failed = True
                raise

        def manager_ready() -> None:
            local()
            resources = read_applied_catalog(request=frozen.catalog_request, state_dir=state)
            api.qualify_manager(copy.deepcopy(frozen), resources)
            _unchanged(resources.files)
            local()

        with private_state._locked_state(anchor / 'shared-pool-handoff-lock'):
            roots()
            if marker.exists() or marker.is_symlink():
                if _json(_read(marker)) != identity:
                    raise ValueError
                record = _json(_read(path))
            else:
                # An interrupted local preparation can precede its dispatch
                # marker only with the exact initial record and no children.
                if any((state / phase).exists() or (state / phase).is_symlink() for phase in _CHILDREN):
                    raise ValueError
                if path.exists() or path.is_symlink():
                    if _json(_read(path)) != record:
                        raise ValueError
                else:
                    private_state._atomic_json(path, record)
                private_state._atomic_json(marker, identity)
            if (set(record) != {*identity, 'status', 'phases', 'files'}
                    or any(record[key] != value for key, value in identity.items())
                    or record['status'] not in {'pending', 'complete'} or set(record['phases']) != set(_CHILDREN)):
                raise ValueError
            expected_files, pending = set(), False
            for phase, suffix in _CHILDREN.items():
                status = record['phases'][phase]
                if status == 'complete' and not pending:
                    expected_files.add(suffix)
                elif status == 'prepared':
                    pending = True
                    if (state / phase).exists() or (state / phase).is_symlink():
                        raise ValueError
                elif status == 'intent' and not pending:
                    pending = True
                    _read(state / suffix)  # A lost dispatched child cannot be recreated.
                else:
                    raise ValueError
            if set(record['files']) != expected_files or (record['status'] == 'complete' and pending):
                raise ValueError
            scope('preflight')
            for phase, suffix in _CHILDREN.items():
                if record['phases'][phase] == 'complete':
                    continue
                scope(phase)
                if record['phases'][phase] == 'prepared':
                    record['phases'][phase] = 'intent'
                    private_state._atomic_json(path, record)
                if phase == 'catalog':
                    stage_participant_catalog(request=frozen.catalog_request,
                        api=_ScopedCatalog(api.catalog, lambda: scope('catalog')), state_dir=state)
                elif phase == 'manager':
                    if not refresh_participant_manager(request=frozen.catalog_request,
                            api=_ScopedManager(api.manager, lambda: scope('manager')), state_dir=state):
                        scope(phase)
                        return False
                    manager_ready()
                else:
                    def registry_scope() -> None:
                        scope('registry')
                        manager_ready()

                    advance_preactivation_registry(request=frozen.inputs.refresh, api=api.registry,
                        state_dir=state / 'registry', qualify=registry_scope)
                scope(phase)
                record['files'][suffix] = hashlib.sha256(_read(state / suffix)).hexdigest()
                record['phases'][phase] = 'complete'
                private_state._atomic_json(path, record)
            scope('terminal')
            manager_ready()
            report = api.registry.observe(frozen.inputs.refresh.model_copy(deep=True))
            if qualify_preactivation_report(frozen.inputs.refresh, report, read_only=True) != 'complete':
                raise ValueError
            scope('terminal')
            if record['status'] != 'complete':
                record['status'] = 'complete'
                private_state._atomic_json(path, record)
            _completed(frozen)
            local()
            return True
    except Exception:
        raise ValueError('preactivation incomplete; preserve original operation evidence') from None
