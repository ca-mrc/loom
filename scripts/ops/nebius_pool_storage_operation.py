"""Retained, closed-only storage correction; no operator entrypoint or cloud write.

The connected adapter supplies current provider, namespace, database, gateway,
drain and aggregate-publication proof. Existing create/CAS/SQL journals own the
effects. This parent preserves the completed installation and serializes with
activation using the same handoff lock.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_closed_registry import ClosedRegistryAPI, RegistryNotDispatchedError
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_retained import (
    RetainedRuntimeReference,
    load_completed_runtime,
)
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    advance_runtime_workloads,
    validate_runtime_transition,
)
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementNotDispatchedError
from scripts.ops.nebius_pool_preactivation_operation import (
    PreactivationReference,
    _read,
    _ScopedCatalog,
    _ScopedManager,
    _unchanged,
)
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
from scripts.ops.nebius_pool_storage_binding import (
    ClosedPoolStorageRefresh,
    StorageCollectorPlan,
    prepare_storage_collector,
)
from scripts.ops.nebius_pool_storage_registry import (
    advance_storage_registry,
    validate_storage_registry,
)
from scripts.ops.nebius_shared_pool_operation import _directory

from loom.nebius_platform_render import EXECUTION_DISK_STORAGE_QUOTAS, digest
from loom_service.environment_management.candidates import _json

_CHILDREN = {'pause': 'pause/transition.json', 'configuration': 'configuration/stage.json',
    'reconfigure': 'reconfigure/transition.json', 'registry': 'registry/registry.json', 'resume': 'resume/transition.json'}
_TRANSITIONS = {'pause': 'stop', 'reconfigure': 'control', 'resume': 'start'}


class StorageReference(PreactivationReference):
    """Separate type and anchor from the application catalog ancestry."""


class StorageInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.closed-pool-storage-inputs.v1'] = 'loom.closed-pool-storage-inputs.v1'
    runtime: RetainedRuntimeReference
    refresh: ClosedPoolStorageRefresh
    history: dict[Path, str]


@dataclass(frozen=True, repr=False)
class RetainedClosedStorage:
    reference: StorageReference
    inputs: StorageInputs
    runtime: DevelopmentRuntimeInstallRequest
    collector: StorageCollectorPlan
    state_dir: Path
    anchor_dir: Path
    files: dict[Path, bytes]

    @property
    def registration(self) -> PoolRegistrationRequest:
        return replace(self.runtime.database.manager.retained.request.registration, spec=self.inputs.refresh.after)


@dataclass(frozen=True, repr=False)
class AppliedStorageResources:
    configuration: dict[str, Any]
    collector: dict[str, Any]
    files: dict[Path, bytes]


@dataclass(frozen=True, repr=False)
class CompletedClosedStorage:
    retained: RetainedClosedStorage
    resources: AppliedStorageResources
    files: dict[Path, bytes]


def _paths(runtime: DevelopmentRuntimeInstallRequest, operation_id: UUID) -> tuple[Path, Path, Path]:
    manager = runtime.database.manager.retained.request.retained
    root, anchor = Path(manager.operation['state_dir']).parent, Path(manager.operation['anchor_dir'])
    _directory(root)
    _directory(anchor)
    return root / 'pool-storage-refresh' / str(operation_id), anchor, root / 'shared-pool-handoff'


def _inputs(runtime: RetainedRuntimeReference, operation_id: UUID, target_disk_type: str
        ) -> tuple[StorageInputs, DevelopmentRuntimeInstallRequest, StorageCollectorPlan, dict[Path, bytes]]:
    from scripts.ops.nebius_shared_pool_entry import _retained_runtime

    files: dict[Path, bytes] = {}
    runtime = RetainedRuntimeReference.model_validate(runtime.model_dump())
    request = _retained_runtime(runtime, files)
    completed = load_completed_runtime(request)
    for path, raw in completed.files.items():
        if path in files and files[path] != raw:
            raise ValueError
        files[path] = raw
    before = request.database.manager.retained.request.registration.spec
    after = before.model_dump(mode='json')
    after['policy_revision'] += 1
    after['quota_identities']['storage'][3] = EXECUTION_DISK_STORAGE_QUOTAS[target_disk_type]
    refresh = ClosedPoolStorageRefresh.model_validate({'schema_version': 'loom.closed-pool-storage-refresh.v1',
        'operation_id': operation_id, 'target_disk_type': target_disk_type, 'before': before, 'after': after})
    plan = prepare_storage_collector(runtime=request, refresh=refresh)
    inputs = StorageInputs(runtime=runtime, refresh=refresh,
        history={path: hashlib.sha256(raw).hexdigest() for path, raw in files.items()})
    _unchanged(files)
    return inputs, request, plan, files


def _anchor(reference: StorageReference, state: Path) -> dict[str, Any]:
    return {'schema': 'loom.closed-pool-storage-anchor.v1', 'reference': reference.model_dump(mode='json'), 'state_dir': str(state)}


def load_closed_storage(*, runtime: RetainedRuntimeReference, reference: StorageReference) -> RetainedClosedStorage:
    try:
        from scripts.ops.nebius_shared_pool_entry import _retained_runtime

        reference = StorageReference.model_validate(reference.model_dump())
        original = _retained_runtime(runtime, {})
        state, anchor, _ = _paths(original, reference.operation_id)
        marker = anchor / (str(reference.operation_id) + '-pool-storage-refresh.json')
        marker_raw, raw = _read(marker), _read(state / 'inputs.json')
        if _json(marker_raw) != _anchor(reference, state) or hashlib.sha256(raw).hexdigest() != reference.inputs_sha256:
            raise ValueError
        supplied = StorageInputs.model_validate(_json(raw))
        if supplied.runtime != runtime or supplied.refresh.operation_id != reference.operation_id:
            raise ValueError
        expected, request, plan, files = _inputs(runtime, reference.operation_id, supplied.refresh.target_disk_type)
        if supplied != expected or any(path in files for path in (marker, state / 'inputs.json')):
            raise ValueError
        files.update({marker: marker_raw, state / 'inputs.json': raw})
        _unchanged(files)
        return RetainedClosedStorage(reference, supplied, request, plan, state, anchor, files)
    except Exception:
        raise ValueError('closed storage history unqualified; preserve evidence') from None


def retain_closed_storage(*, runtime: RetainedRuntimeReference, operation_id: UUID, target_disk_type: str) -> RetainedClosedStorage:
    """Local preparation only; historical authority is never overwritten."""
    try:
        inputs, request, _, files = _inputs(runtime, operation_id, target_disk_type)
        raw = json.dumps(inputs.model_dump(mode='json'), sort_keys=True).encode()
        reference = StorageReference(operation_id=operation_id, inputs_sha256=hashlib.sha256(raw).hexdigest())
        state, anchor, shared = _paths(request, operation_id)
        marker = anchor / (str(operation_id) + '-pool-storage-refresh.json')
        with private_state._locked_state(anchor / 'shared-pool-handoff-lock'):
            _before_handoff(anchor, shared)
            _unchanged(files)
            if marker.exists() or marker.is_symlink():
                return load_closed_storage(runtime=runtime, reference=reference)
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
            return load_closed_storage(runtime=runtime, reference=reference)
    except Exception:
        raise ValueError('closed storage preparation unqualified; preserve evidence') from None


def _before_handoff(anchor: Path, shared: Path) -> None:
    if any(path.exists() or path.is_symlink() for path in (
            shared, anchor / 'shared-pool-handoff.json', anchor / 'shared-pool-recovery.json')):
        raise ValueError


def storage_transition_inputs(retained: RetainedClosedStorage, phase: str
        ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], str]:
    plan = retained.collector
    original = plan.original if phase == 'pause' else _applied_transition(retained,
        'pause' if phase == 'reconfigure' else 'reconfigure')[0]
    target = {'pause': plan.paused, 'reconfigure': plan.reconfigured, 'resume': plan.resumed}[phase]
    return {_key(original): original}, {_key(target): target}, digest({
        'reference': retained.reference.model_dump(mode='json'), 'phase': phase})


def _applied_transition(retained: RetainedClosedStorage, phase: str) -> tuple[dict[str, Any], dict[Path, bytes]]:
    originals, targets, identity = storage_transition_inputs(retained, phase)
    path = retained.state_dir / _CHILDREN[phase]
    raw = _read(path)
    record = _json(raw)
    validate_runtime_transition(record, originals=originals, targets=targets, input_digest=identity, phase=_TRANSITIONS[phase])
    key, = originals
    item = record['resources'][key]
    if item['status'] != 'applied':
        raise ValueError
    observed = copy.deepcopy(item['expected'])
    observed['metadata']['uid'] = _uid(originals[key])
    return observed, {path: raw}


def storage_configuration_identity(retained: RetainedClosedStorage) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    documents = {_key(retained.collector.configuration): retained.collector.configuration}
    binding = asdict(retained.registration.binding)
    return documents, {'schema': 'loom.nebius-management-stage.v1', 'binding': binding,
        'revision': digest({'documents': documents, 'binding': binding}), 'phase': 'closed-pool-storage'}


def read_applied_storage(retained: RetainedClosedStorage) -> AppliedStorageResources:
    files: dict[Path, bytes] = {}
    for phase in _TRANSITIONS:
        collector, child_files = _applied_transition(retained, phase)
        files.update(child_files)
    documents, identity = storage_configuration_identity(retained)
    path = retained.state_dir / _CHILDREN['configuration']
    files[path] = _read(path)
    staged = _json(files[path])
    _validate_record(staged, identity, documents)
    item = staged['resources'][_key(retained.collector.configuration)]
    if item['status'] != 'created':
        raise ValueError
    configuration = copy.deepcopy(item['observed'])
    configuration['metadata']['uid'] = item['uid']
    _uid(configuration)
    validate_storage_registry(request=retained.inputs.refresh, state_dir=retained.state_dir / 'registry', complete=True)
    path = retained.state_dir / _CHILDREN['registry']
    files[path] = _read(path)
    _unchanged(files)
    return AppliedStorageResources(configuration, collector, files)


def _operation_identity(retained: RetainedClosedStorage) -> dict[str, Any]:
    return {'schema': 'loom.closed-pool-storage-operation.v1', 'reference': retained.reference.model_dump(mode='json')}


def _completed(retained: RetainedClosedStorage) -> CompletedClosedStorage:
    resources = read_applied_storage(retained)
    marker = retained.anchor_dir / (str(retained.reference.operation_id) + '-pool-storage-dispatch.json')
    marker_raw, raw = _read(marker), _read(retained.state_dir / 'operation.json')
    identity = _operation_identity(retained)
    expected = {**identity, 'status': 'complete', 'phases': {phase: 'complete' for phase in _CHILDREN},
        'files': {str(path.relative_to(retained.state_dir)): hashlib.sha256(value).hexdigest()
            for path, value in resources.files.items()}}
    if _json(raw) != expected or _json(marker_raw) != identity or retained.files.keys() & resources.files.keys():
        raise ValueError
    files = {**retained.files, **resources.files, marker: marker_raw, retained.state_dir / 'operation.json': raw}
    _unchanged(files)
    return CompletedClosedStorage(retained, resources, files)


def load_completed_storage(*, runtime: RetainedRuntimeReference, reference: StorageReference) -> CompletedClosedStorage:
    try:
        return _completed(load_closed_storage(runtime=runtime, reference=reference))
    except Exception:
        raise ValueError('completed storage correction unqualified; preserve evidence') from None


def storage_runtime_view(retained: RetainedClosedStorage
        ) -> tuple[dict[Path, bytes], dict[str, tuple[dict[str, Any], ...]]]:
    """Only journal-permitted resource alternatives; never an inferred live head."""
    state = retained.state_dir
    marker = retained.anchor_dir / (str(retained.reference.operation_id) + '-pool-storage-dispatch.json')
    files = {path: _read(path) for path in (marker, state / 'operation.json')}
    frame = _json(files[state / 'operation.json'])
    identity = _operation_identity(retained)
    if (_json(files[marker]) != identity or set(frame) != {*identity, 'status', 'phases', 'files'}
            or any(frame[key] != value for key, value in identity.items())
            or frame['status'] not in {'pending', 'complete'} or set(frame['phases']) != set(_CHILDREN)):
        raise ValueError('storage runtime frame differs')
    choices: dict[str, tuple[dict[str, Any], ...]] = {
        _key(row): (row,) for row in (retained.collector.original, retained.collector.previous_configuration)}
    expected_files, pending = set(), False
    for phase, suffix in _CHILDREN.items():
        status = frame['phases'][phase]
        path = state / suffix
        exists = path.exists() or path.is_symlink()
        if status == 'complete' and not pending:
            expected_files.add(suffix)
        elif status in {'prepared', 'intent'}:
            if pending and (status != 'prepared' or exists):
                raise ValueError('storage phase order differs')
            pending = True
        else:
            raise ValueError('storage phase order differs')
        if not exists:
            if status != 'prepared':
                raise ValueError('storage child evidence missing')
            continue
        files[path] = _read(path)
        if status == 'complete' and frame['files'].get(suffix) != hashlib.sha256(files[path]).hexdigest():
            raise ValueError('completed storage evidence changed')
        child = _json(files[path])
        if phase in _TRANSITIONS:
            originals, targets, fingerprint = storage_transition_inputs(retained, phase)
            validate_runtime_transition(child, originals=originals, targets=targets,
                input_digest=fingerprint, phase=_TRANSITIONS[phase])
            key, = originals
            item = child['resources'][key]
            if (status == 'complete' and item['status'] != 'applied') or (status == 'prepared' and item['status'] == 'applied'):
                raise ValueError('storage workload has no matching intent')
            if item['status'] != 'prepared' and status != 'prepared':
                desired = copy.deepcopy(item['expected'])
                desired['metadata']['uid'] = _uid(originals[key])
                choices[key] = (originals[key], desired) if item['status'] == 'intent' else (desired,)
        elif phase == 'configuration':
            documents, stage_identity = storage_configuration_identity(retained)
            _validate_record(child, stage_identity, documents)
            item = child['resources'][_key(retained.collector.configuration)]
            if (status == 'complete' and item['status'] != 'created') or (status == 'prepared' and item['status'] == 'created'):
                raise ValueError('storage configuration has no matching intent')
            if item['status'] == 'created':
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                _uid(actual)
                if _qualified_defaulted(item['desired'], actual) != item['expected']:
                    raise ValueError('storage configuration differs')
                choices[_key(actual)] = (actual,)
        else:
            record = validate_storage_registry(request=retained.inputs.refresh, state_dir=state / phase,
                complete=status == 'complete')
            if status == 'prepared' and record['status'] == 'complete':
                raise ValueError('storage registry has no matching intent')
    if set(frame['files']) != expected_files or (frame['status'] == 'complete' and pending):
        raise ValueError('storage runtime completion differs')
    _unchanged(files)
    return files, choices


class ClosedStorageOperationAPI(Protocol):
    @property
    def configuration(self) -> ManagementStageAPI: ...

    @property
    def registry(self) -> ClosedRegistryAPI[ClosedPoolStorageRefresh]: ...

    def workload(self, phase: str, originals: dict[str, dict[str, Any]],
        targets: dict[str, dict[str, Any]]) -> DevelopmentRuntimeWorkloadAPI: ...

    def qualify(self, retained: RetainedClosedStorage, phase: str) -> None: ...

    def qualify_publication(self, retained: RetainedClosedStorage, resources: AppliedStorageResources) -> bool: ...


class _StorageStage(_ScopedCatalog):
    def __init__(self, api: ManagementStageAPI, check: Callable[[], None], dispatch: Callable[[], None]):
        super().__init__(api, check)
        self.dispatch = dispatch

    def create_resource(self, document: dict[str, Any]) -> None:
        try:
            self.dispatch()
            self.check()
        except Exception:
            raise ManagementNotDispatchedError('storage configuration not dispatched') from None
        try:
            self.api.create_resource(document)
        finally:
            self.check()


class _StorageWorkload(_ScopedManager):
    def __init__(self, api: DevelopmentRuntimeWorkloadAPI, check: Callable[[], None], dispatch: Callable[[], None]):
        super().__init__(api, check)
        self.dispatch = dispatch

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        try:
            self.dispatch()
            self.check()
        except Exception:
            raise ManagementNotDispatchedError('storage collector not dispatched') from None
        try:
            return self.api.patch_workload(key, before, desired)
        finally:
            self.check()


class _StorageRegistry:
    def __init__(self, api: ClosedRegistryAPI[ClosedPoolStorageRefresh], dispatch: Callable[[], None]):
        self.api, self.dispatch = api, dispatch

    def observe(self, request: ClosedPoolStorageRefresh) -> dict[str, Any]:
        return self.api.observe(request)

    def commit(self, request: ClosedPoolStorageRefresh) -> dict[str, Any]:
        try:
            self.dispatch()
        except Exception:
            raise RegistryNotDispatchedError('storage parent failed before database dispatch') from None
        return self.api.commit(request)


def run_closed_storage(*, retained: RetainedClosedStorage, api: ClosedStorageOperationAPI) -> bool:
    """False is known drain/publication wait; unknown effects retain their intent."""
    try:
        frozen = load_closed_storage(runtime=retained.inputs.runtime, reference=retained.reference)
        if frozen != retained:
            raise ValueError
        state, anchor, shared = _paths(frozen.runtime, frozen.reference.operation_id)
        path = state / 'operation.json'
        marker = anchor / (str(frozen.reference.operation_id) + '-pool-storage-dispatch.json')
        identity = _operation_identity(frozen)
        record: dict[str, Any] = {**identity, 'status': 'pending',
            'phases': {phase: 'prepared' for phase in _CHILDREN}, 'files': {}}
        qualification_failed = False

        def roots() -> None:
            if retained != frozen:
                raise ValueError
            _unchanged(frozen.files)
            _before_handoff(anchor, shared)

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
            active = {state / suffix: _read(state / suffix) for suffix in _CHILDREN.values()
                if (state / suffix).exists() or (state / suffix).is_symlink()}
            try:
                api.qualify(copy.deepcopy(frozen), phase)
                _unchanged(active)
                local()
            except Exception:
                qualification_failed = True
                raise

        def dispatch(phase: str) -> None:
            scope(phase)
            _read(state / _CHILDREN[phase])  # Child intent must already exist.
            if record['phases'][phase] == 'prepared':
                record['phases'][phase] = 'intent'
                private_state._atomic_json(path, record)
            if record['phases'][phase] != 'intent':
                raise ValueError
            local()

        with private_state._locked_state(anchor / 'shared-pool-handoff-lock'):
            roots()
            if marker.exists() or marker.is_symlink():
                if _json(_read(marker)) != identity:
                    raise ValueError
                record = _json(_read(path))
            else:
                if any((state / name).exists() or (state / name).is_symlink() for name in _CHILDREN):
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
                exists = (state / phase).exists() or (state / phase).is_symlink()
                if status == 'complete' and not pending:
                    expected_files.add(suffix)
                elif status in {'prepared', 'intent'}:
                    if pending and (status != 'prepared' or exists):
                        raise ValueError
                    pending = True
                    if status == 'intent':
                        _read(state / suffix)
                else:
                    raise ValueError
            if set(record['files']) != expected_files or (record['status'] == 'complete' and pending):
                raise ValueError
            scope('preflight')
            for phase, suffix in _CHILDREN.items():
                if record['phases'][phase] == 'complete':
                    continue
                scope(phase)
                def check(phase: str = phase) -> None:
                    scope(phase)

                def write(phase: str = phase) -> None:
                    dispatch(phase)
                if phase in _TRANSITIONS:
                    originals, targets, fingerprint = storage_transition_inputs(frozen, phase)
                    work = api.workload(phase, copy.deepcopy(originals), copy.deepcopy(targets))
                    if not advance_runtime_workloads(originals=originals, targets=targets, input_digest=fingerprint,
                            phase=_TRANSITIONS[phase], api=_StorageWorkload(work, check, write), state_dir=state / phase):
                        scope(phase)
                        return False
                elif phase == 'configuration':
                    documents, stage_identity = storage_configuration_identity(frozen)
                    _stage_fixed_documents(documents=documents, revision=stage_identity['revision'],
                        phase=stage_identity['phase'], binding=frozen.registration.binding,
                        api=_StorageStage(api.configuration, check, write), state_dir=state / phase)
                else:
                    advance_storage_registry(request=frozen.inputs.refresh, api=_StorageRegistry(api.registry, write),
                        state_dir=state / phase, qualify=check)
                scope(phase)
                if record['phases'][phase] != 'intent':
                    raise ValueError
                record['files'][suffix] = hashlib.sha256(_read(state / suffix)).hexdigest()
                record['phases'][phase] = 'complete'
                private_state._atomic_json(path, record)
            resources = read_applied_storage(frozen)
            scope('terminal')
            ready = api.qualify_publication(copy.deepcopy(frozen), copy.deepcopy(resources))
            _unchanged(resources.files)
            scope('terminal')
            if type(ready) is not bool:
                raise ValueError
            if not ready:
                return False
            if record['status'] != 'complete':
                record['status'] = 'complete'
                private_state._atomic_json(path, record)
            _completed(frozen)
            local()
            return True
    except Exception:
        raise ValueError('closed storage correction incomplete; preserve original operation evidence') from None
