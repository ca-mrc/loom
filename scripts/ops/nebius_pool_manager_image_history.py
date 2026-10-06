"""Bounded image-only ancestry rooted in the original closed pool journals.

These readers grant no publication or write authority. The protected entry must
resolve each selected publication and bind original inputs before dispatch.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import _activation_record
from scripts.ops.nebius_pool_application_delivery import derive_application_build_deployment
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_manager_image import manager_image_target
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup import _startup_record, closed_startup_documents
from scripts.ops.nebius_pool_startup_repair import (
    PoolStartupRepairBinding,
    _exists,
    _repair_record,
    startup_repair_exists,
)

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import ProtectedPublication

STEPS = ('isolate', 'stop', 'template', 'start')
IMAGE_MARKER = 'loom.nebius/manager-image-repair'
MAX_CORRECTIONS = 8


class ManagerImageRepairBinding(PoolStartupRepairBinding):
    ordinal: int = Field(ge=1, le=MAX_CORRECTIONS, strict=True)
    source_repair_sha256: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    predecessor_sha256: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    publication: ProtectedPublication
    candidate: dict[str, Any]
    profile: dict[str, Any]


@dataclass(frozen=True, repr=False)
class ManagerImageEntry:
    binding: ManagerImageRepairBinding
    identity: dict[str, Any]
    documents: tuple[dict[str, Any], ...]
    record: dict[str, Any] | None
    path: Path
    marker: Path
    anchored: bool


def _paths(operation: UUID, ordinal: int, state: Path, anchor: Path) -> tuple[Path, Path]:
    name = f'manager-image-{ordinal:02d}.json'
    return state / name, anchor / (str(operation) + '-' + name)


def _completed(entry: ManagerImageEntry) -> bool:
    return entry.record is not None and all(row['phase'] == 'applied' for row in entry.record['phases'].values())


def prepared_image_record(identity: dict[str, Any]) -> dict[str, Any]:
    return {**identity, 'phases': {name: {'phase': 'prepared', 'before_resource_version': None} for name in STEPS}}


def image_phase_options(documents: tuple[dict[str, Any], ...], record: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    for index, name in enumerate(STEPS):
        phase = record['phases'][name]['phase']
        if phase != 'applied':
            return (documents[index],) if phase == 'prepared' else (documents[index], documents[index + 1])
    return (documents[-1],)


def _read(path: Path) -> dict[str, Any]:
    raw = private_state._private_read(path, limit=4 * 1024**2)
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != json.dumps(value, sort_keys=True).encode():
        raise ValueError
    return value


def _record(path: Path, identity: dict[str, Any]) -> dict[str, Any] | None:
    # Anchor-only is a recoverable enrollment prefix: no remote intent existed.
    if not _exists(path):
        return None
    record = _read(path)
    if (not isinstance(record, dict) or set(record) != {*identity, 'phases'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['phases'], dict) or set(record['phases']) != set(STEPS)):
        raise ValueError
    previous_complete = True
    for name in STEPS:
        row = record['phases'][name]
        if (not isinstance(row, dict) or set(row) != {'phase', 'before_resource_version'}
                or row['phase'] not in {'prepared', 'intent', 'applied'}
                or (not previous_complete and row['phase'] != 'prepared')):
            raise ValueError
        version = row['before_resource_version']
        if row['phase'] == 'prepared':
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        previous_complete = row['phase'] == 'applied'
    return record


def _entry(request: PoolCutoverRequest, binding: ManagerImageRepairBinding, *, state: Path,
        anchor: Path, prior: tuple[ManagerImageEntry, ...]) -> ManagerImageEntry:
    binding = ManagerImageRepairBinding.model_validate(binding.model_dump())
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    path, marker = _paths(operation, binding.ordinal, state, anchor)
    retained = _read(marker) if _exists(marker) else None
    if (not binding.operation_id.int or binding.operation_id == operation
            or binding.operation_id in {row.binding.operation_id for row in prior}
            or binding.ordinal != len(prior) + 1 or (_exists(path) and retained is None)
            or any(not _completed(row) for row in prior)
            or binding.predecessor_sha256 != (_hash(prior[-1].path) if prior else None)
            or binding.source_sha != binding.publication.source_sha
            or binding.candidate.get('candidate_sha') != binding.source_sha
            or binding.profile.get('candidate_sha') != binding.source_sha):
        raise ValueError
    if prior and any(getattr(binding, key) != getattr(prior[0].binding, key) for key in (
            'original_operation_sha256', 'inputs_sha256', 'closure_sha256', 'startup_sha256',
            'activation_sha256', 'source_repair_sha256')):
        raise ValueError
    delivery = request.application_delivery
    if delivery is None:
        raise ValueError
    verify_execution_image_admission(ExecutionImageAdmissionBundleV1.model_validate(binding.profile['image_admission']),
        keyring=ImageAdmissionKeyring.from_json(json.dumps(delivery.before.installation.keyring)),
        required_image_refs=[binding.profile[key] for key in ('task_image_ref', 'runtime_image_ref', 'agent_image_ref')
            if binding.profile.get(key) is not None])
    closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
    _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
    activation_identity, activation = _activation_record(request, state=state, anchor=anchor)
    if (startup is None or any(row['phase'] != 'started' for row in startup['workloads'].values())
            or activation is None or _hash(state / 'cutover.json') != binding.closure_sha256
            or _hash(state / 'startup.json') != binding.startup_sha256):
        raise ValueError
    activation_entry = (private_state._private_read(state / 'activation.json').decode()
        if retained is None else retained['activation_entry'])
    prepared = {**activation_identity, 'opening': 'prepared', 'cancellation': 'prepared',
        'guards': {str(row.participant_id): {'release': 'prepared', 'fence': 'prepared'}
            for row in request.fencing.retirement.migration.guards}}
    if (not isinstance(activation_entry, str)
            or hashlib.sha256(activation_entry.encode()).hexdigest() != binding.activation_sha256
            or json.loads(activation_entry) != prepared):
        raise ValueError
    original = copy.deepcopy(targets[_key(request.manager)])
    if startup_repair_exists(request, state=state, anchor=anchor):
        source_documents, _, _, source = _repair_record(request, state=state, anchor=anchor)
        if (source is None or any(row['phase'] != 'applied' for row in source['phases'].values())
                or binding.source_repair_sha256 != _hash(state / 'startup-repair.json')):
            raise ValueError
        original = copy.deepcopy(source_documents[-1])
    elif binding.source_repair_sha256 is not None or delivery.source_delivery_version != 'v2':
        raise ValueError
    if prior:
        original = copy.deepcopy(prior[-1].documents[-1])
    original['metadata']['uid'] = _uid(request.manager)
    deployment = derive_application_build_deployment(delivery.before,
        request.fencing.retirement.migration.registration.spec)
    desired = manager_image_target(ManagementRefreshRenderRequest(deployment, deployment,
        original, binding.candidate, binding.profile, delivery.repo_root))
    if IMAGE_MARKER in original['metadata'].get('annotations', {}):
        raise ValueError
    isolated = _snapshot(original)
    isolated['metadata'].setdefault('annotations', {})[IMAGE_MARKER] = str(binding.operation_id)
    stopped, replaced = copy.deepcopy(isolated), copy.deepcopy(desired)
    replaced['metadata'].setdefault('annotations', {})[IMAGE_MARKER] = str(binding.operation_id)
    stopped['spec']['replicas'] = replaced['spec']['replicas'] = 0
    documents = (_snapshot(original), isolated, stopped, replaced, desired)
    identity = {'schema': 'loom.nebius-pool-manager-image.v1', 'operation_id': str(operation),
        'state_dir': str(state), 'binding': binding.model_dump(mode='json'),
        'activation_entry': activation_entry, 'workloads_sha256': digest(documents)}
    if retained is not None and retained != identity:
        raise ValueError
    return ManagerImageEntry(binding, identity, documents, _record(path, identity), path, marker, retained is not None)


def load_manager_image_chain(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> tuple[ManagerImageEntry, ...]:
    """Validate every entry, rejecting gaps, orphan records and history drift."""
    try:
        operation = request.fencing.retirement.migration.registration.spec.operation_id
        expected = {_paths(operation, ordinal, state, anchor) for ordinal in range(1, MAX_CORRECTIONS + 1)}
        allowed = {path for pair in expected for path in pair}
        observed = {*state.glob('manager-image-*'), *anchor.glob(str(operation) + '-manager-image-*')}
        if not observed <= allowed:
            raise ValueError
        result: tuple[ManagerImageEntry, ...] = ()
        gap = False
        for ordinal in range(1, MAX_CORRECTIONS + 1):
            path, marker = _paths(operation, ordinal, state, anchor)
            if not _exists(marker):
                if _exists(path):
                    raise ValueError
                gap = True
                continue
            if gap:
                raise ValueError
            identity = _read(marker)
            binding = ManagerImageRepairBinding.model_validate(identity['binding'])
            if binding.ordinal != ordinal:
                raise ValueError
            result += (_entry(request, binding, state=state, anchor=anchor, prior=result),)
        return result
    except Exception:
        raise ValueError('pool_manager_image_history_unqualified') from None


def manager_image_entry(request: PoolCutoverRequest, binding: ManagerImageRepairBinding, *,
        state: Path, anchor: Path) -> ManagerImageEntry:
    try:
        chain = load_manager_image_chain(request, state=state, anchor=anchor)
        if binding.ordinal <= len(chain):
            entry = chain[binding.ordinal - 1]
            if entry.binding != binding or binding.ordinal != len(chain):
                raise ValueError
            return entry
        return _entry(request, binding, state=state, anchor=anchor, prior=chain)
    except Exception:
        raise ValueError('pool_manager_image_entry_unqualified') from None


def manager_image_options(request: PoolCutoverRequest, *, state: Path, anchor: Path,
        choices: dict[str, tuple[dict[str, Any], ...]]) -> dict[str, tuple[dict[str, Any], ...]]:
    chain = load_manager_image_chain(request, state=state, anchor=anchor)
    if not chain:
        return choices
    key = _key(request.manager)
    if len(choices[key]) != 1 or _stable(choices[key][0]) != _stable(chain[0].documents[0]):
        raise ValueError('pool_manager_image_projection_differs')
    tail = chain[-1]
    return {**choices, key: image_phase_options(tail.documents, tail.record or prepared_image_record(tail.identity))}


def qualify_completed_manager_images(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> None:
    if any(not _completed(row) for row in load_manager_image_chain(request, state=state, anchor=anchor)):
        raise ValueError('pool_manager_image_incomplete')


def original_recovery_image(request: PoolCutoverRequest, *, state: Path, anchor: Path) -> ManagerImageEntry | None:
    """An old rollback may win only before the first image's destructive intent.

    Its fence remains byte-for-byte authoritative. A pending metadata isolate
    is invalidated by shutdown, whose exact projection also removes that marker.
    The normal fence reader still validates the full retained identity/record.
    """
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    marker = anchor / (str(operation) + '-startup-fence.json')
    if not _exists(marker) or 'manager_image_sha256' in _read(marker):
        return None
    chain = load_manager_image_chain(request, state=state, anchor=anchor)
    if not chain:
        return None
    _read(state / 'startup-fence.json')
    if len(chain) != 1:
        raise ValueError('pool_manager_image_legacy_recovery_unqualified')
    entry = chain[0]
    record = entry.record or prepared_image_record(entry.identity)
    _, cancellation = _activation_record(request, state=state, anchor=anchor)
    if (record['phases']['isolate']['phase'] not in {'prepared', 'intent'}
            or any(record['phases'][phase] != {'phase': 'prepared', 'before_resource_version': None}
                for phase in ('stop', 'template', 'start'))
            or cancellation is None or cancellation['cancellation'] != 'fenced'
            or any(row['fence'] != 'fenced' for row in cancellation['guards'].values())):
        raise ValueError('pool_manager_image_legacy_recovery_unqualified')
    return entry


def manager_image_paths(operation: str, *, state: Path, anchor: Path) -> tuple[Path, ...]:
    """Include missing records for enrolled tails; no-chain receipts stay unchanged."""
    pairs = (_paths(UUID(operation), ordinal, state, anchor) for ordinal in range(1, MAX_CORRECTIONS + 1))
    return tuple(path for pair in pairs if any(_exists(value) for value in pair) for path in pair)


def manager_image_fence_patches(request: PoolCutoverRequest, before: dict[str, Any], *,
        state: Path, anchor: Path) -> list[dict[str, Any]]:
    from scripts.ops.nebius_pool_startup_fence import marked_startup_document

    chain = load_manager_image_chain(request, state=state, anchor=anchor)
    if not chain or chain[-1].record is None:
        raise ValueError('pool_manager_image_fence_unqualified')
    tail = chain[-1]
    assert tail.record is not None
    index, = (index for index, name in enumerate(STEPS) if tail.record['phases'][name]['phase'] == 'intent')
    version = tail.record['phases'][STEPS[index]]['before_resource_version']
    if (not _matches(before, tail.documents[index], _uid(request.manager))
            or before['metadata']['resourceVersion'] != version):
        raise ValueError('pool_manager_image_fence_unqualified')
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    desired = marked_startup_document(before, operation)
    return [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(request.manager)},
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
        {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
        {'op': 'test', 'path': '/spec', 'value': before['spec']},
        {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']}]
