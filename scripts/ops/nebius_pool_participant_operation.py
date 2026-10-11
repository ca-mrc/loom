"""Durable original inputs for the protected normal-release parent.

Local retention is not publication approval, a completed predecessor, or live
deployment authority. The protected caller supplies the qualified predecessor
and fresh inventory, and must qualify those again before effects. Recovery loads
this original operation; it never prepares a replacement from current resources.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import _participant_release_binding, validate_render
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_ingress_stage import _snapshot
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract
from scripts.ops.nebius_pool_participant_release import (
    PreparedParticipantRelease,
    prepare_manager_catalog_refresh,
    prepare_staging_pool_release,
)
from scripts.ops.nebius_shared_pool_operation import _directory

from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.candidates import ProtectedPublication, _json

_MAX_INPUT_BYTES = 16 * 1024**2


class ParticipantPredecessor(Protocol):
    """A completed root or release returned by its semantic history loader.

    This structural interface is not an attestation. Its consumer must keep the
    completed-history loader and current qualification in the protected parent.
    """

    @property
    def migration(self) -> PoolMigrationRequest: ...

    @property
    def state_dir(self) -> Path: ...

    @property
    def history(self) -> dict[Path, str]: ...


class ParticipantRolloutReference(BaseModel):
    """Bounded non-secret recovery selector, suitable for deployment intent."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-participant-rollout-reference.v1'] = 'loom.nebius-participant-rollout-reference.v1'
    operation_id: UUID
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    publication: ProtectedPublication

    @field_validator('operation_id')
    @classmethod
    def nonzero_operation(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError('participant rollout operation must be nonzero')
        return value


class ParticipantRolloutInputs(BaseModel):
    """Private original snapshot; no credentials or executable caller payload."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-participant-rollout-inputs.v1'] = 'loom.nebius-participant-rollout-inputs.v1'
    predecessor_operation_id: UUID
    predecessor_state_dir: Path
    history: dict[Path, str]
    previous: PoolMigrationRequest
    current: tuple[dict[str, Any], ...]
    config: dict[str, Any]
    candidate: dict[str, Any]
    runtime_profile: ServiceExecutionRuntimeProfileV1
    source: PreparedDevelopmentSource
    publication: ProtectedPublication
    management_origin: str
    rendered_files: dict[str, list[dict[str, Any]]]
    manager: dict[str, Any]
    catalog: dict[str, Any]
    release: PreparedParticipantRelease


@dataclass(frozen=True, repr=False)
class RetainedParticipantRollout:
    reference: ParticipantRolloutReference
    state_dir: Path
    inputs: ParticipantRolloutInputs

    @property
    def release(self) -> PreparedParticipantRelease:
        return self.inputs.release

    @property
    def guard_owner(self) -> str:
        return 'rollout-' + self.reference.operation_id.hex


def _history(inputs: ParticipantRolloutInputs) -> None:
    if not inputs.history or len(inputs.history) > 32768:
        raise ValueError
    _directory(inputs.predecessor_state_dir)
    for path, checksum in inputs.history.items():
        if (not path.is_absolute() or path != path.resolve() or re.fullmatch(r'[0-9a-f]{64}', checksum) is None
                or hashlib.sha256(private_state._private_read(path, limit=_MAX_INPUT_BYTES)).hexdigest() != checksum):
            raise ValueError


def _qualify(inputs: ParticipantRolloutInputs) -> None:
    """Check retained semantics without running the candidate/operator renderer."""
    migration_contract(inputs.previous)
    release, candidate = inputs.release, inputs.candidate
    if (inputs.predecessor_operation_id != inputs.previous.registration.spec.operation_id
            or release.refresh.before != inputs.previous.registration.spec
            or inputs.publication.source_sha != inputs.source.source_sha
            or candidate.get('schema_version') != 'loom.nebius-candidate.v1'
            or candidate.get('candidate_sha') != inputs.source.source_sha
            or candidate.get('source_archive_sha256') != inputs.source.source_archive_sha256
            or type(candidate.get('run_id')) is not int or candidate['run_id'] != inputs.publication.run_id
            or inputs.runtime_profile.candidate_sha != inputs.source.source_sha
            or candidate != release.migration.registration.candidate):
        raise ValueError
    manifest, config, _ = validate_render(release.files)
    _participant_release_binding(release, manifest, config, release.files)
    _, original_config, _ = validate_render(inputs.rendered_files)
    if config != inputs.config or original_config != config:
        raise ValueError
    # These snapshots cannot contain live Secret data; existing snapshot rules
    # also reject malformed/missing immutable Kubernetes identities.
    for row in (*inputs.current, inputs.manager, inputs.catalog):
        _snapshot(row)
    prepare_manager_catalog_refresh(release=release, manager=inputs.manager, catalog=inputs.catalog)
    _history(inputs)


def _paths(reference: ParticipantRolloutReference, state_root: Path, anchor_dir: Path) -> tuple[Path, Path]:
    for root in (state_root, anchor_dir):
        if not root.is_absolute() or root != root.resolve() or root == Path('/'):
            raise ValueError
    if state_root == anchor_dir or state_root in anchor_dir.parents or anchor_dir in state_root.parents:
        raise ValueError
    return state_root / str(reference.operation_id), anchor_dir / (str(reference.operation_id) + '-participant-rollout.json')


def _anchor(reference: ParticipantRolloutReference, state_dir: Path) -> dict[str, Any]:
    return {'schema_version': 'loom.nebius-participant-rollout-anchor.v1',
        'reference': reference.model_dump(mode='json'), 'state_dir': str(state_dir)}


def load_participant_rollout(*, reference: ParticipantRolloutReference,
        state_root: Path, anchor_dir: Path) -> RetainedParticipantRollout:
    """Read the original anchored operation without writes or live recapture."""
    try:
        reference = ParticipantRolloutReference.model_validate(reference.model_dump())
        state, marker = _paths(reference, state_root, anchor_dir)
        for directory in (state_root, state, anchor_dir):
            _directory(directory)
        before = private_state._private_read(marker)
        if _json(before) != _anchor(reference, state):
            raise ValueError
        raw = private_state._private_read(state / 'inputs.json', limit=_MAX_INPUT_BYTES)
        if hashlib.sha256(raw).hexdigest() != reference.inputs_sha256:
            raise ValueError
        inputs = ParticipantRolloutInputs.model_validate(_json(raw))
        if (inputs.publication != reference.publication
                or inputs.release.refresh.after.operation_id != reference.operation_id):
            raise ValueError
        _qualify(inputs)
        if (private_state._private_read(marker) != before
                or private_state._private_read(state / 'inputs.json', limit=_MAX_INPUT_BYTES) != raw):
            raise ValueError
        return RetainedParticipantRollout(reference, state, inputs)
    except Exception:
        raise ValueError('participant rollout history unavailable; preserve evidence') from None


def retain_participant_rollout(*, predecessor: ParticipantPredecessor,
        current: tuple[dict[str, Any], ...], config: dict[str, Any], candidate: dict[str, Any],
        runtime_profile: ServiceExecutionRuntimeProfileV1, operation_id: UUID, management_origin: str,
        repo_root: Path, rendered_files: dict[str, list[dict[str, Any]]],
        source: PreparedDevelopmentSource, publication: ProtectedPublication,
        manager: dict[str, Any], catalog: dict[str, Any], state_root: Path, anchor_dir: Path) -> RetainedParticipantRollout:
    """Freeze a new operation, or require byte-identical input on preparation replay.

    This creates private local records only. The independent anchor precedes any
    remote authority. An anchored missing input is never reconstructed, including
    when a caller happens to have a matching current cluster snapshot.
    """
    try:
        release = prepare_staging_pool_release(previous=predecessor.migration, current=current, config=config,
            candidate=candidate, runtime_profile=runtime_profile, operation_id=operation_id,
            management_origin=management_origin, repo_root=repo_root, rendered_files=rendered_files)
        inputs = ParticipantRolloutInputs(predecessor_operation_id=predecessor.migration.registration.spec.operation_id,
            predecessor_state_dir=predecessor.state_dir, history=copy.deepcopy(predecessor.history),
            previous=copy.deepcopy(predecessor.migration), current=copy.deepcopy(current), config=copy.deepcopy(config),
            candidate=copy.deepcopy(candidate), runtime_profile=runtime_profile.model_copy(deep=True),
            source=source.model_copy(deep=True), publication=publication.model_copy(deep=True),
            management_origin=management_origin, rendered_files=copy.deepcopy(rendered_files),
            manager=copy.deepcopy(manager), catalog=copy.deepcopy(catalog), release=release)
        _qualify(inputs)
        payload = inputs.model_dump(mode='json')
        # Match the shared private atomic writer's serialization exactly, so
        # reference digests bind stored bytes, not merely decoded JSON meaning.
        raw = json.dumps(payload, sort_keys=True).encode()
        if len(raw) > _MAX_INPUT_BYTES:
            raise ValueError
        reference = ParticipantRolloutReference(operation_id=operation_id,
            inputs_sha256=hashlib.sha256(raw).hexdigest(), publication=inputs.publication)
        state, marker = _paths(reference, state_root, anchor_dir)
        if any(state == path or state in path.parents or marker == path for path in inputs.history):
            raise ValueError
        with private_state._locked_state(state_root):
            private_state._private_directory(anchor_dir)
            if marker.exists() or marker.is_symlink():
                return load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
            private_state._private_directory(state)
            path = state / 'inputs.json'
            # Only an interrupted, identical local preparation may precede its
            # anchor. Child journals here would imply lost dispatch authority.
            if any(item.name != 'inputs.json' for item in state.iterdir()):
                raise ValueError
            if path.exists() or path.is_symlink():
                if private_state._private_read(path, limit=_MAX_INPUT_BYTES) != raw:
                    raise ValueError
            else:
                private_state._atomic_json(path, payload)
            _history(inputs)
            private_state._atomic_json(marker, _anchor(reference, state))
            return load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
    except Exception:
        raise ValueError('participant rollout preparation unqualified; preserve evidence') from None
