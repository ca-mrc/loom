"""Forward handoff from current staging into the completed closed dev pool.

These projections qualify source and retained history, not live writer ownership.
The protected connected parent must establish sole Kubernetes/provider writers,
guard/drain and exact recovery evidence before consuming a transition. Old
registration, retirement and rollback stages are never replayed here.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest, registration_documents
from scripts.ops.nebius_staging_pool_attachment import (
    StagingPoolAttachment,
    prepare_staging_pool_attachment,
)

from loom.nebius_platform_render import digest
from loom_service.pool_management.installation import PoolInstallation

MARKER = 'loom.nebius/shared-pool-handoff-operation'
_LEGACY_MARKERS = {'loom.nebius/pool-retirement-operation', 'loom.nebius/pool-cutover-operation',
    'loom.nebius/pool-startup-operation'}


@dataclass(frozen=True, repr=False)
class StagingForwardPlan:
    attachment: StagingPoolAttachment
    legacy: CompletedPoolCutover
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    input_digest: str


def prepare_staging_forward(*, registration: PoolRegistrationRequest, observation: dict[str, Any],
        legacy: CompletedPoolCutover, repo_root: Path) -> StagingForwardPlan:
    """Bind a new initial catalog to current staging and its completed ancestry.

    This does not load a completed dev runtime or grant permission to run the
    handoff. The connected parent must bind that evidence separately. The old
    completion proves ancestry/identity only: its workload specs are deliberately
    not used as today's stop, restart or recovery targets.
    """
    try:
        spec = PoolInstallation.model_validate(registration.spec.model_dump())
        registration_documents(registration)
        if (load_completed_pool(legacy.selector, original=legacy.context.original) != legacy
                or legacy.completion.outcome != 'legacy'
                or spec.operation_id == legacy.context.inputs.installation.operation_id
                or spec.pool_id == legacy.context.inputs.installation.pool_id
                or registration.binding.namespace != 'loom-nebius-management-dev'):
            raise ValueError
        participant, = (row for row in spec.participants if row.environment_class == 'staging')
        attachment = prepare_staging_pool_attachment(participant=participant, observation=observation,
            candidate=registration.candidate, cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, repo_root=repo_root)
        # Initial registration is immutable. Derive and compare before it is
        # frozen, then repeat on the completed closed installation before guard.
        if spec.profiles.image_admission_keyring != attachment.profiles.image_admission_keyring:
            raise ValueError
        for field in ('execution', 'task_images'):
            selected = getattr(attachment.profiles, field)
            ids = {row.profile_id for row in selected}
            registered = {row.profile_id: row for row in getattr(spec.profiles, field) if row.profile_id in ids}
            if registered != {row.profile_id: row for row in selected}:
                raise ValueError
        old_guard, = (row for row in legacy.context.inputs.guards if row.namespace == attachment.guard.namespace)
        old_participant, = (row for row in legacy.context.inputs.installation.participants
            if row.participant_id == old_guard.participant_id)
        if (participant.environment_id != old_participant.environment_id
                or participant.execution_namespace != old_participant.execution_namespace
                or participant.build_namespace != old_participant.build_namespace
                or old_guard.namespace_uid != attachment.guard.namespace_uid
                or old_guard.database is None or attachment.guard.database is None
                or (spec.cluster_id, spec.node_group_id) != (legacy.context.inputs.installation.cluster_id,
                    legacy.context.inputs.installation.node_group_id)):
            raise ValueError
        for current, previous_database in ((attachment.guard.database.statefulset, old_guard.database.statefulset),
                (attachment.guard.database.service, old_guard.database.service)):
            if _key(current) != _key(previous_database) or _uid(current) != _uid(previous_database):
                raise ValueError
        # Old catalog rows are immutable. New guest targets can postdate the
        # historical completion, but existing task/execution identities must
        # remain addressable. Application builds move to the dev data identity.
        current_targets = {row.target_id: set(row.workload_kinds) for row in participant.targets}
        for target in old_participant.targets:
            kinds = set(target.workload_kinds) - {'application_image_build'}
            if kinds and not kinds <= current_targets.get(target.target_id, set()):
                raise ValueError
        rows = {_key(row): row for row in attachment.resources}
        service = rows[f'Deployment:{attachment.guard.namespace}:loom-service']
        collector = rows[f'CronJob:{participant.execution_namespace.name}:loom-execution-capacity-collector']
        originals = {_key(row): copy.deepcopy(row) for row in (
            collector, *attachment.actuators, attachment.guard.controller, service)}
        if len(originals) != len(attachment.actuators) + 3:
            raise ValueError
        old_operation = legacy.selector.operation['operation_id']
        for key, row in originals.items():
            annotations = row['metadata'].get('annotations', {})
            if MARKER in annotations:
                raise ValueError
            previous = legacy.completion.workloads.get(key)
            if previous is None:
                # A newly published guest sibling can legitimately postdate the
                # old recovery. Existing primary processes and marked resources
                # may not silently become unrelated replacement identities.
                if row not in attachment.actuators[1:] or _LEGACY_MARKERS & annotations.keys():
                    raise ValueError
            elif _uid(row) != _uid(previous):
                raise ValueError
            for marker in _LEGACY_MARKERS & annotations.keys():
                if (annotations[marker] != old_operation or previous is None
                        or previous['metadata'].get('annotations', {}).get(marker) != annotations[marker]):
                    raise ValueError
            # Missing old markers must not erase the ancestry under a new
            # publication. Different templates are permitted; lost lineage isn't.
            if previous is not None and any(annotations.get(marker) != value
                    for marker, value in previous['metadata'].get('annotations', {}).items() if marker in _LEGACY_MARKERS):
                raise ValueError
        stopped = {}
        for key, current in originals.items():
            desired = _snapshot(current)
            desired['metadata'].setdefault('annotations', {})[MARKER] = str(spec.operation_id)
            field = 'suspend' if desired['kind'] == 'CronJob' else 'replicas'
            desired['spec'][field] = True if field == 'suspend' else 0
            stopped[key] = desired
        checksum = digest({'registration': spec.model_dump(mode='json'), 'candidate': registration.candidate,
            'legacy': legacy.selector.model_dump(mode='json'), 'history': {str(path): value for path, value in legacy.history.items()},
            'guard': {'namespace_uid': str(attachment.guard.namespace_uid),
                'database_credential_uid': str(attachment.guard.database.credential_uid),
                'database_credential_resource_version': attachment.guard.database.credential_resource_version,
                'actuator_credential_uid': str(attachment.guard.database.actuator_credential_uid),
                'actuator_credential_resource_version': attachment.guard.database.actuator_credential_resource_version},
            'originals': originals, 'stopped': stopped})
        return StagingForwardPlan(attachment, legacy, originals, stopped, checksum)
    except Exception:
        raise ValueError('shared pool staging predecessor unqualified') from None
