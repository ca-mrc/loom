"""Forward handoff from current staging into the completed closed dev pool.

These projections qualify source and retained history, not live writer ownership.
The protected connected parent must establish sole Kubernetes/provider writers,
guard/drain and exact recovery evidence before consuming a transition. Old
registration, retirement and rollback stages are never replayed here.
"""
from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_activation import (
    DevelopmentActivationPlan,
    prepare_development_activation,
)
from scripts.ops.nebius_development_pool_registration import retained_development_participant
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest, registration_documents
from scripts.ops.nebius_pool_runtime import wire_participant
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


@dataclass(frozen=True, repr=False)
class SharedPoolHandoffPlan:
    development: DevelopmentActivationPlan
    staging: StagingForwardPlan
    migration: PoolMigrationRequest
    staging_targets: dict[str, dict[str, Any]]
    history: dict[Path, str]
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
                or registration.binding.kube_system_uid != legacy.context.request.fencing.retirement.migration.registration.binding.kube_system_uid
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


def prepare_shared_pool_handoff(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path) -> SharedPoolHandoffPlan:
    """Compose completed CLOSED dev and current staging without replaying either.

    The complete migration contract is used only for fixed staging projection.
    Fresh dev is already installed, not a standalone writer to retire or register
    again. No guard, grant, workload transition or SQL opening is performed here.
    The protected parent still owns live sole-writer and provider qualification.
    """
    try:
        development = prepare_development_activation(runtime)
        completed = development.runtime
        database = completed.request.database
        pool = database.manager.retained.request
        registration = pool.registration
        participant = retained_development_participant(pool)
        # One verified baseline already runs staging. The handoff does not
        # silently deploy a newer manager/controller candidate into staging.
        if (database.manager.publication.bundle.candidate != registration.candidate
                or len(registration.spec.participants) != 2):
            raise ValueError
        staging = prepare_staging_forward(registration=registration, observation=observation,
            legacy=legacy, repo_root=repo_root)
        controller = completed.resources['Deployment:loom-dev:loom-control-plane']
        dev_guard = PoolGuardTarget(participant_id=participant.participant_id, namespace='loom-dev',
            namespace_uid=UUID(database.foundation.binding.namespace_uid), controller=copy.deepcopy(controller))
        migration = PoolMigrationRequest(registration=registration, guards=(dev_guard, staging.attachment.guard))
        migration_contract(migration)
        attachment = staging.attachment
        service = staging.originals[f'Deployment:{attachment.guard.namespace}:loom-service']
        wired = wire_participant(request=migration, participant_id=attachment.participant.participant_id,
            management_origin='https://' + database.manager.deployment.public_host,
            actuator=attachment.actuators[0], guest_actuators=attachment.actuators[1:],
            service=service, runtime_profile=attachment.runtime_profile)
        # The old collector remains suspended. The completed dev collector is
        # the sole aggregate observer; do not start a second per-env observer.
        targets = copy.deepcopy(staging.stopped)
        for row in wired.values():
            target = _snapshot(row)
            target['metadata'].setdefault('annotations', {})[MARKER] = str(registration.spec.operation_id)
            targets[_key(row)] = target
        if targets.keys() != staging.originals.keys():
            raise ValueError
        history = dict(legacy.history)
        for path, raw in completed.files.items():
            checksum = hashlib.sha256(raw).hexdigest()
            if path in history and history[path] != checksum:
                raise ValueError
            history[path] = checksum
        if any(hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest() != checksum
                for path, checksum in history.items()):
            raise ValueError
        checksum = digest({'development': development.input_digest, 'staging': staging.input_digest,
            'migration': migration_contract(migration), 'staging_targets': targets,
            'history': {str(path): value for path, value in history.items()}})
        return SharedPoolHandoffPlan(development, staging, migration, targets, history, checksum)
    except Exception:
        raise ValueError('shared pool handoff intent unqualified') from None
