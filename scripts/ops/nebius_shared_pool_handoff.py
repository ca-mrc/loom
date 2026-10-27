"""Forward handoff from current staging into the completed closed dev pool.

These projections qualify source and retained history, not live writer ownership.
The protected connected parent must establish sole Kubernetes/provider writers,
guard/drain and exact recovery evidence before consuming a transition. Old
registration, retirement and rollback stages are never replayed here.
"""
from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_activation import (
    DevelopmentActivationPlan,
    prepare_development_activation,
)
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_refresh_predecessor import CompletedRefresh, CompletedUpgrade
from scripts.ops.nebius_pool_material import machine_documents
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest, registration_documents
from scripts.ops.nebius_pool_runtime import participant_readonly_roles, wire_participant
from scripts.ops.nebius_staging_pool_attachment import (
    StagingPoolAttachment,
    prepare_staging_pool_attachment,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.pool_management.installation import PoolInstallation

if TYPE_CHECKING:
    from scripts.ops.nebius_development_schema_upgrade import CompletedDevelopmentSchemaSuccessor
    from scripts.ops.nebius_pool_preactivation_operation import CompletedPreactivationCatalog
    from scripts.ops.nebius_pool_storage_operation import CompletedClosedStorage

MARKER = 'loom.nebius/shared-pool-handoff-operation'
_LEGACY_MARKERS = {'loom.nebius/pool-retirement-operation', 'loom.nebius/pool-cutover-operation',
    'loom.nebius/pool-startup-operation'}


def retired_manager_revision(legacy: CompletedPoolCutover) -> str:
    """Select the completed migration, not a guessed current database revision.

    Callers qualify this completion chain before and after observing its old
    manager. A completed refresh pins its target schema. Initial upgrades retain
    the existing exact-0176 observation contract; this is no version fallback or
    authority to start/migrate a historical manager.
    """
    if legacy.completion.outcome != 'legacy':
        raise ValueError('historical manager requires completed legacy outcome')
    predecessor = legacy.context.predecessor
    if isinstance(predecessor, CompletedRefresh):
        return predecessor.resources.target_manager_revision
    if isinstance(predecessor, CompletedUpgrade):
        return '0176'
    raise ValueError('historical manager predecessor unqualified')


class StagingPoolSource(BaseModel):
    """Pinned selection, not approval; the connected parent resolves publication."""

    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-staging-pool-source.v1']
    publication: ProtectedPublication
    candidate: dict[str, Any]
    observation: dict[str, Any]


@dataclass(frozen=True, repr=False)
class StagingForwardPlan:
    attachment: StagingPoolAttachment
    legacy: CompletedPoolCutover
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    input_digest: str
    source: StagingPoolSource | None = None


@dataclass(frozen=True, repr=False)
class SharedPoolHandoffPlan:
    development: DevelopmentActivationPlan
    staging: StagingForwardPlan
    migration: PoolMigrationRequest
    staging_targets: dict[str, dict[str, Any]]
    staging_material: dict[str, dict[str, Any]]
    staging_role_originals: dict[str, dict[str, Any]]
    staging_role_targets: dict[str, dict[str, Any]]
    staging_reader_authority: dict[str, dict[str, Any]]
    history: dict[Path, str]
    input_digest: str
    preactivation: CompletedPreactivationCatalog | None = None
    storage: CompletedClosedStorage | None = None

    @property
    def effective_runtime_resources(self) -> dict[str, dict[str, Any]]:
        """Current catalog/manager overlay; original installation stays intact."""
        resources = copy.deepcopy(self.development.selection.resources)
        if self.storage is not None and self.development.schema_successor is None:
            for row in (self.storage.resources.configuration, self.storage.resources.collector):
                resources[_key(row)] = copy.deepcopy(row)
        if self.preactivation is not None:
            for row in (self.preactivation.resources.manager, self.preactivation.resources.catalog):
                resources[_key(row)] = copy.deepcopy(row)
        return resources


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
        source = None
        candidate = registration.candidate
        if observation.get('schema_version') == 'loom.nebius-staging-pool-source.v1':
            source = StagingPoolSource.model_validate(copy.deepcopy(observation))
            candidate, observation = source.candidate, source.observation
            if (not source.publication.candidate_id.int
                    or source.publication.source_sha != candidate.get('candidate_sha')
                    or source.publication.run_id != candidate.get('run_id')):
                raise ValueError
        attachment = prepare_staging_pool_attachment(participant=participant, observation=observation,
            candidate=candidate, cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, repo_root=repo_root)
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
            'originals': originals, 'stopped': stopped,
            **({'staging_source': source.model_dump(mode='json')} if source is not None else {})})
        return StagingForwardPlan(attachment, legacy, originals, stopped, checksum, source)
    except Exception:
        raise ValueError('shared pool staging predecessor unqualified') from None


def prepare_shared_pool_handoff(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        preactivation: CompletedPreactivationCatalog | None = None,
        schema_successor: CompletedDevelopmentSchemaSuccessor | None = None,
        storage: CompletedClosedStorage | None = None) -> SharedPoolHandoffPlan:
    """Compose completed CLOSED dev and current staging without replaying either.

    The complete migration contract is used only for fixed staging projection.
    Fresh dev is already installed, not a standalone writer to retire or register
    again. No guard, grant, workload transition or SQL opening is performed here.
    The protected parent still owns live sole-writer and provider qualification.
    """
    try:
        from scripts.ops.nebius_pool_storage_operation import resolve_runtime_storage

        development = prepare_development_activation(runtime, schema_successor=schema_successor)
        completed = development.runtime
        database = completed.request.database
        registration = development.selection.registration
        storage = resolve_runtime_storage(runtime=runtime, preactivation=preactivation,
            schema_successor=schema_successor, storage=storage)
        if storage is not None and schema_successor is None:
            registration = storage.retained.registration
        if preactivation is not None:
            from scripts.ops.nebius_pool_preactivation_operation import load_completed_preactivation

            selected = load_completed_preactivation(runtime=preactivation.retained.inputs.runtime,
                reference=preactivation.retained.reference, repo_root=repo_root)
            if (selected != preactivation or selected.retained.runtime != runtime
                    or selected.retained.inputs.schema_successor != (None if schema_successor is None else schema_successor.reference)):
                raise ValueError
            registration = selected.retained.registration
        participant = development.selection.participant
        # The completed dev runtime retains its own verified registration source.
        # Staging can independently retain a different approved publication.
        if (development.selection.manager_publication.bundle.candidate != registration.candidate
                or len(registration.spec.participants) != 2):
            raise ValueError
        staging = prepare_staging_forward(registration=registration, observation=observation,
            legacy=legacy, repo_root=repo_root)
        controller = development.selection.resources['Deployment:loom-dev:loom-control-plane']
        dev_guard = PoolGuardTarget(participant_id=participant.participant_id, namespace='loom-dev',
            namespace_uid=UUID(database.foundation.binding.namespace_uid), controller=copy.deepcopy(controller))
        migration = PoolMigrationRequest(registration=registration, guards=(dev_guard, staging.attachment.guard))
        migration_contract(migration)
        attachment = staging.attachment
        service = staging.originals[f'Deployment:{attachment.guard.namespace}:loom-service']
        # Pure projection only: never use this candidate selection for catalog,
        # guard identity, database mutation or recovery authority.
        projection = migration if staging.source is None else replace(migration,
            registration=replace(registration, candidate=staging.source.candidate))
        wired = wire_participant(request=projection, participant_id=attachment.participant.participant_id,
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
        # Deliver only the staging participant's new immutable machine Secret.
        # Dev/manager material was already delivered by completed installation;
        # never replay that stage or adopt the old pool's revoked credentials.
        namespaces = {attachment.guard.namespace, attachment.participant.execution_namespace.name}
        material = {key: row for key, row in machine_documents(migration, database.manager.retained.intent.tokens).items()
            if row['metadata']['namespace'] in namespaces}
        if len(material) != 2 or {row['metadata']['namespace'] for row in material.values()} != namespaces:
            raise ValueError
        originals, role_targets, readers = {}, {}, {}
        retained = {_key(row): row for row in attachment.resources}
        staged_namespaces = {attachment.participant.execution_namespace.name, attachment.participant.build_namespace.name}
        reader_name = 'loom-pool-reader-' + attachment.participant.participant_id.hex
        for row in participant_readonly_roles(request=migration):
            key, kind = _key(row), row['kind']
            if kind in {'ClusterRole', 'ClusterRoleBinding'}:
                if row['metadata']['name'] == reader_name:
                    readers[key] = copy.deepcopy(row)
                continue
            if row['metadata']['namespace'] not in staged_namespaces:
                continue
            original = retained[key]
            _uid(original)
            if MARKER in original['metadata'].get('annotations', {}):
                raise ValueError
            if kind == 'RoleBinding':
                # Preserve the qualified existing binding and its metadata.
                if any(original[field] != row[field] for field in ('subjects', 'roleRef')):
                    raise ValueError
            elif kind == 'Role':
                desired = _snapshot(original)
                desired['rules'] = copy.deepcopy(row['rules'])
                desired['metadata'].setdefault('annotations', {})[MARKER] = str(registration.spec.operation_id)
                originals[key], role_targets[key] = copy.deepcopy(original), desired
            else:
                raise ValueError
        if len(originals) != 2 or len(readers) != 2:
            raise ValueError
        history = dict(legacy.history)
        catalog_files = {} if schema_successor is None else dict(schema_successor.files)
        if preactivation is not None:
            if any(path in catalog_files and catalog_files[path] != raw for path, raw in preactivation.files.items()):
                raise ValueError
            catalog_files.update(preactivation.files)
        if storage is not None:
            if any(path in catalog_files and catalog_files[path] != raw for path, raw in storage.files.items()):
                raise ValueError
            catalog_files = {**storage.files, **catalog_files}
        if any(path in completed.files and completed.files[path] != raw for path, raw in catalog_files.items()):
            raise ValueError
        for path, raw in {**completed.files, **catalog_files}.items():
            checksum = hashlib.sha256(raw).hexdigest()
            if path in history and history[path] != checksum:
                raise ValueError
            history[path] = checksum
        if any(hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest() != checksum
                for path, checksum in history.items()):
            raise ValueError
        checksum = digest({'development': development.input_digest, 'staging': staging.input_digest,
            'migration': migration_contract(migration), 'staging_targets': targets,
            'staging_material': material, 'staging_role_originals': originals, 'staging_role_targets': role_targets,
            'staging_reader_authority': readers,
            'history': {str(path): value for path, value in history.items()}})
        return SharedPoolHandoffPlan(development, staging, migration, targets, material, originals, role_targets,
            readers, history, checksum, preactivation, storage)
    except Exception:
        raise ValueError('shared pool handoff intent unqualified') from None
