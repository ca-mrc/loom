"""Connected participant lifecycle behind the installed identity-only command.

One participant lock covers the retained parent and its existing child journals.
Every write is performed through the scoped providers or fixed administration
Jobs. A transport retry never changes operation identity, predecessor or grant.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid5

from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_activation import activate_environment_runtime
from scripts.ops.nebius_environment_foundation import (
    FoundationReferenceV2,
    _read,
    _retain,
    prepare_foundation,
    reconcile_foundation,
)
from scripts.ops.nebius_environment_lifecycle_entry import (
    LifecycleCommandV2,
    LifecycleConfigurationV2,
    LifecycleOperationV2,
    _private,
)
from scripts.ops.nebius_environment_lifecycle_inputs import (
    FoundationInputsV2,
    LifecycleClients,
    LifecycleInputsV2,
    lifecycle_clients,
    lifecycle_production_authority,
)
from scripts.ops.nebius_environment_runtime_install import _observe, stage_environment_runtime
from scripts.ops.nebius_environment_runtime_start import start_environment_runtime
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
    _load,
    dispatch_administration,
    retain_authorized_operation,
)
from scripts.ops.nebius_pool_enrollment_issuer import (
    PoolEnrollmentPolicyV2,
    _grant,
    _policy,
    issue_enrollment,
)

from loom_service.environment_management.candidates import _json
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import (
    PoolInspectionSnapshotV2,
    PoolInspectionV2,
    qualify_inspection,
)
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantOperationV2,
    PoolParticipantReadinessV2,
    PoolWaitingCancellationV2,
)


def inspection(value: dict[str, Any]) -> PoolInspectionSnapshotV2:
    return qualify_inspection(value['report']['receipt'], PoolInspectionV2.model_validate(value['input']['request']),
        UUID(value['input']['authorization']['authorization_id']))


async def apply_reference(policy: PoolEnrollmentPolicyV2, reference: PoolAdministrationReferenceV2,
        connect: Connection) -> dict[str, Any]:
    job = _load(policy.host, reference)
    started = policy.host.anchor_root / (str(reference.authorization_id) + '.started.json')
    fresh = job.authorization.issued_at <= datetime.now(UTC) < job.authorization.expires_at
    action = 'recover' if started.exists() and not fresh else 'apply'
    command = dict(schema_version='loom.pool-administration-command.v2', action=action, reference=reference.model_dump(mode='json'))
    return await asyncio.to_thread(dispatch_administration, json.dumps(command).encode(), action=action, host=policy.host, connect=connect)


async def transition_participant(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        action: Literal['suspend', 'resume', 'retire', 'complete_retirement'], expected_head: UUID,
        connect: Connection, cancel_waiting: tuple[PoolWaitingCancellationV2, ...] = (),
        readiness: PoolParticipantReadinessV2 | None = None) -> dict[str, Any]:
    """Issue from a current authenticated read; caller selects the exact head.

    Readiness is internal protected process evidence, never a runner input. The
    public resume path obtains it from the pinned runtime and platform readers.
    Historical issuance is reused, including its original expiry.
    """
    if not operation_id.int or not expected_head.int:
        raise ValueError('environment_lifecycle_operation_unqualified')
    directory = policy.host.state_root / ('lifecycle-transition-' + str(operation_id))
    with state._locked_state(directory):
        intent = dict(policy_sha256=digest(_policy(policy)), operation_id=str(operation_id), action=action,
            expected_head=str(expected_head), cancel_waiting=[row.model_dump(mode='json') for row in cancel_waiting])
        _retain(directory / 'intent.json', intent)
        authorization_id = uuid5(operation_id, 'lifecycle-authorization')
        anchor = policy.host.anchor_root / (str(authorization_id) + '.json')
        if anchor.exists():
            reference = PoolAdministrationReferenceV2.model_validate(_read(anchor))
            retained = _load(policy.host, reference)
            request = retained.request
            if (not isinstance(request, PoolParticipantOperationV2) or request.operation_id != operation_id
                    or request.action != action or request.expected_head != expected_head or request.cancel_waiting != cancel_waiting):
                raise ValueError('environment_lifecycle_issuance_changed')
        else:
            observed = await asyncio.to_thread(_observe, policy, directory / 'observations', connect,
                require_capacity=action == 'resume')
            if observed is None:
                return dict(status='pending', admission_open=None)
            snapshot = inspection(observed)
            selected = snapshot.participant
            if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                    or selected.head.operation_id != expected_head):
                raise ValueError('environment_lifecycle_predecessor_changed')
            request = PoolParticipantOperationV2(schema_version='loom.pool-participant-operation.v2', operation_id=operation_id,
                pool_id=policy.participant.pool_id, participant_id=policy.participant.participant_id, incarnation=policy.participant.incarnation,
                expected_head=expected_head, expected_binding_sha256=selected.head.binding_sha256,
                pool_authority_sha256=snapshot.pool_authority_sha256, action=action, readiness=readiness, cancel_waiting=cancel_waiting)
            expires = min(snapshot.valid_until, readiness.valid_until) if readiness else snapshot.valid_until
            grant = _grant(policy, request, authorization_id, issued_at=datetime.now(UTC), expires_at=expires)
            reference = retain_authorized_operation(host=policy.host, request=request, authorization=grant,
                evidence_sha256=digest(observed))
        report = await apply_reference(policy, reference, connect)
        if report['status'] != 'complete':
            return dict(status=report['status'], admission_open=None)
        # A retained success is historical; do not present it as a current state
        # after a later transition or replacement incarnation.
        observed = await asyncio.to_thread(_observe, policy, directory / 'current', connect)
        if observed is None:
            return dict(status='pending', admission_open=None)
        snapshot = inspection(observed)
        selected = snapshot.participant
        expected = {'suspend': 'suspended', 'resume': 'active', 'retire': 'retiring', 'complete_retirement': 'retired'}[action]
        if selected is None or selected.head.operation_id != operation_id or selected.head.state != expected:
            raise ValueError('environment_lifecycle_current_head_changed')
        return dict(status='complete', admission_open=action == 'resume' and snapshot.pool.mode == 'global', reference=reference.model_dump(mode='json'))


async def resume_environment(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID, transition_id: UUID,
        predecessor: PoolAdministrationReferenceV2, **clients: Any) -> dict[str, Any]:
    from scripts.ops.nebius_environment_activation_issuer import _recheck_runtime
    from scripts.ops.nebius_environment_runtime_readiness import observe_environment_pre_activation
    from scripts.ops.nebius_environment_runtime_start import load_started_environment_runtime

    boundary_connection = clients.pop('boundary_connection', None)

    original = _load(policy.host, predecessor).request
    if not isinstance(original, PoolParticipantOperationV2) or original.action != 'suspend':
        raise ValueError('environment_resume_predecessor_unqualified')
    # Once issued, recover only the original fixed Job, without generating fresh
    # readiness against the already-resumed successor.
    anchor = policy.host.anchor_root / (str(uuid5(transition_id, 'lifecycle-authorization')) + '.json')
    readiness = None
    if not anchor.exists():
        evidence = await observe_environment_pre_activation(policy=policy, operation_id=operation_id,
            _readiness_predecessor=predecessor, **clients)
        if evidence is None:
            return dict(status='pending', admission_open=None)
        snapshot = inspection(evidence['inspection'])
        selected = snapshot.participant
        if selected is None:
            raise ValueError('environment_resume_participant_missing')
        readiness = PoolParticipantReadinessV2.model_validate(evidence['platform']['readiness'] | {
            'valid_until': evidence['valid_until'], 'participant_binding_sha256': selected.head.binding_sha256,
            'catalog_sha256': selected.catalog_sha256})
        def qualify() -> None:
            if datetime.now(UTC) >= readiness.valid_until or _load(policy.host, predecessor).request != original:
                raise ValueError('environment_resume_evidence_changed')
        started = load_started_environment_runtime(policy=policy, operation_id=operation_id)
        await _recheck_runtime(policy=policy, started=started,
            platform_policy=clients['platform_policy'], runtime=evidence, qualify=qualify, kubernetes=clients['kubernetes'],
            probe_connection=clients.get('probe_connection'), platform_probe_connection=clients.get('platform_probe_connection'))
        if not await asyncio.to_thread(resume_build_policy_ready, policy=policy, resources=started.resources,
                namespace_uids=started.staged.plan['namespace_uids'], qualify=qualify, boundary_connection=boundary_connection):
            return dict(status='pending', admission_open=False)
    return await transition_participant(policy=policy, operation_id=transition_id, action='resume',
        expected_head=predecessor.operation_id, connect=clients['connect'], readiness=readiness)


def resume_build_policy_ready(*, policy: PoolEnrollmentPolicyV2, resources: dict[str, dict[str, Any]],
        namespace_uids: dict[str, str], qualify: Callable[[], None], boundary_connection: Any = None) -> bool:
    """Recheck compiled native admission while existing charged work continues."""
    from scripts.ops.nebius_environment_boundary_live import EnvironmentBoundaryWorkloads
    from scripts.ops.nebius_environment_runtime_install import _resources

    from loom_service.pool_management.profiles import _ApplicationImage, _TaskImage

    if boundary_connection is not None:
        with boundary_connection(resources, {}, namespace_uids, qualify) as boundary:
            return bool(boundary.policy_ready())
    profiles: tuple[_TaskImage | _ApplicationImage, ...] = (*policy.profiles.task_images, *policy.profiles.application_images)
    with _resources(policy, resources, namespace_uids, qualify) as api:
        boundary = EnvironmentBoundaryWorkloads(api, resources, {}, policy.host.runtime.region,
            {key: doc for key, doc in resources.items() if doc['kind'] == 'Deployment'
                and doc['metadata']['namespace'] == policy.participant.execution_namespace.name},
            build_policy={key: doc for key, doc in resources.items() if doc['kind'] in {
                'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}},
            build_profile=profiles[0] if profiles else None)
        return boundary.policy_ready()


async def retire_environment(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID, expected_head: UUID,
        connect: Connection, cancel_waiting: tuple[PoolWaitingCancellationV2, ...] = ()) -> dict[str, Any]:
    """Close admission, drain selected work and retain the completed retirement.

    Waiting requests must be named explicitly for cancellation. Running work
    keeps its original frozen profile and resources until it releases its charge.
    """
    directory = policy.host.state_root / ('environment-retirement-' + str(operation_id))
    anchor = policy.host.anchor_root / ('environment-retirement-' + str(operation_id) + '.json')
    intent = dict(policy_sha256=digest(_policy(policy)), operation_id=str(operation_id), expected_head=str(expected_head),
        cancel_waiting=[row.model_dump(mode='json') for row in cancel_waiting])
    with state._locked_state(directory):
        path = directory / 'plan.json'
        if path.exists():
            plan = _read(path)
            if plan.get('intent') != intent or plan.get('initial_state') not in {'enrolled', 'ready', 'active', 'suspended'}:
                raise ValueError('environment_retirement_history_changed')
        else:
            if anchor.exists():
                raise ValueError('environment_retirement_history_missing')
            observed = await asyncio.to_thread(_observe, policy, directory / 'initial', connect)
            if observed is None:
                return dict(status='pending', admission_open=None)
            selected = inspection(observed).participant
            if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                    or selected.head.operation_id != expected_head
                    or selected.head.state not in {'enrolled', 'ready', 'active', 'suspended'}):
                raise ValueError('environment_retirement_predecessor_changed')
            plan = dict(intent=intent, initial_state=selected.head.state, observation=observed)
            _retain(path, plan)
        _retain(anchor, {'plan_sha256': digest(plan)})
        phases: list[tuple[Literal['suspend', 'retire', 'complete_retirement'], UUID]] = []
        if plan['initial_state'] == 'active':
            phases.append(('suspend', uuid5(operation_id, 'suspend')))
        phases.extend([('retire', uuid5(operation_id, 'retire')), ('complete_retirement', operation_id)])
        predecessor = expected_head
        for index, (action, identity) in enumerate(phases):
            issuance = policy.host.anchor_root / (str(uuid5(identity, 'lifecycle-authorization')) + '.json')
            successor_issued = index + 1 < len(phases) and (policy.host.anchor_root /
                (str(uuid5(phases[index + 1][1], 'lifecycle-authorization')) + '.json')).exists()
            cancellations = cancel_waiting if action == 'retire' else ()
            if successor_issued:
                reference = PoolAdministrationReferenceV2.model_validate(_read(issuance))
                request = _load(policy.host, reference).request
                if (not isinstance(request, PoolParticipantOperationV2) or request.action != action
                        or request.operation_id != identity or request.expected_head != predecessor
                        or request.cancel_waiting != cancellations):
                    raise ValueError('environment_retirement_history_changed')
            else:
                if action == 'complete_retirement' and not issuance.exists():
                    observed = await asyncio.to_thread(_observe, policy, directory / 'drain', connect,
                        include_workload_counts=True)
                    if observed is None:
                        return dict(status='pending', admission_open=False)
                    selected = inspection(observed).participant
                    if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                            or selected.head.operation_id != predecessor or selected.head.state != 'retiring'
                            or selected.workload_counts is None):
                        raise ValueError('environment_retirement_drain_unqualified')
                    if selected.workload_counts.charged or selected.workload_counts.waiting:
                        return dict(status='pending', admission_open=False)
                transition = await transition_participant(policy=policy, operation_id=identity, action=action,
                    expected_head=predecessor, connect=connect, cancel_waiting=cancellations)
                if transition['status'] != 'complete':
                    return transition
            predecessor = identity
        return dict(status='complete', admission_open=False)


async def resume_released_environment(*, policy: PoolEnrollmentPolicyV2, transition_id: UUID,
        predecessor: PoolAdministrationReferenceV2, release_id: UUID | None = None,
        runtime_operation_id: UUID | None = None, **clients: Any) -> dict[str, Any]:
    """Resume a retained release or imported baseline without fresh-install history."""
    from scripts.ops.nebius_environment_foundation import _load as load_foundation
    from scripts.ops.nebius_environment_release import resolve_completed_release_policy
    from scripts.ops.nebius_environment_release_readiness import release_readiness
    from scripts.ops.nebius_environment_runtime_start import load_started_environment_runtime

    if release_id is not None:
        selected_policy, plan, resources, files = resolve_completed_release_policy(policy=policy, release_id=release_id)
        runtime_id = UUID(plan['selection']['runtime_operation_id'])
    else:
        from scripts.ops.nebius_environment_installed_import import (
            load_imported_environment_runtime,
        )

        if runtime_operation_id is None:
            raise ValueError('environment_resume_imported_runtime_missing')
        selected_policy, runtime_id = policy, runtime_operation_id
        imported = load_imported_environment_runtime(policy=policy, operation_id=runtime_id)
        resources, files = imported.resources, imported.files
    original = _load(policy.host, predecessor).request
    if (not isinstance(original, PoolParticipantOperationV2) or original.action != 'suspend'
            or original.expected_binding_sha256 != digest(selected_policy.participant.model_dump(mode='json'))):
        raise ValueError('environment_resume_predecessor_unqualified')
    anchor = policy.host.anchor_root / (str(uuid5(transition_id, 'lifecycle-authorization')) + '.json')
    readiness = None
    if not anchor.exists():
        if load_foundation(policy.foundation_dir, policy.foundation)['mode'] == 'legacy-import':
            from scripts.ops.nebius_environment_installed_import import (
                load_imported_environment_runtime,
            )

            started = load_imported_environment_runtime(policy=policy, operation_id=runtime_id)
        else:
            started = load_started_environment_runtime(policy=policy, operation_id=runtime_id)
        files.update(started.files)
        directory = policy.host.state_root / ('release-resume-' + str(transition_id))
        observed = await asyncio.to_thread(_observe, selected_policy, directory, clients['connect'], require_capacity=True,
            platform_authorization_id=clients['platform_policy'].authority.authorization_id)
        if observed is None:
            return dict(status='pending', admission_open=None)
        snapshot = inspection(observed)
        participant = snapshot.participant
        if (participant is None or participant.binding != selected_policy.participant or participant.profiles != selected_policy.profiles
                or participant.head.operation_id != predecessor.operation_id or participant.head.state != 'suspended'):
            raise ValueError('environment_resume_predecessor_changed')
        def qualify() -> None:
            if (any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items())
                    or _load(policy.host, predecessor).request != original):
                raise ValueError('environment_resume_history_changed')
        evidence = await release_readiness(policy=selected_policy, snapshot=snapshot, resources=resources,
            namespace_uids=started.staged.plan['namespace_uids'], foundation_identities=started.staged.plan['foundation_identities'],
            qualify=qualify, **{key: clients[key] for key in ('platform_policy', 'kubernetes', 'cloud', 'public_http')},
            probe_connection=clients.get('probe_connection'), platform_probe_connection=clients.get('platform_probe_connection'))
        readiness = PoolParticipantReadinessV2.model_validate(evidence['readiness'])
        qualify()
        if not await asyncio.to_thread(resume_build_policy_ready, policy=selected_policy, resources=resources,
                namespace_uids=started.staged.plan['namespace_uids'], qualify=qualify,
                boundary_connection=clients.get('boundary_connection')):
            return dict(status='pending', admission_open=False)
    return await transition_participant(policy=selected_policy, operation_id=transition_id, action='resume',
        expected_head=predecessor.operation_id, connect=clients['connect'], readiness=readiness)


async def execute_lifecycle(*, configuration: LifecycleConfigurationV2, request: LifecycleCommandV2,
        operation: LifecycleOperationV2, unchanged: Callable[[], Any]) -> dict[str, Any]:
    if operation.kind in {'provision', 'import'}:
        foundation_inputs = FoundationInputsV2.model_validate(_json(_private(operation.inputs_path)))
        return await execute_foundation_lifecycle(configuration=configuration, request=request, operation=operation,
            inputs=foundation_inputs, unchanged=unchanged)
    inputs = LifecycleInputsV2.model_validate(_json(_private(operation.inputs_path)))
    policy = inputs.enrollment.policy()
    if request.scope != configuration.scope or policy.host.scope != request.scope:
        raise ValueError('environment_lifecycle_scope_changed')
    if not inputs.runtime_operation_id.int or not inputs.enrollment_operation_id.int:
        raise ValueError('environment_lifecycle_child_identity_unqualified')
    if inputs.current_release_id is not None and not inputs.current_release_id.int:
        raise ValueError('environment_lifecycle_release_identity_unqualified')
    inputs.private_files()
    result = dict(schema_version='loom.environment-lifecycle-result.v2', action=request.action,
        operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'),
        phase=operation.kind, status='prepared')
    if request.action == 'prepare':
        unchanged()
        return result
    async with lifecycle_clients(inputs, policy, unchanged) as clients, lifecycle_production_authority(inputs, clients):
        return await execute_connected_lifecycle(request=request, operation=operation, inputs=inputs, policy=policy,
            clients=clients, unchanged=unchanged)


async def execute_foundation_lifecycle(*, configuration: LifecycleConfigurationV2, request: LifecycleCommandV2,
        operation: LifecycleOperationV2, inputs: FoundationInputsV2, unchanged: Callable[[], Any]) -> dict[str, Any]:
    """Provision or adopt a foundation before selecting its observed namespace UIDs."""
    host = inputs.host_policy()
    if host.scope != request.scope or configuration.scope != request.scope:
        raise ValueError('environment_lifecycle_scope_changed')
    if operation.kind == 'provision':
        if inputs.legacy_foundation is not None or inputs.declaration.operation_id != request.operation_id:
            raise ValueError('environment_lifecycle_provision_identity_changed')
    elif operation.kind == 'import':
        if inputs.legacy_foundation is None:
            raise ValueError('environment_lifecycle_import_source_missing')
    else:
        raise ValueError('environment_lifecycle_foundation_operation_invalid')
    inputs.private_files()
    result = dict(schema_version='loom.environment-lifecycle-result.v2', action=request.action,
        operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'), phase=operation.kind)
    if request.action == 'prepare':
        unchanged()
        return dict(result, status='prepared')
    async with lifecycle_clients(inputs, None, unchanged) as clients, lifecycle_production_authority(inputs, clients):
        state._private_directory(host.state_root)
        state._private_directory(host.anchor_root)
        with state._locked_state(host.state_root / ('lifecycle-' + str(request.scope.participant_id))):
            directory = host.state_root / ('lifecycle-operation-' + str(request.operation_id))
            anchor = host.anchor_root / ('lifecycle-' + str(request.operation_id) + '.json')
            if request.action == 'recover' and not anchor.exists():
                raise ValueError('environment_lifecycle_recovery_missing')
            state._private_directory(directory)
            reference_path = directory / 'foundation-reference.json'
            body = dict(operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'),
                operation=operation.model_dump(mode='json'), inputs_sha256=digest(inputs.model_dump(mode='json')))
            if request.action == 'status':
                if not anchor.exists() or not reference_path.exists():
                    return dict(result, status='pending')
                if (_read(directory / 'intent.json') != body or _read(anchor) != {'intent_sha256': digest(body)}):
                    raise ValueError('environment_lifecycle_history_changed')
                reference = FoundationReferenceV2.model_validate(_read(reference_path))
                await reconcile_foundation(state_dir=inputs.foundation_dir, reference=reference,
                    kubernetes=clients.kubernetes, cloud=clients.cloud, observe_only=True)
                unchanged()
                return dict(result, status='observed')
            if anchor.exists() and not (directory / 'intent.json').exists():
                raise ValueError('environment_lifecycle_history_missing')
            _retain(directory / 'intent.json', body)
            _retain(anchor, {'intent_sha256': digest(body)})
            bundle = await clients.catalog.resolve(inputs.application_publication.candidate_id)
            unchanged()
            if operation.kind == 'provision':
                reference = prepare_foundation(state_dir=inputs.foundation_dir, declaration=inputs.declaration,
                    candidate=bundle.candidate, profile=bundle.profile, keyring=inputs.image_admission_keyring,
                    repo_root=Path(__file__).resolve().parents[2])
            else:
                from scripts.ops.nebius_environment_installed_import import (
                    InstalledLegacyFoundationV2,
                    prepare_installed_legacy_foundation_import,
                )

                if inputs.legacy_foundation is None:
                    raise ValueError('environment_lifecycle_import_source_missing')
                source = InstalledLegacyFoundationV2.model_validate(_json(inputs.legacy_foundation.read()))
                if source.declaration != inputs.declaration or source.publication != inputs.application_publication:
                    raise ValueError('environment_lifecycle_import_source_changed')
                reference = prepare_installed_legacy_foundation_import(state_dir=inputs.foundation_dir,
                    source=source, operation_id=request.operation_id, candidate=bundle.candidate, profile=bundle.profile)
            _retain(reference_path, reference.model_dump(mode='json'))
            observed = await reconcile_foundation(state_dir=inputs.foundation_dir, reference=reference,
                kubernetes=clients.kubernetes, cloud=clients.cloud)
            unchanged()
            return dict(result, status='complete', **({'admission_open': False} if observed.execution_enabled is False else {}))


async def execute_connected_lifecycle(*, request: LifecycleCommandV2, operation: LifecycleOperationV2,
        inputs: LifecycleInputsV2, policy: PoolEnrollmentPolicyV2, clients: LifecycleClients,
        unchanged: Callable[[], Any]) -> dict[str, Any]:
    """Shared installed parent, injectable only at authenticated remote boundaries."""
    result = dict(schema_version='loom.environment-lifecycle-result.v2', action=request.action,
        operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'), phase=operation.kind)
    baseline = policy
    if inputs.current_release_id is not None:
        if operation.kind not in {'suspend', 'resume', 'retire'}:
            raise ValueError('environment_lifecycle_current_release_not_allowed')
        from scripts.ops.nebius_environment_release import resolve_completed_release_policy

        policy, _, _, _ = resolve_completed_release_policy(policy=baseline, release_id=inputs.current_release_id)
    root = policy.host.state_root
    state._private_directory(root)
    # Configurations for this participant share the same operator-pinned state
    # root. The fixed transaction also serializes admission against peer demand.
    with state._locked_state(root / ('lifecycle-' + str(policy.participant.participant_id))):
        directory = root / ('lifecycle-operation-' + str(request.operation_id))
        anchor = policy.host.anchor_root / ('lifecycle-' + str(request.operation_id) + '.json')
        state._private_directory(policy.host.anchor_root)
        if request.action == 'recover' and not anchor.exists():
            raise ValueError('environment_lifecycle_recovery_missing')
        state._private_directory(directory)
        if request.action == 'status':
            observed = await asyncio.to_thread(_observe, policy, directory / 'status', clients.connect)
            unchanged()
            if observed is None:
                return dict(result, status='pending', phase='status')
            snapshot = inspection(observed)
            selected = snapshot.participant
            return dict(result, status='observed', phase='status',
                admission_open=snapshot.pool.mode == 'global' and selected is not None and selected.head.state == 'active')
        body = dict(operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'),
            operation=operation.model_dump(mode='json'), inputs_sha256=digest(inputs.model_dump(mode='json')))
        intent = directory / 'intent.json'
        if anchor.exists() and not intent.exists():
            raise ValueError('environment_lifecycle_history_missing')
        _retain(intent, body)
        _retain(anchor, {'intent_sha256': digest(body)})
        unchanged()
        if operation.kind in {'suspend', 'retire'}:
            if inputs.expected_head is None:
                raise ValueError('environment_lifecycle_predecessor_missing')
            if operation.kind == 'retire':
                transition = await retire_environment(policy=policy, operation_id=request.operation_id,
                    expected_head=inputs.expected_head, cancel_waiting=inputs.cancel_waiting, connect=clients.connect)
            else:
                transition = await transition_participant(policy=policy, operation_id=request.operation_id,
                    action='suspend', expected_head=inputs.expected_head, connect=clients.connect)
            unchanged()
            return dict(result, **{key: transition[key] for key in ('status', 'admission_open')})
        if operation.kind == 'resume':
            if inputs.predecessor is None or inputs.platform is None:
                raise ValueError('environment_lifecycle_resume_inputs_missing')
            from scripts.ops.nebius_environment_foundation import _load as load_foundation

            if inputs.current_release_id is not None or load_foundation(policy.foundation_dir, policy.foundation)['mode'] == 'legacy-import':
                transition = await resume_released_environment(policy=baseline, release_id=inputs.current_release_id,
                    transition_id=request.operation_id, predecessor=inputs.predecessor,
                    runtime_operation_id=inputs.runtime_operation_id,
                    platform_policy=inputs.platform.policy(), **clients.common())
            else:
                transition = await resume_environment(policy=policy, operation_id=inputs.runtime_operation_id,
                    transition_id=request.operation_id, predecessor=inputs.predecessor,
                    platform_policy=inputs.platform.policy(), **clients.common())
            unchanged()
            return dict(result, **{key: transition[key] for key in ('status', 'admission_open')})
        if operation.kind in {'release', 'rollback'}:
            from scripts.ops.nebius_environment_release import release_environment_runtime

            if (inputs.release is None or inputs.platform is None
                    or inputs.release.runtime_operation_id != inputs.runtime_operation_id
                    or (operation.kind == 'rollback') != (inputs.release.rollback_to is not None)):
                raise ValueError('environment_lifecycle_release_inputs_missing')
            released = await release_environment_runtime(policy=policy, operation_id=request.operation_id,
                replacement=inputs.release, platform_policy=inputs.platform.policy(), **clients.common())
            unchanged()
            return dict(result, **{key: released[key] for key in ('status', 'admission_open')})
        if operation.kind not in {'enroll', 'activate'}:
            raise ValueError('environment_lifecycle_operation_not_qualified')
        cursor_path = directory / 'cursor.json'
        if cursor_path.exists():
            cursor = _read(cursor_path)
        elif operation.kind == 'activate' and inputs.predecessor is not None:
            # A new attempt after ready reuses the original completed startup.
            # Re-staging would demand an enrolled head and strand an expired
            # ready/activate attempt even though its runtime is already running.
            from scripts.ops.nebius_environment_runtime_start import (
                load_started_environment_runtime,
            )

            retained = load_started_environment_runtime(policy=policy, operation_id=inputs.runtime_operation_id)
            enrollment = PoolAdministrationReferenceV2.model_validate(retained.staged.plan['enrollment'])
            ready = _load(policy.host, inputs.predecessor).request
            if (not isinstance(ready, PoolParticipantOperationV2) or ready.action != 'ready'
                    or ready.expected_head != enrollment.operation_id or enrollment.operation_id != inputs.enrollment_operation_id):
                raise ValueError('environment_lifecycle_ready_predecessor_unqualified')
            cursor = {'enroll': enrollment.model_dump(mode='json'), 'stage': True, 'start': True}
            state._atomic_json(cursor_path, cursor)
        else:
            cursor = {'enroll': None, 'stage': False, 'start': False}
        if set(cursor) != {'enroll', 'stage', 'start'} or (cursor['start'] and not cursor['stage']):
            raise ValueError('environment_lifecycle_history_changed')
        if cursor['enroll'] is None:
            reference = await issue_enrollment(policy=policy, operation_id=inputs.enrollment_operation_id, **clients.common())
            unchanged()
            if reference is None:
                return dict(result, status='pending', phase='enroll')
            report = await apply_reference(policy, reference, clients.connect)
            if report['status'] != 'complete':
                return dict(result, status=report['status'], phase='enroll')
            cursor['enroll'] = reference.model_dump(mode='json')
            state._atomic_json(cursor_path, cursor)
        enrollment = PoolAdministrationReferenceV2.model_validate(cursor['enroll'])
        if operation.kind == 'enroll':
            observed = await asyncio.to_thread(_observe, policy, directory / 'enrolled-current', clients.connect)
            unchanged()
            if observed is None:
                return dict(result, status='pending')
            selected = inspection(observed).participant
            if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                    or selected.head.operation_id != enrollment.operation_id or selected.head.state != 'enrolled'):
                raise ValueError('environment_lifecycle_enrollment_head_changed')
            return dict(result, status='complete', admission_open=False)
        if inputs.platform is None:
            raise ValueError('environment_lifecycle_platform_missing')
        if not cursor['stage']:
            if inputs.token is None or inputs.registry_scope is None or inputs.registry_credential is None:
                raise ValueError('environment_lifecycle_runtime_material_missing')
            staged = await stage_environment_runtime(policy=policy, enrollment=enrollment, operation_id=inputs.runtime_operation_id,
                token=inputs.token.read().decode(), registry_scope=inputs.registry_scope,
                registry_credential=inputs.registry_credential.read(), registry_sdk=clients.registry_sdk, **clients.common())
            unchanged()
            if staged is None or staged['status'] != 'runtime_staged_closed':
                return dict(result, status='pending', phase='start')
            cursor['stage'] = True
            state._atomic_json(cursor_path, cursor)
        if not cursor['start']:
            started = await start_environment_runtime(policy=policy, operation_id=inputs.runtime_operation_id, **clients.common())
            unchanged()
            if started is None or started['status'] != 'runtime_started_closed':
                return dict(result, status='pending', phase='start')
            cursor['start'] = True
            state._atomic_json(cursor_path, cursor)
        active = await activate_environment_runtime(policy=policy, operation_id=inputs.runtime_operation_id,
            activation_id=request.operation_id, platform_policy=inputs.platform.policy(), predecessor=inputs.predecessor,
            **clients.common())
        unchanged()
        status = 'complete' if active['status'] == 'environment_active' else (
            'blocked' if active['status'] == 'environment_activation_blocked' else 'pending')
        return dict(result, status=status, admission_open=active.get('admission_open'))
