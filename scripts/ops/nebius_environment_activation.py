"""Protected activation parent for one explicitly selected environment runtime.

Clients/policy come from protected delivery, not runner JSON. Startup is already
complete. Fixed child identities and retained grants connect selected boundaries,
ready, active and fresh current-head readback, without restarting peers or loading
legacy staging history. Active admission is not task execution acceptance.
"""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid5

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_activation_issuer import issue_environment_activation
from scripts.ops.nebius_environment_foundation import FoundationCloud, _read, _retain
from scripts.ops.nebius_environment_runtime_boundaries import (
    BoundaryConnection,
    load_environment_boundary_progress,
    prepare_environment_boundaries,
)
from scripts.ops.nebius_environment_runtime_install import _observe
from scripts.ops.nebius_environment_runtime_readiness import ProbeConnection
from scripts.ops.nebius_environment_runtime_start import load_started_environment_runtime
from scripts.ops.nebius_platform_runtime_readiness import PlatformRuntimePolicyV2
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
    _load,
    dispatch_administration,
)
from scripts.ops.nebius_pool_administration_job import validate_administration_report
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2, _policy, _scope

from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection
from loom_service.pool_management.lifecycle_contracts import PoolParticipantOperationV2


def _completed(*, policy: PoolEnrollmentPolicyV2, reference: PoolAdministrationReferenceV2,
        path: Path, checksum: str) -> None:
    report = _read(path)
    if (digest(report) != checksum or report['status'] != 'complete'
            or report['reference'] != reference.model_dump(mode='json')
            or report['action'] not in {'apply', 'recover'}
            or report['schema_version'] != 'loom.pool-administration-dispatch.v2'):
        raise ValueError('environment_activation_receipt_changed')
    validate_administration_report(_load(policy.host, reference), report['proof']['receipt'])


async def activate_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID, activation_id: UUID,
        platform_policy: PlatformRuntimePolicyV2, catalog: GitHubCandidateCatalog,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud, public_http: httpx.AsyncClient,
        connect: Connection, predecessor: PoolAdministrationReferenceV2 | None = None,
        probe_connection: ProbeConnection | None = None, platform_probe_connection: ProbeConnection | None = None,
        boundary_connection: BoundaryConnection | None = None) -> dict[str, Any]:
    """Advance one attempt, or return pending/blocked without renewing authority.

    A fresh attempt after a committed ready transition must explicitly select
    that private ready reference. It cannot infer authorization from a state/name.
    Replays after active perform a fresh ledger read; they never demand empty
    task namespaces or re-run startup/boundary mutations.
    """
    try:
        policy, platform_policy = copy.deepcopy(policy), copy.deepcopy(platform_policy)
        _scope(policy)
        if not activation_id.int or activation_id == operation_id:
            raise ValueError
        started = load_started_environment_runtime(policy=policy, operation_id=operation_id)
        enrollment = PoolAdministrationReferenceV2.model_validate(started.staged.plan['enrollment'])
        if predecessor is not None:
            request = _load(policy.host, predecessor).request
            if (not isinstance(request, PoolParticipantOperationV2) or request.action != 'ready'
                    or request.expected_head != enrollment.operation_id):
                raise ValueError('environment_activation_predecessor_unqualified')
        phases: tuple[Literal['ready', 'activate'], ...] = ('ready', 'activate') if predecessor is None else ('activate',)
        body = dict(schema_version='loom.environment-activation-intent.v2', runtime_operation_id=str(operation_id),
            activation_id=str(activation_id), policy_sha256=digest(_policy(policy)),
            predecessor=predecessor.model_dump(mode='json') if predecessor else None,
            platform_authority=platform_policy.authority.model_dump(mode='json'),
            platform_resources_sha256=digest(platform_policy.resources),
            platform_keys=[platform_policy.manager_key, platform_policy.gateway_key,
                platform_policy.gateway_token_key, platform_policy.catalog_key])
        directory = started.staged.state_dir / ('activation-attempt-' + str(activation_id))
        anchor = policy.host.anchor_root / ('environment-activation-' + str(activation_id) + '.json')
        base = dict(activation_id=str(activation_id), runtime_operation_id=str(operation_id),
            participant_id=str(policy.participant.participant_id), execution_verified=False)
        with state._locked_state(directory):
            intent, cursor_path = directory / 'intent.json', directory / 'cursor.json'
            if not intent.exists() and (anchor.exists() or cursor_path.exists()):
                raise ValueError('environment_activation_history_missing')
            _retain(intent, body)
            if not cursor_path.exists():
                if anchor.exists():
                    raise ValueError('environment_activation_history_missing')
                state._atomic_json(cursor_path, {phase: {'reference': None, 'receipt_sha256': None} for phase in phases})
            _retain(anchor, dict(activation_id=str(activation_id), intent_sha256=digest(body)))
            cursor = _read(cursor_path)
            if set(cursor) != set(phases):
                raise ValueError('environment_activation_history_changed')
            pending = False
            for phase in phases:
                item = cursor[phase]
                if set(item) != {'reference', 'receipt_sha256'} or (pending and item['reference'] is not None):
                    raise ValueError('environment_activation_history_changed')
                if item['reference'] is None and item['receipt_sha256'] is not None:
                    raise ValueError('environment_activation_history_changed')
                pending |= item['receipt_sha256'] is None

            def unchanged() -> None:
                if (_read(intent) != body or _read(cursor_path) != cursor
                        or _read(anchor) != dict(activation_id=str(activation_id), intent_sha256=digest(body))
                        or any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in started.files.items())):
                    raise ValueError('environment_activation_history_changed')

            boundaries = load_environment_boundary_progress(policy=policy, started=started)
            if boundaries.cursor is None or any(value != 'complete' for value in boundaries.cursor['phases'].values()):
                if predecessor is not None or any(item['reference'] is not None for item in cursor.values()):
                    raise ValueError('environment_activation_boundaries_missing')
                prepared = await prepare_environment_boundaries(policy=policy, operation_id=operation_id, platform_policy=platform_policy,
                    catalog=catalog, kubernetes=kubernetes, cloud=cloud, public_http=public_http, connect=connect,
                    probe_connection=probe_connection, platform_probe_connection=platform_probe_connection,
                    boundary_connection=boundary_connection)
                unchanged()
                if prepared is None or prepared['status'] != 'runtime_boundaries_prepared_closed':
                    return dict(base, status='environment_activation_pending', phase='boundaries', admission_open=None)
            reference = predecessor
            for phase in phases:
                unchanged()
                item = cursor[phase]
                transition_id = uuid5(activation_id, phase)
                # Existing issuance is historical retrieval, with the exact same
                # grant/expiry. Missing child history cannot be re-created after
                # ready/active by observing a different predecessor.
                issued = await issue_environment_activation(policy=policy, operation_id=operation_id, platform_policy=platform_policy,
                    catalog=catalog, kubernetes=kubernetes, cloud=cloud, public_http=public_http, connect=connect,
                    probe_connection=probe_connection, platform_probe_connection=platform_probe_connection,
                    boundary_connection=boundary_connection, transition_id=transition_id, action=phase, predecessor=reference)
                unchanged()
                if issued is None:
                    if item['reference'] is not None:
                        raise ValueError('environment_activation_issuance_missing')
                    return dict(base, status='environment_activation_pending', phase=phase, admission_open=None)
                if item['reference'] is None:
                    item['reference'] = issued.model_dump(mode='json')
                    state._atomic_json(cursor_path, cursor)
                elif item['reference'] != issued.model_dump(mode='json'):
                    raise ValueError('environment_activation_issuance_changed')
                reference = issued
                report_path = directory / (phase + '.json')
                if item['receipt_sha256'] is not None:
                    _completed(policy=policy, reference=reference, path=report_path, checksum=item['receipt_sha256'])
                    continue
                start = policy.host.anchor_root / (str(reference.authorization_id) + '.started.json')
                jobs = policy.host.state_root / str(reference.authorization_id) / 'execution' / 'jobs'
                action = 'recover' if start.exists() and jobs.is_dir() else 'apply'
                command = dict(schema_version='loom.pool-administration-command.v2', action=action,
                    reference=reference.model_dump(mode='json'))
                report = await asyncio.to_thread(dispatch_administration, json.dumps(command).encode(),
                    action=action, host=policy.host, connect=connect)
                unchanged()
                if report['status'] != 'complete':
                    return dict(base, status='environment_activation_' + report['status'], phase=phase,
                        admission_open=None, reference=reference.model_dump(mode='json'))
                if report_path.exists():
                    # A reply was retained just before cursor persistence. Verify
                    # that first receipt instead of replacing recovery history.
                    _completed(policy=policy, reference=reference, path=report_path, checksum=digest(_read(report_path)))
                else:
                    _retain(report_path, report)
                item['receipt_sha256'] = digest(_read(report_path))
                state._atomic_json(cursor_path, cursor)
            assert reference is not None
            observation = await asyncio.to_thread(_observe, policy, directory / 'current-head', connect,
                require_capacity=True, platform_authorization_id=platform_policy.authority.authorization_id)
            unchanged()
            if observation is None:
                return dict(base, status='environment_activation_pending', phase='current-head', admission_open=None)
            snapshot = qualify_inspection(observation['report']['receipt'], PoolInspectionV2.model_validate(observation['input']['request']),
                UUID(observation['input']['authorization']['authorization_id']))
            selected = snapshot.participant
            active = _load(policy.host, reference).request
            if (not isinstance(active, PoolParticipantOperationV2) or active.action != 'activate'
                    or selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                    or selected.head.operation_id != reference.operation_id or selected.head.state != 'active'
                    or selected.head.binding_sha256 != active.expected_binding_sha256
                    or snapshot.pool_authority_sha256 != active.pool_authority_sha256):
                raise ValueError('environment_activation_current_head_changed')
            return dict(base, status='environment_active', admission_open=True,
                reference=reference.model_dump(mode='json'), inspection_sha256=digest(observation),
                observed_at=snapshot.observed_at.isoformat(), valid_until=snapshot.valid_until.isoformat())
    except Exception:
        raise ValueError('environment_activation_unqualified') from None
