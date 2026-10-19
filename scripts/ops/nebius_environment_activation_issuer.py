"""Protected scoped ready/activate issuance from connected runtime observations.

Issuance never changes participant state. The existing fixed administration Job
consumes the pinned grant and rechecks head, credentials and physical accounting
transactionally. A new explicit transition ID is required for a new attempt; no
expired intent is silently renewed, and no peer or historical staging is loaded.
"""
from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from functools import partial
from typing import Any, Literal
from uuid import UUID, uuid5

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_foundation import FoundationCloud, _read
from scripts.ops.nebius_environment_runtime_boundaries import (
    BoundaryConnection,
    load_environment_boundary_progress,
    verify_prepared_environment_boundaries,
)
from scripts.ops.nebius_environment_runtime_readiness import (
    ProbeConnection,
    _process_connection,
    observe_environment_pre_activation,
    qualify_readiness_predecessor,
)
from scripts.ops.nebius_environment_runtime_start import (
    StartedEnvironmentRuntimeV2,
    load_started_environment_runtime,
    runtime_resource_path,
)
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_platform_runtime_readiness import (
    PlatformRuntimePolicyV2,
    recheck_platform_resources,
)
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
    _load,
    retain_authorized_operation,
)
from scripts.ops.nebius_pool_enrollment_issuer import (
    PoolEnrollmentPolicyV2,
    _grant,
    _policy,
    _scope,
)

from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantOperationV2,
    PoolParticipantReadinessV2,
    PoolRuntimeReadinessV2,
)


async def _recheck_runtime(*, policy: PoolEnrollmentPolicyV2, started: StartedEnvironmentRuntimeV2,
        platform_policy: PlatformRuntimePolicyV2, runtime: dict[str, Any], qualify: Callable[[], None],
        kubernetes: KubernetesEnvironmentProvider, probe_connection: ProbeConnection | None,
        platform_probe_connection: ProbeConnection | None) -> None:
    """Close later boundary IO against the original process/configuration proof.

    Do not observe a replacement process and bless it using the old settings/DB
    challenge. The fixed administration transaction still checks current authority,
    head and capacity; this readback does not promise atomic Kubernetes snapshots.
    """
    progress = load_environment_boundary_progress(policy=policy, started=started)
    if runtime['environment']['boundary_state_sha256'] != progress.sha256:
        raise ValueError('environment_activation_boundary_changed')
    options = {key: (doc,) for key, doc in started.resources.items()} | progress.options
    binding = platform_policy.host.runtime.binding
    platform_factory: ProbeConnection = platform_probe_connection or partial(_process_connection, policy=policy,
        resources=platform_policy.resources, namespaces={binding.namespace: binding.namespace_uid})
    factory: ProbeConnection = probe_connection or partial(_process_connection, policy=policy,
        resources=started.resources, namespaces=started.staged.plan['namespace_uids'])
    def processes() -> None:
        qualify()
        original = _load(platform_policy.host, platform_policy.authority)
        authority = runtime['platform']['authority']
        if (authority['operation_id'] != str(original.request.operation_id)
                or authority['authorization_id'] != str(original.authorization.authorization_id)
                or authority['request_sha256'] != original.authorization.request_sha256
                or authority['authorization_sha256'] != digest(original.authorization.model_dump(mode='json'))):
            raise ValueError('environment_activation_platform_changed')
        with platform_factory(qualify, policy.host.runtime.region) as probes:
            recheck_platform_resources(policy=platform_policy, probes=probes)
            for key, proof in zip((platform_policy.manager_key, platform_policy.gateway_key), runtime['platform']['processes'], strict=True):
                probes.recheck_process(workload=platform_policy.resources[key], proof=proof)
            recheck_platform_resources(policy=platform_policy, probes=probes)
        with factory(qualify, policy.host.runtime.region) as probes:
            for desired, proof in zip(started.staged.plan['workloads'].values(), runtime['environment']['processes'], strict=True):
                probes.recheck_process(workload=started.resources[_key(desired)], proof=proof)
        qualify()
    for choices in options.values():
        desired, = choices
        actual = await kubernetes._request('GET', runtime_resource_path(desired) + '/' + desired['metadata']['name'])
        if actual is None or not _matches(actual, desired, _uid(desired)):
            raise ValueError('environment_activation_resource_changed')
    await asyncio.to_thread(processes)
    qualify()


def _request(*, policy: PoolEnrollmentPolicyV2, transition_id: UUID, action: Literal['ready', 'activate'],
        predecessor: PoolAdministrationReferenceV2 | None, evidence: dict[str, Any]) -> PoolParticipantOperationV2:
    observed = evidence['runtime']
    inspection = observed['inspection']
    snapshot = qualify_inspection(inspection['report']['receipt'], PoolInspectionV2.model_validate(inspection['input']['request']),
        UUID(inspection['input']['authorization']['authorization_id']))
    enrollment = PoolAdministrationReferenceV2.model_validate(evidence['enrollment'])
    qualify_readiness_predecessor(policy=policy, enrollment=enrollment, snapshot=snapshot, predecessor=predecessor)
    selected = snapshot.participant
    assert selected is not None
    platform = PoolRuntimeReadinessV2.model_validate(observed['platform']['readiness'])
    expires = min(platform.valid_until, datetime.fromisoformat(observed['valid_until']))
    if ((action == 'ready') != (predecessor is None)
            or observed['status'] != 'preactivation_runtime_verified_closed' or observed['admission_open'] is not False
            or snapshot.capacity is None or snapshot.platform_authority is None
            or platform.pool_authority_sha256 != snapshot.pool_authority_sha256
            or platform.observed_at != snapshot.observed_at or platform.valid_until != snapshot.valid_until):
        raise ValueError('environment_activation_evidence_unqualified')
    readiness = PoolParticipantReadinessV2.model_validate(platform.model_dump() | {'valid_until': expires,
        'participant_binding_sha256': selected.head.binding_sha256, 'catalog_sha256': selected.catalog_sha256})
    return PoolParticipantOperationV2(schema_version='loom.pool-participant-operation.v2', operation_id=transition_id,
        pool_id=policy.participant.pool_id, participant_id=policy.participant.participant_id, incarnation=policy.participant.incarnation,
        expected_head=selected.head.operation_id, expected_binding_sha256=selected.head.binding_sha256,
        pool_authority_sha256=snapshot.pool_authority_sha256, action=action, readiness=readiness)


def _retained(*, policy: PoolEnrollmentPolicyV2, transition_id: UUID, action: Literal['ready', 'activate'],
        predecessor: PoolAdministrationReferenceV2 | None, document: dict[str, Any]) -> PoolAdministrationReferenceV2:
    if set(document) != {'request', 'authorization', 'evidence'}:
        raise ValueError('environment_activation_issuance_changed')
    request = _request(policy=policy, transition_id=transition_id, action=action, predecessor=predecessor, evidence=document['evidence'])
    grant = PoolAdministrationAuthorizationV2.model_validate(document['authorization'])
    assert request.readiness is not None
    if (request.model_dump(mode='json') != document['request']
            or grant != _grant(policy, request, uuid5(transition_id, 'activation-authorization'),
                issued_at=grant.issued_at, expires_at=grant.expires_at)
            or not request.readiness.observed_at <= grant.issued_at < grant.expires_at <= request.readiness.valid_until):
        raise ValueError('environment_activation_issuance_changed')
    evidence_sha256 = digest(document['evidence'])
    reference = PoolAdministrationReferenceV2(schema_version='loom.pool-administration-reference.v2',
        **policy.host.scope.model_dump(), operation_id=transition_id, authorization_id=grant.authorization_id,
        request_sha256=grant.request_sha256, document_sha256=digest(dict(schema_version='loom.pool-authorized-operation.v2',
            runtime=asdict(policy.host.runtime), request=document['request'], authorization=document['authorization'],
            evidence_sha256=evidence_sha256)))
    anchor = policy.host.anchor_root / (str(grant.authorization_id) + '.json')
    directory = policy.host.state_root / str(grant.authorization_id)
    if anchor.exists() or anchor.is_symlink() or directory.exists() or directory.is_symlink():
        retained = _load(policy.host, reference)
        if retained.request != request or retained.authorization != grant:
            raise ValueError('environment_activation_issuance_changed')
        return reference  # Historical retrieval does not extend expiry or authorize another apply.
    return retain_authorized_operation(host=policy.host, request=request, authorization=grant, evidence_sha256=evidence_sha256)


async def issue_environment_activation(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID, transition_id: UUID,
        action: Literal['ready', 'activate'], predecessor: PoolAdministrationReferenceV2 | None,
        platform_policy: PlatformRuntimePolicyV2, catalog: GitHubCandidateCatalog,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud, public_http: httpx.AsyncClient,
        connect: Connection, probe_connection: ProbeConnection | None = None,
        platform_probe_connection: ProbeConnection | None = None,
        boundary_connection: BoundaryConnection | None = None) -> PoolAdministrationReferenceV2 | None:
    """Return a pinned grant, or None while read-only qualification is pending."""
    try:
        policy, platform_policy = copy.deepcopy(policy), copy.deepcopy(platform_policy)
        _scope(policy)
        if (action not in {'ready', 'activate'} or (action == 'ready') != (predecessor is None)
                or not operation_id.int or not transition_id.int or operation_id == transition_id):
            raise ValueError
        body = dict(policy_sha256=digest(_policy(policy)), runtime_operation_id=str(operation_id), transition_id=str(transition_id),
            action=action, predecessor=predecessor.model_dump(mode='json') if predecessor else None,
            platform_authority=platform_policy.authority.model_dump(mode='json'),
            platform_resources_sha256=digest(platform_policy.resources), platform_keys=[platform_policy.manager_key,
                platform_policy.gateway_key, platform_policy.gateway_token_key, platform_policy.catalog_key])
        directory = policy.host.state_root / ('runtime-' + str(operation_id)) / ('activation-' + str(transition_id))
        with state._locked_state(directory):
            intent, issued = directory / 'intent.json', directory / 'issued.json'
            if intent.exists() or intent.is_symlink():
                if _read(intent) != body:
                    raise ValueError('environment_activation_intent_conflict')
            else:
                if issued.exists() or issued.is_symlink():
                    raise ValueError('environment_activation_intent_missing')
                state._write_private(intent, json.dumps(body, sort_keys=True).encode())
                state._sync_directory(directory)
            if issued.exists() or issued.is_symlink():
                return _retained(policy=policy, transition_id=transition_id, action=action, predecessor=predecessor, document=_read(issued))
            started = load_started_environment_runtime(policy=policy, operation_id=operation_id)
            progress = load_environment_boundary_progress(policy=policy, started=started)
            if progress.plan is None or progress.cursor is None or any(value != 'complete' for value in progress.cursor['phases'].values()):
                raise ValueError('environment_activation_boundaries_incomplete')
            runtime = await observe_environment_pre_activation(policy=policy, operation_id=operation_id, platform_policy=platform_policy,
                catalog=catalog, kubernetes=kubernetes, cloud=cloud, public_http=public_http, connect=connect,
                probe_connection=probe_connection, platform_probe_connection=platform_probe_connection, _readiness_predecessor=predecessor)
            if runtime is None:
                return None
            if runtime['environment']['boundary_state_sha256'] != progress.sha256:
                raise ValueError('environment_activation_boundary_changed')
            files = {**started.files, **progress.files}
            def qualify() -> None:
                if (datetime.now(UTC) >= datetime.fromisoformat(runtime['valid_until']) or _read(intent) != body
                        or any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items())):
                    raise ValueError('environment_activation_readiness_changed')
            if not await asyncio.to_thread(verify_prepared_environment_boundaries, policy=policy, started=started,
                    progress=progress, qualify=qualify, boundary_connection=boundary_connection):
                return None
            await _recheck_runtime(policy=policy, started=started, platform_policy=platform_policy, runtime=runtime,
                qualify=qualify, kubernetes=kubernetes, probe_connection=probe_connection,
                platform_probe_connection=platform_probe_connection)
            evidence = dict(runtime=runtime, enrollment=started.staged.plan['enrollment'],
                boundary_plan_sha256=digest(progress.plan), boundary_state_sha256=progress.sha256)
            request = _request(policy=policy, transition_id=transition_id, action=action, predecessor=predecessor, evidence=evidence)
            assert request.readiness is not None
            grant = _grant(policy, request, uuid5(transition_id, 'activation-authorization'),
                issued_at=datetime.now(UTC), expires_at=request.readiness.valid_until)
            qualify()
            document = dict(request=request.model_dump(mode='json'), authorization=grant.model_dump(mode='json'), evidence=evidence)
            state._write_private(issued, json.dumps(document, sort_keys=True).encode())
            state._sync_directory(directory)
            return _retained(policy=policy, transition_id=transition_id, action=action, predecessor=predecessor, document=document)
    except Exception:
        raise ValueError('environment_activation_issuance_unqualified') from None
