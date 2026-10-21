"""Protected image-only release and rollback of one active v2 environment.

The original enrollment remains immutable. Each release advances the selected
binding/catalog revision, drains before fixed workload CAS, verifies the actual
processes and reopens only that participant. Historical grants are never renewed.
"""
from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_runtime_transition import _advance_fixed_resources
from scripts.ops.nebius_environment_activation import _completed
from scripts.ops.nebius_environment_foundation import (
    FoundationCloud,
    _read,
    _retain,
    reconcile_foundation,
)
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_release_contracts import (
    EnvironmentReleaseV2,
    release_projection,
)
from scripts.ops.nebius_environment_release_readiness import release_readiness
from scripts.ops.nebius_environment_release_runtime import (
    PHASES,
    EnvironmentReleaseWorkloads,
    release_inputs,
    release_options,
)
from scripts.ops.nebius_environment_runtime_boundaries import (
    BoundaryConnection,
    load_environment_boundary_progress,
)
from scripts.ops.nebius_environment_runtime_install import (
    _observe,
    _resources,
    qualify_runtime_defaulted,
)
from scripts.ops.nebius_environment_runtime_readiness import ProbeConnection
from scripts.ops.nebius_environment_runtime_start import (
    _frozen,
    load_started_environment_runtime,
    runtime_resource_path,
)
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_platform_runtime_readiness import PlatformRuntimePolicyV2
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
    _scope,
)

from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import (
    PoolInspectionSnapshotV2,
    PoolInspectionV2,
    qualify_inspection,
)
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantOperationV2,
    PoolParticipantReadinessV2,
    PoolParticipantReleaseV2,
)
from loom_service.pool_management.profiles import PoolProfileCatalog


def _snapshot(value: dict[str, Any]) -> PoolInspectionSnapshotV2:
    return qualify_inspection(value['report']['receipt'], PoolInspectionV2.model_validate(value['input']['request']),
        UUID(value['input']['authorization']['authorization_id']))


def _directory(policy: PoolEnrollmentPolicyV2, operation_id: UUID) -> Path:
    return policy.host.state_root / ('environment-release-' + str(operation_id))


def _anchor(policy: PoolEnrollmentPolicyV2, operation_id: UUID) -> Path:
    return policy.host.anchor_root / ('environment-release-' + str(operation_id) + '.json')


def _active_descendant(policy: PoolEnrollmentPolicyV2, reference: PoolAdministrationReferenceV2 | None,
        completed: PoolAdministrationReferenceV2, binding_sha256: str) -> None:
    """Allow exact intervening suspend/resume operations on the same revision."""
    seen: set[UUID] = set()
    while reference != completed:
        if reference is None or reference.operation_id in seen or len(seen) >= 64:
            raise ValueError('environment_release_active_chain_unqualified')
        seen.add(reference.operation_id)
        request = _load(policy.host, reference).request
        if (not isinstance(request, PoolParticipantOperationV2) or request.action not in {'suspend', 'resume'}
                or request.expected_binding_sha256 != binding_sha256
                or (request.pool_id, request.participant_id, request.incarnation) != (
                    policy.participant.pool_id, policy.participant.participant_id, policy.participant.incarnation)):
            raise ValueError('environment_release_active_chain_unqualified')
        if request.expected_head == completed.operation_id:
            reference = completed
        else:
            reference = PoolAdministrationReferenceV2.model_validate(_read(policy.host.anchor_root /
                (str(uuid5(request.expected_head, 'lifecycle-authorization')) + '.json')))


def load_completed_release(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        seen: frozenset[UUID] = frozenset()) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[Path, bytes]]:
    """Anchored chain plus fixed child receipts; never adopt a state-only result."""
    if operation_id in seen or len(seen) >= 64:
        raise ValueError('environment_release_history_invalid')
    directory = _directory(policy, operation_id)
    paths = [directory / name for name in ('plan.json', 'cursor.json', 'complete.json')]
    paths.append(_anchor(policy, operation_id))
    files = {path: state._private_read(path, limit=8 * 1024**2) for path in paths}
    plan, cursor, complete, anchor = (json.loads(files[path]) for path in paths)
    if (anchor != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
            or plan['policy_sha256'] != digest(_policy(policy)) or plan['operation_id'] != str(operation_id)
            or cursor['runtime'] != dict.fromkeys(PHASES, 'complete')
            or complete != {'plan_sha256': digest(plan), 'reference': cursor['activate']['reference']}):
        raise ValueError('environment_release_history_changed')
    selection = EnvironmentReleaseV2.model_validate(plan['selection'])
    if selection.previous_release_id is not None:
        previous, originals, prior_files = load_completed_release(policy=policy, operation_id=selection.previous_release_id,
            seen=seen | {operation_id})
        if plan['previous_plan_sha256'] != digest(previous) or originals != plan['originals']:
            raise ValueError('environment_release_predecessor_changed')
        _active_descendant(policy, selection.predecessor, PoolAdministrationReferenceV2.model_validate(
            _read(_directory(policy, selection.previous_release_id) / 'complete.json')['reference']),
            digest(previous['release']['participant']))
        files.update(prior_files)
    options = release_options(plan, directory, cursor['runtime'])
    resources = {key: choices[0] for key, choices in options.items() if len(choices) == 1}
    if len(resources) != len(options):
        raise ValueError('environment_release_history_incomplete')
    release = PoolParticipantReleaseV2.model_validate(plan['release'])
    for phase in ('begin_release', 'commit_release', 'activate'):
        item = cursor[phase]
        reference = PoolAdministrationReferenceV2.model_validate(item['reference'])
        report = directory / (phase + '.json')
        _completed(policy=policy, reference=reference, path=report, checksum=item['receipt_sha256'])
        request = _load(policy.host, reference).request
        if (not isinstance(request, PoolParticipantOperationV2) or request.action != phase
                or request.operation_id != uuid5(operation_id, phase)
                or (phase == 'begin_release' and request.replacement != release)):
            raise ValueError('environment_release_receipt_changed')
        files[report] = state._private_read(report, limit=8 * 1024**2)
    for phase in PHASES:
        path = directory / phase / 'transition.json'
        files[path] = state._private_read(path, limit=8 * 1024**2)
    return plan, resources, files


def resolve_completed_release_policy(*, policy: PoolEnrollmentPolicyV2, release_id: UUID) -> tuple[PoolEnrollmentPolicyV2, dict[str, Any], dict[str, dict[str, Any]], dict[Path, bytes]]:
    """Current immutable runtime contract for later suspend/resume/retirement."""
    plan, resources, files = load_completed_release(policy=policy, operation_id=release_id)
    selected = EnvironmentReleaseV2.model_validate(plan['selection'])
    release = PoolParticipantReleaseV2.model_validate(plan['release'])
    return replace(policy, participant=release.participant, profiles=release.profiles,
        application_publication=selected.publication), plan, resources, files


def require_release_production_authority(policy: PoolEnrollmentPolicyV2,
        source: ProtectedPublication, following: ProtectedPublication) -> None:
    """Baseline plus actual current and replacement publications, still fresh."""
    if policy.participant.environment_class != 'production':
        return
    from scripts.ops.nebius_production_promotion import require_production_authority
    for publication in (policy.application_publication, source, following):
        target = require_production_authority(scope=policy.host.scope, publication=publication)
        row = policy.foundation.identity
        if (target.namespace != row.application_namespace or target.execution_namespace != row.execution_namespace
                or target.public_url != 'https://' + row.public_host or target.database_name != 'loom'
                or target.database_identity != f'kubernetes:{row.cluster_id}/{row.application_namespace}/loom-postgres/loom'):
            raise ValueError('environment_release_production_target_changed')


async def release_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        replacement: EnvironmentReleaseV2, platform_policy: PlatformRuntimePolicyV2, catalog: GitHubCandidateCatalog,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud, public_http: httpx.AsyncClient,
        connect: Connection, probe_connection: ProbeConnection | None = None,
        platform_probe_connection: ProbeConnection | None = None,
        workload_connection: BoundaryConnection | None = None) -> dict[str, Any]:
    """Advance one anchored attempt, returning a truthful pending phase."""
    try:
        policy, replacement, platform_policy = copy.deepcopy((policy, replacement, platform_policy))
        foundation = load_foundation(policy.foundation_dir, policy.foundation)
        imported = foundation.get('mode') == 'legacy-import'
        require_release_production_authority(policy, policy.application_publication, replacement.publication)
        if not imported:
            _scope(policy)
        if not operation_id.int or operation_id in {replacement.runtime_operation_id, replacement.previous_release_id}:
            raise ValueError('environment_release_identity_invalid')
        if imported:
            from scripts.ops.nebius_environment_installed_import import (
                load_imported_environment_runtime,
            )
            started = load_imported_environment_runtime(policy=policy, operation_id=replacement.runtime_operation_id)
            files, baseline = dict(started.files), copy.deepcopy(started.resources)
        else:
            started = load_started_environment_runtime(policy=policy, operation_id=replacement.runtime_operation_id)
            boundaries = load_environment_boundary_progress(policy=policy, started=started)
            if boundaries.cursor is None or any(value != 'complete' for value in boundaries.cursor['phases'].values()):
                raise ValueError('environment_release_boundaries_incomplete')
            files = {**started.files, **boundaries.files}
            baseline = {**started.resources, **{key: choices[0] for key, choices in boundaries.options.items() if len(choices) == 1}}
        source_policy = policy
        previous_plan = None
        historical_profiles: tuple[PoolProfileCatalog, ...] = ()
        publications = {replacement.runtime_operation_id: policy.application_publication}
        if replacement.previous_release_id is not None:
            previous_plan, baseline, prior_files = load_completed_release(policy=policy, operation_id=replacement.previous_release_id)
            files.update(prior_files)
            historical_profiles = tuple(PoolProfileCatalog.model_validate(row) for row in previous_plan['historical_profiles'])
            historical_profiles = (*historical_profiles, PoolProfileCatalog.model_validate(previous_plan['source_profiles']))
            prior = PoolParticipantReleaseV2.model_validate(previous_plan['release'])
            prior_selection = EnvironmentReleaseV2.model_validate(previous_plan['selection'])
            source_policy = replace(policy, participant=prior.participant, profiles=prior.profiles,
                application_publication=prior_selection.publication)
            _active_descendant(policy, replacement.predecessor, PoolAdministrationReferenceV2.model_validate(
                _read(_directory(policy, replacement.previous_release_id) / 'complete.json')['reference']),
                digest(prior.participant.model_dump(mode='json')))
            for path, raw in prior_files.items():
                if path.name == 'plan.json' and path.parent.name.startswith('environment-release-'):
                    prior_record = json.loads(raw)
                    publications[UUID(prior_record['operation_id'])] = EnvironmentReleaseV2.model_validate(prior_record['selection']).publication
        require_release_production_authority(policy, source_policy.application_publication, replacement.publication)
        if replacement.rollback_to is not None:
            if publications.get(replacement.rollback_to) != replacement.publication:
                raise ValueError('environment_release_rollback_target_unqualified')
        for publication in (source_policy.application_publication, replacement.publication):
            existing = catalog.publications.get(publication.candidate_id)
            if existing is not None and existing != publication:
                raise ValueError('environment_release_publication_changed')
            catalog.publications[publication.candidate_id] = publication
        before_bundle = await catalog.resolve(source_policy.application_publication.candidate_id)
        following_bundle = await catalog.resolve(replacement.publication.candidate_id)
        if (before_bundle.candidate['candidate_sha'] == following_bundle.candidate['candidate_sha']
                or (replacement.rollback_to is None and not await catalog.candidate_follows(
                    candidate_id=replacement.publication.candidate_id, current_sha=before_bundle.candidate['candidate_sha']))):
            raise ValueError('environment_release_ancestry_unqualified')
        foundation = load_foundation(policy.foundation_dir, policy.foundation)
        if previous_plan is None and (digest(before_bundle.candidate) != foundation['publication_sha256']
                or digest(before_bundle.profile) != foundation['profile_sha256']):
            raise ValueError('environment_release_baseline_changed')
        if replacement.predecessor is None:
            if not imported or str(replacement.imported_head) != started.staged.plan['imported']['active_head']:
                raise ValueError('environment_release_imported_predecessor_unqualified')
            expected_head = replacement.imported_head
            assert expected_head is not None
        else:
            previous = _load(policy.host, replacement.predecessor).request
            if (not isinstance(previous, PoolParticipantOperationV2) or previous.action not in {'activate', 'resume'}
                    or previous.expected_binding_sha256 != digest(source_policy.participant.model_dump(mode='json'))
                    or (previous.pool_id, previous.participant_id, previous.incarnation) != (
                        policy.participant.pool_id, policy.participant.participant_id, policy.participant.incarnation)):
                raise ValueError('environment_release_active_predecessor_unqualified')
            expected_head = replacement.predecessor.operation_id
        directory = _directory(policy, operation_id)
        anchor = _anchor(policy, operation_id)
        base = dict(operation_id=str(operation_id), participant_id=str(policy.participant.participant_id), execution_verified=False)
        with state._locked_state(directory):
            plan_path, cursor_path = directory / 'plan.json', directory / 'cursor.json'
            if plan_path.exists():
                plan = _read(plan_path)
                if anchor.exists() and _read(anchor) != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}:
                    raise ValueError('environment_release_plan_changed')
                # Extra original foundation objects are frozen only on the first
                # invocation, never re-adopted from a half-replaced runtime.
                for key, doc in baseline.items():
                    if plan['originals'].get(key) != doc:
                        raise ValueError('environment_release_originals_changed')
                baseline = plan['originals']
            else:
                if anchor.exists() or cursor_path.exists():
                    raise ValueError('environment_release_history_missing')
                docs = {_key(doc): doc for group in foundation['files'].values() for doc in group}
                extra = [doc for doc in docs.values() if doc['metadata'].get('namespace') == policy.foundation.identity.application_namespace
                    and (doc['kind'] == 'Deployment' or (doc['kind'] == 'ConfigMap' and doc['metadata']['name'] == 'loom-platform-config'))]
                for doc in extra:
                    key = _key(doc)
                    if key in baseline:
                        continue
                    actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
                    if actual is None:
                        raise ValueError('environment_release_resource_missing')
                    kubernetes._identity(actual, doc, started.staged.plan['foundation_identities']['k8s:' + key])
                    baseline[key] = _frozen(actual)
            release, targets = release_projection(operation_id=operation_id, participant=source_policy.participant,
                profiles=source_policy.profiles, previous=before_bundle, following=following_bundle,
                resources=baseline, application_namespace=policy.foundation.identity.application_namespace, historical_profiles=historical_profiles,
                imported_role_images={key: doc['spec']['template']['spec']['containers'][0]['image']
                    for key, doc in started.resources.items() if imported and doc['kind'] == 'Deployment'
                    and doc['metadata']['name'] in {'loom-web', 'loom-llm-gateway'}})
            desired_plan = dict(schema_version='loom.environment-release-plan.v2', operation_id=str(operation_id),
                policy_sha256=digest(_policy(policy)), selection=replacement.model_dump(mode='json'),
                previous_plan_sha256=digest(previous_plan) if previous_plan else None,
                historical_profiles=[row.model_dump(mode='json') for row in historical_profiles],
                source_profiles=source_policy.profiles.model_dump(mode='json'),
                source_candidate_sha256=digest(before_bundle.candidate), source_profile_sha256=digest(before_bundle.profile),
                following_candidate_sha256=digest(following_bundle.candidate), following_profile_sha256=digest(following_bundle.profile),
                platform_authority=platform_policy.authority.model_dump(mode='json'),
                platform_resources_sha256=digest(platform_policy.resources), originals=baseline,
                release=release.model_dump(mode='json'), targets=targets)
            _retain(plan_path, desired_plan)
            plan = desired_plan
            _retain(anchor, {'operation_id': str(operation_id), 'plan_sha256': digest(plan)})
            if not cursor_path.exists():
                state._atomic_json(cursor_path, {'runtime': dict.fromkeys(PHASES, 'prepared'),
                    **{phase: {'reference': None, 'receipt_sha256': None} for phase in ('begin_release', 'commit_release', 'activate')}})
            cursor = _read(cursor_path)
            if set(cursor) != {'runtime', 'begin_release', 'commit_release', 'activate'}:
                raise ValueError('environment_release_cursor_changed')
            next_policy = replace(source_policy, participant=release.participant, profiles=release.profiles,
                application_publication=replacement.publication)
            def check() -> None:
                require_release_production_authority(policy, source_policy.application_publication, replacement.publication)
                if (_read(plan_path) != plan or _read(cursor_path) != cursor
                        or _read(anchor) != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
                        or any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items())):
                    raise ValueError('environment_release_history_changed')
            async def inspect(selected_policy: PoolEnrollmentPolicyV2, name: str) -> dict[str, Any] | None:
                result = await asyncio.to_thread(_observe, selected_policy, directory / name, connect,
                    require_capacity=True, platform_authorization_id=platform_policy.authority.authorization_id,
                    include_workload_counts=True)
                check()
                return result
            def selected(value: dict[str, Any], selected_policy: PoolEnrollmentPolicyV2,
                    head: UUID, expected_state: str) -> PoolInspectionSnapshotV2:
                snapshot = _snapshot(value)
                row = snapshot.participant
                if (row is None or row.binding != selected_policy.participant or row.profiles != selected_policy.profiles
                        or row.head.operation_id != head or row.head.state != expected_state or row.workload_counts is None):
                    raise ValueError('environment_release_head_changed')
                return snapshot
            async def apply(phase: str, selected_policy: PoolEnrollmentPolicyV2, expected_head: UUID,
                    observation: dict[str, Any] | None = None, evidence: dict[str, Any] | None = None) -> bool:
                check()
                item = cursor[phase]
                transition_id = uuid5(operation_id, phase)
                authorization_id = uuid5(transition_id, 'release-authorization')
                grant_anchor = policy.host.anchor_root / (str(authorization_id) + '.json')
                if item['reference'] is None:
                    if grant_anchor.exists():
                        reference = PoolAdministrationReferenceV2.model_validate(_read(grant_anchor))
                    else:
                        if observation is None:
                            raise ValueError('environment_release_issuance_missing')
                        snapshot = _snapshot(observation)
                        row = snapshot.participant
                        assert row is not None
                        readiness = PoolParticipantReadinessV2.model_validate(evidence['readiness']) if evidence else None
                        request = PoolParticipantOperationV2(schema_version='loom.pool-participant-operation.v2',
                            operation_id=transition_id, pool_id=policy.participant.pool_id,
                            participant_id=policy.participant.participant_id, incarnation=policy.participant.incarnation,
                            expected_head=expected_head, expected_binding_sha256=row.head.binding_sha256,
                            pool_authority_sha256=snapshot.pool_authority_sha256, action=phase,
                            replacement=release if phase == 'begin_release' else None, readiness=readiness)
                        expires = min(snapshot.valid_until, readiness.valid_until) if readiness else snapshot.valid_until
                        grant = _grant(selected_policy, request, authorization_id, issued_at=datetime.now(UTC), expires_at=expires)
                        check()
                        reference = retain_authorized_operation(host=policy.host, request=request, authorization=grant,
                            evidence_sha256=digest({'inspection': observation, 'runtime': evidence}))
                    item['reference'] = reference.model_dump(mode='json')
                    state._atomic_json(cursor_path, cursor)
                reference = PoolAdministrationReferenceV2.model_validate(item['reference'])
                job = _load(policy.host, reference)
                request = job.request
                if (not isinstance(request, PoolParticipantOperationV2) or request.operation_id != transition_id
                        or request.action != phase or request.expected_head != expected_head
                        or request.expected_binding_sha256 != digest(selected_policy.participant.model_dump(mode='json'))
                        or reference.authorization_id != authorization_id
                        or (phase == 'begin_release' and request.replacement != release)):
                    raise ValueError('environment_release_issuance_changed')
                report_path = directory / (phase + '.json')
                if item['receipt_sha256'] is not None:
                    _completed(policy=policy, reference=reference, path=report_path, checksum=item['receipt_sha256'])
                    return True
                started_path = policy.host.anchor_root / (str(reference.authorization_id) + '.started.json')
                fresh = job.authorization.issued_at <= datetime.now(UTC) < job.authorization.expires_at
                action = 'recover' if started_path.exists() and not fresh else 'apply'
                command = dict(schema_version='loom.pool-administration-command.v2', action=action, reference=item['reference'])
                report = await asyncio.to_thread(dispatch_administration, json.dumps(command).encode(),
                    action=action, host=policy.host, connect=connect)
                check()
                if report['status'] != 'complete':
                    return False
                if report_path.exists():
                    _completed(policy=policy, reference=reference, path=report_path, checksum=digest(_read(report_path)))
                else:
                    _retain(report_path, report)
                item['receipt_sha256'] = digest(_read(report_path))
                state._atomic_json(cursor_path, cursor)
                return True
            if cursor['begin_release']['reference'] is None:
                observed = await inspect(source_policy, 'before')
                if observed is None:
                    return dict(base, status='pending', phase='before', admission_open=None)
                selected(observed, source_policy, expected_head, 'active')
            else:
                observed = None
            if not await apply('begin_release', source_policy, expected_head, observed):
                return dict(base, status='pending', phase='begin_release', admission_open=None)
            options = release_options(plan, directory, cursor['runtime'])
            all_started = all(value == 'complete' for value in cursor['runtime'].values())
            if not all_started:
                observed = await inspect(source_policy, 'drain')
                if observed is None:
                    return dict(base, status='pending', phase='drain', admission_open=False)
                snapshot = selected(observed, source_policy, uuid5(operation_id, 'begin_release'), 'suspended')
                assert snapshot.participant is not None and snapshot.participant.workload_counts is not None
                if snapshot.participant.workload_counts.charged:
                    return dict(base, status='pending', phase='drain', admission_open=False,
                        charged=snapshot.participant.workload_counts.charged)
            def factory(before: dict[str, Any], desired: dict[str, Any], current: dict[str, Any], profiles: PoolProfileCatalog) -> Any:
                if workload_connection:
                    return workload_connection(before, desired, started.staged.plan['namespace_uids'], check)
                @contextmanager
                def connected() -> Iterator[EnvironmentReleaseWorkloads]:
                    with _resources(policy, before, started.staged.plan['namespace_uids'], check) as api:
                        readers = {key: doc for key, doc in current.items() if doc['kind'] == 'Deployment'
                            and doc['metadata']['namespace'] == policy.participant.execution_namespace.name}
                        build_profile = next(iter((*profiles.task_images, *profiles.application_images)), None)
                        yield EnvironmentReleaseWorkloads(api, before, desired, policy.host.runtime.region, readers,
                            build_policy={key: doc for key, doc in current.items() if doc['kind'] in {
                                'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}, build_profile=build_profile)
                return connected()
            async def resources(choices: dict[str, tuple[dict[str, Any], ...]]) -> None:
                successors = {'k8s:' + key: docs for key, docs in choices.items()
                    if 'k8s:' + key in started.staged.plan['foundation_identities']}
                await reconcile_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
                    kubernetes=kubernetes, cloud=cloud, observe_only=True, _runtime_successors=successors)
                for docs in choices.values():
                    actual = await kubernetes._request('GET', runtime_resource_path(docs[0]) + '/' + docs[0]['metadata']['name'])
                    if actual is None or not any(_matches(actual, doc, _uid(doc)) for doc in docs):
                        raise ValueError('environment_release_resource_changed')
                check()
            await resources(options)
            if cursor['runtime']['stop'] == 'prepared':
                def inventory() -> None:
                    with factory(baseline, {}, baseline, source_policy.profiles) as api:
                        api.check_inventory()
                await asyncio.to_thread(inventory)
            for phase in PHASES:
                if cursor['runtime'][phase] == 'complete':
                    continue
                before, desired = release_inputs(plan, directory, phase)
                cursor['runtime'][phase] = 'intent'
                state._atomic_json(cursor_path, cursor)
                def advance(before: dict[str, Any] = before, desired: dict[str, Any] = desired, phase: str = phase) -> bool:
                    with factory(before, desired, baseline, next_policy.profiles) as api:
                        return _advance_fixed_resources(originals=before, targets=desired, input_digest='sha256:' + digest(plan),
                            phase=phase, api=api, state_dir=directory / phase, phases=frozenset(PHASES),
                            kinds=frozenset({'Deployment', 'ConfigMap', 'ValidatingAdmissionPolicy'}),
                            qualify_defaulted=qualify_runtime_defaulted)
                if not await asyncio.to_thread(advance):
                    return dict(base, status='pending', phase=phase, admission_open=False)
                check()
                cursor['runtime'][phase] = 'complete'
                state._atomic_json(cursor_path, cursor)
            options = release_options(plan, directory, cursor['runtime'])
            current = {key: docs[0] for key, docs in options.items()}
            await resources(options)
            for phase, selected_policy, expected_head, expected_state in (
                    ('commit_release', source_policy, uuid5(operation_id, 'begin_release'), 'suspended'),
                    ('activate', next_policy, uuid5(operation_id, 'commit_release'), 'ready')):
                evidence, observed = None, None
                if cursor[phase]['reference'] is None:
                    observed = await inspect(selected_policy, phase + '-readiness')
                    if observed is None:
                        return dict(base, status='pending', phase=phase, admission_open=False)
                    snapshot = selected(observed, selected_policy, expected_head, expected_state)
                    def boundaries_ready() -> bool:
                        with factory(current, {}, current, next_policy.profiles) as api:
                            api.check_inventory()
                            return bool(api.policy_ready())
                    if not await asyncio.to_thread(boundaries_ready):
                        return dict(base, status='pending', phase='build-policy', admission_open=False)
                    evidence = await release_readiness(policy=next_policy, platform_policy=platform_policy, snapshot=snapshot,
                        resources=current, namespace_uids=started.staged.plan['namespace_uids'],
                        foundation_identities=started.staged.plan['foundation_identities'], qualify=check,
                        kubernetes=kubernetes, cloud=cloud, public_http=public_http,
                        probe_connection=probe_connection, platform_probe_connection=platform_probe_connection)
                if not await apply(phase, selected_policy, expected_head, observed, evidence):
                    return dict(base, status='pending', phase=phase, admission_open=None)
            observed = await inspect(next_policy, 'current')
            if observed is None:
                return dict(base, status='pending', phase='current', admission_open=None)
            selected(observed, next_policy, uuid5(operation_id, 'activate'), 'active')
            _retain(directory / 'complete.json', {'plan_sha256': digest(plan), 'reference': cursor['activate']['reference']})
            check()
            return dict(base, status='complete', phase='active', admission_open=True,
                reference=cursor['activate']['reference'], binding_revision=next_policy.participant.binding_revision,
                publication_sha256=release.publication_sha256, inspection_sha256=digest(observed))
    except Exception:
        raise ValueError('environment_release_unqualified; preserve evidence') from None
