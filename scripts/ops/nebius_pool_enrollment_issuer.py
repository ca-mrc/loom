"""Protected enrollment issuance from actual publication, foundation and SQL reads.

This issuer grants only closed enrollment, never activation. Its policy/clients
are installed private inputs, not runner-supplied documents or proof digests.
The existing fixed Job/dispatch performs mutations only after this qualification.
"""
from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid5

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_foundation import FoundationCloud, FoundationReferenceV2
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_foundation_readiness import FoundationReadinessV2, observe_foundation
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationHostV2,
    PoolAdministrationReferenceV2,
    _host,
    _load,
    retain_authorized_operation,
)
from scripts.ops.nebius_pool_administration_job import (
    PoolAdministrationJobV2,
    stage_administration_job,
)

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_pool_contract import PoolParticipantV1
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _json,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.manager import CandidateBundle
from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection
from loom_service.pool_management.installation import PoolMachineInstallation
from loom_service.pool_management.lifecycle_contracts import PoolParticipantEnrollmentV2
from loom_service.pool_management.profile_registry import _validate_catalog
from loom_service.pool_management.profiles import PoolProfileCatalog


@dataclass(frozen=True, repr=False)
class PoolEnrollmentPolicyV2:
    host: PoolAdministrationHostV2
    management_origin: str
    foundation_dir: Path
    foundation: FoundationReferenceV2
    application_publication: ProtectedPublication
    manager_publication: ProtectedPublication
    participant: PoolParticipantV1
    profiles: PoolProfileCatalog
    machines: tuple[PoolMachineInstallation, ...]


def _policy(policy: PoolEnrollmentPolicyV2) -> dict[str, Any]:
    _host(policy.host)
    origin = urlsplit(policy.management_origin)
    if (origin.scheme != 'https' or not origin.hostname or origin.username or origin.password
            or origin.path not in {'', '/'} or origin.query or origin.fragment):
        raise ValueError('pool_enrollment_policy_unqualified')
    if (not policy.foundation_dir.is_absolute() or policy.foundation_dir != policy.foundation_dir.resolve()
            or policy.foundation_dir == Path('/') or policy.foundation_dir in policy.host.state_root.parents
            or policy.host.state_root in policy.foundation_dir.parents):
        raise ValueError('pool_enrollment_policy_unqualified')
    body = dict(host={'runtime': asdict(policy.host.runtime), 'scope': policy.host.scope.model_dump(mode='json'),
        'connection': policy.host.connection.model_dump(mode='json'), 'state_root': str(policy.host.state_root),
        'anchor_root': str(policy.host.anchor_root)}, foundation_dir=str(policy.foundation_dir),
        foundation=policy.foundation.model_dump(mode='json'), participant=policy.participant.model_dump(mode='json'),
        profiles=policy.profiles.model_dump(mode='json'), machines=[row.model_dump(mode='json') for row in policy.machines],
        application_publication=policy.application_publication.model_dump(mode='json'),
        manager_publication=policy.manager_publication.model_dump(mode='json'), management_origin=policy.management_origin)
    return _json(json.dumps(body, allow_nan=False).encode())


def _grant(policy: PoolEnrollmentPolicyV2, request: Any, identity: UUID, *,
        issued_at: datetime, expires_at: datetime) -> PoolAdministrationAuthorizationV2:
    return PoolAdministrationAuthorizationV2(schema_version='loom.pool-administration-authorization.v2',
        authorization_id=identity, **policy.host.scope.model_dump(), request_sha256=digest(request.model_dump(mode='json')),
        issued_at=issued_at, expires_at=expires_at)


def _scope(policy: PoolEnrollmentPolicyV2) -> None:
    host, row, selected = policy.host, policy.foundation.identity, policy.participant
    expected = dict(installation_id=row.installation_id, pool_id=row.pool_id, participant_id=row.participant_id,
        environment_id=row.environment_id, incarnation=row.incarnation, authority=row.kind)
    if (host.scope.model_dump() != expected or {name: getattr(selected, name) for name in expected if name != 'authority'}
            != {name: value for name, value in expected.items() if name != 'authority'}
            or selected.environment_class != row.kind or selected.legacy_catalog_environment is not None
            or selected.binding_revision != 1 or selected.execution_namespace.name != row.execution_namespace
            or selected.build_namespace.name != row.build_namespace):
        raise ValueError('pool_enrollment_scope_unqualified')
    # Promotion must be separately verified, not inferred from a dev publication
    # or the ability to choose a production-looking hostname. No production
    # enrollment is issued until that separate protected adapter is connected.
    if row.kind == 'production':
        raise ValueError('pool_enrollment_production_promotion_required')


def _published_profiles(policy: PoolEnrollmentPolicyV2, app: CandidateBundle, catalog: GitHubCandidateCatalog,
        plan: dict[str, Any]) -> None:
    runtime = ServiceExecutionRuntimeProfileV1.model_validate(app.profile)
    keyring = ImageAdmissionKeyring.from_json(json.dumps(policy.profiles.image_admission_keyring))
    if {name: key.public_bytes_raw() for name, key in keyring._keys.items()} != {
            name: key.public_bytes_raw() for name, key in catalog.keyring._keys.items()}:
        raise ValueError('pool_enrollment_trust_unqualified')
    config, = (doc for docs in plan['files'].values() for doc in docs
        if doc['kind'] == 'ConfigMap' and doc['metadata']['name'] == 'loom-platform-config')
    classes = {}
    for name in ('catalog.json', 'guest-catalog.json', 'emulated-auth-catalog.json'):
        if name in config['data']:
            declared = _json(config['data'][name].encode())
            for target in declared['topology']['targets']:
                classes[target['target_id']] = declared['execution_class']
    for row in policy.profiles.execution:
        if (row.candidate_sha, row.runtime_image_ref, row.runtime_binary_sha256) != (
                runtime.candidate_sha, runtime.runtime_image_ref, runtime.runtime_binary_sha256):
            raise ValueError('pool_enrollment_profile_unqualified')
        if classes.get(row.runtime.target_id) != row.execution_class.model_dump(mode='json'):
            raise ValueError('pool_enrollment_capability_unqualified')
    settings = [row.settings for row in policy.profiles.task_images]
    settings.extend(row.settings for row in policy.profiles.application_images)
    for build in settings:
        if build.service_image != app.candidate['images']['service']['image_ref']:
            raise ValueError('pool_enrollment_profile_unqualified')
    policy.profiles.profiles()


def _retain_issued(policy: PoolEnrollmentPolicyV2, operation_id: UUID, retained: dict[str, Any]) -> PoolAdministrationReferenceV2:
    if set(retained) != {'request', 'authorization', 'evidence'}:
        raise ValueError('pool_enrollment_issuance_conflict')
    request = PoolParticipantEnrollmentV2.model_validate(retained['request'])
    grant = PoolAdministrationAuthorizationV2.model_validate(retained['authorization'])
    evidence = retained['evidence']
    foundation = FoundationReadinessV2.model_validate(evidence['foundation'])
    inspection = PoolInspectionV2(schema_version='loom.pool-inspection.v2', operation_id=uuid5(operation_id, 'inspect'),
        installation_id=policy.participant.installation_id, pool_id=policy.participant.pool_id,
        participant_id=policy.participant.participant_id, environment_id=policy.participant.environment_id,
        incarnation=policy.participant.incarnation, environment_class=policy.participant.environment_class)
    snapshot = qualify_inspection(evidence['inspection']['receipt'], inspection, uuid5(operation_id, 'inspect-authorization'))
    expected = PoolParticipantEnrollmentV2(schema_version='loom.pool-participant-enrollment.v2', operation_id=operation_id,
        participant=policy.participant, machines=policy.machines, profiles=policy.profiles,
        pool_authority_sha256=snapshot.pool_authority_sha256, foundation_sha256=digest(foundation.model_dump(mode='json')))
    if (request != expected or foundation.foundation.identity != policy.foundation.identity
            or foundation.foundation.plan_sha256 != policy.foundation.plan_sha256
            or grant != _grant(policy, expected, uuid5(operation_id, 'enroll-authorization'),
                issued_at=grant.issued_at, expires_at=grant.expires_at)
            or grant.issued_at < max(snapshot.observed_at, foundation.verified_at)
            or grant.expires_at > min(snapshot.valid_until, foundation.foundation.observed_at + timedelta(minutes=5),
                *(row.expires_at for row in policy.machines))):
        raise ValueError('pool_enrollment_issuance_conflict')
    return retain_authorized_operation(host=policy.host, request=request, authorization=grant, evidence_sha256=digest(evidence))


def _inspect(policy: PoolEnrollmentPolicyV2, operation_id: UUID, directory: Path, connect: Connection, *,
        require_capacity: bool = False, platform_authorization_id: UUID | None = None) -> dict[str, Any] | None:
    selected = policy.participant
    request = PoolInspectionV2(schema_version='loom.pool-inspection.v2', operation_id=uuid5(operation_id, 'inspect'),
        installation_id=selected.installation_id, pool_id=selected.pool_id, participant_id=selected.participant_id,
        environment_id=selected.environment_id, incarnation=selected.incarnation, environment_class=selected.environment_class,
        require_capacity=require_capacity, platform_authorization_id=platform_authorization_id)
    with state._locked_state(directory):
        path = directory / 'inspection.json'
        if path.exists():
            retained = _json(state._private_read(path))
            if retained['request'] != request.model_dump(mode='json'):
                raise ValueError('pool_enrollment_inspection_conflict')
            grant = PoolAdministrationAuthorizationV2.model_validate(retained['authorization'])
        else:
            now = datetime.now(UTC)
            grant = _grant(policy, request, uuid5(operation_id, 'inspect-authorization'), issued_at=now, expires_at=now + timedelta(minutes=5))
            state._write_private(path, json.dumps({'request': request.model_dump(mode='json'),
                'authorization': grant.model_dump(mode='json')}, sort_keys=True).encode())
            state._sync_directory(directory)
        if not grant.issued_at <= datetime.now(UTC) < grant.expires_at:
            raise ValueError('pool_enrollment_inspection_expired')
        job = PoolAdministrationJobV2(runtime=policy.host.runtime, request=request, authorization=grant,
            action='status', observation_id=uuid5(operation_id, 'inspect-observation'))
        with connect(job) as api:
            stage_administration_job(request=job, api=api, state_dir=directory / 'inspection')
            report = api.administration_report(directory / 'inspection')
        if report is not None:
            if set(report) != {'job_uid', 'pod_uid', 'receipt'} or any(
                    str(UUID(report[key])) != report[key] or not UUID(report[key]).int for key in ('job_uid', 'pod_uid')):
                raise ValueError('pool_enrollment_inspection_unqualified')
            qualify_inspection(report['receipt'], request, grant.authorization_id)
        return report


async def issue_enrollment(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID, catalog: GitHubCandidateCatalog,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud, public_http: httpx.AsyncClient,
        connect: Connection) -> PoolAdministrationReferenceV2 | None:
    """None means the original read-only inspection Job is still pending.

    Exact issuance replay returns its retained grant without observing a new
    predecessor or refreshing expiry. An expired/failed attempt needs a new
    explicit operation; it is never silently retried as a new mutation.
    """
    try:
        policy = copy.deepcopy(policy)
        body = _policy(policy)
        _scope(policy)
        if not operation_id.int:
            raise ValueError
        state._private_directory(policy.host.state_root)
        directory = policy.host.state_root / ('enrollment-' + str(operation_id))
        with state._locked_state(directory):
            intent = directory / 'policy.json'
            if intent.exists():
                if _json(state._private_read(intent, limit=4 * 1024**2)) != body:
                    raise ValueError('pool_enrollment_intent_conflict')
            else:
                if (directory / 'inspection').exists() or (directory / 'issued.json').exists():
                    raise ValueError('pool_enrollment_intent_missing')
                state._write_private(intent, json.dumps(body, sort_keys=True).encode())
                state._sync_directory(directory)
            issued = directory / 'issued.json'
            if issued.exists():
                retained = _json(state._private_read(issued, limit=4 * 1024**2))
                return _retain_issued(policy, operation_id, retained)
        publications = {row.candidate_id: row for row in (policy.application_publication, policy.manager_publication)}
        if any(catalog.publications.get(row.candidate_id) != row for row in (policy.application_publication, policy.manager_publication)):
            raise ValueError('pool_enrollment_publication_unqualified')
        bundles = {identity: await catalog.resolve(identity) for identity in publications}
        app, manager = bundles[policy.application_publication.candidate_id], bundles[policy.manager_publication.candidate_id]
        if policy.host.runtime.service_image != manager.candidate['images']['service']['image_ref']:
            raise ValueError('pool_enrollment_manager_image_unqualified')
        plan = load_foundation(policy.foundation_dir, policy.foundation)
        if (plan['publication_sha256'] != digest(app.candidate) or plan['profile_sha256'] != digest(app.profile)
                or plan['declaration']['kube_system_uid'] != policy.host.runtime.binding.kube_system_uid
                or json.loads(plan['declaration']['platform_config_json'])['kubernetes_api_server'].rstrip('/')
                    != policy.host.connection.endpoint.rstrip('/')
                or str(kubernetes.http.base_url).rstrip('/') != policy.host.connection.endpoint.rstrip('/')):
            raise ValueError('pool_enrollment_foundation_publication_unqualified')
        _published_profiles(policy, app, catalog, plan)
        report = await asyncio.to_thread(_inspect, policy, operation_id, directory, connect)
        if report is None:
            return None
        inspection = _json(state._private_read(directory / 'inspection.json'))
        snapshot = qualify_inspection(report['receipt'], PoolInspectionV2.model_validate(inspection['request']),
            UUID(inspection['authorization']['authorization_id']))
        selected, identity = policy.participant, policy.foundation.identity
        if (snapshot.participant is not None or (snapshot.pool.cluster_id, snapshot.pool.node_group_id) != (
                identity.cluster_id, identity.physical_pool_id)):
            raise ValueError('pool_enrollment_pool_unqualified')
        _validate_catalog(snapshot.pool.row(), selected, policy.profiles)
        foundation = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
            kubernetes=kubernetes, cloud=cloud, public_http=public_http)
        if any(foundation.foundation.namespace_uids[row.name] != str(row.uid)
                for row in (selected.execution_namespace, selected.build_namespace)):
            raise ValueError('pool_enrollment_namespace_unqualified')
        now = datetime.now(UTC)
        expiry = min(snapshot.valid_until, foundation.foundation.observed_at + timedelta(minutes=5),
            *(row.expires_at for row in policy.machines))
        if not snapshot.observed_at <= now < expiry or foundation.foundation.observed_at > now:
            raise ValueError('pool_enrollment_evidence_expired')
        request = PoolParticipantEnrollmentV2(schema_version='loom.pool-participant-enrollment.v2',
            operation_id=operation_id, participant=selected, machines=policy.machines, profiles=policy.profiles,
            pool_authority_sha256=snapshot.pool_authority_sha256, foundation_sha256=digest(foundation.model_dump(mode='json')))
        grant = _grant(policy, request, uuid5(operation_id, 'enroll-authorization'), issued_at=now, expires_at=expiry)
        evidence = {'foundation': foundation.model_dump(mode='json'), 'inspection': report,
            'publications': [row.model_dump(mode='json') for row in publications.values()]}
        with state._locked_state(directory):
            # A concurrent same-ID preparation must return the original intent,
            # not overwrite evidence, timestamps, grant or predecessor.
            if _json(state._private_read(intent, limit=4 * 1024**2)) != body:
                raise ValueError('pool_enrollment_intent_conflict')
            if issued.exists():
                retained = _json(state._private_read(issued, limit=4 * 1024**2))
            else:
                retained = {'request': request.model_dump(mode='json'), 'authorization': grant.model_dump(mode='json'), 'evidence': evidence}
                state._write_private(issued, json.dumps(retained, sort_keys=True).encode())
                state._sync_directory(directory)
            reference = _retain_issued(policy, operation_id, retained)
            _load(policy.host, reference)
            return reference
    except Exception:
        raise ValueError('pool_enrollment_evidence_unqualified') from None
