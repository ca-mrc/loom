"""Protected independent runtime staging; never starts work or opens admission.

The selected environment must be actually enrolled and still closed. The fixed
renderer, real publication resolver, foundation reader and read-only ledger Job
produce all inputs; a runner cannot supply manifests, paths or evidence hashes.
Stopped-workload staging is not installed readiness or activation authority.
"""
from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_build_cloud import (
    DevelopmentRegistryCloudScope,
    qualify_registry_cloud,
)
from scripts.ops.nebius_development_runtime_live import runtime_resource_path
from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec
from scripts.ops.nebius_environment_foundation import FoundationCloud, _read
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_runtime import (
    render_environment_runtime,
    render_runtime_material,
)
from scripts.ops.nebius_foundation_readiness import observe_foundation
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_authority_stage import _defaulted as authority_defaulted
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    _MARKER,
    ManagementStageAPI,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_stage import _defaulted as workload_defaulted
from scripts.ops.nebius_management_supplied import _defaulted as secret_defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
    _load,
)
from scripts.ops.nebius_pool_enrollment_issuer import (
    PoolEnrollmentPolicyV2,
    _inspect,
    _policy,
    _published_profiles,
    _scope,
)

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_kubernetes import NebiusKubernetesCredentials
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection
from loom_service.pool_management.lifecycle_contracts import PoolParticipantEnrollmentV2

ResourceConnection = Callable[[dict[str, dict[str, Any]], dict[str, str], Callable[[], None]], AbstractContextManager[ManagementStageAPI]]


def _defaulted(api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
    """Only inert defaults, never added grants or narrowed admission matching."""
    kind = desired['kind']
    if kind == 'Secret':
        return secret_defaulted(api, desired)
    if kind == 'Deployment':
        return workload_defaulted(api, desired)
    if kind in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}:
        return authority_defaulted(api, desired)
    observed = _snapshot(api.default_resource(desired))
    comparison = copy.deepcopy(observed)
    if kind in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
        qualify_build_policy_spec(desired, observed)
        comparison['spec'] = copy.deepcopy(desired['spec'])
    wanted = copy.deepcopy(desired)
    if kind == 'NetworkPolicy':
        for document in (comparison, wanted):
            for field in ('ingress', 'egress'):
                if document['spec'].get(field) == []:
                    document['spec'].pop(field)
    if comparison != wanted:
        raise ValueError('environment_runtime_defaulting_unqualified')
    return observed


def _uncreated_readers(readers: tuple[dict[str, Any], ...], files: dict[str, list[dict[str, Any]]],
        execution_namespace: str) -> tuple[dict[str, Any], ...]:
    foundation = {_key(doc): doc for documents in files.values() for doc in documents}
    result = []
    for doc in readers:
        if _key(doc) not in foundation:
            result.append(doc)
            continue
        original = foundation[_key(doc)]
        if (_key(doc) != f'ServiceAccount:{execution_namespace}:loom-execution-actuator'
                or {key: value for key, value in doc.items() if key != 'metadata'}
                    != {key: value for key, value in original.items() if key != 'metadata'}
                or original.get('automountServiceAccountToken') is not False):
            raise ValueError('environment_runtime_foundation_collision')
    return tuple(result)


class _Resources(ManagementKubernetesTransport):
    def __init__(self, *, policy: PoolEnrollmentPolicyV2, documents: dict[str, dict[str, Any]],
            namespaces: dict[str, str], qualify: Callable[[], None], credentials: NebiusKubernetesCredentials):
        self.policy, self.documents, self.namespaces, self.guard = policy, copy.deepcopy(documents), dict(namespaces), qualify
        super().__init__(api_server=policy.host.connection.endpoint,
            ssl_context=credentials.ssl_context, token=credentials.get_token_sync())

    def verify_identity(self, binding: ManagementBinding) -> None:
        if binding != self.policy.host.runtime.binding:
            raise ValueError('environment_runtime_connection_unqualified')
        self.guard()
        expected = {'kube-system': binding.kube_system_uid, **self.namespaces}
        for name, uid in expected.items():
            actual = self._request('GET', '/api/v1/namespaces/' + name)
            if (actual is None or actual.get('apiVersion') != 'v1' or actual.get('kind') != 'Namespace'
                    or actual['metadata']['name'] != name or _uid(actual) != uid
                    or actual['metadata'].get('deletionTimestamp') or actual['metadata'].get('ownerReferences')):
                raise ValueError('environment_runtime_namespace_unqualified')
        self.guard()

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        value = copy.deepcopy(document)
        expected = self.documents[_key(value)]
        annotations = value['metadata'].get('annotations', {})
        operation = annotations.pop(_MARKER, None)
        if writing or operation is not None:
            if str(UUID(operation)) != operation or not UUID(operation).int:
                raise ValueError('environment_runtime_intent_unqualified')
        if not annotations and 'annotations' not in expected['metadata']:
            value['metadata'].pop('annotations', None)
        if value != expected:
            raise ValueError('environment_runtime_intent_unqualified')
        self.verify_identity(self.policy.host.runtime.binding)
        return runtime_resource_path(expected)

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        return self._request('GET', self._approved(document) + '/' + document['metadata']['name'])

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        value = self._request('POST', self._approved(document, writing=True) + '?dryRun=All', document=document)
        if value is None:
            raise ValueError('environment_runtime_preview_unavailable')
        return value

    def create_resource(self, document: dict[str, Any]) -> None:
        self._request('POST', self._approved(document, writing=True), document=document)

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('database writes outside runtime staging')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('database writes outside runtime staging')


@contextmanager
def _resources(policy: PoolEnrollmentPolicyV2, documents: dict[str, dict[str, Any]],
        namespaces: dict[str, str], qualify: Callable[[], None]) -> Iterator[ManagementStageAPI]:
    private = {path: state._private_read(path, limit=1024**2)
        for path in (policy.host.connection.ca_file, policy.host.connection.credentials_file)}
    def unchanged() -> None:
        qualify()
        if any(state._private_read(path, limit=1024**2) != raw for path, raw in private.items()):
            raise ValueError('environment_runtime_connection_changed')
    credentials = NebiusKubernetesCredentials(policy.host.connection)
    try:
        with _Resources(policy=policy, documents=documents, namespaces=namespaces, qualify=unchanged, credentials=credentials) as api:
            yield api
            unchanged()
    finally:
        asyncio.run(credentials.close())


def _observe(policy: PoolEnrollmentPolicyV2, directory: Path, connect: Connection) -> dict[str, Any] | None:
    # Refresh a completed observation on each invocation; never renew a mutation
    # grant or silently change its expected participant/head. Pending read Jobs
    # retain their own identity, unlike fire-and-forget repeated inspections.
    with state._locked_state(directory):
        path = directory / 'current.json'
        current = _read(path) if path.exists() else None
        if current is None or current['status'] == 'complete':
            current = {'operation_id': str(uuid4()), 'status': 'pending'}
            state._atomic_json(path, current)
        identity = UUID(current['operation_id'])
        target = directory / str(identity)
        try:
            report = _inspect(policy, identity, target, connect)
        except ValueError as error:
            if str(error) != 'pool_enrollment_inspection_expired':
                raise
            expired = PoolAdministrationAuthorizationV2.model_validate(_read(target / 'inspection.json')['authorization'])
            if datetime.now(UTC) < expired.expires_at:
                raise
            # Keep the old Job and its identity for audit. Only this read-only
            # observation is replaced; mutation grants and runtime intent stay
            # byte-bound to their original operation and predecessor.
            identity = uuid4()
            current = {'operation_id': str(identity), 'status': 'pending'}
            state._atomic_json(path, current)
            target = directory / str(identity)
            report = _inspect(policy, identity, target, connect)
        if report is not None:
            current['status'] = 'complete'
            state._atomic_json(path, current)
            return {'report': report, 'input': _read(target / 'inspection.json')}
        return None


async def stage_environment_runtime(*, policy: PoolEnrollmentPolicyV2, enrollment: PoolAdministrationReferenceV2,
        operation_id: UUID, token: str, registry_scope: DevelopmentRegistryCloudScope, registry_credential: bytes,
        catalog: GitHubCandidateCatalog, kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud,
        public_http: httpx.AsyncClient, connect: Connection, registry_sdk: Any,
        registry_clients: dict[str, Any] | None = None, resource_connection: ResourceConnection | None = None) -> dict[str, Any] | None:
    """Stage fixed private material, readers/isolation, gateway and stopped worker.

    No PATCH/DELETE, quota relaxation, namespace PSA change, standalone collector,
    database migration, application restart or peer operation occurs here. The
    subsequent UID-bound startup/activation parent must consume this exact journal.
    """
    try:
        policy = copy.deepcopy(policy)
        policy_body = _policy(policy)
        _scope(policy)
        if not operation_id.int:
            raise ValueError
        original = _load(policy.host, enrollment)
        if (not isinstance(original.request, PoolParticipantEnrollmentV2)
                or original.request.participant != policy.participant or original.request.profiles != policy.profiles
                or original.request.machines != policy.machines):
            raise ValueError
        directory = policy.host.state_root / ('runtime-' + str(operation_id))
        state._private_directory(directory)
        observation = await asyncio.to_thread(_observe, policy, directory / 'observations', connect)
        if observation is None:
            return None
        snapshot = qualify_inspection(observation['report']['receipt'],
            PoolInspectionV2.model_validate(observation['input']['request']), UUID(observation['input']['authorization']['authorization_id']))
        selected = snapshot.participant
        if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                or selected.head.state != 'enrolled' or selected.head.operation_id != enrollment.operation_id
                or snapshot.pool_authority_sha256 != original.request.pool_authority_sha256):
            raise ValueError
        references = {item.candidate_id: item for item in (policy.application_publication, policy.manager_publication)}
        if any(catalog.publications.get(key) != value for key, value in references.items()):
            raise ValueError
        bundles = {key: await catalog.resolve(key) for key in references}
        app = bundles[policy.application_publication.candidate_id]
        foundation_plan = load_foundation(policy.foundation_dir, policy.foundation)
        if (digest(app.candidate) != foundation_plan['publication_sha256'] or digest(app.profile) != foundation_plan['profile_sha256']
                or policy.host.runtime.service_image != bundles[policy.manager_publication.candidate_id].candidate['images']['service']['image_ref']
                or str(kubernetes.http.base_url).rstrip('/') != policy.host.connection.endpoint.rstrip('/')):
            raise ValueError
        _published_profiles(policy, app, catalog, foundation_plan)
        foundation = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
            kubernetes=kubernetes, cloud=cloud, public_http=public_http)
        declaration = EnvironmentFoundationDeclarationV2.model_validate(foundation_plan['declaration'])
        if (str(declaration.kube_system_uid) != policy.host.runtime.binding.kube_system_uid
                or declaration.platform_config['kubernetes_api_server'].rstrip('/') != policy.host.connection.endpoint.rstrip('/')
                or (snapshot.pool.cluster_id, snapshot.pool.node_group_id) != (declaration.registration.cluster_id, declaration.registration.physical_pool_id)
                or any(foundation.foundation.namespace_uids[row.name] != str(row.uid)
                    for row in (policy.participant.execution_namespace, policy.participant.build_namespace))):
            raise ValueError
        machine, = (row for row in policy.machines if row.workload_scope == 'environment')
        runtime: dict[str, Any] = dict(declaration=declaration, participant=policy.participant, profiles=policy.profiles, machine=machine,
            candidate=app.candidate, runtime_profile=ServiceExecutionRuntimeProfileV1.model_validate(app.profile),
            management_origin=policy.management_origin,
            management_namespace=policy.host.runtime.binding.namespace, repo_root=Path(__file__).resolve().parents[2])
        rendered = render_environment_runtime(**runtime)
        material = _read(policy.foundation_dir / 'material.json')
        if digest(material) != foundation.foundation.identities['credentials:material']:
            raise ValueError
        credentials = render_runtime_material(runtime=runtime, foundation_material=material, token=token,
            registry_scope=registry_scope, registry_credential=registry_credential)
        repositories = tuple(sorted({row.settings.registry_repository for row in policy.profiles.task_images}
            | {row.settings.registry_repository for row in policy.profiles.application_images}))
        await qualify_registry_cloud(sdk=registry_sdk, scope=registry_scope, config=declaration.platform_config,
            credential=registry_credential, repositories=repositories, clients=registry_clients)
        readers = _uncreated_readers(rendered.readers, foundation_plan['files'], policy.participant.execution_namespace.name)
        phases = {'material': credentials, 'readers-isolation': (*readers, *rendered.isolation),
            'gateway-authority': rendered.gateway_authority,
            'workloads': tuple(doc for key, doc in rendered.workloads.items() if key not in {'service', 'control_plane'})}
        documents = {phase: {_key(doc): doc for doc in docs} for phase, docs in phases.items()}
        if any(len(documents[phase]) != len(docs) for phase, docs in phases.items()):
            raise ValueError
        namespaces = {**foundation.foundation.namespace_uids, policy.host.runtime.binding.namespace: policy.host.runtime.binding.namespace_uid}
        plan = {'schema_version': 'loom.environment-runtime-plan.v2', 'operation_id': str(operation_id),
            'policy': policy_body, 'enrollment': enrollment.model_dump(mode='json'), 'documents': documents,
            'registry_scope': registry_scope.model_dump(mode='json')}
        checksum = digest(plan)
        expires = min(snapshot.valid_until, foundation.foundation.observed_at + timedelta(minutes=5), *(row.expires_at for row in policy.machines))
        def qualify() -> None:
            if (not snapshot.observed_at <= datetime.now(UTC) < expires or _policy(policy) != policy_body
                    or _load(policy.host, enrollment) != original or load_foundation(policy.foundation_dir, policy.foundation) != foundation_plan
                    or _read(policy.foundation_dir / 'material.json') != material):
                raise ValueError('environment_runtime_evidence_changed')
        connection = resource_connection or (lambda docs, names, check: _resources(policy, docs, names, check))
        def stage() -> dict[str, Any]:
            with state._locked_state(directory):
                anchor = policy.host.anchor_root / ('runtime-' + str(operation_id) + '.json')
                plan_path, cursor_path = directory / 'plan.json', directory / 'cursor.json'
                identity = {'operation_id': str(operation_id), 'plan_sha256': checksum}
                if anchor.exists() or plan_path.exists() or cursor_path.exists():
                    if _read(anchor) != identity or _read(plan_path) != plan:
                        raise ValueError
                    cursor = _read(cursor_path)
                else:
                    state._write_private(plan_path, json.dumps(plan, sort_keys=True).encode())
                    cursor = {'phases': {phase: 'prepared' for phase in phases}}
                    state._atomic_json(cursor_path, cursor)
                    state._write_private(anchor, json.dumps(identity).encode())
                    state._sync_directory(policy.host.anchor_root)
                if set(cursor) != {'phases'} or set(cursor['phases']) != set(phases):
                    raise ValueError
                proofs = {}
                for phase, docs in documents.items():
                    qualify()
                    prior = cursor['phases'][phase]
                    child = directory / phase
                    if prior not in {'prepared', 'intent', 'complete'} or (prior != 'prepared' and not (child / 'stage.json').exists()):
                        raise ValueError
                    if prior == 'prepared':
                        if child.exists():
                            raise ValueError
                        cursor['phases'][phase] = 'intent'
                        state._atomic_json(cursor_path, cursor)
                    with connection(docs, namespaces, qualify) as api:
                        proofs[phase] = _stage_fixed_documents(documents=docs, revision='sha256:' + checksum, phase='environment-runtime-' + phase,
                            binding=policy.host.runtime.binding, api=api, state_dir=child,
                            default_document=_defaulted)
                    cursor['phases'][phase] = 'complete'
                    state._atomic_json(cursor_path, cursor)
                qualify()
                return {'schema_version': 'loom.environment-runtime-staged.v2', 'operation_id': str(operation_id),
                    'participant_id': str(policy.participant.participant_id), 'plan_sha256': checksum,
                    'status': 'runtime_staged_closed', 'admission_open': False,
                    'resource_uids': {phase: proof['resource_uids'] for phase, proof in proofs.items()}}
        return await asyncio.to_thread(stage)
    except Exception:
        raise ValueError('environment_runtime_install_unqualified') from None
