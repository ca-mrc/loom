"""Legacy shared-development ancestry and exact imported-foundation readback.

The source selector names a completed, anchored shared-pool handoff, never a
namespace or a caller-authored resource inventory. Historical manager resources
prove ancestry only: the imported plan contains this participant's data and
runtime resources, and cannot authorize a manager upgrade or a writer transfer.
Installed generic snapshots use the same GET-only observer with separately
qualified cloud identities; they do not claim shared-handoff provenance.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_bootstrap import _namespace_document, _secret_documents
from scripts.ops.nebius_development_cloud import DevelopmentCloudScope, qualify_development_cloud
from scripts.ops.nebius_environment_foundation import (
    FoundationObservationV2,
    FoundationReferenceV2,
    _read,
    _retain,
)
from scripts.ops.nebius_environment_import_cloud import ImportedCloudScopeV2, qualify_imported_cloud
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_shared_pool_build_activation import shared_forward_build_namespace_view
from scripts.ops.nebius_shared_pool_entry import load_completed_history
from scripts.ops.nebius_shared_pool_forward_census import _forward_options

from loom.nebius_environment_foundation import (
    EnvironmentFoundationDeclarationV2,
    EnvironmentFoundationIdentityV2,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.registry import OperationLease
from loom_service.pool_management.capacity import digest

_LIMIT = 8 * 1024**2


class LegacyFoundationReferenceV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    kind: Literal['shared-pool-handoff-v1'] = 'shared-pool-handoff-v1'
    operation_path: Path
    operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    participant_id: UUID

    @field_validator('operation_path')
    @classmethod
    def canonical(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve() or value.name != 'operation.json':
            raise ValueError('legacy_foundation_source_invalid')
        return value


class LegacyFoundationCloud(Protocol):
    async def qualify_legacy_foundation(self, *, scope: DevelopmentCloudScope,
        config: dict[str, Any], material: dict[str, str]) -> None: ...

    async def qualify_installed_foundation(self, *, scope: ImportedCloudScopeV2,
        config: dict[str, Any], material: dict[str, str]) -> None: ...


class NebiusFoundationCloud(NebiusSdkEnvironmentApi):
    """Current protected SDK credentials, never the source's retired credentials."""

    def __init__(self, sdk: Any, *, clients: dict[str, Any] | None = None,
            legacy_clients: dict[str, Any] | None = None, qualify_mutation: Callable[[], None] | None = None):
        super().__init__(sdk, clients=clients)
        self.sdk, self.legacy_clients = sdk, legacy_clients
        self.qualify_mutation = qualify_mutation

    async def _call(self, method: Any, request: Any, *, key: str | None = None, missing: bool = False) -> Any:
        # Every SDK create/delete (including key revocation) carries its frozen
        # idempotency key. Read-only token/resource qualification remains usable
        # before production contexts exist and after they expire.
        if key is not None and self.qualify_mutation is not None:
            self.qualify_mutation()
        return await super()._call(method, request, key=key, missing=missing)

    async def qualify_legacy_foundation(self, *, scope: DevelopmentCloudScope,
            config: dict[str, Any], material: dict[str, str]) -> None:
        await qualify_development_cloud(sdk=self.sdk, scope=scope, config=config,
            material=material, pending_storage_mib=0, clients=self.legacy_clients)

    async def qualify_installed_foundation(self, *, scope: ImportedCloudScopeV2,
            config: dict[str, Any], material: dict[str, str]) -> None:
        await qualify_imported_cloud(sdk=self.sdk, scope=scope, config=config,
            material=material, clients=self.legacy_clients)


def _source(source: LegacyFoundationReferenceV1, operation_id: UUID) -> tuple[dict[str, Any], dict[str, Any]]:
    """Consume real terminal history once; normal imported reads need no peer history."""
    source = LegacyFoundationReferenceV1.model_validate(source.model_dump())
    historical, completed = load_completed_history(operation_path=source.operation_path,
        operation_sha256=source.operation_sha256)
    binding, handoff = historical.bindings, historical.bindings.plan
    database = historical.runtime.database
    foundation = database.foundation
    spec = completed.migration.registration.spec
    participant, = (row for row in spec.participants if row.participant_id == source.participant_id)
    # This is the retained full dev installer ancestry. A staging attachment
    # contains workload observations, but no complete Secret/PVC provenance;
    # it must never be promoted to a foundation import by name or class alone.
    if (participant.environment_class != 'development'
            or participant.environment_id != UUID(foundation.binding.bootstrap.installation_id)
            or foundation.inputs.config['namespace'] != 'loom-dev'
            or participant.execution_namespace.name != foundation.inputs.config['execution_namespace']):
        raise ValueError('legacy_foundation_source_unsupported')
    originals: dict[str, dict[str, Any]] = {}

    def add(document: dict[str, Any], uid: str | None = None) -> None:
        row = copy.deepcopy(document)
        if uid is not None:
            row['metadata']['uid'] = uid
        _uid(row)
        if row['kind'] == 'Namespace':
            selected = row['metadata']['name'] in {'loom-dev', participant.execution_namespace.name, participant.build_namespace.name}
        else:
            selected = row['metadata'].get('namespace') in {'loom-dev', participant.execution_namespace.name, participant.build_namespace.name}
        if selected:
            if _key(row) in originals and _uid(originals[_key(row)]) != _uid(row):
                raise ValueError('legacy_foundation_resource_replaced')
            originals[_key(row)] = row

    add(_namespace_document(foundation.inputs.binding, foundation.binding.operation_id), foundation.binding.namespace_uid)
    material = copy.deepcopy(foundation.bootstrap['material'])
    for name, document in _secret_documents(material, foundation.inputs.binding, foundation.binding.operation_id).items():
        add(document, foundation.bootstrap['secrets'][name]['uid'])
    for phase, journal in foundation.phases.items():
        if phase == 'storage':
            continue
        for item in journal['resources'].values():
            add(item['observed'], item['uid'])
    storage = foundation.phases['storage']
    add({'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'metadata': {
        'name': 'data-loom-postgres-0', 'namespace': 'loom-dev',
        'labels': {'loom.nebius/development-installation': foundation.binding.bootstrap.installation_id}},
        'spec': storage['pvc_spec']}, storage['pvc_uid'])
    for document in handoff.effective_runtime_resources.values():
        add(document)
    frame, workloads, authority, _ = _forward_options(binding, completed.state_dir)
    build_frame, _, namespaces = shared_forward_build_namespace_view(bindings=binding, state_dir=completed.state_dir)
    if frame != build_frame or frame['outcome'] != 'shared_pool_handoff_complete':
        raise ValueError('legacy_foundation_handoff_incomplete')
    for choices in {**workloads, **authority, **namespaces}.values():
        successor, = choices
        if successor is None:
            raise ValueError('legacy_foundation_handoff_incomplete')
        add(successor)
    for namespace, uid in ((participant.execution_namespace.name, str(participant.execution_namespace.uid)),
            (participant.build_namespace.name, str(participant.build_namespace.uid))):
        if _uid(originals['Namespace:-:' + namespace]) != uid:
            raise ValueError('legacy_foundation_namespace_conflict')
    material['loom-platform-storage'] = {key: base64.b64decode(value, validate=True).decode()
        for key, value in originals['Secret:loom-dev:loom-platform-storage']['data'].items()}
    config = copy.deepcopy(foundation.inputs.config)
    identity = EnvironmentFoundationIdentityV2(schema_version='loom.environment-foundation-identity.v2',
        environment_id=participant.environment_id, incarnation=participant.incarnation, participant_id=participant.participant_id,
        installation_id=spec.installation_id, pool_id=spec.pool_id, kind=participant.environment_class,
        slug='dev', cluster_id=spec.cluster_id, physical_pool_id=spec.node_group_id,
        application_namespace='loom-dev', execution_namespace=participant.execution_namespace.name,
        build_namespace=participant.build_namespace.name, target_id=config['target_id'], public_host=config['public_host'])
    projected = copy.deepcopy(config)
    projected.update(schema_version='loom.nebius-environment-platform.v2', registration=identity.model_dump(mode='json'),
        public_tls_bootstrap=False)
    projected['capacity_policy']['enabled'] = False
    for key in ('task_image_builder', 'shared_ingress_enabled'):
        projected.pop(key, None)
    placement = originals['StatefulSet:loom-dev:loom-postgres']['spec']['template']['spec']
    ingress = database.manager.deployment.installation.foundation
    declaration = EnvironmentFoundationDeclarationV2(schema_version='loom.environment-foundation.v2', operation_id=operation_id,
        kube_system_uid=UUID(foundation.binding.bootstrap.kube_system_uid), registration=identity,
        platform_config_json=json.dumps(projected), public_dns_zone=identity.public_host.partition('.')[2],
        ingress_class_name=ingress.ingress_class_name, ingress_namespace=ingress.ingress_namespace,
        ingress_controller_label=ingress.ingress_controller_label, node_selector=placement['nodeSelector'],
        tolerations=tuple(placement.get('tolerations', [])), backups_enabled=False)
    files: dict[str, list[dict[str, Any]]] = {name: [] for name in ('00-namespaces.yaml', '10-config-network.yaml',
        '20-database.yaml', '30-migrate.yaml', '40-services.yaml', '50-configure.yaml', '60-execution.yaml', '70-public.yaml', '80-backup.yaml')}
    for key, document in originals.items():
        if document['kind'] == 'Namespace':
            filename = '00-namespaces.yaml'
        elif document['kind'] in {'StatefulSet', 'PersistentVolumeClaim'} or key == 'Service:loom-dev:loom-postgres':
            filename = '20-database.yaml'
        elif document['kind'] == 'Deployment' and document['metadata']['namespace'] == 'loom-dev':
            filename = '40-services.yaml'
        else:
            filename = '60-execution.yaml' if document['metadata'].get('namespace') != 'loom-dev' else '10-config-network.yaml'
        files[filename].append(copy.deepcopy(document))
    history = {str(path): checksum for path, checksum in completed.history.items()}
    for path, raw in historical.files.items():
        checksum = hashlib.sha256(raw).hexdigest()
        if str(path) in history and history[str(path)] != checksum:
            raise ValueError('legacy_foundation_history_changed')
        history[str(path)] = checksum
    if any(hashlib.sha256(state._private_read(Path(path), limit=16 * 1024**2)).hexdigest() != checksum
            for path, checksum in history.items()):
        raise ValueError('legacy_foundation_history_changed')
    publication = database.data_runtime.bundle
    identities = {'k8s:' + key: _uid(document) for key, document in originals.items()}
    if len(set(identities.values())) != len(identities):
        raise ValueError('legacy_foundation_resource_ambiguous')
    identities['credentials:material'] = digest(material)
    plan = dict(schema_version='loom.foundation-plan.v2', operation_id=str(operation_id), mode='legacy-import',
        declaration=declaration.model_dump(mode='json'), files=files, publication_sha256=digest(publication.candidate),
        profile_sha256=digest(publication.profile), import_identities=identities, material_sha256=digest(material),
        legacy=dict(source=source.model_dump(mode='json'), history=history, config=config,
            cloud_scope=foundation.inputs.settings.cloud.model_dump(mode='json'), resources=originals,
            volume=dict(name=storage['pv_name'], uid=storage['pv_uid'], spec=storage['pv_spec'])))
    return plan, material


def prepare_legacy_foundation_import(*, state_dir: Path, source: LegacyFoundationReferenceV1,
        operation_id: UUID) -> FoundationReferenceV2:
    """Freeze qualified deployed bytes locally; perform no provider effect."""
    try:
        if not operation_id.int or not source.participant_id.int:
            raise ValueError
        with state._locked_state(state_dir):
            path = state_dir / 'plan.json'
            if path.exists():
                plan = _read(path)
                if (plan.get('mode') != 'legacy-import' or plan['operation_id'] != str(operation_id)
                        or plan['legacy']['source'] != source.model_dump(mode='json')
                        or digest(_read(state_dir / 'material.json')) != plan['material_sha256']):
                    raise ValueError
            else:
                plan, material = _source(source, operation_id)
                if state_dir.resolve() in {Path(path).parent for path in plan['legacy']['history']}:
                    raise ValueError
                _retain(state_dir / 'material.json', material)
                _retain(path, plan)
            return FoundationReferenceV2(operation_id=operation_id,
                identity=EnvironmentFoundationIdentityV2.model_validate(plan['declaration']['registration']), plan_sha256=digest(plan))
    except Exception:
        raise ValueError('legacy_foundation_import_unqualified') from None


async def observe_legacy_foundation(*, state_dir: Path, reference: FoundationReferenceV2, plan: dict[str, Any],
        kubernetes: KubernetesEnvironmentProvider, cloud: LegacyFoundationCloud,
        successors: dict[str, tuple[dict[str, Any], ...]] | None = None) -> FoundationObservationV2:
    """GET-only current identities; import never claims closed intake or activation."""
    try:
        observed_at = datetime.now(UTC)
        declaration = EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
        row, legacy = reference.identity, plan['legacy']
        if str(kubernetes.http.base_url).rstrip('/') != declaration.platform_config['kubernetes_api_server'].rstrip('/'):
            raise ValueError
        identities = plan['import_identities']
        resources = legacy['resources']
        if identities != {'credentials:material': plan['material_sha256'], **{'k8s:' + key: _uid(doc) for key, doc in resources.items()}}:
            raise ValueError
        material = _read(state_dir / 'material.json')
        if digest(material) != plan['material_sha256']:
            raise ValueError
        allowed = {'k8s:' + key for key in resources}
        if successors is not None and (not successors or set(successors) - allowed):
            raise ValueError
        for key, choices in (successors or {}).items():
            if not 1 <= len(choices) <= 2 or any('k8s:' + _key(doc) != key or _uid(doc) != identities[key] for doc in choices):
                raise ValueError
            for desired in choices:
                _qualify_successor(resources[key.removeprefix('k8s:')], desired, application_namespace=row.application_namespace)
        context = ProvisioningContext(OperationLease(reference.operation_id, row.environment_id, 1, 1, reference.operation_id),
            row.model_dump(mode='json'), declaration.platform_config, identities)

        async def cluster() -> None:
            current = await kubernetes._request('GET', '/api/v1/namespaces/kube-system')
            if (current is None or current.get('apiVersion') != 'v1' or current.get('kind') != 'Namespace'
                    or current['metadata'].get('name') != 'kube-system' or _uid(current) != str(declaration.kube_system_uid)
                    or current['metadata'].get('deletionTimestamp') or current['metadata'].get('ownerReferences')):
                raise ValueError

        await cluster()
        installed = legacy['source']['kind'] == 'installed-foundation-v2'
        if installed:
            await cloud.qualify_installed_foundation(scope=ImportedCloudScopeV2.model_validate(legacy['cloud_scope']),
                config=legacy['config'], material=material['loom-platform-storage'])
        else:
            await cloud.qualify_legacy_foundation(scope=DevelopmentCloudScope.model_validate(legacy['cloud_scope']),
                config=legacy['config'], material=material['loom-platform-storage'])
        for key, original in resources.items():
            if original['kind'] in {'ClusterRole', 'ClusterRoleBinding', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
                from scripts.ops.nebius_development_runtime_live import runtime_resource_path
                path = runtime_resource_path(original) + '/' + original['metadata']['name']
            else:
                _, path = kubernetes._path(context, original)
            actual = await kubernetes._request('GET', path)
            choices = (successors or {}).get('k8s:' + key, (original,))
            if actual is None:
                raise ValueError
            if original['kind'] == 'PersistentVolumeClaim':
                kubernetes._identity(actual, original, _uid(original))
                if actual['spec'] != original['spec'] or actual.get('status', {}).get('phase') != 'Bound':
                    raise ValueError
            elif not installed and original['kind'] == 'Namespace' and original['metadata']['name'] == row.application_namespace:
                # The original bootstrap journal predates API defaulting.
                normalized = _snapshot(actual)
                if normalized.get('spec', {}) not in ({}, {'finalizers': ['kubernetes']}):
                    raise ValueError
                normalized.pop('spec', None)
                labels = normalized['metadata'].get('labels', {})
                if labels.pop('kubernetes.io/metadata.name', row.application_namespace) != row.application_namespace:
                    raise ValueError
                if normalized != _snapshot(original) or _uid(actual) != _uid(original):
                    raise ValueError
            elif not any(_matches(actual, desired, _uid(original)) for desired in choices):
                raise ValueError
        volume = legacy['volume']
        actual_volume = await kubernetes._request('GET', '/api/v1/persistentvolumes/' + volume['name'])
        if (actual_volume is None or actual_volume.get('apiVersion') != 'v1' or actual_volume.get('kind') != 'PersistentVolume'
                or actual_volume['metadata'].get('name') != volume['name'] or _uid(actual_volume) != volume['uid']
                or actual_volume.get('spec') != volume['spec'] or actual_volume.get('status', {}).get('phase') != 'Bound'
                or actual_volume['metadata'].get('deletionTimestamp') or actual_volume['metadata'].get('ownerReferences')):
            raise ValueError
        await cluster()
        if digest(_read(state_dir / 'material.json')) != plan['material_sha256']:
            raise ValueError
        result = FoundationObservationV2(operation_id=reference.operation_id, identity=row, plan_sha256=reference.plan_sha256,
            observed_at=observed_at, phase='legacy_bindings_verified', execution_enabled=None,
            namespace_uids={name: identities['k8s:Namespace:-:' + name] for name in row.namespaces}, identities=identities)
        if successors is None:
            state._atomic_json(state_dir / 'observed.json', result.model_dump(mode='json'))
        return result
    except Exception:
        raise ProviderBlockedError('legacy_foundation_observation_unqualified') from None


def _qualify_successor(original: dict[str, Any], desired: dict[str, Any], *, application_namespace: str) -> None:
    """The release parent binds exact journals; keep data and authority immutable."""
    if _snapshot(original) == _snapshot(desired):
        return
    if original['kind'] == 'Deployment':
        return
    if (_key(original) == f'ConfigMap:{application_namespace}:loom-platform-config'):
        from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

        publication = {'candidate_sha', 'task_image_ref', 'agent_image_ref', 'runtime_image_ref',
            'runtime_binary_sha256', 'image_admission', 'prebuilt_image_pins'}
        before, after = copy.deepcopy(original), copy.deepcopy(desired)
        old = before['data'].pop('profile.json', None)
        new = ServiceExecutionRuntimeProfileV1.model_validate_json(after['data'].pop('profile.json'))
        if old is not None and ServiceExecutionRuntimeProfileV1.model_validate_json(old).model_dump(exclude=publication) != new.model_dump(exclude=publication):
            raise ValueError('legacy_foundation_profile_successor_invalid')
        if _snapshot(before) == _snapshot(after):
            return
    if original['kind'] == 'ValidatingAdmissionPolicy':
        # The anchored release parent regenerates these validations from the
        # old/new signed catalogs, retaining old alternatives for waiting jobs.
        # Namespace matching, failure policy, variables and every other field
        # remain exact; generic import itself cannot supply these successors.
        before, after = copy.deepcopy(original), copy.deepcopy(desired)
        for doc in (before, after):
            for validation in doc['spec']['validations']:
                validation.pop('expression')
        if _snapshot(before) == _snapshot(after):
            return
    raise ValueError('legacy_foundation_successor_forbidden')
