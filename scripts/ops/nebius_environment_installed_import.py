"""Installer-owned exact legacy snapshots and explicit runtime predecessors.

The lifecycle installation pins this private document before any runner command.
It is an operator attestation, not a public resource inventory or hash argument.
Every recorded provider identity is independently read before import succeeds.
"""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_foundation import FoundationReferenceV2, _load, _retain
from scripts.ops.nebius_environment_import_cloud import ImportedCloudScopeV2
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.profiles import PoolProfileCatalog

if TYPE_CHECKING:
    from scripts.ops.nebius_environment_runtime_start import StartedEnvironmentRuntimeV2
    from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2

_FILES = ('00-namespaces.yaml', '10-config-network.yaml', '20-database.yaml', '30-migrate.yaml',
    '40-services.yaml', '50-configure.yaml', '60-execution.yaml', '70-public.yaml', '80-backup.yaml')


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class ImportedEnvironmentRuntimeV2(_Strict):
    operation_id: UUID
    participant: PoolParticipantV1
    profiles: PoolProfileCatalog
    application_publication: ProtectedPublication
    active_head: UUID


class InstalledLegacyFoundationV2(_Strict):
    schema_version: Literal['loom.installed-legacy-foundation.v2']
    declaration: EnvironmentFoundationDeclarationV2
    publication: ProtectedPublication
    cloud_scope: ImportedCloudScopeV2
    resources: dict[str, dict[str, Any]] = Field(min_length=15, max_length=500)
    volume: dict[str, Any]
    runtime: ImportedEnvironmentRuntimeV2 | None = None


def _resources(source: InstalledLegacyFoundationV2) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    row, config = source.declaration.registration, source.declaration.platform_config
    result, material = {}, {}
    required = {'Namespace:-:' + name for name in row.namespaces}
    required.update(f'{kind}:{row.application_namespace}:{name}' for kind, name in (
        ('StatefulSet', 'loom-postgres'), ('Service', 'loom-postgres'),
        ('PersistentVolumeClaim', 'data-loom-postgres-0'), ('ConfigMap', 'loom-platform-config'),
        *(('Deployment', name) for name in ('loom-service', 'loom-control-plane', 'loom-web', 'loom-llm-gateway'))))
    names = ('loom-platform-db', config['db_tls_secret_name'], 'loom-platform-auth', 'loom-admin-secret',
        'loom-platform-collector', 'loom-platform-batch-runner', 'loom-platform-storage')
    required.update(f'Secret:{row.application_namespace}:{name}' for name in names)
    if not required <= source.resources.keys():
        raise ValueError('environment_import_foundation_incomplete')
    cluster_kinds = {'ClusterRole', 'ClusterRoleBinding', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}
    for key, doc in source.resources.items():
        uid = _uid(doc)
        namespace = doc['metadata'].get('namespace')
        if (key != _key(doc) or (doc['kind'] == 'Namespace' and (doc['metadata']['name'] not in row.namespaces or namespace is not None))
                or (doc['kind'] in cluster_kinds and namespace is not None)
                or (doc['kind'] not in cluster_kinds | {'Namespace'} and namespace not in row.namespaces)):
            raise ValueError('environment_import_resource_scope_invalid')
        frozen = _snapshot(doc)
        frozen['metadata']['uid'] = uid
        result[key] = frozen
        if doc['kind'] == 'Secret' and namespace == row.application_namespace:
            if 'stringData' in doc or not isinstance(doc.get('data'), dict):
                raise ValueError('environment_import_secret_invalid')
            material[doc['metadata']['name']] = {key: base64.b64decode(value, validate=True).decode()
                for key, value in doc['data'].items()}
    if len({_uid(doc) for doc in result.values()}) != len(result):
        raise ValueError('environment_import_duplicate_identity')
    pvc = result[f'PersistentVolumeClaim:{row.application_namespace}:data-loom-postgres-0']
    pv = source.volume
    _uid(pv)
    _snapshot(pv)
    if (pv.get('kind') != 'PersistentVolume' or pv.get('apiVersion') != 'v1'
            or pv['metadata'].get('namespace') is not None or _uid(pv) in {_uid(doc) for doc in result.values()}
            or pvc['spec'].get('volumeName') != pv['metadata']['name']
            or {key: pv['spec'].get('claimRef', {}).get(key) for key in ('namespace', 'name', 'uid')}
                != dict(namespace=row.application_namespace, name=pvc['metadata']['name'], uid=_uid(pvc))):
        raise ValueError('environment_import_volume_unqualified')
    scope = source.cloud_scope
    if (scope.compute_project_id != config['project_id'] or scope.tenant_id != config['quota_parent_id']
            or scope.region != config['region'] or ('backup' in scope.subjects) != source.declaration.backups_enabled
            or pv['spec'].get('csi', {}).get('volumeHandle') != scope.database_disk_id):
        raise ValueError('environment_import_cloud_scope_invalid')
    if source.runtime is not None:
        _runtime_binding(source, result)
    return result, material


def _runtime_binding(source: InstalledLegacyFoundationV2, resources: dict[str, dict[str, Any]]) -> None:
    from scripts.ops.nebius_development_build_policy import build_policy_documents
    from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec
    from scripts.ops.nebius_pool_runtime import _environment

    from loom.nebius_pool_settings import PoolRuntimeSettings

    runtime = source.runtime
    if runtime is None:
        raise ValueError('environment_import_runtime_missing')
    row, selected = source.declaration.registration, runtime.participant
    fields = ('installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation')
    if (not runtime.operation_id.int or not runtime.active_head.int or runtime.application_publication != source.publication
            or any(getattr(row, key) != getattr(selected, key) for key in fields)
            or selected.environment_class != row.kind
            or (selected.execution_namespace.name, selected.build_namespace.name) != (row.execution_namespace, row.build_namespace)
            or any(str(namespace.uid) != _uid(resources['Namespace:-:' + namespace.name])
                for namespace in (selected.execution_namespace, selected.build_namespace))):
        raise ValueError('environment_import_runtime_binding_invalid')
    runtime.profiles.profiles()
    for doc in build_policy_documents(row.build_namespace, runtime.profiles):
        qualify_build_policy_spec(doc, resources[_key(doc)])
    workloads = {'service': f'Deployment:{row.application_namespace}:loom-service',
        'control_plane': f'Deployment:{row.application_namespace}:loom-control-plane',
        'actuator': f'Deployment:{row.execution_namespace}:loom-execution-actuator'}
    if runtime.profiles.application_images:
        workloads['application_actuator'] = f'Deployment:{row.execution_namespace}:loom-application-image-actuator'
    for role, key in workloads.items():
        document = resources[key]
        if document['spec']['replicas'] != 1:
            raise ValueError('environment_import_runtime_not_started')
        container, = document['spec']['template']['spec']['containers']
        values = _environment(container)
        if role != 'service':
            variable = 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON' if role == 'control_plane' else 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'
            settings = PoolRuntimeSettings.model_validate_json(values[variable]['value'])
            if settings.participant != selected:
                raise ValueError('environment_import_runtime_consumer_invalid')


def prepare_installed_legacy_foundation_import(*, state_dir: Path, source: InstalledLegacyFoundationV2,
        operation_id: UUID, candidate: dict[str, Any], profile: dict[str, Any]) -> FoundationReferenceV2:
    """Retain installed intent; caller resolves its protected publication first.

    This performs no effects and makes no readiness claim. Normal reconciliation
    must read all exact Kubernetes, cloud and material identities before returning
    an observation. Its plan hash is pinned by the protected lifecycle operation.
    """
    source = InstalledLegacyFoundationV2.model_validate(source.model_dump())
    if not operation_id.int:
        raise ValueError('environment_import_operation_invalid')
    resources, material = _resources(source)
    row = source.declaration.registration
    files: dict[str, list[dict[str, Any]]] = {name: [] for name in _FILES}
    for key, document in resources.items():
        if document['kind'] == 'Namespace':
            filename = '00-namespaces.yaml'
        elif document['kind'] in {'StatefulSet', 'PersistentVolumeClaim'} or key == f'Service:{row.application_namespace}:loom-postgres':
            filename = '20-database.yaml'
        elif document['kind'] == 'Deployment' and document['metadata']['namespace'] == row.application_namespace:
            filename = '40-services.yaml'
        else:
            filename = '10-config-network.yaml' if document['metadata'].get('namespace') == row.application_namespace else '60-execution.yaml'
        files[filename].append(document)
    identities = {'credentials:material': digest(material), **{'k8s:' + key: _uid(doc) for key, doc in resources.items()}}
    plan = dict(schema_version='loom.foundation-plan.v2', operation_id=str(operation_id), mode='legacy-import',
        declaration=source.declaration.model_dump(mode='json'), files=files, publication_sha256=digest(candidate),
        profile_sha256=digest(profile), import_identities=identities, material_sha256=digest(material),
        legacy=dict(source=dict(kind='installed-foundation-v2', source_sha256=digest(source.model_dump(mode='json'))),
            config=source.declaration.platform_config, cloud_scope=source.cloud_scope.model_dump(mode='json'),
            resources=resources, volume=dict(name=source.volume['metadata']['name'], uid=_uid(source.volume), spec=source.volume['spec']),
            runtime=source.runtime.model_dump(mode='json') if source.runtime is not None else None))
    with state._locked_state(state_dir):
        _retain(state_dir / 'material.json', material)
        _retain(state_dir / 'plan.json', plan)
    return FoundationReferenceV2(operation_id=operation_id, identity=row, plan_sha256=digest(plan))


def qualify_imported_runtime_policy(policy: PoolEnrollmentPolicyV2) -> ImportedEnvironmentRuntimeV2:
    """Existing participant authority; deliberately distinct from fresh enrollment."""
    from scripts.ops.nebius_pool_enrollment_issuer import _policy

    _policy(policy)
    plan = _load(policy.foundation_dir, policy.foundation)
    if plan['mode'] != 'legacy-import' or plan['legacy']['source']['kind'] != 'installed-foundation-v2':
        raise ValueError('environment_import_runtime_source_invalid')
    runtime = ImportedEnvironmentRuntimeV2.model_validate(plan['legacy']['runtime'])
    row = policy.foundation.identity
    if (runtime.participant != policy.participant or runtime.profiles != policy.profiles
            or runtime.application_publication != policy.application_publication
            or policy.host.scope.model_dump() != dict(installation_id=row.installation_id, pool_id=row.pool_id,
                participant_id=row.participant_id, environment_id=row.environment_id, incarnation=row.incarnation, authority=row.kind)
            or policy.host.runtime.binding.kube_system_uid != plan['declaration']['kube_system_uid']
            or policy.host.connection.endpoint.rstrip('/') != plan['legacy']['config']['kubernetes_api_server'].rstrip('/')):
        raise ValueError('environment_import_runtime_policy_invalid')
    return runtime


def load_imported_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID) -> StartedEnvironmentRuntimeV2:
    """An explicit adopted predecessor, never synthetic stage/start completion.

    The protected policy pins the imported plan. A successful exact observation
    is required, but release must still acquire fresh ledger/process/readiness
    evidence and compare the installed active head before issuing any mutation.
    """
    from scripts.ops.nebius_environment_runtime_install import StagedEnvironmentRuntimeV2
    from scripts.ops.nebius_environment_runtime_start import StartedEnvironmentRuntimeV2
    from scripts.ops.nebius_pool_enrollment_issuer import _policy

    runtime = qualify_imported_runtime_policy(policy)
    if runtime.operation_id != operation_id:
        raise ValueError('environment_import_runtime_operation_invalid')
    paths = [policy.foundation_dir / name for name in ('plan.json', 'material.json', 'observed.json')]
    files = {path: state._private_read(path, limit=8 * 1024**2) for path in paths}
    plan, material, observed = (json.loads(files[path]) for path in paths)
    if (digest(plan) != policy.foundation.plan_sha256 or digest(material) != plan['material_sha256']
            or observed['plan_sha256'] != policy.foundation.plan_sha256 or observed['identities'] != plan['import_identities']):
        raise ValueError('environment_import_runtime_retained_invalid')
    # The data/PVC/Secrets remain foundation inputs. Runtime tools receive only
    # resource kinds supported by their exact read/CAS transport.
    resources = {key: copy.deepcopy(doc) for key, doc in plan['legacy']['resources'].items()
        if doc['kind'] not in {'StatefulSet', 'PersistentVolumeClaim', 'Service', 'Ingress'}}
    row = policy.foundation.identity
    roles = {'service': f'Deployment:{row.application_namespace}:loom-service',
        'control_plane': f'Deployment:{row.application_namespace}:loom-control-plane',
        'actuator': f'Deployment:{row.execution_namespace}:loom-execution-actuator'}
    if runtime.profiles.application_images:
        roles['application_actuator'] = f'Deployment:{row.execution_namespace}:loom-application-image-actuator'
    runtime_plan = dict(schema_version='loom.environment-imported-runtime.v2', operation_id=str(operation_id),
        policy=_policy(policy), expected_head=str(runtime.active_head), workloads={role: resources[key] for role, key in roles.items()},
        namespace_uids={**observed['namespace_uids'], policy.host.runtime.binding.namespace: policy.host.runtime.binding.namespace_uid},
        foundation_identities=plan['import_identities'], imported=runtime.model_dump(mode='json'))
    if any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items()):
        raise ValueError('environment_import_runtime_retained_changed')
    staged = StagedEnvironmentRuntimeV2(policy.foundation_dir, runtime_plan, resources, files)
    return StartedEnvironmentRuntimeV2(staged, resources, files)
