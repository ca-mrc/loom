"""Fixed fresh-dev runtime installation intent and independent phase inventory.

The protected parent must consume this projection with anchored stage/transition
evidence. Preparing it performs no I/O beyond retained private-history reads and
is neither a live prerequisite proof nor authority to activate the physical pool.
"""
from __future__ import annotations

import base64
import copy
import json
from dataclasses import dataclass
from typing import Any

from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
from scripts.ops.nebius_development_build_cloud import (
    DevelopmentRegistryCloudScope,
    registry_public_key,
)
from scripts.ops.nebius_development_build_policy import prepare_build_policy
from scripts.ops.nebius_development_build_runtime import prepare_build_runtime
from scripts.ops.nebius_development_catalog_runtime import catalog_runtime_documents
from scripts.ops.nebius_development_collector_cloud import (
    DevelopmentCollectorCloudScope,
    collector_public_key,
)
from scripts.ops.nebius_development_collector_runtime import (
    prepare_collector_material,
    prepare_collector_runtime,
)
from scripts.ops.nebius_development_network_runtime import prepare_network_runtime
from scripts.ops.nebius_development_runtime_setup import (
    DevelopmentDatabaseRuntime,
    database_runtime_documents,
)
from scripts.ops.nebius_development_shared_runtime import prepare_shared_runtime
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid

from loom.nebius_platform_render import _obj, digest


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimeInstallRequest:
    database: DevelopmentDatabaseRuntime
    collector_scope: DevelopmentCollectorCloudScope
    collector_credential: bytes
    registry_scope: DevelopmentRegistryCloudScope
    registry_credential: bytes
    cache_material: dict[str, str] | None = None


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimePlan:
    fixed: dict[str, dict[str, dict[str, Any]]]
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    targets: dict[str, dict[str, Any]]
    input_digest: str


def prepare_runtime_install(request: DevelopmentRuntimeInstallRequest) -> DevelopmentRuntimePlan:
    """Freeze every original/target and reject incomplete material before writes.

    Runtime phases must requalify this request, publication and live identities.
    A caller-held plan is a comparison value, never arbitrary manifest input.
    SQL and catalog Jobs remain separate ordered stages, not generic workloads.
    """
    try:
        request = copy.deepcopy(request)
        database = request.database
        config = database.foundation.inputs.config
        spec = database.manager.retained.request.registration.spec
        participant, = spec.participants
        namespaces = {'loom-dev', database.manager.retained.request.retained.binding.namespace,
            participant.execution_namespace.name, participant.build_namespace.name}
        if len(namespaces) != 4:
            raise ValueError
        collector_key = collector_public_key(scope=request.collector_scope, config=config,
            credential=request.collector_credential)
        repositories = {profile.settings.registry_repository for profile in spec.profiles.task_images}
        repositories.update(profile.settings.registry_repository for profile in spec.profiles.application_images)
        registry_key = registry_public_key(scope=request.registry_scope, config=config,
            credential=request.registry_credential, repositories=tuple(sorted(repositories)))
        if (collector_key == registry_key or any(getattr(request.collector_scope, field) == getattr(request.registry_scope, field)
                for field in ('account_id', 'key_id', 'group_id'))):
            raise ValueError
        fixed: dict[str, dict[str, dict[str, Any]]] = {
            phase: {} for phase in ('database', 'material', 'authority', 'isolation', 'workloads', 'catalog')}
        classification = {'Secret': 'material', 'ConfigMap': 'material',
            'ServiceAccount': 'authority', 'Role': 'authority', 'RoleBinding': 'authority',
            'ClusterRole': 'authority', 'ClusterRoleBinding': 'authority',
            'NetworkPolicy': 'isolation', 'ValidatingAdmissionPolicy': 'isolation',
            'ValidatingAdmissionPolicyBinding': 'isolation', 'Deployment': 'workloads', 'CronJob': 'workloads'}
        cluster_kinds = {'ClusterRole', 'ClusterRoleBinding', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}
        seen: set[str] = set()

        def add(document: dict[str, Any], phase: str | None = None) -> None:
            key, kind = _key(document), document['kind']
            namespace = document['metadata'].get('namespace')
            if (key in seen or (kind in cluster_kinds and namespace is not None)
                    or (kind not in cluster_kinds and namespace not in namespaces)
                    or any(not set(rule['verbs']) <= {'get', 'list', 'watch'} for rule in document.get('rules', []))):
                raise ValueError
            seen.add(key)
            fixed[phase or classification[kind]][key] = copy.deepcopy(document)

        for phase, documents in (('database', database_runtime_documents(database)),
                ('catalog', catalog_runtime_documents(database))):
            for document in documents.values():
                add(document, phase)
        actuator = prepare_actuator_runtime(database)
        collector = prepare_collector_runtime(database)
        shared = prepare_shared_runtime(database)
        for document in (*database.manager.delivery.configuration, actuator.deployment, *actuator.authority,
                collector.configuration, collector.cronjob, *collector.authority,
                prepare_collector_material(database, scope=request.collector_scope, credential=request.collector_credential),
                *prepare_build_runtime(database, registry_scope=request.registry_scope,
                    registry_credential=request.registry_credential, cache_material=request.cache_material),
                *prepare_network_runtime(database), *prepare_build_policy(database)):
            add(document)
        if database.manager.requires_source_material:
            stored = database.foundation.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
            source = {key: base64.b64decode(stored['source-' + key], validate=True).decode('ascii')
                for key in ('access-key', 'secret-key')}
            secret = _obj('Secret', database.manager.delivery.source_secret_name,
                database.manager.retained.request.retained.binding.namespace)
            secret.update(type='Opaque', immutable=True, data={'credentials.json': base64.b64encode(
                json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).decode()})
            secret['metadata']['labels'] = {'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(database.operation_id)}
            add(secret)
        originals = {_key(document): copy.deepcopy(document) for document in (
            database.manager.original, *shared.original.values())}
        targets = {_key(document): _snapshot(document) for document in (
            database.manager.delivery.deployment, *shared.targets.values())}
        if len(originals) != 3 or originals.keys() != targets.keys() or originals.keys() & seen:
            raise ValueError
        stopped = {}
        for key, original in originals.items():
            _uid(original)
            if targets[key]['spec']['replicas'] != 0:
                raise ValueError
            stopped[key] = _snapshot(original)
            stopped[key]['spec']['replicas'] = 0
        checksum = digest({'reference': database.reference.model_dump(mode='json'),
            'operation_id': str(database.operation_id), 'source': database.manager.publication.source.model_dump(mode='json'),
            'collector': request.collector_scope.model_dump(mode='json'), 'registry': request.registry_scope.model_dump(mode='json'),
            'fixed': fixed, 'originals': originals, 'stopped': stopped, 'targets': targets})
        return DevelopmentRuntimePlan(fixed, originals, stopped, targets, checksum)
    except Exception:
        raise ValueError('development runtime installation intent unqualified') from None
