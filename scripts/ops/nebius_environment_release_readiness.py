"""Fresh release process, data, native-admission and shared-capacity proof."""
from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, cast

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_foundation import FoundationCloud
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_runtime_probes import EnvironmentComponent
from scripts.ops.nebius_environment_runtime_readiness import ProbeConnection, _process_connection
from scripts.ops.nebius_environment_runtime_start import runtime_resource_path
from scripts.ops.nebius_foundation_readiness import observe_foundation
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_platform_runtime_readiness import (
    PlatformRuntimePolicyV2,
    observe_platform_runtime,
    recheck_platform_resources,
)
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionSnapshotV2
from loom_service.pool_management.lifecycle_contracts import PoolParticipantReadinessV2


async def release_readiness(*, policy: PoolEnrollmentPolicyV2, platform_policy: PlatformRuntimePolicyV2,
        snapshot: PoolInspectionSnapshotV2, resources: dict[str, dict[str, Any]], namespace_uids: dict[str, str],
        foundation_identities: dict[str, str], qualify: Callable[[], None], kubernetes: KubernetesEnvironmentProvider,
        cloud: FoundationCloud, public_http: httpx.AsyncClient, probe_connection: ProbeConnection | None = None,
        platform_probe_connection: ProbeConnection | None = None) -> dict[str, Any]:
    """Prove new runtime before committing its catalog and again before intake."""
    if (platform_policy.host.runtime != policy.host.runtime or platform_policy.host.connection != policy.host.connection
            or platform_policy.host.scope.authority != 'platform'
            or (platform_policy.host.scope.installation_id, platform_policy.host.scope.pool_id) != (
                policy.participant.installation_id, policy.participant.pool_id)
            or snapshot.capacity is None or snapshot.platform_authority is None
            or str(kubernetes.http.base_url).rstrip('/') != policy.host.connection.endpoint.rstrip('/')):
        raise ValueError('environment_release_platform_unqualified')
    plan = load_foundation(policy.foundation_dir, policy.foundation)
    declaration = EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
    successors: dict[str, tuple[dict[str, Any], ...]] = {'k8s:' + key: (doc,) for key, doc in resources.items() if 'k8s:' + key in foundation_identities}
    foundation = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
        kubernetes=kubernetes, cloud=cloud, public_http=public_http, _runtime_successors=successors)
    if foundation.foundation.identities != foundation_identities:
        raise ValueError('environment_release_foundation_changed')
    material_path = policy.foundation_dir / 'material.json'
    raw = state._private_read(material_path, limit=8 * 1024**2)
    material = _json(raw)
    if digest(material) != foundation_identities['credentials:material']:
        raise ValueError('environment_release_material_changed')
    expires = min(snapshot.valid_until, foundation.foundation.observed_at + timedelta(minutes=5), *(row.expires_at for row in policy.machines))
    def check() -> None:
        qualify()
        if not snapshot.observed_at <= datetime.now(UTC) < expires or state._private_read(material_path, limit=8 * 1024**2) != raw:
            raise ValueError('environment_release_readiness_expired')
    async def recheck_resources() -> None:
        for doc in resources.values():
            actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
            if actual is None or not _matches(actual, doc, _uid(doc)):
                raise ValueError('environment_release_resource_changed')
        check()
    await recheck_resources()
    workloads = {key: doc for key, doc in resources.items() if doc['kind'] == 'Deployment'
        and doc['metadata']['name'] in {'loom-service', 'loom-control-plane', 'loom-execution-actuator', 'loom-application-image-actuator'}}
    factory = probe_connection or partial(_process_connection, policy=policy, resources=resources, namespaces=namespace_uids)
    binding = platform_policy.host.runtime.binding
    platform_factory = platform_probe_connection or partial(_process_connection, policy=policy,
        resources=platform_policy.resources, namespaces={binding.namespace: binding.namespace_uid})
    machine, = (row for row in policy.machines if row.workload_scope == 'environment')
    def processes() -> list[dict[str, Any]]:
        result = []
        with factory(check, declaration.platform_config['region']) as probes:
            for doc in workloads.values():
                component = cast(EnvironmentComponent, {'loom-service': 'service', 'loom-control-plane': 'controller'}.get(doc['metadata']['name'], 'actuator'))
                variable = {'service': 'LOOM_SVC_DB_URL', 'controller': 'LOOM_CP_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
                container, = doc['spec']['template']['spec']['containers']
                item, = (row for row in container['env'] if row['name'] == variable)
                reference = item['valueFrom']['secretKeyRef']
                if (container.get('envFrom') or set(item) != {'name', 'valueFrom'}
                        or set(item['valueFrom']) != {'secretKeyRef'} or set(reference) != {'name', 'key'}):
                    raise ValueError('environment_release_database_reference_unqualified')
                namespace = doc['metadata']['namespace']
                if namespace == policy.foundation.identity.application_namespace:
                    url = material[reference['name']][reference['key']]
                else:
                    secret = resources['Secret:' + namespace + ':' + reference['name']]
                    url = base64.b64decode(secret['data'][reference['key']], validate=True).decode()
                result.append(probes.qualify_process(workload=doc, component=component,
                    database_namespace=policy.foundation.identity.application_namespace, database_url=url,
                    token_sha256=machine.token_sha256 if component != 'service' else None))
        return result
    proofs = await asyncio.to_thread(processes)
    def platform_read() -> dict[str, Any]:
        with platform_factory(check, policy.host.runtime.region) as probes:
            return observe_platform_runtime(policy=platform_policy, snapshot=snapshot, probes=probes)
    platform = await asyncio.to_thread(platform_read)
    after = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
        kubernetes=kubernetes, cloud=cloud, public_http=public_http, _runtime_successors=successors)
    if (after.foundation.identities != foundation.foundation.identities or after.database_pod_uid != foundation.database_pod_uid
            or after.database_volume_name != foundation.database_volume_name):
        raise ValueError('environment_release_foundation_changed')
    await recheck_resources()
    def recheck() -> None:
        with factory(check, declaration.platform_config['region']) as probes:
            for workload, proof in zip(workloads.values(), proofs, strict=True):
                probes.recheck_process(workload=workload, proof=proof)
        with platform_factory(check, policy.host.runtime.region) as probes:
            recheck_platform_resources(policy=platform_policy, probes=probes)
            for key, proof in zip((platform_policy.manager_key, platform_policy.gateway_key), platform['processes'], strict=True):
                probes.recheck_process(workload=platform_policy.resources[key], proof=proof)
            recheck_platform_resources(policy=platform_policy, probes=probes)
    await asyncio.to_thread(recheck)
    check()
    readiness = PoolParticipantReadinessV2.model_validate(platform['readiness'] | {'valid_until': expires.isoformat(),
        'participant_binding_sha256': digest(policy.participant.model_dump(mode='json')),
        'catalog_sha256': digest({'participant': policy.participant.model_dump(mode='json'),
            'profiles': policy.profiles.model_dump(mode='json')})})
    return dict(readiness=readiness.model_dump(mode='json'), processes=proofs,
        workloads=[_key(doc) for doc in workloads.values()], platform=platform, foundation=after.model_dump(mode='json'))
