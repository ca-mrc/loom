"""Current platform readback for independent environment activation.

The protected installer supplies pinned platform authority and UID-bound resources.
No public runner can supply this policy. Sole-writer establishment belongs to the
platform open/import issuer; this reader qualifies its committed current reference,
fresh authenticated physical capture and installed manager/gateway. It performs no
writer transfer, pool opening, staging inspection or participant mutation.
"""
from __future__ import annotations

import base64
import copy
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from scripts.ops.nebius_environment_runtime_probes import EnvironmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_administration_dispatch import (
    PoolAdministrationHostV2,
    PoolAdministrationReferenceV2,
    _load,
)
from scripts.ops.nebius_pool_migration_guard import qualify_database_destination
from scripts.ops.nebius_pool_runtime_settings import expected_pool_runtime_settings

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionSnapshotV2
from loom_service.pool_management.lifecycle_contracts import PoolRuntimeReadinessV2


@dataclass(frozen=True, repr=False)
class PlatformRuntimePolicyV2:
    host: PoolAdministrationHostV2
    authority: PoolAdministrationReferenceV2
    resources: dict[str, dict[str, Any]]
    manager_key: str
    gateway_key: str
    gateway_token_key: str
    catalog_key: str


def observe_platform_runtime(*, policy: PlatformRuntimePolicyV2, snapshot: PoolInspectionSnapshotV2,
        probes: EnvironmentRuntimeProbes) -> dict[str, Any]:
    """Read the installed platform; never renew or recreate its historical grant.

    The parent obtains snapshot through the authenticated fixed inspection Job.
    This function is internal, like the probe transport; caller-authored JSON
    alone is not accepted by any public dispatch or an activation transaction.
    """
    try:
        policy = copy.deepcopy(policy)
        snapshot = PoolInspectionSnapshotV2.model_validate(snapshot.model_dump())
        host, reference = policy.host, policy.authority
        binding = host.runtime.binding
        original = _load(host, reference)
        origin = snapshot.platform_authority
        if (host.scope.authority != 'platform' or origin is None or snapshot.capacity is None
                or (host.scope.installation_id, host.scope.pool_id) != (snapshot.pool.installation_id, snapshot.pool.pool_id)
                or (origin.operation_id, origin.authorization_id, origin.request_sha256, origin.authorization_sha256) != (
                    reference.operation_id, reference.authorization_id, reference.request_sha256,
                    digest(original.authorization.model_dump(mode='json')))
                or not 1 <= len(policy.resources) <= 64 or probes.region != host.runtime.region):
            raise ValueError
        manager, gateway = policy.resources[policy.manager_key], policy.resources[policy.gateway_key]
        secret, catalog = policy.resources[policy.gateway_token_key], policy.resources[policy.catalog_key]
        database = policy.resources['Secret:' + binding.namespace + ':' + host.runtime.database_secret]
        if (manager['kind'] != 'Deployment' or gateway['kind'] != 'Deployment' or _key(manager) == _key(gateway)
                or secret['kind'] != 'Secret' or catalog['kind'] != 'ConfigMap' or database['kind'] != 'Secret'):
            raise ValueError
        token = base64.b64decode(secret['data']['token'], validate=True).decode()
        if not token or token.strip() != token or len(token.encode()) > 512:
            raise ValueError
        token_sha256 = hashlib.sha256(token.encode()).hexdigest()
        catalog_sha256 = hashlib.sha256(catalog['data']['profiles.json'].encode()).hexdigest()
        admin_url = base64.b64decode(database['data'][host.runtime.database_url_key], validate=True).decode()
        qualify_database_destination(admin_url, binding.namespace)
        machine, = (row for row in snapshot.machines if row.role == 'gateway')
        settings = expected_pool_runtime_settings('gateway', gateway, token_sha256=token_sha256)
        if (settings['pool_id'], settings['installation_id'], settings['admission_epoch'], settings['machine_id']) != (
                str(snapshot.pool.pool_id), str(snapshot.pool.installation_id), snapshot.pool.admission_epoch, str(machine.machine_id)):
            raise ValueError
        container, = gateway['spec']['template']['spec']['containers']
        if (container.get('command') != ['python', '-m', 'loom_service.pool_management'] or container.get('args')
                or container.get('readinessProbe', {}).get('httpGet') != {'path': '/readyz', 'port': 'health'}
                or {'name': 'health', 'containerPort': 9120} not in container.get('ports', [])):
            raise ValueError

        def current() -> None:
            if not snapshot.observed_at <= datetime.now(UTC) < snapshot.valid_until or _load(host, reference) != original:
                raise ValueError
            for name, uid in (('kube-system', binding.kube_system_uid), (binding.namespace, binding.namespace_uid)):
                row = probes.read('/api/v1/namespaces/' + name)
                if (row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace' or row['metadata']['name'] != name
                        or _uid(row) != uid or row['metadata'].get('deletionTimestamp') or row['metadata'].get('ownerReferences')):
                    raise ValueError
            for key, document in policy.resources.items():
                kind = document['kind']
                if (key != _key(document) or document['metadata'].get('namespace') != binding.namespace
                        or kind not in {'Deployment', 'Secret', 'ConfigMap'}):
                    raise ValueError
                base = '/apis/apps/v1' if kind == 'Deployment' else '/api/v1'
                suffix = {'Deployment': 'deployments', 'Secret': 'secrets', 'ConfigMap': 'configmaps'}[kind]
                actual = probes.read(base + '/namespaces/' + binding.namespace + '/' + suffix + '/' + document['metadata']['name'])
                if not _matches(actual, document, _uid(document)):
                    raise ValueError

        current()
        processes = []
        for component, workload in (('manager', manager), ('gateway', gateway)):
            container, = workload['spec']['template']['spec']['containers']
            variable = 'LOOM_SVC_DB_URL' if component == 'manager' else 'LOOM_POOL_GATEWAY_DB_URL'
            field, = (row for row in container['env'] if row['name'] == variable)
            if (container['image'] != host.runtime.service_image or container.get('envFrom')
                    or set(field) != {'name', 'valueFrom'} or set(field['valueFrom']) != {'secretKeyRef'}):
                raise ValueError
            source = field['valueFrom']['secretKeyRef']
            if set(source) != {'name', 'key'} or source['name'] != host.runtime.database_secret:
                raise ValueError
            url = base64.b64decode(database['data'][source['key']], validate=True).decode()
            processes.append(probes.qualify_process(workload=workload, component='manager' if component == 'manager' else 'gateway',
                database_namespace=binding.namespace, database_url=url,
                token_sha256=token_sha256 if component == 'gateway' else None,
                catalog_sha256=catalog_sha256 if component == 'manager' else None))
        current()
        for workload, proof in zip((manager, gateway), processes, strict=True):
            probes.recheck_process(workload=workload, proof=proof)
        current()
        observer, = (row for row in snapshot.machines if row.role == 'observer')
        readiness = PoolRuntimeReadinessV2(observed_at=snapshot.observed_at, valid_until=snapshot.valid_until,
            pool_authority_sha256=snapshot.pool_authority_sha256, observer_machine_id=observer.machine_id,
            gateway_machine_id=machine.machine_id, observer_credential_epoch=observer.credential_epoch,
            gateway_credential_epoch=machine.credential_epoch, capabilities=snapshot.capabilities)
        return dict(status='platform_runtime_verified', platform_operation_id=str(origin.operation_id),
            readiness=readiness.model_dump(mode='json'), processes=processes,
            authority=origin.model_dump(mode='json'), capacity=[row.model_dump(mode='json') for row in snapshot.capacity])
    except Exception:
        raise ValueError('platform_runtime_unqualified') from None
