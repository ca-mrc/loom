"""Fixed catalog setup Job, using only retained dev CP administrator authority."""
from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime
from scripts.ops.nebius_development_shared_runtime import prepare_shared_runtime
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _stage_fixed_documents,
    _validate_record,
)

from loom.execution_contract import ExecutionTargetV1, ExecutionTopologyV1
from loom.nebius_development_catalog import DevelopmentCatalogRequest
from loom.nebius_platform_render import _mount_secret, digest
from loom.pipeline.keys import canonical_digest
from loom_service.environment_management.candidates import _json


@dataclass(frozen=True, repr=False)
class DevelopmentCatalogRuntime:
    documents: tuple[dict[str, Any], dict[str, Any]]


def prepare_catalog_runtime(request: DevelopmentDatabaseRuntime) -> DevelopmentCatalogRuntime:
    """Freeze the original execution class/target with independently bound new code.

    The protected parent must qualify/start the new closed CP, journal this Job,
    verify its sole Pod/receipt and recheck prerequisites before actuator startup.
    No SQL or Kubernetes credential is mounted; no capacity policy is imported.
    """
    try:
        prepare_shared_runtime(request)
        config = request.foundation.inputs.config
        spec = request.manager.retained.request.registration.spec
        participant, = spec.participants
        selection = participant.target(config['target_id'], 'trial')
        execution, = (row for row in spec.profiles.execution if row.profile_id == selection.profile_id)
        target = ExecutionTargetV1(target_id=config['target_id'], logical_pool_id='nebius-cpu',
            execution_class_id=execution.execution_class_id, cluster_scope_id=config['cluster_scope_id'],
            environment='development', provider='nebius', region=config['region'],
            failure_domain=config['cluster_scope_id'], data_residency='eu',
            namespace_name=participant.execution_namespace.name,
            pod_identity_audience=execution.runtime.pod_identity_audience,
            service_account_name=execution.runtime.service_account_name,
            health_role='primary', health_check_id=config['target_id'],
            health_check_interval_seconds=30, health_stale_after_seconds=90)
        catalog = DevelopmentCatalogRequest(schema_version='loom.development-runtime-catalog.v1',
            operation_id=request.operation_id, execution_class=execution.execution_class,
            topology=ExecutionTopologyV1(logical_pool_id='nebius-cpu', execution_class_id=execution.execution_class_id,
                placement_policy='environment-local-health-first', targets=(target,)))
        name = 'loom-dev-catalog-' + request.operation_id.hex
        labels = {'loom.nebius/development-runtime-operation': str(request.operation_id)}
        document = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
            'metadata': {'name': name, 'namespace': 'loom-dev', 'labels': labels.copy()},
            'data': {'catalog.json': catalog.model_dump_json()}}
        job = copy.deepcopy(request.database[1])
        job['metadata'] = copy.deepcopy(document['metadata'])
        job['spec']['template']['metadata']['labels'] = {'app': name, **labels}
        pod = job['spec']['template']['spec']
        container, = pod['containers']
        container['command'] = ['python', '-m', 'loom.nebius_development_catalog']
        container['env'] = [{'name': 'LOOM_DEVELOPMENT_RUNTIME_CATALOG_CONFIG',
            'value': '/var/run/loom-runtime-catalog/catalog.json'}]
        pod['volumes'] = [{'name': 'runtime-catalog', 'configMap': {'name': name}}]
        container['volumeMounts'] = [{'name': 'runtime-catalog', 'mountPath': '/var/run/loom-runtime-catalog', 'readOnly': True}]
        _mount_secret(pod, 'admin', 'loom-admin-secret', '/var/run/loom/admin')
        return DevelopmentCatalogRuntime((document, job))
    except Exception:
        raise ValueError('development catalog runtime unqualified') from None


def catalog_runtime_documents(request: DevelopmentDatabaseRuntime) -> dict[str, dict[str, Any]]:
    """Revalidate the retained request; never accept caller-supplied Job authority."""
    return {_key(document): document for document in prepare_catalog_runtime(request).documents}


def stage_catalog_runtime(*, request: DevelopmentDatabaseRuntime, api: ManagementStageAPI,
                          state_dir: Path) -> dict[str, Any]:
    """Internal fixed child stage; the runtime parent owns the phase-start anchor.

    Its phase-aware API must qualify the new closed CP, completed SQL setup and
    retained identities before writes. A staged Job is not catalog completion.
    """
    try:
        documents = catalog_runtime_documents(request)
        return _stage_fixed_documents(documents=documents, revision=digest(documents),
            phase='development-runtime-catalog', binding=request.manager.retained.request.retained.binding,
            api=api, state_dir=state_dir)
    except Exception:
        raise ValueError('development catalog stage unqualified; preserve evidence') from None


def validate_catalog_runtime_proof(request: DevelopmentDatabaseRuntime, state_dir: Path,
                                   proof: Any) -> None:
    """Bind the catalog receipt to the exact completed child stage and request.

    The connected HTTPS reader must independently prove the live Job/sole Pod,
    successful unrestarted execution, bounded log and unchanged prerequisites.
    This structural check grants neither activation nor live-readback authority.
    """
    try:
        documents = catalog_runtime_documents(request)
        record = _json(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1',
            'binding': asdict(request.manager.retained.request.retained.binding),
            'revision': digest(documents), 'phase': 'development-runtime-catalog'}, documents)
        job, = (item for item in record['resources'].values() if item['desired']['kind'] == 'Job')
        if (not isinstance(proof, dict) or set(proof) != {'job_uid', 'pod_uid', 'catalog'}
                or any(item['status'] != 'created' for item in record['resources'].values())
                or any(str(UUID(proof[key])) != proof[key] or not UUID(proof[key]).int
                    for key in ('job_uid', 'pod_uid')) or job['uid'] != proof['job_uid']):
            raise ValueError
        config, = (document for document in documents.values() if document['kind'] == 'ConfigMap')
        raw = _json(config['data']['catalog.json'].encode())
        catalog = DevelopmentCatalogRequest.model_validate(raw)
        if proof['catalog'] != {'operation_id': str(request.operation_id),
                'target_id': catalog.topology.targets[0].target_id, 'catalog_sha256': canonical_digest(raw)}:
            raise ValueError
    except Exception:
        raise ValueError('development catalog receipt unqualified') from None
