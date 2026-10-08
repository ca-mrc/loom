"""Fixed fresh-dev SQL Job/material stage, without runtime activation authority."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_foundation import (
    RetainedDevelopmentState,
    load_retained_foundation,
)
from scripts.ops.nebius_development_pool_retained import RetainedDevelopmentPoolReference
from scripts.ops.nebius_development_runtime_render import (
    DevelopmentManagerRuntime,
    DevelopmentRuntimePublication,
    prepare_manager_runtime,
)
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_supplied import _defaulted as _secret_defaulted
from sqlalchemy.engine import make_url

from loom.nebius_application_database import _PASSWORD
from loom.nebius_development_runtime_database import _REVISION
from loom.nebius_platform_bootstrap import database_url
from loom.nebius_platform_render import _secret_env, digest
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.deployment import render_management

_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, repr=False)
class DevelopmentDatabaseRuntime:
    reference: RetainedDevelopmentPoolReference
    manager: DevelopmentManagerRuntime
    foundation: RetainedDevelopmentState
    operation_id: UUID
    material: tuple[dict[str, Any], dict[str, Any]]
    database: tuple[dict[str, Any], dict[str, Any]]


def prepare_database_runtime(reference: RetainedDevelopmentPoolReference, *,
        publication: DevelopmentRuntimePublication, operation_id: UUID,
        actuator_password: str, batch_runner_token: str) -> DevelopmentDatabaseRuntime:
    """Use a new source-bound image but retain original dev DB/CA authority.

    Credentials must already be privately retained by the protected parent.
    That parent must requalify publication, live foundation/namespace/material
    identities and history before staging these exact documents. This pure
    projection neither generates credentials nor qualifies live readiness.
    """
    try:
        if (not isinstance(operation_id, UUID) or not operation_id.int
                or _PASSWORD.fullmatch(actuator_password) is None
                or re.fullmatch(r'loom_br_[A-Za-z0-9_-]{32,128}', batch_runner_token) is None):
            raise ValueError
        manager = prepare_manager_runtime(reference, publication=publication)
        retained = manager.retained.request.retained
        foundation = load_retained_foundation(retained.inputs.prerequisites.foundation)
        application = manager.deployment.installation.applications
        binding = foundation.binding
        if (application is None or application.shared.platform_namespace != 'loom-dev'
                or str(application.shared.data_environment_id) != binding.bootstrap.installation_id
                or application.shared.cluster_id != foundation.inputs.config['cluster_id']
                or str(retained.inputs.shared_namespace_uid) != binding.namespace_uid
                or retained.binding.kube_system_uid != binding.bootstrap.kube_system_uid
                or manager.deployment.installation.foundation.platform_config != foundation.inputs.config
                or application.shared.schema_revision != _REVISION):
            raise ValueError
        original = foundation.bootstrap['material']['loom-platform-db']
        history = _json(retained.files[Path(retained.operation['state_dir']) / 'application-material/stage.json'])
        shared, = (item['observed'] for item in history['resources'].values()
            if item['observed']['kind'] == 'Secret'
            and item['observed']['metadata']['name'].startswith('loom-applications-shared-'))
        shared_material = _json(base64.b64decode(shared['data']['shared.json'], validate=True))
        admin = make_url(database_url(original['admin-url'], 'loom-dev'))
        if (admin.username != 'postgres' or admin.password != original['postgres-password']
                or admin.port != 5432 or admin.database != shared_material['database_name']
                or dict(admin.query) != {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}
                or original['ca.crt'] != shared_material['ca_pem']
                or foundation.bootstrap['material']['loom-platform-auth']['secret-store-master-key']
                    != shared_material['secret_store_master_keys']):
            raise ValueError
        participant, = manager.retained.request.registration.spec.participants
        name = 'loom-dev-runtime-' + operation_id.hex
        labels = {'loom.nebius/development-runtime-operation': str(operation_id)}

        def secret(namespace: str, material: dict[str, str]) -> dict[str, Any]:
            return {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'immutable': True,
                'metadata': {'name': name, 'namespace': namespace, 'labels': dict(labels)},
                'data': {key: base64.b64encode(value.encode()).decode() for key, value in material.items()}}

        material = (secret('loom-dev', {'actuator-password': actuator_password, 'batch-runner-token': batch_runner_token}),
            secret(participant.execution_namespace.name, {
                'actuator-url': admin.set(username='loom_actuator', password=actuator_password).render_as_string(hide_password=False),
                'ca.crt': original['ca.crt']}))
        config = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
            'metadata': {'name': name, 'namespace': 'loom-dev', 'labels': dict(labels)},
            'data': {'setup.json': json.dumps({'namespace': 'loom-dev', 'operation_id': str(operation_id),
                'schema_revision': _REVISION}, sort_keys=True, separators=(',', ':'))}}
        target = manager.publication.bundle
        rendered = render_management(manager.deployment, candidate=target.candidate, profile=target.profile, repo_root=_ROOT)
        job = copy.deepcopy(rendered.files['30-migrate.yaml'][0])
        job['metadata'] = {'name': name, 'namespace': 'loom-dev', 'labels': dict(labels)}
        job['spec']['backoffLimit'] = 0
        pod = job['spec']['template']['spec']
        pod.pop('initContainers', None)
        pod.update(automountServiceAccountToken=False, restartPolicy='Never')
        container, = pod['containers']
        container['command'] = ['python', '-m', 'loom.nebius_development_runtime_database']
        container['env'] = [
            {'name': 'LOOM_DEVELOPMENT_RUNTIME_CONFIG', 'value': '/var/run/loom-runtime-setup/setup.json'},
            _secret_env('LOOM_DB_URL', 'loom-platform-db', 'admin-url'),
            _secret_env('LOOM_DB_ACTUATOR_PASSWORD', name, 'actuator-password'),
            _secret_env('LOOM_BATCH_RUNNER_TOKEN', name, 'batch-runner-token'),
        ]
        pod['volumes'] = [row for row in pod['volumes'] if row['name'] == 'db-ca'] + [
            {'name': 'runtime-setup', 'configMap': {'name': name, 'items': [{'key': 'setup.json', 'path': 'setup.json'}]}}]
        container['volumeMounts'] = [row for row in container['volumeMounts'] if row['name'] == 'db-ca'] + [
            {'name': 'runtime-setup', 'mountPath': '/var/run/loom-runtime-setup', 'readOnly': True}]
        job['spec']['template']['metadata']['labels'] = {'app': name, **labels}
        return DevelopmentDatabaseRuntime(reference, manager, foundation, operation_id, material, (config, job))
    except Exception:
        raise ValueError('development runtime database delivery unqualified') from None


def database_runtime_documents(request: DevelopmentDatabaseRuntime) -> dict[str, dict[str, Any]]:
    """Reproduce the fixed projection before accepting any caller-held documents."""
    credentials = request.material[0]['data']
    expected = prepare_database_runtime(request.reference, publication=request.manager.publication,
        operation_id=request.operation_id,
        actuator_password=base64.b64decode(credentials['actuator-password'], validate=True).decode(),
        batch_runner_token=base64.b64decode(credentials['batch-runner-token'], validate=True).decode())
    if expected != request:
        raise ValueError('development runtime database request changed')
    return {_key(document): copy.deepcopy(document) for document in (*expected.material, *expected.database)}


def stage_database_runtime(*, request: DevelopmentDatabaseRuntime, api: ManagementStageAPI,
                           state_dir: Path) -> dict[str, Any]:
    """Fixed internal child stage; a receipt is not SQL success or runtime readiness.

    The protected runtime parent owns the independent phase-start anchor and
    qualifies the selected publication and live original resources. Its API must
    verify both actual namespaces, the retained dev foundation and closed pool
    before each write. Never infer authority from this child journal alone.
    """
    try:
        documents = database_runtime_documents(request)

        def default(api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
            return (_secret_defaulted if desired['kind'] == 'Secret' else _defaulted)(api, desired)

        return _stage_fixed_documents(documents=documents, revision=digest(documents),
            phase='development-runtime-database', binding=request.manager.retained.request.retained.binding,
            api=api, state_dir=state_dir, default_document=default)
    except Exception:
        raise ValueError('development runtime database stage unqualified; preserve evidence') from None


def validate_database_runtime_proof(request: DevelopmentDatabaseRuntime, state_dir: Path,
                                    proof: Any) -> None:
    """Bind the committed SQL receipt to the recorded fixed Job and private token.

    The HTTPS reader must independently qualify the live Job, sole successful Pod,
    bounded log response and unchanged prerequisites. A caller-supplied dictionary
    satisfying this structural contract is not itself live completion evidence.
    """
    try:
        documents = database_runtime_documents(request)
        record = _json(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1',
            'binding': asdict(request.manager.retained.request.retained.binding),
            'revision': digest(documents), 'phase': 'development-runtime-database'}, documents)
        job, = (item for item in record['resources'].values() if item['desired']['kind'] == 'Job')
        if (not isinstance(proof, dict) or set(proof) != {'job_uid', 'pod_uid', 'database'}
                or any(item['status'] != 'created' for item in record['resources'].values())
                or any(str(UUID(proof[key])) != proof[key] or not UUID(proof[key]).int
                    for key in ('job_uid', 'pod_uid')) or job['uid'] != proof['job_uid']):
            raise ValueError
        report = proof['database']
        if not isinstance(report, dict) or type(report.get('role_oid')) is not int or not 0 < report['role_oid'] < 2**32:
            raise ValueError
        token = base64.b64decode(request.material[0]['data']['batch-runner-token'], validate=True)
        if report != {'status': 'development_runtime_database_installed', 'operation_id': str(request.operation_id),
                'role': 'loom_actuator', 'role_oid': report['role_oid'], 'token_sha256': hashlib.sha256(token).hexdigest()}:
            raise ValueError
    except Exception:
        raise ValueError('development runtime database receipt unqualified') from None
