"""Fixed scoped administration Jobs, using the existing create/readback journal.

The installed protected caller owns runtime selection and grant issuance. These
functions accept no public credential/path overrides and are not an SSH grant.
Job creation is not commit proof; an exact completed Job/Pod receipt is historical
evidence, and a current state read requires a separately identified status Job.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_platform_namespace import (
    PlatformNamespaceBindingV2 as PlatformNamespaceBindingV2,
)
from scripts.ops.nebius_pool_registration import HTTPSPoolRegistrationAPI

from loom.nebius_platform_render import digest as render_digest
from loom_service.pool_management.administration import (
    _REQUEST,
    PoolAdministrationAuthorizationV2,
    _Request,
)
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.legacy_adoption import PoolLegacyAdoptionV2
from loom_service.pool_management.lifecycle_contracts import (
    PoolParticipantEnrollmentV2,
    PoolParticipantOperationV2,
    PoolPlatformInstallationV2,
)

_LABEL = r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?'
_HASH = r'[0-9a-f]{64}'
_STATES = {'enrolled', 'ready', 'active', 'suspended', 'releasing', 'retiring', 'retired'}


@dataclass(frozen=True, repr=False)
class PoolAdministrationRuntimeV2:
    binding: PlatformNamespaceBindingV2
    service_image: str
    database_secret: str
    database_url_key: str
    database_ca_key: str
    node_selector: dict[str, str]
    tolerations: tuple[dict[str, str], ...]
    region: str


@dataclass(frozen=True, repr=False)
class PoolAdministrationJobV2:
    runtime: PoolAdministrationRuntimeV2
    request: _Request
    authorization: PoolAdministrationAuthorizationV2
    action: Literal['apply', 'status']
    observation_id: UUID | None = None


def _qualified(value: PoolAdministrationJobV2) -> PoolAdministrationJobV2:
    value = copy.deepcopy(value)
    request = _REQUEST.validate_python(value.request.model_dump())
    grant = PoolAdministrationAuthorizationV2.model_validate(value.authorization.model_dump())
    runtime = value.runtime
    runtime.binding.__post_init__()
    if (str(grant.installation_id) != runtime.binding.installation_id
            or grant.request_sha256 != digest(request.model_dump(mode='json'))
            or grant.authorization_id == request.operation_id
            or value.action not in {'apply', 'status'}
            or (value.observation_id is not None) != (value.action == 'status')
            or (value.observation_id is not None and not value.observation_id.int)
            or re.fullmatch(r'[^\s@]+@sha256:' + _HASH, runtime.service_image) is None
            or re.fullmatch(_LABEL, runtime.database_secret) is None
            or any(re.fullmatch(r'[A-Za-z0-9._-]{1,253}', key) is None
                for key in (runtime.database_url_key, runtime.database_ca_key))
            or not runtime.node_selector or not runtime.region
            or any(not isinstance(key, str) or not isinstance(item, str) or not key or not item
                for key, item in runtime.node_selector.items())
            or any(set(item) - {'key', 'operator', 'value', 'effect'}
                or item.get('operator') not in {'Equal', 'Exists'}
                or item.get('effect') not in {'NoSchedule', 'NoExecute', 'PreferNoSchedule'} for item in runtime.tolerations)):
        raise ValueError('pool_administration_runtime_unqualified')
    if isinstance(request, (PoolPlatformInstallationV2, PoolLegacyAdoptionV2)):
        target = request.before if isinstance(request, PoolLegacyAdoptionV2) else request
        if grant.authority != 'platform' or (grant.installation_id, grant.pool_id) != (target.installation_id, target.pool_id):
            raise ValueError('pool_administration_target_unqualified')
    else:
        participant = request.participant if isinstance(request, PoolParticipantEnrollmentV2) else request
        if (grant.authority == 'platform'
                or (grant.pool_id, grant.participant_id, grant.incarnation) != (participant.pool_id, participant.participant_id, participant.incarnation)):
            raise ValueError('pool_administration_target_unqualified')
        if isinstance(request, PoolParticipantEnrollmentV2) and (
                grant.environment_id != request.participant.environment_id or grant.authority != request.participant.environment_class):
            raise ValueError('pool_administration_target_unqualified')
    return value


def administration_documents(request: PoolAdministrationJobV2) -> dict[str, dict[str, Any]]:
    request = _qualified(request)
    runtime, grant = request.runtime, request.authorization
    # Grant identity distinguishes renewed authorization without changing original
    # SQL operation identity. A status observation can never recreate an apply Job.
    name = 'loom-pool-admin-' + grant.authorization_id.hex
    job_name = name
    if request.action == 'status':
        assert request.observation_id is not None
        job_name = 'loom-pool-read-' + request.observation_id.hex
    labels = {'loom.nebius/management-installation': runtime.binding.installation_id,
        'loom.nebius/pool-operation': str(request.request.operation_id),
        'loom.nebius/pool-authorization': str(grant.authorization_id)}
    grant_json = grant.model_dump_json()
    config = {'apiVersion': 'v1', 'kind': 'ConfigMap',
        'metadata': {'name': name, 'namespace': runtime.binding.namespace, 'labels': labels.copy()},
        'immutable': True, 'data': {'request.json': request.request.model_dump_json(), 'authorization.json': grant_json}}
    root = '/var/run/loom-pool-administration/'
    environment = {'LOOM_POOL_ADMINISTRATION_FILE': root + 'request.json',
        'LOOM_POOL_ADMINISTRATION_AUTHORIZATION_FILE': root + 'authorization.json',
        'LOOM_POOL_ADMINISTRATION_AUTHORIZATION_SHA256': hashlib.sha256(grant_json.encode()).hexdigest(),
        'LOOM_POOL_ADMINISTRATION_ACTION': request.action}
    container = {'name': 'register', 'image': runtime.service_image,
        'command': ['python', '-m', 'loom_service.pool_management.administration'],
        'env': [{'name': key, 'value': value} for key, value in environment.items()] + [
            {'name': 'LOOM_POOL_ADMINISTRATION_DB_URL', 'valueFrom': {'secretKeyRef': {
                'name': runtime.database_secret, 'key': runtime.database_url_key}}}],
        'volumeMounts': [{'name': 'administration', 'mountPath': root.rstrip('/'), 'readOnly': True},
            {'name': 'db-ca', 'mountPath': '/var/run/loom-db', 'readOnly': True}],
        'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
        'resources': {'requests': {'cpu': '100m', 'memory': '256Mi', 'ephemeral-storage': '64Mi'},
            'limits': {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '128Mi'}}}
    pod = {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000, 'runAsGroup': 1000, 'fsGroup': 1000,
            'seccompProfile': {'type': 'RuntimeDefault'}}, 'containers': [container],
        'volumes': [{'name': 'administration', 'configMap': {'name': name}},
            {'name': 'db-ca', 'secret': {'secretName': runtime.database_secret,
                'items': [{'key': runtime.database_ca_key, 'path': 'ca.crt'}], 'defaultMode': 0o440}}],
        'nodeSelector': runtime.node_selector, 'tolerations': list(runtime.tolerations)}
    job = {'apiVersion': 'batch/v1', 'kind': 'Job',
        'metadata': {'name': job_name, 'namespace': runtime.binding.namespace, 'labels': labels.copy()},
        'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 120,
            'template': {'metadata': {'labels': labels.copy()}, 'spec': pod}}}
    return {_key(doc): doc for doc in (config, job)}


def _uuid(value: Any) -> None:
    if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
        raise ValueError('pool_administration_report_unqualified')


def _head(value: Any, *, receipt: bool = False) -> None:
    fields = {'operation_id', 'generation', 'state', 'binding_sha256'}
    if receipt:
        fields |= {'schema_version', 'pool_id', 'participant_id', 'incarnation'}
    if (not isinstance(value, dict) or set(value) != fields or value['state'] not in _STATES
            or type(value['generation']) is not int or value['generation'] < 1
            or not isinstance(value['binding_sha256'], str) or re.fullmatch(_HASH, value['binding_sha256']) is None):
        raise ValueError('pool_administration_report_unqualified')
    _uuid(value['operation_id'])
    if receipt:
        if value['schema_version'] != 'loom.pool-participant-receipt.v2':
            raise ValueError('pool_administration_report_unqualified')
        for key in ('pool_id', 'participant_id', 'incarnation'):
            _uuid(value[key])


def validate_administration_report(request: PoolAdministrationJobV2, report: Any) -> None:
    request = _qualified(request)
    grant, operation = request.authorization, request.request
    if (not isinstance(report, dict) or set(report) != {'schema_version', 'authorization_id', 'request_sha256', 'receipt', 'current'}
            or report['schema_version'] != 'loom.pool-administration-result.v2'
            or report['authorization_id'] != str(grant.authorization_id) or report['request_sha256'] != grant.request_sha256):
        raise ValueError('pool_administration_report_unqualified')
    receipt, current = report['receipt'], report['current']
    if current is not None:
        if grant.authority == 'platform':
            raise ValueError('pool_administration_report_unqualified')
        _head(current)
    if receipt is None:
        if request.action != 'status':
            raise ValueError('pool_administration_report_unqualified')
        return
    if receipt.get('operation_id') != str(operation.operation_id) or receipt.get('pool_id') != str(grant.pool_id):
        raise ValueError('pool_administration_report_unqualified')
    if isinstance(operation, PoolPlatformInstallationV2):
        if receipt != dict(schema_version='loom.pool-platform-installation-receipt.v2', operation_id=str(operation.operation_id),
                pool_id=str(grant.pool_id), installation_sha256=grant.request_sha256, mode='closed', participants=0, machines=2):
            raise ValueError('pool_administration_report_unqualified')
    elif isinstance(operation, PoolLegacyAdoptionV2):
        if (set(receipt) != {'schema_version', 'operation_id', 'pool_id', 'mode', 'admission_epoch', 'pool_authority_sha256', 'participants'}
                or receipt['schema_version'] != 'loom.pool-legacy-adoption-receipt.v2'
                or receipt['mode'] != operation.expected_mode or receipt['admission_epoch'] != operation.before.admission_epoch
                or re.fullmatch(_HASH, receipt['pool_authority_sha256']) is None
                or not isinstance(receipt['participants'], list) or len(receipt['participants']) != len(operation.before.participants)):
            raise ValueError('pool_administration_report_unqualified')
        for child in receipt['participants']:
            _head(child, receipt=True)
        if {child['participant_id'] for child in receipt['participants']} != {str(item.participant_id) for item in operation.before.participants}:
            raise ValueError('pool_administration_report_unqualified')
    else:
        _head(receipt, receipt=True)
        if current is None or (receipt['participant_id'], receipt['incarnation']) != (str(grant.participant_id), str(grant.incarnation)):
            raise ValueError('pool_administration_report_unqualified')
        if isinstance(operation, PoolParticipantOperationV2):
            expected_state = {'ready': 'ready', 'activate': 'active', 'suspend': 'suspended', 'resume': 'active',
                'retire': 'retiring', 'complete_retirement': 'retired', 'begin_release': 'suspended', 'commit_release': 'ready'}[operation.action]
            # Commit changes the binding to its independently qualified ready proof.
            expected_binding = operation.expected_binding_sha256
            if operation.action == 'commit_release':
                assert operation.readiness is not None
                expected_binding = operation.readiness.participant_binding_sha256
        else:
            expected_state, expected_binding = 'enrolled', digest(operation.participant.model_dump(mode='json'))
        if receipt['state'] != expected_state or receipt['binding_sha256'] != expected_binding:
            raise ValueError('pool_administration_report_unqualified')


def _parts(request: PoolAdministrationJobV2, state_dir: Path) -> tuple[tuple[dict[str, Any], str, Path], ...]:
    documents = administration_documents(request)
    return tuple((doc, 'pool-administration-' + doc['kind'].lower(),
        state_dir / ('inputs' if doc['kind'] == 'ConfigMap' else 'jobs') / doc['metadata']['name']) for doc in documents.values())


def stage_administration_job(*, request: PoolAdministrationJobV2, api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    try:
        request = _qualified(request)
        binding = request.runtime.binding
        parts = _parts(request, state_dir)
        resource_uids = {}
        with private_state._locked_state(state_dir):
            for name in ('inputs', 'jobs'):
                private_state._private_directory(state_dir / name)
            for document, phase, directory in parts:
                documents = {_key(document): document}
                result = _stage_fixed_documents(documents=documents,
                    revision=render_digest({'documents': documents, 'binding': asdict(binding)}), phase=phase,
                    binding=binding, api=api, state_dir=directory)
                resource_uids.update(result['resource_uids'])
        return {'status': 'administration_job_staged', 'job_name': parts[1][0]['metadata']['name'], 'resource_uids': resource_uids}
    except Exception:
        raise ValueError('pool_administration_job_unavailable_preserve_evidence') from None


class HTTPSPoolAdministrationJobAPI(HTTPSPoolRegistrationAPI):
    def __init__(self, *, request: PoolAdministrationJobV2, api_server: str,
            ssl_context: ssl.SSLContext, token: str | None = None):
        self.administration = _qualified(request)
        self.documents = administration_documents(self.administration)
        self.binding = self.administration.runtime.binding
        self.region = self.administration.runtime.region
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        result = {}
        for document, phase, directory in _parts(self.administration, state_dir):
            documents = {_key(document): document}
            record = json.loads(private_state._private_read(directory / 'stage.json', limit=4 * 1024**2))
            identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
                'revision': render_digest({'documents': documents, 'binding': asdict(self.binding)}), 'phase': phase}
            _validate_record(record, identity, documents)
            for item in record['resources'].values():
                self.verify_identity(self.binding)
                actual = self.get_resource(item['desired'])
                if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid'] or _snapshot(actual) != item['observed']):
                    raise ValueError('pool_administration_job_unqualified')
                result[actual['kind']] = actual
        return result

    def _validate_execution_report(self, report: Any) -> None:
        validate_administration_report(self.administration, report)

    def administration_report(self, state_dir: Path) -> dict[str, Any] | None:
        return self._execution_report(state_dir)
