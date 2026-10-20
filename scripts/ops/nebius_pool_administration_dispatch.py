"""Generic data-only dispatch into independently pinned host authority.

Only the protected issuer calls retain_authorized_operation, after qualifying
publication, foundation/readiness and (for legacy import) installed ancestry.
The runner supplies references, never grants, credentials, paths or code. This
module installs no SSH key; its main arguments must be fixed by the launcher.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import re
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_management_stage import ManagementStageAPI
from scripts.ops.nebius_platform_namespace import PlatformNamespaceBindingV2
from scripts.ops.nebius_pool_administration_job import (
    HTTPSPoolAdministrationJobAPI,
    PoolAdministrationJobV2,
    PoolAdministrationRuntimeV2,
    _qualified,
    stage_administration_job,
    validate_administration_report,
)

from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.administration import (
    _REQUEST,
    PoolAdministrationAuthorizationV2,
    _Request,
)
from loom_service.pool_management.capacity import digest

_MAX_OPERATION = 4 * 1024 * 1024
_MAX_COMMAND = 16384


class PoolAdministrationScopeV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    installation_id: UUID
    pool_id: UUID
    participant_id: UUID | None = None
    environment_id: UUID | None = None
    incarnation: UUID | None = None
    authority: Literal['platform', 'development', 'staging', 'production']

    @model_validator(mode='after')
    def identity(self) -> PoolAdministrationScopeV2:
        scoped = (self.participant_id, self.environment_id, self.incarnation)
        if (not self.installation_id.int or not self.pool_id.int
                or (self.authority == 'platform' and any(value is not None for value in scoped))
                or (self.authority != 'platform' and any(value is None or not value.int for value in scoped))):
            raise ValueError('pool_administration_scope_unqualified')
        return self


class PoolAdministrationReferenceV2(PoolAdministrationScopeV2):
    schema_version: Literal['loom.pool-administration-reference.v2']
    operation_id: UUID
    authorization_id: UUID
    request_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    document_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @model_validator(mode='after')
    def operation(self) -> PoolAdministrationReferenceV2:
        if not self.operation_id.int or not self.authorization_id.int:
            raise ValueError('pool_administration_reference_unqualified')
        return self


class _Command(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.pool-administration-command.v2']
    action: Literal['prepare', 'apply', 'recover', 'status']
    reference: PoolAdministrationReferenceV2
    observation_id: UUID | None = None

    @model_validator(mode='after')
    def selection(self) -> _Command:
        if (self.observation_id is not None) != (self.action == 'status') or (
                self.observation_id is not None and not self.observation_id.int):
            raise ValueError('pool_administration_command_unqualified')
        return self


@dataclass(frozen=True, repr=False)
class PoolAdministrationHostV2:
    runtime: PoolAdministrationRuntimeV2
    scope: PoolAdministrationScopeV2
    state_root: Path
    anchor_root: Path
    connection: NebiusKubernetesConnection


class AdministrationAPI(ManagementStageAPI, Protocol):
    def administration_report(self, state_dir: Path) -> dict[str, Any] | None: ...


Connection = Callable[[PoolAdministrationJobV2], AbstractContextManager[AdministrationAPI]]


def _scope(document: Any) -> dict[str, Any]:
    return {name: getattr(document, name) for name in PoolAdministrationScopeV2.model_fields}


def _host(host: PoolAdministrationHostV2) -> None:
    PoolAdministrationScopeV2.model_validate(host.scope.model_dump())
    NebiusKubernetesConnection.model_validate(host.connection.model_dump())
    paths = (host.state_root, host.anchor_root, host.connection.ca_file, host.connection.credentials_file)
    if (str(host.scope.installation_id) != host.runtime.binding.installation_id
            or len(set(paths)) != 4 or any(not path.is_absolute() or path == Path('/') or path != path.resolve() for path in paths)
            or any(parent in child.parents for parent in paths[:2] for child in paths if parent != child)):
        raise ValueError('pool_administration_host_unqualified')


def _runtime(value: dict[str, Any]) -> PoolAdministrationRuntimeV2:
    value = copy.deepcopy(value)
    if set(value) != {field.name for field in fields(PoolAdministrationRuntimeV2)}:
        raise ValueError('pool_administration_runtime_unqualified')
    binding = value['binding']
    if (not isinstance(binding, dict) or set(binding) != {field.name for field in fields(PlatformNamespaceBindingV2)}
            or binding.pop('schema_version') != 'loom.platform-namespace.v2'):
        raise ValueError('pool_administration_runtime_unqualified')
    value['binding'] = PlatformNamespaceBindingV2(**binding)
    value['tolerations'] = tuple(value['tolerations'])
    return PoolAdministrationRuntimeV2(**value)


def load_administration_host(path: Path, checksum: str) -> PoolAdministrationHostV2:
    if not path.is_absolute() or path != path.resolve() or re.fullmatch(r'[0-9a-f]{64}', checksum) is None:
        raise ValueError('pool_administration_host_unqualified')
    raw = private_state._private_read(path, limit=65536)
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError('pool_administration_host_unqualified')
    data = _json(raw)
    if set(data) != {'schema_version', 'runtime', 'scope', 'state_root', 'anchor_root', 'connection'} or data['schema_version'] != 'loom.pool-administration-host.v2':
        raise ValueError('pool_administration_host_unqualified')
    host = PoolAdministrationHostV2(runtime=_runtime(data['runtime']), scope=PoolAdministrationScopeV2.model_validate(data['scope']),
        state_root=Path(data['state_root']), anchor_root=Path(data['anchor_root']),
        connection=NebiusKubernetesConnection.model_validate(data['connection']))
    _host(host)
    return host


def _paths(host: PoolAdministrationHostV2, authorization_id: UUID) -> tuple[Path, Path]:
    return host.state_root / str(authorization_id), host.anchor_root / (str(authorization_id) + '.json')


def retain_authorized_operation(*, host: PoolAdministrationHostV2, request: _Request,
        authorization: PoolAdministrationAuthorizationV2, evidence_sha256: str) -> PoolAdministrationReferenceV2:
    """Issuer-only append of exact qualified intent; never called by dispatch."""
    _host(host)
    _qualified(PoolAdministrationJobV2(runtime=host.runtime, request=request, authorization=authorization, action='apply'))
    if _scope(authorization) != _scope(host.scope) or re.fullmatch(r'[0-9a-f]{64}', evidence_sha256) is None:
        raise ValueError('pool_administration_grant_unqualified')
    document = dict(schema_version='loom.pool-authorized-operation.v2', runtime=asdict(host.runtime),
        request=request.model_dump(mode='json'), authorization=authorization.model_dump(mode='json'), evidence_sha256=evidence_sha256)
    reference = PoolAdministrationReferenceV2(schema_version='loom.pool-administration-reference.v2',
        **_scope(host.scope), operation_id=request.operation_id, authorization_id=authorization.authorization_id,
        request_sha256=authorization.request_sha256, document_sha256=digest(document))
    state, anchor = _paths(host, authorization.authorization_id)
    private_state._private_directory(host.state_root)
    private_state._private_directory(host.anchor_root)
    with private_state._locked_state(host.anchor_root):
        if anchor.exists() or state.exists():
            _load(host, reference)
            return reference
        # Existing issuance is historical retrieval, never renewed authority.
        # Freshness is mandatory only when creating a new retained grant; the
        # apply boundary independently refuses expired mutation authority.
        if not authorization.issued_at <= datetime.now(UTC) < authorization.expires_at:
            raise ValueError('pool_administration_grant_expired')
        private_state._private_directory(state)
        private_state._write_private(state / 'authorized.json', json.dumps(document, sort_keys=True).encode())
        private_state._sync_directory(state)
        private_state._write_private(anchor, reference.model_dump_json().encode())
        private_state._sync_directory(host.anchor_root)
    return reference


def _load(host: PoolAdministrationHostV2, reference: PoolAdministrationReferenceV2) -> PoolAdministrationJobV2:
    _host(host)
    reference = PoolAdministrationReferenceV2.model_validate(reference.model_dump())
    state, anchor = _paths(host, reference.authorization_id)
    if (state != state.resolve() or _scope(reference) != _scope(host.scope)
            or PoolAdministrationReferenceV2.model_validate(_json(private_state._private_read(anchor))) != reference):
        raise ValueError('pool_administration_reference_unqualified')
    document = _json(private_state._private_read(state / 'authorized.json', limit=_MAX_OPERATION))
    if (set(document) != {'schema_version', 'runtime', 'request', 'authorization', 'evidence_sha256'}
            or document['schema_version'] != 'loom.pool-authorized-operation.v2'
            or digest(document) != reference.document_sha256 or _runtime(document['runtime']) != host.runtime
            or re.fullmatch(r'[0-9a-f]{64}', document['evidence_sha256']) is None):
        raise ValueError('pool_administration_reference_unqualified')
    request = _REQUEST.validate_python(document['request'])
    authorization = PoolAdministrationAuthorizationV2.model_validate(document['authorization'])
    if (_scope(authorization) != _scope(host.scope) or request.operation_id != reference.operation_id
            or authorization.authorization_id != reference.authorization_id or authorization.request_sha256 != reference.request_sha256):
        raise ValueError('pool_administration_reference_unqualified')
    return _qualified(PoolAdministrationJobV2(runtime=host.runtime, request=request, authorization=authorization, action='apply'))


def dispatch_administration(content: bytes, *, action: str, host: PoolAdministrationHostV2, connect: Connection) -> dict[str, Any]:
    stage = 'request'
    try:
        if type(content) is not bytes or not 0 < len(content) <= _MAX_COMMAND:
            raise ValueError
        command = _Command.model_validate(_json(content))
        if command.action != action:
            raise ValueError
        stage = 'selection'
        original = _load(host, command.reference)
        state, anchor = _paths(host, command.reference.authorization_id)
        if action != 'recover' and not original.authorization.issued_at <= datetime.now(UTC) < original.authorization.expires_at:
            raise ValueError
        base = dict(schema_version='loom.pool-administration-dispatch.v2', reference=command.reference.model_dump(mode='json'),
            action=action, observation_id=str(command.observation_id) if command.observation_id is not None else None)
        if action == 'prepare':
            return dict(base, status='prepared')
        request = PoolAdministrationJobV2(runtime=original.runtime, request=original.request, authorization=original.authorization,
            action='status' if action == 'status' else 'apply', observation_id=command.observation_id)
        job_state = state / 'execution'
        start = anchor.with_name(str(command.reference.authorization_id) + '.started.json')
        stage = 'execution'
        with private_state._locked_state(state):
            if action == 'recover':
                if _json(private_state._private_read(start)) != command.reference.model_dump(mode='json') or not (job_state / 'jobs').is_dir():
                    raise ValueError
            elif action == 'apply':
                if start.exists():
                    if _json(private_state._private_read(start)) != command.reference.model_dump(mode='json') or not (job_state / 'inputs').is_dir():
                        raise ValueError
                else:
                    # An earlier read shares immutable input, not apply authority.
                    # A missing apply anchor with an apply journal is data loss.
                    if (job_state / 'jobs' / ('loom-pool-admin-' + original.authorization.authorization_id.hex)).exists():
                        raise ValueError
                    # Persist the local recovery layout before announcing an
                    # apply attempt. No Kubernetes effect occurs here. A crash
                    # immediately after the start anchor can then resume staging;
                    # an already-started attempt with missing inputs still refuses.
                    private_state._private_directory(job_state)
                    private_state._private_directory(job_state / 'inputs')
                    private_state._private_directory(job_state / 'jobs')
                    private_state._write_private(start, command.reference.model_dump_json().encode())
                    private_state._sync_directory(host.anchor_root)
            with connect(request) as api:
                if action != 'recover':
                    stage_administration_job(request=request, api=api, state_dir=job_state)
                proof = api.administration_report(job_state)
            if _load(host, command.reference) != original:
                raise ValueError
        if proof is None:
            return dict(base, status='pending')
        if set(proof) != {'job_uid', 'pod_uid', 'receipt'}:
            raise ValueError
        for key in ('job_uid', 'pod_uid'):
            if str(UUID(proof[key])) != proof[key] or not UUID(proof[key]).int:
                raise ValueError
        validate_administration_report(request, proof['receipt'])
        return dict(base, status='observed' if action == 'status' else 'complete', proof=proof)
    except Exception:
        return dict(schema_version='loom.pool-administration-dispatch.v2', status='blocked', stage=stage)


@contextmanager
def authenticated_administration(host: PoolAdministrationHostV2, request: PoolAdministrationJobV2, *,
        qualify_mutation: Callable[[], None] | None = None) -> Iterator[AdministrationAPI]:
    _host(host)
    material = {path: private_state._private_read(path, limit=1024**2)
        for path in (host.connection.ca_file, host.connection.credentials_file)}
    credentials = NebiusKubernetesCredentials(host.connection)
    try:
        with HTTPSPoolAdministrationJobAPI(request=request, api_server=host.connection.endpoint,
                ssl_context=credentials.ssl_context, token=credentials.get_token_sync()) as api:
            if qualify_mutation is not None:
                def guard_mutation(outgoing: Any) -> None:
                    if outgoing.method not in {'GET', 'HEAD', 'OPTIONS'}:
                        qualify_mutation()
                api.client.event_hooks['request'].append(guard_mutation)
            yield api
        if any(private_state._private_read(path, limit=1024**2) != raw for path, raw in material.items()):
            raise ValueError('pool_administration_connection_changed')
    finally:
        asyncio.run(credentials.close())


def main(configuration: str, checksum: str, action: str, command: str) -> int:
    """Fixed launcher arguments except bounded public command; no ambient auth."""
    try:
        path = Path(configuration)
        host = load_administration_host(path, checksum)
        with open(os.devnull, 'w') as private_output, redirect_stdout(private_output), redirect_stderr(private_output):
            result = dispatch_administration(command.encode(), action=action, host=host,
                connect=lambda request: authenticated_administration(host, request))
        if load_administration_host(path, checksum) != host:
            raise ValueError
    except Exception:
        result = dict(schema_version='loom.pool-administration-dispatch.v2', status='blocked', stage='installation')
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 1 if result['status'] == 'blocked' else 0
