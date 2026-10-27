"""Source-bound private intake for the complete shared-pool handoff.

Retained installation inputs are historical evidence, not a reusable operator
connection. A separately protected current connection supplies live authority.
The concrete parent, not private JSON, qualifies readiness and permits effects.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_development_runtime_entry import DevelopmentRuntimePrivateInputs
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_operation import (
    operator_home as runtime_operator_home,
)
from scripts.ops.nebius_development_runtime_operation import (
    validate_operation as validate_runtime_operation,
)
from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication
from scripts.ops.nebius_development_runtime_retained import (
    CompletedDevelopmentRuntime,
    load_completed_runtime,
)
from scripts.ops.nebius_development_runtime_retained import (
    RetainedRuntimeReference as RetainedRuntimeReference,
)
from scripts.ops.nebius_development_runtime_setup import prepare_database_runtime
from scripts.ops.nebius_development_schema_upgrade import (
    DevelopmentSchemaReference,
    load_completed_development_schema_successor,
)
from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
from scripts.ops.nebius_pool_migration import PoolGuardTarget, migration_contract
from scripts.ops.nebius_pool_preactivation_operation import (
    PreactivationReference,
    RetainedPreactivationCatalog,
    load_completed_preactivation,
    load_preactivation_catalog,
    run_preactivation_catalog,
)
from scripts.ops.nebius_pool_predecessor import (
    CompletedPoolCutover,
    PoolPredecessorV1,
    load_completed_pool,
)
from scripts.ops.nebius_pool_storage_binding import StorageReference
from scripts.ops.nebius_pool_storage_operation import (
    RetainedClosedStorage,
    load_closed_storage,
    load_completed_storage,
    run_closed_storage,
)
from scripts.ops.nebius_shared_pool_api import (
    HTTPSSharedPoolOperationAPI,
    SharedPoolPhaseEvidence,
)
from scripts.ops.nebius_shared_pool_command import operator_home, validate_operation
from scripts.ops.nebius_shared_pool_handoff import (
    prepare_shared_pool_handoff,
    prepare_staging_forward,
)
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import (
    CompletedSharedPoolOperation,
    _qualify_bindings,
    load_completed_shared_pool_operation,
    run_shared_pool_operation,
)

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.manager import CandidateBundle

ROOT = Path(__file__).resolve().parents[2]
SOURCE_RECORD = ROOT / 'shared-pool-source.json'


class SharedPoolPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-shared-pool-inputs.v1', 'loom.nebius-shared-pool-inputs.v2',
        'loom.nebius-shared-pool-inputs.v3', 'loom.nebius-shared-pool-inputs.v4']
    preactivation: PreactivationReference | None = None
    schema_successor: DevelopmentSchemaReference | None = None
    storage: StorageReference | None = Field(default=None, exclude_if=lambda value: value is None)
    runtime: RetainedRuntimeReference
    legacy: PoolPredecessorV1
    staging_observation_file: Path
    staging_observation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    development_guard: PoolGuardTarget
    operator_connection: NebiusKubernetesConnection

    @model_validator(mode='after')
    def catalog_selection(self) -> Self:
        if self.schema_version.endswith('.v4'):
            if self.storage is None:
                raise ValueError('shared pool physical successor selection missing')
        elif (self.storage is not None or (self.schema_version.endswith('.v3')) != (self.schema_successor is not None)
                or (self.schema_successor is None and (self.schema_version.endswith('.v2')) != (self.preactivation is not None))):
            raise ValueError('shared pool catalog selection differs from input version')
        return self


@dataclass(frozen=True, repr=False)
class SharedPoolEntryContext:
    operation: dict[str, Any]
    inputs: SharedPoolPrivateInputs
    runtime: DevelopmentRuntimeInstallRequest
    legacy: CompletedPoolCutover
    observation: dict[str, Any]
    bindings: SharedPoolGuardBindings
    files: dict[Path, bytes]


@dataclass(frozen=True, repr=False)
class _SharedPoolInputRoots:
    inputs: SharedPoolPrivateInputs
    runtime: DevelopmentRuntimeInstallRequest
    legacy: CompletedPoolCutover
    observation: dict[str, Any]
    files: dict[Path, bytes]


@dataclass(frozen=True, repr=False)
class PreactivationEntryContext:
    operation: dict[str, Any]
    inputs: SharedPoolPrivateInputs
    runtime: DevelopmentRuntimeInstallRequest
    retained: RetainedPreactivationCatalog
    files: dict[Path, bytes]


@dataclass(frozen=True, repr=False)
class StorageEntryContext:
    operation: dict[str, Any]
    inputs: SharedPoolPrivateInputs
    runtime: DevelopmentRuntimeInstallRequest
    retained: RetainedClosedStorage
    files: dict[Path, bytes]


@dataclass(frozen=True, repr=False)
class SharedPoolHistoricalContext:
    """Original inputs only; deliberately has no active operator connection."""

    operation: dict[str, Any]
    runtime: DevelopmentRuntimeInstallRequest
    legacy: CompletedPoolCutover
    observation: dict[str, Any]
    bindings: SharedPoolGuardBindings
    files: dict[Path, bytes]


def _private(path: Path) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise ValueError
    return private_state._private_read(path, limit=4 * 1024**2)


def _retained_runtime(reference: RetainedRuntimeReference, files: dict[Path, bytes]) -> DevelopmentRuntimeInstallRequest:
    """Rebuild completed runtime evidence without retired operator file access."""
    return _retained_completed_runtime(reference, files).request


def _retained_completed_runtime(reference: RetainedRuntimeReference, files: dict[Path, bytes]) -> CompletedDevelopmentRuntime:
    """Load once for a private composing scope; never accept a cached projection.

    The caller retains these exact byte pins and must recheck them before using
    the result after further projection or observation. Every invocation loads
    the source and completed runtime afresh.
    """
    def read(path: Path) -> bytes:
        raw = _private(path)
        if path in files:
            raise ValueError
        files[path] = raw
        return raw

    raw = read(reference.operation_path)
    operation = _json(raw)
    validate_runtime_operation(operation)
    if hashlib.sha256(raw).hexdigest() != reference.operation_sha256:
        raise ValueError
    path = Path(operation['inputs_path'])
    root = path.parent
    if (reference.operation_path != root / 'operation.json'
            and (reference.operation_path.parent.parent != root / 'releases'
                or re.fullmatch(r'[0-9a-f]{64}', reference.operation_path.parent.name) is None)):
        raise ValueError
    raw = read(path)
    if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
        raise ValueError
    inputs = DevelopmentRuntimePrivateInputs.model_validate(_json(raw))
    source_path = reference.operation_path.parent / 'development-runtime-source.json'
    source = PreparedDevelopmentSource.model_validate(_json(read(source_path)))
    if source.source_sha != operation['source_sha']:
        raise ValueError
    paths = (inputs.collector_credential_file, inputs.registry_credential_file,
        inputs.actuator_password_file, inputs.batch_runner_token_file, *(inputs.cache_files or {}).values())
    if len(paths) != len(set(paths)) or set(paths) & {inputs.operator_connection.ca_file, inputs.operator_connection.credentials_file}:
        raise ValueError
    for material in paths:
        read(material)
    publication = DevelopmentRuntimePublication(inputs.publication_source or source, inputs.publication,
        CandidateBundle(inputs.publication.candidate_id, inputs.candidate, inputs.profile))
    database = prepare_database_runtime(inputs.retained, publication=publication, operation_id=UUID(operation['operation_id']),
        data_publication=inputs.data_runtime.selection() if inputs.data_runtime is not None else None,
        actuator_password=files[inputs.actuator_password_file].decode(), batch_runner_token=files[inputs.batch_runner_token_file].decode())
    manager = database.manager.retained.request.retained
    if (manager.binding.installation_id != operation['installation_id'] or manager.binding.namespace != operation['namespace']
            or Path(manager.operation['inputs_path']).parents[3] != runtime_operator_home(operation)
            or inputs.operator_connection.endpoint != database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
        raise ValueError
    request = DevelopmentRuntimeInstallRequest(database, inputs.collector_scope, files[inputs.collector_credential_file],
        inputs.registry_scope, files[inputs.registry_credential_file],
        {key: files[item].decode() for key, item in inputs.cache_files.items()} if inputs.cache_files is not None else None)
    completed = load_completed_runtime(request)
    if files.keys() & completed.files.keys():
        raise ValueError
    files.update(completed.files)
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError
    return completed


def load_inputs(operation: dict[str, Any]) -> SharedPoolEntryContext:
    """Bind exact retained and captured scope before authentication or effects."""
    return _load_inputs(operation, source_record=SOURCE_RECORD, include_operator=True)


def _load_input_roots(operation: dict[str, Any], *, source_record: Path, include_operator: bool) -> _SharedPoolInputRoots:
    try:
        validate_operation(operation)
        path = Path(operation['inputs_path'])
        files = {path: _private(path), source_record: _private(source_record)}
        if len(files) != 2 or hashlib.sha256(files[path]).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = SharedPoolPrivateInputs.model_validate(_json(files[path]))
        source = PreparedDevelopmentSource.model_validate(_json(files[source_record]))
        runtime = _retained_runtime(inputs.runtime, files)
        manager = runtime.database.manager.retained.request.retained
        registration = runtime.database.manager.retained.request.registration
        # Tooling is source-bound by the protected delivery. The completed runtime
        # has its own retained publication and need not be reinstalled to fix tooling.
        if (source.source_sha != operation['source_sha']
                or (operation['installation_id'], operation['namespace'], operation['operation_id']) != (
                    manager.binding.installation_id, manager.binding.namespace, str(registration.spec.operation_id))
                or operator_home(operation) != Path(manager.operation['inputs_path']).parents[3]
                or inputs.operator_connection.endpoint != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError
        paths = (inputs.staging_observation_file, inputs.operator_connection.ca_file, inputs.operator_connection.credentials_file)
        if len(set(paths)) != len(paths) or set(paths) & files.keys():
            raise ValueError
        # Old operator credentials are neither ancestor evidence nor current
        # authority. Normal-release history must remain readable after rotation.
        files.update({item: _private(item) for item in (paths if include_operator else paths[:1])})
        if hashlib.sha256(files[inputs.staging_observation_file]).hexdigest() != inputs.staging_observation_sha256:
            raise ValueError
        observation = _json(files[inputs.staging_observation_file])
        original = load_pool_cutover_inputs(inputs.legacy.operation).original
        legacy = load_completed_pool(inputs.legacy, original=original)
        for historical, checksum in legacy.history.items():
            raw = _private(historical)
            if hashlib.sha256(raw).hexdigest() != checksum or (historical in files and files[historical] != raw):
                raise ValueError
            files[historical] = raw
        if any(_private(item) != raw for item, raw in files.items()):
            raise ValueError
        return _SharedPoolInputRoots(inputs, runtime, legacy, observation, files)
    except Exception:
        raise ValueError('shared pool private input roots unqualified') from None


def _load_inputs(operation: dict[str, Any], *, source_record: Path, include_operator: bool) -> SharedPoolEntryContext:
    try:
        roots = _load_input_roots(operation, source_record=source_record, include_operator=include_operator)
        inputs, runtime, legacy, observation, files = roots.inputs, roots.runtime, roots.legacy, roots.observation, roots.files
        successor = None
        if inputs.schema_successor is not None:
            successor = load_completed_development_schema_successor(runtime=runtime, reference=inputs.schema_successor)
            for historical, raw in successor.files.items():
                if historical in files and files[historical] != raw:
                    raise ValueError
                files[historical] = raw
        preactivation = None
        if inputs.preactivation is not None:
            preactivation = load_completed_preactivation(runtime=inputs.runtime, reference=inputs.preactivation, repo_root=ROOT)
            for historical, raw in preactivation.files.items():
                if historical in files and files[historical] != raw:
                    raise ValueError
                files[historical] = raw
        storage = None
        if inputs.storage is not None:
            storage = load_completed_storage(runtime=inputs.runtime, reference=inputs.storage)
            for historical, raw in storage.files.items():
                if historical in files and files[historical] != raw:
                    raise ValueError
                files[historical] = raw
        plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation, legacy=legacy, repo_root=ROOT,
            preactivation=preactivation, schema_successor=successor, storage=storage)
        dev, = (row for row in plan.migration.guards if row.namespace == 'loom-dev')
        guard = inputs.development_guard
        if (guard.participant_id, guard.namespace, guard.namespace_uid) != (dev.participant_id, dev.namespace, dev.namespace_uid):
            raise ValueError
        migration = replace(plan.migration, guards=tuple(guard if row == dev else row for row in plan.migration.guards))
        bindings = SharedPoolGuardBindings(plan, migration, digest({'handoff': plan.input_digest, 'migration': migration_contract(migration)}))
        _qualify_bindings(runtime=runtime, observation=observation, legacy=legacy, repo_root=ROOT, bindings=bindings)
        if any(_private(item) != raw for item, raw in files.items()):
            raise ValueError
        return SharedPoolEntryContext(dict(operation), inputs, runtime, legacy, observation, bindings, files)
    except Exception:
        raise ValueError('shared pool private inputs unqualified') from None


def load_preactivation_inputs(operation: dict[str, Any]) -> PreactivationEntryContext:
    """Separate pending intake: never manufacture completed handoff authority."""
    try:
        roots = _load_input_roots(operation, source_record=SOURCE_RECORD, include_operator=True)
        inputs, files = roots.inputs, roots.files
        if inputs.preactivation is None:
            raise ValueError('catalog refresh requires an explicit selection')
        retained = load_preactivation_catalog(runtime=inputs.runtime, reference=inputs.preactivation, repo_root=ROOT)
        if retained.inputs.schema_successor != inputs.schema_successor:
            raise ValueError
        if inputs.storage is not None and retained.inputs.storage != inputs.storage:
            raise ValueError('catalog refresh physical ancestry differs')
        if retained.runtime != roots.runtime or retained.inputs.source.model_dump(mode='json') != roots.observation:
            raise ValueError('catalog refresh source differs from captured handoff')
        # Prove this selected successor can satisfy the same pure staging
        # identity/catalog contract that the subsequent handoff will consume.
        prepare_staging_forward(registration=retained.registration, observation=roots.observation,
            legacy=roots.legacy, repo_root=ROOT)
        for path, raw in retained.files.items():
            if path in files and files[path] != raw:
                raise ValueError
            files[path] = raw
        if any(_private(path) != raw for path, raw in files.items()):
            raise ValueError
        return PreactivationEntryContext(dict(operation), inputs, roots.runtime, retained, files)
    except Exception:
        raise ValueError('shared pool preactivation inputs unqualified') from None


def load_storage_inputs(operation: dict[str, Any]) -> StorageEntryContext:
    """Read an explicit pending correction without claiming activation readiness."""
    try:
        roots = _load_input_roots(operation, source_record=SOURCE_RECORD, include_operator=True)
        inputs, files = roots.inputs, roots.files
        if inputs.storage is None or inputs.preactivation is not None or inputs.schema_successor is not None:
            raise ValueError('storage correction requires its own explicit selection')
        retained = load_closed_storage(runtime=inputs.runtime, reference=inputs.storage)
        if retained.runtime != roots.runtime:
            raise ValueError('storage correction runtime differs')
        for path, raw in retained.files.items():
            if path in files and files[path] != raw:
                raise ValueError
            files[path] = raw
        if any(_private(path) != raw for path, raw in files.items()):
            raise ValueError
        return StorageEntryContext(dict(operation), inputs, roots.runtime, retained, files)
    except Exception:
        raise ValueError('shared pool storage inputs unqualified') from None


def load_historical_inputs(*, operation_path: Path, operation_sha256: str) -> SharedPoolHistoricalContext:
    """Read a pinned original handoff without adopting its connection.

    The original operation and source sidecar must remain in their prepared root
    or immutable tooling release. The currently executing candidate's source
    sidecar is unrelated. This does not assert that the handoff was completed;
    its anchored parent and child receipts still require semantic validation.
    """
    try:
        raw = _private(operation_path)
        if (re.fullmatch(r'[0-9a-f]{64}', operation_sha256) is None
                or hashlib.sha256(raw).hexdigest() != operation_sha256):
            raise ValueError
        operation = _json(raw)
        validate_operation(operation)
        root = Path(operation['inputs_path']).parent
        if (operation_path != root / 'operation.json'
                and (operation_path.name != 'operation.json' or operation_path.parent.parent != root / 'releases'
                    or re.fullmatch(r'[0-9a-f]{64}', operation_path.parent.name) is None)):
            raise ValueError
        context = _load_inputs(operation, source_record=operation_path.parent / 'shared-pool-source.json',
            include_operator=False)
        if operation_path in context.files or _private(operation_path) != raw:
            raise ValueError
        files = {**context.files, operation_path: raw}
        if any(_private(path) != before for path, before in files.items()):
            raise ValueError
        return SharedPoolHistoricalContext(dict(operation), context.runtime, context.legacy,
            context.observation, context.bindings, files)
    except Exception:
        raise ValueError('shared pool historical inputs unqualified; preserve evidence') from None


def load_completed_history(*, operation_path: Path, operation_sha256: str
        ) -> tuple[SharedPoolHistoricalContext, CompletedSharedPoolOperation]:
    """Load the original input binding and real terminal children, without I/O authority."""
    try:
        historical = load_historical_inputs(operation_path=operation_path, operation_sha256=operation_sha256)
        completed = load_completed_shared_pool_operation(runtime=historical.runtime,
            observation=historical.observation, legacy=historical.legacy, repo_root=ROOT,
            bindings=historical.bindings, api=SharedPoolPhaseEvidence(bindings=historical.bindings))
        if any(_private(path) != before for path, before in historical.files.items()):
            raise ValueError
        return historical, completed
    except Exception:
        raise ValueError('completed shared pool history unavailable; preserve evidence') from None


@contextmanager
def connected_api(inputs: SharedPoolEntryContext) -> Iterator[HTTPSSharedPoolOperationAPI]:
    """Use the current protected identity; never historical operator credentials."""
    files = dict(inputs.files)

    def unchanged() -> None:
        if any(_private(path) != raw for path, raw in files.items()):
            raise ValueError('shared pool connection inputs changed')

    unchanged()
    connection = inputs.inputs.operator_connection
    context, token = asyncio.run(_transport(connection))
    unchanged()
    with HTTPSSharedPoolOperationAPI(bindings=inputs.bindings, observation=inputs.observation,
            repo_root=ROOT, api_server=connection.endpoint, ssl_context=context, token=token,
            operator_credentials=connection.credentials_file, private_files=files) as api:
        unchanged()
        yield api
        unchanged()


def main(operation_path: str, action: str) -> int:
    """Dispatch one fixed parent action and export only bounded, non-secret state."""
    stage = 'operation'
    try:
        if action not in {'qualify', 'preflight', 'install', 'recover', 'refresh-catalog', 'refresh-storage'}:
            raise ValueError
        path = Path(operation_path)
        raw = _private(path)
        operation = _json(raw)
        validate_operation(operation)
        if action == 'qualify':
            print(json.dumps({'status': 'tooling_qualified'}))
            return 0
        stage = 'inputs'
        if action == 'refresh-storage':
            from scripts.ops.nebius_pool_storage_live import HTTPSClosedStorageOperationAPI

            storage_inputs = load_storage_inputs(operation)
            if path in storage_inputs.files or _private(path) != raw:
                raise ValueError
            storage_inputs.files[path] = raw
            stage = 'connection'
            connection = storage_inputs.inputs.operator_connection
            context, token = asyncio.run(_transport(connection))
            if any(_private(item) != before for item, before in storage_inputs.files.items()):
                raise ValueError
            with HTTPSClosedStorageOperationAPI(retained=storage_inputs.retained,
                    operator_credentials=connection.credentials_file, api_server=connection.endpoint,
                    ssl_context=context, token=token, private_files=storage_inputs.files) as storage_api:
                stage = 'storage'
                complete = run_closed_storage(retained=storage_inputs.retained, api=storage_api)
            result = {'status': 'shared_pool_storage_refreshed' if complete else 'storage_pending',
                'pool_id': str(storage_inputs.retained.inputs.refresh.after.pool_id),
                'storage_id': str(storage_inputs.retained.reference.operation_id),
                'admission_open': False, 'writer_migration_complete': False}
            identity = {key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
            print(json.dumps({**result, **identity}, sort_keys=True))
            return 0
        if action == 'refresh-catalog':
            from scripts.ops.nebius_pool_preactivation_live import HTTPSPreactivationOperationAPI

            prepared = load_preactivation_inputs(operation)
            if path in prepared.files or _private(path) != raw:
                raise ValueError
            prepared.files[path] = raw
            stage = 'connection'
            connection = prepared.inputs.operator_connection
            context, token = asyncio.run(_transport(connection))
            if any(_private(item) != before for item, before in prepared.files.items()):
                raise ValueError
            with HTTPSPreactivationOperationAPI(retained=prepared.retained, repo_root=ROOT,
                    api_server=connection.endpoint, ssl_context=context, token=token, private_files=prepared.files) as catalog_api:
                stage = 'catalog'
                complete = run_preactivation_catalog(retained=prepared.retained, api=catalog_api, repo_root=ROOT)
            result = {'status': 'shared_pool_catalog_refreshed' if complete else 'catalog_pending',
                'pool_id': str(prepared.retained.inputs.refresh.after.pool_id),
                'preactivation_id': str(prepared.retained.reference.operation_id),
                'admission_open': False, 'writer_migration_complete': False}
            identity = {key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
            print(json.dumps({**result, **identity}, sort_keys=True))
            return 0
        inputs = load_inputs(operation)
        if path in inputs.files or _private(path) != raw:
            raise ValueError
        inputs.files[path] = raw
        stage = 'connection'
        with connected_api(inputs) as api:
            stage = 'handoff'
            result = run_shared_pool_operation(runtime=inputs.runtime, observation=inputs.observation,
                legacy=inputs.legacy, repo_root=ROOT, bindings=inputs.bindings, api=api,
                action=cast(Literal['preflight', 'install', 'recover'], action))
        identity = {key: operation[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
        print(json.dumps({**result, **identity}, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked', 'stage': stage}))
        return 1
