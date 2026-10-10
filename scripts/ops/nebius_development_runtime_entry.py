"""Source-bound protected entry for the independent CLOSED development runtime."""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
from scripts.ops.nebius_development_collector_cloud import DevelopmentCollectorCloudScope
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_live import _private
from scripts.ops.nebius_development_pool_retained import RetainedDevelopmentPoolReference
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_development_runtime_api import HTTPSDevelopmentRuntimeAPI
from scripts.ops.nebius_development_runtime_install import (
    DevelopmentRuntimeInstallRequest,
    install_development_runtime,
    prepare_runtime_install,
)
from scripts.ops.nebius_development_runtime_operation import operator_home, validate_operation
from scripts.ops.nebius_development_runtime_render import DevelopmentRuntimePublication
from scripts.ops.nebius_development_runtime_setup import prepare_database_runtime

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_service.environment_management.candidates import ProtectedPublication, _json
from loom_service.environment_management.manager import CandidateBundle

SOURCE_RECORD = Path(__file__).resolve().parents[2] / 'development-runtime-source.json'


class DevelopmentRuntimePrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-development-runtime-inputs.v1']
    retained: RetainedDevelopmentPoolReference
    publication: ProtectedPublication
    candidate: dict[str, Any]
    profile: dict[str, Any]
    collector_scope: DevelopmentCollectorCloudScope
    collector_credential_file: Path
    registry_scope: DevelopmentRegistryCloudScope
    registry_credential_file: Path
    actuator_password_file: Path
    batch_runner_token_file: Path
    cache_files: dict[str, Path] | None = None
    operator_connection: NebiusKubernetesConnection


def load_inputs(operation: dict[str, Any]) -> tuple[DevelopmentRuntimePrivateInputs, DevelopmentRuntimeInstallRequest, dict[Path, bytes]]:
    try:
        validate_operation(operation)
        path = Path(operation['inputs_path'])
        raw, source_raw = _private(path), _private(SOURCE_RECORD)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = DevelopmentRuntimePrivateInputs.model_validate(_json(raw))
        source = PreparedDevelopmentSource.model_validate(_json(source_raw))
        connection = inputs.operator_connection
        paths = [path, SOURCE_RECORD, connection.ca_file, connection.credentials_file,
            inputs.collector_credential_file, inputs.registry_credential_file,
            inputs.actuator_password_file, inputs.batch_runner_token_file, *(inputs.cache_files or {}).values()]
        if len(paths) != len(set(paths)) or any(not item.is_absolute() or item != item.resolve() for item in paths):
            raise ValueError
        files = {item: _private(item) for item in paths}
        if files[path] != raw or files[SOURCE_RECORD] != source_raw:
            raise ValueError
        publication = DevelopmentRuntimePublication(source, inputs.publication,
            CandidateBundle(inputs.publication.candidate_id, inputs.candidate, inputs.profile))
        database = prepare_database_runtime(inputs.retained, publication=publication,
            operation_id=UUID(operation['operation_id']),
            actuator_password=files[inputs.actuator_password_file].decode(),
            batch_runner_token=files[inputs.batch_runner_token_file].decode())
        retained = database.manager.retained.request.retained
        if (source.source_sha != operation['source_sha']
                or retained.binding.installation_id != operation['installation_id']
                or retained.binding.namespace != operation['namespace']
                or Path(retained.operation['inputs_path']).parents[3] != operator_home(operation)
                or connection.endpoint != database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')
                or set(paths) & database.manager.retained.files.keys()
                or set(paths) & database.foundation.files.keys()):
            raise ValueError
        request = DevelopmentRuntimeInstallRequest(database, inputs.collector_scope, files[inputs.collector_credential_file],
            inputs.registry_scope, files[inputs.registry_credential_file],
            {key: files[item].decode() for key, item in inputs.cache_files.items()} if inputs.cache_files is not None else None)
        prepare_runtime_install(request)
        if any(_private(item) != value for item, value in files.items()):
            raise ValueError
        return inputs, request, files
    except Exception:
        raise ValueError('development runtime private inputs unqualified') from None


@contextmanager
def connected_api(inputs: DevelopmentRuntimePrivateInputs, request: DevelopmentRuntimeInstallRequest,
                  files: dict[Path, bytes]) -> Iterator[HTTPSDevelopmentRuntimeAPI]:
    connection = inputs.operator_connection
    context, token = asyncio.run(_transport(connection))
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError('development runtime connection inputs changed')
    with HTTPSDevelopmentRuntimeAPI(request=request, api_server=connection.endpoint,
            ssl_context=context, token=token, operator_credentials=connection.credentials_file, private_files=files) as api:
        yield api


def main(operation_path: str, action: str) -> int:
    stage = 'operation'
    try:
        if action not in {'qualify', 'preflight', 'install'}:
            raise ValueError
        path = Path(operation_path)
        raw = _private(path)
        operation = _json(raw)
        validate_operation(operation)
        if action == 'qualify':
            print(json.dumps({'status': 'tooling_qualified'}))
            return 0
        stage = 'inputs'
        inputs, request, files = load_inputs(operation)
        if _private(path) != raw:
            raise ValueError
        files[path] = raw
        stage = 'connection'
        with connected_api(inputs, request, files) as api:
            stage = 'installation'
            result = install_development_runtime(request=request, api=api, execute=action == 'install')
        print(json.dumps({**result, 'source_sha': operation['source_sha']}, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked', 'stage': stage}))
        return 1
