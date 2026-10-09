"""Private protected entry for a stopped, independent development pool."""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_live import _private
from scripts.ops.nebius_development_management_retained import (
    RetainedManagementReference,
    load_retained_management,
)
from scripts.ops.nebius_development_pool_install import install_development_pool
from scripts.ops.nebius_development_pool_intent import DevelopmentPoolIntent, prepare_intent
from scripts.ops.nebius_development_pool_live import HTTPSDevelopmentPoolInstallAPI
from scripts.ops.nebius_development_pool_operation import operator_home, validate_operation
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource

from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom_service.environment_management.candidates import _json

SOURCE_RECORD = Path(__file__).resolve().parents[2] / 'development-pool-source.json'


class DevelopmentPoolPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-development-pool-inputs.v1']
    retained: RetainedManagementReference
    catalog: dict[str, Any]
    tokens: dict[UUID, SecretStr] = Field(repr=False)
    operator_connection: NebiusKubernetesConnection


def load_inputs(operation: dict[str, Any]) -> tuple[DevelopmentPoolPrivateInputs, DevelopmentPoolIntent, dict[Path, bytes]]:
    try:
        validate_operation(operation)
        path = Path(operation['inputs_path'])
        raw, source_raw = _private(path), _private(SOURCE_RECORD)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError()
        inputs = DevelopmentPoolPrivateInputs.model_validate(_json(raw))
        source = PreparedDevelopmentSource.model_validate(_json(source_raw))
        retained = load_retained_management(inputs.retained)
        connection = inputs.operator_connection
        if (source.source_sha != operation['source_sha']
                or retained.binding.installation_id != operation['installation_id']
                or retained.binding.namespace != operation['namespace']
                or Path(retained.operation['inputs_path']).parents[3] != operator_home(operation)
                or inputs.catalog.get('operation_id') != operation['operation_id']
                or connection.endpoint != retained.inputs.deployment.installation.foundation.platform_config[
                    'kubernetes_api_server'].rstrip('/')):
            raise ValueError()
        paths = {path, SOURCE_RECORD, connection.ca_file, connection.credentials_file}
        if len(paths) != 4 or paths & retained.files.keys():
            raise ValueError()
        files = {item: _private(item) for item in paths}
        if files[path] != raw or files[SOURCE_RECORD] != source_raw:
            raise ValueError()
        intent = prepare_intent(reference=inputs.retained, catalog=inputs.catalog,
            tokens={identity: value.get_secret_value() for identity, value in inputs.tokens.items()})
        if any(_private(item) != value for item, value in files.items()):
            raise ValueError()
        return inputs, intent, files
    except Exception:
        raise ValueError('development pool private inputs unqualified') from None


@contextmanager
def connected_api(inputs: DevelopmentPoolPrivateInputs, intent: DevelopmentPoolIntent,
                  files: dict[Path, bytes]) -> Iterator[HTTPSDevelopmentPoolInstallAPI]:
    context, token = asyncio.run(_transport(inputs.operator_connection))
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError('development pool connection inputs changed')
    with HTTPSDevelopmentPoolInstallAPI(intent=intent, api_server=inputs.operator_connection.endpoint,
            ssl_context=context, token=token, private_files=files) as api:
        yield api


def main(operation_path: str, action: str) -> int:
    stage = 'operation'
    try:
        if action not in {'qualify', 'preflight', 'install'}:
            raise ValueError()
        path = Path(operation_path)
        raw = _private(path)
        operation = _json(raw)
        validate_operation(operation)
        if action == 'qualify':
            print(json.dumps({'status': 'tooling_qualified'}))
            return 0
        stage = 'inputs'
        inputs, intent, files = load_inputs(operation)
        if _private(path) != raw:
            raise ValueError()
        files[path] = raw
        stage = 'connection'
        with connected_api(inputs, intent, files) as api:
            stage = 'installation'
            result = install_development_pool(intent=intent, api=api, execute=action == 'install')
        print(json.dumps({**result, 'source_sha': operation['source_sha']}, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked', 'stage': stage}))
        return 1
