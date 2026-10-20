"""Installed, target-bound lifecycle dispatch; runner input is identity only.

The separately installed forced command pins this file, configuration and each
operation's private input bytes. Installing a new operation is an explicit host
administration action, never a capability of the runner key.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationScopeV2

from loom_service.environment_management.candidates import _json


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class LifecycleOperationV2(_Strict):
    kind: Literal['provision', 'enroll', 'activate', 'suspend', 'resume', 'retire', 'release', 'rollback', 'import']
    inputs_path: Path
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class LifecycleConfigurationV2(_Strict):
    schema_version: Literal['loom.environment-lifecycle-configuration.v2']
    scope: PoolAdministrationScopeV2
    operations: dict[UUID, LifecycleOperationV2] = Field(min_length=1, max_length=128)

    @model_validator(mode='after')
    def scoped(self) -> LifecycleConfigurationV2:
        if self.scope.authority == 'platform' or any(not identity.int for identity in self.operations):
            raise ValueError('environment_lifecycle_configuration_unqualified')
        return self


class LifecycleCommandV2(_Strict):
    schema_version: Literal['loom.environment-lifecycle-command.v2']
    action: Literal['prepare', 'apply', 'recover', 'status']
    scope: PoolAdministrationScopeV2
    operation_id: UUID

    @model_validator(mode='after')
    def scoped(self) -> LifecycleCommandV2:
        if not self.operation_id.int or self.scope.authority == 'platform':
            raise ValueError('environment_lifecycle_command_unqualified')
        return self


def _private(path: Path) -> bytes:
    if not path.is_absolute() or path == Path('/') or path != path.resolve():
        raise ValueError('environment_lifecycle_path_unqualified')
    try:
        return state._private_read(path, limit=8 * 1024**2)
    except (OSError, state.CertificateError):
        raise ValueError('environment_lifecycle_private_input_unqualified') from None


def load_lifecycle_configuration(path: Path, checksum: str) -> LifecycleConfigurationV2:
    raw = _private(path)
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError('environment_lifecycle_configuration_changed')
    config = LifecycleConfigurationV2.model_validate(_json(raw))
    paths = [row.inputs_path for row in config.operations.values()]
    if path in paths or len(set(paths)) != len(paths):
        raise ValueError('environment_lifecycle_input_alias')
    for row in config.operations.values():
        if hashlib.sha256(_private(row.inputs_path)).hexdigest() != row.inputs_sha256:
            raise ValueError('environment_lifecycle_inputs_changed')
    return config


def select_operation(config: LifecycleConfigurationV2, value: dict[str, Any]) -> LifecycleOperationV2:
    command = LifecycleCommandV2.model_validate(value)
    if command.scope != config.scope or command.operation_id not in config.operations:
        raise ValueError('environment_lifecycle_target_not_allowed')
    return config.operations[command.operation_id]


def main(configuration: str, checksum: str, action: str, command: str) -> int:
    """All arguments except bounded command are fixed in the installed launcher."""
    result: dict[str, Any] = {'schema_version': 'loom.environment-lifecycle-result.v2',
        'status': 'blocked', 'phase': 'selection'}
    try:
        if len(command.encode()) > 16384:
            raise ValueError
        request = LifecycleCommandV2.model_validate(_json(command.encode()))
        if request.action != action:
            raise ValueError
        path = Path(configuration)
        config = load_lifecycle_configuration(path, checksum)
        operation = select_operation(config, request.model_dump(mode='json'))
        result.update(operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'), action=action)
        from scripts.ops.nebius_environment_lifecycle import execute_lifecycle
        with open(os.devnull, 'w') as output, redirect_stdout(output), redirect_stderr(output):
            result = asyncio.run(execute_lifecycle(configuration=config, request=request, operation=operation,
                unchanged=lambda: load_lifecycle_configuration(path, checksum)))
        if load_lifecycle_configuration(path, checksum) != config:
            raise ValueError
    except Exception:
        result = dict(result, status='blocked', phase='execution')
        result.pop('admission_open', None)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return int(result['status'] == 'blocked')
