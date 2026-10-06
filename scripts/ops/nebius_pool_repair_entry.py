"""Separate protected tooling authority for the original closed pool operation.

The original candidate, inputs, credentials, dispatch lock and phase journals
remain unchanged. Only this bound continuation may advance repaired activation.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from scripts.ops.nebius_management_entry import EntryError, _private
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_cutover_entry import (
    _CONNECTION_ERRORS,
    PoolCutoverContext,
    connected_pool_api,
    load_pool_cutover_inputs,
)
from scripts.ops.nebius_pool_cutover_live import PoolCutoverChecks
from scripts.ops.nebius_pool_operation import PoolOperationError, run_pool_operation
from scripts.ops.nebius_pool_startup_repair import PoolStartupRepairBinding, _repair_record


class PoolRepairPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-pool-startup-repair-private-inputs.v1']
    original_operation: dict[str, Any]
    binding: PoolStartupRepairBinding


@dataclass(frozen=True, repr=False)
class PoolRepairContext:
    operation: dict[str, Any]
    inputs: PoolRepairPrivateInputs
    original: PoolCutoverContext


def load_pool_repair_inputs(operation: dict[str, Any]) -> PoolRepairContext:
    """Read/qualify both identities and entry history before opening a connection."""
    try:
        validate_operation(operation)
        if operation['schema'] != 'loom.nebius-pool-startup-repair-operation.v1':
            raise ValueError
        raw = _private(Path(operation['inputs_path']), 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = PoolRepairPrivateInputs.model_validate_json(raw)
        original = load_pool_cutover_inputs(inputs.original_operation)
        binding = inputs.binding
        original_digest = hashlib.sha256(json.dumps(inputs.original_operation, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if (str(binding.operation_id) != operation['operation_id'] or binding.source_sha != operation['source_sha']
                or binding.original_operation_sha256 != original_digest
                or binding.inputs_sha256 != original.operation['inputs_sha256']
                or operation['source_sha'] == original.operation['source_sha']
                or operation['original_operation_id'] != original.operation['operation_id']
                or any(operation[key] != original.operation[key] for key in ('installation_id', 'namespace'))
                or Path(operation['inputs_path']).parent.parent.parent != Path(original.operation['inputs_path']).parent.parent.parent
                or original.request.application_delivery is None or original.inputs.source_delivery_version != 'v1'):
            raise ValueError
        _repair_record(original.request, state=Path(original.operation['state_dir']),
            anchor=Path(original.operation['anchor_dir']), binding=binding)
        return PoolRepairContext(operation, inputs, original)
    except Exception:
        raise EntryError('pool repair private inputs unqualified') from None


@dataclass(frozen=True, repr=False)
class _RepairChecks:
    original: PoolCutoverChecks
    context: PoolRepairContext

    def _current(self) -> None:
        if load_pool_repair_inputs(self.context.operation) != self.context:
            raise EntryError('pool repair private inputs changed')

    def preflight(self, request: PoolCutoverRequest) -> None:
        self._current()
        self.original.preflight(request)
        self._current()

    def qualify_initial_capacity(self, request: PoolCutoverRequest) -> None:
        self._current()
        self.original.qualify_initial_capacity(request)
        self._current()

    def qualify_quiescence(self) -> None:
        self._current()
        self.original.qualify_quiescence()
        self._current()


def execute_pool_repair(context: PoolRepairContext, action: str) -> dict[str, Any]:
    """Compose the exact original connection, repair, runtime barriers and recovery."""
    if action not in {'preflight', 'install', 'rollback'} or load_pool_repair_inputs(context.operation) != context:
        raise EntryError('pool repair private binding differs')
    try:
        with connected_pool_api(context.original) as parent:
            parent.checks = _RepairChecks(parent.checks, context)
            result = run_pool_operation(parent=parent, tokens=context.original.tokens,
                action=action, repair_binding=context.inputs.binding)
            if result.get('operation_id') != context.operation['original_operation_id']:
                raise ValueError
            return {**result, 'operation_id': context.operation['operation_id'],
                'original_operation_id': context.operation['original_operation_id'], 'telemetry': parent.guards.telemetry_report()}
    except PoolOperationError:
        raise
    except EntryError as error:
        raise PoolOperationError('private_inputs' if str(error).startswith('pool repair private ') else
            _CONNECTION_ERRORS.get(str(error), 'connection')) from None
    except Exception:
        raise PoolOperationError('connection') from None
