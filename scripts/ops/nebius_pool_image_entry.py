"""Protected image-only continuation of an unchanged pre-opening pool operation."""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx
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
from scripts.ops.nebius_pool_manager_image_history import (
    ManagerImageRepairBinding,
    manager_image_entry,
)
from scripts.ops.nebius_pool_manager_image_stage import qualify_image_entry_closed
from scripts.ops.nebius_pool_operation import PoolOperationError, run_pool_operation

from loom.execution_image_admission import ImageAdmissionKeyring
from loom_service.environment_management.candidates import GitHubCandidateCatalog

SCHEMA_PROOF_PATH = Path(__file__).resolve().parents[2] / 'manager-schema.json'


class ImageRepairPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-pool-manager-image-private-inputs.v1']
    original_operation: dict[str, Any]
    binding: ManagerImageRepairBinding


@dataclass(frozen=True, repr=False)
class ImageRepairContext:
    operation: dict[str, Any]
    inputs: ImageRepairPrivateInputs
    original: PoolCutoverContext


def load_image_repair_inputs(operation: dict[str, Any]) -> ImageRepairContext:
    try:
        validate_operation(operation)
        if operation['schema'] != 'loom.nebius-pool-startup-repair-operation.v2':
            raise ValueError
        raw = _private(Path(operation['inputs_path']), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = ImageRepairPrivateInputs.model_validate_json(raw)
        original = load_pool_cutover_inputs(inputs.original_operation)
        binding = inputs.binding
        original_digest = hashlib.sha256(json.dumps(inputs.original_operation,
            sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if (str(binding.operation_id) != operation['operation_id'] or binding.source_sha != operation['source_sha']
                or binding.original_operation_sha256 != original_digest
                or binding.inputs_sha256 != original.operation['inputs_sha256']
                or operation['original_operation_id'] != original.operation['operation_id']
                or any(operation[key] != original.operation[key] for key in ('installation_id', 'namespace'))
                or Path(operation['inputs_path']).parent.parent.parent != Path(original.operation['inputs_path']).parent.parent.parent):
            raise ValueError
        # The protected bundle builder derives this head from its exact source's
        # Alembic graph; manifest + fixed installer digest bind these bytes. The
        # retained pool's closed-manager SQL independently requires schema0174.
        # A different head needs a separate migration workflow, not this switch.
        expected = {'schema': 'loom.nebius-manager-schema.v1', 'source_sha': binding.source_sha, 'revision': '0174'}
        if _private(SCHEMA_PROOF_PATH, 4096) != json.dumps(expected, sort_keys=True).encode():
            raise ValueError
        state, anchor = Path(original.operation['state_dir']), Path(original.operation['anchor_dir'])
        entry = manager_image_entry(original.request, binding, state=state, anchor=anchor)
        if not entry.anchored:
            qualify_image_entry_closed(entry, state=state, anchor=anchor)
        return ImageRepairContext(operation, inputs, original)
    except Exception:
        raise EntryError('pool image repair private inputs unqualified') from None


async def qualify_image_publication(context: ImageRepairContext, http: httpx.AsyncClient) -> None:
    """Use retained reader/trust; image metadata itself is never approval."""
    installation = context.original.predecessor.deployment.installation
    binding = context.inputs.binding
    catalog = GitHubCandidateCatalog(http,
        token=context.original.original.upgrade.original.material['loom-management-publications']['token'],
        publications=[binding.publication], registry_prefix=installation.registry_prefix,
        keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
    selected = await catalog.resolve(binding.publication.candidate_id)
    if selected.candidate != binding.candidate or selected.profile != binding.profile:
        raise ValueError('pool_manager_image_publication_differs')


async def _publication(context: ImageRepairContext) -> None:
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as http:
        await qualify_image_publication(context, http)


@dataclass(frozen=True, repr=False)
class _ImageChecks:
    original: PoolCutoverChecks
    context: ImageRepairContext

    def _current(self) -> None:
        if load_image_repair_inputs(self.context.operation) != self.context:
            raise EntryError('pool image repair private inputs changed')

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


def execute_image_repair(context: ImageRepairContext, action: str) -> dict[str, Any]:
    if action not in {'preflight', 'install', 'rollback'} or load_image_repair_inputs(context.operation) != context:
        raise EntryError('pool image repair private binding differs')
    try:
        asyncio.run(_publication(context))
    except Exception:
        raise PoolOperationError('publication') from None
    if load_image_repair_inputs(context.operation) != context:
        raise PoolOperationError('private_inputs')
    try:
        with connected_pool_api(context.original) as parent:
            parent.checks = _ImageChecks(parent.checks, context)
            result = run_pool_operation(parent=parent, tokens=context.original.tokens,
                action=action, image_binding=context.inputs.binding)
            if result.get('operation_id') != context.operation['original_operation_id']:
                raise ValueError
            return {**result, 'operation_id': context.operation['operation_id'],
                'original_operation_id': context.operation['original_operation_id'], 'telemetry': parent.guards.telemetry_report()}
    except PoolOperationError:
        raise
    except EntryError as error:
        raise PoolOperationError('private_inputs' if str(error).startswith('pool image repair private ') else
            _CONNECTION_ERRORS.get(str(error), 'connection')) from None
    except Exception:
        raise PoolOperationError('connection') from None
