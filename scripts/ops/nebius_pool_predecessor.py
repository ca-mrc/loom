"""Qualify a terminal cutover as a fixed management-upgrade baseline.

No installer is replayed and no live state is asserted here. Refresh entry must
separately preserve this baseline and qualify the pool's retained authority;
this reader alone does not authorize refresh writes.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
from scripts.ops.nebius_management_refresh_predecessor import CompletedUpgrade, _predecessor_scope
from scripts.ops.nebius_pool_application_delivery import derive_application_build_deployment
from scripts.ops.nebius_pool_completion import PoolCutoverCompletion, load_pool_completion
from scripts.ops.nebius_pool_cutover_entry import PoolCutoverContext, load_pool_cutover_inputs
from scripts.ops.nebius_pool_migration import _hash

from loom_service.environment_management.deployment import ManagementDeployment


class PoolPredecessorV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    kind: Literal['pool-cutover'] = 'pool-cutover'
    operation: dict[str, Any]
    completion_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


@dataclass(frozen=True, repr=False)
class CompletedPoolCutover:
    selector: PoolPredecessorV1
    context: PoolCutoverContext
    completion: PoolCutoverCompletion
    deployment: ManagementDeployment
    active: dict[str, Any]
    history: dict[Path, str]


def load_completed_pool(selector: PoolPredecessorV1, *, original: CompletedUpgrade) -> CompletedPoolCutover:
    """Load one terminal baseline, refusing cyclic or unbounded mixed ancestry."""
    try:
        with _predecessor_scope('pool-cutover', selector.operation['operation_id']):
            return _load_completed_pool(selector, original=original)
    except Exception:
        raise ValueError('pool_predecessor_unqualified') from None


def _load_completed_pool(selector: PoolPredecessorV1, *, original: CompletedUpgrade) -> CompletedPoolCutover:
    """Derive manager configuration from the qualified predecessor and outcome.

    A caller supplies neither a post-cutover Deployment nor a catalog identity.
    The historical input and completion hashes bind those to the same private
    operation, preserving original credentials, installation and database roots.
    """
    try:
        selector = PoolPredecessorV1.model_validate(selector.model_dump())
        context = load_pool_cutover_inputs(selector.operation)
        if context.original != original or context.predecessor.deployment.pool_catalog_operation_id is not None:
            raise ValueError
        completion = load_pool_completion(request=context.request, state_dir=Path(context.operation['state_dir']),
            anchor_dir=Path(context.operation['anchor_dir']), completion_sha256=selector.completion_sha256)
        deployment = context.predecessor.deployment.model_dump(mode='json')
        if completion.outcome == 'global':
            deployment['pool_catalog_operation_id'] = str(context.inputs.installation.operation_id)
        derived = ManagementDeployment.model_validate(deployment)
        if completion.outcome == 'global' and context.request.application_delivery is not None:
            derived = derive_application_build_deployment(context.predecessor.deployment, context.inputs.installation)
        active = copy.deepcopy(completion.workloads[_key(context.request.manager)])
        if _uid(active) != _uid(context.predecessor.active) or _uid(active) != _uid(original.active):
            raise ValueError
        # Keep the same strict renderer that later refreshes use. This proves the
        # derived binding describes the retained manager without an arbitrary
        # credential, command, mount or authority change.
        render_refresh(ManagementRefreshRenderRequest(derived, derived, active,
            context.inputs.candidate, context.inputs.profile, original.upgrade.setup.repo_root))
        history: dict[Path, str] = {}
        for group in (original.history, context.predecessor.history, completion.history,
                {Path(context.operation['inputs_path']): context.operation['inputs_sha256']}):
            for path, checksum in group.items():
                if path in history and history[path] != checksum:
                    raise ValueError
                history[path] = checksum
        if not 1 <= len(history) <= 128 or any(_hash(path) != checksum for path, checksum in history.items()):
            raise ValueError
        return CompletedPoolCutover(selector, context, completion, derived, active, history)
    except Exception:
        raise ValueError('pool_predecessor_unqualified') from None
