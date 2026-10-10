"""Read-only, receipt-bound writer census before shared gateway activation.

The initial census cannot survive intentional workload or Role transitions.
This observer derives only the actual child journals' before/after choices and
rechecks them after the complete snapshot. It neither attests process drain nor
authorizes recovery or the later gateway/active pool successors.
"""
from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_shared_pool_handoff_live import (
    SharedPoolGuardBindings,
    SharedPoolWriterObservation,
    WriterOptions,
    _authority_roots,
    _observe_shared_writer_options,
    _resource_roots,
)
from scripts.ops.nebius_shared_pool_material import validate_shared_staging_material
from scripts.ops.nebius_shared_pool_operation import _files
from scripts.ops.nebius_shared_pool_roles import shared_forward_role_options
from scripts.ops.nebius_shared_pool_workloads import _frame, shared_forward_staging_options

from loom.nebius_platform_render import digest


def _forward_options(bindings: SharedPoolGuardBindings, state: Path
        ) -> tuple[dict[str, Any], WriterOptions, WriterOptions, dict[str, dict[str, str]]]:
    frame = _frame(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    if (frame['outcome'] != 'pending' or recovery.exists() or recovery.is_symlink()
            or any(frame['forward'][phase]['status'] != 'prepared'
                for phase in ('forward-gateway', 'forward-activate', 'forward-verify'))):
        raise ValueError
    roots: WriterOptions = {key: (row,) for key, row in
        _resource_roots(bindings.plan, frozenset({'Deployment', 'StatefulSet', 'CronJob'})).items()}
    roots.update(shared_forward_staging_options(bindings=bindings, state_dir=state))
    authority: WriterOptions = {key: (row,) for key, row in _authority_roots(bindings.plan).items()}
    authority.update(shared_forward_role_options(bindings=bindings, state_dir=state))
    fresh = bindings.plan.staging_reader_authority
    if authority.keys() & fresh.keys():
        raise ValueError
    authority.update({key: (None,) for key in fresh})
    material = state / 'forward-material'
    status = frame['forward']['forward-material']['status']
    path = material / 'material.json'
    if status != 'prepared' or path.exists() or path.is_symlink():
        child = validate_shared_staging_material(bindings=bindings, state_dir=material,
            complete=status == 'complete', prepared=status == 'prepared')
        # A stage intent may precede the independently anchored dispatch flag.
        # That effect-free preparation does not authorize a live new object.
        if child['local']['dispatch']:
            for key in fresh:
                row = child['stage']['resources'][key]
                if row['status'] == 'create_intent':
                    authority[key] = (None, copy.deepcopy(row['expected']))
                elif row['status'] == 'created':
                    expected = copy.deepcopy(row['observed'])
                    expected['metadata']['uid'] = row['uid']
                    authority[key] = (expected,)
    receipts = {phase: _files(state / phase, allow_empty=True)
        for phase in ('forward-stop', 'forward-roles', 'forward-material', 'forward-wire', 'forward-start')
        if (state / phase).exists() or (state / phase).is_symlink()}
    if _frame(bindings, state) != frame:
        raise ValueError
    return frame, roots, authority, receipts


def observe_shared_pool_forward_writers(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        transport: ManagementKubernetesTransport) -> SharedPoolWriterObservation:
    """One current cluster snapshot, qualified against unchanged child receipts.

    Uncertain PATCHes retain both exact recorded alternatives. A completed
    transition retains only its successor. New reader grants require the exact
    operation-marked create intent or recorded created UID; planned names alone
    are not authority. No receipt is written or uncertain effect acknowledged.
    """
    try:
        before = _forward_options(bindings, state_dir)
        result = _observe_shared_writer_options(plan=bindings.plan, transport=transport,
            workload_options=before[1], authority_options=before[2])
        if _forward_options(bindings, state_dir) != before:
            raise ValueError
        return replace(result, input_digest=digest({'handoff': bindings.input_digest,
            'census': result.input_digest, 'frame': before[0], 'receipts': before[3]}))
    except Exception:
        raise ValueError('shared pool forward writer census unqualified') from None
