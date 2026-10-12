"""Whole-cluster writer census qualified by the shared recovery's own receipts.

Frozen forward projections supply only initial alternatives. Recovery intents
and completed effects replace those choices. The common revision-bound RBAC/
process inventory algorithm remains authoritative; no mutation or runtime/SQL
readiness claim is made by this read-only observation.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.ops import nebius_shared_pool_recovery_restart as restart
from scripts.ops import nebius_shared_pool_recovery_roles as roles
from scripts.ops import nebius_shared_pool_recovery_templates as templates
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_shared_pool_forward_census import _frozen_options
from scripts.ops.nebius_shared_pool_handoff_live import (
    SharedPoolGuardBindings,
    SharedPoolWriterObservation,
    WriterOptions,
    _observe_shared_writer_options,
)
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_drain import validate_shared_recovery_drain
from scripts.ops.nebius_shared_pool_recovery_fence import (
    _recovery_frame,
    validate_shared_recovery_fence,
)
from scripts.ops.nebius_shared_pool_recovery_gateway import (
    _readonly,
    validate_shared_recovery_gateway,
)
from scripts.ops.nebius_shared_pool_recovery_machines import validate_shared_recovery_machines
from scripts.ops.nebius_shared_pool_recovery_reopening import (
    validate_shared_recovery_reopening,
    validate_shared_recovery_verification,
)
from scripts.ops.nebius_shared_pool_recovery_workloads import validate_shared_recovery_workloads

from loom.nebius_platform_render import digest


def _pinned(document: dict[str, Any], uid: str) -> dict[str, Any]:
    result = copy.deepcopy(document)
    if result['metadata'].get('uid', uid) != uid:
        raise ValueError('shared recovery census target UID differs')
    result['metadata']['uid'] = uid
    return result


def _receipt_files(state: Path) -> dict[str, dict[str, str]]:
    return {phase: _files(state / phase, allow_empty=True)
        for phase in (*FORWARD_PHASES, *RECOVERY_PHASES)
        if (state / phase).exists() or (state / phase).is_symlink()}


def _recovery_options(bindings: SharedPoolGuardBindings, state: Path
        ) -> tuple[dict[str, Any], WriterOptions, WriterOptions, dict[str, dict[str, str]]]:
    frame = _recovery_frame(bindings, state)
    before, roots, authority, receipts = _frozen_options(bindings, state)
    if (before != frame or (frame['outcome'] == 'staging_restored_dev_closed'
            and any(row['status'] != 'complete' for row in frame['recovery'].values()))):
        raise ValueError
    for phase in RECOVERY_PHASES:
        if (state / phase).exists() or (state / phase).is_symlink():
            receipts[phase] = _files(state / phase, allow_empty=frame['recovery'][phase]['status'] == 'prepared')

    def local_child(phase: str, filename: str) -> tuple[Path, bool, bool] | None:
        status = frame['recovery'][phase]['status']
        path = state / phase / filename
        if status == 'prepared' and not path.exists() and not path.is_symlink():
            return None  # Interrupted local preparation has no effect authority.
        return path, status == 'complete', status == 'prepared'

    validators: tuple[tuple[str, str, Callable[..., dict[str, Any]]], ...] = (
        ('recovery-fence', 'fence.json', validate_shared_recovery_fence),
        ('recovery-drain', 'drain.json', validate_shared_recovery_drain),
        ('recovery-stop', 'stop.json', validate_shared_recovery_workloads),
        ('recovery-revoke', 'machines.json', validate_shared_recovery_machines),
        ('recovery-revoke', 'gateway.json', validate_shared_recovery_gateway),
        ('recovery-restore-workloads', 'templates.json', templates.validate_shared_recovery_templates),
        ('recovery-restore-roles', 'roles.json', roles.validate_shared_recovery_roles),
        ('recovery-restart', 'restart.json', restart.validate_shared_recovery_restart),
        ('recovery-reopen', 'reopen.json', validate_shared_recovery_reopening),
        ('recovery-verify', 'verify.json', validate_shared_recovery_verification),
    )
    # Every validator checks its completed predecessors recursively. Validate
    # the latest present phase's whole chain once, not the same chain separately
    # for every ancestor. Revocation has two independent children; both matter.
    for phase in reversed(RECOVERY_PHASES):
        present = [(local_child(phase, filename), validate)
            for selected, filename, validate in validators if selected == phase]
        if any(local is not None for local, _ in present):
            for local, validate in present:
                if local is not None:
                    path, complete, prepared = local
                    validate(bindings=bindings, state_dir=path.parent, complete=complete, prepared=prepared)
            break

    def child(phase: str, filename: str) -> dict[str, Any] | None:
        local = local_child(phase, filename)
        if local is None:
            return None
        return _read(local[0])

    stopped = child('recovery-stop', 'stop.json')
    if stopped is not None:
        for key, row in stopped['resources'].items():
            if key not in roots:
                raise ValueError
            if row['status'] != 'prepared':
                after = _pinned(row['expected'], _uid(row['before']))
                roots[key] = (copy.deepcopy(row['before']), after) if row['status'] == 'intent' else (after,)

    gateway = child('recovery-revoke', 'gateway.json')
    if gateway is not None:
        for key, row in gateway['roles'].items():
            choices = authority[key]
            if row['status'] in {'create_intent', 'original', 'patch_intent', 'restricted'}:
                original = choices[-1]
                if original is None:
                    raise ValueError
                readonly = _readonly(bindings, original)
                if row['status'] == 'restricted':
                    choices = (readonly,)
                elif row['status'] != 'original':
                    choices = (*choices, readonly)
                if row['uid'] is not None:
                    choices = tuple(_pinned(value, row['uid']) for value in choices if value is not None)
                authority[key] = choices
            elif row['status'] == 'absent':
                authority[key] = (None,)

    # The complete semantic dependency chain was validated above. Reuse the
    # same pure target builders as the children without validating it again.
    # All receipt bytes still have to match before returning these projections.
    restored = child('recovery-restore-workloads', 'templates.json')
    if restored is not None:
        if stopped is None:
            raise ValueError
        originals, targets = templates._project_inputs(bindings, stopped)
        for key, row in restored['resources'].items():
            after = _pinned(targets[key], _uid(originals[key]))
            roots[key] = ((originals[key],) if row['status'] == 'prepared' else (after,)
                if row['status'] == 'restored' else (originals[key], after))

    restored_roles = child('recovery-restore-roles', 'roles.json')
    if restored_roles is not None:
        targets = roles._targets(bindings)
        for key, row in restored_roles['roles'].items():
            sources = authority[key]
            after = _pinned(targets[key], _uid(bindings.plan.staging_role_originals[key]))
            authority[key] = (sources if row['status'] == 'prepared' else (after,)
                if row['status'] == 'restored' else (*sources, after))

    started = child('recovery-restart', 'restart.json')
    if started is not None:
        if stopped is None:
            raise ValueError
        originals, targets = restart._project_inputs(bindings, stopped)
        for key, row in started['resources'].items():
            after = _pinned(targets[key], _uid(originals[key]))
            roots[key] = ((originals[key],) if row['status'] == 'prepared' else (after,)
                if row['status'] == 'started' else (originals[key], after))

    # Intake state is not a Kubernetes writer grant. Its receipts still belong
    # to this census's exact authority and cannot change across the observation.
    if (_recovery_frame(bindings, state) != frame
            or any(_files(state / phase, allow_empty=True) != files for phase, files in receipts.items())):
        raise ValueError
    return frame, roots, authority, receipts


def observe_shared_pool_recovery_writers(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        transport: ManagementKubernetesTransport) -> SharedPoolWriterObservation:
    """One complete revision-bound snapshot, bracketed by exact recovery history."""
    try:
        retained = copy.deepcopy(bindings)
        files = _receipt_files(state_dir)
        before = _recovery_options(retained, state_dir)
        if before[3] != files:
            raise ValueError
        result = _observe_shared_writer_options(plan=retained.plan, transport=transport,
            workload_options=before[1], authority_options=before[2])
        # Semantic projections have already been validated. Reopen every input
        # and receipt, including started children and newly created directories,
        # rather than recursively deriving the same predecessor chain again.
        # No live cluster observation or cross-call result is cached.
        if (bindings != retained or _recovery_frame(retained, state_dir) != before[0]
                or _receipt_files(state_dir) != files):
            raise ValueError
        return replace(result, input_digest=digest({'handoff': bindings.input_digest,
            'census': result.input_digest, 'frame': before[0], 'receipts': before[3]}))
    except Exception:
        raise ValueError('shared pool recovery writer census unqualified') from None
