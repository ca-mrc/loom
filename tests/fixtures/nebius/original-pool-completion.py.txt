"""Immutable terminal migration evidence, not an installed-acceptance claim.

The protected parent qualifies private inputs and supplies its fixed API. No
standalone command, live mutation, arbitrary outcome or workload is accepted.
Historical loading qualifies the phase chain only; consumers still need fresh
authority and runtime readback before an upgrade or any other live operation.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import activation_record
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest, _contract, cutover_documents
from scripts.ops.nebius_pool_legacy_reopening import (
    PoolLegacyReopeningAPI,
    _reopening_record,
    qualify_legacy_reopening,
)
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup import (
    _startup_record,
    closed_startup_documents,
    startup_workload_options,
)

from loom.nebius_platform_render import digest

_RECOVERY = ('startup-fence', 'shutdown', 'machine-retirement', 'gateway-retirement',
    'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening')
_PHASES = ('cutover', 'startup', 'activation', *_RECOVERY)


@dataclass(frozen=True, repr=False)
class PoolCutoverCompletion:
    outcome: Literal['global', 'legacy']
    workloads: dict[str, dict[str, Any]]
    history: dict[Path, str]
    sha256: str


def _exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _phase_hashes(operation: str, state: Path, anchor: Path) -> dict[str, str | None]:
    return {str(path): _hash(path) if _exists(path) else None for phase in _PHASES
        for path in (state / (phase + '.json'), anchor / (operation + '-' + phase + '.json'))}


def _terminal(request: PoolCutoverRequest, state: Path, anchor: Path) -> dict[str, Any]:
    """Derive the outcome and unique final templates from existing phase proofs."""
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    before = _phase_hashes(operation, state, anchor)
    closed, targets = closed_startup_documents(request, state_dir=state, anchor_dir=anchor)
    _, startup = _startup_record(request, state=state, anchor=anchor, closed=closed, targets=targets)
    activation = activation_record(request, state_dir=state, anchor_dir=anchor)
    if activation is None:
        raise ValueError
    if activation['opening'] == 'opened' and activation['cancellation'] == 'prepared':
        if (startup is None or any(row['phase'] != 'started' for row in startup['workloads'].values())
                or any(row != {'release': 'released', 'fence': 'prepared'} for row in activation['guards'].values())
                or any(_exists(path) for phase in _RECOVERY
                    for path in (state / (phase + '.json'), anchor / (operation + '-' + phase + '.json')))):
            raise ValueError
        outcome = 'global'
    else:
        if (activation['cancellation'] != 'fenced'
                or any(row['fence'] != 'fenced' for row in activation['guards'].values())):
            raise ValueError
        _, reopening = _reopening_record(request, state=state, anchor=anchor)
        if reopening is None or any(phase != 'released' for phase in reopening['guards'].values()):
            raise ValueError
        outcome = 'legacy'
    options = startup_workload_options(request, state_dir=state, anchor_dir=anchor)
    if options is None or set(options) != set(closed) or any(len(rows) != 1 for rows in options.values()):
        raise ValueError
    workloads = {}
    for key, rows in options.items():
        value = _stable(rows[0])
        value['metadata']['uid'] = _uid(closed[key])
        workloads[key] = value
    if _phase_hashes(operation, state, anchor) != before:
        raise ValueError
    return {'schema': 'loom.nebius-pool-completion.v1', 'operation_id': operation,
        'state_dir': str(state), 'contract_sha256': digest(_contract(request, cutover_documents(request))),
        'outcome': outcome, 'phase_sha256': before, 'workloads': workloads}


def _identity(receipt: dict[str, Any]) -> dict[str, str]:
    # Use the same byte encoding as the existing atomic journal writer, so even
    # a semantically equal rewrite cannot silently replace frozen evidence.
    checksum = hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()
    return {'schema': 'loom.nebius-pool-completion-anchor.v1', 'operation_id': receipt['operation_id'],
        'state_dir': receipt['state_dir'], 'completion_sha256': checksum}


def _paths(receipt: dict[str, Any], state: Path, anchor: Path) -> tuple[Path, Path]:
    return state / 'completion.json', anchor / (receipt['operation_id'] + '-completion.json')


def _saved(receipt: dict[str, Any], state: Path, anchor: Path) -> bool:
    path, marker = _paths(receipt, state, anchor)
    identity = _identity(receipt)
    if not _exists(marker):
        if _exists(path):
            raise ValueError
        return False
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    if not _exists(path):
        return False  # Only these same anchored bytes can complete local persistence.
    if _hash(path) != identity['completion_sha256']:
        raise ValueError
    return True


def _qualified(receipt: dict[str, Any], state: Path, anchor: Path) -> PoolCutoverCompletion:
    path, marker = _paths(receipt, state, anchor)
    if not _saved(receipt, state, anchor):
        raise ValueError
    history = {Path(name): value for name, value in receipt['phase_sha256'].items() if value is not None}
    history.update({path: _hash(path), marker: _hash(marker)})
    if receipt['outcome'] not in ('global', 'legacy'):
        raise ValueError
    return PoolCutoverCompletion(receipt['outcome'], receipt['workloads'], history, history[path])


def load_pool_completion(*, request: PoolCutoverRequest, state_dir: Path, anchor_dir: Path,
                         completion_sha256: str) -> PoolCutoverCompletion:
    """Read a frozen terminal baseline without executing or replaying a stage."""
    try:
        if _hash(state_dir / 'completion.json') != completion_sha256:
            raise ValueError
        receipt = _terminal(request, state_dir, anchor_dir)
        return _qualified(receipt, state_dir, anchor_dir)
    except Exception:
        raise ValueError('pool_completion_unqualified_preserve_evidence') from None


def complete_pool_cutover(*, request: PoolCutoverRequest, api: PoolLegacyReopeningAPI,
                          state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Freeze a settled outcome, with fresh live readback but no external writes."""
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            receipt = _terminal(request, state, anchor)
            saved = _saved(receipt, state, anchor)

            def observe() -> None:
                api.verify_retained()
                if receipt['outcome'] == 'legacy':
                    if qualify_legacy_reopening(request, api, state=state, anchor=anchor) is not None:
                        raise ValueError
                for key, expected in receipt['workloads'].items():
                    if not _matches(api.read_workload(key), expected, _uid(expected)):
                        raise ValueError
                if (api.pool_state() != ('global' if receipt['outcome'] == 'global' else 'fenced')
                        or any(api.guard_state(str(row.participant_id)) != 'open'
                            for row in request.fencing.retirement.migration.guards)
                        or _phase_hashes(receipt['operation_id'], state, anchor) != receipt['phase_sha256']):
                    raise ValueError

            observe()
            path, marker = _paths(receipt, state, anchor)
            if not saved:
                if not _exists(marker):
                    private_state._atomic_json(marker, _identity(receipt))
                # Recheck after anchoring; live drift must not produce a receipt.
                observe()
                private_state._atomic_json(path, receipt)
            observe()
            completed = _qualified(receipt, state, anchor)
            return {'status': 'pool_cutover_completed', 'operation_id': receipt['operation_id'],
                'outcome': completed.outcome, 'completion_sha256': completed.sha256, 'acceptance_verified': False}
    except Exception:
        raise ValueError('pool_completion_unqualified_preserve_evidence') from None
