"""Compose fixed cutover, opening and recovery stages on one protected parent.

Child journals remain the sole phase authority. No public stage selector,
automatic rollback, fresh write retry or acceptance claim is introduced here.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
from scripts.ops.nebius_pool_activation_stage import activation_record, advance_pool_activation
from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
from scripts.ops.nebius_pool_cutover import stage_pool_cutover
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_gateway_retirement import retire_gateway_roles
from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy
from scripts.ops.nebius_pool_legacy_restart import restart_pool_legacy
from scripts.ops.nebius_pool_machine_retirement import retire_pool_machines
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_role_restoration import restore_pool_roles
from scripts.ops.nebius_pool_shutdown import stop_pool_successors
from scripts.ops.nebius_pool_startup import stage_pool_startup, startup_workload_options
from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_template_restoration import restore_pool_templates

_RECOVERY = ('startup-fence', 'shutdown', 'machine-retirement', 'gateway-retirement',
    'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening')


class PoolOperationError(RuntimeError):
    def __init__(self, stage: str):
        self.stage = 'pool_' + stage.replace('-', '_')
        super().__init__('pool operation unconfirmed; preserve private evidence')


def run_pool_operation(*, parent: HTTPSPoolCutoverAPI, tokens: dict[UUID, str], action: str) -> dict[str, Any]:
    """Run a complete fixed direction or resume its newest journaled phase.

The caller must use the private-input-qualified parent and keep its transports
alive. Preflight is read-only retained-scope qualification, not runtime readiness.
The dispatch lock serializes forward/recovery passes; each child still holds its
own original journal lock and validates its predecessor before any side effect.
"""
    phase = 'operation'
    try:
        if action not in {'preflight', 'install', 'rollback'} or parent.refresh is not None:
            raise ValueError
        state, anchor = parent.state_dir, parent.anchor_dir
        if (state is None or anchor is None or not state.is_absolute() or not anchor.is_absolute()
                or state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        request = parent.request
        operation = str(request.fencing.retirement.migration.registration.spec.operation_id)

        def present(name: str) -> bool:
            # An orphaned marker is evidence, not permission to restart a stage.
            return any(path.exists() or path.is_symlink() for path in (
                state / (name + '.json'), anchor / (operation + '-' + name + '.json')))

        def advance(name: str, success: str, call: Callable[[], dict[str, Any]]) -> dict[str, Any] | None:
            nonlocal phase
            phase = name
            result = call()
            if result.get('operation_id') != operation:
                raise ValueError
            status = result.get('status')
            if status == success:
                return None
            if not isinstance(status, str) or not status.startswith('pending_'):
                raise ValueError
            return {'status': 'pending', 'phase': name, 'operation_id': operation}

        if action == 'preflight':
            phase = 'preflight'
            if any(present(name) for name in ('startup', 'activation', 'completion', *_RECOVERY)):
                # Validate the whole selected chain, without creating a missing
                # child or using initial idle checks against reopened owners.
                if startup_workload_options(request, state_dir=state, anchor_dir=anchor) is None:
                    raise ValueError
                if present('activation') or any(present(name) for name in ('completion', *_RECOVERY)):
                    HTTPSPoolActivationAPI(parent=parent).verify_retained()
                    activation_record(request, state_dir=state, anchor_dir=anchor)
                else:
                    HTTPSPoolStartupAPI(parent=parent).qualify_closed()
                if present('completion'):
                    load_pool_completion(request=request, state_dir=state, anchor_dir=anchor,
                        completion_sha256=_hash(state / 'completion.json'))
            else:
                parent.preflight(request)
            return {'status': 'preflight_qualified', 'operation_id': operation}

        private_state._private_directory(anchor)
        with private_state._locked_state(anchor / 'dispatch'):
            if present('completion'):
                phase = 'completion'
                record = activation_record(request, state_dir=state, anchor_dir=anchor)
                if action == 'rollback' and (record is None or record['cancellation'] != 'fenced'):
                    raise ValueError  # Never invalidate frozen global ancestry.
                return complete_pool_cutover(request=request, api=HTTPSPoolActivationAPI(parent=parent),
                    state_dir=state, anchor_dir=anchor)

            if action == 'install':
                if any(present(name) for name in _RECOVERY):
                    raise ValueError
                if present('activation'):
                    record = activation_record(request, state_dir=state, anchor_dir=anchor)
                    if record is None or record['cancellation'] != 'prepared':
                        raise ValueError
                else:
                    if not present('startup'):
                        pending = advance('cutover', 'pool_runtime_staged_closed', lambda: stage_pool_cutover(
                            request=request, tokens=tokens, api=parent, state_dir=state, anchor_dir=anchor,
                            source_credentials=parent._source_credentials if request.application_delivery is not None else None))
                        if pending is not None:
                            return pending
                    pending = advance('startup', 'pool_startup_staged_closed', lambda: stage_pool_startup(
                        request=request, api=HTTPSPoolStartupAPI(parent=parent), state_dir=state, anchor_dir=anchor))
                    if pending is not None:
                        return pending
                pending = advance('activation', 'pool_activation_complete', lambda: advance_pool_activation(
                    request=request, api=HTTPSPoolActivationAPI(parent=parent), state_dir=state, anchor_dir=anchor))
                if pending is not None:
                    return pending
            else:
                phase = 'cancellation'
                api = HTTPSPoolActivationAPI(parent=parent)
                steps: tuple[tuple[str, str, Callable[[], dict[str, Any]]], ...] = (
                    ('activation', 'pool_activation_cancelled', lambda: advance_pool_activation(
                        request=request, api=api, state_dir=state, anchor_dir=anchor, cancel=True)),
                    ('startup-fence', 'startup_writes_fenced', lambda: fence_pool_startup(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('shutdown', 'pool_successors_stopped', lambda: stop_pool_successors(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('machine-retirement', 'pool_machines_retired', lambda: retire_pool_machines(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('gateway-retirement', 'pool_gateway_roles_retired', lambda: retire_gateway_roles(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('template-restoration', 'pool_legacy_templates_restored_closed', lambda: restore_pool_templates(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('role-restoration', 'pool_legacy_roles_restored_closed', lambda: restore_pool_roles(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('legacy-restart', 'pool_legacy_restart_staged_closed', lambda: restart_pool_legacy(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                    ('legacy-reopening', 'pool_legacy_reopened', lambda: reopen_pool_legacy(
                        request=request, api=api, state_dir=state, anchor_dir=anchor)),
                )
                start = max((index for index, (name, _, _) in enumerate(steps) if present(name)), default=0)
                for name, success, call in steps[start:]:
                    pending = advance(name, success, call)
                    if pending is not None:
                        return pending
            phase = 'completion'
            return complete_pool_cutover(request=request, api=HTTPSPoolActivationAPI(parent=parent),
                state_dir=state, anchor_dir=anchor)
    except Exception:
        raise PoolOperationError(phase) from None
