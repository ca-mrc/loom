"""Compose fixed cutover, opening and recovery stages on one protected parent.

Child journals remain the sole phase authority. No public stage selector,
automatic rollback, fresh write retry or acceptance claim is introduced here.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_management_gateway import POOL_SHUTDOWN_PENDING_REASONS
from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
from scripts.ops.nebius_pool_activation_stage import activation_record, advance_pool_activation
from scripts.ops.nebius_pool_completion import complete_pool_cutover, load_pool_completion
from scripts.ops.nebius_pool_cutover import stage_pool_cutover
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_gateway_retirement import retire_gateway_roles
from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy
from scripts.ops.nebius_pool_legacy_restart import restart_pool_legacy
from scripts.ops.nebius_pool_machine_retirement import retire_pool_machines
from scripts.ops.nebius_pool_manager_image_history import (
    ManagerImageRepairBinding,
    _completed,
    load_manager_image_chain,
    manager_image_entry,
)
from scripts.ops.nebius_pool_manager_image_live import HTTPSPoolManagerImageAPI
from scripts.ops.nebius_pool_manager_image_stage import repair_manager_image
from scripts.ops.nebius_pool_migration import PoolMigrationError, _hash
from scripts.ops.nebius_pool_role_restoration import restore_pool_roles
from scripts.ops.nebius_pool_shutdown import stop_pool_successors
from scripts.ops.nebius_pool_startup import stage_pool_startup, startup_workload_options
from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_startup_repair import (
    PoolStartupRepairBinding,
    _repair_record,
    qualify_completed_startup_repair,
    repair_pool_startup,
    startup_repair_exists,
)
from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI
from scripts.ops.nebius_pool_template_restoration import _template_record, restore_pool_templates

_RECOVERY = ('startup-fence', 'shutdown', 'machine-retirement', 'gateway-retirement',
    'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening')

# Exact locally authored messages only. Unknown text and adapter stages never
# cross the protected report boundary, including a known code with extra data.
_PREFLIGHT_ERRORS = {
    'pool_retained_writer_binding_inventory_unqualified': 'writer_bindings',
    'pool_retained_writer_workload_inventory_unqualified': 'writer_workloads',
    'pool cutover connected prerequisites unqualified': 'connected_prerequisites',
    'pool cutover initial capacity unqualified': 'capacity',
    'pool cutover inputs changed': 'scope',
    'pool cutover namespace differs': 'scope',
    'pool_cutover_database_report_unqualified': 'database_report',
    'pool_cutover_pending_source_unqualified': 'pending_source',
    'pool_cutover_pending_page_unqualified': 'pending_page',
    'pool_management_history_origin_unqualified': 'origin_history',
}
_PREFLIGHT_MIGRATION_ERRORS = {
    'cutover_readiness': 'database_readiness',
    'management_origin_history': 'origin_history',
}


class PoolOperationError(RuntimeError):
    def __init__(self, stage: str):
        self.stage = 'pool_' + stage.replace('-', '_')
        super().__init__('pool operation unconfirmed; preserve private evidence')


def run_pool_operation(*, parent: HTTPSPoolCutoverAPI, tokens: dict[UUID, str], action: str,
                       repair_binding: PoolStartupRepairBinding | None = None,
                       image_binding: ManagerImageRepairBinding | None = None) -> dict[str, Any]:
    """Run a complete fixed direction or resume its newest journaled phase.

The caller must use the private-input-qualified parent and keep its transports
alive. Preflight is read-only retained-scope qualification, not runtime readiness.
The dispatch lock serializes forward/recovery passes; each child still holds its
own original journal lock and validates its predecessor before any side effect.
"""
    phase = 'operation'
    try:
        if (action not in {'preflight', 'install', 'rollback'} or parent.refresh is not None
                or (repair_binding is not None and image_binding is not None)):
            raise ValueError
        state, anchor = parent.state_dir, parent.anchor_dir
        if (state is None or anchor is None or not state.is_absolute() or not anchor.is_absolute()
                or state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        request = parent.request
        operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
        if repair_binding is not None:
            _repair_record(request, state=state, anchor=anchor, binding=repair_binding)
        if image_binding is not None:
            manager_image_entry(request, image_binding, state=state, anchor=anchor)

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
            report = {'status': 'pending', 'phase': name, 'operation_id': operation}
            if name == 'shutdown' and status in POOL_SHUTDOWN_PENDING_REASONS:
                report['pending_reason'] = status
            return report

        if action == 'preflight':
            phase = 'preflight'
            recovering = any(present(name) for name in _RECOVERY)
            if present('activation'):
                activation = activation_record(request, state_dir=state, anchor_dir=anchor)
                recovering = recovering or (activation is not None and activation['cancellation'] != 'prepared')
            # Forward image qualification deliberately rejects cancellation and
            # recovery journals. Recovery instead validates the full retained
            # chain and current fenced workloads below, even for an incomplete tail.
            if image_binding is not None and not recovering:
                image_entry = manager_image_entry(request, image_binding, state=state, anchor=anchor)
                if not _completed(image_entry):
                    HTTPSPoolManagerImageAPI(parent=parent, binding=image_binding).qualify_closed()
            if repair_binding is not None and not recovering and not startup_repair_exists(request, state=state, anchor=anchor):
                HTTPSPoolStartupRepairAPI(parent=parent, binding=repair_binding).qualify_closed()
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
            repaired = startup_repair_exists(request, state=state, anchor=anchor)
            images = load_manager_image_chain(request, state=state, anchor=anchor)
            if action == 'install' and images and image_binding is None:
                raise ValueError
            if action == 'install' and repaired and repair_binding is None and image_binding is None:
                raise ValueError  # Only the separately bound continuation may open.
            if repair_binding is not None:
                _repair_record(request, state=state, anchor=anchor, binding=repair_binding)
            if image_binding is not None:
                manager_image_entry(request, image_binding, state=state, anchor=anchor)
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
                if image_binding is not None and not _completed(manager_image_entry(request, image_binding, state=state, anchor=anchor)):
                    pending = advance('manager-image', 'pool_manager_image_repaired_closed', lambda: repair_manager_image(
                        request=request, binding=image_binding, api=HTTPSPoolManagerImageAPI(parent=parent, binding=image_binding),
                        state_dir=state, anchor_dir=anchor))
                    if pending is not None:
                        return pending
                if repair_binding is not None:
                    ready = False
                    if repaired:
                        try:
                            qualify_completed_startup_repair(request, state=state, anchor=anchor)
                            ready = True
                        except ValueError:
                            pass
                    if not ready:
                        pending = advance('startup-repair', 'pool_startup_repaired_closed', lambda: repair_pool_startup(
                            request=request, binding=repair_binding,
                            api=HTTPSPoolStartupRepairAPI(parent=parent, binding=repair_binding), state_dir=state, anchor_dir=anchor))
                        if pending is not None:
                            return pending
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
                if steps[start][0] == 'template-restoration':
                    phase = 'template-restoration'
                    templates = _template_record(request, state=state, anchor=anchor)[-1]
                    if templates is not None and all(row['phase'] == 'restored' for row in templates['workloads'].values()):
                        # Role preparation freshly proves the same completed
                        # ancestry and live drain before creating its journal.
                        start += 1
                for name, success, call in steps[start:]:
                    pending = advance(name, success, call)
                    if pending is not None:
                        return pending
            phase = 'completion'
            return complete_pool_cutover(request=request, api=HTTPSPoolActivationAPI(parent=parent),
                state_dir=state, anchor_dir=anchor)
    except Exception as error:
        if phase == 'preflight':
            detail = (_PREFLIGHT_MIGRATION_ERRORS.get(error.stage) if isinstance(error, PoolMigrationError)
                else _PREFLIGHT_ERRORS.get(str(error)))
            if detail is not None:
                phase += '_' + detail
        raise PoolOperationError(phase) from None
