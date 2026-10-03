"""Manager-only refresh projection rooted in completed pool migration evidence.

This is a read-only identity projection, not a live verifier or an installation
entrypoint. The connected pool reader must still qualify actual workloads,
backends, credentials, permissions and physical provider scope before writes.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_refresh import render_refresh
from scripts.ops.nebius_management_refresh_install import (
    ManagementRefreshInstallRequest,
    refresh_manager_options,
)
from scripts.ops.nebius_management_refresh_predecessor import (
    CompletedRefresh,
    CompletedUpgrade,
    load_completed_refresh,
    load_completed_upgrade,
)
from scripts.ops.nebius_management_refresh_supersession import (
    FailedRefreshProof,
    load_failed_refresh,
)
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool


@dataclass(frozen=True, repr=False)
class PoolManagerRefresh:
    original: CompletedUpgrade
    predecessor: CompletedPoolCutover | CompletedRefresh
    request: ManagementRefreshInstallRequest
    state_dir: Path
    superseded: FailedRefreshProof | None = None

    def qualify(self) -> CompletedPoolCutover:
        """Re-read root, immediate predecessor and exact inherited pool baseline."""
        try:
            if load_completed_upgrade(self.original.selector) != self.original:
                raise ValueError
            predecessor = self.predecessor
            if isinstance(predecessor, CompletedPoolCutover):
                if load_completed_pool(predecessor.selector, original=self.original) != predecessor:
                    raise ValueError
                pool = predecessor
            elif isinstance(predecessor, CompletedRefresh):
                if (load_completed_refresh(predecessor.selector, original=self.original) != predecessor
                        or predecessor.pool_baseline is None):
                    raise ValueError
                pool = predecessor.pool_baseline
            else:
                raise ValueError
            request, setup = self.request, self.original.upgrade.setup
            render = request.resources.switch.render
            root = Path(self.original.selector.operation['inputs_path']).parent.parent
            expected_state = root / 'refresh' / str(request.resources.switch.operation_id) / 'state'
            if (request.pool_baseline != pool.selector.model_dump(mode='json')
                    or request.installation_anchor != self.original.upgrade.original_anchor
                    or request.resources.binding != setup.binding
                    or request.resources.shared_namespace_uid != setup.shared_namespace_uid
                    or render.before != predecessor.deployment or render.active != predecessor.active
                    or self.state_dir != expected_state or self.state_dir != self.state_dir.resolve()
                    or any(request.history.get(path) != checksum for path, checksum in predecessor.history.items())
                    or _uid(render.active) != _uid(pool.active)):
                raise ValueError
            render_refresh(render)
            failed = self.superseded
            if failed is None:
                if request.resources.switch.initial_stopped is not None:
                    raise ValueError
            elif (load_failed_refresh(failed.request, failed.selector) != failed
                    or request.resources.switch.initial_stopped != failed.stopped
                    or failed.request.resources.switch.operation_id == request.resources.switch.operation_id
                    or failed.request.installation_anchor != request.installation_anchor
                    or failed.request.resources.binding != request.resources.binding
                    or failed.request.resources.shared_namespace_uid != request.resources.shared_namespace_uid
                    or failed.request.resources.manager_revision != request.resources.manager_revision
                    or failed.request.resources.switch.render.active != render.active
                    or failed.request.resources.switch.render.before != render.before
                    or failed.request.pool_baseline != request.pool_baseline
                    or any(request.history.get(path) != checksum for path, checksum in failed.history.items())):
                raise ValueError
            return pool
        except Exception:
            raise ValueError('pool_refresh_projection_unqualified') from None

    def workload_options(self) -> dict[str, tuple[dict[str, Any], ...]]:
        """Permit only exact manager journal states; preserve all other workloads."""
        try:
            pool = self.qualify()
            options: dict[str, tuple[dict[str, Any], ...]] = {
                key: (copy.deepcopy(row),) for key, row in pool.completion.workloads.items()}
            options[_key(pool.context.request.manager)] = refresh_manager_options(
                self.request, state=self.state_dir, anchor=self.state_dir.parent / 'anchor')
            return options
        except Exception:
            raise ValueError('pool_refresh_projection_unqualified') from None
