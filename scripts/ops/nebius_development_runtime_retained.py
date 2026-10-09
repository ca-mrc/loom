"""Read completed fresh-runtime evidence without replaying installation.

This historical projection is not live readiness or permission to open a pool.
The protected activation parent must independently qualify current identities,
sole-writer authority, installed settings and fresh capacity before any write.
"""
from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import (
    _PHASES,
    DevelopmentRuntimeInstallRequest,
    DevelopmentRuntimePlan,
    _child_path,
    _read,
    _runtime_record,
    prepare_runtime_install,
    runtime_workload_options,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _comparison_snapshot

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class CompletedDevelopmentRuntime:
    request: DevelopmentRuntimeInstallRequest
    plan: DevelopmentRuntimePlan
    state_dir: Path
    anchor_dir: Path
    resources: dict[str, dict[str, Any]]
    files: dict[Path, bytes]
    history_sha256: str


def load_completed_runtime(request: DevelopmentRuntimeInstallRequest) -> CompletedDevelopmentRuntime:
    """Require every anchored child and return pool/runtime successor identities.

    The request comes from qualified source/material intake. Retained foundation
    and manager evidence are included in the byte pins, but neither an installer
    nor its retired operator connection is used to consume their history.
    """
    try:
        request = copy.deepcopy(request)
        database = request.database
        pool, foundation = database.manager.retained, database.foundation
        manager = pool.request.retained
        state = Path(manager.operation['state_dir']).parent / 'runtime-installation'
        anchor = Path(manager.operation['anchor_dir'])
        files = dict(pool.files)
        for path, raw in foundation.files.items():
            if path in files and files[path] != raw:
                raise ValueError
            files[path] = raw
        for path in (anchor / 'runtime-installation.json', state / 'installation.json',
                *(_child_path(state, phase) for phase in _PHASES)):
            if not path.is_absolute() or path != path.resolve() or path in files:
                raise ValueError
            files[path] = private_state._private_read(path, limit=4 * 1024**2)

        def unchanged() -> None:
            if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files.items()):
                raise ValueError

        unchanged()
        plan = prepare_runtime_install(request)
        record = _runtime_record(request, plan, state, anchor)
        if any(item['status'] != 'complete' for item in record['phases'].values()):
            raise ValueError
        resources = copy.deepcopy(pool.resources)
        for phase in plan.fixed:
            child = _read(_child_path(state, phase))
            for key, item in child['resources'].items():
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                _uid(actual)
                if (key in resources or _key(actual) != key or _snapshot(actual) != item['observed']
                        or _comparison_snapshot(actual) != item['expected']):
                    raise ValueError
                resources[key] = actual
        # Completed transitions have exactly one successor. Its UID is derived
        # from the original resource, not accepted from an arbitrary projection.
        for key, options in runtime_workload_options(request=request, state_dir=state, _plan=plan).items():
            actual, = options
            if _key(actual) != key:
                raise ValueError
            _uid(actual)
            resources[key] = copy.deepcopy(actual)
        if len({_uid(row) for row in resources.values()}) != len(resources):
            raise ValueError
        unchanged()
        checksum = digest({str(path): hashlib.sha256(raw).hexdigest()
            for path, raw in files.items()})
        return CompletedDevelopmentRuntime(request, plan, state, anchor, resources, files, checksum)
    except Exception:
        raise ValueError('completed development runtime unqualified') from None
