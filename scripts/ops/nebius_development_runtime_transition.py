"""Internal fixed runtime transitions; the connected parent owns phase authority.

No entrypoint accepts workload documents. The parent derives each original and
target from retained history and its fixed plan, and anchors the child journal
before calling this engine. An unknown PATCH outcome is only read back.
"""
from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import _qualified_defaulted
from scripts.ops.nebius_management_switch import _matches

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


class DevelopmentRuntimeWorkloadAPI(Protocol):
    def qualify(self) -> None: ...
    def read_workload(self, key: str) -> dict[str, Any]: ...
    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool: ...
    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool: ...


def validate_runtime_transition(record: dict[str, Any], *, originals: dict[str, dict[str, Any]],
                                targets: dict[str, dict[str, Any]], input_digest: str, phase: str) -> None:
    identity = {'schema': 'loom.nebius-development-runtime-transition.v1', 'input_digest': input_digest,
        'phase': phase, 'originals_digest': digest(originals), 'targets_digest': digest(targets)}
    if (set(record) != {*identity, 'resources'} or any(record[key] != value for key, value in identity.items())
            or set(record['resources']) != set(targets)):
        raise ValueError
    for key, desired in targets.items():
        item = record['resources'][key]
        if (set(item) != {'status', 'expected'} or item['status'] not in {'prepared', 'intent', 'applied'}
                or (item['expected'] is None and item['status'] != 'prepared')
                or (item['expected'] is not None and _qualified_defaulted(desired, item['expected']) != item['expected'])):
            raise ValueError


def advance_runtime_workloads(*, originals: dict[str, dict[str, Any]], targets: dict[str, dict[str, Any]],
                               input_digest: str, phase: str, api: DevelopmentRuntimeWorkloadAPI,
                               state_dir: Path) -> bool:
    """Retain exact UID, preview and CAS; return False only for a known wait.

The transport must compare UID AND resourceVersion atomically, return False only
for an explicit server rejection and prove old Pods drained / target Pods ready.
Completed child phases are read as history by the parent, never replayed after a
subsequent phase intentionally changed their live workload specifications.
"""
    try:
        originals, targets = copy.deepcopy(originals), copy.deepcopy(targets)
        if (phase not in {'stop', 'replace', 'control', 'start'}
                or re.fullmatch(r'sha256:[0-9a-f]{64}', input_digest) is None
                or not originals or originals.keys() != targets.keys()):
            raise ValueError
        for key, desired in targets.items():
            original = originals[key]
            if key != _key(original) or key != _key(desired) or desired['kind'] not in {'Deployment', 'CronJob'}:
                raise ValueError
            _uid(original)
        identity = {'schema': 'loom.nebius-development-runtime-transition.v1', 'input_digest': input_digest,
            'phase': phase, 'originals_digest': digest(originals), 'targets_digest': digest(targets)}
        with private_state._locked_state(state_dir):
            path = state_dir / 'transition.json'
            api.qualify()
            if path.exists() or path.is_symlink():
                record = _json(private_state._private_read(path, limit=4 * 1024**2))
            else:
                for key, original in originals.items():
                    api.qualify()
                    actual = api.read_workload(key)
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                record = {**identity, 'resources': {key: {'status': 'prepared', 'expected': None} for key in originals}}
                private_state._atomic_json(path, record)
            validate_runtime_transition(record, originals=originals, targets=targets,
                input_digest=input_digest, phase=phase)
            # Reject a late changed resource before mutating even the first one.
            for key, original in originals.items():
                api.qualify()
                item = record['resources'][key]
                expected = original if item['status'] == 'prepared' else item['expected']
                if not _matches(api.read_workload(key), expected, _uid(original)):
                    raise ValueError
            # A definitively rejected dry-run remains a resumable known wait.
            # Freeze every successful preview before the first actual PATCH.
            for key, original in originals.items():
                item = record['resources'][key]
                if item['expected'] is None:
                    api.qualify()
                    actual = api.read_workload(key)
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                    preview = api.preview_workload(key, actual, targets[key])
                    if preview is None:
                        return False
                    item['expected'] = _qualified_defaulted(targets[key], preview)
                    private_state._atomic_json(path, record)
            for key, original in originals.items():
                api.qualify()
                item = record['resources'][key]
                if item['status'] == 'prepared':
                    actual = api.read_workload(key)
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                    preview = api.preview_workload(key, actual, targets[key])
                    if preview is None:
                        return False
                    if _qualified_defaulted(targets[key], preview) != item['expected']:
                        raise ValueError
                    item['status'] = 'intent'
                    private_state._atomic_json(path, record)
                    try:
                        accepted = api.patch_workload(key, actual, targets[key])
                    except Exception:
                        accepted = True
                    if accepted is False:
                        item['status'] = 'prepared'
                        private_state._atomic_json(path, record)
                        return False
                    if accepted is not True:
                        raise ValueError
                if not _matches(api.read_workload(key), item['expected'], _uid(original)):
                    raise ValueError
                if item['status'] != 'applied':
                    item['status'] = 'applied'
                    private_state._atomic_json(path, record)
                if api.workload_ready(key, item['expected']) is not True:
                    return False
            for key, original in originals.items():
                api.qualify()
                expected = record['resources'][key]['expected']
                if not _matches(api.read_workload(key), expected, _uid(original)):
                    raise ValueError
                if api.workload_ready(key, expected) is not True:
                    return False
            api.qualify()
            return True
    except Exception:
        raise ValueError('development runtime transition unqualified; preserve evidence') from None
