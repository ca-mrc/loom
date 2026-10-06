"""One stopped manager image switch; ambiguous writes are observed, never retried."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_manager_image_history import (
    STEPS,
    ManagerImageEntry,
    ManagerImageRepairBinding,
    manager_image_entry,
    prepared_image_record,
)
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_startup_repair import _RECOVERY, _exists, _manager_options


class ManagerImageAPI(Protocol):
    def qualify_closed(self) -> None: ...
    def read_workload(self, key: str) -> dict[str, Any]: ...
    def manager_drained(self, key: str, desired: dict[str, Any]) -> bool: ...
    def preview_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def patch_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> bool: ...


def qualify_image_entry_closed(entry: ManagerImageEntry, *, state: Path, anchor: Path) -> None:
    """Historical readers allow later activation; a new write never does."""
    operation = entry.identity['operation_id']
    if (_hash(state / 'activation.json') != entry.binding.activation_sha256
            or any(_exists(path) for phase in _RECOVERY for path in (
                state / (phase + '.json'), anchor / (operation + '-' + phase + '.json')))):
        raise ValueError('pool_manager_image_entry_changed')


def repair_manager_image(*, request: PoolCutoverRequest, binding: ManagerImageRepairBinding,
        api: ManagerImageAPI, state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            entry = manager_image_entry(request, binding, state=state, anchor=anchor)
            record = entry.record or prepared_image_record(entry.identity)
            key, uid = _key(request.manager), _uid(request.manager)

            def observe() -> dict[str, Any]:
                current = manager_image_entry(request, binding, state=state, anchor=anchor)
                if current.identity != entry.identity or current.documents != entry.documents:
                    raise ValueError
                qualify_image_entry_closed(current, state=state, anchor=anchor)
                api.qualify_closed()
                actual = api.read_workload(key)
                if not any(_matches(actual, option, uid) for option in _manager_options(entry.documents, record)):
                    raise ValueError
                return actual

            def save() -> None:
                private_state._atomic_json(entry.path, record)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': entry.identity['operation_id'],
                    'admission_open': False, 'runtime_verified': False}

            observe()
            if not entry.anchored:
                private_state._atomic_json(entry.marker, entry.identity)
            if entry.record is None:
                save()
            for index, phase in enumerate(STEPS):
                item = record['phases'][phase]
                if item['phase'] == 'applied':
                    continue
                actual = observe()
                desired = copy.deepcopy(entry.documents[index + 1])
                if item['phase'] == 'prepared' and phase in {'template', 'start'}:
                    drained = api.manager_drained(key, entry.documents[index])
                    if type(drained) is not bool:
                        raise ValueError
                    if not drained:
                        return result('pending_manager_image_drain')
                if item['phase'] == 'prepared':
                    preview = api.preview_repair(phase, actual, desired)
                    if preview is None:
                        return result('pending_manager_image_update')
                    if _stable(preview) != _stable(desired):
                        raise ValueError
                    observe()
                    version = actual['metadata']['resourceVersion']
                    if not isinstance(version, str) or not 0 < len(version) <= 128:
                        raise ValueError
                    item.update(phase='intent', before_resource_version=version)
                    save()
                    try:
                        accepted = api.patch_repair(phase, actual, desired)
                    except Exception:
                        accepted = None
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        save()
                        return result('pending_manager_image_update')
                    actual = api.read_workload(key)
                if not _matches(actual, desired, uid):
                    if _matches(actual, entry.documents[index], uid):
                        return result('pending_manager_image_outcome')
                    raise ValueError
                if actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'applied'
                save()
            observe()
            return result('pool_manager_image_repaired_closed')
    except Exception:
        raise ValueError('pool_manager_image_unconfirmed_preserve_evidence') from None
