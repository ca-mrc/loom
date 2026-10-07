"""Restore exact retained Role snapshots only after stopped-template recovery."""
from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover import PoolCutoverRequest
from scripts.ops.nebius_pool_gateway_retirement import PoolGatewayRetirementAPI
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_role_fencing import role_fence_documents
from scripts.ops.nebius_pool_template_restoration import (
    RecoveryDrainPending,
    _template_record,
    qualify_template_restoration,
)

from loom.nebius_platform_render import digest

Documents = dict[str, dict[str, Any]]


class PoolRoleRestorationAPI(PoolGatewayRetirementAPI, Protocol):
    def read_legacy_role(self, key: str) -> dict[str, Any]: ...
    def preview_legacy_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None: ...
    def restore_legacy_role(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                            record_intent: Callable[[dict[str, Any]], None]) -> bool | RecoveryDrainPending: ...
    def qualify_legacy_roles(self) -> None: ...


def _paths(request: PoolCutoverRequest, state: Path, anchor: Path) -> tuple[Path, Path]:
    operation = request.fencing.retirement.migration.registration.spec.operation_id
    return state / 'role-restoration.json', anchor / (str(operation) + '-role-restoration.json')


def _role_record(request: PoolCutoverRequest, *, state: Path, anchor: Path
                 ) -> tuple[Documents, Documents, Documents, dict[str, Any], dict[str, Any] | None]:
    _, _, _, _, templates = _template_record(request, state=state, anchor=anchor)
    if templates is None or any(row['phase'] != 'restored' for row in templates['workloads'].values()):
        raise ValueError('pool_role_restoration_closed_templates_required')
    originals = {_key(row): row for row in request.fencing.originals}
    before = role_fence_documents(request.fencing)
    targets = {key: _stable(row) for key, row in originals.items()}
    operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
    identity = {'schema': 'loom.nebius-pool-role-restoration.v1', 'operation_id': operation,
        'state_dir': str(state), 'template_restoration_sha256': _hash(state / 'template-restoration.json'),
        'roles_sha256': digest({'originals': originals, 'before': before, 'targets': targets})}
    path, marker = _paths(request, state, anchor)
    if not marker.exists() and not marker.is_symlink():
        if path.exists() or path.is_symlink():
            raise ValueError
        return originals, before, targets, identity, None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path))
    if (not isinstance(record, dict) or set(record) != {*identity, 'roles'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['roles'], dict) or set(record['roles']) != set(targets)):
        raise ValueError
    for item in record['roles'].values():
        if (not isinstance(item, dict) or set(item) != {'phase', 'before_resource_version'}
                or item['phase'] not in {'prepared', 'intent', 'restored'}):
            raise ValueError
        version = item['before_resource_version']
        if item['phase'] == 'prepared':
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
    return originals, before, targets, identity, record


def restored_role_options(request: PoolCutoverRequest, *, state: Path, anchor: Path
                          ) -> dict[str, tuple[dict[str, Any], ...]] | None:
    if not any(path.exists() or path.is_symlink() for path in _paths(request, state, anchor)):
        return None
    _, before, targets, _, record = _role_record(request, state=state, anchor=anchor)
    if record is None:
        raise ValueError
    return {key: (before[key],) if row['phase'] == 'prepared' else (targets[key],)
        if row['phase'] == 'restored' else (before[key], targets[key]) for key, row in record['roles'].items()}


def qualify_role_restoration(request: PoolCutoverRequest, api: PoolGatewayRetirementAPI, *,
                             state: Path, anchor: Path) -> str | None:
    _role_record(request, state=state, anchor=anchor)
    return qualify_template_restoration(request, api, state=state, anchor=anchor)


def restore_pool_roles(*, request: PoolCutoverRequest, api: PoolRoleRestorationAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        with private_state._locked_state(anchor):
            originals, before, targets, identity, record = _role_record(request, state=state, anchor=anchor)
            path, marker = _paths(request, state, anchor)

            def result(status: str) -> dict[str, Any]:
                return {'status': status, 'operation_id': identity['operation_id'], 'legacy_restore_allowed': False}

            def observe() -> Documents:
                api.verify_retained()
                options = restored_role_options(request, state=state, anchor=anchor)
                actual = {key: api.read_legacy_role(key) for key in originals}
                for key, row in actual.items():
                    choices = (before[key],) if options is None else options[key]
                    if not any(_matches(row, wanted, _uid(originals[key])) for wanted in choices):
                        raise ValueError
                return actual

            def qualify() -> str | None:
                return qualify_role_restoration(request, api, state=state, anchor=anchor)

            if record is None:
                # Existing journals are freshly qualified in the active row or
                # completion path; preparing new evidence still needs this proof.
                observe()
                pending = qualify()
                if pending is not None:
                    return result(pending)
                record = {**identity, 'roles': {key: {'phase': 'prepared', 'before_resource_version': None} for key in targets}}
                private_state._atomic_json(marker, identity)
                private_state._atomic_json(path, record)
            for key, desired in targets.items():
                item = record['roles'][key]
                if item['phase'] == 'restored':
                    continue
                if item['phase'] == 'prepared':
                    # The actual adapter owns the fresh authority/drain proof;
                    # preview only validates the fixed target and CAS shape.
                    actual = api.read_legacy_role(key)
                    preview = api.preview_legacy_role(key, actual, desired)
                    if preview is None:
                        return result('pending_role_restoration_update')
                    if _stable(preview) != desired:
                        raise ValueError
                    def record_intent(fresh: dict[str, Any], *, item: dict[str, Any] = item, key: str = key) -> None:
                        nonlocal actual
                        if item['phase'] != 'prepared' or not _matches(fresh, actual, _uid(originals[key])):
                            raise ValueError
                        version = fresh['metadata']['resourceVersion']
                        if not isinstance(version, str) or not 0 < len(version) <= 128:
                            raise ValueError
                        actual = fresh
                        item.update(phase='intent', before_resource_version=version)
                        private_state._atomic_json(path, record)

                    try:
                        accepted = api.restore_legacy_role(key, actual, desired, record_intent=record_intent)
                    except Exception:
                        if item['phase'] != 'intent':
                            raise
                        accepted = None
                    if isinstance(accepted, RecoveryDrainPending):
                        if item != {'phase': 'prepared', 'before_resource_version': None}:
                            raise ValueError
                        return result(accepted.value)
                    if accepted is False:
                        item.update(phase='prepared', before_resource_version=None)
                        private_state._atomic_json(path, record)
                        return result('pending_role_restoration_update')
                    actual = api.read_legacy_role(key)
                else:
                    # An unknown intent is observation-only and still needs its
                    # own fresh qualification; never resend its PATCH.
                    actual = observe()[key]
                    pending = qualify()
                    if pending is not None:
                        return result(pending)
                if not _matches(actual, desired, _uid(originals[key])):
                    if item['phase'] == 'intent' and _matches(actual, before[key], _uid(originals[key])):
                        return result('pending_role_restoration_outcome')
                    raise ValueError
                if actual['metadata']['resourceVersion'] == item['before_resource_version']:
                    raise ValueError
                item['phase'] = 'restored'
                private_state._atomic_json(path, record)
            observe()
            pending = qualify()
            if pending is not None:
                return result(pending)
            api.qualify_legacy_roles()
            observe()
            pending = qualify()
            return result(pending if pending is not None else 'pool_legacy_roles_restored_closed')
    except Exception:
        raise ValueError('pool_role_restoration_unconfirmed_preserve_evidence') from None
