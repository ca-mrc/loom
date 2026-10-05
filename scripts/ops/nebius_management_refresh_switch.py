"""One retained-manager cutover; no retry of a possibly committed update.

The connected caller owns predecessor, publication, configuration, backup and
migration proof. This journal additionally requires its activation barrier before
starting the candidate. It never alters the historical bootstrap switch.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
from scripts.ops.nebius_management_stage import _qualified_defaulted
from scripts.ops.nebius_management_switch import _matches, _stable

from loom.nebius_platform_render import digest

MARKER = 'loom.nebius/management-refresh-id'


@dataclass(frozen=True, repr=False)
class ManagementRefreshSwitchRequest:
    render: ManagementRefreshRenderRequest
    operation_id: UUID
    initial_stopped: dict[str, Any] | None = None


class ManagementRefreshSwitchAPI(Protocol):
    def read(self) -> dict[str, Any]: ...
    def preview(self, before: dict[str, Any], operation_id: str) -> dict[str, Any]: ...
    def patch(self, before: dict[str, Any], action: str, operation_id: str) -> bool:
        """False only for definite rejection; exceptions may have committed."""
        ...

    def retired(self) -> bool:
        """Exact stopped generation; complete Pod absence and zero owned replicas."""
        ...

    def activation_ready(self) -> bool:
        """Requalify this operation's config/runtime/backup/migration receipts."""
        ...


def refresh_initial(request: ManagementRefreshSwitchRequest) -> dict[str, Any]:
    """An explicit stopped source cannot change the retained runtime or its UID.

    This shape check does not qualify failed history; the protected entry and
    connected preflight must independently bind that history before adoption.
    """
    initial = request.initial_stopped
    if initial is None:
        return request.render.active
    try:
        marker = initial['metadata']['annotations'][MARKER]
        operation = UUID(marker)
        if not operation.int or str(operation) != marker or operation == request.operation_id:
            raise ValueError
        expected = _snapshot(request.render.active)
        expected['metadata'].setdefault('annotations', {})[MARKER] = marker
        expected['spec']['replicas'] = 0
        if not _matches(initial, expected, _uid(request.render.active)):
            raise ValueError
        return initial
    except Exception:
        raise ValueError('management refresh initial stopped source differs') from None


def refresh_switch_identity(request: ManagementRefreshSwitchRequest, state_dir: Path) -> dict[str, Any]:
    """Preserve historical identities, binding an explicit stopped source if set."""
    return {'schema': 'loom.nebius-management-refresh-switch.v1',
        'operation_id': str(request.operation_id), 'state_dir': str(state_dir.absolute()),
        'original_uid': _uid(request.render.active),
        'input_digest': digest({'original': _stable(refresh_initial(request)),
            'target': refresh_target(request, 'activate'),
            'before': request.render.before.model_dump(mode='json'),
            'after': request.render.after.model_dump(mode='json'),
            'candidate': request.render.candidate, 'profile': request.render.profile})}


def refresh_target(request: ManagementRefreshSwitchRequest, action: Literal['retire', 'activate']) -> dict[str, Any]:
    if not isinstance(request.operation_id, UUID) or not request.operation_id.int or action not in {'retire', 'activate'}:
        raise ValueError('management refresh operation identity differs')
    rendered = render_refresh(request.render)
    target = copy.deepcopy(rendered.deployment) if action == 'activate' else _snapshot(request.render.active)
    target['metadata'].setdefault('annotations', {})[MARKER] = str(request.operation_id)
    target['spec']['replicas'] = 1 if action == 'activate' else 0
    return target


def qualify_refresh_drain(request: ManagementRefreshSwitchRequest, *, deployment: dict[str, Any],
                          replicasets: dict[str, Any], pods: dict[str, Any]) -> bool:
    """Qualify bounded, complete current observations; never infer missing status.

    The connected caller reads these collections consistently, then re-reads the
    same stopped Deployment before its activation CAS. Terminating Pods still run
    code and therefore never count as drained. This function performs no I/O.
    """
    try:
        uid, namespace = _uid(request.render.active), request.render.before.namespace
        if not _matches(deployment, refresh_target(request, 'retire'), uid):
            raise ValueError

        def collection(page: dict[str, Any], kind: str, version: str) -> list[dict[str, Any]]:
            rows, metadata = page['items'], page['metadata']
            revision = metadata['resourceVersion']
            if (page['kind'] != kind + 'List' or page['apiVersion'] != version
                    or not isinstance(revision, str) or not 0 < len(revision) <= 128
                    or metadata.get('continue') or not isinstance(rows, list) or len(rows) > 100):
                raise ValueError
            # Kubernetes typed collections omit item TypeMeta. Inherit absent
            # fields only from this verified collection; never replace conflicts.
            normalized = [{'apiVersion': version, 'kind': kind, **row} for row in rows]
            if any(row['kind'] != kind or row['apiVersion'] != version
                    or row['metadata']['namespace'] != namespace for row in normalized):
                raise ValueError
            return normalized

        sets = collection(replicasets, 'ReplicaSet', 'apps/v1')
        current_pods = collection(pods, 'Pod', 'v1')

        def observed_zero(controller: dict[str, Any]) -> bool:
            generation = controller['metadata']['generation']
            status = controller.get('status', {})
            observed = status.get('observedGeneration', 0)
            counters = [controller['spec'].get('replicas', 1)] + [status.get(field, 0) for field in (
                'replicas', 'readyReplicas', 'availableReplicas', 'updatedReplicas',
                'unavailableReplicas', 'fullyLabeledReplicas', 'terminatingReplicas')]
            if (type(generation) is not int or generation <= 0 or type(observed) is not int or observed < 0
                    or any(type(value) is not int or value < 0 for value in counters)):
                raise ValueError
            return observed >= generation and all(value == 0 for value in counters)

        complete = observed_zero(deployment)
        seen = set()
        for replica_set in sets:
            metadata = replica_set['metadata']
            identity = _uid(replica_set)
            owners = metadata['ownerReferences']
            if (identity in seen or metadata.get('labels', {}).get('app') != 'loom-service'
                    or len(owners) != 1 or owners[0].get('controller') is not True):
                raise ValueError
            seen.add(identity)
            owner = dict(owners[0])
            blocking = owner.pop('blockOwnerDeletion', False)
            if type(blocking) is not bool or owner != {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                    'name': 'loom-service', 'uid': uid, 'controller': True}:
                raise ValueError
            ready = observed_zero(replica_set)
            complete = complete and ready and not metadata.get('deletionTimestamp')
        return bool(complete and not current_pods)
    except Exception:
        raise ValueError('management refresh drain observation unqualified') from None


def refresh_switch_record(request: ManagementRefreshSwitchRequest, *, state_dir: Path) -> dict[str, Any] | None:
    """Read the existing fixed switch contract without replaying any operation."""
    identity = refresh_switch_identity(request, state_dir)
    path = state_dir / 'cutover.json'
    if not (path.exists() or path.is_symlink()):
        return None
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'original', 'phase', 'active'}
            or any(record[key] != value for key, value in identity.items())
            or record['original'] != refresh_initial(request)
            or record['phase'] not in {'prepared', 'retire_intent', 'stopped', 'activate_intent', 'active'}
            or (record['phase'] in {'activate_intent', 'active'}) != (record['active'] is not None)):
        raise ValueError('management refresh switch history differs')
    if (record['active'] is not None
            and _qualified_defaulted(refresh_target(request, 'activate'), record['active']) != record['active']):
        raise ValueError('management refresh switch preview differs')
    return record


def switch_refresh(*, request: ManagementRefreshSwitchRequest, api: ManagementRefreshSwitchAPI,
                   state_dir: Path, activate: bool) -> bool:
    """Stop/drain or activate exactly once; an unresolved write stays unresolved."""
    try:
        stopped = refresh_target(request, 'retire')
        desired = refresh_target(request, 'activate')
        original, uid = refresh_initial(request), _uid(request.render.active)
        identity = refresh_switch_identity(request, state_dir)
        with private_state._locked_state(state_dir):
            path = state_dir / 'cutover.json'
            actual = api.read()
            record = refresh_switch_record(request, state_dir=state_dir)
            if record is None:
                if activate or not _matches(actual, original, uid):
                    raise ValueError
                record = {**identity, 'original': copy.deepcopy(original), 'phase': 'prepared', 'active': None}
                private_state._atomic_json(path, record)

            def save(phase: str) -> None:
                record['phase'] = phase
                private_state._atomic_json(path, record)

            operation_id = str(request.operation_id)
            if activate:
                if record['phase'] not in {'stopped', 'activate_intent', 'active'}:
                    raise ValueError
                if api.activation_ready() is not True:
                    return False
                if record['phase'] == 'stopped':
                    if not _matches(actual, stopped, uid):
                        raise ValueError
                    if api.retired() is not True:
                        return False
                    record['active'] = _qualified_defaulted(desired, api.preview(actual, operation_id))
                    save('activate_intent')
                    try:
                        accepted = api.patch(actual, 'activate', operation_id)
                    except Exception:
                        accepted = True
                    if accepted is False:
                        record['active'] = None
                        save('stopped')
                        return False
                    actual = api.read()
                if not _matches(actual, record['active'], uid):
                    raise ValueError('management refresh activation unresolved')
                if record['phase'] != 'active':
                    save('active')
                return True

            if record['phase'] in {'activate_intent', 'active'}:
                raise ValueError
            if record['phase'] == 'prepared':
                if not _matches(actual, original, uid):
                    raise ValueError
                save('retire_intent')
                try:
                    accepted = api.patch(actual, 'retire', operation_id)
                except Exception:
                    accepted = True
                if accepted is False:
                    save('prepared')
                    return False
                actual = api.read()
            if not _matches(actual, stopped, uid):
                raise ValueError('management refresh retirement unresolved')
            if record['phase'] != 'stopped':
                save('stopped')
            return api.retired() is True
    except Exception:
        raise ValueError('management refresh cutover unresolved; preserve recovery evidence') from None
