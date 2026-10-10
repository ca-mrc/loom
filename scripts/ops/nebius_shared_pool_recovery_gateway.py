"""Retire only the frozen shared gateway grants, including uncertain creates.

Keep existing Role UIDs and every binding. A same-name read-only Role closes an
uncertain create without deleting anything or retrying an unknown write. This
is the gateway half of recovery-revoke, not a separately selectable operation.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_development_runtime_live import runtime_resource_path
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_gateway_authority import (
    gateway_review_namespaces,
    review_gateway_rules,
)
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_shared_pool_gateway import validate_shared_gateway
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings, WriterOptions
from scripts.ops.nebius_shared_pool_operation import _files
from scripts.ops.nebius_shared_pool_recovery_machines import (
    SharedRecoveryMachineTransport,
    _active,
    _frame,
    validate_shared_recovery_machines,
)
from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER

from loom.nebius_platform_render import digest

_PHASE = 'recovery-revoke'


def _sources(bindings: SharedPoolGuardBindings, state: Path) -> WriterOptions:
    frame = _frame(bindings, state)
    sources: WriterOptions = {key: (None,) for key in bindings.plan.development.authority}
    phase, child = frame['forward']['forward-gateway']['status'], state / 'forward-gateway'
    if phase != 'prepared' or child.exists() or child.is_symlink():
        files = _files(child, allow_empty=phase == 'prepared')
        if files or phase != 'prepared':
            record = validate_shared_gateway(bindings=bindings, state_dir=child,
                complete=phase == 'complete', prepared=phase == 'prepared')
            if record['local']['dispatch']:
                for key in sources:
                    row = record['stage']['resources'][key]
                    if row['status'] == 'create_intent':
                        sources[key] = (None, copy.deepcopy(row['expected']))
                    elif row['status'] == 'created':
                        value = copy.deepcopy(row['observed'])
                        value['metadata']['uid'] = row['uid']
                        sources[key] = (value,)
        if _files(child, allow_empty=phase == 'prepared') != files:
            raise ValueError
    if _frame(bindings, state) != frame:
        raise ValueError
    return sources


def _readonly(bindings: SharedPoolGuardBindings, source: dict[str, Any]) -> dict[str, Any]:
    value = _snapshot(source)
    if value['kind'] != 'Role' or MARKER in value['metadata'].get('annotations', {}):
        raise ValueError
    value['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
    for rule in value['rules']:
        if not set(rule['verbs']) <= {'get', 'list', 'watch', 'create', 'delete'}:
            raise ValueError
        rule['verbs'] = [verb for verb in rule['verbs'] if verb not in {'create', 'delete'}]
        if not rule['verbs']:
            raise ValueError
    return value


def _identity(bindings: SharedPoolGuardBindings, state: Path, sources: WriterOptions) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-gateway.v1', 'input_digest': bindings.input_digest,
        'operation_id': str(bindings.migration.registration.spec.operation_id), 'state_dir': str(state),
        'sources_sha256': digest(sources)}


def _prepared() -> dict[str, Any]:
    return {'status': 'prepared', 'create_intent': False, 'uid': None, 'before_resource_version': None}


def validate_shared_recovery_gateway(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    sources = _sources(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir, sources)
    record = _read(state_dir / 'gateway.json')
    roles = {key: choices for key, choices in sources.items() if bindings.plan.development.authority[key]['kind'] == 'Role'}
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'roles'} or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['roles'], dict) or record['roles'].keys() != roles.keys()):
        raise ValueError
    unfinished = False
    for key, choices in roles.items():
        row = record['roles'][key]
        if (not isinstance(row, dict) or set(row) != set(_prepared())
                or row['status'] not in {'prepared', 'original', 'create_intent', 'patch_intent', 'restricted', 'absent'}
                or type(row['create_intent']) is not bool or (row['create_intent'] and len(choices) != 2)
                or (complete and row['status'] not in {'restricted', 'absent'})
                or ((prepared or unfinished) and row != _prepared())):
            raise ValueError
        status, uid, version = row['status'], row['uid'], row['before_resource_version']
        if status in {'prepared', 'absent', 'create_intent'}:
            expected = {**_prepared(), 'status': status, 'create_intent': status == 'create_intent'}
            if row != expected or (status == 'absent' and choices != (None,)):
                raise ValueError
        else:
            _uid({'metadata': {'uid': uid}})
            original = choices[-1]
            if original is None or (original['metadata'].get('uid', uid) != uid):
                raise ValueError
            _readonly(bindings, original)
            if version is not None and (not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            if ((status == 'original' and version is not None) or (status == 'patch_intent' and version is None)
                    or (status == 'restricted' and version is None and not row['create_intent'])):
                raise ValueError
        unfinished |= status not in {'restricted', 'absent'}
    if prepared and _frame(bindings, state_dir.parent)['recovery'][_PHASE]['status'] != 'prepared':
        raise ValueError
    return record


def prepare_shared_recovery_gateway(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    sources = _sources(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'gateway.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir, sources),
                'roles': {key: _prepared() for key in sources if bindings.plan.development.authority[key]['kind'] == 'Role'}})
        validate_shared_recovery_gateway(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class HTTPSSharedRecoveryGateway(ManagementKubernetesTransport):
    """Fixed read/create/CAS operations; unknown dispatches have no retry path."""

    def __init__(self, *, bindings: SharedPoolGuardBindings, machines: SharedRecoveryMachineTransport | None,
            api_server: str, ssl_context: ssl.SSLContext, token: str | None = None,
            private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.machines = machines
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.binding = manager.binding
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        self.sources = _sources(self.bindings, self.state.parent)
        self.private_files = dict(private_files or {})
        if api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/'):
            raise ValueError('shared recovery gateway connection differs')
        self._scope()
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _frame(self.bindings, self.state.parent)
        status = frame['recovery'][_PHASE]['status']
        if (frame['outcome'] != 'pending' or status not in {'started', 'complete'}
                or (self.machines is None and status != 'complete')
                or (self.machines is not None and (self.machines.bindings != self.bindings or self.machines.state != self.state))
                or any(private_state._private_read(path) != raw for path, raw in self.private_files.items())
                or self.sources != _sources(self.bindings, self.state.parent)):
            raise ValueError('shared recovery gateway scope differs')
        files = _files(self.state)
        validate_shared_recovery_machines(bindings=self.bindings, state_dir=self.state, complete=True)
        record = validate_shared_recovery_gateway(bindings=self.bindings, state_dir=self.state, complete=status == 'complete')
        if _files(self.state) != files:
            raise ValueError('shared recovery gateway receipts changed')
        return frame, record, files

    def _namespaces(self) -> None:
        namespaces = {ns.name: str(ns.uid) for row in self.bindings.migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}
        namespaces[self.binding.namespace] = self.binding.namespace_uid
        for name, uid in namespaces.items():
            value = self._request('GET', '/api/v1/namespaces/' + name)
            if (value is None or value.get('apiVersion') != 'v1' or value.get('kind') != 'Namespace'
                    or value['metadata'].get('name') != name or _uid(value) != uid):
                raise ValueError('shared recovery gateway namespace differs')
            _snapshot(value)

    def qualify(self) -> bool:
        before = self._scope()
        _active(self.bindings, self.state, 'started')
        if self.machines is None:
            raise ValueError('shared recovery gateway mutation requires the active machine transport')
        self._namespaces()
        drained = self.machines.drained()
        if self.machines.machine_authority() != 'revoked' or self._scope() != before:
            raise ValueError('shared recovery gateway machine authority changed')
        return drained

    def _path(self, key: str) -> str:
        document = self.bindings.plan.development.authority[key]
        return runtime_resource_path(document) + '/' + str(document['metadata']['name'])

    def read(self) -> dict[str, dict[str, Any] | None]:
        before = self._scope()
        self._namespaces()
        values: dict[str, dict[str, Any] | None] = {}
        for key, choices in self.sources.items():
            current = self._request('GET', self._path(key))
            row = before[1]['roles'].get(key)
            allowed = choices
            if row is not None and row['status'] in {'create_intent', 'original', 'patch_intent', 'restricted'}:
                original = choices[-1]
                if original is None:
                    raise ValueError
                readonly = _readonly(self.bindings, original)
                if row['status'] == 'restricted':
                    allowed = (readonly,)
                elif row['status'] != 'original':
                    allowed = (*choices, readonly)
                if row['uid'] is not None:
                    allowed = tuple({**item, 'metadata': {**item['metadata'], 'uid': row['uid']}}
                        for item in allowed if item is not None)
            if current is None:
                if None not in allowed:
                    raise ValueError('shared recovery gateway grant disappeared')
            elif not any(item is not None and _matches(current, item, item['metadata'].get('uid', _uid(current))) for item in allowed):
                raise ValueError('shared recovery gateway grant differs')
            if (row is not None and row['status'] == 'restricted' and row['before_resource_version'] is not None
                    and current is not None and current['metadata']['resourceVersion'] == row['before_resource_version']):
                raise ValueError('shared recovery gateway version barrier missing')
            values[key] = current
        if self._scope() != before:
            raise ValueError('shared recovery gateway authority changed during read')
        return values

    def reduce_role(self, key: str, *, record_intent: Callable[[dict[str, Any]], None]) -> bool:
        scope = self._scope()
        if scope[1]['roles'][key]['status'] != 'original':
            raise ValueError('shared recovery gateway reduction has no original')
        before = self.read()[key]
        if before is None or not self.qualify():
            return False
        desired = _readonly(self.bindings, before)

        def patches(value: dict[str, Any]) -> list[dict[str, Any]]:
            version = value['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            return [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(value)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/rules', 'value': value['rules']},
                {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']},
                {'op': 'replace', 'path': '/rules', 'value': desired['rules']}]

        with self.client.stream('PATCH', self._path(key) + '?dryRun=All', json=patches(before),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if not _patch_result(response, desired=desired, uid=_uid(before)):
                return False
        current = self.read()[key]
        if current is None or not _matches(current, before, _uid(before)) or not self.qualify():
            return False
        if self._scope() != scope:
            raise ValueError('shared recovery gateway reduction scope changed')
        record_intent(current)
        selected = self._scope()
        row = selected[1]['roles'][key]
        if (row['status'] != 'patch_intent' or row['uid'] != _uid(current)
                or row['before_resource_version'] != current['metadata']['resourceVersion']):
            raise ValueError('shared recovery gateway reduction intent differs')
        with self.client.stream('PATCH', self._path(key), json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            accepted = _patch_result(response, desired=desired, uid=_uid(current))
        if self._scope() != selected:
            raise ValueError('shared recovery gateway reduction outcome unconfirmed')
        return accepted

    def create_barrier(self, key: str, *, record_intent: Callable[[], None]) -> bool:
        scope = self._scope()
        choices = self.sources[key]
        if scope[1]['roles'][key] != _prepared() or len(choices) != 2 or choices[-1] is None:
            raise ValueError('shared recovery gateway barrier has no uncertain predecessor')
        if self.read()[key] is not None or not self.qualify():
            return False
        desired = _readonly(self.bindings, choices[-1])
        path = runtime_resource_path(desired)
        preview = self._request('POST', path + '?dryRun=All', document=desired)
        if preview is None or not _matches(preview, desired, _uid(preview)):
            raise ValueError('shared recovery gateway barrier preview differs')
        if self.read()[key] is not None or not self.qualify():
            return False
        if self._scope() != scope:
            raise ValueError('shared recovery gateway barrier scope changed')
        record_intent()
        selected = self._scope()
        if selected[1]['roles'][key] != {**_prepared(), 'status': 'create_intent', 'create_intent': True}:
            raise ValueError('shared recovery gateway barrier intent differs')
        result = self._request('POST', path, document=desired)
        if result is None or not _matches(result, desired, _uid(result)) or self._scope() != selected:
            raise ValueError('shared recovery gateway barrier outcome unconfirmed')
        return True

    def verify_permissions(self) -> None:
        """Read-only after revoke completion; never replay machine/stop transports."""
        before = self._scope()
        validate_shared_recovery_gateway(bindings=self.bindings, state_dir=self.state, complete=True)
        fixed = self.read()

        def inventory() -> dict[str, dict[str, Any]]:
            values = [row for resource, kind in (('roles', 'Role'), ('rolebindings', 'RoleBinding'),
                ('clusterroles', 'ClusterRole'), ('clusterrolebindings', 'ClusterRoleBinding'))
                for row in inventory_resources(self._request, 'rbac.authorization.k8s.io/v1', resource, kind)]
            result = {_key(row): row for row in values}
            if len(result) != len(values) or any(result.get(key) != value for key, value in fixed.items()):
                raise ValueError('shared recovery gateway inventory differs')
            return result

        observed = inventory()
        authority = []
        for value in fixed.values():
            if value is None or value['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                continue
            reference = value['roleRef']
            key = reference['kind'] + ':' + (value['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
            role = fixed.get(key)
            if role is None:
                raise ValueError('shared recovery gateway binding target absent')
            authority.append(role)
        bindings = [row for row in observed.values() if row['kind'] in {'RoleBinding', 'ClusterRoleBinding'}]
        for namespace in gateway_review_namespaces(self.bindings.migration, bindings):
            review_gateway_rules(self.client, manager_namespace=self.binding.namespace, namespace=namespace, authority=authority)
        if inventory() != observed or self.read() != fixed or self._scope() != before:
            raise ValueError('shared recovery gateway permissions changed during review')


def advance_shared_recovery_gateway(*, bindings: SharedPoolGuardBindings,
        api: HTTPSSharedRecoveryGateway, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_gateway(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'gateway.json', record)
                validate_shared_recovery_gateway(bindings=bindings, state_dir=state_dir, complete=False)

            if not api.qualify():
                return False
            # Renderer order, not the JSON serializer's sorted key order.
            for key, choices in api.sources.items():
                if key not in record['roles']:
                    continue
                row = record['roles'][key]
                current = api.read()[key]
                if row['status'] == 'prepared' and choices == (None,):
                    row['status'] = 'absent'
                    save()
                elif row['status'] == 'prepared' and current is None:
                    def create_intent(*, selected_key: str = key) -> None:
                        if record['roles'][selected_key] != _prepared():
                            raise ValueError
                        record['roles'][selected_key].update(status='create_intent', create_intent=True)
                        save()
                    try:
                        accepted = api.create_barrier(key, record_intent=create_intent)
                    except Exception:
                        if row['status'] != 'create_intent':
                            raise
                        accepted = True
                    if not accepted:
                        return False
                    current = api.read()[key]
                if row['status'] in {'prepared', 'create_intent'}:
                    if current is None:
                        return False
                    source = choices[-1]
                    if source is None:
                        raise ValueError
                    row.update(uid=_uid(current), status='restricted'
                        if _matches(current, _readonly(bindings, source), _uid(current)) else 'original')
                    save()
                if row['status'] == 'original':
                    def patch_intent(value: dict[str, Any], *, selected_key: str = key) -> None:
                        selected = record['roles'][selected_key]
                        if selected['status'] != 'original' or selected['uid'] != _uid(value):
                            raise ValueError
                        selected.update(status='patch_intent', before_resource_version=value['metadata']['resourceVersion'])
                        save()
                    try:
                        accepted = api.reduce_role(key, record_intent=patch_intent)
                    except Exception:
                        if row['status'] != 'patch_intent':
                            raise
                        accepted = True
                    if accepted is False:
                        row.update(status='original', before_resource_version=None)
                        save()
                        return False
                    if accepted is not True:
                        raise ValueError
                if row['status'] == 'patch_intent':
                    current = api.read()[key]
                    source = choices[-1]
                    if current is None or source is None or not _matches(current, _readonly(bindings, source), row['uid']):
                        return False
                    if current['metadata']['resourceVersion'] == row['before_resource_version']:
                        raise ValueError
                    row['status'] = 'restricted'
                    save()
            if not api.qualify():
                return False
            api.verify_permissions()
            if not api.qualify():
                return False
            api.read()
            validate_shared_recovery_gateway(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared recovery gateway retirement unconfirmed; preserve retained evidence') from None
