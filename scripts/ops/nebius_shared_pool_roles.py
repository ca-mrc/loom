"""Current-staging Role reductions bound to the shared handoff, not old retirement.

Only the two captured Roles can be patched. Bindings are read, never changed.
The protected parent supplies fresh drain and whole-pool writer qualification;
effective permission reviews here are fresh observations, not permanent grants.
"""
from __future__ import annotations

import copy
import json
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_gateway_authority import subject_review_namespaces
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest

_PHASE = 'forward-roles'


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink()
            or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings) -> dict[str, str]:
    return {'schema': 'loom.nebius-shared-pool-roles.v1', 'input_digest': bindings.input_digest,
        'originals_sha256': digest(bindings.plan.staging_role_originals),
        'targets_sha256': digest(bindings.plan.staging_role_targets)}


def validate_shared_staging_roles(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings)
    record = _read(state_dir / 'roles.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool
            or (complete and prepared) or set(record) != {*identity, 'roles'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['roles'], dict) or set(record['roles']) != set(bindings.plan.staging_role_originals)
            or (prepared and frame['forward'][_PHASE]['status'] != 'prepared')):
        raise ValueError
    unfinished = False
    for key in sorted(record['roles']):
        status = record['roles'][key]
        if (status not in {'prepared', 'intent', 'restricted'} or (unfinished and status != 'prepared')
                or (complete and status != 'restricted') or (prepared and status != 'prepared')):
            raise ValueError
        unfinished |= status != 'restricted'
    return record


def prepare_shared_staging_roles(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'roles.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings),
                'roles': dict.fromkeys(sorted(bindings.plan.staging_role_originals), 'prepared')})
        validate_shared_staging_roles(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def shared_forward_role_options(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> dict[str, tuple[dict[str, Any], ...]]:
    """Recorded forward alternatives only; recovery must qualify its own targets."""
    frame = _frame(bindings, state_dir)
    phase = frame['forward'][_PHASE]['status']
    path = state_dir / _PHASE / 'roles.json'
    statuses = dict.fromkeys(bindings.plan.staging_role_originals, 'prepared')
    if phase != 'prepared' or path.exists() or path.is_symlink():
        statuses = validate_shared_staging_roles(bindings=bindings, state_dir=path.parent,
            complete=phase == 'complete', prepared=phase == 'prepared')['roles']
    result: dict[str, tuple[dict[str, Any], ...]] = {}
    for key, original in bindings.plan.staging_role_originals.items():
        before, after = copy.deepcopy(original), copy.deepcopy(bindings.plan.staging_role_targets[key])
        after['metadata']['uid'] = _uid(original)
        result[key] = (before,) if statuses[key] == 'prepared' else (before, after) if statuses[key] == 'intent' else (after,)
    if _frame(bindings, state_dir) != frame:
        raise ValueError
    return result


class HTTPSSharedStagingRoles(ManagementKubernetesTransport):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[SharedPoolGuardBindings, bool], None], token: str | None = None):
        self.bindings = copy.deepcopy(bindings)
        manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        runtime = self.bindings.plan.development.runtime.request
        if (not callable(qualify_runtime)
                or api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared Role connection differs')
        _active(self.bindings, self.state, 'started')
        self.identity = _identity(self.bindings)
        self.qualify_runtime = qualify_runtime
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _scope(self, *, writing: bool = False) -> None:
        frame = _active(self.bindings, self.state, 'started')
        child = validate_shared_staging_roles(bindings=self.bindings, state_dir=self.state, complete=False)
        if _identity(self.bindings) != self.identity:
            raise ValueError
        self.qualify_runtime(self.bindings, writing)
        migration = self.bindings.migration
        binding = migration.registration.binding
        namespaces = {'kube-system': binding.kube_system_uid, binding.namespace: binding.namespace_uid,
            **{guard.namespace: str(guard.namespace_uid) for guard in migration.guards},
            **{ns.name: str(ns.uid) for participant in migration.registration.spec.participants
                for ns in (participant.execution_namespace, participant.build_namespace)}}
        for name, uid in namespaces.items():
            row = self._request('GET', '/api/v1/namespaces/' + name)
            if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                    or row['metadata'].get('name') != name or _uid(row) != uid):
                raise ValueError
            _snapshot(row)
        if (_identity(self.bindings) != self.identity or _active(self.bindings, self.state, 'started') != frame
                or validate_shared_staging_roles(bindings=self.bindings, state_dir=self.state, complete=False) != child):
            raise ValueError

    def _role_path(self, key: str) -> str:
        row = self.bindings.plan.staging_role_originals[key]
        return '/apis/rbac.authorization.k8s.io/v1/namespaces/' + str(row['metadata']['namespace']) + '/roles/' + str(row['metadata']['name'])

    def read_role(self, key: str) -> dict[str, Any]:
        original = self.bindings.plan.staging_role_originals[key]
        self._scope()
        row = self._request('GET', self._role_path(key))
        if (row is None or _key(row) != key or row.get('apiVersion') != 'rbac.authorization.k8s.io/v1'
                or _uid(row) != _uid(original)):
            raise ValueError
        _snapshot(row)
        return row

    def restrict_role(self, key: str, before: dict[str, Any]) -> bool:
        original = self.bindings.plan.staging_role_originals[key]
        desired = self.bindings.plan.staging_role_targets[key]
        version = before['metadata']['resourceVersion']
        if not _matches(before, original, _uid(original)) or not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        self._scope(writing=True)
        if _read(self.state / 'roles.json')['roles'][key] != 'intent':
            raise ValueError
        patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(original)},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
            {'op': 'test', 'path': '/rules', 'value': before['rules']},
            {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']},
            {'op': 'replace', 'path': '/rules', 'value': desired['rules']}]
        with self.client.stream('PATCH', self._role_path(key), json=patches,
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            return _patch_result(response, desired=desired, uid=_uid(original))

    def _bindings(self) -> list[dict[str, Any]]:
        revision: str | None = None

        def read(method: str, path: str) -> dict[str, Any] | None:
            nonlocal revision
            if revision is not None and 'continue=' not in path:
                path += '&' + urlencode({'resourceVersion': revision, 'resourceVersionMatch': 'Exact'})
            page = self._request(method, path)
            current = None if page is None else page.get('metadata', {}).get('resourceVersion')
            if not isinstance(current, str) or not 0 < len(current) <= 1024 or revision not in (None, current):
                raise ValueError
            revision = current
            return page

        rows = [row for resource, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
            for row in inventory_resources(read, 'rbac.authorization.k8s.io/v1', resource, kind)]
        actual = {_key(row): row for row in rows}
        if len(actual) != len(rows) or len({_uid(row) for row in rows}) != len(rows):
            raise ValueError
        for row in self.bindings.plan.staging.attachment.resources:
            if row['kind'] == 'RoleBinding' and not _matches(actual[_key(row)], row, _uid(row)):
                raise ValueError
        return sorted(rows, key=_key)

    def verify_readonly(self) -> None:
        """Bounded nonpersisted reviews; impersonation is per request, never global."""
        self._scope()
        child = validate_shared_staging_roles(bindings=self.bindings, state_dir=self.state, complete=True)
        originals = self.bindings.plan.staging_role_originals
        before = {key: self.read_role(key) for key in originals}
        if any(not _matches(before[key], self.bindings.plan.staging_role_targets[key], _uid(row)) for key, row in originals.items()):
            raise ValueError
        bindings = self._bindings()
        subjects = set()
        for row in self.bindings.plan.staging.originals.values():
            pod = (row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob'
                else row['spec']['template']['spec'])
            subjects.add((row['metadata']['namespace'], pod.get('serviceAccountName', 'default')))
        for subject in sorted(subjects):
            account_namespace, account = subject
            headers = [('Impersonate-User', f'system:serviceaccount:{account_namespace}:{account}'),
                *(('Impersonate-Group', group) for group in (
                    'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'))]
            for namespace in subject_review_namespaces(self.bindings.migration, bindings, subject=subject):
                with self.client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews',
                        headers=headers, json={'apiVersion': 'authorization.k8s.io/v1',
                            'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': namespace}}) as response:
                    if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                        raise ValueError
                    raw = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        if len(raw) + len(chunk) > 4 * 1024**2:
                            raise ValueError
                        raw.extend(chunk)
                    qualify_pool_reader_rules(json.loads(raw), namespace=namespace)
        if (bindings != self._bindings() or before != {key: self.read_role(key) for key in originals}
                or validate_shared_staging_roles(bindings=self.bindings, state_dir=self.state, complete=True) != child):
            raise ValueError
        self._scope()


def advance_shared_staging_roles(*, bindings: SharedPoolGuardBindings, api: HTTPSSharedStagingRoles,
        state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            api._scope(writing=True)
            path = state_dir / 'roles.json'
            record = validate_shared_staging_roles(bindings=bindings, state_dir=state_dir, complete=False)

            def save(key: str, status: str) -> None:
                if _read(path) != record:
                    raise ValueError
                record['roles'][key] = status
                private_state._atomic_json(path, record)

            for key in sorted(bindings.plan.staging_role_originals):
                original = bindings.plan.staging_role_originals[key]
                actual = api.read_role(key)
                if record['roles'][key] == 'prepared':
                    if not _matches(actual, original, _uid(original)):
                        raise ValueError
                    save(key, 'intent')
                    try:
                        accepted = api.restrict_role(key, actual)
                    except Exception:
                        accepted = True  # Unknown outcomes permit readback only, never redispatch.
                    if accepted is False:
                        save(key, 'prepared')
                        return False
                    if accepted is not True:
                        raise ValueError
                    actual = api.read_role(key)
                if not _matches(actual, bindings.plan.staging_role_targets[key], _uid(original)):
                    raise ValueError
                if record['roles'][key] != 'restricted':
                    save(key, 'restricted')
            api.verify_readonly()
            if validate_shared_staging_roles(bindings=bindings, state_dir=state_dir, complete=True) != record:
                raise ValueError
            return True
    except Exception:
        raise ValueError('shared staging Roles unconfirmed; preserve retained evidence') from None
