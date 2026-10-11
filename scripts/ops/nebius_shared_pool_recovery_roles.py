"""Restore captured current staging Roles under the closed shared recovery parent.

A distinct recovery marker forces a real version barrier even if an uncertain
forward reduction has not committed. Bindings are only observed, never changed.
Unknown PATCH results cannot redispatch. Later restart may observe rights without
replaying restoration or requiring intentionally restarted workloads to be stopped.
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
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import runtime_resource_path
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_gateway_authority import _grants, subject_review_namespaces
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_role_fencing import qualify_pool_reader_rules
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings, WriterOptions
from scripts.ops.nebius_shared_pool_material import validate_shared_staging_material
from scripts.ops.nebius_shared_pool_operation import RECOVERY_PHASES, _files
from scripts.ops.nebius_shared_pool_recovery_gateway import HTTPSSharedRecoveryGateway
from scripts.ops.nebius_shared_pool_recovery_templates import (
    HTTPSSharedRecoveryTemplates,
    validate_shared_recovery_templates,
)
from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER
from scripts.ops.nebius_shared_pool_roles import shared_forward_role_options
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest

_PHASE = 'recovery-restore-roles'
_INDEX = RECOVERY_PHASES.index(_PHASE)
Documents = dict[str, dict[str, Any]]


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or frame['recovery'][_PHASE]['status'] != status
            or any(frame['recovery'][prior]['status'] != 'complete' for prior in RECOVERY_PHASES[:_INDEX])
            or any(frame['recovery'][later]['status'] != 'prepared' for later in RECOVERY_PHASES[_INDEX + 1:])):
        raise ValueError
    return frame


def _targets(bindings: SharedPoolGuardBindings) -> Documents:
    """Pure captured-current Role targets, without live or history authority."""
    targets = {}
    for key, original in bindings.plan.staging_role_originals.items():
        target = _snapshot(original)
        if MARKER in target['metadata'].get('annotations', {}):
            raise ValueError
        target['metadata'].setdefault('annotations', {})[MARKER] = str(bindings.migration.registration.spec.operation_id)
        targets[key] = target
    return targets


def _inputs(bindings: SharedPoolGuardBindings, state: Path) -> tuple[WriterOptions, Documents, WriterOptions]:
    frame = _frame(bindings, state)
    if frame['recovery']['recovery-restore-workloads']['status'] != 'complete':
        raise ValueError
    validate_shared_recovery_templates(bindings=bindings, state_dir=state / 'recovery-restore-workloads', complete=True)
    sources: WriterOptions = dict(shared_forward_role_options(bindings=bindings, state_dir=state))
    targets = _targets(bindings)
    # A cancelled forward material stage may have installed only part of its
    # read-only catalog. Planned names are not proof of installed authority.
    readers: WriterOptions = dict.fromkeys(bindings.plan.staging_reader_authority, (None,))
    material = state / 'forward-material'
    status = frame['forward']['forward-material']['status']
    path = material / 'material.json'
    if status != 'prepared' or path.exists() or path.is_symlink():
        child = validate_shared_staging_material(bindings=bindings, state_dir=material,
            complete=status == 'complete', prepared=status == 'prepared')
        if child['local']['dispatch']:
            for key in readers:
                row = child['stage']['resources'][key]
                if row['status'] == 'create_intent':
                    readers[key] = (None, copy.deepcopy(row['expected']))
                elif row['status'] == 'created':
                    value = copy.deepcopy(row['observed'])
                    value['metadata']['uid'] = row['uid']
                    readers[key] = (value,)
    if _frame(bindings, state) != frame:
        raise ValueError
    return sources, targets, readers


def _catalog(bindings: SharedPoolGuardBindings) -> Documents:
    return {_key(row): copy.deepcopy(row) for row in bindings.plan.staging.attachment.resources
        if row['kind'] in {'Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding'}}


def _identity(bindings: SharedPoolGuardBindings, state: Path, sources: WriterOptions,
        targets: Documents, readers: WriterOptions) -> dict[str, str]:
    if state.name != _PHASE:
        raise ValueError
    return {'schema': 'loom.nebius-shared-recovery-roles.v1', 'input_digest': bindings.input_digest,
        'state_dir': str(state), 'sources_sha256': digest(sources), 'targets_sha256': digest(targets),
        'readers_sha256': digest(readers), 'catalog_sha256': digest(_catalog(bindings))}


def _prepared() -> dict[str, Any]:
    return {'status': 'prepared', 'before_resource_version': None}


def _validate_record(record: dict[str, Any], *, identity: dict[str, str], targets: Documents,
        complete: bool, prepared: bool = False) -> None:
    if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(record) != {*identity, 'roles'} or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['roles'], dict) or record['roles'].keys() != targets.keys()):
        raise ValueError
    unfinished = False
    for key in sorted(targets):
        row = record['roles'][key]
        if (not isinstance(row, dict) or set(row) != set(_prepared()) or row['status'] not in {'prepared', 'intent', 'restored'}
                or (complete and row['status'] != 'restored') or ((prepared or unfinished) and row != _prepared())):
            raise ValueError
        version = row['before_resource_version']
        if row['status'] == 'prepared':
            if version is not None:
                raise ValueError
        elif not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        unfinished |= row['status'] != 'restored'


def validate_shared_recovery_roles(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    sources, targets, readers = _inputs(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir, sources, targets, readers)
    record = _read(state_dir / 'roles.json')
    _validate_record(record, identity=identity, targets=targets, complete=complete, prepared=prepared)
    if ((prepared and frame['recovery'][_PHASE]['status'] != 'prepared')
            or _frame(bindings, state_dir.parent) != frame):
        raise ValueError
    return record


def prepare_shared_recovery_roles(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    sources, targets, readers = _inputs(bindings, state_dir.parent)
    with private_state._locked_state(state_dir):
        path = state_dir / 'roles.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings, state_dir, sources, targets, readers),
                'roles': {key: _prepared() for key in sorted(targets)}})
        validate_shared_recovery_roles(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def _qualify_rules(review: dict[str, Any], *, catalog: Documents, subject: tuple[str, str], namespace: str) -> None:
    """This retained account's actual fixed bindings, never all writers' union."""
    required_rules = []
    for binding in catalog.values():
        if binding['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
            continue
        if binding['kind'] == 'RoleBinding' and binding['metadata']['namespace'] != namespace:
            continue
        if not any(row['kind'] == 'ServiceAccount' and (row['namespace'], row['name']) == subject
                for row in binding.get('subjects', [])):
            continue
        reference = binding['roleRef']
        key = reference['kind'] + ':' + (binding['metadata']['namespace'] if reference['kind'] == 'Role' else '-') + ':' + reference['name']
        required_rules.extend(catalog[key]['rules'])
    required, observed = _grants(required_rules), _grants(review['status']['resourceRules'])
    if not required <= observed:
        raise ValueError('shared Role recovery required rights missing')
    common = copy.deepcopy(review)
    common['status']['resourceRules'] = [
        {'apiGroups': [group], 'resources': [resource], 'verbs': [verb],
            **({'resourceNames': [name]} if name is not None else {})}
        for group, resource, verb, name in observed - required]
    qualify_pool_reader_rules(common, namespace=namespace)


class HTTPSSharedRecoveryRoles(ManagementKubernetesTransport):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            templates: HTTPSSharedRecoveryTemplates | None, gateway: HTTPSSharedRecoveryGateway,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        frame = _frame(self.bindings, self.state.parent)
        self.sources, self.targets, self.readers = _inputs(self.bindings, self.state.parent)
        if _frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared Role recovery dependencies changed')
        self.identity = _identity(self.bindings, self.state, self.sources, self.targets, self.readers)
        self.dependencies = self._dependencies(frame)
        self.templates, self.gateway, self.qualify_runtime = templates, gateway, qualify_runtime
        self.private_files = dict(private_files or {})
        if (not callable(qualify_runtime)
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared Role recovery connection differs')
        self._scope()
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    @staticmethod
    def _dependencies(frame: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy({'forward': frame['forward'],
            'recovery': {phase: frame['recovery'][phase] for phase in RECOVERY_PHASES[:_INDEX]}})

    def _scope(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
        frame = _frame(self.bindings, self.state.parent)
        status = frame['recovery'][_PHASE]['status']
        if (frame['outcome'] != 'pending' or status not in {'started', 'complete'}
                or self._dependencies(frame) != self.dependencies
                or _identity(self.bindings, self.state, self.sources, self.targets, self.readers) != self.identity
                or (self.templates is None and status != 'complete')
                or (self.templates is not None and (self.templates.bindings != self.bindings
                    or self.templates.state != self.state.parent / 'recovery-restore-workloads'))
                or self.gateway.bindings != self.bindings or self.gateway.state != self.state.parent / 'recovery-revoke'):
            raise ValueError('shared Role recovery scope differs')
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=min(len(raw), 4 * 1024**2)) != raw for path, raw in files):
            raise ValueError('shared Role recovery private inputs changed')
        receipts = _files(self.state)
        record = _read(self.state / 'roles.json')
        _validate_record(record, identity=self.identity, targets=self.targets, complete=status == 'complete')
        if _files(self.state) != receipts or _frame(self.bindings, self.state.parent) != frame:
            raise ValueError('shared Role recovery receipts changed')
        return frame, record, receipts

    def _namespaces(self) -> None:
        migration = self.bindings.migration
        identity = migration.registration.binding
        namespaces = {'kube-system': identity.kube_system_uid, identity.namespace: identity.namespace_uid,
            **{guard.namespace: str(guard.namespace_uid) for guard in migration.guards},
            **{ns.name: str(ns.uid) for participant in migration.registration.spec.participants
                for ns in (participant.execution_namespace, participant.build_namespace)}}
        for name, uid in namespaces.items():
            value = self._request('GET', '/api/v1/namespaces/' + name)
            if (value is None or value.get('apiVersion') != 'v1' or value.get('kind') != 'Namespace'
                    or value['metadata'].get('name') != name or _uid(value) != uid):
                raise ValueError('shared Role recovery namespace changed')
            _snapshot(value)

    def _path(self, key: str) -> str:
        document = self.bindings.plan.staging_role_originals[key]
        return runtime_resource_path(document) + '/' + str(document['metadata']['name'])

    def _bindings(self) -> Documents:
        revision: str | None = None

        def read(method: str, path: str) -> dict[str, Any] | None:
            nonlocal revision
            if revision is not None and 'continue=' not in path:
                path += '&' + urlencode({'resourceVersion': revision, 'resourceVersionMatch': 'Exact'})
            page = self._request(method, path)
            current = None if page is None else page.get('metadata', {}).get('resourceVersion')
            if not isinstance(current, str) or not 0 < len(current) <= 1024 or revision not in (None, current):
                raise ValueError('shared Role recovery binding revision differs')
            revision = current
            return page

        rows = [row for resource, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
            for row in inventory_resources(read, 'rbac.authorization.k8s.io/v1', resource, kind)]
        values = {_key(row): row for row in rows}
        if len(values) != len(rows) or len({_uid(row) for row in rows}) != len(rows):
            raise ValueError('shared Role recovery binding identity differs')
        catalog = _catalog(self.bindings)
        for key, row in values.items():
            reference = row['roleRef']
            if reference['kind'] == 'Role':
                target = 'Role:' + str(row['metadata']['namespace']) + ':' + str(reference['name'])
                if target in self.targets and (key not in catalog or not _matches(row, catalog[key], _uid(catalog[key]))):
                    raise ValueError('shared Role recovery foreign inbound grant')
        return values

    def _authority(self) -> tuple[Documents, Documents]:
        before = self._scope()
        observed = self._bindings()
        options: WriterOptions = {key: (value,) for key, value in _catalog(self.bindings).items()}
        options.update(self.readers)
        for key, choices in self.sources.items():
            status = before[1]['roles'][key]['status']
            options[key] = choices if status == 'prepared' else (self.targets[key],) if status == 'restored' else (*choices, self.targets[key])
        catalog = {}
        for key, choices in options.items():
            document = next((row for row in choices if row is not None), self.bindings.plan.staging_reader_authority.get(key))
            if document is None:
                raise ValueError
            current = (observed.get(key) if document['kind'].endswith('Binding') else
                self._request('GET', runtime_resource_path(document) + '/' + str(document['metadata']['name'])))
            if current is None:
                if None not in choices:
                    raise ValueError('shared Role recovery authority disappeared')
                continue
            original = self.bindings.plan.staging_role_originals.get(key)
            uid = _uid(original) if original is not None else _uid(current)
            if not any(row is not None and _matches(current, row, row['metadata'].get('uid', uid)) for row in choices):
                raise ValueError('shared Role recovery authority differs')
            row = before[1]['roles'].get(key)
            if (row is not None and row['status'] == 'restored'
                    and current['metadata']['resourceVersion'] == row['before_resource_version']):
                raise ValueError('shared Role recovery version barrier missing')
            catalog[key] = current
        if self._scope() != before:
            raise ValueError('shared Role recovery authority changed during observation')
        return catalog, observed

    def verify_permissions(self) -> None:
        before = self._scope()
        self._namespaces()
        authority = self._authority()
        subjects = set()
        for value in self.bindings.plan.staging.originals.values():
            pod = (value['spec']['jobTemplate']['spec']['template']['spec'] if value['kind'] == 'CronJob'
                else value['spec']['template']['spec'])
            subjects.add((value['metadata']['namespace'], pod.get('serviceAccountName', 'default')))
        for subject in sorted(subjects):
            account_namespace, account = subject
            headers = [('Impersonate-User', f'system:serviceaccount:{account_namespace}:{account}'),
                *(('Impersonate-Group', group) for group in (
                    'system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated'))]
            for namespace in subject_review_namespaces(self.bindings.migration, list(authority[1].values()), subject=subject):
                with self.client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews', headers=headers,
                        json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': namespace}}) as response:
                    if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                        raise ValueError('shared Role recovery rules review unavailable')
                    raw = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        if len(raw) + len(chunk) > 4 * 1024**2:
                            raise ValueError('shared Role recovery rules review exceeds bound')
                        raw.extend(chunk)
                    _qualify_rules(json.loads(raw), catalog=authority[0], subject=subject, namespace=namespace)
        self._namespaces()
        if self._authority() != authority or self._scope() != before:
            raise ValueError('shared Role recovery permissions changed during review')

    def _ready(self, *, writing: bool = False) -> bool:
        before = self._scope()
        _active(self.bindings, self.state, 'started')
        if self.templates is None:
            raise ValueError('shared Role recovery mutation requires the active template transport')
        self.qualify_runtime(copy.deepcopy(self.request), _PHASE, writing)
        self.gateway.verify_permissions()
        drained = self.templates.drained()
        if type(drained) is not bool:
            raise ValueError('shared Role recovery drain unqualified')
        self.verify_permissions()
        self.qualify_runtime(copy.deepcopy(self.request), _PHASE, writing)
        # Whole-parent qualification can take time. Do not carry an earlier
        # drain/denial observation across it into the real dispatch intent.
        self.gateway.verify_permissions()
        current_drained = self.templates.drained()
        if type(current_drained) is not bool:
            raise ValueError('shared Role recovery final drain unqualified')
        if self._scope() != before:
            raise ValueError('shared Role recovery qualification changed')
        return drained and current_drained

    def restore_role(self, key: str, *, record_intent: Callable[[dict[str, Any]], None]) -> bool:
        _active(self.bindings, self.state, 'started')
        scope = self._scope()
        if scope[1]['roles'][key] != _prepared() or not callable(record_intent):
            raise ValueError('shared Role recovery has no prepared target')
        desired = self.targets[key]

        def patches(value: dict[str, Any]) -> list[dict[str, Any]]:
            version = value['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            changes = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(value)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/rules', 'value': value['rules']},
                {'op': 'test', 'path': '/metadata', 'value': value['metadata']}]
            for field in ('labels', 'annotations'):
                if field in desired['metadata']:
                    changes.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
                elif field in value['metadata']:
                    changes.append({'op': 'remove', 'path': '/metadata/' + field})
            return [*changes, {'op': 'replace', 'path': '/rules', 'value': desired['rules']}]

        if not self._ready(writing=True):
            return False
        current = self._authority()[0][key]
        with self.client.stream('PATCH', self._path(key) + '?dryRun=All', json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if not _patch_result(response, desired=desired, uid=_uid(current)):
                return False
        if not self._ready(writing=True):
            return False
        current = self._authority()[0][key]
        if self._scope() != scope:
            raise ValueError('shared Role recovery pre-dispatch scope changed')
        record_intent(current)
        selected = self._scope()
        if selected[1]['roles'][key] != {'status': 'intent', 'before_resource_version': current['metadata']['resourceVersion']}:
            raise ValueError('shared Role recovery intent differs')
        with self.client.stream('PATCH', self._path(key), json=patches(current),
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            accepted = _patch_result(response, desired=desired, uid=_uid(current))
        if self._scope() != selected:
            raise ValueError('shared Role recovery outcome unconfirmed')
        return accepted


def advance_shared_recovery_roles(*, bindings: SharedPoolGuardBindings,
        api: HTTPSSharedRecoveryRoles, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        frame = _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            record = validate_shared_recovery_roles(bindings=bindings, state_dir=state_dir, complete=False)

            def save() -> None:
                if _active(bindings, state_dir, 'started') != frame:
                    raise ValueError
                private_state._atomic_json(state_dir / 'roles.json', record)
                if api._scope()[1] != record:
                    raise ValueError

            if not api._ready():
                return False
            for key in sorted(api.targets):
                row = record['roles'][key]
                if row['status'] == 'prepared':
                    def intent(value: dict[str, Any], *, selected_key: str = key) -> None:
                        if record['roles'][selected_key] != _prepared() or not any(choice is not None
                                and _matches(value, choice, _uid(choice)) for choice in api.sources[selected_key]):
                            raise ValueError
                        record['roles'][selected_key].update(status='intent', before_resource_version=value['metadata']['resourceVersion'])
                        save()
                    try:
                        accepted = api.restore_role(key, record_intent=intent)
                    except Exception:
                        if row['status'] != 'intent':
                            raise
                        accepted = True  # Persisted unknown writes permit observation only.
                    if accepted is False:
                        row.update(_prepared())
                        save()
                        return False
                    if accepted is not True:
                        raise ValueError
                current = api._authority()[0][key]
                if not _matches(current, api.targets[key], _uid(bindings.plan.staging_role_originals[key])):
                    return False
                if current['metadata']['resourceVersion'] == row['before_resource_version']:
                    raise ValueError
                if row['status'] != 'restored':
                    row['status'] = 'restored'
                    save()
            if not api._ready():
                return False
            validate_shared_recovery_roles(bindings=bindings, state_dir=state_dir, complete=True)
            return True
    except Exception:
        raise ValueError('shared Role recovery unconfirmed; preserve retained evidence') from None
