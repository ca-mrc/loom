"""Fixed retained management Deployment cutover, never a generic update engine.

The protected upgrade qualifies original bootstrap evidence and prerequisites.
This stage only retires the legacy process and installs the fixed new template;
public authentication and worker readiness remain the caller's final barrier.
"""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import ApplicationSetupRequest, HTTPSApplicationSetupAPI
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageError,
    _canonical_quantities,
    _qualified_defaulted,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import render_management

MARKER = 'loom.nebius/management-upgrade-id'


@dataclass(frozen=True, repr=False)
class ManagementSwitchRequest:
    setup: ApplicationSetupRequest
    original: dict[str, Any]


class ManagementSwitchAPI(Protocol):
    def read(self) -> dict[str, Any]: ...
    def preview(self, before: dict[str, Any], operation_id: str) -> dict[str, Any]: ...
    def patch(self, before: dict[str, Any], action: str, operation_id: str) -> bool:
        """False is a definite conflict; exceptions may have committed."""
        ...
    def retired(self) -> bool:
        """Current controller generation/replicas and complete Pod/RS observation."""
        ...


class HTTPSManagementSwitchAPI(HTTPSApplicationSetupAPI):
    """Explicit TLS/auth and only the fixed management Deployment mutation."""

    def __init__(self, *, request: ManagementSwitchRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.target, _ = _target(request)
        self.switch = request
        super().__init__(request=request.setup, phase='admission', api_server=api_server, ssl_context=ssl_context, token=token)
        self.path = '/apis/apps/v1/namespaces/' + self.binding.namespace + '/deployments/loom-service'

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('resource outside fixed management switch')

    def read(self) -> dict[str, Any]:
        self.verify_identity(self.binding)
        value = self._request('GET', self.path)
        if (value is None or value.get('apiVersion') != 'apps/v1' or value.get('kind') != 'Deployment'
                or value.get('metadata', {}).get('namespace') != self.binding.namespace
                or value['metadata'].get('name') != 'loom-service' or _uid(value) != _uid(self.switch.original)):
            raise ManagementStageError('management switch Deployment identity differs')
        _snapshot(value)
        return value

    def _patch(self, before: dict[str, Any], action: str, operation_id: str, *, preview: bool) -> dict[str, Any] | None:
        try:
            expected = (self.switch.original if action == 'retire'
                else _desired(self.switch.original, self.target, 'retire', operation_id))
            desired = _desired(self.switch.original, self.target, action, operation_id)
            version = before['metadata']['resourceVersion']
            if (not _matches(before, expected, _uid(self.switch.original))
                    or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ManagementStageError('management switch snapshot differs')
            annotations = {**before['metadata'].get('annotations', {}), MARKER: operation_id}
            patches = [
                {'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'add', 'path': '/metadata/annotations', 'value': annotations},
                {'op': 'replace', 'path': '/spec/replicas', 'value': desired['spec']['replicas']},
            ]
            if action == 'activate':
                patches.append({'op': 'replace', 'path': '/spec/template', 'value': desired['spec']['template']})
            self.verify_identity(self.binding)
            with self.client.stream('PATCH', self.path + ('?dryRun=All' if preview else ''), json=patches,
                                    headers={'Content-Type': 'application/json-patch+json'}) as response:
                if response.status_code == 409:
                    return None  # Definite precondition failure, not a lost outcome.
                if response.status_code not in {200, 422} or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise ValueError
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(raw) + len(chunk) > 4 * 1024 * 1024:
                        raise ValueError
                    raw.extend(chunk)
                observed = json.loads(raw)
                if response.status_code == 422:
                    # Kubernetes JSON Patch test failures are Invalid/422, not
                    # Conflict/409. Only a complete API rejection can clear the
                    # intent; truncated/proxy replies remain unknown outcomes.
                    if (isinstance(observed, dict) and observed.get('apiVersion') == 'v1'
                            and observed.get('kind') == 'Status' and observed.get('status') == 'Failure'
                            and observed.get('code') == 422 and observed.get('reason') == 'Invalid'):
                        return None
                    raise ValueError
                if (not isinstance(observed, dict) or observed.get('kind') != 'Deployment'
                        or _uid(observed) != _uid(self.switch.original)):
                    raise ValueError
                return observed
        except ManagementStageError:
            raise
        except Exception:
            raise ManagementStageError('management switch update outcome unavailable') from None

    def preview(self, before: dict[str, Any], operation_id: str) -> dict[str, Any]:
        observed = self._patch(before, 'activate', operation_id, preview=True)
        if observed is None:
            raise ManagementStageError('management switch preview conflicted')
        return observed

    def patch(self, before: dict[str, Any], action: str, operation_id: str) -> bool:
        return self._patch(before, action, operation_id, preview=False) is not None

    def _collection(self, kind: str) -> list[dict[str, Any]]:
        version, resource = ('v1', 'pods') if kind == 'Pod' else ('apps/v1', 'replicasets')
        path = ('/api/' if version == 'v1' else '/apis/') + version + '/namespaces/' + self.binding.namespace + '/' + resource
        page = self._request('GET', path + '?limit=100&labelSelector=app%3Dloom-service')
        if (page is None or page.get('kind') != kind + 'List' or page.get('apiVersion') != version
                or not page.get('metadata', {}).get('resourceVersion') or page['metadata'].get('continue')
                or not isinstance(page.get('items'), list) or len(page['items']) > 100
                or any(item.get('metadata', {}).get('namespace') != self.binding.namespace for item in page['items'])):
            raise ManagementStageError('management retirement inventory incomplete')
        return list(page['items'])

    def _legacy_fenced(self) -> bool:
        application = self.switch.setup.deployment.installation.applications
        assert application is not None
        pod = {'apiVersion': 'v1', 'kind': 'Pod', **copy.deepcopy(self.switch.original['spec']['template'])}
        pod['metadata'].update(name='loom-management-retirement-' + uuid4().hex, namespace=self.binding.namespace)
        with self.client.stream('POST', '/api/v1/namespaces/' + self.binding.namespace + '/pods?dryRun=All', json=pod) as response:
            if response.status_code == 201:
                return False
            if response.status_code != 403 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                raise ManagementStageError('legacy management admission unqualified')
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(raw) + len(chunk) > 65536:
                    raise ManagementStageError('legacy management admission response exceeds bound')
                raw.extend(chunk)
            status = json.loads(raw)
            if (status.get('kind') != 'Status' or status.get('reason') != 'Forbidden' or status.get('code') != 403
                    or application.authority.name + '-legacy-pods' not in status.get('message', '')
                    or 'legacy management process is retired' not in status.get('message', '')):
                raise ManagementStageError('legacy management admission denied by another boundary')
            return True

    def retired(self) -> bool:
        try:
            current = self.read()
            if current['spec']['replicas'] != 0:
                return False
            stopped = _desired(self.switch.original, self.target, 'retire', current['metadata']['annotations'][MARKER])
            if not _matches(current, stopped, _uid(self.switch.original)):
                raise ManagementStageError('management retirement template differs')
            status = current.get('status', {})
            if (status.get('observedGeneration', 0) < current['metadata']['generation']
                    or any(status.get(field, 0) != 0 for field in ('replicas', 'readyReplicas', 'availableReplicas', 'updatedReplicas'))):
                return False
            if self._collection('Pod'):
                return False  # A terminating Pod can still execute the legacy worker.
            for row in self._collection('ReplicaSet'):
                owners = row['metadata'].get('ownerReferences', [])
                owner = dict(owners[0]) if len(owners) == 1 else {}
                owner.pop('blockOwnerDeletion', None)
                if owner != {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                        'name': 'loom-service', 'uid': _uid(current), 'controller': True}:
                    raise ManagementStageError('management retirement found foreign controller')
                if row['spec'].get('replicas', 1) != 0 or row.get('status', {}).get('replicas', 0) != 0:
                    return False
            if not self._legacy_fenced():
                return False
            self.verify_identity(self.binding)
            return True
        except ManagementStageError:
            raise
        except Exception:
            raise ManagementStageError('management retirement observation unavailable') from None


def _stable(document: dict[str, Any]) -> dict[str, Any]:
    return _canonical_quantities(_snapshot(document), detached=True)


def _target(request: ManagementSwitchRequest) -> tuple[dict[str, Any], str]:
    setup, original = request.setup, request.original
    binding = setup.binding
    rendered = render_management(setup.deployment, candidate=setup.candidate, profile=setup.profile, repo_root=setup.repo_root)
    target = next(doc for doc in rendered.files['40-services.yaml'] if doc['kind'] == 'Deployment')
    if (setup.deployment.installation.applications is None
            or (str(setup.deployment.installation_id), setup.deployment.namespace) != (binding.installation_id, binding.namespace)
            or original['apiVersion'] != 'apps/v1' or original['kind'] != 'Deployment'
            or original['metadata']['namespace'] != binding.namespace or original['metadata']['name'] != 'loom-service'
            or original['metadata'].get('labels', {}).get('loom.nebius/management-installation') != binding.installation_id
            or MARKER in original['metadata'].get('annotations', {})
            or original['spec']['replicas'] != 1 or target['spec']['replicas'] != 1
            or original['spec']['selector'] != {'matchLabels': {'app': 'loom-service'}}
            or original['spec']['selector'] != target['spec']['selector']
            or original['spec']['template']['spec']['serviceAccountName'] != 'loom-management-provisioner'):
        raise ManagementStageError('management switch original or target differs')
    _uid(original)
    _snapshot(original)
    return target, rendered.revision


def _desired(original: dict[str, Any], target: dict[str, Any], action: str, operation_id: str) -> dict[str, Any]:
    if action not in {'retire', 'activate'} or str(UUID(operation_id)) != operation_id or not UUID(operation_id).int:
        raise ManagementStageError('invalid management switch action')
    desired = _snapshot(original)
    desired['metadata'].setdefault('annotations', {})[MARKER] = operation_id
    desired['spec']['replicas'] = 0 if action == 'retire' else 1
    if action == 'activate':
        desired['spec']['template'] = copy.deepcopy(target['spec']['template'])
    return desired


def _matches(actual: dict[str, Any], expected: dict[str, Any], uid: str) -> bool:
    return _uid(actual) == uid and _stable(actual) == _stable(expected)


def _switch(*, request: ManagementSwitchRequest, api: ManagementSwitchAPI, state_dir: Path, activate: bool) -> bool:
    try:
        target, revision = _target(request)
        identity: dict[str, Any] = {'schema': 'loom.nebius-management-switch.v1', 'binding': asdict(request.setup.binding),
            'shared_namespace_uid': request.setup.shared_namespace_uid, 'revision': revision,
            'original_uid': _uid(request.original), 'original_digest': digest(_stable(request.original)),
            'material_digest': digest(asdict(request.setup.material) if request.setup.material is not None else None)}
        with private_state._locked_state(state_dir):
            path = state_dir / 'switch.json'
            actual = api.read()
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path, limit=4 * 1024 * 1024))
                if (set(record) != {*identity, 'original', 'operation_id', 'phase', 'active'}
                        or any(record[key] != value for key, value in identity.items())
                        or record['phase'] not in {'prepared', 'retire_intent', 'stopped', 'activate_intent', 'active'}
                        or not _matches(record['original'], request.original, identity['original_uid'])):
                    raise ManagementStageError('management switch recovery binding differs')
            else:
                if activate or not _matches(actual, request.original, identity['original_uid']):
                    raise ManagementStageError('management switch needs retained original retirement')
                record = {**identity, 'original': actual, 'operation_id': str(uuid4()), 'phase': 'prepared', 'active': None}
                private_state._atomic_json(path, record)
            operation_id = record['operation_id']
            stopped = _desired(record['original'], target, 'retire', operation_id)
            active_desired = _desired(record['original'], target, 'activate', operation_id)
            if record['active'] is not None:
                if _qualified_defaulted(active_desired, record['active']) != record['active']:
                    raise ManagementStageError('management switch frozen activation differs')
            if (record['phase'] in {'activate_intent', 'active'}) != (record['active'] is not None):
                raise ManagementStageError('management switch phase differs')

            def save(phase: str) -> None:
                record['phase'] = phase
                private_state._atomic_json(path, record)

            if activate:
                if record['phase'] not in {'stopped', 'activate_intent', 'active'}:
                    raise ManagementStageError('management switch retirement is unresolved')
                if record['phase'] == 'stopped':
                    if not _matches(actual, stopped, identity['original_uid']):
                        raise ManagementStageError('retained stopped management differs')
                    if not api.retired():
                        return False
                    record['active'] = _qualified_defaulted(active_desired, api.preview(actual, operation_id))
                    save('activate_intent')
                    try:
                        accepted = api.patch(actual, 'activate', operation_id)
                    except Exception:
                        accepted = True  # Intent remains; read back, never resend.
                    if not accepted:
                        record['active'] = None
                        save('stopped')
                        return False
                    actual = api.read()
                if not _matches(actual, record['active'], identity['original_uid']):
                    raise ManagementStageError('management activation unresolved; preserve intent')
                if record['phase'] != 'active':
                    save('active')
                return True
            if record['phase'] in {'activate_intent', 'active'}:
                raise ManagementStageError('management activation started; retirement cannot repeat')
            if record['phase'] == 'prepared':
                if not _matches(actual, record['original'], identity['original_uid']):
                    raise ManagementStageError('management original changed before retirement')
                save('retire_intent')
                try:
                    accepted = api.patch(actual, 'retire', operation_id)
                except Exception:
                    accepted = True
                if not accepted:
                    save('prepared')
                    return False
                actual = api.read()
            if not _matches(actual, stopped, identity['original_uid']):
                raise ManagementStageError('management retirement unresolved; preserve intent')
            if record['phase'] != 'stopped':
                save('stopped')
            return api.retired()
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError('management switch unavailable; preserve recovery evidence') from None


def retire_management(*, request: ManagementSwitchRequest, api: ManagementSwitchAPI, state_dir: Path) -> bool:
    return _switch(request=request, api=api, state_dir=state_dir, activate=False)


def activate_management(*, request: ManagementSwitchRequest, api: ManagementSwitchAPI, state_dir: Path) -> bool:
    return _switch(request=request, api=api, state_dir=state_dir, activate=True)
