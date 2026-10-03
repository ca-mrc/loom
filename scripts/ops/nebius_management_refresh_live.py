"""Bound HTTPS refresh cutover; no generic update, apply or credential fallback."""
from __future__ import annotations

import json
import ssl
from collections.abc import Callable
from typing import Any

from scripts.ops.nebius_application_setup import ApplicationSetupRequest, HTTPSApplicationSetupAPI
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_refresh_switch import (
    ManagementRefreshSwitchRequest,
    qualify_refresh_drain,
    refresh_initial,
    refresh_target,
)
from scripts.ops.nebius_management_stage import ManagementStageError
from scripts.ops.nebius_management_switch import _matches

from loom_service.environment_management.kubernetes_provider import _contains


class HTTPSManagementRefreshSwitchAPI(HTTPSApplicationSetupAPI):
    """Only one retained Deployment, scoped by explicit cluster/namespace UIDs."""

    def __init__(self, *, request: ManagementRefreshSwitchRequest, binding: ManagementBinding,
                 shared_namespace_uid: str, api_server: str, ssl_context: ssl.SSLContext,
                 activation_check: Callable[[ManagementRefreshSwitchRequest], bool], token: str | None = None,
                 before_write: Callable[[], None] | None = None):
        self.refresh = request
        self.check_activation = activation_check
        self.before_write = before_write
        refresh_target(request, 'activate')
        setup = ApplicationSetupRequest(request.render.after, request.render.candidate, request.render.profile,
            binding, shared_namespace_uid, request.render.repo_root)
        super().__init__(request=setup, phase='config', api_server=api_server, ssl_context=ssl_context, token=token)
        self.path = '/apis/apps/v1/namespaces/' + binding.namespace + '/deployments/loom-service'

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('resource outside fixed management refresh cutover')

    def read(self) -> dict[str, Any]:
        self.verify_identity(self.binding)
        value = self._request('GET', self.path)
        if (value is None or value.get('apiVersion') != 'apps/v1' or value.get('kind') != 'Deployment'
                or value['metadata'].get('namespace') != self.binding.namespace
                or value['metadata'].get('name') != 'loom-service'
                or _uid(value) != _uid(self.refresh.render.active)):
            raise ManagementStageError('management refresh Deployment identity differs')
        _snapshot(value)
        return value

    def _patch(self, before: dict[str, Any], action: str, operation_id: str, *, preview: bool) -> dict[str, Any] | None:
        try:
            if (operation_id != str(self.refresh.operation_id) or action not in {'retire', 'activate'}
                    or (preview and action != 'activate')):
                raise ValueError
            expected = refresh_initial(self.refresh) if action == 'retire' else refresh_target(self.refresh, 'retire')
            desired = refresh_target(self.refresh, 'activate' if action == 'activate' else 'retire')
            version = before['metadata']['resourceVersion']
            if (not _matches(before, expected, _uid(self.refresh.render.active))
                    or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            patches = [
                {'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']},
                {'op': 'replace', 'path': '/spec/replicas', 'value': desired['spec']['replicas']},
            ]
            if action == 'activate':
                patches.append({'op': 'replace', 'path': '/spec/template', 'value': desired['spec']['template']})
            self.verify_identity(self.binding)
            if not preview and self.before_write is not None:
                self.before_write()
            with self.client.stream('PATCH', self.path + ('?dryRun=All' if preview else ''), json=patches,
                                    headers={'Content-Type': 'application/json-patch+json'}) as response:
                if response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise ValueError
                raw = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(raw) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    raw.extend(chunk)
                observed = json.loads(raw)
                if not isinstance(observed, dict):
                    raise ValueError
                status = response.status_code
                if status in {409, 422}:
                    if (observed.get('apiVersion') == 'v1' and observed.get('kind') == 'Status'
                            and observed.get('status') == 'Failure' and observed.get('code') == status
                            and observed.get('reason') == ('Conflict' if status == 409 else 'Invalid')):
                        return None
                    raise ValueError
                if (status != 200 or observed.get('apiVersion') != 'apps/v1' or observed.get('kind') != 'Deployment'
                        or _uid(observed) != _uid(self.refresh.render.active)):
                    raise ValueError
                return observed
        except Exception:
            raise ManagementStageError('management refresh update outcome unavailable') from None

    def preview(self, before: dict[str, Any], operation_id: str) -> dict[str, Any]:
        observed = self._patch(before, 'activate', operation_id, preview=True)
        if observed is None:
            raise ManagementStageError('management refresh preview conflicted')
        return observed

    def patch(self, before: dict[str, Any], action: str, operation_id: str) -> bool:
        return self._patch(before, action, operation_id, preview=False) is not None

    def retired(self) -> bool:
        try:
            original = self.read()
            if original['spec']['replicas'] != 0:
                return False
            suffix = '/namespaces/' + self.binding.namespace
            query = '?limit=100&labelSelector=app%3Dloom-service'
            sets = self._request('GET', '/apis/apps/v1' + suffix + '/replicasets' + query)
            pods = self._request('GET', '/api/v1' + suffix + '/pods' + query)
            if sets is None or pods is None:
                raise ValueError
            if not qualify_refresh_drain(self.refresh, deployment=original, replicasets=sets, pods=pods):
                return False
            final = self.read()
            if final['metadata']['generation'] != original['metadata']['generation']:
                raise ValueError
            return qualify_refresh_drain(self.refresh, deployment=final, replicasets=sets, pods=pods)
        except Exception:
            raise ManagementStageError('management refresh drain observation unavailable') from None

    def activation_ready(self) -> bool:
        try:
            self.verify_identity(self.binding)
            return self.check_activation(self.refresh) is True
        except Exception:
            raise ManagementStageError('management refresh activation qualification unavailable') from None

    def workload_ready(self) -> bool:
        """Qualify the actual single candidate Pod, not only Deployment counters."""
        try:
            before = self.read()
            if not _matches(before, refresh_target(self.refresh, 'activate'), _uid(self.refresh.render.active)):
                raise ValueError

            def ready(controller: dict[str, Any], replicas: int) -> bool:
                generation = controller['metadata']['generation']
                status = controller.get('status', {})
                observed = status.get('observedGeneration', 0)
                values = [status.get(key, 0) for key in ('replicas', 'readyReplicas', 'availableReplicas')]
                if controller['kind'] == 'Deployment':
                    values.append(status.get('updatedReplicas', 0))
                extra = [status.get(key, 0) for key in ('unavailableReplicas', 'terminatingReplicas')]
                if (type(generation) is not int or generation < 1 or type(observed) is not int or observed < 0
                        or any(type(value) is not int or value < 0 for value in (*values, *extra))):
                    raise ValueError
                return observed >= generation and all(value == replicas for value in values) and not any(extra)

            if not ready(before, 1):
                return False
            namespace = self.binding.namespace
            query = '?limit=100&labelSelector=app%3Dloom-service'

            def collection(path: str, version: str, kind: str) -> list[dict[str, Any]]:
                listing = self._request('GET', path + query)
                if (listing is None or listing.get('apiVersion') != version or listing.get('kind') != kind + 'List'
                        or not isinstance(listing.get('items'), list) or len(listing['items']) > 100
                        or listing.get('metadata', {}).get('continue')
                        or not isinstance(listing.get('metadata', {}).get('resourceVersion'), str)
                        or not 0 < len(listing['metadata']['resourceVersion']) <= 128):
                    raise ValueError
                rows = [{'apiVersion': version, 'kind': kind, **row} for row in listing['items']]
                if any(row['kind'] != kind or row['apiVersion'] != version
                        or row['metadata'].get('namespace') != namespace for row in rows):
                    raise ValueError
                return rows

            sets = collection('/apis/apps/v1/namespaces/' + namespace + '/replicasets', 'apps/v1', 'ReplicaSet')
            pods = collection('/api/v1/namespaces/' + namespace + '/pods', 'v1', 'Pod')
            if len(pods) != 1 or pods[0]['metadata'].get('deletionTimestamp'):
                return False
            selected = []
            seen = set()
            for replica in sets:
                uid = _uid(replica)
                owners = replica['metadata'].get('ownerReferences', [])
                if (uid in seen or len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                        'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': 'loom-service',
                        'uid': _uid(before), 'controller': True}.items())):
                    raise ValueError
                seen.add(uid)
                count = replica['spec'].get('replicas', 1)
                if type(count) is not int or count < 0:
                    raise ValueError
                if count > 1 or replica['metadata'].get('deletionTimestamp') or not ready(replica, count):
                    return False
                if count == 1:
                    selected.append(replica)
            if len(selected) != 1:
                return False
            replica, pod = selected[0], pods[0]
            _uid(pod)
            owners = pod['metadata'].get('ownerReferences', [])
            template = before['spec']['template']
            pod_hash = replica['metadata']['labels']['pod-template-hash']
            labels = {**template['metadata']['labels'], 'pod-template-hash': pod_hash}
            if (not isinstance(pod_hash, str) or not pod_hash or len(pod_hash) > 63
                    or replica['spec']['selector'] != {'matchLabels': {
                        **before['spec']['selector']['matchLabels'], 'pod-template-hash': pod_hash}}
                    or not _contains(replica['spec']['template'], template)
                    or len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                        'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
                        'uid': _uid(replica), 'controller': True}.items())):
                raise ValueError
            region = 'topology.kubernetes.io/region'
            if region in pod['metadata'].get('labels', {}) and region not in labels:
                labels[region] = self.refresh.render.after.installation.foundation.platform_config['region']
            wanted, actual = template['spec'], pod['spec']
            if (pod['metadata'].get('labels') != labels
                    or not _contains(pod['metadata'].get('annotations', {}), template['metadata'].get('annotations', {}))
                    or not _matches_backup_template(actual, wanted)
                    or actual.get('securityContext', {}) != wanted.get('securityContext', {})
                    or actual.get('ephemeralContainers', []) != wanted.get('ephemeralContainers', [])
                    or any(actual.get(key, False) != wanted.get(key, False)
                        for key in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'))):
                raise ValueError
            status = pod.get('status', {})
            if status.get('phase') != 'Running' or not any(row.get('type') == 'Ready' and row.get('status') == 'True'
                    for row in status.get('conditions', [])):
                return False
            for field, status_field in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
                expected = wanted.get(field, [])
                for container, declared in zip(actual.get(field, []), expected, strict=True):
                    if (container.keys() - declared.keys() - {'imagePullPolicy', 'terminationMessagePath', 'terminationMessagePolicy'}
                            or container.get('securityContext', {}) != declared.get('securityContext', {})):
                        raise ValueError
                states = status.get(status_field, [])
                if len(states) != len(expected) or {row['name'] for row in states} != {row['name'] for row in expected}:
                    return False
                if field == 'initContainers':
                    if any(type(row.get('state', {}).get('terminated', {}).get('exitCode')) is not int
                            or row['state']['terminated']['exitCode'] != 0 for row in states):
                        raise ValueError
                elif any(row.get('ready') is not True or not isinstance(row.get('state', {}).get('running'), dict) for row in states):
                    return False
            final = self.read()
            if final['metadata']['generation'] != before['metadata']['generation'] or _snapshot(final) != _snapshot(before):
                raise ValueError
            return ready(final, 1)
        except Exception:
            raise ManagementStageError('management refresh workload observation unavailable') from None
