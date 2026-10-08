"""Fixed fresh-runtime SQL delivery before any retained workload is changed.

The protected parent owns publication qualification and its phase-start anchor.
This stage deliberately requires the original completed foundation/manager and
closed pool. After successor patches, the parent must validate the completed
stage against its preserved receipt and phase-aware successor evidence, not
replay initial-installation qualification against changed workloads.
"""
from __future__ import annotations

import copy
import re
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_foundation import HTTPSRetainedDevelopmentFoundation
from scripts.ops.nebius_development_pool_registration_live import (
    HTTPSRetainedDevelopmentManagementAPI,
)
from scripts.ops.nebius_development_runtime_setup import (
    DevelopmentDatabaseRuntime,
    database_runtime_documents,
    validate_database_runtime_proof,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER, _validate_record

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


def _path(document: dict[str, Any]) -> str:
    kind = document['kind']
    if kind == 'Namespace' and document['apiVersion'] == 'v1':
        return '/api/v1/namespaces'
    version, plural = {'Secret': ('v1', 'secrets'), 'ConfigMap': ('v1', 'configmaps'),
        'ServiceAccount': ('v1', 'serviceaccounts'), 'Deployment': ('apps/v1', 'deployments'),
        'Job': ('batch/v1', 'jobs')}[kind]
    namespace = document['metadata']['namespace']
    if document['apiVersion'] != version or not isinstance(namespace, str):
        raise ValueError('development runtime database resource version differs')
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + '/namespaces/' + namespace + '/' + plural


class HTTPSDevelopmentRuntimeDatabaseAPI(HTTPSRetainedDevelopmentManagementAPI):
    """Only two fixed immutable Secrets and the fixed SQL ConfigMap/Job may POST."""

    def __init__(self, *, request: DevelopmentDatabaseRuntime, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None,
                 private_files: dict[Path, bytes] | None = None):
        documents = database_runtime_documents(request)
        self.runtime = copy.deepcopy(request)
        super().__init__(retained=request.manager.retained.request.retained, api_server=api_server,
            ssl_context=ssl_context, token=token, private_files=private_files)
        self.documents = documents
        self.foundation = HTTPSRetainedDevelopmentFoundation(api_server=api_server, ssl_context=ssl_context, token=token)

    def __exit__(self, *args: object) -> None:
        try:
            self.foundation.__exit__(*args)
        finally:
            super().__exit__(*args)

    def _private_inputs(self) -> None:
        # Retain every constraint: a current input path must never shadow a
        # different historical pin merely by appearing later in a merged map.
        files = (*self.runtime.manager.retained.files.items(), *self.runtime.foundation.files.items(),
            *self.private_files.items())
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files):
            raise ValueError('development runtime database inputs changed')

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            super().verify_identity(binding)
            self.foundation.verify_retained(reference=self.retained.inputs.prerequisites.foundation)
            for expected in self.runtime.manager.retained.resources.values():
                actual = self._request('GET', _path(expected) + '/' + expected['metadata']['name'])
                if actual is None or _uid(actual) != _uid(expected) or _snapshot(actual) != _snapshot(expected):
                    raise ValueError
            self._private_inputs()
        except Exception:
            raise ValueError('development runtime database prerequisites unqualified') from None

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired['metadata'].get('annotations', {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and 'annotations' not in expected['metadata']:
                desired['metadata'].pop('annotations', None)
            if desired != expected:
                raise ValueError
            return _path(expected)
        except Exception:
            raise ValueError('resource outside fixed development runtime database stage') from None

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        self.verify_identity(self.binding)
        record = _json(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': digest(self.documents), 'phase': 'development-runtime-database'}, self.documents)
        result = {}
        for key, item in record['resources'].items():
            actual = self.get_resource(item['desired'])
            if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid']
                    or _snapshot(actual) != item['observed']):
                raise ValueError('development runtime database staged resource differs')
            result[key] = actual
        self.verify_identity(self.binding)
        return result

    def database_report(self, state_dir: Path) -> dict[str, Any] | None:
        """GET-only proof of the fixed Job's sole unrestarted successful Pod."""
        try:
            resources = self._recorded(state_dir)
            job, = (row for row in resources.values() if row['kind'] == 'Job')
            status = job.get('status', {})
            conditions = {row['type']: row['status'] for row in status.get('conditions', [])}
            if conditions.get('Failed') == 'True':
                raise ValueError
            if conditions.get('Complete') != 'True':
                return None
            if any(type(status.get(key, 0)) is not int or status.get(key, 0) != count
                    for key, count in (('succeeded', 1), ('active', 0), ('failed', 0))):
                raise ValueError
            name, uid = job['metadata']['name'], _uid(job)
            base = '/api/v1/namespaces/loom-dev/pods'
            listing = self._request('GET', base + '?' + urlencode({
                'labelSelector': 'batch.kubernetes.io/controller-uid=' + uid, 'limit': 2}))
            if (listing is None or listing.get('apiVersion') != 'v1' or listing.get('kind') != 'PodList'
                    or listing.get('metadata', {}).get('continue') or len(listing.get('items', [])) != 1):
                raise ValueError
            pod = {'apiVersion': 'v1', 'kind': 'Pod', **listing['items'][0]}
            meta, pod_uid = pod['metadata'], _uid(pod)
            if (pod['apiVersion'] != 'v1' or pod['kind'] != 'Pod' or meta.get('namespace') != 'loom-dev'
                    or meta.get('deletionTimestamp') or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', meta['name'])):
                raise ValueError
            owners = meta.get('ownerReferences', [])
            if (len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                    'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': uid, 'controller': True}.items())):
                raise ValueError
            labels = {**job['spec']['template']['metadata'].get('labels', {}), 'batch.kubernetes.io/controller-uid': uid}
            region = 'topology.kubernetes.io/region'
            if isinstance(meta.get('labels'), dict) and region not in labels and region in meta['labels']:
                labels[region] = self.runtime.foundation.inputs.config['region']
            expected, actual = job['spec']['template']['spec'], pod['spec']
            if (meta.get('labels') != labels or not _matches_backup_template(actual, expected)
                    or actual.get('securityContext', {}) != expected.get('securityContext', {})
                    or actual.get('ephemeralContainers', []) != expected.get('ephemeralContainers', [])
                    or actual.get('serviceAccountName', 'default') != expected.get('serviceAccountName', 'default')
                    or any(actual.get(field, False) != expected.get(field, False)
                        for field in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'))
                    or pod.get('status', {}).get('phase') != 'Succeeded'):
                raise ValueError
            for field, status_field in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
                names = {row['name'] for row in expected.get(field, [])}
                states = pod['status'].get(status_field, [])
                if (len(states) != len(names) or {row['name'] for row in states} != names
                        or any(type(row.get('restartCount')) is not int or row['restartCount'] != 0
                            or type(row.get('state', {}).get('terminated', {}).get('exitCode')) is not int
                            or row['state']['terminated']['exitCode'] != 0 for row in states)):
                    raise ValueError
                for container, wanted in zip(actual.get(field, []), expected.get(field, []), strict=True):
                    if (container.keys() - wanted.keys() - {'imagePullPolicy', 'terminationMessagePath', 'terminationMessagePolicy'}
                            or container.get('securityContext', {}) != wanted.get('securityContext', {})):
                        raise ValueError
            path = base + '/' + meta['name']
            container, = expected['containers']
            query = urlencode({'container': container['name'], 'limitBytes': 16384, 'timestamps': 'false'})
            self._private_inputs()
            with self.client.stream('GET', path + '/log?' + query) as response:
                if response.status_code != 200 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=8192):
                    if len(content) + len(chunk) > 16384:
                        raise ValueError
                    content.extend(chunk)
            proof = {'job_uid': uid, 'pod_uid': pod_uid, 'database': _json(bytes(content))}
            validate_database_runtime_proof(self.runtime, state_dir, proof)
            if self._request('GET', path) != pod or self._recorded(state_dir) != resources:
                raise ValueError
            self.verify_identity(self.binding)
            return proof
        except Exception:
            raise ValueError('development runtime database execution unqualified; preserve evidence') from None
