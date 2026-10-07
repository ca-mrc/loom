"""Read-only qualification of the fixed refresh probes' actual Kubernetes Pods."""
from __future__ import annotations

import copy
import json
import re
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_refresh_resources import (
    HTTPSManagementRefreshResourcesAPI,
    ManagementRefreshResourcesRequest,
    _revision,
)
from scripts.ops.nebius_management_stage import ManagementStageError, _validate_record

from loom.nebius_management_refresh_probe import SCHEMA, RefreshProbeSettings
from loom_service.environment_management.candidates import _json


def _exact_json(value: Any) -> str:
    # Python equality equates False with 0 and True with 1; evidence must not.
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _convergence_identity(document: dict[str, Any]) -> str:
    """Only terminal accounting/API bookkeeping may settle between strict reads."""
    result = copy.deepcopy(document)
    metadata = result['metadata']
    if 'resourceVersion' in metadata and (not isinstance(metadata['resourceVersion'], str) or not metadata['resourceVersion']):
        raise ValueError
    if 'managedFields' in metadata and (not isinstance(metadata['managedFields'], list)
            or any(not isinstance(row, dict) for row in metadata['managedFields'])):
        raise ValueError
    for field in ('resourceVersion', 'managedFields'):
        metadata.pop(field, None)
    if result['kind'] == 'Pod':
        status = result.get('status', {})
        if 'resources' in status:
            resources = status.pop('resources')
            if (not isinstance(resources, dict) or resources.keys() - {'limits', 'requests'}
                    or any(not isinstance(values, dict)
                        or any(not isinstance(key, str) or not isinstance(value, str)
                            for key, value in values.items()) for values in resources.values())):
                raise ValueError
    return _exact_json(result)


class HTTPSManagementRefreshEvidenceAPI(HTTPSManagementRefreshResourcesAPI):
    def __init__(self, *, request: ManagementRefreshResourcesRequest, phase: str, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        if phase not in {'manager-probe', 'shared-probe', 'post-migration-probe'}:
            raise ManagementStageError('refresh phase has no database probe')
        super().__init__(request=request, phase=phase, api_server=api_server, ssl_context=ssl_context, token=token)
        self.request, self.phase = request, phase

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        record = json.loads(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': _revision(self.request, self.documents), 'phase': 'refresh-' + self.phase}
        _validate_record(record, identity, self.documents)
        result = {}
        for item in record['resources'].values():
            self.verify_identity(self.binding)
            actual = self.get_resource(item['desired'])
            if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid']
                    or _snapshot(actual) != item['observed']):
                raise ValueError
            result[actual['kind']] = actual
        return result

    def probe_report(self, state_dir: Path) -> dict[str, Any] | None:
        """Qualify exact evidence; bounded bookkeeping settling never replays writes."""
        try:
            resources = self._recorded(state_dir)
            job = resources['Job']
            settings = RefreshProbeSettings.model_validate_json(resources['ConfigMap']['data']['probe.json'])
            status = job.get('status', {})
            conditions = {row['type']: row['status'] for row in status.get('conditions', [])}
            if conditions.get('Failed') == 'True':
                raise ValueError
            if conditions.get('Complete') != 'True':
                return None
            if type(status.get('succeeded')) is not int or status['succeeded'] != 1 or status.get('active', 0) != 0:
                raise ValueError
            namespace, name, uid = (job['metadata'][key] for key in ('namespace', 'name', 'uid'))
            base = '/api/v1/namespaces/' + namespace + '/pods'
            listing = self._request('GET', base + '?' + urlencode({
                'labelSelector': 'batch.kubernetes.io/controller-uid=' + uid, 'limit': 2}))
            if (listing is None or listing.get('apiVersion') != 'v1' or listing.get('kind') != 'PodList'
                    or listing.get('metadata', {}).get('continue') or len(listing.get('items', [])) != 1):
                raise ValueError
            pod = {'apiVersion': 'v1', 'kind': 'Pod', **listing['items'][0]}
            pod_uid = _uid(pod)
            meta = pod['metadata']
            if (pod['apiVersion'] != 'v1' or pod['kind'] != 'Pod' or meta.get('namespace') != namespace
                    or meta.get('deletionTimestamp') or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', meta['name'])):
                raise ValueError
            owners = meta.get('ownerReferences', [])
            if (len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                    'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': uid, 'controller': True}.items())):
                raise ValueError
            labels = {**job['spec']['template']['metadata'].get('labels', {}), 'batch.kubernetes.io/controller-uid': uid}
            region_label = 'topology.kubernetes.io/region'
            if isinstance(meta.get('labels'), dict) and region_label not in labels and region_label in meta['labels']:
                labels[region_label] = self.request.switch.render.after.installation.foundation.platform_config['region']
            if meta.get('labels') != labels:
                raise ValueError
            expected, actual = job['spec']['template']['spec'], pod['spec']
            if (not _matches_backup_template(actual, expected)
                    or actual.get('securityContext', {}) != expected.get('securityContext', {})
                    or actual.get('ephemeralContainers', []) != expected.get('ephemeralContainers', [])
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
            query = urlencode({'container': expected['containers'][0]['name'], 'limitBytes': 16384, 'timestamps': 'false'})

            def read_report() -> Any:
                with self.client.stream('GET', path + '/log?' + query) as response:
                    if response.status_code != 200 or response.headers.get('content-encoding', 'identity') != 'identity':
                        raise ValueError
                    content = bytearray()
                    for chunk in response.iter_bytes(chunk_size=8192):
                        if len(content) + len(chunk) > 16384:
                            raise ValueError
                        content.extend(chunk)
                return _json(bytes(content))

            report = read_report()
            if (not isinstance(report, dict)
                    or set(report) != {'schema', 'status', 'mode', 'revision', 'operations_checked'}
                    or report['schema'] != SCHEMA or report['status'] != 'qualified'
                    or report['mode'] != settings.mode or report['revision'] != settings.expected_revision
                    or type(report['operations_checked']) is not int or not 0 <= report['operations_checked'] <= 4096
                    or (settings.mode == 'shared' and report['operations_checked'] != 0)):
                raise ValueError
            original_pod = _convergence_identity(pod)
            original_resources = {kind: _convergence_identity(value) for kind, value in resources.items()}
            for attempt in range(3):
                observed_pod = self._request('GET', path)
                observed_resources = self._recorded(state_dir)
                self.verify_identity(self.binding)
                if _exact_json(observed_pod) == _exact_json(pod) and _exact_json(observed_resources) == _exact_json(resources):
                    return {'job_uid': uid, 'pod_uid': pod_uid, 'probe': report}
                if (observed_pod is None or _convergence_identity(observed_pod) != original_pod
                        or {kind: _convergence_identity(value) for kind, value in observed_resources.items()}
                        != original_resources):
                    raise ValueError
                # Do not retry a failed proof or normalize the accepted snapshots.
                # Only these known non-authoritative fields may change, and the
                # next full read must agree. The original report stays immutable.
                if attempt == 2 or _exact_json(read_report()) != _exact_json(report):
                    raise ValueError
                pod, resources = observed_pod, observed_resources
            raise ValueError
        except Exception:
            raise ManagementStageError('management refresh probe execution unqualified; preserve evidence') from None
