"""GET-only execution evidence shared by the fixed dev SQL and catalog stages.

The phase-aware parent supplies exact recorded resources and their live checks.
This reader has no create, retry, workload-start or admission authority.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Literal
from urllib.parse import urlencode

import httpx
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template

from loom_service.environment_management.candidates import _json


def read_runtime_job(*, client: httpx.Client, read: Callable[[str], dict[str, Any] | None],
        recorded: Callable[[], dict[str, dict[str, Any]]], private_inputs: Callable[[], None],
        region: str, report_field: Literal['database', 'catalog'],
        validate: Callable[[dict[str, Any]], None]) -> dict[str, Any] | None:
    """Prove one unrestarted completed Pod, then recheck every recorded resource.

    `recorded` must qualify live prerequisites and exact staged resource UIDs,
    not merely read local history. `validate` binds the bounded JSON receipt to
    the selected phase, request digest and Job UID. Neither can authorize writes.
    """
    try:
        resources = recorded()
        job, = (row for row in resources.values() if row['kind'] == 'Job')
        if (job.get('apiVersion') != 'batch/v1' or job['metadata'].get('namespace') != 'loom-dev'
                or job['metadata'].get('deletionTimestamp') or report_field not in {'database', 'catalog'}):
            raise ValueError
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
        listing = read(base + '?' + urlencode({'labelSelector': 'batch.kubernetes.io/controller-uid=' + uid, 'limit': 2}))
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
        region_key = 'topology.kubernetes.io/region'
        if isinstance(meta.get('labels'), dict) and region_key not in labels and region_key in meta['labels']:
            labels[region_key] = region
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
        private_inputs()
        with client.stream('GET', path + '/log?' + query) as response:
            if response.status_code != 200 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                raise ValueError
            content = bytearray()
            for chunk in response.iter_bytes(chunk_size=8192):
                if len(content) + len(chunk) > 16384:
                    raise ValueError
                content.extend(chunk)
        proof = {'job_uid': uid, 'pod_uid': pod_uid, report_field: _json(bytes(content))}
        validate(proof)
        if read(path) != pod or recorded() != resources:
            raise ValueError
        return proof
    except Exception:
        raise ValueError('development runtime Job evidence unqualified; preserve evidence') from None
