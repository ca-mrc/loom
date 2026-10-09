"""Pure live rollout inspection; no credentials, mutations or admission claims."""
from __future__ import annotations

import copy
from typing import Any

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_stage import _qualified_defaulted
from scripts.ops.nebius_pool_migration_guard import _runtime_pod_spec

from loom_service.environment_management.kubernetes_provider import _contains


def qualify_started_deployment(*, current: dict[str, Any], children: dict[str, Any],
                               pods: dict[str, Any], region: str) -> bool:
    """One current owned Pod, no lingering old Pods, and complete namespace lists.

    The caller binds the Deployment to its retained UID/template before and after
    this observation. Process configuration and database probes are separate
    barriers; successful controller counters alone never establish readiness.
    """
    try:
        _snapshot(current)
        _uid(current)
        if (current['apiVersion'] != 'apps/v1' or current['kind'] != 'Deployment'
                or type(current['spec']['replicas']) is not int or current['spec']['replicas'] != 1
                or set(current['spec']['selector']) != {'matchLabels'}
                or not current['spec']['selector']['matchLabels']):
            raise ValueError
        namespace = current['metadata']['namespace']
        labels = current['spec']['selector']['matchLabels']

        def collection(document: dict[str, Any], kind: str, version: str) -> list[dict[str, Any]]:
            revision = document['metadata']['resourceVersion']
            if (document['apiVersion'] != version or document['kind'] != kind + 'List'
                    or not isinstance(revision, str) or not 0 < len(revision) <= 128
                    or document['metadata'].get('continue') or not isinstance(document['items'], list)
                    or len(document['items']) > 1000):
                raise ValueError
            rows = [{'apiVersion': version, 'kind': kind, **row} for row in document['items']]
            if (any(row['apiVersion'] != version or row['kind'] != kind or row['metadata']['namespace'] != namespace for row in rows)
                    or len({_uid(row) for row in rows}) != len(rows)):
                raise ValueError
            return rows

        def owned(row: dict[str, Any], parent: dict[str, Any]) -> bool:
            owners = row['metadata'].get('ownerReferences', [])
            if not any(owner.get('uid') == _uid(parent) for owner in owners):
                return False
            if len(owners) != 1:
                raise ValueError
            owner = dict(owners[0])
            blocking = owner.pop('blockOwnerDeletion', False)
            if (type(blocking) is not bool or owner != {'apiVersion': parent['apiVersion'], 'kind': parent['kind'],
                    'name': parent['metadata']['name'], 'uid': _uid(parent), 'controller': True}):
                raise ValueError
            return True

        def labelled(row: dict[str, Any]) -> bool:
            return all(row['metadata'].get('labels', {}).get(key) == value for key, value in labels.items())

        def ready(row: dict[str, Any], count: int) -> bool:
            generation, status = row['metadata']['generation'], row.get('status', {})
            observed = status.get('observedGeneration', 0)
            values = [row['spec'].get('replicas', 1), *(status.get(field, 0) for field in (
                'replicas', 'readyReplicas', 'availableReplicas'))]
            if row['kind'] == 'Deployment':
                values.append(status.get('updatedReplicas', 0))
            extra = [status.get(field, 0) for field in ('unavailableReplicas', 'terminatingReplicas')]
            if (type(generation) is not int or generation < 1 or type(observed) is not int or observed < 0
                    or any(type(value) is not int or value < 0 for value in (*values, *extra))):
                raise ValueError
            return observed >= generation and all(value == count for value in values) and not any(extra)

        replicas = collection(children, 'ReplicaSet', 'apps/v1')
        workload_pods = collection(pods, 'Pod', 'v1')
        selected = [row for row in replicas if owned(row, current)]
        if any(labelled(row) and row not in selected for row in replicas):
            raise ValueError
        selected_pods = [row for row in workload_pods if labelled(row) or any(owned(row, parent) for parent in selected)]
        if not ready(current, 1) or len(selected_pods) != 1:
            return False
        active = []
        for replica in selected:
            count = replica['spec'].get('replicas', 1)
            if type(count) is not int or count < 0:
                raise ValueError
            if count > 1 or replica['metadata'].get('deletionTimestamp') or not ready(replica, count):
                return False
            if count == 1:
                active.append(replica)
        if len(active) != 1:
            return False
        replica, pod = active[0], selected_pods[0]
        if not owned(pod, replica):
            raise ValueError
        if pod['metadata'].get('deletionTimestamp'):
            return False
        template = current['spec']['template']
        pod_hash = replica['metadata']['labels']['pod-template-hash']
        expected_labels = {**template['metadata']['labels'], 'pod-template-hash': pod_hash}
        if (not isinstance(pod_hash, str) or not 0 < len(pod_hash) <= 63
                or replica['spec']['selector'] != {'matchLabels': {**labels, 'pod-template-hash': pod_hash}}
                or not _contains(replica['spec']['template']['metadata'], template['metadata'])):
            raise ValueError
        region_key = 'topology.kubernetes.io/region'
        if region_key in pod['metadata'].get('labels', {}) and region_key not in expected_labels:
            expected_labels[region_key] = region
        if (pod['metadata'].get('labels') != expected_labels
                or not _contains(pod['metadata'].get('annotations', {}), template['metadata'].get('annotations', {}))):
            raise ValueError
        wanted = template['spec']
        actual = _runtime_pod_spec(pod['spec'], wanted)
        for observed in (replica['spec']['template']['spec'], actual):
            if (not _matches_backup_template(observed, wanted)
                    or observed.get('serviceAccountName', 'default') != wanted.get('serviceAccountName', 'default')):
                raise ValueError
            comparable = copy.deepcopy(observed)
            if observed is actual:
                # The scheduler binds Pods to a node, not controller templates.
                # _matches_backup_template already checked exact requested and
                # standard NoExecute tolerations; don't check them a second
                # time as if they were controller admission mutations.
                if 'nodeName' not in wanted:
                    comparable.pop('nodeName', None)
                comparable['tolerations'] = copy.deepcopy(wanted.get('tolerations', []))
            # Reuse container/default/security qualification, including sidecar
            # rejection; Pod-only scheduler/token defaults are handled above.
            _qualified_defaulted(
                {'kind': 'Deployment', 'metadata': {}, 'spec': {'template': {'spec': wanted}}},
                {'kind': 'Deployment', 'metadata': {}, 'spec': {'template': {'spec': comparable}}})
        status = pod.get('status', {})
        if (status.get('phase') != 'Running'
                or not any(row.get('type') == 'Ready' and row.get('status') == 'True' for row in status.get('conditions', []))):
            return False
        for field, status_field in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
            names = {row['name'] for row in wanted.get(field, [])}
            states = status.get(status_field, [])
            if len(states) != len(names) or {row['name'] for row in states} != names:
                return False
            if field == 'initContainers':
                if any(type(row.get('state', {}).get('terminated', {}).get('exitCode')) is not int
                        or row['state']['terminated']['exitCode'] != 0 for row in states):
                    return False
            elif any(row.get('ready') is not True or not isinstance(row.get('state', {}).get('running'), dict) for row in states):
                return False
        return True
    except Exception:
        raise ValueError('development runtime workload readiness unqualified') from None
