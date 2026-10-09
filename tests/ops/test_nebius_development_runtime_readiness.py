"""Readiness requires exact owned live Pods, not only optimistic counters."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import pytest


@pytest.fixture
def running():
    labels = {'app': 'loom-control-plane'}
    template = {'metadata': {'labels': labels}, 'spec': {'serviceAccountName': 'loom-control-plane',
        'automountServiceAccountToken': True, 'containers': [{'name': 'control-plane',
            'image': 'example/loom@sha256:' + 'a' * 64,
            'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}}]}}
    controller = {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'loom-control-plane',
        'namespace': 'loom-dev', 'uid': str(uuid4()), 'generation': 3, 'resourceVersion': '100'},
        'spec': {'replicas': 1, 'selector': {'matchLabels': labels}, 'template': template},
        'status': {'observedGeneration': 3, 'replicas': 1, 'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
    hashed = {**labels, 'pod-template-hash': 'b876c5b4d'}
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': 'loom-control-plane-b876c5b4d',
        'namespace': 'loom-dev', 'uid': str(uuid4()), 'generation': 1, 'resourceVersion': '101', 'labels': hashed,
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': 'loom-control-plane',
            'uid': controller['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': {'replicas': 1, 'selector': {'matchLabels': hashed}, 'template': copy.deepcopy(template)},
        'status': {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
    replica['spec']['template']['metadata']['labels'] = hashed
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'loom-control-plane-b876c5b4d-test',
        'namespace': 'loom-dev', 'uid': str(uuid4()), 'resourceVersion': '102', 'labels': hashed,
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
        'spec': copy.deepcopy(template['spec']), 'status': {'phase': 'Running',
            'conditions': [{'type': 'Ready', 'status': 'True'}],
            'containerStatuses': [{'name': 'control-plane', 'ready': True, 'restartCount': 0,
                'state': {'running': {'startedAt': '2026-10-08T23:00:00Z'}}}]}}
    collections = {'sets': {'apiVersion': 'apps/v1', 'kind': 'ReplicaSetList',
        'metadata': {'resourceVersion': '103'}, 'items': [replica]},
        'pods': {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '104'}, 'items': [pod]}}
    return controller, collections, replica, pod


def observe(running):
    name = 'scripts.ops.nebius_development_runtime_readiness'
    if importlib.util.find_spec(name) is None:
        pytest.fail('development runtime live workload readiness is missing')
    controller, collections, _, _ = running
    return importlib.import_module(name).qualify_started_deployment(
        current=controller, children=collections['sets'], pods=collections['pods'], region='eu-north1')


def test_runtime_readiness_proves_actual_owned_pod_and_all_replicasets(running):
    assert observe(running) is True
    # Other non-overlapping workloads do not block this deployment.
    _, collections, replica, pod = running
    for name, row in (('sets', replica), ('pods', pod)):
        foreign = copy.deepcopy(row)
        foreign['metadata'].update(uid=str(uuid4()), labels={'app': 'unrelated'})
        foreign['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
        collections[name]['items'].append(foreign)
    assert observe(running) is True


def test_runtime_readiness_accepts_scheduler_defaults_and_exact_service_account_projection(running):
    controller, _, replica, pod = running
    for row in (controller, replica):
        row['spec']['template']['spec']['tolerations'] = []
    pod['spec']['nodeName'] = 'development-platform-node'
    pod['spec']['tolerations'] = [{'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
        'effect': 'NoExecute', 'tolerationSeconds': 300} for key in ('not-ready', 'unreachable')]
    pod['spec']['volumes'] = [{'name': 'kube-api-access-abc12', 'projected': {'defaultMode': 420, 'sources': [
        {'serviceAccountToken': {'expirationSeconds': 3607, 'path': 'token'}},
        {'configMap': {'name': 'kube-root-ca.crt', 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}},
        {'downwardAPI': {'items': [{'path': 'namespace', 'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}]}},
    ]}}]
    pod['spec']['containers'][0]['volumeMounts'] = [{'name': 'kube-api-access-abc12', 'readOnly': True,
        'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount'}]
    assert observe(running) is True
    pod['spec']['volumes'][0]['projected']['sources'][1]['configMap']['name'] = 'foreign-trust'
    with pytest.raises(ValueError, match='development runtime workload readiness unqualified'):
        observe(running)


@pytest.mark.parametrize('damage', ['controller-lag', 'replica-lag', 'old-pod', 'terminating-pod', 'unready', 'pending'])
def test_runtime_readiness_waits_for_complete_current_rollout(running, damage):
    controller, collections, replica, pod = running
    if damage == 'controller-lag':
        controller['status']['observedGeneration'] = 2
    elif damage == 'replica-lag':
        replica['status']['observedGeneration'] = 0
    elif damage == 'old-pod':
        old = copy.deepcopy(pod)
        old['metadata']['uid'] = str(uuid4())
        collections['pods']['items'].append(old)
    elif damage == 'terminating-pod':
        pod['metadata']['deletionTimestamp'] = '2026-10-08T23:01:00Z'
    elif damage == 'unready':
        pod['status']['containerStatuses'][0]['ready'] = False
    else:
        pod['status']['phase'] = 'Pending'
    assert observe(running) is False


@pytest.mark.parametrize('damage', ['truncated', 'duplicate', 'foreign-namespace', 'owner', 'replica-template',
    'pod-image', 'pod-service-account', 'host-network', 'sidecar', 'container-security', 'foreign-toleration', 'boolean-count'])
def test_runtime_readiness_rejects_unqualified_live_topology(running, damage):
    controller, collections, replica, pod = running
    if damage == 'truncated':
        collections['pods']['metadata']['continue'] = 'another-page'
    elif damage == 'duplicate':
        collections['sets']['items'].append(copy.deepcopy(replica))
    elif damage == 'foreign-namespace':
        pod['metadata']['namespace'] = 'loom-staging'
    elif damage == 'owner':
        pod['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'replica-template':
        replica['spec']['template']['spec']['containers'][0]['image'] = 'other/image'
    elif damage == 'pod-image':
        pod['spec']['containers'][0]['image'] = 'other/image'
    elif damage == 'pod-service-account':
        pod['spec']['serviceAccountName'] = 'cluster-admin'
    elif damage == 'host-network':
        pod['spec']['hostNetwork'] = True
    elif damage == 'sidecar':
        pod['spec']['containers'].append({'name': 'injected', 'image': 'other/image'})
    elif damage == 'container-security':
        pod['spec']['containers'][0]['securityContext']['privileged'] = True
    elif damage == 'foreign-toleration':
        pod['spec']['tolerations'] = [{'operator': 'Exists'}]
    else:
        controller['status']['replicas'] = True
    with pytest.raises(ValueError, match='development runtime workload readiness unqualified'):
        observe(running)
