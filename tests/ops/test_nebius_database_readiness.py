"""Database topology qualification is reusable without migration authority."""
from __future__ import annotations

import copy
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.mark.parametrize('damage', [None, 'pod_owner', 'database_uid', 'service_uid', 'pagination',
    'extra_pod', 'wrong_pvc', 'backend_uid', 'backend_address'])
def test_retained_database_readiness_needs_no_migration_request(database_guard, damage):
    from scripts.ops.nebius_database_readiness import qualify_database_backend, qualify_database_pod

    _, state = database_guard
    pods = {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '1'},
        'items': [copy.deepcopy(state.pod)]}
    if damage == 'pod_owner':
        pods['items'][0]['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif damage == 'database_uid':
        state.database['metadata']['uid'] = str(uuid4())
    elif damage == 'service_uid':
        state.service['metadata']['uid'] = str(uuid4())
    elif damage == 'pagination':
        pods['metadata']['continue'] = 'next'
    elif damage == 'extra_pod':
        pods['items'].append(copy.deepcopy(state.pod))
    elif damage == 'wrong_pvc':
        pods['items'][0]['spec']['volumes'][-1]['persistentVolumeClaim']['claimName'] = 'foreign'
    elif damage == 'backend_uid':
        state.endpoints['items'][0]['endpoints'][0]['targetRef']['uid'] = str(uuid4())
    elif damage == 'backend_address':
        state.endpoints['items'][0]['endpoints'][0]['addresses'] = ['10.20.0.3']

    def qualify():
        pod = qualify_database_pod(namespace=state.target.namespace,
            database=state.database, service=state.service,
            retained_database=state.target.database.statefulset, retained_service=state.target.database.service,
            listing=pods)
        qualify_database_backend(namespace=state.target.namespace, service=state.service,
            pod=pod, listing=state.endpoints)
        return pod

    if damage is None:
        assert qualify()['metadata']['uid'] == state.pod['metadata']['uid']
    else:
        with pytest.raises(ValueError):
            qualify()
