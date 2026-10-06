"""A pre-opening manager correction must remain an image-only projection."""
from __future__ import annotations

import copy
from dataclasses import replace

import pytest
from tests.ops.test_nebius_management_refresh import (
    application_management_inputs as application_management_inputs,
    build_inputs as build_inputs,
    builder_management_inputs as builder_management_inputs,
    builder_refresh_request as builder_refresh_request,
    management_inputs as management_inputs,
    platform_inputs as platform_inputs,
    runtime_inputs as runtime_inputs,
    source_management_inputs as source_management_inputs,
)


def project(request):
    from scripts.ops.nebius_pool_manager_image import manager_image_target

    return manager_image_target(request)


def test_manager_image_projection_changes_only_main_and_initializer_images(builder_refresh_request):
    request = builder_refresh_request
    original = copy.deepcopy(request)
    expected = copy.deepcopy(request.active)
    for key in ('uid', 'resourceVersion', 'generation'):
        expected['metadata'].pop(key, None)
    expected.pop('status', None)
    pod = expected['spec']['template']['spec']
    for container in (*pod['containers'], *pod['initContainers']):
        container['image'] = request.candidate['images']['service']['image_ref']
    assert project(request) == expected
    assert request == original


@pytest.mark.parametrize('damage', ['configuration', 'env', 'initializer_image', 'source_path', 'replicas', 'image_tag', 'registry'])
def test_manager_image_projection_refuses_non_image_or_unqualified_changes(builder_refresh_request, damage):
    request = copy.deepcopy(builder_refresh_request)
    pod = request.active['spec']['template']['spec']
    if damage == 'configuration':
        after = request.after.model_copy(update={'public_host': 'other.example.com'})
        request = replace(request, after=after)
    elif damage == 'env':
        pod['containers'][0]['env'].append({'name': 'FOREIGN', 'value': '1'})
    elif damage == 'initializer_image':
        pod['initContainers'][0]['image'] = pod['initContainers'][0]['image'].replace('b' * 64, 'e' * 64)
    elif damage == 'source_path':
        mount, = (row for row in pod['containers'][0]['volumeMounts'] if row['name'] == 'application-source')
        mount['mountPath'] = '/foreign'
    elif damage == 'replicas':
        request.active['spec']['replicas'] = 0
    else:
        image = request.candidate['images']['service']['image_ref']
        image = image.split('@')[0] + ':latest' if damage == 'image_tag' else image.replace('/test/', '/foreign/')
        request.candidate['images']['service']['image_ref'] = image
        request.profile['task_image_ref'] = image
    with pytest.raises(ValueError, match='manager_image'):
        project(request)


def test_manager_image_projection_refuses_unchanged_image(builder_refresh_request):
    request = builder_refresh_request
    image = request.active['spec']['template']['spec']['containers'][0]['image']
    request.candidate['images']['service']['image_ref'] = image
    request.profile['task_image_ref'] = image
    with pytest.raises(ValueError, match='manager_image'):
        project(request)
