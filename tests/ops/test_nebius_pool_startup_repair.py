"""Fixed source startup recovery preserves the original closed installation."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_cutover import cutover_documents
from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
from tests.ops.test_nebius_pool_application_cutover import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_application_cutover import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    checks as checks,
)
from tests.ops.test_nebius_pool_application_cutover import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_application_cutover import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_application_cutover import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_application_cutover import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    installation as installation,
)
from tests.ops.test_nebius_pool_application_cutover import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    material as material,
)
from tests.ops.test_nebius_pool_application_cutover import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_application_cutover import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_application_cutover import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    runtime_inputs as runtime_inputs,
)
from tests.ops.test_nebius_pool_application_cutover import (
    save_private,
)


@pytest.fixture
def historical_cutover(builder_cutover_inputs):
    operation, payload, _, credentials = builder_cutover_inputs
    payload.pop('source_delivery_version')
    save_private(operation, payload)
    context = load_pool_cutover_inputs(operation)
    original = copy.deepcopy(cutover_documents(context.request)['runtime'][_key(context.request.manager)])
    original['metadata'].update(uid=context.request.manager['metadata']['uid'], resourceVersion='31')
    original['spec']['replicas'] = 1
    return context, original, credentials


def test_repair_changes_only_source_path_initializer_and_immutable_config(historical_cutover):
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    saved = copy.deepcopy(original)
    repaired, config = source_repair_documents(context.request, original)
    expected = copy.deepcopy(original)
    expected['metadata'].pop('uid')
    expected['metadata'].pop('resourceVersion')
    pod = expected['spec']['template']['spec']
    new_pod = repaired['spec']['template']['spec']
    for volume in pod['volumes']:
        if volume['name'] == 'management-config':
            volume['configMap']['name'] = config['metadata']['name']
    for container in (*pod['containers'], *pod['initContainers']):
        for mount in container.get('volumeMounts', []):
            if mount['name'] == 'application-source':
                mount['mountPath'] = '/run/loom-application-source'
        if container['name'] == 'prepare-application-source':
            new_init, = (row for row in new_pod['initContainers'] if row['name'] == container['name'])
            assert new_init['command'][-1] == '/run/loom-application-source/spool'
            assert new_init['command'] != container['command']
            container['command'] = new_init['command']
    revision = repaired['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision']
    expected['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision'] = revision
    assert config['metadata']['name'] == 'loom-management-applications-' + revision[7:19]
    assert config['kind'] == 'ConfigMap' and config['immutable'] is True
    old_config, = (row for row in cutover_documents(context.request)['configuration'] if row['kind'] == 'ConfigMap'
        and row['metadata']['name'].startswith('loom-management-applications-'))
    old_settings = json.loads(old_config['data']['installation.json'])
    old_settings['applications']['runtime']['source_upload']['spool_directory'] = '/run/loom-application-source/spool'
    assert json.loads(config['data']['installation.json']) == old_settings
    assert repaired == expected
    assert original == saved


@pytest.mark.parametrize('damage', ['current_version', 'uid', 'image', 'env', 'mount', 'replicas'])
def test_repair_refuses_foreign_or_nonhistorical_manager(historical_cutover, damage):
    from scripts.ops.nebius_pool_startup_repair import source_repair_documents

    context, original, _ = historical_cutover
    request = context.request
    if damage == 'current_version':
        request = replace(request, application_delivery=replace(request.application_delivery, source_delivery_version='v2'))
    elif damage == 'uid':
        original['metadata']['uid'] = str(uuid4())
    elif damage == 'replicas':
        original['spec']['replicas'] = 0
    else:
        pod = original['spec']['template']['spec']
        if damage == 'image':
            pod['containers'][0]['image'] = 'foreign'
        elif damage == 'env':
            pod['containers'][0]['env'].append({'name': 'FOREIGN', 'value': '1'})
        else:
            mount, = (row for row in pod['containers'][0]['volumeMounts'] if row['name'] == 'application-source')
            mount['mountPath'] = '/foreign'
    with pytest.raises(ValueError):
        source_repair_documents(request, original)
