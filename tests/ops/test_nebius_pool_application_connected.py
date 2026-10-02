"""Protected source delivery uses the retained shared reader and actual fit proof."""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_ingress_operation import inventory as inventory
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
from tests.ops.test_nebius_pool_cutover_entry import (
    connected_cutover_entry,
    save_private,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    publication_http as publication_http,
)


@pytest.fixture
def connected_builder_entry(builder_cutover_inputs, inventory, monkeypatch):
    operation, payload, root, credentials = builder_cutover_inputs
    config = root.deployment.installation.foundation.platform_config
    guard, = (row for row in payload['guards'] if row['namespace'] == config['namespace'])
    container, = guard['controller']['spec']['template']['spec']['containers']
    prefix = 'LOOM_CP_SERVICE_EXECUTION_SOURCE_'
    # Model the retained shared source consumer, including its source-only keys.
    container['env'] = [row for row in container['env'] if not row['name'].startswith(prefix)] + [
        {'name': prefix + suffix, 'value': value} for suffix, value in (
            ('ENDPOINT', config['storage_endpoint']), ('REGION', config['region']), ('BUCKET', config['buckets']['source']))] + [
        {'name': prefix + suffix, 'valueFrom': {'secretKeyRef': {'name': 'loom-platform-storage', 'key': key}}}
        for suffix, key in (('ACCESS_KEY', 'source-access-key'), ('SECRET_KEY', 'source-secret-key'))]
    save_private(operation, payload)
    context, readers, observed = connected_cutover_entry.__wrapped__((operation, payload, root), monkeypatch)
    pin = payload['application_source_credential']
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'name': 'loom-platform-storage', 'namespace': config['namespace'],
            'uid': pin['uid'], 'resourceVersion': pin['resource_version']},
        'data': {**{'source-' + key: base64.b64encode(value.encode()).decode() for key, value in credentials.items()},
            'secret-key': base64.b64encode(b'broader-data-key-must-not-copy').decode()}}
    observed['source_reads'] = []

    def read(method, path):
        assert (method, path) == ('GET', '/api/v1/namespaces/' + config['namespace'] + '/secrets/loom-platform-storage')
        observed['source_reads'].append(path)
        return copy.deepcopy(secret)

    readers.base._request = read
    node = inventory['nodes'][0]
    node['status']['allocatable'] = {'cpu': '128', 'memory': '256Gi', 'ephemeral-storage': '512Gi', 'pods': '1000'}
    readers.base.inventory = lambda api, resource, kind: copy.deepcopy([node] if kind == 'Node' else [])
    return context, readers, observed, secret, credentials, node


def test_connected_builder_delivers_only_pinned_source_and_rechecks_rotation(connected_builder_entry):
    from scripts.ops import nebius_pool_cutover_entry as entry

    context, _, observed, secret, credentials, _ = connected_builder_entry
    with entry.connected_pool_api(context) as api:
        manager = api.catalog['runtime']['Deployment:' + context.request.manager['metadata']['namespace'] + ':loom-service']
        volume, = (row for row in manager['spec']['template']['spec']['volumes'] if row['name'] == 'application-source-credentials')
        key = 'Secret:' + manager['metadata']['namespace'] + ':' + volume['secret']['secretName']
        document = api.documents[key]
        assert json.loads(base64.b64decode(document['data']['credentials.json'])) == credentials
        assert api._approved(document) == '/api/v1/namespaces/' + manager['metadata']['namespace'] + '/secrets'
        assert observed['source_reads']
        secret['metadata']['uid'] = str(uuid4())
        with pytest.raises(entry.EntryError):
            api.checks.current()
    assert observed['closed'] and observed['guard_calls'] == []
    assert not Path(context.operation['state_dir']).exists()


@pytest.mark.parametrize('damage', ['uid', 'version', 'source', 'deleting'])
def test_connected_builder_rejects_changed_source_before_transport_writes(connected_builder_entry, damage):
    from scripts.ops import nebius_pool_cutover_entry as entry

    context, _, observed, secret, _, _ = connected_builder_entry
    if damage == 'uid':
        secret['metadata']['uid'] = str(uuid4())
    elif damage == 'version':
        secret['metadata']['resourceVersion'] = 'new-version'
    elif damage == 'source':
        secret['data']['source-secret-key'] = base64.b64encode(b'rotated-source').decode()
    else:
        secret['metadata']['deletionTimestamp'] = '2026-10-02T00:00:00Z'
    with pytest.raises(entry.EntryError):
        with entry.connected_pool_api(context):
            pytest.fail('unqualified source reached the protected parent')
    assert observed['source_reads'] and observed['guard_calls'] == []
    assert not Path(context.operation['state_dir']).exists()


def test_builder_preflight_uses_larger_manager_footprint_before_downtime(connected_builder_entry):
    from dataclasses import replace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_application_upgrade_prerequisites import ApplicationUpgradePrerequisites

    context, readers, observed, _, _, node = connected_builder_entry
    # High CPU/RAM cannot hide insufficient ephemeral capacity for source spool.
    # The old manager fits with the same existing personal-app reservation.
    # Existing reservation is 30000Mi; 32Gi fits it and the old manager/jobs,
    # but not the builder manager's additional 4Gi private spool.
    node['status']['allocatable']['ephemeral-storage'] = '32Gi'
    old = replace(context.original.upgrade, setup=replace(context.original.upgrade.setup,
        deployment=context.predecessor.deployment, candidate=context.inputs.candidate, profile=context.inputs.profile))
    ApplicationUpgradePrerequisites(base=readers.base, settings=context.original.inputs.prerequisites).platform_capacity(old)
    with entry.connected_pool_api(context) as api:
        with pytest.raises(entry.EntryError):
            api.checks.preflight(context.request)
        node['status']['allocatable']['ephemeral-storage'] = '512Gi'
        api.checks.preflight(context.request)
    assert observed['guard_calls'] == [] and not Path(context.operation['state_dir']).exists()


async def test_protected_publication_qualifies_application_builder_tools(builder_cutover_inputs, publication_http):
    from scripts.ops import nebius_pool_cutover_entry as entry

    metadata, payload, _, _ = builder_cutover_inputs
    async with httpx.AsyncClient() as http:
        await entry.qualify_pool_publication(entry.load_pool_cutover_inputs(metadata), http)
        profile = payload['installation']['profiles']['application_images'][0]
        profile['settings']['service_image'] = profile['recipe']['trusted_image_ref'] = 'foreign/service@sha256:' + 'f' * 64
        save_private(metadata, payload)
        context = entry.load_pool_cutover_inputs(metadata)
        with pytest.raises(entry.EntryError):
            await entry.qualify_pool_publication(context, http)
