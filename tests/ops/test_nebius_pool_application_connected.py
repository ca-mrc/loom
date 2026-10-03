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


def test_builder_preflight_uses_larger_manager_footprint_before_downtime(connected_builder_entry, monkeypatch):
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
        # Isolate external namespace/writer/readiness observations; exercise the
        # real public preflight, connected entry checks and capacity calculation.
        monkeypatch.setattr(api, '_scope', lambda: None)
        monkeypatch.setattr(api, 'qualify_writer_bindings', lambda: None)
        monkeypatch.setattr(api, '_qualify_database_readiness', lambda: None)
        with pytest.raises(entry.EntryError, match='initial capacity'):
            api.preflight(context.request)
        node['status']['allocatable']['ephemeral-storage'] = '512Gi'
        api.preflight(context.request)
    assert observed['guard_calls'] == [] and not Path(context.operation['state_dir']).exists()


@pytest.mark.timeout(180)
@pytest.mark.parametrize('capacity_loss', ['headroom', 'unready'])
def test_builder_recovery_can_fence_after_installation_capacity_disappears(connected_builder_entry, capacity_loss):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from tests.ops.test_nebius_pool_activation_stage import ActivationAPI
    from tests.ops.test_nebius_pool_cutover import CutoverAPI
    from tests.ops.test_nebius_pool_startup import StartupAPI

    context, _, _, _, credentials, node = connected_builder_entry
    request = context.request
    state, anchor = Path(context.operation['state_dir']), Path(context.operation['anchor_dir'])
    with entry.connected_pool_api(context) as parent:
        # Use the real connected private/source/database/provider checks and
        # platform inventory; only external write transports are doubled.
        parent.checks.preflight(request)
        parent.checks.qualify_initial_capacity(request)
        closed = CutoverAPI(request)
        for document in closed.documents.values():
            document['metadata'].setdefault('resourceVersion', '1')
        assert stage_pool_cutover(request=request, tokens=context.tokens, api=closed,
            source_credentials=credentials, state_dir=state, anchor_dir=anchor)['status'] == 'pool_runtime_staged_closed'
        startup = StartupAPI(request, closed, state)
        assert stage_pool_startup(request=request, api=startup, state_dir=state,
            anchor_dir=anchor)['status'] == 'pool_startup_staged_closed'

        class RecoveryAPI(ActivationAPI):
            def verify_retained(self):
                # Same connected entry check used by HTTPSPoolActivationAPI:
                # it must not inherit fresh-install resource-fit requirements.
                parent.checks.preflight(request)
                super().verify_retained()

        recovery = RecoveryAPI((request, context.tokens, closed, startup, None, state.parent))
        recovery.state = state
        assert advance_pool_activation(request=request, api=recovery, state_dir=state,
            anchor_dir=anchor)['status'] == 'pool_activation_complete'
        recovery.calls.clear()
        recovery.runtime_checks = 0
        recovery.ready = False
        if capacity_loss == 'headroom':
            node['status']['allocatable']['ephemeral-storage'] = '32Gi'
        else:
            node['status']['conditions'] = [{'type': 'Ready', 'status': 'False'}]
        with pytest.raises(entry.EntryError, match='initial capacity'):
            parent.checks.qualify_initial_capacity(request)
        result = advance_pool_activation(request=request, api=recovery, state_dir=state,
            anchor_dir=anchor, cancel=True)
        assert result['status'] == 'pool_activation_cancelled'
        assert recovery.calls == [('fence', None), *(('guard-fence', key) for key in recovery.guards)]
        assert recovery.mode == 'fenced' and set(recovery.guards.values()) == {'fenced'}
        assert recovery.runtime_checks == 0


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


@pytest.mark.timeout(420)
def test_builder_complete_operation_retains_source_for_future_refresh(builder_cutover_inputs, monkeypatch):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from tests.ops.test_nebius_pool_activation_stage import ActivationAPI
    from tests.ops.test_nebius_pool_operation import operation as compose_operation

    operation, _, root, credentials = builder_cutover_inputs
    context = load_pool_cutover_inputs(operation)
    state = compose_operation.__wrapped__((context.request, context.tokens), Path(operation['state_dir']).parent, monkeypatch)
    state.parent.state_dir = Path(operation['state_dir'])
    state.parent.anchor_dir = Path(operation['anchor_dir'])
    state.parent._source_credentials = credentials
    for document in state.parent.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    initialize = ActivationAPI.__init__

    def initialize_at_retained_state(self, selected):
        initialize(self, selected)
        self.state = selected[3].state

    monkeypatch.setattr(ActivationAPI, '__init__', initialize_at_retained_state)
    result = state.run()
    assert result['status'] == 'pool_cutover_completed' and result['outcome'] == 'global'
    assert result['acceptance_verified'] is False
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    assert pool.deployment.application_builder_machine_id is not None
    assert pool.deployment.installation.applications.runtime.build.binding.source.source_bucket == (
        root.deployment.installation.foundation.platform_config['buckets']['source'])
    assert pool.deployment.pool_catalog_operation_id == context.inputs.installation.operation_id
    before = {path: path.read_bytes() for path in pool.history}
    target = render_refresh(ManagementRefreshRenderRequest(pool.deployment, pool.deployment, pool.active,
        context.inputs.candidate, context.inputs.profile, root.upgrade.setup.repo_root)).deployment
    volumes = {row['name']: row for row in target['spec']['template']['spec']['volumes']}
    key = 'Secret:' + target['metadata']['namespace'] + ':' + volumes['application-source-credentials']['secret']['secretName']
    assert json.loads(base64.b64decode(state.parent.resources.resources[key]['data']['credentials.json'])) == credentials
    assert pool.active == pool.completion.workloads[_key(context.request.manager)]
    assert {path: path.read_bytes() for path in before} == before
    assert state.run() == result


@pytest.mark.timeout(420)
def test_builder_rollback_preserves_original_manager_without_source_mounts(builder_cutover_inputs):
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from tests.ops.test_nebius_pool_predecessor import finish_cutover

    operation, _, root, credentials = builder_cutover_inputs
    context, result = finish_cutover(operation, legacy=True, source_credentials=credentials)
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    assert pool.completion.outcome == 'legacy'
    assert pool.deployment == context.predecessor.deployment
    assert pool.deployment.application_builder_machine_id is None
    assert pool.deployment.installation.applications.runtime.build is None
    assert all(row['name'] != 'application-source-credentials' for row in pool.active['spec']['template']['spec']['volumes'])
