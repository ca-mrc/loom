"""Connected manager refresh preserves the installed global pool and recovery."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_management_refresh_connected import (
    connected_refresh as connected_refresh,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import application_material as application_material
from tests.ops.test_nebius_pool_cutover_entry import checks as checks
from tests.ops.test_nebius_pool_cutover_entry import cloud as cloud
from tests.ops.test_nebius_pool_cutover_entry import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover_entry import completed_upgrade as completed_upgrade
from tests.ops.test_nebius_pool_cutover_entry import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover_entry import database_guard as database_guard
from tests.ops.test_nebius_pool_cutover_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_pool_cutover_entry import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover_entry import installation as installation
from tests.ops.test_nebius_pool_cutover_entry import management_inputs as management_inputs
from tests.ops.test_nebius_pool_cutover_entry import material as material
from tests.ops.test_nebius_pool_cutover_entry import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_cutover_entry import private_cutover as private_cutover
from tests.ops.test_nebius_pool_cutover_entry import private_upgrade as private_upgrade
from tests.ops.test_nebius_pool_cutover_entry import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_cutover_entry import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_pool_predecessor import finish_cutover, pool_refresh_http


@pytest.fixture
def pooled_refresh(private_cutover, connected_refresh):
    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    metadata, _, _ = private_refresh(root, pool)
    request = context(metadata).request
    directory, anchor = Path(metadata['state_dir']), Path(metadata['anchor_dir'])
    bound = PoolManagerRefresh(root, pool, request, directory)
    original_api, state = connected_refresh
    state.request, state.directory, state.anchor = request, directory, anchor
    state.operation = metadata
    state.manager.clear()
    state.manager.update(copy.deepcopy(pool.active))
    state.manager['metadata'].update(resourceVersion='30', generation=5)
    manager = _key(pool.context.request.manager)
    with pool_refresh_http(bound)() as (verifier, external):
        state.values.update(external.objects)
        state.values[manager] = state.manager
        external.objects = state.values
        retained = {key: copy.deepcopy(state.values[key]) for key in pool.completion.workloads if key != manager}
        with HTTPSManagementRefreshInstaller(request=request, original=root, predecessor=pool, state_dir=directory,
                api_server=original_api.api_server, ssl_context=original_api.ssl_context, token=original_api.token,
                runtime_ca_pem=None, checks=original_api.checks, pool=verifier) as api:
            yield api, state, external, retained


@pytest.mark.timeout(900)
def test_connected_pool_refresh_completes_without_restoring_other_writers(pooled_refresh):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from tests.ops.test_nebius_management_refresh_connected import run, writes

    api, state, external, retained = pooled_refresh
    external.failure = 'credentials'
    with pytest.raises(ManagementRefreshInstallError):
        run((api, state))
    assert not writes(state)
    external.failure = None
    assert run((api, state))['status'] == 'management_refreshed'
    mutations = writes(state)
    assert len([row for row in mutations if row[0] == 'PATCH']) == 2
    assert state.public_calls and state.storage_calls
    assert {key: state.values[key] for key in retained} == retained
    assert run((api, state))['status'] == 'management_refreshed'
    assert writes(state) == mutations
    external.failure = 'gateway_rights'
    with pytest.raises(ManagementRefreshInstallError):
        api.verify_public(state.request, state.directory)
    assert writes(state) == mutations


@pytest.mark.timeout(900)
@pytest.mark.parametrize('boundary', ['create', 'retire', 'activate', 'activation_callback'])
def test_connected_pool_refresh_rechecks_authority_at_real_write_barriers(pooled_refresh, monkeypatch, boundary):
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from tests.ops.test_nebius_management_refresh_connected import run, writes

    api, state, external, retained = pooled_refresh
    original_transport = ManagementKubernetesTransport.__init__
    injected = False

    def drift(response):
        nonlocal injected
        if injected:
            return
        message = response.request
        child = state.directory / 'switch/cutover.json'
        switch = json.loads(child.read_bytes()) if child.exists() else None
        parent = state.directory / 'refresh.json'
        record = json.loads(parent.read_bytes()) if parent.exists() else None
        matched = (
            boundary == 'create' and message.method == 'POST' and message.url.params.get('dryRun') == 'All'
            and message.url.path.endswith('/configmaps')) or (
            boundary == 'retire' and switch is not None and switch['phase'] == 'retire_intent') or (
            boundary == 'activate' and message.method == 'PATCH' and message.url.params.get('dryRun') == 'All') or (
            boundary == 'activation_callback' and record is not None and record['activation_started']
            and message.url.path.endswith('/log'))
        if matched:
            injected = True
            external.failure = 'guard'

    def transport(self, **kwargs):
        original_transport(self, **kwargs)
        self.client.event_hooks['response'].append(drift)

    monkeypatch.setattr(ManagementKubernetesTransport, '__init__', transport)
    with pytest.raises(ManagementRefreshInstallError) as error:
        run((api, state))
    assert injected and error.value.stage == {'create': 'config', 'retire': 'retire',
        'activate': 'activate', 'activation_callback': 'activate'}[boundary]
    assert not (state.directory / 'completion.json').exists()
    mutations = writes(state)
    patches = [json.loads(message.content) for message in state.calls if message.method == 'PATCH' and not message.url.params]
    if boundary == 'create':
        assert mutations == []
    assert [row[4]['value'] for row in patches] == ([] if boundary in {'create', 'retire'} else [0])
    assert {key: state.values[key] for key in retained} == retained
    if boundary == 'activation_callback':
        # A post-callback drift must fail before even recording activate intent,
        # not only later when the final PATCH barrier rechecks authority.
        assert json.loads((state.directory / 'switch/cutover.json').read_bytes())['phase'] == 'stopped'
    else:
        external.failure = None
        with pytest.raises(ManagementRefreshInstallError):
            run((api, state))
        assert writes(state) == mutations, 'uncertain intent recovery must remain observation-only'


def test_protected_refresh_entry_binds_and_closes_separate_pool_reader(private_cutover, monkeypatch):
    import ssl
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_management_refresh_entry as entry
    from scripts.ops import nebius_pool_cutover_entry as cutover
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    pool = load_completed_pool(PoolPredecessorV1(operation=operation,
        completion_sha256=result['completion_sha256']), original=root)
    metadata, _, _ = private_refresh(root, pool)
    selected = context(metadata)
    connections = []

    @contextmanager
    def checks(inputs, ingress, *, foundation_candidate):
        assert inputs == root.original_inputs and ingress == root.ingress
        assert foundation_candidate == selected.inputs.foundation_candidate
        yield SimpleNamespace(), ssl.create_default_context(), 'operator-test-token'

    @contextmanager
    def reader(actual, *, refresh):
        assert actual == pool.context
        assert refresh == PoolManagerRefresh(root, pool, selected.request, Path(metadata['state_dir']))
        with pool_refresh_http(refresh)() as (verifier, external):
            connections.append(verifier.parent)
            yield verifier.parent
            assert all(call.method == 'GET' or call.url.path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews'
                for call in external.calls)

    monkeypatch.setattr(entry, 'connected_checks', checks)
    monkeypatch.setattr(cutover, 'connected_pool_api', reader)
    with entry.connected_refresh_api(selected, metadata) as api:
        api.pool.qualify()
        assert api.pool.refresh.request == selected.request
        assert not api.pool.parent.client.is_closed
    assert api.client.is_closed and len(connections) == 1
    assert all(client.is_closed for client in (connections[0].client,
        connections[0].fencing.client, connections[0].retirement.client))
    with pytest.raises(RuntimeError, match='caller-failure'):
        with entry.connected_refresh_api(selected, metadata):
            raise RuntimeError('caller-failure')
    assert len(connections) == 2 and connections[1].client.is_closed


@pytest.mark.timeout(900)
def test_connected_pool_refresh_supersedes_failed_probe_without_resetting_pool_history(pooled_refresh):
    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_connected import run, writes
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
    from tests.ops.test_nebius_management_refresh_predecessor import checksum
    from tests.ops.test_nebius_management_refresh_supersession import rewrite_inputs

    old_api, state, _, retained = pooled_refresh
    state.fail_probe = 'shared-probe'
    with pytest.raises(ManagementRefreshInstallError) as error:
        run((old_api, state))
    assert error.value.stage == 'shared-probe' and state.manager['spec']['replicas'] == 0
    selector = {'operation': state.operation, 'refresh_sha256': checksum(state.directory / 'refresh.json'),
        'switch_sha256': checksum(state.directory / 'switch/cutover.json')}
    operation, payload, _ = private_refresh(state.root, old_api.predecessor)
    payload['supersedes'] = selector
    rewrite_inputs(operation, payload)
    selected = context(operation)
    assert selected.superseded is not None
    frozen = {path: path.read_bytes() for path in selected.superseded.history}
    state.request, state.operation = selected.request, operation
    state.directory, state.anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    state.fail_probe = None
    bound = PoolManagerRefresh(selected.original, selected.predecessor, selected.request, state.directory, selected.superseded)
    with pool_refresh_http(bound)() as (verifier, external):
        external.objects = state.values
        with HTTPSManagementRefreshInstaller(request=selected.request, original=selected.original,
                predecessor=selected.predecessor, superseded=selected.superseded, state_dir=state.directory,
                api_server=old_api.api_server, ssl_context=old_api.ssl_context, token=old_api.token,
                runtime_ca_pem=None, checks=old_api.checks, pool=verifier) as api:
            assert run((api, state))['status'] == 'management_refreshed'
            mutations = writes(state)
            assert run((api, state))['status'] == 'management_refreshed'
            assert writes(state) == mutations
    assert state.manager['spec']['replicas'] == 1
    assert {path: path.read_bytes() for path in frozen} == frozen
    assert {key: state.values[key] for key in retained} == retained
