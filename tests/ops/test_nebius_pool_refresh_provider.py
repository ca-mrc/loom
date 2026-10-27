"""Source-selected NRD provider reads preserve retained scope and credentials."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from scripts.ops import nebius_pool_cutover_entry as entry
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_entry import EntryError
from scripts.ops.nebius_platform_rollout_successor import PlatformRolloutSuccessor
from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
from tests.support.nebius_platform import platform_inputs as platform_inputs

from loom.nebius_platform_render import build_platform

PREFIX = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_'
STORAGE = PREFIX + 'QUOTA_STORAGE_NAME'
BUILD_LIMIT = PREFIX + 'BUILD_CONCURRENCY_LIMIT'


@pytest.fixture
def provider(platform_inputs, tmp_path, monkeypatch, request):
    from nebius.api.nebius.compute.v1 import PlatformServiceClient
    from nebius.api.nebius.mk8s.v1 import DiskSpec, NodeGroupServiceClient
    from nebius.api.nebius.quotas.v1 import QuotaAllowanceServiceClient
    from nebius.sdk import SDK
    from tests.unit.test_execution_capacity_collector import (
        _enum,
        _node_group_spec,
        _platform_client,
        _quota,
    )

    config, candidate, profile = copy.deepcopy(platform_inputs)
    mode = getattr(request, 'param', False)
    if mode:
        config['task_image_builder'] = {
            'registry_repository': 'cr.eu-north1.nebius.cloud/test/task-images', 'max_concurrent': 16}
    root = Path(__file__).resolve().parents[2]
    def collector(environment):
        files = build_platform(environment, candidate, profile, {}, repo_root=root)
        return next(row for rows in files.values() for row in rows
            if row['kind'] == 'ConfigMap' and row['metadata']['name'] == 'loom-execution-capacity-collector')
    original = collector(config)
    original['metadata'].update(uid=str(uuid4()), resourceVersion='7')
    selected_config = copy.deepcopy(config)
    selected_config['execution_disk_type'] = 'NETWORK_SSD_NON_REPLICATED'
    if mode is True:
        selected_config['task_image_builder']['max_concurrent'] = None
    selected = collector(selected_config)
    current = copy.deepcopy(original)
    current['data'] = copy.deepcopy(selected['data'])
    current['metadata']['resourceVersion'] = '8'
    if mode == 'bookkeeping':
        # Independent retained/current kubectl apply records, not a copy of the
        # refresh projection. The selected renderer emits neither annotation.
        bookkeeping = 'kubectl.kubernetes.io/last-applied-configuration'
        original['metadata']['annotations'] = {bookkeeping: json.dumps(original, sort_keys=True)}
        current['metadata']['annotations'] = {bookkeeping: json.dumps(selected, sort_keys=True)}
    data = original['data']
    quotas = {name: (config['quota_parent_id'], config['region'], data[PREFIX + 'QUOTA_SERVICE'],
        data[PREFIX + 'QUOTA_' + name.upper() + '_NAME'], data[PREFIX + 'QUOTA_' + name.upper() + '_UNIT'])
        for name in ('nodes', 'vcpu', 'memory', 'storage') if PREFIX + 'QUOTA_' + name.upper() + '_NAME' in data}
    spec = SimpleNamespace(cluster_id=config['cluster_id'], node_group_id=config['execution_node_group_id'], quota_identities=quotas)
    material = b'{"private":"collector-only"}'
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'name': 'loom-execution-capacity-collector-nebius', 'namespace': original['metadata']['namespace'],
            'uid': str(uuid4()), 'resourceVersion': '9'},
        'data': {'credentials.json': base64.b64encode(material).decode()}}
    pin = SimpleNamespace(uid=secret['metadata']['uid'], resource_version='9', sha256=hashlib.sha256(material).hexdigest())
    context = SimpleNamespace(operation={'inputs_path': str(tmp_path / 'inputs.json')},
        request=SimpleNamespace(collector_config=original, collector_credential=pin),
        inputs=SimpleNamespace(installation=spec),
        original=SimpleNamespace(deployment=SimpleNamespace(installation=SimpleNamespace(foundation=SimpleNamespace(platform_config=config)))))
    reference = object()  # Publication qualification belongs to the existing successor resolver tests.
    bound = PoolManagerRefresh(context.original, object(), SimpleNamespace(platform_rollout=reference), tmp_path,
        successor=PlatformRolloutSuccessor(reference, selected_config, {_key(selected): selected}))
    pool = SimpleNamespace(context=context)
    qualified = []
    def qualify(self):
        assert self is bound
        qualified.append(True)
        return pool
    monkeypatch.setattr(PoolManagerRefresh, 'qualify', qualify)
    group = SimpleNamespace(metadata=SimpleNamespace(id=spec.node_group_id, parent_id=spec.cluster_id, resource_version=11),
        spec=_node_group_spec(), status=SimpleNamespace(state=_enum('RUNNING'), node_count=0, target_node_count=0,
            ready_node_count=0, reconciling=False, events=[]))
    group.spec.template.boot_disk = DiskSpec(size_gibibytes=93, type=DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED)
    state = SimpleNamespace(context=context, refresh=bound, current=current, selected=selected, config=selected_config,
        secret=secret, spec=spec, group=group, calls=[], http_calls=[], files=[], closed=[], qualified=qualified, late=None)
    async def quota_read(_self, request, **kwargs):
        state.calls.append('quotas')
        assert request.parent_id == config['quota_parent_id']
        values = {k: (*v[:3], selected['data'][STORAGE], v[4]) if k == 'storage' else v for k,v in quotas.items()}
        return SimpleNamespace(items=[_quota(v[3], v[4], 2000 * 1024**3, 0, i+1) for i,v in enumerate(values.values())], next_page_token='')
    async def group_read(_self, request, **kwargs):
        state.calls.append('group')
        assert request.id == spec.node_group_id
        if state.late:
            state.late()
        return group
    async def platform_read(_self, request, **kwargs):
        assert request.parent_id == config['project_id']
        return await _platform_client().get_by_name(request, **kwargs)
    async def close(_self):
        state.closed.append(True)
    def sdk_init(_self, *, credentials_file_name, **kwargs):
        path = Path(credentials_file_name)
        state.files.append(path)
        assert path.read_bytes() == material
        assert path.stat().st_mode & 0o777 == 0o600
    def get(method, path):
        state.http_calls.append((method, path))
        assert method == 'GET'
        return copy.deepcopy(secret if '/secrets/' in path else current)
    state.base = SimpleNamespace(_request=get)
    monkeypatch.setattr(SDK, '__init__', sdk_init)
    monkeypatch.setattr(SDK, 'close', close)
    for client in (QuotaAllowanceServiceClient, NodeGroupServiceClient, PlatformServiceClient):
        monkeypatch.setattr(client, '__init__', lambda *a, **k: None)
    monkeypatch.setattr(QuotaAllowanceServiceClient, 'list', quota_read)
    monkeypatch.setattr(NodeGroupServiceClient, 'get', group_read)
    monkeypatch.setattr(PlatformServiceClient, 'get_by_name', platform_read)
    return state


@pytest.mark.parametrize('provider', [False, 'retained', True, 'bookkeeping'],
    ids=['no-builder', 'retained-builder', 'resource-only-builder', 'collector-bookkeeping'], indirect=True)
def test_source_bound_nrd_provider_reads_actual_native_group_without_rewriting_history(provider):
    p = provider
    before = copy.deepcopy((p.context.request.collector_config, p.spec.quota_identities, p.current, p.secret, p.selected))
    with pytest.raises(EntryError):
        entry.qualify_pool_provider(p.context, p.base)
    entry.qualify_pool_provider(p.context, p.base, refresh=p.refresh)
    assert p.calls == ['quotas', 'group'] and p.closed == [True]
    assert p.qualified and all(not path.exists() for path in p.files)
    assert (p.context.request.collector_config, p.spec.quota_identities, p.current, p.secret, p.selected) == before
    assert p.http_calls and all(method == 'GET' for method, _ in p.http_calls)
    if p.config.get('task_image_builder', {}).get('max_concurrent', 16) is None:
        assert p.context.request.collector_config['data'][BUILD_LIMIT] == '16'
        assert BUILD_LIMIT not in p.current['data']
    bookkeeping = 'kubectl.kubernetes.io/last-applied-configuration'
    if bookkeeping not in p.current['metadata'].get('annotations', {}):
        return
    original = p.context.request.collector_config
    assert p.current['data'] == p.selected['data'] == p.refresh.provider_options()[0]['data']
    assert bookkeeping not in p.selected['metadata'].get('annotations', {})

    def reset():
        for actual, saved in ((original, before[0]), (p.current, before[2])):
            actual.clear()
            actual.update(copy.deepcopy(saved))
        p.calls.clear()
        p.closed.clear()
        p.files.clear()
        p.late = None

    def snapshot():
        return copy.deepcopy((original, p.spec.quota_identities, p.current, p.secret, p.selected))

    for change in ('added', 'changed', 'removed'):
        reset()
        if change == 'added':
            original['metadata'].pop('annotations')
        elif change == 'changed':
            p.current['metadata']['annotations'][bookkeeping] = json.dumps(p.selected, sort_keys=True, indent=2)
        else:
            p.current['metadata'].pop('annotations')
        recorded = snapshot()
        entry.qualify_pool_provider(p.context, p.base, refresh=p.refresh)
        assert p.calls == ['quotas', 'group'] and p.closed == [True], change
        assert snapshot() == recorded, change
        assert all(not path.exists() for path in p.files), change

    for damage in ('annotation', 'label', 'uid', 'data', 'late_annotation', 'late_label', 'late_uid'):
        reset()
        injected = []
        def drift(damage=damage, injected=injected):
            if damage.endswith('annotation'):
                p.current['metadata'].setdefault('annotations', {})['unrelated'] = 'drift'
            elif damage.endswith('label'):
                p.current['metadata'].setdefault('labels', {})['unrelated'] = 'drift'
            elif damage.endswith('uid'):
                p.current['metadata']['uid'] = str(uuid4())
            else:
                p.current['data'][PREFIX + 'NEBIUS_PROJECT_ID'] = 'foreign'
            injected.append(snapshot())
        if damage.startswith('late_'):
            p.late = drift
        else:
            drift()
        with pytest.raises(EntryError, match='pool cutover provider unqualified'):
            entry.qualify_pool_provider(p.context, p.base, refresh=p.refresh)
        assert len(injected) == 1 and snapshot() == injected[0], damage
        assert p.calls == (['quotas', 'group'] if damage.startswith('late_') else []), damage
        assert all(not path.exists() for path in p.files), damage
        assert (original, p.spec.quota_identities, p.secret, p.selected) == (before[0], before[1], before[3], before[4])

    reset()
    # Remove the source-selected NRD data delta so these failures isolate root
    # bookkeeping rather than an unrelated configuration or provider mismatch.
    p.current['data'] = copy.deepcopy(original['data'])
    recorded = snapshot()
    selected = p.refresh.successor
    try:
        object.__setattr__(p.refresh, 'successor', None)
        assert p.refresh.matches_provider(original, original, original)
        assert not p.refresh.matches_provider(p.current, original, original)
        for refresh in (None, p.refresh):
            with pytest.raises(EntryError, match='pool cutover provider unqualified'):
                entry.qualify_pool_provider(p.context, p.base, refresh=refresh)
            assert p.calls == []  # Reject before any provider/credential use.
            assert snapshot() == recorded
    finally:
        object.__setattr__(p.refresh, 'successor', selected)
        reset()
    assert p.refresh.successor is selected
    assert snapshot() == before
    assert all(method == 'GET' for method, _ in p.http_calls)


@pytest.mark.parametrize('damage', ['uid', 'credential', 'late_config', 'late_credential', 'credential_file',
    'late_projection', 'other_data', 'selected_other_data', 'selected_quota', 'unsupported', 'cluster', 'group', 'other_quota', 'native_disk'])
def test_nrd_projection_rejects_scope_material_and_other_quota_drift(provider, damage):
    p = provider
    if damage == 'uid':
        p.current['metadata']['uid'] = str(uuid4())
    elif damage == 'credential':
        p.secret['metadata']['uid'] = str(uuid4())
    elif damage == 'late_config':
        p.late = lambda: p.current['data'].update({STORAGE: 'foreign'})
    elif damage == 'late_credential':
        p.late = lambda: p.secret['metadata'].update(resourceVersion='10')
    elif damage == 'credential_file':
        p.late = lambda: p.files[-1].write_bytes(b'foreign-private')
    elif damage == 'late_projection':
        p.late = lambda: p.selected['data'].update({STORAGE: 'foreign'})
    elif damage == 'other_data':
        p.current['data'][PREFIX + 'NEBIUS_PROJECT_ID'] = 'foreign'
    elif damage == 'selected_other_data':
        p.selected['data'][PREFIX + 'NEBIUS_PROJECT_ID'] = 'foreign'
    elif damage == 'selected_quota':
        p.selected['data'][STORAGE] = p.current['data'][STORAGE] = 'foreign'
    elif damage == 'unsupported':
        p.config['execution_disk_type'] = 'UNKNOWN'
    elif damage == 'cluster':
        p.group.metadata.parent_id = 'foreign'
    elif damage == 'group':
        p.group.metadata.id = 'foreign'
    elif damage == 'other_quota':
        p.spec.quota_identities['vcpu'] = (*p.spec.quota_identities['vcpu'][:-1], 'foreign')
    else:
        from nebius.api.nebius.mk8s.v1 import DiskSpec
        p.group.spec.template.boot_disk = DiskSpec(size_gibibytes=80, type=DiskSpec.DiskType.NETWORK_SSD)
    with pytest.raises(EntryError) as error:
        entry.qualify_pool_provider(p.context, p.base, refresh=p.refresh)
    assert 'private' not in str(error.value)
    assert all(not path.exists() for path in p.files)


@pytest.mark.parametrize('provider', [True], indirect=True)
@pytest.mark.parametrize('damage', ['live_limit', 'selected_limit', 'late_limit', 'late_projection_limit',
    'other_data', 'selected_other_data', 'other_quota'])
def test_resource_only_builder_provider_rejects_limit_and_unrelated_drift(provider, damage):
    p = provider
    if damage == 'live_limit':
        p.current['data'][BUILD_LIMIT] = '16'
    elif damage == 'selected_limit':
        p.selected['data'][BUILD_LIMIT] = p.current['data'][BUILD_LIMIT] = '16'
    elif damage == 'late_limit':
        p.late = lambda: p.current['data'].update({BUILD_LIMIT: '16'})
    elif damage == 'late_projection_limit':
        p.late = lambda: p.selected['data'].update({BUILD_LIMIT: '16'})
    elif damage == 'other_data':
        p.current['data'][PREFIX + 'NEBIUS_PROJECT_ID'] = 'foreign'
    elif damage == 'selected_other_data':
        p.selected['data'][PREFIX + 'NEBIUS_PROJECT_ID'] = 'foreign'
    else:
        p.spec.quota_identities['vcpu'] = (*p.spec.quota_identities['vcpu'][:-1], 'foreign')
    before = copy.deepcopy((p.context.request.collector_config, p.spec.quota_identities))
    with pytest.raises(EntryError):
        entry.qualify_pool_provider(p.context, p.base, refresh=p.refresh)
    assert all(method == 'GET' for method, _ in p.http_calls)
    assert all(not path.exists() for path in p.files)
    assert (p.context.request.collector_config, p.spec.quota_identities) == before
