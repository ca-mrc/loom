"""The refresh baseline must come from a completed, private-input-bound cutover."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

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


def finish_cutover(operation, *, legacy=False, source_credentials=None):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_completion import complete_pool_cutover
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from tests.ops.test_nebius_pool_activation_stage import ActivationAPI
    from tests.ops.test_nebius_pool_cutover import CutoverAPI
    from tests.ops.test_nebius_pool_startup import StartupAPI

    context = load_pool_cutover_inputs(operation)
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    closed = CutoverAPI(context.request)
    # Completed history omits volatile metadata. A real Kubernetes GET supplies
    # a current version; the external-I/O double must do the same for CAS.
    for document in closed.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    assert stage_pool_cutover(request=context.request, tokens=context.tokens, api=closed,
        state_dir=state, anchor_dir=anchor, source_credentials=source_credentials)['status'] == 'pool_runtime_staged_closed'
    startup = StartupAPI(context.request, closed, state)
    assert stage_pool_startup(request=context.request, api=startup, state_dir=state,
        anchor_dir=anchor)['status'] == 'pool_startup_staged_closed'
    fixture = (context.request, context.tokens, closed, startup, None, state.parent)
    api = ActivationAPI(fixture)
    api.state, api.anchor = state, anchor
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)['status'] == 'pool_activation_complete'
    if legacy:
        # Reuse the real recovery stages and their external-transport doubles.
        # These doubles normally use fixture_root/cutover; private entry uses an
        # operation-qualified state path, which is already held by StartupAPI.
        from unittest.mock import patch

        from scripts.ops.nebius_pool_gateway_retirement import retire_gateway_roles
        from scripts.ops.nebius_pool_legacy_reopening import reopen_pool_legacy
        from scripts.ops.nebius_pool_legacy_restart import restart_pool_legacy
        from scripts.ops.nebius_pool_machine_retirement import retire_pool_machines
        from scripts.ops.nebius_pool_role_restoration import restore_pool_roles
        from scripts.ops.nebius_pool_shutdown import stop_pool_successors
        from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
        from scripts.ops.nebius_pool_template_restoration import restore_pool_templates
        from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI
        from tests.ops.test_nebius_pool_legacy_reopening import ReopeningAPI
        from tests.ops.test_nebius_pool_legacy_restart import RestartAPI
        from tests.ops.test_nebius_pool_machine_retirement import MachineAPI
        from tests.ops.test_nebius_pool_role_restoration import RoleAPI
        from tests.ops.test_nebius_pool_template_restoration import TemplateAPI

        initialize = ActivationAPI.__init__
        def operation_init(self, selected):
            initialize(self, selected)
            self.state = selected[3].state
            self.anchor = anchor
        with patch.object(ActivationAPI, '__init__', operation_init):
            machine = MachineAPI(fixture)
            machine.mode, machine.guards = api.mode, api.guards.copy()
            assert advance_pool_activation(request=context.request, api=machine, state_dir=state,
                anchor_dir=anchor, cancel=True)['status'] == 'pool_activation_cancelled'
            assert fence_pool_startup(request=context.request, api=machine, state_dir=state,
                anchor_dir=anchor)['status'] == 'startup_writes_fenced'
            assert stop_pool_successors(request=context.request, api=machine, state_dir=state,
                anchor_dir=anchor)['status'] == 'pool_successors_stopped'
            assert retire_pool_machines(request=context.request, api=machine, state_dir=state,
                anchor_dir=anchor)['status'] == 'pool_machines_retired'
            api = GatewayAPI(fixture, machine)
            for successor, stage, status in (
                (None, retire_gateway_roles, 'pool_gateway_roles_retired'),
                (TemplateAPI, restore_pool_templates, 'pool_legacy_templates_restored_closed'),
                (RoleAPI, restore_pool_roles, 'pool_legacy_roles_restored_closed'),
                (RestartAPI, restart_pool_legacy, 'pool_legacy_restart_staged_closed'),
                (ReopeningAPI, reopen_pool_legacy, 'pool_legacy_reopened'),
            ):
                if successor is not None:
                    api = successor(fixture, api)
                assert stage(request=context.request, api=api, state_dir=state, anchor_dir=anchor)['status'] == status
    result = complete_pool_cutover(request=context.request, api=api, state_dir=state, anchor_dir=anchor)
    return context, result


@pytest.mark.timeout(420)
def test_completed_pool_derives_catalog_baseline_and_preserves_original_authority(private_cutover):
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool

    operation, _, root = private_cutover
    context, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    before = {path: path.read_bytes() for path in Path(operation['inputs_path']).parent.parent.parent.rglob('*.json')}
    pool = load_completed_pool(selector, original=root)
    assert pool.selector == selector and pool.completion.outcome == 'global'
    assert pool.deployment.pool_catalog_operation_id == context.inputs.installation.operation_id
    original = root.deployment.model_dump(mode='json')
    expected = copy.deepcopy(original)
    expected['pool_catalog_operation_id'] = operation['operation_id']
    assert pool.deployment.model_dump(mode='json') == expected
    assert root.deployment.model_dump(mode='json') == original
    assert pool.active == pool.completion.workloads[_key(context.request.manager)]
    assert pool.active['metadata']['uid'] == root.active['metadata']['uid']
    assert pool.history[Path(operation['inputs_path'])] == operation['inputs_sha256']
    assert set(root.history) <= set(pool.history) and set(pool.completion.history) <= set(pool.history)
    assert len(pool.history) <= 128
    # Exercise the existing next-upgrade renderer: no duplicate pool renderer or
    # removal of its strict catalog-preservation contract is needed.
    render = ManagementRefreshRenderRequest(pool.deployment, pool.deployment, pool.active,
        context.inputs.candidate, context.inputs.profile, root.upgrade.setup.repo_root)
    target = render_refresh(render).deployment
    profiles, = (row for row in target['spec']['template']['spec']['volumes'] if row['name'] == 'pool-profiles')
    assert profiles['configMap']['name'] == 'loom-pool-profiles-' + context.inputs.installation.operation_id.hex
    assert {path: path.read_bytes() for path in before} == before

    # The receipt and private input hashes select one operation, not any other
    # completed upgrade or an arbitrary caller-supplied post-cutover manager.
    for damage in ('input_hash', 'completion_hash', 'operation', 'original'):
        altered = selector.model_dump()
        supplied = root
        if damage == 'input_hash':
            altered['operation']['inputs_sha256'] = '0' * 64
        elif damage == 'completion_hash':
            altered['completion_sha256'] = '0' * 64
        elif damage == 'operation':
            altered['operation']['operation_id'] = str(uuid4())
        else:
            from dataclasses import replace

            active = copy.deepcopy(root.active)
            active['metadata']['uid'] = str(uuid4())
            supplied = replace(root, active=active)
        with pytest.raises(ValueError, match='pool_predecessor_unqualified'):
            load_completed_pool(PoolPredecessorV1.model_validate(altered), original=supplied)
    # Altering still-hashed phase bytes cannot be hidden by an unchanged receipt.
    activation = Path(operation['state_dir']) / 'activation.json'
    record = json.loads(activation.read_bytes())
    record['cancellation'] = 'intent'
    activation.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='pool_predecessor_unqualified'):
        load_completed_pool(selector, original=root)


@pytest.mark.timeout(420)
def test_successive_refresh_completions_keep_the_qualified_pool_baseline(private_cutover):
    import ssl
    from types import SimpleNamespace

    from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_entry import context, private_refresh
    from tests.ops.test_nebius_management_refresh_predecessor import (
        complete_refresh,
        load_refresh,
        refresh_case,
    )

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    metadata, _, _ = private_refresh(root, pool)
    bound = context(metadata)
    assert bound.predecessor == pool
    assert bound.request.pool_baseline == selector.model_dump(mode='json')
    prior = pool
    histories = []
    for _ in range(2):
        refreshed, case = complete_refresh(root, prior, pool_baseline=selector.model_dump(mode='json'))
        completed = load_refresh(refreshed, root)
        assert completed.pool_baseline == pool
        metadata, _, _ = private_refresh(root, completed)
        inherited = context(metadata)
        assert inherited.predecessor == completed
        assert inherited.request.pool_baseline == selector.model_dump(mode='json')
        projected = PoolManagerRefresh(root, completed, inherited.request, Path(metadata['state_dir'])).workload_options()
        assert projected[_key(pool.context.request.manager)] == (completed.active,)
        assert completed.deployment.pool_catalog_operation_id == pool.deployment.pool_catalog_operation_id
        assert completed.active['metadata']['uid'] == pool.active['metadata']['uid']
        assert all(completed.history.get(path) == checksum for path, checksum in pool.history.items())
        assert len(completed.history) <= 128
        histories.append(len(completed.history))
        receipt = json.loads((case[2] / 'completion.json').read_bytes())
        assert receipt['contract']['pool_baseline'] == selector.model_dump(mode='json')
        prior = completed
    assert histories[0] == histories[1], 'ordinary refresh ancestry must remain bounded'
    # Without its pool baseline, a self-consistent pool-wired refresh still fails
    # the rooted check; do not accept its caller-supplied before snapshot alone.
    unbound, _ = complete_refresh(root, prior)
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(unbound, root)
    completion = Path(operation['state_dir']) / 'completion.json'
    saved = completion.read_bytes()
    completion.write_bytes(saved + b'\n')
    with pytest.raises(ValueError, match='refresh_predecessor_unqualified'):
        load_refresh(refreshed, root)
    completion.write_bytes(saved)
    assert load_refresh(refreshed, root) == prior
    # Receipt support must not accidentally admit a live refresh through the
    # legacy manager-only verifier, including when its request drops the field.
    for baseline in (selector.model_dump(mode='json'), None):
        request, _, state, _ = refresh_case(root, prior, pool_baseline=baseline)
        with pytest.raises(ManagementStageError, match='pool-aware'):
            with HTTPSManagementRefreshInstaller(request=request, original=root, predecessor=prior,
                state_dir=state, api_server=root.original_inputs.operator_connection.endpoint, ssl_context=ssl.create_default_context(),
                runtime_ca_pem=None, checks=SimpleNamespace()):
                pass


def test_predecessor_recursion_is_bounded_and_failure_does_not_poison_next_load():
    from contextlib import ExitStack

    from scripts.ops.nebius_management_refresh_predecessor import _predecessor_scope

    operation = str(uuid4())
    with _predecessor_scope('refresh', operation):
        with pytest.raises(ValueError):
            with _predecessor_scope('refresh', operation):
                pytest.fail('cyclic ancestry was admitted')
    with pytest.raises(ValueError), ExitStack() as stack:
        for _ in range(9):
            stack.enter_context(_predecessor_scope('pool-cutover', str(uuid4())))
    with _predecessor_scope('refresh', operation):
        pass  # Both duplicate and depth-limit failures release their scope.


@pytest.mark.timeout(420)
def test_refresh_projects_only_manager_from_anchored_steps_and_uncertain_writes(private_cutover):
    from scripts.ops.nebius_ingress_stage import _snapshot, _uid
    from scripts.ops.nebius_management_refresh_install import ManagementRefreshInstallError
    from scripts.ops.nebius_management_refresh_switch import refresh_target
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import refresh_case

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    request, api, state, _ = case
    bound = PoolManagerRefresh(root, pool, request, state)
    manager = _key(pool.context.request.manager)

    def check(*allowed):
        before = {path: path.read_bytes() for path in (*pool.history, *state.rglob('*.json'))}
        options = bound.workload_options()
        assert set(options) == set(pool.completion.workloads)
        assert tuple(_snapshot(row) for row in options[manager]) == tuple(_snapshot(row) for row in allowed)
        assert all(_uid(row) == _uid(pool.active) for row in options[manager])
        assert {key: value for key, value in options.items() if key != manager} == {
            key: (value,) for key, value in pool.completion.workloads.items() if key != manager}
        assert {path: path.read_bytes() for path in before} == before

    check(pool.active)
    api.switch.failure = 'before'
    with pytest.raises(ManagementRefreshInstallError):
        run(case)
    stopped = refresh_target(request.resources.switch, 'retire')
    check(pool.active, stopped)
    # Simulate readback of the previously uncertain CAS, not a new write.
    api.switch.document = api.switch.desired('retire')
    api.switch.failure, api.pending = None, 'manager-probe'
    assert run(case)['phase'] == 'manager-probe'
    check(stopped)
    assert api.switch.calls == ['retire']
    api.pending, api.switch.failure = None, 'before'
    with pytest.raises(ManagementRefreshInstallError):
        run(case)
    active = json.loads((state / 'switch/cutover.json').read_bytes())['active']
    check(stopped, active)
    api.switch.document = api.switch.desired('activate')
    api.switch.failure = None
    assert run(case)['status'] == 'management_refreshed'
    check(active)
    assert api.switch.calls == ['retire', 'activate']


@pytest.mark.timeout(420)
def test_refresh_projection_rejects_unbound_history_and_tampered_journals(private_cutover):
    from dataclasses import replace

    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import refresh_case

    operation, _, root = private_cutover
    _, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    request, _, state, anchor = case
    bound = PoolManagerRefresh(root, pool, request, state)
    assert run(case)['status'] == 'management_refreshed'
    expected = bound.workload_options()
    for altered in (replace(bound, state_dir=state.parent / 'foreign'),
            replace(bound, request=replace(request, pool_baseline=None)),
            replace(bound, request=replace(request, history={}))):
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            altered.workload_options()
    stopped = copy.deepcopy(pool.active)
    stopped['spec']['replicas'] = 0
    stopped['metadata'].setdefault('annotations', {})['loom.nebius/management-refresh-id'] = str(uuid4())
    unqualified = replace(request, resources=replace(request.resources,
        switch=replace(request.resources.switch, initial_stopped=stopped)))
    with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
        replace(bound, request=unqualified).workload_options()
    parent_path, switch_path = state / 'refresh.json', state / 'switch/cutover.json'
    paths = (parent_path, switch_path, anchor / (str(request.resources.switch.operation_id) + '.json'),
        state / 'post-migration-probe/stage.json', Path(operation['state_dir']) / 'completion.json')
    saved = {path: path.read_bytes() for path in paths}
    for path in paths:
        path.write_bytes(b'{}')
        with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
            bound.workload_options()
        path.write_bytes(saved[path])
    # A valid-looking target is not permission to activate before parent proofs.
    parent = json.loads(saved[parent_path])
    parent['activation_started'] = False
    parent_path.write_text(json.dumps(parent))
    with pytest.raises(ValueError, match='pool_refresh_projection_unqualified'):
        bound.workload_options()
    parent_path.write_bytes(saved[parent_path])
    assert bound.workload_options() == expected


@pytest.mark.timeout(420)
def test_pool_refresh_readback_accepts_real_updated_manager_but_not_other_drift(private_cutover):
    import ssl
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_pool_cutover_entry import qualify_pool_manager_database
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
    from scripts.ops.nebius_pool_origin_history import derive_management_history_target
    from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
    from scripts.ops.nebius_pool_role_fencing import role_fence_documents
    from tests.ops.test_nebius_management_refresh_install import run
    from tests.ops.test_nebius_management_refresh_predecessor import (
        history_credential,
        refresh_case,
    )
    from tests.ops.test_nebius_pool_cutover import (
        cutover_binding_inventory,
        writer_workload_inventory,
    )

    operation, _, root = private_cutover
    context, result = finish_cutover(operation)
    selector = PoolPredecessorV1(operation=operation, completion_sha256=result['completion_sha256'])
    pool = load_completed_pool(selector, original=root)
    case = refresh_case(root, pool, pool_baseline=selector.model_dump(mode='json'))
    assert run(case)['status'] == 'management_refreshed'
    bound = PoolManagerRefresh(root, pool, case[0], case[2])
    request, migration = context.request, context.request.fencing.retirement.migration
    current = {key: rows[0] for key, rows in bound.workload_options().items()}
    manager = _key(request.manager)
    inventories = cutover_binding_inventory.__wrapped__((request, context.tokens)) | writer_workload_inventory(request)
    replacements = {**current, **role_fence_documents(request.fencing)}
    for rows in inventories.values():
        for index, row in enumerate(rows):
            desired = replacements.get(_key(row))
            if desired is not None:
                rows[index] = copy.deepcopy(desired)
                rows[index]['metadata']['uid'] = row['metadata']['uid']
    authority = json.loads((Path(operation['state_dir']) / 'authority/stage.json').read_bytes())
    collections = {'Role': 'roles', 'ClusterRole': 'clusterroles', 'RoleBinding': 'rolebindings', 'ClusterRoleBinding': 'clusterrolebindings'}
    for item in authority['resources'].values():
        row = copy.deepcopy(item['observed'])
        row['metadata']['uid'] = item['uid']
        inventories[collections[row['kind']]].append(row)
    for rows in inventories.values():
        for row in rows:
            row['metadata'].setdefault('resourceVersion', '7')
    reads = []

    def respond(message):
        assert message.method == 'GET'
        resource = message.url.path.rsplit('/', 1)[-1]
        version = ('rbac.authorization.k8s.io/v1' if 'role' in resource else 'batch/v1' if resource in {'cronjobs', 'jobs'}
            else 'v1' if resource in {'pods', 'replicationcontrollers'} else 'apps/v1')
        kind = {'roles': 'Role', 'clusterroles': 'ClusterRole', 'rolebindings': 'RoleBinding', 'clusterrolebindings': 'ClusterRoleBinding',
            'deployments': 'Deployment', 'replicasets': 'ReplicaSet', 'statefulsets': 'StatefulSet', 'daemonsets': 'DaemonSet',
            'replicationcontrollers': 'ReplicationController', 'cronjobs': 'CronJob', 'jobs': 'Job', 'pods': 'Pod'}[resource]
        assert dict(message.url.params) == ({'limit': '100'} if not reads else
            {'limit': '100', 'resourceVersion': '7', 'resourceVersionMatch': 'Exact'})
        reads.append(resource)
        return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
            'metadata': {'resourceVersion': '7'}, 'items': inventories[resource]})

    def binding(actual, original):
        assert actual == migration and original == request.manager

    target = derive_management_history_target(original=root, predecessor=context.predecessor, credential=history_credential(root))
    references = []
    def credential(document, **kwargs):
        assert document == current[manager]
        assert kwargs == {'url_variable': 'LOOM_SVC_DB_URL', 'credential_uid': target.database.credential_uid,
            'credential_resource_version': target.database.credential_resource_version}
        references.append(True)
        return 'postgresql+psycopg://fixture:private-marker@loom-postgres.' + target.namespace + '.svc:5432/loom'
    history = SimpleNamespace(qualify_binding=binding, target=target, _get=lambda *_: copy.deepcopy(current[manager]),
        _database=lambda *_args, **_kwargs: {'metadata': {'uid': target.database.statefulset['metadata']['uid']}},
        _workload_database_url=credential,
        qualify_manager_database=lambda: pytest.fail('projection must not require a ready manager during recovery'))
    qualify_pool_manager_database(context, history, refresh=bound)
    assert len(references) == 2
    with HTTPSPoolCutoverAPI(request=request, tokens=context.tokens, migration=SimpleNamespace(),
            guards=SimpleNamespace(request=migration), checks=SimpleNamespace(), history=history,
            api_server='https://cluster.example', ssl_context=ssl.create_default_context(),
            state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']), refresh=bound) as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(respond))
        api.qualify_writer_bindings()
        for key in (manager, _key(request.services[0])):
            row, = (value for value in inventories['deployments'] if _key(value) == key)
            saved = copy.deepcopy(row)
            row['spec']['replicas'] = 2
            reads.clear()
            with pytest.raises(ValueError, match='workload_inventory_unqualified'):
                api.qualify_writer_bindings()
            row.clear()
            row.update(saved)


def pool_refresh_http(bound):
    """Real HTTP readers; only Kubernetes, database and cloud boundaries doubled."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    import httpx
    from scripts.ops.nebius_pool_gateway_retirement import gateway_retirement_options
    from scripts.ops.nebius_pool_role_fencing import role_fence_documents
    from scripts.ops.nebius_pool_role_restoration import restored_role_options
    from tests.ops.test_nebius_pool_cutover import cutover_binding_inventory

    pool = bound.qualify()
    context, operation = pool.context, pool.context.operation
    request, tokens = context.request, context.tokens
    migration = request.fencing.retirement.migration
    binding = migration.registration.binding
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
    objects = {_key(row): copy.deepcopy(row) for rows in
        cutover_binding_inventory.__wrapped__((request, tokens)).values() for row in rows}
    objects.update({key: copy.deepcopy(rows[0]) for key, rows in bound.workload_options().items()})
    objects.update({_key(guard.database.statefulset): copy.deepcopy(guard.database.statefulset)
        for guard in migration.guards if guard.database is not None})
    roles = restored_role_options(request, state=state, anchor=anchor)
    for original in request.fencing.originals:
        key = _key(original)
        row = copy.deepcopy(role_fence_documents(request.fencing)[key] if roles is None else roles[key][0])
        row['metadata']['uid'] = original['metadata']['uid']
        objects[key] = row
    authority = gateway_retirement_options(request, state=state, anchor=anchor)
    for phase in ('material', 'configuration', 'authority'):
        for key, item in json.loads((state / phase / 'stage.json').read_bytes())['resources'].items():
            row = copy.deepcopy(authority[key][0] if phase == 'authority' and authority is not None else item['observed'])
            row['metadata']['uid'] = item['uid']
            objects[key] = row
    namespaces = {binding.namespace: binding.namespace_uid, 'kube-system': binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}}
    collections = {'Role': 'roles', 'ClusterRole': 'clusterroles', 'RoleBinding': 'rolebindings',
        'ClusterRoleBinding': 'clusterrolebindings', 'Deployment': 'deployments', 'CronJob': 'cronjobs',
        'ReplicaSet': 'replicasets', 'StatefulSet': 'statefulsets', 'DaemonSet': 'daemonsets',
        'ReplicationController': 'replicationcontrollers', 'Job': 'jobs', 'Pod': 'pods',
        'ConfigMap': 'configmaps', 'Secret': 'secrets', 'ServiceAccount': 'serviceaccounts'}
    paths = {}
    for key, row in objects.items():
        row['metadata'].update(resourceVersion='7', generation=1)
        if row['kind'] == 'Deployment':
            row['status'] = {'observedGeneration': 1, 'replicas': row['spec']['replicas']}
        metadata = row['metadata']
        prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
        paths[prefix + ('/namespaces/' + metadata['namespace'] if metadata.get('namespace') else '')
            + '/' + collections[row['kind']] + '/' + metadata['name']] = key
    external = SimpleNamespace(objects=objects, calls=[], failure=None, reviews=[], namespaces=namespaces)
    gateway_user = f'system:serviceaccount:{binding.namespace}:loom-pool-gateway'

    def respond(message):
        external.calls.append(message)
        path = message.url.path
        if path == '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews':
            assert message.method == 'POST'
            user = message.headers['Impersonate-User']
            account_namespace, account = user.split(':')[2:]
            groups = message.headers.get_list('Impersonate-Group')
            assert groups == ['system:serviceaccounts', 'system:serviceaccounts:' + account_namespace, 'system:authenticated']
            namespace = json.loads(message.content)['spec']['namespace']
            external.reviews.append((user, namespace))
            rules, nonresource = [], []
            for row in external.objects.values():
                if row['kind'] not in {'RoleBinding', 'ClusterRoleBinding'}:
                    continue
                if row['kind'] == 'RoleBinding' and row['metadata']['namespace'] != namespace:
                    continue
                matches = any((subject['kind'] == 'ServiceAccount'
                    and (subject['namespace'], subject['name']) == (account_namespace, account))
                    or (subject['kind'] == 'User' and subject['name'] == user)
                    or (subject['kind'] == 'Group' and subject['name'] in groups) for subject in row['subjects'])
                if matches:
                    ref = row['roleRef']
                    role = external.objects[ref['kind'] + ':' + (row['metadata']['namespace'] if ref['kind'] == 'Role' else '-') + ':' + ref['name']]
                    for rule in role['rules']:
                        (nonresource if 'nonResourceURLs' in rule else rules).append(copy.deepcopy(rule))
            if external.failure == 'gateway_rights' and user == gateway_user:
                rules.append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
            if external.failure == 'legacy_rights' and user != gateway_user:
                rules.append({'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['get']})
            if external.failure == 'late_guard' and user == gateway_user:
                external.failure = 'guard'
            return httpx.Response(201, json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                'spec': {}, 'status': {'incomplete': False, 'resourceRules': rules, 'nonResourceRules': nonresource}})
        assert message.method == 'GET', 'refresh qualification must not persist a Kubernetes write'
        if path in {'/api/v1/namespaces/' + name for name in namespaces}:
            name = path.rsplit('/', 1)[-1]
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name,
                'uid': external.namespaces[name], 'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
        if path in paths:
            return httpx.Response(200, json=external.objects[paths[path]])
        resource = path.rsplit('/', 1)[-1]
        kind, = (kind for kind, plural in collections.items() if plural == resource)
        assert message.url.params['limit'] in {'100', '1000'}
        rows = [row for row in external.objects.values() if row['kind'] == kind]
        if '/namespaces/' in path:
            namespace = path.split('/namespaces/')[1].split('/')[0]
            rows = [row for row in rows if row['metadata']['namespace'] == namespace]
        version = ('rbac.authorization.k8s.io/v1' if 'role' in resource else 'batch/v1' if resource in {'cronjobs', 'jobs'}
            else 'v1' if resource in {'pods', 'replicationcontrollers'} else 'apps/v1')
        return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
            'metadata': {'resourceVersion': '7'}, 'items': rows})

    def qualify_binding(actual, manager):
        assert actual == migration and manager == request.manager

    def preflight(actual):
        assert actual == request
        if external.failure == 'provider':
            raise ValueError('private-provider-marker')

    def active():
        assert pool.completion.outcome == 'global'
        if external.failure in {'pool', 'credentials'}:
            raise ValueError('private-database-marker')

    def pool_state(action):
        assert action == 'observe'
        return 'closed' if external.failure == 'pool' else 'fenced' if pool.completion.outcome == 'legacy' else 'global'

    def machines(action):
        assert action == 'observe'
        return 'active' if external.failure == 'credentials' else 'revoked'

    def guard(target, action):
        assert target in migration.guards and action == 'observe'
        return 'fenced' if external.failure == 'guard' else 'open'

    def runtime_role(target, action):
        assert target in migration.guards and action == 'inspect'
        return {'status': 'invalid' if external.failure == 'database_role' else 'qualified'}

    def no_idle(*_args, **_kwargs):
        pytest.fail('opened owners can work: no startup, idle, local-drain or closed-mode probe')

    @contextmanager
    def connect():
        import ssl

        from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
        from scripts.ops.nebius_pool_refresh_live import HTTPSPoolRefreshAPI

        history = SimpleNamespace(qualify_binding=qualify_binding, qualify_active_pool=active,
            activation_pool=pool_state, machine_retirement=machines,
            recovery_pool_drained=lambda: external.failure != 'global_busy', qualify_closed_pool=no_idle)
        guards = SimpleNamespace(request=migration, activation_guard=guard, runtime_role=runtime_role,
            guard=no_idle, recovery_participant_drained=no_idle, cutover_readiness_page=no_idle)
        with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=SimpleNamespace(), guards=guards,
                checks=SimpleNamespace(preflight=preflight, qualify_quiescence=no_idle), history=history,
                api_server=bound.original.original_inputs.operator_connection.endpoint, ssl_context=ssl.create_default_context(),
                state_dir=state, anchor_dir=anchor, refresh=bound) as parent:
            for adapter in (parent, parent.fencing, parent.retirement):
                adapter.client.close()
                adapter.client = httpx.Client(base_url=parent.api_server, transport=httpx.MockTransport(respond))
            yield HTTPSPoolRefreshAPI(parent=parent), external
    return connect
