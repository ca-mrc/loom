"""Real scalar Kubernetes startup; initial SQL/closure qualification is a fixture.

This proves retained UID/template/CAS and lost-response behavior, not healthy
successors, installed credentials, live admission or concurrent-owner acceptance.
"""
from __future__ import annotations

import asyncio
import copy
import os
import ssl
import time
import traceback
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key, _snapshot

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_pool_cutover import CutoverAPI
from tests.ops.test_nebius_pool_cutover import collector_inputs as collector_inputs
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_cutover import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_dormant import dormant_consumer
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1', reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(300)
async def test_actual_startup_preserves_uid_templates_and_resolves_lost_committed_reply(cutover_inputs, tmp_path):
    from kubernetes.client.exceptions import ApiException
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
    from scripts.ops.nebius_pool_retirement import retirement_documents
    from scripts.ops.nebius_pool_startup import stage_pool_startup
    from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI

    from loom_service.pool_management.installation import PoolInstallation

    request, tokens = cutover_inputs
    dormant = dormant_consumer(request.fencing.retirement)
    request = replace(request, fencing=replace(request.fencing,
        retirement=replace(request.fencing.retirement, dormant_consumers=(dormant,))))
    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        binding = request.fencing.retirement.migration.registration.binding
        names = {binding.namespace, *(guard.namespace for guard in request.fencing.retirement.migration.guards),
            *(ns.name for row in request.fencing.retirement.migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace))}
        namespaces = {}
        for name in sorted(names):
            value = await asyncio.to_thread(core.create_namespace, {'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': name, 'labels': {'loom.nebius/management-installation': binding.installation_id,
                    'pod-security.kubernetes.io/enforce': 'restricted'}}})
            namespaces[name] = value.metadata.uid
        kube_system = await asyncio.to_thread(core.read_namespace, 'kube-system')
        binding = replace(binding, namespace_uid=namespaces[binding.namespace], kube_system_uid=kube_system.metadata.uid)

        async def install(document, *, replace_existing=False):
            value = _snapshot(document)
            resource = {'Deployment': 'deployments', 'CronJob': 'cronjobs', 'Role': 'roles',
                'RoleBinding': 'rolebindings', 'ConfigMap': 'configmaps'}[value['kind']]
            path = ('/api/v1' if value['apiVersion'] == 'v1' else '/apis/' + value['apiVersion'])
            path += '/namespaces/' + value['metadata']['namespace'] + '/' + resource
            if replace_existing:
                path += '/' + value['metadata']['name']
            # Bootstrap the complete stopped fixture, not a merge that retains
            # old RollingUpdate defaults after the target selects Recreate.
            expected_uid = value['metadata'].get('uid')
            for attempt in range(10):
                if replace_existing:
                    current = await asyncio.to_thread(core.api_client.call_api, path, 'GET', response_type='object',
                        auth_settings=['BearerToken'], _return_http_data_only=True)
                    expected_uid = expected_uid or current['metadata']['uid']
                    assert current['metadata']['uid'] == expected_uid
                    value['metadata'].update(uid=expected_uid, resourceVersion=current['metadata']['resourceVersion'])
                try:
                    return await asyncio.to_thread(core.api_client.call_api, path, 'PUT' if replace_existing else 'POST',
                        body=value, response_type='object', auth_settings=['BearerToken'], _return_http_data_only=True,
                        header_params={'Content-Type': 'application/json'})
                except ApiException as exc:
                    # Deployment-controller status changes can invalidate the
                    # bootstrap RV. This is before the startup CAS under test.
                    if not replace_existing or exc.status != 409 or attempt == 9:
                        raise
                    await asyncio.sleep(0.05)

        originals = [*retirement_documents(request.fencing.retirement).values(), request.manager, *request.services]
        installed = {}
        for original in originals:
            installed[_key(original)] = await install(original)
        roles = tuple([await install(row) for row in request.fencing.originals])
        collector_config = await install(request.collector_config)
        migration = request.fencing.retirement.migration
        spec = migration.registration.spec.model_dump(mode='json')
        for participant in spec['participants']:
            for field in ('execution_namespace', 'build_namespace'):
                participant[field]['uid'] = namespaces[participant[field]['name']]
        migration = replace(migration,
            registration=replace(migration.registration, binding=binding, spec=PoolInstallation.model_validate(spec)),
            guards=tuple(replace(guard, namespace_uid=UUID(namespaces[guard.namespace]), controller=installed[_key(guard.controller)]) for guard in migration.guards))
        retirement = replace(request.fencing.retirement, migration=migration,
            actuators=tuple(installed[_key(row)] for row in request.fencing.retirement.actuators),
            collectors=tuple(installed[_key(row)] for row in request.fencing.retirement.collectors),
            dormant_consumers=(replace(dormant, actuator=installed[_key(dormant.actuator)], collector=installed[_key(dormant.collector)]),))
        request = replace(request, fencing=replace(request.fencing, retirement=retirement, originals=roles),
            manager=installed[_key(request.manager)], services=tuple(installed[_key(row)] for row in request.services),
            collector_config=collector_config)
        closed = CutoverAPI(request)
        guards = SimpleNamespace(request=migration, guard=lambda target, action: {'status': 'held'},
            runtime_role=lambda target, action: {'status': 'qualified'})
        history = SimpleNamespace(qualify_binding=closed.qualify_binding, qualify_closed_pool=lambda: None)
        configuration = core.api_client.configuration
        tls = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
        tls.load_cert_chain(configuration.cert_file, configuration.key_file)
        state, anchor = tmp_path / 'cutover', tmp_path / 'cutover-anchor'
        with HTTPSPoolCutoverAPI(request=request, tokens=tokens, migration=closed.migration, guards=guards,
                checks=closed, history=history, api_server=configuration.host, ssl_context=tls, state_dir=state, anchor_dir=anchor) as parent:
            # The initial SQL closure/authority proof is the test's boundary.
            # Child CREATE/read and workload dry-run/default receipts are real.
            def default_workload(key, before, desired):
                assert before == closed.documents[key]
                for attempt in range(10):
                    current = parent.read_workload(key)
                    assert current['metadata']['uid'] == before['metadata']['uid']
                    value = _snapshot(desired)
                    value['metadata']['resourceVersion'] = current['metadata']['resourceVersion']
                    response = parent.client.put(parent._workload_path(key) + '?dryRun=All', json=value)
                    # Controller status writes can race this fixture's dry-run
                    # defaulting too. The startup CAS under test is unchanged.
                    if response.status_code == 409 and attempt < 9:
                        time.sleep(0.05)
                        continue
                    response.raise_for_status()
                    return response.json()

            def retain_defaulted_workload(key, before, desired):
                value = default_workload(key, before, desired)
                value['metadata'].update(uid=before['metadata']['uid'], resourceVersion=str(int(before['metadata']['resourceVersion']) + 1))
                closed.documents[key] = value
                return True

            closed.preview_workload = default_workload
            closed.patch_workload = retain_defaulted_workload
            closed.resources = parent
            assert (await asyncio.to_thread(stage_pool_cutover, request=request, tokens=tokens,
                api=closed, state_dir=state, anchor_dir=anchor))['status'] == 'pool_runtime_staged_closed'
            for row in closed.documents.values():
                await install(row, replace_existing=True)
            for row in closed.fencing.roles.values():
                await install(row, replace_existing=True)
            parent.preflight = closed.preflight
            parent.fencing.verify_readonly = closed.fencing.verify_readonly
            api = HTTPSPoolStartupAPI(parent=parent)
            before = {key: await asyncio.to_thread(api.read_workload, key) for key in api.closed}
            from scripts.ops.nebius_management_switch import _stable

            def changed_paths(left, right, path=''):
                if isinstance(left, dict) and isinstance(right, dict):
                    return [value for key in left.keys() | right.keys()
                        for value in changed_paths(left.get(key), right.get(key), path + '/' + key)]
                if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
                    return [value for index, (a, b) in enumerate(zip(left, right, strict=True))
                        for value in changed_paths(a, b, path + '/' + str(index))]
                return [] if left == right else [path]

            differences = {key: changed_paths(_stable(before[key]), _stable(api.closed[key])) for key in before}
            assert not any(differences.values()), {key: paths for key, paths in differences.items() if paths}
            responses, lost = [], []

            def lose_first_committed_start(response):
                if response.request.method == 'PATCH' and not response.request.url.query:
                    responses.append((response.request.url.path, response.status_code))
                    if response.status_code == 200 and not lost:
                        lost.append(response.request.url.path)
                        raise httpx.ReadTimeout('simulated lost reply after actual commit')

            parent.client.event_hooks['response'].append(lose_first_committed_start)
            deadline = time.monotonic() + 150
            while True:
                try:
                    result = await asyncio.to_thread(stage_pool_startup, request=request, api=api, state_dir=state, anchor_dir=anchor)
                except Exception as error:
                    locations = []
                    while error is not None:
                        locations.append((type(error).__name__, [(frame.filename.rsplit('/', 1)[-1], frame.lineno, frame.name)
                            for frame in traceback.extract_tb(error.__traceback__)[-3:]]))
                        error = error.__context__
                    pytest.fail('startup failed (locations only; no secret payloads): ' + repr(locations))
                if result['status'] == 'pool_startup_staged_closed':
                    break
                # Controllers may update resourceVersion during the dry run.
                # Only actual definite rejection allows another mutation attempt.
                assert result['status'] == 'pending_startup_update' and time.monotonic() < deadline
            assert result['admission_open'] is False and result['runtime_verified'] is False
            assert len(lost) == 1 and len([row for row in responses if row[1] == 200]) == 12
            for key, original in before.items():
                current = await asyncio.to_thread(api.read_workload, key)
                expected = copy.deepcopy(original['spec'])
                if key in api.targets:
                    field = 'suspend' if original['kind'] == 'CronJob' else 'replicas'
                    expected[field] = False if field == 'suspend' else 1
                assert current['metadata']['uid'] == original['metadata']['uid'] and current['spec'] == expected
            responses.clear()
            assert await asyncio.to_thread(stage_pool_startup, request=request, api=api, state_dir=state, anchor_dir=anchor) == result
            assert responses == []
            # Actual API-server resolution of the fixed gateway identity. No
            # token issuance, workload-health claim or grant through this proof.
            await asyncio.to_thread(api.qualify_gateway_authority)
            await asyncio.to_thread(core.create_namespace, {'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': 'outside-pool'}})
            await install({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
                'metadata': {'name': 'extra-gateway', 'namespace': 'outside-pool'},
                'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['patch'], 'resourceNames': ['hidden-job']}]})
            await install({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
                'metadata': {'name': 'extra-gateway', 'namespace': 'outside-pool'},
                'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'extra-gateway'},
                'subjects': [{'kind': 'ServiceAccount', 'name': 'loom-pool-gateway', 'namespace': binding.namespace}]})
            with pytest.raises(ValueError, match='pool_startup_gateway_authority_unqualified'):
                await asyncio.to_thread(api.qualify_gateway_authority)
    finally:
        await asyncio.to_thread(container.stop)
