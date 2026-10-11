"""Shared handoff census lifecycle regressions."""
from __future__ import annotations

from tests.ops.test_nebius_shared_pool_handoff import (
    ROOT as ROOT,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    Path as Path,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _exercise_forward_writer_census as _exercise_forward_writer_census,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    _key as _key,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_material as application_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    cloud as cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    connected as connected,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    copy as copy,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    dev_pool_inputs as dev_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    entry as entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    handoff as handoff,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    installation as installation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    inventory as inventory,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    live as live,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    material as material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    preflight as preflight,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publication as publication,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    published_source as published_source,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytest as pytest,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    pytestmark as pytestmark,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    retained as retained,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    route as route,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_guard_transport as shared_guard_transport,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_operation as shared_operation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    shared_runtime as shared_runtime,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_history as staging_history,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    staging_observation as staging_observation,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    tls_material as tls_material,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    uuid4 as uuid4,
)


@pytest.mark.timeout(420)
@pytest.mark.parametrize('forward_census', [False, True, 'recovery', 'participant', 'preparation'],
    ids=['initial', 'forward', 'recovery', 'participant', 'preparation'])
def test_shared_workload_census_binds_current_roots_and_rejects_unaccounted_writers(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, shared_operation, forward_census,
        publisher_cloud):
    """A historical template or a stopped clone must not hide a live writer."""
    import json
    import ssl

    import httpx
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory
    from tests.ops.test_nebius_pool_cutover import writer_descendant

    request, parent = shared_runtime
    _, documents, _ = runtime_http_inventory(request, parent, completed_pool[3].server, handoff[2])
    kinds = {'Deployment': 'deployments', 'StatefulSet': 'statefulsets', 'CronJob': 'cronjobs',
        'DaemonSet': 'daemonsets', 'ReplicaSet': 'replicasets', 'ReplicationController': 'replicationcontrollers',
        'Job': 'jobs', 'Pod': 'pods'}
    rbac_kinds = {'Role': 'roles', 'ClusterRole': 'clusterroles', 'RoleBinding': 'rolebindings',
        'ClusterRoleBinding': 'clusterrolebindings'}
    kinds.update(rbac_kinds)
    roots = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.request.platform_consumers,
        *staging_history.completion.workloads.values(), *documents.values(), *staging_observation['resources'])
        if row['kind'] in {'Deployment', 'StatefulSet', 'CronJob'}}
    inventory = {plural: [] for plural in kinds.values()}
    authority = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.original.retained.values(),
        *staging_history.context.predecessor.retained.values(), *documents.values(),
        *staging_observation['resources'], *staging_history.context.request.platform_authority.resources)
        if row['kind'] in rbac_kinds}
    gateway_stage = json.loads((Path(staging_history.context.operation['state_dir']) / 'authority/stage.json').read_bytes())
    retired_gateway_roles = set()
    for key, item in gateway_stage['resources'].items():
        row = copy.deepcopy(item['observed'])
        row['metadata']['uid'] = item['uid']
        if row['kind'] == 'Role':
            retired_gateway_roles.add(key)
            for rule in row['rules']:
                rule['verbs'] = [verb for verb in rule['verbs'] if verb not in {'create', 'delete'}]
        authority[key] = row
    assert retired_gateway_roles
    for row in authority.values():
        row['metadata']['resourceVersion'] = '401'
        inventory[kinds[row['kind']]].append(row)
    for row in roots.values():
        row['metadata']['resourceVersion'] = '401'
        inventory[kinds[row['kind']]].append(row)
    controller = roots['Deployment:' + staging_observation['namespace'] + ':loom-control-plane']
    replica = writer_descendant(controller, 'ReplicaSet')
    inventory['replicasets'].append(replica)
    inventory['pods'].append(writer_descendant(replica, 'Pod'))
    # Do not hide the installation's real, ownerless one-shot Jobs. They keep
    # their recorded UID/template; only API status and child Pods are supplied.
    jobs = {_key(row): copy.deepcopy(row) for row in (*staging_history.context.original.retained.values(),
        *staging_history.context.predecessor.retained.values(), *documents.values()) if row['kind'] == 'Job'}
    management_jobs = [row for row in jobs.values() if row['metadata']['namespace'] == 'loom-nebius-management-dev']
    assert any(row['metadata']['name'].startswith('loom-management-migrate-') for row in management_jobs)
    assert any(row['metadata']['name'].startswith('loom-management-backup-') for row in management_jobs)
    for job in jobs.values():
        job['metadata']['resourceVersion'] = '401'
        job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'active': 0, 'succeeded': 1}
        pod = writer_descendant(job, 'Pod')
        pod['metadata']['labels'] = copy.deepcopy(job['spec']['template']['metadata']['labels'])
        pod['status'] = {'phase': 'Succeeded', **{states: [{'name': row['name'],
            'state': {'terminated': {'exitCode': 0}}} for row in pod['spec'].get(containers, [])]
            for containers, states in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses'))}}
        inventory['jobs'].append(job)
        inventory['pods'].append(pod)
    calls, mode = [], {'revision': '500'}

    def respond(message):
        calls.append(message)
        assert message.method == 'GET'
        if message.url.path == '/api/v1/namespaces/kube-system':
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                'name': 'kube-system', 'uid': request.database.manager.retained.request.registration.binding.kube_system_uid}})
        plural = message.url.path.rsplit('/', 1)[-1]
        kind, = (kind for kind, name in kinds.items() if name == plural)
        version = message.url.params.get('resourceVersion')
        if plural != 'roles':
            assert version == '500' and message.url.params['resourceVersionMatch'] == 'Exact'
        if plural == 'pods' and mode.get('during_read') is not None:
            mode.pop('during_read')()
        return httpx.Response(200, json={'apiVersion': 'v1' if plural in {'pods', 'replicationcontrollers'}
            else 'batch/v1' if plural in {'jobs', 'cronjobs'} else 'rbac.authorization.k8s.io/v1'
            if kind in rbac_kinds else 'apps/v1', 'kind': kind + 'List',
            'metadata': {'resourceVersion': mode['revision'] if plural == 'pods' else '500', 'continue': ''},
            'items': inventory[plural]})

    with ManagementKubernetesTransport(api_server=request.database.foundation.inputs.config['kubernetes_api_server'],
            ssl_context=ssl.create_default_context()) as transport:
        transport.client.close()
        transport.client = type(completed_pool[3].api.client)(base_url=transport.api_server,
            transport=httpx.MockTransport(respond))
        if forward_census:
            _exercise_forward_writer_census(shared_operation, transport, inventory, kinds, calls, mode,
                recovery=forward_census == 'recovery',
                participant=forward_census if forward_census in {'participant', 'preparation'} else False,
                publisher_cloud=publisher_cloud)
            return
        from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_writers

        def observe():
            return observe_shared_pool_writers(runtime=request, observation=staging_observation,
                legacy=staging_history, repo_root=ROOT, transport=transport)

        result = observe()
        assert result.resource_version == '500'
        assert result.workloads.keys() == roots.keys()
        assert result.workloads[_key(controller)] == controller
        gateway, = (row for row in staging_history.completion.workloads.values()
            if row['metadata']['name'] == 'loom-pool-gateway')
        assert result.workloads[_key(gateway)]['spec']['replicas'] == 0
        original = copy.deepcopy(inventory)
        # Completed Jobs and Pods may have been garbage-collected normally.
        # No absent historical object becomes a required deployment target.
        job_uids = {row['metadata']['uid'] for row in inventory['jobs']}
        inventory['jobs'] = []
        inventory['pods'] = [row for row in inventory['pods']
            if not any(owner['uid'] in job_uids for owner in row['metadata'].get('ownerReferences', []))]
        assert observe().workloads == result.workloads
        original_capture = copy.deepcopy(staging_observation['resources'])
        for damage in ('clone', 'cronjob', 'root_uid', 'old_template', 'revived_gateway', 'missing_root', 'revision',
                'foreign_writer', 'foreign_cron_writer', 'group_writer', 'foreign_subject', 'revived_gateway_role',
                'cross_namespace', 'missing_role', 'captured_writer', 'unrecorded_job', 'job_uid', 'job_template',
                'job_nonterminal', 'job_active', 'job_pod_running', 'job_pod_sidecar', 'job_pod_unowned',
                'job_absent_with_pod'):
            inventory.clear()
            inventory.update(copy.deepcopy(original))
            staging_observation['resources'] = copy.deepcopy(original_capture)
            if damage.startswith('job_') or damage == 'unrecorded_job':
                job = next(row for row in inventory['jobs'] if row['metadata']['namespace'] == 'loom-nebius-management-dev')
                pod, = (row for row in inventory['pods']
                    if row['metadata'].get('ownerReferences', [{}])[0].get('uid') == job['metadata']['uid'])
                if damage == 'unrecorded_job':
                    job['metadata']['name'] += '-unrecorded'
                    pod['metadata']['ownerReferences'][0]['name'] = job['metadata']['name']
                elif damage == 'job_uid':
                    uid = str(uuid4())
                    job['metadata']['uid'] = uid
                    job['spec']['selector']['matchLabels']['batch.kubernetes.io/controller-uid'] = uid
                    for labels in (job['spec']['template']['metadata']['labels'], pod['metadata']['labels']):
                        for label in ('controller-uid', 'batch.kubernetes.io/controller-uid'):
                            labels[label] = uid
                    pod['metadata']['ownerReferences'][0]['uid'] = uid
                elif damage == 'job_template':
                    job['spec']['template']['spec']['containers'][0]['command'] = ['sleep', 'infinity']
                elif damage == 'job_nonterminal':
                    job['status']['conditions'] = []
                elif damage == 'job_active':
                    job['status']['active'] = 1
                elif damage == 'job_pod_running':
                    pod['status']['phase'] = 'Running'
                elif damage == 'job_pod_sidecar':
                    pod['spec']['initContainers'] = [{'name': 'live-sidecar', 'restartPolicy': 'Always', 'image': 'test'}]
                    pod['status']['initContainerStatuses'] = [{'name': 'live-sidecar', 'state': {'running': {}}}]
                elif damage == 'job_absent_with_pod':
                    inventory['jobs'].remove(job)
                else:
                    pod['metadata']['ownerReferences'] = []
            elif damage == 'captured_writer':
                namespace = staging_history.context.inputs.installation.participants[0].execution_namespace.name
                clone = copy.deepcopy(controller)
                clone['metadata'].update(name='captured-foreign-writer', namespace=namespace, uid=str(uuid4()))
                clone['spec']['template']['spec']['serviceAccountName'] = 'captured-foreign-writer'
                role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
                    'metadata': {'name': 'captured-foreign-writer', 'namespace': namespace,
                        'uid': str(uuid4()), 'resourceVersion': '401'},
                    'rules': [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['create']}]}
                binding = {'apiVersion': role['apiVersion'], 'kind': 'RoleBinding',
                    'metadata': {'name': 'captured-foreign-writer', 'namespace': namespace,
                        'uid': str(uuid4()), 'resourceVersion': '401'},
                    'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'captured-foreign-writer'},
                    'subjects': [{'kind': 'ServiceAccount', 'name': 'captured-foreign-writer', 'namespace': namespace}]}
                # A capture is observation, not authority: adding the same
                # unrendered writer to both inputs must not legitimize it.
                for row in (clone, role, binding):
                    staging_observation['resources'].append(copy.deepcopy(row))
                    inventory[kinds[row['kind']]].append(row)
            elif damage in {'foreign_writer', 'foreign_cron_writer', 'group_writer', 'cross_namespace'}:
                role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
                    'metadata': {'name': 'unexpected-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
                    'rules': [{'apiGroups': ['batch'], 'resources': ['cronjobs' if damage == 'foreign_cron_writer' else 'jobs'],
                        'verbs': ['create']}]}
                binding = {'apiVersion': role['apiVersion'], 'kind': 'ClusterRoleBinding',
                    'metadata': {'name': 'unexpected-writer', 'uid': str(uuid4()), 'resourceVersion': '401'},
                    'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': 'unexpected-writer'},
                    'subjects': [{'kind': 'ServiceAccount', 'name': 'foreign-account', 'namespace': 'outside-pool'}]}
                if damage == 'group_writer':
                    binding['subjects'] = [{'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Group', 'name': 'system:authenticated'}]
                elif damage == 'cross_namespace':
                    binding['kind'] = 'RoleBinding'
                    binding['metadata']['namespace'] = 'outside-pool'
                    actuator, = (row for row in roots.values() if row['metadata']['name'] == 'loom-execution-actuator'
                        and row['metadata']['namespace'] == staging_history.context.inputs.installation.participants[0].execution_namespace.name)
                    binding['subjects'] = [{'kind': 'ServiceAccount', 'name': 'loom-execution-actuator',
                        'namespace': actuator['metadata']['namespace']}]
                inventory['clusterroles'].append(role)
                inventory[kinds[binding['kind']]].append(binding)
            elif damage == 'foreign_subject':
                binding = inventory['rolebindings'][0]
                binding['subjects'].append({'kind': 'ServiceAccount', 'name': 'foreign-account', 'namespace': 'outside-pool'})
            elif damage == 'revived_gateway_role':
                role = next(row for row in inventory['roles'] if _key(row) in retired_gateway_roles)
                role['rules'][0]['verbs'].append('create')
            elif damage == 'missing_role':
                inventory['roles'].pop()
            elif damage == 'clone':
                clone = copy.deepcopy(inventory['pods'][0])
                clone['metadata'].update(name='unexpected-writer', uid=str(uuid4()), ownerReferences=[])
                inventory['pods'].append(clone)
            elif damage == 'cronjob':
                rogue = copy.deepcopy(next(row for row in inventory['cronjobs']
                    if row['metadata']['name'] == 'loom-execution-capacity-collector'))
                rogue['metadata'].update(name='unexpected-schedule', uid=str(uuid4()))
                rogue['spec']['suspend'] = True
                rogue['spec']['jobTemplate']['spec']['template']['spec']['serviceAccountName'] = 'foreign-account'
                inventory['cronjobs'].append(rogue)
            elif damage == 'missing_root':
                inventory['deployments'] = [row for row in inventory['deployments'] if _key(row) != _key(controller)]
            elif damage == 'revision':
                mode['revision'] = '501'
            else:
                key = _key(gateway) if damage == 'revived_gateway' else _key(controller)
                row, = (row for row in inventory['deployments'] if _key(row) == key)
                if damage == 'root_uid':
                    row['metadata']['uid'] = str(uuid4())
                elif damage == 'old_template':
                    row['spec'] = copy.deepcopy(staging_history.completion.workloads[key]['spec'])
                    assert row['spec'] != controller['spec']
                else:
                    row['spec']['replicas'] = 1
            with pytest.raises(ValueError, match='shared pool writer census unqualified'):
                observe()
            mode['revision'] = '500'
        staging_observation['resources'] = original_capture
        assert calls and all(row.method == 'GET' for row in calls)
