"""The fresh manager reserves application Pods, never a database per owner."""
from __future__ import annotations

import copy
import ssl
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_route import route as route
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import application_management_inputs as application_management_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def capacity_checks(route, inventory, tmp_path):
    from scripts.ops.nebius_development_management_prerequisites import HTTPSDevelopmentManagementPrerequisites

    request = route[1]
    credential = tmp_path / 'operator.json'
    credential.write_text('{}')
    credential.chmod(0o600)
    # This test owns only inventory sizing, not private input preparation. Actual
    # retained handoff and provider/publication checks have their own coverage.
    api = HTTPSDevelopmentManagementPrerequisites(settings=SimpleNamespace(),
        operator_cloud_credentials=credential, api_server=route[0].api_server,
        ssl_context=ssl.create_default_context(), token='operator')
    inventory['nodes'][0]['status']['allocatable'] = {'cpu': '128', 'memory': '256Gi',
        'ephemeral-storage': '512Gi', 'pods': '1000'}
    calls = []
    def handle(message):
        assert message.method == 'GET'
        resource = message.url.path.split('/')[-1]
        calls.append(resource)
        kinds = {'nodes': ('v1', 'Node'), 'pods': ('v1', 'Pod'), 'persistentvolumeclaims': ('v1', 'PersistentVolumeClaim'),
            'persistentvolumes': ('v1', 'PersistentVolume'), 'horizontalpodautoscalers': ('autoscaling/v2', 'HorizontalPodAutoscaler'),
            'replicationcontrollers': ('v1', 'ReplicationController'), 'deployments': ('apps/v1', 'Deployment'),
            'statefulsets': ('apps/v1', 'StatefulSet'), 'replicasets': ('apps/v1', 'ReplicaSet'),
            'daemonsets': ('apps/v1', 'DaemonSet'), 'jobs': ('batch/v1', 'Job'), 'cronjobs': ('batch/v1', 'CronJob')}
        version, kind = kinds[resource]
        return httpx.Response(200, json={'apiVersion': version, 'kind': kind + 'List',
            'metadata': {'resourceVersion': '7'}, 'items': inventory.get(resource, []) if resource != 'pods' else []})
    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handle))
    with api:
        yield api, request, inventory, calls


def test_fresh_manager_capacity_has_one_database_and_only_api_web_owner_reserve(capacity_checks, monkeypatch):
    from scripts.ops import nebius_development_management_prerequisites as module
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_capacity import qualify_platform_capacity

    api, request, _, calls = capacity_checks
    observed = []
    def fit(**values):
        observed.append(values)
        return qualify_platform_capacity(**values)
    monkeypatch.setattr(module, 'qualify_platform_capacity', fit)
    assert api.platform_capacity(request, render_installation(request)) == 10 * 1024
    values, = observed
    assert values['reserve'].storage_mib == 0 and values['reserve_pods'] > 0
    assert [(row['metadata']['namespace'], row['metadata']['name']) for row in values['planned']
        if row['kind'] == 'StatefulSet'] == [('loom-nebius-management-dev', 'loom-postgres')]
    assert 'persistentvolumeclaims' in calls


def test_capacity_without_an_eligible_platform_node_fails(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_capacity import ManagementCapacityError

    api, request, inventory, _ = capacity_checks
    inventory['nodes'] = []
    with pytest.raises(ManagementCapacityError):
        api.platform_capacity(request, render_installation(request))


def test_other_pending_claims_still_charge_provider_headroom(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation

    api, request, inventory, _ = capacity_checks
    inventory['persistentvolumeclaims'] = [{'metadata': {'namespace': 'foreign', 'name': 'pending'},
        'spec': {'resources': {'requests': {'storage': '3Gi'}}}, 'status': {'phase': 'Pending'}}]
    assert api.platform_capacity(request, render_installation(request)) == 13 * 1024


def test_live_manager_claim_is_not_reserved_twice(capacity_checks):
    from scripts.ops.nebius_development_management_install import render_installation

    api, request, inventory, _ = capacity_checks
    rendered = render_installation(request)
    stateful = next(row for row in rendered.files['20-database.yaml'] if row['kind'] == 'StatefulSet')
    inventory['statefulsets'] = [copy.deepcopy(stateful)]
    inventory['statefulsets'][0]['metadata']['uid'] = '20000000-0000-4000-8000-000000000001'
    inventory['persistentvolumeclaims'] = [{'metadata': {'namespace': request.binding.namespace, 'name': 'data-loom-postgres-0'},
        'spec': {'resources': {'requests': {'storage': '10Gi'}}}, 'status': {'phase': 'Bound', 'capacity': {'storage': '10Gi'}}}]
    assert api.platform_capacity(request, rendered) == 0


@pytest.mark.parametrize('blocked', [None, 'foundation', 'capacity', 'provider', 'backup', 'route'])
def test_connected_preflight_orders_every_barrier_and_never_masks_failure(capacity_checks, monkeypatch, blocked):
    from scripts.ops import nebius_development_management_prerequisites as module
    from scripts.ops.nebius_development_management_install import render_installation
    from scripts.ops.nebius_management_install import ManagementInstallError

    api, request, _, _ = capacity_checks
    rendered, retained, events = render_installation(request), object(), []
    def event(name):
        events.append(name)
        if blocked == name:
            raise RuntimeError('private-provider-payload')
    def foundation(selected):
        assert selected == request
        event('foundation')
        return retained
    def capacity(selected, manifests):
        assert selected == request and manifests == rendered
        event('capacity')
        return 10 * 1024
    async def provider(selected, stored, pending):
        assert selected == request and stored is retained and pending == 10 * 1024
        event('provider')
    def route(selected, *, installed):
        assert selected == request and not installed
        event('route')
    @contextmanager
    def backup(selected):
        assert selected == request
        def listing(**kwargs):
            assert kwargs == {'Bucket': request.deployment.backup_bucket, 'MaxKeys': 1}
            event('backup')
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}
        yield SimpleNamespace(list_objects_v2=listing)
    monkeypatch.setattr(api, 'foundation', foundation)
    monkeypatch.setattr(api, 'platform_capacity', capacity)
    monkeypatch.setattr(api, 'provider_and_publication', provider)
    monkeypatch.setattr(api, '_route', route)
    monkeypatch.setattr(module, 'backup_client', backup)
    sequence = ['foundation', 'capacity', 'provider', 'backup', 'route', 'foundation']
    if blocked is None:
        api.preflight(request, rendered)
        assert events == sequence
    else:
        with pytest.raises(ManagementInstallError) as error:
            api.preflight(request, rendered)
        assert 'private-provider-payload' not in str(error.value)
        assert events == sequence[:sequence.index(blocked) + 1]
