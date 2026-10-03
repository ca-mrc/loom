"""Native refresh cutover and actual rendered compatibility/migration Jobs.

These disposable checks do not establish installed cloud/public acceptance.
"""
from __future__ import annotations

import base64
import copy
import json
import os
import ssl
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from alembic.config import Config
from alembic.script import ScriptDirectory

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_management_refresh import refresh_request as refresh_request
from tests.ops.test_nebius_management_refresh_predecessor import (
    application_material as application_material,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    checks as checks,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    cloud as cloud,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    installation as installation,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    material as material,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    private_upgrade as private_upgrade,
)
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

# These fixtures bootstrap the current source tree, not a historical release.
CURRENT_REVISION = ScriptDirectory.from_config(
    Config(str(Path(__file__).resolve().parents[2] / "database/migrations/alembic.ini"))
).get_current_head()
assert CURRENT_REVISION is not None

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(240)
def test_completed_refresh_with_native_api_quantities_loads_as_next_predecessor(completed_upgrade, monkeypatch):
    """Exercise the API quantity spelling across the real receipt/journal boundary."""
    from kubernetes import client
    from scripts.ops.nebius_ingress_stage import _snapshot

    from tests.ops.test_nebius_management_refresh_predecessor import (
        complete_refresh,
        load,
        load_refresh,
    )
    from tests.ops.test_nebius_management_refresh_switch import API

    root = load(completed_upgrade[0])
    cluster = _start_k3s(ephemeral_storage_floor='2Gi')
    try:
        _, core, _ = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        core.create_namespace({'metadata': {'name': root.deployment.namespace}})
        desired = API.desired

        def native_desired(self, action):
            document = desired(self, action)
            observed = core.api_client.sanitize_for_serialization(apps.create_namespaced_deployment(
                root.deployment.namespace, _snapshot(document), dry_run='All'))
            # The fixture's initial installation is synthetic; retain its
            # defaults and use only the actual API's resource serialization.
            for field in ('containers', 'initContainers'):
                for target, source in zip(document['spec']['template']['spec'].get(field, []),
                        observed['spec']['template']['spec'].get(field, []), strict=True):
                    target['resources'] = source['resources']
            return document

        monkeypatch.setattr(API, 'desired', native_desired)
        selector, case = complete_refresh(root)
        receipt = json.loads((case[2] / 'completion.json').read_text())
        requests = receipt['active']['spec']['template']['spec']['containers'][0]['resources']['requests']
        assert requests['cpu'] == '100m'
        assert requests['memory'] == '268435456'
        before = {path: path.read_bytes() for path in case[2].parent.rglob('*.json')}
        predecessor = load_refresh(selector, root)
        assert predecessor.active['metadata']['uid'] == root.active['metadata']['uid']
        assert {path: path.read_bytes() for path in before} == before
        assert not apps.list_namespaced_deployment(root.deployment.namespace).items
    finally:
        cluster.stop()


@pytest.mark.timeout(240)
def test_repeat_refresh_preserves_retained_identity_and_observes_native_drain(refresh_request, tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_ingress_stage import _snapshot
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh_live import HTTPSManagementRefreshSwitchAPI
    from scripts.ops.nebius_management_refresh_resources import (
        HTTPSManagementRefreshResourcesAPI,
        ManagementRefreshResourcesRequest,
        refresh_resources_ready,
        stage_refresh_resources,
    )
    from scripts.ops.nebius_management_refresh_switch import (
        ManagementRefreshSwitchRequest,
        switch_refresh,
    )

    from loom_service.environment_management.deployment import ManagementDeployment

    cluster = _start_k3s(ephemeral_storage_floor='2Gi')
    try:
        _, core, _ = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        endpoint = 'https://127.0.0.1:' + str(cluster.get_exposed_port(6443))
        kube = yaml.safe_load(cluster.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            kube['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = kube['users'][0]['user']
        certificate, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        certificate.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)

        def local(deployment):
            raw = deployment.model_dump(mode='json')
            raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
            foundation = raw['installation']['foundation']
            platform = json.loads(foundation['platform_config_json'])
            platform['kubernetes_api_server'] = endpoint
            foundation['platform_config_json'] = json.dumps(platform)
            return ManagementDeployment.model_validate(raw)

        request = replace(refresh_request, before=local(refresh_request.before), after=local(refresh_request.after))
        namespace = request.after.namespace
        ns = core.create_namespace({'metadata': {'name': namespace, 'labels': {
            'loom.nebius/management-installation': str(request.after.installation_id),
            'pod-security.kubernetes.io/enforce': 'restricted'}}})
        shared = core.create_namespace({'metadata': {'name': request.after.installation.applications.shared.platform_namespace}})
        binding = ManagementBinding(str(request.after.installation_id), namespace, ns.metadata.uid,
            core.read_namespace('kube-system').metadata.uid)
        for name in ('loom-platform', 'loom-application-provisioner'):
            core.create_namespaced_service_account(namespace, {'metadata': {'name': name}, 'automountServiceAccountToken': False})
        retained = core.create_namespaced_secret(namespace, {'metadata': {'name': 'retained-test-material'},
            'immutable': True, 'stringData': {'test': 'disposable-only'}})
        apps.create_namespaced_deployment(namespace, _snapshot(request.active))
        prior_states = {}
        for iteration in range(2):
            (tmp_path / str(iteration)).mkdir(mode=0o700)
            deadline = time.monotonic() + 30
            while not core.list_namespaced_pod(namespace, label_selector='app=loom-service').items:
                assert time.monotonic() < deadline, 'disposable Deployment did not create a Pod'
                time.sleep(0.2)
            active = core.api_client.sanitize_for_serialization(apps.read_namespaced_deployment('loom-service', namespace))
            if iteration:
                candidate, profile = copy.deepcopy(request.candidate), copy.deepcopy(request.profile)
                candidate['images']['service']['image_ref'] = candidate['images']['service']['image_ref'].split('@')[0] + '@sha256:' + '8' * 64
                profile['task_image_ref'] = candidate['images']['service']['image_ref']
                request = replace(request, before=request.after, candidate=candidate, profile=profile)
            request = replace(request, active=active)
            switch = ManagementRefreshSwitchRequest(request, uuid4())
            resources = ManagementRefreshResourcesRequest(switch, binding, shared.metadata.uid, '0168', '0168')
            for phase in ('config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe'):
                with HTTPSManagementRefreshResourcesAPI(request=resources, phase=phase, api_server=endpoint, ssl_context=trust) as api:
                    args = dict(request=resources, phase=phase, api=api, state_dir=tmp_path / str(iteration) / phase)
                    receipt = stage_refresh_resources(**args)
                    assert stage_refresh_resources(**args) == receipt
                    assert refresh_resources_ready(**args) is (phase == 'config')
            with HTTPSManagementRefreshSwitchAPI(request=switch, binding=binding, shared_namespace_uid=shared.metadata.uid,
                    api_server=endpoint, ssl_context=trust, activation_check=lambda _request: True) as api:
                args = dict(request=switch, api=api, state_dir=tmp_path / str(iteration) / 'switch')
                while not switch_refresh(**args, activate=False):
                    assert time.monotonic() < deadline, 'native Deployment/ReplicaSet drain did not converge'
                    time.sleep(0.2)
                assert not core.list_namespaced_pod(namespace, label_selector='app=loom-service').items
                assert all(row.spec.replicas == 0 and row.status.observed_generation >= row.metadata.generation
                    for row in apps.list_namespaced_replica_set(namespace, label_selector='app=loom-service').items)
                if iteration == 0:
                    stopped = api.read()
                    initial = _snapshot(stopped)
                    initial['metadata']['uid'] = stopped['metadata']['uid']
                    successor = replace(switch, operation_id=uuid4(), initial_stopped=initial)
                    with HTTPSManagementRefreshSwitchAPI(request=successor, binding=binding,
                            shared_namespace_uid=shared.metadata.uid, api_server=endpoint, ssl_context=trust,
                            activation_check=lambda _request: True) as successor_api:
                        next_args = dict(request=successor, api=successor_api,
                            state_dir=tmp_path / str(iteration) / 'successor-switch')
                        while not switch_refresh(**next_args, activate=False):
                            assert time.monotonic() < deadline, 'native stopped adoption did not converge'
                            time.sleep(0.2)
                        adopted = successor_api.read()
                        assert adopted['spec'] == stopped['spec']
                        assert adopted['metadata']['uid'] == stopped['metadata']['uid']
                        assert adopted['metadata']['annotations']['loom.nebius/management-refresh-id'] == str(successor.operation_id)
                        with pytest.raises(ValueError, match='cutover unresolved'):
                            switch_refresh(**args, activate=False)
                        assert not core.list_namespaced_pod(namespace, label_selector='app=loom-service').items
                        assert switch_refresh(**next_args, activate=True) is True
                        assert switch_refresh(**next_args, activate=True) is True
                else:
                    assert switch_refresh(**args, activate=True) is True
                    assert switch_refresh(**args, activate=True) is True
                assert api.read()['metadata']['uid'] == active['metadata']['uid']
            assert core.read_namespaced_secret('retained-test-material', namespace).metadata.uid == retained.metadata.uid
            for path, content in prior_states.items():
                assert path.read_bytes() == content
            prior_states.update({path: path.read_bytes() for path in (tmp_path / str(iteration)).rglob('*.json')})
    finally:
        cluster.stop()


@pytest.mark.timeout(1200)
def test_rendered_refresh_probes_and_migration_execute_against_real_tls_databases(application_management_inputs, tmp_path):
    """Catch image/module/mount/role/defaulting errors hidden by transport fixtures."""
    from kubernetes import client
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest
    from scripts.ops.nebius_management_refresh_evidence import HTTPSManagementRefreshEvidenceAPI
    from scripts.ops.nebius_management_refresh_resources import (
        HTTPSManagementRefreshResourcesAPI,
        ManagementRefreshResourcesRequest,
        refresh_resources_ready,
        stage_refresh_resources,
    )
    from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest

    from loom_service.environment_management.credentials import generate_management_material
    from loom_service.environment_management.deployment import (
        ManagementDeployment,
        render_management,
    )
    from tests.integration.test_execution_actuator_k3s import (
        _build_image,
        _docker,
        _docker_platform,
        _import_image,
    )

    suffix = uuid4().hex
    service_tag = 'cr.eu-north1.nebius.cloud/test/service:refresh-test-' + suffix
    postgres_tag = 'cr.eu-north1.nebius.cloud/test/postgres:refresh-test-' + suffix
    cluster = None
    try:
        _build_image(tag=service_tag, dockerfile='deploy/Dockerfile.service', platform=_docker_platform())
        _docker('pull', 'postgres:16')
        _docker('tag', 'postgres:16', postgres_tag)
        cluster = _start_k3s(ephemeral_storage_floor='2Gi')
        _, core, batch = _load_client(cluster)
        apps = client.AppsV1Api(core.api_client)
        endpoint = 'https://127.0.0.1:' + str(cluster.get_exposed_port(6443))
        kube = yaml.safe_load(cluster.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            kube['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = kube['users'][0]['user']
        certificate, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        certificate.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        service_image = _import_image(cluster, tag=service_tag, root=tmp_path, ordinal=0)
        postgres_image = _import_image(cluster, tag=postgres_tag, root=tmp_path, ordinal=1)
        for node in core.list_node().items:
            core.patch_node(node.metadata.name, {'metadata': {'labels': {
                'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}}})
        raw, candidate, profile = copy.deepcopy(application_management_inputs)
        candidate['images']['service']['image_ref'] = service_image
        profile['task_image_ref'] = service_image
        installation = raw['installation']
        foundation = installation['foundation']
        config = json.loads(foundation['platform_config_json'])
        config.update(kubernetes_api_server=endpoint, postgres_image=postgres_image, backup_image=postgres_image,
                      storage_class='local-path')
        foundation['platform_config_json'] = json.dumps(config)
        installation['applications']['runtime']['kubernetes']['endpoint'] = endpoint
        installation['applications']['shared']['schema_revision'] = CURRENT_REVISION
        for release in installation['applications']['releases']:
            release['schema_revision'] = CURRENT_REVISION
        deployment = ManagementDeployment.model_validate(raw)
        rendered = render_management(deployment, candidate=candidate, profile=profile,
            repo_root=Path(__file__).resolve().parents[2])
        namespace = deployment.namespace
        shared = deployment.installation.applications.shared.platform_namespace
        identities, retained = {}, {}

        def finished_job(name, ns):
            status = batch.read_namespaced_job(name, ns).status
            assert not status.failed, f'rendered Job failed: {ns}/{name}'
            return status.succeeded == 1

        def completed_probe_stable(proof, state):
            previous = None

            def check():
                nonlocal previous
                recorded = proof._recorded(state)
                job = recorded['Job']
                status = job.get('status', {})
                conditions = {row['type']: row['status'] for row in status.get('conditions', [])}
                if conditions.get('Failed') == 'True':
                    raise AssertionError('refresh probe Job failed while settling')
                if conditions.get('Complete') != 'True' or status.get('succeeded') != 1 or status.get('active', 0):
                    return False
                metadata = job['metadata']
                base = '/api/v1/namespaces/' + metadata['namespace'] + '/pods'
                listing = proof._request('GET', base + '?labelSelector=batch.kubernetes.io/controller-uid%3D'
                    + metadata['uid'] + '&limit=2')
                if listing is None or listing.get('metadata', {}).get('continue') or len(listing.get('items', [])) != 1:
                    raise AssertionError('refresh probe did not retain one Pod while settling')
                pod = {'apiVersion': 'v1', 'kind': 'Pod', **listing['items'][0]}
                if pod.get('status', {}).get('phase') != 'Succeeded':
                    return False
                snapshot = (recorded, pod)
                if previous is not None and pod['metadata']['uid'] != previous[1]['metadata']['uid']:
                    raise AssertionError('refresh probe Pod identity changed while settling')
                stable = snapshot == previous
                previous = snapshot
                return stable

            return check

        def wait_for(check, message, timeout=180):
            deadline = time.monotonic() + timeout
            while not check():
                assert time.monotonic() < deadline, message
                time.sleep(0.5)

        for ns in (namespace, shared):
            created = core.create_namespace({'metadata': {'name': ns, 'labels': {
                'loom.nebius/management-installation': str(deployment.installation_id),
                'pod-security.kubernetes.io/enforce': 'restricted'}}})
            identities[ns] = created.metadata.uid
            core.create_namespaced_service_account(ns, {'metadata': {'name': 'loom-platform'},
                'automountServiceAccountToken': False})
            for name, values in generate_management_material(namespace=ns).items():
                if ns == shared and name == 'loom-platform-db':
                    # The installed shared platform uses the explicit psycopg
                    # spelling; exercise both accepted forms in actual Jobs.
                    values['service-url'] = values['service-url'].replace('postgresql://', 'postgresql+psycopg://', 1)
                secret = core.create_namespaced_secret(ns, {'metadata': {'name': name}, 'immutable': True,
                    'stringData': values})
                retained[(ns, name)] = secret.metadata.uid
            cm = copy.deepcopy(rendered.files['10-config-network.yaml'][0])
            cm['metadata']['namespace'] = ns
            environment = json.loads(cm['data']['environment.json'])
            environment['namespace'] = ns
            cm['data']['environment.json'] = json.dumps(environment)
            core.create_namespaced_config_map(ns, cm)
            for source in rendered.files['20-database.yaml']:
                document = copy.deepcopy(source)
                document['metadata']['namespace'] = ns
                if document['kind'] == 'Service':
                    core.create_namespaced_service(ns, document)
                else:
                    apps.create_namespaced_stateful_set(ns, document)
            wait_for(lambda ns=ns: apps.read_namespaced_stateful_set('loom-postgres', ns).status.ready_replicas == 1,
                'TLS PostgreSQL did not become ready: ' + ns)
            job = copy.deepcopy(rendered.files['30-migrate.yaml'][0])
            job['metadata']['namespace'] = ns
            batch.create_namespaced_job(ns, job)
            wait_for(lambda job=job, ns=ns: finished_job(job['metadata']['name'], ns), 'initial management bootstrap did not finish')

        active = next(copy.deepcopy(doc) for doc in rendered.files['40-services.yaml'] if doc['kind'] == 'Deployment')
        active['metadata'].update(uid=str(uuid4()), resourceVersion='1', generation=1)
        request = ManagementRefreshRenderRequest(deployment, deployment, active, candidate, profile,
            Path(__file__).resolve().parents[2])
        binding = ManagementBinding(str(deployment.installation_id), namespace, identities[namespace],
            core.read_namespace('kube-system').metadata.uid)
        resources = ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(request, uuid4()),
            binding, identities[shared], CURRENT_REVISION, CURRENT_REVISION)
        for phase in ('manager-probe', 'shared-probe', 'migration', 'post-migration-probe'):
            state = tmp_path / phase
            phase_deadline = time.monotonic() + 180
            with HTTPSManagementRefreshResourcesAPI(request=resources, phase=phase,
                    api_server=endpoint, ssl_context=trust) as api:
                args = dict(request=resources, phase=phase, api=api, state_dir=state)
                receipt = stage_refresh_resources(**args)
                wait_for(lambda args=args: refresh_resources_ready(**args), 'refresh phase did not finish: ' + phase,
                    timeout=max(0, phase_deadline - time.monotonic()))
                assert stage_refresh_resources(**args) == receipt
            if phase.endswith('probe'):
                with HTTPSManagementRefreshEvidenceAPI(request=resources, phase=phase,
                        api_server=endpoint, ssl_context=trust) as proof:
                    wait_for(completed_probe_stable(proof, state), 'refresh probe readbacks did not settle: ' + phase,
                        timeout=max(0, phase_deadline - time.monotonic()))
                    try:
                        observed = proof.probe_report(state)
                    except Exception as error:
                        # Production errors suppress private API/log values. Keep
                        # that boundary while locating the failed CI invariant.
                        error.add_note(f'refresh probe phase: {phase}')
                        context = error.__context__
                        seen = {id(error)}
                        while context is not None and id(context) not in seen:
                            seen.add(id(context))
                            trace = context.__traceback__
                            while trace is not None:
                                filename = Path(trace.tb_frame.f_code.co_filename).name
                                error.add_note(f'suppressed {type(context).__name__} at {filename}:{trace.tb_lineno}')
                                trace = trace.tb_next
                            context = context.__context__
                        raise
                assert observed is not None
                assert observed['probe'] == {'schema': 'loom.nebius-management-refresh-probe.v1',
                    'status': 'qualified', 'mode': 'shared' if phase == 'shared-probe' else 'manager',
                    'revision': CURRENT_REVISION, 'operations_checked': 0}
        for (ns, name), uid in retained.items():
            assert core.read_namespaced_secret(name, ns).metadata.uid == uid
    finally:
        if cluster is not None:
            cluster.stop()
        # Only this test's unique tags and exported archives are removed.
        import subprocess
        subprocess.run(['docker', 'image', 'rm', service_tag, postgres_tag], capture_output=True, check=False)
        for ordinal in (0, 1):
            (tmp_path / f'image-{ordinal}.tar').unlink(missing_ok=True)
