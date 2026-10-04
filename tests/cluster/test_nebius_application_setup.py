"""Real API setup/defaulting; SQL execution is a separate integration contract."""
from __future__ import annotations

import base64
import copy
import json
import os
import ssl
import time
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import (
    builder_management_inputs as builder_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import (
    source_management_inputs as source_management_inputs,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
                                reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
def test_build_observer_can_read_only_its_build_namespace(builder_management_inputs):
    from kubernetes import client

    from tests.unit.test_nebius_management_render import documents, render

    docs = documents(render(builder_management_inputs))
    role, = [row for row in docs if row['kind'] == 'Role']
    binding, = [row for row in docs if row['kind'] == 'RoleBinding']
    build_namespace = role['metadata']['namespace']
    subject, = binding['subjects']
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        for namespace in (build_namespace, subject['namespace'], 'unrelated-builds'):
            core.create_namespace({'metadata': {'name': namespace}})
        core.create_namespaced_service_account(subject['namespace'], {'metadata': {'name': subject['name']}})
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        rbac.create_namespaced_role(build_namespace, role)
        rbac.create_namespaced_role_binding(build_namespace, binding)
        issued = core.create_namespaced_service_account_token(subject['name'], subject['namespace'],
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        # A trust-only client authenticates as the actual provisioner, never admin.
        config = core.api_client.configuration
        with httpx.Client(base_url=config.host, verify=ssl.create_default_context(cafile=config.ssl_ca_cert),
                trust_env=False, headers={'Authorization': 'Bearer ' + issued.status.token}, timeout=10) as http:
            deadline = time.monotonic() + 20
            while http.get('/api/v1/namespaces/' + build_namespace + '/pods').status_code != 200:
                assert time.monotonic() < deadline, 'build observer read permission did not become effective'
                time.sleep(0.1)
            job = '/apis/batch/v1/namespaces/' + build_namespace + '/jobs/missing'
            assert http.get(job).status_code == 404  # Authorized, absent; not a forbidden read.
            assert http.get('/api/v1/namespaces/' + build_namespace + '/pods/missing/log').status_code == 404
            for method, path in [('POST', job.rsplit('/', 1)[0]), ('PATCH', job), ('DELETE', job),
                    ('GET', '/api/v1/namespaces/' + build_namespace + '/secrets'),
                    ('GET', '/api/v1/namespaces/unrelated-builds/pods'),
                    ('POST', '/api/v1/namespaces/' + build_namespace + '/pods/missing/exec')]:
                assert http.request(method, path, json={}).status_code == 403, (method, path)
    finally:
        container.stop()


@pytest.mark.timeout(180)
def test_application_setup_stages_fixed_resources_on_real_api(tmp_path, application_management_inputs, application_material):
    from kubernetes import client
    from scripts.ops.nebius_application_setup import (
        ApplicationSetupRequest,
        HTTPSApplicationSetupAPI,
        application_setup_ready,
        stage_application_setup,
    )
    from scripts.ops.nebius_management_bootstrap import BootstrapBinding
    from scripts.ops.nebius_management_install import ManagementInstallRequest
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import ManagementStageError
    from scripts.ops.nebius_management_upgrade import ManagementUpgradeRequest
    from scripts.ops.nebius_management_upgrade_live import HTTPSManagementUpgradeAPI

    from loom_service.environment_management.deployment import ManagementDeployment
    from tests.unit.test_nebius_management_render import ROOT

    raw, candidate, profile = copy.deepcopy(application_management_inputs)
    container = _start_k3s()
    try:
        _, core, _ = _load_client(container)
        endpoint = 'https://127.0.0.1:' + str(container.get_exposed_port(6443))
        raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
        foundation = raw['installation']['foundation']
        platform = json.loads(foundation['platform_config_json'])
        platform['kubernetes_api_server'] = endpoint
        foundation['platform_config_json'] = json.dumps(platform)
        deployment = ManagementDeployment.model_validate(raw)
        application = deployment.installation.applications
        config = yaml.safe_load(container.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(
            config['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = config['users'][0]['user']
        certificate, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        certificate.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        management = core.create_namespace({'metadata': {'name': deployment.namespace, 'labels': {
            'loom.nebius/management-installation': str(deployment.installation_id),
            'pod-security.kubernetes.io/enforce': 'restricted',
        }}})
        shared = core.create_namespace({'metadata': {'name': application.shared.platform_namespace}})
        binding = ManagementBinding(str(deployment.installation_id), deployment.namespace,
            management.metadata.uid, core.read_namespace('kube-system').metadata.uid)
        request = ApplicationSetupRequest(deployment, candidate, profile, binding, shared.metadata.uid, ROOT, application_material)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        for phase in ('config', 'admission', 'permissions', 'network', 'material', 'database', 'migration'):
            with HTTPSApplicationSetupAPI(request=request, phase=phase, api_server=endpoint,
                                           ssl_context=trust) as api:
                args = dict(request=request, phase=phase, api=api, state_dir=tmp_path / phase)
                first = stage_application_setup(**args)
                assert stage_application_setup(**args) == first
                assert all(first['resource_uids'].values())
                if phase == 'admission':
                    assert application.authority.name + '-bootstrap' not in {
                        row.metadata.name for row in rbac.list_cluster_role_binding().items}
                    deadline = time.monotonic() + 20
                    while not application_setup_ready(**args):
                        assert time.monotonic() < deadline, 'application admission did not become ready'
                        time.sleep(0.1)
                elif phase in {'database', 'migration'}:
                    # No platform image, DB Secret, or eligible node exists.
                    # Defaulted/persisted Job/config alone cannot prove SQL ran.
                    assert application_setup_ready(**args) is False
        bindings = rbac.list_namespaced_role_binding(shared.metadata.name).items
        assert len(bindings) == 1
        assert bindings[0].subjects[0].name == 'loom-application-provisioner'
        assert bindings[0].subjects[0].namespace == deployment.namespace
        # Exercise the exact installed adapter: an operator mTLS connection issues
        # one short-lived token, then clean trust-only TLS authenticates the new SA.
        # Its dry-runs/reviews must not create a personal probe namespace or grant.
        legacy = copy.deepcopy(raw)
        legacy['installation'].pop('applications')
        legacy['installation']['provider_runtime'] = {'kubernetes': application.runtime.kubernetes.model_dump(mode='json'),
            'cloud_credentials_file': '/var/run/loom-management-cloud/credentials.json'}
        original = ManagementInstallRequest(BootstrapBinding(binding.installation_id, binding.namespace, binding.kube_system_uid),
            ManagementDeployment.model_validate(legacy), candidate, profile, {})
        upgrade = ManagementUpgradeRequest(original, request, tmp_path / 'original', tmp_path / 'original-anchor')

        class UnusedPrerequisites:
            def preflight(self, request):
                pytest.fail('this test qualifies only the installed application subject')

            def public_route(self, request):
                pytest.fail('no public route is installed in this fixture')

        before = {row.metadata.name for row in core.list_namespace().items}
        with HTTPSManagementUpgradeAPI(request=upgrade, api_server=endpoint, ssl_context=trust,
            runtime_ca_pem=base64.b64decode(config['clusters'][0]['cluster']['certificate-authority-data']).decode(),
            checks=UnusedPrerequisites()) as api:
            deadline = time.monotonic() + 20
            while not api.qualify_authority(request, tmp_path):
                assert time.monotonic() < deadline, 'application runtime authority did not become effective'
                time.sleep(0.1)
        assert {row.metadata.name for row in core.list_namespace().items} == before
        wrong = replace(request, shared_namespace_uid=str(uuid4()))
        with HTTPSApplicationSetupAPI(request=wrong, phase='network', api_server=endpoint,
                                       ssl_context=trust) as api:
            with pytest.raises(ManagementStageError, match='shared namespace'):
                stage_application_setup(request=wrong, phase='network', api=api, state_dir=tmp_path / 'wrong-uid')
        assert core.read_namespace(shared.metadata.name).metadata.uid == request.shared_namespace_uid
    finally:
        container.stop()
