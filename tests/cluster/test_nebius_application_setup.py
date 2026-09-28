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

import pytest
import yaml

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
                                reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
def test_application_setup_stages_fixed_resources_on_real_api(tmp_path, application_management_inputs, application_material):
    from kubernetes import client
    from scripts.ops.nebius_application_setup import (
        ApplicationSetupRequest,
        HTTPSApplicationSetupAPI,
        application_setup_ready,
        stage_application_setup,
    )
    from scripts.ops.nebius_management_material import ManagementBinding
    from scripts.ops.nebius_management_stage import ManagementStageError

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
        for phase in ('admission', 'permissions', 'network', 'material', 'database'):
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
                elif phase == 'database':
                    # No platform image, DB Secret, or eligible node exists.
                    # Defaulted/persisted Job/config alone cannot prove SQL ran.
                    assert application_setup_ready(**args) is False
        bindings = rbac.list_namespaced_role_binding(shared.metadata.name).items
        assert len(bindings) == 1
        assert bindings[0].subjects[0].name == 'loom-application-provisioner'
        assert bindings[0].subjects[0].namespace == deployment.namespace
        wrong = replace(request, shared_namespace_uid=str(uuid4()))
        with HTTPSApplicationSetupAPI(request=wrong, phase='network', api_server=endpoint,
                                       ssl_context=trust) as api:
            with pytest.raises(ManagementStageError, match='shared namespace'):
                stage_application_setup(request=wrong, phase='network', api=api, state_dir=tmp_path / 'wrong-uid')
        assert core.read_namespace(shared.metadata.name).metadata.uid == request.shared_namespace_uid
    finally:
        container.stop()
