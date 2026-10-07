"""Actual disposable Kubernetes Secret delivery and Ingress certificate-only CAS."""
from __future__ import annotations

import copy
import json
import os
import ssl
from dataclasses import replace

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.ops.test_nebius_development_management_renewal import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_management_renewal import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_management_renewal import capacity_checks as capacity_checks
from tests.ops.test_nebius_development_management_renewal import cloud as cloud
from tests.ops.test_nebius_development_management_renewal import installation as installation
from tests.ops.test_nebius_development_management_renewal import inventory as inventory
from tests.ops.test_nebius_development_management_renewal import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_management_renewal import manager_entry as manager_entry
from tests.ops.test_nebius_development_management_renewal import material as material
from tests.ops.test_nebius_development_management_renewal import platform_inputs as platform_inputs
from tests.ops.test_nebius_development_management_renewal import provider_checks as provider_checks
from tests.ops.test_nebius_development_management_renewal import renewal as renewal
from tests.ops.test_nebius_development_management_renewal import retained as retained
from tests.ops.test_nebius_development_management_renewal import route as route
from tests.ops.test_nebius_development_management_renewal import tls_material as tls_material

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1',
    reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
def test_real_ingress_patch_preserves_uid_and_route_and_rejects_stale_version(renewal, route, tmp_path):
    from scripts.ops.nebius_development_management_renewal_live import (
        HTTPSDevelopmentManagementRenewalAPI,
    )
    from scripts.ops.nebius_development_management_route import HTTPSDevelopmentManagementRoute
    from scripts.ops.nebius_development_management_tls import deliver_management_tls
    from scripts.ops.nebius_ingress_stage import _snapshot

    from loom_service.environment_management.deployment import ManagementDeployment

    request = renewal[0]
    container = _start_k3s(ephemeral_storage_floor='1Gi')
    try:
        _, core, _ = _load_client(container)
        config = core.api_client.configuration
        trust = ssl.create_default_context(cafile=config.ssl_ca_cert)
        trust.load_cert_chain(config.cert_file, config.key_file)
        endpoint = 'https://127.0.0.1:' + str(container.get_exposed_port(6443))
        raw = request.retained.inputs.deployment.model_dump(mode='json')
        raw['installation']['applications']['runtime']['kubernetes']['endpoint'] = endpoint
        foundation = raw['installation']['foundation']
        platform = json.loads(foundation['platform_config_json'])
        platform['kubernetes_api_server'] = endpoint
        foundation['platform_config_json'] = json.dumps(platform)
        deployment = ManagementDeployment.model_validate(raw)
        binding = request.retained.binding
        namespace = core.create_namespace({'metadata': {'name': binding.namespace, 'labels': {
            'loom.nebius/management-installation': binding.installation_id,
            'pod-security.kubernetes.io/enforce': 'restricted'}}})
        binding = replace(binding, namespace_uid=namespace.metadata.uid,
            kube_system_uid=core.read_namespace('kube-system').metadata.uid)
        document = _snapshot(request.retained.ingress)
        ingress_path = '/apis/networking.k8s.io/v1/namespaces/' + binding.namespace + '/ingresses'
        with httpx.Client(base_url=endpoint, verify=trust, trust_env=False, timeout=20) as observer:
            original = observer.post(ingress_path, json=document).raise_for_status().json()
        retained = replace(request.retained, binding=binding, ingress=original,
            inputs=request.retained.inputs.model_copy(update={'deployment': deployment}))
        request = replace(request, retained=retained)
        with HTTPSDevelopmentManagementRoute(settings=route[0].settings, api_server=endpoint, ssl_context=trust) as router:
            with HTTPSDevelopmentManagementRenewalAPI(request=request, route=router,
                    api_server=endpoint, ssl_context=trust) as api:
                api.verify()
                state = tmp_path / 'real-tls-stage'
                with api.tls(request.material, binding) as tls_api:
                    receipt = deliver_management_tls(material=request.material, binding=binding, api=tls_api, state_dir=state)
                    assert deliver_management_tls(material=request.material, binding=binding, api=tls_api, state_dir=state) == receipt
                secret, = api.documents.values()
                observed = api.get_resource(secret)
                assert observed['immutable'] is True and observed['data'] == secret['data']
                before = api.read_ingress()
                target = _snapshot(before)
                target['metadata']['uid'] = before['metadata']['uid']
                target['spec']['tls'][0]['secretName'] = secret['metadata']['name']
                assert _snapshot(api.preview(before, target)) == _snapshot(target)
                assert api.read_ingress()['spec'] == original['spec'], 'dry run must not change live certificate'
                stale = copy.deepcopy(before)
                stale['metadata']['resourceVersion'] = '0'
                statuses = []
                api.client.event_hooks['response'].append(lambda response: statuses.append(response.status_code)
                    if response.request.method == 'PATCH' else None)
                assert api.patch(stale, target) is False
                assert statuses == [422], 'real API must identify stale JSON Patch as definite rejection'
                assert api.read_ingress()['spec'] == original['spec']
                assert api.patch(api.read_ingress(), target) is True
                after = api.read_ingress()
                assert after['metadata']['uid'] == original['metadata']['uid']
                assert _snapshot(after) == _snapshot(target)
                assert after['metadata']['resourceVersion'] != before['metadata']['resourceVersion']
    finally:
        container.stop()
