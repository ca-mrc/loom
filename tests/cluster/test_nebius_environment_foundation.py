"""Exercise independent foundation intents against real API defaulting, offline workloads."""
from __future__ import annotations

import base64
import copy
import os
import ssl

import httpx
import pytest
import yaml

from loom.nebius_environment_foundation import render_foundation
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProvisioningContext
from loom_service.environment_management.registry import OperationLease
from loom_service.environment_management.steps import ProvisioningStep
from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_foundation import ROOT, declaration

pytestmark = pytest.mark.skipif(os.environ.get('LOOM_RUN_DISPOSABLE_K3S') != '1', reason='requires explicitly disposable Kubernetes')


@pytest.mark.timeout(180)
async def test_real_api_accepts_closed_foundation_and_retains_exact_provider_ids(tmp_path, platform_inputs):
    wanted = declaration(platform_inputs)
    prepared = render_foundation(wanted, platform_inputs[1], profile=platform_inputs[2], keyring={}, repo_root=ROOT)
    container = _start_k3s(ephemeral_storage_floor='2Gi')
    try:
        _load_client(container)
        config = yaml.safe_load(container.exec(['cat', '/etc/rancher/k3s/k3s.yaml']).output)
        trust = ssl.create_default_context(cadata=base64.b64decode(config['clusters'][0]['cluster']['certificate-authority-data']).decode())
        user = config['users'][0]['user']
        certificate, key = tmp_path / 'client.crt', tmp_path / 'client.key'
        certificate.write_bytes(base64.b64decode(user['client-certificate-data']))
        key.write_bytes(base64.b64decode(user['client-key-data']))
        key.chmod(0o600)
        trust.load_cert_chain(certificate, key)
        context = ProvisioningContext(OperationLease(wanted.operation_id, wanted.registration.environment_id,
            1, 1, wanted.operation_id), wanted.registration.model_dump(mode='json'), wanted.platform_config, {})
        steps = []
        for filename in ('00-namespaces.yaml', '60-execution.yaml', '10-config-network.yaml', '20-database.yaml', '70-public.yaml'):
            for original in prepared.files[filename]:
                doc = copy.deepcopy(original)
                if doc['kind'] == 'StatefulSet':
                    doc['spec']['replicas'] = 0  # No image pull or application/database deployment in this API test.
                key = f"k8s:{doc['kind']}:{doc['metadata'].get('namespace', '-')}:{doc['metadata']['name']}"
                steps.append(ProvisioningStep(key, 'kubernetes', doc))
        async with httpx.AsyncClient(base_url='https://127.0.0.1:' + str(container.get_exposed_port(6443)),
                verify=trust, trust_env=False) as http:
            provider = KubernetesEnvironmentProvider(http)
            for step in steps:
                context.identities[step.key] = await provider.apply(context, step)
            before = dict(context.identities)
            for step in steps:
                assert await provider.apply(context, step, create=False) == before[step.key]
            quotas = (await http.get('/api/v1/namespaces/work-red/resourcequotas')).json()['items']
            assert len(quotas) == 1 and quotas[0]['spec']['hard']['pods'] == '0'
    finally:
        container.stop()
