"""Actual Kubernetes admission and RBAC for the installer's fixed gateway output."""
from __future__ import annotations

import asyncio
import copy
import hmac
import json
import os
import ssl
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from tests.integration.test_execution_actuator_k3s import _load_client, _start_k3s
from tests.unit.test_nebius_pool_gateway_render import rendered

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="requires explicitly disposable Kubernetes")


@pytest.mark.timeout(180)
async def test_rendered_gateway_is_disabled_and_its_real_identity_has_only_fixed_namespace_writes(tmp_path):
    from kubernetes import client
    from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND

    container = await asyncio.to_thread(_start_k3s, ephemeral_storage_floor="1Gi")
    try:
        _, core, _ = await asyncio.to_thread(_load_client, container)
        rbac = client.RbacAuthorizationV1Api(core.api_client)
        apps = client.AppsV1Api(core.api_client)
        spec, _, documents = rendered()
        management = "loom-nebius-management"
        namespaces = {ns.name for participant in spec.participants for ns in (participant.execution_namespace, participant.build_namespace)}
        namespace_uids = {}
        for name in [management, "pool-foreign", *sorted(namespaces)]:
            created_namespace = await asyncio.to_thread(core.create_namespace, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
            namespace_uids[name] = created_namespace.metadata.uid
        for resource in documents["configuration"]:
            method = core.create_namespaced_service_account if resource["kind"] == "ServiceAccount" else core.create_namespaced_config_map
            await asyncio.to_thread(method, management, resource)
        for resource in documents["authority"]:
            if resource["kind"] == "ClusterRole":
                await asyncio.to_thread(rbac.create_cluster_role, resource)
            elif resource["kind"] == "ClusterRoleBinding":
                await asyncio.to_thread(rbac.create_cluster_role_binding, resource)
            elif resource["kind"] == "Role":
                await asyncio.to_thread(rbac.create_namespaced_role, resource["metadata"]["namespace"], resource)
            else:
                await asyncio.to_thread(rbac.create_namespaced_role_binding, resource["metadata"]["namespace"], resource)
        deployment, = documents["workload"]
        created = await asyncio.to_thread(apps.create_namespaced_deployment, management, deployment)
        assert created.spec.replicas == 0
        assert not (await asyncio.to_thread(core.list_namespaced_pod, management)).items
        issued = await asyncio.to_thread(core.create_namespaced_service_account_token, "loom-pool-gateway", management,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        configuration = core.api_client.configuration
        async with httpx.AsyncClient(base_url=configuration.host, verify=ssl.create_default_context(cafile=configuration.ssl_ca_cert),
                trust_env=False, headers={"Authorization": "Bearer " + issued.status.token}, timeout=20) as http:
            for namespace in sorted(namespaces):
                assert (await http.get("/api/v1/namespaces/" + namespace)).status_code == 200
                path = "/apis/batch/v1/namespaces/" + namespace + "/jobs"
                job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "pool-role-probe"},
                    "spec": {"suspend": True, "template": {"spec": {"restartPolicy": "Never",
                        "containers": [{"name": "probe", "image": "busybox:1.36"}]}}}}
                assert (await http.post(path, json=job)).status_code == 201
                assert (await http.patch(path + "/pool-role-probe", json={"spec": {"suspend": True}},
                    headers={"Content-Type": "application/merge-patch+json"})).status_code == 403
                assert (await http.delete(path + "/pool-role-probe")).status_code in {200, 202}
                assert (await http.get("/api/v1/namespaces/" + namespace + "/secrets")).status_code == 403
            assert (await http.get("/api/v1/namespaces/pool-foreign")).status_code == 403
            assert (await http.post("/apis/batch/v1/namespaces/pool-foreign/jobs", json=job)).status_code == 403
            assert (await http.get("/api/v1/namespaces")).status_code == 403
            assert (await http.get("/apis/rbac.authorization.k8s.io/v1/clusterroles")).status_code == 403

        # Run the exact fixed probe against this API with projected-style files.
        # Token issuance belongs only to the disposable test bootstrap; the
        # production probe performs GETs and never uses the operator identity.
        token, ca = tmp_path / 'projected-token', tmp_path / 'projected-ca.crt'
        token.write_text(issued.status.token)
        token.chmod(0o440)
        ca.write_bytes(Path(configuration.ssl_ca_cert).read_bytes())
        connection = {'kind': 'projected_service_account', 'endpoint': configuration.host,
            'ca_file': str(ca), 'token_file': str(token)}
        machine, = (row for row in spec.machines if row.role == 'gateway')
        expected = {'pool_id': str(spec.pool_id), 'installation_id': str(spec.installation_id),
            'machine_id': str(machine.machine_id), 'admission_epoch': spec.admission_epoch, 'kubernetes': connection,
            'namespaces': {name: namespace_uids[name] for name in sorted(namespaces)}}
        environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
        environment.update({row['name']: row['value'] for row in deployment['spec']['template']['spec']['containers'][0]['env'] if 'value' in row})
        environment.update(LOOM_POOL_GATEWAY_KUBERNETES=json.dumps(connection),
            LOOM_POOL_GATEWAY_DB_URL='postgresql+psycopg://fixture:private-unused@localhost/loom')

        def probe(wanted):
            nonce = 'ab' * 32
            response = hmac.new(bytes.fromhex(nonce), json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode(), 'sha256').hexdigest()
            return subprocess.run([sys.executable, '-c', BOUND_GATEWAY_KUBERNETES_COMMAND,
                json.dumps(sorted(namespaces)), nonce, response], cwd=tmp_path, env=environment, capture_output=True, timeout=35, check=False)

        good = await asyncio.to_thread(probe, expected)
        assert (good.returncode, good.stdout, good.stderr) == (0, b'{"status": "qualified"}\n', b'')
        wrong = copy.deepcopy(expected)
        wrong['namespaces'][next(iter(namespaces))] = str(uuid4())
        rejected = await asyncio.to_thread(probe, wrong)
        assert rejected.returncode == 1 and rejected.stderr == b'Pool gateway Kubernetes access unqualified\n'
        await asyncio.to_thread(core.create_namespaced_service_account, management,
            {'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'unprivileged'}, 'automountServiceAccountToken': False})
        unprivileged = await asyncio.to_thread(core.create_namespaced_service_account_token, 'unprivileged', management,
            client.AuthenticationV1TokenRequest(spec=client.V1TokenRequestSpec(audiences=[])))
        replacement = tmp_path / 'rotated-token'
        replacement.write_text(unprivileged.status.token)
        replacement.chmod(0o440)
        replacement.replace(token)
        denied = await asyncio.to_thread(probe, expected)
        assert denied.returncode == 1 and denied.stdout == b'' and denied.stderr == b'Pool gateway Kubernetes access unqualified\n'
    finally:
        await asyncio.to_thread(container.stop)
