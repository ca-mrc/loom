"""The installed gateway uses dedicated identity and fixed namespace authority."""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys

import pytest

from tests.integration.test_nebius_pool_installation import installation


def rendered():
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_gateway

    config, raw = installation()
    spec = PoolInstallation.model_validate(config)
    documents = render_gateway(spec, namespace="loom-nebius-management",
        service_image="registry.example/service@sha256:" + "a" * 64, kubernetes_endpoint="https://cluster.example")
    return spec, raw, documents


def test_gateway_deployment_consumes_actual_settings_without_enabling_dispatch(monkeypatch):
    from loom_service.pool_management.__main__ import PoolGatewaySettings

    spec, raw, documents = rendered()
    deployment, = documents["workload"]
    assert deployment["spec"]["replicas"] == 0
    pod = deployment["spec"]["template"]["spec"]
    container, = pod["containers"]
    assert container["command"] == ["python", "-m", "loom_service.pool_management"]
    for row in container["env"]:
        monkeypatch.setenv(row["name"], row.get("value", "postgresql+psycopg://test:test@localhost/test"))
    settings = PoolGatewaySettings(_env_file=None)
    gateway, = [machine for machine in spec.machines if machine.role == "gateway"]
    assert (settings.pool_id, settings.installation_id, settings.machine_id) == (spec.pool_id, spec.installation_id, gateway.machine_id)
    assert settings.admission_epoch == spec.admission_epoch
    assert settings.kubernetes.endpoint == "https://cluster.example"
    assert pod["automountServiceAccountToken"] is False
    assert settings.kubernetes.token_file != settings.bearer_token_file
    assert not any(secret in json.dumps(documents) for secret in raw.values())
    assert next(row for row in container["env"] if row["name"] == "LOOM_POOL_GATEWAY_DB_URL")["valueFrom"]["secretKeyRef"]["key"] == "service-url"


def test_gateway_roles_are_confined_to_registered_namespaces_and_subject():
    spec, _, documents = rendered()
    authority = documents["authority"]
    roles = [row for row in authority if row["kind"] == "Role"]
    expected = {namespace.name for participant in spec.participants for namespace in (participant.execution_namespace, participant.build_namespace)}
    assert {row["metadata"]["namespace"] for row in roles} == expected
    for role in roles:
        assert role["rules"] == [
            {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get", "create", "delete"]},
            {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "create", "delete"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "delete"]}]
    cluster, = [row for row in authority if row["kind"] == "ClusterRole"]
    assert cluster["rules"] == [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get"], "resourceNames": sorted(expected)}]
    for binding in [row for row in authority if row["kind"].endswith("Binding")]:
        assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "loom-pool-gateway", "namespace": "loom-nebius-management"}]


def test_fixed_initializer_produces_the_owner_only_token_the_real_reader_requires(tmp_path):
    from loom_execution_capacity_collector.control_plane import read_owner_only_secret

    _, _, documents = rendered()
    init, = documents["workload"][0]["spec"]["template"]["spec"]["initContainers"]
    source, target = tmp_path / "projected", tmp_path / "owned"
    source.write_text("pool_private_test_token")
    result = subprocess.run([sys.executable, *init["command"][1:3], str(source), str(target)], capture_output=True, timeout=10)
    assert result.returncode == 0
    assert not result.stdout and not result.stderr
    assert target.stat().st_uid == os.getuid() and stat.S_IMODE(target.stat().st_mode) == 0o600
    assert read_owner_only_secret(target) == "pool_private_test_token"


@pytest.mark.parametrize("endpoint", ["http://cluster.example", "https://user:pass@cluster.example", "https://cluster.example/arbitrary"])
def test_gateway_renderer_rejects_unqualified_kubernetes_origin(endpoint):
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_gateway

    config, _ = installation()
    with pytest.raises(ValueError):
        render_gateway(PoolInstallation.model_validate(config), namespace="loom-nebius-management",
            service_image="registry.example/service@sha256:" + "a" * 64, kubernetes_endpoint=endpoint)
