"""The management build reader uses explicit rotating projected credentials."""
from __future__ import annotations

import json

import pytest
import urllib3

from loom_execution_actuator.task_image_controller import NativeBuildKubernetesApi
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection
from tests.unit.test_nebius_kubernetes import connection as connection
from tests.unit.test_nebius_pool_native_observation import fixture


async def test_projected_native_reader_rotates_identity_without_ambient_configuration(connection, tmp_path, monkeypatch):
    from kubernetes import config
    from kubernetes.client.exceptions import ApiException

    from loom_service.environment_management.kubernetes_credentials import (
        create_projected_api_client,
    )

    runtime, _, _, namespace = fixture()
    token = tmp_path / "projected-build-reader-token"
    token.write_text("first-reader-token")
    token.chmod(0o440)
    binding = ProjectedKubernetesConnection(kind="projected_service_account", endpoint=connection.endpoint,
        ca_file=connection.ca_file, token_file=token)
    client, credentials = create_projected_api_client(binding)

    def forbidden(*args, **kwargs):
        pytest.fail("build reader selected ambient Kubernetes credentials")

    monkeypatch.setattr(config, "load_incluster_config", forbidden)
    monkeypatch.setattr(config, "load_kube_config", forbidden)
    calls = []

    def get(url, **kwargs):
        assert url.startswith(binding.endpoint + "/")
        calls.append((url, kwargs["headers"]["authorization"]))
        if "/jobs/" in url:
            replacement = tmp_path / "next-projected-token"
            replacement.write_text("rotated-reader-token")
            replacement.chmod(0o440)
            replacement.replace(token)
            raise ApiException(status=404)
        assert url.endswith("/namespaces/" + runtime.namespace.name)
        return urllib3.HTTPResponse(body=json.dumps(namespace).encode(), status=200)

    monkeypatch.setattr(client.rest_client, "GET", get)
    reader = NativeBuildKubernetesApi(api_client=client)
    try:
        assert await reader.observe_pool(runtime) is None
    finally:
        await reader.close()
        await credentials.close()
    assert [token for _, token in calls] == ["Bearer first-reader-token", "Bearer first-reader-token", "Bearer rotated-reader-token"]
    assert client.configuration.host == binding.endpoint and client.configuration.verify_ssl
    assert client.configuration.ssl_ca_cert == str(binding.ca_file)


def test_explicit_projected_client_cannot_be_combined_with_native_cloud_identity(connection, tmp_path):
    from loom_service.environment_management.kubernetes_credentials import (
        create_projected_api_client,
    )

    token = tmp_path / "projected-build-reader-token"
    token.write_text("reader-token")
    token.chmod(0o440)
    client, _ = create_projected_api_client(ProjectedKubernetesConnection(kind="projected_service_account",
        endpoint=connection.endpoint, ca_file=connection.ca_file, token_file=token))
    try:
        with pytest.raises(ValueError, match="ambiguous_native_build_client"):
            NativeBuildKubernetesApi(connection=connection, api_client=client)
    finally:
        client.close()
