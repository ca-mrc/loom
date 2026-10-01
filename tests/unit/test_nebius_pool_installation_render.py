"""The fixed protected Job executes the actual hash-only registration contract."""
import json

import pytest

from tests.integration.test_nebius_pool_installation import installation


def test_registration_job_mounts_only_fixed_installation_and_database_inputs():
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_registration

    config, raw = installation()
    spec = PoolInstallation.model_validate(config)
    image = "registry.example/service@sha256:" + "a" * 64
    configmap, job = render_registration(spec, namespace="loom-nebius-management", service_image=image)
    assert configmap["immutable"] is True
    assert PoolInstallation.model_validate_json(configmap["data"]["installation.json"]) == spec
    pod = job["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False and pod["restartPolicy"] == "Never"
    assert job["spec"]["backoffLimit"] == 0
    assert pod["securityContext"]["runAsNonRoot"] is True
    container, = pod["containers"]
    assert container["image"] == image
    assert container["command"] == ["python", "-m", "loom_service.pool_management.installation"]
    env = {row["name"]: row for row in container["env"]}
    assert env["LOOM_POOL_INSTALLATION_DB_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": "loom-platform-db", "key": "admin-url"}
    assert env["LOOM_POOL_INSTALLATION_FILE"]["value"] == "/var/run/loom-pool-installation/installation.json"
    assert {row["secret"]["secretName"] for row in pod["volumes"] if "secret" in row} == {"loom-platform-db"}
    assert not any(secret in json.dumps((configmap, job)) for secret in raw.values())
    assert "Role" not in {doc["kind"] for doc in (configmap, job)}  # Staging cannot enable a writer.


@pytest.mark.parametrize("image", ["registry.example/service:latest", "", "registry.example/service@sha256:bad"])
def test_registration_rejects_unpinned_image(image):
    from loom_service.pool_management.installation import PoolInstallation
    from loom_service.pool_management.installation_render import render_registration

    config, _ = installation()
    with pytest.raises(ValueError):
        render_registration(PoolInstallation.model_validate(config), namespace="loom-nebius-management", service_image=image)
