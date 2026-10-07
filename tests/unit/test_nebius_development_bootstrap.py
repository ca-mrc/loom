"""Private shared-dev bootstrap must not inherit standalone execution authority."""

from __future__ import annotations

import copy
import json

import pytest

from loom.nebius_platform_render import NebiusPlatformError, write_platform
from tests.unit.test_nebius_development_foundation import development_inputs as development_inputs
from tests.unit.test_nebius_platform_render import ROOT
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def render(development_inputs):
    from loom.nebius_development_foundation import render_development_foundation

    config, original_candidate, profile = development_inputs
    candidate = {**original_candidate, "source_ref": "refs/heads/dev"}
    return render_development_foundation(config, candidate, profile, {}, repo_root=ROOT)


def named(result, kind, name):
    return next(doc for docs in result.files.values() for doc in docs
                if doc["kind"] == kind and doc["metadata"]["name"] == name)


@pytest.mark.parametrize("storage_gi", [10, 50])
def test_private_bootstrap_has_only_dev_system_objects(development_inputs, storage_gi):
    development_inputs[0]["postgres_storage_gi"] = storage_gi
    result = render(development_inputs)
    assert set(result.files) == {
        "00-namespaces.yaml", "10-config-network.yaml", "20-database.yaml",
        "30-migrate.yaml", "40-services.yaml",
    }
    for docs in result.files.values():
        for doc in docs:
            if doc["kind"] == "Namespace":
                assert doc["metadata"]["name"] == "loom-dev"
            else:
                assert doc["metadata"]["namespace"] == "loom-dev"
            assert doc["kind"] not in {
                "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "Ingress",
                "CronJob", "Secret", "PersistentVolumeClaim", "ValidatingAdmissionPolicy",
            }
            if doc["kind"] == "Service":
                assert doc["spec"].get("type", "ClusterIP") == "ClusterIP"
            if doc["kind"] in {"Job", "Deployment", "StatefulSet"}:
                pod = doc["spec"]["template"]["spec"]
                assert pod["automountServiceAccountToken"] is False
                assert len(pod["containers"]) == 1
    assert {doc["metadata"]["name"] for doc in result.files["40-services.yaml"]
            if doc["kind"] == "Deployment"} == {
        "loom-service", "loom-control-plane", "loom-llm-gateway", "loom-web",
    }
    assert result.platform_envelope.storage_mib == storage_gi * 1024
    assert result.platform_envelope.cpu_millis == 850
    assert result.platform_envelope.memory_mib == 2176
    assert result.platform_envelope.ephemeral_storage_mib == 2560


def test_private_bootstrap_has_no_execution_config_or_worker_credentials(development_inputs):
    from scripts.ops.deploy_nebius_platform import secret_requirements

    result = render(development_inputs)
    cm = named(result, "ConfigMap", "loom-platform-config")
    assert set(cm["data"]) == {"environment.json"}
    config = json.loads(cm["data"]["environment.json"])
    assert config == {"schema_version": "loom.nebius-development-bootstrap.v1",
                      "namespace": "loom-dev", "environment": "development"}
    # Use the actual Secret consumer inventory, not source-string matching.
    required = secret_requirements(result.files, development_inputs[0])
    assert all(namespace == "loom-dev" for namespace, _ in required)
    assert not {"loom-platform-collector", "loom-platform-batch-runner", "loom-model-provider"} & {
        name for _, name in required
    }
    assert not {"actuator-password", "backup-access-key", "backup-secret-key"} & {
        key for keys in required.values() for key in keys
    }
    migration, = result.files["30-migrate.yaml"]
    container, = migration["spec"]["template"]["spec"]["containers"]
    assert container["command"] == ["python", "-m", "loom.nebius_platform_bootstrap", "development-database"]
    assert {row["name"] for row in container["env"]} == {
        "LOOM_PLATFORM_CONFIG", "LOOM_DB_URL", "LOOM_DB_SERVICE_PASSWORD",
        "LOOM_DB_CONTROL_PLANE_PASSWORD", "LOOM_DB_GATEWAY_PASSWORD",
    }


def test_private_runtime_settings_do_not_start_work(development_inputs, monkeypatch):
    from loom_control_plane.app import create_app as cp_app
    from loom_control_plane.config import ControlPlaneSettings
    from loom_service.app import create_app as service_app
    from loom_service.config import LoomServiceSettings

    result = render(development_inputs)
    for name, model, factory in (("loom-service", LoomServiceSettings, service_app),
                                 ("loom-control-plane", ControlPlaneSettings, cp_app)):
        container, = named(result, "Deployment", name)["spec"]["template"]["spec"]["containers"]
        with monkeypatch.context() as scoped:
            for row in container["env"]:
                value = row.get("value", "test-secret-" + "x" * 32)
                if row["name"].endswith("_DB_URL"):
                    value = "postgresql+psycopg://test:test@loom-postgres.loom-dev.svc/loom"
                scoped.setenv(row["name"], value)
            settings = model()
            assert factory(settings) is not None
            if name == "loom-service":
                assert settings.service_mode == "api_only"
                assert settings.service_execution_runtime_profile_json == "{}"
                assert settings.batch_runner_cp_token is None
                assert settings.pool_submission_source is None
            else:
                assert settings.service_execution_scheduler_enabled is False
                assert settings.service_execution_materializer_enabled is False
                assert settings.global_pool is None


def test_private_network_peers_never_include_staging_or_execution(development_inputs):
    result = render(development_inputs)
    for docs in result.files.values():
        for doc in docs:
            if doc["kind"] != "NetworkPolicy":
                continue
            for rule in doc["spec"].get("ingress", []):
                assert rule["from"]
                for peer in rule["from"]:
                    assert peer["namespaceSelector"]["matchLabels"] == {"kubernetes.io/metadata.name": "loom-dev"}


def test_capable_candidate_does_not_restore_execution_authority(development_inputs):
    config, candidate, profile = development_inputs
    config["task_identity_policy"] = {"mode": "private-root-v1", "target_id": config["target_id"],
                                      "execution_namespace": config["execution_namespace"]}
    config["guest_execution_target"] = {"target_id": "nebius-dev-guest"}
    profile.update(guest_runtime="qemu-tcg-v1", supports_task_identity=True)
    before = copy.deepcopy(development_inputs)
    result = render(development_inputs)
    assert development_inputs == before
    assert {doc["metadata"]["name"] for doc in result.files["00-namespaces.yaml"]} == {"loom-dev"}
    assert set(named(result, "ConfigMap", "loom-platform-config")["data"]) == {"environment.json"}


@pytest.mark.parametrize("field,value", [("namespace", "loom-nebius-platform"),
                                       ("namespace", "loom-dev-alice"), ("environment", "staging")])
def test_private_bootstrap_rejects_noncanonical_foundation(development_inputs, field, value):
    development_inputs[0][field] = value
    with pytest.raises(NebiusPlatformError):
        render(development_inputs)


def test_private_bootstrap_cannot_be_consumed_by_standalone_rollout(development_inputs, tmp_path):
    from scripts.ops.deploy_nebius_platform import load_render

    result = render(development_inputs)
    write_platform(result.files, *development_inputs[:2], tmp_path)
    with pytest.raises((OSError, ValueError)):
        load_render(tmp_path)


def test_private_bootstrap_requires_dev_publication(development_inputs):
    from loom.nebius_development_foundation import render_development_foundation

    config, candidate, profile = development_inputs
    with pytest.raises(NebiusPlatformError, match="dev publication"):
        render_development_foundation(config, candidate, profile, {}, repo_root=ROOT)
