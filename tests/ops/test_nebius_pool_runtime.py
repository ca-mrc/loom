"""Protected runtime wiring reaches real settings without changing retained data."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_migration import migration_request
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render as render_manager
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def env(document):
    return {row["name"]: row for row in document["spec"]["template"]["spec"]["containers"][0]["env"]}


@pytest.fixture
def runtime_inputs(platform_inputs, management_inputs):
    from loom.nebius_platform_render import build_platform
    from loom_service.pool_management.installation import PoolInstallation

    request = migration_request()
    spec = request.registration.spec.model_dump(mode="json")
    config, candidate, profile = copy.deepcopy(platform_inputs)
    candidate["source_ref"] = "refs/heads/dev"
    config["task_image_builder"] = {"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "max_concurrent": 2}
    controllers, actuators, services, guards = {}, {}, {}, []
    for index, target in enumerate(request.guards):
        participant = spec["participants"][index]
        target = replace(target, namespace=f"loom-nebius-platform-{index}")
        participant["execution_namespace"]["name"] = f"loom-nebius-exec-{index}"
        config.update(namespace=target.namespace, execution_namespace=participant["execution_namespace"]["name"],
            environment="development", target_id="pool-target-" + str(index))
        participant["build_namespace"]["name"] = config["execution_namespace"] + "-build"
        participant["targets"][0]["target_id"] = config["target_id"]
        docs = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
        flattened = [row for group in docs.values() for row in group]
        cp = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-control-plane")
        actuator = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-execution-actuator")
        service = next(row for row in flattened if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-service")
        # The independent platform renderer supplies the retained Pod shape;
        # production/staging identities are supplied by their original install.
        env(cp)["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] = participant["environment_class"]
        for deployment in (cp, actuator, service):
            deployment["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        controllers[target.participant_id], actuators[target.participant_id], services[target.participant_id] = cp, actuator, service
        guards.append(replace(target, controller=cp))
        for kind, runtime_key in (("execution", "runtime"), ("task_images", "target")):
            row = spec["profiles"][kind][index]
            row[runtime_key].update(target_id=config["target_id"], namespace=participant["execution_namespace" if kind == "execution" else "build_namespace"]["name"])
        spec["profiles"]["task_images"][index]["settings"] = json.loads(env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"])
    new_candidate = copy.deepcopy(candidate)
    new_candidate["candidate_sha"] = "d" * 40
    for image in new_candidate["images"].values():
        image["image_ref"] = image["image_ref"].replace("b" * 64, "e" * 64)
    for row in spec["profiles"]["task_images"]:
        row["settings"]["service_image"] = new_candidate["images"]["service"]["image_ref"]
    request = replace(request, registration=replace(request.registration,
        spec=PoolInstallation.model_validate(spec), candidate=new_candidate), guards=tuple(guards))
    manager = next(row for row in render_manager(management_inputs).files["40-services.yaml"] if row["kind"] == "Deployment")
    manager["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    manager["metadata"]["labels"]["loom.nebius/management-installation"] = request.registration.binding.installation_id
    return request, actuators, services, manager


def test_manager_catalog_is_mounted_without_changing_existing_authorities(runtime_inputs, tmp_path, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_manager

    from loom_service.config import LoomServiceSettings
    from loom_service.pool_management.profiles import load_pool_profiles

    request, _, _, original = runtime_inputs
    before = copy.deepcopy(original)
    target = wire_manager(request=request, original=original)
    assert original == before and target["spec"]["replicas"] == 0
    original_pod, pod = (row["spec"]["template"]["spec"] for row in (original, target))
    assert pod["serviceAccountName"] == original_pod["serviceAccountName"]
    assert pod["containers"][0]["image"] == request.registration.candidate["images"]["service"]["image_ref"]
    assert [row for row in pod["volumes"] if "secret" in row or "projected" in row] == [
        row for row in original_pod["volumes"] if "secret" in row or "projected" in row]
    for name, row in env(target).items():
        if "value" in row:
            monkeypatch.setenv(name, row["value"])
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql+psycopg://test:test@localhost/test")
    monkeypatch.setenv("LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN", "test-token")
    settings = LoomServiceSettings(_env_file=None)
    assert str(settings.pool_profiles_file) == "/var/run/loom-pool-profiles/profiles.json"
    volume, = [row for row in pod["volumes"] if row["name"] == "pool-profiles"]
    assert volume["configMap"]["name"] == "loom-pool-profiles-" + request.registration.spec.operation_id.hex
    catalog = tmp_path / "profiles.json"
    catalog.write_text(request.registration.spec.profiles.model_dump_json())
    assert len(load_pool_profiles(catalog).execution) == 3


def test_all_participant_processes_consume_same_binding_and_preserve_data(runtime_inputs, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_participant

    from loom_control_plane.config import ControlPlaneSettings
    from loom_execution_actuator.config import ExecutionActuatorSettings
    from loom_service.config import LoomServiceSettings

    request, actuators, services, _ = runtime_inputs
    for target in request.guards:
        with monkeypatch.context() as patch:
            originals = copy.deepcopy((target.controller, actuators[target.participant_id], services[target.participant_id]))
            result = wire_participant(request=request, participant_id=target.participant_id, management_origin="https://manage.example.com",
                actuator=actuators[target.participant_id], service=services[target.participant_id])
            assert (target.controller, actuators[target.participant_id], services[target.participant_id]) == originals
            parsed = []
            for name, cls, prefix in (("control_plane", ControlPlaneSettings, "LOOM_CP_"),
                    ("actuator", ExecutionActuatorSettings, "LOOM_EXECUTION_ACTUATOR_"), ("service", LoomServiceSettings, "LOOM_SVC_")):
                deployment = result[name]
                assert deployment["spec"]["replicas"] == 0
                for key, value in env(deployment).items():
                    if "value" in value:
                        patch.setenv(key, value["value"])
                patch.setenv(prefix + "DB_URL", "postgresql+psycopg://test:test@localhost/test")
                if name != "actuator":
                    patch.setenv(prefix + "MINIO_ACCESS_KEY", "unused")
                    patch.setenv(prefix + "MINIO_SECRET_KEY", "unused")
                if name == "control_plane":
                    patch.setenv("LOOM_CP_STEP_JWT_SIGNING_KEY", "unused-test-key")
                else:
                    patch.setenv("LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID", "test-actuator")
                parsed.append(cls(_env_file=None))
            cp, actuator, service = parsed
            assert cp.global_pool == actuator.global_pool
            assert cp.global_pool.participant.participant_id == target.participant_id
            assert service.pool_submission_source.data_environment_id == cp.global_pool.participant.environment_id
            assert service.pool_submission_source.kind == "environment"
            for key, original in zip(("control_plane", "actuator", "service"), originals, strict=True):
                current = result[key]
                component = "execution_actuator" if key == "actuator" else key
                assert current["spec"]["template"]["spec"]["containers"][0]["image"] == request.registration.candidate["images"][component]["image_ref"]
                old_refs = {name: row for name, row in env(original).items() if "valueFrom" in row}
                assert {name: row for name, row in env(current).items() if "valueFrom" in row} == old_refs
                assert current["metadata"]["uid"] == original["metadata"]["uid"]
                if key != "service":
                    pod = current["spec"]["template"]["spec"]
                    credential, = [row for row in pod["volumes"] if row["name"] == "pool-token-source"]
                    machine, = [row for row in request.registration.spec.machines if row.participant_id == target.participant_id]
                    assert credential["secret"]["secretName"] == "loom-pool-machine-" + machine.machine_id.hex
                    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["fsGroup"] == 1000
                    initializer, = [row for row in pod["initContainers"] if row["name"] == "prepare-pool-token"]
                    assert initializer["image"] == request.registration.candidate["images"]["service"]["image_ref"]
            assert not any("pool-token" in row["name"] for row in result["service"]["spec"]["template"]["spec"]["volumes"])


@pytest.mark.parametrize("damage", ["namespace", "target", "builder", "shared_api", "http", "already_global"])
def test_runtime_wiring_refuses_unqualified_participant_inputs(runtime_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_participant

    request, actuators, services, _ = runtime_inputs
    target = request.guards[0]
    actuator, service = copy.deepcopy(actuators[target.participant_id]), copy.deepcopy(services[target.participant_id])
    origin = "https://manage.example.com"
    if damage == "namespace":
        actuator["metadata"]["namespace"] = "foreign"
    elif damage == "target":
        env(actuator)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"] = "foreign"
    elif damage == "builder":
        value = json.loads(env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"])
        value["source_bucket"] = "foreign"
        env(actuator)["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"] = json.dumps(value)
    elif damage == "shared_api":
        service["metadata"]["namespace"] = "loom-personal-alice"
    elif damage == "http":
        origin = "http://manage.example.com"
    elif damage == "already_global":
        actuator["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL", "value": "{}"})
    with pytest.raises(ValueError):
        wire_participant(request=request, participant_id=target.participant_id, management_origin=origin, actuator=actuator, service=service)
