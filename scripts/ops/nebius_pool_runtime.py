"""Pure, disabled runtime targets for the protected shared-pool cutover.

These functions never apply resources or grant authority. The parent retains
original UIDs/templates, closes intake and retires old writers before applying
or starting any target. Replays compare that retained original, not a new read.
"""
from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract

from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_service.pool_management.installation_render import mount_machine_token


def _image(request: PoolMigrationRequest, component: str) -> str:
    value = request.registration.candidate["images"][component]["image_ref"]
    if not isinstance(value, str) or re.fullmatch(r".+@sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("unqualified pool runtime image")
    return value


def _disabled(original: dict[str, Any], *, namespace: str, name: str,
              container_name: str, image: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    _uid(original)
    _snapshot(original)
    if (original.get("apiVersion") != "apps/v1" or original.get("kind") != "Deployment"
            or original["metadata"].get("namespace") != namespace or original["metadata"].get("name") != name
            or type(original["spec"].get("replicas")) is not int or original["spec"]["replicas"] != 1):
        raise ValueError("unqualified original pool process")
    result = copy.deepcopy(original)
    pod = result["spec"]["template"]["spec"]
    container, = pod["containers"]
    if container.get("name") != container_name or container.get("envFrom"):
        raise ValueError("unqualified pool process container")
    container["image"] = image
    result["spec"]["replicas"] = 0
    result["spec"]["strategy"] = {"type": "Recreate"}
    result.pop("status", None)
    return result, pod, container


def _environment(container: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = container["env"]
    result = {row["name"]: row for row in rows}
    if len(result) != len(rows):
        raise ValueError("duplicate process setting")
    return result


def wire_manager(*, request: PoolMigrationRequest, original: dict[str, Any]) -> dict[str, Any]:
    try:
        migration_contract(request)
        binding, spec = request.registration.binding, request.registration.spec
        result, pod, container = _disabled(original, namespace=binding.namespace, name="loom-service",
            container_name="loom-service", image=_image(request, "service"))
        settings = _environment(container)
        if (original["metadata"].get("labels", {}).get("loom.nebius/management-installation") != binding.installation_id
                or settings["LOOM_SVC_SERVICE_MODE"].get("value") != "management"
                or "LOOM_SVC_POOL_PROFILES_FILE" in settings
                or any(row["name"] == "pool-profiles" for row in pod.get("volumes", []))
                or any(row["mountPath"].startswith("/var/run/loom-pool-profiles") for row in container.get("volumeMounts", []))):
            raise ValueError
        pod.setdefault("volumes", []).append({"name": "pool-profiles", "configMap": {
            "name": "loom-pool-profiles-" + spec.operation_id.hex, "items": [{"key": "profiles.json", "path": "profiles.json"}]}})
        container.setdefault("volumeMounts", []).append({"name": "pool-profiles", "mountPath": "/var/run/loom-pool-profiles", "readOnly": True})
        container["env"].append({"name": "LOOM_SVC_POOL_PROFILES_FILE", "value": "/var/run/loom-pool-profiles/profiles.json"})
        return result
    except Exception:
        raise ValueError("pool_manager_runtime_unqualified") from None


def wire_participant(*, request: PoolMigrationRequest, participant_id: UUID, management_origin: str,
                     actuator: dict[str, Any], service: dict[str, Any]) -> dict[str, dict[str, Any]]:
    try:
        migration_contract(request)
        spec = request.registration.spec
        participant, = (row for row in spec.participants if row.participant_id == participant_id)
        guard, = (row for row in request.guards if row.participant_id == participant_id)
        machine, = (row for row in spec.machines if row.participant_id == participant_id)
        cp, cp_pod, cp_container = _disabled(guard.controller, namespace=guard.namespace, name="loom-control-plane",
            container_name="loom-control-plane", image=_image(request, "control_plane"))
        worker, worker_pod, worker_container = _disabled(actuator, namespace=participant.execution_namespace.name,
            name="loom-execution-actuator", container_name="actuator", image=_image(request, "execution_actuator"))
        api, _, api_container = _disabled(service, namespace=guard.namespace, name="loom-service",
            container_name="loom-service", image=_image(request, "service"))
        cp_env, worker_env, api_env = (_environment(row) for row in (cp_container, worker_container, api_container))
        cp_setting = "LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON"
        worker_setting = "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL"
        api_setting = "LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"
        if (cp_setting in cp_env or worker_setting in worker_env or api_setting in api_env
                or api_env.get("LOOM_SVC_SERVICE_MODE", {"value": "full"}).get("value") != "full"
                or cp_env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"].get("value") != participant.environment_class
                or worker_env["LOOM_EXECUTION_ACTUATOR_NAMESPACE"].get("value") != participant.execution_namespace.name):
            raise ValueError
        target_id = worker_env["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"]
        participant.target(target_id, "trial")
        build_target = participant.target(target_id, "task_image_build")
        profile, = (row for row in spec.profiles.task_images if row.profile_id == build_target.profile_id)
        existing = NativeTaskImageSettings.model_validate_json(worker_env["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"])
        service_image = _image(request, "service")
        # Only the qualified candidate may replace the trusted preparation image;
        # source/storage/registry/sandbox and local pool identity remain unchanged.
        expected = existing.model_copy(update={"service_image": service_image})
        if (expected != profile.settings or profile.settings.pool_id != cp_env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID"]["value"]
                or profile.settings.namespace != participant.build_namespace.name):
            raise ValueError
        token_path = mount_machine_token(cp_pod, machine_id=machine.machine_id, service_image=service_image)
        mount_machine_token(worker_pod, machine_id=machine.machine_id, service_image=service_image, default_uid=65532)
        runtime = PoolRuntimeSettings(participant=participant, environment=participant.environment_class,
            logical_pool_id=profile.settings.pool_id, management_origin=management_origin, bearer_token_file=Path(token_path))
        cp_container["env"].append({"name": cp_setting, "value": runtime.model_dump_json()})
        worker_container["env"].append({"name": worker_setting, "value": runtime.model_dump_json()})
        worker_env["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"]["value"] = profile.settings.model_dump_json()
        source = PoolSubmissionSourceV1(kind="environment", data_environment_id=participant.environment_id, application=None)
        api_container["env"].append({"name": api_setting, "value": source.model_dump_json()})
        return {"control_plane": cp, "actuator": worker, "service": api}
    except Exception:
        raise ValueError("pool_participant_runtime_unqualified") from None
