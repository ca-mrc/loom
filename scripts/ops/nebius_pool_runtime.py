"""Pure, disabled runtime targets for the protected shared-pool cutover.

These functions never apply resources or grant authority. The parent retains
original UIDs/templates, closes intake and retires old writers before applying
or starting any target. Replays compare that retained original, not a new read.
"""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract

from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.nebius_platform_render import _obj
from loom.nebius_pool_priority import PoolSubmissionSourceV1
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_execution_capacity_collector.config import PoolCapacityCollectorSettings
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
                     actuator: dict[str, Any], service: dict[str, Any],
                     runtime_profile: ServiceExecutionRuntimeProfileV1) -> dict[str, dict[str, Any]]:
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
        execution_target = participant.target(target_id, "trial")
        execution_profile, = (row for row in spec.profiles.execution if row.profile_id == execution_target.profile_id)
        runtime_profile = ServiceExecutionRuntimeProfileV1.model_validate(runtime_profile.model_dump())
        previous_profile = ServiceExecutionRuntimeProfileV1.model_validate_json(api_env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["value"])
        publication_fields = {"candidate_sha", "task_image_ref", "agent_image_ref", "runtime_image_ref", "runtime_binary_sha256", "image_admission"}
        if ({key: value for key, value in previous_profile.model_dump().items() if key not in publication_fields}
                != {key: value for key, value in runtime_profile.model_dump().items() if key not in publication_fields}
                or (runtime_profile.candidate_sha, runtime_profile.execution_class_id, runtime_profile.runtime_image_ref, runtime_profile.runtime_binary_sha256)
                    != (execution_profile.candidate_sha, execution_profile.execution_class_id, execution_profile.runtime_image_ref, execution_profile.runtime_binary_sha256)
                or runtime_profile.candidate_sha != request.registration.candidate["candidate_sha"]
                or runtime_profile.task_image_ref != _image(request, "service")
                or runtime_profile.runtime_image_ref != _image(request, "execution_runtime")
                or runtime_profile.logical_pool_id != cp_env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID"]["value"]):
            raise ValueError
        agent_component = next((key for key in ("harbor_runtime", "worker") if key in request.registration.candidate["images"]), None)
        if runtime_profile.agent_image_ref != (_image(request, agent_component) if agent_component else None):
            raise ValueError
        keyring_json = json.dumps(spec.profiles.image_admission_keyring, sort_keys=True, separators=(",", ":"))
        verify_execution_image_admission(runtime_profile.image_admission, keyring=ImageAdmissionKeyring.from_json(keyring_json),
            required_image_refs=[value for value in (runtime_profile.task_image_ref, runtime_profile.runtime_image_ref, runtime_profile.agent_image_ref) if value is not None])
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
        api_env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["value"] = runtime_profile.model_dump_json()
        for container, settings, prefix in ((cp_container, cp_env, "LOOM_CP_"),
                (worker_container, worker_env, "LOOM_EXECUTION_ACTUATOR_")):
            key = prefix + "EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON"
            if key in settings:
                if set(settings[key]) != {"name", "value"}:
                    raise ValueError
                settings[key]["value"] = keyring_json
            else:
                container["env"].append({"name": key, "value": keyring_json})
        source = PoolSubmissionSourceV1(kind="environment", data_environment_id=participant.environment_id, application=None)
        api_container["env"].append({"name": api_setting, "value": source.model_dump_json()})
        return {"control_plane": cp, "actuator": worker, "service": api}
    except Exception:
        raise ValueError("pool_participant_runtime_unqualified") from None


def wire_collector(*, request: PoolMigrationRequest, original: dict[str, Any], config_map: dict[str, Any],
                   management_origin: str) -> dict[str, tuple[dict[str, Any], ...]]:
    """Reuse development's cloud reader; return a suspended pool-only CronJob.

The parent must suspend/drain ALL previous collectors before changing this
template. Its existing SA needs read-only nodes/Pods/DaemonSets, not Job writes.
No cloud credential is copied or replaced. The historical token filename is
retained for the existing two-file initializer, but now holds observer authority.
"""
    try:
        migration_contract(request)
        spec = request.registration.spec
        development, = (row for row in spec.participants if row.environment_class == "development")
        observer, = (row for row in spec.machines if row.role == "observer")
        namespace, name = development.execution_namespace.name, "loom-execution-capacity-collector"
        for document, kind, version in ((original, "CronJob", "batch/v1"), (config_map, "ConfigMap", "v1")):
            _uid(document)
            _snapshot(document)
            if (document.get("apiVersion") != version or document.get("kind") != kind
                    or document["metadata"].get("name") != name or document["metadata"].get("namespace") != namespace):
                raise ValueError
        origin = urlsplit(management_origin)
        if (origin.scheme != "https" or not origin.hostname or origin.username is not None or origin.password is not None
                or origin.path not in {"", "/"} or origin.query or origin.fragment):
            raise ValueError
        result = copy.deepcopy(original)
        pod = result["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        container, = pod["containers"]
        initializer, = pod["initContainers"]
        if (container.get("name") != "collector" or container.get("command") != ["python", "-m", "loom_execution_capacity_collector"]
                or container.get("envFrom") != [{"configMapRef": {"name": name}}]
                or pod.get("serviceAccountName") != name or result["spec"].get("concurrencyPolicy") != "Forbid"
                or initializer.get("command") != ["python", "-m", "loom_execution_capacity_collector.secret_init",
                    "--source", "/var/run/loom-projected", "--destination", "/var/run/loom-owned/credentials"]):
            raise ValueError
        prefix = "LOOM_EXECUTION_CAPACITY_COLLECTOR_"
        env = _environment(container)
        paths = {prefix + "NEBIUS_CREDENTIALS_FILE": "/var/run/loom-owned/credentials/nebius-credentials.json",
            prefix + "CONTROL_PLANE_BEARER_TOKEN_FILE": "/var/run/loom-owned/credentials/control-plane-token"}
        if (set(env) != set(paths) or any(env[key] != {"name": key, "value": value} for key, value in paths.items())
                or not all(isinstance(key, str) and key.startswith(prefix) and isinstance(value, str)
                    for key, value in config_map["data"].items())):
            raise ValueError
        supplied = {key.removeprefix(prefix).lower(): value for key, value in config_map["data"].items()}
        legacy = {"target_id", "pool_id", "namespace", "node_label_selector", "control_plane_url", "source",
            "request_attempts", "build_concurrency_limit"}
        if ("collection_mode" in supplied or not set(supplied) <= set(PoolCapacityCollectorSettings.model_fields) | legacy
                or {"management_url", "management_bearer_token_file"} & set(supplied)):
            raise ValueError
        values = {key: field.default for key, field in PoolCapacityCollectorSettings.model_fields.items() if not field.is_required()}
        values.update({key: value for key, value in supplied.items() if key not in legacy})
        values.update(pool_id=spec.pool_id, management_url=management_origin,
            nebius_credentials_file=paths[prefix + "NEBIUS_CREDENTIALS_FILE"],
            management_bearer_token_file=paths[prefix + "CONTROL_PLANE_BEARER_TOKEN_FILE"])
        settings = PoolCapacityCollectorSettings(_env_file=None, **values)
        quotas = {key: (settings.nebius_quota_parent_id or settings.nebius_project_id, settings.nebius_region,
            settings.quota_service, getattr(settings, "quota_" + key + "_name"), getattr(settings, "quota_" + key + "_unit"))
            for key in ("nodes", "vcpu", "memory", "storage") if getattr(settings, "quota_" + key + "_name") is not None}
        if (settings.nebius_node_group_id != spec.node_group_id or settings.kubernetes_connection is not None
                or quotas != spec.quota_identities):
            raise ValueError
        projected, = (row for row in pod["volumes"] if row["name"] == "projected-credentials")
        expected_sources = [
            {"secret": {"name": name + "-nebius", "items": [{"key": "credentials.json", "path": "nebius-credentials.json"}]}},
            {"secret": {"name": name + "-control-plane", "items": [{"key": "token", "path": "control-plane-token"}]}}]
        if projected["projected"]["sources"] != expected_sources:
            raise ValueError
        projected["projected"]["sources"][1]["secret"]["name"] = "loom-pool-machine-" + observer.machine_id.hex
        config_name = "loom-pool-collector-" + spec.operation_id.hex
        data = {prefix + key.upper(): str(value) for key, value in settings.model_dump(mode="json").items()
            if value is not None and key not in {"nebius_credentials_file", "management_bearer_token_file"}}
        data[prefix + "COLLECTION_MODE"] = "pool"
        configuration = {"apiVersion": "v1", "kind": "ConfigMap", "immutable": True,
            "metadata": {"name": config_name, "namespace": namespace, "labels": {
                "loom.nebius/management-installation": str(spec.installation_id), "loom.nebius/pool-operation": str(spec.operation_id)}},
            "data": data}
        container["envFrom"] = [{"configMapRef": {"name": config_name}}]
        container["env"] = [{"name": prefix + key.upper(), "value": str(getattr(settings, key))}
            for key in ("nebius_credentials_file", "management_bearer_token_file")]
        container["image"] = initializer["image"] = _image(request, "execution_actuator")
        result["spec"]["suspend"] = True
        result.pop("status", None)
        return {"configuration": (configuration,), "workload": (result,)}
    except Exception:
        raise ValueError("pool_collector_runtime_unqualified") from None


def participant_readonly_roles(*, request: PoolMigrationRequest) -> tuple[dict[str, Any], ...]:
    """Fixed replacements for the existing actuator and native-builder roles.

Not sufficient alone to fence a writer: the parent must also qualify the full
binding inventory and stop all old processes before granting gateway authority.
The existing node-usage reader is unchanged; CPs need no Kubernetes permission.
"""
    migration_contract(request)
    documents: list[dict[str, Any]] = []
    for participant in request.registration.spec.participants:
        subject = {"kind": "ServiceAccount", "name": "loom-execution-actuator", "namespace": participant.execution_namespace.name}
        identity_name = "loom-pool-reader-" + participant.participant_id.hex
        identity = _obj("ClusterRole", identity_name, None, api="rbac.authorization.k8s.io/v1")
        identity["rules"] = [{"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get"],
            "resourceNames": [participant.execution_namespace.name, participant.build_namespace.name]}]
        binding = _obj("ClusterRoleBinding", identity_name, None, api="rbac.authorization.k8s.io/v1")
        binding.update(subjects=[subject.copy()], roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": identity_name})
        documents.extend((identity, binding))
        for namespace, name in ((participant.execution_namespace.name, "loom-execution-actuator"),
                (participant.build_namespace.name, "loom-task-image-builder")):
            role = _obj("Role", name, namespace, api="rbac.authorization.k8s.io/v1")
            role["rules"] = [{"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["get"]},
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]
            if namespace == participant.build_namespace.name:
                role["rules"].append({"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]})
            binding = _obj("RoleBinding", name, namespace, api="rbac.authorization.k8s.io/v1")
            binding.update(subjects=[subject.copy()], roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name})
            documents.extend((role, binding))
    for document in documents:
        document["metadata"]["labels"] = {"loom.nebius/management-installation": str(request.registration.spec.installation_id),
            "loom.nebius/pool-operation": str(request.registration.spec.operation_id)}
    return tuple(documents)
