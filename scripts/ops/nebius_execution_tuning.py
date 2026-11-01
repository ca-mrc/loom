#!/usr/bin/env python3
"""Persist and apply standalone native execution tuning without replacing task Jobs."""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    DeploymentError,
    Kubectl,
    verify_cluster_identity,
)

from loom.nebius_platform_render import (  # noqa: E402
    _regional_config,
    _task_image_builder_documents,
    _task_image_builder_settings,
    postgres_settings,
    task_image_builder_resource_ceiling,
    validate_environment,
)
from loom.nebius_rollout_guard import LOCK_KEY  # noqa: E402
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings  # noqa: E402

_API = '''import json,os,sys,tomllib,urllib.request,urllib.error
from pathlib import Path
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs): return None
token=tomllib.loads(Path(os.environ['LOOM_CP_ADMIN_SECRET_FILE']).read_text())['admin']['token']
method,path,body=sys.argv[1:]
request=urllib.request.Request('http://127.0.0.1:8080'+path,method=method,
 data=None if body=='null' else body.encode(),
 headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
try:
 with urllib.request.build_opener(NoRedirect).open(request,timeout=30) as response:
  print(json.dumps(json.load(response)))
except urllib.error.HTTPError as error:
 print(json.dumps({'error_status':error.code}))
'''


# Use the same transaction lock and singleton as ordinary rollout acquisition.
# Active work is allowed for tuning; admission pauses briefly, never cancelled.
_TUNING_ACQUIRE_SQL = f"""
WITH mutex AS (SELECT pg_try_advisory_xact_lock({LOCK_KEY}) AS acquired)
INSERT INTO nebius_rollout_guard(id, owner, candidate_sha)
SELECT 1, :owner, :candidate WHERE (SELECT acquired FROM mutex)
ON CONFLICT (id) DO NOTHING RETURNING id
"""
_TUNING_RELEASE_SQL = """
DELETE FROM nebius_rollout_guard
WHERE id = 1 AND owner = :owner AND candidate_sha = :candidate RETURNING id
"""
_GUARD = '''import asyncio,json,sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession,create_async_engine
from loom_control_plane.config import ControlPlaneSettings
async def main():
 settings=ControlPlaneSettings()
 engine=create_async_engine(settings.db_engine_url,connect_args=settings.db_engine_connect_args)
 try:
  async with AsyncSession(engine) as session,session.begin():
   value=await session.scalar(text(sys.argv[1]),{'owner':sys.argv[2],'candidate':sys.argv[3]})
  print(json.dumps({'ok':value is not None}))
 finally:await engine.dispose()
try:asyncio.run(main())
except Exception:
 print('Tuning guard unavailable; retained guard ownership must be inspected',file=sys.stderr)
 raise SystemExit(1)
'''


@contextmanager
def _tuning_guard(kube: Kubectl, namespace: str, candidate: str) -> Iterator[None]:
    if not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise DeploymentError("invalid tuning candidate")
    owner = "execution-tuning-" + uuid4().hex
    print(f"Execution tuning guard: owner={owner} candidate={candidate}", file=sys.stderr)

    def execute(query: str) -> bool:
        return bool(json.loads(kube.run(
            "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c",
            _GUARD, query, owner, candidate,
        ))["ok"])

    if not execute(_TUNING_ACQUIRE_SQL):
        raise DeploymentError("a version rollout owns the platform, or another tuning operation is active")
    try:
        yield
    finally:
        if not execute(_TUNING_RELEASE_SQL):
            raise DeploymentError("tuning guard release unconfirmed; inspect its exact retained owner")


def _api(kube: Kubectl, namespace: str, method: str, path: str, body: object = None) -> dict[str, Any]:
    result = json.loads(kube.run(
        "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c",
        _API, method, path, json.dumps(body),
    ))
    if "error_status" in result:
        raise DeploymentError(f"control-plane tuning request rejected: HTTP {result['error_status']}")
    return cast(dict[str, Any], result)


def _owners(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {config["target_id"]: config, **{
        row["target_id"]: row for row in config.get("regional_execution_targets", [])
    }}


def _builders(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Expose resource intent without storage locations or credential contents."""
    result = {}
    for target, owner in _owners(config).items():
        builder = owner.get("task_image_builder")
        result[target] = {"enabled": builder is not None}
        if builder is not None:
            result[target].update({key: builder.get(key) for key in (
                "cpu_request_millis", "cpu_millis", "memory_mib", "ephemeral_storage_mib", "max_concurrent", "resource_ceiling",
            )})
    return result


def _env(deployment: dict[str, Any]) -> dict[str, str]:
    return {row["name"]: row["value"] for row in
            deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
            if "value" in row}


def _deployments(kube: Kubectl, namespace: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], json.loads(
        kube.run("get", "deployments", "-n", namespace, "-o", "json"),
    )["items"])


def _runtime(kube: Kubectl, controller: dict[str, Any]) -> dict[str, Any]:
    name, ns = controller["metadata"]["name"], controller["metadata"]["namespace"]
    if controller["spec"].get("replicas", 1) == 0:
        if name == "loom-control-plane":
            raise DeploymentError("control-plane has no running replicas")
        return {"command_concurrency": None, "configured_command_concurrency": int(
            _env(controller).get("LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY", "1"),
        )}
    if name == "loom-control-plane":
        script = ("import json; from loom_control_plane.config import ControlPlaneSettings; "
                  "s=ControlPlaneSettings(); "
                  "print(json.dumps({'placement_policy':s.service_execution_placement_policy}))")
    else:
        script = ("import json; from loom_execution_actuator.config import ExecutionActuatorSettings; "
                  "s=ExecutionActuatorSettings(); b=s.task_image_builder; "
                  "print(json.dumps({'command_concurrency':getattr(s,'command_concurrency',None), "
                  "'builder_enabled':b is not None, "
                  "'builder_request_support':b is not None and hasattr(b,'cpu_request_millis'), "
                  "'builder_resources':None if b is None else {"
                  "'effective_cpu_request_millis':getattr(b,'cpu_request_millis',None) or b.cpu_millis, "
                  "'cpu_millis':b.cpu_millis,'memory_mib':b.memory_mib,"
                  "'ephemeral_storage_mib':b.ephemeral_storage_mib,'namespace':b.namespace}}))")
    return cast(dict[str, Any], json.loads(kube.run(
        "exec", "-n", ns, "deployment/" + name, "--", "python", "-c", script,
    )))


def _read(kube: Kubectl, namespace: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    cm = kube.get("configmap", "loom-platform-config", namespace)
    config, profile = (json.loads(cm["data"][key]) for key in ("environment.json", "profile.json"))
    validate_environment(config)
    if config["namespace"] != namespace or config.get("registration"):
        raise DeploymentError("tuning requires the bound standalone platform")
    cp = kube.get("deployment", "loom-control-plane", namespace)
    actuators = [row for row in _deployments(kube, config["execution_namespace"])
                 if "LOOM_EXECUTION_ACTUATOR_TARGET_ID" in _env(row)]
    if not cp or not actuators:
        raise DeploymentError("execution controllers unavailable")
    return cm, config, profile, [cp, *actuators]


def _stage_postgres_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str, settings: dict[str, int],
) -> dict[str, Any]:
    """Save restart-requiring settings for the next normal maintenance rollout."""
    cm, config, profile, _ = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("database settings candidate differs from installed platform")
    if not settings:
        raise DeploymentError("at least one Postgres setting is required")
    updated = deepcopy(config)
    updated["postgres_settings"] = {**postgres_settings(config), **settings}
    validate_environment(updated)
    if updated != config:
        kube.run("patch", "configmap", "loom-platform-config", "-n", namespace, "--type=json", "-p",
                 json.dumps([
                     {"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]},
                     {"op": "replace", "path": "/data/environment.json", "value": json.dumps(updated)},
                 ]))
    saved = json.loads(kube.get("configmap", "loom-platform-config", namespace)["data"]["environment.json"])
    if saved.get("postgres_settings") != updated["postgres_settings"]:
        raise DeploymentError("saved Postgres settings differ")
    return {"candidate_sha": profile["candidate_sha"], "postgres_settings": saved["postgres_settings"],
            "database_restarted": False, "effective_on_next_deployment": True}


def inspect(kube: Kubectl, namespace: str) -> dict[str, Any]:
    _, config, profile, controllers = _read(kube, namespace)
    policies = _api(kube, namespace, "GET", "/admin/execution-capacity/status")["targets"]
    return {
        "candidate_sha": profile["candidate_sha"],
        "persisted": {
            "placement_policy": config.get("execution_placement_policy", "environment-local-health-first"),
            "command_concurrency": config.get("execution_command_concurrency", 1),
            "builders": _builders(config),
            "create_rates": {key: row["capacity_policy"]["max_create_per_minute"]
                             for key, row in _owners(config).items()},
        },
        "controllers": [{
            "name": row["metadata"]["name"],
            **_runtime(kube, row),
            "ready_replicas": row.get("status", {}).get("readyReplicas", 0),
            "replicas": row["spec"].get("replicas", 1),
        } for row in controllers],
        "live_create_rates": {row["target_id"]: row["policy"]["max_create_per_minute"]
                              for row in policies if row["target_id"] in _owners(config) and row.get("policy")},
    }


def _updated_builders(
    config: dict[str, Any], resources: dict[str, dict[str, int]], enable_regional: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not resources and not enable_regional:
        raise DeploymentError("at least one build setting is required")
    updated = deepcopy(config)
    owners = _owners(updated)
    regional_ids = {row["target_id"] for row in updated.get("regional_execution_targets", [])}
    if len(set(enable_regional)) != len(enable_regional) or any(target not in regional_ids for target in enable_regional):
        raise DeploymentError("builder activation intent requires a distinct configured regional target")
    primary = config.get("task_image_builder")
    for target in enable_regional:
        if primary is None:
            raise DeploymentError("regional builder requires reviewed primary build/storage settings")
        if owners[target].get("task_image_builder") is None:
            owners[target]["task_image_builder"] = deepcopy(primary)
    limits = {"cpu_millis", "memory_mib", "ephemeral_storage_mib"}
    for target, values in resources.items():
        if (target not in owners or owners[target].get("task_image_builder") is None
                or not values or set(values) - (limits | {"cpu_request_millis"})
                or any(type(value) is not int for value in values.values())):
            raise DeploymentError("build resources require a configured builder and integer resource settings")
        owner = owners[target]
        effective_config = updated if target == updated["target_id"] else _regional_config(updated, owner)
        settings = _task_image_builder_settings(effective_config, service_image="test.invalid/service@sha256:" + "a" * 64)
        ceiling = task_image_builder_resource_ceiling(owner["task_image_builder"], settings)
        if any(key in values and values[key] > ceiling[key] for key in limits):
            raise DeploymentError("build tuning must preserve existing hard resource ceilings")
        owner["task_image_builder"]["resource_ceiling"] = ceiling
        owner["task_image_builder"].update(values)
    validate_environment(updated)
    return updated


def _stage_builders_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    resources: dict[str, dict[str, int]], enable_regional: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Persist future build intent; installation and permission checks own activation.

    Existing Jobs retain their frozen settings. Enabling a regional builder must
    go through the normal reviewed regional namespace/Secret/RBAC rollout.
    """
    cm, config, profile, _ = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("build settings candidate differs from installed platform")
    updated = _updated_builders(config, resources, enable_regional)
    if updated != config:
        kube.run("patch", "configmap", "loom-platform-config", "-n", namespace, "--type=json", "-p",
                 json.dumps([
                     {"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]},
                     {"op": "replace", "path": "/data/environment.json", "value": json.dumps(updated)},
                 ]))
    saved = json.loads(kube.get("configmap", "loom-platform-config", namespace)["data"]["environment.json"])
    if any(_owners(saved)[target].get("task_image_builder") != owner.get("task_image_builder")
           for target, owner in _owners(updated).items()):
        raise DeploymentError("saved build settings differ")
    return {"candidate_sha": profile["candidate_sha"], "builders": _builders(saved),
            "controllers_restarted": False, "task_jobs_replaced": False,
            "effective_on_next_deployment": True,
            "regional_preflight_required": list(enable_regional)}


def _apply_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    placement_policy: str | None = None, command_concurrency: int | None = None,
    create_rates: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Save desired values first, then converge controllers/API; retry is safe.

    Uncertain or partial application remains a failed operation. Re-run the same
    values to converge and read back; never rewrite batches or execution leases.
    """
    cm, config, profile, controllers = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("tuning candidate differs from the installed platform")
    if any(_runtime(kube, row).get("command_concurrency") is None
           for row in controllers if row["metadata"]["name"] != "loom-control-plane"
           and row["spec"].get("replicas", 1) > 0):
        raise DeploymentError("installed actuator lacks tuning support; deploy the combined candidate first")
    if placement_policy is None and command_concurrency is None and not create_rates:
        raise DeploymentError("at least one tuning value is required")
    updated = deepcopy(config)
    if placement_policy is not None:
        updated["execution_placement_policy"] = placement_policy
    if command_concurrency is not None:
        updated["execution_command_concurrency"] = command_concurrency
    owners = _owners(updated)
    for target, rate in (create_rates or {}).items():
        if target not in owners or type(rate) is not int or rate < 1:
            raise DeploymentError("create rate requires a known physical owner and positive integer")
        owners[target]["capacity_policy"]["max_create_per_minute"] = rate
    validate_environment(updated)
    # Persist rate changes from the current policy, preserving all other limits.
    live = _api(kube, namespace, "GET", "/admin/execution-capacity/status")["targets"]
    policies = {row["target_id"]: row.get("policy") for row in live}
    bodies = {}
    for target, rate in (create_rates or {}).items():
        policy = policies.get(target)
        if policy is None:
            raise DeploymentError("capacity owner policy unavailable")
        saved = _owners(config)[target]["capacity_policy"]
        if any(policy.get(key) != value for key, value in saved.items() if key != "max_create_per_minute"):
            raise DeploymentError("saved and live non-rate capacity limits differ; preserve existing limits")
        bodies[target] = {key: policy[key] for key in saved}
        bodies[target]["reason"] = policy.get("reason")
        bodies[target]["max_create_per_minute"] = rate
    operations = [{"op": "test", "path": "/metadata/resourceVersion",
                   "value": cm["metadata"]["resourceVersion"]}]
    if updated != config:
        operations.append({"op": "replace", "path": "/data/environment.json", "value": json.dumps(updated)})
        if placement_policy is not None:
            catalog = json.loads(cm["data"]["catalog.json"])
            catalog["topology"]["placement_policy"] = placement_policy
            operations.append({"op": "replace", "path": "/data/catalog.json", "value": json.dumps(catalog)})
        kube.run("patch", "configmap", "loom-platform-config", "-n", namespace,
                 "--type=json", "-p", json.dumps(operations))
    for target, body in bodies.items():
        observed = _api(kube, namespace, "PUT", "/admin/execution-capacity-policies/" + quote(target, safe=""), body)
        if any(observed.get(key) != value for key, value in body.items()):
            raise DeploymentError("capacity-policy readback differs; retry the retained tuning values")
    restarted = []
    for controller in controllers:
        env = _env(controller)
        changes = {}
        if placement_policy is not None and controller["metadata"]["name"] == "loom-control-plane":
            changes["LOOM_CP_SERVICE_EXECUTION_PLACEMENT_POLICY"] = placement_policy
        if command_concurrency is not None and "LOOM_EXECUTION_ACTUATOR_TARGET_ID" in env:
            changes["LOOM_EXECUTION_ACTUATOR_COMMAND_CONCURRENCY"] = str(command_concurrency)
        changes = {key: value for key, value in changes.items() if env.get(key) != value}
        if changes:
            name, ns = controller["metadata"]["name"], controller["metadata"]["namespace"]
            kube.run("set", "env", "deployment/" + name, "-n", ns,
                     *(key + "=" + value for key, value in changes.items()))
            if controller["spec"].get("replicas", 1) > 0:
                restarted.append((name, ns))
    for name, ns in restarted:
        kube.run("rollout", "status", "deployment/" + name, "-n", ns, "--timeout=300s", timeout=330)
    result = inspect(kube, namespace)
    if result["candidate_sha"] != expected_candidate:
        raise DeploymentError("platform candidate changed during tuning; inspect before continuing")
    if any(result["persisted"]["create_rates"].get(target) != rate
           or result["live_create_rates"].get(target) != rate for target, rate in (create_rates or {}).items()):
        raise DeploymentError("saved and live creation rates differ; inspect before continuing")
    for controller in result["controllers"]:
        if controller["ready_replicas"] != controller["replicas"]:
            raise DeploymentError("controller is not ready")
        if controller["name"] == "loom-control-plane":
            if controller["placement_policy"] != result["persisted"]["placement_policy"]:
                raise DeploymentError("saved and live placement differ")
        elif (controller["command_concurrency"] if controller["replicas"] > 0
              else controller["configured_command_concurrency"]) != result["persisted"]["command_concurrency"]:
            raise DeploymentError("saved and live command concurrency differ")
    result["restarted_controllers"] = [name for name, _ in restarted]
    result["task_jobs_replaced"] = False
    return result


def stage_postgres(
    kube: Kubectl, namespace: str, *, expected_candidate: str, settings: dict[str, int],
) -> dict[str, Any]:
    """Persist database intent while excluding deployments and other tuning."""
    with _tuning_guard(kube, namespace, expected_candidate):
        return _stage_postgres_locked(kube, namespace, expected_candidate=expected_candidate, settings=settings)


def stage_builders(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    resources: dict[str, dict[str, int]], enable_regional: tuple[str, ...] = (),
) -> dict[str, Any]:
    with _tuning_guard(kube, namespace, expected_candidate):
        return _stage_builders_locked(kube, namespace, expected_candidate=expected_candidate,
                                      resources=resources, enable_regional=enable_regional)


def _builder_kube(kube: Kubectl, config: dict[str, Any], target: str) -> Kubectl:
    if target == config["target_id"]:
        return kube
    owner = _owners(config)[target]
    view = json.loads(kube.run("config", "view", "-o", "json"))
    clusters = {row["name"]: row["cluster"] for row in view.get("clusters", [])}
    contexts = [row["name"] for row in view.get("contexts", [])
                if clusters.get(row["context"].get("cluster"), {}).get("server") == owner["kubernetes_api_server"]]
    if len(contexts) != 1:
        raise DeploymentError("existing builder requires its unique retained regional operator context")
    remote = Kubectl(kube.kubeconfig, context=contexts[0], local_executable=kube.local_executable)
    verify_cluster_identity(remote, owner, owner["cluster_id"])
    return remote


def _job_envelopes(kube: Kubectl, namespace: str) -> dict[str, Any]:
    jobs = json.loads(kube.run("get", "jobs", "-n", namespace, "-o", "json"))["items"]
    return {row["metadata"]["name"]: {
        "uid": row["metadata"]["uid"], "deadline": row["spec"].get("activeDeadlineSeconds"),
        "containers": [{"name": c["name"], "resources": c.get("resources", {})}
                       for c in row["spec"]["template"]["spec"].get("containers", [])],
        "init_containers": [{"name": c["name"], "resources": c.get("resources", {})}
                            for c in row["spec"]["template"]["spec"].get("initContainers", [])],
    } for row in jobs}


def _same_quota(actual: dict[str, Any], expected: dict[str, Any]) -> bool:
    from kubernetes.utils.quantity import parse_quantity

    return set(actual) == set(expected) and all(parse_quantity(actual[key]) == parse_quantity(value)
                                               for key, value in expected.items())


def _retain_live_quota(
    actual: dict[str, Any], desired: dict[str, Any], ceiling: dict[str, int], count: int | None,
) -> dict[str, Any]:
    """Old frozen attempts may still create Jobs; tighten quota only after idle."""
    from kubernetes.utils.quantity import parse_quantity

    if set(actual) != set(desired):
        raise DeploymentError("existing build quota shape differs from configured builder")
    if count is None:
        return desired
    retained = deepcopy(desired)
    for key in ("pods", "count/jobs.batch", "count/configmaps"):
        if parse_quantity(actual[key]) != parse_quantity(desired[key]):
            raise DeploymentError("existing build quota object-count limit differs")
    for resource, field, unit in (("cpu", "cpu_millis", "m"), ("memory", "memory_mib", "Mi"),
                                  ("ephemeral-storage", "ephemeral_storage_mib", "Mi")):
        request, limit = (parse_quantity(actual[kind + "." + resource]) for kind in ("requests", "limits"))
        if (not 0 < request <= limit <= parse_quantity(str(ceiling[field] * count) + unit)
                or (resource != "cpu" and request != limit)):
            raise DeploymentError("existing build quota exceeds retained resource ceilings")
        for kind in ("requests", "limits"):
            key = kind + "." + resource
            if parse_quantity(actual[key]) > parse_quantity(desired[key]):
                amount = parse_quantity(actual[key]) / parse_quantity("1" + unit)
                if amount != int(amount):
                    raise DeploymentError("existing build quota unit differs from integer resource settings")
                retained[key] = str(int(amount)) + unit
    return retained


def _apply_builders_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str, resources: dict[str, dict[str, int]],
) -> dict[str, Any]:
    cm, config, profile, controllers = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("build settings candidate differs from installed platform")
    updated = _updated_builders(config, resources)
    allowed = {"cpu_request_millis", "cpu_millis", "memory_mib", "ephemeral_storage_mib"}
    prepared = {}
    for target in resources:
        matches = [row for row in controllers if _env(row).get("LOOM_EXECUTION_ACTUATOR_TARGET_ID") == target]
        if len(matches) != 1 or matches[0]["spec"].get("replicas", 1) < 1:
            raise DeploymentError("live build tuning requires one running existing actuator")
        controller = matches[0]
        runtime = _runtime(kube, controller)
        if not runtime.get("builder_enabled") or not runtime.get("builder_request_support"):
            raise DeploymentError("live build tuning requires an already enabled supporting builder; use staged protected deployment first")
        raw = _env(controller).get("LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER")
        if not raw:
            raise DeploymentError("existing actuator builder configuration unavailable")
        current = NativeTaskImageSettings.model_validate_json(raw)
        owner = _owners(updated)[target]
        regional = updated if target == updated["target_id"] else _regional_config(updated, owner)
        desired = _task_image_builder_settings(regional, service_image=current.service_image, storage_config=updated)
        if desired is None or any(getattr(current, key) != getattr(desired, key)
                                  for key in NativeTaskImageSettings.model_fields if key not in allowed):
            raise DeploymentError("live build tuning cannot change namespace, credential references, runtime or identity settings")
        desired.job_config()
        remote = _builder_kube(kube, config, target)
        before = {ns: _job_envelopes(remote, ns) for ns in (owner["execution_namespace"], desired.namespace)}
        quota = remote.get("resourcequota", "loom-task-image-build-capacity", desired.namespace)
        if not quota or not quota["metadata"].get("resourceVersion"):
            raise DeploymentError("existing build quota identity unavailable")
        quota_doc = next(d for d in _task_image_builder_documents(regional, desired) if d["kind"] == "ResourceQuota")
        hard = _retain_live_quota(quota["spec"].get("hard", {}), quota_doc["spec"]["hard"],
            task_image_builder_resource_ceiling(owner["task_image_builder"], desired), desired.max_concurrent)
        prepared[target] = (controller, remote, current, desired, before, quota, hard)
    if updated != config:
        kube.run("patch", "configmap", "loom-platform-config", "-n", namespace, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/data/environment.json", "value": json.dumps(updated)},
        ]))
    restarted = []
    snapshots = {}
    for target, (controller, remote, current, desired, before, quota, hard) in prepared.items():
        if not _same_quota(quota["spec"].get("hard", {}), hard):
            remote.run("patch", "resourcequota", "loom-task-image-build-capacity", "-n", desired.namespace,
                       "--type=json", "-p", json.dumps([
                           {"op": "test", "path": "/metadata/resourceVersion", "value": quota["metadata"]["resourceVersion"]},
                           {"op": "replace", "path": "/spec/hard", "value": hard},
                       ]))
        name, ns = controller["metadata"]["name"], controller["metadata"]["namespace"]
        if any(getattr(current, key) != getattr(desired, key) for key in allowed):
            kube.run("set", "env", "deployment/" + name, "-n", ns,
                     "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER=" + desired.model_dump_json())
            kube.run("rollout", "status", "deployment/" + name, "-n", ns, "--timeout=300s", timeout=330)
            restarted.append(name)
        observed = _runtime(kube, controller)
        expected = {"effective_cpu_request_millis": desired.cpu_request_millis or desired.cpu_millis,
                    "cpu_millis": desired.cpu_millis, "memory_mib": desired.memory_mib,
                    "ephemeral_storage_mib": desired.ephemeral_storage_mib, "namespace": desired.namespace}
        if observed.get("builder_resources") != expected:
            raise DeploymentError("live builder resource readback differs; retry retained tuning intent")
        actual_quota = remote.get("resourcequota", "loom-task-image-build-capacity", desired.namespace)
        if not _same_quota(actual_quota.get("spec", {}).get("hard", {}), hard):
            raise DeploymentError("live builder quota readback differs")
        after = {ns: _job_envelopes(remote, ns) for ns in before}
        for ns, jobs in before.items():
            if any(name in after[ns] and after[ns][name] != job for name, job in jobs.items()):
                raise DeploymentError("an existing Job UID or frozen resource envelope changed during tuning")
        snapshots[target] = {"before": {ns: {name: row["uid"] for name, row in jobs.items()} for ns, jobs in before.items()},
                             "after": {ns: {name: row["uid"] for name, row in jobs.items()} for ns, jobs in after.items()}}
    saved_cm = kube.get("configmap", "loom-platform-config", namespace)
    if (json.loads(saved_cm["data"]["profile.json"])["candidate_sha"] != expected_candidate
            or json.loads(saved_cm["data"]["environment.json"]) != updated):
        raise DeploymentError("saved builder intent changed during live tuning")
    return {"candidate_sha": expected_candidate, "builders": _builders(updated),
            "restarted_controllers": restarted, "job_uid_snapshots": snapshots, "task_jobs_replaced": False}


def apply_builders(
    kube: Kubectl, namespace: str, *, expected_candidate: str, resources: dict[str, dict[str, int]],
) -> dict[str, Any]:
    with _tuning_guard(kube, namespace, expected_candidate):
        return _apply_builders_locked(kube, namespace, expected_candidate=expected_candidate, resources=resources)


def apply(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    placement_policy: str | None = None, command_concurrency: int | None = None,
    create_rates: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Hold the deployment mutex through persistence, convergence and readback."""
    with _tuning_guard(kube, namespace, expected_candidate):
        return _apply_locked(kube, namespace, expected_candidate=expected_candidate,
                             placement_policy=placement_policy, command_concurrency=command_concurrency,
                             create_rates=create_rates)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("inspect", "apply", "stage-postgres", "stage-builders", "apply-builders"))
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--expected-candidate")
    parser.add_argument("--placement-policy", choices=("environment-local-health-first", "environment-local-capacity-balanced"))
    parser.add_argument("--command-concurrency", type=int, choices=range(1, 9))
    parser.add_argument("--create-rate", action="append", default=[], metavar="TARGET=PER_MINUTE")
    parser.add_argument("--enable-regional-builder", action="append", default=[], metavar="TARGET",
                        help="stage explicit regional opt-in for the next normal protected deployment")
    build_fields = {"cpu-request-millis": "cpu_request_millis", "cpu-limit-millis": "cpu_millis",
                    "memory-mib": "memory_mib", "storage-mib": "ephemeral_storage_mib"}
    for name in build_fields:
        parser.add_argument("--build-" + name, action="append", default=[], metavar="TARGET=VALUE")
    for name in ("cpu-request-millis", "cpu-limit-millis", "memory-request-mib", "memory-limit-mib", "shared-buffers-mib"):
        parser.add_argument("--postgres-" + name, type=int)
    args = parser.parse_args()
    try:
        kube = Kubectl(args.kubeconfig)
        database_settings = {name: getattr(args, "postgres_" + name) for name in (
            "cpu_request_millis", "cpu_limit_millis", "memory_request_mib", "memory_limit_mib", "shared_buffers_mib",
        ) if getattr(args, "postgres_" + name) is not None}
        build_resources: dict[str, dict[str, int]] = {}
        for argument, field in build_fields.items():
            for value in getattr(args, "build_" + argument.replace("-", "_")):
                target, amount = value.split("=", 1)
                selected = build_resources.setdefault(target, {})
                if field in selected:
                    raise ValueError
                selected[field] = int(amount)
        if args.operation in {"stage-builders", "apply-builders"}:
            if database_settings or args.placement_policy is not None or args.command_concurrency is not None or args.create_rate:
                raise DeploymentError("stage build settings separately from database or live execution tuning")
            if args.operation == "apply-builders":
                if args.enable_regional_builder:
                    raise DeploymentError("new regional builders require staged protected deployment")
                result = apply_builders(kube, args.namespace, expected_candidate=args.expected_candidate or "", resources=build_resources)
            else:
                result = stage_builders(kube, args.namespace, expected_candidate=args.expected_candidate or "",
                                        resources=build_resources, enable_regional=tuple(args.enable_regional_builder))
        elif build_resources or args.enable_regional_builder:
            raise DeploymentError("build settings require stage-builders and a normal protected deployment")
        elif args.operation == "stage-postgres":
            if args.placement_policy is not None or args.command_concurrency is not None or args.create_rate:
                raise DeploymentError("stage database settings separately from ordinary live tuning")
            result = stage_postgres(kube, args.namespace, expected_candidate=args.expected_candidate or "",
                                    settings=database_settings)
        elif database_settings:
            raise DeploymentError("Postgres settings require stage-postgres and a normal maintenance deployment")
        elif args.operation == "inspect":
            result = inspect(kube, args.namespace)
        else:
            rates = {}
            for value in args.create_rate:
                target, rate = value.split("=", 1)
                if target in rates:
                    raise ValueError
                rates[target] = int(rate)
            result = apply(kube, args.namespace, expected_candidate=args.expected_candidate or "",
                           placement_policy=args.placement_policy, command_concurrency=args.command_concurrency,
                           create_rates=rates)
        print(json.dumps(result, sort_keys=True))
    except DeploymentError as error:
        print(f"Execution tuning did not converge: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    except (KeyError, TypeError, ValueError):
        print("Invalid execution tuning input or readback; no successful convergence was recorded.", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
