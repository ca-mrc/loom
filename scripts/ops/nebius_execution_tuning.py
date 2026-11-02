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
    EXECUTION_DISK_STORAGE_QUOTAS,
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

# Only counts leave the installed CP. The tuning guard pauses new admission;
# incompatible frozen builds finish naturally before pressure GC is enabled.
_CACHE_READER_STATE = '''import asyncio,json,sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from loom_control_plane.config import ControlPlaneSettings
async def main():
 s=ControlPlaneSettings();e=create_async_engine(s.db_engine_url,connect_args=s.db_engine_connect_args)
 totals={'native_incompatible':0,'application_incompatible':0,'management_owned':0}
 try:
  async with e.connect() as c, c.begin():
   await c.execute(text('SET TRANSACTION READ ONLY'))
   await c.execute(text("SET LOCAL statement_timeout='10s'"))
   for scope in json.loads(sys.argv[1]):
    params={k:scope[k] for k in ('endpoint','region','bucket','image','budget')}
    native="""WITH live AS (
     SELECT a.native_build,nullif(a.native_build#>>'{configmap,data,claim.json}','')::jsonb claim
     FROM task_image_materialization_attempts a
     WHERE a.native_build IS NOT NULL AND a.native_capacity_released_at IS NULL
     AND (a.native_capacity_reserved_at IS NOT NULL OR EXISTS (
      SELECT 1 FROM task_image_materializations m WHERE m.id=a.materialization_id
      AND m.lease_epoch=a.lease_epoch AND m.state IN('claimed','running'))))
     SELECT count(*) FROM live WHERE claim->>'storage_endpoint'=:endpoint
     AND claim->>'storage_region'=:region AND claim->>'cache_bucket'=:bucket
     AND (native_build#>>'{job,spec,template,spec,initContainers,0,image}' IS DISTINCT FROM :image
      OR coalesce(claim->>'cache_total_mib','4096')::int <> :budget)"""
    app="""SELECT count(*) FROM nebius_application_build_attempts a
     WHERE a.phase IN('queued','running','settling')
     AND a.claim_json->>'storage_endpoint'=:endpoint AND a.claim_json->>'storage_region'=:region
     AND a.claim_json->>'cache_bucket'=:bucket
     AND (a.claim_json#>>'{recipe,trusted_image_ref}' IS DISTINCT FROM :image
      OR coalesce(a.claim_json->>'cache_total_mib','4096')::int <> :budget)"""
    totals['native_incompatible']+=int(await c.scalar(text(native),params) or 0)
    totals['application_incompatible']+=int(await c.scalar(text(app),params) or 0)
    managed="""SELECT EXISTS (
     SELECT 1 FROM nebius_cache_runtime_policies WHERE storage_endpoint=:endpoint
      AND storage_region=:region AND cache_bucket=:bucket
     UNION ALL
     SELECT 1 FROM nebius_pool_profile_documents d JOIN nebius_pool_participants p
      ON p.pool_id=d.pool_id WHERE d.family IN('task_images','application_images')
      AND d.document_json#>>'{profile,settings,storage_endpoint}'=:endpoint
      AND d.document_json#>>'{profile,settings,storage_region}'=:region
      AND d.document_json#>>'{profile,settings,cache_bucket}'=:bucket
      AND jsonb_path_exists(p.binding_json,'$.targets[*] ? (@.profile_id == $selected)'::jsonpath,
       jsonb_build_object('selected',d.profile_id::text)))"""
    totals['management_owned']+=int(bool(await c.scalar(text(managed),params)))
  print(json.dumps(totals))
 finally:await e.dispose()
try:asyncio.run(main())
except Exception:
 print('Cache reader compatibility unavailable; no successful policy convergence recorded',file=sys.stderr)
 raise SystemExit(1)
'''

_IDLE_STATE = '''import asyncio,json,sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from loom.nebius_rollout_guard import ACTIVITY_SQL
from loom_control_plane.config import ControlPlaneSettings
async def main():
 s=ControlPlaneSettings();e=create_async_engine(s.db_engine_url,connect_args=s.db_engine_connect_args)
 try:
  async with e.connect() as c, c.begin():
   await c.execute(text('SET TRANSACTION READ ONLY'))
   await c.execute(text("SET LOCAL statement_timeout='10s'"))
   counts=dict((await c.execute(text(ACTIVITY_SQL))).mappings().one())
  print(json.dumps({'idle':not any(counts.values())}))
 finally:await e.dispose()
try:asyncio.run(main())
except Exception:
 print('Disk maintenance idle readback unavailable',file=sys.stderr)
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
            result[target]["cache_total_mib"] = builder.get("cache_total_mib", 4096)
            result[target]["cache_gc_policy"] = builder.get("cache_gc_policy", "legacy-seven-day")
            result[target]["reuse_task_cache_for_verifier"] = builder.get("reuse_task_cache_for_verifier", False)
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
        ), "builder_configuration_source": "unknown"}
    if name == "loom-control-plane":
        script = ("import json; import loom.nebius_platform_render as renderer; from loom_control_plane.config import ControlPlaneSettings; "
                  "s=ControlPlaneSettings(); "
                  "print(json.dumps({'placement_policy':s.service_execution_placement_policy, "
                  "'warm_fit_support':hasattr(s,'prefer_incoming_warm_fit'), "
                  "'regional_disk_configuration_support':getattr(renderer,'REGIONAL_DISK_CONFIGURATION_SUPPORTED',False), "
                  "'prefer_incoming_warm_fit':getattr(s,'prefer_incoming_warm_fit',None), "
                  "'warm_fit_configuration_source':'control-plane', "
                  "'warm_fit_requires_manager_policy':getattr(s,'global_pool',None) is not None}))")
    else:
        script = ("import json; from loom_execution_actuator.config import ExecutionActuatorSettings; "
                  "s=ExecutionActuatorSettings(); b=s.task_image_builder; "
                  "print(json.dumps({'command_concurrency':getattr(s,'command_concurrency',None), "
                  "'builder_enabled':b is not None, "
                  "'builder_request_support':b is not None and hasattr(b,'cpu_request_millis'), "
                  "'builder_cache_budget_support':b is not None and hasattr(b,'cache_total_mib'), "
                  "'builder_cache_policy_support':b is not None and hasattr(b,'cache_gc_policy') and hasattr(b,'reuse_task_cache_for_verifier'), "
                  "'builder_configuration_source':'management-profile' if getattr(s,'global_pool',None) is not None else 'actuator', "
                  "'builder_resources':None if b is None else {"
                  "'effective_cpu_request_millis':getattr(b,'cpu_request_millis',None) or b.cpu_millis, "
                  "'cpu_millis':b.cpu_millis,'memory_mib':b.memory_mib,"
                  "'ephemeral_storage_mib':b.ephemeral_storage_mib,'namespace':b.namespace,"
                  "'cache_total_mib':getattr(b,'cache_total_mib',None), "
                  "'cache_gc_policy':getattr(b,'cache_gc_policy',None), "
                  "'reuse_task_cache_for_verifier':getattr(b,'reuse_task_cache_for_verifier',None)}}))")
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
            "prefer_incoming_warm_fit": config.get("prefer_incoming_warm_fit", False),
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
    config: dict[str, Any], resources: dict[str, dict[str, Any]], enable_regional: tuple[str, ...] = (),
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
    numeric = limits | {"cpu_request_millis", "cache_total_mib"}
    policies = {"cache_gc_policy", "reuse_task_cache_for_verifier"}
    for target, values in resources.items():
        if (target not in owners or owners[target].get("task_image_builder") is None
                or not values or set(values) - (numeric | policies)
                or any(type(value) is not int for key, value in values.items() if key in numeric)
                or ("cache_gc_policy" in values and (not isinstance(values["cache_gc_policy"], str)
                    or values["cache_gc_policy"] not in {"legacy-seven-day", "budget-pressure"}))
                or ("reuse_task_cache_for_verifier" in values and type(values["reuse_task_cache_for_verifier"]) is not bool)):
            raise DeploymentError("build settings require a configured builder and strictly typed resource/cache values")
        owner = owners[target]
        effective_config = updated if target == updated["target_id"] else _regional_config(updated, owner)
        settings = _task_image_builder_settings(effective_config, service_image="test.invalid/service@sha256:" + "a" * 64)
        ceiling = task_image_builder_resource_ceiling(owner["task_image_builder"], settings)
        if any(key in values and values[key] > ceiling[key] for key in limits):
            raise DeploymentError("build tuning must preserve existing hard resource ceilings")
        if set(values) & (limits | {"cpu_request_millis"}):
            owner["task_image_builder"]["resource_ceiling"] = ceiling
        owner["task_image_builder"].update(values)
    # This is a bucket-wide limit. Controllers sharing it must use one saved
    # budget, including during independent regional tuning operations.
    budgets: dict[str, tuple[int, str]] = {}
    for owner in owners.values():
        builder = owner.get("task_image_builder")
        if builder is None or builder.get("cache_bucket") is None:
            continue
        budget = (builder.get("cache_total_mib", 4096), builder.get("cache_gc_policy", "legacy-seven-day"))
        bucket = builder["cache_bucket"]
        if bucket in budgets and budgets[bucket] != budget:
            raise DeploymentError("builders sharing a cache bucket require the same total budget and GC policy; tune them together")
        budgets[bucket] = budget
    validate_environment(updated)
    return updated


def _stage_builders_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    resources: dict[str, dict[str, Any]], enable_regional: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Persist future build intent; installation and permission checks own activation.

    Existing Jobs retain their frozen settings. Enabling a regional builder must
    go through the normal reviewed regional namespace/Secret/RBAC rollout.
    """
    cm, config, profile, controllers = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("build settings candidate differs from installed platform")
    updated = _updated_builders(config, resources, enable_regional)
    for target in set(resources) | set(enable_regional):
        matches = [row for row in controllers if _env(row).get("LOOM_EXECUTION_ACTUATOR_TARGET_ID") == target]
        sources = [_runtime(kube, row).get("builder_configuration_source", "unknown") for row in matches]
        if "management-profile" in sources:
            raise DeploymentError("global native build settings are owned by management profiles; use normal management configuration")
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
    prefer_incoming_warm_fit: bool | None = None,
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
    if prefer_incoming_warm_fit is not None:
        if type(prefer_incoming_warm_fit) is not bool:
            raise DeploymentError("warm-fit preference requires a boolean")
        cp = next(row for row in controllers if row["metadata"]["name"] == "loom-control-plane")
        if not _runtime(kube, cp).get("warm_fit_support"):
            raise DeploymentError("installed control-plane lacks warm-fit support; deploy the combined candidate first")
    if placement_policy is None and command_concurrency is None and not create_rates and prefer_incoming_warm_fit is None:
        raise DeploymentError("at least one tuning value is required")
    updated = deepcopy(config)
    if placement_policy is not None:
        updated["execution_placement_policy"] = placement_policy
    if prefer_incoming_warm_fit is not None:
        updated["prefer_incoming_warm_fit"] = prefer_incoming_warm_fit
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
        if prefer_incoming_warm_fit is not None and controller["metadata"]["name"] == "loom-control-plane":
            changes["LOOM_CP_PREFER_INCOMING_WARM_FIT"] = str(prefer_incoming_warm_fit).lower()
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
            if (prefer_incoming_warm_fit is not None or result["persisted"]["prefer_incoming_warm_fit"]) and (
                    not controller.get("warm_fit_support")
                    or controller.get("prefer_incoming_warm_fit") is not result["persisted"]["prefer_incoming_warm_fit"]):
                raise DeploymentError("saved and live warm-fit preference differ")
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
    resources: dict[str, dict[str, Any]], enable_regional: tuple[str, ...] = (),
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
    kube: Kubectl, namespace: str, *, expected_candidate: str, resources: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    cm, config, profile, controllers = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("build settings candidate differs from installed platform")
    updated = _updated_builders(config, resources)
    cache_policies = {"cache_gc_policy", "reuse_task_cache_for_verifier"}
    allowed = {"cpu_request_millis", "cpu_millis", "memory_mib", "ephemeral_storage_mib", "cache_total_mib", *cache_policies}
    prepared = {}
    for target in resources:
        matches = [row for row in controllers if _env(row).get("LOOM_EXECUTION_ACTUATOR_TARGET_ID") == target]
        if len(matches) != 1 or matches[0]["spec"].get("replicas", 1) < 1:
            raise DeploymentError("live build tuning requires one running existing actuator")
        controller = matches[0]
        runtime = _runtime(kube, controller)
        if runtime.get("builder_configuration_source") == "management-profile":
            raise DeploymentError("global native build settings are owned by management profiles; use normal management configuration")
        if "cache_total_mib" in resources[target] and runtime.get("builder_configuration_source") != "actuator":
            raise DeploymentError("cache budget tuning requires a known standalone builder configuration source")
        if not runtime.get("builder_enabled") or not runtime.get("builder_request_support"):
            raise DeploymentError("live build tuning requires an already enabled supporting builder; use staged protected deployment first")
        if "cache_total_mib" in resources[target] and not runtime.get("builder_cache_budget_support"):
            raise DeploymentError("installed builder lacks cache budget support; stage and deploy the supporting candidate first")
        if set(resources[target]) & cache_policies and not runtime.get("builder_cache_policy_support"):
            raise DeploymentError("installed builder lacks cache policy support; stage and deploy the supporting candidate first")
        raw = _env(controller).get("LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER")
        if not raw:
            raise DeploymentError("existing actuator builder configuration unavailable")
        current = NativeTaskImageSettings.model_validate_json(raw)
        owner = _owners(updated)[target]
        regional = updated if target == updated["target_id"] else _regional_config(updated, owner)
        desired = _task_image_builder_settings(regional, service_image=current.service_image, storage_config=updated)
        if (desired is not None and not runtime.get("builder_cache_budget_support")
                and "cache_total_mib" in desired.model_dump()):
            raise DeploymentError("saved cache budget requires a supporting builder candidate before live tuning")
        if desired is not None and not runtime.get("builder_cache_policy_support") and cache_policies & desired.model_fields_set:
            raise DeploymentError("saved cache policy requires a supporting builder candidate before live tuning")
        if desired is None or any(getattr(current, key) != getattr(desired, key)
                                  for key in NativeTaskImageSettings.model_fields if key not in allowed):
            raise DeploymentError("live build tuning cannot change namespace, credential references, runtime or identity settings")
        if any(getattr(current, key) != getattr(desired, key) for key in allowed - resources[target].keys()):
            raise DeploymentError("saved and live unrequested builder settings differ; include the intended changes explicitly or deploy staged settings first")
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
    # The first pressure transition needs compatible readers, including frozen
    # attempts which have not created a Job yet. Existing execution leases are
    # unaffected and no workload is cancelled/re-submitted by this check.
    scopes = {}
    for _, remote, current, desired, _, _, _ in prepared.values():
        if (desired.cache_gc_policy != "budget-pressure" or desired.cache_bucket is None
                or (current.cache_gc_policy == "budget-pressure" and current.cache_total_mib == desired.cache_total_mib)):
            continue
        if desired.service_image != profile.get("task_image_ref"):
            raise DeploymentError("pressure cache policy requires the published candidate service image; finish normal deployment convergence first")
        service = kube.get("deployment", "loom-service", namespace)
        images = [row.get("image") for row in service.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])]
        if images != [desired.service_image]:
            raise DeploymentError("pressure cache policy requires the installed supporting service image for future build readers")
        jobs = json.loads(remote.run("get", "jobs", "-n", desired.namespace, "-o", "json"))["items"]
        for job in jobs:
            if any(row.get("type") in {"Complete", "Failed"} and row.get("status") == "True"
                   for row in job.get("status", {}).get("conditions", [])):
                continue
            pod = job.get("spec", {}).get("template", {}).get("spec", {})
            readers = [row for row in (*pod.get("initContainers", []), *pod.get("containers", []))
                       if row.get("name") in {"prepare", "publish"}]
            if len(readers) != 2 or any(row.get("image") != desired.service_image for row in readers):
                raise DeploymentError("pressure cache policy requires incompatible or unknown build Jobs to finish naturally")
        key = (desired.storage_endpoint, desired.storage_region, desired.cache_bucket)
        scopes[key] = {"endpoint": key[0], "region": key[1], "bucket": key[2],
                       "image": desired.service_image, "budget": desired.cache_total_mib}
    if scopes:
        readers = json.loads(kube.run("exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c",
                                     _CACHE_READER_STATE, json.dumps(list(scopes.values()))))
        if readers.get("management_owned"):
            raise DeploymentError("shared cache scope is management-owned; use the normal management cache-policy owner")
        if set(readers) != {"native_incompatible", "application_incompatible", "management_owned"} or any(type(value) is not int or value != 0 for value in readers.values()):
            raise DeploymentError("pressure cache policy requires incompatible frozen cache readers to finish and clean up naturally")
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
        if observed.get("builder_configuration_source") != "actuator":
            raise DeploymentError("live builder configuration source changed; inspect management ownership before continuing")
        expected = {"effective_cpu_request_millis": desired.cpu_request_millis or desired.cpu_millis,
                    "cpu_millis": desired.cpu_millis, "memory_mib": desired.memory_mib,
                    "ephemeral_storage_mib": desired.ephemeral_storage_mib, "namespace": desired.namespace}
        observed_resources = deepcopy(observed.get("builder_resources"))
        if observed.get("builder_cache_budget_support"):
            expected["cache_total_mib"] = desired.cache_total_mib
        elif "cache_total_mib" in resources[target]:
            raise DeploymentError("live builder lost cache budget support; inspect before continuing")
        elif isinstance(observed_resources, dict):
            if observed_resources.pop("cache_total_mib", None) is not None:
                raise DeploymentError("builder cache budget capability readback differs")
        if observed.get("builder_cache_policy_support"):
            expected.update({key: getattr(desired, key) for key in cache_policies})
        elif set(resources[target]) & cache_policies:
            raise DeploymentError("live builder lost cache policy support; inspect before continuing")
        elif isinstance(observed_resources, dict):
            if any(observed_resources.pop(key, None) is not None for key in cache_policies):
                raise DeploymentError("builder cache policy capability readback differs")
        if observed_resources != expected:
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


def _apply_regional_disks_locked(
    kube: Kubectl, namespace: str, *, expected_candidate: str, disk_types: dict[str, str],
) -> dict[str, Any]:
    """Align saved source and collector accounting under the caller's guard.

    This never mutates cloud disks. An infrastructure maintenance caller retains
    its normal idle rollout guard across Terraform, this step and fresh placement
    qualification; ordinary callers use apply_regional_disks below.
    """
    cm, config, profile, controllers = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("disk configuration candidate differs from installed platform")
    cp = next(row for row in controllers if row["metadata"]["name"] == "loom-control-plane")
    runtime = _runtime(kube, cp)
    if not runtime.get("regional_disk_configuration_support"):
        raise DeploymentError("installed renderer lacks regional disk configuration support")
    if runtime.get("warm_fit_requires_manager_policy"):
        raise DeploymentError("global disk accounting is owned by management profiles; use normal management maintenance")
    idle = json.loads(kube.run(
        "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c", _IDLE_STATE,
    ))
    if idle.get("idle") is not True:
        raise DeploymentError("disk configuration maintenance requires drained execution and build work")
    regional = {row["target_id"]: row for row in config.get("regional_execution_targets", [])}
    if not disk_types or any(target not in regional or kind not in EXECUTION_DISK_STORAGE_QUOTAS
                             for target, kind in disk_types.items()):
        raise DeploymentError("disk configuration requires existing regional targets and supported disk types")
    updated = deepcopy(config)
    for row in updated["regional_execution_targets"]:
        if row["target_id"] in disk_types:
            row["execution_disk_type"] = disk_types[row["target_id"]]
    validate_environment(updated)
    key = "LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_NAME"
    # All regional collectors execute in the primary namespace. Their existing
    # node-group binding must agree before any source or accounting changes.
    collectors = {}
    for target in disk_types:
        obj = kube.get("configmap", target + "-collector", config["execution_namespace"])
        if (not obj or not obj.get("metadata", {}).get("uid")
                or obj.get("data", {}).get("LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID") != regional[target]["execution_node_group_id"]
                or key not in obj.get("data", {})):
            raise DeploymentError("regional collector binding unavailable or differs from saved owner")
        collectors[target] = obj
    if updated != config:
        kube.run("patch", "configmap", "loom-platform-config", "-n", namespace, "--type=json", "-p",
                 json.dumps([
                     {"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]},
                     {"op": "replace", "path": "/data/environment.json", "value": json.dumps(updated)},
                 ]))
    for target, obj in collectors.items():
        desired = EXECUTION_DISK_STORAGE_QUOTAS[disk_types[target]]
        if obj["data"][key] != desired:
            kube.run("patch", "configmap", target + "-collector", "-n", config["execution_namespace"],
                     "--type=json", "-p", json.dumps([
                         {"op": "test", "path": "/metadata/uid", "value": obj["metadata"]["uid"]},
                         {"op": "test", "path": "/metadata/resourceVersion", "value": obj["metadata"]["resourceVersion"]},
                         {"op": "replace", "path": "/data/" + key, "value": desired},
                     ]))
    saved_cm = kube.get("configmap", "loom-platform-config", namespace)
    saved = json.loads(saved_cm["data"]["environment.json"])
    if (saved != updated or json.loads(saved_cm["data"]["profile.json"])["candidate_sha"] != expected_candidate
            or any(kube.get("configmap", target + "-collector", config["execution_namespace"])["data"][key]
                   != EXECUTION_DISK_STORAGE_QUOTAS[kind] for target, kind in disk_types.items())):
        raise DeploymentError("saved disk configuration or collector accounting differs; retry the same intent")
    return {"candidate_sha": expected_candidate, "regional_disk_types": disk_types,
            "cloud_disks_changed": False, "database_restarted": False, "task_jobs_replaced": False,
            "placement_qualification_required": True}


def apply_regional_disks(
    kube: Kubectl, namespace: str, *, expected_candidate: str, disk_types: dict[str, str],
) -> dict[str, Any]:
    with _tuning_guard(kube, namespace, expected_candidate):
        return _apply_regional_disks_locked(kube, namespace, expected_candidate=expected_candidate, disk_types=disk_types)


def apply_builders(
    kube: Kubectl, namespace: str, *, expected_candidate: str, resources: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    with _tuning_guard(kube, namespace, expected_candidate):
        return _apply_builders_locked(kube, namespace, expected_candidate=expected_candidate, resources=resources)


def apply(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    placement_policy: str | None = None, command_concurrency: int | None = None,
    create_rates: dict[str, int] | None = None,
    prefer_incoming_warm_fit: bool | None = None,
) -> dict[str, Any]:
    """Hold the deployment mutex through persistence, convergence and readback."""
    with _tuning_guard(kube, namespace, expected_candidate):
        return _apply_locked(kube, namespace, expected_candidate=expected_candidate,
                             placement_policy=placement_policy, command_concurrency=command_concurrency,
                             create_rates=create_rates, prefer_incoming_warm_fit=prefer_incoming_warm_fit)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("inspect", "apply", "stage-postgres", "stage-builders", "apply-builders", "apply-regional-disks"))
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--expected-candidate")
    parser.add_argument("--placement-policy", choices=("environment-local-health-first", "environment-local-capacity-balanced"))
    parser.add_argument("--command-concurrency", type=int, choices=range(1, 9))
    parser.add_argument("--prefer-incoming-warm-fit", choices=("true", "false"),
                        help="persist the independent control-plane preference; global managers retain their own normal owner policy")
    parser.add_argument("--create-rate", action="append", default=[], metavar="TARGET=PER_MINUTE")
    parser.add_argument("--regional-disk-type", action="append", default=[], metavar="TARGET=TYPE",
                        help="configuration/accounting only; cloud disk maintenance and fresh placement qualification are separate")
    parser.add_argument("--enable-regional-builder", action="append", default=[], metavar="TARGET",
                        help="stage explicit regional opt-in for the next normal protected deployment")
    build_fields = {"cpu-request-millis": "cpu_request_millis", "cpu-limit-millis": "cpu_millis",
                    "memory-mib": "memory_mib", "storage-mib": "ephemeral_storage_mib",
                    "cache-total-mib": "cache_total_mib", "cache-gc-policy": "cache_gc_policy",
                    "reuse-task-cache-for-verifier": "reuse_task_cache_for_verifier"}
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
        build_resources: dict[str, dict[str, Any]] = {}
        for argument, field in build_fields.items():
            for value in getattr(args, "build_" + argument.replace("-", "_")):
                target, amount = value.split("=", 1)
                selected = build_resources.setdefault(target, {})
                if field in selected:
                    raise ValueError
                if field == "cache_gc_policy":
                    if amount not in {"legacy-seven-day", "budget-pressure"}:
                        raise ValueError
                    selected[field] = amount
                elif field == "reuse_task_cache_for_verifier":
                    if amount not in {"true", "false"}:
                        raise ValueError
                    selected[field] = amount == "true"
                else:
                    selected[field] = int(amount)
        if args.operation == "apply-regional-disks":
            if (database_settings or build_resources or args.enable_regional_builder or args.placement_policy is not None
                    or args.command_concurrency is not None or args.create_rate or args.prefer_incoming_warm_fit is not None):
                raise DeploymentError("apply regional disk configuration separately from other settings")
            disk_types = {}
            for value in args.regional_disk_type:
                target, kind = value.split("=", 1)
                if target in disk_types:
                    raise ValueError
                disk_types[target] = kind
            result = apply_regional_disks(kube, args.namespace, expected_candidate=args.expected_candidate or "", disk_types=disk_types)
        elif args.regional_disk_type:
            raise DeploymentError("regional disk configuration requires apply-regional-disks")
        elif args.operation in {"stage-builders", "apply-builders"}:
            if database_settings or args.placement_policy is not None or args.command_concurrency is not None or args.create_rate or args.prefer_incoming_warm_fit is not None:
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
            if args.placement_policy is not None or args.command_concurrency is not None or args.create_rate or args.prefer_incoming_warm_fit is not None:
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
                           create_rates=rates, prefer_incoming_warm_fit=None if args.prefer_incoming_warm_fit is None else args.prefer_incoming_warm_fit == "true")
        print(json.dumps(result, sort_keys=True))
    except DeploymentError as error:
        print(f"Execution tuning did not converge: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    except (KeyError, TypeError, ValueError):
        print("Invalid execution tuning input or readback; no successful convergence was recorded.", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
