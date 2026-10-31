#!/usr/bin/env python3
"""Persist and apply standalone native execution tuning without replacing task Jobs."""
from __future__ import annotations

import argparse
import json
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import DeploymentError, Kubectl  # noqa: E402

from loom.nebius_platform_render import postgres_settings, validate_environment  # noqa: E402

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


def _api(kube: Kubectl, namespace: str, method: str, path: str, body: object = None) -> dict:
    result = json.loads(kube.run(
        "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c",
        _API, method, path, json.dumps(body),
    ))
    if "error_status" in result:
        raise DeploymentError(f"control-plane tuning request rejected: HTTP {result['error_status']}")
    return result


def _owners(config: dict) -> dict[str, dict]:
    return {config["target_id"]: config, **{
        row["target_id"]: row for row in config.get("regional_execution_targets", [])
    }}


def _env(deployment: dict) -> dict[str, str]:
    return {row["name"]: row["value"] for row in
            deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
            if "value" in row}


def _deployments(kube: Kubectl, namespace: str) -> list[dict]:
    return json.loads(kube.run("get", "deployments", "-n", namespace, "-o", "json"))["items"]


def _runtime(kube: Kubectl, controller: dict) -> dict:
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
                  "s=ExecutionActuatorSettings(); "
                  "print(json.dumps({'command_concurrency':getattr(s,'command_concurrency',None)}))")
    return json.loads(kube.run("exec", "-n", ns, "deployment/" + name, "--", "python", "-c", script))


def _read(kube: Kubectl, namespace: str) -> tuple[dict, dict, dict, list[dict]]:
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


def stage_postgres(
    kube: Kubectl, namespace: str, *, expected_candidate: str, settings: dict[str, int],
) -> dict[str, Any]:
    """Save restart-requiring settings for the next normal maintenance rollout."""
    cm, config, profile, _ = _read(kube, namespace)
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("database settings candidate differs from installed platform")
    if not settings:
        raise DeploymentError("at least one Postgres setting is required")
    held = kube.run(
        "exec", "-n", namespace, "statefulset/loom-postgres", "--", "psql", "-X", "-qAt",
        "-U", "postgres", "-d", "loom", "-v", "ON_ERROR_STOP=1", "-c",
        "BEGIN READ ONLY; SELECT count(*) FROM nebius_rollout_guard; ROLLBACK;",
    )
    if held.strip() != "0":
        raise DeploymentError("a version rollout owns the platform; wait for its normal completion")
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


def apply(
    kube: Kubectl, namespace: str, *, expected_candidate: str,
    placement_policy: str | None = None, command_concurrency: int | None = None,
    create_rates: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Save desired values first, then converge controllers/API; retry is safe.

    Uncertain or partial application remains a failed operation. Re-run the same
    values to converge and read back; never rewrite batches or execution leases.
    """
    cm, config, profile, controllers = _read(kube, namespace)
    held = kube.run(
        "exec", "-n", namespace, "statefulset/loom-postgres", "--", "psql", "-X", "-qAt",
        "-U", "postgres", "-d", "loom", "-v", "ON_ERROR_STOP=1", "-c",
        "BEGIN READ ONLY; SELECT count(*) FROM nebius_rollout_guard; ROLLBACK;",
    )
    if held.strip() != "0":
        raise DeploymentError("a version rollout owns the platform; wait for its normal completion")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate) or profile["candidate_sha"] != expected_candidate:
        raise DeploymentError("tuning candidate differs from the installed platform")
    if any(_runtime(kube, row).get("command_concurrency") is None
           for row in controllers if row["metadata"]["name"] != "loom-control-plane"
           and row["spec"].get("replicas", 1) > 0):
        raise DeploymentError("installed actuator lacks tuning support; deploy the combined candidate first")
    if placement_policy is command_concurrency is None and not create_rates:
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("inspect", "apply", "stage-postgres"))
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--expected-candidate")
    parser.add_argument("--placement-policy", choices=("environment-local-health-first", "environment-local-capacity-balanced"))
    parser.add_argument("--command-concurrency", type=int, choices=range(1, 9))
    parser.add_argument("--create-rate", action="append", default=[], metavar="TARGET=PER_MINUTE")
    for name in ("cpu-request-millis", "cpu-limit-millis", "memory-request-mib", "memory-limit-mib", "shared-buffers-mib"):
        parser.add_argument("--postgres-" + name, type=int)
    args = parser.parse_args()
    try:
        kube = Kubectl(args.kubeconfig)
        database_settings = {name: getattr(args, "postgres_" + name) for name in (
            "cpu_request_millis", "cpu_limit_millis", "memory_request_mib", "memory_limit_mib", "shared_buffers_mib",
        ) if getattr(args, "postgres_" + name) is not None}
        if args.operation == "stage-postgres":
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
