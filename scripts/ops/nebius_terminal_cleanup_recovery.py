#!/usr/bin/env python3
"""Inspect or submit one bounded ordinary cleanup, without deploying the platform."""

from __future__ import annotations

import argparse
import ast
import copy
import json
import sys
from pathlib import Path
from typing import Any, cast
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops import nebius_archive_recovery as transport  # noqa: E402
from scripts.ops import nebius_certificates as private_state  # noqa: E402
from scripts.ops.deploy_nebius_platform import Kubectl  # noqa: E402
from scripts.ops.nebius_candidate import read_json  # noqa: E402
from scripts.ops.nebius_terminal_archive_recovery import qualify_publication  # noqa: E402

from loom_control_plane.pending_archive_recovery import ArchiveRecoveryRequest  # noqa: E402
from loom_control_plane.terminal_archive_recovery import (  # noqa: E402
    TerminalArchiveRecoveryRequest,
)
from loom_control_plane.terminal_cleanup_recovery import (  # noqa: E402
    TerminalCleanupRecoveryRequest,
)


def cleanup_job(api: Kubectl, request: TerminalCleanupRecoveryRequest) -> dict[str, Any]:
    # Reuse the current platform qualification and the native actuator's own
    # namespace, Secret references, TLS mount and service account. No RBAC or
    # cross-namespace credential copy is created by this operation.
    transport.qualify_platform(api, cast(ArchiveRecoveryRequest, request))
    source = api.get("deployment", "loom-execution-actuator", request.execution_namespace)["spec"][
        "template"
    ]["spec"]
    if (
        len(source["containers"]) != 1
        or not source.get("serviceAccountName")
        or source.get("automountServiceAccountToken", True) is False
    ):
        raise ValueError("installed_actuator_ambiguous")
    installed = source["containers"][0]
    env: dict[str, dict[str, Any]] = {}
    values: dict[str, Any] = {}
    for item in installed.get("envFrom", []):
        ref = item.get("configMapRef", {})
        if ref.get("name"):
            data = api.get("configmap", ref["name"], request.execution_namespace).get("data", {})
            for key, value in data.items():
                name = item.get("prefix", "") + key
                env[name] = {
                    "name": name,
                    "valueFrom": {"configMapKeyRef": {"name": ref["name"], "key": key}},
                }
                values[name] = value
    direct = installed.get("env", [])
    for row in direct:
        name = row["name"]
        env[name] = row
        values[name] = row.get("value")
        ref = row.get("valueFrom", {}).get("configMapKeyRef", {})
        if ref.get("name"):
            values[name] = (
                api.get("configmap", ref["name"], request.execution_namespace)
                .get("data", {})
                .get(ref["key"])
            )
    prefix = "LOOM_EXECUTION_ACTUATOR_"
    required = {prefix + name for name in ("CONTROLLER_ID", "TARGET_ID", "NAMESPACE", "DB_URL")}
    if (
        not required <= env.keys()
        or prefix + "GLOBAL_POOL" in env
        or prefix + "KUBERNETES_ENDPOINT" in env
        or len({row["name"] for row in direct}) != len(direct)
        or values[prefix + "TARGET_ID"] != request.target_id
        or values[prefix + "NAMESPACE"] != request.execution_namespace
    ):
        raise ValueError("installed_actuator_scope_changed")
    database_ref = env[prefix + "DB_URL"].get("valueFrom", {}).get("secretKeyRef", {})
    if not database_ref.get("name") or not database_ref.get("key"):
        raise ValueError("installed_database_secret_required")
    ca_mounts = [
        row
        for row in installed.get("volumeMounts", [])
        if row["mountPath"] == "/var/run/loom-db" and row.get("readOnly")
    ]
    volumes = {row["name"]: row for row in source.get("volumes", [])}
    if len(ca_mounts) != 1 or "secret" not in volumes.get(ca_mounts[0]["name"], {}):
        raise ValueError("installed_database_ca_unqualified")
    pods = json.loads(
        api.run(
            "get", "pods", "-n", request.namespace, "-l", "app=loom-control-plane", "-o", "json"
        )
    )["items"]
    nodes = {row["spec"].get("nodeName") for row in pods}
    if len(nodes) != 1 or not all(nodes):
        raise ValueError("existing_control_plane_node_required")
    name = "loom-cleanup-" + str(request.lease_id)[:8] + "-" + request.digest[7:19]
    labels = {"app": "loom-terminal-cleanup-recovery"}
    container = {
        "name": "cleanup",
        "image": request.image_ref,
        "imagePullPolicy": "IfNotPresent",
        "command": [
            "python",
            "-m",
            "loom_control_plane.terminal_cleanup_recovery",
            "--request-json",
            request.model_dump_json(),
        ],
        "env": [copy.deepcopy(env[key]) for key in sorted(required)],
        "resources": {
            "requests": {"cpu": "100m", "memory": "256Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"},
        },
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [copy.deepcopy(ca_mounts[0]), {"name": "tmp", "mountPath": "/tmp"}],
    }
    pod = {
        "serviceAccountName": source["serviceAccountName"],
        "automountServiceAccountToken": True,
        "restartPolicy": "Never",
        "terminationGracePeriodSeconds": 10,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 10001,
            "runAsGroup": 10001,
            "fsGroup": 10001,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "nodeSelector": {
            **source.get("nodeSelector", {}),
            "kubernetes.io/hostname": next(iter(nodes)),
        },
        "containers": [container],
        "volumes": [
            copy.deepcopy(volumes[ca_mounts[0]["name"]]),
            {"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}},
        ],
    }
    for key in ("tolerations", "imagePullSecrets"):
        if key in source:
            pod[key] = copy.deepcopy(source[key])
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": request.execution_namespace,
            "labels": labels,
            "annotations": {"loom.nebius/archive-request": request.digest},
        },
        "spec": {
            "backoffLimit": 0,
            "parallelism": 1,
            "completions": 1,
            "activeDeadlineSeconds": 300,
            "ttlSecondsAfterFinished": 3600,
            "template": {"metadata": {"labels": labels}, "spec": pod},
        },
    }


def inspect_cleanup(
    api: Kubectl, *, namespace: str, team_id: UUID, lease_id: UUID
) -> dict[str, Any]:
    tree = ast.parse((ROOT / "src/loom_control_plane/terminal_cleanup_recovery.py").read_text())
    nodes: list[ast.stmt] = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef | ast.AsyncFunctionDef)
        and node.name in {"TerminalCleanupBinding", "capture_cleanup_binding"}
    ]
    program = """import asyncio,json
from datetime import UTC,datetime
from typing import Annotated
from uuid import UUID
from pydantic import BaseModel,ConfigDict,Field
from sqlalchemy import select,text
from sqlalchemy.ext.asyncio import AsyncSession,async_sessionmaker,create_async_engine
from loom.db.schema import ServiceExecutionCommand,ServiceExecutionLease,Trial
from loom.pipeline.keys import canonical_document,digest_bytes
from loom_control_plane.config import ControlPlaneSettings
class RecoveryRefusedError(RuntimeError): pass
Sha256=Annotated[str,Field(pattern=r"^sha256:[0-9a-f]{64}$")]
"""
    program += ast.unparse(ast.Module(body=nodes, type_ignores=[]))
    program += "\n_INSPECT=" + repr({"team_id": str(team_id), "lease_id": str(lease_id)}) + "\n"
    program += """async def inspect():
 settings=ControlPlaneSettings()
 engine=create_async_engine(settings.db_engine_url,connect_args=settings.db_engine_connect_args)
 try:
  sessions=async_sessionmaker(engine,expire_on_commit=False)
  async with sessions.begin() as session:
   await session.execute(text("SET TRANSACTION READ ONLY"))
   await session.execute(text("SET LOCAL statement_timeout='15s'"))
   binding=await capture_cleanup_binding(session,team_id=UUID(_INSPECT['team_id']),lease_id=UUID(_INSPECT['lease_id']))
   head=(await session.scalars(text("SELECT version_num FROM alembic_version"))).one()
   print(json.dumps({'read_only':True,'schema_head':head,'binding':binding.model_dump(mode='json')}))
 finally: await engine.dispose()
try: asyncio.run(inspect())
except Exception as error:
 print(json.dumps({'status':'blocked','reason':str(error) if isinstance(error,RecoveryRefusedError) else 'cleanup_inspection_incomplete','error_class':type(error).__name__}))
"""
    result = json.loads(
        api.run(
            "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c", program
        )
    )
    if result.get("status") == "blocked" or result.get("binding", {}).get("team_id") != str(
        team_id
    ):
        raise ValueError("cleanup_inspection_incomplete")
    return cast(dict[str, Any], result)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--publication-run-id")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--inspect-team-id", type=UUID)
    parser.add_argument("--inspect-lease-id", type=UUID)
    parser.add_argument("--namespace")
    args = parser.parse_args()
    try:
        api = Kubectl(args.kubeconfig)
        if args.inspect_team_id or args.inspect_lease_id:
            if (
                not (args.inspect_team_id and args.inspect_lease_id and args.namespace)
                or args.apply
                or args.request
            ):
                raise ValueError("inspection_arguments_invalid")
            result = inspect_cleanup(
                api,
                namespace=args.namespace,
                team_id=args.inspect_team_id,
                lease_id=args.inspect_lease_id,
            )
            with private_state._locked_state(args.evidence_dir):
                private_state._atomic_json(args.evidence_dir / "installed-projection.json", result)
            print(json.dumps({"status": "inspected", "read_only": True}))
            return 0
        if not args.request or not args.publication_run_id:
            raise ValueError("request_and_publication_required")
        request = TerminalCleanupRecoveryRequest.model_validate(read_json(args.request))
        publication = qualify_publication(
            cast(TerminalArchiveRecoveryRequest, request), args.publication_run_id
        )
        job = cleanup_job(api, request)
        with private_state._locked_state(args.evidence_dir):
            private_state._atomic_json(args.evidence_dir / "publication.json", publication)
            private_state._atomic_json(args.evidence_dir / "reviewed-job.json", job)
        receipt = (
            transport.submit_once(api, job, args.evidence_dir / "submission")
            if args.apply
            else {"status": "prepared"}
        )
        print(json.dumps(receipt, sort_keys=True))
        return 0
    except Exception as error:
        reason = (
            str(error)
            if isinstance(error, ValueError) and str(error).replace("_", "").isalnum()
            else "terminal_cleanup_operator_incomplete"
        )
        print(
            json.dumps(
                {"status": "blocked", "reason": reason[:120], "error_class": type(error).__name__}
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
