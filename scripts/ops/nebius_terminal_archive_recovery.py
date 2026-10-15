#!/usr/bin/env python3
"""Prepare or launch one exact terminal archive Job; never deploy the platform."""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops import nebius_archive_recovery as transport  # noqa: E402
from scripts.ops import nebius_certificates as private_state  # noqa: E402
from scripts.ops.deploy_nebius_platform import Kubectl  # noqa: E402
from scripts.ops.nebius_candidate import read_json, validate_identity  # noqa: E402
from scripts.ops.nebius_idle_rollout import (  # noqa: E402
    candidate_follows,
    candidate_schema_head,
    canonical_repository_name,
    select_publication,
)

from loom_control_plane.terminal_archive_recovery import (  # noqa: E402
    TerminalArchiveRecoveryRequest,
)


def qualify_publication(request: TerminalArchiveRecoveryRequest, run_id: str) -> dict:
    selected = select_publication(run_id)
    if selected.get("status") != "ready" or selected.get("sha") != request.candidate_sha:
        raise ValueError("publication_binding_changed")
    # Storage-only compatibility is explicit. No migration or new 0176 baseline
    # admission is performed against the installed 0175 database.
    if (not candidate_follows(request.installed_candidate, request.candidate_sha)
            or request.schema_head not in {"0175", "0176"}
            or candidate_schema_head(request.candidate_sha) not in {request.schema_head, "0176"}):
        raise ValueError("archive_candidate_schema_unsupported")
    with tempfile.TemporaryDirectory(prefix="loom-terminal-archive-") as directory:
        result = subprocess.run(["gh", "run", "download", run_id, "--repo", canonical_repository_name(),
            "--name", selected["artifact"], "--dir", directory], capture_output=True, check=False, timeout=120)
        if result.returncode:
            raise ValueError("publication_download_failed")
        candidate = read_json(Path(directory) / "candidate.json")
    validate_identity(candidate, require_current_images=True)
    if (candidate["candidate_sha"] != request.candidate_sha or str(candidate["run_id"]) != run_id
            or candidate["images"]["control_plane"]["image_ref"] != request.image_ref):
        raise ValueError("published_image_changed")
    return {**selected, "image_ref": request.image_ref}


def recovery_job(request: TerminalArchiveRecoveryRequest, control_plane: dict, *, node_name: str) -> dict:
    # Retain the qualified environment, Secret references, CA, mounted immutable
    # platform binding, resource limits and no-retry behavior of the existing lane.
    job = transport.recovery_job(request, control_plane)
    if not node_name:
        raise ValueError("existing_control_plane_node_required")
    pod = job["spec"]["template"]["spec"]
    pod["nodeSelector"] = {**pod.get("nodeSelector", {}), "kubernetes.io/hostname": node_name}
    container = pod["containers"][0]
    container["command"][2] = "loom_control_plane.terminal_archive_recovery"
    job["metadata"]["labels"]["app"] = "loom-terminal-archive-recovery"
    job["spec"]["template"]["metadata"]["labels"]["app"] = "loom-terminal-archive-recovery"
    return job


def inspect_archive(api: Kubectl, *, namespace: str, team_id: UUID, lease_id: UUID) -> dict:
    """Fresh read-only binding using installed models, with no source-data export."""
    tree = ast.parse((ROOT / "src/loom_control_plane/terminal_archive_recovery.py").read_text())
    names = {"TerminalArchiveBinding", "qualify_deleted", "capture_binding"}
    nodes = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)
             or (isinstance(node, ast.ClassDef | ast.AsyncFunctionDef | ast.FunctionDef) and node.name in names)]
    program = ast.unparse(ast.Module(body=nodes, type_ignores=[]))
    program += "\n_INSPECT = " + repr({"team_id": str(team_id), "lease_id": str(lease_id)}) + "\n"
    program += '''async def inspect():
 settings=ControlPlaneSettings()
 engine=create_async_engine(settings.db_engine_url,connect_args=settings.db_engine_connect_args)
 try:
  sessions=async_sessionmaker(engine,expire_on_commit=False)
  async with sessions.begin() as session:
   await session.execute(text("SET TRANSACTION READ ONLY"))
   await session.execute(text("SET LOCAL statement_timeout='15s'"))
   binding=await capture_binding(session,team_id=UUID(_INSPECT["team_id"]),lease_id=UUID(_INSPECT["lease_id"]))
   head=(await session.scalars(text("SELECT version_num FROM alembic_version"))).one()
   print(json.dumps({"read_only":True,"schema_head":head,"binding":binding.model_dump(mode="json")}))
 finally:await engine.dispose()
try:
 asyncio.run(inspect())
except Exception as error:
 reason=str(error) if isinstance(error,RecoveryRefusedError) and re.fullmatch(r"[a-z][a-z0-9_]{0,119}",str(error)) else "installed_archive_inspection_incomplete"
 print(json.dumps({"read_only":True,"status":"blocked","reason":reason,"error_class":type(error).__name__}))
'''
    result = json.loads(api.run("exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c", program))
    if result.get("status") == "blocked":
        raise ValueError(result["reason"])
    if not result.get("read_only") or result.get("binding", {}).get("team_id") != str(team_id):
        raise ValueError("installed_archive_binding_unqualified")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--publication-run-id")
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--inspect-team-id", type=UUID)
    parser.add_argument("--inspect-lease-id", type=UUID)
    parser.add_argument("--namespace")
    args = parser.parse_args()
    try:
        api = Kubectl(args.kubeconfig)
        if args.inspect_team_id or args.inspect_lease_id:
            if not (args.inspect_team_id and args.inspect_lease_id and args.namespace) or args.apply or args.request:
                raise ValueError("inspection_arguments_invalid")
            result = inspect_archive(api, namespace=args.namespace, team_id=args.inspect_team_id, lease_id=args.inspect_lease_id)
            with private_state._locked_state(args.evidence_dir):
                private_state._atomic_json(args.evidence_dir / "installed-projection.json", result)
            print(json.dumps({"status": "inspected", "read_only": True}))
            return 0
        if not args.request or not args.publication_run_id:
            raise ValueError("request_and_publication_required")
        request = TerminalArchiveRecoveryRequest.model_validate(read_json(args.request))
        publication = qualify_publication(request, args.publication_run_id)
        control_plane = transport.qualify_platform(api, request)
        pods = json.loads(api.run("get", "pods", "-n", request.namespace, "-l", "app=loom-control-plane", "-o", "json"))["items"]
        nodes = sorted({pod["spec"].get("nodeName", "") for pod in pods})
        if not nodes or not nodes[0]:
            raise ValueError("existing_control_plane_node_required")
        job = recovery_job(request, control_plane, node_name=nodes[0])
        with private_state._locked_state(args.evidence_dir):
            private_state._atomic_json(args.evidence_dir / "publication.json", publication)
            private_state._atomic_json(args.evidence_dir / "reviewed-job.json", job)
        receipt = transport.submit_once(api, job, args.evidence_dir / "submission") if args.apply else {"status": "prepared"}
        print(json.dumps(receipt, sort_keys=True))
        return 0
    except Exception as error:
        # Never print credentials, kubectl argv, object contents or SQL errors.
        safe = str(error) if isinstance(error, ValueError) and str(error).replace("_", "").isalnum() else "terminal_archive_operator_incomplete"
        print(json.dumps({"status": "blocked", "reason": safe[:120],
                          "error_class": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
