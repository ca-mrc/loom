#!/usr/bin/env python3
"""Launch one qualified archival worker without changing platform deployments."""
from __future__ import annotations

import argparse
import ast
import copy
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops import nebius_certificates as private_state  # noqa: E402
from scripts.ops.deploy_nebius_platform import Kubectl, verify_cluster_identity  # noqa: E402
from scripts.ops.nebius_candidate import (  # noqa: E402
    read_json,
    source_archive_digest,
    validate_identity,
)
from scripts.ops.nebius_idle_rollout import (  # noqa: E402
    candidate_follows,
    candidate_schema_head,
    select_publication,
)

from loom.nebius_platform_render import digest  # noqa: E402
from loom_control_plane.pending_archive_recovery import (  # noqa: E402
    JOB_TIMEOUT,
    ArchiveRecoveryRequest,
)

REPOSITORY = "qianyi-sun/loom"
_ENV_REQUIRED = {
    "LOOM_CP_DB_URL", "LOOM_CP_MINIO_ENDPOINT", "LOOM_CP_MINIO_REGION",
    "LOOM_CP_MINIO_ACCESS_KEY", "LOOM_CP_MINIO_SECRET_KEY",
    "LOOM_CP_ARTIFACTS_BUCKET", "LOOM_CP_TRAJECTORIES_BUCKET",
    "LOOM_CP_SERVICE_EXECUTION_SOURCE_ENDPOINT", "LOOM_CP_SERVICE_EXECUTION_SOURCE_REGION",
    "LOOM_CP_SERVICE_EXECUTION_SOURCE_BUCKET", "LOOM_CP_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
    "LOOM_CP_SERVICE_EXECUTION_SOURCE_SECRET_KEY", "LOOM_CP_SERVICE_EXECUTION_SOURCE_RETENTION_SEC",
}


def recovery_job(request: ArchiveRecoveryRequest, control_plane: dict[str, Any]) -> dict[str, Any]:
    """Copy only qualified connection references and placement, never selectors."""
    original = control_plane["spec"]["template"]["spec"]
    containers = original["containers"]
    if len(containers) != 1 or containers[0]["image"] != request.installed_image_ref:
        raise ValueError("installed_image_changed")
    env = {row["name"]: row for row in containers[0]["env"]}
    if len(env) != len(containers[0]["env"]) or not _ENV_REQUIRED <= env.keys():
        raise ValueError("installed_connection_configuration_missing")
    ca_volume = {"name": "db-ca", "secret": {"secretName": "loom-platform-db", "defaultMode": 0o440,
                 "items": [{"key": "ca.crt", "path": "ca.crt"}]}}
    ca_mount = {"name": "db-ca", "mountPath": "/var/run/loom-db", "readOnly": True}
    if ([row for row in original.get("volumes", []) if row["name"] == "db-ca"] != [ca_volume]
            or [row for row in containers[0].get("volumeMounts", []) if row["name"] == "db-ca"] != [ca_mount]):
        raise ValueError("installed_database_ca_unqualified")
    selected = [copy.deepcopy(env[key]) for key in sorted(_ENV_REQUIRED | ({"LOOM_CP_DB_URL_POOL"} & env.keys()))]
    selected += [{"name": "LOOM_CP_STEP_JWT_SIGNING_KEY", "value": "archival-only-no-runtime-token-authority"}]
    name = "loom-archive-" + str(request.lease_id)[:8] + "-" + request.digest[7:19]
    labels = {"app": "loom-archive-recovery", "loom.nebius/archive-recovery": request.digest[7:23]}
    container = {
        "name": "archive", "image": request.image_ref, "imagePullPolicy": "IfNotPresent",
        "command": ["python", "-m", "loom_control_plane.pending_archive_recovery",
                    "--request-json", request.model_dump_json(), "--platform", "/var/run/loom-platform"],
        "env": selected,
        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                      "limits": {"cpu": "1", "memory": "1Gi"}},
        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]}},
        "volumeMounts": [ca_mount, {"name": "platform", "mountPath": "/var/run/loom-platform", "readOnly": True},
                         {"name": "tmp", "mountPath": "/tmp"}],
    }
    pod = {"serviceAccountName": "loom-platform", "automountServiceAccountToken": False,
           "restartPolicy": "Never", "terminationGracePeriodSeconds": 10,
           "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001,
                               "seccompProfile": {"type": "RuntimeDefault"}},
           "containers": [container], "volumes": [ca_volume,
               {"name": "platform", "configMap": {"name": "loom-platform-config",
                   "items": [{"key": key, "path": key} for key in ("environment.json", "profile.json")]}},
               {"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}},
           ]}
    for key in ("nodeSelector", "tolerations", "imagePullSecrets"):
        if key in original:
            pod[key] = copy.deepcopy(original[key])
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name, "namespace": request.namespace, "labels": labels,
                         "annotations": {"loom.nebius/archive-request": request.digest}},
            "spec": {"backoffLimit": 0, "parallelism": 1, "completions": 1,
                     "activeDeadlineSeconds": JOB_TIMEOUT,
                     "template": {"metadata": {"labels": labels}, "spec": pod}}}


def _readback(api: Kubectl, job: dict[str, Any]) -> dict[str, Any]:
    metadata = job["metadata"]
    current = api.get("job", metadata["name"], metadata["namespace"])
    if not current:
        raise ValueError("submitted_job_missing_preserve_intent")
    # Kubernetes adds defaults and controller labels; qualify all supplied leaves.
    def includes(actual: Any, expected: Any) -> bool:
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(key in actual and includes(actual[key], value) for key, value in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and len(actual) == len(expected) and all(
                includes(left, right) for left, right in zip(actual, expected, strict=True))
        return actual == expected
    if not includes(current, job) or not current["metadata"].get("uid"):
        raise ValueError("submitted_job_changed")
    return {"status": "submitted", "job_uid": current["metadata"]["uid"],
            "job_name": metadata["name"], "request_sha256": metadata["annotations"]["loom.nebius/archive-request"]}


def submit_once(api: Kubectl, job: dict[str, Any], state_dir: Path) -> dict[str, Any]:
    """Persist intent before create; an uncertain response never authorizes retry."""
    with private_state._locked_state(state_dir):
        marker = state_dir / "submission.json"
        identity = {"schema": "loom.pending-archive-submission.v1", "job_sha256": digest(job),
                    "job_name": job["metadata"]["name"], "namespace": job["metadata"]["namespace"]}
        if marker.exists() or marker.is_symlink():
            if json.loads(private_state._private_read(marker)) != identity:
                raise ValueError("submission_intent_changed")
        else:
            if api.get("job", identity["job_name"], identity["namespace"]):
                raise ValueError("existing_job_has_no_submission_intent")
            private_state._atomic_json(marker, identity)
            manifest = state_dir / "job.json"
            private_state._atomic_json(manifest, job)
            api.run("create", "-f", str(manifest))
        receipt = _readback(api, job)
        private_state._atomic_json(state_dir / "submission-receipt.json", receipt)
        return receipt


def qualify_publication(request: ArchiveRecoveryRequest, run_id: str) -> dict[str, Any]:
    selected = select_publication(run_id)
    if selected.get("status") != "ready" or selected.get("sha") != request.candidate_sha:
        raise ValueError("publication_binding_changed")
    # This operator's source is itself part of the protected published candidate.
    source_digest = source_archive_digest(request.candidate_sha)
    if (not candidate_follows(request.installed_candidate, request.candidate_sha)
            or candidate_schema_head(request.candidate_sha) != request.schema_head):
        raise ValueError("candidate_compatibility_changed")
    with tempfile.TemporaryDirectory(prefix="loom-archive-publication-") as directory:
        result = subprocess.run(["gh", "run", "download", run_id, "--repo", REPOSITORY,
            "--name", selected["artifact"], "--dir", directory], capture_output=True, check=False, timeout=120)
        if result.returncode:
            raise ValueError("publication_download_failed")
        candidate = read_json(Path(directory) / "candidate.json")
    validate_identity(candidate, require_current_images=True)
    if (candidate["candidate_sha"] != request.candidate_sha or str(candidate["run_id"]) != run_id
            or candidate["images"]["control_plane"]["image_ref"] != request.image_ref):
        raise ValueError("published_image_changed")
    return {**selected, "operator_source_sha256": source_digest, "image_ref": request.image_ref}


def qualify_platform(api: Kubectl, request: ArchiveRecoveryRequest) -> dict[str, Any]:
    data = api.get("configmap", "loom-platform-config", request.namespace)["data"]
    config = json.loads(data["environment.json"])
    profile = json.loads(data["profile.json"])
    if (config["cluster_id"] != request.cluster_id or config["namespace"] != request.namespace
            or profile["candidate_sha"] != request.installed_candidate):
        raise ValueError("platform_binding_changed")
    verify_cluster_identity(api, config, request.cluster_id)
    cp = api.get("deployment", "loom-control-plane", request.namespace)
    replicas = cp["spec"].get("replicas", 1)
    if (replicas < 1 or cp.get("status", {}).get("readyReplicas", 0) != replicas
            or cp.get("status", {}).get("observedGeneration", 0) != cp["metadata"]["generation"]):
        raise ValueError("control_plane_not_ready")
    pods = json.loads(api.run("get", "pods", "-n", request.namespace, "-l", "app=loom-control-plane", "-o", "json"))["items"]
    if len(pods) != replicas:
        raise ValueError("control_plane_pods_ambiguous")
    for pod in pods:
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if (pod["metadata"].get("deletionTimestamp") or len(statuses) != 1 or not statuses[0].get("ready")
                or len(pod["spec"]["containers"]) != 1
                or pod["spec"]["containers"][0]["image"] != request.installed_image_ref
                or not statuses[0].get("imageID", "").endswith(request.installed_image_ref.split("@", 1)[1])):
            raise ValueError("installed_worker_image_changed")
    return cp


def inspect_archive(api: Kubectl, *, namespace: str, team_id: UUID, lease_id: UUID) -> dict[str, Any]:
    """Run only the checked-in read-only projector with installed dependencies."""
    module_path = ROOT / "src/loom_control_plane/pending_archive_recovery.py"
    tree = ast.parse(module_path.read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)
             or (isinstance(node, ast.ClassDef | ast.AsyncFunctionDef)
             and node.name in {"RecoveryRefusedError", "project_oracle"})]
    program = ast.unparse(ast.Module(body=nodes, type_ignores=[]))
    program += "\n_INSPECT = " + repr({"team_id": str(team_id), "lease_id": str(lease_id)}) + "\n"
    program += r'''async def inspect():
 settings=ControlPlaneSettings()
 engine=create_async_engine(settings.db_engine_url,connect_args=settings.db_engine_connect_args)
 canonical=MinioObjectStore(endpoint_url=settings.minio_endpoint,access_key=settings.minio_access_key.get_secret_value(),secret_key=settings.minio_secret_key.get_secret_value(),region=settings.minio_region)
 config=ServiceExecutionSourceConfig.from_settings(settings)
 if config is None: raise RecoveryRefusedError("independent_source_store_required")
 source=config.build_store(MinioObjectStore)
 try:
  sessions=async_sessionmaker(engine,expire_on_commit=False)
  worker=ServiceExecutionMaterializer(session_factory=sessions,source_store=source,source_bucket=config.bucket,canonical_store=canonical,artifacts_bucket=settings.artifacts_bucket,trajectories_bucket=settings.trajectories_bucket)
  async with sessions.begin() as session:
   await session.execute(text("SET TRANSACTION READ ONLY"))
   await session.execute(text("SET LOCAL statement_timeout='10s'"))
   lease=await session.get(ServiceExecutionLease,UUID(_INSPECT["lease_id"]))
   if lease is None or str(lease.team_id)!=_INSPECT["team_id"]: raise RecoveryRefusedError("source_identity_missing")
   artifact=(await session.scalars(select(Artifact).where(Artifact.control_producer_kind=="service_execution",Artifact.control_producer_id==lease.id))).one()
   projection=await project_oracle(session,worker,team_id=lease.team_id,lease_id=lease.id)
   result={"team_id":str(lease.team_id),"trial_id":str(lease.trial_id),"lease_id":str(lease.id),"artifact_id":str(artifact.id),"upload_session_id":str(lease.output_upload_session_id),"attempt":lease.attempt,"generation":lease.output_generation,"output_manifest_sha256":lease.output_manifest_sha256,"output_marker_sha256":lease.output_marker_sha256,"schema_head":(await session.scalars(text("SELECT version_num FROM alembic_version"))).one(),**projection}
   print(json.dumps({"read_only":True,"observed_at":datetime.now(UTC).isoformat(),"binding":result}))
 finally:source.close();canonical.close();await engine.dispose()
asyncio.run(inspect())
'''
    result = json.loads(api.run("exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-c", program))
    if not result.get("read_only") or result.get("binding", {}).get("team_id") != str(team_id):
        raise ValueError("installed_projection_unqualified")
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
        api = Kubectl(args.kubeconfig, context="loom-rollout")
        if args.inspect_lease_id or args.inspect_team_id:
            if not (args.inspect_lease_id and args.inspect_team_id and args.namespace) or args.apply or args.request:
                raise ValueError("inspection_arguments_invalid")
            observed = inspect_archive(api, namespace=args.namespace, team_id=args.inspect_team_id,
                                       lease_id=args.inspect_lease_id)
            with private_state._locked_state(args.evidence_dir):
                private_state._atomic_json(args.evidence_dir / "installed-projection.json", observed)
            print(json.dumps({"status": "inspected", "read_only": True}))
            return 0
        if not args.request or not args.publication_run_id:
            raise ValueError("request_and_publication_required")
        request = ArchiveRecoveryRequest.model_validate(read_json(args.request))
        publication = qualify_publication(request, args.publication_run_id)
        job = recovery_job(request, qualify_platform(api, request))
        with private_state._locked_state(args.evidence_dir):
            private_state._atomic_json(args.evidence_dir / "publication.json", publication)
            private_state._atomic_json(args.evidence_dir / "reviewed-job.json", job)
        if args.apply:
            result = submit_once(api, job, args.evidence_dir / "submission")
        else:
            result = {"status": "prepared", "request_sha256": request.digest, "job_sha256": digest(job)}
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print("Archive recovery incomplete; preserve evidence and inspect the exact Job", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
