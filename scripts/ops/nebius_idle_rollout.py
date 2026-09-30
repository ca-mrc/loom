#!/usr/bin/env python3
"""Deploy a published dev candidate once, only when the platform is idle."""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import DeploymentError, Kubectl, deploy  # noqa: E402
from scripts.ops.nebius_rollout_reporting import emit_result, explanation  # noqa: E402

from loom.nebius_platform_render import build_platform, write_platform  # noqa: E402

REPOSITORY = "qianyi-sun/loom"


def github(path: str, payload: dict | None = None) -> dict:
    command = ["gh", "api", f"repos/{REPOSITORY}/{path}"]
    if payload is not None:
        command += ["--method", "POST", "--input", "-"]
    result = subprocess.run(command, input=json.dumps(payload) if payload is not None else None,
                            text=True, capture_output=True, check=False)
    if result.returncode:
        raise DeploymentError("GitHub deployment API failed")
    return json.loads(result.stdout)


def select_publication(run_id: str | None) -> dict:
    if run_id is None:
        runs = github("actions/workflows/nebius-candidate.yml/runs?branch=dev&event=push&status=success&per_page=1")
        if not runs["workflow_runs"]:
            return {"status": "skipped_no_candidate"}
        run_id = str(runs["workflow_runs"][0]["id"])
    if not run_id.isdigit():
        raise DeploymentError("publication run ID must be numeric")
    run = github(f"actions/runs/{run_id}")
    if (run["conclusion"] != "success" or run["head_branch"] != "dev"
        or run["head_repository"]["full_name"] != REPOSITORY
        or run["path"] != ".github/workflows/nebius-candidate.yml"
        or run["event"] not in {"push", "workflow_dispatch"}):
        raise DeploymentError("not a successful same-repository dev publication")
    sha = run["head_sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise DeploymentError("invalid publication commit")
    artifact = f"nebius-candidate-{sha}-{run_id}-{run['run_attempt']}"
    artifacts = github(f"actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    if not any(row["name"] == artifact and not row["expired"] for row in artifacts):
        # harness-only publications are intentionally not platform deployments.
        return {"status": "skipped_no_platform_candidate", "sha": sha, "run_id": run_id}
    if subprocess.run(["git", "cat-file", "-e", sha + ":scripts/ops/nebius_idle_rollout.py"],
                      cwd=ROOT, capture_output=True).returncode:
        return {"status": "skipped_before_idle_rollout_support", "sha": sha, "run_id": run_id}
    return {"status": "ready", "sha": sha, "run_id": run_id, "artifact": artifact}


def select_recovery(run_id: str, directory: Path) -> dict:
    """Recover the candidate and owner recorded by a failed protected rollout."""
    if not run_id.isdigit():
        raise DeploymentError("recovery run ID must be numeric")
    run = github(f"actions/runs/{run_id}")
    if (run.get("conclusion") != "failure" or run.get("status") != "completed"
            or run.get("head_branch") != "dev" or run.get("head_repository", {}).get("full_name") != REPOSITORY
            or run.get("path") != ".github/workflows/nebius-rollout.yml"):
        raise DeploymentError("recovery requires a terminal failed same-repository dev rollout")
    directory.mkdir(parents=True, exist_ok=False)
    result = subprocess.run([
        "gh", "run", "download", run_id, "--repo", REPOSITORY,
        "--name", f"nebius-rollout-{run_id}-{run['run_attempt']}", "--dir", str(directory),
    ], capture_output=True, text=True, check=False)
    if result.returncode:
        raise DeploymentError("failed rollout evidence unavailable")
    records = list(directory.glob("deployment-*.json"))
    if len(records) != 1:
        raise DeploymentError("recovery requires one immutable failed deployment record")
    record = json.loads(records[0].read_text())
    sha = record.get("candidate_sha", "")
    if (record.get("schema_version") != "loom.nebius-deployment.v1" or record.get("status") != "failed"
            or record.get("mode") != "apply" or record.get("dispatch_paused") is not True
            or not re.fullmatch(r"[0-9a-f]{40}", sha)
            or not re.fullmatch(r"rollout-[0-9a-f]{32}", record.get("guard_owner", ""))
            or record.get("target_replacement")):
        raise DeploymentError("failed rollout is outside ordinary candidate recovery")
    runs = github("actions/workflows/nebius-candidate.yml/runs?branch=dev&status=success&head_sha=" + sha + "&per_page=100")
    matches = [row for row in runs["workflow_runs"] if row.get("head_sha") == sha]
    if not matches:
        raise DeploymentError("failed candidate publication unavailable")
    selected = None
    for match in matches:
        publication = select_publication(str(match["id"]))
        if publication.get("status") == "ready" and publication.get("sha") == sha:
            selected = publication
            break
    if selected is None:
        raise DeploymentError("failed candidate publication is not recoverable")
    selected["recovery_evidence"] = str(records[0])
    return selected


def candidate_schema_head(sha: str) -> str:
    """Read the pinned candidate's migration graph when recovery uses newer tools."""
    from loom.db.schema_startup import service_schema_head

    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise DeploymentError("invalid recovery candidate commit")
    result = subprocess.run(["git", "archive", sha, "database/migrations"],
                            cwd=ROOT, capture_output=True, check=False)
    if result.returncode:
        raise DeploymentError("recovery candidate migration source unavailable")
    with tempfile.TemporaryDirectory(prefix="loom-recovery-schema-") as directory:
        with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
            archive.extractall(directory, filter="data")
        return service_schema_head(Path(directory) / "database/migrations/alembic.ini")


def candidate_follows(current: str, candidate: str) -> bool:
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (current, candidate)):
        raise DeploymentError("invalid current or candidate commit")
    # Unknown history is an error, never permission to downgrade.
    for sha in (current, candidate):
        if subprocess.run(["git", "cat-file", "-e", sha + "^{commit}"], cwd=ROOT,
                          capture_output=True).returncode:
            raise DeploymentError("deployment requires Git history for both commits")
    result = subprocess.run(["git", "merge-base", "--is-ancestor", current, candidate],
                            cwd=ROOT, capture_output=True)
    if result.returncode not in {0, 1}:
        raise DeploymentError("candidate ancestry check failed")
    return result.returncode == 0


def rollout(args: argparse.Namespace) -> dict:
    kube = Kubectl(args.kubeconfig)
    data = kube.get("configmap", "loom-platform-config", args.namespace)["data"]
    config = json.loads(data["environment.json"])
    if config["namespace"] != args.namespace or config.get("regional_execution_targets"):
        raise DeploymentError("idle rollout supports the existing single-primary platform only")
    candidate = json.loads((args.publication_dir / "candidate.json").read_text())
    sha = candidate["candidate_sha"]
    if sha != args.candidate:
        raise DeploymentError("publication does not identify the selected commit")
    current = json.loads(data["profile.json"])["candidate_sha"]
    if getattr(args, "recovery_evidence", None) and current != sha:
        raise DeploymentError("recovery binding differs from the failed candidate and live platform")
    if current != sha and not candidate_follows(current, sha):
        return {"status": "skipped_superseded", "candidate_sha": sha}
    profile = json.loads((args.publication_dir / "runtime-profile.json").read_text())
    # Preserve all live settings: task requests, builder concurrency, resource IDs.
    files = build_platform(config, candidate, profile, json.loads(data["keyring.json"]), repo_root=ROOT)
    args.render_dir = args.evidence_dir / "rendered"
    write_platform(files, config, candidate, args.render_dir)
    args.apply = True
    args.retry_failed_jobs = False
    args.expected_current_candidate = current
    if recovery := getattr(args, "recovery_evidence", None):
        record = json.loads(recovery.read_text())
        if (record.get("candidate_sha") != sha or current != sha
                or record.get("cluster_id") != config["cluster_id"]
                or record.get("namespace") != config["namespace"]
                or record.get("execution_namespace") != config["execution_namespace"]
                or record.get("schema_version") != "loom.nebius-deployment.v1"
                or record.get("mode") != "apply"
                or not re.fullmatch(r"rollout-[0-9a-f]{32}", record.get("guard_owner", ""))
                or record.get("status") != "failed" or record.get("dispatch_paused") is not True
                or record.get("target_replacement")):
            raise DeploymentError("recovery binding differs from the failed candidate and live platform")
        args.migration_schema_head = candidate_schema_head(sha)
        args.resume_guard_owner = record["guard_owner"]
        args.retry_failed_jobs = True
    deployment_id = None
    if args.github:
        deployment_id = github("deployments", {
            "ref": sha, "environment": "nebius-integration", "auto_merge": False,
            "required_contexts": [], "transient_environment": False, "production_environment": False,
            "description": "Checking whether Nebius is idle; no waiting or retry",
        })["id"]

    def report(state: str, description: str) -> None:
        if deployment_id is not None:
            github(f"deployments/{deployment_id}/statuses", {
                "state": state, "description": description,
                "log_url": f"https://github.com/{REPOSITORY}/actions/runs/{os.environ['GITHUB_RUN_ID']}",
                "environment_url": "https://" + config["public_host"],
                "auto_inactive": state == "success",
            })

    try:
        report("in_progress", "Check idle, then backup and rollout; busy environments are skipped")
        result = deploy(args, kube=kube)
    except Exception:
        report("failure", "Rollout failed; inspect phase evidence and dispatch pause before recovery")
        raise
    if result["status"] == "complete":
        report("success", "Candidate deployed; HTTPS and workload versions verified; dispatch resumed")
    else:
        report("inactive", ("Skipped: " + explanation(result))[:140])
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    select = sub.add_parser("select")
    select.add_argument("--run-id")
    select.add_argument("--recovery-run-id")
    select.add_argument("--recovery-dir", type=Path)
    run = sub.add_parser("run")
    run.add_argument("--publication-dir", type=Path, required=True)
    run.add_argument("--candidate", required=True)
    run.add_argument("--kubeconfig", type=Path, required=True)
    run.add_argument("--expected-cluster-id", required=True)
    run.add_argument("--namespace", default="loom-nebius-platform")
    run.add_argument("--evidence-dir", type=Path, required=True)
    run.add_argument("--github", action="store_true")
    run.add_argument("--recovery-evidence", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "select":
            if args.recovery_run_id:
                if args.run_id or args.recovery_dir is None:
                    raise DeploymentError("recovery requires its own evidence directory and no publication override")
                result = select_recovery(args.recovery_run_id, args.recovery_dir)
            else:
                result = select_publication(args.run_id)
        else:
            result = rollout(args)
        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a") as stream:
                for key in ("status", "sha", "run_id", "artifact", "recovery_evidence"):
                    if key in result:
                        stream.write(f"{key}={result[key]}\n")
        emit_result(result)
        return 0
    except Exception as exc:
        print(f"Idle rollout failed ({type(exc).__name__}); inspect sanitized deployment evidence", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
