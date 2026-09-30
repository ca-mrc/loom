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
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import DeploymentError, Kubectl, deploy  # noqa: E402
from scripts.ops.nebius_rollout_reporting import emit_result, explanation  # noqa: E402

from loom.nebius_platform_render import build_platform, write_platform  # noqa: E402

REPOSITORY = "qianyi-sun/loom"


def github(path: str, payload: dict | None = None) -> dict | list:
    command = ["gh", "api", f"repos/{REPOSITORY}/{path}"]
    if payload is not None:
        command += ["--method", "POST", "--input", "-"]
    result = subprocess.run(command, input=json.dumps(payload) if payload is not None else None,
                            text=True, capture_output=True, check=False)
    if result.returncode:
        raise DeploymentError("GitHub deployment API failed")
    return json.loads(result.stdout)


def recovery_record(value: dict) -> dict:
    """Project a persisted intent; the database alone establishes its held pause."""
    if (not isinstance(value, dict)
            or value.get("schema_version") != "loom.nebius-deployment.v1"
            or value.get("status") not in {"failed", "running"} or value.get("mode") != "apply"
            or not isinstance(value.get("candidate_sha"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", value["candidate_sha"])
            or not isinstance(value.get("guard_owner"), str)
            or not re.fullmatch(r"rollout-[0-9a-f]{32}", value["guard_owner"])
            or value.get("target_replacement") is not None):
        raise DeploymentError("failed rollout is outside ordinary candidate recovery")
    for key in ("cluster_id", "namespace", "execution_namespace"):
        if not isinstance(value.get(key), str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value[key]):
            raise DeploymentError("invalid persisted recovery platform binding")
    previous = value.get("previous_candidate_sha")
    if previous is not None and (not isinstance(previous, str) or not re.fullmatch(r"[0-9a-f]{40}", previous)):
        raise DeploymentError("invalid persisted previous candidate")
    return {key: value[key] for key in (
        "schema_version", "status", "mode", "guard_owner", "candidate_sha", "cluster_id", "namespace",
        "execution_namespace", "previous_candidate_sha", "run_id", "run_attempt",
    ) if key in value}


def persisted_recovery_intent(run_id: str, attempt: int) -> dict:
    """Find the server-side request that survived the original runner's loss."""
    matches = []
    page = 1
    while True:
        rows = github(f"deployments?environment=nebius-integration&per_page=100&page={page}")
        if not isinstance(rows, list):
            raise DeploymentError("persisted rollout intent inventory unavailable")
        for row in rows:
            if not isinstance(row, dict):
                raise DeploymentError("persisted rollout intent inventory unavailable")
            payload = row.get("payload")
            if (not isinstance(payload, dict) or str(payload.get("run_id")) != run_id
                    or str(payload.get("run_attempt")) != str(attempt)):
                continue
            if row.get("environment") != "nebius-integration" or row.get("sha") != payload.get("candidate_sha"):
                raise DeploymentError("persisted rollout intent binding differs")
            matches.append(recovery_record(payload))
        if len(rows) < 100:
            break
        page += 1
    if len(matches) != 1:
        raise DeploymentError("recovery requires one persisted rollout intent for the failed attempt")
    return matches[0]


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
    if (run.get("conclusion") not in {"failure", "timed_out"} or run.get("status") != "completed"
            or run.get("head_branch") != "dev" or run.get("head_repository", {}).get("full_name") != REPOSITORY
            or run.get("path") != ".github/workflows/nebius-rollout.yml"):
        raise DeploymentError("recovery requires a terminal failed same-repository dev rollout")
    directory.mkdir(parents=True, exist_ok=False)
    result = subprocess.run([
        "gh", "run", "download", run_id, "--repo", REPOSITORY,
        "--name", f"nebius-rollout-{run_id}-{run['run_attempt']}", "--dir", str(directory),
    ], capture_output=True, text=True, check=False)
    records = list(directory.glob("deployment-*.json"))
    if not result.returncode and records:
        if len(records) != 1:
            raise DeploymentError("recovery requires one immutable failed deployment record")
        record_path = records[0]
        record = recovery_record(json.loads(record_path.read_text()))
    else:
        record = persisted_recovery_intent(run_id, run["run_attempt"])
        record_path = directory / "deployment-intent.json"
        record_path.write_text(json.dumps(record, sort_keys=True) + "\n")
    sha = record["candidate_sha"]
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
    selected["recovery_evidence"] = str(record_path)
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
    recovery = getattr(args, "recovery_evidence", None)
    record = None
    if recovery:
        record = recovery_record(json.loads(recovery.read_text()))
        if (record["candidate_sha"] != sha or current not in {sha, record.get("previous_candidate_sha", sha)}
                or any(record[key] != config[key] for key in ("cluster_id", "namespace", "execution_namespace"))):
            raise DeploymentError("recovery binding differs from the failed candidate and live platform")
    elif current != sha and not candidate_follows(current, sha):
        return {"status": "skipped_superseded", "candidate_sha": sha}
    profile = json.loads((args.publication_dir / "runtime-profile.json").read_text())
    # Preserve all live settings: task requests, builder concurrency, resource IDs.
    files = build_platform(config, candidate, profile, json.loads(data["keyring.json"]), repo_root=ROOT)
    args.render_dir = args.evidence_dir / "rendered"
    write_platform(files, config, candidate, args.render_dir)
    args.apply = True
    args.retry_failed_jobs = False
    args.expected_current_candidate = current
    args.guard_owner = "rollout-" + uuid.uuid4().hex
    if record is not None:
        args.migration_schema_head = candidate_schema_head(sha)
        args.resume_guard_owner = record["guard_owner"]
        args.guard_owner = record["guard_owner"]
        args.retry_failed_jobs = True
    deployment_id = None
    if args.github:
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
        if (run_id or attempt) and (not run_id.isdigit() or not attempt.isdigit() or int(attempt) < 1):
            raise DeploymentError("GitHub rollout requires its exact run and attempt")
        intent = {"schema_version": "loom.nebius-deployment.v1", "mode": "apply", "status": "running",
                  "guard_owner": args.guard_owner, "candidate_sha": sha, "previous_candidate_sha": current,
                  **{key: config[key] for key in ("cluster_id", "namespace", "execution_namespace")}}
        if run_id:
            intent.update(run_id=int(run_id), run_attempt=int(attempt))
        deployment_id = github("deployments", {
            "ref": sha, "environment": "nebius-integration", "auto_merge": False,
            "required_contexts": [], "transient_environment": False, "production_environment": False,
            "description": "Checking whether Nebius is idle; no waiting or retry",
            "payload": intent,
        })["id"]

    def report(state: str, description: str) -> None:
        if deployment_id is not None:
            github(f"deployments/{deployment_id}/statuses", {
                "state": state, "description": description,
                **({"log_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"} if run_id else {}),
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
