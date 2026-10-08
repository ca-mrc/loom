#!/usr/bin/env python3
"""Plan or apply operator-reviewed manifests for the Nebius integration platform."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "src"))

from loom.nebius_platform_render import canonical, validate_environment  # noqa: E402
from loom.nebius_task_identity_policy import identity_policy_documents  # noqa: E402

PHASE_FILES = (
    "00-namespaces.yaml",
    "10-config-network.yaml",
    "20-database.yaml",
    "30-migrate.yaml",
    "40-services.yaml",
    "50-configure.yaml",
    "60-execution.yaml",
    "70-public.yaml",
    "80-backup.yaml",
)


class DeploymentError(ValueError):
    """A deployment boundary failed; its message contains no secret values."""


class TaskIdentityPolicyDeniedError(DeploymentError):
    def __init__(self, policy_name: str):
        self.policy_name = policy_name
        super().__init__("task identity policy rejected the admission probe")


def load_render(
    render_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Read operator-reviewed manifests once; namespace boundaries remain explicit."""
    files = {
        filename: list(yaml.safe_load_all((render_dir / filename).read_text()))
        for filename in PHASE_FILES
    }
    configs = [
        row
        for row in files["10-config-network.yaml"]
        if row["kind"] == "ConfigMap" and row["metadata"]["name"] == "loom-platform-config"
    ]
    if len(configs) != 1:
        raise DeploymentError("platform configuration identity is ambiguous")
    config = json.loads(configs[0]["data"]["environment.json"])
    validate_environment(config)
    policy_file = render_dir / "00-task-identity-policy.yaml"
    if config.get("task_identity_policy") is not None:
        try:
            policy_docs = list(yaml.safe_load_all(policy_file.read_text()))
        except OSError as exc:
            raise DeploymentError("task identity policy artifact is missing") from exc
        if policy_docs != identity_policy_documents(config["execution_namespace"], config["target_id"],
                guest_target_id=config.get("guest_execution_target", {}).get("target_id"),
                emulated_auth_target_id=config.get("emulated_auth_execution_target", {}).get("target_id")):
            raise DeploymentError("task identity policy differs from the target-bound contract")
        files[policy_file.name] = policy_docs
    elif policy_file.exists():
        raise DeploymentError("task identity policy requires an explicit target configuration")
    execution_namespaces = [row for row in files["00-namespaces.yaml"]
                            if row["kind"] == "Namespace" and row["metadata"]["name"] == config["execution_namespace"]]
    expected_pss = "baseline" if config.get("task_identity_policy") is not None else "restricted"
    if (len(execution_namespaces) != 1 or execution_namespaces[0]["metadata"].get("labels", {}).get(
            "pod-security.kubernetes.io/enforce") != expected_pss):
        raise DeploymentError("execution namespace policy differs from the explicit configuration")
    if config["schema_version"] == "loom.nebius-managed-environment.v1":
        raise DeploymentError("managed children require the environment-management lifecycle")
    namespaces = {config["namespace"], config["execution_namespace"]}
    if config.get("task_image_builder") is not None:
        namespaces.add(config["execution_namespace"] + "-build")
    for filename, rows in files.items():
        for row in rows:
            metadata = row["metadata"]
            namespace = metadata.get("namespace")
            if row["kind"] == "Namespace":
                namespace = metadata["name"]
            if row["kind"] in {"ClusterRole", "ClusterRoleBinding"}:
                if metadata["name"] not in {
                    config["execution_namespace"] + "-collector",
                    config["execution_namespace"] + "-actuator-usage",
                }:
                    raise DeploymentError(
                        "cluster resource does not belong to the integration target"
                    )
                continue
            if row["kind"] in {"ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"}:
                if (filename != "00-task-identity-policy.yaml"
                        or metadata["name"] != config["execution_namespace"] + "-private-root-v1"):
                    raise DeploymentError("admission policy does not belong to the integration target")
                continue
            if namespace not in namespaces:
                raise DeploymentError(
                    f"rendered resource targets a different namespace: {filename}"
                )
    profile = json.loads(configs[0]["data"]["profile.json"])
    deployment = {
        "candidate_sha": profile["candidate_sha"],
        "cluster_id": config["cluster_id"],
        "namespace": config["namespace"],
        "execution_namespace": config["execution_namespace"],
        "target_id": config["target_id"],
        "public_origin": "https://" + config["public_host"],
    }
    return deployment, config, files


def secret_requirements(
    files: dict[str, list[dict[str, Any]]], config: dict[str, Any]
) -> dict[tuple[str, str], set[str]]:
    result: dict[tuple[str, str], set[str]] = {}

    def walk(value: Any, namespace: str) -> None:
        if isinstance(value, list):
            for child in value:
                walk(child, namespace)
        elif isinstance(value, dict):
            ref = value.get("secretKeyRef")
            if isinstance(ref, dict) and not ref.get("optional", False):
                result.setdefault((namespace, ref["name"]), set()).add(ref["key"])
            secret = value.get("secret")
            if isinstance(secret, dict) and not secret.get("optional", False):
                name = secret.get("secretName", secret.get("name"))
                if name:
                    keys = result.setdefault((namespace, name), set())
                    keys.update(item["key"] for item in secret.get("items", []))
                    if name in {config["tls_secret_name"], config["db_tls_secret_name"]}:
                        keys.update(("tls.crt", "tls.key"))
                    if name == "loom-admin-secret":
                        keys.add("secrets.toml")
            for child in value.values():
                walk(child, namespace)

    for rows in files.values():
        for doc in rows:
            namespace = doc["metadata"].get("namespace")
            if namespace:
                walk(doc, namespace)
    # Build Jobs are created on demand, so their mounted Secrets do not appear
    # in the static Deployment. Check their names/keys before enabling the loop.
    builder = config.get("task_image_builder")
    if builder is not None:
        namespace = config["execution_namespace"] + "-build"
        result[namespace, "loom-task-build-source"] = {"access-key", "secret-key"}
        result[namespace, "loom-task-build-registry"] = {"credentials.json"}
        if builder.get("cache_bucket"):
            result[namespace, "loom-task-build-cache"] = {"access-key", "secret-key"}
    return result


class Kubectl:
    def __init__(self, kubeconfig: Path, *, context: str | None = None):
        self.kubeconfig = kubeconfig
        self.context = context

    def run(self, *args: str, timeout: int = 90, preserve_output: bool = False) -> str:
        command = [
                "kubectl",
                "--kubeconfig",
                str(self.kubeconfig),
                *(["--context", self.context] if self.context else []),
                *([] if args[0] in {"wait", "rollout"} else ["--request-timeout=30s"]),
                *args,
            ]
        stdin = None
        if target := os.environ.get("LOOM_DEPLOY_SSH_TARGET"):
            if not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target):
                raise DeploymentError("invalid deployment SSH target")
            if args[0] in {"apply", "create"} and args[1] == "-f":
                stdin = Path(args[2]).read_text()
                command[command.index("-f") + 1] = "-"
            command = [
                "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15",
                "-o", "ServerAliveCountMax=3", "-o", "IdentitiesOnly=yes",
                "-o", "UserKnownHostsFile=" + os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"],
                "-i", os.environ["LOOM_DEPLOY_SSH_KEY_FILE"], target, shlex.join(command),
            ]
        result = subprocess.run(
            command,
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode:
            denied = re.search(
                r"ValidatingAdmissionPolicy '([a-z0-9-]+-private-root-v1)' with binding '[a-z0-9-]+' denied request",
                result.stderr,
            )
            if denied:
                raise TaskIdentityPolicyDeniedError(denied[1])
            # Preserve API reason codes without serializing message bodies,
            # URLs, Secret data, or arbitrary application logs.
            match = re.search(r"Error from server \(([A-Za-z]{1,64})\)", result.stderr)
            reason = (
                match.group(1)
                if match
                else (
                    "DeadlineExceeded" if "timed out waiting" in result.stderr else "CommandFailed"
                )
            )
            raise DeploymentError(
                f"kubectl {args[0]} failed with exit code {result.returncode}: {reason}"
            )
        return result.stdout if preserve_output else result.stdout.strip()

    def get(self, kind: str, name: str, namespace: str) -> dict[str, Any]:
        result = self.run("get", kind, name, "-n", namespace, "--ignore-not-found", "-o", "json")
        return json.loads(result) if result else {}

    def legacy_pool_completion(self, operation: str) -> dict[str, Any]:
        from scripts.ops.nebius_legacy_pool_completion import (
            canonical_uuid,
            read_completion,
            validate_projection,
        )

        try:
            canonical_uuid(operation)
            target = os.environ.get("LOOM_DEPLOY_SSH_TARGET")
            if not target:
                return read_completion(operation)
            if not re.fullmatch(r"[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+", target):
                raise ValueError
            command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15",
                "-o", "ServerAliveCountMax=3", "-o", "IdentitiesOnly=yes",
                "-o", "UserKnownHostsFile=" + os.environ["LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE"],
                "-i", os.environ["LOOM_DEPLOY_SSH_KEY_FILE"], target,
                shlex.join(["loom-nebius-legacy-pool-completion-v1", operation])]
            result = subprocess.run(command, capture_output=True, text=True, timeout=90, check=False)
            if result.returncode or len(result.stdout) > 512 * 1024:
                raise ValueError
            return validate_projection(json.loads(result.stdout), operation)
        except Exception:
            raise DeploymentError("legacy pool completion unavailable; preserve protected recovery") from None


def install_task_identity_policy(
    kube: Kubectl, config: dict[str, Any], snapshot_root: Path,
) -> None:
    """Prove policy enforcement before the caller may relax namespace PSS.

    The deployment guard has already drained active work. Every failure leaves
    restricted PSS enforced. Neither probe creates a workload or pulls an image.
    """
    namespace = config["execution_namespace"]
    name = namespace + "-private-root-v1"
    bootstrap = snapshot_root / "identity-policy-bootstrap.yaml"
    bootstrap.write_text(yaml.safe_dump_all([
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": namespace, "labels": {"pod-security.kubernetes.io/enforce": "restricted"},
        }},
        {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
            "name": "loom-execution-policy-probe", "namespace": namespace,
        }, "automountServiceAccountToken": False},
    ]))
    kube.run("apply", "-f", str(bootstrap))
    kube.run("apply", "-f", str(snapshot_root / "00-task-identity-policy.yaml"))
    deadline = time.monotonic() + 30
    while True:
        policy = kube.get("validatingadmissionpolicy", name, namespace)
        status = policy.get("status", {})
        if (status.get("observedGeneration") == policy.get("metadata", {}).get("generation")
                and status.get("observedGeneration", 0) > 0):
            # The status controller publishes the generation only after type
            # checking. Clearing warnings through server-side apply can omit
            # the empty typeChecking parent from an otherwise current status.
            if status.get("typeChecking", {}).get("expressionWarnings"):
                raise DeploymentError("task identity admission policy has type-checking warnings")
            break
        if time.monotonic() >= deadline:
            raise DeploymentError("task identity admission policy was not observed by the API server")
        time.sleep(0.2)
    pod: dict[str, Any] = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": "loom-execution-policy-probe", "namespace": namespace,
    }, "spec": {
        "automountServiceAccountToken": False, "serviceAccountName": "loom-execution-policy-probe",
        "restartPolicy": "Never", "securityContext": {
            "runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532,
            "seccompProfile": {"type": "RuntimeDefault"},
        }, "containers": [{"name": "execution", "image": "invalid.local/admission-only:unused",
                           # ResourceQuota also validates dry-run Pods. These
                           # bounded declarations create no workload or usage.
                           "resources": {
                               "requests": {"cpu": "10m", "memory": "16Mi"},
                               "limits": {"cpu": "10m", "memory": "16Mi"},
                           }, "securityContext": {
                               "runAsNonRoot": True, "allowPrivilegeEscalation": False,
                               "capabilities": {"drop": ["ALL"]},
                           }}],
    }}
    probe = snapshot_root / "identity-policy-probe.yaml"
    probe.write_text(yaml.safe_dump(pod))
    while True:
        try:
            kube.run("apply", "-f", str(probe), "--dry-run=server")
            break
        except TaskIdentityPolicyDeniedError as exc:
            if exc.policy_name != name:
                raise DeploymentError("another policy rejected the identity probe") from exc
            # Admission evaluation has its own informer/cache. A corrected
            # policy's type-check status can arrive before it stops rejecting
            # this safe Pod. PSS stays restricted throughout both probes.
            if time.monotonic() >= deadline:
                raise DeploymentError("task identity admission policy did not accept the positive probe") from exc
        time.sleep(0.2)
    pod["spec"]["containers"][0]["securityContext"]["capabilities"]["add"] = ["NET_BIND_SERVICE"]
    probe.write_text(yaml.safe_dump(pod))
    while True:
        try:
            kube.run("apply", "-f", str(probe), "--dry-run=server")
        except TaskIdentityPolicyDeniedError as exc:
            if exc.policy_name != name:
                raise DeploymentError("another policy rejected the identity probe") from exc
            return
        if time.monotonic() >= deadline:
            raise DeploymentError("task identity admission policy did not reject the negative probe")
        time.sleep(0.2)


def job_complete(job: dict[str, Any]) -> bool:
    return any(
        condition.get("type") == "Complete" and condition.get("status") == "True"
        for condition in job.get("status", {}).get("conditions", [])
    )


def job_failed(job: dict[str, Any]) -> bool:
    return any(
        condition.get("type") == "Failed" and condition.get("status") == "True"
        for condition in job.get("status", {}).get("conditions", [])
    )


def wait_for_job(kube: Kubectl, name: str, namespace: str, seconds: int) -> None:
    """Return on either terminal condition, preserving failures for diagnosis."""
    deadline = time.monotonic() + seconds
    while True:
        job = kube.get("job", name, namespace)
        if job_failed(job):
            raise DeploymentError(f"job {name} reached Failed condition")
        if job_complete(job):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeploymentError(f"job {name} completion timed out")
        time.sleep(min(5, remaining))


def verify_cluster_identity(
    kube: Kubectl,
    config: dict[str, Any],
    expected_cluster_id: str,
) -> None:
    """Check the existing independent target before deployment or maintenance."""
    if config["cluster_id"] != expected_cluster_id:
        raise DeploymentError("expected cluster id does not match environment")
    view = json.loads(kube.run("config", "view", "--minify", "-o", "json"))
    clusters = view.get("clusters", [])
    if (
        len(clusters) != 1
        or clusters[0]["cluster"].get("server") != config["kubernetes_api_server"]
    ):
        raise DeploymentError("selected kubeconfig API server does not match environment")
    cluster = clusters[0]["cluster"]
    if not clusters[0]["name"].endswith(expected_cluster_id.removeprefix("mk8s")):
        raise DeploymentError(
            "selected kubeconfig cluster identity does not match expected Nebius cluster"
        )
    if cluster.get("insecure-skip-tls-verify") or not (
        cluster.get("certificate-authority") or cluster.get("certificate-authority-data")
    ):
        raise DeploymentError("selected cluster must use its trusted certificate authority")
    nodes = json.loads(
        kube.run("get", "nodes", "-l", "loom.nebius/platform=integration", "-o", "json")
    )["items"]
    if not nodes or not any(
        node["metadata"].get("labels", {}).get("loom.nebius/node-role") == "system"
        for node in nodes
    ):
        raise DeploymentError("independent integration system nodes are not provisioned")
    for node in nodes:
        provider_id = node.get("spec", {}).get("providerID", "")
        if re.fullmatch(r"nebius://computeinstance-[a-z0-9]+", provider_id) is None or node[
            "metadata"
        ]["name"] != provider_id.removeprefix("nebius://"):
            raise DeploymentError("integration node provider identity is not Nebius")


def verify_ingress_mode(kube: Kubectl, config: dict[str, Any]) -> None:
    """Ordinary application deployment preserves a previously installed route."""
    shared = config.get("shared_ingress_enabled", False)
    public = kube.get("service", "loom-web", config["namespace"])
    expected_selector = {"app": "loom-shared-ingress" if shared else "loom-web"}
    if ((shared and not public)
            or (public and public.get("spec", {}).get("selector") != expected_selector)):
        raise DeploymentError("ingress cutover requires its protected installation procedure")
    if shared:
        controller = kube.get("deployment", "loom-shared-ingress", config["namespace"])
        status = controller.get("status", {})
        replicas = controller.get("spec", {}).get("replicas", 0)
        if (not replicas
                or status.get("observedGeneration", 0) != controller.get("metadata", {}).get("generation")
                or status.get("availableReplicas", 0) < replicas
                or status.get("updatedReplicas", 0) < replicas):
            raise DeploymentError("shared ingress controller is not ready")


def migration_readiness(kube: Kubectl, namespace: str, expected_head: str | None = None) -> dict[str, Any]:
    """Inspect the existing schema/access boundary without changing credentials."""
    from loom.db.schema_startup import service_schema_head

    def query(statement: str) -> Any:
        output = kube.run(
            "exec", "-n", namespace, "statefulset/loom-postgres", "--",
            "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom",
            "-c", "BEGIN READ ONLY; SET LOCAL statement_timeout='10s'; " + statement + "; ROLLBACK;",
        )
        try:
            return json.loads(output)
        except (ValueError, TypeError):
            raise DeploymentError("application_database_readiness_unavailable") from None

    observed = query("""SELECT json_build_object(
        'version_table', to_regclass('public.alembic_version') IS NOT NULL,
        'access_schema', to_regnamespace('loom_application_access') IS NOT NULL,
        'access_guard', EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='loom_application_access' AND p.proname='migration_ready' AND p.pronargs=0))""")
    if (not isinstance(observed, dict) or set(observed) != {"version_table", "access_schema", "access_guard"}
            or type(observed["version_table"]) is not bool
            or type(observed["access_schema"]) is not bool or type(observed["access_guard"]) is not bool):
        raise DeploymentError("application_database_readiness_unavailable")
    revision = query("SELECT COALESCE(to_json((SELECT version_num FROM public.alembic_version)), 'null'::json)") if observed["version_table"] else None
    if revision is not None and not isinstance(revision, str):
        raise DeploymentError("application_database_readiness_unavailable")
    needed = revision != (expected_head or service_schema_head())
    if needed and observed["access_schema"]:
        if not observed["access_guard"]:
            raise DeploymentError("application_database_schema_guard_not_installed")
        ready = query("SELECT to_json(loom_application_access.migration_ready())")
        if type(ready) is not bool:
            raise DeploymentError("application_database_readiness_unavailable")
        if not ready:
            raise DeploymentError("application_database_access_active")
    return {"migration_needed": needed}


def verify_standalone_pool_boundary(kube: Kubectl, config: dict[str, Any], *,
        completions: dict[str, dict[str, Any]] | None = None, allow_load: bool = True) -> None:
    """The legacy standalone renderer cannot retire or overwrite global wiring.

    Retirement metadata survives rollback. Its anchored terminal legacy outcome
    permits ordinary upgrades; it never excuses current global wiring. Reuse
    that immutable proof under the idle guard, but reread live marker and UID.
    """
    from scripts.ops.nebius_legacy_pool_completion import validate_projection

    from loom.nebius_guest_target import guest_target_ids

    if completions is None:
        completions = {}

    processes = [(config["namespace"], name) for name in ("loom-service", "loom-control-plane")]
    processes.append((config["execution_namespace"], "loom-execution-actuator"))
    for guest in guest_target_ids(config):
        processes.append((config["execution_namespace"], guest + "-actuator"))
    settings = {"LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON", "LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL",
        "LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON"}
    for namespace, name in processes:
        current = kube.get("deployment", name, namespace)
        annotations = current.get("metadata", {}).get("annotations", {})
        if any(
            row.get("name") in settings
            for container in current.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
            for row in container.get("env", [])
        ):
            raise DeploymentError("pool runtime preservation requires its protected cutover or refresh")
        if "loom.nebius/pool-retirement-operation" in annotations:
            operation = annotations["loom.nebius/pool-retirement-operation"]
            try:
                if operation not in completions:
                    if not allow_load:
                        raise ValueError
                    completions[operation] = validate_projection(kube.legacy_pool_completion(operation), operation)
                proof = completions[operation]
                key = "Deployment:" + namespace + ":" + name
                if key not in proof["workloads"] or proof["workloads"][key] != current.get("metadata", {}).get("uid"):
                    raise ValueError
            except Exception:
                raise DeploymentError("historical pool retirement requires its completed legacy rollback") from None


def preflight(
    kube: Kubectl,
    manifest: dict[str, Any],
    config: dict[str, Any],
    files: dict[str, list[dict[str, Any]]],
    expected_cluster_id: str,
    *, pool_completions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    verify_cluster_identity(kube, config, expected_cluster_id)
    verify_ingress_mode(kube, config)
    verify_standalone_pool_boundary(kube, config, completions=pool_completions)
    for (namespace, secret), required in sorted(secret_requirements(files, config).items()):
        # Return only names of populated keys, never secret values.
        observed = kube.run(
            "get",
            "secret",
            secret,
            "-n",
            namespace,
            "-o",
            r'go-template={{range $key,$value := .data}}{{if $value}}{{$key}}{{"\n"}}{{end}}{{end}}',
        )
        if not observed or not required <= set(observed.splitlines()):
            raise DeploymentError(f"required secret keys missing: {namespace}/{secret}")
    ns = config["namespace"]
    database = kube.get("statefulset", "loom-postgres", ns)
    pvc = kube.get("pvc", "data-loom-postgres-0", ns)
    if pvc and not database:
        raise DeploymentError(
            "existing database volume has no StatefulSet; use the explicit restore procedure"
        )
    migration = files["30-migrate.yaml"][0]
    migration_job = kube.get("job", migration["metadata"]["name"], ns)
    current = kube.get("configmap", "loom-platform-config", ns)
    same_candidate = False
    try:
        same_candidate = (
            json.loads(current["data"]["profile.json"])["candidate_sha"]
            == manifest["candidate_sha"]
            and json.loads(current["data"]["environment.json"]) == config
        )
    except (KeyError, ValueError, TypeError):
        pass
    backup_required = bool(database) and not (same_candidate and job_complete(migration_job))
    if backup_required and not kube.get("cronjob", "loom-platform-backup", ns):
        raise DeploymentError("upgrade requires an existing working backup CronJob before mutation")
    return {
        "database_exists": bool(database),
        "backup_required": backup_required,
        "migration_complete": job_complete(migration_job),
        "migration_failed": job_failed(migration_job),
    }


def _public_smoke_once(origin: str, environment: str) -> None:
    for path in ("/api/v1/health", "/loom-frontend-config.json"):
        with urllib.request.urlopen(origin + path, timeout=30) as response:
            if response.status != 200 or response.geturl() != origin + path:
                raise DeploymentError("public HTTPS smoke did not reach the intended endpoint")
            payload = json.loads(response.read(1024 * 1024))
            if not isinstance(payload, dict):
                raise DeploymentError("public HTTPS smoke did not return JSON")
            if path.endswith("config.json") and (
                payload.get("environment") != environment
                or payload.get("apiRouteBase") != origin + "/api"
            ):
                raise DeploymentError("public frontend points to a different environment")


def public_smoke(origin: str, environment: str) -> None:
    deadline = time.monotonic() + 180
    while True:
        try:
            _public_smoke_once(origin, environment)
            return
        except (OSError, ValueError):
            if time.monotonic() >= deadline:
                raise DeploymentError(
                    "public HTTPS API/frontend smoke failed after readiness interval"
                ) from None
            time.sleep(5)


def rollout_guard_observation_sql(*, owner: str, candidate: str) -> str:
    """Fixed read-only recovery query, also usable after the controller stops."""
    owner_literal = "'" + owner.replace("'", "''") + "'"
    candidate_literal = "'" + candidate.replace("'", "''") + "'"
    return ("BEGIN READ ONLY; SET LOCAL statement_timeout='10s'; "
            "SELECT json_build_object('status', COALESCE((SELECT CASE WHEN owner = "
            + owner_literal + " AND candidate_sha = " + candidate_literal
            + " THEN 'held' ELSE 'skipped_locked' END FROM public.nebius_rollout_guard WHERE id = 1), 'open')); ROLLBACK;")


def rollout_guard(kube: Kubectl, namespace: str, action: str, owner: str, candidate: str) -> dict[str, Any]:
    if action == "observe":
        # The service being repaired may not have a running Pod. Read the
        # durable database directly; observation never changes the guard.
        query = rollout_guard_observation_sql(owner=owner, candidate=candidate)
        output = kube.run("exec", "-n", namespace, "statefulset/loom-postgres", "--", "psql",
                          "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query)
    else:
        output = kube.run(
            "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python", "-m",
            "loom.nebius_rollout_guard", action, "--owner", owner, "--candidate", candidate,
        )
    result: dict[str, Any] = json.loads(output)
    if result.get("status") not in {"acquired", "released", "skipped_busy", "skipped_locked", "held", "open"}:
        raise DeploymentError("invalid rollout guard response")
    return result


def observe_rollout_guard(kube: Kubectl, namespace: str, owner: str, candidate: str) -> dict[str, Any]:
    """Read durable ownership after an ambiguous response without exposing errors."""
    try:
        result = rollout_guard(kube, namespace, "observe", owner, candidate)
        if result.get("status") in {"held", "open", "skipped_locked"}:
            return result
    except Exception:
        pass
    return {"status": "unavailable"}


def validate_target_replacement(
    current: dict[str, Any], config: dict[str, Any], retire_target: str | None,
) -> None:
    """Require an exact operator decision before replacing an immutable target."""
    data = current.get("data", {})
    previous = json.loads(data["environment.json"]) if "environment.json" in data else None
    # The catalog stores environment in each immutable execution target. Reject
    # relabeling before changing workloads, even without --retire-target, and
    # recheck under the rollout guard. Target retirement is not a data migration.
    if previous is not None and previous.get("environment") != config["environment"]:
        raise DeploymentError(
            "environment reclassification requires a separate migration of data and execution "
            "target identities; ordinary platform rollout cannot change it"
        )
    for guest_field in ("guest_execution_target", "emulated_auth_execution_target"):
        previous_guest = previous.get(guest_field) if previous else None
        proposed_guest = config.get(guest_field)
        if previous_guest is not None and previous_guest != proposed_guest:
            raise DeploymentError("installed guest target cannot be removed or renamed by platform rollout")
        if retire_target is not None and (previous_guest is not None or proposed_guest is not None):
            raise DeploymentError("primary replacement with a guest sibling requires a separate retirement protocol")
    if retire_target is None:
        if previous and previous["target_id"] != config["target_id"]:
            raise DeploymentError("changed primary target requires --retire-target naming the installed target")
        return
    if (not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", retire_target)
            or previous is None or previous["target_id"] != retire_target
            or retire_target == config["target_id"]):
        raise DeploymentError("target retirement must name the distinct installed primary target")
    if previous.get("regional_execution_targets") or config.get("regional_execution_targets"):
        raise DeploymentError("target retirement supports only a single-primary platform")
    for key in ("cluster_id", "namespace", "execution_namespace"):
        if previous[key] != config[key]:
            raise DeploymentError("target replacement cannot change its platform boundary")


# Execute stdlib-only code in the current control-plane Pod. The installed
# candidate need not already contain this operator-side deployment enhancement.
# Credentials remain in the Pod; redirects and environment proxies are disabled.
TARGET_RETIRE_PROGRAM = '''
import json
import os
import sys
import tomllib
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def target_action(action, previous_id, target_id, *, secret_file, origin):
    with Path(secret_file).open("rb") as stream:
        token = tomllib.load(stream)["admin"]["token"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    def request(method, path, body=None):
        req = urllib.request.Request(origin + path,
            data=json.dumps(body).encode() if body is not None else None, method=method,
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        with opener.open(req, timeout=30) as response:
            if response.status != 200:
                raise ValueError("target request rejected")
            return json.load(response)
    rows = request("GET", "/admin/execution-capacity/status")["targets"]
    states = {row["target_id"]: row["desired_state"] for row in rows}
    if len(states) != len(rows) or previous_id not in states:
        raise ValueError("target inventory mismatch")
    if action in {"validate", "retire"}:
        if target_id in states:
            raise ValueError("replacement target must be a fresh identity")
        if action == "retire":
            expected = {"target_id": previous_id, "desired_state": "retired",
                        "observed_state": "retired", "health_status": "unhealthy"}
            body = {key: value for key, value in expected.items() if key != "target_id"}
            body.update(observed_at=datetime.now(UTC).isoformat(), error_code="target_replaced")
            result = request("POST", "/admin/service-execution/targets/" + previous_id + "/health", body)
            if any(result.get(key) != value for key, value in expected.items()):
                raise ValueError("target retirement readback mismatch")
    elif action == "verify":
        if states.get(previous_id) != "retired" or states.get(target_id) != "active":
            raise ValueError("target replacement activation mismatch")
    else:
        raise ValueError("invalid target action")
    return {"previous_target_id": previous_id, "target_id": target_id, "status": action}

if __name__ == "__main__":
    try:
        print(json.dumps(target_action(sys.argv[1], sys.argv[2], sys.argv[3],
            secret_file=os.environ["LOOM_CP_ADMIN_SECRET_FILE"],
            origin="http://127.0.0.1:8080")))
    except Exception:
        print("Target replacement check failed", file=sys.stderr)
        sys.exit(1)
'''


def execution_target_action(
    kube: Kubectl, namespace: str, action: str, previous_id: str, target_id: str,
) -> None:
    expected = {"previous_target_id": previous_id, "target_id": target_id, "status": action}
    try:
        result = json.loads(kube.run(
            "exec", "-n", namespace, "deployment/loom-control-plane", "--", "python",
            "-c", TARGET_RETIRE_PROGRAM, action, previous_id, target_id,
        ))
    except Exception:
        raise DeploymentError("target replacement request failed; inspect the recorded deployment phase") from None
    if result != expected:
        raise DeploymentError("target replacement readback mismatch; inspect the recorded deployment phase")


def verify_deployed_images(kube: Kubectl, files: dict[str, list[dict[str, Any]]]) -> None:
    """Check live Deployment templates and readiness against the fixed candidate."""
    for filename in ("40-services.yaml", "60-execution.yaml"):
        for desired in files[filename]:
            if desired["kind"] != "Deployment":
                continue
            metadata = desired["metadata"]
            live = kube.get("deployment", metadata["name"], metadata["namespace"])
            expected = {c["name"]: c["image"] for c in desired["spec"]["template"]["spec"]["containers"]}
            observed = {c["name"]: c["image"] for c in live.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])}
            status = live.get("status", {})
            replicas = desired["spec"].get("replicas", 1)
            if (observed != expected or status.get("observedGeneration", 0) < live.get("metadata", {}).get("generation", 1)
                or status.get("updatedReplicas", 0) != replicas or status.get("availableReplicas", 0) != replicas
                or status.get("replicas", 0) != replicas):
                raise DeploymentError("candidate workload readback failed: " + metadata["name"])


def deploy(args: argparse.Namespace, *, kube: Kubectl | None = None) -> dict[str, Any]:
    snapshot = tempfile.TemporaryDirectory(prefix="loom-nebius-deploy-")
    snapshot_root = Path(snapshot.name)
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = args.evidence_dir / f"deployment-{uuid.uuid4().hex}.json"
    evidence: dict[str, Any] = {
        "schema_version": "loom.nebius-deployment.v1",
        "mode": "apply" if args.apply else "plan",
        "status": "running",
        "phases": [],
    }

    resume_owner = getattr(args, "resume_guard_owner", None)
    if resume_owner is not None and re.fullmatch(r"rollout-[0-9a-f]{32}", resume_owner) is None:
        raise DeploymentError("invalid recovery guard owner")
    guard_owner = resume_owner or getattr(args, "guard_owner", None) or "rollout-" + uuid.uuid4().hex
    if re.fullmatch(r"rollout-[0-9a-f]{32}", guard_owner) is None:
        raise DeploymentError("invalid rollout guard owner")
    if resume_owner:
        evidence["guard_owner"] = guard_owner
    guard_acquired = False
    mutation_started = False

    def phase(name: str) -> None:
        evidence["phases"].append({"name": name, "started_at": datetime.now(UTC).isoformat()})
        evidence_path.write_bytes(canonical(evidence))
        print(f"Nebius deployment: {name}", flush=True)

    try:
        phase("read-render")
        manifest, config, files = load_render(args.render_dir)
        for filename, rows in files.items():
            (snapshot_root / filename).write_bytes(
                yaml.safe_dump_all(rows, sort_keys=False).encode()
            )
        evidence.update(
            {
                key: manifest[key]
                for key in (
                    "candidate_sha",
                    "cluster_id",
                    "namespace",
                    "execution_namespace",
                )
            }
        )
        kube = kube or Kubectl(args.kubeconfig)
        phase("preflight")
        pool_completions: dict[str, dict[str, Any]] = {}
        state = preflight(kube, manifest, config, files, args.expected_cluster_id, pool_completions=pool_completions)
        evidence["preflight"] = state
        retire_target = getattr(args, "retire_target", None)
        current = kube.get("configmap", "loom-platform-config", config["namespace"])
        validate_target_replacement(current, config, retire_target)
        try:
            previous_candidate = json.loads(current["data"]["profile.json"])["candidate_sha"]
            if isinstance(previous_candidate, str) and re.fullmatch(r"[0-9a-f]{40}", previous_candidate):
                evidence["previous_candidate_sha"] = previous_candidate
        except (KeyError, ValueError, TypeError):
            pass
        # Freeze data, not the mutable Kubernetes object returned by a caller.
        replacement_source = canonical(current.get("data", {}))
        if retire_target is not None:
            if not state["database_exists"]:
                raise DeploymentError("target retirement requires an existing platform database")
            evidence["target_replacement"] = {
                "previous_target_id": retire_target, "target_id": config["target_id"],
                "previous_target_retired": False, "active_target_verified": False,
            }
        if not args.apply:
            evidence["status"] = "planned"
            return evidence
        if state["migration_failed"] and not args.retry_failed_jobs:
            raise DeploymentError(
                "candidate migration previously failed; fix the cause and explicitly retry failed jobs"
            )
        ns = config["namespace"]
        # Fresh installations have no running services. Every existing platform
        # upgrade uses the same guard, whether invoked locally or by Actions.
        if state["database_exists"]:
            evidence["guard_owner"] = guard_owner
            phase("resume-owned-guard" if resume_owner else "check-idle")
            guard = rollout_guard(kube, ns, "observe" if resume_owner else "acquire", guard_owner, manifest["candidate_sha"])
            evidence["guard"] = guard
            if resume_owner and guard["status"] != "held":
                raise DeploymentError("recovery requires the original candidate and held guard owner")
            if not resume_owner and guard["status"] != "acquired":
                evidence["status"] = guard["status"]
                return evidence
            guard_acquired = True
            mutation_started = bool(resume_owner)
            evidence["guard_owner"] = guard_owner
            phase("idle-reserved")
            # A same-candidate ingress cutover may have finished after preflight
            # but before we acquired the shared rollout guard.
            verify_ingress_mode(kube, config)
            verify_standalone_pool_boundary(kube, config, completions=pool_completions, allow_load=False)
            current = kube.get("configmap", "loom-platform-config", ns)
            validate_target_replacement(current, config, retire_target)
            if retire_target is not None and canonical(current.get("data", {})) != replacement_source:
                raise DeploymentError("target replacement source changed before guard acquisition")
            expected_current = getattr(args, "expected_current_candidate", None)
            if expected_current is not None:
                current = kube.get("configmap", "loom-platform-config", ns)
                if json.loads(current["data"]["profile.json"])["candidate_sha"] != expected_current:
                    if resume_owner:
                        raise DeploymentError("recovery candidate was superseded; original pause retained")
                    rollout_guard(kube, ns, "release", guard_owner, manifest["candidate_sha"])
                    guard_acquired = False
                    evidence["status"] = "skipped_superseded"
                    return evidence
            phase("check-schema-readiness")
            state.update(migration_readiness(kube, ns, getattr(args, "migration_schema_head", None)))
            if retire_target is not None:
                phase("validate-fresh-target")
                execution_target_action(kube, ns, "validate", retire_target, config["target_id"])

        def apply_file(filename: str) -> None:
            nonlocal mutation_started
            mutation_started = True
            phase(filename.removesuffix(".yaml"))
            kube.run("apply", "-f", str(snapshot_root / filename))

        def wait_job(name: str, seconds: int = 660) -> None:
            wait_for_job(kube, name, ns, seconds)

        def run_job(filename: str) -> None:
            name = files[filename][0]["metadata"]["name"]
            existing = kube.get("job", name, ns)
            if job_complete(existing):
                phase(filename.removesuffix(".yaml") + "-already-complete")
                return
            if job_failed(existing):
                if not args.retry_failed_jobs:
                    raise DeploymentError(
                        "candidate job previously failed; fix the cause and explicitly retry failed jobs"
                    )
                phase("retry-failed-job-" + name)
                kube.run("delete", "job", name, "-n", ns, "--cascade=foreground", "--wait=true")
            if not existing or job_failed(existing):
                apply_file(filename)
            wait_job(name)

        if state["backup_required"]:
            phase("pre-upgrade-backup")
            backup_name = (
                "loom-predeploy-" + manifest["candidate_sha"][:12] + "-" + uuid.uuid4().hex[:8]
            )
            evidence["backup_job"] = backup_name
            kube.run("create", "job", backup_name, "--from=cronjob/loom-platform-backup", "-n", ns)
            wait_job(backup_name, 1860)
        if retire_target is not None:
            # A lost response can follow a committed state change. From this
            # point every failure must retain the durable operator-owned pause.
            mutation_started = True
            phase("retire-previous-target")
            execution_target_action(kube, ns, "retire", retire_target, config["target_id"])
            evidence["target_replacement"]["previous_target_retired"] = True
        if config.get("task_identity_policy") is not None:
            mutation_started = True
            phase("install-and-verify-task-identity-policy")
            install_task_identity_policy(kube, config, snapshot_root)
        for filename in ("00-namespaces.yaml", "10-config-network.yaml", "20-database.yaml"):
            apply_file(filename)
        phase("database-ready")
        kube.run(
            "rollout",
            "status",
            "statefulset/loom-postgres",
            "-n",
            ns,
            "--timeout=600s",
            timeout=640,
        )
        apply_file("80-backup.yaml")
        run_job("30-migrate.yaml")
        apply_file("40-services.yaml")
        for name in ("loom-service", "loom-control-plane", "loom-llm-gateway", "loom-web"):
            kube.run(
                "rollout", "status", f"deployment/{name}", "-n", ns, "--timeout=300s", timeout=340
            )
        run_job("50-configure.yaml")
        apply_file("60-execution.yaml")
        for actuator in [
            row["metadata"]["name"] for row in files["60-execution.yaml"]
            if row["kind"] == "Deployment"
        ]:
            kube.run(
                "rollout",
                "status",
                "deployment/" + actuator,
                "-n",
                config["execution_namespace"],
                "--timeout=300s",
                timeout=340,
            )
        apply_file("70-public.yaml")
        phase("public-load-balancer-ready")
        kube.run(
            "wait",
            "--for=jsonpath={.status.loadBalancer.ingress}",
            "service/loom-web",
            "-n",
            ns,
            "--timeout=300s",
            timeout=340,
        )
        phase("public-https-smoke")
        public_smoke(manifest["public_origin"], config["environment"])
        phase("candidate-readback")
        verify_deployed_images(kube, files)
        if retire_target is not None:
            phase("target-replacement-readback")
            execution_target_action(kube, ns, "verify", retire_target, config["target_id"])
            evidence["target_replacement"]["active_target_verified"] = True
        if guard_acquired:
            rollout_guard(kube, ns, "release", guard_owner, manifest["candidate_sha"])
            guard_acquired = False
        evidence["status"] = "complete"
        return evidence
    except Exception as exc:
        # A command may commit in the database before its response is lost.
        # Resolve ownership from the database, including before acquire returns.
        observation = {"status": "unavailable" if "guard_owner" in evidence else "open"}
        if kube is not None and "state" in locals() and state["database_exists"] and "guard_owner" in evidence:
            observation = observe_rollout_guard(kube, config["namespace"], guard_owner, manifest["candidate_sha"])
            # Only this attempt's confirmed, pre-mutation reservation can be
            # released. An inherited recovery pause always stays with its owner.
            if observation["status"] == "held" and not mutation_started and not resume_owner:
                try:
                    rollout_guard(kube, config["namespace"], "release", guard_owner, manifest["candidate_sha"])
                except Exception:
                    pass
                observation = observe_rollout_guard(kube, config["namespace"], guard_owner, manifest["candidate_sha"])
        evidence["guard_observation"] = observation
        evidence["dispatch_paused"] = observation["status"] != "open"
        evidence["status"] = "failed"
        if kube is not None and "config" in locals():
            failures: list[dict[str, Any]] = []
            for namespace in (config["namespace"], config["execution_namespace"]):
                try:
                    pods = json.loads(kube.run("get", "pods", "-n", namespace, "-o", "json"))
                    for pod in pods.get("items", []):
                        status = pod.get("status", {})
                        for container in status.get("initContainerStatuses", []) + status.get(
                            "containerStatuses", []
                        ):
                            for state_name in ("waiting", "terminated"):
                                state = container.get("state", {}).get(state_name, {})
                                reason = state.get("reason", "")
                                if (
                                    re.fullmatch(r"[A-Za-z]{1,64}", reason)
                                    and reason != "Completed"
                                ):
                                    failure = {"namespace": namespace, "reason": reason}
                                    if isinstance(state.get("exitCode"), int):
                                        failure["exit_code"] = state["exitCode"]
                                    failures.append(failure)
                        for condition in status.get("conditions", []):
                            reason = condition.get("reason", "")
                            if condition.get("status") == "False" and re.fullmatch(
                                r"[A-Za-z]{1,64}", reason
                            ):
                                failures.append({"namespace": namespace, "reason": reason})
                except Exception:
                    failures.append({"namespace": namespace, "reason": "DiagnosticUnavailable"})
            evidence["workload_failures"] = failures

        evidence["error_type"] = type(exc).__name__
        if isinstance(exc, DeploymentError):
            evidence["reason"] = str(exc)
            if str(exc) in {
                "application_database_access_active", "application_database_schema_guard_not_installed",
                "application_database_readiness_unavailable",
            }:
                evidence["reason_code"] = str(exc)
        raise
    finally:
        evidence["finished_at"] = datetime.now(UTC).isoformat()
        evidence_path.write_bytes(canonical(evidence))
        snapshot.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("render-dir", "kubeconfig", "evidence-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--retry-failed-jobs", action="store_true")
    parser.add_argument("--retire-target", help="exact installed primary ID replaced by the reviewed render")
    args = parser.parse_args()
    try:
        result = deploy(args)
    except Exception as exc:
        print(
            f"Nebius deployment failed ({type(exc).__name__}); inspect sanitized phase evidence",
            file=sys.stderr,
        )
        return 1
    print("Nebius deployment " + result["status"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
