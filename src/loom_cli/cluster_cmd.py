"""`loom cluster` — Kubernetes operator surface.

Cluster mode targets an independently operated Kubernetes cluster
(cluster-deploy.md). The dev/demo path stays on `loom service`;
nothing here changes that.

The command group provides status, render, audit, preflight, lifecycle,
maintenance, and protected-release operations.

Lazy-imports the python `kubernetes` package so users who don't
need cluster mode aren't forced to install it. The CLI surfaces a
clear `pip install loom[cluster]` hint when the import fails.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import sys
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC
from importlib import resources
from pathlib import Path
from typing import Any, cast

from loom_cli.cluster_backup_guard import (
    DEFAULT_BACKUP_MAX_AGE_HOURS,
    PROTECTED_ENVIRONMENTS,
    BackupTraversalLimits,
    infer_environment,
    validate_backup_manifest,
    write_backup_manifest,
)
from loom_cli.cluster_config import (
    ClusterConfig,
    cluster_config_from_mapping,
    lifecycle_inventory_buckets,
    load_cluster_config,
)
from loom_cli.cluster_drift import compute_drift
from loom_cli.cluster_release_gate import (
    collect_release_gate_report,
    format_release_gate_json,
    format_release_gate_markdown,
    format_release_gate_table,
    query_live_alembic_heads,
)
from loom_cli.cluster_rollout_evidence import (
    build_docker_image_evidence,
    docker_image_inspect,
    load_docker_inspect_json,
    normalize_cluster_status_format,
    render_rollout_evidence_json,
)
from loom_cli.cluster_sandbox_deadline import (
    FAILURE_CLASS as SANDBOX_DEADLINE_FAILURE_CLASS,
)
from loom_cli.cluster_sandbox_deadline import (
    RECOVERY_KIND as SANDBOX_DEADLINE_RECOVERY_KIND,
)
from loom_cli.cluster_sandbox_deadline import (
    diagnostic_summaries,
    format_sandbox_deadline_note,
    pod_names_from_status,
    sandbox_deadline_diagnostics_for_deployment,
    status_has_sandbox_deadline_failures,
)
from loom_cli.cluster_workload_trust import (
    PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS,
    workload_contract_from_mapping,
    workload_contract_profile_from_file,
)
from loom_cli.file_checks import (
    require_real_file as _require_real_file,
)
from loom_cli.minio_storage_preflight import (
    DEFAULT_BUCKETS,
    DEFAULT_STOP_FREE_PERCENT,
    DEFAULT_WARN_FREE_PERCENT,
    DEFAULT_WARN_GROWTH_BYTES_PER_HOUR,
    MinioStorageThresholds,
    build_minio_storage_preflight,
    render_minio_storage_preflight_json,
    render_minio_storage_preflight_table,
)
from loom_cli.runtime_resources import load_bundled_schema, read_bundled_text
from loom_config.doctor import (
    DoctorReport,
)
from loom_config.doctor import (
    reconcile as _doctor_reconcile,
)
from loom_config.doctor import (
    reconcile_rendered as _doctor_reconcile_rendered,
)

# Repo root: cluster_cmd.py → loom_cli → src → loom (parents[2])
_REPO_ROOT = Path(__file__).resolve().parents[2]

# These are the Deployments cluster-deploy.md §Component map lists as
# the in-cluster surface. The status command renders one row per
# component plus stateful sets (postgres, minio) when running in the
# default `--storage in-cluster` mode.
#
# Each tuple is (logical name, k8s label selector value). Adjust if a
# deployment manifest renames itself; the selectors are intentionally
# decoupled from the python list so a misspelled selector fails
# loudly (component reads as not-found) rather than silently
# skipping.
_COMPONENT_DEPLOYMENTS: tuple[tuple[str, str], ...] = (
    ("loom-service", "loom-service"),
    ("loom-control-plane", "loom-control-plane"),
    ("loom-llm-gateway", "loom-llm-gateway"),
    ("loom-web", "loom-web"),
    # loom-worker is emitted as a Deployment on static-host-path
    # profiles and as a StatefulSet on dynamic-storage profiles.
    # `_collect_workload` probes both shapes so status covers
    # either without needing a config plumb-through.
    ("loom-worker", "loom-worker"),
)

# No component currently uses the generic DaemonSet status list. Dedicated
# router and sandbox DaemonSets are reported through their own checks.
_COMPONENT_DAEMONSETS: tuple[tuple[str, str], ...] = ()

_COMPONENT_STATEFULSETS: tuple[tuple[str, str], ...] = (
    ("postgres", "loom-postgres"),
    ("minio", "loom-minio"),
)

# Logical-name → human description for the status output. Falls
# through to the component name itself if unknown.
_COMPONENT_DESCRIPTIONS: dict[str, str] = {
    "loom-service": "Public REST API",
    "loom-control-plane": "Internal scheduler + worker control",
    "loom-llm-gateway": "Provider-connection facade",
    "loom-web": "SPA (paused by default)",
    "loom-worker": "Trial runner workload",
    "postgres": "Postgres (state)",
    "minio": "Object store (trajectories + ATIF)",
}




@dataclass(frozen=True, slots=True)
class _ClusterUpConfigSnapshot:
    path: Path | None
    config: ClusterConfig
    raw_config: dict[str, Any]
    workload_contract_profile: object
    protected_target: str | None


class _ClusterConfigSnapshotRenderError(ValueError):
    """Typed config construction failed before the immutable up snapshot."""


class _ClusterConfigSnapshotTomlError(ValueError):
    """The single cluster-config snapshot is not syntactically valid TOML."""


def _positive_backup_limit(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("backup traversal limits must be integers") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("backup traversal limits must be positive")
    return parsed


def _add_backup_traversal_limit_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backup-max-files",
        type=_positive_backup_limit,
        default=None,
        help=(
            "Explicit file traversal ceiling for backup validation. "
            "Omit to keep the conservative default."
        ),
    )
    parser.add_argument(
        "--backup-max-entries",
        type=_positive_backup_limit,
        default=None,
        help=(
            "Explicit combined file/directory traversal ceiling for backup "
            "validation. Omit to keep the conservative default."
        ),
    )
    parser.add_argument(
        "--backup-max-total-bytes",
        type=_positive_backup_limit,
        default=None,
        help=(
            "Explicit total byte traversal ceiling for backup validation. "
            "Omit to keep the conservative default."
        ),
    )


def _backup_traversal_limits_from_args(
    args: argparse.Namespace,
) -> BackupTraversalLimits | None:
    max_files = getattr(args, "backup_max_files", None)
    max_entries = getattr(args, "backup_max_entries", None)
    max_total_bytes = getattr(args, "backup_max_total_bytes", None)
    if max_files is None and max_entries is None and max_total_bytes is None:
        return None
    defaults = BackupTraversalLimits()
    return BackupTraversalLimits(
        max_files=max_files if max_files is not None else defaults.max_files,
        max_entries=max_entries if max_entries is not None else defaults.max_entries,
        max_total_bytes=(
            max_total_bytes if max_total_bytes is not None else defaults.max_total_bytes
        ),
    )








_PROTECTED_CLUSTER_NAMESPACES = {
    "loom-staging": "staging",
    "loom-prod": "production",
    "loom-production": "production",
}
_PROTECTED_CLUSTER_HOST_ROOTS = {
    "staging": Path("/data/loom-staging"),
    "production": Path("/data/loom-prod"),
}


def _protected_cluster_config_target(raw: dict[str, Any]) -> str | None:
    targets: set[str] = set()
    if "runtime_environment" in raw:
        runtime_environment = str(raw.get("runtime_environment") or "").strip()
        if runtime_environment in PROTECTED_ENVIRONMENTS:
            targets.add(runtime_environment)
    if "namespace" in raw:
        namespace = str(raw.get("namespace") or "").strip()
        namespace_target = _PROTECTED_CLUSTER_NAMESPACES.get(namespace)
        if namespace_target is not None:
            targets.add(namespace_target)
    if "persistent_storage_host_path_root" in raw:
        host_root_raw = str(raw.get("persistent_storage_host_path_root") or "").strip()
        host_root = Path(host_root_raw)
        if host_root_raw and host_root.is_absolute():
            try:
                resolved_root = host_root.resolve(strict=False)
                for environment, protected_root in _PROTECTED_CLUSTER_HOST_ROOTS.items():
                    if resolved_root.is_relative_to(protected_root.resolve(strict=False)):
                        targets.add(environment)
            except (OSError, RuntimeError) as exc:
                raise ValueError(
                    "persistent_storage_host_path_root could not be resolved safely"
                ) from exc
    if len(targets) > 1:
        raise ValueError(
            "conflicting protected cluster config targets: " + ", ".join(sorted(targets))
        )
    return next(iter(targets), None)


def _load_cluster_up_config_snapshot(path: Path | None) -> _ClusterUpConfigSnapshot:
    raw: dict[str, Any]
    if path is None:
        raw = {}
    else:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise _ClusterConfigSnapshotTomlError(str(exc)) from exc
    try:
        config = cluster_config_from_mapping(raw)
    except ValueError as exc:
        raise _ClusterConfigSnapshotRenderError(str(exc)) from exc
    return _ClusterUpConfigSnapshot(
        path=path,
        config=config,
        raw_config=raw,
        workload_contract_profile=raw.get("workload_contract"),
        protected_target=_protected_cluster_config_target(raw),
    )




@dataclass
class ComponentStatus:
    """One row in the status table."""

    name: str
    kind: str  # "Deployment" | "DaemonSet" | "StatefulSet"
    ready: int
    desired: int
    available: bool
    generation: int | None = None
    observed_generation: int | None = None
    updated: int | None = None
    # `total_replicas` counts every pod the Deployment controller knows about,
    # including old-template pods that haven't terminated yet. Used by the
    # #203 convergence check: if total > updated, the rollout still has old
    # pods around and isn't fully swapped.
    total_replicas: int | None = None
    pod_health_ok: bool = True
    last_restart_reason: str | None = None
    note: str | None = None
    failure_class: str | None = None
    runtime_recovery: str | None = None
    runtime_failure_diagnostics: list[dict[str, Any]] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        """A component is healthy when it has reached the desired
        replica count. A `desired=0` component (e.g., the default
        `loom-web` paused state, or `loom-worker` when an operator
        scales it to zero) is healthy by definition — the operator
        configured it that way.

        Before #128 caught it, this required `desired > 0` which
        meant `loom cluster up --wait` could never succeed against
        the default config (where `replicas.web = 0`). Available-
        replica check is dropped for desired=0 since available
        only makes sense when there's something to be available.
        """
        if not self.pod_health_ok:
            return False
        if not self.rollout_converged:
            return False
        if self.desired == 0:
            return self.ready == 0
        return self.available and self.ready == self.desired

    @property
    def rollout_converged(self) -> bool:
        if self.generation is not None and self.observed_generation is not None:
            if self.observed_generation < self.generation:
                return False
        if self.kind == "Deployment" and self.updated is None and self.desired > 0:
            return False
        if self.updated is not None and self.updated < self.desired:
            return False
        # #203: catch the "old pod still serving" state.
        # `.status.replicas` counts EVERY pod the Deployment controller owns —
        # both old-template pods (still running because their generation
        # observed_generation held or they haven't finished terminating) and
        # new-template pods (updated). If total > updated, the rollout has
        # not yet swapped: old pods are still around, they may still be
        # selected by the Service, and \`loom cluster up\` must not report
        # this as ready.
        if (
            self.kind == "Deployment"
            and self.updated is not None
            and self.total_replicas is not None
            and self.total_replicas > self.updated
        ):
            return False
        return True


@dataclass
class IngressEndpoint:
    """One ingress host + path → service mapping."""

    host: str
    paths: list[str] = field(default_factory=list)
    tls: bool = False


@dataclass(frozen=True)
class DeploymentImageCheck:
    deployment: str
    container: str
    expected_image: str
    live_image: str | None
    error: str | None = None

    @property
    def drifted(self) -> bool:
        return self.error is not None or self.live_image != self.expected_image

    def drift_message(self) -> str:
        if self.error is not None:
            return (
                f"Deployment {self.deployment} container {self.container} "
                f"image convergence check failed: {self.error}"
            )
        return (
            f"Deployment {self.deployment} container {self.container} "
            "image drift: "
            f"rendered {self.expected_image}, live {self.live_image or '<missing>'}"
        )

    def evidence_line(self) -> str:
        return (
            f"  - {self.deployment}/{self.container}: "
            f"rendered={self.expected_image} live={self.live_image or '<missing>'}"
        )


@dataclass
class ClusterStatus:
    namespace: str
    context: str | None
    components: list[ComponentStatus]
    ingresses: list[IngressEndpoint]
    warnings: list[str]
    blocking_warnings: list[str] = field(default_factory=list)

    @property
    def all_ready(self) -> bool:
        return (
            bool(self.components)
            and all(c.healthy for c in self.components)
            and not self.blocking_warnings
        )


def _format_table(status: ClusterStatus) -> str:
    """Stable human-readable table. Greppable + pipeable. Component
    state goes through stdout; warnings go to the same stream but
    after a blank line so they don't clobber the table."""
    lines: list[str] = []
    lines.append(
        f"namespace: {status.namespace}"
        + (f" (context: {status.context})" if status.context else ""),
    )
    lines.append("")
    lines.append(
        f"{'COMPONENT':<22} {'KIND':<14} {'READY':<8} {'STATUS':<10} DESCRIPTION",
    )
    for c in status.components:
        ready_str = f"{c.ready}/{c.desired}"
        state = "ready" if c.healthy else "not-ready"
        desc = _COMPONENT_DESCRIPTIONS.get(c.name, "")
        if c.note:
            desc = f"{desc} — {c.note}" if desc else c.note
        lines.append(
            f"{c.name:<22} {c.kind:<14} {ready_str:<8} {state:<10} {desc}",
        )
    if status.ingresses:
        lines.append("")
        lines.append("INGRESS")
        for ing in status.ingresses:
            scheme = "https" if ing.tls else "http"
            for p in ing.paths or ["/"]:
                lines.append(f"  {scheme}://{ing.host}{p}")
    if status.warnings:
        lines.append("")
        lines.append("WARNINGS")
        for w in status.warnings:
            lines.append(f"  - {w}")
    return "\n".join(lines) + "\n"


def _format_json(status: ClusterStatus) -> str:
    """Machine-readable variant for CI / scripting. Includes the same
    information as the table; structure stable across versions."""
    obj: dict[str, Any] = {
        "namespace": status.namespace,
        "context": status.context,
        "all_ready": status.all_ready,
        "components": [
            {
                "name": c.name,
                "kind": c.kind,
                "ready": c.ready,
                "desired": c.desired,
                "available": c.available,
                "healthy": c.healthy,
                "last_restart_reason": c.last_restart_reason,
                "note": c.note,
                "generation": c.generation,
                "observed_generation": c.observed_generation,
                "updated": c.updated,
                "pod_health_ok": c.pod_health_ok,
                "failure_class": c.failure_class,
                "runtime_recovery": c.runtime_recovery,
                "runtime_failure_diagnostics": c.runtime_failure_diagnostics,
            }
            for c in status.components
        ],
        "ingresses": [{"host": i.host, "paths": i.paths, "tls": i.tls} for i in status.ingresses],
        "warnings": status.warnings,
        "blocking_warnings": status.blocking_warnings,
    }
    return json.dumps(obj, indent=2) + "\n"


def _load_clients(
    context: str | None,
) -> tuple[object, object, object, object]:
    """Lazy import + load kube config. Returns (apps_v1, networking_v1,
    core_v1, storage_v1) API clients. Raises a RuntimeError with a
    friendly install hint if the kubernetes package isn't available;
    falls through to the kubernetes lib's own ConfigException on
    context errors.

    `storage_v1` is used by `loom cluster preflight` to check for a
    default StorageClass; `status` ignores it. Kept in the same
    helper so every cluster subcommand uses one auth path."""
    try:
        from kubernetes import client, config
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "the 'kubernetes' package is required for `loom cluster` "
            "commands. install it with `pip install loom[cluster]` "
            "(or `uv add 'loom[cluster]'`).",
        ) from exc

    # Try in-cluster first (when running inside a Job/Pod), then fall
    # back to kubeconfig. The order matters: an operator running
    # `loom cluster status` from a laptop won't have in-cluster creds.
    try:
        config.load_kube_config(context=context)
    except Exception:
        # Re-raised so the caller maps to exit code 2 with a helpful
        # message. `config.ConfigException` is the documented type but
        # we catch broadly because the kubernetes lib raises a variety
        # of exception types here depending on what's missing.
        config.load_incluster_config()
    return (
        client.AppsV1Api(),
        client.NetworkingV1Api(),
        client.CoreV1Api(),
        client.StorageV1Api(),
    )


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _deployment_convergence_note(
    *,
    generation: int | None,
    observed_generation: int | None,
    updated: int | None,
    desired: int,
    total_replicas: int | None = None,
) -> str | None:
    if generation is not None and observed_generation is not None:
        if observed_generation < generation:
            return f"stale-generation: observed {observed_generation} < generation {generation}"
    if updated is None and desired > 0:
        return f"updated-replicas: unknown/{desired}"
    if updated is not None and updated < desired:
        return f"updated-replicas: {updated}/{desired}"
    # #203: report old pods still around even after the updated count
    # matches desired.
    if updated is not None and total_replicas is not None and total_replicas > updated:
        return f"stale-pods: {total_replicas - updated} old"
    return None


def _combine_notes(*notes: str | None) -> str | None:
    present = [note for note in notes if note]
    if not present:
        return None
    return "; ".join(present)


_BLOCKING_POD_WAITING_REASONS = frozenset(
    {
        "CrashLoopBackOff",
        "CreateContainerConfigError",
        "CreateContainerError",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RunContainerError",
    }
)

_BLOCKING_POD_TERMINATED_REASONS = frozenset(
    {
        "Error",
        "OOMKilled",
        "StartError",
    }
)


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _labels(obj: Any) -> dict[str, str]:
    raw = _get_field(obj, "labels", {}) or {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _deployment_selector_labels(deployment: Any, *, k8s_name: str) -> dict[str, str]:
    spec = _get_field(deployment, "spec")
    selector = _get_field(spec, "selector")
    match_labels = _get_field(selector, "match_labels")
    if isinstance(match_labels, dict) and match_labels:
        return {str(key): str(value) for key, value in match_labels.items()}
    return {"app": k8s_name}


def _pod_matches_selector(pod: Any, selector: dict[str, str]) -> bool:
    pod_labels = _labels(_get_field(pod, "metadata"))
    return all(pod_labels.get(key) == value for key, value in selector.items())


def _container_images(pod_spec: Any) -> frozenset[str]:
    images: set[str] = set()
    for container in _get_field(pod_spec, "containers", []) or []:
        image = _get_field(container, "image")
        if image:
            images.add(str(image))
    return frozenset(images)


def _deployment_template_images(deployment: Any) -> frozenset[str]:
    template = _get_field(_get_field(deployment, "spec"), "template")
    return _container_images(_get_field(template, "spec"))


def _pod_template_matches_deployment(pod: Any, deployment_images: frozenset[str]) -> bool:
    if not deployment_images:
        return True
    pod_images = _container_images(_get_field(pod, "spec"))
    return deployment_images.issubset(pod_images)


def _pod_container_failure_reasons(pod: Any) -> list[str]:
    reasons: list[str] = []
    statuses = _get_field(_get_field(pod, "status"), "container_statuses", []) or []
    for container_status in statuses:
        state = _get_field(container_status, "state")
        waiting = _get_field(state, "waiting")
        waiting_reason = _get_field(waiting, "reason")
        if waiting_reason in _BLOCKING_POD_WAITING_REASONS:
            reasons.append(str(waiting_reason))
            continue
        terminated = _get_field(state, "terminated")
        terminated_reason = _get_field(terminated, "reason")
        if terminated_reason in _BLOCKING_POD_TERMINATED_REASONS:
            reasons.append(str(terminated_reason))
            continue
    phase = _get_field(_get_field(pod, "status"), "phase")
    if phase == "Failed":
        reasons.append("Failed")
    return sorted(set(reasons))


def _deployment_pod_health_note(
    deployment: Any,
    *,
    k8s_name: str,
    pods: list[Any],
) -> str | None:
    selector = _deployment_selector_labels(deployment, k8s_name=k8s_name)
    deployment_images = _deployment_template_images(deployment)
    failures: list[str] = []
    for pod in pods:
        if not _pod_matches_selector(pod, selector):
            continue
        if not _pod_template_matches_deployment(pod, deployment_images):
            continue
        reasons = _pod_container_failure_reasons(pod)
        if not reasons:
            continue
        pod_name = str(_get_field(_get_field(pod, "metadata"), "name", "<unknown>"))
        failures.append(f"{pod_name} {', '.join(reasons)}")
    if not failures:
        return None
    return f"pod-health: {'; '.join(sorted(failures))}"


def _effective_kube_context(context: str | None) -> str | None:
    if context:
        return context
    try:
        from kubernetes import config
    except ModuleNotFoundError:
        return None
    try:
        _contexts, active_context = config.list_kube_config_contexts()
    except Exception:
        return None
    if not isinstance(active_context, dict):
        return None
    name = active_context.get("name")
    return str(name) if name else None


def _read_optional_stateful_set(api: Any, name: str, namespace: str) -> Any | None:
    """Best-effort StatefulSet read. Returns the resource on success,
    None if the api client can't read StatefulSets or the name is not
    found. Used by `_collect_workload` to transparently cover the
    Deployment/StatefulSet dual shape for loom-worker (#673)."""
    reader = getattr(api, "read_namespaced_stateful_set", None)
    if not callable(reader):
        return None
    try:
        return reader(name=name, namespace=namespace)
    except Exception:
        return None


def _collect_workload(
    api: Any,
    namespace: str,
    deployments: tuple[tuple[str, str], ...],
    daemonsets: tuple[tuple[str, str], ...],
    statefulsets: tuple[tuple[str, str], ...],
    *,
    pods: list[Any] | None = None,
    events: list[Any] | None = None,
) -> list[ComponentStatus]:
    """Read each workload's ready/desired counts. A workload that
    isn't deployed at all surfaces as ready=0/desired=0 with a
    `not-found` note — operators expect to see every expected
    component, not just the ones that happen to exist."""
    out: list[ComponentStatus] = []
    apps = api  # AppsV1Api
    # Each tuple is (display_name, k8s_resource_name). For deployments
    # + daemonsets the two are identical; for statefulsets they
    # diverge ("postgres" → "loom-postgres"). The API call MUST use
    # the k8s name; the status row carries the display name.
    for display_name, k8s_name in deployments:
        try:
            d = apps.read_namespaced_deployment(name=k8s_name, namespace=namespace)
        except Exception as exc:
            # #673: loom-worker on dynamic-storage profiles is a
            # StatefulSet rather than a Deployment. If the Deployment
            # read 404s and a StatefulSet with the same name exists,
            # report the StatefulSet's status under the same display
            # name so wait_for_ready doesn't spuriously stall.
            if k8s_name == "loom-worker":
                sts = _read_optional_stateful_set(apps, k8s_name, namespace)
                if sts is not None:
                    stat = getattr(sts, "status", None)
                    spec = getattr(sts, "spec", None)
                    desired = int(getattr(spec, "replicas", 0) or 0)
                    ready = int(getattr(stat, "ready_replicas", 0) or 0)
                    out.append(
                        ComponentStatus(
                            name=display_name,
                            kind="StatefulSet",
                            ready=ready,
                            desired=desired,
                            available=ready > 0,
                            generation=_int_or_none(
                                getattr(getattr(sts, "metadata", None), "generation", None),
                            ),
                            observed_generation=_int_or_none(
                                getattr(stat, "observed_generation", None),
                            ),
                            updated=_int_or_none(getattr(stat, "updated_replicas", None)),
                        )
                    )
                    continue
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="Deployment",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
            continue
        try:
            spec = d.spec
            stat = d.status
            metadata = getattr(d, "metadata", None)
            desired = int(spec.replicas or 0)
            generation = _int_or_none(getattr(metadata, "generation", None))
            observed_generation = _int_or_none(getattr(stat, "observed_generation", None))
            updated = _int_or_none(getattr(stat, "updated_replicas", None))
            # #203: `.status.replicas` counts total pods including old
            # template pods that haven't terminated yet. Compare against
            # `updated_replicas` to detect the "old pod still around" case
            # that ready_replicas==desired alone can mask.
            total_replicas = _int_or_none(getattr(stat, "replicas", None))
            convergence_note = _deployment_convergence_note(
                generation=generation,
                observed_generation=observed_generation,
                updated=updated,
                desired=desired,
                total_replicas=total_replicas,
            )
            sandbox_diagnostics = sandbox_deadline_diagnostics_for_deployment(
                deployment=d,
                fallback_name=k8s_name,
                pods=pods or [],
                events=events or [],
            )
            sandbox_note = format_sandbox_deadline_note(sandbox_diagnostics)
            pod_health_note = (
                _deployment_pod_health_note(
                    d,
                    k8s_name=k8s_name,
                    pods=pods or [],
                )
                if desired > 0
                else None
            )
            pod_health_ok = pod_health_note is None and not sandbox_diagnostics
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="Deployment",
                    ready=int(stat.ready_replicas or 0),
                    desired=desired,
                    available=(stat.available_replicas or 0) > 0,
                    generation=generation,
                    observed_generation=observed_generation,
                    updated=updated,
                    total_replicas=total_replicas,
                    pod_health_ok=pod_health_ok,
                    note=_combine_notes(
                        convergence_note,
                        sandbox_note,
                        pod_health_note,
                    ),
                    failure_class=(SANDBOX_DEADLINE_FAILURE_CLASS if sandbox_diagnostics else None),
                    runtime_recovery=(
                        SANDBOX_DEADLINE_RECOVERY_KIND if sandbox_diagnostics else None
                    ),
                    runtime_failure_diagnostics=diagnostic_summaries(
                        sandbox_diagnostics,
                    ),
                )
            )
        except Exception as exc:  # ApiException 404 most commonly
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="Deployment",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
    for display_name, k8s_name in daemonsets:
        try:
            d = apps.read_namespaced_daemon_set(name=k8s_name, namespace=namespace)
            stat = d.status
            desired = int(stat.desired_number_scheduled or 0)
            ready = int(stat.number_ready or 0)
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="DaemonSet",
                    ready=ready,
                    desired=desired,
                    available=ready > 0,
                )
            )
        except Exception as exc:
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="DaemonSet",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
    for display_name, k8s_name in statefulsets:
        try:
            s = apps.read_namespaced_stateful_set(name=k8s_name, namespace=namespace)
            spec = s.spec
            stat = s.status
            ready = int(stat.ready_replicas or 0)
            desired = int(spec.replicas or 0)
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="StatefulSet",
                    ready=ready,
                    desired=desired,
                    available=ready > 0,
                )
            )
        except Exception as exc:
            out.append(
                ComponentStatus(
                    name=display_name,
                    kind="StatefulSet",
                    ready=0,
                    desired=0,
                    available=False,
                    note=_exception_to_note(exc),
                )
            )
    return out


def _exception_to_note(exc: Exception) -> str:
    """Map a k8s API exception to a short status note. 404 →
    `not-found` so operators see which components haven't been
    deployed; other errors get the exception's type name + a
    truncated message."""
    cls = type(exc).__name__
    if cls == "ApiException":
        status = getattr(exc, "status", None)
        if status == 404:
            return "not-found"
        return f"k8s {status}: {str(exc)[:80]}"
    return f"{cls}: {str(exc)[:80]}"


def _collect_ingresses(api: Any, namespace: str) -> list[IngressEndpoint]:
    """Snapshot the ingress hosts + paths. NetworkingV1Api.
    list_namespaced_ingress lists each ingress; rules.host carries
    the host. Empty list when no ingress controller is installed
    (silently absent in the output)."""
    try:
        result = api.list_namespaced_ingress(namespace=namespace)
    except Exception:
        return []
    out: list[IngressEndpoint] = []
    for ing in result.items:
        spec = ing.spec
        if spec is None:
            continue
        # TLS hostnames are in spec.tls; flag them so the URL hint
        # uses https://.
        tls_hosts: set[str] = set()
        for t in spec.tls or []:
            for h in t.hosts or []:
                tls_hosts.add(h)
        for rule in spec.rules or []:
            host = rule.host or ""
            paths: list[str] = []
            http = getattr(rule, "http", None)
            if http is not None:
                for p in http.paths or []:
                    if p.path:
                        paths.append(p.path)
            out.append(
                IngressEndpoint(
                    host=host,
                    paths=paths,
                    tls=(host in tls_hosts),
                )
            )
    return out


def collect_status(
    apps_v1: Any,
    networking_v1: Any,
    core_v1: Any,
    namespace: str,
    *,
    context: str | None,
) -> ClusterStatus:
    """Pure-collection function — the api clients are passed in so
    tests can inject fakes. Keeps the network-touching glue
    (`_load_clients`) out of the assertion surface."""
    namespace_pods, pod_list_error = _list_namespace_pods(core_v1, namespace)
    namespace_events, event_list_error = _list_namespace_events(core_v1, namespace)
    components = _collect_workload(
        apps_v1,
        namespace,
        _COMPONENT_DEPLOYMENTS,
        _COMPONENT_DAEMONSETS,
        _COMPONENT_STATEFULSETS,
        pods=namespace_pods,
        events=namespace_events,
    )
    ingresses = _collect_ingresses(networking_v1, namespace)
    warnings: list[str] = []
    blocking_warnings: list[str] = _collect_kube_system_rollout_blockers(core_v1)
    if pod_list_error is not None:
        blocking_warnings.append(pod_list_error)
    if event_list_error is not None:
        warnings.append(event_list_error)
    warnings.extend(blocking_warnings)
    # Surface common missing-secret cases without poking each
    # component's pod-level events (that's preflight's job; this is
    # status). list_namespaced_secret with a name filter is cheap.
    if not _secret_present(core_v1, namespace, "loom-secrets"):
        warnings.append(
            "Secret 'loom-secrets' is missing — components that read "
            "from it (postgres URL, minio creds, JWT keys) will fail "
            "to start. See cluster-deploy.md §Bootstrap or run "
            "`loom cluster preflight`.",
        )
    return ClusterStatus(
        namespace=namespace,
        context=context,
        components=components,
        ingresses=ingresses,
        warnings=warnings,
        blocking_warnings=blocking_warnings,
    )


def _list_namespace_pods(core_v1: Any, namespace: str) -> tuple[list[Any], str | None]:
    try:
        return list(core_v1.list_namespaced_pod(namespace=namespace).items), None
    except Exception as exc:
        return [], (
            f"cannot inspect managed pods in namespace {namespace}: {_exception_to_note(exc)}"
        )


def _list_namespace_events(core_v1: Any, namespace: str) -> tuple[list[Any], str | None]:
    try:
        return list(core_v1.list_namespaced_event(namespace=namespace).items), None
    except AttributeError:
        return [], None
    except Exception as exc:
        return [], (
            f"cannot inspect pod events in namespace {namespace}: {_exception_to_note(exc)}"
        )


def _secret_present(api: Any, namespace: str, name: str) -> bool:
    """Existence check via list+filter (cheaper than read+catch when
    the secret is gone) — only ever called once per command, so the
    cost is negligible either way."""
    try:
        api.read_namespaced_secret(name=name, namespace=namespace)
        return True
    except Exception:
        return False


_ROLLOUT_CONTROL_PLANE_POD_PREFIXES: tuple[str, ...] = (
    "kube-apiserver",
    "kube-controller-manager",
    "kube-scheduler",
    "etcd",
)


def _collect_kube_system_rollout_blockers(core_v1: Any) -> list[str]:
    try:
        pods = core_v1.list_namespaced_pod(namespace="kube-system").items
    except Exception:
        return []
    blockers: list[str] = []
    for pod in pods:
        name = str(getattr(getattr(pod, "metadata", None), "name", ""))
        if not name.startswith(_ROLLOUT_CONTROL_PLANE_POD_PREFIXES):
            continue
        if _pod_ready(pod):
            continue
        reason = _pod_unready_reason(pod)
        blockers.append(f"kube-system pod {name} is not Ready ({reason})")
    return blockers


def _pod_ready(pod: Any) -> bool:
    for condition in getattr(getattr(pod, "status", None), "conditions", None) or []:
        if getattr(condition, "type", None) == "Ready":
            return str(getattr(condition, "status", "")).lower() == "true"
    return False


def _pod_unready_reason(pod: Any) -> str:
    reasons: list[str] = []
    statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
    for container_status in statuses:
        state = getattr(container_status, "state", None)
        waiting = getattr(state, "waiting", None)
        reason = getattr(waiting, "reason", None)
        if reason:
            reasons.append(str(reason))
    if reasons:
        return ", ".join(sorted(set(reasons)))
    for condition in getattr(getattr(pod, "status", None), "conditions", None) or []:
        if getattr(condition, "type", None) == "Ready":
            return f"Ready={getattr(condition, 'status', 'unknown')}"
    return "Ready condition missing"


# ──────────────────────────────────────────────────────────────────────
# render (#76 Phase 1B)
# ──────────────────────────────────────────────────────────────────────


# Order matters: Postgres + MinIO come before the workloads that
# depend on them so `kubectl apply -f - < render` brings the cluster
# up in a topologically sound order. Ingress is last so it doesn't
# accept traffic until the backing Services exist.
_TEMPLATE_ORDER: tuple[str, ...] = (
    "persistent-storage.yaml.j2",
    "postgres.yaml.j2",
    # Multi-node HA shape (#637): CNPG Cluster CR. Renders only when
    # topology.multi_node = true; otherwise expands to empty output
    # and single-node postgres.yaml.j2 above owns the Postgres shape.
    "postgres-cnpg.yaml.j2",
    "pgbouncer.yaml.j2",
    "minio.yaml.j2",
    # Multi-node MinIO distributed shape (#610). Same conditional
    # rendering as postgres-cnpg above.
    "minio-distributed.yaml.j2",
    "control-plane.yaml.j2",
    "staging-data-lifecycle.yaml.j2",
    # #672 family-runs: orchestrator sibling. Ships with the CP env
    # block (same LOOM_CP_* prefix) so the deployment is trivially
    # co-configured.
    "family-orchestrator.yaml.j2",
    # #1212 controller ships disabled (replicas=0); activation is a separate gate.
    "pipeline-orchestrator.yaml.j2",
    "loom-service.yaml.j2",
    "llm-gateway.yaml.j2",
    # HPA for loom-llm-gateway (#547). Renders only when
    # gateway_hpa.enabled=true in the profile — off by default.
    "gateway-hpa.yaml.j2",
    "worker.yaml.j2",
    "web.yaml.j2",
    "ingress.yaml.j2",
    # gateway-router DaemonSet — per-node TCP proxy giving sandbox
    # Docker containers a stable hostPort:30443 endpoint to dial the
    # in-cluster gateway through. Carries its own NetworkPolicy.
    "gateway-router.yaml.j2",
    # llm-gateway-sandbox DaemonSet (#547 item #3) — TLS-terminating
    # HTTP CONNECT proxy on hostPort 8443 that verifies the step-JWT
    # before tunneling to the in-cluster gateway. Renders only when
    # llm_gateway_sandbox.enabled=true in the profile. Sits in front
    # of gateway-router for authenticated sandbox traffic. Off by
    # default; requires operator-provisioned loom-sandbox-gateway-tls
    # Secret.
    "llm-gateway-sandbox.yaml.j2",
    # Egress xDS control plane + Envoy proxy (#190 Phase C). The
    # xds-server reads provider_connections from Postgres + serves
    # CDS+RDS; Envoy fetches the dynamic config + acts as a forward
    # proxy for outbound provider traffic. Default replicas = 0 in
    # the schema; operators scale up when enabling sandbox-isolated
    # egress. NOT yet consumed by gateway-router (PR-C2 wires that).
    "egress-xds.yaml.j2",
    "egress-proxy.yaml.j2",
    # NetworkPolicies last — they reference workloads via podSelectors,
    # and listing them after the workloads keeps the rendered output
    # naturally ordered for human review (workload, then its policy).
    "network-policies.yaml.j2",
    "nebius-canonical-access.yaml.j2",
    "nebius-private-entry.yaml.j2",
    # Grafana dashboards ConfigMap — auto-discovered by the
    # kube-prometheus-stack sidecar (grafana_dashboard: "1" label).
    # Listed last so removal doesn't break the core apply ordering.
    "grafana-dashboards.yaml.j2",
)


_PERSISTENT_STORAGE_DYNAMIC = "dynamic"
_PERSISTENT_STORAGE_STATIC_HOST_PATH = "static-host-path"
_PERSISTENT_STORAGE_BACKENDS = frozenset(
    {
        _PERSISTENT_STORAGE_DYNAMIC,
        _PERSISTENT_STORAGE_STATIC_HOST_PATH,
    }
)
_FRONTEND_ENVIRONMENTS = frozenset(
    {
        "local",
        "development",
        "staging",
        "production",
    }
)
_RUNTIME_ENVIRONMENTS = frozenset(
    {
        "local",
        "development",
        "staging",
        "production",
    }
)

_KUBERNETES_NAMESPACE_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_CLUSTER_SCOPED_RENDER_KINDS = frozenset({"PersistentVolume"})
_NAMESPACED_RENDER_KINDS = frozenset(
    {
        "Cluster",  # CloudNativePG custom resource
        "ConfigMap",
        "CronJob",
        "DaemonSet",
        "Deployment",
        "HorizontalPodAutoscaler",
        "Ingress",
        "NetworkPolicy",
        "PersistentVolumeClaim",
        "PodDisruptionBudget",
        "Service",
        "StatefulSet",
    }
)
_TARGET_FLAG_UNSET = object()


def _validate_kubernetes_namespace(namespace: str) -> str:
    if (
        not namespace
        or len(namespace) > 63
        or _KUBERNETES_NAMESPACE_RE.fullmatch(namespace) is None
    ):
        raise ValueError(
            "namespace must be a valid Kubernetes DNS label: 1-63 lowercase "
            "alphanumeric or '-' characters, starting and ending with alphanumeric"
        )
    return namespace


def _resolve_config_target(
    args: argparse.Namespace,
    *,
    defer_toml_errors: bool = False,
) -> None:
    """Resolve one command target from its config and explicit flags.

    A supplied cluster config is authoritative for namespace and runtime
    environment. Explicit flags remain useful as safety assertions, but may
    not override that target. Commands without a config retain the historical
    ``loom`` namespace default and namespace-based environment inference.
    """

    cfg_path = Path(args.config).resolve() if getattr(args, "config", None) else None
    raw_config: dict[str, Any] | None = None
    if cfg_path is not None:
        if not cfg_path.exists():
            raise FileNotFoundError(f"cluster config not found: {cfg_path}")
        try:
            raw_config = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError:
            if not defer_toml_errors:
                raise
            # Preflight/up/down already have command-specific config error
            # handling (including protected workload-contract diagnostics).
            # Preserve that richer path instead of replacing it here.
            raw_config = None
        if raw_config is not None and not defer_toml_errors:
            # ``status`` has no later render/preflight config load, so keep it
            # from accepting unknown or malformed schema fields merely because
            # the target keys themselves parsed.
            load_cluster_config(cfg_path)

    _apply_config_target_fields(args, raw_config)


def _apply_config_target_fields(
    args: argparse.Namespace,
    raw_config: dict[str, Any] | None,
    *,
    explicit_namespace: str | None | object = _TARGET_FLAG_UNSET,
    explicit_environment: str | None | object = _TARGET_FLAG_UNSET,
) -> None:
    """Apply already-read target fields without taking a second snapshot."""

    if explicit_namespace is _TARGET_FLAG_UNSET:
        explicit_namespace = getattr(args, "namespace", None)
    assert explicit_namespace is None or isinstance(explicit_namespace, str)
    config_namespace = raw_config.get("namespace") if raw_config is not None else None
    if config_namespace is not None:
        if not isinstance(config_namespace, str):
            raise ValueError("cluster config namespace must be a string")
        if config_namespace != config_namespace.strip():
            raise ValueError("cluster config namespace must not contain surrounding whitespace")
        _validate_kubernetes_namespace(config_namespace)
        if explicit_namespace is not None and explicit_namespace != config_namespace:
            raise ValueError(
                f"--namespace {explicit_namespace!r} conflicts with cluster config "
                f"namespace {config_namespace!r}"
            )
        namespace = config_namespace
    else:
        namespace = explicit_namespace or "loom"
        _validate_kubernetes_namespace(namespace)
    args.namespace = namespace

    if hasattr(args, "environment") and raw_config is not None:
        config_environment = raw_config.get("runtime_environment")
        if config_environment is None:
            return
        if not isinstance(config_environment, str):
            raise ValueError("cluster config runtime_environment must be a string")
        if config_environment != config_environment.strip() or not config_environment:
            raise ValueError(
                "cluster config runtime_environment must be non-empty and must not "
                "contain surrounding whitespace"
            )
        if explicit_environment is _TARGET_FLAG_UNSET:
            explicit_environment = args.environment
        assert explicit_environment is None or isinstance(explicit_environment, str)
        if explicit_environment is not None and explicit_environment != config_environment:
            raise ValueError(
                f"--environment {explicit_environment!r} conflicts with cluster config "
                f"runtime_environment {config_environment!r}"
            )
        args.environment = config_environment
        # A protected namespace remains authoritative even when the config is
        # internally inconsistent (for example loom-staging + production).
        infer_environment(
            environment=config_environment,
            namespace=namespace,
        )


def _add_rendered_namespace_metadata(yaml_text: str, *, namespace: str) -> str:
    """Embed the configured namespace in every namespaced rendered object.

    Templates intentionally remain readable source YAML. This final render
    guard parses every document, preserves its text/comments, and inserts the
    namespace only when it is absent. Future cluster-scoped template kinds
    must be added to ``_CLUSTER_SCOPED_RENDER_KINDS`` explicitly, keeping this
    safety boundary fail-closed for manual ``kubectl apply -f`` workflows.
    """

    import yaml  # type: ignore[import-untyped]

    chunks = yaml_text.split("\n---\n")
    namespaced_chunks: list[str] = []
    for chunk in chunks:
        document = yaml.safe_load(chunk)
        if not isinstance(document, dict):
            namespaced_chunks.append(chunk)
            continue
        kind = str(document.get("kind") or "")
        metadata = document.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError(f"rendered {kind or '<unknown>'} object is missing metadata")
        rendered_namespace = metadata.get("namespace")
        if rendered_namespace is not None:
            if rendered_namespace != namespace:
                raise ValueError(
                    f"rendered {kind} object namespace {rendered_namespace!r} "
                    f"does not match cluster config namespace {namespace!r}"
                )
            namespaced_chunks.append(chunk)
            continue
        if kind in _CLUSTER_SCOPED_RENDER_KINDS:
            namespaced_chunks.append(chunk)
            continue
        if kind not in _NAMESPACED_RENDER_KINDS:
            raise ValueError(
                f"rendered {kind or '<unknown>'} object has unknown namespace scope; "
                "classify the kind before adding it to cluster render"
            )
        updated, substitutions = re.subn(
            r"(?m)^metadata:\n",
            f"metadata:\n  namespace: {namespace}\n",
            chunk,
            count=1,
        )
        if substitutions != 1:
            raise ValueError(
                f"rendered {kind or '<unknown>'} object has no top-level metadata block"
            )
        namespaced_chunks.append(updated)
    return "\n---\n".join(namespaced_chunks)


def _normalise_static_host_path_root(config: ClusterConfig) -> str | None:
    backend = config.persistent_storage_backend
    if backend not in _PERSISTENT_STORAGE_BACKENDS:
        raise ValueError(
            "persistent_storage_backend must be one of "
            f"{sorted(_PERSISTENT_STORAGE_BACKENDS)!r}; got {backend!r}"
        )
    if backend == _PERSISTENT_STORAGE_DYNAMIC:
        return None
    root = config.persistent_storage_host_path_root.strip().rstrip("/")
    if not root:
        raise ValueError(
            "persistent_storage_host_path_root is required when "
            "persistent_storage_backend = 'static-host-path'"
        )
    if not root.startswith("/") or root == "/":
        raise ValueError(
            "persistent_storage_host_path_root must be an absolute host "
            "path below an operator-managed data directory, for example "
            "/data/loom-staging"
        )
    return root


def _join_host_path(root: str, child: str) -> str:
    return f"{root.rstrip('/')}/{child}"


def _persistent_storage_context(config: ClusterConfig) -> dict[str, Any]:
    root = _normalise_static_host_path_root(config)
    if root is None:
        return {
            "static_host_path_storage": False,
            "postgres_pv_name": "",
            "minio_pv_name": "",
            "worker_trajectories_pv_name": "",
            "persistent_volumes": [],
        }

    namespace = config.namespace
    postgres_pv_name = f"{namespace}-postgres-data"
    minio_pv_name = f"{namespace}-minio-data"
    worker_pv_name = f"{namespace}-worker-trajectories-data"
    return {
        "static_host_path_storage": True,
        "postgres_pv_name": postgres_pv_name,
        "minio_pv_name": minio_pv_name,
        "worker_trajectories_pv_name": worker_pv_name,
        "persistent_volumes": [
            {
                "name": postgres_pv_name,
                "claim_name": "data-loom-postgres-0",
                "storage_gi": config.postgres_storage_gi,
                "host_path": _join_host_path(root, "postgres"),
            },
            {
                "name": minio_pv_name,
                "claim_name": "data-loom-minio-0",
                "storage_gi": config.minio_storage_gi,
                "host_path": _join_host_path(root, "minio"),
            },
            {
                "name": worker_pv_name,
                "claim_name": "loom-worker-trajectories",
                "storage_gi": config.worker_trajectory_storage_gi,
                "host_path": _join_host_path(root, "trajectories"),
            },
        ],
    }


def _normalise_frontend_path(raw: str, field_name: str) -> str:
    value = raw.strip().rstrip("/")
    if value in {"", "/"}:
        return ""
    if not value.startswith("/"):
        raise ValueError(f"{field_name} must be empty, /, /prod, /staging, or /dev")
    if value not in {"/prod", "/staging", "/dev"}:
        raise ValueError(f"{field_name} must be empty, /, /prod, /staging, or /dev")
    return value


def _frontend_route_context(config: ClusterConfig) -> dict[str, Any]:
    environment = config.frontend_environment.strip()
    if environment not in _FRONTEND_ENVIRONMENTS:
        raise ValueError(
            "frontend_environment must be one of "
            f"{sorted(_FRONTEND_ENVIRONMENTS)!r}; got {environment!r}",
        )
    label = config.frontend_environment_label.strip()
    if not label:
        raise ValueError("frontend_environment_label must not be empty")

    route_path = _normalise_frontend_path(
        config.frontend_route_path,
        "frontend_route_path",
    )
    api_base_path = _normalise_frontend_path(
        config.frontend_api_base_path or route_path,
        "frontend_api_base_path",
    )
    if api_base_path != route_path:
        raise ValueError(
            "frontend_api_base_path must match frontend_route_path for dev/staging/prod route isolation",
        )
    if environment == "production" and route_path != "/prod":
        raise ValueError("production frontend_environment must use frontend_route_path=/prod")
    if environment != "production" and route_path == "/prod":
        raise ValueError("non-production frontend_environment must not use /prod")
    if environment == "production" and "beta" in label.lower():
        raise ValueError("production frontend_environment_label must not contain beta")
    if route_path in {"/prod", "/staging", "/dev"} and config.ingress_host != "yylx.world":
        raise ValueError(
            f"frontend_route_path={route_path!r} must use ingress_host='yylx.world'",
        )

    # Optional route-transition marker: the predecessor route the live env still
    # serves while a route migration is in flight (e.g. "/dev" while moving to
    # "/staging"). Consumed only by the rollout preflight (baseline probes hit the
    # predecessor); the manifest templates render the target route_path. Must be a
    # protected route distinct from the target when present.
    route_path_from = _normalise_frontend_path(
        config.frontend_route_path_from,
        "frontend_route_path_from",
    )
    if route_path_from:
        if route_path_from == route_path:
            raise ValueError(
                "frontend_route_path_from must differ from frontend_route_path "
                "(leave it empty when no route migration is in flight)",
            )
        if config.ingress_host != "yylx.world":
            raise ValueError(
                f"frontend_route_path_from={route_path_from!r} must use ingress_host='yylx.world'",
            )

    return {
        "frontend_environment": environment,
        "frontend_environment_label": label,
        "frontend_route_path": route_path,
        "frontend_api_base_path": api_base_path,
        "frontend_public_origin": f"https://{config.ingress_host}",
        "frontend_prefixed_route": bool(route_path),
    }


def _runtime_environment_context(config: ClusterConfig) -> dict[str, str]:
    environment = config.runtime_environment.strip()
    if environment not in _RUNTIME_ENVIRONMENTS:
        raise ValueError(
            "runtime_environment must be one of "
            f"{sorted(_RUNTIME_ENVIRONMENTS)!r}; got {environment!r}",
        )
    return {"runtime_environment": environment}


def _ingress_redirect_hosts(config: ClusterConfig) -> tuple[str, ...]:
    try:
        ipaddress.ip_address(config.ingress_host)
    except ValueError:
        pass
    else:
        if config.ingress_redirect_hosts:
            raise ValueError("ingress_redirect_hosts cannot be used with an IP ingress_host")
        return ()

    canonical = config.ingress_host.strip()
    if not canonical:
        raise ValueError("ingress_host must not be empty")
    if canonical.startswith("www."):
        expected_redirect_host = canonical[4:]
    else:
        expected_redirect_host = f"www.{canonical}"

    hosts: list[str] = []
    seen: set[str] = set()
    for raw in config.ingress_redirect_hosts:
        host = raw.strip()
        if not host:
            raise ValueError("ingress_redirect_hosts entries must not be empty")
        if "/" in host or ":" in host:
            raise ValueError(f"ingress_redirect_hosts entries must be hostnames: {host!r}")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise ValueError(f"ingress_redirect_hosts entries must be hostnames: {host!r}")
        if host == canonical:
            raise ValueError("ingress_redirect_hosts must not include ingress_host")
        if host != expected_redirect_host:
            raise ValueError(
                "ingress_redirect_hosts currently supports only the www/non-www "
                f"counterpart of ingress_host: expected {expected_redirect_host!r}",
            )
        if host in seen:
            raise ValueError(f"duplicate ingress_redirect_hosts entry: {host!r}")
        seen.add(host)
        hosts.append(host)
    return tuple(hosts)


@dataclass(frozen=True)
class ProviderEgressRule:
    cidr: str
    port: int


_BLOCKED_PROVIDER_EGRESS_CIDRS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
)


def _parse_provider_egress_target(raw: str) -> ProviderEgressRule:
    target = raw.strip()
    if not target:
        raise ValueError("provider_egress_allowlist entries must not be empty")

    if target.startswith("["):
        host_end = target.find("]")
        if host_end == -1 or len(target) <= host_end + 2 or target[host_end + 1] != ":":
            raise ValueError(f"provider_egress_allowlist entry {raw!r} must be IP-or-CIDR:TCP-port")
        host = target[1:host_end]
        port_text = target[host_end + 2 :]
    else:
        host, sep, port_text = target.rpartition(":")
        if not sep or not host:
            raise ValueError(f"provider_egress_allowlist entry {raw!r} must be IP-or-CIDR:TCP-port")
        if ":" in host:
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} uses IPv6; wrap IPv6 "
                "CIDRs in brackets, for example [2001:db8::1/128]:8443"
            )

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} has invalid TCP port "
            f"{port_text!r}; expected 1-65535"
        ) from exc
    if port < 1 or port > 65535:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} has invalid TCP port {port}; expected 1-65535"
        )

    try:
        network = ipaddress.ip_network(host, strict=False)
    except ValueError as exc:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r}: target must be an "
            "IP address or CIDR; Kubernetes NetworkPolicy cannot enforce "
            "DNS hostnames, so resolve the provider host first"
        ) from exc

    if network.prefixlen == 0:
        raise ValueError(
            f"provider_egress_allowlist entry {raw!r} is too broad; "
            "approve a specific provider IP or CIDR instead"
        )
    for blocked in _BLOCKED_PROVIDER_EGRESS_CIDRS:
        if blocked.version != network.version:
            continue
        if network.overlaps(blocked):
            raise ValueError(
                f"provider_egress_allowlist entry {raw!r} overlaps reserved "
                f"range {blocked}; refusing to render provider egress policy"
            )

    return ProviderEgressRule(cidr=str(network), port=port)


def _provider_egress_rules(config: ClusterConfig) -> list[dict[str, int | str]]:
    rules = {_parse_provider_egress_target(target) for target in config.provider_egress_allowlist}
    return [
        {"cidr": rule.cidr, "port": rule.port}
        for rule in sorted(
            rules,
            key=lambda item: (
                ipaddress.ip_network(item.cidr, strict=False).version,
                int(ipaddress.ip_network(item.cidr, strict=False).network_address),
                ipaddress.ip_network(item.cidr, strict=False).prefixlen,
                item.port,
            ),
        )
    ]


def _reject_gateway_local_providers(config: ClusterConfig) -> None:
    """The gateway's `local/<name>/<model>` route is retired (#2054).

    The key stays accepted so existing configs with an empty list still
    load; a non-empty list would render routes nothing can call.
    """
    if config.gateway_local_providers:
        raise ValueError(
            "gateway_local_providers is retired (#2054): register the endpoint "
            "as an OpenAI-compatible Provider Connection and remove the entries"
        )


def render_manifests(config: ClusterConfig) -> str:
    """Render every template and join with `---` separators. Output is
    valid YAML that can be piped directly into `kubectl apply -f -`.

    Templates are loaded from `loom_cli.templates.k8s` via a Jinja2
    FileSystemLoader so that `{% import "_env.j2" as env_macros %}`
    directives in individual templates can resolve the shared macro.
    The package path is resolved once via `importlib.resources` so
    packaging (sdist + wheel) continues to pick up the templates
    without a separate MANIFEST.in entry.
    """
    try:
        from jinja2 import Environment, FileSystemLoader, StrictUndefined
    except ModuleNotFoundError as exc:
        # jinja2 is a core dep in pyproject.toml; this should never
        # fire in a correctly-installed environment but the error
        # message helps developers running a partial install.
        raise RuntimeError(
            "the 'jinja2' package is required for `loom cluster render`. "
            "if you installed Loom in development mode, run `uv sync` "
            "(or `pip install -e .`) to pick up dependencies.",
        ) from exc

    namespace = _validate_kubernetes_namespace(config.namespace)
    pkg_path = resources.files("loom_cli.templates.k8s")
    env = Environment(
        # FileSystemLoader is required so that `{% import "_env.j2" %}`
        # in per-service templates can resolve the shared macro file.
        loader=FileSystemLoader(str(pkg_path)),
        # StrictUndefined makes a missing variable error LOUDLY instead
        # of rendering an empty string. Better to fail at render time
        # than silently emit a manifest with `image: loom-service:`
        # (no tag) that operators chase for an hour.
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        # trim_blocks + lstrip_blocks remove the newline after block
        # tags and leading whitespace before them, which is required
        # for the `_env.j2` macro to emit clean YAML without spurious
        # blank lines. Verified against all existing templates — parsed
        # YAML is identical with or without these flags.
        trim_blocks=True,
        lstrip_blocks=True,
    )
    ctx = config.to_render_context()
    from loom_cli.nebius_canonical_render import nebius_canonical_context

    ctx.update(nebius_canonical_context(config))
    from loom_cli.nebius_private_entry import validate_private_entry

    validate_private_entry(config)
    ctx["nebius_private_entry_revision"] = hashlib.sha256(
        pkg_path.joinpath("nebius-private-entry.yaml.j2").read_bytes()
    ).hexdigest()
    ctx.update(_persistent_storage_context(config))
    ctx.update(_frontend_route_context(config))
    ctx.update(_runtime_environment_context(config))
    ctx["ingress_redirect_hosts"] = _ingress_redirect_hosts(config)
    ctx["provider_egress_rules"] = _provider_egress_rules(config)
    _reject_gateway_local_providers(config)
    ctx["lifecycle_inventory_buckets"] = lifecycle_inventory_buckets(config)
    try:
        ipaddress.ip_address(config.ingress_host)
        ctx["ingress_host_is_ip"] = True
    except ValueError:
        ctx["ingress_host_is_ip"] = False
    ctx["schema"] = load_bundled_schema()
    # Load package-owned dashboard JSON for the grafana-dashboards.yaml.j2
    # template. The canonical copies live under deploy/grafana/dashboards/;
    # byte-equality tests keep the wheel copies synchronized. Pre-serialised
    # strings let the Jinja2 `indent` filter embed them in the ConfigMap.
    ctx["operator_overview_json"] = read_bundled_text("grafana", "operator-overview.json")
    ctx["control_plane_json"] = read_bundled_text("grafana", "control-plane.json")
    ctx["llm_gateway_json"] = read_bundled_text("grafana", "llm-gateway.json")
    ctx["loom_service_json"] = read_bundled_text("grafana", "loom-service.json")
    ctx["worker_json"] = read_bundled_text("grafana", "worker.json")
    # Egress proxy bootstrap (#190 Phase C). Mounted as a ConfigMap so
    # operators can pin the Envoy config without a deploy/Dockerfile change.
    # The canonical deploy copy and package-owned wheel copy are byte-checked;
    # the template embeds it via `| indent` into the ConfigMap data.
    ctx["envoy_egress_bootstrap"] = read_bundled_text("envoy", "egress-proxy.yaml")
    chunks: list[str] = []
    for name in _TEMPLATE_ORDER:
        rendered = env.get_template(name).render(**ctx)
        if not rendered.strip():
            continue
        # Each rendered file is itself one-or-more YAML docs. Splice
        # with `---\n` between files. Files that already end with a
        # trailing newline merge cleanly; the join trims any
        # double-blank-line drift.
        chunks.append(rendered.rstrip() + "\n")
    return _add_rendered_namespace_metadata(
        "\n---\n".join(chunks),
        namespace=namespace,
    )


def _rendered_deployment_images(yaml_text: str) -> dict[str, dict[str, str]]:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "the 'PyYAML' package is required for deployment image drift checks.",
        ) from exc

    images: dict[str, dict[str, str]] = {}
    for doc in yaml.safe_load_all(yaml_text):
        # Deployments are the primary shape; the loom-worker workload
        # is emitted as a StatefulSet on dynamic-storage profiles
        # (#673) — treat it the same as far as image drift is
        # concerned so the rollout builder still sees every managed
        # loom-* image.
        if not isinstance(doc, dict) or doc.get("kind") not in ("Deployment", "StatefulSet"):
            continue
        metadata = doc.get("metadata")
        spec = doc.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            continue
        name = metadata.get("name")
        if not name:
            continue
        template = spec.get("template")
        if not isinstance(template, dict):
            continue
        pod_spec = template.get("spec")
        if not isinstance(pod_spec, dict):
            continue
        containers = pod_spec.get("containers")
        if not isinstance(containers, list):
            continue
        by_container: dict[str, str] = {}
        for container in containers:
            if not isinstance(container, dict):
                continue
            container_name = container.get("name")
            image = container.get("image")
            if container_name and image:
                by_container[str(container_name)] = str(image)
        if by_container:
            images[str(name)] = by_container
    return images


def rendered_image_checks(
    apps_v1: Any,
    namespace: str,
    rendered_manifests: str,
) -> list[DeploymentImageCheck]:
    checks: list[DeploymentImageCheck] = []
    for deployment_name, expected_images in _rendered_deployment_images(
        rendered_manifests,
    ).items():
        try:
            deployment = apps_v1.read_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
            )
        except Exception as exc:
            # #673: loom-worker on dynamic-storage profiles is a
            # StatefulSet, not a Deployment. Fall back to the
            # StatefulSet shape before reporting a drift error.
            sts = _read_optional_stateful_set(apps_v1, deployment_name, namespace)
            if sts is not None:
                deployment = sts
            else:
                for container_name, expected_image in expected_images.items():
                    checks.append(
                        DeploymentImageCheck(
                            deployment=deployment_name,
                            container=container_name,
                            expected_image=expected_image,
                            live_image=None,
                            error=_exception_to_note(exc),
                        ),
                    )
                continue
        pod_template = getattr(getattr(deployment, "spec", None), "template", None)
        pod_spec = getattr(pod_template, "spec", None)
        live_containers: dict[str, str] = {}
        for container in getattr(pod_spec, "containers", None) or []:
            container_name = container.name
            image = container.image
            if container_name and image:
                live_containers[str(container_name)] = str(image)
        for container_name, expected_image in expected_images.items():
            live_image = live_containers.get(container_name)
            checks.append(
                DeploymentImageCheck(
                    deployment=deployment_name,
                    container=container_name,
                    expected_image=expected_image,
                    live_image=live_image,
                )
            )
    return checks


def rendered_image_drifts(
    apps_v1: Any,
    namespace: str,
    rendered_manifests: str,
) -> list[str]:
    return [
        check.drift_message()
        for check in rendered_image_checks(apps_v1, namespace, rendered_manifests)
        if check.drifted
    ]


def _render(args: argparse.Namespace) -> int:
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    sys.stdout.write(manifests)
    return 0


def _kubectl_get_json(
    kind: str,
    namespace: str,
    name: str,
    *,
    context: str | None,
) -> dict[str, Any] | None:
    """Read one live object as JSON (read-only). Returns None if it doesn't exist."""
    import subprocess

    cmd = ["kubectl", "get", kind, name, "-o", "json"]
    if namespace:
        cmd.extend(["-n", namespace])
    if context:
        cmd.extend(["--context", context])
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        if "NotFound" in stderr or "not found" in stderr.lower():
            return None
        raise RuntimeError(f"kubectl get {kind}/{name} failed: {stderr[:200]}")
    return cast("dict[str, Any]", json.loads(proc.stdout))


def _read_live_objects(
    desired: list[dict[str, Any]],
    *,
    context: str | None,
    getter: Callable[..., dict[str, Any] | None] | None = None,
) -> list[dict[str, Any]]:
    """Read the live counterpart of each desired object (read-only); skip absent ones."""
    # Resolve the reader at call time (not as a default arg) so it stays patchable.
    resolve = getter if getter is not None else _kubectl_get_json
    live: list[dict[str, Any]] = []
    for obj in desired:
        kind = str(obj.get("kind", ""))
        metadata = obj.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        name = str(metadata.get("name", ""))
        namespace = str(metadata.get("namespace", ""))
        if not kind or not name:
            continue
        found = resolve(kind, namespace, name, context=context)
        if found is not None:
            live.append(found)
    return live


def _reconcile(args: argparse.Namespace) -> int:
    """Read-only shadow reconcile: render desired, read live, report drift as JSON."""
    import yaml

    if not getattr(args, "shadow", False):
        sys.stderr.write("error: only --shadow (read-only observe) is supported today\n")
        return 2
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    desired = [
        doc for doc in yaml.safe_load_all(manifests) if isinstance(doc, dict) and doc.get("kind")
    ]
    try:
        live = _read_live_objects(desired, context=args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    report = compute_drift(
        desired,
        live,
        environment=config.namespace,
        target=args.target or config.image_tag,
    )
    sys.stdout.write(json.dumps(report.to_dict(), indent=2) + "\n")
    return 0


def _release_manifest(args: argparse.Namespace) -> int:
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    try:
        from loom_cli.cluster_release_manifest import (
            build_release_manifest,
            render_release_manifest_json,
            write_release_manifest,
        )

        if args.expected_image_identities_json:
            expected_image_identities = json.loads(
                Path(args.expected_image_identities_json)
                .resolve()
                .read_text(
                    encoding="utf-8",
                )
            )
            if not isinstance(expected_image_identities, dict):
                raise ValueError("expected image identities JSON root must be an object")
        else:
            expected_image_identities = None

        manifest = build_release_manifest(
            config=config,
            config_path=cfg_path,
            rendered_manifests=manifests,
            environment=args.environment,
            image_tag=args.image_tag,
            git_sha=args.git_sha,
            generated_at=args.generated_at,
            expected_image_identities=expected_image_identities,
        )
        if args.output:
            write_release_manifest(Path(args.output), manifest)
        else:
            sys.stdout.write(render_release_manifest_json(manifest))
    except (OSError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: release manifest failed: {exc}\n")
        return 2
    return 0


def _rollout_evidence_docker_images(args: argparse.Namespace) -> int:
    try:
        if args.inspect_json:
            docs = load_docker_inspect_json(Path(args.inspect_json))
        else:
            inspect_targets = args.image or args.expect_repo_tag
            if not inspect_targets:
                raise ValueError(
                    "provide --image or --expect-repo-tag when --inspect-json is omitted"
                )
            docs = docker_image_inspect(inspect_targets)
        evidence = build_docker_image_evidence(
            docs,
            expected_repo_tags=args.expect_repo_tag,
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        evidence = {
            "schema_version": 1,
            "ok": False,
            "images": [],
            "expected_repo_tags": args.expect_repo_tag,
            "diagnostics": [
                {
                    "code": "docker_image_evidence_unavailable",
                    "message": str(exc),
                },
            ],
        }
    sys.stdout.write(render_rollout_evidence_json(evidence))
    return 0 if evidence["ok"] else 1


def _minio_storage_preflight(args: argparse.Namespace) -> int:
    previous_report: dict[str, Any] | None = None
    if args.previous_evidence:
        try:
            loaded = json.loads(Path(args.previous_evidence).resolve().read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError("previous storage preflight JSON root must be an object")
            previous_report = loaded
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            sys.stderr.write(f"error: previous storage preflight invalid: {exc}\n")
            return 2
    try:
        report = build_minio_storage_preflight(
            namespace=args.namespace,
            pod=args.pod,
            data_path=args.data_path,
            bucket_names=args.bucket or list(DEFAULT_BUCKETS),
            thresholds=MinioStorageThresholds(
                warn_free_percent=args.warn_free_percent,
                stop_free_percent=args.stop_free_percent,
                warn_growth_bytes_per_hour=args.warn_growth_bytes_per_hour,
            ),
            estimated_batch_bytes=args.estimated_batch_bytes,
            previous_report=previous_report,
        )
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: minio storage preflight failed: {exc}\n")
        return 2

    rendered = (
        render_minio_storage_preflight_json(report)
        if args.format == "json"
        else render_minio_storage_preflight_table(report)
    )
    if args.output:
        try:
            Path(args.output).resolve().write_text(
                render_minio_storage_preflight_json(report),
                encoding="utf-8",
            )
        except OSError as exc:
            sys.stderr.write(f"error: cannot write storage preflight evidence: {exc}\n")
            return 2
    sys.stdout.write(rendered)
    if report.get("outcome") == "stop" and not args.allow_storage_stop_override:
        return 1
    return 0




def _release_gate(args: argparse.Namespace) -> int:
    try:
        manifest_path = Path(args.manifest).resolve()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"error: release gate manifest invalid: {exc}\n")
        return 2

    try:
        apps_v1, _net_v1, core_v1, _storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    try:
        if args.config:
            config_bytes = Path(args.config).resolve().read_bytes()
            cluster_config_sha256 = hashlib.sha256(config_bytes).hexdigest()
        else:
            cluster_config_sha256 = manifest.get("cluster_config", {}).get("sha256")
        if args.rendered_manifest:
            rendered_manifest_text = (
                Path(args.rendered_manifest)
                .resolve()
                .read_text(
                    encoding="utf-8",
                )
            )
            rendered_manifest_sha256 = hashlib.sha256(
                rendered_manifest_text.encode("utf-8"),
            ).hexdigest()
        else:
            rendered_manifest_sha256 = manifest.get("rendered_manifest", {}).get("sha256")
    except OSError as exc:
        sys.stderr.write(f"error: release gate input invalid: {exc}\n")
        return 2

    minio_storage_preflight_artifact: dict[str, Any] | None = None
    minio_storage_preflight_path: str | None = None
    minio_storage_preflight_error: str | None = None
    if args.minio_storage_preflight:
        minio_path = Path(args.minio_storage_preflight).resolve()
        minio_storage_preflight_path = str(minio_path)
        try:
            loaded_minio_storage = json.loads(minio_path.read_text(encoding="utf-8"))
            if not isinstance(loaded_minio_storage, dict):
                raise ValueError("MinIO storage preflight JSON root must be an object")
            minio_storage_preflight_artifact = loaded_minio_storage
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            minio_storage_preflight_error = str(exc)

    if args.dry_run:
        live_alembic = None
        live_alembic_heads = list(manifest.get("alembic", {}).get("expected_heads", []) or [])
        database_target = "env:LOOM_CP_DB_URL"
        live_alembic_error = None
        live_alembic_evidence = {"mode": "dry-run"}
    else:
        live_alembic = query_live_alembic_heads(
            namespace=args.namespace,
            context=args.context,
        )
        live_alembic_heads = live_alembic.heads
        database_target = live_alembic.database_target
        live_alembic_error = live_alembic.error
        live_alembic_evidence = live_alembic.evidence

    report = collect_release_gate_report(
        manifest=manifest,
        apps_v1=apps_v1,
        core_v1=core_v1,
        namespace=args.namespace,
        rendered_manifest_sha256=(
            str(rendered_manifest_sha256) if rendered_manifest_sha256 else None
        ),
        cluster_config_sha256=(str(cluster_config_sha256) if cluster_config_sha256 else None),
        live_alembic_heads=live_alembic_heads,
        database_target=database_target,
        live_alembic_error=live_alembic_error,
        live_alembic_evidence=live_alembic_evidence,
        minio_storage_preflight_artifact=minio_storage_preflight_artifact,
        minio_storage_preflight_path=minio_storage_preflight_path,
        minio_storage_preflight_error=minio_storage_preflight_error,
    )

    if args.environment:
        manifest_environment = manifest.get("release", {}).get("environment")
        if manifest_environment != args.environment:
            from loom_cli.cluster_release_gate import ReleaseGateCheck, ReleaseGateReport

            report = ReleaseGateReport(
                environment=str(manifest_environment or ""),
                namespace=report.namespace,
                checks=[
                    *report.checks,
                    ReleaseGateCheck(
                        name="release-environment",
                        outcome="fail",
                        detail="release manifest environment does not match requested gate environment",
                        evidence={
                            "expected_environment": manifest_environment,
                            "requested_environment": args.environment,
                        },
                        remediation="use the release manifest for the target environment",
                    ),
                ],
            )

    if args.format == "json":
        sys.stdout.write(format_release_gate_json(report))
    elif args.format == "markdown":
        sys.stdout.write(format_release_gate_markdown(report))
    else:
        sys.stdout.write(format_release_gate_table(report))
    return 0 if report.all_pass else 1


def _audit(args: argparse.Namespace) -> int:
    """`loom cluster audit` — render manifests and check the
    public/internal boundary (#77). Renders without touching the
    cluster, so it's safe to run anywhere as a static check (CI
    pre-merge, Compose smoke, operator dry-run)."""
    from loom_cli.cluster_boundary import audit_boundary, format_violations

    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    try:
        manifests = render_manifests(config)
    except (RuntimeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    violations = audit_boundary(manifests)
    sys.stdout.write(format_violations(violations))
    return 0 if not violations else 1


def _status(args: argparse.Namespace) -> int:
    try:
        _resolve_config_target(args)
        environment, target_environment_failure = _resolve_target_environment(
            environment=args.environment,
            namespace=args.namespace,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: target config invalid: {exc}\n")
        return 2
    if target_environment_failure is not None:
        sys.stderr.write(
            "error: protected-target-environment conflict — refusing to inspect "
            "a different target than the cluster config.\n"
        )
        return 2
    assert environment is not None
    try:
        apps_v1, net_v1, core_v1, _storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        # Install-hint path.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        # Kubeconfig / context errors. Map to exit 2 (cluster
        # unreachable) per the documented exit-code contract.
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    try:
        status = collect_status(
            apps_v1,
            net_v1,
            core_v1,
            args.namespace,
            context=args.context,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: {type(exc).__name__}: {exc}\n",
        )
        return 2

    if args.format == "json":
        sys.stdout.write(_format_json(status))
    else:
        sys.stdout.write(_format_table(status))

    return 0 if status.all_ready else 1


def _rollout_evidence_cluster_status(args: argparse.Namespace) -> int:
    try:
        normalized_format, diagnostics = normalize_cluster_status_format(
            args.status_format,
        )
    except ValueError as exc:
        evidence = {
            "schema_version": 1,
            "ok": False,
            "diagnostics": [
                {
                    "code": "cluster_status_format_invalid",
                    "message": str(exc),
                    "requested_format": args.status_format,
                },
            ],
        }
        sys.stdout.write(render_rollout_evidence_json(evidence))
        return 2

    if diagnostics:
        sys.stderr.write(
            render_rollout_evidence_json(
                {
                    "schema_version": 1,
                    "ok": True,
                    "diagnostics": diagnostics,
                }
            )
        )
    try:
        apps_v1, net_v1, core_v1, _storage_v1 = _load_clients(args.context)
        status = collect_status(
            apps_v1,
            net_v1,
            core_v1,
            args.namespace,
            context=args.context,
        )
    except Exception as exc:
        evidence = {
            "schema_version": 1,
            "ok": False,
            "diagnostics": [
                {
                    "code": "cluster_status_evidence_unavailable",
                    "message": str(exc),
                },
            ],
        }
        sys.stdout.write(render_rollout_evidence_json(evidence))
        return 2

    if normalized_format == "json":
        sys.stdout.write(_format_json(status))
    else:
        sys.stdout.write(_format_table(status))

    return 0 if status.all_ready else 1


# ──────────────────────────────────────────────────────────────────────
# preflight (#76 Phase 2A — API-side read-only checks)
# ──────────────────────────────────────────────────────────────────────


# Secrets the cluster manifests (post-rendering) require. Phase 1B's
# control-plane.yaml + loom-service.yaml mount `loom-admin-secret`;
# every component reads from `loom-secrets`. Tracking these by name
# here keeps the preflight in sync with the template surface — a
# future PR that adds a new Secret reference must add it here too.
_REQUIRED_SECRETS: tuple[str, ...] = (
    "loom-secrets",
    "loom-admin-secret",
)

# PodSecurityStandard label keys (k8s 1.25+). `restricted` enforce
# blocks the worker's hostPath docker-socket mount (per the spec's
# spike #02). The preflight surfaces this as a warning, not a fail,
# because the operator may have decided to use a different driver
# (sysbox / kata) where restricted enforce is fine.
_PSS_ENFORCE_LABEL = "pod-security.kubernetes.io/enforce"

_PreflightOutcome = str  # "pass" | "fail" | "warn"


@dataclass
class PreflightCheck:
    """One check + its outcome. `detail` is a single line of human-
    readable context; `remediation` is an optional hint operators
    can act on (e.g. how to create the missing Secret)."""

    name: str
    outcome: _PreflightOutcome
    detail: str
    remediation: str | None = None


@dataclass
class PreflightReport:
    namespace: str
    context: str | None
    checks: list[PreflightCheck]

    @property
    def all_pass(self) -> bool:
        return all(c.outcome == "pass" for c in self.checks)

    @property
    def any_fail(self) -> bool:
        return any(c.outcome == "fail" for c in self.checks)


def _check_namespace_exists(
    core_v1: Any,
    namespace: str,
) -> PreflightCheck:
    """First check: does the namespace exist? Every subsequent check
    is namespace-scoped so this gates the rest."""
    try:
        core_v1.read_namespace(name=namespace)
    except Exception as exc:
        return PreflightCheck(
            name="namespace-exists",
            outcome="fail",
            detail=f"namespace {namespace!r} not found ({_exception_to_note(exc)})",
            remediation=(
                f"kubectl create namespace {namespace}\n# Then re-run `loom cluster preflight`."
            ),
        )
    return PreflightCheck(
        name="namespace-exists",
        outcome="pass",
        detail=f"namespace {namespace!r} present",
    )


def _check_required_secrets(
    core_v1: Any,
    namespace: str,
) -> list[PreflightCheck]:
    """One check per Secret listed in `_REQUIRED_SECRETS`. Missing
    Secret = fail with a `kubectl create secret` hint."""
    out: list[PreflightCheck] = []
    for name in _REQUIRED_SECRETS:
        if _secret_present(core_v1, namespace, name):
            out.append(
                PreflightCheck(
                    name=f"secret-{name}",
                    outcome="pass",
                    detail=f"Secret {name!r} present in {namespace}",
                )
            )
        else:
            out.append(
                PreflightCheck(
                    name=f"secret-{name}",
                    outcome="fail",
                    detail=f"Secret {name!r} missing in {namespace}",
                    remediation=(
                        f"# See cluster-deploy.md §Bootstrap for the keys "
                        f"{name!r} expects. Quick example:\n"
                        f"kubectl create secret generic {name} \\\n"
                        f"  --namespace={namespace} \\\n"
                        f"  --from-literal=<key>=<value>"
                    ),
                )
            )
    return out


def _check_ingress_class_installed(
    networking_v1: Any,
) -> PreflightCheck:
    """An IngressClass resource is required for the `loom-ingress`
    Ingress to actually route traffic. Most clusters use `nginx`
    (ingress-nginx) but the check is class-agnostic — we just need
    *something* registered."""
    try:
        result = networking_v1.list_ingress_class()
    except Exception as exc:
        return PreflightCheck(
            name="ingress-class-installed",
            outcome="fail",
            detail=(f"failed to list IngressClass resources: {_exception_to_note(exc)}"),
            remediation=(
                "Check kubectl + cluster networking access; see cluster-deploy.md §Prerequisites."
            ),
        )
    classes = [getattr(it.metadata, "name", "<unknown>") for it in (result.items or [])]
    if not classes:
        return PreflightCheck(
            name="ingress-class-installed",
            outcome="fail",
            detail="no IngressClass resources registered in the cluster",
            remediation=(
                "Install an ingress controller (e.g. ingress-nginx):\n"
                "  helm install ingress-nginx ingress-nginx "
                "--repo https://kubernetes.github.io/ingress-nginx"
            ),
        )
    return PreflightCheck(
        name="ingress-class-installed",
        outcome="pass",
        detail=f"IngressClass(es) present: {', '.join(classes)}",
    )


def _check_default_storage_class(
    storage_v1: Any,
) -> PreflightCheck:
    """At least one StorageClass marked default. Postgres + MinIO
    StatefulSets and the worker trajectories PVC bind against this."""
    try:
        result = storage_v1.list_storage_class()
    except Exception as exc:
        return PreflightCheck(
            name="default-storage-class",
            outcome="fail",
            detail=(f"failed to list StorageClass resources: {_exception_to_note(exc)}"),
        )
    items = result.items or []
    if not items:
        return PreflightCheck(
            name="default-storage-class",
            outcome="fail",
            detail="no StorageClass resources registered",
            remediation=(
                "Install a CSI driver appropriate for your cluster "
                "(e.g. local-path-provisioner:\n"
                "  kubectl apply -f "
                "https://raw.githubusercontent.com/rancher/"
                "local-path-provisioner/master/deploy/local-path-storage.yaml"
                ")"
            ),
        )
    default_names: list[str] = []
    for sc in items:
        anns = getattr(sc.metadata, "annotations", None) or {}
        # Both keys are used in the wild — the in-tree storage class
        # uses the unprefixed key, beta and modern CSI uses the
        # storage.k8s.io prefix.
        is_default = (
            anns.get("storageclass.kubernetes.io/is-default-class") == "true"
            or anns.get("storageclass.beta.kubernetes.io/is-default-class") == "true"
        )
        if is_default:
            default_names.append(getattr(sc.metadata, "name", "<unknown>"))
    if not default_names:
        return PreflightCheck(
            name="default-storage-class",
            outcome="warn",
            detail=(
                "StorageClass resources exist but none is marked "
                "default — PVCs without an explicit storageClassName "
                "will not bind"
            ),
            remediation=(
                "Pick one StorageClass and annotate it as default:\n"
                "  kubectl annotate sc <name> "
                "storageclass.kubernetes.io/is-default-class=true"
            ),
        )
    return PreflightCheck(
        name="default-storage-class",
        outcome="pass",
        detail=f"default StorageClass: {', '.join(default_names)}",
    )


def _storage_class_is_default(sc: Any) -> bool:
    anns = getattr(sc.metadata, "annotations", None) or {}
    return (
        anns.get("storageclass.kubernetes.io/is-default-class") == "true"
        or anns.get("storageclass.beta.kubernetes.io/is-default-class") == "true"
    )


_CRITICAL_STATE_PVCS: tuple[str, ...] = (
    "data-loom-postgres-0",
    "data-loom-minio-0",
    "loom-worker-trajectories",
)


def _object_metadata_name(obj: Any) -> str:
    return str(getattr(getattr(obj, "metadata", None), "name", "") or "")


def _pv_host_path(pv: Any) -> str:
    host_path = getattr(getattr(pv, "spec", None), "host_path", None)
    return str(getattr(host_path, "path", "") or "")


def _pv_local_path(pv: Any) -> str:
    local = getattr(getattr(pv, "spec", None), "local", None)
    return str(getattr(local, "path", "") or "")


def _pv_claim_ref(pv: Any) -> tuple[str, str]:
    claim_ref = getattr(getattr(pv, "spec", None), "claim_ref", None)
    return (
        str(getattr(claim_ref, "namespace", "") or ""),
        str(getattr(claim_ref, "name", "") or ""),
    )


def _storage_class_map(storage_classes: list[Any]) -> dict[str, Any]:
    return {_object_metadata_name(sc): sc for sc in storage_classes if _object_metadata_name(sc)}


def _storage_class_provisioner(
    *,
    storage_class_name: str,
    storage_classes_by_name: dict[str, Any],
) -> str:
    sc = storage_classes_by_name.get(storage_class_name)
    return str(getattr(sc, "provisioner", "") or "") if sc is not None else ""


def _check_existing_critical_pvc_storage(
    core_v1: Any,
    storage_classes: list[Any],
    *,
    namespace: str,
    environment: str,
    cluster_config: ClusterConfig | None,
) -> PreflightCheck | None:
    try:
        pvc_result = core_v1.list_namespaced_persistent_volume_claim(
            namespace=namespace,
        )
        pv_result = core_v1.list_persistent_volume()
    except Exception as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                "failed to list PersistentVolumeClaims/PersistentVolumes "
                f"for protected environment {environment!r}: {_exception_to_note(exc)}"
            ),
        )

    pvcs = {
        _object_metadata_name(pvc): pvc
        for pvc in (pvc_result.items or [])
        if _object_metadata_name(pvc)
    }
    critical_present = [name for name in _CRITICAL_STATE_PVCS if name in pvcs]
    if not critical_present:
        return None

    pvs = {
        _object_metadata_name(pv): pv for pv in (pv_result.items or []) if _object_metadata_name(pv)
    }
    storage_classes_by_name = _storage_class_map(storage_classes)
    problems: list[str] = []
    missing: list[str] = []
    ok_bindings: list[str] = []

    for pvc_name in _CRITICAL_STATE_PVCS:
        pvc = pvcs.get(pvc_name)
        if pvc is None:
            missing.append(pvc_name)
            continue
        problem_count = len(problems)
        pvc_spec = getattr(pvc, "spec", None)
        pvc_status = getattr(pvc, "status", None)
        phase = str(getattr(pvc_status, "phase", "") or "")
        volume_name = str(getattr(pvc_spec, "volume_name", "") or "")
        if phase and phase != "Bound":
            problems.append(f"{pvc_name} phase={phase}")
        if not volume_name:
            problems.append(f"{pvc_name} has no bound volumeName")
            continue
        pv = pvs.get(volume_name)
        if pv is None:
            problems.append(f"{pvc_name}->{volume_name} PV is missing")
            continue

        pv_spec = getattr(pv, "spec", None)
        reclaim_policy = str(getattr(pv_spec, "persistent_volume_reclaim_policy", "") or "")
        if reclaim_policy != "Retain":
            problems.append(
                f"{pvc_name}->{volume_name} reclaimPolicy={reclaim_policy or '<unset>'}"
            )

        claim_namespace, claim_name = _pv_claim_ref(pv)
        if claim_namespace and claim_namespace != namespace:
            problems.append(f"{pvc_name}->{volume_name} claimRef namespace={claim_namespace}")
        if claim_name and claim_name != pvc_name:
            problems.append(f"{pvc_name}->{volume_name} claimRef name={claim_name}")

        storage_class_name = str(
            getattr(pv_spec, "storage_class_name", "")
            or getattr(pvc_spec, "storage_class_name", "")
            or ""
        )
        provisioner = _storage_class_provisioner(
            storage_class_name=storage_class_name,
            storage_classes_by_name=storage_classes_by_name,
        )
        host_path = _pv_host_path(pv)
        local_path = _pv_local_path(pv)
        if "local-path" in provisioner:
            problems.append(f"{pvc_name}->{volume_name} provisioner={provisioner}")
        if local_path:
            if "local-path-provisioner" in local_path:
                problems.append(f"{pvc_name}->{volume_name} localPath={local_path}")
            else:
                problems.append(f"{pvc_name}->{volume_name} uses local volume path={local_path}")
        if host_path and not host_path.startswith("/data/"):
            problems.append(f"{pvc_name}->{volume_name} hostPath={host_path} is outside /data")
        if not host_path and not local_path and not storage_class_name:
            problems.append(
                f"{pvc_name}->{volume_name} has no hostPath, local volume, or StorageClass to audit"
            )
        if len(problems) == problem_count:
            ok_bindings.append(f"{pvc_name}->{volume_name}")

    if problems:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} critical PVCs are "
                "not on a durable Retain boundary: " + "; ".join(problems)
            ),
            remediation=(
                "Move Postgres, MinIO, and worker trajectories to external "
                "storage or explicit host-managed Retain PVs under /data "
                "before treating this environment as preproduction durable."
            ),
        )
    if missing:
        configured_check = _check_configured_static_host_path_storage(
            cluster_config=cluster_config,
            environment=environment,
        )
        if configured_check is not None and configured_check.outcome == "pass":
            return PreflightCheck(
                name="protected-storage-boundary",
                outcome="pass",
                detail=(
                    f"protected environment {environment!r} present critical PVCs "
                    "are bound to audited Retain PVs: "
                    + ", ".join(ok_bindings)
                    + "; missing critical PVCs will be created from "
                    "static-host-path config: " + ", ".join(missing)
                ),
            )
        if configured_check is not None:
            return configured_check
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} critical PVCs are "
                "not on a durable Retain boundary: "
                + "; ".join(f"{name} is missing" for name in missing)
            ),
            remediation=(
                "Move Postgres, MinIO, and worker trajectories to external "
                "storage or explicit host-managed Retain PVs under /data "
                "before treating this environment as preproduction durable."
            ),
        )
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} critical PVCs are bound "
            "to audited Retain PVs: " + ", ".join(ok_bindings)
        ),
    )


def _check_configured_static_host_path_storage(
    *,
    cluster_config: ClusterConfig | None,
    environment: str,
) -> PreflightCheck | None:
    if cluster_config is None:
        return None
    try:
        root = _normalise_static_host_path_root(cluster_config)
    except ValueError as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} has invalid "
                f"persistent storage config: {exc}"
            ),
        )
    if root is None:
        return None
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} render config declares "
            f"static-host-path Retain PVs under {root}; critical PVCs are "
            "not created yet"
        ),
    )


def _check_protected_storage_boundary(
    core_v1: Any,
    storage_v1: Any,
    *,
    environment: str,
    namespace: str,
    cluster_config: ClusterConfig | None = None,
) -> PreflightCheck:
    try:
        result = storage_v1.list_storage_class()
    except Exception as exc:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                "failed to list StorageClass resources for protected "
                f"environment {environment!r}: {_exception_to_note(exc)}"
            ),
        )
    storage_classes = list(result.items or [])
    existing_check = _check_existing_critical_pvc_storage(
        core_v1,
        storage_classes,
        namespace=namespace,
        environment=environment,
        cluster_config=cluster_config,
    )
    if existing_check is not None:
        return existing_check

    configured_check = _check_configured_static_host_path_storage(
        cluster_config=cluster_config,
        environment=environment,
    )
    if configured_check is not None:
        return configured_check

    default_classes = [sc for sc in storage_classes if _storage_class_is_default(sc)]
    if not default_classes:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(f"protected environment {environment!r} has no default StorageClass to audit"),
        )
    unsafe: list[str] = []
    for sc in default_classes:
        name = getattr(sc.metadata, "name", "<unknown>")
        provisioner = str(getattr(sc, "provisioner", "") or "")
        reclaim_policy = str(getattr(sc, "reclaim_policy", "") or "")
        reasons: list[str] = []
        if "local-path" in provisioner:
            reasons.append(f"provisioner={provisioner}")
        if reclaim_policy == "Delete":
            reasons.append("reclaimPolicy=Delete")
        if reasons:
            unsafe.append(f"{name} ({', '.join(reasons)})")
    if unsafe:
        return PreflightCheck(
            name="protected-storage-boundary",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} uses disposable "
                "storage boundary: " + "; ".join(unsafe)
            ),
            remediation=(
                "Move critical state to external Postgres/MinIO or explicit "
                "host-managed Retain volumes before treating this environment "
                "as preproduction durable."
            ),
        )
    return PreflightCheck(
        name="protected-storage-boundary",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} default StorageClass "
            "does not use local-path or Delete reclaim policy"
        ),
    )


def _check_backup_manifest(
    manifest_path: Path | None,
    *,
    environment: str,
    namespace: str,
    max_age_hours: int,
    limits: BackupTraversalLimits | None = None,
) -> PreflightCheck:
    problems = validate_backup_manifest(
        manifest_path,
        environment=environment,
        namespace=namespace,
        max_age_hours=max_age_hours,
        limits=limits,
    )
    if problems:
        return PreflightCheck(
            name="backup-manifest",
            outcome="fail",
            detail="; ".join(problems),
            remediation=(
                "Create a fresh metadata manifest with `loom cluster backup "
                "manifest ...` after dumping Postgres, mirroring MinIO, and "
                "backing up Kubernetes/runtime secrets."
            ),
        )
    assert manifest_path is not None
    return PreflightCheck(
        name="backup-manifest",
        outcome="pass",
        detail=f"recent backup manifest verified: {manifest_path}",
    )


def _check_workload_trust_contract(
    *,
    environment: str,
    workload_contract_profile: object,
) -> PreflightCheck:
    """Fail closed when a protected profile is not the v1 trust tuple."""
    try:
        contract = workload_contract_from_mapping(workload_contract_profile)
    except ValueError as exc:
        return PreflightCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail=(f"protected environment {environment!r} has invalid workload contract: {exc}"),
            remediation=(
                "Declare the exact v1 [workload_contract] tuple: "
                "internal_trusted with all three capability flags set to false."
            ),
        )

    violations = contract.v1_violations()
    if violations:
        return PreflightCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail=(
                f"protected environment {environment!r} violates the v1 "
                "workload trust contract: " + "; ".join(violations)
            ),
            remediation=(
                "Set workload_trust_mode=internal_trusted and all workload "
                "capability flags to false before applying this protected release."
            ),
        )
    return PreflightCheck(
        name="workload-trust-contract",
        outcome="pass",
        detail=(
            f"protected environment {environment!r} declares the exact v1 "
            "internal_trusted workload contract"
        ),
    )


def _protected_workload_trust_contract_check(
    *,
    environment: str,
    workload_contract_profile: object,
) -> PreflightCheck | None:
    if environment not in PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS:
        return None
    return _check_workload_trust_contract(
        environment=environment,
        workload_contract_profile=workload_contract_profile,
    )


def _check_pss_enforce(
    core_v1: Any,
    namespace: str,
) -> PreflightCheck:
    """The worker Deployment bind-mounts the host docker socket. PSS
    `restricted` (k8s 1.25+ default for new namespaces) rejects this
    at admission. Warn (not fail) so operators using a non-Docker
    driver aren't blocked."""
    try:
        ns = core_v1.read_namespace(name=namespace)
    except Exception:
        return PreflightCheck(
            name="pss-enforce",
            outcome="warn",
            detail="could not read namespace metadata",
        )
    labels = getattr(ns.metadata, "labels", None) or {}
    enforce = labels.get(_PSS_ENFORCE_LABEL)
    if enforce == "restricted":
        return PreflightCheck(
            name="pss-enforce",
            outcome="warn",
            detail=(
                f"namespace {namespace!r} has PSS enforce=restricted; "
                "the worker Deployment's hostPath docker.sock mount "
                "will be rejected"
            ),
            remediation=(
                "Either relax to enforce=privileged (worker docker "
                "driver), or switch worker to a non-Docker driver:\n"
                f"  kubectl label namespace {namespace} "
                f"{_PSS_ENFORCE_LABEL}=privileged --overwrite"
            ),
        )
    if enforce is None:
        return PreflightCheck(
            name="pss-enforce",
            outcome="pass",
            detail=(f"namespace {namespace!r} has no PSS enforce label (no admission restriction)"),
        )
    return PreflightCheck(
        name="pss-enforce",
        outcome="pass",
        detail=(f"namespace {namespace!r} PSS enforce={enforce!r} (non-restricted)"),
    )


def collect_preflight(
    core_v1: Any,
    networking_v1: Any,
    storage_v1: Any,
    namespace: str,
    *,
    context: str | None,
    environment: str | None = None,
    backup_manifest: Path | None = None,
    backup_max_age_hours: int = DEFAULT_BACKUP_MAX_AGE_HOURS,
    backup_limits: BackupTraversalLimits | None = None,
    cluster_config: ClusterConfig | None = None,
    workload_contract_profile: object = None,
) -> PreflightReport:
    """Pure-collection function — every API client passed in so tests
    can inject fakes. If `namespace-exists` fails, the namespace-
    scoped checks (Secrets, PSS) are skipped to avoid burying the
    real problem in cascade failures."""
    checks: list[PreflightCheck] = []
    ns_check = _check_namespace_exists(core_v1, namespace)
    checks.append(ns_check)

    # Cluster-scoped checks always run, even when the namespace is
    # missing — they're useful diagnostics on their own.
    checks.append(_check_ingress_class_installed(networking_v1))
    checks.append(_check_default_storage_class(storage_v1))
    env_name = infer_environment(environment=environment, namespace=namespace)
    workload_contract_check = _protected_workload_trust_contract_check(
        environment=env_name,
        workload_contract_profile=workload_contract_profile,
    )
    if workload_contract_check is not None:
        checks.append(workload_contract_check)
    if env_name in {"staging", "production"}:
        checks.append(
            _check_protected_storage_boundary(
                core_v1,
                storage_v1,
                environment=env_name,
                namespace=namespace,
                cluster_config=cluster_config,
            )
        )
        checks.append(
            _check_backup_manifest(
                backup_manifest,
                environment=env_name,
                namespace=namespace,
                max_age_hours=backup_max_age_hours,
                limits=backup_limits,
            )
        )

    if ns_check.outcome == "pass":
        checks.extend(_check_required_secrets(core_v1, namespace))
        checks.append(_check_pss_enforce(core_v1, namespace))

    return PreflightReport(
        namespace=namespace,
        context=context,
        checks=checks,
    )


def _format_preflight_table(report: PreflightReport) -> str:
    """Stable human-readable preflight output. Greppable. Each check
    appears on its own line with the outcome marker and detail; failed
    checks include their remediation hint inline (indented)."""
    lines: list[str] = []
    lines.append(
        f"namespace: {report.namespace}"
        + (f" (context: {report.context})" if report.context else ""),
    )
    lines.append("")
    lines.append(f"{'CHECK':<32} {'OUTCOME':<8} DETAIL")
    for c in report.checks:
        lines.append(f"{c.name:<32} {c.outcome:<8} {c.detail}")
        if c.remediation and c.outcome in ("fail", "warn"):
            for rline in c.remediation.splitlines():
                lines.append(f"    {rline}")
    return "\n".join(lines) + "\n"


def _format_preflight_json(report: PreflightReport) -> str:
    obj = {
        "namespace": report.namespace,
        "context": report.context,
        "all_pass": report.all_pass,
        "any_fail": report.any_fail,
        "checks": [
            {
                "name": c.name,
                "outcome": c.outcome,
                "detail": c.detail,
                "remediation": c.remediation,
            }
            for c in report.checks
        ],
    }
    return json.dumps(obj, indent=2) + "\n"


def _append_schema_doctor_check(
    report: PreflightReport,
    doctor_report: DoctorReport,
) -> None:
    if doctor_report.ok:
        report.checks.append(
            PreflightCheck(
                name="schema-doctor",
                outcome="pass",
                detail="schema reconciliation clean",
            )
        )
        return
    report.checks.append(
        PreflightCheck(
            name="schema-doctor",
            outcome="fail",
            detail=f"{len(doctor_report.violations)} schema violation(s)",
            remediation="\n".join(
                f"  - {v.kind}: {v.entry}: {v.detail}" for v in doctor_report.violations
            ),
        )
    )


def _append_target_schema_doctor_check(
    report: PreflightReport,
    *,
    core_v1: Any,
    namespace: str,
    config: ClusterConfig,
    rendered_manifests: str | None = None,
) -> None:
    schema = load_bundled_schema()
    try:
        manifests = rendered_manifests or render_manifests(config)
        doctor_report = _doctor_reconcile_rendered(
            schema,
            core_v1,
            namespace=namespace,
            rendered_manifests=manifests,
        )
    except Exception as exc:
        report.checks.append(
            PreflightCheck(
                name="schema-doctor",
                outcome="warn",
                detail=f"doctor could not run: {type(exc).__name__}: {exc}",
            )
        )
    else:
        _append_schema_doctor_check(report, doctor_report)


def _protected_target_environment_failure_check() -> PreflightCheck:
    return PreflightCheck(
        name="protected-target-environment",
        outcome="fail",
        detail=("explicit environment conflicts with an authoritative protected namespace"),
        remediation=(
            "Use the protected namespace's matching environment or a non-protected namespace."
        ),
    )


def _resolve_target_environment(
    *,
    environment: str | None,
    namespace: str,
) -> tuple[str | None, PreflightCheck | None]:
    try:
        return infer_environment(environment=environment, namespace=namespace), None
    except ValueError:
        return None, _protected_target_environment_failure_check()


def _preflight(args: argparse.Namespace) -> int:
    try:
        _resolve_config_target(args, defer_toml_errors=True)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: target config invalid: {exc}\n")
        return 2
    environment, target_environment_failure = _resolve_target_environment(
        environment=args.environment,
        namespace=args.namespace,
    )
    if target_environment_failure is not None:
        report = PreflightReport(
            namespace=args.namespace,
            context=args.context,
            checks=[target_environment_failure],
        )
        if args.format == "json":
            sys.stdout.write(_format_preflight_json(report))
        else:
            sys.stdout.write(_format_preflight_table(report))
        return 1
    assert environment is not None
    try:
        _apps_v1, net_v1, core_v1, storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        workload_contract_profile = workload_contract_profile_from_file(cfg_path)
        workload_contract_check = _protected_workload_trust_contract_check(
            environment=environment,
            workload_contract_profile=workload_contract_profile,
        )
        if workload_contract_check is not None and workload_contract_check.outcome == "fail":
            report = PreflightReport(
                namespace=args.namespace,
                context=args.context,
                checks=[workload_contract_check],
            )
            if args.format == "json":
                sys.stdout.write(_format_preflight_json(report))
            else:
                sys.stdout.write(_format_preflight_table(report))
            return 1
        cluster_config = load_cluster_config(cfg_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: config invalid: {exc}\n")
        return 2
    try:
        effective_context = _effective_kube_context(args.context)
        report = collect_preflight(
            core_v1,
            net_v1,
            storage_v1,
            args.namespace,
            context=effective_context,
            environment=args.environment,
            backup_manifest=(
                Path(args.backup_manifest).resolve() if args.backup_manifest else None
            ),
            backup_max_age_hours=args.backup_max_age_hours,
            backup_limits=_backup_traversal_limits_from_args(args),
            cluster_config=cluster_config,
            workload_contract_profile=workload_contract_profile,
        )
    except Exception as exc:
        sys.stderr.write(
            f"error: failed to read cluster state: {type(exc).__name__}: {exc}\n",
        )
        return 2
    if not args.no_doctor:
        _append_target_schema_doctor_check(
            report,
            core_v1=core_v1,
            namespace=args.namespace,
            config=cluster_config,
        )

    if args.format == "json":
        sys.stdout.write(_format_preflight_json(report))
    else:
        sys.stdout.write(_format_preflight_table(report))
    # Exit 1 only when something explicitly failed; warns alone keep
    # exit 0 so CI scripts don't have to special-case them.
    return 1 if report.any_fail else 0


def _backup_manifest(args: argparse.Namespace) -> int:
    try:
        write_backup_manifest(
            environment=args.environment,
            namespace=args.namespace,
            output_path=Path(args.output).resolve(),
            components={
                "postgres": Path(args.postgres_dump).resolve(),
                "minio": Path(args.minio_snapshot).resolve(),
                "k8s_secrets": Path(args.k8s_secrets).resolve(),
            },
        )
    except (OSError, ValueError, FileNotFoundError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    sys.stdout.write(f"backup manifest written: {Path(args.output).resolve()}\n")
    return 0


def _backup_check(args: argparse.Namespace) -> int:
    problems = validate_backup_manifest(
        Path(args.manifest).resolve(),
        environment=args.environment,
        namespace=args.namespace,
        max_age_hours=args.max_age_hours,
        min_remaining_hours=args.min_remaining_hours,
        limits=_backup_traversal_limits_from_args(args),
    )
    if problems:
        for problem in problems:
            sys.stderr.write(f"error: {problem}\n")
        return 1
    sys.stdout.write(f"backup manifest verified: {Path(args.manifest).resolve()}\n")
    return 0


def _doctor(args: argparse.Namespace) -> int:
    try:
        _apps_v1, _net_v1, core_v1, _storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2
    schema = load_bundled_schema()
    report = _doctor_reconcile(schema, core_v1, namespace=args.namespace)
    schema_ok = report.ok
    if schema_ok:
        print("[ok] schema reconciliation clean")
    else:
        for v in report.violations:
            print(f"  [fail] [{v.kind}] {v.entry}: {v.detail}")

    lifecycle_ok = _doctor_check_storage_lifecycle(args)
    if lifecycle_ok is None:
        # Operator did not opt in; doctor result unchanged.
        return 0 if schema_ok else 1
    return 0 if (schema_ok and lifecycle_ok) else 1


def _doctor_check_storage_lifecycle(
    args: argparse.Namespace,
) -> bool | None:
    """Optional sub-check: compare live bucket lifecycle against the
    operator's storage-lifecycle.toml.

    Returns ``None`` when the operator did not pass
    ``--storage-lifecycle-config``; returns True/False once invoked.
    Print output goes to stdout (clean) or stderr (drift detected).
    """
    config_path = getattr(args, "storage_lifecycle_config", None)
    if config_path is None:
        return None


    from loom.storage_retention_doctor import (
        check_lifecycle_drift,
        format_drift_report,
    )
    from loom.storage_retention_loader import load_retention_config

    try:
        cfg = load_retention_config(Path(config_path))
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"  [fail] storage_lifecycle: {exc}\n")
        return False

    from loom.storage_credentials import (
        UnsupportedAuthKindError,
        build_s3_client,
    )

    auth_kind = os.environ.get("LOOM_SVC_STORAGE_AUTH_KIND", "static_keys")
    endpoint = getattr(args, "storage_lifecycle_endpoint", None) or os.environ.get(
        "LOOM_SVC_MINIO_ENDPOINT", "http://loom-minio:9000"
    )
    region = os.environ.get("LOOM_SVC_MINIO_REGION", "us-east-1")
    access_key = os.environ.get(
        "LOOM_SVC_MINIO_ACCESS_KEY",
    ) or os.environ.get("MINIO_ROOT_USER", "")
    secret_key = os.environ.get(
        "LOOM_SVC_MINIO_SECRET_KEY",
    ) or os.environ.get("MINIO_ROOT_PASSWORD", "")

    try:
        s3 = build_s3_client(
            endpoint_url=endpoint,
            auth_kind=auth_kind,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )
    except UnsupportedAuthKindError as exc:
        sys.stderr.write(f"  [fail] storage_lifecycle: {exc}\n")
        return False
    except ValueError as exc:
        sys.stderr.write(f"  [fail] storage_lifecycle: {exc}\n")
        return False
    try:
        drifts = check_lifecycle_drift(s3, cfg)
    except Exception as exc:
        sys.stderr.write(
            f"  [fail] storage_lifecycle: cannot reach object store: {type(exc).__name__}: {exc}\n",
        )
        return False

    report = format_drift_report(drifts)
    if not report:
        print("[ok] storage lifecycle rules match config")
        return True
    sys.stderr.write(report + "\n")
    return False


# ──────────────────────────────────────────────────────────────────────
# up (#76 Phase 3 — orchestrate preflight → render → apply → wait)
# ──────────────────────────────────────────────────────────────────────

# Default deadline for the whole "wait for ready" loop after apply.
# Heuristic: image pull on a cold cluster can take a few minutes;
# Postgres readiness probe is ~5s after init; MinIO is fast. 10 min
# covers cold-start safely. Operators can override via `--timeout`.
_DEFAULT_UP_TIMEOUT_SEC = 600
_DEFAULT_UP_POLL_INTERVAL_SEC = 5.0
_MAX_EXACT_RENDERED_MANIFEST_BYTES = 16 * 1024 * 1024


@dataclass
class ApplyResult:
    """Outcome of the kubectl apply step. `summary_lines` captures
    kubectl's own per-object reporting (`deployment.apps/loom-service
    configured`) so operators see what changed."""

    returncode: int
    summary_lines: list[str]
    stderr: str


@dataclass(frozen=True)
class PruneResult:
    deleted: list[str]
    retained: list[str]
    not_found: list[str]
    failed: list[tuple[str, str]]

    @property
    def has_evidence(self) -> bool:
        return bool(self.deleted or self.retained or self.not_found or self.failed)

    @property
    def ok(self) -> bool:
        return not self.failed


def _is_k8s_not_found(exc: Exception) -> bool:
    return type(exc).__name__ == "ApiException" and getattr(exc, "status", None) == 404


def prune_disabled_profile_resources(
    apps_v1: Any,
    networking_v1: Any,
    _core_v1: Any,
    config: ClusterConfig,
    *,
    namespace: str,
) -> PruneResult:
    """Remove resources intentionally omitted by profile toggles.

    The #440 contract is deliberately narrow: when a profile disables the
    in-cluster worker, remove stale live worker compute/network resources but
    preserve the worker trajectory PVC unless an operator uses an explicit
    destructive teardown path.
    """
    if config.k8s_worker.enabled:
        return PruneResult(deleted=[], retained=[], not_found=[], failed=[])

    deleted: list[str] = []
    not_found: list[str] = []
    failed: list[tuple[str, str]] = []

    def _delete(label: str, fn: Any) -> None:
        try:
            fn()
        except Exception as exc:
            if _is_k8s_not_found(exc):
                not_found.append(label)
                return
            failed.append((label, _exception_to_note(exc)))
            return
        deleted.append(label)

    # Dynamic-storage profiles render loom-worker as a StatefulSet
    # (#673); static-host-path profiles keep the Deployment shape.
    # Try both — the not-found path is a no-op in either direction.
    _delete(
        "deployment.apps/loom-worker",
        lambda: apps_v1.delete_namespaced_deployment(
            name="loom-worker",
            namespace=namespace,
        ),
    )
    delete_sts = getattr(apps_v1, "delete_namespaced_stateful_set", None)
    if callable(delete_sts):
        _delete(
            "statefulset.apps/loom-worker",
            lambda: delete_sts(
                name="loom-worker",
                namespace=namespace,
            ),
        )
    # StatefulSet's headless Service (#673). Not-found is fine when the
    # profile never rendered the StatefulSet shape.
    delete_svc = getattr(_core_v1, "delete_namespaced_service", None)
    if callable(delete_svc):
        _delete(
            "service/loom-worker",
            lambda: delete_svc(
                name="loom-worker",
                namespace=namespace,
            ),
        )
    _delete(
        "networkpolicy.networking.k8s.io/loom-worker",
        lambda: networking_v1.delete_namespaced_network_policy(
            name="loom-worker",
            namespace=namespace,
        ),
    )

    return PruneResult(
        deleted=deleted,
        retained=["persistentvolumeclaim/loom-worker-trajectories"],
        not_found=not_found,
        failed=failed,
    )


def apply_manifests(
    yaml_text: str,
    namespace: str,
    *,
    context: str | None,
    extra_args: tuple[str, ...] = (),
    apps_v1: Any | None = None,
) -> ApplyResult:
    """Pipe rendered manifests into `kubectl apply -f -`. We shell out
    to kubectl rather than use the python client's apply path because
    kubectl handles server-side-apply + multi-doc YAML + namespace
    auto-creation natively. The result's returncode passes through;
    callers map it to the right exit code.

    `extra_args` lets tests inject `--dry-run=server` etc. without
    cluttering the route signature. Production callers leave it empty.
    """
    import shutil
    import subprocess

    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl is required for `loom cluster up`. install from "
            "https://kubernetes.io/docs/tasks/tools/ and ensure it's "
            "on PATH.",
        )

    apply_text = yaml_text
    statefulset_patches: list[tuple[str, dict[str, Any]]] = []
    if apps_v1 is not None:
        prepared = _prepare_statefulsets_for_apply(
            yaml_text,
            apps_v1=apps_v1,
            namespace=namespace,
        )
        if prepared.returncode != 0:
            return ApplyResult(
                returncode=prepared.returncode,
                summary_lines=[],
                stderr=prepared.stderr,
            )
        apply_text = prepared.yaml_text
        statefulset_patches = prepared.statefulset_patches

    summary_lines: list[str] = []
    stderr_parts: list[str] = []

    cmd: list[str] = ["kubectl", "apply", "-n", namespace, "-f", "-"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(extra_args)

    if apply_text.strip():
        proc = subprocess.run(
            cmd,
            input=apply_text,
            capture_output=True,
            text=True,
            check=False,
        )
        summary_lines.extend(line for line in proc.stdout.splitlines() if line.strip())
        if proc.stderr:
            stderr_parts.append(proc.stderr)
        if proc.returncode != 0:
            return ApplyResult(
                returncode=proc.returncode,
                summary_lines=summary_lines,
                stderr="".join(stderr_parts),
            )

    for name, patch in statefulset_patches:
        patch_cmd = [
            "kubectl",
            "patch",
            "statefulset",
            name,
            "-n",
            namespace,
            "--type",
            "merge",
            "-p",
            json.dumps(patch, sort_keys=True),
        ]
        if context:
            patch_cmd.extend(["--context", context])
        proc = subprocess.run(
            patch_cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        summary_lines.extend(line for line in proc.stdout.splitlines() if line.strip())
        if proc.stderr:
            stderr_parts.append(proc.stderr)
        if proc.returncode != 0:
            return ApplyResult(
                returncode=proc.returncode,
                summary_lines=summary_lines,
                stderr="".join(stderr_parts),
            )

    return ApplyResult(
        returncode=0,
        summary_lines=summary_lines,
        stderr="".join(stderr_parts),
    )


@dataclass(frozen=True)
class _PreparedApply:
    returncode: int
    yaml_text: str
    statefulset_patches: list[tuple[str, dict[str, Any]]]
    stderr: str = ""


_STATEFULSET_MUTABLE_SPEC_FIELDS = {
    "replicas",
    "template",
    "updateStrategy",
    "revisionHistoryLimit",
    "persistentVolumeClaimRetentionPolicy",
    "minReadySeconds",
    "ordinals",
}
_STATEFULSET_IMMUTABLE_SPEC_FIELDS = (
    "podManagementPolicy",
    "selector",
    "serviceName",
    "volumeClaimTemplates",
)


def _prepare_statefulsets_for_apply(
    yaml_text: str,
    *,
    apps_v1: Any,
    namespace: str,
) -> _PreparedApply:
    import yaml

    docs = [doc for doc in yaml.safe_load_all(yaml_text) if doc]
    apply_docs: list[dict[str, Any]] = []
    patches: list[tuple[str, dict[str, Any]]] = []
    errors: list[str] = []

    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "StatefulSet":
            apply_docs.append(doc)
            continue
        metadata = _dict_child(doc, "metadata")
        name = str(metadata.get("name") or "")
        if not name:
            apply_docs.append(doc)
            continue
        try:
            live = apps_v1.read_namespaced_stateful_set(
                name=name,
                namespace=namespace,
            )
        except KeyError:
            apply_docs.append(doc)
            continue
        except Exception as exc:
            if _is_k8s_not_found(exc):
                apply_docs.append(doc)
                continue
            errors.append(
                f"could not read existing StatefulSet {name!r}: {_exception_to_note(exc)}"
            )
            continue

        drift = _statefulset_immutable_drift(name, doc, _k8s_mapping(live, apps_v1))
        if drift:
            errors.extend(drift)
            continue
        patch = _statefulset_mutable_patch(doc)
        if patch:
            patches.append((name, patch))

    if errors:
        return _PreparedApply(
            returncode=1,
            yaml_text="",
            statefulset_patches=[],
            stderr=(
                "existing StatefulSet immutable drift blocks safe apply:\n"
                + "\n".join(f"  {line}" for line in errors)
                + "\n"
            ),
        )
    return _PreparedApply(
        returncode=0,
        yaml_text=yaml.safe_dump_all(apply_docs, sort_keys=False) if apply_docs else "",
        statefulset_patches=patches,
    )


def _k8s_mapping(obj: Any, apps_v1: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    serializer = getattr(getattr(apps_v1, "api_client", None), "sanitize_for_serialization", None)
    if callable(serializer):
        serialised = serializer(obj)
        if isinstance(serialised, dict):
            return serialised
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        mapped = to_dict()
        if isinstance(mapped, dict):
            return mapped
    return {}


def _dict_child(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, dict) else {}


def _statefulset_immutable_drift(
    name: str,
    desired: dict[str, Any],
    live: dict[str, Any],
) -> list[str]:
    desired_spec = _dict_child(desired, "spec")
    live_spec = _dict_child(live, "spec")
    drift: list[str] = []
    for field_name in _STATEFULSET_IMMUTABLE_SPEC_FIELDS:
        desired_value = desired_spec.get(field_name)
        live_value = live_spec.get(field_name)
        if field_name == "podManagementPolicy":
            desired_value = desired_value or "OrderedReady"
            live_value = live_value or "OrderedReady"
        if field_name == "volumeClaimTemplates":
            desired_value = _normalise_volume_claim_templates(desired_value)
            live_value = _normalise_volume_claim_templates(live_value)
        else:
            desired_value = _prune_k8s_defaults(desired_value)
            live_value = _prune_k8s_defaults(live_value)
        if desired_value != live_value:
            drift.append(
                f"{name}: spec.{field_name} differs from the live StatefulSet; "
                "this field is immutable"
            )
    return drift


def _normalise_volume_claim_templates(value: Any) -> Any:
    if not isinstance(value, list):
        return _prune_k8s_defaults(value)
    normalised: list[Any] = []
    for item in value:
        if not isinstance(item, dict):
            normalised.append(item)
            continue
        claim = dict(item)
        claim.pop("apiVersion", None)
        claim.pop("kind", None)
        claim.pop("status", None)
        spec = claim.get("spec")
        if isinstance(spec, dict):
            spec = dict(spec)
            spec.pop("volumeName", None)
            if spec.get("storageClassName") in ("", None):
                spec.pop("storageClassName", None)
            if spec.get("volumeMode") in ("Filesystem", None):
                spec.pop("volumeMode", None)
            for optional_key in ("dataSource", "dataSourceRef", "selector"):
                if spec.get(optional_key) in (None, {}, []):
                    spec.pop(optional_key, None)
            claim["spec"] = spec
        normalised.append(_prune_k8s_defaults(claim))
    return normalised


def _prune_k8s_defaults(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            pruned = _prune_k8s_defaults(child)
            if pruned in (None, {}, []):
                continue
            out[key] = pruned
        return out
    if isinstance(value, list):
        return [_prune_k8s_defaults(item) for item in value]
    return value


def _statefulset_mutable_patch(doc: dict[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    metadata = _dict_child(doc, "metadata")
    metadata_patch = {key: metadata[key] for key in ("labels", "annotations") if key in metadata}
    if metadata_patch:
        patch["metadata"] = metadata_patch
    spec = _dict_child(doc, "spec")
    spec_patch = {key: spec[key] for key in _STATEFULSET_MUTABLE_SPEC_FIELDS if key in spec}
    if spec_patch:
        patch["spec"] = spec_patch
    return patch


def wait_for_ready(
    apps_v1: Any,
    networking_v1: Any,
    core_v1: Any,
    namespace: str,
    *,
    context: str | None,
    timeout_sec: int = _DEFAULT_UP_TIMEOUT_SEC,
    poll_interval_sec: float = _DEFAULT_UP_POLL_INTERVAL_SEC,
    _sleep: Any = None,
    _now: Any = None,
) -> ClusterStatus:
    """Poll `collect_status` until every component is healthy or the
    deadline expires. Returns the final ClusterStatus regardless —
    callers inspect `all_ready` to decide the exit code.

    `_sleep` + `_now` are test seams. Production passes
    `time.sleep` / `time.monotonic`.
    """
    import time

    sleep_fn = _sleep if _sleep is not None else time.sleep
    now_fn = _now if _now is not None else time.monotonic

    deadline = now_fn() + timeout_sec
    while True:
        status = collect_status(
            apps_v1,
            networking_v1,
            core_v1,
            namespace,
            context=context,
        )
        if status.all_ready:
            return status
        if now_fn() >= deadline:
            return status
        sleep_fn(poll_interval_sec)


def recover_sandbox_deadline_pods(
    core_v1: Any,
    namespace: str,
    status: ClusterStatus,
    *,
    max_pods: int,
    dry_run: bool,
) -> list[str]:
    """Delete only pods classified as pod sandbox deadline stalls.

    This is intentionally narrow: it never scans arbitrary selectors and never
    deletes merely-unready pods. The diagnostics come from the same status
    object that blocked readiness, so the operation remains bounded and
    auditable.
    """
    if max_pods <= 0:
        return []
    pods = pod_names_from_status(status)[:max_pods]
    if dry_run:
        return pods
    recovered: list[str] = []
    for pod in pods:
        core_v1.delete_namespaced_pod(name=pod, namespace=namespace)
        recovered.append(pod)
    return recovered


def _up(args: argparse.Namespace) -> int:
    explicit_namespace = args.namespace
    explicit_environment = args.environment
    try:
        _apply_config_target_fields(
            args,
            None,
            explicit_namespace=explicit_namespace,
            explicit_environment=explicit_environment,
        )
    except ValueError as exc:
        sys.stderr.write(f"error: target config invalid: {exc}\n")
        return 2
    environment, target_environment_failure = _resolve_target_environment(
        environment=args.environment,
        namespace=args.namespace,
    )
    if target_environment_failure is not None:
        sys.stderr.write(
            "error: protected-target-environment preflight failed — refusing to apply.\n"
        )
        sys.stderr.write(
            _format_preflight_table(
                PreflightReport(
                    namespace=args.namespace,
                    context=args.context,
                    checks=[target_environment_failure],
                )
            )
        )
        return 1
    assert environment is not None
    if environment in PROTECTED_ENVIRONMENTS:
        sys.stderr.write("error: hosted environments require the Nebius deployment entrypoint; see docs/runbooks/nebius-deployment.md.\n")
        return 1
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        snapshot = _load_cluster_up_config_snapshot(cfg_path)
    except _ClusterConfigSnapshotTomlError as exc:
        if environment in PROTECTED_WORKLOAD_TRUST_ENVIRONMENTS:
            sys.stderr.write(
                "error: workload-trust-contract preflight failed — refusing to apply.\n"
            )
            sys.stderr.write(
                _format_preflight_table(
                    PreflightReport(
                        namespace=args.namespace,
                        context=args.context,
                        checks=[
                            PreflightCheck(
                                name="workload-trust-contract",
                                outcome="fail",
                                detail=(
                                    f"protected environment {environment!r} has "
                                    f"invalid cluster config TOML: {exc}"
                                ),
                                remediation=(
                                    "Repair the cluster config TOML and declare the "
                                    "exact v1 [workload_contract] tuple before applying."
                                ),
                            )
                        ],
                    )
                )
            )
            return 1
        sys.stderr.write(f"error: preflight config invalid: {exc}\n")
        return 2
    except _ClusterConfigSnapshotRenderError as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"error: preflight config invalid: {exc}\n")
        return 2
    if snapshot.protected_target is not None:
        sys.stderr.write("error: hosted configurations require the Nebius deployment entrypoint; see docs/runbooks/nebius-deployment.md.\n")
        return 1
    try:
        _apply_config_target_fields(
            args,
            snapshot.raw_config,
            explicit_namespace=explicit_namespace,
            explicit_environment=explicit_environment,
        )
    except ValueError as exc:
        sys.stderr.write(f"error: target config invalid: {exc}\n")
        return 2
    environment, target_environment_failure = _resolve_target_environment(
        environment=args.environment,
        namespace=args.namespace,
    )
    if target_environment_failure is not None:
        sys.stderr.write(
            "error: protected-target-environment preflight failed — refusing to apply.\n"
        )
        return 1
    assert environment is not None
    effective_environment = snapshot.protected_target or environment
    workload_contract_check = _protected_workload_trust_contract_check(
        environment=effective_environment,
        workload_contract_profile=snapshot.workload_contract_profile,
    )
    if workload_contract_check is not None and workload_contract_check.outcome == "fail":
        sys.stderr.write("error: workload-trust-contract preflight failed — refusing to apply.\n")
        sys.stderr.write(
            _format_preflight_table(
                PreflightReport(
                    namespace=args.namespace,
                    context=args.context,
                    checks=[workload_contract_check],
                )
            )
        )
        return 1
    return _up_impl(args, snapshot)


def _run_migration_job(
    *,
    namespace: str,
    context: str | None,
    image_tag: str,
    container_registry: str,
    wait: bool,
    timeout_sec: int,
) -> bool:
    """Render and apply the sanctioned Alembic migration Job, then (unless
    ``wait`` is False) block until it completes.

    The control-plane and service assert the schema is at Alembic head on
    startup and refuse to boot otherwise. ``cluster up`` applies the app
    manifest, so without this step the app pods crash-loop on a fresh or
    behind database. ``alembic upgrade head`` is idempotent, so running it
    on every ``up`` is safe. Returns True on success (or on apply-only when
    ``wait`` is False), False on any failure.
    """
    import shutil
    import subprocess
    from datetime import datetime

    import yaml as _yaml

    from loom_cli.cluster_migration import render_migration_manifest

    if shutil.which("kubectl") is None:
        sys.stderr.write(
            "error: kubectl is required to run the migration Job.\n",
        )
        return False

    job_suffix = datetime.now(UTC).strftime("%Y%m%dt%H%M%Sz")
    manifest = render_migration_manifest(
        image_tag=image_tag,
        namespace=namespace,
        job_suffix=job_suffix,
        container_registry=container_registry,
    )
    try:
        job_name = next(d for d in _yaml.safe_load_all(manifest) if d)["metadata"]["name"]
    except (StopIteration, KeyError, TypeError):
        sys.stderr.write("error: could not determine migration Job name.\n")
        return False

    # Wait for Postgres to accept connections before applying the Job. On a
    # fresh cluster `up` applies the app + migration in quick succession
    # while the Postgres StatefulSet is still starting; the migration pod's
    # backoffLimit=1 means a connection-refused racing that start fails the
    # Job outright. (staging/prod migrate as a separate post-Postgres step.)
    pg_wait = [
        "kubectl",
        "wait",
        "-n",
        namespace,
        "--for=condition=ready",
        "pod",
        "-l",
        "app=loom-postgres",
        f"--timeout={timeout_sec}s",
    ]
    if context:
        pg_wait.extend(["--context", context])
    pg = subprocess.run(pg_wait, capture_output=True, text=True)
    if pg.returncode != 0:
        sys.stderr.write(
            f"error: Postgres did not become ready before the migration Job: {pg.stderr.strip()}\n",
        )
        return False
    sys.stdout.write("  Postgres ready; applying migration Job...\n")

    apply_cmd = ["kubectl", "apply", "-n", namespace, "-f", "-"]
    if context:
        apply_cmd.extend(["--context", context])
    applied = subprocess.run(apply_cmd, input=manifest, capture_output=True, text=True)
    if applied.returncode != 0:
        sys.stderr.write(
            f"error: applying migration Job failed (exit {applied.returncode}):\n"
            f"{applied.stderr}\n",
        )
        return False
    sys.stdout.write(
        f"  job.batch/{job_name} applied (alembic upgrade head)\n",
    )

    if not wait:
        sys.stdout.write(
            "  Skipping migration wait (--no-wait); app pods start once the Job completes.\n",
        )
        return True

    sys.stdout.write(f"  Waiting for migration Job {job_name} to complete...\n")
    wait_cmd = [
        "kubectl",
        "wait",
        "-n",
        namespace,
        f"job/{job_name}",
        "--for=condition=complete",
        f"--timeout={timeout_sec}s",
    ]
    if context:
        wait_cmd.extend(["--context", context])
    waited = subprocess.run(wait_cmd, capture_output=True, text=True)
    if waited.returncode != 0:
        logs_cmd = ["kubectl", "logs", "-n", namespace, f"job/{job_name}", "--tail=40"]
        if context:
            logs_cmd.extend(["--context", context])
        logs = subprocess.run(logs_cmd, capture_output=True, text=True)
        sys.stderr.write(
            f"error: migration Job {job_name} did not complete: "
            f"{waited.stderr.strip()}\n{logs.stdout}\n",
        )
        return False
    sys.stdout.write(f"  Migration Job {job_name} complete.\n")
    return True


def _up_impl(
    args: argparse.Namespace,
    snapshot: _ClusterUpConfigSnapshot,
) -> int:
    if args.sandbox_deadline_max_pods <= 0:
        sys.stderr.write("error: --sandbox-deadline-max-pods must be > 0\n")
        return 2

    try:
        apps_v1, net_v1, core_v1, storage_v1 = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    config = snapshot.config
    workload_contract_profile = snapshot.workload_contract_profile

    try:
        if args.rendered_manifest is None:
            manifests = render_manifests(config)
        else:
            rendered_path = Path(args.rendered_manifest)
            _require_real_file(rendered_path, label="exact rendered rollout manifest")
            rendered_size = rendered_path.stat().st_size
            if not 0 < rendered_size <= _MAX_EXACT_RENDERED_MANIFEST_BYTES:
                raise ValueError("exact rendered rollout manifest size is invalid")
            manifests = rendered_path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, UnicodeError, ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    # 1. Preflight
    if not args.skip_preflight:
        try:
            effective_context = _effective_kube_context(args.context)
            report = collect_preflight(
                core_v1,
                net_v1,
                storage_v1,
                args.namespace,
                context=effective_context,
                environment=args.environment,
                backup_manifest=(
                    Path(args.backup_manifest).resolve() if args.backup_manifest else None
                ),
                backup_max_age_hours=args.backup_max_age_hours,
                backup_limits=_backup_traversal_limits_from_args(args),
                cluster_config=config,
                workload_contract_profile=workload_contract_profile,
            )
            _append_target_schema_doctor_check(
                report,
                core_v1=core_v1,
                namespace=args.namespace,
                config=config,
                rendered_manifests=manifests,
            )
        except Exception as exc:
            sys.stderr.write(
                f"error: preflight failed: {type(exc).__name__}: {exc}\n",
            )
            return 2
        if report.any_fail:
            sys.stderr.write(
                "error: preflight checks failed — refusing to apply. "
                "Re-run with `loom cluster preflight` to see details, "
                "or pass `--skip-preflight` if you know what you're "
                "doing.\n",
            )
            sys.stderr.write(_format_preflight_table(report))
            return 1
        sys.stdout.write("Preflight: all checks passed.\n")

    # 2. Render
    # 3. Apply
    try:
        result = apply_manifests(
            manifests,
            args.namespace,
            context=args.context,
            apps_v1=apps_v1,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl apply failed (exit {result.returncode}):\n{result.stderr}\n",
        )
        return 1
    for line in result.summary_lines:
        sys.stdout.write(f"  {line}\n")

    prune_result = prune_disabled_profile_resources(
        apps_v1,
        net_v1,
        core_v1,
        config,
        namespace=args.namespace,
    )
    if prune_result.has_evidence:
        sys.stdout.write("Pruned disabled-profile resources:\n")
        for label in prune_result.deleted:
            sys.stdout.write(f"  {label} deleted\n")
        for label in prune_result.not_found:
            sys.stdout.write(f"  {label} already absent\n")
        for label in prune_result.retained:
            sys.stdout.write(f"  {label} retained\n")
        for label, error in prune_result.failed:
            sys.stderr.write(f"  {label} prune FAILED: {error}\n")
    if not prune_result.ok:
        sys.stderr.write(
            "error: disabled-profile resource prune failed; refusing to accept "
            "rollout with stale resources.\n",
        )
        return 1

    # 3b. Database migration (opt-in via --migrate). The control-plane and
    # service assert the schema is at Alembic head on startup and refuse to
    # boot otherwise. `cluster up` deliberately leaves migration an explicit
    # operator action for staging/production (see staging-smoke.yml, which
    # orchestrates it between two `up` calls); single-node local/dev bring-up
    # opts in so it's one command. Runs after apply (postgres + secrets
    # exist) and before the readiness wait; under --no-wait we apply the Job
    # but don't block on it.
    if getattr(args, "migrate", False):
        migrated = _run_migration_job(
            namespace=args.namespace,
            context=args.context,
            image_tag=config.image_tag,
            container_registry=getattr(config, "container_registry", "") or "",
            wait=not args.no_wait,
            timeout_sec=args.timeout,
        )
        if not migrated:
            sys.stderr.write(
                "error: database migration failed; the control-plane and "
                "service will not start until the schema is at head. Fix "
                "the cause and re-run.\n",
            )
            return 1

    if args.no_wait:
        sys.stdout.write(
            "Applied. Skipping readiness wait (--no-wait).\n",
        )
        return 0

    # 4. Wait for ready
    sys.stdout.write(
        f"Waiting up to {args.timeout}s for components to become ready...\n",
    )
    final = wait_for_ready(
        apps_v1,
        net_v1,
        core_v1,
        args.namespace,
        context=args.context,
        timeout_sec=args.timeout,
        poll_interval_sec=args.poll_interval,
    )
    sys.stdout.write(_format_table(final))
    if (
        not final.all_ready
        and args.recover_sandbox_deadlines
        and status_has_sandbox_deadline_failures(final)
    ):
        try:
            recovered_pods = recover_sandbox_deadline_pods(
                core_v1,
                args.namespace,
                final,
                max_pods=args.sandbox_deadline_max_pods,
                dry_run=False,
            )
        except Exception as exc:
            sys.stderr.write(
                f"error: sandbox deadline recovery failed: {type(exc).__name__}: {exc}\n",
            )
            return 1
        if recovered_pods:
            sys.stdout.write(
                "\nRecovered "
                f"{len(recovered_pods)} pod sandbox deadline stall(s): "
                + ", ".join(recovered_pods)
                + "\n",
            )
            sys.stdout.write(
                "Retrying readiness after bounded sandbox-deadline cleanup...\n",
            )
            final = wait_for_ready(
                apps_v1,
                net_v1,
                core_v1,
                args.namespace,
                context=args.context,
                timeout_sec=args.timeout,
                poll_interval_sec=args.poll_interval,
            )
            sys.stdout.write(_format_table(final))
        else:
            sys.stderr.write(
                "error: sandbox deadline recovery found no classified pods "
                "within the configured bound.\n",
            )
    if final.all_ready:
        image_checks = rendered_image_checks(apps_v1, args.namespace, manifests)
        image_drifts = [check.drift_message() for check in image_checks if check.drifted]
        if image_drifts:
            sys.stderr.write("error: deployment image drift detected after rollout:\n")
            for drift in image_drifts:
                sys.stderr.write(f"  - {drift}\n")
            return 1
        if image_checks:
            sys.stdout.write("\nDeployment image convergence verified:\n")
            for check in image_checks:
                sys.stdout.write(f"{check.evidence_line()}\n")
        _print_up_next_steps(args.namespace)
        return 0
    sys.stderr.write(
        f"error: components did not reach ready state within {args.timeout}s.\n",
    )
    return 1


def _print_up_next_steps(namespace: str) -> None:
    """Post-success guidance printed after `loom cluster up`.

    Names operator-driven steps that are NOT part of the apply +
    readiness flow but ARE required for the cluster to behave the way
    operators expect. Today this is storage retention; future
    additions belong here rather than buried in the runbook.
    """
    sys.stdout.write(
        "\nNext steps:\n"
        "  1. Apply object-store retention policy (idempotent; required\n"
        "     to bound trajectory + artifact disk growth):\n"
        f"       kubectl port-forward -n {namespace} \\\n"
        "         service/loom-minio 9000:9000 &\n"
        "       loom cluster bootstrap-storage-lifecycle \\\n"
        "         --config config/storage-lifecycle.toml \\\n"
        "         --endpoint http://localhost:9000\n"
        "  2. Verify cluster + retention health:\n"
        "       loom cluster doctor \\\n"
        "         --storage-lifecycle-config config/storage-lifecycle.toml \\\n"
        "         --storage-lifecycle-endpoint http://localhost:9000\n",
    )


@dataclass
class DeleteResult:
    """Outcome of `kubectl delete -f -`. `summary_lines` carries
    kubectl's own per-object reporting; `--ignore-not-found` keeps
    re-runs after a partial delete idempotent."""

    returncode: int
    summary_lines: list[str]
    stderr: str


def delete_manifests(
    yaml_text: str,
    namespace: str,
    *,
    context: str | None,
    extra_args: tuple[str, ...] = (),
) -> DeleteResult:
    """Pipe rendered manifests into
    `kubectl delete -f - --ignore-not-found`. Mirrors `apply_manifests`
    so operators see symmetric output. `--ignore-not-found` lets a
    second `down` run finish cleanly after a previous teardown
    partially completed."""
    import shutil
    import subprocess

    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl is required for `loom cluster down`. install from "
            "https://kubernetes.io/docs/tasks/tools/ and ensure it's "
            "on PATH.",
        )

    cmd: list[str] = [
        "kubectl",
        "delete",
        "-n",
        namespace,
        "-f",
        "-",
        "--ignore-not-found",
    ]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(extra_args)

    proc = subprocess.run(
        cmd,
        input=yaml_text,
        capture_output=True,
        text=True,
        check=False,
    )
    summary_lines = [line for line in proc.stdout.splitlines() if line.strip()]
    return DeleteResult(
        returncode=proc.returncode,
        summary_lines=summary_lines,
        stderr=proc.stderr,
    )


@dataclass
class PVCDeleteResult:
    """Outcome of `delete_pvcs`. Split so the caller can report what
    succeeded even when something later in the list fails — operators
    need to know whether postgres got wiped before minio failed."""

    deleted: list[str]
    failed: list[tuple[str, str]]  # (name, error-message)


def delete_pvcs(
    core_v1: Any,
    namespace: str,
) -> PVCDeleteResult:
    """Delete every PVC in the namespace. StatefulSet
    volumeClaimTemplates create PVCs (`data-loom-postgres-0`, etc.)
    that survive StatefulSet deletion — operators have to drop them
    explicitly to reclaim disk.

    Per-PVC errors are caught so a failure on one PVC doesn't hide
    what was actually wiped before it. The caller decides whether
    `failed` is fatal (it usually is — partial-wipe state is the
    operator's problem to resolve)."""
    pvcs = core_v1.list_namespaced_persistent_volume_claim(
        namespace=namespace,
    )
    deleted: list[str] = []
    failed: list[tuple[str, str]] = []
    for pvc in pvcs.items:
        name = pvc.metadata.name
        try:
            core_v1.delete_namespaced_persistent_volume_claim(
                name=name,
                namespace=namespace,
            )
            deleted.append(name)
        except Exception as exc:
            # Catch broadly: the k8s client can raise ApiException,
            # connection errors, or programming bugs in the underlying
            # client lib. The caller surfaces (name, repr) to the
            # operator either way.
            failed.append((name, f"{type(exc).__name__}: {exc}"))
    return PVCDeleteResult(deleted=deleted, failed=failed)


def delete_namespace_resource(
    core_v1: Any,
    namespace: str,
) -> None:
    """Delete the namespace itself. Cascades to every resource in it,
    including any objects not produced by `render_manifests` (operator
    one-offs, ad-hoc Secrets). Use with `--delete-namespace` when the
    operator wants the slate fully clean."""
    core_v1.delete_namespace(name=namespace)


def _guard_protected_destructive_down(args: argparse.Namespace) -> int | None:
    if not (args.with_volumes or args.delete_namespace):
        return None
    try:
        environment = infer_environment(
            environment=args.environment,
            namespace=args.namespace,
        )
    except ValueError:
        sys.stderr.write(
            "error: protected-target-environment conflict — refusing destructive operation.\n",
        )
        return 1
    if environment not in PROTECTED_ENVIRONMENTS:
        return None
    manifest = Path(args.backup_manifest).resolve() if args.backup_manifest else None
    problems = validate_backup_manifest(
        manifest,
        environment=environment,
        namespace=args.namespace,
        max_age_hours=args.backup_max_age_hours,
    )
    if problems:
        sys.stderr.write(
            f"error: refusing destructive operation in protected "
            f"environment {environment!r}; backup manifest is not valid:\n",
        )
        for problem in problems:
            sys.stderr.write(f"  - {problem}\n")
        return 1
    if args.acknowledge_data_loss != environment:
        sys.stderr.write(
            "error: destructive protected-environment operation requires "
            f"`--acknowledge-data-loss {environment}` after verifying restore "
            "readiness.\n",
        )
        return 1
    return None


def _down(args: argparse.Namespace) -> int:
    try:
        _resolve_config_target(args, defer_toml_errors=True)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: render failed: target config invalid: {exc}\n")
        return 2
    environment, target_environment_failure = _resolve_target_environment(
        environment=args.environment,
        namespace=args.namespace,
    )
    if target_environment_failure is not None:
        sys.stderr.write("error: protected-target-environment conflict — refusing teardown.\n")
        return 1
    assert environment is not None
    guard_result = _guard_protected_destructive_down(args)
    if guard_result is not None:
        return guard_result

    try:
        _, _, core_v1, _ = _load_clients(args.context)
    except RuntimeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(
            f"error: cannot connect to cluster: {type(exc).__name__}: {exc}\n",
        )
        return 2

    # 1. Render manifests so we know what to delete. Doing this from
    # the same config keeps `up` and `down` symmetric — if the
    # operator changed cluster-config.toml between the two calls,
    # `down` only removes objects the *current* config would have
    # produced. The `--ignore-not-found` flag on kubectl forgives
    # objects that have already been deleted manually.
    try:
        cfg_path = Path(args.config).resolve() if args.config else None
        config = load_cluster_config(cfg_path)
        manifests = render_manifests(config)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        sys.stderr.write(f"error: render failed: {exc}\n")
        return 2

    # 2. Confirm. Teardown is destructive; require explicit `--yes`
    # or an interactive y/N prompt before touching anything.
    if not args.yes:
        prompt = f"This will delete Loom resources in namespace '{args.namespace}'"
        if args.with_volumes:
            prompt += " AND its PersistentVolumeClaims (data loss)"
        if args.delete_namespace:
            prompt += f" AND the '{args.namespace}' namespace itself"
        prompt += ". Continue? [y/N]: "
        sys.stdout.write(prompt)
        sys.stdout.flush()
        try:
            reply = sys.stdin.readline().strip().lower()
        except (KeyboardInterrupt, EOFError):
            # EOFError covers `stdin=None` edge cases; closed-pipe
            # stdin (the common CI case without --yes) returns "" from
            # readline() which falls through to the abort path below.
            sys.stdout.write("\naborted.\n")
            return 1
        if reply not in ("y", "yes"):
            sys.stdout.write("aborted.\n")
            return 1

    # 3. Delete manifests.
    try:
        result = delete_manifests(
            manifests,
            args.namespace,
            context=args.context,
        )
    except RuntimeError as exc:
        # kubectl missing.
        sys.stderr.write(f"error: {exc}\n")
        return 2
    if result.returncode != 0:
        sys.stderr.write(
            f"error: kubectl delete failed (exit {result.returncode}):\n{result.stderr}\n",
        )
        return 1
    for line in result.summary_lines:
        sys.stdout.write(f"  {line}\n")

    # 4. Optional volume teardown.
    if args.with_volumes:
        try:
            pvc_result = delete_pvcs(core_v1, args.namespace)
        except Exception as exc:
            # The initial `list_namespaced_persistent_volume_claim` call
            # failed — we never got to per-PVC deletes. Per-PVC failures
            # are reported below via pvc_result.failed.
            sys.stderr.write(
                f"error: failed to list PVCs in namespace "
                f"'{args.namespace}': {type(exc).__name__}: {exc}\n",
            )
            return 1
        for name in pvc_result.deleted:
            sys.stdout.write(f"  persistentvolumeclaim/{name} deleted\n")
        for name, err in pvc_result.failed:
            sys.stderr.write(
                f"  persistentvolumeclaim/{name} FAILED: {err}\n",
            )
        if not pvc_result.deleted and not pvc_result.failed:
            sys.stdout.write(
                f"  (no PVCs found in namespace '{args.namespace}')\n",
            )
        if pvc_result.failed:
            sys.stderr.write(
                f"error: {len(pvc_result.failed)} PVC(s) failed to "
                f"delete; namespace '{args.namespace}' is now in a "
                f"partial-wipe state.\n",
            )
            return 1

    # 5. Optional namespace teardown.
    if args.delete_namespace:
        try:
            delete_namespace_resource(core_v1, args.namespace)
        except Exception as exc:
            sys.stderr.write(
                f"error: failed to delete namespace: {type(exc).__name__}: {exc}\n",
            )
            return 1
        sys.stdout.write(f"  namespace/{args.namespace} deleted\n")

    sys.stdout.write("Cluster down: complete.\n")
    return 0


def _render_migration(args: argparse.Namespace) -> int:
    """Handler for `loom cluster render-migration` (#332)."""
    from datetime import datetime

    from loom_cli.cluster_migration import render_migration_manifest

    if args.job_suffix is None:
        job_suffix = datetime.now(UTC).strftime("%Y%m%dt%H%M%Sz")
    else:
        job_suffix = args.job_suffix

    manifest = render_migration_manifest(
        image_tag=args.image_tag,
        namespace=args.namespace,
        job_suffix=job_suffix,
        container_registry=args.container_registry,
        registry_digest=args.registry_digest,
    )
    sys.stdout.write(manifest)
    if not manifest.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _bootstrap_evidence_paths(args: argparse.Namespace) -> int:
    """Emit sudo-install script for operator-writable rollout evidence dirs.

    Delegates to :mod:`loom_cli.cluster_bootstrap_evidence_paths` for the
    rendering + validation logic. This CLI shim only wires argparse defaults
    and error-code translation.
    """

    from loom_cli.cluster_bootstrap_evidence_paths import (
        DEFAULT_EVIDENCE_PATHS,
        ServiceDirCollisionError,
        render_bootstrap_script,
    )

    operator_user = args.operator_user or os.environ.get("USER")
    if not operator_user:
        sys.stderr.write("error: --operator-user not supplied and $USER unset\n")
        return 2

    if args.evidence_paths is None:
        evidence_paths: tuple[str, ...] = DEFAULT_EVIDENCE_PATHS
    else:
        evidence_paths = tuple(
            name.strip() for name in args.evidence_paths.split(",") if name.strip()
        )

    try:
        script = render_bootstrap_script(
            rollout_root=Path(args.rollout_root),
            operator_user=operator_user,
            evidence_paths=evidence_paths,
        )
    except ServiceDirCollisionError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    sys.stdout.write(script)
    if not script.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _bootstrap_secrets(args: argparse.Namespace) -> int:
    from loom_config.bootstrap import render_bootstrap_command

    schema = load_bundled_schema()
    # Resolve pgbouncer_enabled: CLI flag overrides schema default.
    pgbouncer_entry = schema.render_config.get("pgbouncer")
    schema_pgbouncer_default: bool = bool(
        pgbouncer_entry.fields.get("enabled", False)
        if pgbouncer_entry and pgbouncer_entry.fields
        else False
    )
    cli_pgbouncer: bool | None = getattr(args, "pgbouncer", None)
    pgbouncer_enabled: bool = schema_pgbouncer_default if cli_pgbouncer is None else cli_pgbouncer
    print(
        render_bootstrap_command(
            schema,
            namespace=args.namespace,
            smoke_defaults=args.smoke_defaults,
            rotate=args.rotate,
            pgbouncer_enabled=pgbouncer_enabled,
            admin_secret=getattr(args, "admin_secret", False),
        )
    )
    return 0


def _derive_pool_dsn(args: argparse.Namespace) -> int:
    from loom_config.bootstrap import _rewrite_dsn_host_port

    try:
        pool_dsn = _rewrite_dsn_host_port(
            args.dsn,
            host="loom-pgbouncer",
            port=6432,
        )
    except ValueError as exc:
        print(f"derive-pool-dsn: {exc}", file=sys.stderr)
        return 1
    print(pool_dsn)
    return 0



def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom cluster",
        description=(
            "Inspect Loom Kubernetes deployments and manage disposable local "
            "clusters (docs/architecture/cluster-deploy.md). Hosted deployments "
            "use the Nebius deployment entrypoint; see "
            "docs/runbooks/nebius-deployment.md."
        ),
    )
    sub = parser.add_subparsers(dest="cluster_cmd", required=True)

    p_status = sub.add_parser(
        "status",
        help=("Show component readiness + ingress endpoints for the configured Loom namespace."),
    )
    p_status.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_status.add_argument(
        "--namespace",
        default=None,
        help=(
            "Kubernetes namespace. With --config, inferred from the config and "
            "an explicit value must match; otherwise defaults to loom."
        ),
    )
    p_status.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment assertion. With --config, an explicit value "
            "must match runtime_environment."
        ),
    )
    p_status.add_argument(
        "--config",
        default=None,
        help=("Path to cluster-config.toml used to infer and validate namespace and environment."),
    )
    p_status.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_status.set_defaults(handler=_status)

    p_rollout_evidence = sub.add_parser(
        "rollout-evidence",
        help="Collect version-compatible protected-rollout evidence.",
    )
    rollout_sub = p_rollout_evidence.add_subparsers(
        dest="rollout_evidence_cmd",
        required=True,
    )
    p_rollout_docker = rollout_sub.add_parser(
        "docker-images",
        help=(
            "Collect Docker image tag evidence from JSON inspect output or "
            "live `docker image inspect`."
        ),
    )
    p_rollout_docker.add_argument(
        "--image",
        action="append",
        default=[],
        help="Image reference to inspect with Docker. Repeatable.",
    )
    p_rollout_docker.add_argument(
        "--expect-repo-tag",
        action="append",
        default=[],
        help="RepoTag that must appear in Docker inspect RepoTags. Repeatable.",
    )
    p_rollout_docker.add_argument(
        "--inspect-json",
        default=None,
        help="Path to saved `docker image inspect` JSON for dry-run evidence.",
    )
    p_rollout_docker.set_defaults(handler=_rollout_evidence_docker_images)

    p_rollout_status = rollout_sub.add_parser(
        "cluster-status",
        help="Run `loom cluster status` with current and compatibility formats.",
    )
    p_rollout_status.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_rollout_status.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_rollout_status.add_argument(
        "--status-format",
        default="json",
        help="Status output format: json, table, or compatibility alias text.",
    )
    p_rollout_status.set_defaults(handler=_rollout_evidence_cluster_status)

    p_render = sub.add_parser(
        "render",
        help=(
            "Render Kubernetes manifests to stdout from a "
            "cluster-config.toml. Apply with `kubectl apply -f -`."
        ),
    )
    p_render.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults "
            "(produces the same shape as deploy/k8s/*.yaml)."
        ),
    )
    p_render.set_defaults(handler=_render)

    p_reconcile = sub.add_parser(
        "reconcile",
        help=(
            "Read-only shadow reconcile: render the desired manifests and report "
            "desired-vs-live drift as JSON. Never writes (requires --shadow)."
        ),
    )
    p_reconcile.add_argument(
        "--config",
        default=None,
        help="Path to cluster-config.toml. Omit for all defaults.",
    )
    p_reconcile.add_argument(
        "--context",
        default=None,
        help="kubectl context for the read-only live read.",
    )
    p_reconcile.add_argument(
        "--target",
        default=None,
        help="Identity label for the desired version (defaults to the config image tag).",
    )
    p_reconcile.add_argument(
        "--shadow",
        action="store_true",
        help="Read-only: observe and report drift, never write. Required today.",
    )
    p_reconcile.set_defaults(handler=_reconcile)

    p_preflight = sub.add_parser(
        "preflight",
        help=(
            "Verify the target cluster meets prerequisites: required "
            "Secrets, IngressClass installed, default StorageClass, "
            "PSS labels. Exits 0 on all-pass (warns allowed), 1 on "
            "any-fail, 2 on cluster unreachable."
        ),
    )
    p_preflight.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_preflight.add_argument(
        "--namespace",
        default=None,
        help=(
            "Kubernetes namespace. With --config, inferred from the config and "
            "an explicit value must match; otherwise defaults to loom."
        ),
    )
    p_preflight.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name. Protected environments "
            "(staging/production) get storage and backup "
            "guard checks. With --config, inferred from runtime_environment "
            "and an explicit value must match; otherwise inferred from namespace."
        ),
    )
    p_preflight.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Protected preflight uses this "
            "to recognize static host-path Retain PVs before first apply."
        ),
    )
    p_preflight.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Path to a recent `loom cluster backup manifest` JSON file "
            "for protected environment checks."
        ),
    )
    p_preflight.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age for protected "
            f"environments (default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    _add_backup_traversal_limit_arguments(p_preflight)
    p_preflight.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format. JSON for CI/scripting.",
    )
    p_preflight.add_argument(
        "--no-doctor",
        dest="no_doctor",
        action="store_true",
        help=(
            "Skip schema-vs-cluster reconciliation (use when applying "
            "to an empty cluster where loom-secrets does not yet exist)."
        ),
    )
    p_preflight.set_defaults(handler=_preflight)

    p_backup = sub.add_parser(
        "backup",
        help=("Create or verify metadata manifests for protected staging backups."),
    )
    backup_sub = p_backup.add_subparsers(dest="backup_cmd", required=True)
    p_backup_manifest = backup_sub.add_parser(
        "manifest",
        help=(
            "Write a metadata-only manifest after Postgres, MinIO, and "
            "Kubernetes/runtime secrets have been backed up."
        ),
    )
    p_backup_manifest.add_argument("--environment", required=True)
    p_backup_manifest.add_argument("--namespace", required=True)
    p_backup_manifest.add_argument("--output", required=True)
    p_backup_manifest.add_argument("--postgres-dump", required=True)
    p_backup_manifest.add_argument("--minio-snapshot", required=True)
    p_backup_manifest.add_argument("--k8s-secrets", required=True)
    p_backup_manifest.set_defaults(handler=_backup_manifest)

    p_backup_check = backup_sub.add_parser(
        "check",
        help="Verify a backup manifest is recent and complete.",
    )
    p_backup_check.add_argument("--environment", required=True)
    p_backup_check.add_argument("--namespace", required=True)
    p_backup_check.add_argument("--manifest", required=True)
    p_backup_check.add_argument(
        "--max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
    )
    p_backup_check.add_argument(
        "--min-remaining-hours",
        type=int,
        default=0,
        help=(
            "Minimum remaining freshness window required before the "
            "manifest reaches --max-age-hours. Useful for long protected "
            "rollouts that must not discover expiry at mutation time."
        ),
    )
    _add_backup_traversal_limit_arguments(p_backup_check)
    p_backup_check.set_defaults(handler=_backup_check)

    p_up = sub.add_parser(
        "up",
        help=(
            "Bring a Loom cluster up: preflight → render → kubectl "
            "apply → wait for components to become ready. Composes "
            "the read-only `preflight`, `render`, and `status` "
            "commands."
        ),
    )
    p_up.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_up.add_argument(
        "--namespace",
        default=None,
        help=(
            "Kubernetes namespace. With --config, inferred from the config and "
            "an explicit value must match; otherwise defaults to loom."
        ),
    )
    p_up.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name passed through to preflight. "
            "With --config, inferred from runtime_environment and an explicit "
            "value must match. Protected environments can require backup checks."
        ),
    )
    p_up.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Recent `loom cluster backup manifest` JSON file passed "
            "through to preflight for protected environments."
        ),
    )
    p_up.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age during preflight "
            f"(default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    _add_backup_traversal_limit_arguments(p_up)
    p_up.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults (see `loom cluster render --help`)."
        ),
    )
    p_up.add_argument(
        "--rendered-manifest",
        type=Path,
        default=None,
        help=(
            "Apply an exact previously rendered manifest instead of rendering "
            "again. Protected rollouts use this to preserve immutable image digests."
        ),
    )
    p_up.add_argument(
        "--skip-preflight",
        dest="skip_preflight",
        action="store_true",
        help=(
            "Skip the preflight checks. Use sparingly — usually "
            "intended for re-applying after a known transient "
            "preflight failure."
        ),
    )
    p_up.add_argument(
        "--no-wait",
        dest="no_wait",
        action="store_true",
        help=(
            "Apply manifests and return immediately without waiting "
            "for components to reach ready state."
        ),
    )
    p_up.add_argument(
        "--migrate",
        dest="migrate",
        action="store_true",
        help=(
            "Run the DB migration Job (`alembic upgrade head`, as the "
            "sanctioned app=loom-migration Job) after apply and before the "
            "readiness wait. Off by default: `cluster up` leaves migration "
            "an explicit operator step for staging/production. Pass this for "
            "single-node local/dev bring-up, where the control-plane and "
            "service otherwise crash-loop on a schema that isn't at head."
        ),
    )
    p_up.add_argument(
        "--timeout",
        type=int,
        default=_DEFAULT_UP_TIMEOUT_SEC,
        help=(f"Wait timeout in seconds (default: {_DEFAULT_UP_TIMEOUT_SEC})."),
    )
    p_up.add_argument(
        "--poll-interval",
        dest="poll_interval",
        type=float,
        default=_DEFAULT_UP_POLL_INTERVAL_SEC,
        help=(
            f"Poll interval in seconds during the readiness wait "
            f"(default: {_DEFAULT_UP_POLL_INTERVAL_SEC})."
        ),
    )
    p_up.add_argument(
        "--recover-sandbox-deadlines",
        action="store_true",
        help=(
            "When readiness stalls on classified containerd pod "
            "sandbox deadline failures, delete only those classified pods "
            "and retry readiness once. Preflight still runs first unless "
            "--skip-preflight is explicitly supplied."
        ),
    )
    p_up.add_argument(
        "--sandbox-deadline-max-pods",
        type=int,
        default=4,
        help=(
            "Maximum classified pod sandbox deadline pods to delete during "
            "the bounded recovery retry (default: 4)."
        ),
    )
    p_up.set_defaults(handler=_up)

    p_down = sub.add_parser(
        "down",
        help=(
            "Tear down a Loom cluster: kubectl delete of the rendered "
            "manifests. PVCs and the namespace itself are preserved "
            "unless `--with-volumes` / `--delete-namespace` are passed."
        ),
    )
    p_down.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_down.add_argument(
        "--namespace",
        default=None,
        help=(
            "Kubernetes namespace. With --config, inferred from the config and "
            "an explicit value must match; otherwise defaults to loom."
        ),
    )
    p_down.add_argument(
        "--environment",
        default=None,
        help=(
            "Logical environment name. Protected environments "
            "(staging/production) require a recent backup "
            "manifest and acknowledgement before --with-volumes or "
            "--delete-namespace. With --config, inferred from "
            "runtime_environment and an explicit value must match."
        ),
    )
    p_down.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Must match the config used "
            "for `loom cluster up`; resources outside the rendered set "
            "are not touched (use --delete-namespace to nuke them all)."
        ),
    )
    p_down.add_argument(
        "--yes",
        "-y",
        dest="yes",
        action="store_true",
        help=(
            "Skip the destructive-action confirmation prompt. Intended "
            "for CI/scripted teardowns; production operators should "
            "leave the prompt on."
        ),
    )
    p_down.add_argument(
        "--with-volumes",
        dest="with_volumes",
        action="store_true",
        help=(
            "Also delete PersistentVolumeClaims in the namespace. "
            "StatefulSet PVCs survive normal teardown; pass this when "
            "you want to wipe the database + object store too. "
            "DESTRUCTIVE — data is unrecoverable."
        ),
    )
    p_down.add_argument(
        "--backup-manifest",
        default=None,
        help=(
            "Recent verified backup manifest required before destructive "
            "protected-environment operations."
        ),
    )
    p_down.add_argument(
        "--backup-max-age-hours",
        type=int,
        default=DEFAULT_BACKUP_MAX_AGE_HOURS,
        help=(
            "Maximum accepted backup manifest age for destructive protected "
            f"operations (default: {DEFAULT_BACKUP_MAX_AGE_HOURS})."
        ),
    )
    p_down.add_argument(
        "--acknowledge-data-loss",
        default=None,
        metavar="ENVIRONMENT",
        help=(
            "Explicit acknowledgement required for protected "
            "--with-volumes/--delete-namespace operations. Value must match "
            "the logical environment, for example `staging`."
        ),
    )
    p_down.add_argument(
        "--delete-namespace",
        dest="delete_namespace",
        action="store_true",
        help=(
            "Also delete the namespace. Cascades to every resource in "
            "it, including objects not produced by `cluster render`."
        ),
    )
    p_down.set_defaults(handler=_down)

    p_audit = sub.add_parser(
        "audit",
        help=(
            "Render manifests and check the public/internal boundary: "
            "no LoadBalancer/NodePort Services, no Ingress backends "
            "outside the allowlist, TLS on public Ingress, explicit "
            "Web/API paths, no unsafe hostPort declarations. Exits "
            "0 on clean, 1 on any violation."
        ),
    )
    p_audit.add_argument(
        "--config",
        default=None,
        help=(
            "Path to cluster-config.toml. Omit for all defaults. "
            "Only the Web/API ingress is public; the LLM gateway "
            "remains internal-only."
        ),
    )
    p_audit.set_defaults(handler=_audit)

    p_release_manifest = sub.add_parser(
        "release-manifest",
        help=("Render a safe release manifest artifact before applying a protected rollout."),
    )
    p_release_manifest.add_argument(
        "--config",
        default=None,
        help="Path to cluster-config.toml. Omit for all defaults.",
    )
    p_release_manifest.add_argument(
        "--environment",
        required=True,
        help="Logical rollout environment, for example staging.",
    )
    p_release_manifest.add_argument(
        "--image-tag",
        required=True,
        help="Immutable release image tag being rolled out.",
    )
    p_release_manifest.add_argument(
        "--git-sha",
        default=None,
        help="Candidate git SHA. Defaults to `git rev-parse HEAD`.",
    )
    p_release_manifest.add_argument(
        "--generated-at",
        default=None,
        help="Optional UTC timestamp override for deterministic tests.",
    )
    p_release_manifest.add_argument(
        "--expected-image-identities-json",
        default=None,
        help=(
            "Optional JSON object keyed by Deployment/container with expected "
            "image, repo_digest, or image_id values from build evidence."
        ),
    )
    p_release_manifest.add_argument(
        "--output",
        default=None,
        help="Output JSON path. Defaults to stdout.",
    )
    p_release_manifest.set_defaults(handler=_release_manifest)

    p_minio_storage = sub.add_parser(
        "minio-storage-preflight",
        help=(
            "Record MinIO /data filesystem, bucket usage, thresholds, and "
            "headroom before a large staging run."
        ),
    )
    p_minio_storage.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace containing the MinIO pod (default: loom).",
    )
    p_minio_storage.add_argument(
        "--pod",
        default="loom-minio-0",
        help="MinIO pod name to exec into (default: loom-minio-0).",
    )
    p_minio_storage.add_argument(
        "--data-path",
        default="/data",
        help="MinIO data mount path inside the pod (default: /data).",
    )
    p_minio_storage.add_argument(
        "--bucket",
        action="append",
        default=None,
        help=(
            "Bucket directory under --data-path to measure. Repeatable. "
            "Defaults to artifacts, trajectories, loom-benchmarks, and loom-tasks."
        ),
    )
    p_minio_storage.add_argument(
        "--warn-free-percent",
        type=float,
        default=DEFAULT_WARN_FREE_PERCENT,
        help=f"Warn when effective free space falls below this percent (default: {DEFAULT_WARN_FREE_PERCENT}).",
    )
    p_minio_storage.add_argument(
        "--stop-free-percent",
        type=float,
        default=DEFAULT_STOP_FREE_PERCENT,
        help=f"Stop when effective free space falls below this percent (default: {DEFAULT_STOP_FREE_PERCENT}).",
    )
    p_minio_storage.add_argument(
        "--warn-growth-bytes-per-hour",
        type=int,
        default=DEFAULT_WARN_GROWTH_BYTES_PER_HOUR,
        help=(
            "Warn when artifacts+trajectories growth exceeds this rate and "
            f"--previous-evidence is supplied (default: {DEFAULT_WARN_GROWTH_BYTES_PER_HOUR})."
        ),
    )
    p_minio_storage.add_argument(
        "--estimated-batch-bytes",
        type=int,
        default=None,
        help="Optional expected additional bytes for the requested batch.",
    )
    p_minio_storage.add_argument(
        "--previous-evidence",
        default=None,
        help="Optional prior minio-storage-preflight JSON for growth-rate alerting.",
    )
    p_minio_storage.add_argument(
        "--output",
        default=None,
        help="Write machine-readable JSON evidence to this path.",
    )
    p_minio_storage.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table).",
    )
    p_minio_storage.add_argument(
        "--allow-storage-stop-override",
        action="store_true",
        help=(
            "Exit 0 even when the preflight outcome is stop. Use only when "
            "the operator explicitly accepts the storage risk."
        ),
    )
    p_minio_storage.set_defaults(handler=_minio_storage_preflight)

    p_release_gate = sub.add_parser(
        "release-gate",
        help=("Compare a release manifest against live cluster image, render, and Alembic state."),
    )
    p_release_gate.add_argument(
        "--manifest",
        required=True,
        help="Release manifest JSON produced by `loom cluster release-manifest`.",
    )
    p_release_gate.add_argument(
        "--config",
        default=None,
        help="Cluster config path to hash and compare against the release manifest.",
    )
    p_release_gate.add_argument(
        "--rendered-manifest",
        default=None,
        help="Rendered manifest YAML path to hash and compare against the release manifest.",
    )
    p_release_gate.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_release_gate.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_release_gate.add_argument(
        "--environment",
        default=None,
        help="Optional logical environment guard; must match the manifest when set.",
    )
    p_release_gate.add_argument(
        "--minio-storage-preflight",
        default=None,
        help=(
            "JSON artifact from `loom cluster minio-storage-preflight --output`. "
            "When supplied, release-gate fails if the artifact outcome is stop."
        ),
    )
    p_release_gate.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not execute the in-cluster Alembic probe. Intended for CLI "
            "wiring tests and pre-live validation only."
        ),
    )
    p_release_gate.add_argument(
        "--format",
        choices=["table", "json", "markdown"],
        default="table",
        help="Output format. JSON for automation; Markdown for release reports.",
    )
    p_release_gate.set_defaults(handler=_release_gate)

    p_doctor = sub.add_parser(
        "doctor",
        help="Reconcile config schema against a live cluster.",
    )
    p_doctor.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace (default: loom).",
    )
    p_doctor.add_argument(
        "--context",
        default=None,
        help="kubeconfig context (default: current context).",
    )
    p_doctor.add_argument(
        "--storage-lifecycle-config",
        default=None,
        help=(
            "Optional. Path to storage-lifecycle.toml. When set, doctor "
            "also compares live bucket lifecycle rules against the "
            "rendered config and reports drift. Requires "
            "LOOM_SVC_MINIO_ACCESS_KEY + LOOM_SVC_MINIO_SECRET_KEY in "
            "the environment."
        ),
    )
    p_doctor.add_argument(
        "--storage-lifecycle-endpoint",
        default=None,
        help=(
            "Object-store endpoint URL for the lifecycle check. "
            "Defaults to $LOOM_SVC_MINIO_ENDPOINT or "
            "http://loom-minio:9000."
        ),
    )
    p_doctor.set_defaults(handler=_doctor)

    p_boot = sub.add_parser(
        "bootstrap-secrets",
        help="Emit the kubectl command to create loom-secrets from schema.",
    )
    p_boot.add_argument("--namespace", default="loom")
    p_boot.add_argument(
        "--smoke-defaults",
        action="store_true",
        help="Use test-grade placeholder values (for smoke workflows).",
    )
    p_boot.add_argument(
        "--rotate",
        action="store_true",
        help="Run each entry's `generate` command to mint fresh values.",
    )
    p_boot.add_argument(
        "--admin-secret",
        dest="admin_secret",
        action="store_true",
        help=(
            "Also emit a throwaway `loom-admin-secret` create command "
            "(requires --smoke-defaults). Off by default so callers that "
            "create it themselves don't collide; pass it for one-shot "
            "single-node local/dev bring-up where `cluster up` preflight "
            "requires the secret."
        ),
    )
    p_boot.add_argument(
        "--pgbouncer",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Emit *-db-url-pool secrets derived from direct DSN "
            "(default: schema default for pgbouncer.enabled)."
        ),
    )
    p_boot.set_defaults(handler=_bootstrap_secrets)

    p_derive = sub.add_parser(
        "derive-pool-dsn",
        help="Derive the PgBouncer pool DSN from a direct-to-Postgres DSN "
        "by rewriting host and port.",
    )
    p_derive.add_argument(
        "dsn",
        help="Direct-to-Postgres DSN (postgresql+psycopg://user:pass@host:port/db)",
    )
    p_derive.set_defaults(handler=_derive_pool_dsn)

    p_lifecycle = sub.add_parser(
        "bootstrap-storage-lifecycle",
        help=(
            "Apply storage retention rules from storage-lifecycle.toml "
            "to the configured object store. Idempotent."
        ),
    )
    p_lifecycle.add_argument(
        "--config",
        default=str(_REPO_ROOT / "config" / "storage-lifecycle.example.toml"),
        help=(
            "Path to storage-lifecycle.toml. Defaults to the bundled "
            "example so operators can preview without committing config."
        ),
    )
    p_lifecycle.add_argument(
        "--endpoint",
        default=None,
        help=(
            "Object-store endpoint URL. Defaults to "
            "$LOOM_SVC_MINIO_ENDPOINT or http://loom-minio:9000."
        ),
    )
    p_lifecycle.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Render rules to stdout as JSON without contacting the "
            "store. Useful for review before a real apply."
        ),
    )
    from loom_cli.storage_lifecycle_cmd import bootstrap_storage_lifecycle

    p_lifecycle.set_defaults(handler=bootstrap_storage_lifecycle)

    p_evidence = sub.add_parser(
        "bootstrap-evidence-paths",
        help=(
            "Emit a sudo-install script that creates operator-writable "
            "rollout evidence directories under a protected /data root."
        ),
    )
    p_evidence.add_argument(
        "--rollout-root",
        required=True,
        help=("Absolute path to the environment data root (e.g. /data/loom-staging)."),
    )
    p_evidence.add_argument(
        "--operator-user",
        default=None,
        help=("POSIX username to own the created dirs. Defaults to $USER."),
    )
    p_evidence.add_argument(
        "--evidence-paths",
        default=None,
        help=(
            "Comma-separated leaf names to create under --rollout-root. "
            "Defaults to `rollouts,evidence,logs`."
        ),
    )
    p_evidence.set_defaults(handler=_bootstrap_evidence_paths)

    p_render_migration = sub.add_parser(
        "render-migration",
        help=(
            "Render a one-off Alembic migration Job with the sanctioned "
            "app=loom-migration label. Pipe to kubectl apply -f -."
        ),
    )
    p_render_migration.add_argument(
        "--image-tag",
        required=True,
        help=(
            "Release image tag. The Job runs loom-control-plane:<image-tag> alembic upgrade head."
        ),
    )
    p_render_migration.add_argument(
        "--namespace",
        default="loom",
        help="Kubernetes namespace. Defaults to `loom`.",
    )
    p_render_migration.add_argument(
        "--job-suffix",
        default=None,
        help=(
            "Uniqueness token appended to the Job name so a re-run "
            "against the same image tag doesn't collide. Defaults to a "
            "UTC timestamp."
        ),
    )
    p_render_migration.add_argument(
        "--container-registry",
        default="",
        help=(
            "Optional registry prefix (no trailing slash) applied to the "
            "loom-control-plane image reference. Match "
            "cluster.toml's `container_registry` value on multi-node "
            "clusters that pull from an internal registry."
        ),
    )
    p_render_migration.add_argument(
        "--registry-digest",
        default="",
        help=(
            "Optional immutable sha256 manifest digest for loom-control-plane. "
            "Protected multi-node rollouts pass the digest published by the "
            "rollout image step."
        ),
    )
    p_render_migration.set_defaults(handler=_render_migration)

    from loom_cli.cluster_taskset_fence_canary import (
        add_subparser as _add_taskset_fence_canary_subparser,
    )

    _add_taskset_fence_canary_subparser(sub)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
