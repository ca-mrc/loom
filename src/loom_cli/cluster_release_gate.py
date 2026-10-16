"""Release-manifest convergence checks for protected cluster rollouts."""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Literal

from loom_cli.cluster_sandbox_deadline import (
    FAILURE_CLASS as SANDBOX_DEADLINE_FAILURE_CLASS,
)
from loom_cli.cluster_sandbox_deadline import (
    RECOVERY_KIND as SANDBOX_DEADLINE_RECOVERY_KIND,
)
from loom_cli.cluster_sandbox_deadline import (
    diagnostic_summaries,
    sandbox_deadline_diagnostics_for_deployment,
)
from loom_cli.cluster_workload_trust import (
    workload_contract_environment,
    workload_contract_from_mapping,
)

_Outcome = Literal["pass", "fail"]
_SAFE_WORKLOAD_CONTRACT_ENV_VALUES = frozenset({"internal_trusted", "True", "False"})


@dataclass(frozen=True)
class ReleaseGateCheck:
    name: str
    outcome: _Outcome
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)
    remediation: str | None = None


@dataclass(frozen=True)
class LiveAlembicHeads:
    heads: list[str]
    database_target: str = "env:LOOM_CP_DB_URL"
    error: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReleaseGateReport:
    environment: str
    namespace: str
    checks: list[ReleaseGateCheck]

    @property
    def all_pass(self) -> bool:
        return all(check.outcome == "pass" for check in self.checks)


def _get_field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _labels(obj: Any) -> dict[str, str]:
    raw = _get_field(obj, "labels", {}) or {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _deployment_selector_labels(deployment: Any, *, fallback_name: str) -> dict[str, str]:
    selector = _get_field(_get_field(deployment, "spec"), "selector")
    match_labels = _get_field(selector, "match_labels")
    if isinstance(match_labels, dict) and match_labels:
        return {str(key): str(value) for key, value in match_labels.items()}
    return {"app": fallback_name}


def _pod_matches_selector(pod: Any, selector: dict[str, str]) -> bool:
    pod_labels = _labels(_get_field(pod, "metadata"))
    return all(pod_labels.get(key) == value for key, value in selector.items())


def _pod_ready(pod: Any) -> bool:
    for condition in _get_field(_get_field(pod, "status"), "conditions", []) or []:
        if _get_field(condition, "type") == "Ready":
            return str(_get_field(condition, "status", "")).lower() == "true"
    return False


def _container_image_by_name(pod_spec: Any) -> dict[str, str]:
    images: dict[str, str] = {}
    for container in _get_field(pod_spec, "containers", []) or []:
        name = _get_field(container, "name")
        image = _get_field(container, "image")
        if name and image:
            images[str(name)] = str(image)
    return images


def _container_env_by_name(container: Any) -> dict[str, str]:
    values: dict[str, str] = {}
    for entry in _get_field(container, "env", []) or []:
        name = _get_field(entry, "name")
        value = _get_field(entry, "value")
        if name and isinstance(value, str):
            values[str(name)] = value
    return values


def _safe_workload_contract_actual(value: str | None) -> str | None:
    if value is None or value in _SAFE_WORKLOAD_CONTRACT_ENV_VALUES:
        return value
    return "[REDACTED]"


def _workload_contract_check(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    namespace: str,
) -> ReleaseGateCheck:
    """Require manifest and live loom-service trust contracts to agree exactly."""
    try:
        contract = workload_contract_from_mapping(manifest.get("workload_contract"))
    except ValueError as exc:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail=f"release manifest has invalid workload contract: {exc}",
            remediation=(
                "Rebuild the release manifest from a protected profile with the exact "
                "v1 workload contract."
            ),
        )

    violations = contract.v1_violations()
    if violations:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail="release manifest violates the v1 workload trust contract",
            evidence={"candidate_valid": False, "violations": violations},
            remediation=(
                "Use internal_trusted with all workload capability flags false, then "
                "rebuild and apply the candidate."
            ),
        )

    expected = workload_contract_environment(contract)
    actual: dict[str, str | None] = {name: None for name in expected}
    try:
        deployment = apps_v1.read_namespaced_deployment(
            name="loom-service",
            namespace=namespace,
        )
    except Exception as exc:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="fail",
            detail="could not inspect the live loom-service Deployment workload contract",
            evidence={"expected": expected, "actual": actual, "error": _exception_note(exc)},
            remediation="restore Deployment read access, then rerun the release gate",
        )

    containers = _get_field(
        _get_field(_get_field(deployment, "spec"), "template"),
        "spec",
    )
    loom_service = next(
        (
            container
            for container in _get_field(containers, "containers", []) or []
            if _get_field(container, "name") == "loom-service"
        ),
        None,
    )
    if loom_service is not None:
        live_env = _container_env_by_name(loom_service)
        actual.update(
            {name: _safe_workload_contract_actual(live_env.get(name)) for name in expected}
        )

    if actual == expected:
        return ReleaseGateCheck(
            name="workload-trust-contract",
            outcome="pass",
            detail="live loom-service workload contract matches release manifest",
            evidence={"expected": expected, "actual": actual},
        )
    return ReleaseGateCheck(
        name="workload-trust-contract",
        outcome="fail",
        detail="live loom-service workload contract does not match release manifest",
        evidence={"expected": expected, "actual": actual},
        remediation=(
            "Apply the candidate loom-service Deployment and wait for the exact v1 "
            "workload contract environment values before accepting release."
        ),
    )


def _container_status_by_name(pod: Any) -> dict[str, Any]:
    statuses: dict[str, Any] = {}
    for status in _get_field(_get_field(pod, "status"), "container_statuses", []) or []:
        name = _get_field(status, "name")
        if name:
            statuses[str(name)] = status
    return statuses


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _exception_note(exc: Exception) -> str:
    cls = type(exc).__name__
    status = getattr(exc, "status", None)
    if cls == "ApiException" and status:
        return f"k8s {status}: {str(exc)[:80]}"
    return f"{cls}: {str(exc)[:80]}"


def _is_not_found(exc: Exception) -> bool:
    if type(exc).__name__ == "ApiException" and getattr(exc, "status", None) == 404:
        return True
    return isinstance(exc, KeyError)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _sha_from_ref(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"sha256:[0-9a-fA-F]{64}", value)
    return match.group(0).lower() if match else None


def _image_identity_matches(
    *,
    expected_repo_digest: str | None,
    expected_image_id: str | None,
    live_image_id: str | None,
) -> bool:
    live_digest = _sha_from_ref(live_image_id)
    expected_digest = _sha_from_ref(expected_repo_digest)
    if expected_digest and live_digest == expected_digest:
        return True
    if expected_image_id and live_image_id:
        return live_image_id == expected_image_id
    return False


def _runtime_identity_kind(live_image_id: str | None) -> str:
    if not live_image_id:
        return "missing"
    return "runtime"


def _local_image_ref_key(image: str | None) -> str | None:
    if not image:
        return None
    for prefix in ("docker.io/library/", "library/"):
        if image.startswith(prefix):
            return image.removeprefix(prefix)
    return image


def _status_image_matches_template(
    *,
    live_image: str | None,
    pod_template_image: str | None,
) -> bool:
    if not live_image or not pod_template_image:
        return False
    return _local_image_ref_key(live_image) == _local_image_ref_key(pod_template_image)


def _hash_check(
    *,
    name: str,
    expected: str | None,
    live: str | None,
    drift_detail: str,
    remediation: str,
) -> ReleaseGateCheck:
    evidence = {
        "expected_sha256": expected,
        "live_sha256": live,
    }
    if expected and live and expected == live:
        return ReleaseGateCheck(
            name=name,
            outcome="pass",
            detail=f"{name} matches release manifest",
            evidence=evidence,
        )
    return ReleaseGateCheck(
        name=name,
        outcome="fail",
        detail=drift_detail,
        evidence=evidence,
        remediation=remediation,
    )


def _deployment_rollout_issue(deployment: Any) -> tuple[str, dict[str, Any]] | None:
    spec = _get_field(deployment, "spec")
    status = _get_field(deployment, "status")
    metadata = _get_field(deployment, "metadata")
    desired = _int_or_none(_get_field(spec, "replicas")) or 0
    generation = _int_or_none(_get_field(metadata, "generation"))
    observed_generation = _int_or_none(_get_field(status, "observed_generation"))
    updated_replicas = _int_or_none(_get_field(status, "updated_replicas"))
    ready_replicas = _int_or_none(_get_field(status, "ready_replicas"))
    total_replicas = _int_or_none(_get_field(status, "replicas"))
    evidence = {
        "generation": generation,
        "observed_generation": observed_generation,
        "desired_replicas": desired,
        "updated_replicas": updated_replicas,
        "ready_replicas": ready_replicas,
        "total_replicas": total_replicas,
    }
    if generation is not None and observed_generation is not None:
        if observed_generation < generation:
            return "Deployment rollout is not target-generation converged", evidence
    if desired > 0:
        if updated_replicas is None or updated_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
        if ready_replicas is None or ready_replicas < desired:
            return "Deployment rollout is not target-generation converged", evidence
    if (
        updated_replicas is not None
        and total_replicas is not None
        and total_replicas > updated_replicas
    ):
        return "Deployment rollout is not target-generation converged", evidence
    return None


def _list_namespace_events(core_v1: Any, namespace: str) -> list[Any]:
    try:
        return list(core_v1.list_namespaced_event(namespace=namespace).items)
    except AttributeError:
        return []
    except Exception:
        return []


def _image_identity_checks(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
) -> list[ReleaseGateCheck]:
    rendered = manifest.get("rendered_manifest", {})
    identities = rendered.get("deployment_image_identities", {})
    if not isinstance(identities, dict) or not identities:
        return [
            ReleaseGateCheck(
                name="image-identities-recorded",
                outcome="fail",
                detail="release manifest does not record expected image digests or IDs",
                evidence={
                    "deployment_image_identities": identities
                    if isinstance(identities, dict)
                    else None
                },
                remediation="regenerate the release manifest with expected immutable image identities",
            )
        ]

    pod_list_error: str | None
    try:
        pods = list(core_v1.list_namespaced_pod(namespace=namespace).items)
    except Exception as exc:
        pods = []
        pod_list_error = _exception_note(exc)
    else:
        pod_list_error = None
    events = _list_namespace_events(core_v1, namespace)

    checks: list[ReleaseGateCheck] = []
    for deployment_name, by_container in identities.items():
        if not isinstance(by_container, dict):
            continue
        try:
            deployment = apps_v1.read_namespaced_deployment(
                name=str(deployment_name),
                namespace=namespace,
            )
        except Exception as exc:
            for container_name in by_container:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="cannot inspect live Deployment image identity",
                        evidence={"error": _exception_note(exc)},
                    )
                )
            continue

        generation = _get_field(_get_field(deployment, "metadata"), "generation")
        observed_generation = _get_field(_get_field(deployment, "status"), "observed_generation")
        spec = _get_field(deployment, "spec")
        status = _get_field(deployment, "status")
        desired_replicas = _int_or_none(_get_field(spec, "replicas")) or 0
        updated_replicas = _int_or_none(_get_field(status, "updated_replicas"))
        ready_replicas = _int_or_none(_get_field(status, "ready_replicas"))
        rollout_issue = _deployment_rollout_issue(deployment)
        selector = _deployment_selector_labels(deployment, fallback_name=str(deployment_name))
        pod_template_spec = _get_field(
            _get_field(spec, "template"),
            "spec",
        )
        template_images = _container_image_by_name(pod_template_spec)
        matching_ready_pods = [
            pod for pod in pods if _pod_matches_selector(pod, selector) and _pod_ready(pod)
        ]
        sandbox_diagnostics = sandbox_deadline_diagnostics_for_deployment(
            deployment=deployment,
            fallback_name=str(deployment_name),
            pods=pods,
            events=events,
        )
        sandbox_diagnostic_evidence = diagnostic_summaries(sandbox_diagnostics)

        for container_name, expected_identity in by_container.items():
            if not isinstance(expected_identity, dict):
                expected_identity = {}
            expected_image = str(expected_identity.get("image") or "")
            expected_repo_digest = expected_identity.get("repo_digest")
            expected_image_id = expected_identity.get("image_id")
            base_evidence = {
                "deployment": str(deployment_name),
                "container": str(container_name),
                "expected_image": expected_image,
                "expected_repo_digest": expected_repo_digest,
                "expected_image_id": expected_image_id,
                "generation": generation,
                "observed_generation": observed_generation,
                "desired_replicas": desired_replicas,
                "updated_replicas": updated_replicas,
                "ready_replicas": ready_replicas,
                "pod_template_image": template_images.get(str(container_name)),
                "selector": selector,
            }
            if rollout_issue is not None:
                detail, rollout_evidence = rollout_issue
                if sandbox_diagnostics:
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail=("node runtime sandbox deadline blocked Deployment rollout"),
                            evidence={
                                **base_evidence,
                                **rollout_evidence,
                                "failure_class": SANDBOX_DEADLINE_FAILURE_CLASS,
                                "runtime_recovery": SANDBOX_DEADLINE_RECOVERY_KIND,
                                "sandbox_deadline_diagnostics": (sandbox_diagnostic_evidence),
                            },
                            remediation=(
                                "rerun `loom cluster up "
                                "--recover-sandbox-deadlines` so the "
                                "preflighted rollout path deletes only "
                                "classified sandbox-deadline pods and "
                                "retries readiness once; if it still fails, "
                                "inspect containerd/kubelet on the node"
                            ),
                        )
                    )
                    continue
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail=detail,
                        evidence={**base_evidence, **rollout_evidence},
                        remediation="wait for the Deployment rollout to converge before accepting release",
                    )
                )
                continue
            pod_template_image = template_images.get(str(container_name))
            if expected_image and pod_template_image != expected_image:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="Deployment template image does not match release manifest",
                        evidence={
                            **base_evidence,
                            "identity_strategy": "deployment-template-image",
                        },
                        remediation="apply the rendered manifest for this release before accepting rollout",
                    )
                )
                continue
            if desired_replicas == 0:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="pass",
                        detail="zero-replica Deployment template image matches release manifest",
                        evidence={
                            **base_evidence,
                            "identity_strategy": "zero-replica-template-image",
                            "zero_replica": True,
                        },
                    )
                )
                continue
            if pod_list_error is not None:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="cannot inspect target-generation Ready pods",
                        evidence={**base_evidence, "error": pod_list_error},
                    )
                )
                continue
            if not matching_ready_pods:
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="no Ready pods found for managed Deployment",
                        evidence=base_evidence,
                    )
                )
                continue

            candidate_evidence: dict[str, Any] | None = None
            saw_target_generation_pod = False
            for pod in matching_ready_pods:
                pod_spec_images = _container_image_by_name(_get_field(pod, "spec"))
                if (
                    pod_template_image
                    and pod_spec_images.get(str(container_name)) != pod_template_image
                ):
                    continue
                saw_target_generation_pod = True
                statuses = _container_status_by_name(pod)
                container_status = statuses.get(str(container_name))
                pod_name = _get_field(_get_field(pod, "metadata"), "name")
                if container_status is None:
                    candidate_evidence = {
                        **base_evidence,
                        "pod": pod_name,
                        "runtime_identity_kind": "missing",
                    }
                    continue
                live_image_id = _get_field(container_status, "image_id")
                live_image = _get_field(container_status, "image")
                runtime_identity_kind = _runtime_identity_kind(
                    str(live_image_id) if live_image_id else None
                )
                status_image_matches_template = _status_image_matches_template(
                    live_image=str(live_image) if live_image else None,
                    pod_template_image=pod_template_image,
                )
                evidence = {
                    **base_evidence,
                    "pod": pod_name,
                    "live_image": live_image,
                    "live_image_id": live_image_id,
                    "runtime_identity_kind": runtime_identity_kind,
                    "status_image_matches_template": status_image_matches_template,
                    "status_image_stale": (
                        live_image is not None
                        and pod_template_image is not None
                        and not status_image_matches_template
                    ),
                }
                candidate_evidence = evidence
                if _image_identity_matches(
                    expected_repo_digest=(
                        str(expected_repo_digest) if expected_repo_digest else None
                    ),
                    expected_image_id=(str(expected_image_id) if expected_image_id else None),
                    live_image_id=(str(live_image_id) if live_image_id else None),
                ):
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="pass",
                            detail="Ready pod image identity matches release manifest",
                            evidence={
                                **evidence,
                                "identity_strategy": "runtime-image-id-or-repo-digest",
                                "expected_digest": _sha_from_ref(
                                    str(expected_repo_digest) if expected_repo_digest else None
                                ),
                                "runtime_identity_mismatch": False,
                            },
                        )
                    )
                    break
            else:
                if not saw_target_generation_pod:
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail="no target-generation Ready pods found for managed Deployment",
                            evidence=base_evidence,
                            remediation="wait for the Deployment rollout to converge before accepting release",
                        )
                    )
                    continue
                if (
                    candidate_evidence
                    and candidate_evidence.get("runtime_identity_kind") == "missing"
                ):
                    checks.append(
                        ReleaseGateCheck(
                            name=f"image-identity:{deployment_name}/{container_name}",
                            outcome="fail",
                            detail="Ready pod is missing runtime image identity",
                            evidence=candidate_evidence,
                            remediation="wait for kubelet container status imageID before accepting release",
                        )
                    )
                    continue
                checks.append(
                    ReleaseGateCheck(
                        name=f"image-identity:{deployment_name}/{container_name}",
                        outcome="fail",
                        detail="Ready pod image identity does not match release manifest",
                        evidence={
                            **(candidate_evidence or base_evidence),
                            "identity_strategy": "runtime-image-id-or-repo-digest",
                            "expected_digest": _sha_from_ref(
                                str(expected_repo_digest) if expected_repo_digest else None
                            ),
                            "runtime_identity_mismatch": True,
                        },
                        remediation="roll the Deployment to pods built from the release manifest image digest",
                    )
                )
    return checks


def _alembic_check(
    *,
    manifest: dict[str, Any],
    live_alembic_heads: list[str],
    database_target: str,
    live_alembic_error: str | None,
    live_alembic_evidence: dict[str, Any] | None,
) -> ReleaseGateCheck:
    alembic = manifest.get("alembic", {})
    expected_heads = sorted(str(head) for head in alembic.get("expected_heads", []) or [])
    compatible_heads = sorted(
        str(head) for head in alembic.get("compatible_heads", expected_heads) or []
    )
    live_heads = sorted(str(head) for head in live_alembic_heads)
    evidence = {
        "expected_heads": expected_heads,
        "compatible_heads": compatible_heads,
        "live_heads": live_heads,
        "database_target": database_target,
    }
    if live_alembic_evidence:
        evidence.update(live_alembic_evidence)
    if live_alembic_error is not None:
        return ReleaseGateCheck(
            name="alembic-heads",
            outcome="fail",
            detail=f"could not query live DB revision state through {database_target}",
            evidence={**evidence, "error": live_alembic_error},
            remediation="restore DB connectivity, then rerun the release gate",
        )
    live_set = set(live_heads)
    expected_set = set(expected_heads)
    compatible_set = set(compatible_heads)
    if live_set == expected_set or (live_set and live_set.issubset(compatible_set)):
        return ReleaseGateCheck(
            name="alembic-heads",
            outcome="pass",
            detail=f"live DB revision matches {database_target}",
            evidence=evidence,
        )
    return ReleaseGateCheck(
        name="alembic-heads",
        outcome="fail",
        detail=f"live DB revision does not match {database_target}",
        evidence=evidence,
        remediation="run alembic upgrade head before accepting release",
    )


def _disabled_k8s_worker_check(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
) -> ReleaseGateCheck | None:
    cluster_config = manifest.get("cluster_config")
    if not isinstance(cluster_config, dict):
        return None
    if cluster_config.get("k8s_worker_enabled") is not False:
        return None

    # Dynamic-storage profiles render loom-worker as a StatefulSet
    # (#673); static-host-path profiles keep the Deployment shape.
    # Either kind lingering after k8s_worker.enabled flips false is
    # a fail. Look at both.
    deployment_found = False
    deployment_error: str | None = None
    desired_replicas: int | None = None
    ready_replicas: int | None = None
    updated_replicas: int | None = None
    workload_kind: str | None = None
    try:
        deployment = apps_v1.read_namespaced_deployment(
            name="loom-worker",
            namespace=namespace,
        )
    except Exception as exc:
        if not _is_not_found(exc):
            deployment_error = _exception_note(exc)
    else:
        deployment_found = True
        workload_kind = "Deployment"
        desired_replicas = _int_or_none(_get_field(_get_field(deployment, "spec"), "replicas")) or 0
        status = _get_field(deployment, "status")
        ready_replicas = _int_or_none(_get_field(status, "ready_replicas")) or 0
        updated_replicas = _int_or_none(_get_field(status, "updated_replicas")) or 0

    if not deployment_found and deployment_error is None:
        read_sts = getattr(apps_v1, "read_namespaced_stateful_set", None)
        if callable(read_sts):
            try:
                sts = read_sts(name="loom-worker", namespace=namespace)
            except Exception as exc:
                if not _is_not_found(exc):
                    deployment_error = _exception_note(exc)
            else:
                deployment_found = True
                workload_kind = "StatefulSet"
                desired_replicas = (
                    _int_or_none(_get_field(_get_field(sts, "spec"), "replicas")) or 0
                )
                status = _get_field(sts, "status")
                ready_replicas = _int_or_none(_get_field(status, "ready_replicas")) or 0
                updated_replicas = _int_or_none(_get_field(status, "updated_replicas")) or 0

    ready_pods: list[str] = []
    pod_list_error: str | None = None
    try:
        pods = list(core_v1.list_namespaced_pod(namespace=namespace).items)
    except Exception as exc:
        pod_list_error = _exception_note(exc)
    else:
        for pod in pods:
            labels = _labels(_get_field(pod, "metadata"))
            if labels.get("app") != "loom-worker":
                continue
            if not _pod_ready(pod):
                continue
            name = _get_field(_get_field(pod, "metadata"), "name")
            if name:
                ready_pods.append(str(name))

    evidence: dict[str, Any] = {
        "deployment": "loom-worker",
        "namespace": namespace,
        "deployment_found": deployment_found,
        "workload_kind": workload_kind,
        "desired_replicas": desired_replicas,
        "ready_replicas": ready_replicas,
        "updated_replicas": updated_replicas,
        "ready_pods": ready_pods,
    }
    if deployment_error is not None:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker prune state is unverifiable",
            evidence={**evidence, "deployment_error": deployment_error},
            remediation=(
                "restore Kubernetes Deployment/StatefulSet read access and rerun release-gate"
            ),
        )
    if pod_list_error is not None:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker pod state is unverifiable",
            evidence={**evidence, "pod_list_error": pod_list_error},
            remediation="restore Kubernetes Pod list access and rerun release-gate",
        )
    if deployment_found or ready_pods:
        return ReleaseGateCheck(
            name="disabled-k8s-worker-pruned",
            outcome="fail",
            detail="disabled k8s worker remains live",
            evidence=evidence,
            remediation=(
                "rerun `loom cluster up` with the disabled-worker profile or "
                "delete stale deploy/loom-worker (or statefulset/loom-worker) "
                "and networkpolicy/loom-worker; preserve "
                "persistentvolumeclaim/loom-worker-trajectories unless an "
                "operator explicitly approves artifact deletion"
            ),
        )
    return ReleaseGateCheck(
        name="disabled-k8s-worker-pruned",
        outcome="pass",
        detail="disabled k8s worker resources are absent",
        evidence=evidence,
    )


def _minio_storage_preflight_check(
    *,
    artifact: dict[str, Any] | None,
    artifact_path: str | None,
    artifact_error: str | None,
) -> ReleaseGateCheck | None:
    if artifact is None and artifact_error is None:
        return None
    evidence: dict[str, Any] = {"artifact": artifact_path}
    if artifact_error is not None:
        return ReleaseGateCheck(
            name="minio-storage-pressure",
            outcome="fail",
            detail="MinIO storage preflight artifact is unreadable",
            evidence={**evidence, "error": artifact_error},
            remediation=(
                "rerun `loom cluster minio-storage-preflight --output ...` "
                "and pass the JSON artifact to release-gate"
            ),
        )
    assert artifact is not None
    filesystem = artifact.get("filesystem") if isinstance(artifact, dict) else {}
    thresholds = artifact.get("thresholds") if isinstance(artifact, dict) else {}
    checks = artifact.get("checks") if isinstance(artifact, dict) else []
    if not isinstance(filesystem, dict):
        filesystem = {}
    if not isinstance(thresholds, dict):
        thresholds = {}
    outcome = str(artifact.get("outcome") or "unknown")
    evidence.update(
        {
            "outcome": outcome,
            "free_bytes": filesystem.get("free_bytes"),
            "free_percent": filesystem.get("free_percent"),
            "used_percent": filesystem.get("used_percent"),
            "warn_free_percent": thresholds.get("warn_free_percent"),
            "stop_free_percent": thresholds.get("stop_free_percent"),
            "checks": checks if isinstance(checks, list) else [],
        }
    )
    if outcome == "stop":
        return ReleaseGateCheck(
            name="minio-storage-pressure",
            outcome="fail",
            detail="MinIO storage preflight reports stop",
            evidence=evidence,
            remediation=(
                "reclaim MinIO artifacts/trajectories, provision storage, "
                "or record an explicit operator override before large runs"
            ),
        )
    detail = (
        "MinIO storage preflight warns" if outcome == "warn" else "MinIO storage preflight passed"
    )
    return ReleaseGateCheck(
        name="minio-storage-pressure",
        outcome="pass",
        detail=detail,
        evidence=evidence,
    )


def collect_release_gate_report(
    *,
    manifest: dict[str, Any],
    apps_v1: Any,
    core_v1: Any,
    namespace: str,
    rendered_manifest_sha256: str | None,
    cluster_config_sha256: str | None,
    live_alembic_heads: list[str],
    database_target: str = "env:LOOM_CP_DB_URL",
    live_alembic_error: str | None = None,
    live_alembic_evidence: dict[str, Any] | None = None,
    minio_storage_preflight_artifact: dict[str, Any] | None = None,
    minio_storage_preflight_path: str | None = None,
    minio_storage_preflight_error: str | None = None,
) -> ReleaseGateReport:
    environment = str(manifest.get("release", {}).get("environment") or "")
    expected_rendered = manifest.get("rendered_manifest", {}).get("sha256")
    expected_config = manifest.get("cluster_config", {}).get("sha256")
    checks = [
        _hash_check(
            name="rendered-manifest-sha256",
            expected=str(expected_rendered) if expected_rendered else None,
            live=rendered_manifest_sha256,
            drift_detail="rendered manifest hash drift",
            remediation="rerender from the release manifest inputs before accepting rollout",
        ),
        _hash_check(
            name="cluster-config-sha256",
            expected=str(expected_config) if expected_config else None,
            live=cluster_config_sha256,
            drift_detail="cluster config hash drift",
            remediation="use the cluster config that produced the release manifest",
        ),
    ]
    checks.extend(
        _image_identity_checks(
            manifest=manifest,
            apps_v1=apps_v1,
            core_v1=core_v1,
            namespace=namespace,
        )
    )
    checks.append(
        _workload_contract_check(
            manifest=manifest,
            apps_v1=apps_v1,
            namespace=namespace,
        )
    )
    checks.append(
        _alembic_check(
            manifest=manifest,
            live_alembic_heads=live_alembic_heads,
            database_target=database_target,
            live_alembic_error=live_alembic_error,
            live_alembic_evidence=live_alembic_evidence,
        )
    )
    disabled_k8s_worker_check = _disabled_k8s_worker_check(
        manifest=manifest,
        apps_v1=apps_v1,
        core_v1=core_v1,
        namespace=namespace,
    )
    if disabled_k8s_worker_check is not None:
        checks.append(disabled_k8s_worker_check)
    minio_storage_check = _minio_storage_preflight_check(
        artifact=minio_storage_preflight_artifact,
        artifact_path=minio_storage_preflight_path,
        artifact_error=minio_storage_preflight_error,
    )
    if minio_storage_check is not None:
        checks.append(minio_storage_check)
    return ReleaseGateReport(
        environment=environment,
        namespace=namespace,
        checks=checks,
    )


def _redact(text: str) -> str:
    text = re.sub(
        r"postgres(?:ql)?(?:\+[^:]+)?://[^\s\"']+",
        "postgresql://<redacted>",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?i)(password|token|secret)=\S+", r"\1=<redacted>", text)
    return text


def query_live_alembic_heads(
    *,
    namespace: str,
    context: str | None = None,
    runner: Any | None = None,
    timeout_sec: int = 60,
) -> LiveAlembicHeads:
    script = (
        "import json, os\n"
        "from alembic.runtime.migration import MigrationContext\n"
        "from sqlalchemy import create_engine\n"
        "db_url = os.environ['LOOM_CP_DB_URL']\n"
        "engine = create_engine(db_url, connect_args={'connect_timeout': 10})\n"
        "with engine.connect() as conn:\n"
        "    heads = sorted(MigrationContext.configure(conn).get_current_heads())\n"
        "print(json.dumps({'database_target':'env:LOOM_CP_DB_URL','heads':heads}))\n"
    )
    cmd = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        "deploy/loom-control-plane",
    ]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(["--", "python", "-c", script])
    if runner is None:
        try:
            proc = subprocess.run(
                cmd,
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            return LiveAlembicHeads(
                heads=[],
                error=f"kubectl exec timed out after {timeout_sec}s",
                evidence={
                    "command": [
                        "kubectl",
                        "exec",
                        "-n",
                        namespace,
                        "deploy/loom-control-plane",
                        "--",
                        "python",
                        "-c",
                        "<script>",
                    ],
                    "stderr": _redact(_safe_text(exc.stderr).strip())[:500],
                    "stdout": _redact(_safe_text(exc.output).strip())[:500],
                },
            )
        returncode, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
    else:
        try:
            returncode, stdout, stderr = runner(cmd)
        except subprocess.TimeoutExpired as exc:
            return LiveAlembicHeads(
                heads=[],
                error=f"kubectl exec timed out after {timeout_sec}s",
                evidence={
                    "command": [
                        "kubectl",
                        "exec",
                        "-n",
                        namespace,
                        "deploy/loom-control-plane",
                        "--",
                        "python",
                        "-c",
                        "<script>",
                    ],
                    "stderr": _redact(_safe_text(exc.stderr).strip())[:500],
                    "stdout": _redact(_safe_text(exc.output).strip())[:500],
                },
            )
    if returncode != 0:
        return LiveAlembicHeads(
            heads=[],
            error=f"kubectl exec exited {returncode}",
            evidence={
                "command": [
                    "kubectl",
                    "exec",
                    "-n",
                    namespace,
                    "deploy/loom-control-plane",
                    "--",
                    "python",
                    "-c",
                    "<script>",
                ],
                "stderr": _redact(stderr.strip())[:500],
                "stdout": _redact(stdout.strip())[:500],
            },
        )
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return LiveAlembicHeads(
            heads=[],
            error=f"invalid JSON from Alembic probe: {exc}",
            evidence={"stdout": _redact(stdout.strip())[:500]},
        )
    heads = data.get("heads", [])
    if not isinstance(heads, list):
        return LiveAlembicHeads(
            heads=[],
            error="Alembic probe JSON did not contain a heads list",
            evidence={"stdout": _redact(stdout.strip())[:500]},
        )
    database_target = data.get("database_target") or "env:LOOM_CP_DB_URL"
    return LiveAlembicHeads(
        heads=sorted(str(head) for head in heads),
        database_target=str(database_target),
        evidence={
            "database_target": str(database_target),
            "heads": sorted(str(head) for head in heads),
        },
    )


def release_gate_report_to_dict(report: ReleaseGateReport) -> dict[str, Any]:
    return {
        "environment": report.environment,
        "namespace": report.namespace,
        "all_pass": report.all_pass,
        "component_evidence": build_component_evidence_rows(report),
        "checks": [
            {
                "name": check.name,
                "outcome": check.outcome,
                "detail": check.detail,
                "evidence": check.evidence,
                "remediation": check.remediation,
            }
            for check in report.checks
        ],
    }


def format_release_gate_json(report: ReleaseGateReport) -> str:
    return json.dumps(release_gate_report_to_dict(report), indent=2, sort_keys=True) + "\n"


def _first_text(*values: Any) -> str:
    for value in values:
        if value is not None and str(value) != "":
            return str(value)
    return ""


def _replica_readiness(evidence: dict[str, Any]) -> str:
    desired = evidence.get("desired_replicas")
    ready = evidence.get("ready_replicas")
    if desired is None:
        return ""
    if ready is None:
        return f"?/{desired} ready"
    return f"{ready}/{desired} ready"


def _kubernetes_component_row(check: ReleaseGateCheck) -> dict[str, Any] | None:
    if not check.name.startswith("image-identity:"):
        return None
    evidence = check.evidence
    deployment = evidence.get("deployment")
    container = evidence.get("container")
    if deployment is None or container is None:
        component = check.name.removeprefix("image-identity:")
    else:
        component = f"{deployment}/{container}"
    row_evidence = []
    if evidence.get("pod"):
        row_evidence.append(f"pod={evidence['pod']}")
    if evidence.get("identity_strategy"):
        row_evidence.append(f"strategy={evidence['identity_strategy']}")
    if evidence.get("zero_replica"):
        row_evidence.append("zero-replica")
    return {
        "surface": "kubernetes",
        "component": component,
        "expected_release": _first_text(evidence.get("expected_image")),
        "expected_digest": _first_text(
            evidence.get("expected_repo_digest"),
            evidence.get("expected_image_id"),
            evidence.get("expected_digest"),
        ),
        "live_release": _first_text(
            evidence.get("live_image"),
            evidence.get("pod_template_image"),
        ),
        "live_digest": _first_text(evidence.get("live_image_id")),
        "generation": evidence.get("generation"),
        "readiness": _replica_readiness(evidence),
        "restart_crash_reason": _first_text(
            evidence.get("restart_crash_reason"),
            evidence.get("waiting_reason"),
            evidence.get("terminated_reason"),
        ),
        "evidence": row_evidence,
        "outcome": check.outcome,
        "detail": check.detail,
    }


def _minio_storage_component_row(check: ReleaseGateCheck) -> dict[str, Any] | None:
    if check.name != "minio-storage-pressure":
        return None
    evidence = check.evidence
    row_evidence = []
    if evidence.get("artifact"):
        row_evidence.append(str(evidence["artifact"]))
    free_percent = evidence.get("free_percent")
    stop_free_percent = evidence.get("stop_free_percent")
    readiness = (
        f"free={free_percent}% stop={stop_free_percent}%"
        if free_percent is not None and stop_free_percent is not None
        else str(evidence.get("outcome") or "")
    )
    return {
        "surface": "object-store",
        "component": "minio-storage",
        "expected_release": f"stop_free_percent={stop_free_percent}",
        "expected_digest": "",
        "live_release": _first_text(evidence.get("outcome")),
        "live_digest": "",
        "generation": "",
        "readiness": readiness,
        "restart_crash_reason": "" if check.outcome == "pass" else check.detail,
        "evidence": row_evidence,
        "outcome": check.outcome,
        "detail": check.detail,
    }


def build_component_evidence_rows(report: ReleaseGateReport) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for check in report.checks:
        kubernetes_row = _kubernetes_component_row(check)
        if kubernetes_row is not None:
            rows.append(kubernetes_row)
        minio_storage_row = _minio_storage_component_row(check)
        if minio_storage_row is not None:
            rows.append(minio_storage_row)
    return rows


def _markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        text = ", ".join(str(item) for item in value if item is not None and str(item) != "")
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _code_cell(value: Any) -> str:
    text = _markdown_cell(value)
    if not text:
        return ""
    return f"`{text.replace('`', '')}`"


def _release_identity_cell(row: dict[str, Any], *, release_key: str, digest_key: str) -> str:
    release = _first_text(row.get(release_key))
    digest = _first_text(row.get(digest_key))
    if release and digest and digest != release:
        return f"{release} / {digest}"
    return _first_text(release, digest)


def format_release_gate_markdown(report: ReleaseGateReport) -> str:
    lines = [
        f"Release gate: `{report.environment}` / namespace `{report.namespace}`",
        "",
        "| Surface | Component | Expected | Live | Generation/job | Readiness | Restart/crash | Evidence | Result |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in build_component_evidence_rows(report):
        expected = _release_identity_cell(
            row,
            release_key="expected_release",
            digest_key="expected_digest",
        )
        live = _release_identity_cell(
            row,
            release_key="live_release",
            digest_key="live_digest",
        )
        result = "PASS" if row.get("outcome") == "pass" else "FAIL"
        lines.append(
            "| "
            + " | ".join(
                [
                    _markdown_cell(row.get("surface")),
                    _markdown_cell(row.get("component")),
                    _code_cell(expected),
                    _code_cell(live),
                    _code_cell(row.get("generation")),
                    _markdown_cell(row.get("readiness")),
                    _markdown_cell(row.get("restart_crash_reason")),
                    _code_cell(row.get("evidence")),
                    result,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def format_release_gate_table(report: ReleaseGateReport) -> str:
    lines = [
        f"environment: {report.environment}",
        f"namespace: {report.namespace}",
        "",
        f"{'CHECK':<42} {'OUTCOME':<8} DETAIL",
    ]
    for check in report.checks:
        lines.append(f"{check.name:<42} {check.outcome:<8} {check.detail}")
        if check.remediation and check.outcome == "fail":
            lines.append(f"  remediation: {check.remediation}")
    return "\n".join(lines) + "\n"
