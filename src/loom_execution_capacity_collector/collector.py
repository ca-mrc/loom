"""One complete fail-closed provider/cluster observation transaction."""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime
from typing import Any

from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import (
    CapacityObservationReceipt,
    CapacityObservationV1,
    CapacityPlacement,
    KubernetesCapacitySnapshot,
    NodeStateCounts,
    NodeTemplateSample,
    ProviderCapacitySnapshot,
    ResourceTotals,
)
from loom_execution_capacity_collector.control_plane import CapacityControlPlaneClient
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader
from loom_execution_capacity_collector.nebius import NebiusCapacityReader


class CapacityCollectionError(RuntimeError):
    """Complete source snapshots disagree on their shared target identity."""


def _matching_samples(
    provider: ProviderCapacitySnapshot, kubernetes: KubernetesCapacitySnapshot
) -> list[NodeTemplateSample]:
    """Never attach an old or foreign Node sample to a newly changed template."""
    group = provider.node_group
    if group is None or provider.autoscaler_state != "ready":
        return []
    template = group.template
    expected_version = (
        str(template.get("kubernetes_version", "")).removeprefix("v").removesuffix(".x").split(".")
    )
    if not expected_version[0]:
        return []
    effects = {
        "NO_SCHEDULE": "NoSchedule",
        "NO_EXECUTE": "NoExecute",
        "PREFER_NO_SCHEDULE": "PreferNoSchedule",
    }

    def taints(rows: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
        return {
            (row["key"], row.get("value", ""), effects.get(row["effect"], row["effect"]))
            for row in rows
        }

    matched = []
    for sample in kubernetes.template_samples:
        context = kubernetes.node_templates.get(sample.node_uid)
        if context is None:
            continue
        labels = context["labels"]
        version = sample.kubelet_version.removeprefix("v").split(".")
        if (
            labels.get("nebius.com/node-group-id") != group.id
            or labels.get("node.kubernetes.io/instance-type") != template.get("platform")
            or any(labels.get(key) != value for key, value in template.get("labels", {}).items())
            or version[: len(expected_version)] != expected_version
            or sample.pod_slots != template.get("max_pods")
            or taints(context["taints"]) != taints(template.get("taints", []))
        ):
            continue
        os_name = re.sub(r"[^a-z0-9]", "", str(template.get("os", "")).lower())
        if os_name and not re.sub(r"[^a-z0-9]", "", context["os_image"].lower()).startswith(
            os_name
        ):
            continue
        capacity = context["capacity"]
        if capacity["cpu_millis"] != group.raw_node.cpu_millis:
            continue
        # Preset labels are not exposed on every Nebius Node (the actual CPU
        # sample exposes platform only). Reject larger/old nodes; never inflate
        # the observed allocatable up to the new provider shape.
        if any(
            capacity[key] > getattr(group.raw_node, key)
            or getattr(sample.allocatable, key) > capacity[key]
            for key in ("cpu_millis", "memory_mib", "storage_mib")
        ):
            continue
        matched.append(sample)
    return matched


async def collect_capacity_observation(
    settings: ExecutionCapacityCollectorSettings,
    *,
    control_plane: Any | None = None,
    provider: Any | None = None,
    kubernetes: Any | None = None,
    now: datetime | None = None,
) -> CapacityObservationReceipt:
    """Capture all sources before publishing one immutable observation."""

    owns_control_plane = control_plane is None
    owns_provider = provider is None
    owns_kubernetes = kubernetes is None
    control_plane = control_plane or CapacityControlPlaneClient(
        origin=settings.control_plane_url,
        bearer_token_file=settings.control_plane_bearer_token_file,
        timeout_seconds=settings.request_timeout_seconds,
        attempts=settings.request_attempts,
    )
    try:
        provider = provider or NebiusCapacityReader(settings)
        kubernetes = kubernetes or InClusterKubernetesCapacityReader(
            request_timeout_seconds=settings.request_timeout_seconds,
            connection=settings.kubernetes_connection,
        )
        policy = await control_plane.fetch_policy(
            target_id=settings.target_id,
            pool_id=settings.pool_id,
        )
        if policy.target_scope is not None and (
            policy.target_scope.owner_target_id != settings.target_id
            or policy.target_scope.namespace_name != settings.namespace
        ):
            raise CapacityCollectionError("capacity collector target scope does not match settings")
        provider_snapshot, kubernetes_snapshot = await asyncio.gather(
            provider.capture(policy),
            kubernetes.capture(
                namespace=settings.namespace,
                target_id=settings.target_id,
                node_label_selector=settings.node_label_selector,
                target_scope=policy.target_scope,
            ),
        )
        # Nebius quota usage can lag node-group inventory while autoscaling. Use
        # the complete node-group snapshot as a conservative floor so the
        # observation never overstates headroom during that convergence window.
        raw_node = (
            provider_snapshot.node_group.raw_node
            if provider_snapshot.node_group is not None
            else ResourceTotals(
                cpu_millis=policy.node_cpu_millis,
                memory_mib=policy.node_memory_mib,
                storage_mib=policy.node_storage_mib,
            )
        )
        provider_used_nodes = max(provider_snapshot.used_nodes, provider_snapshot.node_count)
        provider_used_vcpu_millis = max(
            provider_snapshot.used_vcpu_millis,
            provider_snapshot.node_count * raw_node.cpu_millis,
        )
        provider_used_memory_mib = max(
            provider_snapshot.used_memory_mib,
            provider_snapshot.node_count * raw_node.memory_mib,
        )
        provider_used_storage_mib = max(
            provider_snapshot.used_storage_mib,
            provider_snapshot.node_count * raw_node.storage_mib,
        )
        if provider_snapshot.autoscaler_state == "ready" and (
            kubernetes_snapshot.active_nodes != provider_snapshot.node_count
            or kubernetes_snapshot.ready_nodes != provider_snapshot.ready_node_count
        ):
            raise CapacityCollectionError(
                "ready provider and Kubernetes node inventories do not agree"
            )
        observed_at = (now or datetime.now(UTC)).astimezone(UTC)
        active_nodes = max(
            kubernetes_snapshot.active_nodes,
            provider_snapshot.node_count,
            provider_snapshot.target_node_count,
        )
        missing_nodes = max(0, active_nodes - kubernetes_snapshot.active_nodes)
        deleting_nodes = max(
            0,
            kubernetes_snapshot.active_nodes - provider_snapshot.target_node_count,
        )
        non_ready_nodes = max(
            0,
            kubernetes_snapshot.active_nodes - kubernetes_snapshot.ready_nodes - deleting_nodes,
        )
        missing_desired_nodes = max(
            0,
            provider_snapshot.target_node_count - kubernetes_snapshot.active_nodes,
        )
        stalled = provider_snapshot.autoscaler_state == "stalled"
        node_states = NodeStateCounts(
            desired=provider_snapshot.target_node_count,
            creating=missing_desired_nodes + (0 if stalled else non_ready_nodes),
            ready=kubernetes_snapshot.ready_nodes,
            failed=non_ready_nodes if stalled else 0,
            deleting=deleting_nodes,
        )
        provisioned_cpu = (
            kubernetes_snapshot.provisioned.cpu_millis + missing_nodes * policy.node_cpu_millis
        )
        provisioned_memory = (
            kubernetes_snapshot.provisioned.memory_mib + missing_nodes * policy.node_memory_mib
        )
        provisioned_storage = (
            kubernetes_snapshot.provisioned.storage_mib + missing_nodes * policy.node_storage_mib
        )
        placement = None
        if provider_snapshot.node_group is not None:
            # Kubernetes capacity is filesystem/OS-visible, not the billable VM
            # and disk allocation. Keep this charge separate from Pod placement.
            provisioned_cpu = active_nodes * raw_node.cpu_millis
            provisioned_memory = active_nodes * raw_node.memory_mib
            provisioned_storage = active_nodes * raw_node.storage_mib
            placement = CapacityPlacement(
                build_concurrency_limit=settings.build_concurrency_limit,
                target_scope=policy.target_scope,
                quota_resources=provider_snapshot.quota_resources,
                node_group=provider_snapshot.node_group,
                nodes=kubernetes_snapshot.nodes,
                pending_pods=kubernetes_snapshot.pending_pods,
                daemonsets=kubernetes_snapshot.daemonsets,
                template_samples=_matching_samples(provider_snapshot, kubernetes_snapshot),
            )
        identity = {
            "schema_version": "loom.execution-capacity-collector-source.v1",
            "target_id": settings.target_id,
            "policy_version": policy.version,
            "provider": provider_snapshot.source_versions,
            "kubernetes": kubernetes_snapshot.source_versions,
            "observed_at": observed_at.isoformat(),
        }
        if policy.target_scope is not None:
            identity["target_scope"] = policy.target_scope.model_dump(mode="json")
        observation = CapacityObservationV1(
            target_id=settings.target_id,
            source=settings.source,
            source_version=canonical_digest(identity),
            observed_at=observed_at,
            provider_capacity_state=provider_snapshot.provider_capacity_state,
            provider_capacity_reason=provider_snapshot.provider_capacity_reason,
            autoscaler_state=provider_snapshot.autoscaler_state,
            autoscaler_reason=provider_snapshot.autoscaler_reason,
            provider_quota_nodes=provider_snapshot.quota_nodes,
            provider_quota_vcpu_millis=provider_snapshot.quota_vcpu_millis,
            provider_quota_memory_mib=provider_snapshot.quota_memory_mib,
            provider_quota_storage_mib=provider_snapshot.quota_storage_mib,
            provider_used_nodes=provider_used_nodes,
            provider_used_vcpu_millis=provider_used_vcpu_millis,
            provider_used_memory_mib=provider_used_memory_mib,
            provider_used_storage_mib=provider_used_storage_mib,
            active_nodes=active_nodes,
            node_states=node_states,
            provisioned_vcpu_millis=provisioned_cpu,
            provisioned_memory_mib=provisioned_memory,
            provisioned_storage_mib=provisioned_storage,
            allocatable_cpu_millis=kubernetes_snapshot.allocatable.cpu_millis,
            allocatable_memory_mib=kubernetes_snapshot.allocatable.memory_mib,
            allocatable_storage_mib=kubernetes_snapshot.allocatable.storage_mib,
            requested_cpu_millis=kubernetes_snapshot.requested.cpu_millis,
            requested_memory_mib=kubernetes_snapshot.requested.memory_mib,
            requested_storage_mib=kubernetes_snapshot.requested.storage_mib,
            pending_jobs=kubernetes_snapshot.pending_jobs,
            unschedulable_jobs=kubernetes_snapshot.unschedulable_jobs,
            image_pull_backoff_jobs=kubernetes_snapshot.image_pull_backoff_jobs,
            pending_reasons=kubernetes_snapshot.pending_reasons,
            placement=placement,
        )
        return await control_plane.publish(observation)
    finally:
        if owns_kubernetes and kubernetes is not None:
            await kubernetes.close()
        if owns_provider and provider is not None:
            await provider.close()
        if owns_control_plane:
            await control_plane.close()


__all__ = ["CapacityCollectionError", "collect_capacity_observation"]
