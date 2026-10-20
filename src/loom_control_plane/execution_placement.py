"""Per-node placement arithmetic shared by transactional capacity admission.

These are conservative request/slot calculations, not a cloud reservation or a
replacement for the Kubernetes scheduler. Provider charges use the raw preset;
Pod fit uses observed allocatable resources after resident DaemonSets.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeTemplateSample,
    QuotaResource,
    ResourceTotals,
)


class PlacementUnavailableError(ValueError):
    pass


def quota_identity(quota: QuotaResource) -> tuple[str, ...]:
    return (quota.parent_id, quota.region, quota.service, quota.name, quota.unit)


def require_provider_quota_headroom(
    current: CapacityPlacement, *, additional_nodes: int,
    peers: Iterable[tuple[CapacityPlacement, int]],
) -> None:
    """Check qualified physical projections, independent of any database.

    Callers must serialize every shared quota identity and qualify observation
    scope/freshness and placement first. Account usage is not summed across
    snapshots: use its highest observation, floored by distinct native groups,
    then add unobserved node charges exactly once per physical group.
    """
    if isinstance(additional_nodes, bool) or not isinstance(additional_nodes, int) or additional_nodes < 0:
        raise ValueError("additional_nodes must be a nonnegative integer")

    def amounts(placement: CapacityPlacement) -> dict[str, int]:
        raw = placement.node_group.raw_node
        return {"nodes": 1, "vcpu": raw.cpu_millis, "memory": raw.memory_mib, "storage": raw.storage_mib}

    charges = {key: additional_nodes * value for key, value in amounts(current).items()}
    native_floor = {key: current.node_group.node_count * value for key, value in amounts(current).items()}
    used = {key: quota.used for key, quota in current.quota_resources.items()}
    limits = {key: quota.limit for key, quota in current.quota_resources.items()}
    groups = {current.node_group.id}
    node_ids = {node.uid for node in current.nodes}
    provider_ids = {node.provider_id for node in current.nodes}
    for peer, count in peers:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("additional_nodes must be a nonnegative integer")
        shared = {
            key for key, quota in current.quota_resources.items()
            if key in peer.quota_resources and quota_identity(quota) == quota_identity(peer.quota_resources[key])
        }
        if not shared:
            continue
        if (peer.node_group.id in groups or node_ids & {node.uid for node in peer.nodes}
                or provider_ids & {node.provider_id for node in peer.nodes}):
            raise PlacementUnavailableError("execution_capacity_overlapping_targets")
        groups.add(peer.node_group.id)
        node_ids.update(node.uid for node in peer.nodes)
        provider_ids.update(node.provider_id for node in peer.nodes)
        for key in shared:
            charges[key] += count * amounts(peer)[key]
            native_floor[key] += peer.node_group.node_count * amounts(peer)[key]
            used[key] = max(used[key], peer.quota_resources[key].used)
            limits[key] = min(limits[key], peer.quota_resources[key].limit)
    for key in ("nodes", "vcpu", "memory", "storage"):
        if key not in current.quota_resources:
            continue
        if max(used[key], native_floor[key]) + charges[key] > limits[key]:
            raise PlacementUnavailableError(f"execution_capacity_provider_quota_{key}_exceeded")


def vector(value: ResourceTotals) -> tuple[int, int, int]:
    return value.cpu_millis, value.memory_mib, value.storage_mib


def _mentions_label(value: Any, labels: set[str]) -> bool:
    """Conservatively include selectors, affinity and topology label references."""
    if isinstance(value, dict):
        return any(key in labels or _mentions_label(child, labels) for key, child in value.items())
    if isinstance(value, list):
        return any(_mentions_label(child, labels) for child in value)
    return isinstance(value, str) and value in labels


def _compatible_node_template(current: CapacityPlacement, old: CapacityPlacement) -> bool:
    new_template, old_template = current.node_group.template, old.node_group.template
    new_labels, old_labels = new_template.get("labels", {}), old_template.get("labels", {})
    if any(not isinstance(labels, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()
    ) for labels in (new_labels, old_labels)):
        return False
    if new_template == old_template:
        return True
    if {k: v for k, v in new_template.items() if k != "labels"} != {
        k: v for k, v in old_template.items() if k != "labels"
    }:
        return False
    if any(key not in new_labels or new_labels[key] != value for key, value in old_labels.items()):
        return False  # Removing/changing labels still needs a matching observed sample.
    added = set(new_labels) - set(old_labels)
    return not any(_mentions_label(daemon.scheduling, added) for daemon in current.daemonsets)


def _compatible_daemonsets(current: CapacityPlacement, old: CapacityPlacement) -> bool:
    """Packing fingerprint: same DaemonSet set, requests, and scheduling.

    Ignore ``generation``. Controller revision counters bump on any spec write
    and must not invalidate measured cold-start samples when reserved resources
    and placement rules are unchanged.
    """
    current_by_uid = {row.uid: row for row in current.daemonsets}
    old_by_uid = {row.uid: row for row in old.daemonsets}
    if current_by_uid.keys() != old_by_uid.keys():
        return False
    return all(
        current_by_uid[uid].requests == old_by_uid[uid].requests
        and current_by_uid[uid].scheduling == old_by_uid[uid].scheduling
        for uid in current_by_uid
    )


def compatible_template(current: CapacityPlacement, old: CapacityPlacement) -> bool:
    return (
        current.node_group.id == old.node_group.id
        and current.node_group.raw_node == old.node_group.raw_node
        and _compatible_daemonsets(current, old)
        and _compatible_node_template(current, old)
    )


def cold_sample(
    placement: CapacityPlacement,
    historical: Iterable[CapacityPlacement] = (),
) -> NodeTemplateSample | None:
    samples = list(placement.template_samples)
    if not samples:
        for old in historical:
            if compatible_template(placement, old) and old.template_samples:
                samples = old.template_samples
                break
    if not samples:
        return None
    # A conservative componentwise minimum avoids choosing a lucky node sample.
    first = samples[0]
    allocatable = ResourceTotals(
        cpu_millis=min(row.allocatable.cpu_millis for row in samples),
        memory_mib=min(row.allocatable.memory_mib for row in samples),
        storage_mib=min(row.allocatable.storage_mib for row in samples),
    )
    resident = ResourceTotals(
        cpu_millis=max(row.daemonset_requests.cpu_millis for row in samples),
        memory_mib=max(row.daemonset_requests.memory_mib for row in samples),
        storage_mib=max(row.daemonset_requests.storage_mib for row in samples),
    )
    return first.model_copy(
        update={
            "allocatable": allocatable,
            "daemonset_requests": resident,
            "pod_slots": min(row.pod_slots for row in samples),
            "daemonset_slots": max(row.daemonset_slots for row in samples),
        }
    )


@dataclass(frozen=True)
class PlacementPlan:
    # Cold nodes include already creating nodes that have no Ready Node yet.
    cold_nodes: int
    additional_nodes: int
    observed_lease_ids: frozenset[str]


@dataclass(frozen=True)
class PlacementConstraint:
    """Frozen Kubernetes node affinity for a still-unobserved reservation."""

    node_uids: frozenset[str]
    allow_cold: bool = False


def plan_placement(
    placement: CapacityPlacement,
    demands: Iterable[tuple[str, ResourceTotals]],
    *,
    sample: NodeTemplateSample | None,
    constraints: dict[str, PlacementConstraint] | None = None,
) -> PlacementPlan:
    bins: list[list[int]] = []
    bin_nodes: list[str | None] = []
    constraints = constraints or {}
    observed: set[str] = set()
    for node in placement.nodes:
        observed.update(f"{pod.lease_id}:{pod.generation}" for pod in node.managed_pods)
        if node.ready and not node.unschedulable and not node.deleting:
            bin_nodes.append(node.uid)
            bins.append(
                [
                    *(
                        max(0, a - b)
                        for a, b in zip(
                            vector(node.allocatable), vector(node.requested), strict=True
                        )
                    ),
                    max(0, node.pod_slots - node.used_pod_slots),
                ]
            )
    queued: dict[str, ResourceTotals] = {}
    for pod in placement.pending_pods:
        key = f"{pod.lease_id}:{pod.generation}"
        if key in queued or key in observed:
            raise PlacementUnavailableError("execution_capacity_duplicate_pod_identity")
        queued[key] = pod.requests
    for lease_id, resources in demands:
        if lease_id not in observed:
            # The durable envelope may be more conservative than a visible Pod.
            prior = queued.get(lease_id)
            queued[lease_id] = (
                resources
                if prior is None
                else ResourceTotals(
                    **{
                        key: max(getattr(prior, key), getattr(resources, key))
                        for key in ("cpu_millis", "memory_mib", "storage_mib")
                    }
                )
            )
    cold = (
        None
        if sample is None
        else [
            *(
                max(0, a - b)
                for a, b in zip(
                    vector(sample.allocatable), vector(sample.daemonset_requests), strict=True
                )
            ),
            max(0, sample.pod_slots - sample.daemonset_slots),
        ]
    )
    cold_count = 0
    pending_node_uids = {
        node.uid for node in placement.nodes
        if not node.ready and not node.unschedulable and not node.deleting
    }
    # A future bin can count an already-provisioned, NotReady node only if
    # every demand placed in that bin is allowed to land on that exact node.
    cold_pending: dict[int, set[str]] = {}

    def demand_order(item: tuple[str, ResourceTotals]) -> tuple[int | str, ...]:
        key, resources = item
        constraint = constraints.get(key)
        # Pack immovable bindings before flexible bootstrap work, then prefer
        # smaller allowed sets. Unconstrained best-fit ordering stays unchanged.
        priority = 0 if constraint is None else 1 if constraint.allow_cold else 2
        choices = 0 if constraint is None else -len(constraint.node_uids)
        return priority, choices, *vector(resources), key

    # Stable best-fit decreasing avoids summing disjoint CPU and memory holes.
    # Preserve a deterministic order across transaction retries.
    for key, resources in sorted(queued.items(), key=demand_order, reverse=True):
        request = [*vector(resources), 1]
        constraint = constraints.get(key)
        fitting = [
            i
            for i, free in enumerate(bins)
            if all(a <= b for a, b in zip(request, free, strict=True))
            and (constraint is None or bin_nodes[i] in constraint.node_uids
                 or (bin_nodes[i] is None and constraint.allow_cold))
        ]
        if fitting:
            index = min(
                fitting, key=lambda i: tuple(b - a for a, b in zip(request, bins[i], strict=True))
            )
        else:
            if constraint is not None and not constraint.allow_cold:
                raise PlacementUnavailableError("execution_capacity_bound_node_unavailable")
            if cold is None:
                raise PlacementUnavailableError("execution_capacity_node_allocatable_unknown")
            if any(a > b for a, b in zip(request, cold, strict=True)):
                raise PlacementUnavailableError(
                    "execution_capacity_workload_exceeds_node_allocatable"
                )
            bins.append(cold.copy())
            bin_nodes.append(None)
            index = len(bins) - 1
            cold_pending[index] = pending_node_uids.copy()
            cold_count += 1
        if index in cold_pending and constraint is not None:
            cold_pending[index].intersection_update(constraint.node_uids)
        bins[index] = [b - a for a, b in zip(request, bins[index], strict=True)]
    # Distinct compatible NotReady nodes each offset at most one future bin.
    # Anonymous provider nodes have no excluded Kubernetes name yet and can
    # offset remaining bins. Never spend an excluded observed node as credit.
    matched: dict[str, int] = {}

    def match_pending(index: int, seen: set[str]) -> bool:
        for uid in sorted(cold_pending[index]):
            if uid in seen:
                continue
            seen.add(uid)
            prior = matched.get(uid)
            if prior is None or match_pending(prior, seen):
                matched[uid] = index
                return True
        return False

    pending_observed = (sum(match_pending(index, set()) for index in cold_pending)
                        if constraints else min(cold_count, len(pending_node_uids)))
    pending_anonymous = max(0, placement.node_group.node_count - len(placement.nodes))
    return PlacementPlan(
        cold_nodes=cold_count,
        additional_nodes=max(0, cold_count - pending_observed - pending_anonymous),
        observed_lease_ids=frozenset(observed),
    )
