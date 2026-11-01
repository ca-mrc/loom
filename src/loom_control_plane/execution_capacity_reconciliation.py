"""Reconcile lagging inventories only with committed, exact-Pod deletion proof.

The provider's VM/quota inventory remains unchanged. A release removes only the
observed Pod's own request and slot, never a lease's estimated resource envelope.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionEvent,
    ServiceExecutionLease,
    TaskImageMaterializationAttempt,
)
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    ManagedPodPlacement,
    ResourceTotals,
)


@dataclass(frozen=True)
class ReleasedPod:
    lease_id: str
    generation: int
    pod_uid: str
    confirmed_at: datetime


def reconcile_released_pods(
    placement: CapacityPlacement, releases: list[ReleasedPod], *, now: datetime,
) -> CapacityPlacement:
    """Copy the inventory; never mutate immutable observations or spend a UID twice."""
    pods = [pod for node in placement.nodes for pod in node.managed_pods] + placement.pending_pods
    identities = Counter((pod.lease_id, pod.generation) for pod in pods)
    uids = Counter(pod.uid for pod in pods)
    released = {(row.lease_id, row.generation, row.pod_uid) for row in releases
                if row.confirmed_at.utcoffset() is not None and row.confirmed_at <= now}

    def matches(pod: ManagedPodPlacement) -> bool:
        return (identities[pod.lease_id, pod.generation] == 1 and uids[pod.uid] == 1
                and (pod.lease_id, pod.generation, pod.uid) in released)

    nodes = []
    for node in placement.nodes:
        removed = [pod for pod in node.managed_pods if matches(pod)]
        values = {key: getattr(node.requested, key) - sum(getattr(pod.requests, key) for pod in removed)
                  for key in ("cpu_millis", "memory_mib", "storage_mib")}
        if not removed or min(values.values()) < 0 or len(removed) > node.used_pod_slots:
            nodes.append(node)
            continue
        nodes.append(node.model_copy(update={
            "requested": ResourceTotals(**values),
            "used_pod_slots": node.used_pod_slots - len(removed),
            "managed_pods": [pod for pod in node.managed_pods if not matches(pod)],
        }))
    return placement.model_copy(update={
        "nodes": nodes,
        "pending_pods": [pod for pod in placement.pending_pods if not matches(pod)],
    })


def _time(value: object) -> datetime | None:
    try:
        value = datetime.fromisoformat(value) if isinstance(value, str) else value
        return value if isinstance(value, datetime) and value.utcoffset() is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def released_pending_counts(original: CapacityPlacement, corrected: CapacityPlacement) -> dict[str, int]:
    """Count only classified exact Pods actually removed by reconciliation.

    Old observations lack per-Pod classifications. Their aggregate counters stay
    conservative even when exact physical absence permits resource reconciliation.
    """
    before = [pod for node in original.nodes for pod in node.managed_pods] + original.pending_pods
    after = {(pod.uid, pod.lease_id, pod.generation)
             for node in corrected.nodes for pod in node.managed_pods}
    after.update((pod.uid, pod.lease_id, pod.generation) for pod in corrected.pending_pods)
    identities = Counter((pod.lease_id, pod.generation) for pod in before)
    uids = Counter(pod.uid for pod in before)
    counts = {key: 0 for key in ("pending_jobs", "unschedulable_jobs", "image_pull_backoff_jobs")}
    for pod in before:
        if ((pod.uid, pod.lease_id, pod.generation) in after or pod.pending_counts is None
                or identities[pod.lease_id, pod.generation] != 1 or uids[pod.uid] != 1):
            continue
        counts["pending_jobs"] += int(pod.pending_counts.pending)
        counts["unschedulable_jobs"] += int(pod.pending_counts.unschedulable)
        counts["image_pull_backoff_jobs"] += int(pod.pending_counts.image_pull_backoff)
    return counts


async def reconcile_capacity_placement(
    session: AsyncSession, placement: CapacityPlacement, *, target_ids: frozenset[str], now: datetime,
) -> CapacityPlacement:
    """Read only the durable owners referenced by this physical inventory.

    Admission holds its ordinary capacity transaction lock. A concurrent receipt
    that has not committed is invisible and cannot release capacity early. New
    reservations are still packed after this correction under that same lock.
    """
    pods = [pod for node in placement.nodes for pod in node.managed_pods] + placement.pending_pods
    native_ids, execution_ids = set(), set()
    for pod in pods:
        try:
            if pod.lease_id.startswith("task-image:"):
                native_ids.add(UUID(pod.lease_id.removeprefix("task-image:")))
            else:
                execution_ids.add(UUID(pod.lease_id))
        except ValueError:
            continue  # Foreign Pods and unknown historical identities retain their charge.
    releases = []
    if native_ids:
        attempts = (await session.execute(select(
            TaskImageMaterializationAttempt.materialization_id,
            TaskImageMaterializationAttempt.lease_epoch,
            TaskImageMaterializationAttempt.native_capacity_target_id.label("target_id"),
            TaskImageMaterializationAttempt.native_capacity_released_at.label("released_at"),
            TaskImageMaterializationAttempt.native_build["capacity_release_receipt"].label("receipt"),
            TaskImageMaterializationAttempt.native_build["job_uid"].astext.label("job_uid"),
            TaskImageMaterializationAttempt.native_build["pod_uid"].astext.label("pod_uid"),
        ).where(
            TaskImageMaterializationAttempt.materialization_id.in_(native_ids),
            TaskImageMaterializationAttempt.native_capacity_target_id.in_(target_ids),
            TaskImageMaterializationAttempt.native_capacity_released_at.is_not(None),
        ))).mappings().all()
        for attempt in attempts:
            receipt = attempt["receipt"]
            if not isinstance(receipt, dict):
                continue
            at = _time(receipt.get("confirmed_at"))
            if (at is None or at > now or at != _time(attempt["released_at"])
                    or receipt.get("pods_absent") is not True
                    or receipt.get("target_id") != attempt["target_id"]
                    or type(receipt.get("lease_epoch")) is not int
                    or receipt.get("lease_epoch") != attempt["lease_epoch"]
                    or not attempt["job_uid"] or receipt.get("job_uid") != attempt["job_uid"]
                    or not attempt["pod_uid"] or receipt.get("pod_uid") != attempt["pod_uid"]):
                continue
            releases.append(ReleasedPod(f"task-image:{attempt['materialization_id']}",
                                        attempt["lease_epoch"], attempt["pod_uid"], at))
    if execution_ids:
        rows = (await session.execute(select(
            ServiceExecutionLease.id, ServiceExecutionLease.resource_generation,
            ServiceExecutionLease.pod_uid, ServiceExecutionLease.job_uid,
            ServiceExecutionLease.target_id,
            ServiceExecutionEvent.payload_json["cleanup_receipt"].label("receipt"),
            ServiceExecutionEvent.payload_json["target_id"].astext.label("event_target_id"),
        ).join(
            ServiceExecutionEvent, ServiceExecutionEvent.lease_id == ServiceExecutionLease.id,
        ).where(
            ServiceExecutionLease.id.in_(execution_ids),
            ServiceExecutionLease.target_id.in_(target_ids),
            ServiceExecutionLease.cleanup_state == "complete",
            ServiceExecutionLease.deleted_at <= now,
            ServiceExecutionEvent.generation == ServiceExecutionLease.generation,
            ServiceExecutionEvent.event_kind == "kubernetes_observed",
            ServiceExecutionEvent.observed_at <= now,
            ServiceExecutionEvent.payload_json["normalized_state"].astext == "deleted",
            ServiceExecutionEvent.payload_json["cleanup_receipt"].is_not(None),
        ))).mappings().all()
        for lease in rows:
            receipt = lease["receipt"]
            at = _time(receipt.get("confirmed_at")) if isinstance(receipt, dict) else None
            if (not isinstance(receipt, dict) or receipt.get("pods_absent") is not True
                    or at is None or at > now
                    or not lease["pod_uid"] or receipt.get("pod_uid") != lease["pod_uid"]
                    or not lease["job_uid"] or receipt.get("job_uid") != lease["job_uid"]
                    or type(receipt.get("resource_generation")) is not int
                    or receipt.get("resource_generation") != lease["resource_generation"]
                    or lease["event_target_id"] != lease["target_id"]):
                continue
            releases.append(ReleasedPod(str(lease["id"]), lease["resource_generation"], lease["pod_uid"], at))
    return reconcile_released_pods(placement, releases, now=now)
