"""Bound Pod readback. This is not deletion, output-drain or release authority."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from loom_service.pool_management.gateway_journal import PoolGatewayEffect


@dataclass(frozen=True)
class PoolPodReference:
    name: str
    uid: UUID
    resource_version: str
    terminating: bool


@dataclass(frozen=True)
class PoolPodInventory:
    reservation_id: UUID
    create_effect_id: UUID
    namespace_uid: UUID
    job_uid: UUID
    resource_version: str
    pods: tuple[PoolPodReference, ...]


def owned_pod(value: dict[str, Any], created: PoolGatewayEffect, *, uid: UUID, resource_version: str) -> PoolPodReference | None:
    """Contradictory candidate identity blocks, never silently becomes foreign.

    Scan all namespace Pods, not only a label selector: a stripped/changed marker
    cannot hide a residual Pod still tied to the original Job name or owner UID.
    Unrelated namespace workloads are neither adopted nor included as our Pods.
    """
    metadata = value["metadata"]
    expected_metadata = created.document["metadata"]
    name = metadata.get("name")
    labels, annotations, owners = (metadata.get("labels", {}), metadata.get("annotations", {}), metadata.get("ownerReferences", []))
    if (value.get("apiVersion") != "v1" or value.get("kind") != "Pod"
            or not isinstance(name, str) or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?", name) is None
            or metadata.get("namespace") != expected_metadata["namespace"]
            or not isinstance(labels, dict) or not isinstance(annotations, dict) or not isinstance(owners, list)
            or any(not isinstance(owner, dict) for owner in owners)):
        raise ValueError("pool_pod_identity_conflict")
    job_uid, job_name = str(created.observed_uid), expected_metadata["name"]
    candidate = (any(owner.get("uid") == job_uid for owner in owners)
        or annotations.get("loom.nebius/pool-reservation-id") == str(created.reservation_id)
        or any(labels.get(key) == job_name for key in ("batch.kubernetes.io/job-name", "job-name"))
        or name.startswith(job_name + "-"))
    if not candidate:
        return None
    template = created.document["spec"]["template"]["metadata"]
    if (len(owners) != 1 or owners[0].get("controller") is not True
            or any(owners[0].get(key) != wanted for key, wanted in {
                "apiVersion": "batch/v1", "kind": "Job", "name": job_name, "uid": job_uid}.items())
            or not name.startswith(job_name + "-")
            or any(labels.get(key) != wanted for key, wanted in template["labels"].items())
            or any(annotations.get(key) != wanted for key, wanted in template["annotations"].items())
            or any(key in labels and labels[key] != job_uid for key in ("controller-uid", "batch.kubernetes.io/controller-uid"))
            or any(key in labels and labels[key] != job_name for key in ("job-name", "batch.kubernetes.io/job-name"))):
        raise ValueError("pool_pod_identity_conflict")
    return PoolPodReference(name, uid, resource_version, bool(metadata.get("deletionTimestamp")))


def require_unstarted_pod_absence(value: dict[str, Any], *, namespace: str, job_name: str, reservation_id: UUID) -> None:
    """No observed Job UID exists: any possible child blocks, never invent a UID."""
    metadata = value["metadata"]
    name = metadata.get("name")
    labels, annotations, owners = metadata.get("labels", {}), metadata.get("annotations", {}), metadata.get("ownerReferences", [])
    if (value.get("apiVersion") != "v1" or value.get("kind") != "Pod" or metadata.get("namespace") != namespace
            or not isinstance(name, str) or re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?", name) is None
            or not isinstance(labels, dict) or not isinstance(annotations, dict) or not isinstance(owners, list)
            or any(not isinstance(owner, dict) for owner in owners)):
        raise ValueError("pool_pod_identity_conflict")
    if (name.startswith(job_name + "-") or any(owner.get("name") == job_name for owner in owners)
            or annotations.get("loom.nebius/pool-reservation-id") == str(reservation_id)
            or any(labels.get(key) == job_name for key in ("job-name", "batch.kubernetes.io/job-name"))):
        raise ValueError("pool_unobserved_job_has_pod_candidate")
