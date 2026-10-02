"""Qualify native result identity, never Kubernetes cleanup or write authority."""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1


def qualify_native_observation(observed: dict[str, Any], runtime: PoolNativeRuntimeV1) -> None:
    try:
        if runtime.receipt.job_uid is None or runtime.job_effect_id is None:
            raise ValueError
        labels = {"app.kubernetes.io/managed-by": "loom-pool-gateway",
            "app.kubernetes.io/component": "task-image-builder",
            "loom.materialization-id": str(runtime.receipt.request_key.local_work_id),
            "loom.lease-epoch": str(runtime.lease_epoch)}
        if runtime.receipt.request_key.workload_kind == "application_image_build":
            labels = {"app.kubernetes.io/managed-by": "loom-pool-gateway",
                "app.kubernetes.io/component": "application-image-builder",
                "loom.application-build-id": str(runtime.receipt.request_key.local_work_id),
                "loom.build-attempt": str(runtime.lease_epoch)}
        annotations = {"loom.openai.com/target-id": runtime.target_id,
            "loom.nebius/pool-reservation-id": str(runtime.receipt.reservation_id),
            "loom.nebius/pool-plan-sha256": runtime.receipt.plan_sha256,
            "loom.nebius/pool-effect-id": str(runtime.job_effect_id)}

        def metadata(value: dict[str, Any]) -> dict[str, Any]:
            row: dict[str, Any] = value["metadata"]
            uid = UUID(row["uid"])
            if (not uid.int or str(uid) != row["uid"] or row["namespace"] != runtime.namespace.name
                    or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", row["resourceVersion"]) is None
                    or any(row.get("labels", {}).get(key) != want for key, want in labels.items())
                    or any(row.get("annotations", {}).get(key) != want for key, want in annotations.items())):
                raise ValueError
            return row

        job = metadata(observed)
        if (observed["apiVersion"] != "batch/v1" or observed["kind"] != "Job"
                or job["uid"] != str(runtime.receipt.job_uid) or job["name"] != runtime.job_name):
            raise ValueError
        pods = observed.get("pods", [])
        if not isinstance(pods, list) or len(pods) > 1:
            raise ValueError
        for pod in pods:
            row = metadata(pod)
            owners = row.get("ownerReferences", [])
            if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod"
                    or re.fullmatch(re.escape(runtime.job_name) + r"-[a-z0-9-]{1,63}", row["name"]) is None
                    or len(owners) != 1 or owners[0].get("controller") is not True
                    or any(owners[0].get(key) != want for key, want in {
                        "apiVersion": "batch/v1", "kind": "Job", "name": runtime.job_name,
                        "uid": str(runtime.receipt.job_uid)}.items())
                    or any(key in row["labels"] and row["labels"][key] != str(runtime.receipt.job_uid)
                        for key in ("controller-uid", "batch.kubernetes.io/controller-uid"))
                    or any(key in row["labels"] and row["labels"][key] != runtime.job_name
                        for key in ("job-name", "batch.kubernetes.io/job-name"))):
                raise ValueError
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError("pool_native_observation_identity_conflict") from None
