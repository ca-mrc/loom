"""Qualify SDK observations before existing execution/result normalization."""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1


def qualify_execution_observation(job: Any, pods: list[Any], runtime: PoolExecutionRuntimeV1) -> None:
    try:
        if runtime.receipt.job_uid is None or runtime.job_effect_id is None:
            raise ValueError
        labels = {"app.kubernetes.io/managed-by": "loom-execution-actuator",
            "app.kubernetes.io/component": "execution-unit",
            "loom.openai.com/lease-id": str(runtime.receipt.request_key.local_work_id),
            "loom.openai.com/generation": str(runtime.resource_generation)}
        annotations = {"loom.openai.com/target-id": runtime.target_id,
            "loom.openai.com/execution-unit-key": str(runtime.execution_unit_key),
            "loom.nebius/pool-reservation-id": str(runtime.receipt.reservation_id),
            "loom.nebius/pool-plan-sha256": runtime.receipt.plan_sha256,
            "loom.nebius/pool-effect-id": str(runtime.job_effect_id)}

        def metadata(value: Any) -> Any:
            row = value.metadata
            uid = UUID(row.uid)
            if (not uid.int or str(uid) != row.uid or row.namespace != runtime.namespace.name
                    or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", row.resource_version) is None
                    or any((row.labels or {}).get(key) != want for key, want in labels.items())
                    or any((row.annotations or {}).get(key) != want for key, want in annotations.items())):
                raise ValueError
            return row

        row = metadata(job)
        if (job.api_version != "batch/v1" or job.kind != "Job" or row.name != runtime.job_name
                or row.uid != str(runtime.receipt.job_uid) or len(pods) > 1):
            raise ValueError
        for pod in pods:
            row = metadata(pod)
            owners = row.owner_references or []
            # Qualified PodList supplies missing item type metadata in Kubernetes.
            if (getattr(pod, "api_version", None) not in {None, "v1"}
                    or getattr(pod, "kind", None) not in {None, "Pod"}
                    or re.fullmatch(re.escape(runtime.job_name) + r"-[a-z0-9-]{1,63}", row.name) is None
                    or len(owners) != 1 or owners[0].controller is not True
                    or any(getattr(owners[0], key, None) != want for key, want in {
                        "api_version": "batch/v1", "kind": "Job", "name": runtime.job_name,
                        "uid": str(runtime.receipt.job_uid)}.items())
                    or any(key in row.labels and row.labels[key] != str(runtime.receipt.job_uid)
                        for key in ("controller-uid", "batch.kubernetes.io/controller-uid"))
                    or any(key in row.labels and row.labels[key] != runtime.job_name
                        for key in ("job-name", "batch.kubernetes.io/job-name"))):
                raise ValueError
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError("pool_execution_observation_identity_conflict") from None
