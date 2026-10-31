"""Durable, UID-fenced kubelet samples; sampling maxima are not kernel peaks."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionLease, Trial, TrialResourceUsage
from loom.execution_runtime_contract import ContainerResourcesV1, ExecutionRuntimePlanV1
from loom.models.resource_usage import (
    ContainerRole,
    ResourceCounters,
    ResourceLimits,
    TrialResourceUsageReport,
)
from loom.resource_usage_store import report_values
from loom_execution_actuator.contracts import ActuatorContractError, KubernetesJobObservation


@dataclass(frozen=True)
class ResourceSample:
    counters: ResourceCounters
    started_at: datetime | None = None


def _started(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result if result.utcoffset() is not None else None
    except ValueError:
        return None


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def pod_samples(
    summary: dict[str, Any], *, namespace: str, pod_uid: str
) -> dict[str, ResourceSample]:
    """Never match names alone. Pod storage includes logs/rootfs/volumes exactly once."""
    pod = next(
        (
            p
            for p in summary.get("pods", [])
            if p.get("podRef", {}).get("uid") == pod_uid
            and p.get("podRef", {}).get("namespace") == namespace
        ),
        None,
    )
    if pod is None:
        return {}
    samples = {}
    for container in pod.get("containers", []):
        cpu = container.get("cpu") or {}
        memory = container.get("memory") or {}
        usage = _integer(cpu.get("usageCoreNanoSeconds"))
        rootfs = _integer((container.get("rootfs") or {}).get("usedBytes"))
        logs = _integer((container.get("logs") or {}).get("usedBytes"))
        samples[container["name"]] = ResourceSample(
            ResourceCounters(
                cpu_usage_usec=None if usage is None else usage // 1000,
                cpu_sampled_max_nanocores=_integer(cpu.get("usageNanoCores")),
                memory_current_bytes=_integer(memory.get("usageBytes")),
                memory_sampled_max_bytes=_integer(memory.get("usageBytes")),
                filesystem_sampled_max_bytes=(
                    rootfs + logs if rootfs is not None and logs is not None else None
                ),
            ),
            _started(container.get("startTime")),
        )
    samples["pod"] = ResourceSample(
        ResourceCounters(
            ephemeral_storage_sampled_max_bytes=_integer(
                (pod.get("ephemeral-storage") or {}).get("usedBytes")
            )
        )
    )
    return samples


async def persist_native_usage(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    observation: KubernetesJobObservation,
    samples: dict[str, ResourceSample],
    now: datetime,
    terminal: bool,
    diagnostic: str | None = None,
) -> None:
    """The actuator's DB transaction owns authority; no new public write surface."""
    current = await session.scalar(
        select(ServiceExecutionLease).where(ServiceExecutionLease.id == lease.id).with_for_update()
    )
    if current is None:
        raise ActuatorContractError("usage lease disappeared")
    if (
        current.generation != lease.generation
        or current.target_id != observation.target_id
        or current.resource_generation != observation.resource_generation
        or current.job_uid != observation.job_uid
        or (observation.pod_uid is not None and current.pod_uid != observation.pod_uid)
    ):
        raise ActuatorContractError("usage execution identity changed")
    pod_uid = observation.pod_uid or current.pod_uid
    if pod_uid is None or current.runtime_contract_json is None:
        return  # No container identity exists; never invent one for an unstarted Job.
    plan = ExecutionRuntimePlanV1.model_validate(current.runtime_contract_json)
    roles: dict[str, tuple[ContainerRole, ContainerResourcesV1 | None]] = {
        "execution": ("controller" if plan.sidecars else "agent", plan.execution_resources)
    }
    for sidecar in plan.sidecars:
        role: ContainerRole = (
            "verifier"
            if sidecar.role_name == "verifier-sandbox"
            else "task"
            if sidecar.role_name == "task-sandbox"
            else "sidecar"
        )
        roles[sidecar.role_name] = (role, sidecar.resources)
    roles["runtime-materializer"] = ("sidecar", None)
    roles["pod"] = ("pod", None)
    trial = await session.get(Trial, current.trial_id)
    assert trial is not None
    image_refs = {
        "execution": plan.agent_image_ref or plan.task_image_ref,
        "runtime-materializer": plan.runtime_image_ref,
        **{sidecar.role_name: sidecar.image_ref for sidecar in plan.sidecars},
    }
    for name, (role, resources) in roles.items():
        sample = samples.get(name, ResourceSample(ResourceCounters()))
        started_at = sample.started_at
        if started_at is None and name != "pod":
            # A missing final summary must finalize the previously sampled epoch,
            # not create a second unknown-epoch row.
            previous = await session.scalar(
                select(TrialResourceUsage)
                .where(
                    TrialResourceUsage.execution_lease_id == current.id,
                    TrialResourceUsage.resource_generation == current.resource_generation,
                    TrialResourceUsage.pod_uid == pod_uid,
                    TrialResourceUsage.role_name == name,
                )
                .order_by(TrialResourceUsage.first_observed_at.desc())
                .limit(1)
            )
            if previous is not None:
                started_at = previous.container_started_at
        epoch = started_at.isoformat() if started_at else "unknown"

        # Existing execution-key format is a stable dedup identity, not an integrity gate.
        key = hashlib.sha256(
            f"{current.id}:{current.resource_generation}:{pod_uid}:{name}:{epoch}".encode()
        ).hexdigest()
        row = await session.scalar(
            select(TrialResourceUsage).where(
                TrialResourceUsage.trial_id == current.trial_id,
                TrialResourceUsage.attempt_count == current.attempt,
                TrialResourceUsage.execution_key == key,
            )
        )
        counters = sample.counters
        has_values = any(value is not None for value in counters.model_dump().values())
        if row is None:
            older = (
                await session.scalars(
                    select(TrialResourceUsage).where(
                        TrialResourceUsage.execution_lease_id == current.id,
                        TrialResourceUsage.resource_generation == current.resource_generation,
                        TrialResourceUsage.pod_uid == pod_uid,
                        TrialResourceUsage.role_name == name,
                        TrialResourceUsage.finalized_at.is_(None),
                    )
                )
            ).all()
            for prior in older:
                prior.finalized_at = max(now, prior.last_observed_at)
                prior_has_counters = any(
                    getattr(prior, field) is not None for field in ResourceCounters.model_fields
                )
                first_observation = prior.container_started_at is None and not prior_has_counters
                prior.terminal_reason = (
                    "first_container_observation" if first_observation else "container_restarted"
                )
                prior.diagnostic_code = (
                    "prestart_sample_unavailable"
                    if first_observation
                    else "container_incarnation_changed"
                )
                prior.completeness = "partial" if prior_has_counters else "unavailable"
            report = TrialResourceUsageReport(
                trial_id=current.trial_id,
                attempt_count=current.attempt,
                execution_lease_id=current.id,
                resource_generation=current.resource_generation,
                target_id=current.target_id,
                pod_uid=pod_uid,
                execution_key=key,
                container_role=role,
                role_name=name,
                backend="nebius_kubernetes",
                candidate_sha=plan.candidate_sha,
                image_digest=image_refs[name].rsplit("@", 1)[-1] if name in image_refs else None,
                container_started_at=started_at,
                architecture=current.workload_requirements_json.get("cpu_architecture"),
                source="kubelet_summary",
                observation_seq=0,
                first_observed_at=now,
                last_observed_at=now,
                finalized_at=now if terminal else None,
                terminal_reason=observation.normalized_state.value if terminal else None,
                completeness="partial" if has_values or not terminal else "unavailable",
                diagnostic_code=diagnostic or "sampled_maxima_not_kernel_peaks",
                limits=ResourceLimits(
                    cpu_cores=resources.cpu_millis / 1000,
                    memory_bytes=resources.memory_mib * 1048576,
                )
                if resources
                else ResourceLimits(),
                counters=counters,
            )
            session.add(
                TrialResourceUsage(
                    **report_values(report, lifecycle_authority_id=trial.lifecycle_authority_id)
                )
            )
            continue
        if row.finalized_at is not None:
            continue  # Terminal delivery is idempotent, late stale samples cannot rewrite it.
        if (
            counters.cpu_usage_usec is not None
            and row.cpu_usage_usec is not None
            and counters.cpu_usage_usec < row.cpu_usage_usec
        ):
            row.diagnostic_code = "counter_reset_without_new_incarnation"
        elif row.diagnostic_code != "counter_reset_without_new_incarnation":
            # Keep the latest collection failure, or clear it on real recovery.
            # Counter resets still preclude an exact whole-incarnation total.
            if diagnostic or has_values:
                row.diagnostic_code = diagnostic or "sampled_maxima_not_kernel_peaks"
        if has_values:
            for field, value in counters.model_dump().items():
                previous = getattr(row, field)
                if value is not None:
                    setattr(
                        row,
                        field,
                        value
                        if field == "memory_current_bytes" or previous is None
                        else max(previous, value),
                    )
            row.observation_seq += 1
            row.last_observed_at = max(row.last_observed_at, now)
        if terminal:
            row.finalized_at = max(now, row.last_observed_at)
            row.terminal_reason = observation.normalized_state.value
            known = any(getattr(row, field) is not None for field in ResourceCounters.model_fields)
            row.completeness = "partial" if known else "unavailable"
