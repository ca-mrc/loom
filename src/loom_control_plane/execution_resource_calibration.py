"""Evidence-gated #1503 resource calibration and Nebius capacity forecasts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionResourceCalibration,
    ExecutionResourceProfileBinding,
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Task,
    Trial,
    TrialResourceUsage,
)
from loom.execution_contract import ExecutionClassV1
from loom.models.task import TaskConfig
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity import _latest_observation
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets

MIN_TRIAL_ATTEMPTS = 1_000
MIN_EVIDENCE_DURATION_SECONDS = 14 * 24 * 60 * 60
MIN_BATCH_CONCURRENCY = 100
MAX_USAGE_RECORDS = 200_000
_MIB = 1024 * 1024
_CALIBRATION_LOCK = text(
    "SELECT pg_advisory_xact_lock(hashtextextended('execution-resource-calibration', 1552))"
)


@dataclass(frozen=True)
class CalibrationAttemptSample:
    trial_id: UUID
    attempt: int
    task_id: str
    batch_id: UUID | None
    started_at: datetime
    stopped_at: datetime
    cpu_average_millis: int
    memory_peak_upper_bound_mib: int
    pids_peak_upper_bound: int
    io_write_upper_bound_mib: int
    configured_cpu_millis: int
    configured_ephemeral_storage_mib: int
    throttled: bool
    oom: bool
    memory_limit_hit: bool


@dataclass(frozen=True)
class CalibrationProjection:
    trial_attempts: int
    distinct_tasks: int
    evidence_duration_seconds: int
    peak_batch_concurrency: int
    throttled_attempts: int
    oom_attempts: int
    memory_limit_attempts: int
    blockers: tuple[str, ...]
    percentiles: dict[str, dict[str, int]]
    recommended_cpu_millis: int
    recommended_memory_mib: int
    recommended_ephemeral_storage_mib: int
    recommended_pids: int

    @property
    def eligible(self) -> bool:
        return not self.blockers


def _clean(value: str, *, name: str, maximum: int) -> str:
    result = str(value).strip()
    if not result or len(result) > maximum:
        raise ValueError(f"{name} must contain 1 to {maximum} characters")
    return result


def _utc(value: datetime, *, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _round_up(value: int, quantum: int, *, minimum: int) -> int:
    return max(minimum, ((value + quantum - 1) // quantum) * quantum)


def _percentile(values: list[int], numerator: int, denominator: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(len(ordered) * numerator / denominator))
    return ordered[rank - 1]


def _peak_batch_concurrency(samples: list[CalibrationAttemptSample]) -> int:
    by_batch: dict[UUID, list[tuple[datetime, int]]] = {}
    for sample in samples:
        if sample.batch_id is None:
            continue
        events = by_batch.setdefault(sample.batch_id, [])
        events.append((sample.started_at, 1))
        events.append((sample.stopped_at, -1))
    peak = 0
    for events in by_batch.values():
        active = 0
        for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
            active += delta
            peak = max(peak, active)
    return peak


def derive_resource_calibration(
    samples: list[CalibrationAttemptSample],
    *,
    incomplete_attempts: int = 0,
) -> CalibrationProjection:
    """Derive exact nearest-rank percentiles and fail-closed eligibility."""

    metrics = {
        "cpu_average_millis": [sample.cpu_average_millis for sample in samples],
        "memory_peak_upper_bound_mib": [sample.memory_peak_upper_bound_mib for sample in samples],
        "pids_peak_upper_bound": [sample.pids_peak_upper_bound for sample in samples],
        "io_write_upper_bound_mib": [sample.io_write_upper_bound_mib for sample in samples],
    }
    percentiles = {
        name: {
            "p50": _percentile(values, 50, 100),
            "p95": _percentile(values, 95, 100),
            "p99": _percentile(values, 99, 100),
            "p995": _percentile(values, 995, 1000),
        }
        for name, values in metrics.items()
    }
    evidence_duration = (
        max(sample.stopped_at for sample in samples) - min(sample.started_at for sample in samples)
        if samples
        else timedelta(0)
    )
    duration_seconds = max(0, math.floor(evidence_duration.total_seconds()))
    distinct_tasks = len({sample.task_id for sample in samples})
    peak_concurrency = _peak_batch_concurrency(samples)
    throttled = sum(sample.throttled for sample in samples)
    oom = sum(sample.oom for sample in samples)
    memory_limit = sum(sample.memory_limit_hit for sample in samples)
    blockers: list[str] = []
    if len(samples) < MIN_TRIAL_ATTEMPTS:
        blockers.append("resource_calibration_trial_attempts_insufficient")
    if duration_seconds < MIN_EVIDENCE_DURATION_SECONDS:
        blockers.append("resource_calibration_duration_insufficient")
    if peak_concurrency < MIN_BATCH_CONCURRENCY:
        blockers.append("resource_calibration_high_concurrency_batch_missing")
    if incomplete_attempts:
        blockers.append("resource_calibration_telemetry_incomplete")
    if throttled:
        blockers.append("resource_calibration_cpu_throttling_observed")
    if oom:
        blockers.append("resource_calibration_oom_observed")
    if memory_limit:
        blockers.append("resource_calibration_memory_limit_observed")

    cpu_p995 = percentiles["cpu_average_millis"]["p995"]
    memory_p995 = percentiles["memory_peak_upper_bound_mib"]["p995"]
    pids_p995 = percentiles["pids_peak_upper_bound"]["p995"]
    io_write_p995 = percentiles["io_write_upper_bound_mib"]["p995"]
    return CalibrationProjection(
        trial_attempts=len(samples),
        distinct_tasks=distinct_tasks,
        evidence_duration_seconds=duration_seconds,
        peak_batch_concurrency=peak_concurrency,
        throttled_attempts=throttled,
        oom_attempts=oom,
        memory_limit_attempts=memory_limit,
        blockers=tuple(sorted(blockers)),
        percentiles=percentiles,
        recommended_cpu_millis=max(
            max((sample.configured_cpu_millis for sample in samples), default=100),
            _round_up(math.ceil(cpu_p995 * 1.25), 100, minimum=100),
        ),
        recommended_memory_mib=_round_up(math.ceil(memory_p995 * 1.20), 64, minimum=64),
        recommended_ephemeral_storage_mib=max(
            max(
                (sample.configured_ephemeral_storage_mib for sample in samples),
                default=1024,
            ),
            _round_up(math.ceil(io_write_p995 * 1.25), 256, minimum=1024),
        ),
        recommended_pids=_round_up(math.ceil(pids_p995 * 1.20), 8, minimum=8),
    )


def _complete_attempt_sample(
    rows: list[TrialResourceUsage],
    trial: Trial,
    task: Task,
) -> CalibrationAttemptSample | None:
    required = (
        "cpu_usage_usec",
        "cpu_throttled_periods",
        "memory_peak_bytes",
        "memory_events_max",
        "memory_events_oom",
        "memory_events_oom_kill",
        "pids_peak",
        "io_write_bytes",
        "cpu_limit_cores",
    )
    if any(
        row.completeness != "complete"
        or row.finalized_at is None
        or any(getattr(row, name) is None for name in required)
        for row in rows
    ):
        return None
    started_at = min(_utc(row.first_observed_at, name="first_observed_at") for row in rows)
    stopped_at = max(_utc(row.last_observed_at, name="last_observed_at") for row in rows)
    elapsed_usec = math.floor((stopped_at - started_at).total_seconds() * 1_000_000)
    cpu_usage_usec = sum(int(row.cpu_usage_usec or 0) for row in rows)
    if elapsed_usec <= 0:
        return None
    try:
        task_config = TaskConfig.model_validate(task.config)
    except ValueError:
        return None
    storage_mib = task_config.environment.storage_mb
    if storage_mib is None:
        return None
    return CalibrationAttemptSample(
        trial_id=trial.id,
        attempt=rows[0].attempt_count,
        task_id=trial.task_id,
        batch_id=trial.batch_id,
        started_at=started_at,
        stopped_at=stopped_at,
        cpu_average_millis=math.ceil(cpu_usage_usec * 1000 / elapsed_usec),
        memory_peak_upper_bound_mib=math.ceil(
            sum(int(row.memory_peak_bytes or 0) for row in rows) / _MIB
        ),
        pids_peak_upper_bound=sum(int(row.pids_peak or 0) for row in rows),
        io_write_upper_bound_mib=math.ceil(
            sum(int(row.io_write_bytes or 0) for row in rows) / _MIB
        ),
        configured_cpu_millis=math.ceil(
            sum(float(row.cpu_limit_cores or 0) for row in rows) * 1000
        ),
        configured_ephemeral_storage_mib=storage_mib,
        throttled=any(int(row.cpu_throttled_periods or 0) > 0 for row in rows),
        oom=any(
            int(row.memory_events_oom or 0) > 0 or int(row.memory_events_oom_kill or 0) > 0
            for row in rows
        ),
        memory_limit_hit=any(int(row.memory_events_max or 0) > 0 for row in rows),
    )


async def create_execution_resource_calibration(
    session: AsyncSession,
    *,
    target_id: str,
    source_pool_id: str,
    source_architecture: str,
    resource_profile: str,
    candidate_sha: str,
    source_version: str,
    window_started_at: datetime,
    window_stopped_at: datetime,
    now: datetime | None = None,
) -> tuple[ExecutionResourceCalibration, bool]:
    target_id = _clean(target_id, name="target_id", maximum=120)
    source_pool_id = _clean(source_pool_id, name="source_pool_id", maximum=80)
    resource_profile = _clean(resource_profile, name="resource_profile", maximum=120)
    source_version = _clean(source_version, name="source_version", maximum=160)
    if source_architecture not in {"x86_64", "arm64"}:
        raise ValueError("source_architecture must be x86_64 or arm64")
    if len(candidate_sha) != 40 or any(
        character not in "0123456789abcdef" for character in candidate_sha
    ):
        raise ValueError("candidate_sha must be 40 lowercase hex characters")
    start = _utc(window_started_at, name="window_started_at")
    stop = _utc(window_stopped_at, name="window_stopped_at")
    current_time = _utc(now or datetime.now(UTC), name="now")
    if stop <= start or stop > current_time + timedelta(seconds=60):
        raise ValueError("calibration window must be closed and ordered")
    if stop - start > timedelta(days=90):
        raise ValueError("calibration window cannot exceed 90 days")

    await session.execute(_CALIBRATION_LOCK)
    target = await session.get(ServiceExecutionTarget, target_id)
    if target is None or target.provider != "nebius":
        raise ValueError("Nebius execution target does not exist")
    execution_class = await session.get(ServiceExecutionClass, target.execution_class_id)
    if execution_class is None:
        raise ValueError("execution target class does not exist")
    class_contract = ExecutionClassV1.model_validate(execution_class.spec_json)
    if class_contract.cpu_architecture != source_architecture:
        raise ValueError("calibration architecture does not match the target execution class")

    # Retained trial usage can predate native routing; preserve its historical
    # pool attribution without treating that field as current scheduling intent.
    route_matches = or_(
        Trial.execution_route_pool_name == source_pool_id,
        and_(
            Trial.execution_route_pool_name.is_(None),
            Trial.autoscaler_pool_name == source_pool_id,
        ),
    )
    filters = (
        TrialResourceUsage.resource_profile == resource_profile,
        TrialResourceUsage.candidate_sha == candidate_sha,
        TrialResourceUsage.architecture == source_architecture,
        TrialResourceUsage.finalized_at >= start,
        TrialResourceUsage.finalized_at < stop,
        route_matches,
    )
    record_count = int(
        await session.scalar(
            select(func.count(TrialResourceUsage.id))
            .join(Trial, Trial.id == TrialResourceUsage.trial_id)
            .join(Task, Task.id == Trial.task_id)
            .where(*filters)
        )
        or 0
    )
    if record_count > MAX_USAGE_RECORDS:
        raise ValueError("calibration query exceeds the bounded usage-record limit")
    result = await session.execute(
        select(TrialResourceUsage, Trial, Task)
        .join(Trial, Trial.id == TrialResourceUsage.trial_id)
        .join(Task, Task.id == Trial.task_id)
        .where(*filters)
        .order_by(
            TrialResourceUsage.trial_id,
            TrialResourceUsage.attempt_count,
            TrialResourceUsage.execution_key,
        )
    )
    grouped: dict[tuple[UUID, int], tuple[Trial, Task, list[TrialResourceUsage]]] = {}
    evidence_rows: list[dict[str, object]] = []
    for usage, trial, task in result.all():
        key = (usage.trial_id, usage.attempt_count)
        grouped.setdefault(key, (trial, task, []))[2].append(usage)
        evidence_rows.append(
            {
                "id": str(usage.id),
                "observation_seq": usage.observation_seq,
                "updated_at": _utc(usage.updated_at, name="updated_at").isoformat(),
            }
        )
    samples: list[CalibrationAttemptSample] = []
    incomplete_attempts = 0
    for trial, task, usage_rows in grouped.values():
        sample = _complete_attempt_sample(usage_rows, trial, task)
        if sample is None:
            incomplete_attempts += 1
        else:
            samples.append(sample)
    projection = derive_resource_calibration(
        samples,
        incomplete_attempts=incomplete_attempts,
    )
    source_query_sha256 = canonical_digest(
        {
            "schema_version": "loom.execution-resource-calibration-query.v1",
            "rows": evidence_rows,
        }
    )
    evidence = {
        "schema_version": "loom.execution-resource-calibration.v1",
        "target_id": target_id,
        "target_execution_class_id": target.execution_class_id,
        "source_pool_id": source_pool_id,
        "source_architecture": source_architecture,
        "resource_profile": resource_profile,
        "candidate_sha": candidate_sha,
        "source_version": source_version,
        "window_started_at": start.isoformat(),
        "window_stopped_at": stop.isoformat(),
        "acceptance_thresholds": {
            "minimum_trial_attempts": MIN_TRIAL_ATTEMPTS,
            "minimum_evidence_duration_seconds": MIN_EVIDENCE_DURATION_SECONDS,
            "minimum_batch_concurrency": MIN_BATCH_CONCURRENCY,
        },
        "trial_attempts": projection.trial_attempts,
        "distinct_tasks": projection.distinct_tasks,
        "usage_records": record_count,
        "incomplete_attempts": incomplete_attempts,
        "evidence_duration_seconds": projection.evidence_duration_seconds,
        "peak_batch_concurrency": projection.peak_batch_concurrency,
        "throttled_attempts": projection.throttled_attempts,
        "oom_attempts": projection.oom_attempts,
        "memory_limit_attempts": projection.memory_limit_attempts,
        "eligible": projection.eligible,
        "blockers": list(projection.blockers),
        "percentiles": projection.percentiles,
        "recommendation": {
            "cpu_millis": projection.recommended_cpu_millis,
            "memory_mib": projection.recommended_memory_mib,
            "ephemeral_storage_mib": projection.recommended_ephemeral_storage_mib,
            "pids": projection.recommended_pids,
            "cpu_method": ("max_configured_limit_or_p995_mean_plus_25_percent_rounded_100m"),
            "memory_method": "p995_sum_of_container_peaks_plus_20_percent_rounded_64mib",
            "storage_method": (
                "max_task_contract_or_p995_cumulative_writes_plus_25_percent_rounded_256mib"
            ),
            "pids_method": "p995_sum_of_container_peaks_plus_20_percent_rounded_8",
        },
        "source_query_sha256": source_query_sha256,
        "task_set_sha256": canonical_digest(
            {
                "schema_version": "loom.execution-resource-calibration-task-set.v1",
                "task_ids": sorted({sample.task_id for sample in samples}),
            }
        ),
    }
    evidence_sha256 = canonical_digest(evidence)
    existing = (
        await session.execute(
            select(ExecutionResourceCalibration).where(
                ExecutionResourceCalibration.target_id == target_id,
                ExecutionResourceCalibration.source_pool_id == source_pool_id,
                ExecutionResourceCalibration.resource_profile == resource_profile,
                ExecutionResourceCalibration.candidate_sha == candidate_sha,
                ExecutionResourceCalibration.source_version == source_version,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.evidence_sha256 != evidence_sha256:
            raise ValueError("calibration source version already has different evidence")
        return existing, False
    row = ExecutionResourceCalibration(
        id=uuid4(),
        target_id=target_id,
        source_pool_id=source_pool_id,
        source_architecture=source_architecture,
        resource_profile=resource_profile,
        candidate_sha=candidate_sha,
        source_version=source_version,
        window_started_at=start,
        window_stopped_at=stop,
        trial_attempts=projection.trial_attempts,
        distinct_tasks=projection.distinct_tasks,
        usage_records=record_count,
        incomplete_attempts=incomplete_attempts,
        evidence_duration_seconds=projection.evidence_duration_seconds,
        peak_batch_concurrency=projection.peak_batch_concurrency,
        throttled_attempts=projection.throttled_attempts,
        oom_attempts=projection.oom_attempts,
        memory_limit_attempts=projection.memory_limit_attempts,
        eligible=projection.eligible,
        blockers_json=list(projection.blockers),
        percentiles_json=projection.percentiles,
        recommended_cpu_millis=projection.recommended_cpu_millis,
        recommended_memory_mib=projection.recommended_memory_mib,
        recommended_ephemeral_storage_mib=(projection.recommended_ephemeral_storage_mib),
        recommended_pids=projection.recommended_pids,
        evidence_json=evidence,
        evidence_sha256=evidence_sha256,
    )
    session.add(row)
    await session.flush()
    return row, True


async def upsert_execution_resource_profile_binding(
    session: AsyncSession,
    *,
    target_id: str,
    calibration_id: UUID,
    enabled: bool,
    reason: str | None,
    now: datetime | None = None,
) -> ExecutionResourceProfileBinding:
    target_id = _clean(target_id, name="target_id", maximum=120)
    clean_reason = reason.strip() if reason is not None else None
    if clean_reason == "":
        clean_reason = None
    if clean_reason is not None and len(clean_reason) > 500:
        raise ValueError("reason must contain at most 500 characters")
    if enabled and clean_reason is None:
        raise ValueError("enabled resource profile binding requires an acceptance reason")
    await session.execute(_CALIBRATION_LOCK)
    calibration = await session.get(ExecutionResourceCalibration, calibration_id)
    if calibration is None or calibration.target_id != target_id:
        raise ValueError("resource calibration does not belong to the target")
    if enabled and not calibration.eligible:
        raise ValueError("ineligible resource calibration cannot be enabled")
    row = await session.get(ExecutionResourceProfileBinding, target_id, with_for_update=True)
    current_time = _utc(now or datetime.now(UTC), name="now")
    if row is None:
        row = ExecutionResourceProfileBinding(
            target_id=target_id,
            calibration_id=calibration_id,
            enabled=enabled,
            reason=clean_reason,
            updated_at=current_time,
        )
        session.add(row)
    else:
        row.calibration_id = calibration_id
        row.enabled = enabled
        row.reason = clean_reason
        row.version += 1
        row.updated_at = current_time
    await session.flush()
    return row


def _fit_slots(
    cpu: int, memory: int, storage: int, calibration: ExecutionResourceCalibration
) -> int:
    return min(
        cpu // calibration.recommended_cpu_millis,
        memory // calibration.recommended_memory_mib,
        storage // calibration.recommended_ephemeral_storage_mib,
    )


async def fetch_execution_resource_profile_status(
    session: AsyncSession,
    *,
    pool_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    current_time = _utc(now or datetime.now(UTC), name="now")
    statement = select(ServiceExecutionTarget).where(ServiceExecutionTarget.provider == "nebius")
    if pool_id is not None:
        statement = statement.where(ServiceExecutionTarget.logical_pool_id == pool_id)
    targets = (await session.execute(statement.order_by(ServiceExecutionTarget.id))).scalars().all()
    results: list[dict[str, object]] = []
    for target in targets:
        binding = await session.get(ExecutionResourceProfileBinding, target.id)
        calibration = (
            await session.get(ExecutionResourceCalibration, binding.calibration_id)
            if binding is not None
            else None
        )
        latest_calibrations = (
            (
                await session.execute(
                    select(ExecutionResourceCalibration)
                    .where(ExecutionResourceCalibration.target_id == target.id)
                    .order_by(
                        ExecutionResourceCalibration.created_at.desc(),
                        ExecutionResourceCalibration.id.desc(),
                    )
                    .limit(20)
                )
            )
            .scalars()
            .all()
        )
        group = await resolve_capacity_targets(session, target.id)
        policy = await session.get(ExecutionCapacityPolicy, group.owner.id)
        observation = await _latest_observation(session, group.owner.id)
        blockers: list[str] = []
        if binding is None or not binding.enabled:
            blockers.append("resource_profile_binding_unavailable")
        if calibration is None:
            blockers.append("resource_calibration_unavailable")
        elif not calibration.eligible:
            blockers.extend(str(value) for value in calibration.blockers_json)
        if policy is None or not policy.enabled:
            blockers.append("resource_forecast_capacity_policy_unavailable")
        if observation is None:
            blockers.append("resource_forecast_capacity_observation_unavailable")
        observation_fresh = bool(
            observation is not None
            and policy is not None
            and group.matches_observation_scope(observation.observation_json)
            and observation.observed_at <= current_time + timedelta(seconds=60)
            and current_time
            <= observation.observed_at + timedelta(seconds=policy.observation_max_age_seconds)
        )
        if group.owner.desired_state != "active":
            blockers.append("resource_forecast_capacity_owner_not_active")
        if observation is not None and not group.matches_observation_scope(observation.observation_json):
            blockers.append("resource_forecast_capacity_target_scope_unavailable")
        if observation is not None and not observation_fresh:
            blockers.append("resource_forecast_capacity_observation_stale")
        if observation is not None:
            if observation.provider_capacity_state != "available":
                blockers.append("resource_forecast_provider_capacity_unavailable")
            if observation.autoscaler_state not in {"ready", "scaling"}:
                blockers.append("resource_forecast_autoscaler_unavailable")
        if observation is not None and policy is not None:
            limits = (
                (observation.active_nodes, policy.max_nodes, "policy_nodes"),
                (
                    observation.provisioned_vcpu_millis,
                    policy.max_vcpu_millis,
                    "policy_vcpu",
                ),
                (
                    observation.provisioned_memory_mib,
                    policy.max_memory_mib,
                    "policy_memory",
                ),
                (
                    observation.provisioned_storage_mib,
                    policy.max_storage_mib,
                    "policy_storage",
                ),
                (
                    observation.provider_used_nodes,
                    observation.provider_quota_nodes,
                    "provider_quota_nodes",
                ),
                (
                    observation.provider_used_vcpu_millis,
                    observation.provider_quota_vcpu_millis,
                    "provider_quota_vcpu",
                ),
                (
                    observation.provider_used_memory_mib,
                    observation.provider_quota_memory_mib,
                    "provider_quota_memory",
                ),
                (
                    observation.provider_used_storage_mib,
                    observation.provider_quota_storage_mib,
                    "provider_quota_storage",
                ),
            )
            blockers.extend(
                f"resource_forecast_{label}_exhausted"
                for used, limit, label in limits
                if used >= limit
            )
        if target.desired_state != "active":
            blockers.append("resource_forecast_target_not_active")
        if target.health_status != "healthy":
            blockers.append("resource_forecast_target_unhealthy")
        observed_fit = 0
        immediate_executable = 0
        scale_nodes = 0
        slots_per_node = 0
        configured_scale_slots = 0
        accepted_profile = bool(
            binding is not None
            and binding.enabled
            and calibration is not None
            and calibration.eligible
        )
        if accepted_profile and calibration is not None and observation is not None:
            observed_fit = _fit_slots(
                max(0, observation.allocatable_cpu_millis - observation.requested_cpu_millis),
                max(0, observation.allocatable_memory_mib - observation.requested_memory_mib),
                max(0, observation.allocatable_storage_mib - observation.requested_storage_mib),
                calibration,
            )
        if (
            accepted_profile
            and calibration is not None
            and observation is not None
            and policy is not None
        ):
            node_headrooms = [
                max(0, policy.max_nodes - observation.active_nodes),
                max(0, observation.provider_quota_nodes - observation.provider_used_nodes),
                max(0, policy.max_vcpu_millis - observation.provisioned_vcpu_millis)
                // policy.node_cpu_millis,
                max(0, policy.max_memory_mib - observation.provisioned_memory_mib)
                // policy.node_memory_mib,
                max(0, policy.max_storage_mib - observation.provisioned_storage_mib)
                // policy.node_storage_mib,
                max(
                    0,
                    observation.provider_quota_vcpu_millis - observation.provider_used_vcpu_millis,
                )
                // policy.node_cpu_millis,
                max(
                    0,
                    observation.provider_quota_memory_mib - observation.provider_used_memory_mib,
                )
                // policy.node_memory_mib,
                max(
                    0,
                    observation.provider_quota_storage_mib - observation.provider_used_storage_mib,
                )
                // policy.node_storage_mib,
            ]
            scale_nodes = min(node_headrooms)
            slots_per_node = _fit_slots(
                policy.node_cpu_millis,
                policy.node_memory_mib,
                policy.node_storage_mib,
                calibration,
            )
            configured_scale_slots = scale_nodes * slots_per_node
        canonical_blockers = sorted(set(blockers))
        forecast_fresh = not canonical_blockers and observation_fresh
        if forecast_fresh:
            immediate_executable = observed_fit
        results.append(
            {
                "target_id": target.id,
                "pool_id": target.logical_pool_id,
                "forecast_is_fresh": forecast_fresh,
                "binding": (
                    {
                        "calibration_id": str(binding.calibration_id),
                        "enabled": binding.enabled,
                        "reason": binding.reason,
                        "version": binding.version,
                    }
                    if binding is not None
                    else None
                ),
                "calibration": (
                    {
                        "id": str(calibration.id),
                        "eligible": calibration.eligible,
                        "source_pool_id": calibration.source_pool_id,
                        "resource_profile": calibration.resource_profile,
                        "candidate_sha": calibration.candidate_sha,
                        "source_version": calibration.source_version,
                        "trial_attempts": calibration.trial_attempts,
                        "distinct_tasks": calibration.distinct_tasks,
                        "evidence_duration_seconds": calibration.evidence_duration_seconds,
                        "peak_batch_concurrency": calibration.peak_batch_concurrency,
                        "recommended_cpu_millis": calibration.recommended_cpu_millis,
                        "recommended_memory_mib": calibration.recommended_memory_mib,
                        "recommended_ephemeral_storage_mib": (
                            calibration.recommended_ephemeral_storage_mib
                        ),
                        "recommended_pids": calibration.recommended_pids,
                        "evidence_sha256": calibration.evidence_sha256,
                    }
                    if calibration is not None
                    else None
                ),
                "observed_fit_slots": observed_fit,
                "immediate_executable_slots": immediate_executable,
                "configured_additional_nodes": scale_nodes,
                "configured_slots_per_node": slots_per_node,
                "configured_scale_headroom_slots": configured_scale_slots,
                "configured_total_fit_slots": observed_fit + configured_scale_slots,
                "blockers": canonical_blockers,
                "recent_calibrations": [
                    {
                        "id": str(row.id),
                        "eligible": row.eligible,
                        "source_pool_id": row.source_pool_id,
                        "resource_profile": row.resource_profile,
                        "candidate_sha": row.candidate_sha,
                        "source_version": row.source_version,
                        "trial_attempts": row.trial_attempts,
                        "peak_batch_concurrency": row.peak_batch_concurrency,
                        "blockers": row.blockers_json,
                        "evidence_sha256": row.evidence_sha256,
                        "created_at": row.created_at.isoformat(),
                    }
                    for row in latest_calibrations
                ],
            }
        )
    return {"targets": results}


__all__ = [
    "CalibrationAttemptSample",
    "CalibrationProjection",
    "create_execution_resource_calibration",
    "derive_resource_calibration",
    "fetch_execution_resource_profile_status",
    "upsert_execution_resource_profile_binding",
]
