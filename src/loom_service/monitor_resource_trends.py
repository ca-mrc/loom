"""Read bounded, measured resource trends for physical execution pools."""

from datetime import UTC, datetime, timedelta
from typing import Literal

from pydantic import ValidationError
from sqlalchemy import case, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ExecutionCapacityObservation, ServiceExecutionTarget
from loom_execution_capacity_collector.contracts import PoolResourceUsage
from loom_service import wire_responses as wire

TrendWindow = Literal["15m", "1h", "24h"]
_Resource = Literal["cpu", "memory", "disk"]
_WINDOWS = {"15m": (900, 30), "1h": (3600, 60), "24h": (86400, 300)}
_FRESH_SECONDS = 90
# A 15-second collector yields at most 5,760 observations over 24 hours.
_MAX_OBSERVATIONS = 6000
_MAX_POOLS = 100


def _metric(
    rows: list[PoolResourceUsage],
    kind: _Resource,
    *,
    now: datetime,
    duration: int,
    step: int,
    current_usage: PoolResourceUsage | None,
) -> wire.MonitorTrendMetric:
    # Keep the original source clock. Multiple collector observations may read
    # the same node snapshot and must never make it appear newer than it is.
    end = int(now.timestamp())
    buckets: dict[int, tuple[datetime, float]] = {}
    latest = None
    for row in rows:
        sample = getattr(row, kind)
        if sample is None or sample.sampled_at > now:
            continue
        if latest is None or sample.sampled_at > latest.sampled_at:
            latest = sample
        at = sample.sampled_at
        bucket = int(at.timestamp()) // step
        if at.timestamp() < end - duration:
            continue
        previous_bucket = buckets.get(bucket)
        if previous_bucket is None or at > previous_bucket[0]:
            buckets[bucket] = (at, 100 * sample.used / sample.total)
    points: list[wire.MonitorTrendPoint] = []
    previous_stamp = None
    gap_seconds = max(75, 1.5 * step)
    for stamp, value in sorted(buckets.values()):
        if previous_stamp is not None and (stamp - previous_stamp).total_seconds() > gap_seconds:
            gap = previous_stamp + timedelta(seconds=gap_seconds)
            points.append({"timestamp": gap.isoformat(), "value": None})
        points.append({"timestamp": stamp.isoformat(), "value": value})
        previous_stamp = stamp

    current = getattr(current_usage, kind) if current_usage is not None else None
    if current is not None and current.sampled_at > now:
        current = None
    latest = current if current is not None else latest
    ready = current_usage.ready_nodes if current_usage is not None else None
    covered = current.covered_nodes if current is not None else (0 if ready is not None else None)
    if latest is None:
        return {
            "status": "unavailable", "last_sample_at": None, "points": [],
            "latest": None, "covered_nodes": covered, "ready_nodes": ready,
        }
    age = (now - latest.sampled_at).total_seconds()
    return {
        "status": "live" if current is not None and age <= _FRESH_SECONDS else "stale",
        "last_sample_at": latest.sampled_at.isoformat(),
        "points": points,
        "latest": {"used": latest.used, "total": latest.total},
        "covered_nodes": covered,
        "ready_nodes": ready,
    }


async def load_resource_trends(
    session: AsyncSession, *, window: TrendWindow, target_id: str | None
) -> wire.MonitorResourceTrends | None:
    owner = ServiceExecutionTarget
    pool_rows = (await session.execute(
        select(owner.id, owner.logical_pool_id, owner.region)
        .where(
            owner.provider == "nebius",
            owner.spec_json["capacity_owner_target_id"].as_string().is_(None),
            owner.desired_state.in_(("active", "draining")),
        )
        .order_by(case((owner.desired_state == "active", 0), else_=1), owner.region, owner.id)
        .limit(_MAX_POOLS)
    )).all()
    pools: list[wire.MonitorTrendPool] = [
        {"target_id": row.id, "pool_id": row.logical_pool_id, "region": row.region}
        for row in pool_rows
    ]
    selected = target_id or (pools[0]["target_id"] if pools else None)
    if target_id and not any(pool["target_id"] == target_id for pool in pools):
        return None

    now = datetime.now(UTC)
    duration, step = _WINDOWS[window]
    observations: list[PoolResourceUsage] = []
    current_usage = None
    if selected is not None:
        observation = ExecutionCapacityObservation
        usage = observation.observation_json["resource_usage"]
        earliest = now - timedelta(seconds=duration + _FRESH_SECONDS)
        rows = (await session.execute(
            select(usage, observation.observed_at)
            .where(
                observation.target_id == selected,
                observation.observed_at >= earliest,
                observation.observed_at <= now,
                usage.astext.is_not(None),
            )
            .order_by(observation.observed_at.desc())
            .limit(_MAX_OBSERVATIONS)
        )).all()
        for raw, observed_at in rows:
            try:
                projection = PoolResourceUsage.model_validate(raw)
            except ValidationError:
                # Malformed optional measurements cannot hide other history.
                continue
            if not observations and (now - observed_at).total_seconds() <= _FRESH_SECONDS:
                current_usage = projection
            observations.append(projection)

        if window != "24h":
            # Preserve the last measured reading when a short chart has no
            # samples. Fetch at most one older observation per missing metric.
            for kind in ("cpu", "memory", "disk"):
                if any(getattr(row, kind) is not None for row in observations):
                    continue
                raw = await session.scalar(
                    select(usage)
                    .where(
                        observation.target_id == selected,
                        observation.observed_at >= now - timedelta(hours=24, seconds=_FRESH_SECONDS),
                        observation.observed_at < earliest,
                        usage[kind].astext.is_not(None),
                    )
                    .order_by(observation.observed_at.desc())
                    .limit(1)
                )
                if raw is not None:
                    try:
                        observations.append(PoolResourceUsage.model_validate(raw))
                    except ValidationError:
                        continue

    def series(kind: _Resource) -> wire.MonitorTrendMetric:
        return _metric(observations, kind, now=now, duration=duration, step=step,
                       current_usage=current_usage)

    return {
        "pools": pools, "target_id": selected, "window": window,
        "generated_at": now.isoformat(), "step_seconds": step,
        "metrics": {"cpu": series("cpu"), "memory": series("memory"), "disk": series("disk")},
    }
