"""Shared stale-running trial diagnostics for reclaim and debug evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class StaleRunningDecision:
    decision: str
    reason: str
    reclaimable: bool
    runtime_sec: float | None
    silence_sec: float | None
    agent_timeout_sec: float | None
    hard_deadline_sec: float | None
    last_event_at: datetime | None
    last_llm_call_at: datetime | None
    last_activity_at: datetime | None
    worker_last_seen_at: datetime | None
    worker_heartbeat_age_sec: float | None
    worker_heartbeat_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "reason": self.reason,
            "reclaimable": self.reclaimable,
            "runtime_sec": self.runtime_sec,
            "silence_sec": self.silence_sec,
            "agent_timeout_sec": self.agent_timeout_sec,
            "hard_deadline_sec": self.hard_deadline_sec,
            "last_event_at": _iso(self.last_event_at),
            "last_llm_call_at": _iso(self.last_llm_call_at),
            "last_activity_at": _iso(self.last_activity_at),
            "worker_last_seen_at": _iso(self.worker_last_seen_at),
            "worker_heartbeat_age_sec": self.worker_heartbeat_age_sec,
            "worker_heartbeat_status": self.worker_heartbeat_status,
        }


def effective_agent_timeout_sec(
    *,
    trial_config: Any,
    task_config: Any,
    step_config: Any | None = None,
) -> float | None:
    """Resolve the execution budget shared by steps and trial watchdogs.

    A concrete ``step_config`` resolves that step exactly.  Trial-level
    callers omit it and receive the largest effective step budget, so the
    watchdog cannot pre-empt a valid step override.
    """
    trial_config_d = _as_mapping(trial_config)
    task_config_d = _as_mapping(task_config)
    base = _float_or_none(_nested_get(task_config_d, ("agent", "timeout_sec")))
    if step_config is not None:
        step_override = _float_or_none(
            _nested_get(_as_mapping(step_config), ("agent", "timeout_sec")),
        )
        if step_override is not None:
            base = step_override
    else:
        step_timeouts = [
            value
            for step in task_config_d.get("steps") or []
            if (
                value := _float_or_none(
                    _nested_get(_as_mapping(step), ("agent", "timeout_sec")),
                )
            )
            is not None
        ]
        if step_timeouts:
            base = max([base, *step_timeouts] if base is not None else step_timeouts)
    override = _float_or_none(trial_config_d.get("override_agent_timeout_sec"))
    if override is None:
        harbor = _as_mapping(trial_config_d.get("harbor_config"))
        override = _float_or_none(harbor.get("agent_timeout_sec")) if harbor else None
    if override is not None:
        base = override
    if base is None:
        return None
    multiplier = _float_or_none(trial_config_d.get("agent_timeout_multiplier")) or 1.0
    return base * multiplier


def evaluate_stale_running_trial(
    *,
    state: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    trial_config: Any,
    task_config: Any,
    last_event_at: datetime | None,
    last_llm_call_at: datetime | None,
    worker_last_seen_at: datetime | None,
    now: datetime,
    worker_heartbeat_expiry_sec: float,
    timeout_multiplier: float,
    grace_sec: float,
    silence_sec: float,
) -> StaleRunningDecision:
    now = _aware(now)
    started_at = _aware_or_none(started_at)
    finished_at = _aware_or_none(finished_at)
    last_event_at = _aware_or_none(last_event_at)
    last_llm_call_at = _aware_or_none(last_llm_call_at)
    worker_last_seen_at = _aware_or_none(worker_last_seen_at)

    agent_timeout_sec = effective_agent_timeout_sec(
        trial_config=trial_config,
        task_config=task_config,
    )
    hard_deadline_sec = (
        agent_timeout_sec * timeout_multiplier + grace_sec
        if agent_timeout_sec is not None and timeout_multiplier > 0
        else None
    )
    runtime_sec = (
        max(0.0, ((finished_at or now) - started_at).total_seconds())
        if started_at is not None
        else None
    )
    last_activity_at = _latest_not_none(started_at, last_event_at, last_llm_call_at)
    silence_value_sec = (
        max(0.0, (now - last_activity_at).total_seconds())
        if last_activity_at is not None
        else None
    )
    worker_age_sec = (
        max(0.0, (now - worker_last_seen_at).total_seconds())
        if worker_last_seen_at is not None
        else None
    )
    if worker_age_sec is None:
        heartbeat_status = "unknown"
    elif worker_age_sec <= worker_heartbeat_expiry_sec:
        heartbeat_status = "fresh"
    else:
        heartbeat_status = "stale"

    def keep(reason: str) -> StaleRunningDecision:
        return StaleRunningDecision(
            decision="keep",
            reason=reason,
            reclaimable=False,
            runtime_sec=runtime_sec,
            silence_sec=silence_value_sec,
            agent_timeout_sec=agent_timeout_sec,
            hard_deadline_sec=hard_deadline_sec,
            last_event_at=last_event_at,
            last_llm_call_at=last_llm_call_at,
            last_activity_at=last_activity_at,
            worker_last_seen_at=worker_last_seen_at,
            worker_heartbeat_age_sec=worker_age_sec,
            worker_heartbeat_status=heartbeat_status,
        )

    if state != "running":
        return keep("not_running")
    if started_at is None:
        return keep("not_started")
    if heartbeat_status != "fresh":
        return keep(f"worker_heartbeat_{heartbeat_status}")
    if hard_deadline_sec is None or runtime_sec is None:
        return keep("missing_timeout")
    if runtime_sec <= hard_deadline_sec:
        return keep("within_timeout")
    if silence_value_sec is None:
        return keep("missing_activity")
    if silence_value_sec <= silence_sec:
        return keep("recent_activity")

    return StaleRunningDecision(
        decision="reclaim",
        reason="fresh_worker_timeout_and_silent",
        reclaimable=True,
        runtime_sec=runtime_sec,
        silence_sec=silence_value_sec,
        agent_timeout_sec=agent_timeout_sec,
        hard_deadline_sec=hard_deadline_sec,
        last_event_at=last_event_at,
        last_llm_call_at=last_llm_call_at,
        last_activity_at=last_activity_at,
        worker_last_seen_at=worker_last_seen_at,
        worker_heartbeat_age_sec=worker_age_sec,
        worker_heartbeat_status=heartbeat_status,
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    config = getattr(value, "config", None)
    if isinstance(config, dict):
        return config
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        if isinstance(dumped, dict):
            return dumped
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        return attributes
    return {}


def _nested_get(value: Any, path: tuple[str, ...]) -> Any:
    item: Any = value
    for key in path:
        item = item.get(key) if isinstance(item, dict) else getattr(item, key, None)
    return item


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _latest_not_none(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return max(present)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _aware_or_none(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return _aware(value)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
