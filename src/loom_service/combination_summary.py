"""Per-combination batch reward and usage summaries."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import func, select

from loom.db.schema import LlmCall
from loom_llm_gateway.rate_card import (
    COST_META_CONFIDENCE_KEY,
    COST_META_SOURCE_KEY,
)
from loom_service.usage_accounting import (
    cost_meta_filter as _cost_meta_filter,
)
from loom_service.usage_accounting import (
    empty_usage_projection,
    summarize_usage_counts,
    usage_status_filter,
)

_SCORED_TRIAL_STATES = frozenset({"succeeded", "failed"})


class _CombinationTrial(Protocol):
    @property
    def id(self) -> UUID: ...

    @property
    def combination_idx(self) -> int: ...

    @property
    def state(self) -> str: ...

    @property
    def result(self) -> dict[str, Any] | None: ...


@dataclass
class _UsageAccumulator:
    llm_calls_count: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_cost_usd: Decimal = Decimal("0")
    priced_llm_calls_count: int = 0
    token_only_llm_calls_count: int = 0
    price_unknown_llm_calls_count: int = 0
    failed_upstream_llm_calls_count: int = 0
    partial_usage_llm_calls_count: int = 0
    missing_usage_llm_calls_count: int = 0
    cost_source_counts: dict[str, int] = field(
        default_factory=lambda: {
            "operator-supplied": 0,
            "rate-card": 0,
            "tokens-only": 0,
            "unpriced": 0,
        },
    )
    cost_confidence_counts: dict[str, int] = field(
        default_factory=lambda: {
            "configured": 0,
            "not_applicable": 0,
            "unavailable": 0,
        },
    )

    def add_row(self, row: Any) -> None:
        self.llm_calls_count += int(row.llm_calls_count or 0)
        self.total_prompt_tokens += int(row.total_prompt_tokens or 0)
        self.total_completion_tokens += int(row.total_completion_tokens or 0)
        self.total_cost_usd += row.total_cost_usd or Decimal("0")
        self.priced_llm_calls_count += int(row.priced_llm_calls_count or 0)
        self.token_only_llm_calls_count += int(row.token_only_llm_calls_count or 0)
        self.price_unknown_llm_calls_count += int(row.price_unknown_llm_calls_count or 0)
        self.failed_upstream_llm_calls_count += int(
            row.failed_upstream_llm_calls_count or 0,
        )
        self.partial_usage_llm_calls_count += int(row.partial_usage_llm_calls_count or 0)
        self.missing_usage_llm_calls_count += int(row.missing_usage_llm_calls_count or 0)
        self.cost_source_counts["operator-supplied"] += int(
            row.cost_source_operator_supplied_count or 0,
        )
        self.cost_source_counts["rate-card"] += int(row.cost_source_rate_card_count or 0)
        self.cost_source_counts["tokens-only"] += int(
            row.cost_source_tokens_only_count or 0,
        )
        self.cost_source_counts["unpriced"] += int(row.cost_source_unpriced_count or 0)
        self.cost_confidence_counts["configured"] += int(
            row.cost_confidence_configured_count or 0,
        )
        self.cost_confidence_counts["not_applicable"] += int(
            row.cost_confidence_not_applicable_count or 0,
        )
        self.cost_confidence_counts["unavailable"] += int(
            row.cost_confidence_unavailable_count or 0,
        )

    def summarize(self) -> dict[str, Any]:
        out = summarize_usage_counts(
            llm_calls_count=self.llm_calls_count,
            total_prompt_tokens=self.total_prompt_tokens,
            total_completion_tokens=self.total_completion_tokens,
            total_cost_usd=self.total_cost_usd,
            priced_llm_calls_count=self.priced_llm_calls_count,
            token_only_llm_calls_count=self.token_only_llm_calls_count,
            price_unknown_llm_calls_count=self.price_unknown_llm_calls_count,
            failed_upstream_llm_calls_count=self.failed_upstream_llm_calls_count,
            partial_usage_llm_calls_count=self.partial_usage_llm_calls_count,
            missing_usage_llm_calls_count=self.missing_usage_llm_calls_count,
            cost_source_counts=self.cost_source_counts,
            cost_confidence_counts=self.cost_confidence_counts,
        )
        out.pop("total_cost_usd", None)
        return out


def _price_unknown_call_filter() -> Any:
    return LlmCall.rate_card_hash.like("facade:rate-card:missing%") | _cost_meta_filter(
        COST_META_SOURCE_KEY, "unpriced"
    )


def _priced_call_filter() -> Any:
    return (
        ~LlmCall.rate_card_hash.like("facade:tokens-only%")
        & ~_price_unknown_call_filter()
        & (LlmCall.rate_card_hash != "failed-upstream")
    )


def _rollup_from_result(result: dict[str, Any] | None) -> float | None:
    if not result:
        return None
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        return float(reward) if reward is not None else None
    except (TypeError, ValueError):
        return None


def _model_display(combo: dict[str, Any]) -> str:
    provider_model_id = combo.get("provider_model_id")
    if provider_model_id:
        return str(provider_model_id)
    agent_model = combo.get("agent_model")
    if isinstance(agent_model, dict):
        name = agent_model.get("name")
        provider = agent_model.get("provider")
        if name:
            return str(name)
        if provider:
            return str(provider)
    return "no model"


def combination_label(combo: dict[str, Any]) -> str:
    """Use the same readable identity in API summaries and delivery exports."""
    label = combo.get("label")
    if isinstance(label, str) and label.strip():
        return label
    agent_name = str(combo.get("agent_name") or "unknown agent")
    if combo.get("agent_version"):
        agent_name += "@" + str(combo["agent_version"])
    return f"{agent_name} / {_model_display(combo)}"


def _combo_n_per_task(combo: dict[str, Any]) -> int:
    try:
        return max(int(combo.get("n_per_task", 1) or 1), 0)
    except (TypeError, ValueError):
        return 1


def _expected_counts_by_combination(
    combinations: Sequence[dict[str, Any]],
    *,
    expected_trial_count: int | None,
    required_worker_pool_count: int,
    fanout_errors: Sequence[dict[str, Any]] | None,
) -> dict[int, int | None]:
    if expected_trial_count is None:
        return {}

    fanout_counts = _fanout_error_counts_by_combination(
        combinations,
        fanout_errors=fanout_errors,
    )
    if fanout_counts is None:
        return {idx: None for idx in range(len(combinations))}

    expected_total = max(int(expected_trial_count), 0)
    original_expected_total = expected_total + sum(fanout_counts.values())
    coverage_count = max(int(required_worker_pool_count or 0), 0)
    base_expected = original_expected_total - coverage_count
    n_values = [_combo_n_per_task(combo) for combo in combinations]
    n_total = sum(n_values)
    if base_expected < 0 or n_total <= 0 or base_expected % n_total != 0:
        return {idx: None for idx in range(len(combinations))}

    task_count = base_expected // n_total
    return {
        idx: max(
            0,
            task_count * n + (coverage_count if idx == 0 else 0) - fanout_counts[idx],
        )
        for idx, n in enumerate(n_values)
    }


def _fanout_error_counts_by_combination(
    combinations: Sequence[dict[str, Any]],
    *,
    fanout_errors: Sequence[dict[str, Any]] | None,
) -> dict[int, int] | None:
    counts = {idx: 0 for idx in range(len(combinations))}
    seen_keys: set[str] = set()
    for item in fanout_errors or []:
        if not isinstance(item, dict):
            continue
        raw_key = item.get("idempotency_key")
        if raw_key:
            key = str(raw_key)
            if key in seen_keys:
                continue
            seen_keys.add(key)
        raw_idx = item.get("combination_idx")
        if raw_idx is None:
            return None
        try:
            idx = int(raw_idx)
        except (TypeError, ValueError):
            return None
        if idx not in counts:
            return None
        counts[idx] += 1
    return counts


async def _usage_by_combination_idx(
    session: Any,
    trials: Sequence[_CombinationTrial],
) -> dict[int, dict[str, Any]]:
    if not trials:
        return {}

    trial_to_combo = {trial.id: int(trial.combination_idx) for trial in trials}
    rows = (
        await session.execute(
            select(
                LlmCall.trial_id,
                func.coalesce(func.sum(LlmCall.input_tokens), 0).label(
                    "total_prompt_tokens",
                ),
                func.coalesce(func.sum(LlmCall.output_tokens), 0).label(
                    "total_completion_tokens",
                ),
                func.count(LlmCall.id).label("llm_calls_count"),
                func.coalesce(func.sum(LlmCall.cost_usd), 0).label("total_cost_usd"),
                func.count(LlmCall.id)
                .filter(_priced_call_filter())
                .label("priced_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash.like("facade:tokens-only%"))
                .label("token_only_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_price_unknown_call_filter())
                .label("price_unknown_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(LlmCall.rate_card_hash == "failed-upstream")
                .label("failed_upstream_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "operator-supplied"))
                .label("cost_source_operator_supplied_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"))
                .label("cost_source_rate_card_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only"))
                .label("cost_source_tokens_only_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_SOURCE_KEY, "unpriced"))
                .label("cost_source_unpriced_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured"))
                .label("cost_confidence_configured_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "not_applicable"))
                .label("cost_confidence_not_applicable_count"),
                func.count(LlmCall.id)
                .filter(_cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable"))
                .label("cost_confidence_unavailable_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("partial"))
                .label("partial_usage_llm_calls_count"),
                func.count(LlmCall.id)
                .filter(usage_status_filter("missing"))
                .label("missing_usage_llm_calls_count"),
            )
            .where(LlmCall.trial_id.in_(list(trial_to_combo)))
            .group_by(LlmCall.trial_id),
        )
    ).all()

    accumulators: dict[int, _UsageAccumulator] = {}
    for row in rows:
        combination_idx = trial_to_combo.get(row.trial_id)
        if combination_idx is None:
            continue
        accumulators.setdefault(combination_idx, _UsageAccumulator()).add_row(row)
    return {idx: accumulator.summarize() for idx, accumulator in accumulators.items()}


async def combination_summary_for_batch(
    session: Any,
    *,
    combinations: Sequence[dict[str, Any]],
    trials: Sequence[_CombinationTrial],
    expected_trial_count: int | None = None,
    required_worker_pool_count: int = 0,
    fanout_errors: Sequence[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build a deterministic summary for Batch.combinations.

    Legacy batches have an empty combinations list; return an empty summary
    so callers can expose the field without changing single-combination UI.
    """

    if not combinations:
        return []

    grouped: dict[int, list[_CombinationTrial]] = {idx: [] for idx in range(len(combinations))}
    for trial in trials:
        grouped.setdefault(int(trial.combination_idx), []).append(trial)

    expected_by_idx = _expected_counts_by_combination(
        combinations,
        expected_trial_count=expected_trial_count,
        required_worker_pool_count=required_worker_pool_count,
        fanout_errors=fanout_errors,
    )
    usage_by_idx = await _usage_by_combination_idx(session, trials)
    empty_usage = empty_usage_projection()
    empty_usage.pop("total_cost_usd", None)
    rows: list[dict[str, Any]] = []
    for idx, combo in enumerate(combinations):
        group = grouped.get(idx, [])
        reward_sum = 0.0
        scored_count = 0
        for trial in group:
            if str(trial.state) not in _SCORED_TRIAL_STATES:
                continue
            reward = _rollup_from_result(trial.result)
            if reward is None:
                continue
            reward_sum += reward
            scored_count += 1
        succeeded_count = sum(1 for trial in group if str(trial.state) == "succeeded")
        failed_count = sum(1 for trial in group if str(trial.state) == "failed")
        completed_count = sum(
            1 for trial in group if str(trial.state) in {"succeeded", "failed", "cancelled"}
        )
        row = {
            "combination_idx": idx,
            "label": combination_label(combo),
            "agent_name": combo.get("agent_name"),
            "agent_version": combo.get("agent_version"),
            "agent_model": combo.get("agent_model"),
            "provider_connection_id": (
                str(combo["provider_connection_id"])
                if combo.get("provider_connection_id") is not None
                else None
            ),
            "provider_model_id": combo.get("provider_model_id"),
            "n_per_task": _combo_n_per_task(combo),
            "expected_trial_count": expected_by_idx.get(idx),
            "trial_count": len(group),
            "completed_trial_count": completed_count,
            "scored_trial_count": scored_count,
            "succeeded_count": succeeded_count,
            "failed_count": failed_count,
            "aggregate_reward": (reward_sum / scored_count if scored_count > 0 else None),
        }
        row.update(usage_by_idx.get(idx, empty_usage))
        rows.append(row)
    return rows
