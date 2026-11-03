from __future__ import annotations

from decimal import Decimal

import pytest

from loom_service.usage_accounting import summarize_usage_counts


def test_tokens_only_usage_has_no_monetary_amount() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=120,
        total_completion_tokens=30,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=1,
        price_unknown_llm_calls_count=0,
    )

    assert summary["total_prompt_tokens"] == 120
    assert summary["total_completion_tokens"] == 30
    assert summary["total_tokens"] == 150
    assert summary["total_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == "not_applicable"
    assert summary["pricing_modes"] == ["tokens-only"]


def test_priced_usage_exposes_estimated_cost_amount() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=2,
        total_prompt_tokens=1000,
        total_completion_tokens=400,
        total_cost_usd=Decimal("0.012345"),
        priced_llm_calls_count=2,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
    )

    assert summary["estimated_cost_usd"] == 0.012345
    assert summary["cost_currency"] == "USD"
    assert summary["cost_status"] == "estimated"
    assert summary["pricing_modes"] == ["priced"]


def test_missing_rate_card_is_price_unknown_not_zero_cost() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=500,
        total_completion_tokens=50,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=1,
    )

    assert summary["total_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == "price_unknown"
    assert summary["pricing_modes"] == ["price-unknown"]


def test_failed_upstream_usage_is_not_priced() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=0,
        total_completion_tokens=0,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
        failed_upstream_llm_calls_count=1,
        missing_usage_llm_calls_count=1,
    )

    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == "failed_upstream"
    assert summary["pricing_modes"] == ["failed-upstream"]
    assert summary["priced_llm_calls_count"] == 0
    assert summary["failed_upstream_llm_calls_count"] == 1
    assert summary["usage_reporting_status"] == "missing"


def test_mixed_usage_marks_partial_cost_status() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=3,
        total_prompt_tokens=2000,
        total_completion_tokens=800,
        total_cost_usd=Decimal("0.040000"),
        priced_llm_calls_count=1,
        token_only_llm_calls_count=1,
        price_unknown_llm_calls_count=1,
    )

    assert summary["estimated_cost_usd"] == 0.04
    assert summary["cost_currency"] == "USD"
    assert summary["cost_status"] == "mixed"
    assert summary["pricing_modes"] == [
        "priced",
        "tokens-only",
        "price-unknown",
    ]


def test_incomplete_provider_usage_marks_estimate_confidence() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=3,
        total_prompt_tokens=2000,
        total_completion_tokens=800,
        total_cost_usd=Decimal("0.040000"),
        priced_llm_calls_count=3,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
        partial_usage_llm_calls_count=1,
        missing_usage_llm_calls_count=1,
    )

    assert summary["estimated_cost_usd"] == 0.04
    assert summary["partial_usage_llm_calls_count"] == 1
    assert summary["missing_usage_llm_calls_count"] == 1
    assert summary["usage_reporting_status"] == "partial"
    assert summary["usage_estimate_confidence"] == "partial"
    assert summary["cost_estimate_confidence"] == "partial"


def test_all_missing_usage_does_not_report_a_priced_zero_as_free() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=2,
        total_prompt_tokens=0,
        total_completion_tokens=0,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=2,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
        missing_usage_llm_calls_count=2,
        cost_confidence_counts={"configured": 2},
    )

    assert summary["total_cost_usd"] == 0.0  # Legacy storage subtotal.
    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_estimate_confidence"] == "missing"


@pytest.mark.parametrize(
    ("token_only", "price_unknown", "failed_upstream", "cost_status", "confidence"),
    [
        (0, 1, 0, "price_unknown", "unavailable"),
        (1, 0, 0, "not_applicable", "not_applicable"),
        (0, 0, 1, "failed_upstream", "unavailable"),
    ],
)
@pytest.mark.parametrize("usage_status", ["missing", "partial"])
def test_incomplete_usage_preserves_unpriced_cost_confidence(
    token_only: int, price_unknown: int, failed_upstream: int,
    cost_status: str, confidence: str, usage_status: str,
) -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1, total_prompt_tokens=0, total_completion_tokens=0,
        total_cost_usd=Decimal("0"), priced_llm_calls_count=0,
        token_only_llm_calls_count=token_only,
        price_unknown_llm_calls_count=price_unknown,
        failed_upstream_llm_calls_count=failed_upstream,
        missing_usage_llm_calls_count=int(usage_status == "missing"),
        partial_usage_llm_calls_count=int(usage_status == "partial"),
    )

    assert summary["estimated_cost_usd"] is None
    assert summary["cost_currency"] is None
    assert summary["cost_status"] == cost_status
    assert summary["cost_estimate_confidence"] == confidence
    assert summary["usage_reporting_status"] == usage_status
    assert summary["usage_estimate_confidence"] == usage_status


def test_explicit_priced_zero_with_complete_usage_is_preserved() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=10,
        total_completion_tokens=20,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=1,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
    )

    assert summary["estimated_cost_usd"] == 0.0
    assert summary["cost_estimate_confidence"] == "configured"


def test_cost_projection_carries_source_and_confidence_metadata() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=2,
        total_prompt_tokens=1000,
        total_completion_tokens=400,
        total_cost_usd=Decimal("0.012345"),
        priced_llm_calls_count=2,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
        cost_source_counts={"operator-supplied": 2},
        cost_confidence_counts={"configured": 2},
    )

    assert summary["estimated_cost_usd"] == 0.012345
    assert summary["cost_status"] == "estimated"
    assert summary["cost_estimate_source"] == "operator-supplied"
    assert summary["cost_estimate_confidence"] == "configured"


def test_unpriced_projection_is_unknown_not_zero_confidence() -> None:
    summary = summarize_usage_counts(
        llm_calls_count=1,
        total_prompt_tokens=500,
        total_completion_tokens=50,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=1,
        cost_source_counts={"unpriced": 1},
        cost_confidence_counts={"unavailable": 1},
    )

    assert summary["estimated_cost_usd"] is None
    assert summary["cost_status"] == "price_unknown"
    assert summary["cost_estimate_source"] == "unpriced"
    assert summary["cost_estimate_confidence"] == "unavailable"
