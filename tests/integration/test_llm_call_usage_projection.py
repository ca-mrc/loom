"""Migration 0182 (#2687): insert-time cost/usage projection on `llm_calls`.

Verifies:
- `record_call` / `record_failed_call` write the projected columns.
- Usage filters read the projection for new rows and fall back to
  `provider_extras` for rows written before 0182 (NULL columns).
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.llm_calls import record_call, record_failed_call, usage_projection_columns
from loom_llm_gateway.rate_card import COST_META_CONFIDENCE_KEY, COST_META_SOURCE_KEY
from loom_service.usage_accounting import cost_meta_filter, usage_status_filter
from tests.integration.test_llm_calls_attempt_column import team_and_trial  # noqa: F401


def test_projection_mirrors_json_text_extraction() -> None:
    assert usage_projection_columns({}) == {
        "cost_source": "", "cost_confidence": "", "usage_status": "",
    }
    assert usage_projection_columns({
        "_loom_cost_source": "rate-card",
        "_loom_cost_confidence": None,
        "_loom_usage_status": True,
    }) == {"cost_source": "rate-card", "cost_confidence": "", "usage_status": "true"}


async def test_record_paths_write_projection(
    postgres_url: str, team_and_trial: tuple[str, str],  # noqa: F811
) -> None:
    team_id, trial_id = (UUID(value) for value in team_and_trial)
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await record_call(
                session, team_id=team_id, trial_id=trial_id, step_id="priced",
                dialect="openai_chat", model="gpt-4o",
                usage=TokenUsage(input_tokens=5, output_tokens=1, provider_extras={
                    "_loom_cost_source": "rate-card",
                    "_loom_cost_confidence": "configured",
                    "_loom_usage_status": "partial",
                }),
                cost_usd=0.001, rate_card_hash="abc",
            )
            await record_call(
                session, team_id=team_id, trial_id=trial_id, step_id="bare",
                dialect="openai_chat", model="gpt-4o",
                usage=TokenUsage(input_tokens=5, output_tokens=1),
                cost_usd=0, rate_card_hash="abc",
            )
            await record_failed_call(
                session, team_id=team_id, trial_id=trial_id, step_id="failed",
                dialect="openai_chat", model="gpt-4o", failure_category="upstream_5xx",
            )
    finally:
        await engine.dispose()

    sync = create_engine(postgres_url)
    try:
        with sessionmaker(sync)() as s:
            rows = {
                row.step_id: (row.cost_source, row.cost_confidence, row.usage_status)
                for row in s.execute(select(LlmCall).where(LlmCall.trial_id == trial_id)).scalars()
            }
    finally:
        sync.dispose()
    assert rows == {
        "priced": ("rate-card", "configured", "partial"),
        "bare": ("", "", ""),
        "failed": ("", "", "missing"),
    }


def test_filters_prefer_projection_and_fall_back_for_legacy_rows(
    postgres_url: str, team_and_trial: tuple[str, str],  # noqa: F811
) -> None:
    team_id, trial_id = team_and_trial
    priced = {
        "_loom_cost_source": "rate-card",
        "_loom_cost_confidence": "configured",
        "_loom_usage_status": "partial",
    }
    rows = [
        # Written before 0182: NULL projection, keys only in the JSON.
        {"provider_extras": priced},
        # Projected row: the columns win, so the stale JSON is never consulted.
        {
            "provider_extras": {"_loom_cost_source": "tokens-only", "_loom_usage_status": "missing"},
            **usage_projection_columns(priced),
        },
        # Projected row whose keys were absent: '' must not fall back to the JSON.
        {"provider_extras": {"_loom_usage_status": "missing"}, **usage_projection_columns({})},
        # Legacy local call keeps its unpriced interpretation.
        {"provider_extras": {}, "rate_card_hash": "local-server-no-card"},
    ]
    sync = create_engine(postgres_url)
    try:
        with sessionmaker(sync)() as s:
            for index, values in enumerate(rows):
                s.execute(insert(LlmCall).values(
                    team_id=team_id, trial_id=trial_id, step_id=f"row-{index}",
                    dialect="openai_chat", model="gpt-4o", input_tokens=1, output_tokens=1,
                    cost_usd=Decimal("0"), **{"rate_card_hash": "abc", **values},
                ))
            s.commit()

            def count(condition: object) -> int:
                return s.execute(
                    select(func.count(LlmCall.id).filter(condition))
                    .where(LlmCall.trial_id == trial_id),
                ).scalar_one()

            assert count(cost_meta_filter(COST_META_SOURCE_KEY, "rate-card")) == 2
            assert count(cost_meta_filter(COST_META_SOURCE_KEY, "tokens-only")) == 0
            assert count(cost_meta_filter(COST_META_SOURCE_KEY, "unpriced")) == 1
            assert count(cost_meta_filter(COST_META_CONFIDENCE_KEY, "configured")) == 2
            assert count(cost_meta_filter(COST_META_CONFIDENCE_KEY, "unavailable")) == 1
            assert count(usage_status_filter("partial")) == 2
            assert count(usage_status_filter("missing")) == 0
    finally:
        sync.dispose()
