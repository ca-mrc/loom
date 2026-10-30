"""Chunked backfill of the 0182 `llm_calls` usage projection (#2687)."""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall
from loom_llm_gateway.llm_calls import usage_projection_columns
from loom_llm_gateway.rate_card import COST_META_SOURCE_KEY
from loom_service.llm_call_projection_backfill import _main, backfill, count_unprojected
from loom_service.usage_accounting import cost_meta_filter, usage_status_filter
from tests.integration.test_llm_calls_attempt_column import team_and_trial  # noqa: F401

_PRICED = {
    "_loom_cost_source": "rate-card",
    "_loom_cost_confidence": "configured",
    "_loom_usage_status": "partial",
}


async def test_backfill_projects_old_rows_resumably_without_changing_readers(
    postgres_url: str, team_and_trial: tuple[str, str],  # noqa: F811
) -> None:
    team_id, trial_id = team_and_trial
    extras = [
        _PRICED,
        {},
        {"_loom_cost_source": None, "_loom_usage_status": True},
        *({"_loom_usage_status": "missing"} for _ in range(4)),
    ]
    projected_row = {
        "step_id": "projected",
        "provider_extras": {"_loom_cost_source": "tokens-only"},
        **usage_projection_columns(_PRICED),
    }
    sync = create_engine(postgres_url)
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    rows = [
        {"step_id": f"old-{index}", "provider_extras": value}
        for index, value in enumerate(extras)
    ] + [projected_row]
    try:
        with sessionmaker(sync)() as s:
            for values in rows:
                s.execute(insert(LlmCall).values(
                    team_id=team_id, trial_id=trial_id, dialect="openai_chat", model="gpt-4o",
                    input_tokens=1, output_tokens=1, cost_usd=Decimal("0"),
                    rate_card_hash="abc", **values,
                ))
            s.commit()

            def reader_counts() -> tuple[int, ...]:
                s.rollback()
                return tuple(
                    s.execute(
                        select(func.count(LlmCall.id).filter(condition))
                        .where(LlmCall.trial_id == trial_id),
                    ).scalar_one()
                    for condition in (
                        cost_meta_filter(COST_META_SOURCE_KEY, "rate-card"),
                        usage_status_filter("partial"),
                        usage_status_filter("missing"),
                        usage_status_filter("true"),
                    )
                )

            before = reader_counts()
            progress: list[dict[str, object]] = []
            assert await backfill(
                factory, chunk_size=3, min_pause_seconds=0, report=progress.append,
            ) == len(extras)
            assert [step["through_id"] for step in progress][-1] == "end"
            assert len(progress) > 2
            async with factory() as session:
                assert await count_unprojected(session) == 0
            assert reader_counts() == before

            s.rollback()
            by_step = {
                row.step_id: (row.cost_source, row.cost_confidence, row.usage_status)
                for row in s.execute(
                    select(LlmCall).where(LlmCall.trial_id == trial_id),
                ).scalars()
            }
            assert by_step["old-0"] == ("rate-card", "configured", "partial")
            assert by_step["old-1"] == ("", "", "")
            assert by_step["old-2"] == ("", "", "true")
            # An already projected row keeps its columns, whatever the JSON says.
            assert by_step["projected"] == ("rate-card", "configured", "partial")

            assert await backfill(factory, chunk_size=3, min_pause_seconds=0) == 0
    finally:
        await engine.dispose()
        sync.dispose()


async def test_command_counts_by_default_and_applies_on_request(
    postgres_url: str,
    team_and_trial: tuple[str, str],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    team_id, trial_id = team_and_trial
    monkeypatch.setattr(
        "loom_service.llm_call_projection_backfill.LoomServiceSettings",
        lambda: SimpleNamespace(db_engine_url=postgres_url, db_engine_connect_args={}),
    )
    sync = create_engine(postgres_url)
    try:
        with sessionmaker(sync)() as s:
            s.execute(insert(LlmCall).values(
                team_id=team_id, trial_id=trial_id, step_id="old", dialect="openai_chat",
                model="gpt-4o", input_tokens=1, output_tokens=1, cost_usd=Decimal("0"),
                rate_card_hash="abc", provider_extras=_PRICED,
            ))
            s.commit()

            async def run(*, apply: bool) -> list[dict[str, object]]:
                await _main(argparse.Namespace(apply=apply, chunk_size=500, min_pause_seconds=0))
                return [json.loads(line) for line in capsys.readouterr().out.splitlines()]

            assert await run(apply=False) == [{"remaining": 1}]
            assert s.execute(
                select(LlmCall.usage_status).where(LlmCall.trial_id == trial_id),
            ).scalar_one() is None
            s.rollback()
            lines = await run(apply=True)
            assert lines[-1] == {"remaining": 0}
            assert s.execute(
                select(LlmCall.usage_status).where(LlmCall.trial_id == trial_id),
            ).scalar_one() == "partial"
    finally:
        sync.dispose()
