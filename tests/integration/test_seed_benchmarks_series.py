"""`_seed_benchmarks_from_entrypoints` populates + backfills `series`.

Two failure modes the SPA's grouped Benchmarks page surfaced:

1. Fresh seed didn't write `adapter.series` into the new Benchmark
   row, so every benchmark from a clean `loom service up` landed in
   the SPA's "Other" bucket.
2. Pre-PR-1 deployments have rows with `series=NULL` that re-seed
   skipped via the `existing is not None` short-circuit, leaving them
   stuck in "Other" forever.

The fix writes `series` on insert AND backfills NULLs on rows that
already exist; this test pins both paths."""

from __future__ import annotations

import pytest
from loom_benchmarks.registry import REGISTRY
from scripts.seed_test_data import _seed_benchmarks_from_entrypoints
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark


@pytest.fixture
def session(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    sl = sessionmaker(engine)
    registry_ids = tuple(sorted({*REGISTRY, *(adapter.name for adapter in REGISTRY.values())}))
    with sl() as s:
        s.execute(delete(Benchmark).where(Benchmark.id.in_(registry_ids)))
        s.commit()
    with sl() as s:
        yield s
    with sl() as s:
        s.execute(delete(Benchmark).where(Benchmark.id.in_(registry_ids)))
        s.commit()
    engine.dispose()


def test_fresh_seed_writes_series_from_adapter(session) -> None:
    _seed_benchmarks_from_entrypoints(session)
    session.commit()
    rows = {
        b.id: b.series for b in session.execute(
            select(Benchmark).where(Benchmark.id.in_([
                "aime-22", "aime-25",
                "swe-bench", "swe-bench-multimodal",
                "humaneval",
            ])),
        ).scalars().all()
    }
    # AIME siblings group under "aime".
    assert rows.get("aime-22") == "aime"
    assert rows.get("aime-25") == "aime"
    # SWE-Bench siblings group under "swe-bench".
    assert rows.get("swe-bench") == "swe-bench"
    assert rows.get("swe-bench-multimodal") == "swe-bench"
    # PR-2 (no Other): standalone code benchmarks land in series='code'.
    assert rows.get("humaneval") == "code"


def test_reseed_backfills_null_series_on_existing_rows(session) -> None:
    """Pre-PR-1 row simulation: insert an `aime-22` row
    with series=NULL, then re-run the seed. The seed must NOT skip the
    backfill just because the row already exists."""
    session.execute(insert(Benchmark).values(
        id="aime-22",
        display_name="AIME (legacy seed)",
        upstream_kind="huggingface",
        upstream_locator="AI-MO/aimo-validation-aime",
        upstream_revision="main",
        license_spdx="proprietary-MAA",
        license_url="",
        splits=["train"],
        series=None,
    ))
    session.commit()
    _seed_benchmarks_from_entrypoints(session)
    session.commit()
    series = session.execute(
        select(Benchmark.series).where(Benchmark.id == "aime-22"),
    ).scalar_one()
    assert series == "aime"
