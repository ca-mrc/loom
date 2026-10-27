from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom_cli.benchmark_readiness import run_readiness_audit


@pytest.fixture
def fake_benchmark_rows(postgres_url: str) -> Iterator[None]:
    sync_engine = create_engine(postgres_url)
    try:
        with sessionmaker(sync_engine)() as session:
            session.execute(delete(TaskRow).where(TaskRow.benchmark_id == "fake-bench"))
            session.execute(delete(Benchmark).where(Benchmark.id == "fake-bench"))
            session.execute(
                insert(Benchmark).values(
                    id="fake-bench",
                    display_name="Fake Bench",
                    upstream_kind="huggingface",
                    upstream_locator="fake/source",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="",
                    splits=["test"],
                    series="fake",
                    # Retained legacy catalogs may be admitted while their
                    # missing normalized configs still need repair diagnostics.
                    execution_state="runnable",
                )
            )
            session.execute(
                insert(TaskRow).values(
                    id="fake-bench/legacy",
                    checksum="sha256:" + "1" * 64,
                    config={},
                    source="hf://PRHW/loom-benchmark-fake-bench@main/legacy/",
                    license="MIT",
                    benchmark_id="fake-bench",
                    tags={},
                )
            )
            session.commit()
        yield
    finally:
        with sessionmaker(sync_engine)() as session:
            session.execute(delete(TaskRow).where(TaskRow.benchmark_id == "fake-bench"))
            session.execute(delete(Benchmark).where(Benchmark.id == "fake-bench"))
            session.commit()
        sync_engine.dispose()


async def test_readiness_audit_reports_legacy_placeholder_from_db(
    fake_benchmark_rows: None,
    postgres_url: str,
) -> None:
    items = await run_readiness_audit(
        db_url=postgres_url,
        benchmark="fake-bench",
        registry_names={"fake-bench"},
    )

    assert len(items) == 1
    assert items[0].id == "fake-bench"
    assert items[0].raw_task_count == 1
    assert items[0].valid_task_config_count == 0
    assert items[0].readiness_state == "blocked"
    assert items[0].blocker_reason == "manifest_legacy_missing_task_config"


async def test_readiness_audit_reports_pending_catalog_before_structural_repair(
    fake_benchmark_rows: None,
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(update(Benchmark).where(Benchmark.id == "fake-bench")
                            .values(execution_state="pending"))
            session.commit()
    finally:
        engine.dispose()

    items = await run_readiness_audit(
        db_url=postgres_url, benchmark="fake-bench", registry_names={"fake-bench"},
    )

    assert len(items) == 1
    assert items[0].readiness_state == "blocked"
    assert items[0].blocker_reason == "benchmark_not_runnable"
    assert items[0].catalog_admission_state == "pending"
    assert items[0].qualification_state == "unknown"
    assert items[0].valid_task_config_count == 0
