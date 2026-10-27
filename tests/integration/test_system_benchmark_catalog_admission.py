"""System fixture setup uses actual source admission without running a model."""

from pathlib import Path

import pytest
from fastapi import HTTPException
from loom_benchmarks.base import BenchmarkInstance
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_profiles import reject_non_runnable_benchmark_profiles
from loom.catalog_policy import catalog_support_policy
from loom.db.schema import Benchmark
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import
from tests.system.benchmark_catalog_admission import admit_fixture_catalog, pin_fixture_adapter
from tests.system.test_benchmark_all_smoke import CASES, _load_adapter_class, _resolve_rows


@pytest.mark.parametrize("benchmark", ["humaneval", "swe-bench"])
async def test_system_fixture_admission_keeps_reviewed_product_policy(
    postgres_url, tmp_path, monkeypatch, benchmark,
):
    case = next(case for case in CASES if case.benchmark_id == benchmark)
    adapter_class = _load_adapter_class(case)
    rows = _resolve_rows(case, tmp_path)

    def instances(self, *, source_dir: Path, split: str):
        if split != case.target_split:
            return
        for identity, raw in rows:
            yield BenchmarkInstance(instance_id=identity, split=split, raw=raw)

    monkeypatch.setattr(adapter_class, "list_instances", instances)
    monkeypatch.setattr("loom_benchmark_tool.import_cmd.fetch_upstream", lambda *args, **kwargs: tmp_path)
    policy = catalog_support_policy(benchmark)
    profile = pin_fixture_adapter(monkeypatch, benchmark, retain_policy_identity=policy.state != "supported")
    store = FakeObjectStore()
    await run_import(benchmark=benchmark, db_url=postgres_url, object_store=store,
                     bucket="fixtures", cache_dir=tmp_path / "cache", instance_ids=case.instance_ids)
    await admit_fixture_catalog(db_url=postgres_url, profile_id=profile, object_store=store)
    engine = create_async_engine(postgres_url)
    try:
        async with async_sessionmaker(engine)() as session:
            record = await session.get(Benchmark, profile)
            assert record.execution_state == "runnable"
            assert record.upstream_kind == "https-tarball"
            assert record.upstream_locator == f"fixture://loom-system/{benchmark}"
            assert record.upstream_revision == "fixture-v1"
            if policy.state != "supported":
                assert profile == benchmark
                with pytest.raises(HTTPException) as rejected:
                    await reject_non_runnable_benchmark_profiles(session, [profile])
                assert rejected.value.detail["reason"] == policy.state
            else:
                assert profile.startswith("system-fixture-humaneval-")
                await reject_non_runnable_benchmark_profiles(session, [profile])
    finally:
        await engine.dispose()


def test_system_tb21_subset_fixture_cannot_impersonate_locked_materialization():
    from loom_benchmark_terminal_bench_2.upstream import TB21LockError

    case = next(case for case in CASES if case.locked_materialization_rejection is not None)
    with pytest.raises(TB21LockError, match=case.locked_materialization_rejection):
        list(_load_adapter_class(case)().list_instances(source_dir=case.override_source_dir,
                                                       split=case.target_split))
