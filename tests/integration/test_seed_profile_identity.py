"""Discovery aliases never become physical profile rows during bootstrap."""

from scripts.seed_test_data import _seed_benchmarks_from_entrypoints
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from loom.db.schema import Benchmark, BenchmarkAlias
from loom_benchmark_tool.audit_cmd import activate_tb21_alias, audit_tb21_profile
from tests.integration.test_tb21_publish_register_audit import (
    PROFILE,
    _persist_auditable_profile,
)
from tests.integration.test_tb21_publish_register_audit import (
    db as db,
)

PUBLIC_ALIAS = "terminal-bench-2"


def _seed_only_tb21(monkeypatch, postgres_url):
    from loom_benchmarks.registry import REGISTRY

    adapter = REGISTRY[PUBLIC_ALIAS]
    assert adapter.name == PROFILE
    monkeypatch.setattr("loom_benchmarks.registry.REGISTRY", {PUBLIC_ALIAS: adapter})
    engine = create_engine(postgres_url)
    try:
        with Session(engine) as session:
            assert _seed_benchmarks_from_entrypoints(session) == 1
            session.commit()
            assert _seed_benchmarks_from_entrypoints(session) == 0
            session.commit()
    finally:
        engine.dispose()


async def test_seed_uses_physical_identity_and_preserves_canonical_tb21_promotion(
    db, postgres_url, tmp_path, monkeypatch,
):
    _seed_only_tb21(monkeypatch, postgres_url)
    async with db.begin():
        assert await db.get(Benchmark, PUBLIC_ALIAS) is None
        placeholder = await db.get(Benchmark, PROFILE)
        assert placeholder is not None and placeholder.execution_state == "pending"
        assert placeholder.imported_by == "seed_test_data.py"
    store = await _persist_auditable_profile(db, tmp_path)
    async with db.begin():
        audit = await audit_tb21_profile(db, object_store=store)
    await activate_tb21_alias(db, object_store=store, audit_evidence=audit)
    async with db.begin():
        assert (await db.get(BenchmarkAlias, PUBLIC_ALIAS)).benchmark_id == PROFILE
        assert (await db.get(Benchmark, PROFILE)).execution_state == "runnable"
        assert await db.get(Benchmark, PUBLIC_ALIAS) is None


async def test_seed_does_not_silently_remove_an_old_alias_named_stub(db, postgres_url, monkeypatch):
    old = Benchmark(
        id=PUBLIC_ALIAS, display_name="Old discovery stub", upstream_kind="harbor-package",
        upstream_locator="fixture:old-stub", upstream_revision="", license_spdx="MIT",
        license_url="", splits=["test"], execution_state="pending", imported_by="seed_test_data.py",
    )
    db.add(old)
    await db.commit()
    try:
        _seed_only_tb21(monkeypatch, postgres_url)
        db.expire_all()
        async with db.begin():
            retained = await db.get(Benchmark, PUBLIC_ALIAS)
            assert retained is not None and retained.display_name == "Old discovery stub"
            assert retained.upstream_locator == "fixture:old-stub"
            assert retained.execution_state == "pending"
            assert await db.get(BenchmarkAlias, PUBLIC_ALIAS) is None
            assert (await db.get(Benchmark, PROFILE)).execution_state == "pending"
    finally:
        # Remove only this test's legacy stub from its disposable database.
        await db.execute(delete(Benchmark).where(Benchmark.id == PUBLIC_ALIAS))
        await db.commit()
