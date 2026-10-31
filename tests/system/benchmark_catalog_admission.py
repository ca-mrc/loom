"""Real catalog admission for synthetic system-test adapter materializations.

Fixture provenance is explicit and separate from upstream benchmark acceptance.
No fixture guesses a remote commit or grants lifecycle state by direct SQL.
"""

from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_admission import admit_benchmark_profile
from loom.benchmark_admission_legacy_sources import prepare_legacy_source_validation
from loom.catalog_policy import catalog_support_policy
from loom.db.schema import Benchmark
from loom.models.benchmark_profile import BenchmarkProfileSpec
from loom_benchmark_tool.db_url import normalize_db_url


def pin_fixture_adapter(monkeypatch, benchmark: str, *, retain_policy_identity: bool = False) -> str:
    """Pin a reviewed adapter to synthetic fixture input before it is imported."""
    from loom_benchmarks.base import UpstreamSource

    from loom_benchmark_tool import import_cmd

    original = import_cmd._resolve_adapter
    adapter = deepcopy(original(benchmark))
    if retain_policy_identity:
        assert catalog_support_policy(benchmark).state != "supported"
    else:
        assert catalog_support_policy(benchmark).state == "supported"
        adapter.name = f"system-fixture-{benchmark}-{uuid4().hex[:10]}"
    adapter.upstream_source = UpstreamSource(
        kind="https-tarball", locator=f"fixture://loom-system/{benchmark}", revision="fixture-v1",
    )

    def resolve(requested, **kwargs):
        return adapter if requested == benchmark else original(requested, **kwargs)

    monkeypatch.setattr(import_cmd, "_resolve_adapter", resolve)
    return adapter.name


async def admit_fixture_catalog(*, db_url: str, profile_id: str, object_store) -> None:
    """Use the production source validator and common owner, without a Trial."""
    engine = create_async_engine(normalize_db_url(db_url))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            async with session.begin():
                benchmark = await session.get(Benchmark, profile_id)
                assert benchmark is not None
                provenance = benchmark.profile_provenance
                selected = tuple(provenance["selected_task_ids"])
                selection = provenance["selection"]
                spec = BenchmarkProfileSpec(
                    id=profile_id,
                    origin={
                        "kind": benchmark.upstream_kind, "locator": benchmark.upstream_locator,
                        "revision": benchmark.upstream_revision, "release": "fixture-v1",
                        "importer": "loom.system-fixture.v1",
                        "subset": selected if selection == "subset" else (),
                    },
                    expected_task_count=provenance["source_task_count"],
                    selected_task_ids=selected, selection=selection,
                )
                validation = await prepare_legacy_source_validation(
                    session, spec=spec, object_store=object_store,
                )
            async with session.begin():
                await admit_benchmark_profile(session, spec=spec, validation=validation)
    finally:
        await engine.dispose()
