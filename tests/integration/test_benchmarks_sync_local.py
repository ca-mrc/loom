"""Integration test for `loom datasets sync-config` local-folder path
(issue #234).

Uses the session-scoped `postgres_url` fixture (Postgres 16 with the
current Alembic head applied) to UPSERT a sample TOML + 3 task.toml
bundles, then verifies the rows, idempotent re-run, and
checksum-update-on-mutation.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.config.benchmarks import load_benchmarks_config
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom_cli.benchmarks_sync import (
    LOCAL_FOLDER_KIND,
    SYNC_IMPORTED_BY,
    SyncError,
    TaskCounts,
    sync,
)

_TASK_TOML = """\
schema_version = "1"

[task]
id = "{tid}"
name = "Sample task {tid}"

[environment]
os = "linux"
docker_image = "python:3.11-alpine"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def _write_bundle(root: Path, tid: str) -> Path:
    d = root / tid
    d.mkdir(parents=True)
    (d / "task.toml").write_text(_TASK_TOML.format(tid=tid))
    (d / "instruction.md").write_text(f"do {tid}\n")
    return d


def _write_toml(path: Path, entry_id: str) -> None:
    path.write_text(
        f'''schema_version = 1

[[local]]
id = "{entry_id}"
display_name = "Test bundle"
series = "test"
license_spdx = "MIT"
'''
    )


def _write_toml_with_source_subdir(
    path: Path, entry_id: str, source_subdir: str,
) -> None:
    path.write_text(
        f'''schema_version = 1

[[local]]
id = "{entry_id}"
display_name = "Test bundle"
series = "test"
license_spdx = "MIT"
source_subdir = "{source_subdir}"
'''
    )


@pytest.fixture
def fixtures_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("fixtures")


@pytest.fixture
def benchmark_layout(fixtures_root: Path) -> tuple[Path, Path]:
    """Layout: <fixtures_root>/team-evals/{alpha,beta,nested/gamma}/task.toml"""
    bench_root = fixtures_root / "team-evals"
    bench_root.mkdir()
    _write_bundle(bench_root, "alpha")
    _write_bundle(bench_root, "beta")
    _write_bundle(bench_root / "nested", "gamma")
    return fixtures_root, bench_root


@pytest.fixture
def toml_path(tmp_path: Path) -> Path:
    p = tmp_path / "benchmarks.toml"
    _write_toml(p, "team-evals")
    return p


@pytest.mark.asyncio
async def test_local_sync_creates_rows(
    postgres_url: str,
    fixtures_root: Path,
    benchmark_layout: tuple[Path, Path],
    toml_path: Path,
) -> None:
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg,
                fixtures_root=fixtures_root,
                session=session,
                registry_names=set(),  # no entry-points to collide with
            )

        assert plan.tasks == {
            "team-evals": TaskCounts(inserted=3, updated=0, unchanged=0),
        }
        assert [(r.kind, r.id, r.action) for r in plan.rows] == [
            ("local", "team-evals", "INSERT"),
        ]

        async with factory() as session:
            result = await session.execute(
                select(Benchmark).where(Benchmark.id == "team-evals"),
            )
            bench = result.scalar_one()
            assert bench.display_name == "Test bundle"
            assert bench.series == "test"
            assert bench.license_spdx == "MIT"
            assert bench.upstream_kind == LOCAL_FOLDER_KIND
            assert bench.upstream_locator == str(fixtures_root / "team-evals")
            assert bench.upstream_revision == ""
            assert bench.splits == []
            assert bench.imported_by == SYNC_IMPORTED_BY

            tasks = (await session.execute(
                select(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )).scalars().all()
            ids = sorted(t.id for t in tasks)
            assert ids == [
                "team-evals/alpha",
                "team-evals/beta",
                "team-evals/nested/gamma",
            ]
            for t in tasks:
                assert t.source == f"fixture://{t.id}"
                assert t.license == "MIT"
                assert t.benchmark_id == "team-evals"
                # Authored files retain their local names; catalog configs
                # carry the qualified execution identity for shared admission.
                assert t.config["task"]["id"] == t.id
                assert len(t.checksum) == 64  # sha256 hex
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_source_subdir_keeps_public_task_ids(
    postgres_url: str, tmp_path: Path,
) -> None:
    fixtures_root = tmp_path / "fixtures"
    bench_root = fixtures_root / "team-evals"
    tasks_root = bench_root / "tasks"
    tasks_root.mkdir(parents=True)
    _write_bundle(tasks_root, "alpha")
    toml_path = tmp_path / "benchmarks.toml"
    _write_toml_with_source_subdir(toml_path, "team-evals", "tasks")
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg,
                fixtures_root=fixtures_root,
                session=session,
                registry_names=set(),
            )
        assert plan.tasks == {
            "team-evals": TaskCounts(inserted=1, updated=0, unchanged=0),
        }

        async with factory() as session:
            task = (await session.execute(
                select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
            )).scalar_one()
            assert task.source == "fixture://team-evals/tasks/alpha"
            assert task.benchmark_id == "team-evals"
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_idempotent(
    postgres_url: str,
    fixtures_root: Path,
    benchmark_layout: tuple[Path, Path],
    toml_path: Path,
) -> None:
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )

        async with factory() as session:
            plan_two = await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )
        # Second pass: benchmark row matches → SKIP unchanged. Tasks
        # match by checksum → all unchanged, zero writes.
        assert [r.action for r in plan_two.rows] == ["SKIP"]
        assert plan_two.rows[0].reason == "unchanged"
        assert plan_two.tasks == {
            "team-evals": TaskCounts(inserted=0, updated=0, unchanged=3),
        }

        async with factory() as session:
            count = (await session.execute(
                select(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )).scalars().all()
            assert len(count) == 3
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_updates_checksum_on_mutation(
    postgres_url: str,
    fixtures_root: Path,
    benchmark_layout: tuple[Path, Path],
    toml_path: Path,
) -> None:
    _, bench_root = benchmark_layout
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )

        async with factory() as session:
            initial = (await session.execute(
                select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
            )).scalar_one()
            initial_checksum = initial.checksum

        # Mutate alpha's instruction.md → bundle checksum should change.
        (bench_root / "alpha" / "instruction.md").write_text("do alpha v2\n")

        async with factory() as session:
            plan = await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )
        # Per-task SELECT-then-UPSERT: only alpha changed; siblings
        # stay unchanged with zero writes.
        assert plan.tasks == {
            "team-evals": TaskCounts(inserted=0, updated=1, unchanged=2),
        }

        async with factory() as session:
            updated = (await session.execute(
                select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
            )).scalar_one()
            assert updated.checksum != initial_checksum

            sibling = (await session.execute(
                select(TaskRow).where(TaskRow.id == "team-evals/beta"),
            )).scalar_one()
            # beta is untouched — checksum stable across runs.
            # (Sanity: this row was inserted by the first sync; second
            # sync UPSERTs it with the same checksum.)
            assert len(sibling.checksum) == 64
    finally:
        async with factory() as session:
            await _cleanup(session)
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_skips_missing_source_dir(
    postgres_url: str, tmp_path: Path,
) -> None:
    fixtures_root = tmp_path / "no-such-dir"
    fixtures_root.mkdir()
    # benchmarks.toml registers "ghost" but <fixtures_root>/ghost/ doesn't exist
    toml_path = tmp_path / "benchmarks.toml"
    _write_toml(toml_path, "ghost")
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )
        assert [r.action for r in plan.rows] == ["SKIP"]
        assert "missing" in plan.rows[0].reason

        async with factory() as session:
            result = await session.execute(
                select(Benchmark).where(Benchmark.id == "ghost"),
            )
            assert result.scalar_one_or_none() is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_skips_empty_source_dir(
    postgres_url: str, tmp_path: Path,
) -> None:
    """Spec: missing/empty source dir → WARN+SKIP without zombie row."""
    fixtures_root = tmp_path / "fixtures"
    (fixtures_root / "ghost").mkdir(parents=True)  # exists but empty
    toml_path = tmp_path / "benchmarks.toml"
    _write_toml(toml_path, "ghost")
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
            )
        assert [r.action for r in plan.rows] == ["SKIP"]
        assert "empty" in plan.rows[0].reason

        # Crucial: no zombie row for an empty registered benchmark.
        async with factory() as session:
            result = await session.execute(
                select(Benchmark).where(Benchmark.id == "ghost"),
            )
            assert result.scalar_one_or_none() is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_aborts_on_invalid_task_toml(
    postgres_url: str, tmp_path: Path,
) -> None:
    """A malformed task.toml under a [[local]] entry aborts with SyncError."""
    fixtures_root = tmp_path / "fixtures"
    bench_root = fixtures_root / "bad"
    bench_root.mkdir(parents=True)
    bundle = bench_root / "broken"
    bundle.mkdir()
    (bundle / "task.toml").write_text("this is = NOT = valid TOML\n")
    toml_path = tmp_path / "benchmarks.toml"
    _write_toml(toml_path, "bad")
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            with pytest.raises(SyncError) as exc:
                await sync(
                    cfg, fixtures_root=fixtures_root,
                    session=session, registry_names=set(),
                )
        assert "invalid task.toml" in str(exc.value)
        assert "broken" in str(exc.value)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_sync_dry_run_writes_no_rows(
    postgres_url: str,
    fixtures_root: Path,
    benchmark_layout: tuple[Path, Path],
    toml_path: Path,
) -> None:
    """`--dry-run` parses + computes the plan but writes nothing."""
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            plan = await sync(
                cfg, fixtures_root=fixtures_root,
                session=session, registry_names=set(),
                dry_run=True,
            )

        assert plan.tasks == {
            "team-evals": TaskCounts(inserted=3, updated=0, unchanged=0),
        }
        assert [r.action for r in plan.rows] == ["INSERT"]

        async with factory() as session:
            bench = (await session.execute(
                select(Benchmark).where(Benchmark.id == "team-evals"),
            )).scalar_one_or_none()
            assert bench is None
            tasks = (await session.execute(
                select(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )).scalars().all()
            assert tasks == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_preflight_collision_with_registry(
    postgres_url: str,
    fixtures_root: Path,
    benchmark_layout: tuple[Path, Path],
    toml_path: Path,
) -> None:
    cfg = load_benchmarks_config(toml_path)
    assert cfg is not None
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            with pytest.raises(SyncError) as exc:
                await sync(
                    cfg, fixtures_root=fixtures_root,
                    session=session,
                    # Simulate REGISTRY shadowing
                    registry_names={"team-evals"},
                )
        assert "collides" in str(exc.value)
    finally:
        await engine.dispose()


async def _cleanup(session) -> None:  # type: ignore[no-untyped-def]
    """Best-effort wipe of test rows so re-runs don't accumulate."""
    from sqlalchemy import delete
    await session.execute(
        delete(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
    )
    await session.execute(
        delete(Benchmark).where(Benchmark.id == "team-evals"),
    )
    await session.commit()
