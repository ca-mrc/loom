"""Publish validated local benchmark folders to object storage.

This is the production-oriented companion to ``validate-local`` and the
dev-only ``sync-config`` path: validate the same folder contract, upload each
task bundle under a checksum-addressed ``s3://`` prefix the worker already
knows how to materialize, and upsert benchmark/task rows into the service
database without overwriting bytes referenced by an existing row.
"""

from __future__ import annotations

import shutil
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import ColumnElement, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.config.benchmarks import LocalBenchmarkEntry
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task_checksum import task_checksum
from loom.nebius_terminus_ingest import (
    NEBIUS_TERMINUS_PROFILE,
    NebiusTerminusProfileStats,
    adapt_bundle_for_nebius_terminus,
    merge_profile_stats,
    preflight_nebius_terminus_admission,
    resolve_execution_profile,
)
from loom.service_execution_materialization import (
    prepare_service_execution_input_manifest,
)
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    collect_task_dir_compatibility_issues,
    format_compatibility_issues,
)
from loom.task_image_materialization import ensure_task_image_materializations
from loom.terminal_bench_normalize import (
    normalize_terminal_bench_task_toml,
)
from loom.trajectory.storage import (
    ObjectStore,
    bundle_file_metadata_sha256,
    discard_staged_bundle_file_metadata,
)
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.upload import upload_task_dir
from loom_cli.benchmark_types import AdapterBenchmarkEntry, PreparedAdapterBenchmark
from loom_cli.local_benchmark_validate import (
    LocalBenchmarkValidationError,
    validate_local_benchmark,
)

PUBLISH_IMPORTED_BY = "local-benchmark-publish"
S3_FOLDER_KIND = "s3-folder"


@dataclass(frozen=True)
class LocalBenchmarkPublishStats:
    benchmark_id: str
    task_count: int
    inserted: int
    updated: int
    unchanged: int
    uploaded_objects: int
    compat_flattened_files: int
    bucket: str
    source_prefix: str
    execution_profile: str | None = None
    profile_stats: NebiusTerminusProfileStats | None = None
    warnings: tuple[str, ...] = ()


async def publish_local_benchmark(
    root: Path,
    *,
    db_url: str,
    object_store: ObjectStore,
    bucket: str,
    benchmark_id: str | None = None,
    display_name: str | None = None,
    series: str | None = None,
    license_spdx: str | None = None,
    source_subdir: str | None = None,
    imported_by: str | None = None,
    compat_flatten_environment: bool = False,
    source_registration_mode: str = "legacy",
    create_bucket: bool = False,
    execution_profile: str | None = None,
    prepared_adapter: PreparedAdapterBenchmark | None = None,
) -> LocalBenchmarkPublishStats:
    """Publish to an existing bucket; bucket creation is an explicit bootstrap option."""

    if source_registration_mode not in {"legacy", "versioned-v1"}:
        raise ValueError("unsupported task source registration mode")
    try:
        profile = resolve_execution_profile(execution_profile)
    except ValueError as exc:
        raise LocalBenchmarkValidationError(str(exc), exit_code=2) from exc

    result = prepared_adapter or validate_local_benchmark(
        root,
        benchmark_id=benchmark_id,
        display_name=display_name,
        series=series,
        license_spdx=license_spdx,
        source_subdir=source_subdir,
    )
    native_origin = (result.manifest.get("benchmark_profile_provenance", {}).get("upstream_origin")
                     if isinstance(result, PreparedAdapterBenchmark) else None)
    if source_registration_mode == "versioned-v1" or native_origin is not None:
        from loom_cli.local_benchmark_source_publish import publish_versioned_local_benchmark

        return await publish_versioned_local_benchmark(
            result,
            db_url=db_url,
            object_store=object_store,
            bucket=bucket,
            imported_by=imported_by,
            compat_flatten_environment=compat_flatten_environment,
            create_bucket=create_bucket,
            execution_profile=profile,
        )
    entry = result.entry
    if create_bucket:
        await object_store.ensure_bucket(bucket)

    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    inserted = updated = unchanged = uploaded_objects = 0
    compat_flattened_files = 0
    profile_stats = NebiusTerminusProfileStats()
    source_prefix = f"s3://{bucket}/{entry.id}/"
    try:
        async with session_factory() as session:
            prepared_rows = []
            for task_toml in result.task_tomls:
                bundle_dir = task_toml.parent
                rel = bundle_dir.relative_to(result.task_root)
                adapter_task = (
                    prepared_adapter.tasks[rel.as_posix()] if prepared_adapter else {}
                )
                task_id = adapter_task.get("task_id") or (
                    entry.id if rel == Path(".") else f"{entry.id}/{rel.as_posix()}"
                )
                publication_rel = Path(task_id.removeprefix(entry.id + "/")) if prepared_adapter else rel

                # #369: bundles whose files live under `environment/` but
                # whose Dockerfile does `COPY . /app/` and references
                # `/app/<file>` need the tree flattened so the files land
                # at `/app/<file>` and not `/app/environment/<file>`. We
                # stage each bundle into a scratch dir, flatten there,
                # and upload from the scratch dir. The operator's
                # on-disk bundle stays untouched.
                with tempfile.TemporaryDirectory(
                    prefix="loom-publish-stage-",
                ) as stage_root:
                    staged = Path(stage_root) / "bundle"
                    shutil.copytree(bundle_dir, staged, symlinks=False)
                    if compat_flatten_environment:
                        compat_flattened_files += len(
                            _flatten_environment_subdir(staged),
                        )

                    compatibility_issues = [
                        issue
                        for issue in collect_task_dir_compatibility_issues(staged)
                        if issue.severity == CompatibilitySeverity.ERROR
                    ]
                    if compatibility_issues:
                        raise LocalBenchmarkValidationError(
                            "task bundle compatibility preflight failed for "
                            f"{task_id}:\n" + format_compatibility_issues(compatibility_issues),
                        )

                    with (staged / task_toml.name).open("rb") as f:
                        raw_cfg: dict[str, Any] = tomllib.load(f)
                    # #341: normalize Terminal-Bench-shaped task.toml
                    # to a Loom TaskConfig dict before persisting. The
                    # S3-uploaded bundle preserves the operator's
                    # original files (plus the flattened compat copies
                    # from #369); the DB row's `config` JSONB carries
                    # the Loom-schema form so the worker validates.
                    raw_cfg = normalize_terminal_bench_task_toml(raw_cfg, task_id=task_id)
                    if profile == NEBIUS_TERMINUS_PROFILE:
                        # #1996: adapt staged verifier + Loom config after
                        # normalize so TB absolute verifier paths are corrected
                        # before checksum / SEI / admission preflight.
                        raw_cfg, adapt_stats = adapt_bundle_for_nebius_terminus(
                            staged,
                            raw_cfg,
                        )
                    else:
                        adapt_stats = None
                    discard_staged_bundle_file_metadata(staged)
                    checksum = task_checksum(staged)
                    metadata_digest = bundle_file_metadata_sha256(staged).removeprefix(
                        "sha256:",
                    )
                    revision_prefix = _task_revision_prefix(
                        entry.id,
                        publication_rel,
                        checksum,
                        metadata_digest,
                    )
                    prefix = f"{revision_prefix}bundle/"
                    source = f"s3://{bucket}/{prefix}"
                    uploaded_objects += await upload_task_dir(
                        store=object_store,
                        bucket=bucket,
                        prefix=prefix,
                        task_dir=staged,
                    )
                    # Publication metadata must stay outside the bundle prefix:
                    # both ordinary materialization and the native builder require
                    # its objects to match the task's original files exactly.
                    manifest_key = f"{revision_prefix}service-execution-input.json"
                    manifest_body, sei_provenance = prepare_service_execution_input_manifest(
                        staged,
                        task_checksum=checksum,
                        bucket=bucket,
                        manifest_key=manifest_key,
                    )
                    await object_store.put_object(
                        bucket=bucket,
                        key=manifest_key,
                        body=manifest_body,
                    )
                    uploaded_objects += 1
                    upstream_provenance = {}
                    if raw_cfg.get("upstream_origin") is not None:
                        upstream_provenance = {
                            "upstream_origin": raw_cfg["upstream_origin"],
                            "upstream_task_id": raw_cfg.get("upstream_task_id"),
                        }
                    source_provenance = {
                        **upstream_provenance,
                        **adapter_task.get("source_provenance", {}),
                        "bundle_file_metadata_sha256": f"sha256:{metadata_digest}",
                        **sei_provenance,
                    }
                    if adapt_stats is not None:
                        reasons = preflight_nebius_terminus_admission(
                            raw_cfg,
                            source_provenance,
                        )
                        if reasons:
                            raise LocalBenchmarkValidationError(
                                "nebius-terminus admission preflight failed for "
                                f"{task_id}: {', '.join(reasons)}",
                            )
                        profile_stats = merge_profile_stats(
                            profile_stats,
                            adapt_stats,
                            preflight_ok=True,
                        )
                prepared_rows.append({
                    "id": task_id, "checksum": checksum, "config": raw_cfg,
                    "source": source, "source_provenance": source_provenance,
                    "license": adapter_task.get("license_spdx", entry.license_spdx),
                    "benchmark_id": entry.id,
                    **({"tags": adapter_task["tags"]} if prepared_adapter else {}),
                })

            # Register only after every upload/preflight succeeded. Reuse the
            # historical physical-profile lock rather than bypassing activation.
            exact_existing = False
            if entry.id == "terminal-bench-2@tb2.1-r6":
                from loom_benchmark_tool.register_cmd import (
                    _locked_tb21_registration_preflight,
                    _PreparedTask,
                )
                assert prepared_adapter is not None
                exact_existing = await _locked_tb21_registration_preflight(
                    session, manifest=prepared_adapter.manifest,
                    prepared_tasks=[_PreparedTask(
                        task_id=row["id"], checksum=row["checksum"], config=row["config"],
                        source=row["source"], license_spdx=row["license"], benchmark_id=entry.id,
                        tags=row["tags"], source_provenance=row["source_provenance"],
                    ) for row in prepared_rows],
                )
            if exact_existing:
                unchanged = len(prepared_rows)
            else:
                await _upsert_benchmark(
                    session, entry=entry, source_prefix=source_prefix, imported_by=imported_by,
                    adapter_manifest=prepared_adapter.manifest if prepared_adapter else None,
                )
                for values in prepared_rows:
                    existing = await _get_task(session, values["id"])
                    if existing is None:
                        inserted += 1
                    elif any(getattr(existing, key) != value for key, value in values.items()):
                        updated += 1
                    else:
                        unchanged += 1
                    task_row = (await session.execute(
                        pg_insert(TaskRow).values(**values).on_conflict_do_update(
                            index_elements=["id"],
                            set_={key: value for key, value in values.items() if key != "id"},
                        ).returning(TaskRow).execution_options(populate_existing=True),
                    )).scalar_one()
                    await ensure_task_image_materializations(session, task_row=task_row)

            await session.commit()
    finally:
        await engine.dispose()

    return LocalBenchmarkPublishStats(
        benchmark_id=entry.id,
        task_count=result.task_count,
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        uploaded_objects=uploaded_objects,
        compat_flattened_files=compat_flattened_files,
        bucket=bucket,
        source_prefix=source_prefix,
        execution_profile=profile,
        profile_stats=profile_stats if profile is not None else None,
        warnings=prepared_adapter.warnings if prepared_adapter else (),
    )


async def _upsert_benchmark(
    session: AsyncSession,
    *,
    entry: LocalBenchmarkEntry | AdapterBenchmarkEntry,
    source_prefix: str,
    imported_by: str | None,
    adapter_manifest: dict[str, Any] | None = None,
) -> None:
    origin = None
    values: dict[str, Any] = dict(
        display_name=entry.display_name,
        upstream_kind=S3_FOLDER_KIND,
        upstream_locator=source_prefix,
        upstream_revision="",
        license_spdx=entry.license_spdx,
        license_url="",
        series=entry.series,
        splits=[],
        imported_by=imported_by or PUBLISH_IMPORTED_BY,
    )
    if adapter_manifest is not None:
        values.update({key: adapter_manifest.get(key, "") for key in (
            "upstream_kind", "upstream_locator", "upstream_revision", "license_url",
        )})
        values["splits"] = adapter_manifest["splits"]
        values["profile_provenance"] = adapter_manifest["benchmark_profile_provenance"]
        origin = values["profile_provenance"].get("upstream_origin")
        if origin is not None:
            # Import and conversion never establish executable or full-profile qualification.
            values["execution_state"] = "pending"
        if entry.id == "terminal-bench-2@tb2.1-r6":
            values["execution_state"] = "pending"
    statement = pg_insert(Benchmark).values(id=entry.id, **values)
    # PostgreSQL evaluates this predicate against the conflicting row under its
    # own lock, including concurrent first imports. A prior SELECT cannot bind
    # an identity that does not exist yet.
    predicate: ColumnElement[bool] = Benchmark.profile_provenance["upstream_origin"].is_(None)
    if origin is not None:
        predicate = Benchmark.profile_provenance["upstream_origin"] == statement.excluded.profile_provenance["upstream_origin"]
    result = await session.execute(statement.on_conflict_do_update(
        index_elements=["id"], set_=values, where=predicate,
    ).returning(Benchmark.id))
    if result.scalar_one_or_none() is None:
        raise ValueError("benchmark id is already bound to another upstream origin; use a distinct version/subset id")


def _task_prefix(benchmark_id: str, rel: Path) -> str:
    if rel == Path("."):
        return f"{benchmark_id}/"
    return f"{benchmark_id}/{rel.as_posix().strip('/')}/"


def _task_revision_prefix(
    benchmark_id: str,
    rel: Path,
    checksum: str,
    metadata_digest: str,
) -> str:
    # v1 mixed the input manifest with task files. A new namespace also repairs
    # unchanged republishes without deleting or changing previously frozen inputs.
    return f"{_task_prefix(benchmark_id, rel)}.loom-revisions-v2/{checksum}/{metadata_digest}/"


async def _get_task(session, task_id: str):  # type: ignore[no-untyped-def]
    return (
        await session.execute(
            select(TaskRow).where(TaskRow.id == task_id),
        )
    ).scalar_one_or_none()


def _flatten_environment_subdir(bundle_dir: Path) -> list[str]:
    """Copy every file under ``bundle_dir/environment/`` up to
    ``bundle_dir/`` in place so a Dockerfile that does ``COPY . /app/``
    and expects ``/app/<file>`` finds the file even when the bundle
    stores it as ``environment/<file>``.

    Existing top-level files are preserved (name collisions skip the
    copy — the operator's intent at the root wins). The
    ``environment/`` tree itself is preserved after flattening so
    Dockerfiles that DO expect ``environment/<file>`` at the build
    context root still find it there. Returns the list of relative
    paths that were flattened (empty when no ``environment/`` subdir
    exists). #369.
    """
    env_dir = bundle_dir / "environment"
    if not env_dir.is_dir():
        return []
    flattened: list[str] = []
    for src in sorted(env_dir.rglob("*")):
        if not src.is_file() or src.is_symlink():
            continue
        rel = src.relative_to(env_dir)
        dst = bundle_dir / rel
        if dst.exists():
            continue  # top-level name wins on collision
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        flattened.append(rel.as_posix())
    return flattened
