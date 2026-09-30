"""Opt-in local producer for journaled, versioned task sources.

Preparation owns short journal transactions; the final transaction alone owns
benchmark, task, source and image admission. No storage call holds catalog locks.
Existing local producers remain opt-in; native Harbor imports use this durable publisher.
"""

from __future__ import annotations

import shutil
import tempfile
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import tomli_w
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task
from loom.models.task import TaskConfig
from loom.nebius_terminus_ingest import (
    NEBIUS_TERMINUS_PROFILE,
    NebiusTerminusProfileStats,
    adapt_bundle_for_nebius_terminus,
    merge_profile_stats,
    preflight_nebius_terminus_admission,
)
from loom.task_bundle_catalog import publish_task_bundle_catalog
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    collect_task_dir_compatibility_issues,
    format_compatibility_issues,
)
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1, task_bundle_catalog_prefix
from loom.task_bundle_source_journal import TaskBundleUpload
from loom.task_bundle_source_publisher import TaskBundleSourcePublisher
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url
from loom_cli.benchmark_types import PreparedAdapterBenchmark
from loom_cli.local_benchmark_publish import (
    LocalBenchmarkPublishStats,
    _flatten_environment_subdir,
    _upsert_benchmark,
)
from loom_cli.local_benchmark_validate import (
    LocalBenchmarkValidationError,
    LocalBenchmarkValidationResult,
)


def _validate_staged_package_compatibility(
    bundle: Path, *, task_id: str, native_source: bool,
) -> None:
    issues = [
        issue for issue in collect_task_dir_compatibility_issues(bundle)
        if issue.severity == CompatibilitySeverity.ERROR
    ]
    if not issues:
        return
    if native_source:
        # Native preparation owns projection into canonical TaskConfig and
        # records original package defects. Legacy producers still own their
        # later normalization/adaptation; do not validate raw Harbor as Loom.
        task = TaskConfig.model_validate(tomllib.loads((bundle / "task.toml").read_text()))
        issues = [issue for issue in issues if issue not in task.import_blockers]
    if issues:
        raise LocalBenchmarkValidationError(
            f"task bundle compatibility preflight failed for {task_id}:\n"
            + format_compatibility_issues(issues),
        )


async def publish_versioned_local_benchmark(
    result: LocalBenchmarkValidationResult | PreparedAdapterBenchmark,
    *,
    db_url: str,
    object_store: ObjectStore,
    bucket: str,
    imported_by: str | None,
    compat_flatten_environment: bool,
    create_bucket: bool = False,
    execution_profile: str | None = None,
) -> LocalBenchmarkPublishStats:
    entry = result.entry
    adapter_tasks = {item["task_id"]: item for item in result.tasks.values()} if isinstance(result, PreparedAdapterBenchmark) else {}
    native_origin = (result.manifest.get("benchmark_profile_provenance", {}).get("upstream_origin")
                     if isinstance(result, PreparedAdapterBenchmark) else None)
    source_prefix = f"s3://{bucket}/{task_bundle_catalog_prefix(entry.id)}"
    engine = create_async_engine(normalize_db_url(db_url), isolation_level="READ COMMITTED")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    publisher = TaskBundleSourcePublisher(
        sessions, object_store, upload_lifetime=timedelta(hours=24),
    )
    prepared: dict[str, tuple[TaskBundleSourceSpecV1, TaskBundleUpload]] = {}
    inserted = updated = unchanged = uploaded_objects = compat_flattened_files = 0
    profile_stats = NebiusTerminusProfileStats()
    try:
        if create_bucket:
            await object_store.ensure_bucket(bucket)
        for task_toml in result.task_tomls:
            relative = task_toml.parent.relative_to(result.task_root)
            adapter_task = result.tasks[relative.as_posix()] if isinstance(result, PreparedAdapterBenchmark) else {}
            task_id = adapter_task.get("task_id") or (entry.id if relative == Path(".") else f"{entry.id}/{relative.as_posix()}")
            with tempfile.TemporaryDirectory(prefix="loom-source-stage-") as stage_root:
                staged = Path(stage_root) / "bundle"
                shutil.copytree(task_toml.parent, staged, symlinks=False)
                if compat_flatten_environment:
                    compat_flattened_files += len(_flatten_environment_subdir(staged))
                _validate_staged_package_compatibility(
                    staged, task_id=task_id, native_source=native_origin is not None,
                )
                adapt_stats = None
                if execution_profile == NEBIUS_TERMINUS_PROFILE:
                    authored = staged / "task.toml"
                    normalized = normalize_terminal_bench_task_toml(
                        tomllib.loads(authored.read_text()), task_id=task_id,
                    )
                    adapted, adapt_stats = adapt_bundle_for_nebius_terminus(staged, normalized)
                    # Versioned registration binds config to the staged authored
                    # bytes; capture the opt-in adaptation before registration.
                    adapted_document = TaskConfig.model_validate(adapted).model_dump(
                        mode="json", exclude_none=True,
                    )
                    authored.write_text(tomli_w.dumps(adapted_document))
                registration = prepare_task_bundle_registration(
                    staged, task_id=task_id,
                )
                spec = TaskBundleSourceSpecV1.from_registration(registration, bucket=bucket)
                if adapt_stats is not None:
                    reasons = preflight_nebius_terminus_admission(spec.task_config, spec.provenance)
                    if reasons:
                        raise LocalBenchmarkValidationError(
                            f"nebius-terminus admission preflight failed for {task_id}: "
                            + ", ".join(reasons),
                        )
                    profile_stats = merge_profile_stats(profile_stats, adapt_stats, preflight_ok=True)
                ticket = await publisher.prepare(spec, staged)
                prepared[task_id] = (spec, ticket)
                uploaded_objects += 0 if ticket.available else len(ticket.intents)

        # Serialize a whole benchmark before any task/image/source lock. Upload
        # failures above leave only recovery records, never a partial catalog.
        async with sessions.begin() as session:
            await _upsert_benchmark(
                session, entry=entry, source_prefix=source_prefix, imported_by=imported_by,
                adapter_manifest=result.manifest if isinstance(result, PreparedAdapterBenchmark) else None,
            )
            tasks = []
            for task_id, (spec, _) in sorted(prepared.items()):
                existing = await session.scalar(
                    select(Task).where(Task.id == task_id).with_for_update(),
                )
                values = dict(
                    checksum=spec.manifest.task_checksum,
                    config=spec.task_config,
                    source=spec.source_uri,
                    source_provenance={**adapter_tasks.get(task_id, {}).get("source_provenance", {}), **spec.provenance},
                    license=entry.license_spdx,
                    benchmark_id=entry.id,
                    **({"tags": adapter_tasks[task_id]["tags"]} if task_id in adapter_tasks else {}),
                )
                if existing is None:
                    inserted += 1
                elif any(getattr(existing, key) != value for key, value in values.items()):
                    updated += 1
                else:
                    unchanged += 1
                task = (
                    await session.execute(
                        pg_insert(Task).values(id=task_id, **values).on_conflict_do_update(
                            index_elements=["id"], set_=values,
                        ).returning(Task).execution_options(populate_existing=True),
                    )
                ).scalar_one()
                tasks.append(task)
            await publish_task_bundle_catalog(
                session, tasks=tasks,
                uploads={task_id: ticket for task_id, (_, ticket) in prepared.items()},
                now=datetime.now(UTC),
            )
    finally:
        await engine.dispose()
    return LocalBenchmarkPublishStats(
        benchmark_id=entry.id, task_count=result.task_count, inserted=inserted,
        updated=updated, unchanged=unchanged, uploaded_objects=uploaded_objects,
        compat_flattened_files=compat_flattened_files, bucket=bucket,
        source_prefix=source_prefix, execution_profile=execution_profile,
        profile_stats=profile_stats if execution_profile else None,
        warnings=result.warnings if isinstance(result, PreparedAdapterBenchmark) else (),
    )
