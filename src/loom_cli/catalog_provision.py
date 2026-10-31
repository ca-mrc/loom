"""Staging benchmark and agent catalog provisioning.

Copies the production-ready benchmark/task catalog from a known-good source
environment into the staging target and reconciles the S3 task bundles the
worker needs at runtime. It also materializes the service-mode agent catalog
into the target `agents` table as an auditable restore snapshot. This is
intentionally separate from seed_test_data: it does not create teams, invites,
tokens, runs, or fixture artifacts.
"""

from __future__ import annotations

import asyncio
import io
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar, cast

from botocore.exceptions import ClientError
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_publication import execution_profile_provenance, prepare_profile_publication
from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    TaskAuditSource,
    build_readiness_item,
)
from loom.db.schema import Agent as AgentModel
from loom.db.schema import (
    Benchmark,
    TaskBundleSource,
    TaskBundleSourceIncarnation,
    TaskBundleSourceReference,
    TaskBundleSourceVersion,
    TaskBundleSourceWrite,
)
from loom.db.schema import Task as TaskModel
from loom.models.task import TaskConfig
from loom.task_bundle_catalog import publish_task_bundle_catalog
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import TaskBundleUpload
from loom.task_bundle_source_publisher import SourceObjectReader, TaskBundleSourcePublisher
from loom.task_image_materialization import ensure_task_image_materializations
from loom.trajectory.immutable_object import ImmutableObjectVersion, read_verified_object_version
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url
from loom_service.agent_catalog import AgentEntry, list_agents

POSTGRES_CATALOG_UPSERT_BATCH_SIZE = 1000
AGENT_CATALOG_VERSION = "service-catalog-v2"
AGENT_CATALOG_SPEC_SCHEMA_VERSION = 2
_WORKSPACE_EXEC_BENCHMARK_STEMS = (
    "skillflow",
    "skilllearnbench",
    "swe-bench",
)
T = TypeVar("T")


def _batched(items: list[T], size: int) -> Iterable[list[T]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for start in range(0, len(items), size):
        yield items[start : start + size]


@dataclass(frozen=True)
class AgentRow:
    name: str
    version: str
    mode: str
    spec: dict[str, Any]


@dataclass(frozen=True)
class BenchmarkRow:
    id: str
    display_name: str
    upstream_kind: str
    upstream_locator: str
    upstream_revision: str
    license_spdx: str
    license_url: str
    splits: list[str]
    series: str | None
    imported_by: str | None
    execution_state: str = "pending"
    profile_provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskRow:
    id: str
    checksum: str
    config: dict[str, Any]
    source: str | None
    license: str | None
    benchmark_id: str | None
    tags: dict[str, str]
    source_provenance: dict[str, Any] = field(default_factory=dict)
    registered_source_spec: dict[str, Any] | None = None
    registered_source_versions: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogRows:
    benchmarks: list[BenchmarkRow]
    tasks: list[TaskRow]
    agents: list[AgentRow] = field(default_factory=list)


@dataclass(frozen=True)
class ObjectInfo:
    bucket: str
    key: str
    size: int
    etag: str | None = None


@dataclass(frozen=True)
class ProvisionStats:
    ready_agents: int
    ready_benchmarks: int
    ready_tasks: int
    source_objects: int
    target_objects_uploaded: int
    target_objects_skipped: int
    target_objects_missing: int
    bytes_uploaded: int
    bytes_skipped: int


class CatalogStore(Protocol):
    async def load_rows(self) -> CatalogRows: ...

    async def upsert_rows(self, rows: CatalogRows) -> None: ...


class CatalogObjectStore(Protocol):
    async def ensure_bucket(self, bucket: str) -> None: ...

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]: ...

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None: ...

    async def get_object(self, *, bucket: str, key: str) -> bytes: ...

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None: ...


def _immutable_profile_provenance(value: dict[str, Any]) -> dict[str, Any]:
    return execution_profile_provenance(value)


def _profile_identity(row: BenchmarkRow | Benchmark) -> dict[str, Any]:
    return {
        "upstream_kind": row.upstream_kind, "upstream_locator": row.upstream_locator,
        "upstream_revision": row.upstream_revision,
        "profile_provenance": _immutable_profile_provenance(dict(row.profile_provenance or {})),
    }


def _task_identity(row: TaskRow | TaskModel) -> dict[str, Any]:
    return {key: getattr(row, key) for key in (
        "checksum", "config", "source", "benchmark_id", "source_provenance",
    )}


def _assert_catalog_rows_absent_or_exact(*, existing: CatalogRows, incoming: CatalogRows) -> None:
    """Reject any physical-profile drift before copying destination bytes."""
    existing_by_id = {row.id: row for row in existing.benchmarks}
    existing_tasks = {row.id: row for row in existing.tasks}
    for profile in incoming.benchmarks:
        old = existing_by_id.get(profile.id)
        new_tasks = {row.id: row for row in incoming.tasks if row.benchmark_id == profile.id}
        if old is None:
            if set(new_tasks) & set(existing_tasks):
                raise ValueError("immutable catalog task identity collision during provisioning")
            continue
        if old.execution_state == "historical":
            raise ValueError("historical profiles cannot be restored; use a new physical profile")
        old_tasks = {key: row for key, row in existing_tasks.items() if row.benchmark_id == profile.id}
        if (_profile_identity(old) != _profile_identity(profile)
            or set(old_tasks) != set(new_tasks)
            or any(_task_identity(old_tasks[key]) != _task_identity(row) for key, row in new_tasks.items())):
            raise ValueError("immutable catalog profile drift during provisioning; use a new physical profile")


def _fail_closed_restore_rows(*, existing: CatalogRows, incoming: CatalogRows) -> CatalogRows | None:
    """Disable affected destination selectors before any storage changes."""
    incoming_by_id = {row.id: row for row in incoming.benchmarks}
    affected = {row.id for row in existing.benchmarks if row.id in incoming_by_id}
    if not affected:
        return None
    return CatalogRows(
        benchmarks=[incoming_by_id[key] for key in sorted(affected)],
        tasks=[row for row in existing.tasks if row.benchmark_id in affected],
    )


class PostgresCatalogStore:
    def __init__(self, db_url: str) -> None:
        self.db_url = normalize_db_url(db_url)

    async def load_rows(self) -> CatalogRows:
        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                agent_rows = list((await session.scalars(select(AgentModel))).all())
                benchmark_rows = list((await session.scalars(select(Benchmark))).all())
                task_rows = list(
                    (
                        await session.scalars(
                            select(TaskModel).where(TaskModel.benchmark_id.is_not(None)),
                        )
                    ).all(),
                )
                # Logical immutable source facts are portable; available-store
                # incarnations and their physical versions are deliberately not.
                source_specs = {
                    (row.source_uri, owner): dict(row.spec_json)
                    for row, owner in (await session.execute(
                        select(TaskBundleSource, TaskBundleSourceReference.owner_id)
                        .join(TaskBundleSourceIncarnation)
                        .join(TaskBundleSourceReference, TaskBundleSourceReference.source_id == TaskBundleSource.id)
                        .where(TaskBundleSourceIncarnation.state == "available")
                        .where(TaskBundleSourceReference.kind == "catalog")
                    )).all()
                }
                source_versions: dict[tuple[str, str], dict[str, str]] = {}
                receipts = (await session.execute(
                    select(TaskBundleSource.source_uri, TaskBundleSourceReference.owner_id,
                           TaskBundleSourceWrite.object_key, TaskBundleSourceVersion.version_id)
                    .join(TaskBundleSourceIncarnation, TaskBundleSourceIncarnation.source_id == TaskBundleSource.id)
                    .join(TaskBundleSourceWrite, TaskBundleSourceWrite.incarnation_id == TaskBundleSourceIncarnation.id)
                    .join(TaskBundleSourceVersion, TaskBundleSourceVersion.write_id == TaskBundleSourceWrite.id)
                    .join(TaskBundleSourceReference, TaskBundleSourceReference.source_id == TaskBundleSource.id)
                    .where(TaskBundleSourceIncarnation.state == "available",
                           TaskBundleSourceVersion.state == "available",
                           TaskBundleSourceReference.kind == "catalog")
                    .order_by(TaskBundleSourceVersion.observed_at.desc(), TaskBundleSourceVersion.version_id)
                )).all()
                for uri, owner, key, version_id in receipts:
                    source_versions.setdefault((uri, owner), {}).setdefault(key, version_id)

        finally:
            await engine.dispose()

        return CatalogRows(
            agents=[
                AgentRow(
                    name=row.name,
                    version=row.version,
                    mode=row.mode,
                    spec=dict(row.spec),
                )
                for row in agent_rows
            ],
            benchmarks=[
                BenchmarkRow(
                    id=row.id,
                    display_name=row.display_name,
                    upstream_kind=row.upstream_kind,
                    upstream_locator=row.upstream_locator,
                    upstream_revision=row.upstream_revision,
                    license_spdx=row.license_spdx,
                    license_url=row.license_url,
                    splits=list(row.splits),
                    series=row.series,
                    imported_by=row.imported_by,
                    execution_state=row.execution_state,
                    profile_provenance=dict(row.profile_provenance or {}),
                )
                for row in benchmark_rows
            ],
            tasks=[
                TaskRow(
                    id=row.id,
                    checksum=row.checksum,
                    config=dict(row.config),
                    source=row.source,
                    license=row.license,
                    benchmark_id=row.benchmark_id,
                    tags=dict(row.tags or {}),
                    source_provenance=dict(row.source_provenance or {}),
                    registered_source_spec=source_specs.get((row.source, row.id)) if row.source else None,
                    registered_source_versions=source_versions.get((row.source, row.id), {}) if row.source else {},
                )
                for row in task_rows
            ],
        )

    async def upsert_rows(
        self, rows: CatalogRows, *, uploads: dict[str, TaskBundleUpload] | None = None,
        preserve_profiles: frozenset[str] = frozenset(),
    ) -> None:
        if not rows.agents and not rows.benchmarks and not rows.tasks:
            return

        engine = create_async_engine(self.db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                from loom.benchmark_admission import invalidate_benchmark_profile

                states: dict[str, str] = {}
                provenances: dict[str, dict[str, Any]] = {}
                # Destination registration and admission are local authorities.
                # Never inherit a lifecycle/qualification claim from the source.
                for profile in sorted(rows.benchmarks, key=lambda row: row.id):
                    local_state = await prepare_profile_publication(
                        session, profile_id=profile.id, profile_identity=_profile_identity(profile),
                        tasks=[dict(id=row.id, **_task_identity(row)) for row in rows.tasks if row.benchmark_id == profile.id],
                        reject_semantic_changes=True,
                    )
                    existing_profile = await session.get(Benchmark, profile.id)
                    preserve = existing_profile is not None and profile.id in preserve_profiles
                    if existing_profile is not None and not preserve:
                        await invalidate_benchmark_profile(session, profile.id)
                    states[profile.id] = local_state if preserve else "pending"
                    incoming_provenance = _immutable_profile_provenance(profile.profile_provenance)
                    if preserve and existing_profile is not None:
                        incoming_provenance = {**existing_profile.profile_provenance, **incoming_provenance}
                    provenances[profile.id] = incoming_provenance
                if rows.benchmarks:
                    for bench_batch in _batched(
                        rows.benchmarks,
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        bench_values = [
                            {
                                "id": row.id,
                                "display_name": row.display_name,
                                "upstream_kind": row.upstream_kind,
                                "upstream_locator": row.upstream_locator,
                                "upstream_revision": row.upstream_revision,
                                "license_spdx": row.license_spdx,
                                "license_url": row.license_url,
                                "splits": row.splits,
                                "series": row.series,
                                "imported_by": row.imported_by,
                                "execution_state": states[row.id],
                                "profile_provenance": provenances[row.id],
                            }
                            for row in bench_batch
                        ]
                        bench_insert = pg_insert(Benchmark).values(bench_values)
                        await session.execute(
                            bench_insert.on_conflict_do_update(
                                index_elements=["id"],
                                set_={
                                    "display_name": (bench_insert.excluded.display_name),
                                    "upstream_kind": (bench_insert.excluded.upstream_kind),
                                    "upstream_locator": (bench_insert.excluded.upstream_locator),
                                    "upstream_revision": (bench_insert.excluded.upstream_revision),
                                    "license_spdx": (bench_insert.excluded.license_spdx),
                                    "license_url": bench_insert.excluded.license_url,
                                    "splits": bench_insert.excluded.splits,
                                    "series": bench_insert.excluded.series,
                                    "imported_by": bench_insert.excluded.imported_by,
                                    "execution_state": bench_insert.excluded.execution_state,
                                    "profile_provenance": (
                                        bench_insert.excluded.profile_provenance
                                    ),
                                },
                            ),
                        )

                if rows.agents:
                    for agent_batch in _batched(
                        rows.agents,
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        agent_values = [
                            {
                                "name": row.name,
                                "version": row.version,
                                "mode": row.mode,
                                "spec": row.spec,
                            }
                            for row in agent_batch
                        ]
                        agent_insert = pg_insert(AgentModel).values(agent_values)
                        updated = set((await session.execute(
                            agent_insert.on_conflict_do_update(
                                index_elements=["name", "version"],
                                set_={
                                    "mode": agent_insert.excluded.mode,
                                    "spec": agent_insert.excluded.spec,
                                },
                                where=(AgentModel.mode != "native-runtime")
                                & (agent_insert.excluded.mode != "native-runtime"),
                            ).returning(AgentModel.name, AgentModel.version),
                        )).all())
                        for value in agent_values:
                            key = (value["name"], value["version"])
                            if key in updated:
                                continue
                            existing = await session.get(AgentModel, key, populate_existing=True)
                            if existing is None or (existing.mode, existing.spec) != (
                                value["mode"], value["spec"],
                            ):
                                raise ValueError("catalog cannot overwrite a published agent version")

                registered_tasks = []
                if rows.tasks:
                    for task_batch in _batched(
                        sorted(rows.tasks, key=lambda row: row.id),
                        POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
                    ):
                        task_values = [
                            {
                                "id": row.id,
                                "checksum": row.checksum,
                                "config": row.config,
                                "source": row.source,
                                "license": row.license,
                                "benchmark_id": row.benchmark_id,
                                "tags": row.tags,
                                "source_provenance": row.source_provenance,
                            }
                            for row in task_batch
                        ]
                        task_insert = pg_insert(TaskModel).values(task_values)
                        stored_tasks = (
                            (
                                await session.execute(
                                    task_insert.on_conflict_do_update(
                                        index_elements=["id"],
                                        set_={
                                            "checksum": task_insert.excluded.checksum,
                                            "config": task_insert.excluded.config,
                                            "source": task_insert.excluded.source,
                                            "license": task_insert.excluded.license,
                                            "benchmark_id": (task_insert.excluded.benchmark_id),
                                            "tags": task_insert.excluded.tags,
                                            "source_provenance": (
                                                task_insert.excluded.source_provenance
                                            ),
                                        },
                                    )
                                    .returning(TaskModel)
                                    .execution_options(populate_existing=True),
                                )
                            )
                            .scalars()
                            .all()
                        )
                        for task_row in sorted(stored_tasks, key=lambda row: row.id):
                            if uploads is not None and task_row.id in uploads:
                                registered_tasks.append(task_row)
                            elif task_row.config:
                                await ensure_task_image_materializations(session, task_row=task_row)
                if registered_tasks:
                    await publish_task_bundle_catalog(
                        session, tasks=registered_tasks, uploads=uploads or {}, now=datetime.now(UTC),
                    )
                await session.commit()
        finally:
            await engine.dispose()


class Boto3CatalogObjectStore:
    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        auth_kind: str = "static_keys",
    ) -> None:
        from loom.storage_credentials import build_s3_client

        self._client = build_s3_client(
            endpoint_url=endpoint_url,
            auth_kind=auth_kind,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )

    async def ensure_bucket(self, bucket: str) -> None:
        def _do() -> None:
            try:
                self._client.head_bucket(Bucket=bucket)
                return
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {"404", "NoSuchBucket", "NotFound"}:
                    raise
            try:
                self._client.create_bucket(Bucket=bucket)
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
                    raise

        await asyncio.to_thread(_do)

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]:
        def _do() -> list[ObjectInfo]:
            paginator = self._client.get_paginator("list_objects_v2")
            out: list[ObjectInfo] = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    out.append(
                        ObjectInfo(
                            bucket=bucket,
                            key=str(obj["Key"]),
                            size=int(obj.get("Size", 0)),
                            etag=_clean_etag(obj.get("ETag")),
                        ),
                    )
            return out

        return await asyncio.to_thread(_do)

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None:
        def _do() -> ObjectInfo | None:
            try:
                obj = self._client.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            return ObjectInfo(
                bucket=bucket,
                key=key,
                size=int(obj.get("ContentLength", 0)),
                etag=_clean_etag(obj.get("ETag")),
            )

        return await asyncio.to_thread(_do)

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        def _do() -> bytes:
            obj = self._client.get_object(Bucket=bucket, Key=key)
            return bytes(obj["Body"].read())

        return await asyncio.to_thread(_do)

    async def get_registered_object(self, version: ImmutableObjectVersion) -> bytes:
        def _do() -> bytes:
            output = io.BytesIO()
            read_verified_object_version(
                self._client, version, max_bytes=version.size_bytes, destination=output,
            )
            return output.getvalue()
        return await asyncio.to_thread(_do)

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        def _do() -> None:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)

        await asyncio.to_thread(_do)


async def provision_ready_benchmark_catalog(
    *,
    source_catalog: CatalogStore,
    target_catalog: CatalogStore,
    source_objects: CatalogObjectStore,
    target_objects: CatalogObjectStore,
    target_bucket: str,
    imported_by: str | None = None,
    target_source_store: ObjectStore | None = None,
) -> ProvisionStats:
    source_rows = await source_catalog.load_rows()
    ready_rows = _ready_catalog_rows(source_rows, target_bucket=target_bucket)
    ready_agents = agent_rows_from_service_catalog(imported_by=imported_by)
    ready_rows = replace(ready_rows, agents=ready_agents)
    if imported_by is not None:
        ready_rows = replace(
            ready_rows,
            benchmarks=[replace(row, imported_by=imported_by) for row in ready_rows.benchmarks],
        )
    source_tasks_by_id = {row.id: row for row in source_rows.tasks}
    object_copy_rows = replace(
        ready_rows,
        tasks=[replace(row, source=source_tasks_by_id[row.id].source) for row in ready_rows.tasks],
    )

    target_rows = await target_catalog.load_rows()
    _assert_catalog_rows_absent_or_exact(
        existing=target_rows,
        incoming=ready_rows,
    )
    # An exact destination profile already retained by its own available
    # journal needs no reactivation. Raw legacy copies still require their
    # existing fresh-audit boundary before any public selector can run.
    target_tasks = {row.id: row for row in target_rows.tasks}
    preserve_profiles = frozenset(
        profile.id for profile in target_rows.benchmarks
        if profile.id in {row.id for row in ready_rows.benchmarks}
        and any(row.benchmark_id == profile.id for row in ready_rows.tasks)
        and all(
            row.id in target_tasks and target_tasks[row.id].registered_source_spec is not None
            for row in ready_rows.tasks if row.benchmark_id == profile.id
        )
    )
    fail_closed_rows = _fail_closed_restore_rows(
        existing=target_rows,
        incoming=replace(ready_rows, benchmarks=[
            row for row in ready_rows.benchmarks if row.id not in preserve_profiles
        ]),
    )
    if fail_closed_rows is not None:
        # Any affected destination profile must be pending before object copy;
        # a fresh target-local admission follows the complete catalog restore.
        await target_catalog.upsert_rows(fail_closed_rows)
    await target_objects.ensure_bucket(target_bucket)
    source_registered = {
        row.id: row for row in object_copy_rows.tasks if row.registered_source_spec is not None
    }
    if any(row.source_provenance.get("bundle_content_manifest_sha256")
           and row.id not in source_registered for row in object_copy_rows.tasks):
        raise ValueError("source_unavailable: catalog references an unavailable registered source")
    uploads: dict[str, TaskBundleUpload] = {}
    registered_uploaded = registered_skipped = registered_bytes = 0
    if source_registered:
        if not callable(getattr(source_objects, "get_registered_object", None)):
            raise ValueError("source_exact_version_reader_required: registered versions must be read exactly")
        if target_source_store is None or not isinstance(target_catalog, PostgresCatalogStore):
            raise ValueError("destination_source_authority_required: versioned destination publisher is required")
        engine = create_async_engine(target_catalog.db_url, isolation_level="READ COMMITTED")
        try:
            publisher = TaskBundleSourcePublisher(
                async_sessionmaker(engine, expire_on_commit=False), target_source_store,
            )
            target_by_id = {row.id: row for row in ready_rows.tasks}
            for task_id, row in sorted(source_registered.items()):
                source_spec = TaskBundleSourceSpecV1.model_validate_json(json.dumps(row.registered_source_spec))
                if (source_spec.source_uri != row.source or source_spec.catalog_task_id != row.id
                    or source_spec.task_config != row.config
                    or source_spec.manifest.task_checksum != row.checksum.removeprefix("sha256:")):
                    raise ValueError("source_snapshot_drift: registered catalog copy differs from source authority")
                spec = TaskBundleSourceSpecV1.model_validate({**source_spec.model_dump(), "bucket": target_bucket})
                ticket = await publisher.prepare_copy(
                    spec, source_spec=source_spec, source_store=cast(SourceObjectReader, source_objects),
                    source_versions=row.registered_source_versions,
                )
                uploads[task_id] = ticket
                if ticket.available:
                    registered_skipped += len(spec.objects)
                else:
                    registered_uploaded += len(ticket.intents)
                    registered_bytes += sum(item.size_bytes for item in ticket.intents)
                target_by_id[task_id] = replace(
                    target_by_id[task_id], source=spec.source_uri,
                    source_provenance={**row.source_provenance, **spec.provenance},
                    registered_source_spec=spec.model_dump(mode="json"),
                )
            ready_rows = replace(ready_rows, tasks=[target_by_id[row.id] for row in ready_rows.tasks])
        finally:
            await engine.dispose()
    # Journaled inventories include transport objects outside bundle prefixes;
    # the publisher above transfers them exactly once. Legacy layouts retain
    # their existing copy/audit boundary and remain pending until validated.
    stats = await _copy_s3_bundle_objects(
        rows=replace(object_copy_rows, tasks=[row for row in object_copy_rows.tasks if row.id not in source_registered]),
        source_objects=source_objects, target_objects=target_objects, target_bucket=target_bucket,
    )
    stats = replace(
        stats, ready_tasks=len(ready_rows.tasks),
        source_objects=stats.source_objects + registered_uploaded + registered_skipped,
        target_objects_uploaded=stats.target_objects_uploaded + registered_uploaded,
        target_objects_skipped=stats.target_objects_skipped + registered_skipped,
        bytes_uploaded=stats.bytes_uploaded + registered_bytes,
    )
    if stats.target_objects_missing == 0:
        if uploads and isinstance(target_catalog, PostgresCatalogStore):
            await target_catalog.upsert_rows(
                ready_rows, uploads=uploads, preserve_profiles=preserve_profiles,
            )
        else:
            await target_catalog.upsert_rows(ready_rows)
    return stats


def _ready_catalog_rows(rows: CatalogRows, *, target_bucket: str) -> CatalogRows:
    _require_republished_workspace_contracts(rows.tasks)
    tasks_by_benchmark: dict[str, list[TaskRow]] = defaultdict(list)
    for task in rows.tasks:
        if task.benchmark_id:
            tasks_by_benchmark[task.benchmark_id].append(task)

    ready_benchmarks: list[BenchmarkRow] = []
    ready_tasks: list[TaskRow] = []
    for benchmark in rows.benchmarks:
        tasks = tasks_by_benchmark.get(benchmark.id, [])
        readiness = build_readiness_item(
            BenchmarkAuditSource(
                id=benchmark.id,
                display_name=benchmark.display_name,
                series=benchmark.series,
                upstream_kind=benchmark.upstream_kind,
                upstream_locator=benchmark.upstream_locator,
                upstream_revision=benchmark.upstream_revision,
            ),
            tasks=[
                TaskAuditSource(id=task.id, config=task.config, source=task.source)
                for task in tasks
            ],
            registry_names={benchmark.id},
        )
        if readiness.readiness_state != "runnable":
            continue
        if benchmark.execution_state == "historical":
            continue
        # Source-store versions and qualification are not portable. The
        # destination's shared admission checks its actual registered sources
        # and capability contract before a selector can execute.
        benchmark = replace(
            benchmark, execution_state="pending",
            profile_provenance=_immutable_profile_provenance(benchmark.profile_provenance),
        )
        ready_benchmarks.append(benchmark)
        ready_tasks.extend(
            _rewrite_s3_task_source(task, target_bucket=target_bucket)
            for task in tasks
            if _has_valid_task_config(task)
        )

    return CatalogRows(benchmarks=ready_benchmarks, tasks=ready_tasks)


def _requires_workspace_exec(benchmark_id: str | None) -> bool:
    if benchmark_id is None:
        return False
    return any(
        benchmark_id == stem
        or benchmark_id.startswith(f"{stem}-")
        or benchmark_id.startswith(f"{stem}@")
        for stem in _WORKSPACE_EXEC_BENCHMARK_STEMS
    )


def _require_republished_workspace_contracts(tasks: Iterable[TaskRow]) -> None:
    stale: list[str] = []
    for task in tasks:
        if not _requires_workspace_exec(task.benchmark_id):
            continue
        try:
            config = TaskConfig.model_validate(task.config)
        except ValidationError:
            continue
        if "workspace_exec" not in config.required_agent_capabilities:
            stale.append(task.id)
    if stale:
        raise ValueError(
            "republish workspace benchmark tasks with "
            "required_agent_capabilities=[\"workspace_exec\"] before "
            f"catalog provisioning; stale task examples: {sorted(stale)[:5]}",
        )


def agent_rows_from_service_catalog(
    *,
    imported_by: str | None = None,
    entries: Iterable[AgentEntry] | None = None,
) -> list[AgentRow]:
    """Materialize the service-mode agent catalog into DB rows.

    `loom_service.agent_catalog` remains the source of truth for supported
    agents. Staging/staging provisioning writes an auditable DB snapshot so
    restore drills can prove benchmarks, tasks, and agents were restored
    through the same official operator path.
    """
    out: list[AgentRow] = []
    for entry in entries if entries is not None else list_agents():
        spec = dict(entry.to_dict())
        spec["catalog_provenance"] = {
            "source": "loom_service.agent_catalog",
            "schema_version": AGENT_CATALOG_SPEC_SCHEMA_VERSION,
            "provisioned_by": imported_by,
        }
        out.append(
            AgentRow(
                name=entry.name,
                version=AGENT_CATALOG_VERSION,
                mode=entry.kind,
                spec=spec,
            )
        )
    return out


async def _copy_s3_bundle_objects(
    *,
    rows: CatalogRows,
    source_objects: CatalogObjectStore,
    target_objects: CatalogObjectStore,
    target_bucket: str,
) -> ProvisionStats:
    source_infos: dict[tuple[str, str], ObjectInfo] = {}
    missing_prefixes = 0
    for task in rows.tasks:
        parsed = _parse_s3_uri(task.source)
        if parsed is None:
            continue
        bucket, prefix = parsed
        infos = await source_objects.list_objects(bucket=bucket, prefix=prefix)
        if not infos:
            missing_prefixes += 1
            continue
        for info in infos:
            source_infos[(info.bucket, info.key)] = info

    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    bytes_skipped = 0
    for (bucket, key), info in sorted(source_infos.items()):
        target_info = await target_objects.head_object(bucket=target_bucket, key=key)
        if target_info is not None and _objects_match(info, target_info):
            skipped += 1
            bytes_skipped += info.size
            continue
        body = await source_objects.get_object(bucket=bucket, key=key)
        await target_objects.put_object(bucket=target_bucket, key=key, body=body)
        uploaded += 1
        bytes_uploaded += len(body)

    return ProvisionStats(
        ready_agents=len(rows.agents),
        ready_benchmarks=len(rows.benchmarks),
        ready_tasks=len(rows.tasks),
        source_objects=len(source_infos),
        target_objects_uploaded=uploaded,
        target_objects_skipped=skipped,
        target_objects_missing=missing_prefixes,
        bytes_uploaded=bytes_uploaded,
        bytes_skipped=bytes_skipped,
    )


def _has_valid_task_config(task: TaskRow) -> bool:
    try:
        TaskConfig.model_validate(task.config)
    except ValidationError:
        return False
    return True


def _rewrite_s3_task_source(task: TaskRow, *, target_bucket: str) -> TaskRow:
    parsed = _parse_s3_uri(task.source)
    if parsed is None:
        return task
    _source_bucket, prefix = parsed
    provenance = dict(task.source_provenance)
    service_input = provenance.get("service_execution_input")
    if isinstance(service_input, dict):
        service_input = dict(service_input)
        parsed_manifest = _parse_s3_uri(service_input.get("manifest_uri"))
        if parsed_manifest is not None:
            service_input["manifest_uri"] = f"s3://{target_bucket}/{parsed_manifest[1]}"
        provenance["service_execution_input"] = service_input
    return replace(task, source=f"s3://{target_bucket}/{prefix}", source_provenance=provenance)


def _parse_s3_uri(source: str | None) -> tuple[str, str] | None:
    if source is None or not source.startswith("s3://"):
        return None
    rest = source[len("s3://") :]
    if "/" not in rest:
        return None
    bucket, key = rest.split("/", 1)
    if not bucket or not key:
        return None
    return bucket, key


def _objects_match(source: ObjectInfo, target: ObjectInfo) -> bool:
    if source.size != target.size:
        return False
    if source.etag and target.etag:
        return source.etag == target.etag
    return True


def _clean_etag(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.strip('"')
