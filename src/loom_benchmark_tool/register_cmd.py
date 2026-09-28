"""`python -m loom_benchmark_tool register <benchmark>` — per-deploy
counterpart to `publish`.

Two sources are supported:

- `source="hf"` (legacy): reads the manifest from
  `{hf_org}/loom-benchmark-{benchmark}` on HF Hub. Task rows point at
  `hf://{repo_id}@{revision}/{hf_path}`; pass `mirror_to_object_store`
  to also copy every bundle into internal S3/MinIO and rewrite the
  source URLs to `s3://…`.

- `source="object-store"`: reads the manifest straight from
  `s3://{bucket}/{benchmark_id}/{revision}/manifest.json` — the layout
  produced by the retired direct publisher. Task rows point at
  `s3://{bucket}/{benchmark_id}/{revision}/{hf_path}` with no HF hop or
  mirror step, because the bytes are already where the worker will
  materialize them.

New manifests carry validated per-task `TaskConfig` payloads, so
registered rows are runnable immediately after this command commits.
Legacy manifests that lack `task_config` remain catalog placeholders:
they preserve metadata and source pointers but are not counted as
runnable until republished or backfilled.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from loom_bundle_checksum import sha256_of_dir
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.task_image_materialization import ensure_task_image_materializations
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    ObjectStore,
    bundle_file_metadata_body,
    bundle_file_metadata_sha256,
    restore_bundle_file_metadata_sidecar,
)
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles
from loom_benchmark_tool.manifest import (
    MANIFEST_FILENAME,
    read_manifest_from_hf,
    repo_id_for,
    tb21_workspace_policy_isolated,
)

RegisterSource = Literal["hf", "object-store"]
_TB21_PROFILE_ID = "terminal-bench-2@tb2.1-r6"


def _hf_source_url(
    *,
    repo_id: str,
    revision: str,
    hf_path: str,
) -> str:
    """Canonical `hf://` source URL. The worker's hf:// dispatcher
    parses this exact shape; keep the format here in lockstep with
    `loom_worker.main_loop._materialize_hf_dir`."""
    return f"hf://{repo_id}@{revision}/{hf_path}"


def _object_store_source_url(
    *,
    bucket: str,
    benchmark_id: str,
    revision: str,
    hf_path: str,
) -> str:
    """Canonical `s3://` source URL for the direct-publish layout.

    Matches the key prefix the historical direct publisher
    writes under and the `s3://{bucket}/{prefix}` shape the worker's
    S3Materializer parses."""
    return f"s3://{bucket}/{benchmark_id}/{revision}/{hf_path}"


async def _read_manifest_from_object_store(
    *,
    object_store: ObjectStore,
    bucket: str,
    benchmark_id: str,
    revision: str,
) -> dict[str, Any]:
    """Fetch and parse `manifest.json` from the direct-publish layout.

    Raises FileNotFoundError-style exceptions if the object is missing,
    ValueError if the payload isn't a JSON object or the embedded
    `benchmark_id` doesn't match what we asked for."""
    key = f"{benchmark_id}/{revision}/{MANIFEST_FILENAME}"
    body = await object_store.get_object(bucket=bucket, key=key)
    try:
        manifest = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"manifest at s3://{bucket}/{key} is not valid JSON: {exc}",
        ) from exc
    if not isinstance(manifest, dict):
        raise ValueError(
            f"manifest at s3://{bucket}/{key} must be a JSON object, got {type(manifest).__name__}",
        )
    if manifest.get("benchmark_id") != benchmark_id:
        raise ValueError(
            f"manifest at s3://{bucket}/{key} declares benchmark_id="
            f"{manifest.get('benchmark_id')!r}, expected {benchmark_id!r}",
        )
    return cast(dict[str, Any], manifest)


@dataclass(frozen=True)
class MirrorResult:
    source: str
    uploaded: int
    skipped: int
    bytes_uploaded: int
    bytes_skipped: int


@dataclass(frozen=True)
class _PreparedTask:
    task_id: str
    checksum: str
    config: dict[str, Any]
    source: str
    license_spdx: str
    benchmark_id: str
    tags: dict[str, str]
    source_provenance: dict[str, Any]
    file_metadata_sha256: str | None = None


def _immutable_profile_provenance(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if key != "activation_audit"}


def _stable_runtime_tags(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(item) for key, item in value.items() if key != "runtime_source_mirrored_at"
    }


def _assert_existing_tb21_profile_is_immutable(
    *,
    benchmark: Benchmark,
    task_rows: list[TaskRow],
    manifest: dict[str, Any],
    prepared_tasks: list[_PreparedTask],
) -> None:
    """Allow exact idempotence while rejecting in-place physical-profile drift."""
    expected_benchmark = {
        "display_name": manifest["display_name"],
        "upstream_kind": manifest["upstream_kind"],
        "upstream_locator": manifest["upstream_locator"],
        "upstream_revision": manifest.get("upstream_revision", ""),
        "license_spdx": manifest["license_spdx"],
        "license_url": manifest.get("license_url", ""),
        "splits": list(manifest.get("splits", ["test"])),
        "series": manifest.get("series"),
        "profile_provenance": _immutable_profile_provenance(
            manifest.get("benchmark_profile_provenance") or {},
        ),
    }
    observed_benchmark = {
        "display_name": benchmark.display_name,
        "upstream_kind": benchmark.upstream_kind,
        "upstream_locator": benchmark.upstream_locator,
        "upstream_revision": benchmark.upstream_revision,
        "license_spdx": benchmark.license_spdx,
        "license_url": benchmark.license_url,
        "splits": list(benchmark.splits),
        "series": benchmark.series,
        "profile_provenance": _immutable_profile_provenance(
            benchmark.profile_provenance,
        ),
    }
    drift: list[str] = []
    if observed_benchmark != expected_benchmark:
        drift.append("benchmark identity/provenance")

    observed_by_id = {row.id: row for row in task_rows}
    expected_by_id = {task.task_id: task for task in prepared_tasks}
    if set(observed_by_id) != set(expected_by_id):
        drift.append("task set")
    for task_id in sorted(set(observed_by_id) & set(expected_by_id)):
        row = observed_by_id[task_id]
        expected = expected_by_id[task_id]
        observed_identity = {
            "checksum": row.checksum,
            "config": row.config,
            "source": row.source,
            "license": row.license,
            "benchmark_id": row.benchmark_id,
            "tags": _stable_runtime_tags(row.tags),
            "source_provenance": row.source_provenance,
        }
        expected_identity = {
            "checksum": expected.checksum,
            "config": expected.config,
            "source": expected.source,
            "license": expected.license_spdx,
            "benchmark_id": expected.benchmark_id,
            "tags": _stable_runtime_tags(expected.tags),
            "source_provenance": expected.source_provenance,
        }
        if observed_identity != expected_identity:
            drift.append(f"task {task_id}")
    if drift:
        raise ValueError(
            "immutable TB2.1 physical profile drift detected in "
            f"{', '.join(drift)}; publish a new physical profile ID instead",
        )


async def _locked_tb21_registration_preflight(
    session: AsyncSession,
    *,
    manifest: dict[str, Any],
    prepared_tasks: list[_PreparedTask],
) -> bool:
    """Return True for an exact existing profile; False when absent.

    Call inside a transaction. The advisory lock serializes absent-row checks,
    while row locks make the existing identity snapshot stable for comparison.
    """
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:profile_id))"),
        {"profile_id": _TB21_PROFILE_ID},
    )
    existing_benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == _TB21_PROFILE_ID).with_for_update(),
    )
    existing_tasks = list(
        (
            await session.scalars(
                select(TaskRow)
                .where(TaskRow.benchmark_id == _TB21_PROFILE_ID)
                .order_by(TaskRow.id)
                .with_for_update(),
            )
        ).all()
    )
    if existing_benchmark is not None:
        _assert_existing_tb21_profile_is_immutable(
            benchmark=existing_benchmark,
            task_rows=existing_tasks,
            manifest=manifest,
            prepared_tasks=prepared_tasks,
        )
        for task_row in existing_tasks:
            if task_row.config:
                await ensure_task_image_materializations(session, task_row=task_row)
        return True
    colliding = await session.scalar(
        select(TaskRow.id).where(
            TaskRow.id.in_([task.task_id for task in prepared_tasks]),
        )
    )
    if colliding is not None:
        raise ValueError(
            "immutable TB2.1 task ID collision detected; publish a new physical profile ID instead",
        )
    return False


def _safe_key_part(value: str) -> str:
    return value.strip("/").replace("/", "__")


def _validate_relative_prefix(value: str, *, label: str) -> str:
    prefix = value.strip("/")
    if not prefix:
        raise ValueError(f"{label} must not be empty")
    parts = Path(prefix).parts
    if prefix.startswith("/") or ".." in parts:
        raise ValueError(f"{label} contains unsafe path segments: {value!r}")
    return prefix


def _bundle_checksum(bundle_dir: Path) -> str:
    return sha256_of_dir(bundle_dir)


def _copy_hf_snapshot_bundle_for_registration(
    *,
    snapshot_root: Path,
    hf_path: str,
    out_root: Path,
    expected_metadata_sha256: str | None,
    require_sidecar: bool,
) -> Path:
    """Copy HF cache bytes into an owned tree, then restore trusted modes.

    HF snapshot entries may be symlinks into a process-shared blob cache. We
    intentionally read each file's bytes and create a new regular file before
    validating or chmodding, so registration cannot mutate shared cache state.
    """
    relative_prefix = _validate_relative_prefix(hf_path, label="hf_path")
    source = snapshot_root / relative_prefix
    if source.is_symlink() or not source.is_dir():
        raise ValueError(f"HF bundle path is missing or not a regular directory: {hf_path!r}")
    destination = out_root / relative_prefix
    if destination.exists():
        raise ValueError(f"duplicate HF bundle path in manifest: {hf_path!r}")
    try:
        for path in sorted(source.rglob("*")):
            rel = path.relative_to(source)
            target = destination / rel
            if path.is_dir():
                if path.is_symlink():
                    raise ValueError(f"HF bundle contains a symlink directory: {rel.as_posix()!r}")
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not path.is_file():
                raise ValueError(f"HF bundle contains a non-file entry: {rel.as_posix()!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
            target.chmod(0o644)
        sidecar = destination / BUNDLE_FILE_METADATA_NAME
        if sidecar.exists():
            restore_bundle_file_metadata_sidecar(
                destination,
                expected_sha256=expected_metadata_sha256,
                remove=True,
            )
        elif require_sidecar:
            raise ValueError("HF bundle file metadata sidecar is required")
        return destination
    except BaseException:
        shutil.rmtree(destination, ignore_errors=True)
        raise


def _mirror_prefix(
    *,
    repo_id: str,
    revision: str,
    task_id: str,
    checksum: str,
    hf_path: str,
    file_metadata_sha256: str,
) -> str:
    benchmark_id = task_id.split("/", 1)[0]
    return (
        f"{_safe_key_part(benchmark_id)}/"
        f"{_safe_key_part(repo_id)}/"
        f"{_safe_key_part(revision)}/"
        f"{_validate_relative_prefix(hf_path, label='hf_path')}/"
        f"{_safe_key_part(checksum)}/"
        f"{_safe_key_part(file_metadata_sha256)}/"
    )


async def mirror_manifest_task_bundle(
    *,
    repo_id: str,
    revision: str,
    task_id: str,
    checksum: str,
    hf_path: str,
    snapshot_root: Path,
    object_store: ObjectStore,
    bucket: str,
    expected_file_metadata_sha256: str | None = None,
) -> MirrorResult:
    """Mirror one manifest task bundle from a local HF snapshot into S3/MinIO.

    Re-running against the same snapshot is idempotent: existing byte-identical
    objects are skipped, while missing or changed objects are written.
    """
    relative_prefix = _validate_relative_prefix(hf_path, label="hf_path")
    bundle_dir = snapshot_root / relative_prefix
    if not bundle_dir.is_dir():
        raise FileNotFoundError(
            f"HF bundle path {relative_prefix!r} not found under {snapshot_root}",
        )
    actual_checksum = _bundle_checksum(bundle_dir)
    if actual_checksum != checksum:
        raise ValueError(
            "HF bundle checksum mismatch for "
            f"{task_id}: manifest={checksum} actual={actual_checksum}",
        )
    actual_metadata_sha256 = bundle_file_metadata_sha256(bundle_dir)
    if (
        expected_file_metadata_sha256 is not None
        and actual_metadata_sha256 != expected_file_metadata_sha256
    ):
        raise ValueError(
            "HF bundle file mode metadata mismatch for "
            f"{task_id}: manifest={expected_file_metadata_sha256} "
            f"actual={actual_metadata_sha256}",
        )
    validate_task_dir_dockerfiles(bundle_dir)

    await object_store.ensure_bucket(bucket)
    target_prefix = _mirror_prefix(
        repo_id=repo_id,
        revision=revision,
        task_id=task_id,
        checksum=checksum,
        hf_path=hf_path,
        file_metadata_sha256=actual_metadata_sha256,
    )
    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    bytes_skipped = 0
    for path in sorted(bundle_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(bundle_dir).as_posix()
        if rel == BUNDLE_FILE_METADATA_NAME:
            continue
        key = f"{target_prefix}{rel}"
        body = path.read_bytes()
        try:
            existing = await object_store.get_object(bucket=bucket, key=key)
        except Exception:
            existing = None
        if existing == body:
            skipped += 1
            bytes_skipped += len(body)
            continue
        await object_store.put_object(bucket=bucket, key=key, body=body)
        uploaded += 1
        bytes_uploaded += len(body)

    metadata_key = f"{target_prefix}{BUNDLE_FILE_METADATA_NAME}"
    metadata_body = bundle_file_metadata_body(bundle_dir)
    try:
        existing_metadata = await object_store.get_object(
            bucket=bucket,
            key=metadata_key,
        )
    except Exception:
        existing_metadata = None
    if existing_metadata != metadata_body:
        await object_store.put_object(
            bucket=bucket,
            key=metadata_key,
            body=metadata_body,
        )

    if uploaded + skipped == 0:
        raise FileNotFoundError(f"HF bundle path {relative_prefix!r} has no files")

    return MirrorResult(
        source=f"s3://{bucket}/{target_prefix}",
        uploaded=uploaded,
        skipped=skipped,
        bytes_uploaded=bytes_uploaded,
        bytes_skipped=bytes_skipped,
    )


async def _download_hf_bundle_snapshot(
    *,
    repo_id: str,
    revision: str,
    hf_token: str | None,
    tasks: list[dict[str, Any]],
    chunk_size: int | None = None,
    chunk_sleep_secs: float = 300.0,
) -> Path:
    """Snapshot-download the HF bundles for `tasks` into the shared HF cache.

    When `chunk_size` is set, `tasks` is split into batches and
    `snapshot_download` is called once per batch with `chunk_sleep_secs` seconds
    of sleep between batches. This keeps the total `resolve` API calls per
    5-minute window below HF's 5000/5min free-tier budget, at the cost of extra
    wall time. Because `snapshot_download` returns the same snapshot directory
    for a given `repo_id` + `revision`, files from every batch accumulate under
    the same returned path.
    """
    from huggingface_hub import snapshot_download

    patterns = [
        f"{_validate_relative_prefix(str(task['hf_path']), label='hf_path')}/*" for task in tasks
    ]

    if chunk_size is None or chunk_size <= 0 or len(patterns) <= chunk_size:
        single_shot = await asyncio.to_thread(
            snapshot_download,
            repo_id=repo_id,
            revision=revision,
            repo_type="dataset",
            allow_patterns=patterns,
            token=hf_token,
        )
        return Path(single_shot)

    snapshot_path: str | None = None
    total_batches = (len(patterns) + chunk_size - 1) // chunk_size
    for batch_idx in range(total_batches):
        start = batch_idx * chunk_size
        batch_patterns = patterns[start : start + chunk_size]
        print(
            f"mirror snapshot: batch {batch_idx + 1}/{total_batches} "
            f"({len(batch_patterns)} bundles)",
            flush=True,
        )
        snapshot_path = await asyncio.to_thread(
            snapshot_download,
            repo_id=repo_id,
            revision=revision,
            repo_type="dataset",
            allow_patterns=batch_patterns,
            token=hf_token,
        )
        if batch_idx < total_batches - 1 and chunk_sleep_secs > 0:
            print(
                f"mirror snapshot: sleeping {chunk_sleep_secs}s to stay under HF resolve budget",
                flush=True,
            )
            await asyncio.sleep(chunk_sleep_secs)
    assert snapshot_path is not None
    return Path(snapshot_path)


def task_config_from_manifest_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return validated TaskConfig payload for a manifest task entry.

    Legacy manifests do not carry task config data; those rows remain explicit
    non-runnable placeholders until republished or backfilled.
    """
    config = entry.get("task_config")
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise TypeError("manifest task_config must be an object")
    parsed = TaskConfig.model_validate(config)
    expected_task_id = entry.get("task_id")
    if expected_task_id is not None and parsed.task.id != expected_task_id:
        raise ValueError(
            "manifest task_config.task.id does not match task_id "
            f"({parsed.task.id!r} != {expected_task_id!r})",
        )
    return config


def validate_profile_registration(
    manifest: dict[str, Any], *, source: RegisterSource, mirror_to_object_store: bool,
) -> None:
    """Preserve the physical-profile admission contract for every producer."""
    manifest_tasks = list(manifest["tasks"])
    profile_provenance = dict(manifest.get("benchmark_profile_provenance") or {})
    if manifest.get("benchmark_id") == _TB21_PROFILE_ID:
        if source == "hf" and not mirror_to_object_store:
            raise ValueError(
                "TB2.1 HF registration requires mirror_to_object_store; "
                "use source='object-store' for a direct publish",
            )
        if not tb21_workspace_policy_isolated(
            profile_provenance.get("workspace_staging_policy"),
        ):
            raise ValueError("TB2.1 profile is missing private workspace isolation provenance")
        for task in manifest_tasks:
            task_provenance = task.get("source_provenance")
            if not isinstance(task_provenance, dict) or not tb21_workspace_policy_isolated(
                task_provenance.get("workspace_staging_policy"),
            ):
                raise ValueError("TB2.1 task is missing private workspace isolation provenance")
            verifier_asset = task_provenance.get("verifier_asset")
            if (
                not isinstance(verifier_asset, dict)
                or not isinstance(verifier_asset.get("script_path"), str)
                or not verifier_asset["script_path"].startswith("/")
                or not isinstance(verifier_asset.get("sha256"), str)
                or not verifier_asset["sha256"].startswith("sha256:")
                or verifier_asset.get("mode") != "0755"
                or not isinstance(
                    task_provenance.get("bundle_file_metadata_sha256"),
                    str,
                )
                or not task_provenance["bundle_file_metadata_sha256"].startswith("sha256:")
            ):
                raise ValueError("TB2.1 task is missing verifier asset provenance")


async def run_register(
    *,
    benchmark: str,
    db_url: str,
    source: RegisterSource = "hf",
    hf_org: str = "",
    hf_token: str | None = None,
    revision: str = "main",
    registered_by: str | None = None,
    mirror_to_object_store: bool = False,
    object_store: ObjectStore | None = None,
    bucket: str = "loom-benchmarks",
    chunk_size: int | None = None,
    chunk_sleep_secs: float = 300.0,
    manifest: dict[str, Any] | None = None,
    activate_alias: bool = False,
) -> dict[str, Any]:
    """Read manifest from the selected source, upsert Benchmark + Task
    rows. Returns `{"registered": N, "skipped": M, "source": str,
    "repo_id": str, "revision": str, …}`.

    The manifest's `task_count` MUST match the length of `tasks`; the
    publish path guarantees this. We trust it here rather than
    re-walking the source tree.

    Source-specific requirements:

    - `source="hf"` (default): `hf_org` is required. `mirror_to_object_store`
      may be set to also mirror bundles into `object_store`+`bucket`.
    - `source="object-store"`: `object_store`, `bucket`, and `revision` are
      required. `mirror_to_object_store` is a no-op (the bundles are
      already in the bucket).
    """
    if activate_alias:
        raise ValueError("register never activates benchmark aliases; run datasets activate")
    if source == "hf":
        if not hf_org:
            raise ValueError("source='hf' requires hf_org")
    elif source == "object-store":
        if object_store is None:
            raise ValueError("source='object-store' requires object_store")
        if not revision or revision == "main":
            raise ValueError(
                "source='object-store' requires an explicit --revision "
                "(the content-addressed revision emitted by publish)",
            )
        if mirror_to_object_store:
            raise ValueError(
                "source='object-store' + mirror_to_object_store is redundant "
                "— the bundles are already in the target bucket",
            )
    else:  # pragma: no cover — argparse constrains this
        raise ValueError(f"unknown register source: {source!r}")

    benchmark_id_hint = benchmark  # publish uses adapter.name; matches for our built-in adapters
    if source == "hf":
        repo_id = repo_id_for(hf_org, benchmark)
        manifest = manifest or read_manifest_from_hf(
            hf_org=hf_org,
            benchmark=benchmark,
            hf_token=hf_token,
            revision=revision,
        )
    else:
        assert object_store is not None
        manifest = manifest or await _read_manifest_from_object_store(
            object_store=object_store,
            bucket=bucket,
            benchmark_id=benchmark_id_hint,
            revision=revision,
        )
        repo_id = f"s3://{bucket}/{manifest['benchmark_id']}"

    manifest_tasks = list(manifest["tasks"])
    profile_provenance = dict(manifest.get("benchmark_profile_provenance") or {})
    # TB2.1 is a security-sensitive physical profile.  Registration stores its
    # immutable bytes and provenance, but cannot itself make those bytes
    # submit-able: only the fresh object-store audit in `datasets activate`
    # promotes the row and its public alias together.
    execution_state = "pending" if manifest.get("benchmark_id") == _TB21_PROFILE_ID else "runnable"
    validate_profile_registration(
        manifest, source=source, mirror_to_object_store=mirror_to_object_store,
    )

    snapshot_root: Path | None = None
    mirrored = 0
    mirror_uploaded = 0
    mirror_skipped = 0
    mirror_bytes_uploaded = 0
    mirror_bytes_skipped = 0
    mirrored_at = datetime.now(UTC).isoformat()
    owned_snapshot_temp: tempfile.TemporaryDirectory[str] | None = None
    if source == "hf" and mirror_to_object_store:
        if object_store is None:
            raise ValueError("mirror_to_object_store requires object_store")
        downloaded_snapshot_root = await _download_hf_bundle_snapshot(
            repo_id=repo_id,
            revision=revision,
            hf_token=hf_token,
            tasks=manifest_tasks,
            chunk_size=chunk_size,
            chunk_sleep_secs=chunk_sleep_secs,
        )
        owned_snapshot_temp = tempfile.TemporaryDirectory(prefix="loom-hf-register-")
        snapshot_root = Path(owned_snapshot_temp.name)
        try:
            for task in manifest_tasks:
                provenance = task.get("source_provenance")
                task_provenance = provenance if isinstance(provenance, dict) else {}
                expected_metadata_sha256 = task_provenance.get(
                    "bundle_file_metadata_sha256",
                )
                _copy_hf_snapshot_bundle_for_registration(
                    snapshot_root=downloaded_snapshot_root,
                    hf_path=task["hf_path"],
                    out_root=snapshot_root,
                    expected_metadata_sha256=(
                        expected_metadata_sha256
                        if isinstance(expected_metadata_sha256, str)
                        else None
                    ),
                    require_sidecar=(manifest.get("benchmark_id") == _TB21_PROFILE_ID),
                )
        except BaseException:
            owned_snapshot_temp.cleanup()
            raise

    registered = 0
    legacy_placeholders = 0
    skipped = 0
    prepared_tasks: list[_PreparedTask] = []
    for task in manifest_tasks:
        if source == "hf":
            task_source = _hf_source_url(
                repo_id=repo_id,
                revision=revision,
                hf_path=task["hf_path"],
            )
        else:
            task_source = _object_store_source_url(
                bucket=bucket,
                benchmark_id=manifest["benchmark_id"],
                revision=revision,
                hf_path=task["hf_path"],
            )
        tags = dict(task.get("tags") or {})
        source_provenance = dict(task.get("source_provenance") or {})
        config = task_config_from_manifest_entry(task)
        if not config:
            legacy_placeholders += 1
        provenance_metadata_sha256 = source_provenance.get(
            "bundle_file_metadata_sha256",
        )
        file_metadata_sha256 = (
            provenance_metadata_sha256 if isinstance(provenance_metadata_sha256, str) else None
        )
        if source == "object-store":
            tags.update(
                {
                    "runtime_source_kind": "internal_object_store",
                    "runtime_source_mirrored_at": mirrored_at,
                }
            )
        if source == "hf" and mirror_to_object_store:
            assert snapshot_root is not None
            relative_prefix = _validate_relative_prefix(task["hf_path"], label="hf_path")
            file_metadata_sha256 = bundle_file_metadata_sha256(
                snapshot_root / relative_prefix,
            )
            expected_metadata_sha256 = source_provenance.get(
                "bundle_file_metadata_sha256",
            )
            if (
                expected_metadata_sha256 is not None
                and expected_metadata_sha256 != file_metadata_sha256
            ):
                raise ValueError(
                    "HF bundle file mode metadata does not match manifest provenance "
                    f"for {task['task_id']}",
                )
            target_prefix = _mirror_prefix(
                repo_id=repo_id,
                revision=revision,
                task_id=task["task_id"],
                checksum=task["checksum"],
                hf_path=task["hf_path"],
                file_metadata_sha256=file_metadata_sha256,
            )
            task_source = f"s3://{bucket}/{target_prefix}"
            tags.update(
                {
                    "hf_repo_id": repo_id,
                    "hf_revision": revision,
                    "hf_path": task["hf_path"],
                    "hf_checksum": task["checksum"],
                    "runtime_source_kind": "internal_object_store",
                    "runtime_source_mirrored_at": mirrored_at,
                }
            )
        prepared_tasks.append(
            _PreparedTask(
                task_id=task["task_id"],
                checksum=task["checksum"],
                config=config,
                source=task_source,
                license_spdx=task.get("license_spdx", manifest["license_spdx"]),
                benchmark_id=manifest["benchmark_id"],
                tags=tags,
                source_provenance=source_provenance,
                file_metadata_sha256=file_metadata_sha256,
            )
        )

    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        exact_existing = False
        if manifest.get("benchmark_id") == _TB21_PROFILE_ID:
            async with session_factory() as session, session.begin():
                exact_existing = await _locked_tb21_registration_preflight(
                    session,
                    manifest=manifest,
                    prepared_tasks=prepared_tasks,
                )
            if exact_existing:
                skipped = len(prepared_tasks)

        # Network I/O is deliberately outside database transactions. Prefixes
        # are content-addressed and the profile is still absent/pending; a
        # failed registration may leave safe orphan objects but can never
        # rewrite an already-activated physical profile.
        if not exact_existing and mirror_to_object_store:
            assert object_store is not None
            assert snapshot_root is not None
            prepared_by_id = {task.task_id: task for task in prepared_tasks}
            for raw_task in manifest_tasks:
                prepared = prepared_by_id[raw_task["task_id"]]
                mirror = await mirror_manifest_task_bundle(
                    repo_id=repo_id,
                    revision=revision,
                    task_id=raw_task["task_id"],
                    checksum=raw_task["checksum"],
                    hf_path=raw_task["hf_path"],
                    snapshot_root=snapshot_root,
                    object_store=object_store,
                    bucket=bucket,
                    expected_file_metadata_sha256=prepared.source_provenance.get(
                        "bundle_file_metadata_sha256",
                    )
                    or prepared.file_metadata_sha256,
                )
                mirrored += 1
                mirror_uploaded += mirror.uploaded
                mirror_skipped += mirror.skipped
                mirror_bytes_uploaded += mirror.bytes_uploaded
                mirror_bytes_skipped += mirror.bytes_skipped

        if not exact_existing:
            async with session_factory() as session, session.begin():
                continue_writes = True
                if manifest.get("benchmark_id") == _TB21_PROFILE_ID:
                    # Reacquire and recheck after object upload. A concurrent
                    # exact registration wins idempotently; drift still fails.
                    exact_existing = await _locked_tb21_registration_preflight(
                        session,
                        manifest=manifest,
                        prepared_tasks=prepared_tasks,
                    )
                    continue_writes = not exact_existing
                    if exact_existing:
                        skipped = len(prepared_tasks)

                if continue_writes:
                    series = manifest.get("series")
                    update_set: dict[str, Any] = {
                        "display_name": manifest["display_name"],
                        "upstream_revision": manifest.get("upstream_revision", ""),
                        "imported_by": registered_by or "loom_benchmark_tool:register",
                    }
                    if series is not None:
                        update_set["series"] = series
                    if "benchmark_profile_provenance" in manifest:
                        update_set["profile_provenance"] = profile_provenance
                    await session.execute(
                        pg_insert(Benchmark)
                        .values(
                            id=manifest["benchmark_id"],
                            display_name=manifest["display_name"],
                            upstream_kind=manifest["upstream_kind"],
                            upstream_locator=manifest["upstream_locator"],
                            upstream_revision=manifest.get("upstream_revision", ""),
                            license_spdx=manifest["license_spdx"],
                            license_url=manifest.get("license_url", ""),
                            splits=manifest.get("splits", ["test"]),
                            series=series,
                            execution_state=execution_state,
                            profile_provenance=profile_provenance,
                            imported_by=registered_by or "loom_benchmark_tool:register",
                        )
                        .on_conflict_do_update(
                            index_elements=["id"],
                            set_=update_set,
                        ),
                    )
                    for task in prepared_tasks:
                        task_row = (
                            await session.execute(
                                pg_insert(TaskRow)
                                .values(
                                    id=task.task_id,
                                    checksum=task.checksum,
                                    config=task.config,
                                    source=task.source,
                                    license=task.license_spdx,
                                    benchmark_id=task.benchmark_id,
                                    tags=task.tags,
                                    source_provenance=task.source_provenance,
                                )
                                .on_conflict_do_update(
                                    index_elements=["id"],
                                    set_={
                                        "checksum": task.checksum,
                                        "source": task.source,
                                        "license": task.license_spdx,
                                        "benchmark_id": task.benchmark_id,
                                        "tags": task.tags,
                                        "config": task.config,
                                        "source_provenance": task.source_provenance,
                                    },
                                )
                                .returning(TaskRow)
                            )
                        ).scalar_one()
                        if task.config:
                            await ensure_task_image_materializations(
                                session,
                                task_row=task_row,
                            )
                        registered += 1
    finally:
        await engine.dispose()
        if owned_snapshot_temp is not None:
            owned_snapshot_temp.cleanup()

    return {
        "registered": registered,
        "legacy_placeholders": legacy_placeholders,
        "skipped": skipped,
        "mirrored": mirrored,
        "mirror_uploaded": mirror_uploaded,
        "mirror_skipped": mirror_skipped,
        "mirror_bytes_uploaded": mirror_bytes_uploaded,
        "mirror_bytes_skipped": mirror_bytes_skipped,
        "source": source,
        "repo_id": repo_id,
        "revision": revision,
    }
