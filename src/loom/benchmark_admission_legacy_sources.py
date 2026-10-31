"""Reviewed adoption boundary for pre-journal canonical task bundle sources.

One fresh materialization verifies the registered checksum and authored config
through existing registration/compatibility code. No uploaded JSON is a receipt.
Journaled sources always retain their journal's availability/retirement authority.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, NoReturn
from urllib.parse import urlsplit
from uuid import uuid4

from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from loom.benchmark_admission import AdapterValidation
    from loom.db.schema import Task
    from loom.models.benchmark_profile import BenchmarkProfileSpec
    from loom.trajectory.storage import ObjectStore


def has_journal_markers(task: Task) -> bool:
    """Strong source declarations cannot fall back to legacy byte validation."""
    provenance = task.source_provenance or {}
    if not isinstance(provenance, Mapping):
        return True
    return (
        "/loom-task-bundles/v1/" in (task.source or "")
        or "bundle_content_manifest_sha256" in provenance
        or "bundle_task_identity" in provenance
    )


def _safe_relative_path(path: str) -> bool:
    return bool(path) and not (
        path.startswith("/") or "\\" in path or "\x00" in path
        or any(part in {"", ".", ".."} for part in path.rstrip("/").split("/"))
        or PurePosixPath(path).is_absolute()
    )


async def prepare_legacy_source_validation(
    session: AsyncSession, *, spec: BenchmarkProfileSpec, object_store: ObjectStore | None = None,
    fixtures_root: Path | None = None,
) -> AdapterValidation | None:
    """Prepare trusted byte observations outside the final catalog-lock scope."""
    from loom.benchmark_admission import (
        AdapterValidation,
        AdmissionFinding,
        BenchmarkAdmissionError,
        snapshot_profile,
        snapshot_task,
    )
    from loom.benchmark_admission_contracts import CANONICAL, profile_validation_policy
    from loom.db.schema import Benchmark, Task, TaskBundleSource
    from loom.models.task import TaskConfig, normalize_steps
    from loom.task_bundle_compat import validate_task_dir_compatibility
    from loom.task_bundle_registration import prepare_task_bundle_registration
    from loom_worker.materializers import (
        FixtureMaterializer,
        HFMaterializer,
        Materializer,
        S3Materializer,
        _parse_hf_source,
    )

    benchmark = await session.scalar(
        select(Benchmark).where(Benchmark.id == spec.id).execution_options(populate_existing=True),
    )
    if benchmark is None or profile_validation_policy(benchmark) != CANONICAL:
        return None
    rows = list(await session.scalars(
        select(Task).where(Task.benchmark_id == spec.id).order_by(Task.id)
        .execution_options(populate_existing=True),
    ))
    profile_facts = snapshot_profile(benchmark)
    task_facts = {row.id: snapshot_task(row) for row in rows}
    verified = []

    def reject(code: str, message: str, row: Task) -> NoReturn:
        raise BenchmarkAdmissionError(AdmissionFinding(code, message, row.id))

    for row in rows:
        source = row.source
        if not isinstance(row.source_provenance or {}, Mapping):
            reject("task_metadata_invalid", "task source provenance must be an object", row)
        if not source or has_journal_markers(row):
            continue
        if await session.scalar(select(TaskBundleSource.id).where(
            TaskBundleSource.id == hashlib.sha256(source.encode()).hexdigest(),
        )) is not None:
            continue
        materializer: Materializer
        if source.startswith("s3://"):
            parsed = urlsplit(source)
            if (
                not parsed.netloc or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or not _safe_relative_path(parsed.path.removeprefix("/"))
            ):
                reject("legacy_source_invalid", "legacy object source has an invalid bundle path", row)
            if object_store is None:
                reject("source_access_required", "legacy object source admission requires current object-store access", row)
            materializer = S3Materializer(object_store)
        elif source.startswith("hf://"):
            try:
                repo, revision, path = _parse_hf_source(source)
            except ValueError:
                reject("legacy_source_invalid", "legacy Hub source has an invalid bundle locator", row)
            if (
                re.fullmatch(r"[A-Za-z0-9._-]+/[A-Za-z0-9._-]+", repo) is None
                or any(part in {".", ".."} for part in repo.split("/"))
                or re.fullmatch(r"[0-9a-f]{40}", revision) is None
                or not _safe_relative_path(path)
            ):
                reject("legacy_source_unpinned", "legacy Hub source requires an immutable commit and safe bundle path", row)
            materializer = HFMaterializer()
        elif source.startswith("fixture://"):
            if fixtures_root is None:
                reject("source_access_required", "fixture admission requires an explicit worker fixtures root", row)
            relative = source.removeprefix("fixture://")
            if not _safe_relative_path(relative):
                reject("legacy_source_invalid", "fixture source has an unsafe relative path", row)
            try:
                root = fixtures_root.resolve(strict=True)
                bundle_root = (root / relative).resolve(strict=True)
                profile_root = Path(benchmark.upstream_locator).resolve(strict=True)
                if (
                    benchmark.upstream_kind != "local-folder" or not bundle_root.is_dir()
                    or not bundle_root.is_relative_to(root) or not bundle_root.is_relative_to(profile_root)
                ):
                    reject("legacy_source_invalid", "fixture source is outside its declared source root", row)
                for directory, names, files in os.walk(bundle_root, followlinks=False):
                    if any((Path(directory) / name).is_symlink() for name in [*names, *files]):
                        reject("legacy_source_invalid", "fixture sources must contain regular files and directories", row)
            except OSError:
                reject("legacy_source_unavailable", "fixture source root is unavailable", row)
            materializer = FixtureMaterializer(fixtures_root=root)
        else:
            # Other schemes need their existing owning authority. In particular
            # unknown schemes cannot attest arbitrary host files.
            continue
        with tempfile.TemporaryDirectory(prefix="loom-catalog-source-audit-") as temporary:
            if source.startswith("hf://"):
                materializer = HFMaterializer(cache_dir=Path(temporary) / "hub")
            bundle = Path(temporary) / "bundle"
            bundle.mkdir()
            try:
                await materializer.materialize(source=source, task_dir=bundle, trial_id=uuid4())
            except Exception:
                reject("legacy_source_unavailable", "legacy task source could not be materialized", row)
            try:
                registration = prepare_task_bundle_registration(
                    bundle, task_id=row.id, check_target_architecture=False,
                )
                validate_task_dir_compatibility(bundle)
                expected = normalize_steps(TaskConfig.model_validate(row.config))
                document = expected.model_dump(mode="json")
                document["environment"]["network_policies_supported"] = sorted(
                    expected.environment.network_policies_supported,
                )
            except (OSError, ValueError):
                reject("legacy_source_invalid", "legacy bundle config or execution contract is invalid", row)
            if registration.manifest.task_checksum != row.checksum.removeprefix("sha256:"):
                reject("legacy_source_checksum_mismatch", "legacy bundle bytes differ from the registered checksum", row)
            if registration.task_config_document != document:
                reject("legacy_source_config_drift", "authored bundle config differs from the catalog config", row)
            declared_modes = (row.source_provenance or {}).get("bundle_file_metadata_sha256")
            if declared_modes is not None and (
                not isinstance(declared_modes, str)
                or declared_modes.removeprefix("sha256:") != registration.manifest.bundle_file_metadata_sha256
            ):
                reject("legacy_source_metadata_drift", "legacy bundle modes differ from registered source facts", row)
        verified.append(row.id)
    if not verified:
        return None
    return AdapterValidation(
        contract=CANONICAL.contract, profile_facts=profile_facts, task_facts=task_facts,
        legacy_source_authority=True, legacy_source_task_ids=tuple(verified),
    )
