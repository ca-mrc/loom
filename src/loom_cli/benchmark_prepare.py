"""Prepare upstream adapters for the ordinary benchmark publication pipeline."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any, cast

from loom_benchmarks.base import BenchmarkAdapter
from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY

from loom.license_policy import tags_with_license_execution_policy
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import bundle_file_metadata_sha256
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles
from loom_benchmark_tool.import_cmd import _select_instances, _validate_instance_id
from loom_benchmark_tool.manifest import load_task_config_from_bundle
from loom_cli.benchmark_types import AdapterBenchmarkEntry, PreparedAdapterBenchmark


def prepare_adapter_benchmark(
    benchmark: str,
    *,
    cache_dir: Path,
    staging_dir: Path,
    refresh: bool = False,
    limit: int | None = None,
    instance_ids: set[str] | None = None,
    adapter_override: BenchmarkAdapter | None = None,
) -> PreparedAdapterBenchmark:
    try:
        adapter = adapter_override if adapter_override is not None else REGISTRY[benchmark]
    except KeyError as exc:
        raise ValueError(f"unknown benchmark adapter: {benchmark}") from exc
    # Profile ids such as terminal-bench-2@tb2.1-r6 are valid upstream identities.
    if not re.fullmatch(r"[a-z0-9][a-z0-9@._-]*", adapter.name):
        raise ValueError("benchmark adapter name must be a single path component")
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        source_dir = _prepare_adapter_source(adapter=adapter, cache_dir=cache_dir, refresh=refresh)
    except PermissionError as exc:
        raise ValueError(
            f"benchmark cache {cache_dir.resolve()} is not writable or contains files "
            "owned by another user; choose a writable --cache-dir or LOOM_BENCHMARK_CACHE"
        ) from exc
    origin = getattr(getattr(adapter, "spec", None), "origin", None)
    if origin is not None:
        if limit is not None:
            raise ValueError("native Harbor subsets require explicit instance ids and a distinct profile identity")
        requested = set(origin.subset) if instance_ids is None else instance_ids
        if requested != set(origin.subset):
            raise ValueError("native Harbor selected instance ids must match the persisted origin.subset")
        instance_ids = requested or None
    selected = _select_instances(
        adapter, source_dir=source_dir, instance_ids=instance_ids, limit=limit
    )
    tasks: dict[str, dict[str, Any]] = {}
    ids: set[str] = set()
    tomls: list[Path] = []
    warnings: list[str] = []
    for index, (split, inst) in enumerate(selected):
        _validate_instance_id(inst.instance_id)
        # Physical staging names need not encode ids (HumanEval/0 and
        # HumanEval_0 must never overwrite each other).
        relative = f"task-{index:06d}"
        bundle = staging_dir / relative
        bundle.mkdir()
        converted = adapter.convert_instance(inst, out_dir=bundle)
        warnings.extend(f"{converted.task_id}: {message}" for message in converted.warnings)
        if not converted.task_id.startswith(adapter.name + "/") or converted.task_id in ids:
            raise ValueError(f"invalid or duplicate adapter task id: {converted.task_id}")
        _validate_instance_id(converted.task_id.removeprefix(adapter.name + "/"))
        ids.add(converted.task_id)
        if origin is None:
            validate_task_dir_dockerfiles(bundle)
        config = load_task_config_from_bundle(bundle)
        if config["task"]["id"] != converted.task_id:
            raise ValueError(f"adapter task.toml id does not match {converted.task_id}")
        checksum = task_checksum(bundle)
        provenance = _adapter_task_source_provenance(
            adapter,
            instance=inst,
            bundle_dir=bundle,
            task_config=config,
            checksum=checksum,
        )
        metadata = bundle_file_metadata_sha256(bundle)
        if provenance.get("bundle_file_metadata_sha256") not in (None, metadata):
            raise ValueError("adapter bundle file mode provenance does not match staged bundle")
        provenance["bundle_file_metadata_sha256"] = metadata
        tags = tags_with_license_execution_policy(
            inst.tags, getattr(adapter, "license_execution_policy", None)
        )
        tags["split"] = split
        tags["required_artifacts_contract"] = _required_artifact_contract_tag(config)
        tasks[relative] = {
            "task_id": converted.task_id,
            "license_spdx": converted.license_spdx,
            "tags": tags,
            "source_provenance": provenance,
        }
        tomls.append(bundle / "task.toml")
    if not tomls:
        raise ValueError(f"benchmark {benchmark} selected no tasks")
    profile_provenance = _adapter_profile_provenance(adapter)
    if origin is not None:
        profile_provenance["compatibility"] = {
            "imported": True, "runtime_verified": False,
            "blocked_tasks": sum(item["source_provenance"]["compatibility"]["status"] == "blocked" for item in tasks.values()),
            "task_count": len(tasks),
        }
    manifest = {
        "benchmark_id": adapter.name,
        "display_name": adapter.display_name,
        "series": getattr(adapter, "series", None),
        "license_spdx": adapter.license_spdx,
        "license_url": adapter.license_url,
        "splits": list(adapter.splits),
        "upstream_kind": adapter.upstream_source.kind,
        "upstream_locator": adapter.upstream_source.locator,
        "upstream_revision": adapter.upstream_source.revision or "",
        "benchmark_profile_provenance": profile_provenance,
        "tasks": list(tasks.values()),
    }
    from loom_benchmark_tool.register_cmd import validate_profile_registration

    validate_profile_registration(manifest, source="object-store", mirror_to_object_store=False)
    return PreparedAdapterBenchmark(
        AdapterBenchmarkEntry(
            adapter.name,
            adapter.display_name,
            getattr(adapter, "series", None),
            adapter.license_spdx,
        ),
        staging_dir,
        tuple(tomls),
        manifest,
        tasks,
        tuple(warnings),
    )


def _adapter_profile_provenance(adapter: object) -> dict[str, object]:
    provider = getattr(adapter, "profile_provenance", None)
    if provider is None:
        return {}
    value = provider()
    if not isinstance(value, dict):
        raise TypeError("adapter profile_provenance() must return an object")
    return dict(value)


def _adapter_task_source_provenance(
    adapter: object,
    *,
    instance: object,
    bundle_dir: Path,
    task_config: dict[str, object],
    checksum: str,
) -> dict[str, object]:
    provider = getattr(adapter, "task_source_provenance", None)
    if provider is None:
        return {}
    value = provider(
        instance=instance,
        bundle_dir=bundle_dir,
        task_config=task_config,
        checksum=checksum,
    )
    if not isinstance(value, dict):
        raise TypeError("adapter task_source_provenance() must return an object")
    return dict(value)


def _required_artifact_contract_tag(task_config: dict[str, object]) -> str:
    """Record whether the published config carries verifier-required outputs.

    The explicit ``none`` value matters as much as ``declared``: downstream
    diagnostics can distinguish a task that intentionally has no required
    outputs from an older manifest that predates the contract metadata.
    """
    raw_steps = task_config.get("steps")
    if isinstance(raw_steps, list):
        for raw_step in raw_steps:
            if not isinstance(raw_step, dict):
                continue
            required = raw_step.get("required_artifacts")
            if isinstance(required, list) and any(
                isinstance(item, str) and bool(item.strip()) for item in required
            ):
                return "declared"
    return "none"


def _prepare_adapter_source(
    *,
    adapter: Any,
    cache_dir: Path,
    refresh: bool,
) -> Path:
    """Fetch an adapter's execution source and any independent audit source.

    Most adapters have one upstream.  Audited physical profiles may also
    declare ``audit_source`` plus ``audit_manifest_source``.  The latter is
    fetched independently at its immutable revision and copied beneath the
    execution materialization's ``audit/`` directory before the adapter is
    allowed to enumerate instances.  This keeps the generic publish command
    usable without weakening the profile's two-authority verification gate.
    """
    source_dir = cast(
        Path,
        fetch_upstream(
            adapter.upstream_source,
            cache_root=cache_dir,
            refresh=refresh,
        ),
    )
    audit_source = getattr(adapter, "audit_source", None)
    audit_manifest_source = getattr(adapter, "audit_manifest_source", None)
    if audit_source is None and audit_manifest_source is None:
        return source_dir
    if audit_source is None or not isinstance(audit_manifest_source, str):
        raise ValueError(
            "audited benchmark adapters must declare both audit_source and audit_manifest_source",
        )
    audit_root = cast(
        Path,
        fetch_upstream(
            audit_source,
            cache_root=cache_dir,
            refresh=refresh,
        ),
    )
    audit_checkout = audit_root / "repo" if audit_source.kind == "git" else audit_root
    source_manifest = audit_checkout / audit_manifest_source
    if not source_manifest.is_file():
        raise ValueError(
            f"audit source is missing required manifest {audit_manifest_source!r}",
        )
    target_manifest = source_dir / "audit" / audit_manifest_source
    target_manifest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_manifest, target_manifest)
    return source_dir
