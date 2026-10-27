"""`python -m loom_benchmark_tool import <benchmark>` — end-to-end ingestion.

Steps per invocation:

1. Resolve the adapter from REGISTRY.
2. `fetch_upstream` populates a cached source dir.
3. Convert the selected instances into a fresh staging directory.
4. Delegate immutable bundle uploads and atomic pending-profile registration
   to the shared publisher. Existing accepted admission survives exact republish.

`tasks.source` is set to the s3:// prefix so the worker's Plan 13
`_materialize_task_dir` pulls the bundle on claim.
"""

from __future__ import annotations

import copy
import os
import re
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from loom_benchmarks.base import BenchmarkAdapter, BenchmarkInstance, UpstreamSource
from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY

from loom.license_policy import tags_with_license_execution_policy
from loom.trajectory.storage import ObjectStore

# Permissive but bounded: forward-slashes are fine (HumanEval IDs are
# `HumanEval/0`); reject anything that could traverse out of the
# benchmark namespace or smuggle in a control char / NUL / shell-special
# byte that would break the S3 prefix or TOML interpolation downstream.
_INSTANCE_ID_RE = re.compile(r"^[A-Za-z0-9._/+\-]+$")


def _validate_instance_id(instance_id: str) -> None:
    if not _INSTANCE_ID_RE.match(instance_id):
        raise ValueError(
            f"instance_id {instance_id!r} contains characters outside "
            f"[A-Za-z0-9._/+\\-]; reject to keep S3 prefix + task_id safe",
        )
    parts = instance_id.split("/")
    if "" in parts or ".." in parts or "." in parts:
        raise ValueError(
            f"instance_id {instance_id!r} contains empty / .. / . segments; reject",
        )


def _normalize_instance_ids(
    instance_ids: Iterable[str] | None,
) -> frozenset[str] | None:
    if instance_ids is None:
        return None
    normalized = frozenset(i.strip() for i in instance_ids if i.strip())
    if not normalized:
        return None
    for instance_id in normalized:
        _validate_instance_id(instance_id)
    return normalized


def _select_instances(
    adapter: BenchmarkAdapter,
    *,
    source_dir: Path,
    instance_ids: Iterable[str] | None,
    limit: int | None,
    census: dict[str, Any] | None = None,
) -> list[tuple[str, BenchmarkInstance]]:
    """Return the adapter instances selected for conversion.

    Without explicit ids, `limit` preserves the historical per-split cap.
    With explicit ids, scan every split to confirm all requested ids exist
    before callers write DB rows or publish a partial HF staging tree.
    """
    requested = _normalize_instance_ids(instance_ids)
    selected: list[tuple[str, BenchmarkInstance]] = []
    found: set[str] = set()
    all_ids: set[str] = set()

    for split in adapter.splits:
        count = 0
        for inst in adapter.list_instances(source_dir=source_dir, split=split):
            if census is not None:
                _validate_instance_id(inst.instance_id)
                if inst.instance_id in all_ids:
                    raise ValueError("duplicate upstream task identity in source census")
                all_ids.add(inst.instance_id)
            if requested is not None:
                if inst.instance_id not in requested:
                    continue
                found.add(inst.instance_id)
                if limit is None or len(selected) < limit:
                    selected.append((split, inst))
                continue

            if limit is not None and count >= limit:
                if census is not None:
                    continue
                break
            selected.append((split, inst))
            count += 1

    if requested is not None:
        missing = sorted(requested - found)
        if missing:
            formatted = ", ".join(missing)
            raise ValueError(f"requested instance_id(s) not found: {formatted}")

    if census is not None:
        selected_ids = {instance.instance_id for _split, instance in selected}
        census.update({
            "source_task_count": len(all_ids),
            "selected_task_ids": sorted(selected_ids),
            "selection": "full" if selected_ids == all_ids else "subset",
        })
    return selected


def _resolve_adapter(
    benchmark: str,
    *,
    benchmarks_config_path: Path | None = None,
) -> BenchmarkAdapter:
    """Resolve a benchmark name to its effective adapter.

    Plain case: `benchmark` is in REGISTRY → return that adapter.

    Remap case: `benchmark` is a `[[remap]]` id in
    `config/benchmarks.toml` → return a shallow copy of the
    `inherit` adapter with `.name` + `.upstream_source` overridden
    so all downstream paths (S3 prefix, task_id, on-disk task.toml's
    task.id) carry the remap's id instead of the base adapter's.
    """
    if benchmark in REGISTRY:
        return REGISTRY[benchmark]

    from loom.config.benchmarks import load_benchmarks_config, resolve_config_path

    cfg_path = benchmarks_config_path or resolve_config_path()
    cfg = load_benchmarks_config(cfg_path) if cfg_path else None
    if cfg is None:
        raise KeyError(
            f"no benchmark adapter registered for {benchmark!r} "
            f"(available: {sorted(REGISTRY)}); not found in "
            f"config/benchmarks.toml [[remap]] either",
        )
    remap = next((r for r in cfg.remap if r.id == benchmark), None)
    if remap is None:
        raise KeyError(
            f"no benchmark adapter registered for {benchmark!r} "
            f"(available: {sorted(REGISTRY)}; remaps: "
            f"{sorted(r.id for r in cfg.remap)})",
        )
    if remap.inherit not in REGISTRY:
        raise KeyError(
            f"remap {benchmark!r} inherits from {remap.inherit!r} "
            f"which is not in REGISTRY (available: {sorted(REGISTRY)})",
        )
    base = REGISTRY[remap.inherit]
    # deepcopy guards against adapters that ever cache state on the
    # instance — `copy.copy` would have aliased mutable instance attrs
    # back to the base. Current adapters are class-attr-only so the cost
    # is negligible.
    remapped = copy.deepcopy(base)
    # Instance-level overrides shadow class attrs from CatalogBackedAdapter.
    remapped.name = remap.id
    remapped.display_name = remap.display_name
    remapped.upstream_source = UpstreamSource(
        kind=remap.upstream_kind,
        locator=remap.upstream_locator,
    )
    remapped.license_spdx = remap.license_spdx
    remapped.license_url = remap.license_url
    if remap.splits is not None:
        remapped.splits = tuple(remap.splits)
    return remapped


async def run_import(
    *,
    benchmark: str,
    db_url: str,
    object_store: ObjectStore,
    bucket: str,
    cache_dir: Path,
    limit: int | None = None,
    instance_ids: Iterable[str] | None = None,
    imported_by: str | None = None,
    refresh: bool = False,
    benchmarks_config_path: Path | None = None,
) -> dict[str, int]:
    """Import upstream tasks for a benchmark.

    `benchmark` resolves via REGISTRY first, then via
    `config/benchmarks.toml` `[[remap]]` entries (issue #234 PR-2).
    Remapped benchmarks reuse the inherited adapter's parsing logic
    but write rows keyed on the remap's id.
    """
    # Honor the explicit override, then the env var, then the default.
    cfg_path = benchmarks_config_path
    if cfg_path is None and (
        env_path := os.environ.get(
            "LOOM_BENCHMARKS_CONFIG_PATH",
        )
    ):
        cfg_path = Path(env_path)
    adapter = _resolve_adapter(benchmark, benchmarks_config_path=cfg_path)
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_dir = fetch_upstream(
        adapter.upstream_source,
        cache_root=cache_dir,
        refresh=refresh,
    )
    census: dict[str, Any] = {}
    selected_instances = _select_instances(
        adapter,
        source_dir=source_dir,
        instance_ids=instance_ids, limit=limit, census=census,
    )

    # Retain this import surface's adapter/remap/fetch API, then reuse the same
    # publisher as datasets publish. Conversion and immutable storage upload
    # finish before the shared final catalog transaction takes any row locks.
    from loom_benchmark_tool.manifest import load_task_config_from_bundle
    from loom_cli.benchmark_prepare import (
        _adapter_profile_provenance,
        _adapter_task_source_provenance,
    )
    from loom_cli.benchmark_types import AdapterBenchmarkEntry, PreparedAdapterBenchmark
    from loom_cli.local_benchmark_publish import publish_local_benchmark

    with tempfile.TemporaryDirectory(prefix="loom-import-prepare-") as temporary:
        root = Path(temporary)
        task_tomls: list[Path] = []
        tasks: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []
        identities: set[str] = set()
        for index, (split, inst) in enumerate(selected_instances):
            _validate_instance_id(inst.instance_id)
            relative = f"task-{index:06d}"
            bundle = root / relative
            bundle.mkdir()
            converted = adapter.convert_instance(inst, out_dir=bundle)
            if not converted.task_id.startswith(adapter.name + "/") or converted.task_id in identities:
                raise ValueError("invalid or duplicate converted task identity")
            _validate_instance_id(converted.task_id.removeprefix(adapter.name + "/"))
            identities.add(converted.task_id)
            config = load_task_config_from_bundle(bundle)
            if config["task"]["id"] != converted.task_id:
                raise ValueError("adapter task.toml identity differs from converted task")
            tags = tags_with_license_execution_policy(
                inst.tags, getattr(adapter, "license_execution_policy", None),
            )
            tags["split"] = split
            tasks[relative] = {
                "task_id": converted.task_id, "license_spdx": converted.license_spdx,
                "tags": tags,
                "source_provenance": _adapter_task_source_provenance(
                    adapter, instance=inst, bundle_dir=bundle, task_config=config,
                    checksum=converted.checksum,
                ),
            }
            task_tomls.append(bundle / "task.toml")
            warnings.extend(converted.warnings)
        if not tasks:
            raise ValueError("benchmark import selected no tasks")
        manifest = {
            "benchmark_id": adapter.name, "display_name": adapter.display_name,
            "series": getattr(adapter, "series", None),
            "license_spdx": adapter.license_spdx, "license_url": adapter.license_url,
            "splits": list(adapter.splits), "upstream_kind": adapter.upstream_source.kind,
            "upstream_locator": adapter.upstream_source.locator,
            "upstream_revision": adapter.upstream_source.revision or "",
            "benchmark_profile_provenance": {
                **_adapter_profile_provenance(adapter), **census,
                "selected_task_ids": sorted(identity.removeprefix(adapter.name + "/") for identity in identities),
            },
            "tasks": list(tasks.values()),
        }
        prepared = PreparedAdapterBenchmark(
            AdapterBenchmarkEntry(adapter.name, adapter.display_name,
                                  getattr(adapter, "series", None), adapter.license_spdx),
            root, tuple(task_tomls), manifest, tasks, tuple(warnings),
        )
        await publish_local_benchmark(
            root, prepared_adapter=prepared, db_url=db_url, object_store=object_store,
            bucket=bucket, imported_by=imported_by,
        )
        return {"converted": len(tasks), "warnings": len(warnings)}
