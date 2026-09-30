"""Publish either an upstream adapter or a local folder through one backend."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from loom_cli import local_benchmark_publish
from loom_cli.local_benchmark_publish import LocalBenchmarkPublishStats


def default_benchmark_cache() -> Path:
    override = os.environ.get("LOOM_BENCHMARK_CACHE")
    if override:
        return Path(override).expanduser()
    root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return root / "loom" / "benchmarks"


async def publish_benchmark(
    path: Path | None = None,
    *,
    benchmark: str | None = None,
    harbor_source: Path | None = None,
    cache_dir: Path | None = None,
    refresh: bool = False,
    limit: int | None = None,
    instance_ids: set[str] | None = None,
    **publication: Any,
) -> LocalBenchmarkPublishStats:
    if sum(value is not None for value in (path, benchmark, harbor_source)) != 1:
        raise ValueError("choose exactly one local PATH, --benchmark SLUG or --harbor-source SPEC.json")
    if benchmark is None and harbor_source is None:
        if refresh or limit is not None or instance_ids or cache_dir is not None:
            raise ValueError(
                "--cache-dir, --refresh, --limit and --instance-id require --benchmark"
            )
        assert path is not None
        return await local_benchmark_publish.publish_local_benchmark(path, **publication)
    if harbor_source is not None and (publication.get("execution_profile") or publication.get("compat_flatten_environment")):
        raise ValueError("native Harbor intake preserves official semantics; publish a distinct derived profile for repairs")
    if any(
        publication.get(key) is not None
        for key in (
            "benchmark_id",
            "display_name",
            "series",
            "license_spdx",
            "source_subdir",
        )
    ):
        raise ValueError(
            "benchmark metadata comes from the adapter; metadata flags require local PATH"
        )
    try:
        from loom_cli.benchmark_prepare import prepare_adapter_benchmark

        with tempfile.TemporaryDirectory(prefix="loom-adapter-prepare-") as temporary:
            root = Path(temporary)
            adapter = None
            if harbor_source is not None:
                from loom_cli.harbor_benchmark_prepare import (
                    HarborBenchmarkSpec,
                    HarborNativeAdapter,
                )

                spec = HarborBenchmarkSpec.model_validate_json(harbor_source.read_bytes())
                adapter = HarborNativeAdapter(spec)
                benchmark = spec.id
            assert benchmark is not None
            prepared = prepare_adapter_benchmark(
                benchmark,
                adapter_override=adapter,
                cache_dir=(cache_dir or default_benchmark_cache()).expanduser(),
                staging_dir=root,
                refresh=refresh,
                limit=limit,
                instance_ids=instance_ids,
            )
            return await local_benchmark_publish.publish_local_benchmark(
                root, prepared_adapter=prepared, **publication
            )
    except ModuleNotFoundError as exc:
        if (exc.name or "").split(".")[0] not in {
            "loom_benchmarks", "loom_benchmark_terminal_bench_2", "datasets",
        }:
            raise
        raise ValueError(
            "--benchmark requires optional upstream adapter dependencies "
            f"(missing {exc.name}). From the Loom repository, run "
            "`uv sync --locked --extra rollout`, then retry with `uv run loom datasets publish`. "
            "Local PATH publication does not require these dependencies."
        ) from exc
