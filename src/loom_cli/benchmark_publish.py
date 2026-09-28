"""Publish either an upstream adapter or a local folder through one backend."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from loom_cli import local_benchmark_publish
from loom_cli.benchmark_prepare import prepare_adapter_benchmark
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
    cache_dir: Path | None = None,
    refresh: bool = False,
    limit: int | None = None,
    instance_ids: set[str] | None = None,
    **publication: Any,
) -> LocalBenchmarkPublishStats:
    if (path is None) == (benchmark is None):
        raise ValueError("choose exactly one local PATH or --benchmark SLUG")
    if benchmark is None:
        if refresh or limit is not None or instance_ids or cache_dir is not None:
            raise ValueError(
                "--cache-dir, --refresh, --limit and --instance-id require --benchmark"
            )
        assert path is not None
        return await local_benchmark_publish.publish_local_benchmark(path, **publication)
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
    with tempfile.TemporaryDirectory(prefix="loom-adapter-prepare-") as temporary:
        root = Path(temporary)
        prepared = prepare_adapter_benchmark(
            benchmark,
            cache_dir=(cache_dir or default_benchmark_cache()).expanduser(),
            staging_dir=root,
            refresh=refresh,
            limit=limit,
            instance_ids=instance_ids,
        )
        return await local_benchmark_publish.publish_local_benchmark(
            root, prepared_adapter=prepared, **publication
        )
