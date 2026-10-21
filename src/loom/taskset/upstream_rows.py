"""Upstream row iterators for TaskSet materialization (#242 sub-plan 3)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import httpx

from loom.models.taskset import TaskSetSource


class UpstreamFetchError(Exception):
    """Wraps transport/parse failures when resolving upstream rows."""


def _parse_jsonl_text(text: str) -> Iterator[dict[str, Any]]:
    # JSONL records use physical newlines; JSON escapes and Unicode string
    # separators belong to the value and must reach the JSON parser intact.
    for line_no, line in enumerate(text.split("\n"), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise UpstreamFetchError(
                f"invalid json on line {line_no}: {exc}",
            ) from exc
        if not isinstance(parsed, dict):
            raise UpstreamFetchError(
                f"jsonl line {line_no} must be an object, got {type(parsed).__name__}",
            )
        yield parsed


def _find_git_jsonl(source_dir: Path) -> Path:
    root_candidate = source_dir / "repo" / "data.jsonl"
    if root_candidate.is_file():
        return root_candidate
    flat_candidate = source_dir / "data.jsonl"
    if flat_candidate.is_file():
        return flat_candidate
    matches = sorted(source_dir.rglob("*.jsonl"))
    if not matches:
        raise UpstreamFetchError("no jsonl file found in git upstream")
    return matches[0]


def _iter_hf_rows(
    source: TaskSetSource,
    *,
    cache_root: Path,
) -> Iterator[dict[str, Any]]:
    try:
        import datasets
        from loom_benchmarks.base import UpstreamSource
        from loom_benchmarks.fetch import fetch_upstream
    except ImportError as exc:
        raise UpstreamFetchError(
            "hf source requires loom-benchmarks and datasets packages",
        ) from exc

    upstream = UpstreamSource(
        kind="huggingface",
        locator=source.locator,
        revision=source.revision,
        subset=source.subset,
    )
    try:
        fetched = fetch_upstream(upstream, cache_root=cache_root)
    except Exception as exc:
        raise UpstreamFetchError(str(exc)) from exc

    kwargs: dict[str, Any] = {
        "revision": source.revision,
        "cache_dir": str(fetched),
    }
    ds = datasets.load_dataset(source.locator, source.subset, **kwargs)
    split_name = source.split
    if split_name is None:
        split_name = next(iter(ds.keys()))
    if split_name not in ds:
        raise UpstreamFetchError(f"split {split_name!r} not found in dataset")
    for row in ds[split_name]:
        yield cast(dict[str, Any], dict(row))


def _iter_git_rows(
    source: TaskSetSource,
    *,
    cache_root: Path,
) -> Iterator[dict[str, Any]]:
    try:
        from loom_benchmarks.base import UpstreamSource
        from loom_benchmarks.fetch import fetch_upstream
    except ImportError as exc:
        raise UpstreamFetchError(
            "git source requires loom-benchmarks package",
        ) from exc

    upstream = UpstreamSource(
        kind="git",
        locator=source.locator,
        revision=source.revision,
        subset=source.subset,
    )
    try:
        fetched = fetch_upstream(upstream, cache_root=cache_root)
    except Exception as exc:
        raise UpstreamFetchError(str(exc)) from exc

    jsonl_path = _find_git_jsonl(fetched)
    yield from _parse_jsonl_text(jsonl_path.read_text(encoding="utf-8"))


def iter_upstream_rows(
    source: TaskSetSource,
    *,
    cache_root: Path,
) -> Iterator[dict[str, Any]]:
    """Yield upstream row dicts for a manifest ``source`` block."""
    if source.type == "jsonl-inline":
        yield from _parse_jsonl_text(source.locator)
        return

    if source.type == "https":
        try:
            resp = httpx.get(source.locator, timeout=120.0, follow_redirects=True)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise UpstreamFetchError(str(exc)) from exc
        yield from _parse_jsonl_text(resp.text)
        return

    if source.type == "git":
        yield from _iter_git_rows(source, cache_root=cache_root)
        return

    if source.type == "hf":
        yield from _iter_hf_rows(source, cache_root=cache_root)
        return

    raise UpstreamFetchError(f"unsupported source type: {source.type}")
