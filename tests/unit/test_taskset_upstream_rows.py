"""Unit tests for upstream row iterators (#242 sub-plan 3)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.models.taskset import TaskSetSource
from loom.taskset.upstream_rows import UpstreamFetchError, iter_upstream_rows


def test_jsonl_inline_yields_rows() -> None:
    source = TaskSetSource(
        type="jsonl-inline",
        locator='{"id":"1","question":"q1"}\n{"id":"2","question":"q2"}\n',
    )
    rows = list(iter_upstream_rows(source, cache_root=Path("/tmp")))
    assert len(rows) == 2
    assert rows[0]["id"] == "1"


@pytest.mark.parametrize("trailing_newline", ["", "\n"])
def test_jsonl_preserves_newlines_inside_strings(tmp_path: Path, trailing_newline: str) -> None:
    row = {"id": "1", "question": "first line\nsecond line", "literal": r"keep\n"}
    source = TaskSetSource(type="jsonl-inline", locator=json.dumps(row) + trailing_newline)
    assert list(iter_upstream_rows(source, cache_root=tmp_path)) == [row]


def test_jsonl_rejects_escaped_record_delimiters(tmp_path: Path) -> None:
    source = TaskSetSource(type="jsonl-inline", locator=r'{"id":"1"}\n{"id":"2"}')
    with pytest.raises(UpstreamFetchError, match="invalid json on line 1"):
        list(iter_upstream_rows(source, cache_root=tmp_path))
