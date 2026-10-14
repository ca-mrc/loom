"""Pinned legacy loaders must preserve rows without executing remote Python."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import datasets
import huggingface_hub
import pytest
import requests
from huggingface_hub import constants as hub_constants
from loom_benchmarks.adapters.hendrycks_math import HendrycksMATHAdapter
from loom_benchmarks.adapters.livecodebench import LiveCodeBenchAdapter
from loom_benchmarks.fetch import cache_key, fetch_upstream


@pytest.fixture
def source_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Replace only the Hub transport; use real Arrow generation and adapters."""
    files: dict[str, list[dict[str, object]]] = {}
    downloads: list[tuple[str, str, str]] = []

    def download(*, repo_id, filename, revision, repo_type):
        assert repo_type == "dataset"
        assert filename.endswith(".jsonl")
        downloads.append((repo_id, revision, filename))
        path = tmp_path / "upstream" / repo_id / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(row) + "\n" for row in files[filename]))
        return str(path)

    def no_remote_script(*args, **kwargs):
        raise AssertionError("remote dataset scripts must not be loaded")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", download)
    monkeypatch.setattr(datasets, "load_dataset", no_remote_script)
    return files, downloads


def test_math_fetch_and_list_preserve_order_defaults_and_ids(tmp_path, source_files):
    files, downloads = source_files
    subjects = [
        "algebra", "counting_and_probability", "geometry", "intermediate_algebra",
        "number_theory", "prealgebra", "precalculus",
    ]
    for subject in subjects:
        for split in ("train", "test"):
            files[f"data/{subject}_{split}.jsonl"] = [
                {"problem": f"{split} {subject}", "solution": "\\boxed{2}"},
            ]
    files["data/algebra_test.jsonl"] = [
        {"problem": "first", "level": "Level 3", "type": "Algebra", "solution": "\\boxed{1}"},
        {"problem": "second", "level": None, "type": None, "solution": "\\boxed{2}"},
    ]
    files["data/prompts_4shot.jsonl"] = [{"problem": "fewshot", "solution": "answer"}]
    adapter = HendrycksMATHAdapter()
    source = fetch_upstream(adapter.upstream_source, cache_root=tmp_path / "cache")
    assert (source / ".fetch_complete").is_file()
    instances = list(adapter.list_instances(source_dir=source, split="test"))
    assert [i.raw["problem"] for i in instances] == [
        "first", "second", "test counting_and_probability", "test geometry",
        "test intermediate_algebra", "test number_theory", "test prealgebra",
        "test precalculus",
    ]
    assert [i.instance_id for i in instances] == [f"test/{i:05d}" for i in range(8)]
    assert instances[0].tags == {"math_split": "test", "level": "Level 3", "type": "Algebra"}
    assert instances[1].raw["level"] is None
    assert instances[2].raw == {
        "problem": "test counting_and_probability", "level": "0",
        "type": "Unknown", "solution": "\\boxed{2}",
    }
    assert all(repo == "HuggingFaceTB/MATH" and rev == "140a673f1f7182daf7923fdc7108e8cdbf97df46"
               for repo, rev, _ in downloads)
    converted = adapter.convert_instance(instances[0], out_dir=tmp_path / "task")
    assert converted.task_id == "hendrycks-math/test/00000"
    assert json.loads((tmp_path / "task/answer_key.json").read_text())["answer"] == "1"


def test_livecodebench_preserves_all_six_files_and_private_cases(tmp_path, source_files):
    files, downloads = source_files
    for index, filename in enumerate(
        ["test.jsonl", "test2.jsonl", "test3.jsonl", "test4.jsonl", "test5.jsonl", "test6.jsonl"],
    ):
        files[filename] = [{
            "question_title": "echo", "question_content": "Echo the input.",
            "platform": "fixture", "question_id": f"q{index}", "contest_id": index,
            "contest_date": "2026-01-01", "starter_code": "", "difficulty": "easy",
            "public_test_cases": '[{"input":"x","output":"x","testtype":"stdin"}]',
            "private_test_cases": "opaque-private-cases", "metadata": "{}",
        }]
    adapter = LiveCodeBenchAdapter()
    source = fetch_upstream(adapter.upstream_source, cache_root=tmp_path / "cache")
    instances = list(adapter.list_instances(source_dir=source, split="test"))
    assert [i.instance_id for i in instances] == ["q0", "q1", "q2", "q3", "q4", "q5"]
    assert instances[0].raw["contest_id"] == "0"
    assert all(i.raw["private_test_cases"] == "opaque-private-cases" for i in instances)
    assert all(repo == "livecodebench/code_generation_lite" and rev == "0fe84c3912ea0c4d4a78037083943e8f0c4dd505"
               for repo, rev, _ in downloads)
    assert (source / ".fetch_complete").is_file()


@pytest.mark.parametrize("change", [
    {"revision": "main"}, {"locator": "unreviewed/dataset"}, {"subset": "unknown"},
])
def test_unreviewed_script_source_fails_without_completion(tmp_path, source_files, change):
    _, downloads = source_files
    src = replace(HendrycksMATHAdapter.upstream_source, **change)
    with pytest.raises(ValueError, match=r"reviewed.*source"):
        fetch_upstream(src, cache_root=tmp_path)
    assert downloads == []
    assert not (tmp_path / "huggingface" / cache_key(src) / ".fetch_complete").exists()


def test_failed_download_does_not_mark_source_complete(tmp_path, monkeypatch):
    def unavailable(**kwargs):
        raise OSError("fixture transport unavailable")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", unavailable)
    src = LiveCodeBenchAdapter.upstream_source
    with pytest.raises(OSError, match="fixture transport unavailable"):
        fetch_upstream(src, cache_root=tmp_path)
    assert not (tmp_path / "huggingface" / cache_key(src) / ".fetch_complete").exists()


def test_existing_hub_snapshot_is_reused_without_network(tmp_path, monkeypatch):
    src = replace(HendrycksMATHAdapter.upstream_source, subset="algebra")
    hub_cache = tmp_path / "hub"
    snapshot = hub_cache / "datasets--HuggingFaceTB--MATH" / "snapshots" / src.revision
    for filename in ["algebra_train.jsonl", "algebra_test.jsonl", "prompts_4shot.jsonl"]:
        path = snapshot / "data" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"problem": filename, "solution": "\\boxed{3}"}) + "\n")
    monkeypatch.setattr(hub_constants, "HF_HUB_CACHE", str(hub_cache))

    def no_network(*args, **kwargs):
        raise AssertionError("a pinned cached snapshot must not require a network request")

    monkeypatch.setattr(requests.Session, "request", no_network)
    monkeypatch.setattr(HendrycksMATHAdapter, "upstream_source", src)
    source = fetch_upstream(src, cache_root=tmp_path / "loom-source")
    instances = list(HendrycksMATHAdapter().list_instances(source_dir=source, split="test"))
    assert instances[0].instance_id == "test/00000"
    assert instances[0].raw["problem"] == "algebra_test.jsonl"
    assert (source / ".fetch_complete").is_file()
    assert not (source / "datasets--HuggingFaceTB--MATH").exists()
