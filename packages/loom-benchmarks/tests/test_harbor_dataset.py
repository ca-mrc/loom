"""Offline Harbor package materialization contract."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from pathlib import Path

import pytest
from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.harbor_dataset import (
    HarborDatasetError,
    HarborPublisherDependencyError,
    download_harbor_dataset,
)


def test_harbor_package_source_is_cacheable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Path]] = []

    async def fake_download(source: UpstreamSource, output_dir: Path) -> object:
        calls.append((source.locator, output_dir))
        (output_dir / "harbor-materialization.json").write_text("{}")
        return object()

    monkeypatch.setattr("loom_benchmarks.harbor_dataset.download_harbor_dataset", fake_download)
    source = UpstreamSource(
        kind="harbor-package",
        locator="terminal-bench/terminal-bench-2-1",
        revision="6",
    )

    first = fetch_upstream(source, cache_root=tmp_path)
    second = fetch_upstream(source, cache_root=tmp_path)

    assert first == second
    assert calls == [("terminal-bench/terminal-bench-2-1", first)]
    assert (first / ".fetch_complete").read_text() == "ok"


def test_download_harbor_dataset_records_resolved_package_digests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PackageTaskId:
        def __init__(self, name: str, digest: str) -> None:
            self.org, self.name = name.split("/", 1)
            self.ref = digest

        def get_name(self) -> str:
            return f"{self.org}/{self.name}"

    class Metadata:
        def __init__(self) -> None:
            self.name = "terminal-bench/terminal-bench-2-1"
            self.version = "sha256:" + "d" * 64
            self.task_ids = [
                PackageTaskId("terminal-bench/a", "sha256:" + "a" * 64),
                PackageTaskId("terminal-bench/b", "sha256:" + "b" * 64),
            ]

    class Downloaded:
        def __init__(self, task_id: PackageTaskId, path: Path) -> None:
            self.id = task_id
            self.downloaded_path = path

    metadata_calls: list[str] = []
    download_calls: list[tuple[str, Path, bool]] = []

    class Client:
        def __init__(self) -> None:
            self.metadata = Metadata()

        async def get_dataset_metadata(self, ref: str) -> Metadata:
            metadata_calls.append(ref)
            return self.metadata

        async def download_dataset(
            self, ref: str, *, output_dir: Path, export: bool,
        ) -> list[Downloaded]:
            download_calls.append((ref, output_dir, export))
            result = []
            for task_id in self.metadata.task_ids:
                path = output_dir / task_id.name
                path.mkdir()
                (path / "task.toml").write_text("# native task unchanged\n")
                result.append(Downloaded(task_id, path))
            (output_dir / "metric.py").write_text("# dataset metric unchanged\n")
            return result

    package_module = types.ModuleType("harbor.registry.client.package")
    package_module.PackageDatasetClient = Client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "harbor", types.ModuleType("harbor"))
    monkeypatch.setitem(sys.modules, "harbor.registry", types.ModuleType("harbor.registry"))
    monkeypatch.setitem(sys.modules, "harbor.registry.client", types.ModuleType("harbor.registry.client"))
    monkeypatch.setitem(sys.modules, "harbor.registry.client.package", package_module)

    source = UpstreamSource(
        kind="harbor-package",
        locator="terminal-bench/terminal-bench-2-1",
        revision="6",
    )
    materialization = asyncio.run(download_harbor_dataset(source, tmp_path))

    for name in ("a", "b"):
        assert (tmp_path / "tasks" / name / "task.toml").read_text() == "# native task unchanged\n"
        assert not (tmp_path / name).exists()
    assert (tmp_path / "metric.py").read_text() == "# dataset metric unchanged\n"
    assert materialization.root == tmp_path
    assert materialization.dataset == source.locator
    assert materialization.revision == "6"
    assert materialization.metadata_version == "sha256:" + "d" * 64
    assert materialization.package_digests == {
        "terminal-bench/a": "sha256:" + "a" * 64,
        "terminal-bench/b": "sha256:" + "b" * 64,
    }
    assert metadata_calls == ["terminal-bench/terminal-bench-2-1@6"]
    assert download_calls == [("terminal-bench/terminal-bench-2-1@6", tmp_path, True)]
    assert json.loads((tmp_path / "harbor-materialization.json").read_text()) == {
        "dataset": "terminal-bench/terminal-bench-2-1",
        "metadata_version": "sha256:" + "d" * 64,
        "package_digests": materialization.package_digests,
        "revision": "6",
        "schema_version": 1,
    }


def test_download_harbor_dataset_rejects_missing_metadata_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Metadata:
        def __init__(self) -> None:
            self.name = "terminal-bench/terminal-bench-2-1"
            self.version = None
            self.task_ids: list[object] = []

    class Client:
        async def get_dataset_metadata(self, ref: str) -> Metadata:
            assert ref == "terminal-bench/terminal-bench-2-1@6"
            return Metadata()

        async def download_dataset(self, *args: object, **kwargs: object) -> list[object]:
            return []

    package_module = types.ModuleType("harbor.registry.client.package")
    package_module.PackageDatasetClient = Client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "harbor", types.ModuleType("harbor"))
    monkeypatch.setitem(sys.modules, "harbor.registry", types.ModuleType("harbor.registry"))
    monkeypatch.setitem(sys.modules, "harbor.registry.client", types.ModuleType("harbor.registry.client"))
    monkeypatch.setitem(sys.modules, "harbor.registry.client.package", package_module)

    source = UpstreamSource(
        kind="harbor-package",
        locator="terminal-bench/terminal-bench-2-1",
        revision="6",
    )
    with pytest.raises(HarborDatasetError, match="metadata version"):
        asyncio.run(download_harbor_dataset(source, tmp_path))


def test_missing_harbor_publisher_dependency_is_actionable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force the dependency boundary even when Harbor is installed for operator smoke.
    monkeypatch.setitem(sys.modules, "harbor.registry.client.package", None)
    monkeypatch.delitem(sys.modules, "harbor.registry.client", raising=False)
    monkeypatch.delitem(sys.modules, "harbor.registry", raising=False)
    monkeypatch.delitem(sys.modules, "harbor", raising=False)
    source = UpstreamSource(
        kind="harbor-package",
        locator="terminal-bench/terminal-bench-2-1",
        revision="6",
    )

    with pytest.raises(HarborPublisherDependencyError, match="pinned Harbor catalog-publisher dependency"):
        asyncio.run(download_harbor_dataset(source, tmp_path))
