"""fetch_upstream dispatcher contract — fully offline (Plan 14 Task 4)."""

from __future__ import annotations

import hashlib
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path

import httpx
import pytest
from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.fetch import _looks_like_sha, cache_key, fetch_upstream


def test_cache_key_stable() -> None:
    src = UpstreamSource(
        kind="huggingface", locator="openai_humaneval", revision="abc",
    )
    k1 = cache_key(src)
    k2 = cache_key(src)
    assert k1 == k2
    expected = hashlib.sha256(b"openai_humaneval\x00abc").hexdigest()
    assert k1 == expected


def test_cache_key_includes_subset() -> None:
    src1 = UpstreamSource(kind="huggingface", locator="mmlu", revision="r1")
    src2 = UpstreamSource(
        kind="huggingface", locator="mmlu", revision="r1", subset="abstract_algebra",
    )
    assert cache_key(src1) != cache_key(src2)


def test_fetch_huggingface_reuses_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class _FakeDS:
        @staticmethod
        def load_dataset(
            name: str, subset: str | None = None, revision: str | None = None,
            cache_dir: str | None = None,
        ) -> dict[str, str]:
            calls.append(name)
            assert cache_dir is not None
            (Path(cache_dir) / "DONE").write_text("ok")
            return {"_": "ok"}

    monkeypatch.setattr("loom_benchmarks.fetch.datasets", _FakeDS)
    src = UpstreamSource(
        kind="huggingface", locator="openai_humaneval", revision="r1",
    )
    p1 = fetch_upstream(src, cache_root=tmp_path)
    p2 = fetch_upstream(src, cache_root=tmp_path)
    assert p1 == p2
    assert len(calls) == 1  # second call reused cache
    assert (p1 / "DONE").exists()


def test_fetch_tarball_extracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("hello.txt")
        payload = b"world\n"
        info.size = len(payload)
        tar.addfile(info, BytesIO(payload))
    tar_bytes = buf.getvalue()

    class _R:
        content = tar_bytes
        def raise_for_status(self) -> None:
            return None

    def _fake_get(url: str, **kw: object) -> _R:
        return _R()

    monkeypatch.setattr(httpx, "get", _fake_get)

    src = UpstreamSource(
        kind="https-tarball",
        locator="https://example.com/data.tar.gz",
        revision="v1",
    )
    out = fetch_upstream(src, cache_root=tmp_path)
    assert (out / "hello.txt").read_text() == "world\n"


def test_fetch_refresh_redownloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class _FakeDS:
        @staticmethod
        def load_dataset(
            name: str, subset: str | None = None, revision: str | None = None,
            cache_dir: str | None = None,
        ) -> dict[str, str]:
            calls.append(name)
            assert cache_dir is not None
            (Path(cache_dir) / "OK").write_text("ok")
            return {"_": "ok"}

    monkeypatch.setattr("loom_benchmarks.fetch.datasets", _FakeDS)
    src = UpstreamSource(kind="huggingface", locator="x", revision="r")
    fetch_upstream(src, cache_root=tmp_path)
    fetch_upstream(src, cache_root=tmp_path, refresh=True)
    assert len(calls) == 2


def test_looks_like_sha_recognizes_short_and_full() -> None:
    assert _looks_like_sha("abc1234")
    assert _looks_like_sha("d0d4f3c2c8a5a2e6e3e2f6b3f7e2c5c9c8b9d4e7")
    assert not _looks_like_sha("main")
    assert not _looks_like_sha("v1.0")
    assert not _looks_like_sha("HEAD")
    assert not _looks_like_sha("ABCDEF1")  # uppercase: not a SHA
    assert not _looks_like_sha("xyz1234")


def test_git_fetch_uses_init_fetch_checkout_for_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SHA revision must go through git init + fetch + checkout,
    not `git clone --branch <sha>` which silently fails."""
    import subprocess as _sp
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], **kw: object) -> object:
        calls.append(cmd)

        class _R:
            returncode = 0
        return _R()

    monkeypatch.setattr(_sp, "run", _fake_run)
    monkeypatch.setattr(
        "loom_benchmarks.fetch.subprocess.run", _fake_run,
    )

    src = UpstreamSource(
        kind="git",
        locator="https://example.com/repo.git",
        revision="d0d4f3c2c8a5a2e6e3e2f6b3f7e2c5c9c8b9d4e7",
    )
    fetch_upstream(src, cache_root=tmp_path)
    # 4 calls: init, remote config, fetch, checkout — NOT a single `clone`.
    cmds = [c[0:2] for c in calls]
    assert ["git", "init"] in cmds
    assert ["git", "config", "remote.origin.url", src.locator] in calls
    assert ["git", "fetch"] in cmds
    assert ["git", "checkout"] in cmds
    assert not any(c[:2] == ["git", "clone"] for c in calls)


def test_pinned_git_fetch_recovers_after_failed_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=upstream, check=True)
    rows = upstream / "data.jsonl"
    rows.write_text('{"id":"pinned"}\n')
    subprocess.run(["git", "add", "data.jsonl"], cwd=upstream, check=True)
    commit_cmd = [
        "git", "-c", "user.name=Fetch Test", "-c", "user.email=fetch@example.test",
        "-c", "commit.gpgsign=false", "commit", "-qm",
    ]
    subprocess.run([*commit_cmd, "pinned"], cwd=upstream, check=True)
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=upstream, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    rows.write_text('{"id":"newer-head"}\n')
    subprocess.run(["git", "add", "data.jsonl"], cwd=upstream, check=True)
    subprocess.run([*commit_cmd, "newer"], cwd=upstream, check=True)

    src = UpstreamSource(kind="git", locator=str(upstream), revision=revision)
    cache_root = tmp_path / "cache"
    target = cache_root / "git" / cache_key(src)
    real_run = subprocess.run
    failed_once = False

    def fail_first_fetch(
        cmd: list[str], *, cwd: Path, check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal failed_once
        if cmd[:2] == ["git", "fetch"] and not failed_once:
            failed_once = True
            raise subprocess.CalledProcessError(128, cmd, stderr="fetch interrupted")
        return real_run(cmd, cwd=cwd, check=check)

    monkeypatch.setattr("loom_benchmarks.fetch.subprocess.run", fail_first_fetch)
    with pytest.raises(subprocess.CalledProcessError, match="128"):
        fetch_upstream(src, cache_root=cache_root)
    assert (target / "repo" / ".git").is_dir()
    assert not (target / ".fetch_complete").exists()
    retained = target / "repo" / "retained-local-file"
    retained.write_text("preserve existing cache data")

    result = fetch_upstream(src, cache_root=cache_root)
    assert (result / "repo" / "data.jsonl").read_text() == '{"id":"pinned"}\n'
    assert real_run(
        ["git", "rev-parse", "HEAD"], cwd=result / "repo", check=True,
        capture_output=True, text=True,
    ).stdout.strip() == revision
    assert retained.read_text() == "preserve existing cache data"
    assert (result / ".fetch_complete").read_text() == "ok"

    def reject_refetch(*args: object, **kwargs: object) -> None:
        pytest.fail("a completed source must be reused without invoking Git")

    monkeypatch.setattr("loom_benchmarks.fetch.subprocess.run", reject_refetch)
    assert fetch_upstream(src, cache_root=cache_root) == result


def test_git_fetch_uses_clone_for_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], **kw: object) -> object:
        calls.append(cmd)

        class _R:
            returncode = 0
        return _R()

    monkeypatch.setattr(
        "loom_benchmarks.fetch.subprocess.run", _fake_run,
    )

    src = UpstreamSource(
        kind="git",
        locator="https://example.com/repo.git",
        revision="main",
    )
    fetch_upstream(src, cache_root=tmp_path)
    assert len(calls) == 1
    assert calls[0][:4] == ["git", "clone", "--depth", "1"]
    assert "--branch" in calls[0]
    assert "main" in calls[0]


def test_unknown_kind_rejected(tmp_path: Path) -> None:
    src = UpstreamSource(
        kind="ftp",  # type: ignore[arg-type]
        locator="ftp://nope",
    )
    with pytest.raises(ValueError, match=r"unknown UpstreamSource\.kind"):
        fetch_upstream(src, cache_root=tmp_path)
