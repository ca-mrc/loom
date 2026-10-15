"""Kind-dispatched upstream fetchers (benchmark integrations spec §4).

Four transports cover every adapter in the v1 slate:

- `huggingface` — uses `datasets.load_dataset` and the HF cache_dir.
- `git` — shallow clone (`--depth 1`) into `<cache>/git/<hash>/repo`.
- `https-tarball` — single GET + extract into `<cache>/https-tarball/<hash>/`.
- `harbor-package` — resolve a pinned Harbor Hub dataset into
  `<cache>/harbor-package/<hash>/`.

The cache key is `sha256(locator || "\\0" || (revision or "") [|| "\\0" || subset])`.
Idempotent: a sentinel file `.fetch_complete` short-circuits redundant
downloads. `refresh=True` wipes the cache for one source.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import shutil
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path

import datasets  # type: ignore[import-untyped]
import httpx

from loom_benchmarks.base import UpstreamSource

# `git clone --branch <ref>` accepts tags + branches but NOT raw commit
# SHAs. When the revision looks like a full or short SHA we fall back to
# `git init && git fetch <sha> && git checkout FETCH_HEAD` so adapters
# that pin by content-addressed commit (the spec's recommended default)
# don't silently end up cloning HEAD instead.
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")


def _looks_like_sha(rev: str) -> bool:
    return bool(_SHA_RE.match(rev))


def cache_key(src: UpstreamSource) -> str:
    h = hashlib.sha256()
    h.update(src.locator.encode("utf-8"))
    h.update(b"\x00")
    h.update((src.revision or "").encode("utf-8"))
    if src.subset:
        h.update(b"\x00")
        h.update(src.subset.encode("utf-8"))
    return h.hexdigest()


def fetch_upstream(
    src: UpstreamSource, *, cache_root: Path, refresh: bool = False,
) -> Path:
    """Resolve a cached path containing the upstream data. Idempotent."""
    target = cache_root / src.kind / cache_key(src)
    sentinel = target / ".fetch_complete"
    if refresh and target.exists():
        shutil.rmtree(target)
    if sentinel.exists():
        return target
    target.mkdir(parents=True, exist_ok=True)
    if src.kind == "huggingface":
        if src.trust_remote_code:
            from loom_benchmarks.scripted_huggingface import load_scripted_source

            load_scripted_source(src, cache_dir=target)
        else:
            datasets.load_dataset(
                src.locator, src.subset, revision=src.revision, cache_dir=str(target),
            )
    elif src.kind == "git":
        repo_dir = target / "repo"
        if src.revision and _looks_like_sha(src.revision):
            # SHA pin: init → fetch the exact object → checkout.
            repo_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "init", "-q"], cwd=repo_dir, check=True)
            subprocess.run(
                ["git", "remote", "add", "origin", src.locator],
                cwd=repo_dir, check=True,
            )
            subprocess.run(
                ["git", "fetch", "--depth", "1", "origin", src.revision],
                cwd=repo_dir, check=True,
            )
            subprocess.run(
                ["git", "checkout", "-q", "FETCH_HEAD"],
                cwd=repo_dir, check=True,
            )
        else:
            cmd = ["git", "clone", "--depth", "1"]
            if src.revision:
                cmd += ["--branch", src.revision]
            cmd += [src.locator, str(repo_dir)]
            subprocess.run(cmd, check=True)
        _materialize_git_lfs_pointers(repo_dir)
    elif src.kind == "https-tarball":
        resp = httpx.get(src.locator, timeout=120.0, follow_redirects=True)
        resp.raise_for_status()
        with tarfile.open(fileobj=BytesIO(resp.content), mode="r:*") as tar:
            # filter="data" rejects unsafe features (absolute paths,
            # device nodes, parent traversal). The Python 3.14 default
            # but we set it explicitly so 3.12/3.13 also get it.
            tar.extractall(target, filter="data")
    elif src.kind == "harbor-package":
        # Harbor is catalog-publisher tooling, not a routine developer
        # dependency. Keep the import and its optional dependency check on
        # this explicit transport path.
        from loom_benchmarks.harbor_dataset import download_harbor_dataset

        asyncio.run(download_harbor_dataset(src, target))
    else:
        raise ValueError(f"unknown UpstreamSource.kind: {src.kind}")
    sentinel.write_text("ok")
    return target


_LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1"


def _materialize_git_lfs_pointers(repo_dir: Path) -> None:
    """Replace git-lfs pointer files under `repo_dir` with their real blobs.

    Git's default `clone` does not fetch LFS blobs — a repo that uses LFS
    for large binary assets (skillflow / skillflow-iterative ship .pptx /
    .xlsx / .pdf / .tsv via LFS) ends up with 130-byte pointer files
    on disk instead of the real content. Downstream `publish` then
    uploads those pointers to HuggingFace, which rejects the commit with
    "LFS pointer pointed to a file that does not exist" because the
    pointed-at blobs were never uploaded.

    Detect LFS pointers post-clone by their spec-v1 header and delegate
    to `git-lfs pull`. Skip cleanly if either git-lfs isn't installed
    or the repo has no LFS content — both cases print a short warning
    and leave the clone as-is.
    """
    has_pointer = False
    for path in repo_dir.rglob("*"):
        if not path.is_file() or path.stat().st_size > 4096:
            continue
        try:
            if path.read_bytes(
            ).startswith(_LFS_POINTER_PREFIX):
                has_pointer = True
                break
        except OSError:
            continue
    if not has_pointer:
        return
    try:
        subprocess.run(
            ["git", "lfs", "install", "--local"],
            cwd=repo_dir, check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # git-lfs binary not installed on the host, or `git-lfs install`
        # rejected the local repo. Fall back to leaving the pointers in
        # place; downstream will surface a clearer LFS-specific error
        # message than we could produce here.
        import warnings
        warnings.warn(
            f"fetch_upstream: {repo_dir} contains git-lfs pointer files "
            "but `git lfs install` failed (is git-lfs installed?). "
            "Downstream publish/import may fail with LFS-related errors.",
            stacklevel=3,
        )
        return
    subprocess.run(["git", "lfs", "pull"], cwd=repo_dir, check=True)
