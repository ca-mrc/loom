"""Real Git captures bind current bytes without leaking owner state or racing."""
from __future__ import annotations

import hashlib
import os
import subprocess

import pytest


def git(root, *args):
    return subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "-C", str(root), *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.name", "Source fixture")
    git(root, "config", "user.email", "source@example.invalid")
    return root


def capture(root):
    from loom_cli.application_source import capture_application_source

    return capture_application_source(root)


def commit(root):
    git(root, "add", ".")
    git(root, "commit", "-qm", "fixture")
    return git(root, "rev-parse", "HEAD")


def test_current_bytes_include_dirty_untracked_and_deletions_without_changing_checkout(repo):
    (repo / "api.py").write_bytes(b"committed\n")
    (repo / "deleted").write_bytes(b"old\n")
    head = commit(repo)
    (repo / "api.py").write_bytes(b"local edit\n")
    (repo / "deleted").unlink()
    (repo / "new file").write_bytes(b"untracked\n")
    before = git(repo, "status", "--porcelain=v1")
    index = (repo / ".git/index").read_bytes()
    with capture(repo) as source:
        assert source.base_commit == head
        assert source.root != repo and not source.root.is_relative_to(repo)
        assert source.root.stat().st_mode & 0o777 == 0o700
        assert [f.path for f in source.manifest.files] == ["api.py", "new file"]
        assert (source.root / "api.py").read_bytes() == b"local edit\n"
        assert (source.root / "new file").read_bytes() == b"untracked\n"
        assert source.manifest.files[0].sha256 == "sha256:" + hashlib.sha256(b"local edit\n").hexdigest()
        saved = source.root
    assert not saved.exists()
    assert git(repo, "status", "--porcelain=v1") == before
    assert (repo / ".git/index").read_bytes() == index
    assert (repo / "api.py").read_bytes() == b"local edit\n"


def test_git_ignore_selects_untracked_only_and_dockerignore_is_authored_source(repo):
    (repo / "tracked.cache").write_text("tracked")
    commit(repo)
    (repo / ".gitignore").write_text("*.cache\n")
    (repo / ".dockerignore").write_text("private-build-dir\n")
    (repo / "ignored.cache").write_text("must not copy")
    with capture(repo) as source:
        assert [f.path for f in source.manifest.files] == [".dockerignore", ".gitignore", "tracked.cache"]
        assert (source.root / ".dockerignore").read_text() == "private-build-dir\n"


@pytest.mark.parametrize("name", [
    ".loom/state", ".codex/data", ".claude/data", ".worktrees/data", "worktrees/data", ".venv/data",
    "AGENTS.md", "src/MEMORY.md", "NEW_SESSION_BRIEFING.md", ".env", "deploy/.env.secret",
    "deploy/private.pem", "auth/private.key", "auth/id_rsa", "auth/id_ed25519",
])
def test_mandatory_exclusions_apply_even_to_tracked_files(repo, name):
    private = repo / name
    private.parent.mkdir(parents=True, exist_ok=True)
    private.write_text("fixture private content")
    (repo / "app.py").write_text("authored")
    git(repo, "add", "-f", ".")
    with capture(repo) as source:
        assert [f.path for f in source.manifest.files] == ["app.py"]


def test_tracked_deploy_env_symlink_is_excluded_without_following_it(repo, tmp_path):
    private = tmp_path / "private"
    private.write_text("fixture private content")
    private.chmod(0)
    (repo / "deploy").mkdir()
    (repo / "deploy/.env").symlink_to(private)
    (repo / ".env.example").write_text("PUBLIC_EXAMPLE=1\n")
    git(repo, "add", "-f", ".")
    with capture(repo) as source:
        assert [f.path for f in source.manifest.files] == [".env.example"]


def test_unborn_and_linked_worktrees_preserve_modes_and_safe_links(repo, tmp_path):
    (repo / "src").mkdir()
    (repo / "src/run").write_bytes(b"#!/bin/sh\n")
    (repo / "src/run").chmod(0o755)
    (repo / "alias").symlink_to("src")
    with capture(repo) as source:
        assert source.base_commit is None
        assert [(f.path, f.mode) for f in source.manifest.files] == [("alias", "0777"), ("src/run", "0755")]
        assert os.readlink(source.root / "alias") == "src"
        assert (source.root / "src/run").stat().st_mode & 0o777 == 0o755
    head = commit(repo)
    linked = tmp_path / "linked"
    git(repo, "worktree", "add", "--detach", str(linked), head)
    (linked / "src/run").write_bytes(b"dirty worktree\n")
    with capture(linked) as source:
        assert source.base_commit == head
        assert (source.root / "src/run").read_bytes() == b"dirty worktree\n"
        assert ".git" not in [f.path for f in source.manifest.files]
    assert (repo / "src/run").read_bytes() == b"#!/bin/sh\n"


def test_ambient_git_overrides_and_fsmonitor_cannot_select_another_tree_or_execute(repo, tmp_path, monkeypatch):
    other = tmp_path / "other"
    other.mkdir()
    git(other, "init", "-q")
    (other / "wrong").write_text("wrong")
    (repo / "app").write_text("right")
    commit(repo)
    marker = tmp_path / "executed"
    hook = repo / ".git/monitor"
    hook.write_text(f"#!/bin/sh\ntouch '{marker}'\n")
    hook.chmod(0o755)
    git(repo, "config", "core.fsmonitor", str(hook))
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))
    monkeypatch.setenv("GIT_INDEX_FILE", str(other / "elsewhere"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(hook))
    with capture(repo) as source:
        assert [f.path for f in source.manifest.files] == ["app"]
        assert (source.root / "app").read_text() == "right"
    assert not marker.exists()


@pytest.mark.parametrize("target", ["../outside", "/etc/passwd", "missing", ".env", "alias"])
def test_unsafe_or_excluded_link_targets_fail_before_a_snapshot_is_yielded(repo, target):
    (repo / "app").write_text("app")
    (repo / ".env").write_text("fixture private content")
    (repo / "alias").symlink_to(target)
    with pytest.raises(ValueError):
        with capture(repo):
            pytest.fail("unsafe snapshot yielded")


@pytest.mark.parametrize("kind", ["hardlink", "fifo", "parent-link", "submodule", "unmerged"])
def test_unsupported_files_and_index_states_are_not_silently_omitted(repo, tmp_path, kind):
    (repo / "src").mkdir()
    path = repo / "src/app"
    path.write_text("app")
    head = commit(repo)
    if kind == "hardlink":
        os.link(path, tmp_path / "other-link")
    elif kind == "fifo":
        path.unlink()
        os.mkfifo(path)
    elif kind == "parent-link":
        (repo / "src").rename(tmp_path / "outside")
        (repo / "src").symlink_to(tmp_path / "outside")
    elif kind == "submodule":
        git(repo, "update-index", "--add", "--cacheinfo", f"160000,{head},sub")
    else:
        blob = git(repo, "rev-parse", "HEAD:src/app")
        subprocess.run(["git", "-C", str(repo), "update-index", "--index-info"], check=True,
                       input=f"0 {'0' * 40}\tsrc/app\n100644 {blob} 1\tsrc/app\n".encode())
    with pytest.raises(ValueError):
        with capture(repo):
            pytest.fail("unsupported source yielded")


@pytest.mark.parametrize("change", ["content", "replacement", "inventory", "parent", "root"])
def test_included_files_parents_and_inventory_are_rechecked_after_copy(repo, monkeypatch, change):
    from loom_cli import application_source as module

    (repo / "src").mkdir()
    path = repo / "src/app"
    path.write_text("original")
    original = module.capture_application_source_file
    changed = False

    def race(*args, **kwargs):
        nonlocal changed
        result = original(*args, **kwargs)
        if not changed:
            changed = True
            if change == "content":
                path.write_text("modified")
            elif change == "replacement":
                path.unlink()
                path.write_text("original")
            elif change == "inventory":
                (repo / "new").write_text("new")
            elif change == "parent":
                (repo / "src").rename(repo / "old")
                (repo / "src").mkdir()
                path.write_text("original")
            else:
                repo.rename(repo.with_name("old-root"))
                repo.mkdir()
        return result

    monkeypatch.setattr(module, "capture_application_source_file", race)
    with pytest.raises(ValueError):
        with capture(repo):
            pytest.fail("racy snapshot yielded")


@pytest.mark.parametrize("kind", ["nonrepo", "subdirectory", "empty"])
def test_capture_requires_a_complete_nonempty_worktree_root(repo, tmp_path, kind):
    target = repo
    if kind == "nonrepo":
        target = tmp_path
    elif kind == "subdirectory":
        target = repo / "sub"
        target.mkdir()
        (target / "app").write_text("app")
    with pytest.raises(ValueError):
        with capture(target):
            pytest.fail("invalid root yielded")


def test_snapshot_cleanup_on_consumer_error_and_temp_root_inside_checkout(repo, monkeypatch):
    import tempfile

    (repo / "app").write_text("app")
    monkeypatch.setattr(tempfile, "tempdir", str(repo))
    with pytest.raises(RuntimeError, match="consumer"):
        with capture(repo) as source:
            saved = source.root
            assert not saved.is_relative_to(repo)
            raise RuntimeError("consumer")
    assert not saved.exists()
    assert (repo / "app").read_text() == "app"
