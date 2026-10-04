"""Application source identity must bind authored bytes, modes and safe links."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest
import rfc8785


def entry(path="src/api.py", content=b"print('local')\n", *, mode="0644", link_target=None):
    return dict(path=path, size_bytes=len(content), sha256="sha256:" + hashlib.sha256(content).hexdigest(),
                mode=mode, link_target=link_target)


def manifest(*files):
    from loom.application_source import ApplicationSourceManifestV1

    return ApplicationSourceManifestV1.model_validate({"files": files})


def test_source_identity_binds_content_and_executable_mode_not_a_git_commit():
    from loom.application_source import parse_application_source_manifest

    source = manifest(entry())
    expected = rfc8785.dumps({"schema_version": "loom.application-source.v1", "files": [entry()]})
    assert source.canonical_bytes() == expected
    assert source.digest == "sha256:" + hashlib.sha256(expected).hexdigest()
    assert parse_application_source_manifest(expected, expected_digest=source.digest) == source
    assert manifest(entry(content=b"changed")).digest != source.digest
    assert manifest(entry(mode="0755")).digest != source.digest
    assert manifest(entry(content=b"")).files[0].size_bytes == 0


@pytest.mark.parametrize("path", ["/absolute", "../escape", "a/../b", "a//b", "./a", "a/", "a\\b", "a\x00b",
                                      "a\nb", "a/" * 64 + "b", "é" * 513])
def test_source_rejects_noncanonical_or_unbounded_paths(path):
    with pytest.raises(ValueError):
        manifest(entry(path))


@pytest.mark.parametrize("files", [[], [entry("z"), entry("a")], [entry("a"), entry("a")],
                                    [entry("a"), entry("a/b")],
                                    [entry("a"), entry("a-"), entry("a/b")]])
def test_source_inventory_is_sorted_unique_and_has_no_file_directory_collision(files):
    with pytest.raises(ValueError):
        manifest(*files)


@pytest.mark.parametrize("field,value", [("size_bytes", True), ("size_bytes", "1"), ("size_bytes", -1),
                                           ("size_bytes", 512 * 1024**2 + 1), ("mode", "0777"),
                                           ("mode", "0600"), ("sha256", "sha256:" + "x" * 64),
                                           ("link_target", "file")])
def test_source_entry_rejects_coercions_and_invalid_metadata(field, value):
    with pytest.raises(ValueError):
        manifest(entry() | {field: value})


def test_source_total_bytes_and_entry_count_are_bounded():
    with pytest.raises(ValueError):
        manifest(entry("a") | {"size_bytes": 300 * 1024**2}, entry("b") | {"size_bytes": 300 * 1024**2})
    with pytest.raises(ValueError):
        manifest(*(entry(f"file-{index:05}", b"") for index in range(25_001)))


def test_deep_source_manifest_validation_does_not_expand_every_ancestor():
    import tracemalloc

    from loom.application_source import parse_application_source_manifest

    files = [entry(f"r{index:05}/" + "aa/" * 60 + "a/" * 2 + "f", b"")
             for index in range(1000)]
    body = rfc8785.dumps({"schema_version": "loom.application-source.v1", "files": files})
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    tracemalloc.start()
    try:
        source = parse_application_source_manifest(body, expected_digest=digest)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert len(source.files) == 1000
    assert source.files[-1].path == files[-1]["path"]
    # This is a ~335KiB, zero-content upload. Expanding all 63,000 ancestors
    # takes tens of MiB before extraction and multiplies across active owners.
    # Keep generous headroom for the parser/model/canonicalization allocations.
    assert peak < 8 * 1024**2


def test_relative_file_and_directory_links_bind_target_bytes_without_dereference():
    source = manifest(entry("alias", b"src", mode="0777", link_target="src"),
                      entry("forward", b"alias/api.py", mode="0777", link_target="alias/api.py"), entry())
    assert source.files[0].link_target == "src"
    assert source.files[1].sha256 == "sha256:" + hashlib.sha256(b"alias/api.py").hexdigest()
    manifest(entry("src/link", b"../target", mode="0777", link_target="../target"), entry("target"))


@pytest.mark.parametrize("target", ["/etc/passwd", "../outside", "missing", "a", "b/../../outside", "bad\\target", "bad\ntarget"])
def test_links_cannot_escape_dangle_or_cycle(target):
    with pytest.raises(ValueError):
        manifest(entry("a", target.encode(), mode="0777", link_target=target), entry("b", b"a", mode="0777", link_target="a"))


def test_link_metadata_must_match_target_and_cannot_contain_descendants():
    with pytest.raises(ValueError):
        manifest(entry("a", b"b", mode="0777", link_target="b") | {"sha256": "sha256:" + "0" * 64}, entry("b"))
    with pytest.raises(ValueError):
        manifest(entry("a", b"b", mode="0777", link_target="b"), entry("a/file"), entry("b"))


@pytest.mark.parametrize("change", ["whitespace", "duplicate", "digest", "oversize"])
def test_parser_requires_exact_bounded_canonical_document(change):
    from loom.application_source import parse_application_source_manifest

    source = manifest(entry())
    body = source.canonical_bytes()
    if change == "whitespace":
        body += b"\n"
    elif change == "duplicate":
        body = body.replace(b'{"files":', b'{"schema_version":"wrong","files":', 1)
    elif change == "oversize":
        body = b" " * (8 * 1024**2 + 1)
    digest = "sha256:" + hashlib.sha256(body).hexdigest() if change != "digest" else "sha256:" + "0" * 64
    with pytest.raises(ValueError):
        parse_application_source_manifest(body, expected_digest=digest)


def test_verified_staged_read_returns_exact_declared_bytes_and_link(tmp_path):
    from loom.application_source import read_application_source_file

    (tmp_path / "src").mkdir()
    (tmp_path / "src/api.py").write_bytes(b"print('local')\n")
    (tmp_path / "alias").symlink_to("src/api.py")
    source = manifest(entry("alias", b"src/api.py", mode="0777", link_target="src/api.py"), entry())
    assert read_application_source_file(tmp_path, source.files[0]) == b"src/api.py"
    assert read_application_source_file(tmp_path, source.files[1]) == b"print('local')\n"


@pytest.mark.parametrize("change", ["bytes", "mode", "file-link", "parent-link", "hardlink", "fifo"])
def test_verified_staged_read_rejects_changed_or_unsafe_content(tmp_path, change):
    from loom.application_source import read_application_source_file

    directory = tmp_path / "src"
    directory.mkdir()
    path = directory / "api.py"
    path.write_bytes(b"print('local')\n")
    source = manifest(entry())
    if change == "bytes":
        path.write_bytes(b"print('other')\n")
    elif change == "mode":
        path.chmod(0o755)
    elif change == "parent-link":
        directory.rename(tmp_path / "actual")
        directory.symlink_to("actual", target_is_directory=True)
    elif change == "hardlink":
        os.link(path, tmp_path / "another")
    else:
        path.unlink()
        if change == "fifo":
            os.mkfifo(path)
        else:
            path.symlink_to(Path(__file__))
    with pytest.raises(ValueError):
        read_application_source_file(tmp_path, source.files[0])


def test_verified_read_rejects_nonowned_directory(tmp_path, monkeypatch):
    from loom.application_source import read_application_source_file

    (tmp_path / "a").write_text("a")
    source = manifest(entry("a", b"a"))
    other_uid = os.getuid() + 1
    monkeypatch.setattr(os, "getuid", lambda: other_uid)
    with pytest.raises(ValueError):
        read_application_source_file(tmp_path, source.files[0])


def test_verified_read_detects_in_place_change_during_open_descriptor_read(tmp_path, monkeypatch):
    from loom.application_source import read_application_source_file

    path = tmp_path / "a"
    path.write_bytes(b"original")
    source = manifest(entry("a", b"original"))
    original = os.read
    changed = False

    def race(descriptor, count):
        nonlocal changed
        body = original(descriptor, count)
        if body and not changed:
            changed = True
            path.write_bytes(b"modified")
        return body

    monkeypatch.setattr(os, "read", race)
    with pytest.raises(ValueError):
        read_application_source_file(tmp_path, source.files[0])
