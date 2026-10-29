from __future__ import annotations

import json
import tarfile
from pathlib import Path

import pytest

from loom.agent.terminus2 import native_artifacts as artifacts


def _ref(name: str, session: str) -> dict[str, str]:
    return {"trajectory_path": name, "session_id": session}


def _write(root: Path, name: str, session: str, refs: list[dict[str, str]] | None = None) -> bytes:
    body = json.dumps({
        "session_id": session,
        "steps": [{"observation": {"results": [{"subagent_trajectory_ref": refs or []}]}}],
    }).encode()
    (root / name).write_bytes(body)
    return body


def _members(path: Path) -> dict[str, bytes]:
    with tarfile.open(path) as archive:
        assert all(member.isfile() for member in archive)
        return {member.name: archive.extractfile(member).read() for member in archive}


def test_recursive_closure_preserves_native_bytes_and_replaces_stale_archive(tmp_path: Path) -> None:
    first = "trajectory.summarization-1-summary.json"
    second = "trajectory.summarization-1-questions.json"
    _write(tmp_path, "trajectory.json", "main", [_ref(first, "one")])
    original = _write(tmp_path, first, "one", [_ref(second, "two")])
    other = _write(tmp_path, second, "two", [_ref(first, "one")])
    (tmp_path / artifacts.HARBOR_SUBAGENT_ARCHIVE).write_bytes(b"old stale archive")
    called: list[str] = []
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: called.append(p.name))
    assert path == tmp_path / artifacts.HARBOR_SUBAGENT_ARCHIVE
    assert _members(path) == {first: original, second: other}
    assert sorted(called) == sorted([first, second])
    _write(tmp_path, "trajectory.json", "main")
    assert artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None) is None
    assert not path.exists()


def test_invalid_children_produce_diagnostics_and_keep_valid_file(tmp_path: Path) -> None:
    names = [f"trajectory.summarization-{i}-summary.json" for i in range(1, 7)]
    refs = [_ref(name, str(i)) for i, name in enumerate(names)]
    refs.append(_ref(names[3], "conflicting"))
    _write(tmp_path, "trajectory.json", "main", refs)
    valid = _write(tmp_path, names[0], "0")
    # The next file is absent, matching Harbor's swallowed open failure.
    (tmp_path / names[2]).write_bytes(b"")
    _write(tmp_path, names[3], "3")
    _write(tmp_path, names[4], "different-session")
    (tmp_path / names[5]).write_text('{"session_id":"5","steps":{}}')
    result = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None)
    files = _members(result)
    assert files[names[0]] == valid
    issues = json.loads(files.pop(artifacts.HARBOR_SUBAGENT_ISSUES))["issues"]
    assert set(files) == {names[0]}
    assert {issue["reason"] for issue in issues} == {
        "missing", "invalid_json", "conflicting_session", "session_mismatch", "invalid_trajectory",
    }


def test_unsafe_paths_and_symlinks_are_never_read_or_sanitized(tmp_path: Path) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text("private material")
    linked = "trajectory.summarization-1-summary.json"
    (tmp_path / linked).symlink_to(outside)
    refs = [_ref(name, "child") for name in (linked, "../outside.json", str(outside), "a\\b.json")]
    _write(tmp_path, "trajectory.json", "main", refs)
    called: list[Path] = []
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=called.append)
    files = _members(path)
    assert set(files) == {artifacts.HARBOR_SUBAGENT_ISSUES}
    assert not called
    assert outside.read_text() == "private material"
    assert {issue["reason"] for issue in json.loads(files[artifacts.HARBOR_SUBAGENT_ISSUES])["issues"]} == {
        "unsafe_file", "unsafe_path",
    }


def test_sanitizer_runs_before_pack_and_credential_failure_removes_stale_archive(tmp_path: Path) -> None:
    name = "trajectory.summarization-1-summary.json"
    _write(tmp_path, "trajectory.json", "main", [_ref(name, "child")])
    _write(tmp_path, name, "child")
    with (tmp_path / name).open("w") as stream:
        json.dump({"session_id": "child", "steps": [], "api_key": "secret"}, stream)

    def sanitize(path: Path) -> None:
        path.write_text(path.read_text().replace("secret", "[REDACTED]"))

    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=sanitize)
    assert b"secret" not in _members(path)[name]

    def block(path: Path) -> None:
        raise RuntimeError("credential material remains")

    with pytest.raises(RuntimeError, match="credential material remains"):
        artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=block)
    assert not path.exists()


def test_unreferenced_children_do_not_survive_lifecycle(tmp_path: Path) -> None:
    stale = tmp_path / artifacts.HARBOR_SUBAGENT_ARCHIVE
    stale.write_bytes(b"old")
    _write(tmp_path, "trajectory.summarization-1-summary.json", "orphan")
    assert artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None) is None
    assert not stale.exists()


def test_three_native_phases_are_preserved_without_packaging_unreferenced_files(tmp_path: Path) -> None:
    expected = {}
    refs = []
    for phase in ("summary", "questions", "answers"):
        name = f"trajectory.summarization-1-{phase}.json"
        refs.append(_ref(name, phase))
        expected[name] = _write(tmp_path, name, phase)
    _write(tmp_path, "trajectory.json", "main", refs)
    _write(tmp_path, "trajectory.summarization-2-summary.json", "orphan")
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None)
    assert _members(path) == expected


def test_late_conflict_excludes_unreachable_descendants(tmp_path: Path) -> None:
    first = "trajectory.summarization-1-summary.json"
    second = "trajectory.summarization-1-questions.json"
    third = "trajectory.summarization-1-answers.json"
    _write(tmp_path, "trajectory.json", "main", [_ref(first, "one")])
    _write(tmp_path, first, "one", [_ref(second, "two"), _ref(first, "conflict")])
    _write(tmp_path, second, "two", [_ref(third, "three")])
    _write(tmp_path, third, "three")
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None)
    files = _members(path)
    assert set(files) == {artifacts.HARBOR_SUBAGENT_ISSUES}
    assert json.loads(files[artifacts.HARBOR_SUBAGENT_ISSUES])["issues"] == [
        {"trajectory_path": first, "reason": "conflicting_session"},
    ]


def test_invalid_main_and_linked_root_do_not_publish_or_touch_outside_files(tmp_path: Path) -> None:
    stale = tmp_path / artifacts.HARBOR_SUBAGENT_ARCHIVE
    stale.write_bytes(b"old")
    (tmp_path / "trajectory.json").write_bytes(b"incomplete JSON")
    assert artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None) is None
    assert not stale.exists()
    linked = tmp_path / "linked-root"
    linked.symlink_to(tmp_path, target_is_directory=True)
    stale.write_bytes(b"keep outside linked root")
    assert artifacts.prepare_subagent_archive(linked, sanitize_trajectory=lambda p: None) is None
    assert stale.read_bytes() == b"keep outside linked root"


def test_count_and_byte_limits_keep_archive_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    names = [f"trajectory.summarization-{i}-summary.json" for i in range(1, 4)]
    _write(tmp_path, "trajectory.json", "main", [_ref(name, name) for name in names])
    for name in names:
        _write(tmp_path, name, name)
    monkeypatch.setattr(artifacts, "MAX_SUBAGENT_FILES", 1)
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None)
    files = _members(path)
    issues = json.loads(files.pop(artifacts.HARBOR_SUBAGENT_ISSUES))["issues"]
    assert len(files) == 1
    assert any(issue["reason"] == "count_limit" for issue in issues)
    monkeypatch.setattr(artifacts, "MAX_SUBAGENT_FILES", 4096)
    monkeypatch.setattr(artifacts, "MAX_HARBOR_ARCHIVE_BYTES", 40 * 1024)
    (tmp_path / names[0]).write_bytes(b" " * (41 * 1024))
    path = artifacts.prepare_subagent_archive(tmp_path, sanitize_trajectory=lambda p: None)
    assert path.stat().st_size <= artifacts.MAX_HARBOR_ARCHIVE_BYTES
    assert any(
        issue["reason"] == "size_limit"
        for issue in json.loads(_members(path)[artifacts.HARBOR_SUBAGENT_ISSUES])["issues"]
    )


@pytest.mark.parametrize("name", [
    "../trajectory.summarization-1-summary.json", "/trajectory.summarization-1-summary.json",
    "trajectory.summarization-0-summary.json", "trajectory.summarization-1-other.json",
    "trajectory.summarization-1-summary.json/extra", "trajectory.summarization-1-summary.json\n",
    "trajectory.summarization-" + "1" * 100 + "-summary.json",
])
def test_name_validator_rejects_non_native_names(name: str) -> None:
    assert not artifacts.is_harbor_subagent_name(name)
