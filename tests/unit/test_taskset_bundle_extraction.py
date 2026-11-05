"""Bundle extraction preserves executable intent without trusting archive modes."""

import io
import stat
import tarfile

import pytest

from loom.taskset.materialize import (
    _BundleSizeExceededError,
    _safe_extract_tar_bytes,
    _UnsafeBundleArchiveError,
)


def _archive(entries):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        for member, body in entries:
            member.size = len(body)
            archive.addfile(member, io.BytesIO(body))
    return payload.getvalue()


@pytest.mark.parametrize("authored_mode,expected_mode", [
    (0o775, 0o755),
    (0o010, 0o755),
    (0o644, 0o644),
    (0o666, 0o644),
    (0o4775, 0o755),
    (0o2775, 0o755),
    (0o1666, 0o644),
])
def test_regular_file_modes_are_normalized(tmp_path, authored_mode, expected_mode):
    member = tarfile.TarInfo("nested/helper")
    member.mode = authored_mode
    _safe_extract_tar_bytes(_archive([(member, b"authored bytes\n")]), destination=tmp_path)
    restored = tmp_path / member.name
    assert restored.read_bytes() == b"authored bytes\n"
    assert stat.S_IMODE(restored.stat().st_mode) == expected_mode


@pytest.mark.parametrize("first_mode,last_mode", [(0o755, 0o644), (0o644, 0o755)])
def test_duplicate_regular_member_uses_last_content_and_mode(tmp_path, first_mode, last_mode):
    first = tarfile.TarInfo("helper")
    first.mode = first_mode
    last = tarfile.TarInfo("helper")
    last.mode = last_mode
    _safe_extract_tar_bytes(
        _archive([(first, b"first\n"), (last, b"last\n")]), destination=tmp_path,
    )
    assert (tmp_path / "helper").read_bytes() == b"last\n"
    assert stat.S_IMODE((tmp_path / "helper").stat().st_mode) == last_mode


@pytest.mark.parametrize("name,member_type", [
    ("../escape", tarfile.REGTYPE),
    ("/escape", tarfile.REGTYPE),
    ("nested\\escape", tarfile.REGTYPE),
    ("link", tarfile.SYMTYPE),
    ("link", tarfile.LNKTYPE),
    ("device", tarfile.CHRTYPE),
    ("fifo", tarfile.FIFOTYPE),
])
def test_executable_modes_do_not_allow_unsafe_members(tmp_path, name, member_type):
    member = tarfile.TarInfo(name)
    member.type = member_type
    member.mode = 0o7777
    member.linkname = "../escape"
    with pytest.raises(_UnsafeBundleArchiveError):
        _safe_extract_tar_bytes(_archive([(member, b"")]), destination=tmp_path)
    assert not list(tmp_path.iterdir())


def test_executable_member_still_obeys_extracted_byte_limit(tmp_path):
    member = tarfile.TarInfo("helper")
    member.mode = 0o755
    with pytest.raises(_BundleSizeExceededError):
        _safe_extract_tar_bytes(
            _archive([(member, b"too large")]), destination=tmp_path, max_bundle_bytes=2,
        )
    assert not (tmp_path / "helper").exists()
